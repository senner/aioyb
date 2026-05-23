"""Pool wrapping asyncpg.Pool with YB smart-driver behavior.

`aioyb.create_pool(...)` returns a `YBPool` that:
  - bootstraps a `YBCluster` from the seed DSN
  - rewrites each pool connection's host/port to round-robin across the
    topology-preferred nodes
  - shares one cluster across multiple pool instances if you pass an
    existing `cluster=` argument

This is a thin convenience layer — the actual wire protocol is asyncpg's.

Phase 2 ideas (not yet implemented):
  - per-shard routing using `yb_table_properties()`
  - connection ban on repeated failure (failover hint)
  - separate primary / read-replica pools with read_committed routing
"""
from __future__ import annotations

import itertools
import logging
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

import asyncpg

from aioyb.cluster import DEFAULT_REFRESH_INTERVAL, TServer, YBCluster

log = logging.getLogger("aioyb.pool")


class YBPool:
    """Thin facade over `asyncpg.Pool`.

    `acquire()` / `release()` / `fetch*()` calls all forward unchanged.
    The smart-driver behavior happens at connection-creation time via
    `_yb_connect`, which the underlying pool calls when it needs a new
    physical connection.
    """

    def __init__(
        self,
        cluster: YBCluster,
        pool: asyncpg.Pool,
        load_balance: bool = True,
    ):
        self._cluster = cluster
        self._pool = pool
        self._load_balance = load_balance
        self._rr_cycle: Optional[itertools.cycle] = None

    @property
    def cluster(self) -> YBCluster:
        return self._cluster

    def _next_node(self) -> Optional[TServer]:
        if not self._load_balance:
            return None
        # Healthy nodes only — HealthMonitor filters out `dead` and orders
        # by (topology_rank, latency_ms) so we naturally prefer responsive
        # nodes in the right placement zone.
        nodes = self._cluster.healthy_nodes()
        if not nodes:
            # No healthy nodes — fall back to topology-preferred so we
            # at least try to connect rather than failing outright.
            nodes = self._cluster.preferred_nodes()
            if not nodes:
                return None
        self._rr_cycle = itertools.cycle(nodes)
        return next(self._rr_cycle)

    # asyncpg.Pool surface — forward most things
    def acquire(self, *, timeout: Optional[float] = None):
        return self._pool.acquire(timeout=timeout)

    def release(self, connection):
        return self._pool.release(connection)

    async def fetch(self, *args, **kwargs):
        return await self._pool.fetch(*args, **kwargs)

    async def fetchrow(self, *args, **kwargs):
        return await self._pool.fetchrow(*args, **kwargs)

    async def fetchval(self, *args, **kwargs):
        return await self._pool.fetchval(*args, **kwargs)

    async def execute(self, *args, **kwargs):
        return await self._pool.execute(*args, **kwargs)

    async def close(self) -> None:
        try:
            await self._pool.close()
        finally:
            await self._cluster.stop()


async def create_pool(
    dsn: str,
    *,
    load_balance: bool = False,
    topology_keys: Optional[str] = None,
    yb_servers_refresh_interval: float = DEFAULT_REFRESH_INTERVAL,
    direct_port: int = 5433,
    conn_mgr_port: Optional[int] = None,
    cluster: Optional[YBCluster] = None,
    min_size: int = 1,
    max_size: int = 10,
    init: Optional[Callable[[asyncpg.Connection], Awaitable[Any]]] = None,
    **asyncpg_kwargs: Any,
) -> YBPool:
    """Drop-in `asyncpg.create_pool` plus YB smart-driver behavior.

    :param dsn: seed DSN used for the initial node discovery (any
        reachable tserver works)
    :param load_balance: round-robin new connections across known tservers
    :param topology_keys: official YB syntax,
        e.g. ``"cloud1.region1.zone1:1,cloud2.region2.zone2:2"``
    :param yb_servers_refresh_interval: how often (seconds) to re-discover
        the cluster membership
    :param direct_port: YB's direct YSQL port (default 5433). Used for
        `yb_servers()` discovery and HealthMonitor pings — we always want
        these against the real Postgres backend, not the pooler.
    :param conn_mgr_port: YB Connection Manager port (default None = off,
        commonly 6433 when enabled via `--enable_ysql_conn_mgr=true`). When
        set, application traffic is routed through the conn manager on
        each selected tserver. This stacks cleanly with client-side load
        balancing: aioyb picks the tserver, the pooler multiplexes within
        it. When None, app traffic goes direct (no server-side pooling).
    :param cluster: optional pre-built `YBCluster` (shares state across
        multiple pools — useful when one app holds primary + replica pools)
    """
    if cluster is None:
        cluster = YBCluster(
            seed_dsn=dsn,
            refresh_interval=yb_servers_refresh_interval,
            topology_keys=topology_keys,
            direct_port=direct_port,
            conn_mgr_port=conn_mgr_port,
        )
        await cluster.start()

    # Build a `connect` callable that rewrites the DSN host/port per
    # round-robin. asyncpg.create_pool accepts a `connect` arg only on
    # newer versions; if absent we fall back to plain create_pool and
    # accept that load_balance becomes a no-op until we replace the
    # pool with a manual implementation.
    async def _yb_connect(**_kwargs):
        node = pool_holder["pool"]._next_node() if "pool" in pool_holder else None
        if node is None:
            return await asyncpg.connect(dsn=dsn, **asyncpg_kwargs)
        # App traffic uses cluster.app_port() — pooler (6433) if configured,
        # else direct (5433). Either way the host is the selected tserver.
        port = cluster.app_port() if cluster is not None else node.port
        return await asyncpg.connect(
            host=node.host,
            port=port,
            **{k: v for k, v in asyncpg_kwargs.items() if k not in ("host", "port")},
        )

    pool_holder: dict[str, YBPool] = {}

    inner_pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=min_size,
        max_size=max_size,
        init=init,
        connect=_yb_connect,
        **asyncpg_kwargs,
    )

    yb_pool = YBPool(cluster=cluster, pool=inner_pool, load_balance=load_balance)
    pool_holder["pool"] = yb_pool
    return yb_pool
