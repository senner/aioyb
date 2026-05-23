"""Active health + latency monitor for the discovered YB tablet servers.

`yb_servers()` tells us which nodes the cluster THINKS exist; it doesn't
tell us which of those are actually reachable from THIS client, or how
fast each one responds. The HealthMonitor closes that gap by:

  - Periodically pinging every known node (default every 30 s — faster
    than the node-list refresh interval).
  - Timing the ping (`SELECT 1`) to maintain a moving-average per-node
    latency.
  - Demoting a node to `degraded` after `degrade_threshold_ms` exceeded
    OR `degrade_failures` consecutive failed pings.
  - Demoting to `dead` after `dead_failures` consecutive failures —
    nodes in `dead` are excluded from `healthy_nodes()` until a ping
    succeeds again.
  - Promoting back to `live` on the first successful ping after demotion.

`YBCluster.healthy_nodes()` consults this monitor, filters to status==
`live` or `degraded`, and sorts by ascending latency. `YBPool` uses that
list when handing out new connections — so the round-robin only ever
lands on responsive, low-latency tservers.

Configurable thresholds let you tune for either "fail fast in
geo-distributed" or "tolerate brief blips in single-region" workloads.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

import asyncpg

if TYPE_CHECKING:
    from aioyb.cluster import TServer

log = logging.getLogger("aioyb.health")

DEFAULT_PING_INTERVAL = 30.0           # seconds between health pings
DEFAULT_PING_TIMEOUT = 3.0             # seconds before a ping is failure
DEFAULT_DEGRADE_THRESHOLD_MS = 250.0   # latency above which a node is degraded
DEFAULT_DEGRADE_FAILURES = 1           # failed pings before degraded
DEFAULT_DEAD_FAILURES = 3              # failed pings before dead
DEFAULT_LATENCY_EMA_ALPHA = 0.3        # 0..1; higher = react faster to changes


@dataclass
class NodeHealth:
    """Per-node liveness + observed latency."""
    address: str               # "host:port" — primary key
    status: str = "unknown"    # unknown | live | degraded | dead
    latency_ms: float = 0.0    # exponentially-weighted moving average
    last_checked: float = 0.0  # epoch seconds
    consecutive_failures: int = 0
    last_error: Optional[str] = None


class HealthMonitor:
    """Background task that pings every known node and tracks NodeHealth."""

    def __init__(
        self,
        *,
        ping_interval: float = DEFAULT_PING_INTERVAL,
        ping_timeout: float = DEFAULT_PING_TIMEOUT,
        degrade_threshold_ms: float = DEFAULT_DEGRADE_THRESHOLD_MS,
        degrade_failures: int = DEFAULT_DEGRADE_FAILURES,
        dead_failures: int = DEFAULT_DEAD_FAILURES,
        latency_ema_alpha: float = DEFAULT_LATENCY_EMA_ALPHA,
        connect_kwargs: Optional[dict] = None,
    ):
        self.ping_interval = ping_interval
        self.ping_timeout = ping_timeout
        self.degrade_threshold_ms = degrade_threshold_ms
        self.degrade_failures = degrade_failures
        self.dead_failures = dead_failures
        self.latency_ema_alpha = latency_ema_alpha
        # Extra asyncpg.connect kwargs (user, database, password, ssl …)
        self.connect_kwargs = connect_kwargs or {}

        self._health: dict[str, NodeHealth] = {}
        self._loop_task: Optional[asyncio.Task] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._nodes_provider = lambda: []  # set by start(); returns list[TServer]

    def health_of(self, address: str) -> NodeHealth:
        return self._health.get(address, NodeHealth(address=address))

    def all(self) -> dict[str, NodeHealth]:
        return dict(self._health)

    def healthy_addresses(self, *, include_degraded: bool = True) -> list[str]:
        """Live (and optionally degraded) addresses, sorted by ascending latency."""
        accepted = {"live", "degraded"} if include_degraded else {"live"}
        items = [
            h for h in self._health.values()
            if h.status in accepted
        ]
        items.sort(key=lambda h: (h.status != "live", h.latency_ms))
        return [h.address for h in items]

    async def start(self, nodes_provider) -> None:
        """Kick off the background ping loop.

        :param nodes_provider: 0-arg callable returning current list[TServer].
            Re-evaluated each cycle so we pick up cluster membership changes.
        """
        self._nodes_provider = nodes_provider
        self._stop_event = asyncio.Event()
        # Prime the table with whatever nodes exist now so consumers see
        # something other than an empty dict before the first cycle.
        await self._cycle()
        self._loop_task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        if self._loop_task is not None:
            try:
                await self._loop_task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.ping_interval,
                )
            except asyncio.TimeoutError:
                try:
                    await self._cycle()
                except Exception as exc:
                    log.warning("aioyb health cycle failed: %r", exc)

    async def _cycle(self) -> None:
        nodes = list(self._nodes_provider())
        if not nodes:
            return
        await asyncio.gather(
            *(self._ping(n) for n in nodes),
            return_exceptions=True,
        )

    async def _ping(self, node: "TServer") -> None:
        address = f"{node.host}:{node.port}"
        entry = self._health.setdefault(address, NodeHealth(address=address))
        started = time.monotonic()
        try:
            conn = await asyncio.wait_for(
                asyncpg.connect(host=node.host, port=node.port, **self.connect_kwargs),
                timeout=self.ping_timeout,
            )
            try:
                await asyncio.wait_for(
                    conn.fetchval("SELECT 1"),
                    timeout=self.ping_timeout,
                )
            finally:
                await conn.close()
        except Exception as exc:  # asyncpg errors, timeouts, OS-level connect errors
            entry.consecutive_failures += 1
            entry.last_error = repr(exc)
            entry.last_checked = time.time()
            if entry.consecutive_failures >= self.dead_failures:
                entry.status = "dead"
            elif entry.consecutive_failures >= self.degrade_failures:
                entry.status = "degraded"
            log.debug("ping %s FAIL (%d/%d): %r",
                      address, entry.consecutive_failures, self.dead_failures, exc)
            return

        elapsed_ms = (time.monotonic() - started) * 1000.0
        # EMA over recent latencies — smooths transient spikes without
        # hiding sustained regressions.
        if entry.latency_ms == 0:
            entry.latency_ms = elapsed_ms
        else:
            entry.latency_ms = (
                self.latency_ema_alpha * elapsed_ms
                + (1.0 - self.latency_ema_alpha) * entry.latency_ms
            )
        entry.consecutive_failures = 0
        entry.last_error = None
        entry.last_checked = time.time()
        if entry.latency_ms > self.degrade_threshold_ms:
            entry.status = "degraded"
        else:
            entry.status = "live"
        log.debug("ping %s OK   %.1fms (ema=%.1fms) status=%s",
                  address, elapsed_ms, entry.latency_ms, entry.status)
