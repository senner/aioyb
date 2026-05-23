"""Cluster state — periodically discovers YB tablet servers and applies
topology preferences.

Public API:
    cluster = YBCluster(seed_dsn, refresh_interval=300, topology_keys=None)
    await cluster.start()              # bootstraps + starts refresh task
    nodes = cluster.preferred_nodes()  # ordered list per topology_keys
    await cluster.stop()

The cluster keeps the current node list in memory and refreshes it on a
fixed interval (default 300s, matching the official YB sync driver
default). Each refresh runs `SELECT host, port, node_type, cloud, region,
zone, public_ip FROM yb_servers()` against any currently-known node.

Topology preference syntax matches the official YB drivers:
    "cloud1.region1.zone1:1,cloud2.region2.zone2:2,cloud3.region3.zone3:3"
where `:N` is the preference rank (1 = primary, 2 = first fallback, ...).
Nodes with no matching placement get the lowest preference.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

import asyncpg

log = logging.getLogger("aioyb.cluster")

DEFAULT_REFRESH_INTERVAL = 300  # seconds, matches official YB driver default


@dataclass(frozen=True)
class TServer:
    """One YB tablet server."""
    host: str
    port: int
    node_type: str          # primary | read_replica
    cloud: Optional[str]
    region: Optional[str]
    zone: Optional[str]
    public_ip: Optional[str]

    @property
    def placement(self) -> str:
        """`cloud.region.zone` triple, used for topology_keys matching."""
        return f"{self.cloud or '*'}.{self.region or '*'}.{self.zone or '*'}"


@dataclass
class _TopologyKey:
    cloud: str
    region: str
    zone: str
    preference: int


def _parse_topology_keys(spec: str) -> list[_TopologyKey]:
    """`"aws.us-east-1.zone-a:1,aws.us-west-2.zone-b:2"` -> list of keys."""
    if not spec:
        return []
    keys: list[_TopologyKey] = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        placement, _, pref = part.partition(":")
        cloud, region, zone = (placement.split(".") + ["", ""])[:3]
        try:
            pref_int = int(pref) if pref else 1
        except ValueError:
            pref_int = 1
        keys.append(_TopologyKey(cloud, region, zone, pref_int))
    return keys


def _topology_rank(server: TServer, keys: list[_TopologyKey]) -> int:
    """Lower rank = more preferred. No-match servers get a large rank."""
    if not keys:
        return 0
    for key in keys:
        if (
            (key.cloud == "*" or key.cloud == (server.cloud or ""))
            and (key.region == "*" or key.region == (server.region or ""))
            and (key.zone == "*" or key.zone == (server.zone or ""))
        ):
            return key.preference
    return 9999  # no-match fallback


@dataclass
class YBCluster:
    seed_dsn: str
    refresh_interval: float = DEFAULT_REFRESH_INTERVAL
    topology_keys: Optional[str] = None
    # YugabyteDB tserver exposes two YSQL ports:
    #   5433 — direct YSQL (the real Postgres backend). Used for DDL,
    #          migrations, `yb_servers()` discovery, and HealthMonitor
    #          pings (we want to test the backend, not the pooler).
    #   6433 — Connection Manager (optional, requires
    #          `--enable_ysql_conn_mgr=true`). Transaction-mode pooler
    #          baked into newer YB versions; cuts per-tserver memory
    #          overhead on high-fan-in workloads.
    # `direct_port` is always used for discovery + health. `conn_mgr_port`
    # is used for app traffic when set; if None, app traffic also takes
    # the direct port (i.e. no server-side pooling). The two are
    # complementary, not competing: aioyb does client-side load balancing
    # across tservers, the conn mgr multiplexes within each one.
    direct_port: int = 5433
    conn_mgr_port: Optional[int] = None

    # populated at runtime
    _nodes: list[TServer] = field(default_factory=list)
    _refresh_task: Optional[asyncio.Task] = None
    _stop_event: Optional[asyncio.Event] = None
    _parsed_keys: list[_TopologyKey] = field(default_factory=list)
    _health: object = None  # HealthMonitor; typed `object` to dodge circular import

    def __post_init__(self):
        self._parsed_keys = _parse_topology_keys(self.topology_keys or "")

    def app_port(self) -> int:
        """Port that application traffic uses — pooler if configured, else direct."""
        return self.conn_mgr_port if self.conn_mgr_port is not None else self.direct_port

    async def start(self, *, health: object = None, health_connect_kwargs: Optional[dict] = None) -> None:
        """Bootstrap node list and start periodic refresh + health monitor.

        :param health: optional pre-built `HealthMonitor`. If omitted, one
            is created with default thresholds.
        :param health_connect_kwargs: forwarded to the default HealthMonitor's
            `asyncpg.connect()` (user / database / password / ssl).
        """
        self._stop_event = asyncio.Event()
        await self.refresh_now()
        if health is None:
            from aioyb.health import HealthMonitor  # local import: avoid cycle
            health = HealthMonitor(connect_kwargs=health_connect_kwargs)
        self._health = health
        await self._health.start(self._nodes_snapshot)  # type: ignore[attr-defined]
        self._refresh_task = asyncio.create_task(self._refresh_loop())

    async def stop(self) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        if self._health is not None:
            await self._health.stop()  # type: ignore[attr-defined]
        if self._refresh_task is not None:
            try:
                await self._refresh_task
            except asyncio.CancelledError:
                pass

    def _nodes_snapshot(self) -> list[TServer]:
        """0-arg callable handed to HealthMonitor so it always sees the
        current node list even after a refresh swaps it out."""
        return list(self._nodes)

    async def refresh_now(self) -> list[TServer]:
        """Force-refresh the node list against the cluster."""
        connect_dsn = self._nodes[0].host if self._nodes else None
        # On first call self._nodes is empty — use the seed DSN directly.
        async with await asyncpg.connect(self.seed_dsn) as conn:
            # `yb_servers()` is a YB built-in returning the live tserver list.
            rows = await conn.fetch(
                """
                SELECT host, port, node_type, cloud, region, zone, public_ip
                  FROM yb_servers()
                """
            )
        nodes = [
            TServer(
                host=row["host"],
                port=row["port"],
                node_type=row["node_type"],
                cloud=row["cloud"],
                region=row["region"],
                zone=row["zone"],
                public_ip=row["public_ip"],
            )
            for row in rows
        ]
        self._nodes = nodes
        log.debug("aioyb: discovered %d tservers", len(nodes))
        return nodes

    def preferred_nodes(self) -> list[TServer]:
        """Current node list, sorted by topology preference (most → least)."""
        if not self._parsed_keys:
            return list(self._nodes)
        return sorted(
            self._nodes,
            key=lambda s: _topology_rank(s, self._parsed_keys),
        )

    def healthy_nodes(self, *, include_degraded: bool = True) -> list[TServer]:
        """Nodes that are responsive AND topology-preferred.

        Two-stage filter:
          1. Drop anything the HealthMonitor marked `dead` (or `degraded`
             if `include_degraded=False`).
          2. Among survivors, sort by topology preference first, then
             ascending latency within each preference tier.

        Returns the list `YBPool` uses for round-robin connection selection.
        """
        if self._health is None:
            return self.preferred_nodes()
        addr_to_node = {f"{n.host}:{n.port}": n for n in self._nodes}
        ok_addrs = self._health.healthy_addresses(  # type: ignore[attr-defined]
            include_degraded=include_degraded,
        )
        alive: list[TServer] = []
        for addr in ok_addrs:
            node = addr_to_node.get(addr)
            if node is not None:
                alive.append(node)
        # Sort by (topology_rank, latency_ms) so preferred zones win,
        # ties broken by who's actually fast right now.
        def _key(node: TServer) -> tuple[int, float]:
            rank = _topology_rank(node, self._parsed_keys)
            lat = self._health.health_of(f"{node.host}:{node.port}").latency_ms  # type: ignore[attr-defined]
            return (rank, lat)
        alive.sort(key=_key)
        return alive

    async def _refresh_loop(self) -> None:
        assert self._stop_event is not None
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(),
                    timeout=self.refresh_interval,
                )
            except asyncio.TimeoutError:
                # interval elapsed without stop — do the refresh
                try:
                    await self.refresh_now()
                except Exception as exc:
                    log.warning("aioyb: refresh failed: %r", exc)
