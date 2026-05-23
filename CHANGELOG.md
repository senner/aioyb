# Changelog

All notable changes to **aioyb** are documented here.

This project adheres to [Semantic Versioning](https://semver.org/).
Until 0.1.0 the API may break between any two releases.

## 0.0.2.dev0 — 2026-05-23

### Added
- **Connection Manager (port 6433) integration.** `create_pool()` and
  `YBCluster` accept `direct_port=` (default 5433) and `conn_mgr_port=`
  (default `None` = off, set to 6433 when the cluster has
  `--enable_ysql_conn_mgr=true`). Discovery + health always go to the
  direct port so we test the real Postgres backend; app traffic goes to
  the pooler if set. The two layers are complementary — aioyb picks the
  tserver (client-side load balance), the conn mgr multiplexes within
  it (server-side pool).
- `YBCluster.app_port()` helper returns whichever port app traffic
  should use.

## 0.0.1.dev0 — 2026-05-23

Initial scaffold release. Pre-alpha; published to claim the PyPI name
and invite collaboration.

### Added
- `aioyb.create_pool(dsn, load_balance=True, topology_keys=...)` —
  drop-in replacement for `asyncpg.create_pool` that adds smart-driver
  behavior.
- `YBCluster` — bootstraps from a seed DSN and discovers live tablet
  servers via `SELECT * FROM yb_servers()`. Refreshes on a
  configurable interval (default 300 s, matching the official YB
  sync driver default).
- `HealthMonitor` — independent ping loop (default every 30 s) per
  known node. Times each `SELECT 1` and maintains an
  exponentially-weighted moving-average latency. Demotes through
  `live → degraded → dead` based on configurable thresholds
  (`degrade_threshold_ms`, `degrade_failures`, `dead_failures`).
  One successful ping rehabilitates a dead node.
- `YBCluster.healthy_nodes()` — filters out `dead` (optionally
  `degraded`) nodes and sorts the rest by `(topology_rank,
  latency_ms)`. This is the list `YBPool` rotates over for new
  connection placement.
- Topology-key parsing (`cloud.region.zone:N`) + node ranking,
  matching the official YB sync driver's syntax.
- `version_patch` — monkey-patches asyncpg's
  `_parse_server_version_string` to strip YugabyteDB's `-YB-...`
  suffix, fixing the documented `ValueError` when asyncpg connects
  to YB.
- `YBPool` — thin facade over `asyncpg.Pool` exposing the standard
  surface (`acquire` / `fetch*` / `execute` / `close`).
- Single smoke test for the version patch.
- Apache-2.0 LICENSE.
- README + `docs/STATUS.md` with the phased implementation roadmap.

### Known limitations
- Round-robin connection rotation depends on
  `asyncpg.create_pool(connect=…)` — verified on asyncpg ≥ 0.30; older
  versions silently skip the rotation. Phase 1 will replace with a
  manual pool that doesn't depend on the connect-callback path.
- No connection failover, no node-ban-on-failure. Phase 2.
- Topology fallback levels (`:1` → `:2` if no `:1` nodes available) are
  ranked but not yet "fall through" routed. Phase 3.
- No metrics or structured-log events. Phase 4.
- Tests cover only the version patch — `YBCluster` / `YBPool` lack
  integration tests that hit a real YB cluster.
