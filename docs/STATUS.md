# aioyb — implementation status

**Pre-alpha. Skeleton only.** Substantial work remaining; see roadmap below.

## What works today (skeleton)

- `aioyb.create_pool(dsn, load_balance=True, topology_keys=...)` API
- `YBCluster` — bootstraps from a seed DSN, discovers tservers via
  `SELECT * FROM yb_servers()`, refreshes on a configurable interval
- Topology-preference parsing + ranking (`cloud.region.zone:N` syntax
  matching the official sync driver)
- asyncpg version-string patch — strips `-YB-...` suffix before
  delegating to asyncpg's parser (fixes the `ValueError` known bug)
- Round-robin node selection at connection-creation time via a `connect=`
  callback to `asyncpg.create_pool`
- Single smoke test for the version patch

## Roadmap (in rough priority order)

### Phase 1 — make the round-robin actually work end-to-end

- [ ] Verify `asyncpg.create_pool(connect=...)` is supported on the
      pinned asyncpg version (check on >=0.30, may need newer)
- [ ] If `connect=` isn't supported on the asyncpg version we pin,
      build a manual pool that doesn't rely on it
- [ ] Acceptance test: 100 connections against a 3-node local YB
      cluster, assert ~33 land on each node

### Phase 2 — failure handling

- [ ] Connection ban: if a node fails N connect attempts in M seconds,
      stop trying it until the next refresh
- [ ] Failover: on `acquire()`-time failure, retry once against the
      next preferred node
- [ ] Distinguish primary / read-replica nodes; opt-in `read_only=True`
      pool routes to replicas only

### Phase 3 — topology / shard awareness

- [ ] `topology_keys` fallback levels (currently we just rank — needs
      "no nodes at preference 1, fall through to preference 2" logic
      with cluster-membership awareness)
- [ ] Per-shard routing via `yb_table_properties()` (advanced; mostly
      useful for read-after-write consistency)

### Phase 4 — observability

- [ ] Metrics: per-node connection count, refresh-task health,
      connect failure counts
- [ ] Structured log events on membership changes, failovers, bans

### Phase 5 — packaging + publishing

- [x] Type stubs (`py.typed`)
- [x] PyPI publish: `aioyb` (claim the name)
- [ ] CI on YugabyteDB 2024.x / 2025.x latest
- [ ] Submit upstream to YugabyteDB as an officially-supported async
      driver (their docs currently recommend `aiopg`, which wraps the
      blocking psycopg2 — a real async driver is a community gap)

## Known issues

- The skeleton's `_yb_connect` closure captures `pool_holder` by
  reference — works but is awkward. A cleaner solution is to subclass
  `asyncpg.Pool` once we confirm the `connect=` arg path. (Phase 1.)
- No connection lifetime management beyond what asyncpg gives. YB's
  tserver list can change without notice; long-lived connections may
  outlive their host. Need a soft TTL on connections + recycle on
  refresh-driven membership change.
- `cluster.refresh_now()` always connects to the seed DSN. Should
  fall back to any known node + rotate on failure.

## Why not just use psycopg3-async?

psycopg3 has an async API but the YB sync smart driver is built on
**psycopg2** — its smart features haven't been ported to psycopg3.
`aiopg` wraps psycopg2's blocking core and only achieves concurrency by
running queries in a thread pool. Neither closes the gap for native
async with smart-driver behavior. `aioyb` (built on asyncpg) does.
