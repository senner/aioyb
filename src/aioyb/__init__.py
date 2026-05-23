"""aioyb — async smart-driver features for YugabyteDB via asyncpg.

Public surface mirrors asyncpg's: drop-in `create_pool` that adds:
  - topology-aware load balancing
  - cluster-wide tablet-server discovery via `yb_servers()`
  - automatic refresh of the node list
  - asyncpg version-string parsing fix for YB's mixed format

See `README.md` for the design.

Usage::

    import aioyb
    pool = await aioyb.create_pool(
        dsn="postgresql://yugabyte@host:5433/mydb",
        load_balance=True,
        topology_keys="aws.us-east-1.us-east-1a:1",
    )
"""
from __future__ import annotations

__version__ = "0.0.1.dev0"

# Apply the version-string parsing patch on import so callers don't have
# to remember it explicitly. The patch is idempotent.
from aioyb.version_patch import apply as _apply_version_patch  # noqa: E402

_apply_version_patch()

from aioyb.cluster import YBCluster  # noqa: E402
from aioyb.health import HealthMonitor, NodeHealth  # noqa: E402
from aioyb.pool import YBPool, create_pool  # noqa: E402

__all__ = [
    "__version__",
    "YBCluster",
    "YBPool",
    "HealthMonitor",
    "NodeHealth",
    "create_pool",
]
