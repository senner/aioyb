"""Smallest possible aioyb example.

Run against a local YB cluster:

    podman run -d --name yb --rm -p 5433:5433 -p 7000:7000 \
        yugabytedb/yugabyte:latest \
        bin/yugabyted start --background=false

    pip install -e .
    python examples/basic.py
"""
from __future__ import annotations

import asyncio
import os

import aioyb


async def main() -> None:
    pool = await aioyb.create_pool(
        dsn=os.environ.get(
            "YB_DSN",
            "postgresql://yugabyte@127.0.0.1:5433/yugabyte",
        ),
        load_balance=True,
        # topology_keys="cloud1.region1.zone1:1",  # uncomment for multi-zone
        yb_servers_refresh_interval=60,
        min_size=2,
        max_size=10,
    )

    print(f"discovered nodes ({len(pool.cluster.preferred_nodes())}):")
    for node in pool.cluster.preferred_nodes():
        print(f"  {node.host}:{node.port}  {node.placement}  ({node.node_type})")

    async with pool.acquire() as conn:
        version = await conn.fetchval("SELECT version()")
        print(f"\nconnected to: {version}")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
