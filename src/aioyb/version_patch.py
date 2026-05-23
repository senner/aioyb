"""Fix asyncpg's server-version parser to accept YugabyteDB's version string.

asyncpg uses `_parse_server_version_string()` which assumes the server
returns a strictly-integer version like ``"15.4"``. YugabyteDB returns
strings like ``"11.2-YB-2.20.0.0-b0"`` — the leading ``11.2`` is the
PostgreSQL-compat version, then ``-YB-`` marks the start of the YB
build identifier.

asyncpg's strict parser raises ``ValueError`` trying to int-convert the
``"2-YB-2"`` suffix. We monkey-patch its parser to drop everything from
``-YB-`` onward before delegating to the original parser, so YB connections
succeed without touching asyncpg itself.

Applied automatically on `import aioyb`. Idempotent.
"""
from __future__ import annotations

import re

_YB_SUFFIX_RE = re.compile(r"-YB-.*$", re.IGNORECASE)

_applied = False


def apply() -> None:
    """Monkey-patch asyncpg's server-version parser. Safe to call repeatedly."""
    global _applied
    if _applied:
        return
    try:
        from asyncpg import serverversion
    except ImportError:
        # asyncpg not installed in this environment — no-op.
        return

    original = serverversion.split_server_version_string

    def yb_aware_split(version_string: str):
        cleaned = _YB_SUFFIX_RE.sub("", version_string).strip()
        if not cleaned:
            # Fall back to original — let asyncpg raise its own error.
            return original(version_string)
        return original(cleaned)

    serverversion.split_server_version_string = yb_aware_split
    _applied = True
