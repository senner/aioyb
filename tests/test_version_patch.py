"""Unit test for the asyncpg version-string parser patch."""
from __future__ import annotations

import pytest


def test_yb_version_string_parses_after_patch():
    """asyncpg rejects YB's `11.2-YB-2.20.0.0-b0` natively; we strip
    the `-YB-` suffix before delegating."""
    import aioyb  # noqa: F401  triggers patch.apply()
    from asyncpg import serverversion

    parsed = serverversion.split_server_version_string("11.2-YB-2.20.0.0-b0")
    # asyncpg returns a namedtuple-like ServerVersion(major=..., minor=..., ...)
    # We just check the parse didn't raise + the major matches the leading 11.
    assert parsed.major == 11


def test_plain_postgres_version_still_parses():
    import aioyb  # noqa: F401
    from asyncpg import serverversion

    parsed = serverversion.split_server_version_string("15.4")
    assert parsed.major == 15


def test_patch_is_idempotent():
    from aioyb.version_patch import apply
    apply()
    apply()
    apply()
    # No assertion needed — just don't raise.


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
