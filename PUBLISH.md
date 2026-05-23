# Publishing aioyb

Build artifacts live in `dist/`. Both pass `twine check`. This doc has
the commands to upload them; running them is up to you (uploads are
irreversible name claims on the target index).

## Build (already done)

    python -m build
    python -m twine check dist/*

Re-run if `pyproject.toml`, README, source, or version changes.

## GitHub repo description

Use these when filling in the new repo at https://github.com/senner/aioyb:

**Short description** (`About` field, ≤ 350 chars, shows on the repo card):

> Async smart-driver features for YugabyteDB on asyncpg — topology-aware
> load balancing, automatic tablet-server discovery via `yb_servers()`,
> and a fix for asyncpg's known YB version-string parsing bug. Drop-in
> replacement for `asyncpg.create_pool`.

**Topics** (the chip list under About):

    yugabytedb  asyncpg  asyncio  postgresql  python  smart-driver
    load-balancing  distributed-sql  database  ysql

**Website** (URL field): `https://pypi.org/project/aioyb/`
(once published)

## First-time push to GitHub

    cd /Users/ttsnet/sourcecode/aioyb
    git add .
    git commit -m "Initial scaffold"
    git branch -M main
    git remote add origin git@github.com:senner/aioyb.git
    git push -u origin main

## Upload to Wildcard private devpi (`https://bteamg.wildcardcorp.com/pypi/`)

devpi is the internal Wildcard index. The server is auth-gated — every
read and write requires a valid devpi user account (the `pypi/` route on
Caddy uses `forward_auth` against devpi-lockdown). Anonymous enumeration
returns 401, so `devpi use -l` won't work until you've logged in.

**Setup once per machine:**

    pip install --user devpi-client
    ~/.local/bin/devpi use https://bteamg.wildcardcorp.com/pypi/
    ~/.local/bin/devpi login <your-devpi-username>
    # ↑ prompts for password OR uses a devpi token if you have one
    ~/.local/bin/devpi use -l                       # list visible indexes
    ~/.local/bin/devpi use <user>/<index>           # pin to e.g. wildcard/dev

**Upload from the project root:**

    ~/.local/bin/devpi upload --no-vcs \
        dist/aioyb-0.0.1.dev0.tar.gz \
        dist/aioyb-0.0.1.dev0-py3-none-any.whl

Or with twine + a devpi token:

    python -m twine upload \
        --repository-url https://bteamg.wildcardcorp.com/pypi/<user>/<index>/+upload \
        --username <user> --password <devpi-token> \
        dist/*

## Upload to PyPI (public)

Requires a PyPI API token in `~/.pypirc` (or pass via env / flags).

**Test on TestPyPI first** to catch metadata problems before claiming the
public name:

    python -m twine upload --repository testpypi dist/*

Verify the install + import:

    pip install --index-url https://test.pypi.org/simple/ \
                --extra-index-url https://pypi.org/simple/ aioyb
    python -c "import aioyb; print(aioyb.__version__)"

Then real PyPI:

    python -m twine upload dist/*

## After publish

- Tag the commit: `git tag v0.0.1.dev0 && git push --tags`
- Bump `version` in `pyproject.toml` for the next iteration (don't
  re-publish the same version — PyPI rejects, devpi may accept but
  it's bad hygiene).
- Update `CHANGELOG.md` for the next version.

## Verify metadata before uploading

Quick sanity check that the rendered metadata + long description look
right:

    python -m twine check dist/*
    python -m pip show -v aioyb               # if installed
    tar -tzf dist/aioyb-0.0.1.dev0.tar.gz     # see what's in the sdist
    unzip -l dist/aioyb-0.0.1.dev0-py3-none-any.whl
