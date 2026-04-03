# SEC Backend Extraction Plan

## Goal
Extract Sentinel's SEC backend into a standalone reusable Python package with minimal behavior changes and minimal Sentinel breakage.

## Steps
1. Copy `Sentinel/scripts/sec/*.py` into `src/copetech_sec/`.
2. Export `SECDataFetcher` from `src/copetech_sec/__init__.py`.
3. Add package metadata in `pyproject.toml` and a usage-focused `README.md`.
4. Update Sentinel SEC wrapper scripts to import `copetech_sec` instead of using `sys.path` hacks and `sec.*` imports.
5. Add a local path dependency from `Sentinel/scripts` to `copetech-sec`.
6. Run minimal import/smoke verification.

## Low-risk choices
- Keep module internals mostly unchanged.
- Keep Sentinel wrapper scripts thin.
- Avoid redesigning public behavior during extraction.

## Follow-up ideas
- Extract CLI wrappers into package console scripts later.
- Add shared `copetech-market-core` models if multiple projects need common schemas.
