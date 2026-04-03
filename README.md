# CopeTech-Edgar

Reusable EDGAR/SEC backend extracted from Sentinel.

## What it provides
- `SECDataFetcher` facade/orchestrator
- Form 4 insider transaction processing
- company facts / financial summaries
- filing/document fetching helpers
- optional SQL/cache helpers
- supply chain parsing utilities

## Install (local editable)

```bash
uv pip install -e /Users/copeharder/Programming/CopeTech-Edgar
```

Or from a consumer `pyproject.toml` via a local path source.

## Usage

```python
from copetech_sec import SECDataFetcher
```

This package preserves Sentinel's existing SEC backend behavior as closely as possible while making it reusable across projects.
