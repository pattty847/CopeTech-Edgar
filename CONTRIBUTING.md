# Contributing

Install the development environment and run the hermetic suite:

```bash
uv sync --extra dev
uv run pytest -q
python3 -m py_compile $(rg --files src/copetech_sec -g '*.py')
uv build
git diff --check
```

Tests must not contact SEC. Record minimized public SEC fixtures under
`tests/fixtures/sec/`, document their accession/source and transformations, and keep
network acquisition outside the test process.

Preserve these boundaries:

- transport raises typed SEC errors;
- resource layers translate only documented absence;
- issuer caches use CIK identity;
- raw filing documents are immutable and accession-keyed;
- parser output changes require fixtures, contract tests, and changelog entries.

Keep commits focused by subsystem and do not commit operational deployment details or
credentials.
