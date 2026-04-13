# CopeNet EDGAR parser limitations and troubleshooting

## Known limitations (as implemented)

1. **Script wrappers depend on source-layout execution**
   - `src/copetech_sec/sec_fetch_*.py` are executable helper scripts that assume source-tree execution.
   - Prefer direct package imports in applications (`from copetech_sec import SECDataFetcher`) for long-term integration.

2. **Network + SEC policy sensitivity**
   - Missing/weak user-agent strings can cause request failures or throttling.
   - SEC endpoint reliability and rate limiting can affect data completeness.

3. **Form parsing is schema-heuristic, not full ontology mapping**
   - Form 4 XML parsing targets common fields and patterns; unusual filing variants may produce partial data.

4. **Signal semantics are heuristic**
   - Signal classes and scoring are rules-based and intentionally lightweight.
   - Output is useful for triage/summary, not as authoritative trading interpretation.

5. **Cache freshness policy is simple**
   - File cache freshness logic is mostly date-based and may not satisfy strict real-time freshness requirements.

## Troubleshooting checklist

### Empty or partial insider payload
- Confirm ticker validity.
- Increase `days_back` and/or `filing_limit`.
- Ensure `SEC_API_USER_AGENT` is set.
- Retry with `use_cache=False` to bypass stale cache assumptions.

### Requests failing or timing out
- Verify outbound network access.
- Ensure `user_agent` includes a valid contact email.
- Consider increasing patience/retries at call-site orchestration level.

### Unexpected event counts after amendments
- Verify whether filings include `4/A`; amendment replacement logic intentionally supersedes base event identity matches.

### Debugging tip
Enable logging before runs:

```python
import logging
logging.basicConfig(level=logging.INFO)
```
