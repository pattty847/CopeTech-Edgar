# Error and resource contract

CopeTech-Edgar distinguishes “the SEC has no such resource” from “we failed to determine
whether it exists.”

## Transport errors

All SEC acquisition failures derive from `CopeTechEdgarError`:

- `SecNotFoundError`: HTTP 404. Resource APIs may translate this to `None` when absence is
  part of their documented domain.
- `SecAccessDeniedError`: SEC rejected the caller or user agent.
- `SecRateLimitError`: throttling continued after the retry budget.
- `SecTransportError`: connection or timeout failure after retries.
- `SecMalformedResponseError`: a successful response did not match the requested JSON
  representation.
- `SecResponseTooLargeError`: the bounded response ceiling was exceeded.
- `SecRequestError`: another HTTP failure.

Only `SecNotFoundError` is routinely translated into an empty domain result. Retrying,
authentication, malformed-response, and transport failures propagate so consumers can
mark evidence unavailable or stale instead of reporting false absence.

The optional FastAPI service maps upstream absence to 404, temporary transport/throttle
failures to 503, and other invalid upstream responses to 502.

## Filing search metadata

`SECDataFetcher.get_filings_page()` returns:

```json
{
  "items": [],
  "metadata": {
    "retrievedAt": "ISO-8601 timestamp",
    "source": "sec-submissions",
    "sourceFiles": ["CIK0000320193.json"],
    "truncated": false,
    "nextCursor": null,
    "warnings": []
  }
}
```

The resource traverses `submissions.filings.files[]` when a requested window reaches
beyond the `recent` block. `limit` and `cursor` page the normalized, accession-deduplicated
timeline. The legacy `get_filings_by_form()` API remains a compatibility wrapper returning
only `items`.

## Cache identity

Issuer resources and derived filing indexes are keyed by canonical ten-digit CIK, not
ticker. Tickers are mutable lookup aliases; CIKs identify SEC registrants. The ticker map
has a 24-hour TTL and is stored with retrieval metadata. Historical submissions files are
cached by their SEC filename.
