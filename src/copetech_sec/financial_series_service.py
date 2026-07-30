"""Acquisition, persistence, and query facade for canonical financial series."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from .financial_metrics import list_supported_metrics
from .financial_series import extract_financial_facts, resolve_financial_series
from .financial_series_store import FinancialSeriesStore


FetchFacts = Callable[[str, bool], Awaitable[Optional[dict[str, Any]]]]


class FinancialSeriesService:
    def __init__(self, fetch_facts: FetchFacts, store_path: str | Path):
        self._fetch_facts = fetch_facts
        self._store = FinancialSeriesStore(store_path)

    @staticmethod
    def supported_metrics() -> list[dict[str, Any]]:
        return list_supported_metrics()

    async def get_series(
        self,
        symbol: str,
        *,
        metric: str = "revenue",
        frequency: str = "quarterly",
        basis: str = "canonical",
        alignment: str = "availability",
        as_of: str | None = None,
        start: str | None = None,
        end: str | None = None,
        refresh: bool = False,
        include_provenance: bool = True,
    ) -> dict[str, Any] | None:
        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        facts = await self._fetch_facts(normalized, use_cache=not refresh)
        source_warning = None
        if facts:
            retrieved_at = datetime.now(timezone.utc).isoformat()
            extracted = extract_financial_facts(
                facts,
                symbol=normalized,
                metric=metric,
                retrieved_at=retrieved_at,
            )
            await self._store.upsert_facts(extracted)
        else:
            source_warning = "source_refresh_failed_using_persisted_facts"
        rows = await self._store.load_facts(normalized, metric)
        if not rows:
            return None
        payload = resolve_financial_series(
            rows,
            symbol=normalized,
            metric=metric,
            frequency=frequency,
            basis=basis,
            alignment=alignment,
            as_of=as_of,
            start=start,
            end=end,
        )
        payload["retrievedAt"] = max(
            (str(row.get("retrieved_at") or "") for row in rows),
            default=None,
        )
        payload["rawFactCount"] = len(rows)
        if source_warning:
            payload["warnings"] = sorted(
                set(payload.get("warnings") or []) | {source_warning}
            )
        if not include_provenance:
            for observation in payload["observations"]:
                observation.pop("sources", None)
        return payload
