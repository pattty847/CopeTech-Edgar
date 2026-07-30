"""Acquisition, persistence, and query facade for canonical financial series."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from .financial_metrics import list_supported_metrics
from .eps_series import resolve_diluted_eps_ttm
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

    async def _refresh_and_load(
        self,
        symbol: str,
        *,
        metric: str,
        refresh: bool,
    ) -> tuple[list[dict[str, Any]], str | None]:
        facts = await self._fetch_facts(symbol, use_cache=not refresh)
        source_warning = None
        if facts:
            retrieved_at = datetime.now(timezone.utc).isoformat()
            extracted = extract_financial_facts(
                facts,
                symbol=symbol,
                metric=metric,
                retrieved_at=retrieved_at,
            )
            await self._store.append_facts(extracted)
        else:
            source_warning = "source_refresh_failed_using_persisted_facts"
        return await self._store.load_facts(symbol, metric), source_warning

    async def _refresh_metrics_and_load(
        self,
        symbol: str,
        *,
        metrics: tuple[str, ...],
        refresh: bool,
    ) -> tuple[dict[str, list[dict[str, Any]]], str | None]:
        facts = await self._fetch_facts(symbol, use_cache=not refresh)
        source_warning = None
        if facts:
            retrieved_at = datetime.now(timezone.utc).isoformat()
            for metric in metrics:
                await self._store.append_facts(
                    extract_financial_facts(
                        facts,
                        symbol=symbol,
                        metric=metric,
                        retrieved_at=retrieved_at,
                    )
                )
        else:
            source_warning = "source_refresh_failed_using_persisted_facts"
        loaded = {
            metric: await self._store.load_facts(symbol, metric)
            for metric in metrics
        }
        return loaded, source_warning

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
        split_events: list[tuple[str, float]] | None = None,
    ) -> dict[str, Any] | None:
        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        canonical_eps_ttm = (
            metric == "diluted_eps"
            and frequency == "ttm"
            and basis == "canonical"
        )
        if canonical_eps_ttm:
            loaded, source_warning = await self._refresh_metrics_and_load(
                normalized,
                metrics=("diluted_eps", "diluted_shares"),
                refresh=refresh,
            )
            rows = loaded["diluted_eps"]
            supporting_rows = loaded["diluted_shares"]
        else:
            rows, source_warning = await self._refresh_and_load(
                normalized,
                metric=metric,
                refresh=refresh,
            )
            supporting_rows = []
        if not rows:
            return None
        if canonical_eps_ttm:
            payload = resolve_diluted_eps_ttm(
                rows,
                supporting_rows,
                symbol=normalized,
                split_events=split_events,
                alignment=alignment,
                as_of=as_of,
                start=start,
                end=end,
            )
        else:
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
        if canonical_eps_ttm:
            payload["supportingRawFactCount"] = len(supporting_rows)
        if source_warning:
            payload["warnings"] = sorted(
                set(payload.get("warnings") or []) | {source_warning}
            )
        if not include_provenance:
            for observation in payload["observations"]:
                observation.pop("sources", None)
                observation.pop("availabilitySource", None)
                observation.pop("selectedSource", None)
        return payload

    async def get_valuation_series(
        self,
        symbol: str,
        *,
        price_observations: list[dict[str, Any]],
        split_events: list[tuple[str, float]] | None = None,
        price_source: str = "caller",
        price_basis: str = "split_adjusted",
        refresh: bool = False,
        include_provenance: bool = True,
    ) -> dict[str, Any] | None:
        from .valuation_series import derive_trailing_pe_series

        normalized = symbol.strip().upper()
        if not normalized:
            raise ValueError("symbol is required")
        loaded, source_warning = await self._refresh_metrics_and_load(
            normalized,
            metrics=("diluted_eps", "diluted_shares"),
            refresh=refresh,
        )
        rows = loaded["diluted_eps"]
        diluted_share_rows = loaded["diluted_shares"]
        if not rows:
            return None
        payload = derive_trailing_pe_series(
            rows,
            price_observations,
            symbol=normalized,
            diluted_share_rows=diluted_share_rows,
            split_events=split_events,
            price_source=price_source,
            price_basis=price_basis,
            include_provenance=include_provenance,
        )
        payload["retrievedAt"] = max(
            (str(row.get("retrieved_at") or "") for row in rows),
            default=None,
        )
        payload["rawFactCount"] = len(rows)
        payload["supportingRawFactCount"] = len(diluted_share_rows)
        if source_warning:
            payload["warnings"] = sorted(
                set(payload.get("warnings") or []) | {source_warning}
            )
        return payload
