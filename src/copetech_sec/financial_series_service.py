"""Acquisition, persistence, and query facade for canonical financial series."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Optional

from .financial_metrics import list_supported_metrics
from .derived_series import (
    get_derived_definition,
    is_derived_metric,
    list_derived_metrics,
    resolve_derived_series,
)
from .eps_series import resolve_diluted_eps_ttm
from .financial_series import extract_financial_facts, resolve_financial_series
from .financial_series_store import FinancialSeriesStore


FetchFacts = Callable[[str, bool], Awaitable[Optional[dict[str, Any]]]]

# Market-cap multiples served by the generic trailing-multiple engine. The
# denominator is either a TTM flow (kind "ttm": a base metric or a derived
# composite resolved from its components' TTM windows) or a point-in-time
# balance (kind "instant"). `invert` turns a multiple into a yield;
# `adjustment` names a derived instant series added to the market cap for
# enterprise-value numerators.
TRAILING_MULTIPLE_METRICS: dict[str, dict[str, Any]] = {
    "trailing_ps": {
        "label": "Trailing P/S",
        "denominator": "revenue",
        "components": ("revenue",),
        "kind": "ttm",
    },
    "trailing_pfcf": {
        "label": "Trailing P/FCF",
        "denominator": "fcf",
        "components": ("operating_cash_flow", "capex"),
        "kind": "ttm",
    },
    "trailing_pb": {
        "label": "Trailing P/B",
        "denominator": "stockholders_equity",
        "components": ("stockholders_equity",),
        "kind": "instant",
    },
    "fcf_yield": {
        "label": "FCF yield",
        "denominator": "fcf",
        "components": ("operating_cash_flow", "capex"),
        "kind": "ttm",
        "invert": True,
    },
    "ev_s": {
        "label": "EV/S",
        "denominator": "revenue",
        "components": ("revenue",),
        "kind": "ttm",
        "adjustment": "net_debt",
    },
}


class FinancialSeriesService:
    def __init__(self, fetch_facts: FetchFacts, store_path: str | Path):
        self._fetch_facts = fetch_facts
        self._store = FinancialSeriesStore(store_path)

    @staticmethod
    def supported_metrics() -> list[dict[str, Any]]:
        return list_supported_metrics() + list_derived_metrics()

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
        # Facts are keyed to the issuer, so read by CIK: a second ticker on the same CIK
        # (GOOG/GOOGL) never gets its own rows, because the first one already wrote them.
        cik = (facts or {}).get("cik")
        return await self._store.load_facts(symbol, metric, cik=cik), source_warning

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
        cik = (facts or {}).get("cik")
        loaded = {
            metric: await self._store.load_facts(symbol, metric, cik=cik)
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
        if is_derived_metric(metric):
            return await self._get_derived_series(
                normalized,
                metric=metric,
                frequency=frequency,
                basis=basis,
                alignment=alignment,
                as_of=as_of,
                start=start,
                end=end,
                refresh=refresh,
                include_provenance=include_provenance,
            )
        canonical_eps_ttm = (
            metric == "diluted_eps"
            and frequency == "ttm"
            and basis == "canonical"
        )
        if canonical_eps_ttm:
            loaded, source_warning = await self._refresh_metrics_and_load(
                normalized,
                metrics=("diluted_eps", "diluted_shares", "net_income"),
                refresh=refresh,
            )
            rows = loaded["diluted_eps"]
            supporting_rows = loaded["diluted_shares"]
            income_rows = loaded["net_income"]
        else:
            rows, source_warning = await self._refresh_and_load(
                normalized,
                metric=metric,
                refresh=refresh,
            )
            supporting_rows = []
            income_rows = []
        if not rows:
            return None
        if canonical_eps_ttm:
            payload = resolve_diluted_eps_ttm(
                rows,
                supporting_rows,
                symbol=normalized,
                split_events=split_events,
                net_income_rows=income_rows,
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

    async def _get_derived_series(
        self,
        symbol: str,
        *,
        metric: str,
        frequency: str,
        basis: str,
        alignment: str,
        as_of: str | None,
        start: str | None,
        end: str | None,
        refresh: bool,
        include_provenance: bool,
    ) -> dict[str, Any] | None:
        definition = get_derived_definition(metric)
        components = definition.required + definition.optional
        loaded, source_warning = await self._refresh_metrics_and_load(
            symbol,
            metrics=components,
            refresh=refresh,
        )
        all_rows = [row for rows in loaded.values() for row in rows]
        if not all_rows:
            return None
        component_payloads = {
            component: resolve_financial_series(
                rows,
                symbol=symbol,
                metric=component,
                frequency=frequency,
                basis=basis,
                alignment=alignment,
                as_of=as_of,
                start=start,
                end=end,
            )
            for component, rows in loaded.items()
            if rows
        }
        payload = resolve_derived_series(
            component_payloads,
            symbol=symbol,
            metric=metric,
            frequency=frequency,
            basis=basis,
            alignment=alignment,
            as_of=as_of,
        )
        payload["retrievedAt"] = max(
            (str(row.get("retrieved_at") or "") for row in all_rows),
            default=None,
        )
        payload["rawFactCount"] = len(all_rows)
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
        metric: str = "trailing_pe",
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
        if metric in TRAILING_MULTIPLE_METRICS:
            return await self._get_trailing_multiple_series(
                normalized,
                metric=metric,
                price_observations=price_observations,
                split_events=split_events,
                price_source=price_source,
                price_basis=price_basis,
                refresh=refresh,
                include_provenance=include_provenance,
            )
        if metric != "trailing_pe":
            raise ValueError(f"unsupported valuation metric {metric!r}")
        loaded, source_warning = await self._refresh_metrics_and_load(
            normalized,
            metrics=("diluted_eps", "diluted_shares", "net_income"),
            refresh=refresh,
        )
        rows = loaded["diluted_eps"]
        diluted_share_rows = loaded["diluted_shares"]
        net_income_rows = loaded["net_income"]
        if not rows:
            return None
        payload = derive_trailing_pe_series(
            rows,
            price_observations,
            symbol=normalized,
            diluted_share_rows=diluted_share_rows,
            net_income_rows=net_income_rows,
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

    async def _get_trailing_multiple_series(
        self,
        symbol: str,
        *,
        metric: str,
        price_observations: list[dict[str, Any]],
        split_events: list[tuple[str, float]] | None,
        price_source: str,
        price_basis: str,
        refresh: bool,
        include_provenance: bool,
    ) -> dict[str, Any] | None:
        from .valuation_series import (
            build_share_windows,
            derive_trailing_multiple_series,
        )

        spec = TRAILING_MULTIPLE_METRICS[metric]
        denominator_frequency = "instant" if spec["kind"] == "instant" else "ttm"
        adjustment_metric = spec.get("adjustment")
        adjustment_components: tuple[str, ...] = ()
        if adjustment_metric:
            adjustment_definition = get_derived_definition(adjustment_metric)
            adjustment_components = (
                adjustment_definition.required + adjustment_definition.optional
            )
        share_metrics = (
            "diluted_shares",
            "net_income",
            "diluted_eps",
            "shares_outstanding",
        )
        loaded, source_warning = await self._refresh_metrics_and_load(
            symbol,
            metrics=tuple(spec["components"]) + adjustment_components + share_metrics,
            refresh=refresh,
        )
        denominator_rows = [
            row for component in spec["components"] for row in loaded[component]
        ]
        if not denominator_rows:
            return None
        resolve_frequency = "quarterly" if spec["kind"] == "instant" else "ttm"
        if len(spec["components"]) == 1:
            denominator_payload = resolve_financial_series(
                loaded[spec["components"][0]],
                symbol=symbol,
                metric=spec["components"][0],
                frequency=resolve_frequency,
            )
        else:
            denominator_payload = resolve_derived_series(
                {
                    component: resolve_financial_series(
                        loaded[component],
                        symbol=symbol,
                        metric=component,
                        frequency=resolve_frequency,
                    )
                    for component in spec["components"]
                    if loaded[component]
                },
                symbol=symbol,
                metric=spec["denominator"],
                frequency=resolve_frequency,
                basis="canonical",
                alignment="availability",
            )
        adjustment_observations = None
        if adjustment_metric:
            # Instant components joined on their shared balance date; the engine
            # then as-of joins the composite onto each price bar.
            adjustment_observations = resolve_derived_series(
                {
                    component: resolve_financial_series(
                        loaded[component],
                        symbol=symbol,
                        metric=component,
                        frequency="quarterly",
                    )
                    for component in adjustment_components
                    if loaded[component]
                },
                symbol=symbol,
                metric=adjustment_metric,
                frequency="quarterly",
                basis="canonical",
                alignment="availability",
            )["observations"]
        shares = build_share_windows(
            loaded["diluted_shares"],
            loaded["net_income"],
            loaded["diluted_eps"],
            symbol=symbol,
            instant_share_rows=loaded["shares_outstanding"],
        )
        payload = derive_trailing_multiple_series(
            price_observations,
            denominator_payload["observations"],
            shares,
            symbol=symbol,
            metric=metric,
            label=spec["label"],
            denominator_metric=spec["denominator"],
            denominator_frequency=denominator_frequency,
            invert=bool(spec.get("invert")),
            adjustment_observations=adjustment_observations,
            adjustment_metric=adjustment_metric,
            split_events=split_events,
            price_source=price_source,
            price_basis=price_basis,
            include_provenance=include_provenance,
        )
        all_rows = [row for rows in loaded.values() for row in rows]
        payload["retrievedAt"] = max(
            (str(row.get("retrieved_at") or "") for row in all_rows),
            default=None,
        )
        payload["rawFactCount"] = len(denominator_rows)
        payload["supportingRawFactCount"] = len(loaded["diluted_shares"])
        if source_warning:
            payload["warnings"] = sorted(
                set(payload.get("warnings") or []) | {source_warning}
            )
        return payload
