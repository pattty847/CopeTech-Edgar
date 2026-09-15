"""Run every supported financial metric against one captured Company Facts payload."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from copetech_sec.financial_series_service import FinancialSeriesService


VALUATION_METRICS = (
    "trailing_pe",
    "trailing_ps",
    "trailing_pfcf",
    "trailing_pb",
    "fcf_yield",
    "ev_s",
    "ev_ebitda",
)

OBSERVATION_LIMITS = {
    "annual": 5,
    "quarterly": 12,
    "ttm": 12,
}


async def build_metric_matrix(
    ticker: str,
    company_facts: dict[str, Any],
    *,
    store_path: str | Path,
) -> dict[str, Any]:
    """Resolve all public fundamentals locally after one caller-owned SEC fetch.

    The injected acquisition callback always returns the captured payload. Metric
    iteration therefore cannot create additional SEC traffic.
    """

    async def captured_facts(_symbol: str, use_cache: bool = True) -> dict[str, Any]:
        del use_cache
        return company_facts

    service = FinancialSeriesService(captured_facts, store_path)
    metric_results: list[dict[str, Any]] = []
    for metric_info in sorted(service.supported_metrics(), key=lambda item: item["id"]):
        metric = str(metric_info["id"])
        for frequency in metric_info.get("frequencies") or ():
            result = await _resolve_metric(service, ticker, metric, str(frequency))
            valid_units = list(metric_info.get("validUnits") or [])
            result["expectedUnit"] = valid_units[0] if len(valid_units) == 1 else None
            metric_results.append(result)

    valuation_results = [
        {
            "metric": metric,
            "frequency": "ttm",
            "state": "external_input_missing",
            "reason": (
                "valuation requires separately versioned split-adjusted prices and"
                " split events; this SEC-only audit does not invent them"
            ),
            "observations": [],
            "warnings": ["external_price_input_missing"],
        }
        for metric in VALUATION_METRICS
    ]
    return {
        "ticker": ticker.upper(),
        "cik": _normalized_cik(company_facts.get("cik")),
        "entityName": company_facts.get("entityName"),
        "taxonomies": sorted((company_facts.get("facts") or {}).keys()),
        "metrics": metric_results,
        "valuations": valuation_results,
    }


async def _resolve_metric(
    service: FinancialSeriesService,
    ticker: str,
    metric: str,
    frequency: str,
) -> dict[str, Any]:
    try:
        payload = await service.get_series(
            ticker,
            metric=metric,
            frequency=frequency,
            basis="canonical",
            alignment="availability",
            include_provenance=True,
        )
    except Exception as exc:  # One broken metric must not erase the issuer audit.
        return {
            "metric": metric,
            "frequency": frequency,
            "state": "error",
            "error": f"{type(exc).__name__}: {exc}",
            "observations": [],
            "warnings": [],
        }
    if payload is None:
        return {
            "metric": metric,
            "frequency": frequency,
            "state": "unavailable",
            "observations": [],
            "warnings": [],
        }

    observations = list(payload.get("observations") or [])
    limit = OBSERVATION_LIMITS[frequency]
    observations = observations[-limit:]
    return {
        "metric": metric,
        "frequency": frequency,
        "state": _series_state(observations),
        "rawFactCount": payload.get("rawFactCount"),
        "normalizationVersion": payload.get("normalizationVersion"),
        "observations": observations,
        "warnings": list(payload.get("warnings") or []),
    }


def _series_state(observations: list[dict[str, Any]]) -> str:
    if not observations:
        return "unavailable"
    kinds = {"derived" if row.get("derived") else "reported" for row in observations}
    return next(iter(kinds)) if len(kinds) == 1 else "mixed"


def _normalized_cik(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return f"{int(value):010d}"
    except (TypeError, ValueError):
        return str(value)
