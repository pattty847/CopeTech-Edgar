"""Point-in-time valuation series derived from prices and immutable SEC facts."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Iterable

from .eps_series import resolve_diluted_eps_ttm
from .financial_series import NORMALIZATION_VERSION


def derive_trailing_pe_series(
    financial_fact_rows: Iterable[dict[str, Any]],
    price_observations: Iterable[dict[str, Any]],
    *,
    symbol: str,
    diluted_share_rows: Iterable[dict[str, Any]] = (),
    net_income_rows: Iterable[dict[str, Any]] = (),
    split_events: Iterable[tuple[str, float]] | None = None,
    price_source: str = "caller",
    price_basis: str = "split_adjusted",
    stale_after_days: int = 180,
    include_provenance: bool = True,
) -> dict[str, Any]:
    """Build split-consistent trailing P/E without allowing future SEC facts.

    Prices must already be adjusted for every split in the supplied history. TTM
    diluted EPS is resolved from annual EPS or reconstructed with weighted-average
    diluted shares on that same current-share basis.
    """

    if price_basis != "split_adjusted":
        raise ValueError("trailing P/E requires price_basis='split_adjusted'")
    if stale_after_days < 1:
        raise ValueError("stale_after_days must be positive")

    rows = list(financial_fact_rows)
    share_rows = list(diluted_share_rows)
    income_rows = list(net_income_rows)
    prices = sorted(
        (_normalize_price(row) for row in price_observations),
        key=lambda row: row["timestamp"],
    )
    splits = None if split_events is None else sorted(
        (_normalize_split(event) for event in split_events),
        key=lambda event: event[0],
    )
    observations: list[dict[str, Any]] = []
    warnings: set[str] = set()
    if splits is None:
        warnings.add("split_history_unverified")
    eps_payload = resolve_diluted_eps_ttm(
        rows,
        share_rows,
        symbol=symbol,
        split_events=splits,
        net_income_rows=income_rows,
        alignment="availability",
    )

    for price in prices:
        timestamp = price["timestamp"]
        eligible = [
            observation
            for observation in eps_payload["observations"]
            if observation["availableAt"] <= timestamp
        ]
        eps = max(
            eligible,
            key=lambda observation: (
                observation["periodEnd"],
                observation["availableAt"],
            ),
            default=None,
        )
        if eps is None:
            empty = _empty_valuation_observation(
                price,
                price_source=price_source,
                quality_flags=["no_point_in_time_ttm_eps"],
            )
            if not include_provenance:
                empty.pop("sources")
                empty.pop("priceSource")
            observations.append(empty)
            continue

        flags = set(eps.get("qualityFlags") or [])
        available_at = str(eps["availableAt"])
        adjusted_eps = float(eps["value"])
        is_stale = (
            _parse_date(timestamp) - _parse_date(available_at)
        ).days > stale_after_days
        if is_stale:
            flags.add("stale_eps")
        if splits is None:
            flags.add("split_history_unverified")

        pe_value = None
        if adjusted_eps <= 0:
            flags.add("non_positive_ttm_eps")
        elif not is_stale:
            pe_value = float(price["close"]) / adjusted_eps

        source_rows = list(eps.get("sources") or [])
        observation = {
            "timestamp": timestamp,
            "alignedAt": timestamp,
            "value": round(pe_value, 6) if pe_value is not None else None,
            "unit": "ratio",
            "price": price["close"],
            "priceBasis": price_basis,
            "priceSource": {
                "provider": price_source,
                "timestamp": timestamp,
                "basis": price_basis,
            },
            "epsTtm": adjusted_eps,
            "epsTtmAdjusted": adjusted_eps,
            "epsSplitAdjustmentFactor": 1.0,
            "epsAvailableAt": available_at,
            "epsPeriodEnd": eps["periodEnd"],
            "qualityFlags": sorted(flags),
            "sources": source_rows,
        }
        if not include_provenance:
            observation.pop("sources")
            observation.pop("priceSource")
        observations.append(observation)
        warnings.update(flags)

    return {
        "symbol": symbol.upper(),
        "metric": "trailing_pe",
        "label": "Trailing P/E",
        "frequency": "price",
        "alignment": "price_timestamp",
        "priceBasis": price_basis,
        "epsMetric": "diluted_eps",
        "epsFrequency": "ttm",
        "normalizationVersion": NORMALIZATION_VERSION,
        "observations": observations,
        "warnings": sorted(warnings),
    }


def _normalize_price(row: dict[str, Any]) -> dict[str, Any]:
    raw_timestamp = row.get("timestamp", row.get("time", row.get("date")))
    if isinstance(raw_timestamp, (int, float)):
        timestamp = datetime.fromtimestamp(
            raw_timestamp,
            tz=timezone.utc,
        ).date().isoformat()
    else:
        timestamp = str(raw_timestamp or "")[:10]
    close = row.get("close", row.get("value"))
    try:
        parsed_timestamp = _parse_date(timestamp).isoformat()
        parsed_close = float(close)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"invalid price observation: {row!r}") from exc
    if parsed_close <= 0:
        raise ValueError(f"price close must be positive: {row!r}")
    return {"timestamp": parsed_timestamp, "close": parsed_close}


def _normalize_split(event: tuple[str, float]) -> tuple[str, float]:
    timestamp, raw_ratio = event
    ratio = float(raw_ratio)
    if ratio <= 0:
        raise ValueError(f"split ratio must be positive: {event!r}")
    return _parse_date(timestamp).isoformat(), ratio


def _empty_valuation_observation(
    price: dict[str, Any],
    *,
    price_source: str,
    quality_flags: list[str],
) -> dict[str, Any]:
    return {
        "timestamp": price["timestamp"],
        "alignedAt": price["timestamp"],
        "value": None,
        "unit": "ratio",
        "price": price["close"],
        "priceBasis": "split_adjusted",
        "priceSource": {
            "provider": price_source,
            "timestamp": price["timestamp"],
            "basis": "split_adjusted",
        },
        "epsTtm": None,
        "epsTtmAdjusted": None,
        "epsSplitAdjustmentFactor": None,
        "epsAvailableAt": None,
        "epsPeriodEnd": None,
        "qualityFlags": quality_flags,
        "sources": [],
    }


def _parse_date(value: Any) -> date:
    return date.fromisoformat(str(value)[:10])
