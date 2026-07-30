"""Point-in-time valuation series derived from prices and immutable SEC facts."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Iterable

from .financial_series import NORMALIZATION_VERSION, resolve_financial_series


def derive_trailing_pe_series(
    financial_fact_rows: Iterable[dict[str, Any]],
    price_observations: Iterable[dict[str, Any]],
    *,
    symbol: str,
    split_events: Iterable[tuple[str, float]] | None = None,
    price_source: str = "caller",
    price_basis: str = "split_adjusted",
    stale_after_days: int = 180,
    include_provenance: bool = True,
) -> dict[str, Any]:
    """Build split-consistent trailing P/E without allowing future SEC facts.

    Prices must already be adjusted for every split in the supplied history. EPS is
    resolved independently as of each price timestamp, then divided by splits that
    occurred after that EPS became available so numerator and denominator share the
    same current-share basis.
    """

    if price_basis != "split_adjusted":
        raise ValueError("trailing P/E requires price_basis='split_adjusted'")
    if stale_after_days < 1:
        raise ValueError("stale_after_days must be positive")

    rows = list(financial_fact_rows)
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

    for price in prices:
        timestamp = price["timestamp"]
        eps_payload = resolve_financial_series(
            rows,
            symbol=symbol,
            metric="diluted_eps",
            frequency="ttm",
            basis="canonical",
            alignment="availability",
            as_of=timestamp,
        )
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
            observations.append(
                _empty_valuation_observation(
                    price,
                    price_source=price_source,
                    quality_flags=["no_point_in_time_ttm_eps"],
                )
            )
            continue

        flags = set(eps.get("qualityFlags") or [])
        available_at = str(eps["availableAt"])
        split_factor = _split_factor_after(available_at, splits or [])
        adjusted_eps = float(eps["value"]) / split_factor
        if split_factor != 1.0:
            flags.add("eps_split_adjusted")
        if (_parse_date(timestamp) - _parse_date(available_at)).days > stale_after_days:
            flags.add("stale_eps")
        if splits is None:
            flags.add("split_history_unverified")

        pe_value = None
        if adjusted_eps <= 0:
            flags.add("non_positive_ttm_eps")
        else:
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
            "epsTtm": float(eps["value"]),
            "epsTtmAdjusted": adjusted_eps,
            "epsSplitAdjustmentFactor": split_factor,
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


def _split_factor_after(
    available_at: str,
    split_events: Iterable[tuple[str, float]],
) -> float:
    factor = 1.0
    for timestamp, ratio in split_events:
        if timestamp > available_at:
            factor *= ratio
    return factor


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
