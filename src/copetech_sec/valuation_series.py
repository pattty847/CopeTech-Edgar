"""Point-in-time valuation series derived from prices and immutable SEC facts."""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any, Iterable

from .eps_series import _split_factor_after, resolve_diluted_eps_ttm
from .financial_series import NORMALIZATION_VERSION, resolve_financial_series


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


def derive_trailing_multiple_series(
    price_observations: Iterable[dict[str, Any]],
    ttm_observations: Iterable[dict[str, Any]],
    share_windows: Iterable[dict[str, Any]],
    *,
    symbol: str,
    metric: str,
    label: str,
    denominator_metric: str,
    denominator_frequency: str = "ttm",
    invert: bool = False,
    adjustment_observations: Iterable[dict[str, Any]] | None = None,
    adjustment_metric: str | None = None,
    split_events: Iterable[tuple[str, float]] | None = None,
    price_source: str = "caller",
    price_basis: str = "split_adjusted",
    stale_after_days: int = 180,
    include_provenance: bool = True,
) -> dict[str, Any]:
    """Trailing market-cap multiple: price × point-in-time shares ÷ SEC value.

    The numerator is an implied market cap: the split-adjusted close times the
    latest share count known at that bar, restated onto the current share basis
    (multiplied by every split after its filing) so it matches the basis the
    price history is stored on. Cover-page shares-outstanding counts are used
    when the caller supplies them in `share_windows`; diluted weighted-average
    shares are the fallback. The denominator is either a TTM flow (revenue,
    FCF) or an instant balance (equity for P/B). `invert` flips the ratio for
    yields; `adjustment_observations` add a point-in-time balance (net debt)
    to the market cap for enterprise-value multiples. Every observation
    carries all inputs so the arithmetic is auditable.
    """

    if price_basis != "split_adjusted":
        raise ValueError(f"{metric} requires price_basis='split_adjusted'")
    if stale_after_days < 1:
        raise ValueError("stale_after_days must be positive")

    prices = sorted(
        (_normalize_price(row) for row in price_observations),
        key=lambda row: row["timestamp"],
    )
    splits = None if split_events is None else sorted(
        (_normalize_split(event) for event in split_events),
        key=lambda event: event[0],
    )
    denominators = sorted(
        ttm_observations,
        key=lambda row: (row["availableAt"], row["periodEnd"]),
    )
    shares = sorted(
        share_windows,
        key=lambda row: (row["availableAt"], row["periodEnd"]),
    )
    adjustments = (
        None
        if adjustment_observations is None
        else sorted(
            adjustment_observations,
            key=lambda row: (row["availableAt"], row["periodEnd"]),
        )
    )
    observations: list[dict[str, Any]] = []
    warnings: set[str] = set()
    if splits is None:
        warnings.add("split_history_unverified")

    for price in prices:
        timestamp = price["timestamp"]
        denominator = max(
            (row for row in denominators if row["availableAt"] <= timestamp),
            key=lambda row: (row["periodEnd"], row["availableAt"]),
            default=None,
        )
        share_window = max(
            (row for row in shares if row["availableAt"] <= timestamp),
            key=lambda row: (row["periodEnd"], row["availableAt"]),
            default=None,
        )
        adjustment = None
        if adjustments is not None:
            adjustment = max(
                (row for row in adjustments if row["availableAt"] <= timestamp),
                key=lambda row: (row["periodEnd"], row["availableAt"]),
                default=None,
            )
        adjustment_missing = adjustments is not None and adjustment is None
        if denominator is None or share_window is None or adjustment_missing:
            missing = (
                (["no_point_in_time_ttm_denominator"] if denominator is None else [])
                + (["no_point_in_time_share_count"] if share_window is None else [])
                + (["no_point_in_time_adjustment"] if adjustment_missing else [])
            )
            observation = _empty_multiple_observation(
                price,
                price_source=price_source,
                quality_flags=sorted(set(missing) | ({"split_history_unverified"} if splits is None else set())),
            )
            if not include_provenance:
                observation.pop("sources")
                observation.pop("priceSource")
            observations.append(observation)
            warnings.update(observation["qualityFlags"])
            continue

        flags = set(denominator.get("qualityFlags") or []) | set(
            share_window.get("qualityFlags") or []
        )
        if adjustment is not None:
            flags |= set(adjustment.get("qualityFlags") or [])
        denominator_available = str(denominator["availableAt"])
        shares_available = str(share_window["availableAt"])
        # The most-stale required input governs: a fresh balance sheet cannot
        # rescue a ten-month-old TTM denominator.
        oldest_needed = min(
            [denominator_available, shares_available]
            + ([str(adjustment["availableAt"])] if adjustment is not None else [])
        )
        is_stale = (
            _parse_date(timestamp) - _parse_date(oldest_needed)
        ).days > stale_after_days
        if is_stale:
            flags.add("stale_fundamentals")
        value = None
        adjusted_shares = None
        ttm_value = float(denominator["value"])
        if splits is None:
            flags.add("split_history_unverified")
        else:
            adjusted_shares = float(share_window["value"]) * _split_factor_after(
                shares_available, splits
            )
            if adjusted_shares <= 0:
                flags.add("non_positive_share_count")
            elif not is_stale:
                numerator = float(price["close"]) * adjusted_shares
                if adjustment is not None:
                    numerator += float(adjustment["value"])
                if numerator <= 0:
                    flags.add("non_positive_enterprise_value")
                elif invert:
                    # A negative FCF multiple is not useful, but a negative FCF
                    # *yield* is an economically meaningful result. Zero likewise
                    # means a zero yield, not an unavailable observation.
                    value = ttm_value / numerator
                elif ttm_value <= 0:
                    flags.add("non_positive_ttm_denominator")
                else:
                    value = numerator / ttm_value

        observation = {
            "timestamp": timestamp,
            "alignedAt": timestamp,
            "value": round(value, 6) if value is not None else None,
            "unit": "ratio",
            "price": price["close"],
            "priceBasis": price_basis,
            "priceSource": {
                "provider": price_source,
                "timestamp": timestamp,
                "basis": price_basis,
            },
            "denominatorTtm": ttm_value,
            "denominatorAvailableAt": denominator_available,
            "denominatorPeriodEnd": denominator["periodEnd"],
            "sharesOutstanding": adjusted_shares,
            "sharesAvailableAt": shares_available,
            "sharesBasis": "split_adjusted",
            "adjustmentValue": (
                float(adjustment["value"]) if adjustment is not None else None
            ),
            "adjustmentAvailableAt": (
                str(adjustment["availableAt"]) if adjustment is not None else None
            ),
            "qualityFlags": sorted(flags),
            "sources": list(denominator.get("sources") or [])
            + list(share_window.get("sources") or [])
            + (list(adjustment.get("sources") or []) if adjustment is not None else []),
        }
        if not include_provenance:
            observation.pop("sources")
            observation.pop("priceSource")
        observations.append(observation)
        warnings.update(flags)

    return {
        "symbol": symbol.upper(),
        "metric": metric,
        "label": label,
        "frequency": "price",
        "alignment": "price_timestamp",
        "priceBasis": price_basis,
        "denominatorMetric": denominator_metric,
        "denominatorFrequency": denominator_frequency,
        "adjustmentMetric": adjustment_metric,
        "inverted": invert,
        "normalizationVersion": NORMALIZATION_VERSION,
        "observations": observations,
        "warnings": sorted(warnings),
    }


def build_share_windows(
    share_rows: Iterable[dict[str, Any]],
    net_income_rows: Iterable[dict[str, Any]],
    eps_rows: Iterable[dict[str, Any]],
    *,
    symbol: str,
    instant_share_rows: Iterable[dict[str, Any]] = (),
) -> list[dict[str, Any]]:
    """Share-count observations for market-cap numerators, one per window.

    Cover-page shares-outstanding counts (`instant_share_rows`, the dei facts)
    are true point-in-time counts and are dated later than the balance sheet
    they accompany, so where present they win the most-recent-known selection
    naturally; they carry `point_in_time_shares_outstanding`. Diluted
    weighted-average windows fill the rest of the timeline. Issuers that tag
    counts only dimensionally (Alphabet's per-class counts) are missing a
    consolidated figure in Company Facts, so windows absent from both series
    are recovered as net income ÷ diluted EPS, flagged
    `diluted_shares_derived_from_net_income` — the same recovery the TTM EPS
    engine uses, documented there as agreeing within 0.22%.
    """

    def _observations(rows: list[dict[str, Any]], metric: str) -> list[dict[str, Any]]:
        if not rows:
            return []
        output = []
        for frequency in ("quarterly", "annual"):
            payload = resolve_financial_series(
                rows,
                symbol=symbol,
                metric=metric,
                frequency=frequency,
                basis="reported",
            )
            output.extend(payload["observations"])
        return output

    windows: dict[tuple[str, str], dict[str, Any]] = {}
    for observation in _observations(list(share_rows), "diluted_shares"):
        windows[(observation["periodStart"], observation["periodEnd"])] = observation

    instant_rows = list(instant_share_rows)
    if instant_rows:
        payload = resolve_financial_series(
            instant_rows,
            symbol=symbol,
            metric="shares_outstanding",
            frequency="quarterly",
            basis="reported",
        )
        for observation in payload["observations"]:
            key = (observation["periodStart"], observation["periodEnd"])
            windows[key] = {
                **observation,
                "qualityFlags": sorted(
                    set(observation.get("qualityFlags") or [])
                    | {"point_in_time_shares_outstanding"}
                ),
            }

    income = {
        (row["periodStart"], row["periodEnd"]): row
        for row in _observations(list(net_income_rows), "net_income")
    }
    for eps in _observations(list(eps_rows), "diluted_eps"):
        key = (eps["periodStart"], eps["periodEnd"])
        if key in windows or key not in income:
            continue
        eps_value = float(eps["value"])
        if eps_value == 0:
            continue
        matching_income = income[key]
        windows[key] = {
            **eps,
            "value": float(matching_income["value"]) / eps_value,
            "unit": "shares",
            "availableAt": max(eps["availableAt"], matching_income["availableAt"]),
            "qualityFlags": sorted(
                set(eps.get("qualityFlags") or [])
                | set(matching_income.get("qualityFlags") or [])
                | {"diluted_shares_derived_from_net_income"}
            ),
            "sources": list(eps.get("sources") or [])
            + list(matching_income.get("sources") or []),
        }
    return sorted(windows.values(), key=lambda row: (row["periodEnd"], row["availableAt"]))


def _empty_multiple_observation(
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
        "denominatorTtm": None,
        "denominatorAvailableAt": None,
        "denominatorPeriodEnd": None,
        "sharesOutstanding": None,
        "sharesAvailableAt": None,
        "sharesBasis": "split_adjusted",
        "adjustmentValue": None,
        "adjustmentAvailableAt": None,
        "qualityFlags": quality_flags,
        "sources": [],
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
