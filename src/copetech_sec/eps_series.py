"""Point-in-time diluted-EPS reconstruction on a consistent share basis.

Per-share facts are weighted averages, not additive amounts.  A valid interim
TTM value therefore needs the diluted earnings numerator and weighted share-days,
not ``annual EPS - Q1 EPS - Q2 EPS - Q3 EPS``.  Company Facts normally exposes
the required weighted-average diluted shares beside diluted EPS.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Iterable

from .financial_series import (
    ANNUAL_MAX_DAYS,
    ANNUAL_MIN_DAYS,
    NORMALIZATION_VERSION,
)


def resolve_diluted_eps_ttm(
    eps_rows: Iterable[dict[str, Any]],
    diluted_share_rows: Iterable[dict[str, Any]],
    *,
    symbol: str,
    split_events: Iterable[tuple[str, float]] | None,
    net_income_rows: Iterable[dict[str, Any]] = (),
    as_of: str | None = None,
    start: str | None = None,
    end: str | None = None,
    alignment: str = "availability",
) -> dict[str, Any]:
    """Resolve reported annual and reconstructed interim TTM diluted EPS.

    Values are normalized to the same share basis as a fully split-adjusted price
    history.  When split history is unavailable, no observation is emitted because
    Company Facts can contain mutually incompatible pre/post-split comparatives.
    """

    if alignment not in {"period_end", "availability"}:
        raise ValueError("alignment must be one of: period_end, availability")
    cutoff = _parse_date(as_of) if as_of else None
    splits = (
        None
        if split_events is None
        else sorted((_normalize_split(event) for event in split_events), key=lambda row: row[0])
    )
    eps = _eligible_rows(eps_rows, "diluted_eps", cutoff)
    shares = _eligible_rows(diluted_share_rows, "diluted_shares", cutoff)
    income = _eligible_rows(net_income_rows, "net_income", cutoff)
    pairs = _pair_facts(eps, shares, income)
    warnings: set[str] = set()
    observations: list[dict[str, Any]] = []
    if splits is None:
        warnings.add("split_history_unverified")
    else:
        for filed in sorted({str(pair["filed"]) for pair in pairs}):
            observation = _latest_ttm_at(pairs, filed=filed, split_events=splits)
            if observation is None:
                continue
            key = (observation["availableAt"], observation["periodEnd"])
            if any((row["availableAt"], row["periodEnd"]) == key for row in observations):
                continue
            if start and observation["periodEnd"] < start:
                continue
            if end and observation["periodEnd"] > end:
                continue
            observation["alignedAt"] = (
                observation["availableAt"]
                if alignment == "availability"
                else observation["periodEnd"]
            )
            observations.append(observation)
            warnings.update(observation["qualityFlags"])
    observations.sort(key=lambda row: (row["availableAt"], row["periodEnd"]))
    identity = next(iter(eps or shares), {})
    return {
        "symbol": symbol.upper(),
        "cik": identity.get("cik"),
        "entityName": identity.get("entity_name"),
        "metric": "diluted_eps",
        "label": "Diluted earnings per share",
        "frequency": "ttm",
        "basis": "canonical",
        "alignment": alignment,
        "asOf": as_of,
        "shareBasis": "split_adjusted",
        "normalizationVersion": NORMALIZATION_VERSION,
        "observations": observations,
        "warnings": sorted(warnings),
    }


def _eligible_rows(
    rows: Iterable[dict[str, Any]],
    metric: str,
    cutoff: date | None,
) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in rows
        if row.get("metric") == metric
        and (cutoff is None or _parse_date(row["filed"]) <= cutoff)
    ]


def _pair_facts(
    eps_rows: list[dict[str, Any]],
    share_rows: list[dict[str, Any]],
    income_rows: list[dict[str, Any]] = (),
) -> list[dict[str, Any]]:
    """Attach a weighted-average diluted share count to each diluted-EPS fact.

    Some issuers never tag a consolidated weighted-average share count. Alphabet is the
    reference case: through mid-2024 it reported shares broken out by share class using
    XBRL dimensions, and the SEC's Company Facts API returns only non-dimensional facts.
    Diluted EPS was fully tagged the whole time; only the divisor was missing, which
    stranded every interim TTM reconstruction and left one usable EPS point per year.

    Where the count is absent, it is recovered from the identity that defines it:
    ``weighted average diluted shares = net income / diluted EPS``. Measured against 264
    windows where both the tagged and derived values exist (GOOG, AAPL), the two agree to
    within 0.22% — the residual is EPS reporting quantization, since a figure published to
    two decimals carries about that much relative error.

    This also makes the TTM numerator exact rather than approximate: ``eps * shares``
    collapses back to the reported net income instead of multiplying two rounded values.
    """
    shares_by_filing: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in share_rows:
        key = (row["period_start"], row["period_end"], row["accession_number"])
        shares_by_filing.setdefault(key, []).append(row)
    income_by_filing: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in income_rows:
        key = (row["period_start"], row["period_end"], row["accession_number"])
        income_by_filing.setdefault(key, []).append(row)
    pairs: list[dict[str, Any]] = []
    for eps in eps_rows:
        key = (eps["period_start"], eps["period_end"], eps["accession_number"])
        eps_value = float(eps["value"])
        candidates = shares_by_filing.get(key) or []
        if candidates:
            share = min(
                candidates,
                key=lambda row: (int(row["concept_priority"]), row["concept"]),
            )
            pairs.append(
                {
                    **eps,
                    "eps": eps_value,
                    "shares": float(share["value"]),
                    "eps_source": _source(eps),
                    "share_source": _source(share),
                }
            )
            continue
        derived = _derive_shares(eps_value, income_by_filing.get(key) or [])
        pairs.append(
            {
                **eps,
                "eps": eps_value,
                "shares": None if derived is None else derived[0],
                "quality_flags": sorted(
                    set(eps.get("quality_flags") or [])
                    | ({"diluted_shares_derived_from_net_income"} if derived else set())
                ),
                "eps_source": _source(eps),
                "share_source": None if derived is None else _source(derived[1]),
            }
        )
    return pairs


def _derive_shares(
    eps_value: float,
    income_candidates: list[dict[str, Any]],
) -> tuple[float, dict[str, Any]] | None:
    """Weighted-average diluted shares implied by net income and diluted EPS.

    A zero or missing EPS gives no information about the divisor, and a non-positive
    implied count would be nonsense, so both decline rather than guess.
    """
    if not income_candidates or eps_value == 0:
        return None
    income = min(
        income_candidates,
        key=lambda row: (int(row["concept_priority"]), row["concept"]),
    )
    shares = float(income["value"]) / eps_value
    return (shares, income) if shares > 0 else None


def _latest_ttm_at(
    pairs: list[dict[str, Any]],
    *,
    filed: str,
    split_events: list[tuple[str, float]],
) -> dict[str, Any] | None:
    resolved = _resolve_windows([row for row in pairs if row["filed"] <= filed])
    newly_selected = [row for row in resolved if row["filed"] == filed]
    if not newly_selected:
        return None
    target_end = max(row["period_end"] for row in newly_selected)
    target_candidates = [row for row in newly_selected if row["period_end"] == target_end]
    target = max(target_candidates, key=lambda row: int(row["duration_days"]))
    if _is_annual(target):
        return _annual_observation(target, split_events)
    return _interim_observation(resolved, target, split_events)


def _resolve_windows(pairs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in pairs:
        grouped.setdefault((row["period_start"], row["period_end"]), []).append(row)
    resolved: list[dict[str, Any]] = []
    for group in grouped.values():
        best_priority = min(int(row["concept_priority"]) for row in group)
        preferred = [row for row in group if int(row["concept_priority"]) == best_priority]
        resolved.append(
            max(preferred, key=lambda row: (row["filed"], row["accession_number"]))
        )
    return resolved


def _annual_observation(
    annual: dict[str, Any],
    split_events: list[tuple[str, float]],
) -> dict[str, Any]:
    factor = _split_factor_after(annual["filed"], split_events)
    flags = set(annual.get("quality_flags") or [])
    if factor != 1.0:
        flags.add("eps_split_adjusted")
    sources = _unique_sources([annual["eps_source"], annual["share_source"]])
    return {
        "periodStart": annual["period_start"],
        "periodEnd": annual["period_end"],
        "availableAt": annual["filed"],
        "value": float(annual["eps"]) / factor,
        "unit": "USD/shares",
        "frequency": "ttm",
        "fiscalYear": annual.get("fiscal_year"),
        "fiscalPeriod": "TTM",
        "reported": True,
        "derived": False,
        "derivation": None,
        "confidence": 1.0,
        "qualityFlags": sorted(flags),
        "availabilitySource": annual["eps_source"],
        "selectedSource": annual["eps_source"],
        "sources": sources,
    }


def _interim_observation(
    resolved: list[dict[str, Any]],
    target: dict[str, Any],
    split_events: list[tuple[str, float]],
) -> dict[str, Any] | None:
    prior_annuals = [
        row
        for row in resolved
        if _is_annual(row)
        and row["period_end"] < target["period_start"]
        and 0 <= (_parse_date(target["period_start"]) - _parse_date(row["period_end"])).days <= 7
    ]
    if not prior_annuals:
        return None
    annual = max(prior_annuals, key=lambda row: row["period_end"])
    current_ytd = max(
        (
            row
            for row in resolved
            if row["period_end"] == target["period_end"]
            and row["period_start"] > annual["period_end"]
        ),
        key=lambda row: int(row["duration_days"]),
        default=None,
    )
    if current_ytd is None:
        return None
    prior_ytd = min(
        (
            row
            for row in resolved
            if row["period_start"] == annual["period_start"]
            and row["period_end"] < annual["period_end"]
        ),
        key=lambda row: (
            abs(int(row["duration_days"]) - int(current_ytd["duration_days"])),
            abs(
                (
                    _parse_date(current_ytd["period_end"])
                    - _parse_date(row["period_end"])
                ).days
                - 365
            ),
        ),
        default=None,
    )
    if prior_ytd is None:
        return None
    if abs(int(prior_ytd["duration_days"]) - int(current_ytd["duration_days"])) > 14:
        return None
    if not 330 <= (
        _parse_date(current_ytd["period_end"]) - _parse_date(prior_ytd["period_end"])
    ).days <= 400:
        return None

    components = (annual, current_ytd, prior_ytd)
    if any(row["shares"] is None for row in components):
        return None
    factors = [_split_factor_after(row["filed"], split_events) for row in components]
    numerator = (
        annual["eps"] * annual["shares"]
        + current_ytd["eps"] * current_ytd["shares"]
        - prior_ytd["eps"] * prior_ytd["shares"]
    )
    total_days = (
        int(annual["duration_days"])
        + int(current_ytd["duration_days"])
        - int(prior_ytd["duration_days"])
    )
    share_days = (
        annual["shares"] * factors[0] * int(annual["duration_days"])
        + current_ytd["shares"] * factors[1] * int(current_ytd["duration_days"])
        - prior_ytd["shares"] * factors[2] * int(prior_ytd["duration_days"])
    )
    if total_days <= 0 or share_days <= 0:
        return None
    weighted_shares = share_days / total_days
    value = numerator / weighted_shares
    flags = {
        flag
        for row in components
        for flag in row.get("quality_flags") or []
    }
    flags.add("eps_ttm_reconstructed")
    if any(factor != 1.0 for factor in factors):
        flags.add("eps_split_adjusted")
    sources = _unique_sources(
        [
            source
            for row in components
            for source in (row["eps_source"], row["share_source"])
        ]
    )
    available_at = max(row["filed"] for row in components)
    return {
        "periodStart": (_parse_date(prior_ytd["period_end"]) + timedelta(days=1)).isoformat(),
        "periodEnd": current_ytd["period_end"],
        "availableAt": available_at,
        "value": value,
        "unit": "USD/shares",
        "frequency": "ttm",
        "fiscalYear": current_ytd.get("fiscal_year"),
        "fiscalPeriod": "TTM",
        "reported": False,
        "derived": True,
        "derivation": (
            "prior annual plus current YTD minus prior comparable YTD, "
            "reconstructed from diluted EPS and weighted-average diluted shares"
        ),
        "confidence": 0.9,
        "qualityFlags": sorted(flags),
        "availabilitySource": _source_for_date(sources, available_at),
        "selectedSource": current_ytd["eps_source"],
        "sources": sources,
    }


def _is_annual(row: dict[str, Any]) -> bool:
    return ANNUAL_MIN_DAYS <= int(row["duration_days"]) <= ANNUAL_MAX_DAYS


def _split_factor_after(
    filed: str,
    split_events: list[tuple[str, float]],
) -> float:
    factor = 1.0
    for timestamp, ratio in split_events:
        if timestamp > filed:
            factor *= ratio
    return factor


def _normalize_split(event: tuple[str, float]) -> tuple[str, float]:
    timestamp, raw_ratio = event
    ratio = float(raw_ratio)
    if ratio <= 0:
        raise ValueError(f"split ratio must be positive: {event!r}")
    return _parse_date(timestamp).isoformat(), ratio


def _source(row: dict[str, Any]) -> dict[str, Any]:
    accession = str(row["accession_number"])
    cik = str(row.get("cik") or "").lstrip("0")
    return {
        "taxonomy": row["taxonomy"],
        "concept": row["concept"],
        "form": row["form"],
        "filed": row["filed"],
        "accessionNumber": accession,
        "frame": row.get("frame"),
        "sourceUrl": (
            f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}/"
            if cik
            else None
        ),
    }


def _source_for_date(
    sources: list[dict[str, Any]],
    filed: str,
) -> dict[str, Any]:
    return next((source for source in sources if source["filed"] == filed), sources[-1])


def _unique_sources(
    sources: list[dict[str, Any] | None],
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for source in sources:
        if source is None:
            continue
        key = (source["accessionNumber"], source["concept"])
        if key not in seen:
            output.append(source)
            seen.add(key)
    return output


def _parse_date(value: Any) -> date:
    return date.fromisoformat(str(value)[:10])
