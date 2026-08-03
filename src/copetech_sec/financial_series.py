"""Canonical, provenance-preserving financial-series normalization.

Company Facts is an excellent acquisition source, but it is not a ready-made time
series.  A single economic fact can appear in several later filings, calendar
frames do not equal issuer fiscal quarters, and a 10-Q commonly carries both a
standalone quarter and a year-to-date value.  This module keeps those distinctions
explicit and resolves them only at query time.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

from .financial_metrics import MetricDefinition, get_metric_definition


NORMALIZATION_VERSION = 3
QUARTER_MIN_DAYS = 70
QUARTER_MAX_DAYS = 110
# Cumulative (year-to-date) windows from Q2/Q3 filings: roughly six and nine
# months, with slack for 52/53-week fiscal calendars.
SEMIANNUAL_MIN_DAYS = 150
SEMIANNUAL_MAX_DAYS = 200
NINE_MONTH_MIN_DAYS = 240
NINE_MONTH_MAX_DAYS = 300
ANNUAL_MIN_DAYS = 330
ANNUAL_MAX_DAYS = 400
SUPPORTED_FORMS = frozenset(
    {"10-Q", "10-Q/A", "10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
)


def extract_financial_facts(
    facts_data: dict[str, Any],
    *,
    symbol: str,
    metric: str,
    retrieved_at: str | None = None,
) -> list[dict[str, Any]]:
    definition = _metric(metric)
    timestamp = retrieved_at or datetime.now(timezone.utc).isoformat()
    rows: list[dict[str, Any]] = []
    for concept_priority, (taxonomy, concept) in enumerate(definition.concepts):
        concept_data = (
            facts_data.get("facts", {}).get(taxonomy, {}).get(concept) or {}
        )
        units = concept_data.get("units") or {}
        for unit, entries in units.items():
            if unit not in definition.valid_units or not isinstance(entries, list):
                continue
            for entry in entries:
                normalized = _normalize_raw_fact(
                    entry,
                    symbol=symbol,
                    cik=facts_data.get("cik"),
                    entity_name=facts_data.get("entityName"),
                    metric=metric,
                    taxonomy=taxonomy,
                    concept=concept,
                    concept_priority=concept_priority,
                    unit=unit,
                    retrieved_at=timestamp,
                )
                if normalized is not None:
                    rows.append(normalized)
    return rows


def resolve_financial_series(
    rows: Iterable[dict[str, Any]],
    *,
    symbol: str,
    metric: str,
    frequency: str = "quarterly",
    basis: str = "canonical",
    as_of: str | None = None,
    start: str | None = None,
    end: str | None = None,
    alignment: str = "availability",
) -> dict[str, Any]:
    definition = _metric(metric)
    if frequency not in {"quarterly", "annual", "ttm"}:
        raise ValueError("frequency must be one of: quarterly, annual, ttm")
    if basis not in {"reported", "canonical"}:
        raise ValueError("basis must be one of: reported, canonical")
    if alignment not in {"period_end", "availability"}:
        raise ValueError("alignment must be one of: period_end, availability")
    cutoff = _parse_date(as_of) if as_of else None
    if start:
        _parse_date(start)
    if end:
        _parse_date(end)
    candidates = [
        dict(row)
        for row in rows
        if row.get("metric") == metric
        and (cutoff is None or _parse_date(row.get("filed")) <= cutoff)
    ]
    quarterly = _resolve_duration_windows(candidates, definition, cadence="quarterly")
    annual = _resolve_duration_windows(candidates, definition, cadence="annual")
    if basis == "canonical" and definition.aggregation == "sum":
        if definition.ytd_cadence:
            ytd = _resolve_duration_windows(candidates, definition, cadence="ytd")
            quarterly = _add_quarters_derived_from_ytd(quarterly, ytd)
        quarterly = _add_derived_fourth_quarters(quarterly, annual)
    if frequency == "quarterly":
        observations = quarterly
    elif frequency == "annual":
        observations = annual
    else:
        observations = _trailing_twelve_months(quarterly, definition)
    observations = [
        row
        for row in observations
        if (not start or row["periodEnd"] >= start)
        and (not end or row["periodEnd"] <= end)
    ]
    for observation in observations:
        observation["alignedAt"] = (
            observation["availableAt"]
            if alignment == "availability"
            else observation["periodEnd"]
        )
    observations.sort(key=lambda row: (row["periodEnd"], row["availableAt"]))
    warnings = sorted(
        {
            warning
            for row in observations
            for warning in row.get("qualityFlags", [])
            if warning
        }
    )
    entity_name = next(
        (str(row.get("entity_name")) for row in candidates if row.get("entity_name")),
        None,
    )
    cik = next((row.get("cik") for row in candidates if row.get("cik") is not None), None)
    return {
        "symbol": symbol.upper(),
        "cik": cik,
        "entityName": entity_name,
        "metric": metric,
        "label": definition.label,
        "frequency": frequency,
        "basis": basis,
        "alignment": alignment,
        "asOf": as_of,
        "normalizationVersion": NORMALIZATION_VERSION,
        "observations": observations,
        "warnings": warnings,
    }


def _normalize_raw_fact(
    entry: dict[str, Any],
    *,
    symbol: str,
    cik: Any,
    entity_name: Any,
    metric: str,
    taxonomy: str,
    concept: str,
    concept_priority: int,
    unit: str,
    retrieved_at: str,
) -> dict[str, Any] | None:
    form = str(entry.get("form") or "")
    start = str(entry.get("start") or "")
    end = str(entry.get("end") or "")
    filed = str(entry.get("filed") or "")
    accession = str(entry.get("accn") or "")
    if (
        form not in SUPPORTED_FORMS
        or not start
        or not end
        or not filed
        or not accession
        or entry.get("val") is None
    ):
        return None
    try:
        duration_days = (_parse_date(end) - _parse_date(start)).days
        value = float(entry["val"])
    except (TypeError, ValueError):
        return None
    quality_flags: list[str] = []
    if form.endswith("/A"):
        quality_flags.append("amended_filing")
    return {
        "symbol": symbol.upper(),
        "cik": cik,
        "entity_name": entity_name,
        "metric": metric,
        "taxonomy": taxonomy,
        "concept": concept,
        "concept_priority": concept_priority,
        "value": value,
        "unit": unit,
        "period_start": start,
        "period_end": end,
        "duration_days": duration_days,
        "fiscal_year": entry.get("fy"),
        "fiscal_period": entry.get("fp"),
        "form": form,
        "filed": filed,
        "accession_number": accession,
        "frame": entry.get("frame"),
        "retrieved_at": retrieved_at,
        "normalization_version": NORMALIZATION_VERSION,
        "quality_flags": quality_flags,
    }


def _resolve_duration_windows(
    rows: list[dict[str, Any]],
    definition: MetricDefinition,
    *,
    cadence: str,
) -> list[dict[str, Any]]:
    if cadence == "quarterly":
        accepted = lambda days: QUARTER_MIN_DAYS <= days <= QUARTER_MAX_DAYS
    elif cadence == "ytd":
        accepted = lambda days: (
            SEMIANNUAL_MIN_DAYS <= days <= SEMIANNUAL_MAX_DAYS
            or NINE_MONTH_MIN_DAYS <= days <= NINE_MONTH_MAX_DAYS
        )
    else:
        accepted = lambda days: ANNUAL_MIN_DAYS <= days <= ANNUAL_MAX_DAYS
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        if accepted(int(row.get("duration_days") or -1)):
            key = (row["period_start"], row["period_end"], row["unit"])
            grouped.setdefault(key, []).append(row)
    return [
        _resolve_window(group, definition, cadence=cadence)
        for group in grouped.values()
    ]


def _resolve_window(
    group: list[dict[str, Any]],
    definition: MetricDefinition,
    *,
    cadence: str,
) -> dict[str, Any]:
    best_priority = min(int(row["concept_priority"]) for row in group)
    preferred = [row for row in group if int(row["concept_priority"]) == best_priority]
    selected = max(preferred, key=lambda row: (row["filed"], row["accession_number"]))
    same_value = [
        row
        for row in group
        if _values_match(float(row["value"]), float(selected["value"]))
    ]
    first_available = min(row["filed"] for row in same_value)
    identity_source = min(
        same_value,
        key=lambda row: (row["filed"], row["accession_number"]),
    )
    distinct_values = {
        round(float(row["value"]), 6)
        for row in group
    }
    flags = set(selected.get("quality_flags") or [])
    if len(distinct_values) > 1:
        flags.add("conflicting_filing_values")
    if len({row["concept"] for row in group}) > 1:
        flags.add("multiple_concepts_available")
    confidence = 1.0
    if "conflicting_filing_values" in flags:
        confidence -= 0.2
    availability_source = _source_from_fact(identity_source)
    selected_source = _source_from_fact(selected)
    sources = [availability_source]
    if selected_source["accessionNumber"] != availability_source["accessionNumber"]:
        sources.append(selected_source)
    return {
        "periodStart": selected["period_start"],
        "periodEnd": selected["period_end"],
        "availableAt": first_available,
        "value": selected["value"],
        "unit": selected["unit"],
        "frequency": cadence,
        "fiscalYear": identity_source.get("fiscal_year"),
        "fiscalPeriod": identity_source.get("fiscal_period"),
        "reported": True,
        "derived": False,
        "derivation": None,
        "confidence": round(max(0.0, confidence), 2),
        "qualityFlags": sorted(flags),
        # `availableAt` must always be substantiated by a returned source. A later
        # filing often repeats an earlier comparative value and wins selection, but
        # returning only that later filing made the observation appear to predate its
        # evidence. Keep both roles explicit while retaining `sources` for consumers
        # that want the complete provenance set.
        "availabilitySource": availability_source,
        "selectedSource": selected_source,
        "sources": sources,
    }


def _add_quarters_derived_from_ytd(
    quarterly: list[dict[str, Any]],
    ytd: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Derive standalone Q2/Q3 from cumulative windows: Qn = YTDn − YTDn−1.

    Cash-flow statements in Q2/Q3 10-Qs carry only year-to-date figures, so the
    quarterly cadence would otherwise hold nothing but fiscal Q1. Windows chain
    on a shared period start: the six-month cumulative minus the first quarter
    yields Q2, the nine-month cumulative minus the six-month yields Q3. Each
    derivation uses only reported windows — a derived quarter never feeds
    another derivation, so one bad filing cannot cascade.
    """
    output = list(quarterly)
    existing_windows = {(row["periodStart"], row["periodEnd"]) for row in quarterly}
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in quarterly + ytd:
        grouped.setdefault((row["periodStart"], row["unit"]), []).append(row)
    for group in grouped.values():
        ordered = sorted(group, key=lambda row: row["periodEnd"])
        for index in range(1, len(ordered)):
            shorter, cumulative = ordered[index - 1], ordered[index]
            if cumulative["frequency"] != "ytd":
                continue
            increment_days = (
                _parse_date(cumulative["periodEnd"]) - _parse_date(shorter["periodEnd"])
            ).days
            if not (QUARTER_MIN_DAYS <= increment_days <= QUARTER_MAX_DAYS):
                continue
            window = (_next_day(shorter["periodEnd"]), cumulative["periodEnd"])
            if window in existing_windows:
                continue
            value = float(cumulative["value"]) - float(shorter["value"])
            flags = (
                set(shorter.get("qualityFlags") or [])
                | set(cumulative.get("qualityFlags") or [])
                | {"derived_from_ytd"}
            )
            implausible = value < 0 and float(shorter["value"]) >= 0 and float(cumulative["value"]) >= 0
            if implausible:
                flags.add("implausible_ytd_residual")
            existing_windows.add(window)
            output.append(
                {
                    "periodStart": window[0],
                    "periodEnd": window[1],
                    "availableAt": max(shorter["availableAt"], cumulative["availableAt"]),
                    "value": value,
                    "unit": cumulative["unit"],
                    "frequency": "quarterly",
                    "fiscalYear": cumulative.get("fiscalYear"),
                    "fiscalPeriod": cumulative.get("fiscalPeriod"),
                    "reported": False,
                    "derived": True,
                    "derivation": "cumulative year-to-date minus the preceding shorter window",
                    "confidence": min(
                        0.9 if not implausible else 0.55,
                        float(shorter["confidence"]),
                        float(cumulative["confidence"]),
                    ),
                    "qualityFlags": sorted(flags),
                    "availabilitySource": max(
                        (shorter, cumulative), key=lambda row: row["availableAt"]
                    )["availabilitySource"],
                    "selectedSource": cumulative["selectedSource"],
                    "sources": shorter["sources"] + cumulative["sources"],
                }
            )
    return output


def _add_derived_fourth_quarters(
    quarterly: list[dict[str, Any]],
    annual: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    output = list(quarterly)
    existing_windows = {(row["periodStart"], row["periodEnd"]) for row in quarterly}
    for annual_row in annual:
        contained = sorted(
            [
                row
                for row in quarterly
                if row["periodStart"] >= annual_row["periodStart"]
                and row["periodEnd"] <= annual_row["periodEnd"]
                and row["unit"] == annual_row["unit"]
            ],
            key=lambda row: row["periodStart"],
        )
        if len(contained) != 3:
            continue
        gaps = [
            (
                _parse_date(contained[index]["periodStart"])
                - _parse_date(contained[index - 1]["periodEnd"])
            ).days
            for index in range(1, len(contained))
        ]
        if any(gap < 0 or gap > 7 for gap in gaps):
            continue
        fourth_start = _next_day(contained[-1]["periodEnd"])
        window = (fourth_start, annual_row["periodEnd"])
        if window in existing_windows:
            continue
        value = float(annual_row["value"]) - sum(float(row["value"]) for row in contained)
        flags = {"derived_q4"}
        if value < 0 and all(float(row["value"]) >= 0 for row in contained):
            flags.add("implausible_annual_residual")
        output.append(
            {
                "periodStart": fourth_start,
                "periodEnd": annual_row["periodEnd"],
                "availableAt": annual_row["availableAt"],
                "value": value,
                "unit": annual_row["unit"],
                "frequency": "quarterly",
                "fiscalYear": annual_row.get("fiscalYear"),
                "fiscalPeriod": "Q4",
                "reported": False,
                "derived": True,
                "derivation": "annual minus the three reported standalone quarters",
                "confidence": 0.9 if len(flags) == 1 else 0.55,
                "qualityFlags": sorted(flags),
                "availabilitySource": annual_row["availabilitySource"],
                "selectedSource": annual_row["selectedSource"],
                "sources": annual_row["sources"]
                + [source for row in contained for source in row["sources"]],
            }
        )
    return output


def _trailing_twelve_months(
    quarterly: list[dict[str, Any]],
    definition: MetricDefinition,
) -> list[dict[str, Any]]:
    if definition.aggregation != "sum":
        return []
    ordered = sorted(quarterly, key=lambda row: row["periodEnd"])
    output: list[dict[str, Any]] = []
    for index in range(3, len(ordered)):
        window = ordered[index - 3 : index + 1]
        if len({row["unit"] for row in window}) != 1:
            continue
        gaps = [
            (_parse_date(window[i]["periodStart"]) - _parse_date(window[i - 1]["periodEnd"])).days
            for i in range(1, 4)
        ]
        if any(gap < 0 or gap > 7 for gap in gaps):
            continue
        flags = sorted({flag for row in window for flag in row["qualityFlags"]})
        output.append(
            {
                "periodStart": window[0]["periodStart"],
                "periodEnd": window[-1]["periodEnd"],
                "availableAt": max(row["availableAt"] for row in window),
                "value": sum(float(row["value"]) for row in window),
                "unit": window[-1]["unit"],
                "frequency": "ttm",
                "fiscalYear": window[-1].get("fiscalYear"),
                "fiscalPeriod": "TTM",
                "reported": False,
                "derived": True,
                "derivation": "sum of four canonical standalone quarters",
                "confidence": min(float(row["confidence"]) for row in window),
                "qualityFlags": flags,
                "availabilitySource": max(
                    window,
                    key=lambda row: row["availableAt"],
                )["availabilitySource"],
                "selectedSource": window[-1]["selectedSource"],
                "sources": [source for row in window for source in row["sources"]],
            }
        )
    return output


def _source_from_fact(row: dict[str, Any]) -> dict[str, Any]:
    accession = str(row["accession_number"])
    cik = str(row.get("cik") or "").lstrip("0")
    source_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession.replace('-', '')}/"
        if cik
        else None
    )
    return {
        "taxonomy": row["taxonomy"],
        "concept": row["concept"],
        "form": row["form"],
        "filed": row["filed"],
        "accessionNumber": accession,
        "frame": row.get("frame"),
        "sourceUrl": source_url,
    }


def _metric(metric: str) -> MetricDefinition:
    return get_metric_definition(metric)


def _parse_date(value: Any) -> date:
    return date.fromisoformat(str(value))


def _next_day(value: str) -> str:
    return (_parse_date(value) + timedelta(days=1)).isoformat()


def _values_match(left: float, right: float) -> bool:
    tolerance = max(abs(left), abs(right), 1.0) * 1e-9
    return abs(left - right) <= tolerance
