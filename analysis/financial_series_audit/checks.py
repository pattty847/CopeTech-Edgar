"""Pure correctness and review checks for financial-series audit payloads."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
import math
from typing import Any, Literal, Mapping, Sequence


FINDING_SCHEMA_VERSION = 1
Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class AuditFinding:
    """One stable, JSON-ready audit result."""

    code: str
    severity: Severity
    message: str
    symbol: str
    metric: str | None = None
    frequency: str | None = None
    period_end: str | None = None
    context: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "schemaVersion": FINDING_SCHEMA_VERSION,
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "symbol": self.symbol,
            "metric": self.metric,
            "frequency": self.frequency,
            "periodEnd": self.period_end,
            "context": dict(self.context),
        }


_PROVENANCE_FIELDS = ("taxonomy", "concept", "filed", "accessionNumber")
_AGGREGATE_DEBT_CONCEPTS = frozenset(
    {
        "DebtLongtermAndShorttermCombinedAmount",
        "LongTermDebt",
        "LongTermDebtAndCapitalLeaseObligations",
    }
)
_COMPONENT_DEBT_CONCEPTS = frozenset(
    {
        "LongTermDebtCurrent",
        "DebtCurrent",
        "LongTermDebtAndCapitalLeaseObligationsCurrent",
        "LongTermDebtNoncurrent",
    }
)


def check_financial_series(
    payload: Mapping[str, Any],
    *,
    expected_unit: str | None = None,
) -> list[AuditFinding]:
    """Check one production-shaped financial-series payload without I/O."""

    symbol = str(payload.get("symbol") or "").upper()
    metric = _optional_string(payload.get("metric"))
    frequency = _optional_string(payload.get("frequency"))
    observations = [
        row for row in payload.get("observations") or [] if isinstance(row, Mapping)
    ]
    findings: list[AuditFinding] = []

    findings.extend(
        _duplicate_window_findings(
            observations,
            symbol=symbol,
            metric=metric,
            frequency=frequency,
        )
    )
    findings.extend(
        _payload_assumed_zero_findings(
            payload,
            symbol=symbol,
            metric=metric,
            frequency=frequency,
        )
    )

    for row in observations:
        period_end = _optional_string(row.get("periodEnd"))
        value = row.get("value")
        if not _is_finite_number(value):
            findings.append(
                _finding(
                    "nonfinite_value",
                    "error",
                    "The observation value is not a finite number.",
                    symbol,
                    metric,
                    frequency,
                    period_end,
                    value=repr(value),
                )
            )

        actual_unit = _optional_string(row.get("unit"))
        if expected_unit is not None and actual_unit != expected_unit:
            findings.append(
                _finding(
                    "unexpected_unit",
                    "error",
                    f"Expected unit {expected_unit!r}, but the observation uses {actual_unit!r}.",
                    symbol,
                    metric,
                    frequency,
                    period_end,
                    expectedUnit=expected_unit,
                    actualUnit=actual_unit,
                )
            )

        sources = row.get("sources")
        valid_sources = _valid_sources(sources)
        if not valid_sources:
            findings.append(
                _finding(
                    "missing_provenance",
                    "error",
                    "The observation has no complete filing provenance.",
                    symbol,
                    metric,
                    frequency,
                    period_end,
                )
            )
        else:
            available_at = _iso_date(row.get("availableAt"))
            availability_source = row.get("availabilitySource")
            evidence_at = (
                _iso_date(availability_source.get("filed"))
                if isinstance(availability_source, Mapping)
                else None
            )
            if available_at is not None and evidence_at is not None and available_at < evidence_at:
                findings.append(
                    _finding(
                        "availability_before_evidence",
                        "error",
                        "The observation becomes available before its latest required filing evidence.",
                        symbol,
                        metric,
                        frequency,
                        period_end,
                        availableAt=available_at.isoformat(),
                        latestRequiredEvidenceAt=evidence_at.isoformat(),
                    )
                )

        flags = [str(flag) for flag in row.get("qualityFlags") or []]
        if any("assumed_zero" in flag for flag in flags):
            findings.append(
                _finding(
                    "missing_input_assumed_zero",
                    "error",
                    "A derived value treats an unavailable input as zero.",
                    symbol,
                    metric,
                    frequency,
                    period_end,
                    qualityFlags=flags,
                )
            )

        if _mixes_debt_parent_and_components(row, valid_sources):
            findings.append(
                _finding(
                    "aggregate_component_debt_double_count",
                    "error",
                    "The derivation uses aggregate debt together with debt components.",
                    symbol,
                    metric,
                    frequency,
                    period_end,
                    inputMetrics=_input_metrics(row),
                    concepts=sorted(
                        {
                            str(source.get("concept"))
                            for source in valid_sources
                            if source.get("concept")
                        }
                    ),
                )
            )

    findings.extend(
        _concept_transition_findings(
            observations,
            symbol=symbol,
            metric=metric,
            frequency=frequency,
        )
    )
    return findings


def check_balance_equation(
    series_by_metric: Mapping[str, Mapping[str, Any]],
    *,
    relative_tolerance: float = 0.01,
    absolute_tolerance: float = 1.0,
) -> list[AuditFinding]:
    """Warn when assets differ materially from liabilities plus equity."""

    if relative_tolerance < 0 or absolute_tolerance < 0:
        raise ValueError("balance-equation tolerances must be non-negative")
    required = ("total_assets", "total_liabilities", "stockholders_equity")
    if any(metric not in series_by_metric for metric in required):
        return []

    indexed = {
        metric: _latest_by_period_end(series_by_metric[metric]) for metric in required
    }
    common_periods = set.intersection(*(set(rows) for rows in indexed.values()))
    identity = series_by_metric["total_assets"]
    symbol = str(identity.get("symbol") or "").upper()
    frequency = _optional_string(identity.get("frequency"))
    findings: list[AuditFinding] = []
    for period_end in sorted(common_periods):
        assets = float(indexed["total_assets"][period_end]["value"])
        liabilities = float(indexed["total_liabilities"][period_end]["value"])
        equity = float(indexed["stockholders_equity"][period_end]["value"])
        residual = assets - liabilities - equity
        tolerance = absolute_tolerance + relative_tolerance * max(abs(assets), 1.0)
        if abs(residual) <= tolerance:
            continue
        findings.append(
            _finding(
                "balance_equation_mismatch",
                "warning",
                "Assets differ materially from liabilities plus stockholders' equity.",
                symbol,
                "balance_equation",
                frequency,
                period_end,
                assets=assets,
                liabilities=liabilities,
                stockholdersEquity=equity,
                residual=residual,
                tolerance=tolerance,
            )
        )
    return findings


def _duplicate_window_findings(
    observations: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
    metric: str | None,
    frequency: str | None,
) -> list[AuditFinding]:
    findings: list[AuditFinding] = []
    by_window: dict[tuple[Any, Any], int] = {}
    by_annual_end: dict[Any, int] = {}
    for row in observations:
        window = (row.get("periodStart"), row.get("periodEnd"))
        by_window[window] = by_window.get(window, 0) + 1
        if frequency == "annual":
            period_end = row.get("periodEnd")
            by_annual_end[period_end] = by_annual_end.get(period_end, 0) + 1
    for window, count in by_window.items():
        if count > 1:
            findings.append(
                _finding(
                    "duplicate_economic_window",
                    "error",
                    "More than one observation resolves to the same economic window.",
                    symbol,
                    metric,
                    frequency,
                    _optional_string(window[1]),
                    periodStart=window[0],
                    count=count,
                )
            )
    for period_end, count in by_annual_end.items():
        if count > 1:
            findings.append(
                _finding(
                    "duplicate_annual_period_end",
                    "error",
                    "More than one annual observation uses the same period end.",
                    symbol,
                    metric,
                    frequency,
                    _optional_string(period_end),
                    count=count,
                )
            )
    return findings


def _payload_assumed_zero_findings(
    payload: Mapping[str, Any],
    *,
    symbol: str,
    metric: str | None,
    frequency: str | None,
) -> list[AuditFinding]:
    flags = [str(flag) for flag in payload.get("warnings") or []]
    if not any("assumed_zero" in flag for flag in flags):
        return []
    return [
        _finding(
            "missing_input_assumed_zero",
            "error",
            "The series reports that an unavailable input was treated as zero.",
            symbol,
            metric,
            frequency,
            None,
            qualityFlags=flags,
        )
    ]


def _concept_transition_findings(
    observations: Sequence[Mapping[str, Any]],
    *,
    symbol: str,
    metric: str | None,
    frequency: str | None,
) -> list[AuditFinding]:
    selected: list[tuple[str, str, str]] = []
    for row in sorted(observations, key=lambda item: str(item.get("periodEnd") or "")):
        # Derived metrics already inherit and expose their component provenance.
        # Audit concept changes on the reported inputs once, not again on every ratio.
        if row.get("reported") is not True:
            continue
        source = row.get("selectedSource")
        if not isinstance(source, Mapping):
            continue
        taxonomy = _optional_string(source.get("taxonomy"))
        concept = _optional_string(source.get("concept"))
        period_end = _optional_string(row.get("periodEnd"))
        if taxonomy and concept and period_end:
            selected.append((period_end, taxonomy, concept))

    transitions: dict[tuple[str, str, str, str], list[tuple[str, str]]] = {}
    for previous, current in zip(selected, selected[1:]):
        if previous[1:] == current[1:]:
            continue
        key = (previous[1], previous[2], current[1], current[2])
        transitions.setdefault(key, []).append((previous[0], current[0]))

    findings: list[AuditFinding] = []
    for key, windows in transitions.items():
        previous_taxonomy, previous_concept, current_taxonomy, current_concept = key
        findings.append(
            _finding(
                "selected_concept_transition",
                "warning",
                "The selected taxonomy concept changes across reported periods.",
                symbol,
                metric,
                frequency,
                windows[0][1],
                previousPeriodEnd=windows[0][0],
                previousTaxonomy=previous_taxonomy,
                previousConcept=previous_concept,
                currentTaxonomy=current_taxonomy,
                currentConcept=current_concept,
                transitionCount=len(windows),
                lastTransitionPeriodEnd=windows[-1][1],
            )
        )
    return findings


def _valid_sources(raw_sources: Any) -> list[Mapping[str, Any]]:
    if not isinstance(raw_sources, Sequence) or isinstance(raw_sources, (str, bytes)):
        return []
    return [
        source
        for source in raw_sources
        if isinstance(source, Mapping)
        and all(source.get(field) not in (None, "") for field in _PROVENANCE_FIELDS)
    ]


def _mixes_debt_parent_and_components(
    row: Mapping[str, Any],
    sources: Sequence[Mapping[str, Any]],
) -> bool:
    input_metrics = set(_input_metrics(row))
    debt_children = {"long_term_debt", "debt_current", "debt_noncurrent", "short_term_borrowings"}
    if "total_debt" in input_metrics and input_metrics.intersection(debt_children):
        return True
    if "long_term_debt" in input_metrics and input_metrics.intersection(
        {"debt_current", "debt_noncurrent"}
    ):
        return True
    concepts = {str(source.get("concept") or "") for source in sources}
    return bool(
        concepts.intersection(_AGGREGATE_DEBT_CONCEPTS)
        and concepts.intersection(_COMPONENT_DEBT_CONCEPTS)
    )


def _input_metrics(row: Mapping[str, Any]) -> list[str]:
    raw = row.get("inputMetrics", row.get("componentsUsed", ()))
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return []
    return [str(metric) for metric in raw]


def _latest_by_period_end(payload: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    indexed: dict[str, Mapping[str, Any]] = {}
    for row in payload.get("observations") or []:
        if not isinstance(row, Mapping) or not _is_finite_number(row.get("value")):
            continue
        period_end = _optional_string(row.get("periodEnd"))
        if period_end is None:
            continue
        previous = indexed.get(period_end)
        if previous is None or str(row.get("availableAt") or "") > str(
            previous.get("availableAt") or ""
        ):
            indexed[period_end] = row
    return indexed


def _finding(
    code: str,
    severity: Severity,
    message: str,
    symbol: str,
    metric: str | None,
    frequency: str | None,
    period_end: str | None,
    **context: Any,
) -> AuditFinding:
    return AuditFinding(
        code=code,
        severity=severity,
        message=message,
        symbol=symbol,
        metric=metric,
        frequency=frequency,
        period_end=period_end,
        context=context,
    )


def _is_finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _iso_date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _optional_string(value: Any) -> str | None:
    return None if value in (None, "") else str(value)
