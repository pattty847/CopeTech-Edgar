from __future__ import annotations

import math
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from analysis.financial_series_audit.checks import (
    FINDING_SCHEMA_VERSION,
    check_balance_equation,
    check_financial_series,
)


def source(concept: str = "Assets", *, filed: str = "2025-02-01") -> dict:
    return {
        "taxonomy": "us-gaap",
        "concept": concept,
        "filed": filed,
        "accessionNumber": f"accession-{concept}",
    }


def observation(
    value: float = 100.0,
    *,
    start: str = "2024-01-01",
    end: str = "2024-12-31",
    unit: str = "USD",
    concept: str = "Assets",
) -> dict:
    selected = source(concept)
    return {
        "periodStart": start,
        "periodEnd": end,
        "availableAt": "2025-02-01",
        "value": value,
        "unit": unit,
        "reported": True,
        "derived": False,
        "qualityFlags": [],
        "availabilitySource": selected,
        "selectedSource": selected,
        "sources": [selected],
    }


def payload(*rows: dict, metric: str = "total_assets", frequency: str = "annual") -> dict:
    return {
        "symbol": "TEST",
        "metric": metric,
        "frequency": frequency,
        "observations": list(rows),
        "warnings": [],
    }


def codes(findings) -> list[str]:
    return [finding.code for finding in findings]


def test_finding_shape_is_versioned_and_json_ready():
    row = observation(unit="shares")
    (finding,) = check_financial_series(payload(row), expected_unit="USD")

    assert finding.to_dict() == {
        "schemaVersion": FINDING_SCHEMA_VERSION,
        "code": "unexpected_unit",
        "severity": "error",
        "message": "Expected unit 'USD', but the observation uses 'shares'.",
        "symbol": "TEST",
        "metric": "total_assets",
        "frequency": "annual",
        "periodEnd": "2024-12-31",
        "context": {"expectedUnit": "USD", "actualUnit": "shares"},
    }


def test_duplicate_annual_period_end_and_economic_window_are_errors():
    row = observation()
    findings = check_financial_series(payload(row, dict(row)))

    assert codes(findings) == [
        "duplicate_economic_window",
        "duplicate_annual_period_end",
    ]
    assert all(finding.severity == "error" for finding in findings)


def test_nonfinite_value_and_missing_provenance_are_errors():
    row = observation(math.inf)
    row["sources"] = []
    findings = check_financial_series(payload(row))

    assert codes(findings) == ["nonfinite_value", "missing_provenance"]


def test_availability_cannot_predate_filing_evidence():
    row = observation()
    row["availableAt"] = "2025-01-01"
    row["availabilitySource"] = source("Liabilities", filed="2025-03-01")
    findings = check_financial_series(payload(row))

    assert codes(findings) == ["availability_before_evidence"]
    assert findings[0].context["latestRequiredEvidenceAt"] == "2025-03-01"


def test_assumed_zero_flags_are_errors_at_series_and_observation_levels():
    row = observation()
    row["qualityFlags"] = ["debt_concepts_missing_assumed_zero"]
    series = payload(row)
    series["warnings"] = ["optional_input_assumed_zero"]
    findings = check_financial_series(series)

    assert codes(findings).count("missing_input_assumed_zero") == 2


def test_aggregate_and_component_debt_inputs_are_never_combined():
    row = observation(125.0, concept="DebtLongtermAndShorttermCombinedAmount")
    row.update(
        {
            "reported": False,
            "derived": True,
            "inputMetrics": ["total_debt", "debt_current", "cash_equivalents"],
            "sources": row["sources"] + [source("LongTermDebtCurrent")],
        }
    )
    findings = check_financial_series(payload(row, metric="net_debt"))

    assert codes(findings) == ["aggregate_component_debt_double_count"]


def test_selected_concept_transition_is_a_warning():
    older = observation(end="2023-12-31", concept="Revenues")
    newer = observation(end="2024-12-31", concept="RevenueFromContractWithCustomerExcludingAssessedTax")
    findings = check_financial_series(payload(older, newer, metric="revenue"))

    assert codes(findings) == ["selected_concept_transition"]
    assert findings[0].severity == "warning"
    assert findings[0].context["previousConcept"] == "Revenues"


def test_balance_equation_mismatch_is_a_warning():
    series = {
        "total_assets": payload(observation(100.0), metric="total_assets"),
        "total_liabilities": payload(
            observation(60.0, concept="Liabilities"), metric="total_liabilities"
        ),
        "stockholders_equity": payload(
            observation(30.0, concept="StockholdersEquity"),
            metric="stockholders_equity",
        ),
    }
    findings = check_balance_equation(
        series,
        relative_tolerance=0.01,
        absolute_tolerance=0.0,
    )

    assert codes(findings) == ["balance_equation_mismatch"]
    assert findings[0].severity == "warning"
    assert findings[0].context["residual"] == 10.0


def test_balanced_equation_and_missing_inputs_do_not_emit_findings():
    series = {
        "total_assets": payload(observation(100.0), metric="total_assets"),
        "total_liabilities": payload(
            observation(60.0, concept="Liabilities"), metric="total_liabilities"
        ),
        "stockholders_equity": payload(
            observation(40.0, concept="StockholdersEquity"),
            metric="stockholders_equity",
        ),
    }

    assert check_balance_equation(series) == []
    assert check_balance_equation({"total_assets": series["total_assets"]}) == []
