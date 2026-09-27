"""Blocking, full-matrix contracts for recorded inputs (no SEC access)."""

from __future__ import annotations

from typing import Any

from copetech_sec.financial_series_service import FinancialSeriesService

from .corpus import CorpusIssuer
from .run import _audit_matrix


def validate_recorded_matrix(issuer: CorpusIssuer, matrix: dict[str, Any]) -> None:
    """Check all outputs, including metrics with no manually reviewed value yet.

    Missing expected values remain coverage warnings: a minimized input may lack
    a complete history. Exceptions, structural failures, and missing mandatory
    comparability disclosures are never acceptable in the deterministic gate.
    """
    expected = {
        (definition["id"], frequency)
        for definition in FinancialSeriesService.supported_metrics()
        for frequency in definition["frequencies"]
    }
    actual = [(row["metric"], row["frequency"]) for row in matrix["metrics"]]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        raise AssertionError(f"{issuer.ticker}: incomplete or duplicated metric/frequency matrix")
    blockers = [
        finding.to_dict() for finding in _audit_matrix(issuer, matrix)
        if finding.severity == "error" or finding.code == "missing_comparability_warning"
    ]
    if blockers:
        raise AssertionError(f"{issuer.ticker}: recorded matrix failed: {blockers}")


def expectation_coverage(matrix: dict[str, Any], expected: dict[str, Any]) -> list[dict[str, Any]]:
    """Expose assertion gaps without turning parser output into expected truth."""
    counts: dict[tuple[str, str], int] = {}
    unavailable: dict[tuple[str, str], int] = {}
    for period in expected["periods"]:
        for metric in period["metrics"]:
            key = (metric, period["frequency"])
            counts[key] = counts.get(key, 0) + 1
        for metric in period.get("unavailableMetrics", []):
            key = (metric, period["frequency"])
            unavailable[key] = unavailable.get(key, 0) + 1
    return [
        {
            "metric": row["metric"], "frequency": row["frequency"],
            "observations": len(row["observations"]),
            "valueAssertions": counts.get((row["metric"], row["frequency"]), 0),
            "unavailableAssertions": unavailable.get((row["metric"], row["frequency"]), 0),
            "filingCitationsProvided": bool(expected.get("reviewedFilings")),
        }
        for row in matrix["metrics"]
    ]
