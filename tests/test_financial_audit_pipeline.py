from __future__ import annotations

import asyncio
import json
from pathlib import Path

from analysis.financial_series_audit.pipeline import build_metric_matrix
from analysis.financial_series_audit.report import write_run_report


def _fact(value: float, *, concept: str) -> dict:
    return {
        "val": value,
        "start": "2024-01-01",
        "end": "2024-12-31",
        "filed": "2025-02-01",
        "accn": f"{concept}-accession",
        "form": "10-K",
        "fy": 2024,
        "fp": "FY",
    }


def test_pipeline_resolves_all_metrics_without_additional_acquisition(tmp_path: Path):
    payload = {
        "cik": 1,
        "entityName": "Audit Fixture",
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {"USD": [_fact(100, concept="revenue")]}
                }
            }
        },
    }

    report = asyncio.run(
        build_metric_matrix("TEST", payload, store_path=tmp_path / "series.sqlite3")
    )

    annual_revenue = next(
        row
        for row in report["metrics"]
        if row["metric"] == "revenue" and row["frequency"] == "annual"
    )
    assert annual_revenue["state"] == "reported"
    assert annual_revenue["observations"][0]["value"] == 100
    assert len(report["valuations"]) == 7
    assert {row["state"] for row in report["valuations"]} == {
        "external_input_missing"
    }


def test_report_writes_json_csv_and_manual_review_packet(tmp_path: Path):
    source = {
        "taxonomy": "us-gaap",
        "concept": "Revenues",
        "accessionNumber": "0000000001-25-000001",
    }
    issuer = {
        "ticker": "TEST",
        "cik": "0000000001",
        "entityName": "Audit Fixture",
        "metrics": [
            {
                "metric": "revenue",
                "frequency": "annual",
                "state": "reported",
                "observations": [
                    {
                        "periodEnd": "2024-12-31",
                        "value": 100,
                        "unit": "USD",
                        "selectedSource": source,
                        "qualityFlags": [],
                    }
                ],
                "warnings": [],
            }
        ],
        "valuations": [],
    }

    run_dir = write_run_report(tmp_path, [issuer], [])

    summary = json.loads((run_dir / "summary.json").read_text())
    assert summary["issuerCount"] == 1
    summary_observation = summary["issuers"][0]["metrics"][0]["observations"][0]
    assert "sources" not in summary_observation
    assert summary_observation["sourceCount"] == 0
    assert "TEST" in (run_dir / "coverage.csv").read_text()
    review = (run_dir / "manual-review.md").read_text()
    assert "[ ] | revenue | 100 USD" in review
    assert "0000000001-25-000001-index.html" in review
