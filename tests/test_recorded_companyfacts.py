from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import pytest

from analysis.financial_series_audit.pipeline import build_metric_matrix
from analysis.financial_series_audit.corpus import corpus_by_ticker
from analysis.financial_series_audit.fixtures import canonical_json_sha256
from analysis.financial_series_audit.gate import expectation_coverage, validate_recorded_matrix


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "sec" / "companyfacts"


def test_recorded_corpus_has_exactly_the_pinned_issuers():
    assert {path.name.upper() for path in FIXTURE_ROOT.iterdir() if path.is_dir()} == set(corpus_by_ticker())


@pytest.mark.parametrize("symbol", sorted(corpus_by_ticker()))
def test_recorded_companyfacts_match_independently_reviewed_periods(tmp_path: Path, symbol: str):
    fixture_dir = FIXTURE_ROOT / symbol.lower()
    payload = json.loads((fixture_dir / "companyfacts.json").read_text())
    expected = json.loads((fixture_dir / "expected.json").read_text())
    manifest = json.loads((fixture_dir / "manifest.json").read_text())
    inventory = json.loads((fixture_dir / "concept-index.json").read_text())
    assert str(payload["cik"]).zfill(10) == corpus_by_ticker()[symbol].cik
    assert canonical_json_sha256(payload) == manifest["fixtureContentSha256"]
    assert canonical_json_sha256(inventory) == manifest["conceptInventoryContentSha256"]
    matrix = asyncio.run(
        build_metric_matrix(
            symbol,
            payload,
            store_path=tmp_path / f"{symbol.lower()}-series.sqlite3",
        )
    )
    validate_recorded_matrix(corpus_by_ticker()[symbol], matrix)
    if output := os.environ.get("FUNDAMENTALS_COVERAGE_DIR"):
        destination = Path(output)
        destination.mkdir(parents=True, exist_ok=True)
        (destination / f"{symbol.lower()}.json").write_text(json.dumps({
            "symbol": symbol, "coverage": expectation_coverage(matrix, expected),
            "valuations": matrix["valuations"],
        }, indent=2) + "\n")
    series = {
        (row["frequency"], row["metric"]): row
        for row in matrix["metrics"]
    }

    for period in expected["periods"]:
        frequency = period["frequency"]
        period_end = period["periodEnd"]
        for metric, metric_expected in period["metrics"].items():
            matches = [
                row
                for row in series[(frequency, metric)]["observations"]
                if row["periodEnd"] == period_end
            ]
            assert len(matches) == 1, (symbol, frequency, metric, period_end)
            observation = matches[0]
            assert observation["value"] == metric_expected["value"]
            if "concept" in metric_expected:
                assert (
                    observation["selectedSource"]["concept"]
                    == metric_expected["concept"]
                )
            if "inputMetrics" in metric_expected:
                assert observation["inputMetrics"] == metric_expected["inputMetrics"]
            for field in ("periodStart", "availableAt", "unit", "fiscalYear", "fiscalPeriod", "evidenceMetrics"):
                if field in metric_expected:
                    assert observation[field] == metric_expected[field]
            if "qualityFlags" in metric_expected:
                assert set(metric_expected["qualityFlags"]) <= set(observation["qualityFlags"])
        for metric in period.get("unavailableMetrics", []):
            assert not any(
                row["periodEnd"] == period_end
                for row in series[(frequency, metric)]["observations"]
            )

    for absent in expected.get("absentWindows", []):
        assert not any(
            row["periodStart"] == absent["periodStart"]
            and row["periodEnd"] == absent["periodEnd"]
            for row in series[(absent["frequency"], absent["metric"])][
                "observations"
            ]
        )
