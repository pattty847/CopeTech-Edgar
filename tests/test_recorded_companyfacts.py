from __future__ import annotations

import asyncio
import json
from pathlib import Path

from analysis.financial_series_audit.pipeline import build_metric_matrix


FIXTURE_ROOT = Path(__file__).parent / "fixtures" / "sec" / "companyfacts"


def test_recorded_companyfacts_match_independently_reviewed_periods(tmp_path: Path):
    for symbol in ("AAPL", "SOFI"):
        fixture_dir = FIXTURE_ROOT / symbol.lower()
        payload = json.loads((fixture_dir / "companyfacts.json").read_text())
        expected = json.loads((fixture_dir / "expected.json").read_text())
        matrix = asyncio.run(
            build_metric_matrix(
                symbol,
                payload,
                store_path=tmp_path / f"{symbol.lower()}-series.sqlite3",
            )
        )
        series = {
            (row["frequency"], row["metric"]): row
            for row in matrix["metrics"]
        }

        for period in expected["periods"]:
            frequency = period["frequency"]
            period_end = period["periodEnd"]
            for metric, metric_expected in period["metrics"].items():
                observation = next(
                    row
                    for row in series[(frequency, metric)]["observations"]
                    if row["periodEnd"] == period_end
                )
                assert observation["value"] == metric_expected["value"]
                if "concept" in metric_expected:
                    assert (
                        observation["selectedSource"]["concept"]
                        == metric_expected["concept"]
                    )
                if "inputMetrics" in metric_expected:
                    assert observation["inputMetrics"] == metric_expected["inputMetrics"]
