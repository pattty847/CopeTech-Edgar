from __future__ import annotations

import math
import tempfile
import unittest
from pathlib import Path

from analysis.phase0_thesis.consensus_score import (
    compute_herd_deviation,
    compute_self_deviation,
    generate_consensus_signals,
)
from analysis.phase0_thesis.db import connect, init_schema, upsert_rows


class Phase0ConsensusScoreTests(unittest.TestCase):
    def test_first_filing_has_null_self_deviation_baseline(self) -> None:
        self.assertIsNone(compute_self_deviation([], 100))
        self.assertIsNone(compute_self_deviation([50], 100))

    def test_never_held_scale_jump_scores_higher_than_repeat_holding(self) -> None:
        repeat = compute_self_deviation([90, 100, 110], 105)
        unusual = compute_self_deviation([10, 20, 30], 250)

        self.assertIsNotNone(repeat)
        self.assertIsNotNone(unusual)
        self.assertGreater(unusual, repeat)

    def test_buy_against_herd_has_positive_herd_deviation(self) -> None:
        score = compute_herd_deviation(200, [-100, -75, -25, 200])

        self.assertIsNotNone(score)
        self.assertGreater(score, 0)

    def test_generate_signals_stores_nulls_when_baselines_are_thin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            con = connect(Path(temp_dir) / "phase0.duckdb")
            init_schema(con)
            upsert_rows(con, "filings", [
                self._filing("m1", "2024-03-31"),
                self._filing("m1", "2024-06-30"),
            ])
            upsert_rows(con, "holdings", [
                self._holding("m1", "2024-03-31", "OLD", 100),
                self._holding("m1", "2024-06-30", "OLD", 125),
                self._holding("m1", "2024-06-30", "NEW", 200),
            ])
            upsert_rows(con, "cusip_ticker_map", [
                {
                    "cusip": "NEW",
                    "ticker": "NEW",
                    "issuer_name": "NEW CO",
                    "mapping_confidence": "openfigi",
                    "source_updated_at": "now",
                }
            ])

            signals = generate_consensus_signals(con)
            new_signal = next(row for row in signals if row["cusip"] == "NEW")

            self.assertEqual(len(signals), 2)
            self.assertTrue(new_signal["self_dev_z"] is None or math.isnan(new_signal["self_dev_z"]))
            con.close()

    @staticmethod
    def _filing(manager_cik: str, report_date: str) -> dict:
        return {
            "manager_cik": manager_cik,
            "accession_no": f"{manager_cik}-{report_date}",
            "form_type": "13F-HR",
            "report_date": report_date,
            "filing_date": report_date,
            "is_canonical": True,
        }

    @staticmethod
    def _holding(manager_cik: str, report_date: str, cusip: str, value: float) -> dict:
        return {
            "manager_cik": manager_cik,
            "report_date": report_date,
            "accession_no": f"{manager_cik}-{report_date}",
            "cusip": cusip,
            "issuer_name": f"{cusip} CO",
            "put_call": "",
            "shares": 1,
            "value_usd": value,
            "holding_kind": "common",
        }


if __name__ == "__main__":
    unittest.main()
