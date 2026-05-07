from __future__ import annotations

import unittest

from analysis.phase0_thesis.metrics import compute_manager_metrics, compute_quarter_diffs


class Phase0MetricsTests(unittest.TestCase):
    def test_synthetic_portfolio_metrics_match_hand_computed_values(self) -> None:
        prior = [
            self._holding("A", "", 100, "common"),
            self._holding("B", "CALL", 50, "call"),
        ]
        current = [
            self._holding("A", "", 150, "common"),
            self._holding("B", "CALL", 25, "call"),
            self._holding("C", "", 75, "common"),
        ]

        metrics = compute_manager_metrics(prior, current)

        self.assertEqual(metrics["total_portfolio_value_usd"], 250)
        self.assertEqual(metrics["holding_count"], 3)
        self.assertEqual(metrics["top10_concentration"], 1.0)
        self.assertEqual(metrics["options_ratio"], 0.1)
        self.assertAlmostEqual(metrics["qoq_turnover"], 1.0)
        self.assertEqual(metrics["new_position_count"], 1)
        self.assertEqual(metrics["exit_count"], 0)

    def test_empty_quarter_avoids_divide_by_zero(self) -> None:
        metrics = compute_manager_metrics([], [])

        self.assertEqual(metrics["total_portfolio_value_usd"], 0.0)
        self.assertEqual(metrics["top10_concentration"], 0.0)
        self.assertEqual(metrics["qoq_turnover"], 0.0)
        self.assertEqual(metrics["options_ratio"], 0.0)

    def test_quarter_diffs_detect_exits_and_increases(self) -> None:
        diffs = compute_quarter_diffs(
            [self._holding("A", "", 100, "common"), self._holding("B", "", 50, "common")],
            [self._holding("A", "", 180, "common")],
        )

        by_cusip = {row["cusip"]: row for row in diffs}
        self.assertEqual(by_cusip["A"]["direction"], "increased")
        self.assertEqual(by_cusip["B"]["direction"], "sold_out")

    @staticmethod
    def _holding(cusip: str, put_call: str, value: float, holding_kind: str) -> dict:
        return {
            "cusip": cusip,
            "put_call": put_call,
            "issuer_name": f"ISSUER {cusip}",
            "value_usd": value,
            "holding_kind": holding_kind,
        }


if __name__ == "__main__":
    unittest.main()
