from __future__ import annotations

import unittest

from analysis.phase0_thesis.summary import finviz_lite_stats, top_change_mapping_candidates, top_changes_for_manager


class Phase0SummaryTests(unittest.TestCase):
    def test_finviz_lite_stats_match_expected_counts(self) -> None:
        prior = [
            self._holding("A", 100),
            self._holding("B", 50),
            self._holding("C", 25),
        ]
        current = [
            self._holding("A", 125),
            self._holding("B", 10),
            self._holding("D", 80),
        ]

        stats = finviz_lite_stats(prior, current)

        self.assertEqual(stats["market_value_this_q"], 215)
        self.assertEqual(stats["market_value_prev_q"], 175)
        self.assertEqual(stats["number_of_holdings"], 3)
        self.assertEqual(stats["new_purchases"], 1)
        self.assertEqual(stats["added_to"], 1)
        self.assertEqual(stats["reduced"], 1)
        self.assertEqual(stats["sold_out"], 1)

    def test_top_changes_rank_by_absolute_flow(self) -> None:
        rows = top_changes_for_manager(
            "m",
            "Manager",
            "2025-12-31",
            [self._holding("A", 100), self._holding("B", 500)],
            [self._holding("A", 150), self._holding("B", 100), self._holding("C", 250)],
            limit=2,
        )

        self.assertEqual([row["cusip"] for row in rows], ["B", "C"])

    def test_mapping_candidates_are_unique_and_limited(self) -> None:
        rows = [
            {"cusip": "A", "issuer_name": "A", "value_change_usd": 100},
            {"cusip": "A", "issuer_name": "A", "value_change_usd": 90},
            {"cusip": "B", "issuer_name": "B", "value_change_usd": -80},
        ]

        candidates = top_change_mapping_candidates(rows, limit=2)

        self.assertEqual(candidates, [{"cusip": "A", "issuer_name": "A"}, {"cusip": "B", "issuer_name": "B"}])

    @staticmethod
    def _holding(cusip: str, value: float) -> dict:
        return {
            "cusip": cusip,
            "put_call": "",
            "issuer_name": f"{cusip} CO",
            "value_usd": value,
            "holding_kind": "common",
        }


if __name__ == "__main__":
    unittest.main()
