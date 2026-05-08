from __future__ import annotations

import unittest

import pandas as pd

from analysis.phase0_thesis.report import manager_summary_table, top_changes_table


class Phase0ReportTests(unittest.TestCase):
    def test_manager_summary_table_formats_money_and_percent(self) -> None:
        table = manager_summary_table(
            pd.DataFrame([
                {
                    "manager_name": "Goblin Capital",
                    "report_date": "2025-12-31",
                    "market_value_this_q": 1_500_000_000,
                    "market_value_prev_q": 1_250_000_000,
                    "number_of_holdings": 42,
                    "new_purchases": 3,
                    "added_to": 10,
                    "reduced": 8,
                    "top_10_holdings_pct": 0.25,
                }
            ])
        )

        self.assertIn("$1.50B", table)
        self.assertIn("25.00%", table)
        self.assertIn("| 42 |", table)

    def test_top_changes_table_sorts_by_absolute_change(self) -> None:
        table = top_changes_table(
            pd.DataFrame([
                {"manager_name": "M", "ticker": "SML", "issuer_name": "Small", "put_call": "", "direction": "added", "value_change_usd": 10},
                {"manager_name": "M", "ticker": "LRG", "issuer_name": "Large", "put_call": "CALL", "direction": "reduced", "value_change_usd": -1_000_000_000},
            ]),
            limit=1,
        )

        self.assertIn("Large", table)
        self.assertIn("LRG", table)
        self.assertNotIn("Small", table)


if __name__ == "__main__":
    unittest.main()
