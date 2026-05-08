from __future__ import annotations

import unittest

from analysis.phase0_thesis.ingest import (
    canonicalize_filings,
    dedupe_holdings,
    infer_holding_kind,
    normalize_holding,
    select_filings_window,
)


class Phase0IngestionTests(unittest.TestCase):
    def test_amendment_supersession_marks_latest_amendment_canonical(self) -> None:
        filings = [
            {
                "accession_no": "original",
                "form": "13F-HR",
                "report_date": "2025-03-31",
                "filing_date": "2025-05-10",
            },
            {
                "accession_no": "amended",
                "form": "13F-HR/A",
                "report_date": "2025-03-31",
                "filing_date": "2025-05-20",
            },
        ]

        canonicalized = canonicalize_filings(filings)

        self.assertFalse(next(row for row in canonicalized if row["accession_no"] == "original")["is_canonical"])
        self.assertTrue(next(row for row in canonicalized if row["accession_no"] == "amended")["is_canonical"])

    def test_cusip_put_call_dedupe_keeps_same_issuer_options(self) -> None:
        rows = [
            self._holding("CALL", value_usd=100),
            self._holding("PUT", value_usd=200),
            self._holding("CALL", value_usd=300),
        ]

        deduped = dedupe_holdings(rows)

        self.assertEqual(len(deduped), 2)
        self.assertEqual(
            sorted(row["put_call"] for row in deduped),
            ["CALL", "PUT"],
        )
        call_row = next(row for row in deduped if row["put_call"] == "CALL")
        self.assertEqual(call_row["shares"], 2)
        self.assertEqual(call_row["value_usd"], 400)

    def test_normalize_holding_uses_full_usd_value(self) -> None:
        normalized = normalize_holding(
            "1446194",
            "2025-12-31",
            "accession",
            {
                "cusip": "037833100",
                "issuer": "Apple Inc.",
                "shares": 100,
                "value_thousands": 1450,
                "value": 1_450_000,
            },
        )

        self.assertEqual(normalized["value_usd"], 1_450_000)

    def test_window_selects_eight_quarter_range_across_year_boundary(self) -> None:
        filings = [
            {"form": "13F-HR", "report_date": "2023-12-31"},
            {"form": "13F-HR", "report_date": "2024-03-31"},
            {"form": "13F-HR", "report_date": "2025-12-31"},
            {"form": "13F-HR", "report_date": "2026-03-31"},
            {"form": "4", "report_date": "2025-03-31"},
        ]

        selected = select_filings_window(filings)

        self.assertEqual([row["report_date"] for row in selected], ["2024-03-31", "2025-12-31"])

    def test_holding_kind_classification(self) -> None:
        self.assertEqual(infer_holding_kind({"put_call": "CALL"}), "call")
        self.assertEqual(infer_holding_kind({"put_call": "PUT"}), "put")
        self.assertEqual(infer_holding_kind({"issuer": "SPDR S&P 500 ETF TRUST"}), "etf_or_index")
        self.assertEqual(infer_holding_kind({"cusip": "037833100", "shares": 10}), "common")

    @staticmethod
    def _holding(put_call: str, value_usd: float) -> dict:
        return {
            "manager_cik": "0001",
            "report_date": "2025-03-31",
            "accession_no": "a",
            "cusip": "123456789",
            "issuer_name": "SAME ISSUER",
            "put_call": put_call,
            "shares": 1,
            "value_usd": value_usd,
            "holding_kind": put_call.lower(),
        }


if __name__ == "__main__":
    unittest.main()
