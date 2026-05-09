#!/usr/bin/env python3
"""Tests for Form8KProcessor — item-code parsing and event aggregation."""

from __future__ import annotations

import unittest
from typing import Any

from copetech_sec.form8k_processor import Form8KProcessor, ITEM_CODE_MAP


class ParseItemsTests(unittest.TestCase):
    def test_parses_known_codes_with_labels_and_categories(self):
        items = Form8KProcessor.parse_items_string("2.02,5.02,9.01")
        self.assertEqual(len(items), 3)
        codes = [item["code"] for item in items]
        self.assertEqual(codes, ["2.02", "5.02", "9.01"])
        self.assertEqual(items[0]["category"], "financial_results")
        self.assertEqual(items[1]["category"], "exec_change")
        self.assertEqual(items[2]["category"], "exhibits")

    def test_handles_whitespace_and_empty_segments(self):
        items = Form8KProcessor.parse_items_string("  2.02 ,, 5.02 ,")
        codes = [item["code"] for item in items]
        self.assertEqual(codes, ["2.02", "5.02"])

    def test_unknown_codes_are_kept_with_unknown_category(self):
        items = Form8KProcessor.parse_items_string("99.99")
        self.assertEqual(items, [{"code": "99.99", "label": "Unknown item", "category": "unknown"}])

    def test_returns_empty_list_for_empty_or_none_input(self):
        self.assertEqual(Form8KProcessor.parse_items_string(""), [])
        self.assertEqual(Form8KProcessor.parse_items_string(None), [])

    def test_dedupes_repeated_codes(self):
        items = Form8KProcessor.parse_items_string("2.02,2.02,5.02")
        codes = [item["code"] for item in items]
        self.assertEqual(codes, ["2.02", "5.02"])

    def test_item_code_map_is_complete_for_common_codes(self):
        for code in ["1.01", "2.02", "5.02", "7.01", "8.01", "9.01"]:
            self.assertIn(code, ITEM_CODE_MAP)
            descriptor = ITEM_CODE_MAP[code]
            self.assertIn("label", descriptor)
            self.assertIn("category", descriptor)

    def test_has_high_signal_detects_exec_change(self):
        items = Form8KProcessor.parse_items_string("5.02")
        self.assertTrue(Form8KProcessor.has_high_signal(items))

    def test_has_high_signal_returns_false_for_routine_disclosure(self):
        items = Form8KProcessor.parse_items_string("7.01,8.01,9.01")
        self.assertFalse(Form8KProcessor.has_high_signal(items))


class Get8KEventsTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _filing(accession: str, filing_date: str, items: str) -> dict[str, Any]:
        return {
            "accession_no": accession,
            "filing_date": filing_date,
            "report_date": filing_date,
            "form": "8-K",
            "url": f"https://www.sec.gov/{accession}/",
            "primary_document": f"{accession}.htm",
            "items": items,
        }

    async def _processor(self, filings: list[dict]) -> Form8KProcessor:
        async def fetch_filings(ticker: str, *, days_back: int, use_cache: bool) -> list[dict]:
            return filings

        return Form8KProcessor(fetch_filings_func=fetch_filings)

    async def test_returns_events_sorted_newest_first(self):
        processor = await self._processor(
            [
                self._filing("ACC-1", "2026-05-10", "2.02,9.01"),
                self._filing("ACC-2", "2026-04-30", "5.02"),
                self._filing("ACC-3", "2026-05-12", "7.01"),
            ]
        )
        payload = await processor.get_8k_events("aapl", days_back=180)
        self.assertEqual(payload["symbol"], "AAPL")
        dates = [event["filing_date"] for event in payload["events"]]
        self.assertEqual(dates, ["2026-05-12", "2026-05-10", "2026-04-30"])

    async def test_high_signal_flag_and_totals(self):
        processor = await self._processor(
            [
                self._filing("ACC-1", "2026-05-10", "2.02,9.01"),  # financial_results → high
                self._filing("ACC-2", "2026-04-30", "7.01"),       # disclosure only → not high
                self._filing("ACC-3", "2026-04-25", "5.02"),       # exec_change → high
            ]
        )
        payload = await processor.get_8k_events("AAPL", days_back=180)

        events_by_acc = {event["accession_no"]: event for event in payload["events"]}
        self.assertTrue(events_by_acc["ACC-1"]["high_signal"])
        self.assertFalse(events_by_acc["ACC-2"]["high_signal"])
        self.assertTrue(events_by_acc["ACC-3"]["high_signal"])

        totals = payload["totals"]
        self.assertEqual(totals["event_count"], 3)
        self.assertEqual(totals["high_signal_count"], 2)
        # Category counts include every parsed item, not unique events
        self.assertEqual(totals["category_counts"]["financial_results"], 1)
        self.assertEqual(totals["category_counts"]["exhibits"], 1)
        self.assertEqual(totals["category_counts"]["disclosure"], 1)
        self.assertEqual(totals["category_counts"]["exec_change"], 1)

    async def test_category_filter_excludes_non_matching_events(self):
        processor = await self._processor(
            [
                self._filing("ACC-1", "2026-05-10", "2.02"),
                self._filing("ACC-2", "2026-04-30", "7.01"),
                self._filing("ACC-3", "2026-04-25", "5.02,9.01"),
            ]
        )
        payload = await processor.get_8k_events(
            "AAPL", days_back=180, categories=["exec_change"]
        )
        self.assertEqual(len(payload["events"]), 1)
        self.assertEqual(payload["events"][0]["accession_no"], "ACC-3")
        # Only the exec_change item survives the filter on ACC-3
        self.assertEqual([item["code"] for item in payload["events"][0]["items"]], ["5.02"])

    async def test_filing_with_no_items_is_dropped_when_filter_active(self):
        processor = await self._processor(
            [self._filing("ACC-1", "2026-05-10", "")]
        )
        payload = await processor.get_8k_events("AAPL", categories=["exec_change"])
        self.assertEqual(payload["events"], [])

    async def test_filing_with_no_items_is_kept_when_filter_inactive(self):
        processor = await self._processor(
            [self._filing("ACC-1", "2026-05-10", "")]
        )
        payload = await processor.get_8k_events("AAPL")
        self.assertEqual(len(payload["events"]), 1)
        self.assertEqual(payload["events"][0]["item_count"], 0)
        self.assertFalse(payload["events"][0]["high_signal"])

    async def test_filing_limit_truncates_input(self):
        filings = [
            self._filing(f"ACC-{i}", f"2026-05-{i:02d}", "8.01")
            for i in range(1, 11)
        ]
        processor = await self._processor(filings)
        payload = await processor.get_8k_events("AAPL", filing_limit=3)
        self.assertEqual(len(payload["events"]), 3)


if __name__ == "__main__":
    unittest.main()
