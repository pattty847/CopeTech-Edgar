#!/usr/bin/env python3
"""Tests for FinancialDataProcessor.

Builds a minimal company-facts fixture exercising:
  - Quarterly + annual extraction with `frame` field
  - YTD/cumulative deduplication when 10-Q reports both 90-day and 270-day windows
  - Instant-metric handling (balance sheet items have no start date)
  - Sort order (newest first)
  - source_form/period_end derived from latest entry
"""

from __future__ import annotations

import unittest
from typing import Any

from copetech_sec.financial_processor import FinancialDataProcessor


def _entry(*, val: float, end: str, fp: str, fy: int, frame: str | None, form: str,
           start: str | None = None, filed: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "val": val,
        "end": end,
        "fp": fp,
        "fy": fy,
        "form": form,
    }
    if frame is not None:
        out["frame"] = frame
    if start is not None:
        out["start"] = start
    if filed is not None:
        out["filed"] = filed
    return out


def _facts() -> dict[str, Any]:
    """Synthetic SEC companyfacts payload covering revenue, assets, EPS."""
    return {
        "entityName": "Acme Corp",
        "cik": 1234567890,
        "facts": {
            "us-gaap": {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {
                        "USD": [
                            # Q1 2025: 90 days, valid quarterly
                            _entry(val=100, start="2025-01-01", end="2025-03-31",
                                   fp="Q1", fy=2025, frame="CY2025Q1", form="10-Q",
                                   filed="2025-05-01"),
                            # Q2 2025: 90 days, valid quarterly
                            _entry(val=110, start="2025-04-01", end="2025-06-30",
                                   fp="Q2", fy=2025, frame="CY2025Q2", form="10-Q",
                                   filed="2025-08-01"),
                            # YTD H1 2025: 181 days — must be filtered out
                            _entry(val=210, start="2025-01-01", end="2025-06-30",
                                   fp="Q2", fy=2025, frame=None, form="10-Q",
                                   filed="2025-08-01"),
                            # FY 2024 annual: 365 days
                            _entry(val=400, start="2024-01-01", end="2024-12-31",
                                   fp="FY", fy=2024, frame="CY2024", form="10-K",
                                   filed="2025-02-15"),
                        ]
                    }
                },
                "Assets": {
                    "units": {
                        "USD": [
                            # Instant metric — no start date is fine
                            _entry(val=5000, end="2025-06-30", fp="Q2", fy=2025,
                                   frame="CY2025Q2I", form="10-Q", filed="2025-08-01"),
                            _entry(val=4800, end="2024-12-31", fp="FY", fy=2024,
                                   frame="CY2024", form="10-K", filed="2025-02-15"),
                        ]
                    }
                },
                "EarningsPerShareBasic": {
                    "units": {
                        "USD/shares": [
                            _entry(val=1.25, start="2025-04-01", end="2025-06-30",
                                   fp="Q2", fy=2025, frame="CY2025Q2", form="10-Q",
                                   filed="2025-08-01"),
                        ]
                    }
                },
            }
        },
    }


class FinancialProcessorTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.facts_payload = _facts()

        async def fetch_facts(_ticker: str, use_cache: bool = True) -> dict[str, Any]:
            return self.facts_payload

        self.processor = FinancialDataProcessor(fetch_facts_func=fetch_facts)

    def test_format_period_handles_quarter_frame(self):
        self.assertEqual(
            self.processor._format_period({"frame": "CY2024Q3"}),
            "Q3 2024",
        )

    def test_format_period_handles_annual_frame(self):
        self.assertEqual(
            self.processor._format_period({"frame": "CY2024"}),
            "2024",
        )

    def test_format_period_falls_back_to_fp_fy(self):
        self.assertEqual(
            self.processor._format_period({"fp": "Q2", "fy": 2026, "frame": ""}),
            "Q2 2026",
        )

    def test_is_quarterly_uses_frame_first(self):
        self.assertTrue(self.processor._is_quarterly({"frame": "CY2024Q1"}))
        self.assertFalse(self.processor._is_quarterly({"frame": "CY2024"}))

    def test_calculate_duration_days(self):
        self.assertEqual(
            self.processor._calculate_duration_days(
                {"start": "2024-01-01", "end": "2024-03-31"}
            ),
            90,
        )
        self.assertIsNone(
            self.processor._calculate_duration_days({"start": None, "end": "2024-03-31"})
        )

    def test_metric_requires_duration(self):
        self.assertTrue(self.processor._metric_requires_duration("revenue"))
        self.assertFalse(self.processor._metric_requires_duration("assets"))

    def test_get_fact_history_filters_ytd_quarterly_value(self):
        history = self.processor._get_fact_history(
            self.facts_payload,
            "us-gaap",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            metric_key="revenue",
        )
        assert history is not None
        quarterly_values = [e["value"] for e in history["quarterly"]]
        self.assertIn(100, quarterly_values)
        self.assertIn(110, quarterly_values)
        self.assertNotIn(210, quarterly_values)
        # Newest first
        self.assertEqual(history["quarterly"][0]["date"], "2025-06-30")

    def test_get_fact_history_returns_annual_entry(self):
        history = self.processor._get_fact_history(
            self.facts_payload,
            "us-gaap",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
            metric_key="revenue",
        )
        assert history is not None
        annual_values = [e["value"] for e in history["annual"]]
        self.assertEqual(annual_values, [400])

    def test_get_fact_history_returns_none_for_missing_concept(self):
        result = self.processor._get_fact_history(
            self.facts_payload, "us-gaap", "TotallyMadeUpTag"
        )
        self.assertIsNone(result)

    def test_get_fact_history_keeps_instant_metric_without_start(self):
        history = self.processor._get_fact_history(
            self.facts_payload, "us-gaap", "Assets", metric_key="assets"
        )
        assert history is not None
        # Assets entries have no `start` — must still be kept
        self.assertEqual(len(history["quarterly"]), 1)
        self.assertEqual(history["quarterly"][0]["value"], 5000)
        self.assertEqual(history["annual"][0]["value"], 4800)

    async def test_get_financial_summary_shape(self):
        summary = await self.processor.get_financial_summary("acme")
        assert summary is not None

        self.assertEqual(summary["ticker"], "ACME")
        self.assertEqual(summary["entityName"], "Acme Corp")
        self.assertEqual(summary["cik"], 1234567890)
        # Latest entry is Q2 2025 from a 10-Q
        self.assertEqual(summary["period_end"], "2025-06-30")
        self.assertEqual(summary["source_form"], "10-Q")

        for key in ("revenue", "assets", "eps"):
            self.assertIsNotNone(summary[key], f"{key} should be populated")
            self.assertIn("quarterly", summary[key])
            self.assertIn("annual", summary[key])

        # Metrics declared in KEY_FINANCIAL_SUMMARY_METRICS but absent from fixture
        # should be present as None rather than missing entirely.
        self.assertIsNone(summary["net_income"])
        self.assertIsNone(summary["liabilities"])

    async def test_get_financial_summary_returns_none_when_facts_missing(self):
        async def fetch_none(_ticker: str, use_cache: bool = True) -> None:
            return None

        processor = FinancialDataProcessor(fetch_facts_func=fetch_none)
        self.assertIsNone(await processor.get_financial_summary("zzz"))

    async def test_get_financial_summary_returns_none_when_no_metrics_match(self):
        async def fetch_unrelated(_ticker: str, use_cache: bool = True) -> dict[str, Any]:
            return {"entityName": "Empty", "cik": 1, "facts": {"us-gaap": {}}}

        processor = FinancialDataProcessor(fetch_facts_func=fetch_unrelated)
        self.assertIsNone(await processor.get_financial_summary("empty"))


class TrendComputationTests(unittest.IsolatedAsyncioTestCase):
    def test_safe_pct_change_handles_zero_and_none(self):
        self.assertIsNone(FinancialDataProcessor._safe_pct_change(100, 0))
        self.assertIsNone(FinancialDataProcessor._safe_pct_change(None, 100))
        self.assertIsNone(FinancialDataProcessor._safe_pct_change(100, None))
        self.assertEqual(
            FinancialDataProcessor._safe_pct_change(110, 100), 0.1
        )
        self.assertEqual(
            FinancialDataProcessor._safe_pct_change(80, 100), -0.2
        )

    def test_decorate_quarterly_series_computes_qoq_and_yoy(self):
        # Newest first: Q4 2025 ... Q1 2024 (so index+4 is the same quarter prior year)
        series = [
            {"period": "Q4 2025", "date": "2025-12-31", "value": 132, "form": "10-K"},
            {"period": "Q3 2025", "date": "2025-09-30", "value": 120, "form": "10-Q"},
            {"period": "Q2 2025", "date": "2025-06-30", "value": 110, "form": "10-Q"},
            {"period": "Q1 2025", "date": "2025-03-31", "value": 100, "form": "10-Q"},
            {"period": "Q4 2024", "date": "2024-12-31", "value": 110, "form": "10-K"},
            {"period": "Q3 2024", "date": "2024-09-30", "value": 100, "form": "10-Q"},
        ]
        decorated = FinancialDataProcessor._decorate_series_with_pct_changes(series, cadence="quarterly")
        # Newest entry: Q4 2025=132. QoQ vs Q3 2025=120 → 0.1. YoY vs Q4 2024=110 → 0.2.
        self.assertAlmostEqual(decorated[0]["qoq_pct"], 0.1)
        self.assertAlmostEqual(decorated[0]["yoy_pct"], 0.2)
        # Oldest entry: insufficient lookback → both None.
        self.assertIsNone(decorated[-1].get("qoq_pct"))
        self.assertIsNone(decorated[-1].get("yoy_pct"))

    def test_decorate_annual_series_computes_yoy_only(self):
        series = [
            {"period": "2025", "date": "2025-12-31", "value": 440, "form": "10-K"},
            {"period": "2024", "date": "2024-12-31", "value": 400, "form": "10-K"},
        ]
        decorated = FinancialDataProcessor._decorate_series_with_pct_changes(series, cadence="annual")
        self.assertAlmostEqual(decorated[0]["yoy_pct"], 0.1)
        self.assertNotIn("qoq_pct", decorated[0])
        self.assertIsNone(decorated[-1].get("yoy_pct"))

    def test_compute_trend_preserves_top_level_metadata(self):
        summary = {
            "ticker": "ACME",
            "entityName": "Acme Corp",
            "cik": 1234567890,
            "source_form": "10-Q",
            "period_end": "2025-06-30",
            "revenue": {
                "quarterly": [
                    {"period": "Q2 2025", "date": "2025-06-30", "value": 110, "form": "10-Q"},
                    {"period": "Q1 2025", "date": "2025-03-31", "value": 100, "form": "10-Q"},
                ],
                "annual": [],
            },
            "net_income": None,
            "assets": None,
            "liabilities": None,
            "equity": None,
            "operating_cash_flow": None,
            "investing_cash_flow": None,
            "financing_cash_flow": None,
            "eps": None,
        }
        trend = FinancialDataProcessor.compute_trend(summary, periods=4)
        self.assertEqual(trend["ticker"], "ACME")
        self.assertEqual(trend["period_end"], "2025-06-30")
        self.assertEqual(trend["periods_requested"], 4)
        self.assertIsNone(trend["metrics"]["net_income"])
        revenue_quarterly = trend["metrics"]["revenue"]["quarterly"]
        self.assertAlmostEqual(revenue_quarterly[0]["qoq_pct"], 0.1)

    def test_compute_trend_returns_empty_dict_for_falsy_input(self):
        self.assertEqual(FinancialDataProcessor.compute_trend(None), {})
        self.assertEqual(FinancialDataProcessor.compute_trend({}), {})

    async def test_get_financial_trend_returns_none_when_facts_missing(self):
        async def fetch_none(_ticker: str, use_cache: bool = True) -> None:
            return None

        processor = FinancialDataProcessor(fetch_facts_func=fetch_none)
        self.assertIsNone(await processor.get_financial_trend("zzz"))


if __name__ == "__main__":
    unittest.main()
