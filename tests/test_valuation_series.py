from __future__ import annotations

import unittest

from copetech_sec.financial_series import extract_financial_facts
from copetech_sec.valuation_series import derive_trailing_pe_series


def _eps_fact(
    value: float,
    start: str,
    end: str,
    filed: str,
    accession: str,
    *,
    form: str = "10-Q",
) -> dict:
    return {
        "val": value,
        "start": start,
        "end": end,
        "filed": filed,
        "accn": accession,
        "form": form,
        "fy": int(end[:4]),
        "fp": "Q1",
    }


def _rows(*entries: dict) -> list[dict]:
    payload = {
        "cik": 1234,
        "entityName": "Valuation Fixture",
        "facts": {
            "us-gaap": {
                "EarningsPerShareDiluted": {
                    "units": {"USD/shares": list(entries)}
                }
            }
        },
    }
    return extract_financial_facts(
        payload,
        symbol="TEST",
        metric="diluted_eps",
        retrieved_at="2026-07-01T00:00:00+00:00",
    )


class TrailingPeSeriesTests(unittest.TestCase):
    def test_uses_only_eps_available_at_each_price_timestamp(self):
        q1 = _eps_fact(1, "2025-01-01", "2025-03-31", "2025-04-20", "q1")
        q2 = _eps_fact(1, "2025-04-01", "2025-06-30", "2025-07-20", "q2")
        q3 = _eps_fact(1, "2025-07-01", "2025-09-30", "2025-10-20", "q3")
        q4 = _eps_fact(1, "2025-10-01", "2025-12-31", "2026-02-20", "q4")
        amended_q4 = {
            **q4,
            "val": 2,
            "filed": "2026-03-20",
            "accn": "q4-amended",
            "form": "10-Q/A",
        }

        payload = derive_trailing_pe_series(
            _rows(q1, q2, q3, q4, amended_q4),
            [
                {"time": "2026-02-01", "close": 40},
                {"time": "2026-03-01", "close": 40},
                {"time": "2026-04-01", "close": 40},
            ],
            symbol="TEST",
            split_events=[],
            price_source="fixture",
        )

        before_ttm, before_amendment, after_amendment = payload["observations"]
        self.assertIsNone(before_ttm["value"])
        self.assertEqual(before_amendment["epsTtm"], 4)
        self.assertEqual(before_amendment["value"], 10)
        self.assertEqual(after_amendment["epsTtm"], 5)
        self.assertEqual(after_amendment["value"], 8)
        self.assertEqual(
            after_amendment["epsAvailableAt"],
            "2026-03-20",
        )

    def test_non_positive_eps_emits_null_pe(self):
        rows = _rows(
            _eps_fact(-1, "2025-01-01", "2025-03-31", "2025-04-20", "q1"),
            _eps_fact(0, "2025-04-01", "2025-06-30", "2025-07-20", "q2"),
            _eps_fact(0, "2025-07-01", "2025-09-30", "2025-10-20", "q3"),
            _eps_fact(0, "2025-10-01", "2025-12-31", "2026-02-20", "q4"),
        )

        observation = derive_trailing_pe_series(
            rows,
            [{"time": "2026-03-01", "close": 40}],
            symbol="LOSS",
            split_events=[],
        )["observations"][0]

        self.assertIsNone(observation["value"])
        self.assertIn("non_positive_ttm_eps", observation["qualityFlags"])

    def test_adjusts_eps_to_the_split_adjusted_price_basis(self):
        rows = _rows(
            _eps_fact(1, "2025-01-01", "2025-03-31", "2025-04-20", "q1"),
            _eps_fact(1, "2025-04-01", "2025-06-30", "2025-07-20", "q2"),
            _eps_fact(1, "2025-07-01", "2025-09-30", "2025-10-20", "q3"),
            _eps_fact(1, "2025-10-01", "2025-12-31", "2026-02-20", "q4"),
        )

        observation = derive_trailing_pe_series(
            rows,
            [{"time": "2026-03-01", "close": 20}],
            symbol="SPLIT",
            split_events=[("2026-06-01", 2.0)],
        )["observations"][0]

        self.assertEqual(observation["epsTtm"], 4)
        self.assertEqual(observation["epsTtmAdjusted"], 2)
        self.assertEqual(observation["value"], 10)
        self.assertIn("eps_split_adjusted", observation["qualityFlags"])

    def test_rejects_unadjusted_prices(self):
        with self.assertRaisesRegex(ValueError, "split_adjusted"):
            derive_trailing_pe_series(
                [],
                [],
                symbol="TEST",
                price_basis="raw",
            )


if __name__ == "__main__":
    unittest.main()
