from __future__ import annotations

import unittest

from copetech_sec.eps_series import resolve_diluted_eps_ttm
from copetech_sec.financial_series import extract_financial_facts
from copetech_sec.valuation_series import derive_trailing_pe_series


def _fact(
    value: float,
    start: str,
    end: str,
    filed: str,
    accession: str,
    *,
    form: str,
    fp: str,
) -> dict:
    return {
        "val": value,
        "start": start,
        "end": end,
        "filed": filed,
        "accn": accession,
        "form": form,
        "fy": int(end[:4]),
        "fp": fp,
    }


def _rows(
    eps_entries: list[dict],
    share_entries: list[dict] | None = None,
) -> tuple[list[dict], list[dict]]:
    concepts = {
        "EarningsPerShareDiluted": {
            "units": {"USD/shares": eps_entries}
        }
    }
    if share_entries is not None:
        concepts["WeightedAverageNumberOfDilutedSharesOutstanding"] = {
            "units": {"shares": share_entries}
        }
    payload = {
        "cik": 1234,
        "entityName": "Valuation Fixture",
        "facts": {"us-gaap": concepts},
    }
    return (
        extract_financial_facts(
            payload,
            symbol="TEST",
            metric="diluted_eps",
            retrieved_at="2026-07-01T00:00:00+00:00",
        ),
        extract_financial_facts(
            payload,
            symbol="TEST",
            metric="diluted_shares",
            retrieved_at="2026-07-01T00:00:00+00:00",
        ),
    )


class TrailingPeSeriesTests(unittest.TestCase):
    def test_uses_only_annual_eps_available_at_each_price_timestamp(self):
        annual = _fact(
            4,
            "2025-01-01",
            "2025-12-31",
            "2026-02-20",
            "fy",
            form="10-K",
            fp="FY",
        )
        amended = {
            **annual,
            "val": 5,
            "filed": "2026-03-20",
            "accn": "fy-amended",
            "form": "10-K/A",
        }
        eps_rows, share_rows = _rows([annual, amended])

        payload = derive_trailing_pe_series(
            eps_rows,
            [
                {"time": "2026-02-01", "close": 40},
                {"time": "2026-03-01", "close": 40},
                {"time": "2026-04-01", "close": 40},
            ],
            symbol="TEST",
            diluted_share_rows=share_rows,
            split_events=[],
            price_source="fixture",
        )

        before_ttm, before_amendment, after_amendment = payload["observations"]
        self.assertIsNone(before_ttm["value"])
        self.assertEqual(before_amendment["epsTtm"], 4)
        self.assertEqual(before_amendment["value"], 10)
        self.assertEqual(after_amendment["epsTtm"], 5)
        self.assertEqual(after_amendment["value"], 8)
        self.assertEqual(after_amendment["epsAvailableAt"], "2026-03-20")

    def test_non_positive_eps_emits_null_pe(self):
        eps_rows, share_rows = _rows(
            [
                _fact(
                    -1,
                    "2025-01-01",
                    "2025-12-31",
                    "2026-02-20",
                    "fy",
                    form="10-K",
                    fp="FY",
                )
            ]
        )

        observation = derive_trailing_pe_series(
            eps_rows,
            [{"time": "2026-03-01", "close": 40}],
            symbol="LOSS",
            diluted_share_rows=share_rows,
            split_events=[],
        )["observations"][0]

        self.assertIsNone(observation["value"])
        self.assertIn("non_positive_ttm_eps", observation["qualityFlags"])

    def test_adjusts_annual_eps_to_the_split_adjusted_price_basis(self):
        eps_rows, share_rows = _rows(
            [
                _fact(
                    4,
                    "2025-01-01",
                    "2025-12-31",
                    "2026-02-20",
                    "fy",
                    form="10-K",
                    fp="FY",
                )
            ]
        )

        observation = derive_trailing_pe_series(
            eps_rows,
            [{"time": "2026-03-01", "close": 20}],
            symbol="SPLIT",
            diluted_share_rows=share_rows,
            split_events=[("2026-06-01", 2.0)],
        )["observations"][0]

        self.assertEqual(observation["epsTtm"], 2)
        self.assertEqual(observation["epsTtmAdjusted"], 2)
        self.assertEqual(observation["value"], 10)
        self.assertIn("eps_split_adjusted", observation["qualityFlags"])

    def test_reconstructs_interim_ttm_with_weighted_shares_across_a_split(self):
        eps_rows, share_rows = _rows(
            [
                _fact(100, "2021-01-01", "2021-12-31", "2022-02-02", "annual", form="10-K", fp="FY"),
                _fact(2.5, "2021-01-01", "2021-06-30", "2022-07-28", "prior-ytd", form="10-Q", fp="Q2"),
                _fact(2, "2022-01-01", "2022-06-30", "2022-07-28", "current-ytd", form="10-Q", fp="Q2"),
            ],
            [
                _fact(10, "2021-01-01", "2021-12-31", "2022-02-02", "annual", form="10-K", fp="FY"),
                _fact(200, "2021-01-01", "2021-06-30", "2022-07-28", "prior-ytd", form="10-Q", fp="Q2"),
                _fact(200, "2022-01-01", "2022-06-30", "2022-07-28", "current-ytd", form="10-Q", fp="Q2"),
            ],
        )

        payload = resolve_diluted_eps_ttm(
            eps_rows,
            share_rows,
            symbol="ALPHABET",
            split_events=[("2022-07-15", 20)],
        )
        latest = payload["observations"][-1]

        self.assertAlmostEqual(latest["value"], 4.5, places=6)
        self.assertLessEqual(latest["value"], 10)
        self.assertIn("eps_split_adjusted", latest["qualityFlags"])
        self.assertIn("eps_ttm_reconstructed", latest["qualityFlags"])

    def test_prior_ytd_amendment_recomputes_the_current_interim_ttm(self):
        annual = _fact(
            4,
            "2024-01-01",
            "2024-12-31",
            "2025-02-15",
            "annual",
            form="10-K",
            fp="FY",
        )
        prior_ytd = _fact(
            2,
            "2024-01-01",
            "2024-06-30",
            "2025-07-25",
            "prior-ytd",
            form="10-Q",
            fp="Q2",
        )
        current_ytd = _fact(
            3,
            "2025-01-01",
            "2025-06-30",
            "2025-07-25",
            "current-ytd",
            form="10-Q",
            fp="Q2",
        )
        amended_prior = {
            **prior_ytd,
            "val": 1,
            "filed": "2025-08-15",
            "accn": "prior-ytd-amended",
            "form": "10-Q/A",
        }
        eps_entries = [annual, prior_ytd, current_ytd, amended_prior]
        share_entries = [
            {**entry, "val": 10}
            for entry in eps_entries
        ]
        eps_rows, share_rows = _rows(eps_entries, share_entries)

        payload = derive_trailing_pe_series(
            eps_rows,
            [
                {"time": "2025-08-01", "close": 30},
                {"time": "2025-09-01", "close": 30},
            ],
            symbol="TEST",
            diluted_share_rows=share_rows,
            split_events=[],
        )

        before, after = payload["observations"]
        self.assertAlmostEqual(before["epsTtm"], 5.0, places=2)
        self.assertAlmostEqual(after["epsTtm"], 6.0, places=2)
        self.assertEqual(after["epsAvailableAt"], "2025-08-15")
        self.assertAlmostEqual(before["value"], 6.0, places=2)
        self.assertAlmostEqual(after["value"], 5.0, places=2)

    def test_share_count_change_does_not_create_a_false_positive_sofi_ttm(self):
        eps_rows, share_rows = _rows(
            [
                _fact(-1, "2021-01-01", "2021-12-31", "2022-03-01", "annual", form="10-K", fp="FY"),
                _fact(-0.8, "2021-01-01", "2021-09-30", "2022-11-09", "prior-ytd", form="10-Q", fp="Q3"),
                _fact(-0.2, "2022-01-01", "2022-09-30", "2022-11-09", "current-ytd", form="10-Q", fp="Q3"),
            ],
            [
                _fact(100, "2021-01-01", "2021-12-31", "2022-03-01", "annual", form="10-K", fp="FY"),
                _fact(100, "2021-01-01", "2021-09-30", "2022-11-09", "prior-ytd", form="10-Q", fp="Q3"),
                _fact(500, "2022-01-01", "2022-09-30", "2022-11-09", "current-ytd", form="10-Q", fp="Q3"),
            ],
        )

        payload = resolve_diluted_eps_ttm(
            eps_rows,
            share_rows,
            symbol="SOFI",
            split_events=[],
        )

        self.assertLess(payload["observations"][-1]["value"], 0)

    def test_missing_share_facts_leave_an_interim_gap(self):
        eps_rows, share_rows = _rows(
            [
                _fact(1, "2025-01-01", "2025-03-31", "2025-04-20", "q1", form="10-Q", fp="Q1"),
            ]
        )

        payload = derive_trailing_pe_series(
            eps_rows,
            [{"time": "2025-05-01", "close": 40}],
            symbol="NO-SHARES",
            diluted_share_rows=share_rows,
            split_events=[],
        )

        self.assertIsNone(payload["observations"][0]["value"])
        self.assertIn(
            "no_point_in_time_ttm_eps",
            payload["observations"][0]["qualityFlags"],
        )

    def test_stale_ttm_eps_becomes_a_gap_instead_of_a_multiple(self):
        eps_rows, share_rows = _rows(
            [
                _fact(
                    4,
                    "2024-01-01",
                    "2024-12-31",
                    "2025-02-01",
                    "fy",
                    form="10-K",
                    fp="FY",
                )
            ]
        )

        observation = derive_trailing_pe_series(
            eps_rows,
            [{"time": "2025-09-01", "close": 40}],
            symbol="STALE",
            diluted_share_rows=share_rows,
            split_events=[],
        )["observations"][0]

        self.assertIsNone(observation["value"])
        self.assertIn("stale_eps", observation["qualityFlags"])

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
