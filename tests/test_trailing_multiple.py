from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from copetech_sec.financial_series import extract_financial_facts, resolve_financial_series
from copetech_sec.financial_series_service import FinancialSeriesService
from copetech_sec.valuation_series import (
    build_share_windows,
    derive_trailing_multiple_series,
)


def fact(
    value: float,
    start: str,
    end: str,
    filed: str,
    accession: str,
    *,
    form: str = "10-Q",
    fy: int = 2025,
    fp: str = "Q1",
) -> dict:
    return {
        "val": value,
        "start": start,
        "end": end,
        "filed": filed,
        "accn": accession,
        "form": form,
        "fy": fy,
        "fp": fp,
        "frame": None,
    }


QUARTERS = [
    ("2024-04-01", "2024-06-30", "2024-07-25"),
    ("2024-07-01", "2024-09-30", "2024-10-25"),
    ("2024-10-01", "2024-12-31", "2025-01-25"),
    ("2025-01-01", "2025-03-31", "2025-04-25"),
]


def company_facts(concepts: dict[str, tuple[list[float], str]]) -> dict:
    built: dict[str, dict] = {}
    for concept, (values, unit) in concepts.items():
        built[concept] = {
            "units": {
                unit: [
                    fact(value, start, end, filed, f"{concept}-{index}")
                    for index, (value, (start, end, filed)) in enumerate(
                        zip(values, QUARTERS)
                    )
                ]
            }
        }
    return {"cik": 320193, "entityName": "Fixture Corp", "facts": {"us-gaap": built}}


def revenue_ttm(payload: dict) -> list[dict]:
    rows = extract_financial_facts(payload, symbol="TEST", metric="revenue")
    return resolve_financial_series(
        rows, symbol="TEST", metric="revenue", frequency="ttm"
    )["observations"]


class TrailingMultipleTests(unittest.TestCase):
    def payload(self) -> dict:
        return company_facts(
            {
                # 4 quarters of revenue → TTM 100, and 10 shares each quarter.
                "RevenueFromContractWithCustomerExcludingAssessedTax": (
                    [25.0, 25.0, 25.0, 25.0],
                    "USD",
                ),
                "WeightedAverageNumberOfDilutedSharesOutstanding": (
                    [10.0, 10.0, 10.0, 10.0],
                    "shares",
                ),
            }
        )

    def shares(self, payload: dict) -> list[dict]:
        return build_share_windows(
            extract_financial_facts(payload, symbol="TEST", metric="diluted_shares"),
            [],
            [],
            symbol="TEST",
        )

    def test_ps_is_price_times_shares_over_ttm_revenue(self):
        payload = self.payload()
        series = derive_trailing_multiple_series(
            [{"time": "2025-05-01", "close": 40.0}],
            revenue_ttm(payload),
            self.shares(payload),
            symbol="TEST",
            metric="trailing_ps",
            label="Trailing P/S",
            denominator_metric="revenue",
            split_events=[],
        )
        (observation,) = series["observations"]
        # market cap 40 × 10 = 400 over TTM revenue 100 → 4×
        self.assertAlmostEqual(observation["value"], 4.0)
        self.assertEqual(observation["denominatorTtm"], 100.0)
        self.assertEqual(observation["sharesOutstanding"], 10.0)

    def test_unverified_split_history_suppresses_every_value(self):
        payload = self.payload()
        series = derive_trailing_multiple_series(
            [{"time": "2025-05-01", "close": 40.0}],
            revenue_ttm(payload),
            self.shares(payload),
            symbol="TEST",
            metric="trailing_ps",
            label="Trailing P/S",
            denominator_metric="revenue",
            split_events=None,
        )
        (observation,) = series["observations"]
        self.assertIsNone(observation["value"])
        self.assertIn("split_history_unverified", observation["qualityFlags"])

    def test_shares_are_restated_onto_the_current_split_basis(self):
        payload = self.payload()
        series = derive_trailing_multiple_series(
            [{"time": "2025-06-10", "close": 20.0}],
            revenue_ttm(payload),
            self.shares(payload),
            symbol="TEST",
            metric="trailing_ps",
            label="Trailing P/S",
            denominator_metric="revenue",
            # 2:1 split after the last share filing: the price basis halves,
            # the share count doubles, and the multiple is unchanged.
            split_events=[("2025-06-01", 2.0)],
        )
        (observation,) = series["observations"]
        self.assertEqual(observation["sharesOutstanding"], 20.0)
        self.assertAlmostEqual(observation["value"], 4.0)

    def test_point_in_time_no_lookahead(self):
        payload = self.payload()
        series = derive_trailing_multiple_series(
            # Before the 4th quarter was filed, no TTM revenue existed.
            [{"time": "2025-02-01", "close": 40.0}],
            revenue_ttm(payload),
            self.shares(payload),
            symbol="TEST",
            metric="trailing_ps",
            label="Trailing P/S",
            denominator_metric="revenue",
            split_events=[],
        )
        (observation,) = series["observations"]
        self.assertIsNone(observation["value"])
        self.assertIn("no_point_in_time_ttm_denominator", observation["qualityFlags"])

    def test_negative_fcf_yield_is_plotted_instead_of_suppressed(self):
        payload = self.payload()
        denominator = revenue_ttm(payload)
        denominator[0] = {**denominator[0], "value": -20.0}
        series = derive_trailing_multiple_series(
            [{"time": "2025-05-01", "close": 40.0}],
            denominator,
            self.shares(payload),
            symbol="TEST",
            metric="fcf_yield",
            label="FCF yield",
            denominator_metric="fcf",
            invert=True,
            split_events=[],
        )
        (observation,) = series["observations"]
        # Negative free cash flow is a meaningful negative yield: -20 / 400.
        self.assertAlmostEqual(observation["value"], -0.05)
        self.assertNotIn("non_positive_ttm_denominator", observation["qualityFlags"])

    def test_missing_share_tags_fall_back_to_net_income_over_eps(self):
        payload = company_facts(
            {
                "NetIncomeLoss": ([20.0, 20.0, 20.0, 20.0], "USD"),
                "EarningsPerShareDiluted": ([2.0, 2.0, 2.0, 2.0], "USD/shares"),
            }
        )
        shares = build_share_windows(
            [],
            extract_financial_facts(payload, symbol="TEST", metric="net_income"),
            extract_financial_facts(payload, symbol="TEST", metric="diluted_eps"),
            symbol="TEST",
        )
        self.assertTrue(shares)
        self.assertTrue(
            all(window["value"] == 10.0 for window in shares)
        )
        self.assertTrue(
            all(
                "diluted_shares_derived_from_net_income" in window["qualityFlags"]
                for window in shares
            )
        )


class TrailingMultipleServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_service_serves_trailing_ps_end_to_end(self):
        payload = company_facts(
            {
                "RevenueFromContractWithCustomerExcludingAssessedTax": (
                    [25.0, 25.0, 25.0, 25.0],
                    "USD",
                ),
                "WeightedAverageNumberOfDilutedSharesOutstanding": (
                    [10.0, 10.0, 10.0, 10.0],
                    "shares",
                ),
            }
        )

        async def fetch_facts(symbol: str, use_cache: bool = True) -> dict:
            return payload

        with tempfile.TemporaryDirectory() as tmp:
            service = FinancialSeriesService(fetch_facts, Path(tmp) / "store.sqlite3")
            series = await service.get_valuation_series(
                "TEST",
                price_observations=[{"time": "2025-05-01", "close": 40.0}],
                metric="trailing_ps",
                split_events=[],
            )

        self.assertEqual(series["metric"], "trailing_ps")
        self.assertEqual(series["denominatorMetric"], "revenue")
        (observation,) = series["observations"]
        self.assertAlmostEqual(observation["value"], 4.0)

    async def test_service_serves_trailing_pfcf_from_derived_fcf(self):
        payload = company_facts(
            {
                "NetCashProvidedByUsedInOperatingActivities": (
                    [30.0, 30.0, 30.0, 30.0],
                    "USD",
                ),
                "PaymentsToAcquirePropertyPlantAndEquipment": (
                    [5.0, 5.0, 5.0, 5.0],
                    "USD",
                ),
                "WeightedAverageNumberOfDilutedSharesOutstanding": (
                    [10.0, 10.0, 10.0, 10.0],
                    "shares",
                ),
            }
        )

        async def fetch_facts(symbol: str, use_cache: bool = True) -> dict:
            return payload

        with tempfile.TemporaryDirectory() as tmp:
            service = FinancialSeriesService(fetch_facts, Path(tmp) / "store.sqlite3")
            series = await service.get_valuation_series(
                "TEST",
                price_observations=[{"time": "2025-05-01", "close": 40.0}],
                metric="trailing_pfcf",
                split_events=[],
            )

        self.assertEqual(series["metric"], "trailing_pfcf")
        (observation,) = series["observations"]
        # market cap 400 over TTM FCF (120 − 20) = 100 → 4×
        self.assertAlmostEqual(observation["value"], 4.0)

    async def test_service_preserves_pre_amendment_denominator_for_older_prices(self):
        payload = self.payload_with_revenue_restatement()

        async def fetch_facts(symbol: str, use_cache: bool = True) -> dict:
            return payload

        with tempfile.TemporaryDirectory() as tmp:
            service = FinancialSeriesService(fetch_facts, Path(tmp) / "store.sqlite3")
            series = await service.get_valuation_series(
                "TEST",
                price_observations=[
                    {"time": "2025-05-01", "close": 40.0},
                    {"time": "2025-05-20", "close": 40.0},
                ],
                metric="trailing_ps",
                split_events=[],
            )

        before, after = series["observations"]
        self.assertEqual(before["denominatorTtm"], 100.0)
        self.assertAlmostEqual(before["value"], 4.0)
        self.assertEqual(after["denominatorTtm"], 125.0)
        self.assertAlmostEqual(after["value"], 3.2)
        self.assertLessEqual(before["denominatorAvailableAt"], before["timestamp"])
        self.assertLessEqual(after["denominatorAvailableAt"], after["timestamp"])

    @staticmethod
    def payload_with_revenue_restatement() -> dict:
        payload = company_facts(
            {
                "RevenueFromContractWithCustomerExcludingAssessedTax": (
                    [25.0, 25.0, 25.0, 25.0],
                    "USD",
                ),
                "WeightedAverageNumberOfDilutedSharesOutstanding": (
                    [10.0, 10.0, 10.0, 10.0],
                    "shares",
                ),
            }
        )
        revenue = payload["facts"]["us-gaap"][
            "RevenueFromContractWithCustomerExcludingAssessedTax"
        ]["units"]["USD"]
        start, end, _filed = QUARTERS[0]
        revenue.append(
            fact(
                50.0,
                start,
                end,
                "2025-05-15",
                "RevenueFromContractWithCustomerExcludingAssessedTax-amended",
                form="10-Q/A",
            )
        )
        return payload


if __name__ == "__main__":
    unittest.main()
