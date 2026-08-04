from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from copetech_sec.financial_series_service import FinancialSeriesService
from copetech_sec.roic_series import resolve_roic_series


def ttm_obs(value: float, start: str, end: str, available: str) -> dict:
    return {
        "periodStart": start,
        "periodEnd": end,
        "availableAt": available,
        "value": value,
        "unit": "USD",
        "frequency": "ttm",
        "confidence": 1.0,
        "qualityFlags": [],
        "availabilitySource": {"form": "10-K", "filed": available, "accessionNumber": "a"},
        "selectedSource": {"form": "10-K", "filed": available, "accessionNumber": "a"},
        "sources": [],
        "fiscalYear": 2025,
    }


def balance_obs(value: float, end: str, available: str) -> dict:
    return {**ttm_obs(value, end, end, available), "frequency": "instant"}


def payload(observations: list[dict]) -> dict:
    return {"cik": 1, "entityName": "Fixture", "observations": observations, "warnings": []}


WINDOW = ("2025-01-01", "2025-12-31")


class RoicTests(unittest.TestCase):
    def test_roic_averages_beginning_and_ending_invested_capital(self):
        series = resolve_roic_series(
            payload([ttm_obs(100.0, *WINDOW, "2026-02-01")]),
            payload([ttm_obs(20.0, *WINDOW, "2026-02-01")]),
            payload([ttm_obs(100.0, *WINDOW, "2026-02-01")]),
            payload(
                [
                    balance_obs(360.0, "2024-12-31", "2025-02-01"),
                    balance_obs(440.0, "2025-12-31", "2026-02-01"),
                ]
            ),
            symbol="TEST",
        )
        (observation,) = series["observations"]
        # NOPAT = 100 × (1 − 0.2) = 80; avg IC = (360 + 440) / 2 = 400 → 20%
        self.assertAlmostEqual(observation["value"], 0.2)
        self.assertNotIn("single_period_invested_capital", observation["qualityFlags"])
        self.assertEqual(observation["availableAt"], "2026-02-01")

    def test_missing_beginning_balance_is_flagged_single_period(self):
        series = resolve_roic_series(
            payload([ttm_obs(100.0, *WINDOW, "2026-02-01")]),
            payload([ttm_obs(20.0, *WINDOW, "2026-02-01")]),
            payload([ttm_obs(100.0, *WINDOW, "2026-02-01")]),
            payload([balance_obs(400.0, "2025-12-31", "2026-02-01")]),
            symbol="TEST",
        )
        (observation,) = series["observations"]
        self.assertAlmostEqual(observation["value"], 0.2)
        self.assertIn("single_period_invested_capital", observation["qualityFlags"])

    def test_non_positive_pretax_windows_are_skipped_and_counted(self):
        series = resolve_roic_series(
            payload([ttm_obs(100.0, *WINDOW, "2026-02-01")]),
            payload([ttm_obs(20.0, *WINDOW, "2026-02-01")]),
            payload([ttm_obs(-5.0, *WINDOW, "2026-02-01")]),
            payload([balance_obs(400.0, "2025-12-31", "2026-02-01")]),
            symbol="TEST",
        )
        self.assertEqual(series["observations"], [])
        self.assertIn("roic_windows_skipped_non_positive_pretax", series["warnings"])

    def test_absurd_tax_rates_are_clamped_and_flagged(self):
        series = resolve_roic_series(
            payload([ttm_obs(100.0, *WINDOW, "2026-02-01")]),
            payload([ttm_obs(-30.0, *WINDOW, "2026-02-01")]),  # tax benefit
            payload([ttm_obs(100.0, *WINDOW, "2026-02-01")]),
            payload([balance_obs(400.0, "2025-12-31", "2026-02-01")]),
            symbol="TEST",
        )
        (observation,) = series["observations"]
        # rate clamps to 0 → NOPAT = 100 → 25%
        self.assertAlmostEqual(observation["value"], 0.25)
        self.assertIn("effective_tax_rate_clamped", observation["qualityFlags"])

    def test_non_ttm_frequency_returns_empty_with_warning(self):
        series = resolve_roic_series(
            payload([]), payload([]), payload([]), payload([]),
            symbol="TEST",
            frequency="quarterly",
        )
        self.assertEqual(series["observations"], [])
        self.assertIn("roic_available_only_as_ttm", series["warnings"])


def fact(value, start, end, filed, accn, *, form="10-Q", unit_shares=False):
    return {
        "val": value, "start": start, "end": end, "filed": filed, "accn": accn,
        "form": form, "fy": 2025, "fp": "Q1", "frame": None,
    }


class Phase4ServiceTests(unittest.IsolatedAsyncioTestCase):
    QUARTERS = [
        ("2024-04-01", "2024-06-30", "2024-07-25"),
        ("2024-07-01", "2024-09-30", "2024-10-25"),
        ("2024-10-01", "2024-12-31", "2025-01-25"),
        ("2025-01-01", "2025-03-31", "2025-04-25"),
    ]

    def facts_payload(self, concepts: dict[str, list[float]]) -> dict:
        built = {}
        for concept, values in concepts.items():
            built[concept] = {
                "units": {
                    "USD": [
                        fact(v, s, e, f, f"{concept}-{i}")
                        for i, (v, (s, e, f)) in enumerate(zip(values, self.QUARTERS))
                    ]
                }
            }
        return {"cik": 1, "entityName": "Fixture", "facts": {"us-gaap": built}}

    async def test_gross_profit_metric_is_served_with_the_fallback(self):
        payload = self.facts_payload(
            {
                "RevenueFromContractWithCustomerExcludingAssessedTax": [100.0] * 4,
                "CostOfRevenue": [60.0] * 4,
            }
        )

        async def fetch(symbol, use_cache=True):
            return payload

        with tempfile.TemporaryDirectory() as tmp:
            service = FinancialSeriesService(fetch, Path(tmp) / "s.db")
            series = await service.get_series("TEST", metric="gross_profit")

        self.assertTrue(series["observations"])
        self.assertTrue(all(row["value"] == 40.0 for row in series["observations"]))
        self.assertTrue(
            all(
                "gross_profit_derived_from_cost_of_revenue" in row["qualityFlags"]
                for row in series["observations"]
            )
        )

    async def test_supported_metrics_dedupes_shadowed_ids(self):
        entries = FinancialSeriesService.supported_metrics()
        ids = [entry["id"] for entry in entries]
        self.assertEqual(len(ids), len(set(ids)))
        gross = next(entry for entry in entries if entry["id"] == "gross_profit")
        self.assertTrue(gross.get("derived"))
        self.assertIn("roic", ids)
        self.assertIn("ebitda", ids)
        self.assertIn("interest_coverage", ids)
        self.assertIn("invested_capital", ids)
        by_id = {entry["id"]: entry for entry in entries}
        self.assertEqual(by_id["stockholders_equity"]["frequencies"], ["quarterly", "annual"])
        self.assertEqual(by_id["diluted_shares"]["frequencies"], ["quarterly", "annual"])
        self.assertEqual(by_id["revenue_per_share"]["frequencies"], ["quarterly", "annual"])
        self.assertEqual(by_id["revenue"]["frequencies"], ["quarterly", "ttm", "annual"])
        self.assertEqual(by_id["diluted_eps"]["frequencies"], ["quarterly", "ttm", "annual"])
        self.assertEqual(by_id["roic"]["frequencies"], ["ttm"])

    async def test_interest_coverage_ttm(self):
        payload = self.facts_payload(
            {
                "OperatingIncomeLoss": [50.0] * 4,
                "InterestExpense": [5.0] * 4,
            }
        )

        async def fetch(symbol, use_cache=True):
            return payload

        with tempfile.TemporaryDirectory() as tmp:
            service = FinancialSeriesService(fetch, Path(tmp) / "s.db")
            series = await service.get_series(
                "TEST", metric="interest_coverage", frequency="ttm"
            )

        (observation,) = series["observations"]
        self.assertAlmostEqual(observation["value"], 10.0)

    async def test_net_interest_fallback_is_disclosed_on_coverage(self):
        payload = self.facts_payload(
            {
                "OperatingIncomeLoss": [50.0] * 4,
                "InterestIncomeExpenseNet": [5.0] * 4,
            }
        )

        async def fetch(symbol, use_cache=True):
            return payload

        with tempfile.TemporaryDirectory() as tmp:
            service = FinancialSeriesService(fetch, Path(tmp) / "s.db")
            series = await service.get_series(
                "TEST", metric="interest_coverage", frequency="ttm"
            )

        (observation,) = series["observations"]
        self.assertAlmostEqual(observation["value"], 10.0)
        self.assertIn(
            "net_interest_used_for_interest_expense",
            observation["qualityFlags"],
        )


if __name__ == "__main__":
    unittest.main()
