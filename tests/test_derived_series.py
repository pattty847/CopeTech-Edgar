from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from copetech_sec.derived_series import (
    list_derived_metrics,
    resolve_derived_series,
)
from copetech_sec.financial_series import (
    extract_financial_facts,
    resolve_financial_series,
)
from copetech_sec.financial_series_service import FinancialSeriesService


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


def company_facts(concepts: dict[str, list[dict]], unit: str = "USD") -> dict:
    return {
        "cik": 320193,
        "entityName": "Fixture Corp",
        "facts": {
            "us-gaap": {
                concept: {"units": {unit: entries}}
                for concept, entries in concepts.items()
            }
        },
    }


def merge_facts(*payloads: dict) -> dict:
    merged = {"cik": 320193, "entityName": "Fixture Corp", "facts": {"us-gaap": {}}}
    for payload in payloads:
        merged["facts"]["us-gaap"].update(payload["facts"]["us-gaap"])
    return merged


def resolved(payload: dict, symbol: str, metric: str, frequency: str = "quarterly") -> dict:
    rows = extract_financial_facts(payload, symbol=symbol, metric=metric)
    return resolve_financial_series(
        rows, symbol=symbol, metric=metric, frequency=frequency
    )


def component_payload(value: float, *, unit: str = "USD") -> dict:
    source = {
        "taxonomy": "us-gaap",
        "concept": "FixtureConcept",
        "form": "10-Q",
        "filed": "2025-04-25",
        "accessionNumber": "fixture",
    }
    return {
        "cik": 1,
        "entityName": "Fixture Corp",
        "warnings": [],
        "observations": [
            {
                "periodStart": "2025-01-01",
                "periodEnd": "2025-03-31",
                "availableAt": "2025-04-25",
                "alignedAt": "2025-04-25",
                "value": value,
                "unit": unit,
                "frequency": "quarterly",
                "fiscalYear": 2025,
                "fiscalPeriod": "Q1",
                "confidence": 1.0,
                "qualityFlags": [],
                "availabilitySource": source,
                "selectedSource": source,
                "sources": [source],
            }
        ],
    }


class DerivedSeriesTests(unittest.TestCase):
    WINDOW = ("2025-01-01", "2025-03-31")

    def _quarter(self, concept: str, value: float, filed: str, accession: str) -> dict:
        return company_facts(
            {concept: [fact(value, *self.WINDOW, filed, accession)]}
        )

    def test_gross_margin_prefers_reported_gross_profit(self):
        payload = merge_facts(
            self._quarter("RevenueFromContractWithCustomerExcludingAssessedTax", 100.0, "2025-04-25", "rev"),
            self._quarter("GrossProfit", 40.0, "2025-04-25", "gp"),
            self._quarter("CostOfGoodsAndServicesSold", 65.0, "2025-04-25", "cor"),
        )
        series = resolve_derived_series(
            {
                "revenue": resolved(payload, "TEST", "revenue"),
                "gross_profit": resolved(payload, "TEST", "gross_profit"),
                "cost_of_revenue": resolved(payload, "TEST", "cost_of_revenue"),
            },
            symbol="TEST",
            metric="gross_margin",
            frequency="quarterly",
            basis="canonical",
            alignment="availability",
        )
        (observation,) = series["observations"]
        self.assertAlmostEqual(observation["value"], 0.4)
        self.assertEqual(observation["unit"], "ratio")
        self.assertTrue(observation["derived"])
        self.assertEqual(
            observation["selectedSource"]["concept"],
            "GrossProfit",
        )
        self.assertNotIn(
            "gross_profit_derived_from_cost_of_revenue",
            observation["qualityFlags"],
        )

    def test_gross_margin_falls_back_to_cost_of_revenue_with_flag(self):
        payload = merge_facts(
            self._quarter("RevenueFromContractWithCustomerExcludingAssessedTax", 100.0, "2025-04-25", "rev"),
            self._quarter("CostOfGoodsAndServicesSold", 65.0, "2025-04-25", "cor"),
        )
        series = resolve_derived_series(
            {
                "revenue": resolved(payload, "TEST", "revenue"),
                "cost_of_revenue": resolved(payload, "TEST", "cost_of_revenue"),
            },
            symbol="TEST",
            metric="gross_margin",
            frequency="quarterly",
            basis="canonical",
            alignment="availability",
        )
        (observation,) = series["observations"]
        self.assertAlmostEqual(observation["value"], 0.35)
        self.assertIn(
            "gross_profit_derived_from_cost_of_revenue",
            observation["qualityFlags"],
        )

    def test_first_available_fallback_keeps_its_semantic_warning(self):
        payload = merge_facts(
            self._quarter("CostOfServices", 60.0, "2025-04-20", "services"),
            self._quarter("CostOfRevenue", 60.0, "2025-04-25", "cost-revenue"),
        )
        extracted = extract_financial_facts(
            payload,
            symbol="TEST",
            metric="cost_of_revenue",
        )
        series = resolve_financial_series(
            extracted,
            symbol="TEST",
            metric="cost_of_revenue",
            frequency="quarterly",
        )

        (observation,) = series["observations"]
        self.assertEqual(observation["availableAt"], "2025-04-20")
        self.assertEqual(observation["selectedSource"]["concept"], "CostOfRevenue")
        self.assertIn(
            "cost_of_services_may_not_equal_total_cost_of_revenue",
            observation["qualityFlags"],
        )

    def test_fcf_subtracts_capex_and_takes_latest_availability(self):
        payload = merge_facts(
            self._quarter("NetCashProvidedByUsedInOperatingActivities", 90.0, "2025-04-25", "ocf"),
            self._quarter("PaymentsToAcquirePropertyPlantAndEquipment", 30.0, "2025-05-02", "capex"),
        )
        series = resolve_derived_series(
            {
                "operating_cash_flow": resolved(payload, "TEST", "operating_cash_flow"),
                "capex": resolved(payload, "TEST", "capex"),
            },
            symbol="TEST",
            metric="fcf",
            frequency="quarterly",
            basis="canonical",
            alignment="availability",
        )
        (observation,) = series["observations"]
        self.assertAlmostEqual(observation["value"], 60.0)
        self.assertEqual(observation["unit"], "USD")
        # FCF is not knowable until the later of its two components was filed.
        self.assertEqual(observation["availableAt"], "2025-05-02")
        self.assertEqual(observation["alignedAt"], "2025-05-02")

    def test_windows_that_do_not_align_are_skipped(self):
        revenue = self._quarter(
            "RevenueFromContractWithCustomerExcludingAssessedTax", 100.0, "2025-04-25", "rev"
        )
        operating_income = company_facts(
            {
                "OperatingIncomeLoss": [
                    fact(25.0, "2025-01-05", "2025-03-31", "2025-04-25", "oi")
                ]
            }
        )
        series = resolve_derived_series(
            {
                "revenue": resolved(revenue, "TEST", "revenue"),
                "operating_income": resolved(operating_income, "TEST", "operating_income"),
            },
            symbol="TEST",
            metric="operating_margin",
            frequency="quarterly",
            basis="canonical",
            alignment="availability",
        )
        self.assertEqual(series["observations"], [])

    def test_revenue_per_share_ttm_is_empty_with_warning(self):
        revenue = self._quarter(
            "RevenueFromContractWithCustomerExcludingAssessedTax", 100.0, "2025-04-25", "rev"
        )
        shares = company_facts(
            {
                "WeightedAverageNumberOfDilutedSharesOutstanding": [
                    fact(50.0, *self.WINDOW, "2025-04-25", "sh")
                ]
            },
            unit="shares",
        )
        series = resolve_derived_series(
            {
                "revenue": resolved(revenue, "TEST", "revenue", frequency="ttm"),
                "diluted_shares": resolved(shares, "TEST", "diluted_shares", frequency="ttm"),
            },
            symbol="TEST",
            metric="revenue_per_share",
            frequency="ttm",
            basis="canonical",
            alignment="availability",
        )
        self.assertEqual(series["observations"], [])
        self.assertIn(
            "ttm_unavailable_for_weighted_average_component", series["warnings"]
        )

    def test_revenue_per_share_quarterly(self):
        revenue = self._quarter(
            "RevenueFromContractWithCustomerExcludingAssessedTax", 100.0, "2025-04-25", "rev"
        )
        shares = company_facts(
            {
                "WeightedAverageNumberOfDilutedSharesOutstanding": [
                    fact(50.0, *self.WINDOW, "2025-04-25", "sh")
                ]
            },
            unit="shares",
        )
        series = resolve_derived_series(
            {
                "revenue": resolved(revenue, "TEST", "revenue"),
                "diluted_shares": resolved(shares, "TEST", "diluted_shares"),
            },
            symbol="TEST",
            metric="revenue_per_share",
            frequency="quarterly",
            basis="canonical",
            alignment="availability",
        )
        (observation,) = series["observations"]
        self.assertAlmostEqual(observation["value"], 2.0)
        self.assertEqual(observation["unit"], "USD/shares")

    def test_listing_exposes_derived_metrics(self):
        listed = {entry["id"]: entry for entry in list_derived_metrics()}
        self.assertIn("gross_margin", listed)
        self.assertTrue(listed["gross_margin"]["derived"])
        self.assertIn("revenue", listed["gross_margin"]["components"])

    def test_each_remaining_composite_formula_uses_consistent_units_and_signs(self):
        cases = {
            "fcf_margin": ({"operating_cash_flow": 90, "capex": 30, "revenue": 200}, 0.30),
            "operating_margin": ({"operating_income": 50, "revenue": 200}, 0.25),
            "rnd_intensity": ({"rnd_expense": 20, "revenue": 200}, 0.10),
            "sbc_burden": ({"sbc": 10, "revenue": 200}, 0.05),
            "capex_intensity": ({"capex": 30, "revenue": 200}, 0.15),
            "ebitda": ({"operating_income": 50, "dep_amort": 15}, 65.0),
            "invested_capital": (
                {
                    "stockholders_equity": 100,
                    "debt_current_total": 10,
                    "debt_noncurrent": 90,
                    "cash_equivalents": 30,
                    "short_term_investments": 20,
                },
                150.0,
            ),
        }
        for metric, (values, expected) in cases.items():
            with self.subTest(metric=metric):
                series = resolve_derived_series(
                    {component: component_payload(value) for component, value in values.items()},
                    symbol="TEST",
                    metric=metric,
                    frequency="quarterly",
                    basis="canonical",
                    alignment="availability",
                )
                (observation,) = series["observations"]
                self.assertAlmostEqual(observation["value"], expected)

    def test_ratio_composites_decline_zero_divisors_instead_of_emitting_infinity(self):
        cases = {
            "gross_margin": {"revenue": 0, "gross_profit": 10},
            "operating_margin": {"revenue": 0, "operating_income": 10},
            "rnd_intensity": {"revenue": 0, "rnd_expense": 10},
            "fcf_margin": {"revenue": 0, "operating_cash_flow": 10, "capex": 2},
            "sbc_burden": {"revenue": 0, "sbc": 10},
            "capex_intensity": {"revenue": 0, "capex": 10},
            "revenue_per_share": {"revenue": 10, "diluted_shares": 0},
            "interest_coverage": {"operating_income": 10, "interest_expense": 0},
        }
        for metric, values in cases.items():
            with self.subTest(metric=metric):
                series = resolve_derived_series(
                    {component: component_payload(value) for component, value in values.items()},
                    symbol="TEST",
                    metric=metric,
                    frequency="quarterly",
                    basis="canonical",
                    alignment="availability",
                )
                self.assertEqual(series["observations"], [])


class DerivedSeriesServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_service_resolves_derived_metric_end_to_end(self):
        payload = merge_facts(
            company_facts(
                {
                    "RevenueFromContractWithCustomerExcludingAssessedTax": [
                        fact(200.0, "2025-01-01", "2025-03-31", "2025-04-25", "rev")
                    ],
                    "OperatingIncomeLoss": [
                        fact(50.0, "2025-01-01", "2025-03-31", "2025-04-25", "oi")
                    ],
                }
            )
        )

        async def fetch_facts(symbol: str, use_cache: bool = True) -> dict:
            return payload

        with tempfile.TemporaryDirectory() as tmp:
            service = FinancialSeriesService(
                fetch_facts, Path(tmp) / "store.sqlite3"
            )
            series = await service.get_series("TEST", metric="operating_margin")

        self.assertEqual(series["metric"], "operating_margin")
        self.assertEqual(series["label"], "Operating margin")
        (observation,) = series["observations"]
        self.assertAlmostEqual(observation["value"], 0.25)
        self.assertGreater(series["rawFactCount"], 0)

    async def test_supported_metrics_include_base_and_derived(self):
        async def fetch_facts(symbol: str, use_cache: bool = True) -> None:
            return None

        ids = {entry["id"] for entry in FinancialSeriesService.supported_metrics()}
        self.assertLessEqual(
            {"revenue", "operating_income", "gross_margin", "fcf", "fcf_margin"},
            ids,
        )


if __name__ == "__main__":
    unittest.main()
