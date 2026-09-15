from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from copetech_sec.derived_series import resolve_derived_series
from copetech_sec.financial_series import (
    extract_financial_facts,
    resolve_financial_series,
)
from copetech_sec.financial_series_service import FinancialSeriesService


def instant_fact(
    value: float,
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
        "end": end,
        "filed": filed,
        "accn": accession,
        "form": form,
        "fy": fy,
        "fp": fp,
        "frame": None,
    }


def duration_fact(value: float, start: str, end: str, filed: str, accession: str, *, form: str = "10-Q") -> dict:
    return {
        "val": value,
        "start": start,
        "end": end,
        "filed": filed,
        "accn": accession,
        "form": form,
        "fy": 2025,
        "fp": "Q1",
        "frame": None,
    }


def company_facts(taxonomies: dict[str, dict[str, tuple[list[dict], str]]]) -> dict:
    return {
        "cik": 320193,
        "entityName": "Fixture Corp",
        "facts": {
            taxonomy: {
                concept: {"units": {unit: entries}}
                for concept, (entries, unit) in concepts.items()
            }
            for taxonomy, concepts in taxonomies.items()
        },
    }


BALANCE_DATES = [
    ("2025-03-31", "2025-04-25", "10-Q"),
    ("2025-06-30", "2025-07-25", "10-Q"),
    ("2025-12-31", "2026-02-01", "10-K"),
]


def instant_series(concept: str, values: list[float], taxonomy: str = "us-gaap", unit: str = "USD") -> dict:
    return company_facts(
        {
            taxonomy: {
                concept: (
                    [
                        instant_fact(
                            value,
                            end,
                            filed,
                            f"{concept}-{index}",
                            form=form,
                            fp="FY" if form == "10-K" else "Q1",
                        )
                        for index, (value, (end, filed, form)) in enumerate(
                            zip(values, BALANCE_DATES)
                        )
                    ],
                    unit,
                )
            }
        }
    )


def merge(*payloads: dict) -> dict:
    merged: dict = {"cik": 320193, "entityName": "Fixture Corp", "facts": {}}
    for payload in payloads:
        for taxonomy, concepts in payload["facts"].items():
            merged["facts"].setdefault(taxonomy, {}).update(concepts)
    return merged


def resolved(payload: dict, metric: str, frequency: str = "quarterly") -> dict:
    rows = extract_financial_facts(payload, symbol="TEST", metric=metric)
    return resolve_financial_series(
        rows, symbol="TEST", metric=metric, frequency=frequency
    )


class InstantMetricTests(unittest.TestCase):
    def test_balance_dates_resolve_with_point_in_time_availability(self):
        series = resolved(
            instant_series("StockholdersEquity", [100.0, 110.0, 120.0]),
            "stockholders_equity",
        )
        observations = series["observations"]
        self.assertEqual(len(observations), 3)
        self.assertEqual(
            [(row["periodStart"], row["periodEnd"]) for row in observations],
            [(end, end) for end, _filed, _form in BALANCE_DATES],
        )
        self.assertEqual(observations[-1]["availableAt"], "2026-02-01")
        self.assertEqual(observations[0]["frequency"], "instant")

    def test_annual_frequency_keeps_only_annual_filing_dates(self):
        series = resolved(
            instant_series("StockholdersEquity", [100.0, 110.0, 120.0]),
            "stockholders_equity",
            frequency="annual",
        )
        (observation,) = series["observations"]
        self.assertEqual(observation["periodEnd"], "2025-12-31")

    def test_annual_frequency_keeps_year_end_restated_by_a_later_quarterly_filing(self):
        payload = instant_series("StockholdersEquity", [100.0, 110.0, 120.0])
        entries = payload["facts"]["us-gaap"]["StockholdersEquity"]["units"]["USD"]
        entries.append(
            instant_fact(
                125.0,
                "2025-12-31",
                "2026-04-25",
                "year-end-restated",
                form="10-Q",
                fp="Q1",
            )
        )

        series = resolved(payload, "stockholders_equity", frequency="annual")

        (observation,) = series["observations"]
        self.assertEqual(observation["periodEnd"], "2025-12-31")
        self.assertEqual(observation["value"], 125.0)
        self.assertEqual(observation["availableAt"], "2026-04-25")

    def test_annual_frequency_rejects_comparisons_and_transaction_dates(self):
        payload = company_facts(
            {
                "us-gaap": {
                    "StockholdersEquity": (
                        [
                            instant_fact(
                                80,
                                "2019-12-31",
                                "2022-03-01",
                                "fy21",
                                form="10-K",
                                fy=2021,
                                fp="FY",
                            ),
                            instant_fact(
                                0,
                                "2020-07-09",
                                "2021-03-17",
                                "fy20",
                                form="10-K",
                                fy=2020,
                                fp="FY",
                            ),
                            instant_fact(
                                90,
                                "2020-12-31",
                                "2021-03-17",
                                "fy20",
                                form="10-K",
                                fy=2020,
                                fp="FY",
                            ),
                            instant_fact(
                                100,
                                "2021-12-31",
                                "2022-03-01",
                                "fy21",
                                form="10-K",
                                fy=2021,
                                fp="FY",
                            ),
                        ],
                        "USD",
                    )
                }
            }
        )

        series = resolved(payload, "stockholders_equity", frequency="annual")

        self.assertEqual(
            [
                (row["periodEnd"], row["fiscalYear"], row["fiscalPeriod"])
                for row in series["observations"]
            ],
            [("2020-12-31", 2020, "FY"), ("2021-12-31", 2021, "FY")],
        )

    def test_ttm_is_meaningless_for_instants(self):
        series = resolved(
            instant_series("StockholdersEquity", [100.0, 110.0, 120.0]),
            "stockholders_equity",
            frequency="ttm",
        )
        self.assertEqual(series["observations"], [])
        self.assertIn("ttm_not_applicable_for_instant_metric", series["warnings"])

    def test_dei_shares_outstanding_resolves(self):
        series = resolved(
            instant_series(
                "EntityCommonStockSharesOutstanding",
                [50.0, 50.0, 48.0],
                taxonomy="dei",
                unit="shares",
            ),
            "shares_outstanding",
        )
        self.assertEqual(len(series["observations"]), 3)
        self.assertEqual(series["observations"][-1]["value"], 48.0)


class InstantCompositeTests(unittest.TestCase):
    def test_sofi_2023_uses_reported_aggregate_debt(self):
        payload = merge(
            instant_series(
                "CashAndCashEquivalentsAtCarryingValue",
                [30.0, 30.0, 3_085_020_000.0],
            ),
            instant_series(
                "DebtLongtermAndShorttermCombinedAmount",
                [40.0, 40.0, 5_233_416_000.0],
            ),
        )
        series = resolve_derived_series(
            {
                "cash_equivalents": resolved(payload, "cash_equivalents", frequency="annual"),
                "total_debt": resolved(payload, "total_debt", frequency="annual"),
            },
            symbol="SOFI",
            metric="net_debt",
            frequency="annual",
            basis="canonical",
            alignment="availability",
        )

        self.assertEqual(series["observations"][-1]["value"], 2_148_396_000.0)
        self.assertEqual(
            series["observations"][-1]["selectedSource"]["concept"],
            "DebtLongtermAndShorttermCombinedAmount",
        )

    def test_normal_issuer_keeps_current_plus_noncurrent_debt_fallback(self):
        payload = merge(
            instant_series("CashAndCashEquivalentsAtCarryingValue", [30.0, 30.0, 30.0]),
            instant_series("LongTermDebtCurrent", [10.0, 10.0, 10.0]),
            instant_series("LongTermDebtNoncurrent", [90.0, 90.0, 90.0]),
        )
        series = resolve_derived_series(
            {
                metric: resolved(payload, metric)
                for metric in ("cash_equivalents", "debt_current", "debt_noncurrent")
            },
            symbol="AAPL",
            metric="net_debt",
            frequency="quarterly",
            basis="canonical",
            alignment="availability",
        )

        self.assertEqual([row["value"] for row in series["observations"]], [70.0] * 3)

    def test_parent_total_wins_over_duplicate_debt_components(self):
        payload = merge(
            instant_series("CashAndCashEquivalentsAtCarryingValue", [30.0, 30.0, 30.0]),
            instant_series("DebtLongtermAndShorttermCombinedAmount", [100.0, 100.0, 100.0]),
            instant_series("LongTermDebtCurrent", [10.0, 10.0, 10.0]),
            instant_series("LongTermDebtNoncurrent", [90.0, 90.0, 90.0]),
        )
        series = resolve_derived_series(
            {
                metric: resolved(payload, metric)
                for metric in ("cash_equivalents", "total_debt", "debt_current", "debt_noncurrent")
            },
            symbol="TEST",
            metric="net_debt",
            frequency="quarterly",
            basis="canonical",
            alignment="availability",
        )

        self.assertEqual([row["value"] for row in series["observations"]], [70.0] * 3)
        self.assertTrue(all(len(row["sources"]) == 2 for row in series["observations"]))

    def test_multiple_total_debt_concepts_select_priority_instead_of_summing(self):
        payload = merge(
            instant_series("DebtLongtermAndShorttermCombinedAmount", [100.0, 100.0, 100.0]),
            instant_series("LongTermDebt", [95.0, 95.0, 95.0]),
        )

        series = resolved(payload, "total_debt")

        self.assertEqual([row["value"] for row in series["observations"]], [100.0] * 3)
        self.assertTrue(
            all(
                "multiple_concepts_available" in row["qualityFlags"]
                for row in series["observations"]
            )
        )

    def test_working_capital_joins_on_the_balance_date(self):
        payload = merge(
            instant_series("AssetsCurrent", [200.0, 210.0, 220.0]),
            instant_series("LiabilitiesCurrent", [150.0, 155.0, 160.0]),
        )
        series = resolve_derived_series(
            {
                "current_assets": resolved(payload, "current_assets"),
                "current_liabilities": resolved(payload, "current_liabilities"),
            },
            symbol="TEST",
            metric="working_capital",
            frequency="quarterly",
            basis="canonical",
            alignment="availability",
        )
        self.assertEqual(
            [row["value"] for row in series["observations"]], [50.0, 55.0, 60.0]
        )

    def test_net_debt_sums_debt_and_nets_cash(self):
        payload = merge(
            instant_series("CashAndCashEquivalentsAtCarryingValue", [30.0, 30.0, 30.0]),
            instant_series("ShortTermInvestments", [20.0, 20.0, 20.0]),
            instant_series("LongTermDebtCurrent", [10.0, 10.0, 10.0]),
            instant_series("LongTermDebtNoncurrent", [90.0, 90.0, 90.0]),
        )
        series = resolve_derived_series(
            {
                metric: resolved(payload, metric)
                for metric in (
                    "cash_equivalents",
                    "short_term_investments",
                    "debt_current",
                    "debt_noncurrent",
                )
            },
            symbol="TEST",
            metric="net_debt",
            frequency="quarterly",
            basis="canonical",
            alignment="availability",
        )
        self.assertEqual([row["value"] for row in series["observations"]], [50.0] * 3)

    def test_net_debt_without_debt_tags_is_unknown_not_zero(self):
        payload = instant_series(
            "CashAndCashEquivalentsAtCarryingValue", [30.0, 30.0, 30.0]
        )
        series = resolve_derived_series(
            {"cash_equivalents": resolved(payload, "cash_equivalents")},
            symbol="TEST",
            metric="net_debt",
            frequency="quarterly",
            basis="canonical",
            alignment="availability",
        )
        self.assertEqual(series["observations"], [])
        self.assertIn("debt_concepts_missing", series["warnings"])


class DebtPipelineTests(unittest.IsolatedAsyncioTestCase):
    async def test_sofi_2023_raw_facts_flow_through_store_and_service(self):
        payload = company_facts(
            {
                "us-gaap": {
                    "CashAndCashEquivalentsAtCarryingValue": (
                        [instant_fact(3_085_020_000, "2023-12-31", "2024-02-27", "0001818874-24-000026", form="10-K", fy=2023, fp="FY")],
                        "USD",
                    ),
                    "DebtLongtermAndShorttermCombinedAmount": (
                        [instant_fact(5_233_416_000, "2023-12-31", "2024-02-27", "0001818874-24-000026", form="10-K", fy=2023, fp="FY")],
                        "USD",
                    ),
                    # This gross parent has dimensioned warehouse/securitization
                    # children in the filing. It must not replace or augment the
                    # net carrying amount above.
                    "DebtInstrumentCarryingAmount": (
                        [instant_fact(5_259_584_000, "2023-12-31", "2024-02-27", "0001818874-24-000026", form="10-K", fy=2023, fp="FY")],
                        "USD",
                    ),
                }
            }
        )

        async def fetch_facts(symbol: str, use_cache: bool = True) -> dict:
            return payload

        with tempfile.TemporaryDirectory() as tmp:
            service = FinancialSeriesService(fetch_facts, Path(tmp) / "facts.sqlite3")
            series = await service.get_series("SOFI", metric="net_debt", frequency="annual")

        self.assertIsNotNone(series)
        (observation,) = series["observations"]
        self.assertEqual(observation["value"], 2_148_396_000)
        self.assertEqual(observation["selectedSource"]["concept"], "DebtLongtermAndShorttermCombinedAmount")
        self.assertNotIn("DebtInstrumentCarryingAmount", {source["concept"] for source in observation["sources"]})


class InstantValuationServiceTests(unittest.IsolatedAsyncioTestCase):
    QUARTERS = [
        ("2024-04-01", "2024-06-30", "2024-07-25"),
        ("2024-07-01", "2024-09-30", "2024-10-25"),
        ("2024-10-01", "2024-12-31", "2025-01-25"),
        ("2025-01-01", "2025-03-31", "2025-04-25"),
    ]

    def full_payload(self) -> dict:
        duration_concepts = {
            "RevenueFromContractWithCustomerExcludingAssessedTax": [25.0] * 4,
            "NetCashProvidedByUsedInOperatingActivities": [30.0] * 4,
            "PaymentsToAcquirePropertyPlantAndEquipment": [5.0] * 4,
            "OperatingIncomeLoss": [20.0] * 4,
            "DepreciationDepletionAndAmortization": [5.0] * 4,
            "WeightedAverageNumberOfDilutedSharesOutstanding": [10.0] * 4,
        }
        built: dict = {"us-gaap": {}}
        for concept, values in duration_concepts.items():
            unit = "shares" if "Shares" in concept else "USD"
            built["us-gaap"][concept] = (
                [
                    duration_fact(value, start, end, filed, f"{concept}-{index}")
                    for index, (value, (start, end, filed)) in enumerate(
                        zip(values, self.QUARTERS)
                    )
                ],
                unit,
            )
        payload = company_facts(built)
        return merge(
            payload,
            instant_series("StockholdersEquity", [100.0, 100.0, 100.0]),
            instant_series("CashAndCashEquivalentsAtCarryingValue", [10.0, 10.0, 10.0]),
            instant_series("LongTermDebtNoncurrent", [110.0, 110.0, 110.0]),
        )

    async def _series(self, metric: str) -> dict:
        payload = self.full_payload()

        async def fetch_facts(symbol: str, use_cache: bool = True) -> dict:
            return payload

        with tempfile.TemporaryDirectory() as tmp:
            service = FinancialSeriesService(fetch_facts, Path(tmp) / "store.sqlite3")
            return await service.get_valuation_series(
                "TEST",
                # Every input (Q4 flows and the 2025-03-31 balance sheet) was
                # filed 2025-04-25, so this bar sits inside the freshness window.
                price_observations=[{"time": "2025-05-01", "close": 40.0}],
                metric=metric,
                split_events=[],
            )

    async def test_trailing_pb_uses_the_latest_equity_balance(self):
        series = await self._series("trailing_pb")
        self.assertEqual(series["denominatorFrequency"], "instant")
        (observation,) = series["observations"]
        # market cap 40 × 10 = 400 over equity 100 → 4×
        self.assertAlmostEqual(observation["value"], 4.0)

    async def test_fcf_yield_is_the_inverted_multiple(self):
        series = await self._series("fcf_yield")
        self.assertTrue(series["inverted"])
        (observation,) = series["observations"]
        # TTM FCF 100 over market cap 400 → 25%
        self.assertAlmostEqual(observation["value"], 0.25)

    async def test_ev_s_adds_net_debt_to_the_market_cap(self):
        series = await self._series("ev_s")
        self.assertEqual(series["adjustmentMetric"], "net_debt")
        self.assertIn("net_debt_not_comparable_for_financial_companies", series["warnings"])
        (observation,) = series["observations"]
        # EV = 400 + (110 − 10) = 500 over TTM revenue 100 → 5×
        self.assertAlmostEqual(observation["value"], 5.0)
        self.assertEqual(observation["adjustmentValue"], 100.0)

    async def test_ev_ebitda_uses_ttm_operating_income_plus_depreciation(self):
        series = await self._series("ev_ebitda")
        (observation,) = series["observations"]
        # EV 500 over TTM EBITDA: (20 + 5) × 4 = 100 → 5×.
        self.assertAlmostEqual(observation["denominatorTtm"], 100.0)
        self.assertAlmostEqual(observation["value"], 5.0)


if __name__ == "__main__":
    unittest.main()
