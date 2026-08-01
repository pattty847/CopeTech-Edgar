from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from copetech_sec.financial_series import (
    extract_financial_facts,
    resolve_financial_series,
)
from copetech_sec.financial_series_service import FinancialSeriesService
from copetech_sec.financial_series_store import FinancialSeriesStore


def fact(
    value: float,
    start: str,
    end: str,
    filed: str,
    accession: str,
    *,
    form: str,
    fy: int,
    fp: str,
    frame: str | None = None,
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
        "frame": frame,
    }


def company_facts(
    contract_revenue: list[dict] | None = None,
    revenues: list[dict] | None = None,
    diluted_eps: list[dict] | None = None,
    basic_eps: list[dict] | None = None,
) -> dict:
    concepts = {}
    if contract_revenue is not None:
        concepts["RevenueFromContractWithCustomerExcludingAssessedTax"] = {
            "units": {"USD": contract_revenue}
        }
    if revenues is not None:
        concepts["Revenues"] = {"units": {"USD": revenues}}
    if diluted_eps is not None:
        concepts["EarningsPerShareDiluted"] = {
            "units": {"USD/shares": diluted_eps}
        }
    if basic_eps is not None:
        concepts["EarningsPerShareBasic"] = {
            "units": {"USD/shares": basic_eps}
        }
    return {
        "cik": 1045810,
        "entityName": "Fixture Corp",
        "facts": {"us-gaap": concepts},
    }


class FinancialSeriesNormalizationTests(unittest.TestCase):
    def test_diluted_eps_is_canonical_and_never_substitutes_basic_eps(self):
        diluted = fact(
            1.2,
            "2025-01-01",
            "2025-03-31",
            "2025-04-25",
            "diluted",
            form="10-Q",
            fy=2025,
            fp="Q1",
        )
        basic = {**diluted, "val": 1.4, "accn": "basic"}
        payload = company_facts(diluted_eps=[diluted], basic_eps=[basic])

        diluted_rows = extract_financial_facts(
            payload,
            symbol="TEST",
            metric="diluted_eps",
        )
        basic_rows = extract_financial_facts(
            payload,
            symbol="TEST",
            metric="basic_eps",
        )

        self.assertEqual([row["value"] for row in diluted_rows], [1.2])
        self.assertEqual([row["value"] for row in basic_rows], [1.4])
        self.assertEqual(diluted_rows[0]["concept"], "EarningsPerShareDiluted")

    def test_negative_diluted_eps_is_preserved(self):
        payload = company_facts(
            diluted_eps=[
                fact(
                    -0.42,
                    "2025-01-01",
                    "2025-03-31",
                    "2025-05-01",
                    "loss",
                    form="10-Q",
                    fy=2025,
                    fp="Q1",
                )
            ]
        )

        rows = extract_financial_facts(
            payload,
            symbol="LOSS",
            metric="diluted_eps",
        )
        series = resolve_financial_series(
            rows,
            symbol="LOSS",
            metric="diluted_eps",
        )

        self.assertEqual(series["observations"][0]["value"], -0.42)

    def test_diluted_eps_never_derives_a_fourth_quarter_or_sums_ttm(self):
        entries = [
            fact(
                value,
                start,
                end,
                filed,
                accession,
                form=form,
                fy=2025,
                fp=fp,
            )
            for value, start, end, filed, accession, form, fp in [
                (0.10, "2025-01-01", "2025-03-31", "2025-04-25", "q1", "10-Q", "Q1"),
                (0.20, "2025-04-01", "2025-06-30", "2025-07-25", "q2", "10-Q", "Q2"),
                (0.30, "2025-07-01", "2025-09-30", "2025-10-25", "q3", "10-Q", "Q3"),
                (1.00, "2025-01-01", "2025-12-31", "2026-02-10", "fy", "10-K", "FY"),
            ]
        ]
        rows = extract_financial_facts(
            company_facts(diluted_eps=entries),
            symbol="TEST",
            metric="diluted_eps",
        )

        quarterly = resolve_financial_series(
            rows,
            symbol="TEST",
            metric="diluted_eps",
            frequency="quarterly",
            basis="canonical",
        )
        ttm = resolve_financial_series(
            rows,
            symbol="TEST",
            metric="diluted_eps",
            frequency="ttm",
            basis="canonical",
        )

        self.assertEqual(len(quarterly["observations"]), 3)
        self.assertFalse(any(row["derived"] for row in quarterly["observations"]))
        self.assertEqual(ttm["observations"], [])

    def test_stitches_revenue_concepts_by_economic_window(self):
        payload = company_facts(
            contract_revenue=[
                fact(
                    10,
                    "2023-01-01",
                    "2023-03-31",
                    "2023-04-25",
                    "a1",
                    form="10-Q",
                    fy=2023,
                    fp="Q1",
                )
            ],
            revenues=[
                fact(
                    20,
                    "2024-01-01",
                    "2024-03-31",
                    "2024-04-25",
                    "a2",
                    form="10-Q",
                    fy=2024,
                    fp="Q1",
                )
            ],
        )
        rows = extract_financial_facts(payload, symbol="GOOGL", metric="revenue")
        series = resolve_financial_series(
            rows, symbol="GOOGL", metric="revenue", frequency="quarterly"
        )
        self.assertEqual(
            [row["periodEnd"] for row in series["observations"]],
            ["2023-03-31", "2024-03-31"],
        )

    def test_comparative_repeat_dedupes_by_window_and_keeps_original_fiscal_identity(self):
        original = fact(
            44_062,
            "2025-01-27",
            "2025-04-27",
            "2025-05-28",
            "original",
            form="10-Q",
            fy=2026,
            fp="Q1",
        )
        comparative = fact(
            44_062,
            "2025-01-27",
            "2025-04-27",
            "2026-05-20",
            "repeat",
            form="10-Q",
            fy=2027,
            fp="Q1",
            frame="CY2025Q1",
        )
        rows = extract_financial_facts(
            company_facts(revenues=[original, comparative]),
            symbol="NVDA",
            metric="revenue",
        )
        series = resolve_financial_series(
            rows, symbol="NVDA", metric="revenue", frequency="quarterly"
        )
        self.assertEqual(len(series["observations"]), 1)
        observation = series["observations"][0]
        self.assertEqual(observation["availableAt"], "2025-05-28")
        self.assertEqual(observation["fiscalYear"], 2026)
        self.assertEqual(
            observation["availabilitySource"]["accessionNumber"],
            "original",
        )
        self.assertEqual(
            observation["selectedSource"]["accessionNumber"],
            "repeat",
        )
        self.assertEqual(
            [source["accessionNumber"] for source in observation["sources"]],
            ["original", "repeat"],
        )
        self.assertEqual(
            observation["availableAt"],
            min(source["filed"] for source in observation["sources"]),
        )

    def test_derives_q4_and_ttm_from_annual_and_three_quarters(self):
        rows = [
            fact(
                value,
                start,
                end,
                filed,
                accession,
                form=form,
                fy=2025,
                fp=fp,
            )
            for value, start, end, filed, accession, form, fp in [
                (10, "2025-01-01", "2025-03-31", "2025-04-25", "q1", "10-Q", "Q1"),
                (20, "2025-04-01", "2025-06-30", "2025-07-25", "q2", "10-Q", "Q2"),
                (30, "2025-07-01", "2025-09-30", "2025-10-25", "q3", "10-Q", "Q3"),
                (100, "2025-01-01", "2025-12-31", "2026-02-10", "fy", "10-K", "FY"),
            ]
        ]
        extracted = extract_financial_facts(
            company_facts(revenues=rows), symbol="NVDA", metric="revenue"
        )
        quarterly = resolve_financial_series(
            extracted,
            symbol="NVDA",
            metric="revenue",
            frequency="quarterly",
            basis="canonical",
        )
        q4 = quarterly["observations"][-1]
        self.assertEqual(q4["value"], 40)
        self.assertEqual(q4["availableAt"], "2026-02-10")
        self.assertEqual(q4["qualityFlags"], ["derived_q4"])
        ttm = resolve_financial_series(
            extracted,
            symbol="NVDA",
            metric="revenue",
            frequency="ttm",
            basis="canonical",
        )
        self.assertEqual(ttm["observations"][0]["value"], 100)
        self.assertEqual(ttm["observations"][0]["availableAt"], "2026-02-10")

    def test_does_not_derive_q4_from_noncontiguous_quarters(self):
        rows = [
            fact(
                value,
                start,
                end,
                filed,
                accession,
                form=form,
                fy=2025,
                fp=fp,
            )
            for value, start, end, filed, accession, form, fp in [
                (10, "2025-01-01", "2025-03-31", "2025-04-25", "q1", "10-Q", "Q1"),
                # Missing Q2; these two later windows still fit inside the annual period.
                (20, "2025-07-01", "2025-09-30", "2025-10-25", "q3", "10-Q", "Q3"),
                (30, "2025-10-01", "2025-12-20", "2026-01-25", "q4", "10-Q", "Q4"),
                (100, "2025-01-01", "2025-12-31", "2026-02-10", "fy", "10-K", "FY"),
            ]
        ]
        extracted = extract_financial_facts(
            company_facts(revenues=rows), symbol="TEST", metric="revenue"
        )

        series = resolve_financial_series(
            extracted,
            symbol="TEST",
            metric="revenue",
            frequency="quarterly",
            basis="canonical",
        )

        self.assertFalse(
            any(row["derived"] for row in series["observations"]),
            series["observations"],
        )

    def test_amendment_changes_only_queries_at_or_after_filing_date(self):
        original = fact(
            10,
            "2025-01-01",
            "2025-03-31",
            "2025-04-25",
            "q1",
            form="10-Q",
            fy=2025,
            fp="Q1",
        )
        amended = deepcopy(original)
        amended.update(
            {"val": 12, "filed": "2025-05-10", "accn": "q1a", "form": "10-Q/A"}
        )
        rows = extract_financial_facts(
            company_facts(revenues=[original, amended]),
            symbol="TEST",
            metric="revenue",
        )
        before = resolve_financial_series(
            rows,
            symbol="TEST",
            metric="revenue",
            as_of="2025-05-01",
        )
        after = resolve_financial_series(
            rows,
            symbol="TEST",
            metric="revenue",
            as_of="2025-05-10",
        )
        self.assertEqual(before["observations"][0]["value"], 10)
        self.assertEqual(after["observations"][0]["value"], 12)
        self.assertEqual(after["observations"][0]["availableAt"], "2025-05-10")
        self.assertIn("conflicting_filing_values", after["warnings"])


class FinancialSeriesServiceTests(unittest.IsolatedAsyncioTestCase):
    async def test_persists_facts_and_serves_them_when_refresh_fails(self):
        payload = company_facts(
            revenues=[
                fact(
                    10,
                    "2025-01-01",
                    "2025-03-31",
                    "2025-04-25",
                    "q1",
                    form="10-Q",
                    fy=2025,
                    fp="Q1",
                )
            ]
        )
        calls = 0

        async def fetch(_symbol: str, use_cache: bool = True):
            nonlocal calls
            calls += 1
            return payload if calls == 1 else None

        with tempfile.TemporaryDirectory() as tmpdir:
            service = FinancialSeriesService(
                fetch,
                Path(tmpdir) / "financial-series.sqlite3",
            )
            first = await service.get_series("TEST")
            second = await service.get_series("TEST", refresh=True)
        assert first is not None and second is not None
        self.assertEqual(first["observations"], second["observations"])
        self.assertIn("source_refresh_failed_using_persisted_facts", second["warnings"])


class FinancialSeriesStoreTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _normalized_row(
        *,
        value: float = 10,
        normalization_version: int = 1,
        retrieved_at: str = "2025-04-25T12:00:00+00:00",
    ) -> dict:
        payload = company_facts(
            revenues=[
                fact(
                    value,
                    "2025-01-01",
                    "2025-03-31",
                    "2025-04-25",
                    "q1",
                    form="10-Q",
                    fy=2025,
                    fp="Q1",
                )
            ]
        )
        row = extract_financial_facts(
            payload,
            symbol="TEST",
            metric="revenue",
            retrieved_at=retrieved_at,
        )[0]
        row["normalization_version"] = normalization_version
        return row

    async def test_fact_versions_are_append_only_and_content_addressed(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = FinancialSeriesStore(Path(tmpdir) / "facts.sqlite3")
            first = self._normalized_row()
            repeated = {
                **first,
                "retrieved_at": "2025-04-26T12:00:00+00:00",
            }
            revised = {
                **first,
                "value": 12,
                "retrieved_at": "2025-04-27T12:00:00+00:00",
            }

            self.assertEqual(await store.append_facts([first]), 1)
            self.assertEqual(await store.append_facts([repeated]), 0)
            self.assertEqual(await store.append_facts([revised]), 1)

            self.assertEqual(await store.count_versions("TEST", "revenue"), 2)
            resolved = await store.load_facts("TEST", "revenue")
            self.assertEqual(len(resolved), 1)
            self.assertEqual(resolved[0]["value"], 12)

    async def test_latest_normalization_wins_without_rewriting_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = FinancialSeriesStore(Path(tmpdir) / "facts.sqlite3")
            version_one = self._normalized_row(
                value=10,
                normalization_version=1,
                retrieved_at="2025-04-27T12:00:00+00:00",
            )
            version_two = self._normalized_row(
                value=11,
                normalization_version=2,
                retrieved_at="2025-04-26T12:00:00+00:00",
            )

            await store.append_facts([version_one, version_two])

            self.assertEqual(await store.count_versions("TEST", "revenue"), 2)
            resolved = await store.load_facts("TEST", "revenue")
            self.assertEqual(resolved[0]["normalization_version"], 2)
            self.assertEqual(resolved[0]["value"], 11)


if __name__ == "__main__":
    unittest.main()


class SharedCikMultiTickerTests(unittest.IsolatedAsyncioTestCase):
    """An issuer files once; two tickers must both resolve to that filing.

    Alphabet trades as GOOG and GOOGL under one CIK. Fact identity and version ranking
    were already keyed by CIK, but the read filtered on symbol — so whichever ticker was
    ingested first claimed the rows and the second one's identical facts were dropped by
    `INSERT OR IGNORE`, leaving it with no financial history at all. BRK.A/BRK.B,
    FOX/FOXA and UA/UAA share the shape.
    """

    @staticmethod
    def _rows(symbol: str) -> list[dict]:
        payload = company_facts(
            revenues=[
                fact(10, "2025-01-01", "2025-03-31", "2025-04-25", "q1", form="10-Q", fy=2025, fp="Q1")
            ]
        )
        payload["cik"] = 1652044
        return extract_financial_facts(
            payload,
            symbol=symbol,
            metric="revenue",
            retrieved_at="2025-04-25T12:00:00+00:00",
        )

    async def test_second_ticker_on_the_same_cik_still_resolves(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = FinancialSeriesStore(Path(tmpdir) / "facts.sqlite3")
            await store.append_facts(self._rows("GOOG"))
            # The identical facts arriving under the sibling ticker are correctly ignored:
            # they are the same economic fact from the same accession.
            self.assertEqual(await store.append_facts(self._rows("GOOGL")), 0)

            by_symbol = await store.load_facts("GOOGL", "revenue")
            by_cik = await store.load_facts("GOOGL", "revenue", cik=1652044)

            self.assertEqual(by_symbol, [], "symbol-only reads are what stranded the sibling")
            self.assertEqual(len(by_cik), 1)
            self.assertEqual(by_cik[0]["value"], 10)

    async def test_zero_padded_ciks_match_the_bare_form_the_store_writes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = FinancialSeriesStore(Path(tmpdir) / "facts.sqlite3")
            await store.append_facts(self._rows("GOOG"))

            # `get_cik_for_ticker` returns the 10-digit padded form.
            padded = await store.load_facts("GOOGL", "revenue", cik="0001652044")

            self.assertEqual(len(padded), 1)

    async def test_an_unrelated_issuer_is_not_pulled_in_by_cik(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = FinancialSeriesStore(Path(tmpdir) / "facts.sqlite3")
            await store.append_facts(self._rows("GOOG"))

            other = await store.load_facts("MSFT", "revenue", cik=789019)

            self.assertEqual(other, [])
