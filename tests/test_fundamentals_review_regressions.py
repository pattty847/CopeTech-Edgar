"""Regressions from PR #3 review; expectations are hand-calculated."""

import asyncio
import copy
from pathlib import Path

import pytest

from analysis.financial_series_audit.corpus import corpus_by_ticker
from analysis.financial_series_audit.fixtures import minimize_company_facts
from analysis.financial_series_audit.gate import validate_recorded_matrix
from analysis.financial_series_audit.pipeline import build_metric_matrix
from copetech_sec.financial_series import extract_financial_facts, resolve_financial_series
from copetech_sec.financial_series_service import FinancialSeriesService
from copetech_sec.financial_series_store import FinancialSeriesStore
from copetech_sec.financial_revisions import resolve_derived_revisions


def fact(value, end, filed, accession, *, fy=2023, start=None, form="10-K"):
    row = dict(val=value, end=end, filed=filed, accn=accession, fy=fy, fp="FY", form=form)
    if start:
        row["start"] = start
    return row


def payload(concepts):
    return {"cik": 1, "entityName": "Review fixture", "facts": {"us-gaap": {
        concept: {"units": {"USD": rows}} for concept, rows in concepts.items()
    }}}


def test_fiscal_year_different_from_calendar_year_survives_storage_and_restatement(tmp_path):
    data = payload({
        "Assets": [
            fact(100, "2024-01-28", "2024-03-15", "fy23"),
            fact(110, "2024-01-28", "2025-03-15", "fy24", fy=2024),
            fact(200, "2025-02-02", "2025-03-15", "fy24", fy=2024),
        ],
        "Revenues": [
            fact(300, "2024-01-28", "2024-03-15", "fy23", start="2023-01-30"),
            fact(400, "2025-02-02", "2025-03-15", "fy24", fy=2024, start="2024-01-29"),
        ],
    })

    async def run():
        async def fetch(*args, **kwargs):
            return data
        service = FinancialSeriesService(fetch, tmp_path / "facts.sqlite")
        early = await service.get_series("TEST", metric="total_assets", frequency="annual", as_of="2024-04-01")
        current = await service.get_series("TEST", metric="total_assets", frequency="annual")
        assert [(r["value"], r["fiscalYear"]) for r in early["observations"]] == [(100, 2023)]
        assert [(r["value"], r["fiscalYear"]) for r in current["observations"]] == [(110, 2023), (200, 2024)]
        assert current["observations"][0]["availableAt"] == "2025-03-15"
    asyncio.run(run())


def test_sparse_metric_cannot_turn_transaction_date_into_annual_balance():
    data = payload({
        "StockholdersEquity": [fact(50, "2024-07-01", "2025-03-01", "fy24", fy=2024)],
        "Revenues": [fact(200, "2024-12-31", "2025-03-01", "fy24", fy=2024, start="2024-01-01")],
    })
    # Even minimization must retain the report's actual annual context.
    minimized = minimize_company_facts(data, target_period_ends=["2024-07-01"])
    rows = extract_financial_facts(minimized, symbol="TEST", metric="stockholders_equity")
    assert resolve_financial_series(rows, symbol="TEST", metric="stockholders_equity", frequency="annual")["observations"] == []
    assert len(resolve_financial_series(rows, symbol="TEST", metric="stockholders_equity")["observations"]) == 1


@pytest.mark.parametrize("metric,early_value,late_value", [("net_debt", 90, 110), ("invested_capital", 140, 160)])
def test_debt_decision_evidence_controls_availability_and_revisions(tmp_path, metric, early_value, late_value):
    data = payload({
        concept: [fact(value, "2023-12-31", filed, filed)]
        for concept, value, filed in [
            ("DebtLongtermAndShorttermCombinedAmount", 100, "2024-02-01"),
            ("ShortTermBorrowings", 20, "2024-02-01"),
            ("CashAndCashEquivalentsAtCarryingValue", 10, "2024-02-01"),
            ("StockholdersEquity", 50, "2024-02-01"),
            ("LongTermDebtCurrent", 30, "2024-04-01"),
            ("LongTermDebtNoncurrent", 70, "2024-04-01"),
        ]
    })
    async def run():
        async def fetch(*args, **kwargs):
            return data
        service = FinancialSeriesService(fetch, tmp_path / "facts.sqlite")
        early = await service.get_series("TEST", metric=metric, frequency="annual", as_of="2024-03-01")
        late = await service.get_series("TEST", metric=metric, frequency="annual")
        assert early["observations"][0]["value"] == early_value
        row = late["observations"][0]
        assert row["value"] == late_value
        assert row["availableAt"] == row["alignedAt"] == "2024-04-01"
        assert row["availabilitySource"]["filed"] == "2024-04-01"
        assert "debt_noncurrent" not in row["inputMetrics"]
        assert {"debt_noncurrent", "long_term_debt_current"} <= set(row["evidenceMetrics"])
        from copetech_sec.derived_series import get_derived_definition
        definition = get_derived_definition(metric)
        rows = {c: extract_financial_facts(data, symbol="TEST", metric=c) for c in definition.required + definition.optional}
        revisions = resolve_derived_revisions(rows, symbol="TEST", metric=metric, frequency="annual")
        assert [(r["value"], r["availableAt"]) for r in revisions] == [(early_value, "2024-02-01"), (late_value, "2024-04-01")]
    asyncio.run(run())


def test_gate_rejects_crash_in_a_metric_without_numeric_expectations(tmp_path, monkeypatch):
    original = FinancialSeriesService.get_series
    async def broken(self, *args, **kwargs):
        if kwargs.get("metric") == "interest_coverage":
            raise RuntimeError("injected resolver failure")
        return await original(self, *args, **kwargs)
    monkeypatch.setattr(FinancialSeriesService, "get_series", broken)
    matrix = asyncio.run(build_metric_matrix("AAPL", payload({}), store_path=tmp_path / "facts.sqlite"))
    with pytest.raises(AssertionError, match="metric_resolution_error"):
        validate_recorded_matrix(corpus_by_ticker()["AAPL"], matrix)


def test_audit_keeps_old_history_and_gate_detects_old_corruption(tmp_path):
    data = payload({"Revenues": [
        fact(100 + year, f"{year}-12-31", f"{year+1}-02-01", f"fy{year}", fy=year, start=f"{year}-01-01")
        for year in range(2010, 2025)
    ]})
    matrix = asyncio.run(build_metric_matrix("AAPL", data, store_path=tmp_path / "facts.sqlite"))
    validate_recorded_matrix(corpus_by_ticker()["AAPL"], matrix)
    revenue = next(r for r in matrix["metrics"] if r["metric"] == "revenue" and r["frequency"] == "annual")
    assert len(revenue["observations"]) == 15
    revenue["observations"][0]["value"] = float("nan")
    with pytest.raises(AssertionError, match="nonfinite_value"):
        validate_recorded_matrix(corpus_by_ticker()["AAPL"], matrix)


def test_missing_comparability_warning_is_blocking(tmp_path):
    data = payload({
        "NetCashProvidedByUsedInOperatingActivities": [fact(100, "2023-12-31", "2024-02-01", "fy23", start="2023-01-01")],
        "PaymentsToAcquirePropertyPlantAndEquipment": [fact(20, "2023-12-31", "2024-02-01", "fy23", start="2023-01-01")],
    })
    matrix = asyncio.run(build_metric_matrix("SOFI", data, store_path=tmp_path / "facts.sqlite"))
    validate_recorded_matrix(corpus_by_ticker()["SOFI"], matrix)
    fcf = next(r for r in matrix["metrics"] if r["metric"] == "fcf" and r["frequency"] == "annual")
    assert fcf["observations"][0]["value"] == 80
    fcf["warnings"] = []
    for row in fcf["observations"]:
        row["qualityFlags"] = []
    with pytest.raises(AssertionError, match="missing_comparability_warning"):
        validate_recorded_matrix(corpus_by_ticker()["SOFI"], matrix)


def test_existing_ledger_migrates_annual_context_without_losing_old_rows(tmp_path):
    import aiosqlite
    data = payload({"Assets": [fact(100, "2024-01-28", "2024-03-15", "fy23")]})
    rows = extract_financial_facts(data, symbol="TEST", metric="total_assets")
    async def run():
        store = FinancialSeriesStore(tmp_path / "facts.sqlite")
        await store.append_facts(rows)
        async with aiosqlite.connect(store.db_path) as db:
            await db.execute("ALTER TABLE financial_fact_versions DROP COLUMN annual_period_end")
            await db.commit()
        await store.initialize()
        old = await store.load_facts("TEST", "total_assets")
        assert len(old) == 1 and old[0]["value"] == 100
        # New normalization versions preserve the old ledger entry.
        revised = copy.deepcopy(rows)
        revised[0]["normalization_version"] += 1
        await store.append_facts(revised)
        assert await store.count_versions("TEST", "total_assets") == 2
        assert (await store.load_facts("TEST", "total_assets"))[0]["annual_period_end"] == "2024-01-28"
    asyncio.run(run())


def test_msft_total_revenue_precedes_product_subtotal(tmp_path):
    # Independently transcribed from the 2016 comparative income statement in:
    # https://www.sec.gov/Archives/edgar/data/789019/000156459017020171/msft-10q_20170930.htm
    # USD millions: product 14,968 + service/other 6,960 = total 21,928;
    # total gross profit 14,084. Product revenue cannot be the denominator.
    values = {
        "SalesRevenueGoodsNet": 14_968_000_000,
        "SalesRevenueNet": 21_928_000_000,
        "GrossProfit": 14_084_000_000,
    }
    data = payload({
        concept: [{
            **fact(value, "2016-09-30", "2017-10-26", "0001564590-17-020171",
                   fy=2018, start="2016-07-01", form="10-Q"),
            "fp": "Q1",
        }] for concept, value in values.items()
    })
    data["cik"] = 789019
    async def run():
        async def fetch(*args, **kwargs):
            return data
        service = FinancialSeriesService(fetch, tmp_path / "msft.sqlite")
        revenue = await service.get_series("MSFT", metric="revenue", as_of="2017-10-26")
        margin = await service.get_series("MSFT", metric="gross_margin", as_of="2017-10-26")
        row, = revenue["observations"]
        assert row["value"] == 21_928_000_000
        assert row["selectedSource"]["concept"] == "SalesRevenueNet"
        assert margin["observations"][0]["value"] == pytest.approx(14_084 / 21_928)
    asyncio.run(run())
