"""Regressions for arithmetic on unresolved periods; values are hand calculated."""

import asyncio
from copy import deepcopy
from itertools import permutations
import json

import pytest

from analysis.financial_series_audit.checks import check_financial_series
from analysis.financial_series_audit.pipeline import _resolve_metric
from analysis.financial_series_audit.report import write_run_report
from copetech_sec.derived_series import resolve_derived_series
from copetech_sec.financial_series import extract_financial_facts, resolve_financial_series
from copetech_sec.financial_series_service import FinancialSeriesService
from copetech_sec.roic_series import resolve_roic_series


def fact(value, start, end, accession, filed="2026-02-01", form="10-Q"):
    return dict(val=value, start=start, end=end, accn=accession, filed=filed,
                form=form, fy=2025, fp="FY" if form == "10-K" else "Q3")


def company_facts(entries):
    return {"cik":1, "entityName":"Conflict Fixture", "facts":{"us-gaap":{
        "NetCashProvidedByUsedInOperatingActivities":{"units":{"USD":entries}}
    }}}


def resolve(entries, **kwargs):
    rows = extract_financial_facts(company_facts(entries), symbol="TEST",
                                   metric="operating_cash_flow", retrieved_at="2026-03-01")
    return resolve_financial_series(rows, symbol="TEST", metric="operating_cash_flow", **kwargs)


FY = [
    fact(10, "2025-01-01", "2025-03-31", "q1", "2025-04-25"),
    fact(25, "2025-01-01", "2025-06-30", "h1", "2025-07-25"),
    fact(45, "2025-01-01", "2025-09-30", "nm", "2025-10-25"),
    fact(70, "2025-01-01", "2025-12-31", "fy", form="10-K"),
]


@pytest.mark.parametrize("alternate_value", [70, 71])
def test_conflicting_annual_period_withholds_q4_and_ttm_in_every_order(alternate_value):
    alternate = fact(alternate_value, "2024-12-31", "2025-12-31", "alternate",
                     "2026-03-01", "10-K")
    entries = FY + [alternate]
    expected = resolve(entries)
    assert [r["value"] for r in expected["observations"]] == [10, 15, 20]
    assert expected["warnings"] == ["ambiguous_derivation_inputs", "derived_from_ytd"]
    assert expected["ambiguities"][0]["stage"] == "q4_annual"
    assert {r["selectedSource"]["accessionNumber"] for r in expected["ambiguities"][0]["candidates"]} == {"fy", "alternate"}
    assert resolve(entries, frequency="ttm")["observations"] == []
    assert len(resolve(entries, frequency="annual")["observations"]) == 2
    for order in permutations(entries):
        assert resolve(list(order)) == expected
    before = resolve(entries, as_of="2026-02-28")
    assert [r["value"] for r in before["observations"]] == [10, 15, 20, 25]
    assert before["ambiguities"] == []


def test_conflicting_ytd_end_cannot_feed_q3_or_q4():
    alternate = fact(26, "2024-12-31", "2025-06-30", "alternate")
    result = resolve(FY + [alternate])
    assert [r["value"] for r in result["observations"]] == [10]
    assert any(a["stage"] == "ytd" for a in result["ambiguities"])
    december = resolve(FY + [alternate], start="2025-12-01")
    assert december["observations"] == []
    assert any(a["stage"] == "q4" and a["periodEnd"] == "2025-12-31"
               for a in december["ambiguities"])


def test_quarter_only_conflict_does_not_suppress_unique_annual_fcf():
    annual = resolve(FY + [fact(26, "2024-12-31", "2025-06-30", "alternate")],
                     frequency="annual")
    assert annual["ambiguities"] == []
    capex = component(10)
    capex["observations"][0].update(periodStart="2025-01-01", frequency="annual")
    result = resolve_derived_series(
        {"operating_cash_flow":annual, "capex":capex}, symbol="TEST", metric="fcf",
        frequency="annual", basis="canonical", alignment="availability",
    )
    assert result["observations"][0]["value"] == 60  # Unique annual 70 - 10.
    assert result["ambiguities"] == []


def test_multiple_ytd_conflicts_have_deterministic_dependency_evidence():
    entries = FY + [
        fact(26, "2024-12-31", "2025-06-30", "alternate-h1"),
        fact(46, "2024-12-31", "2025-09-30", "alternate-nm"),
    ]
    result = resolve(entries, frequency="ttm")
    assert result["observations"] == []
    assert resolve(list(reversed(entries)), frequency="ttm") == result


def test_conflicting_reported_quarters_remain_visible_but_cannot_feed_ttm():
    entries = [
        fact(10, "2025-01-01", "2025-03-31", "q1"),
        fact(20, "2025-04-01", "2025-06-30", "q2"),
        fact(21, "2025-04-02", "2025-06-30", "alternate"),
        fact(30, "2025-07-01", "2025-09-30", "q3"),
        fact(40, "2025-10-01", "2025-12-31", "q4"),
    ]
    assert len(resolve(entries)["observations"]) == 5
    result = resolve(entries, frequency="ttm")
    assert result["observations"] == []
    assert result["ambiguities"][-1]["stage"] == "ttm"


def test_valid_zero_flows_and_derivations_keep_provenance():
    entries = [{**f, "val":0} for f in FY]
    result = resolve(entries)
    assert [r["value"] for r in result["observations"]] == [0, 0, 0, 0]
    assert all(r["sources"] for r in result["observations"])
    assert result["ambiguities"] == []
    assert resolve(entries, frequency="ttm")["observations"][0]["value"] == 0


def test_ttm_does_not_emit_a_value_at_an_explicitly_blocked_end():
    entries = FY + [
        fact(25, "2025-10-01", "2025-12-31", "reported-q4"),
        fact(71, "2024-12-31", "2025-12-31", "alternate", form="10-K"),
    ]
    assert [r["value"] for r in resolve(entries)["observations"]] == [10, 15, 20, 25]
    result = resolve(entries, frequency="ttm")
    assert result["observations"] == []
    assert "ambiguous_derivation_inputs" in result["warnings"]


def component(value):
    source = dict(taxonomy="us-gaap", concept="Fixture", filed="2026-02-01",
                  accessionNumber=str(value), form="10-K")
    row = dict(periodStart="2025-10-01", periodEnd="2025-12-31", value=value,
               unit="USD", frequency="quarterly", availableAt="2026-02-01",
               confidence=1, qualityFlags=[], selectedSource=source,
               availabilitySource=source, sources=[source])
    return dict(cik=1, entityName="Conflict Fixture", observations=[row], warnings=[])


def derived(components, metric="ebitda"):
    return resolve_derived_series(components, symbol="TEST", metric=metric,
                                 frequency="quarterly", basis="canonical", alignment="availability")


def test_duplicate_exact_component_window_is_withheld_not_last_wins():
    depreciation = component(477)
    depreciation["observations"] += component(458)["observations"]
    inputs = {"operating_income":component(1000), "dep_amort":depreciation}
    result = derived(inputs)
    assert result["observations"] == []  # Previously 1458 or 1477 by input order.
    assert "ambiguous_derivation_inputs" in result["warnings"]
    assert {r["value"] for r in result["ambiguities"][0]["candidates"]} == {477, 458}
    reversed_inputs = deepcopy(inputs)
    reversed_inputs["dep_amort"]["observations"].reverse()
    assert derived(reversed_inputs) == result
    assert len(inputs["dep_amort"]["observations"]) == 2


def test_conflicting_optional_input_cannot_be_treated_as_missing_for_fallback():
    gross_profit = component(40)
    gross_profit["observations"] += component(41)["observations"]
    result = derived({"revenue":component(100), "cost_of_revenue":component(60),
                      "gross_profit":gross_profit}, metric="gross_profit")
    assert result["observations"] == []
    assert result["ambiguities"]


@pytest.mark.parametrize("metric", ["gross_profit", "gross_margin"])
@pytest.mark.parametrize("gross_profit", [0, 40])
def test_unused_cost_conflict_keeps_preferred_gross_profit(metric, gross_profit):
    cost = component(60)
    alternate = component(61)["observations"][0]
    alternate["periodStart"] = "2025-10-02"
    cost["observations"].append(alternate)
    result = derived({"revenue":component(100), "gross_profit":component(gross_profit),
                      "cost_of_revenue":cost}, metric=metric)
    row = result["observations"][0]
    assert row["value"] == (gross_profit if metric == "gross_profit" else gross_profit / 100)
    assert "cost_of_revenue" not in row["inputMetrics"]
    assert "cost_of_revenue" not in row["evidenceMetrics"]
    assert {s["accessionNumber"] for s in row["sources"]}.isdisjoint({"60", "61"})
    assert result["ambiguities"][0]["component"] == "cost_of_revenue"
    assert any(f.code == "ambiguous_derivation_inputs" and f.severity == "error"
               for f in check_financial_series(result))


def test_unambiguous_composite_zero_is_not_missing():
    result = derived({"operating_income":component(0), "dep_amort":component(0)})
    assert result["observations"][0]["value"] == 0
    assert len(result["observations"][0]["sources"]) == 2
    assert result["ambiguities"] == []


def test_roic_does_not_overwrite_conflicting_tax_or_fallback_past_ambiguous_balance():
    flows = [component(value) for value in (100, 20, 100)]
    for payload in flows:
        payload["observations"][0].update(periodStart="2025-01-01", frequency="ttm")
    balances = component(100)
    ending = balances["observations"][0]
    ending.update(periodStart="2025-12-31", frequency="quarterly")
    beginning = {**deepcopy(ending), "periodStart":"2025-01-01", "periodEnd":"2025-01-01"}
    balances["observations"].append(beginning)
    assert resolve_roic_series(*flows, balances, symbol="TEST")["observations"][0]["value"] == .8
    conflicted_balances = deepcopy(balances)
    conflicted_balances["observations"].append({**deepcopy(beginning), "value":200})
    result = resolve_roic_series(*flows, conflicted_balances, symbol="TEST")
    assert result["observations"] == []
    assert result["ambiguities"][0]["component"] == "invested_capital"
    assert "ambiguous_derivation_inputs" in result["warnings"]
    conflicted_flows = deepcopy(flows)
    conflicted_flows[1]["observations"].append({**deepcopy(flows[1]["observations"][0]), "value":21})
    result = resolve_roic_series(*conflicted_flows, balances, symbol="TEST")
    assert result["observations"] == []
    assert result["ambiguities"][0]["component"] == "tax_expense"


def test_service_pipeline_report_and_audit_retain_blocking_ambiguity(tmp_path):
    entries = FY + [fact(71, "2024-12-31", "2025-12-31", "alternate", form="10-K")]
    async def fetch(*args, **kwargs):
        return company_facts(entries)
    service = FinancialSeriesService(fetch, tmp_path / "facts.sqlite3")
    result = asyncio.run(_resolve_metric(service, "TEST", "operating_cash_flow", "ttm"))
    assert result["state"] == "unavailable"
    assert result["ambiguities"][0]["candidates"][0]["sources"]
    findings = check_financial_series({"symbol":"TEST", **result})
    assert any(f.code == "ambiguous_derivation_inputs" and f.severity == "error" for f in findings)
    run_dir = write_run_report(tmp_path / "reports", [dict(ticker="TEST", metrics=[result], valuations=[])],
                               [f.to_dict() for f in findings])
    saved = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    assert saved["issuers"][0]["metrics"][0]["ambiguities"] == result["ambiguities"]
    public = asyncio.run(service.get_series("TEST", metric="operating_cash_flow",
                                            frequency="ttm", include_provenance=False))
    assert all("sources" not in r and "selectedSource" not in r
               for a in public["ambiguities"] for r in a["candidates"])


def test_withheld_optional_q4_cannot_trigger_gross_profit_fallback(tmp_path):
    gross_profit = [
        fact(20, "2025-01-01", "2025-03-31", "q1"),
        fact(30, "2025-04-01", "2025-06-30", "q2"),
        fact(31, "2025-04-02", "2025-06-30", "alternate", "2026-02-15"),
        fact(40, "2025-07-01", "2025-09-30", "q3"),
        fact(100, "2025-01-01", "2025-12-31", "annual", form="10-K"),
    ]
    payload = {"cik":1, "facts":{"us-gaap":{
        concept:{"units":{"USD":entries}} for concept, entries in {
            "GrossProfit":gross_profit,
            "Revenues":[fact(100, "2025-10-01", "2025-12-31", "revenue")],
            "CostOfRevenue":[fact(60, "2025-10-01", "2025-12-31", "cost")],
        }.items()
    }}}
    async def fetch(*args, **kwargs):
        return payload
    service = FinancialSeriesService(fetch, tmp_path / "facts.sqlite3")
    before = asyncio.run(service.get_series("TEST", metric="gross_profit", start="2025-12-01",
                                            as_of="2026-02-14"))
    assert before["observations"][0]["value"] == 10  # 100 - 20 - 30 - 40.
    after = asyncio.run(service.get_series("TEST", metric="gross_profit", start="2025-12-01"))
    assert after["observations"] == []  # Must not fall back to revenue - cost = 40.
    assert any(a["component"] == "gross_profit" and a["periodEnd"] == "2025-12-31"
               for a in after["ambiguities"] if "component" in a)
