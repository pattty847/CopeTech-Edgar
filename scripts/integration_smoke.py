#!/usr/bin/env python3
"""Live SEC smoke test for Tier 2 + Tier 3 features.

This script bypasses FastAPI and calls SECDataFetcher methods directly so any
schema mismatch surfaces as a clear parse failure rather than an HTTP 500.

Usage:
    SEC_API_USER_AGENT="Your Name your@email.com" \\
        .venv/bin/python scripts/integration_smoke.py

Optional overrides:
    .venv/bin/python scripts/integration_smoke.py --ticker NVDA --manager-cik 0001067983

Output: one section per feature. Any FAIL line should be pasted back to me with
the saved raw payload (it'll be under scripts/_smoke_artifacts/) so I can harden
the parser for whatever shape SEC sent us.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import traceback
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from copetech_sec.sec_api import SECDataFetcher  # noqa: E402


ARTIFACT_DIR = Path(__file__).resolve().parent / "_smoke_artifacts"


def banner(title: str) -> None:
    print(f"\n{'=' * 70}\n  {title}\n{'=' * 70}")


def ok(message: str) -> None:
    print(f"  PASS  {message}")


def fail(message: str) -> None:
    print(f"  FAIL  {message}")


def info(message: str) -> None:
    print(f"  ..    {message}")


def save_artifact(name: str, payload: Any) -> Path:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    path = ARTIFACT_DIR / f"{name}.json"
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return path


async def check_company_info(fetcher: SECDataFetcher, ticker: str) -> bool:
    banner(f"[01] Company info — {ticker}")
    try:
        info_payload = await fetcher.get_company_info(ticker)
    except Exception as exc:
        fail(f"raised: {exc}")
        traceback.print_exc()
        return False
    if info_payload is None:
        fail("returned None")
        return False
    info(f"name={info_payload.get('name')!r} cik={info_payload.get('cik')}")
    if not info_payload.get("cik"):
        fail("no CIK")
        return False
    ok("company info shape looks right")
    return True


async def check_insider_signals_and_clusters(fetcher: SECDataFetcher, ticker: str) -> bool:
    banner(f"[02] Insider signals + cluster detector — {ticker}")
    try:
        payload = await fetcher.get_insider_signal_payload(ticker, days_back=180, filing_limit=25)
    except Exception as exc:
        fail(f"get_insider_signal_payload raised: {exc}")
        traceback.print_exc()
        return False

    events = payload.get("events", [])
    clusters = payload.get("clusters", [])
    info(f"events={len(events)} clusters={len(clusters)}")
    if not events:
        info("no Form 4 events in window — cluster detector cannot be exercised")
    else:
        ok(f"first event keys: {sorted(events[0].keys())[:8]}...")

    # Sanity: re-run cluster detection at a lower threshold to confirm it's wired
    try:
        relaxed = fetcher.form4_processor.detect_cluster_buys(events, window_days=30, min_unique_insiders=2)
        info(f"relaxed clusters (win=30d, min=2): {len(relaxed)}")
    except Exception as exc:
        fail(f"detect_cluster_buys raised: {exc}")
        return False

    save_artifact(f"insider_signals_{ticker}", payload)
    ok("insider signal payload returned")
    return True


async def check_financial_trend(fetcher: SECDataFetcher, ticker: str) -> bool:
    banner(f"[03] XBRL financial trend — {ticker}")
    try:
        trend = await fetcher.get_financial_trend(ticker, periods=6)
    except Exception as exc:
        fail(f"get_financial_trend raised: {exc}")
        traceback.print_exc()
        return False
    if trend is None:
        fail("returned None (company facts unavailable?)")
        return False

    info(f"ticker={trend.get('ticker')} period_end={trend.get('period_end')} source_form={trend.get('source_form')}")
    metrics = trend.get("metrics", {})
    populated = [key for key, value in metrics.items() if value]
    info(f"populated metrics: {populated}")

    revenue = metrics.get("revenue")
    if revenue and revenue.get("quarterly"):
        latest = revenue["quarterly"][0]
        info(
            f"latest revenue: {latest.get('period')} value={latest.get('value')} "
            f"qoq={latest.get('qoq_pct')} yoy={latest.get('yoy_pct')}"
        )

    save_artifact(f"trend_{ticker}", trend)
    ok("financial trend computed")
    return True


async def check_planned_sales(fetcher: SECDataFetcher, ticker: str) -> bool:
    banner(f"[04] Form 144 planned sales — {ticker}")
    try:
        payload = await fetcher.get_planned_insider_sales(ticker, days_back=180, filing_limit=10)
    except Exception as exc:
        fail(f"get_planned_insider_sales raised: {exc}")
        traceback.print_exc()
        return False

    records = payload.get("records", [])
    totals = payload.get("totals", {})
    info(f"record_count={totals.get('record_count')} unique_filers={totals.get('unique_filers')}")
    info(f"planned_shares={totals.get('planned_shares')} aggregate_value={totals.get('aggregate_market_value')}")

    if records:
        sample = records[0]
        info(
            f"sample: filer={sample.get('account_name')!r} class={sample.get('security_class')!r} "
            f"shares={sample.get('planned_shares')} value={sample.get('aggregate_market_value')}"
        )
        missing_critical = [
            key for key in ("issuer_symbol", "planned_shares", "aggregate_market_value")
            if sample.get(key) is None
        ]
        if missing_critical:
            fail(f"sample record missing critical fields: {missing_critical}")
            save_artifact(f"planned_sales_{ticker}_raw", payload)
            return False
    else:
        info("no Form 144 filings in window for this ticker (not a parser failure)")

    save_artifact(f"planned_sales_{ticker}", payload)
    ok("Form 144 parser ran without crashing")
    return True


async def check_8k_events(fetcher: SECDataFetcher, ticker: str) -> bool:
    banner(f"[05] Form 8-K events — {ticker}")
    try:
        payload = await fetcher.get_8k_events(ticker, days_back=180, filing_limit=20)
    except Exception as exc:
        fail(f"get_8k_events raised: {exc}")
        traceback.print_exc()
        return False

    events = payload.get("events", [])
    totals = payload.get("totals", {})
    info(f"event_count={totals.get('event_count')} high_signal_count={totals.get('high_signal_count')}")
    info(f"category_counts={totals.get('category_counts')}")

    if events:
        latest = events[0]
        info(
            f"latest: {latest.get('filing_date')} items=" +
            ", ".join(f"{item['code']}({item['category']})" for item in latest.get("items", []))
        )
        unknown = [
            item for event in events for item in event.get("items", [])
            if item.get("category") == "unknown"
        ]
        if unknown:
            codes = sorted({item["code"] for item in unknown})
            fail(f"unknown item codes encountered (extend ITEM_CODE_MAP): {codes}")
            save_artifact(f"8k_events_{ticker}_unknown", payload)
            return False
    else:
        info("no 8-K filings in window for this ticker")

    save_artifact(f"8k_events_{ticker}", payload)
    ok("8-K item parser ran without crashing")
    return True


async def check_13f_changes(fetcher: SECDataFetcher, manager_cik: str) -> bool:
    banner(f"[06] 13F QoQ changes — CIK {manager_cik}")
    try:
        payload = await fetcher.get_13f_holdings_changes(manager_cik, days_back=365 * 2, top_n=10)
    except Exception as exc:
        fail(f"get_13f_holdings_changes raised: {exc}")
        traceback.print_exc()
        return False

    if payload.get("current_filing") is None:
        fail("no 13F-HR filings found for this manager in window")
        return False

    info(f"manager={payload.get('manager_name')!r}")
    info(
        f"current_filing={payload['current_filing'].get('accession_no')} "
        f"({payload['current_filing'].get('filing_date')})"
    )
    if payload.get("prior_filing"):
        info(
            f"prior_filing={payload['prior_filing'].get('accession_no')} "
            f"({payload['prior_filing'].get('filing_date')})"
        )

    changes = payload.get("changes") or {}
    totals = changes.get("totals", {})
    info(
        f"new={len(changes.get('new_positions', []))} "
        f"inc={len(changes.get('increased', []))} "
        f"red={len(changes.get('reduced', []))} "
        f"out={len(changes.get('sold_out', []))} "
        f"unchanged={changes.get('unchanged_count')}"
    )
    info(
        f"prior_value={totals.get('prior_value')} current_value={totals.get('current_value')} "
        f"turnover_pct={totals.get('turnover_pct')}"
    )

    # Manager filings reliably have positive current_value; zero means the
    # information-table fetch silently 404'd (wrong CIK in archive URL, etc.).
    if (totals.get("current_value") or 0) == 0 and not any(
        changes.get(bucket) for bucket in ("new_positions", "increased", "reduced", "sold_out")
    ):
        fail("13F payload has zero current_value and empty buckets — info table fetch likely failed silently")
        save_artifact(f"13f_changes_{manager_cik}_empty", payload)
        return False

    top_new = changes.get("new_positions", [])
    if top_new:
        info(f"top new: {top_new[0].get('issuer')!r} change={top_new[0].get('value_change')}")

    save_artifact(f"13f_changes_{manager_cik}", payload)
    ok("13F QoQ diff computed")
    return True


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticker", default="AAPL", help="Ticker for company-level checks")
    parser.add_argument(
        "--manager-cik", default="0001067983",
        help="Manager CIK for 13F (default: Berkshire Hathaway)",
    )
    parser.add_argument(
        "--planned-sales-ticker", default="META",
        help="Ticker for Form 144 check (META execs file Form 144 frequently)",
    )
    args = parser.parse_args()

    if not os.environ.get("SEC_API_USER_AGENT"):
        print(
            "ERROR: SEC_API_USER_AGENT not set. SEC rejects requests without it.\n"
            "  export SEC_API_USER_AGENT='Your Name your@email.com'"
        )
        return 2

    fetcher = SECDataFetcher()
    results: dict[str, bool] = {}
    try:
        results["company_info"] = await check_company_info(fetcher, args.ticker)
        results["insider_clusters"] = await check_insider_signals_and_clusters(fetcher, args.ticker)
        results["financial_trend"] = await check_financial_trend(fetcher, args.ticker)
        results["planned_sales"] = await check_planned_sales(fetcher, args.planned_sales_ticker)
        results["8k_events"] = await check_8k_events(fetcher, args.ticker)
        results["13f_changes"] = await check_13f_changes(fetcher, args.manager_cik)
    finally:
        await fetcher.close()

    banner("Summary")
    for name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  {status:<5} {name}")
    print(f"\nArtifacts saved to: {ARTIFACT_DIR}")

    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
