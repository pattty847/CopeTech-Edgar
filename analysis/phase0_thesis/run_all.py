from __future__ import annotations

import asyncio
import argparse
import json
import logging

from .backtest import run_backtest_async
from .config import BACKTEST_CSV, DB_PATH, MANAGER_SET, REPORT_PATH
from .consensus_score import generate_consensus_signals
from .cusip_map import CusipMapper
from .db import connect, init_schema
from .ingest import ingest_managers
from .metrics import compute_and_store_metrics
from .report import write_report
from .summary import SUMMARY_CSV, TOP_CHANGES_CSV, generate_manager_summary, top_change_mapping_candidates


async def run_pipeline(summary_only: bool = False, map_limit: int = 250) -> dict[str, int | str]:
    logging.info("Starting Phase 0 13F ingestion")
    ingest_summary = await ingest_managers(DB_PATH, MANAGER_SET)

    con = connect(DB_PATH)
    init_schema(con)
    try:
        logging.info("Computing manager metrics")
        metric_rows = compute_and_store_metrics(con)

        logging.info("Writing Finviz-lite manager summary")
        summary_rows, change_rows = generate_manager_summary(con)

        if summary_only:
            holdings = top_change_mapping_candidates(change_rows, limit=map_limit)
            logging.info("Summary-only mode: mapping top %s changed CUSIPs", len(holdings))
        else:
            holdings = con.execute("""
                SELECT DISTINCT cusip, issuer_name
                FROM holdings
                WHERE cusip IS NOT NULL AND cusip != ''
            """).fetchdf().to_dict("records")
            logging.info("Full mode: mapping %s unique CUSIPs", len(holdings))
        CusipMapper(con).map_holdings(holdings)

        logging.info("Generating consensus-breaking signals")
        signal_rows = generate_consensus_signals(con)

        if summary_only:
            backtest_rows = []
        else:
            logging.info("Running yfinance backtest")
            backtest_rows = await run_backtest_async(con, BACKTEST_CSV)

        logging.info("Writing report")
        report_path = write_report(con, BACKTEST_CSV, REPORT_PATH)
    finally:
        con.close()

    return {
        **ingest_summary,
        "summary_rows": len(summary_rows),
        "top_change_rows": len(change_rows),
        "metrics": len(metric_rows),
        "signals": len(signal_rows),
        "backtest_rows": len(backtest_rows),
        "database": str(DB_PATH),
        "summary_csv": str(SUMMARY_CSV),
        "top_changes_csv": str(TOP_CHANGES_CSV),
        "backtest_csv": str(BACKTEST_CSV),
        "report": report_path,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Phase 0 13F thesis pipeline.")
    parser.add_argument("--summary-only", action="store_true", help="Skip full CUSIP mapping/backtest and map only top changes.")
    parser.add_argument("--map-limit", type=int, default=250, help="CUSIPs to map in summary-only mode.")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(json.dumps(asyncio.run(run_pipeline(summary_only=args.summary_only, map_limit=args.map_limit)), indent=2))


if __name__ == "__main__":
    main()
