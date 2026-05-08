#!/usr/bin/env python3
"""Fetch latest 13F holdings for an institutional manager CIK."""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from copetech_sec.sec_api import SECDataFetcher
from copetech_sec.thirteenf_processor import SIG_CIK


async def main():
    cik = sys.argv[1] if len(sys.argv) > 1 else SIG_CIK
    row_limit = int(sys.argv[2]) if len(sys.argv) > 2 else 25
    fetcher = SECDataFetcher()
    try:
        payload = await fetcher.get_latest_13f_holdings(cik, row_limit=row_limit)
        print("THIRTEENF_DATA:" + json.dumps(payload))
    except Exception as exc:
        print("ERROR_DATA:" + json.dumps({"error": str(exc)}))
        sys.exit(1)
    finally:
        await fetcher.close()


if __name__ == "__main__":
    asyncio.run(main())
