#!/usr/bin/env python3
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

from copetech_sec.market_data import PriceCandleFetcher


class PriceCandleFetcherTests(unittest.IsolatedAsyncioTestCase):
    def test_normalizes_yfinance_history(self):
        history = pd.DataFrame(
            {
                "Open": [100.123456, 101.0],
                "High": [105.0, 106.0],
                "Low": [99.0, 100.0],
                "Close": [104.0, 102.0],
                "Volume": [1000, 2000],
            },
            index=pd.to_datetime(["2026-04-29", "2026-04-30"]),
        )
        fetcher = PriceCandleFetcher(cache_dir=tempfile.mkdtemp())

        candles = fetcher._normalize_history(history)

        self.assertEqual(len(candles), 2)
        self.assertEqual(candles[0]["time"], "2026-04-29")
        self.assertEqual(candles[0]["open"], 100.1235)
        self.assertEqual(candles[1]["volume"], 2000)

    async def test_get_daily_candles_uses_cache(self):
        calls = {"count": 0}

        def downloader(*_args, **_kwargs):
            calls["count"] += 1
            return pd.DataFrame(
                {
                    "Open": [100.0],
                    "High": [101.0],
                    "Low": [99.0],
                    "Close": [100.5],
                    "Volume": [1000],
                },
                index=pd.to_datetime([datetime.now(timezone.utc).date().isoformat()]),
            )

        with tempfile.TemporaryDirectory() as tmpdir:
            fetcher = PriceCandleFetcher(cache_dir=tmpdir, downloader=downloader)
            first = await fetcher.get_daily_candles("AAPL", 180)
            second = await fetcher.get_daily_candles("AAPL", 180)

            self.assertEqual(first, second)
            self.assertEqual(calls["count"], 1)
            self.assertTrue((Path(tmpdir) / "market_data" / "AAPL_180_1d.json").exists())

    def test_empty_history_returns_empty_list(self):
        fetcher = PriceCandleFetcher(cache_dir=tempfile.mkdtemp())
        self.assertEqual(fetcher._normalize_history(pd.DataFrame()), [])


if __name__ == "__main__":
    unittest.main()
