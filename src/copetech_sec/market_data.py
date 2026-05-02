from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import re
from typing import Any, Callable

import pandas as pd


DownloadFunc = Callable[..., pd.DataFrame]


class PriceCandleFetcher:
    def __init__(
        self,
        cache_dir: str,
        ttl_seconds: int = 21600,
        downloader: DownloadFunc | None = None,
    ):
        self.cache_dir = Path(cache_dir) / "market_data"
        self.ttl_seconds = ttl_seconds
        self.downloader = downloader

    async def get_daily_candles(self, symbol: str, days_back: int) -> list[dict[str, Any]]:
        normalized = self._normalize_symbol(symbol)
        cache_path = self._cache_path(normalized, days_back)
        cached = self._load_cache(cache_path)
        if cached is not None:
            return cached

        candles = await asyncio.to_thread(self._download_daily_candles, normalized, days_back)
        self._save_cache(cache_path, candles)
        return candles

    def _download_daily_candles(self, symbol: str, days_back: int) -> list[dict[str, Any]]:
        downloader = self.downloader
        if downloader is None:
            import yfinance as yf

            downloader = yf.download

        period_days = max(days_back + 10, 30)
        history = downloader(
            symbol,
            period=f"{period_days}d",
            interval="1d",
            auto_adjust=False,
            progress=False,
            threads=False,
        )
        candles = self._normalize_history(history)
        cutoff = datetime.now(timezone.utc).date() - timedelta(days=days_back)
        return [candle for candle in candles if datetime.strptime(candle["time"], "%Y-%m-%d").date() >= cutoff]

    def _normalize_history(self, history: pd.DataFrame | None) -> list[dict[str, Any]]:
        if history is None or history.empty:
            return []

        frame = history.copy()
        if isinstance(frame.columns, pd.MultiIndex):
            if "Open" in frame.columns.get_level_values(0):
                frame.columns = frame.columns.get_level_values(0)
            else:
                frame.columns = frame.columns.get_level_values(-1)

        frame = frame.rename(columns={column: str(column).lower() for column in frame.columns})
        required = {"open", "high", "low", "close"}
        if not required.issubset(frame.columns):
            logging.warning("Market data missing required OHLC columns: %s", sorted(frame.columns))
            return []

        candles: list[dict[str, Any]] = []
        for index, row in frame.iterrows():
            try:
                open_price = float(row["open"])
                high_price = float(row["high"])
                low_price = float(row["low"])
                close_price = float(row["close"])
            except (TypeError, ValueError):
                continue

            if not all(pd.notna(value) for value in [open_price, high_price, low_price, close_price]):
                continue

            date_value = pd.Timestamp(index).date().isoformat()
            volume_value = row.get("volume", 0)
            try:
                volume = int(volume_value) if pd.notna(volume_value) else 0
            except (TypeError, ValueError):
                volume = 0

            candles.append(
                {
                    "time": date_value,
                    "open": round(open_price, 4),
                    "high": round(high_price, 4),
                    "low": round(low_price, 4),
                    "close": round(close_price, 4),
                    "volume": volume,
                }
            )

        return candles

    def _cache_path(self, symbol: str, days_back: int) -> Path:
        safe_symbol = re.sub(r"[^A-Z0-9.-]", "_", symbol.upper())
        return self.cache_dir / f"{safe_symbol}_{days_back}_1d.json"

    def _load_cache(self, path: Path) -> list[dict[str, Any]] | None:
        try:
            if not path.exists():
                return None
            payload = json.loads(path.read_text())
            fetched_at = datetime.fromisoformat(payload["fetched_at"])
            if fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - fetched_at > timedelta(seconds=self.ttl_seconds):
                return None
            candles = payload.get("candles")
            return candles if isinstance(candles, list) else None
        except (OSError, ValueError, KeyError, TypeError) as exc:
            logging.warning("Market data cache read failed for %s: %s", path, exc)
            return None

    def _save_cache(self, path: Path, candles: list[dict[str, Any]]) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                json.dumps(
                    {
                        "fetched_at": datetime.now(timezone.utc).isoformat(),
                        "candles": candles,
                    }
                )
            )
        except OSError as exc:
            logging.warning("Market data cache write failed for %s: %s", path, exc)

    @staticmethod
    def _normalize_symbol(symbol: str) -> str:
        return symbol.strip().upper()
