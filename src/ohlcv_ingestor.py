"""
src/ohlcv_ingestor.py — Fetch OHLCV candles from Binance via ccxt.

Pulls historical and incremental candle data for configured pairs and
timeframes, inserts into the ohlcv table. Uses INSERT OR IGNORE semantics
(ON CONFLICT DO NOTHING) so re-runs are safe.
"""

import os
import time
import logging
from datetime import datetime, timezone

import ccxt
import pandas as pd
from sqlalchemy import insert, text
from dotenv import load_dotenv

from src.db import get_engine, ohlcv

load_dotenv()
log = logging.getLogger(__name__)

PAIRS = ["BTC/USDT", "ETH/USDT", "SOL/USDT"]
TIMEFRAMES = ["1h", "4h", "1d"]

# Binance max candles per request
BATCH_LIMIT = 1000


class OHLCVIngestor:
    def __init__(self):
        self.exchange = ccxt.binance({
            "apiKey":    os.getenv("BINANCE_API_KEY", ""),
            "secret":    os.getenv("BINANCE_API_SECRET", ""),
            "enableRateLimit": True,
        })
        self.engine = get_engine()

    def fetch_candles(self, pair: str, timeframe: str, since_ms: int = None) -> pd.DataFrame:
        """
        Fetch up to BATCH_LIMIT candles for one pair/timeframe.

        Args:
            pair:       e.g. 'BTC/USDT'
            timeframe:  e.g. '1h'
            since_ms:   Unix timestamp in milliseconds; if None, fetches most recent candles.

        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
        """
        raw = self.exchange.fetch_ohlcv(pair, timeframe, since=since_ms, limit=BATCH_LIMIT)
        if not raw:
            return pd.DataFrame()

        df = pd.DataFrame(raw, columns=["ts_ms", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        df["pair"] = pair
        df["timeframe"] = timeframe
        return df[["pair", "timeframe", "timestamp", "open", "high", "low", "close", "volume"]]

    def fetch_full_history(self, pair: str, timeframe: str, since_ms: int) -> pd.DataFrame:
        """
        Page through Binance history from since_ms to present, BATCH_LIMIT candles at a time.

        Args:
            pair, timeframe: as above
            since_ms: start of history in Unix ms

        Returns:
            Concatenated DataFrame of all pages.
        """
        all_frames = []
        cursor = since_ms

        while True:
            df = self.fetch_candles(pair, timeframe, since_ms=cursor)
            if df.empty:
                break
            all_frames.append(df)
            log.info(f"  Fetched {len(df)} candles for {pair}/{timeframe} from {df['timestamp'].iloc[0]}")

            if len(df) < BATCH_LIMIT:
                break  # Last page

            # Advance cursor to just after last candle
            last_ts_ms = int(df["timestamp"].iloc[-1].timestamp() * 1000)
            cursor = last_ts_ms + 1
            time.sleep(self.exchange.rateLimit / 1000)

        if not all_frames:
            return pd.DataFrame()
        return pd.concat(all_frames, ignore_index=True)

    def insert_candles(self, df: pd.DataFrame) -> int:
        """
        Insert candles into ohlcv table. ON CONFLICT DO NOTHING — safe to re-run.

        Returns:
            Number of rows inserted (excludes skipped duplicates).
        """
        if df.empty:
            return 0

        records = df.to_dict(orient="records")
        inserted = 0

        with self.engine.begin() as conn:
            for record in records:
                result = conn.execute(
                    insert(ohlcv).values(**record).on_conflict_do_nothing(
                        constraint="uq_ohlcv"
                    )
                )
                inserted += result.rowcount

        return inserted

    def get_latest_timestamp(self, pair: str, timeframe: str):
        """Return the most recent timestamp in the DB for this pair/timeframe, or None."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT MAX(timestamp) FROM ohlcv WHERE pair = :pair AND timeframe = :tf"),
                {"pair": pair, "tf": timeframe}
            ).scalar()
        return result

    def run(self, pairs=None, timeframes=None, lookback_days=90):
        """
        Main entry point. For each pair/timeframe:
          - If data exists: fetch only new candles since latest DB timestamp
          - If no data: fetch lookback_days of history

        Args:
            pairs:         list of pair strings, defaults to PAIRS
            timeframes:    list of timeframe strings, defaults to TIMEFRAMES
            lookback_days: initial history window when no data exists
        """
        pairs = pairs or PAIRS
        timeframes = timeframes or TIMEFRAMES

        for pair in pairs:
            for tf in timeframes:
                log.info(f"Processing {pair} / {tf}")

                latest = self.get_latest_timestamp(pair, tf)

                if latest:
                    # Incremental: start from latest candle
                    since_ms = int(latest.timestamp() * 1000) + 1
                    log.info(f"  Incremental from {latest}")
                else:
                    # Full history: lookback_days from now
                    since_dt = datetime.now(timezone.utc).timestamp() - (lookback_days * 86400)
                    since_ms = int(since_dt * 1000)
                    log.info(f"  Full history: {lookback_days}-day lookback")

                df = self.fetch_full_history(pair, tf, since_ms)

                if df.empty:
                    log.info(f"  No new candles for {pair}/{tf}")
                    continue

                n = self.insert_candles(df)
                log.info(f"  Inserted {n} new candles ({len(df) - n} duplicates skipped)")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ingestor = OHLCVIngestor()
    ingestor.run()
