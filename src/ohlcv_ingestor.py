"""
src/ohlcv_ingestor.py — Fetch OHLCV candles from Kraken via ccxt.

Pulls historical and incremental candle data for configured pairs and
timeframes, inserts into the ohlcv table. Uses ON CONFLICT DO NOTHING so
re-runs are safe. near_event/event_type/mins_from_event are populated later
by candle_tagger.py after market_events are loaded.
"""

import os
import time
import logging
from datetime import datetime, timezone

import ccxt
import pandas as pd
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert
from dotenv import load_dotenv

from src.db import get_engine, ohlcv

load_dotenv()
log = logging.getLogger(__name__)

PAIRS = ["BTC/USD", "ETH/USD", "SOL/USD"]
TIMEFRAMES = ["5m", "15m", "1h", "4h", "1d"]

# Kraken max candles per request
BATCH_LIMIT = 720


class OHLCVIngestor:
    def __init__(self):
        self.exchange = ccxt.kraken({
            "enableRateLimit": True,
        })
        self.engine = get_engine()

    def fetch_candles(self, pair: str, timeframe: str, since_ms: int = None) -> pd.DataFrame:
        """
        Fetch up to BATCH_LIMIT candles for one pair/timeframe.

        Args:
            pair:       e.g. 'BTC/USD'
            timeframe:  e.g. '1h'
            since_ms:   Unix timestamp in milliseconds; if None, fetches most recent candles.

        Returns:
            DataFrame with columns: pair, timeframe, open_time, open, high, low, close, volume
        """
        raw = self.exchange.fetch_ohlcv(pair, timeframe, since=since_ms, limit=BATCH_LIMIT)
        if not raw:
            return pd.DataFrame()

        # columns must match ccxt OHLCV schema: [timestamp_ms, open, high, low, close, volume]
        df = pd.DataFrame(raw, columns=["ts_ms", "open", "high", "low", "close", "volume"])
        df["open_time"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
        df["pair"] = pair
        df["timeframe"] = timeframe
        return df[["pair", "timeframe", "open_time", "open", "high", "low", "close", "volume"]]

    def fetch_full_history(self, pair: str, timeframe: str, since_ms: int) -> pd.DataFrame:
        """
        Page through Kraken history from since_ms to present, BATCH_LIMIT candles at a time.
        """
        all_frames = []
        cursor = since_ms

        while True:
            df = self.fetch_candles(pair, timeframe, since_ms=cursor)
            if df.empty:
                break
            all_frames.append(df)
            log.info(f"  Fetched {len(df)} candles for {pair}/{timeframe} from {df['open_time'].iloc[0]}")

            if len(df) < BATCH_LIMIT:
                break

            last_ts_ms = int(df["open_time"].iloc[-1].timestamp() * 1000)
            cursor = last_ts_ms + 1
            time.sleep(self.exchange.rateLimit / 1000)

        if not all_frames:
            return pd.DataFrame()
        return pd.concat(all_frames, ignore_index=True)

    def insert_candles(self, df: pd.DataFrame) -> int:
        """Insert candles into ohlcv table. ON CONFLICT DO NOTHING."""
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

    def get_latest_open_time(self, pair: str, timeframe: str):
        """Return the most recent open_time in the DB for this pair/timeframe, or None."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text("SELECT MAX(open_time) FROM ohlcv WHERE pair = :pair AND timeframe = :tf"),
                {"pair": pair, "tf": timeframe}
            ).scalar()
        return result

    def run(self, pairs=None, timeframes=None, lookback_days=90):
        """
        Main entry point. For each pair/timeframe:
          - If data exists: fetch only new candles since latest open_time in DB
          - If no data: fetch lookback_days of history
        """
        pairs = pairs or PAIRS
        timeframes = timeframes or TIMEFRAMES

        for pair in pairs:
            for tf in timeframes:
                log.info(f"Processing {pair} / {tf}")

                latest = self.get_latest_open_time(pair, tf)

                if latest:
                    since_ms = int(latest.timestamp() * 1000) + 1
                    log.info(f"  Incremental from {latest}")
                else:
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
