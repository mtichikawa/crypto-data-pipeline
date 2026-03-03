"""
src/events_ingestor.py — Economic calendar ingestion and near_event tagging.

Loads high-impact macro events (CPI, NFP, FOMC, etc.) into market_events,
then tags candles in the ohlcv table whose timestamp falls within ±2 candles
of a high-impact event. T2 uses these markers to annotate chart images; T3
uses them as a signal feature.

Data source: CryptoPanic API (economic category) or a manually maintained
seed JSON at data/events_seed.json for offline development.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from sqlalchemy import insert, update, select, text
from dotenv import load_dotenv

from src.db import get_engine, market_events, ohlcv

load_dotenv()
log = logging.getLogger(__name__)

# Timeframe duration in minutes — used for ±2-candle window calculation
TIMEFRAME_MINUTES = {"1h": 60, "4h": 240, "1d": 1440}

# High-impact event name patterns — near_event only tags these
HIGH_IMPACT_KEYWORDS = ["CPI", "NFP", "FOMC", "PPI", "GDP", "ISM", "Retail Sales",
                        "Fed", "Interest Rate", "Unemployment", "Payroll"]


class EventsIngestor:
    def __init__(self):
        self.engine = get_engine()
        self.api_key = os.getenv("CRYPTOPANIC_API_KEY", "")

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def load_from_seed(self, seed_path: str = "data/events_seed.json") -> list[dict]:
        """
        Load events from a local seed JSON file.

        Seed format (list of dicts):
            [
              {
                "event_time": "2025-09-06T12:30:00Z",
                "event_name": "NFP",
                "impact": "high",
                "actual": "187K",
                "forecast": "170K",
                "previous": "157K"
              },
              ...
            ]
        """
        path = Path(seed_path)
        if not path.exists():
            log.warning(f"Seed file not found: {seed_path}")
            return []
        with open(path) as f:
            return json.load(f)

    def fetch_from_cryptopanic(self, pages: int = 3) -> list[dict]:
        """
        Pull macro/economic news from CryptoPanic API and convert to event records.

        CryptoPanic doesn't provide a structured economic calendar, so this
        method pulls 'important' category posts and extracts event-like records
        from headlines. For a proper calendar, swap in a Forex Factory or
        Investing.com scraper here.

        Args:
            pages: number of API pages to fetch (50 items/page)

        Returns:
            List of event dicts compatible with market_events schema.
        """
        if not self.api_key:
            log.warning("CRYPTOPANIC_API_KEY not set — skipping API fetch")
            return []

        events = []
        base_url = "https://cryptopanic.com/api/v1/posts/"

        for page in range(1, pages + 1):
            try:
                resp = requests.get(base_url, params={
                    "auth_token": self.api_key,
                    "filter": "important",
                    "public": "true",
                    "page": page,
                }, timeout=10)
                resp.raise_for_status()
                data = resp.json()

                for post in data.get("results", []):
                    title = post.get("title", "")
                    published = post.get("published_at", "")
                    if not published:
                        continue

                    # Classify impact based on known keywords
                    impact = "low"
                    for kw in HIGH_IMPACT_KEYWORDS:
                        if kw.upper() in title.upper():
                            impact = "high"
                            break

                    events.append({
                        "event_time": published,
                        "event_name": title[:100],
                        "impact": impact,
                        "actual": None,
                        "forecast": None,
                        "previous": None,
                    })

            except requests.RequestException as e:
                log.error(f"CryptoPanic API error (page {page}): {e}")
                break

        log.info(f"Fetched {len(events)} events from CryptoPanic")
        return events

    def insert_events(self, events: list[dict]) -> int:
        """Insert events into market_events. ON CONFLICT DO NOTHING."""
        inserted = 0
        with self.engine.begin() as conn:
            for ev in events:
                result = conn.execute(
                    insert(market_events).values(
                        event_time=ev["event_time"],
                        event_name=ev["event_name"],
                        impact=ev.get("impact", "low"),
                        actual=ev.get("actual"),
                        forecast=ev.get("forecast"),
                        previous=ev.get("previous"),
                        near_event=False,
                    ).on_conflict_do_nothing(constraint="uq_market_events")
                )
                inserted += result.rowcount

        log.info(f"Inserted {inserted} new events")
        return inserted

    # ── near_event tagging ────────────────────────────────────────────────────

    def tag_near_events(self, timeframe: str = "1h"):
        """
        For each high-impact event, mark ohlcv candles within ±2 candles as near_event=True.

        Only tags candles in the specified timeframe (default 1h — finest grain).
        The window is ±2 * timeframe_minutes on each side of the event_time.

        Args:
            timeframe: which ohlcv timeframe to tag
        """
        minutes = TIMEFRAME_MINUTES.get(timeframe, 60)
        window = timedelta(minutes=minutes * 2)

        with self.engine.begin() as conn:
            # Fetch all high-impact events
            rows = conn.execute(
                select(market_events.c.event_time, market_events.c.event_name)
                .where(market_events.c.impact == "high")
            ).fetchall()

            tagged = 0
            for row in rows:
                et = row.event_time
                if et.tzinfo is None:
                    et = et.replace(tzinfo=timezone.utc)

                result = conn.execute(
                    update(ohlcv)
                    .where(
                        ohlcv.c.timeframe == timeframe,
                        ohlcv.c.timestamp >= et - window,
                        ohlcv.c.timestamp <= et + window,
                    )
                    .values(near_event=True)
                )
                tagged += result.rowcount

            log.info(f"Tagged {tagged} candles as near_event for {len(rows)} high-impact events")

    # ── Main ──────────────────────────────────────────────────────────────────

    def run(self, use_seed: bool = True, use_api: bool = True, tag_timeframe: str = "1h"):
        """
        Full events pipeline: ingest from seed + API, then tag near_event candles.

        Args:
            use_seed:      load events from data/events_seed.json
            use_api:       fetch from CryptoPanic API
            tag_timeframe: timeframe to apply near_event tags on
        """
        all_events = []

        if use_seed:
            seed_events = self.load_from_seed()
            log.info(f"Loaded {len(seed_events)} events from seed file")
            all_events.extend(seed_events)

        if use_api:
            api_events = self.fetch_from_cryptopanic()
            all_events.extend(api_events)

        if all_events:
            self.insert_events(all_events)

        self.tag_near_events(timeframe=tag_timeframe)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ingestor = EventsIngestor()
    ingestor.run()
