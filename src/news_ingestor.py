"""
src/news_ingestor.py — Crypto and macro news headline ingestion.

Pulls headlines from CryptoPanic API and/or RSS feeds (CoinDesk, CoinTelegraph),
tags each headline with a pair_tag (BTC/ETH/SOL/crypto/macro), and stores
them in news_headlines. These are consumed by T3's FinBERT text signal path.

Deduplication: ON CONFLICT on url — safe to re-run.
"""

import logging
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
from dotenv import load_dotenv

from src.db import get_engine, news_headlines

load_dotenv()
log = logging.getLogger(__name__)

RSS_FEEDS = {
    "coindesk":      "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "cointelegraph": "https://cointelegraph.com/rss",
}

# Keyword → pair_tag mapping (first match wins)
PAIR_TAG_RULES = [
    (["bitcoin", "btc"],         "BTC"),
    (["ethereum", "eth", "ether"], "ETH"),
    (["solana", "sol"],          "SOL"),
    (["fed", "fomc", "cpi", "nfp", "inflation", "rate", "gdp",
      "treasury", "macro", "recession"], "macro"),
]


def classify_pair_tag(text: str) -> str:
    """Return a pair_tag based on keywords in the headline."""
    lower = text.lower()
    for keywords, tag in PAIR_TAG_RULES:
        if any(kw in lower for kw in keywords):
            return tag
    return "crypto"


class NewsIngestor:
    def __init__(self):
        self.engine = get_engine()
        self.api_key = os.getenv("CRYPTOPANIC_API_KEY", "")

    # ── Fetchers ──────────────────────────────────────────────────────────────

    def fetch_from_cryptopanic(self, pages: int = 5) -> list[dict]:
        """
        Pull headlines from CryptoPanic API.

        Args:
            pages: number of pages to fetch (50 items/page)

        Returns:
            List of dicts ready for news_headlines insertion.
        """
        if not self.api_key:
            log.warning("CRYPTOPANIC_API_KEY not set — skipping CryptoPanic fetch")
            return []

        records = []
        base_url = "https://cryptopanic.com/api/v1/posts/"

        for page in range(1, pages + 1):
            try:
                resp = requests.get(base_url, params={
                    "auth_token": self.api_key,
                    "public": "true",
                    "page": page,
                }, timeout=10)
                resp.raise_for_status()
                data = resp.json()

                for post in data.get("results", []):
                    headline = post.get("title", "").strip()
                    url      = post.get("url", "")
                    pub      = post.get("published_at", "")
                    if not headline or not pub:
                        continue

                    records.append({
                        "published_at": pub,
                        "source":       "cryptopanic",
                        "headline":     headline,
                        "url":          url,
                        "pair_tag":     classify_pair_tag(headline),
                    })

            except requests.RequestException as e:
                log.error(f"CryptoPanic API error (page {page}): {e}")
                break

        log.info(f"Fetched {len(records)} headlines from CryptoPanic")
        return records

    def fetch_from_rss(self, feed_name: str, feed_url: str) -> list[dict]:
        """
        Parse an RSS feed and return headline records.

        Args:
            feed_name: human-readable source name (stored in DB)
            feed_url:  RSS feed URL

        Returns:
            List of dicts ready for news_headlines insertion.
        """
        records = []
        try:
            resp = requests.get(feed_url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            resp.raise_for_status()
            root = ET.fromstring(resp.content)

            for item in root.iter("item"):
                title    = (item.findtext("title") or "").strip()
                link     = (item.findtext("link") or "").strip()
                pub_date = item.findtext("pubDate") or ""

                if not title:
                    continue

                try:
                    pub_dt = parsedate_to_datetime(pub_date).astimezone(timezone.utc).isoformat()
                except Exception:
                    pub_dt = datetime.now(timezone.utc).isoformat()

                records.append({
                    "published_at": pub_dt,
                    "source":       feed_name,
                    "headline":     title,
                    "url":          link,
                    "pair_tag":     classify_pair_tag(title),
                })

        except Exception as e:
            log.error(f"RSS fetch error ({feed_name}): {e}")

        log.info(f"Fetched {len(records)} headlines from {feed_name} RSS")
        return records

    # ── Insertion ─────────────────────────────────────────────────────────────

    def insert_headlines(self, records: list[dict]) -> int:
        """Insert headlines into news_headlines. ON CONFLICT on url — deduplicates."""
        inserted = 0
        with self.engine.begin() as conn:
            for rec in records:
                result = conn.execute(
                    insert(news_headlines).values(**rec)
                    .on_conflict_do_nothing(constraint="uq_news_url")
                )
                inserted += result.rowcount
        log.info(f"Inserted {inserted} new headlines ({len(records) - inserted} duplicates skipped)")
        return inserted

    # ── Main ──────────────────────────────────────────────────────────────────

    def run(self, use_cryptopanic: bool = True, use_rss: bool = True):
        """
        Full news pipeline: pull from CryptoPanic and/or RSS, insert into DB.

        Args:
            use_cryptopanic: fetch from CryptoPanic API
            use_rss:         fetch from RSS feeds (CoinDesk, CoinTelegraph)
        """
        all_records = []

        if use_cryptopanic:
            all_records.extend(self.fetch_from_cryptopanic())

        if use_rss:
            for name, url in RSS_FEEDS.items():
                all_records.extend(self.fetch_from_rss(name, url))

        if all_records:
            self.insert_headlines(all_records)
        else:
            log.info("No headlines fetched")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ingestor = NewsIngestor()
    ingestor.run()
