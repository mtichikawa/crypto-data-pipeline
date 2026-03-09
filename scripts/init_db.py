"""
scripts/init_db.py — Create all tables in PostgreSQL.

Run once before the first pipeline execution:
    python scripts/init_db.py

Safe to re-run — uses CREATE TABLE IF NOT EXISTS semantics via SQLAlchemy's
checkfirst=True. Will not drop or alter existing tables.
"""

import logging
from src.db import get_engine, metadata

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def main():
    engine = get_engine()
    log.info(f"Connecting to: {engine.url}")
    metadata.create_all(engine, checkfirst=True)
    log.info("Tables created (or already exist): ohlcv, market_events, news_headlines")


if __name__ == "__main__":
    main()
# note: see README for usage examples
