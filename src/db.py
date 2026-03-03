"""
src/db.py — SQLAlchemy engine and table definitions.

Run scripts/init_db.py to create tables. This module is imported by all
ingestors to get a shared engine and table references.
"""

import os
from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, MetaData, Table, Column,
    Integer, String, Numeric, Boolean, Text,
    UniqueConstraint, TIMESTAMP
)

load_dotenv()


def get_engine():
    """Build SQLAlchemy engine from environment variables."""
    host     = os.getenv("DB_HOST", "localhost")
    port     = os.getenv("DB_PORT", "5432")
    dbname   = os.getenv("DB_NAME", "crypto_pipeline")
    user     = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url, pool_pre_ping=True)


metadata = MetaData()

ohlcv = Table(
    "ohlcv", metadata,
    Column("id",        Integer, primary_key=True, autoincrement=True),
    Column("pair",      String(20),  nullable=False),
    Column("timeframe", String(10),  nullable=False),
    Column("timestamp", TIMESTAMP(timezone=True), nullable=False),
    Column("open",      Numeric(20, 8), nullable=False),
    Column("high",      Numeric(20, 8), nullable=False),
    Column("low",       Numeric(20, 8), nullable=False),
    Column("close",     Numeric(20, 8), nullable=False),
    Column("volume",    Numeric(30, 8), nullable=False),
    UniqueConstraint("pair", "timeframe", "timestamp", name="uq_ohlcv"),
)

market_events = Table(
    "market_events", metadata,
    Column("id",         Integer, primary_key=True, autoincrement=True),
    Column("event_time", TIMESTAMP(timezone=True), nullable=False),
    Column("event_name", String(100), nullable=False),
    Column("impact",     String(10),  nullable=False),   # high / medium / low
    Column("actual",     String(50),  nullable=True),
    Column("forecast",   String(50),  nullable=True),
    Column("previous",   String(50),  nullable=True),
    Column("near_event", Boolean,     default=False),
    UniqueConstraint("event_time", "event_name", name="uq_market_events"),
)

news_headlines = Table(
    "news_headlines", metadata,
    Column("id",           Integer, primary_key=True, autoincrement=True),
    Column("published_at", TIMESTAMP(timezone=True), nullable=False),
    Column("source",       String(100), nullable=False),
    Column("headline",     Text,        nullable=False),
    Column("url",          Text,        nullable=True),
    Column("pair_tag",     String(20),  nullable=True),  # BTC / ETH / SOL / crypto / macro
    UniqueConstraint("url", name="uq_news_url"),
)
