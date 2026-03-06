# crypto-data-pipeline

A production-grade market data pipeline that powers the [trading signal engine](https://github.com/mtichikawa/trading-signal-engine) (T3) and [backtester](https://github.com/mtichikawa/trading-backtester) (T4) in a 5-repo LLM trading system.

## Overview

This pipeline pulls live OHLCV data from Kraken via `ccxt`, ingests economic calendar events with volatility tagging, and collects crypto news headlines for downstream FinBERT text signal generation. All data lands in PostgreSQL and is queryable by the rest of the trading arc.

The system is designed for research and paper trading — not live execution.

> **Exchange note:** Kraken is used instead of Binance due to Binance's US geo-restrictions.

---

## Architecture

```
Kraken API (ccxt)
      │
      ▼
  ohlcv table          ← raw OHLCV candles, multi-pair, multi-timeframe
      │                   near_event / event_type / mins_from_event tagged here
      │
  market_events table  ← economic calendar (CPI, NFP, FOMC, etc.)
      │
  news_headlines table ← crypto/macro headlines for T3 FinBERT path

      ▼
PostgreSQL (local)
      │
      ├──► T2: trading-chart-generator  (reads ohlcv + market_events)
      └──► T3: trading-signal-engine    (reads ohlcv + market_events + news_headlines)
```

---

## Database Schema

### `ohlcv`
Raw candlestick data from Kraken.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | |
| pair | VARCHAR | e.g. `BTC/USD` |
| timeframe | VARCHAR | `5m`, `15m`, `1h`, `4h`, `1d` |
| open_time | TIMESTAMPTZ | candle open time (UTC) |
| open | NUMERIC | |
| high | NUMERIC | |
| low | NUMERIC | |
| close | NUMERIC | |
| volume | NUMERIC | |
| near_event | BOOLEAN | True if candle falls within ±2 candles of a high-impact macro event |
| event_type | VARCHAR | e.g. `CPI`, `NFP`, `FOMC` (set when near_event=True) |
| mins_from_event | INTEGER | signed minutes from nearest event (negative = before) |

Unique constraint on `(pair, timeframe, open_time)` — safe to re-run ingestion.

### `market_events`
Economic calendar events.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | |
| event_time | TIMESTAMPTZ | scheduled release time (UTC) |
| event_name | VARCHAR | e.g. `CPI`, `NFP`, `ISM` |
| impact | VARCHAR | `high`, `medium`, `low` |
| actual | VARCHAR | reported value (nullable until released) |
| forecast | VARCHAR | consensus estimate |
| previous | VARCHAR | prior period value |

Unique constraint on `(event_time, event_name)`.

### `news_headlines`
Crypto and macro news headlines for the T3 FinBERT text signal path.

| Column | Type | Description |
|--------|------|-------------|
| id | SERIAL PK | |
| published_at | TIMESTAMPTZ | article publish time (UTC) |
| source | VARCHAR | e.g. `coindesk`, `cointelegraph` |
| headline | TEXT | article title |
| url | TEXT | source URL |
| pair_tag | VARCHAR | `BTC`, `ETH`, `SOL`, `crypto`, `macro` |

---

## Pairs and Timeframes

| Pairs | Timeframes |
|-------|------------|
| BTC/USD | 5m, 15m, 1h, 4h, 1d |
| ETH/USD | 5m, 15m, 1h, 4h, 1d |
| SOL/USD | 5m, 15m, 1h, 4h, 1d |

---

## Data Sources

| Data | Source | Method |
|------|--------|--------|
| OHLCV candles | Kraken | `ccxt` library |
| Economic calendar | CryptoPanic API / manual seed JSON | REST API + JSON |
| News headlines | CryptoPanic API, CoinDesk RSS, CoinTelegraph RSS | REST / RSS ingestion |

---

## Project Structure

```
crypto-data-pipeline/
├── src/
│   ├── __init__.py
│   ├── db.py              # SQLAlchemy engine + table definitions
│   ├── ohlcv_ingestor.py  # Kraken OHLCV fetcher via ccxt (incremental)
│   ├── events_ingestor.py # Economic calendar ingestion + near_event tagging
│   └── news_ingestor.py   # Headline ingestion from CryptoPanic / RSS
├── scripts/
│   ├── init_db.py         # Create tables (run once)
│   └── run_pipeline.py    # Full pipeline: OHLCV + events + news
├── notebooks/
│   └── pipeline_exploration.ipynb
├── tests/
│   └── __init__.py
├── data/                  # gitignored
├── .env                   # DB credentials + API keys (gitignored)
├── .env.example
├── requirements.txt
└── README.md
```

---

## Setup

### 1. Clone and install

```bash
git clone https://github.com/mtichikawa/crypto-data-pipeline.git
cd crypto-data-pipeline
pip install -r requirements.txt
```

### 2. Configure environment

```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials and API keys
```

### 3. Initialize the database

```bash
python scripts/init_db.py
```

### 4. Run the pipeline

```bash
# Full run: OHLCV + events + news
python scripts/run_pipeline.py

# OHLCV only
python scripts/run_pipeline.py --ohlcv-only
```

---

## Trading System Arc

This repo is T1 in a 5-part series:

| Repo | Role |
|------|------|
| **T1 · crypto-data-pipeline** (this repo) | Market data foundation |
| T2 · trading-chart-generator | Annotated candlestick chart images |
| T3 · trading-signal-engine | LLM vision + FinBERT dual-path signals |
| T4 · trading-backtester | Signal backtesting with vectorbt |
| T5 · trading-dashboard | Live paper P&L dashboard |

---

## Contact

Mike Ichikawa — projects.ichikawa@gmail.com · [mtichikawa.github.io](https://mtichikawa.github.io)
