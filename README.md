# crypto-data-pipeline · T1

Live market data foundation for the T1–T5 paper trading arc. Pulls OHLCV candles from Kraken via ccxt, tags macro market events, and logs news headlines for downstream FinBERT sentiment analysis. All data lands in PostgreSQL and is consumed by T2 (chart generation) and T3 (signal engine).

Designed for research and paper trading — not live execution.

---

## Trading Arc

| Repo | Role | Status |
|------|------|--------|
| **T1 · crypto-data-pipeline** | Live OHLCV ingestion · market event tagging | Shipped Mar 6 |
| T2 · trading-chart-generator | Candlestick PNGs + JSON sidecars · 25/25 tests | Shipped Mar 10 |
| T3 · trading-signal-engine | Technical indicators + FinBERT sentiment · 51/51 tests | Shipped Mar 16 |
| T4 · trading-backtester | Backtesting + parameter sweep · 72/72 tests | Shipped Mar 26 |
| T5 · trading-dashboard | Streamlit oversight UI · 8/8 tests | Shipped Mar 31 · [Live Demo](https://mtichikawa-trading.streamlit.app) |

> Exchange note: Kraken is used instead of Binance due to Binance's US geo-restrictions.

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
  news_headlines table ← crypto/macro headlines for T3 FinBERT sentiment path
      │
      ▼
PostgreSQL (local)
      │
      ├──► T2: trading-chart-generator  (reads ohlcv)
      └──► T3: trading-signal-engine    (reads ohlcv + news_headlines)
```

---

## Database Schema

### `ohlcv`

| Column | Type | Description |
|--------|------|-------------|
| pair | VARCHAR | e.g. `BTC/USD` |
| timeframe | VARCHAR | `5m`, `15m`, `1h`, `4h`, `1d` |
| open_time | TIMESTAMPTZ | candle open time (UTC) |
| open / high / low / close / volume | NUMERIC | OHLCV values |
| near_event | BOOLEAN | True if within ±2 candles of a macro event |
| event_type | VARCHAR | `CPI`, `NFP`, `FOMC`, etc. |
| mins_from_event | INTEGER | signed minutes from nearest event |

Unique constraint on `(pair, timeframe, open_time)` — safe to re-run.

### `market_events`

| Column | Type | Description |
|--------|------|-------------|
| event_time | TIMESTAMPTZ | scheduled release time (UTC) |
| event_name | VARCHAR | `CPI`, `NFP`, `ISM`, etc. |
| impact | VARCHAR | `high`, `medium`, `low` |
| actual / forecast / previous | VARCHAR | values (nullable until released) |

### `news_headlines`

| Column | Type | Description |
|--------|------|-------------|
| published_at | TIMESTAMPTZ | article publish time (UTC) |
| source | VARCHAR | `coindesk`, `cointelegraph`, etc. |
| headline | TEXT | article title |
| pair_tag | VARCHAR | `BTC`, `ETH`, `SOL`, `crypto`, `macro` |

---

## Pairs and Timeframes

| Pairs | Timeframes |
|-------|------------|
| BTC/USD | 5m, 15m, 1h, 4h, 1d |
| ETH/USD | 5m, 15m, 1h, 4h, 1d |
| SOL/USD | 5m, 15m, 1h, 4h, 1d |

---

## Project Structure

```
crypto-data-pipeline/
├── src/
│   ├── db.py              # SQLAlchemy engine + table definitions
│   ├── ohlcv_ingestor.py  # Kraken OHLCV fetcher via ccxt (incremental)
│   ├── events_ingestor.py # Economic calendar ingestion + near_event tagging
│   └── news_ingestor.py   # Headline ingestion from CryptoPanic / RSS
├── scripts/
│   ├── init_db.py         # Create tables (run once)
│   └── run_pipeline.py    # Full pipeline: OHLCV + events + news
├── notebooks/
│   └── pipeline_exploration.ipynb
├── requirements.txt
└── .env.example
```

---

## Setup

```bash
git clone https://github.com/mtichikawa/crypto-data-pipeline.git
cd crypto-data-pipeline
pip install -r requirements.txt
cp .env.example .env
# Edit .env with PostgreSQL credentials
python scripts/init_db.py
```

## Run

```bash
# Full pipeline: OHLCV + events + news
python scripts/run_pipeline.py

# OHLCV only
python scripts/run_pipeline.py --ohlcv-only
```

---

## Contact

Mike Ichikawa · [projects.ichikawa@gmail.com](mailto:projects.ichikawa@gmail.com) · [mtichikawa.github.io](https://mtichikawa.github.io)
