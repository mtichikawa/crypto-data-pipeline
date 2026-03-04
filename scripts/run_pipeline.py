"""
scripts/run_pipeline.py — Full pipeline runner.

Usage:
    python scripts/run_pipeline.py              # OHLCV + events + news
    python scripts/run_pipeline.py --ohlcv-only
    python scripts/run_pipeline.py --news-only
    python scripts/run_pipeline.py --lookback 180
"""

import argparse
import logging
import sys

from src.ohlcv_ingestor import OHLCVIngestor
from src.events_ingestor import EventsIngestor
from src.news_ingestor import NewsIngestor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="crypto-data-pipeline runner")
    parser.add_argument("--ohlcv-only",  action="store_true", help="Run OHLCV ingestion only")
    parser.add_argument("--events-only", action="store_true", help="Run events ingestion only")
    parser.add_argument("--news-only",   action="store_true", help="Run news ingestion only")
    parser.add_argument("--lookback",    type=int, default=90,
                        help="Days of OHLCV history to fetch on first run (default: 90)")
    return parser.parse_args()


def main():
    args = parse_args()

    run_ohlcv  = args.ohlcv_only  or not (args.events_only or args.news_only)
    run_events = args.events_only or not (args.ohlcv_only  or args.news_only)
    run_news   = args.news_only   or not (args.ohlcv_only  or args.events_only)

    log.info("=" * 60)
    log.info("crypto-data-pipeline")
    log.info(f"  OHLCV:  {'yes' if run_ohlcv else 'skip'}")
    log.info(f"  Events: {'yes' if run_events else 'skip'}")
    log.info(f"  News:   {'yes' if run_news else 'skip'}")
    log.info("=" * 60)

    if run_ohlcv:
        log.info("--- OHLCV ingestion ---")
        OHLCVIngestor().run(lookback_days=args.lookback)

    if run_events:
        log.info("--- Events ingestion ---")
        EventsIngestor().run()

    if run_news:
        log.info("--- News ingestion ---")
        NewsIngestor().run()

    log.info("Pipeline complete.")


if __name__ == "__main__":
    main()
# reviewed
