"""Smoke tests: imports work and basic structures load."""


def test_db_module_imports():
    from src import db  # noqa: F401


def test_ohlcv_ingestor_imports():
    from src import ohlcv_ingestor  # noqa: F401


def test_events_ingestor_imports():
    from src import events_ingestor  # noqa: F401


def test_news_ingestor_imports():
    from src import news_ingestor  # noqa: F401
