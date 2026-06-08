import argparse
import logging

from etl.config import LOOKBACK_YEARS_INITIAL, LOOKBACK_YEARS_PATTERN
from etl.db import load_to_duckdb
from etl.nasdaq_tickers import get_sample_tickers
from etl.price_analysis import filter_favorable_tickers, score_tickers
from etl.yfinance_client import build_dividend_events_for_tickers

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    logger.info("Starting dividend analysis pipeline")

    tickers = get_sample_tickers()
    logger.info("Selected %d tickers: %s", len(tickers), ", ".join(tickers))

    initial_events = build_dividend_events_for_tickers(
        tickers, lookback_years=LOOKBACK_YEARS_INITIAL
    )
    logger.info("Loaded %d dividend events (1-year lookback)", len(initial_events))

    qualifying_tickers = filter_favorable_tickers(initial_events)
    logger.info(
        "Found %d tickers with favorable price-drop pattern: %s",
        len(qualifying_tickers),
        ", ".join(qualifying_tickers),
    )

    if not qualifying_tickers:
        logger.warning("No qualifying tickers found; loading empty scores")
        events_df = initial_events
        scores_df = score_tickers(initial_events, lookback_years=LOOKBACK_YEARS_PATTERN)
    else:
        events_df = build_dividend_events_for_tickers(
            qualifying_tickers, lookback_years=LOOKBACK_YEARS_PATTERN
        )
        scores_df = score_tickers(events_df, lookback_years=LOOKBACK_YEARS_PATTERN)
        logger.info(
            "Scored %d tickers over %d-year window",
            len(scores_df),
            LOOKBACK_YEARS_PATTERN,
        )

    events_count, scores_count = load_to_duckdb(events_df, scores_df)
    logger.info(
        "Loaded %d events and %d ticker scores into DuckDB",
        events_count,
        scores_count,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run dividend analysis ETL pipeline")
    parser.parse_args()
    run_pipeline()


if __name__ == "__main__":
    main()
