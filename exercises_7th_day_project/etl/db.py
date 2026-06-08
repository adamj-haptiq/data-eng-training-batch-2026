from datetime import datetime, timezone

import duckdb
import pandas as pd

from etl.config import DATA_DIR, DB_PATH

CREATE_RAW_DIVIDEND_EVENTS = """
CREATE TABLE IF NOT EXISTS raw_dividend_events (
    ticker VARCHAR,
    ex_dividend_date DATE,
    dividend_amount DOUBLE,
    close_day_before DOUBLE,
    close_on_ex_div DOUBLE,
    price_drop DOUBLE,
    loaded_at TIMESTAMP
);
"""

CREATE_RAW_TICKER_SCORES = """
CREATE TABLE IF NOT EXISTS raw_ticker_scores (
    ticker VARCHAR PRIMARY KEY,
    total_events INTEGER,
    favorable_events INTEGER,
    consistency_ratio DOUBLE,
    lookback_years INTEGER,
    loaded_at TIMESTAMP
);
"""


def get_connection(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path or DB_PATH))


def create_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(CREATE_RAW_DIVIDEND_EVENTS)
    conn.execute(CREATE_RAW_TICKER_SCORES)


def clear_tables(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute("DELETE FROM raw_dividend_events")
    conn.execute("DELETE FROM raw_ticker_scores")


def load_dividend_events(
    conn: duckdb.DuckDBPyConnection, events_df: pd.DataFrame
) -> int:
    if events_df.empty:
        return 0

    df = events_df.copy()
    df["loaded_at"] = datetime.now(timezone.utc)
    conn.execute("DELETE FROM raw_dividend_events")
    conn.register("_events_df", df)
    conn.execute("""
        INSERT INTO raw_dividend_events
        SELECT
            ticker,
            ex_dividend_date,
            dividend_amount,
            close_day_before,
            close_on_ex_div,
            price_drop,
            loaded_at
        FROM _events_df
    """)
    conn.unregister("_events_df")
    return len(df)


def load_ticker_scores(
    conn: duckdb.DuckDBPyConnection, scores_df: pd.DataFrame
) -> int:
    if scores_df.empty:
        return 0

    df = scores_df.copy()
    df["loaded_at"] = datetime.now(timezone.utc)
    conn.execute("DELETE FROM raw_ticker_scores")
    conn.register("_scores_df", df)
    conn.execute("""
        INSERT INTO raw_ticker_scores
        SELECT
            ticker,
            total_events,
            favorable_events,
            consistency_ratio,
            lookback_years,
            loaded_at
        FROM _scores_df
    """)
    conn.unregister("_scores_df")
    return len(df)


def load_to_duckdb(
    events_df: pd.DataFrame,
    scores_df: pd.DataFrame,
    db_path: str | None = None,
) -> tuple[int, int]:
    conn = get_connection(db_path)
    try:
        create_tables(conn)
        events_count = load_dividend_events(conn, events_df)
        scores_count = load_ticker_scores(conn, scores_df)
        return events_count, scores_count
    finally:
        conn.close()
