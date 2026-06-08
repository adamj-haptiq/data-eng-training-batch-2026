import pandas as pd

from etl.db import create_tables, get_connection, load_to_duckdb


def test_create_tables_and_load(temp_db_path):
    events_df = pd.DataFrame(
        {
            "ticker": ["TICKA"],
            "ex_dividend_date": ["2024-03-15"],
            "dividend_amount": [0.65],
            "close_day_before": [130.0],
            "close_on_ex_div": [129.8],
            "price_drop": [0.2],
        }
    )
    scores_df = pd.DataFrame(
        {
            "ticker": ["TICKA"],
            "total_events": [8],
            "favorable_events": [8],
            "consistency_ratio": [1.0],
            "lookback_years": [5],
        }
    )

    events_count, scores_count = load_to_duckdb(
        events_df, scores_df, db_path=temp_db_path
    )
    assert events_count == 1
    assert scores_count == 1

    conn = get_connection(temp_db_path)
    try:
        events = conn.execute("SELECT COUNT(*) FROM raw_dividend_events").fetchone()[0]
        scores = conn.execute("SELECT COUNT(*) FROM raw_ticker_scores").fetchone()[0]
        assert events == 1
        assert scores == 1
    finally:
        conn.close()


def test_create_tables_idempotent(temp_db_path):
    conn = get_connection(temp_db_path)
    try:
        create_tables(conn)
        create_tables(conn)
        tables = conn.execute("SHOW TABLES").fetchall()
        table_names = {t[0] for t in tables}
        assert "raw_dividend_events" in table_names
        assert "raw_ticker_scores" in table_names
    finally:
        conn.close()
