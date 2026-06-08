from datetime import date
from unittest.mock import patch

import pandas as pd

from etl.db import get_connection
from etl.pipeline import run_pipeline


def test_pipeline_with_mocked_data(tmp_path, monkeypatch):
    monkeypatch.setattr("etl.db.DB_PATH", tmp_path / "dev.duckdb")
    monkeypatch.setattr("etl.db.DATA_DIR", tmp_path)

    events = pd.DataFrame(
        {
            "ticker": ["TICKA"] * 4 + ["TICKB"] * 3,
            "ex_dividend_date": [
                date(2024, 3, 15),
                date(2024, 6, 15),
                date(2024, 9, 15),
                date(2024, 12, 15),
                date(2024, 4, 1),
                date(2024, 7, 1),
                date(2024, 10, 1),
            ],
            "dividend_amount": [0.65, 0.65, 0.65, 0.65, 0.55, 0.55, 0.55],
            "close_day_before": [130.0] * 7,
            "close_on_ex_div": [129.8] * 7,
            "price_drop": [0.2, 0.3, 0.25, 0.15, 0.2, 0.1, 0.3],
        }
    )

    with patch("etl.pipeline.get_sample_tickers", return_value=["TICKA", "TICKB"]):
        with patch(
            "etl.pipeline.build_dividend_events_for_tickers",
            return_value=events,
        ):
            run_pipeline()

    conn = get_connection()
    try:
        event_count = conn.execute("SELECT COUNT(*) FROM raw_dividend_events").fetchone()[0]
        score_count = conn.execute("SELECT COUNT(*) FROM raw_ticker_scores").fetchone()[0]
        assert event_count > 0
        assert score_count >= 2

        top_tickers = conn.execute("""
            SELECT ticker FROM raw_ticker_scores
            ORDER BY consistency_ratio DESC, total_events DESC
            LIMIT 2
        """).fetchall()
        assert len(top_tickers) == 2
        assert top_tickers[0][0] == "TICKA"
    finally:
        conn.close()
