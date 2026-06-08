from datetime import date

import pandas as pd

from etl.db import load_to_duckdb
from etl.gx_validation import run_validation
from etl.price_analysis import score_tickers


def test_gx_checkpoint_passes(tmp_path, monkeypatch):
    monkeypatch.setattr("etl.gx_validation.DB_PATH", tmp_path / "dev.duckdb")
    events = pd.DataFrame(
        {
            "ticker": ["TICKA"] * 4,
            "ex_dividend_date": [
                date(2024, 3, 15),
                date(2024, 6, 15),
                date(2024, 9, 15),
                date(2024, 12, 15),
            ],
            "dividend_amount": [0.65, 0.65, 0.65, 0.65],
            "close_day_before": [130.0, 131.0, 132.0, 133.0],
            "close_on_ex_div": [129.8, 130.7, 131.8, 132.7],
            "price_drop": [0.2, 0.3, 0.2, 0.3],
        }
    )
    scores = score_tickers(events, lookback_years=5)
    load_to_duckdb(events, scores, db_path=str(tmp_path / "dev.duckdb"))
    assert run_validation(tmp_path / "dev.duckdb") is True
