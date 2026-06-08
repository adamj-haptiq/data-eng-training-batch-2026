from datetime import date

import pandas as pd
import pytest

from etl.price_analysis import (
    compute_price_drop,
    enrich_dividend_events,
    filter_favorable_events,
    filter_favorable_tickers,
    score_tickers,
)


def _sample_scored_events() -> pd.DataFrame:
    return pd.DataFrame(
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


def test_compute_price_drop():
    assert compute_price_drop(100.0, 99.5) == 0.5


def test_enrich_dividend_events(sample_dividends_df, sample_prices_df):
    result = enrich_dividend_events(sample_dividends_df, sample_prices_df)
    assert len(result) == 3
    assert "price_drop" in result.columns
    assert result.iloc[0]["price_drop"] == pytest.approx(0.2)


def test_filter_favorable_events():
    events = pd.DataFrame(
        {
            "ticker": ["A", "B"],
            "price_drop": [0.1, 0.5],
            "dividend_amount": [0.5, 0.3],
        }
    )
    favorable = filter_favorable_events(events)
    assert len(favorable) == 1
    assert favorable.iloc[0]["ticker"] == "A"


def test_filter_favorable_tickers():
    events = pd.DataFrame(
        {
            "ticker": ["A", "A", "B"],
            "price_drop": [0.1, 0.2, 0.5],
            "dividend_amount": [0.5, 0.5, 0.3],
        }
    )
    tickers = filter_favorable_tickers(events)
    assert tickers == ["A"]


def test_score_tickers_ranks_by_consistency():
    events = _sample_scored_events()
    scores = score_tickers(events, lookback_years=5)
    assert len(scores) >= 2
    assert scores.iloc[0]["ticker"] == "TICKA"
    assert scores.iloc[0]["consistency_ratio"] == 1.0
    assert scores.iloc[0]["total_events"] >= 3


def test_score_tickers_excludes_insufficient_events():
    events = pd.DataFrame(
        {
            "ticker": ["X"],
            "ex_dividend_date": [date(2024, 1, 1)],
            "dividend_amount": [0.5],
            "price_drop": [0.1],
        }
    )
    scores = score_tickers(events, lookback_years=5)
    assert scores.empty
