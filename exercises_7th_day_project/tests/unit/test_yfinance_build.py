from unittest.mock import patch

import pandas as pd
import pytest

from etl.yfinance_client import build_dividend_events_for_tickers


def test_build_dividend_events_for_tickers_mocked():
    dividends = pd.DataFrame(
        {
            "ticker": ["TEST"],
            "ex_dividend_date": [pd.Timestamp("2024-03-15")],
            "dividend_amount": [0.5],
        }
    )
    prices = pd.DataFrame(
        {
            "ticker": ["TEST", "TEST"],
            "date": [pd.Timestamp("2024-03-14"), pd.Timestamp("2024-03-15")],
            "close": [100.0, 99.6],
        }
    )

    with patch(
        "etl.yfinance_client.get_dividends_for_ticker",
        return_value=dividends,
    ):
        with patch(
            "etl.yfinance_client.get_price_history",
            return_value=prices,
        ):
            result = build_dividend_events_for_tickers(["TEST"], lookback_years=1)

    assert len(result) == 1
    assert result.iloc[0]["price_drop"] == pytest.approx(0.4)


def test_build_dividend_events_empty_tickers():
    result = build_dividend_events_for_tickers([], lookback_years=1)
    assert result.empty
