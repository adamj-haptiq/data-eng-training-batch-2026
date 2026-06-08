from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd

from etl.yfinance_client import (
    get_dividends_for_ticker,
    get_price_history,
    ticker_paid_dividend_last_year,
)


def test_get_dividends_for_ticker_mocked():
    mock_dividends = pd.Series(
        [0.5, 0.5],
        index=pd.to_datetime(["2024-03-15", "2024-09-15"]),
    )
    mock_ticker = MagicMock()
    mock_ticker.dividends = mock_dividends

    with patch("etl.yfinance_client.yf.Ticker", return_value=mock_ticker):
        result = get_dividends_for_ticker("TEST", start_year=2024, end_year=2024)

    assert len(result) == 2
    assert result.iloc[0]["ticker"] == "TEST"
    assert result.iloc[0]["dividend_amount"] == 0.5


def test_get_price_history_mocked():
    mock_history = pd.DataFrame(
        {"Close": [100.0, 99.5]},
        index=pd.to_datetime(["2024-03-14", "2024-03-15"]),
    )
    mock_ticker = MagicMock()
    mock_ticker.history.return_value = mock_history

    with patch("etl.yfinance_client.yf.Ticker", return_value=mock_ticker):
        result = get_price_history("TEST", date(2024, 3, 1), date(2024, 3, 31))

    assert len(result) == 2
    assert "close" in result.columns


def test_ticker_paid_dividend_last_year_mocked():
    with patch(
        "etl.yfinance_client.get_dividends_for_ticker",
        return_value=pd.DataFrame({"ticker": ["TEST"]}),
    ):
        assert ticker_paid_dividend_last_year("TEST") is True

    with patch(
        "etl.yfinance_client.get_dividends_for_ticker",
        return_value=pd.DataFrame(),
    ):
        assert ticker_paid_dividend_last_year("TEST") is False
