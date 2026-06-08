from unittest.mock import patch

from etl.nasdaq_tickers import (
    fetch_nasdaq_symbols,
    filter_dividend_payers,
    get_sample_tickers,
)


def test_fetch_nasdaq_symbols_mocked():
    mock_response = {
        "data": {
            "table": {
                "rows": [
                    {"symbol": "AAPL"},
                    {"symbol": "MSFT"},
                    {"symbol": "BRK/A"},
                ]
            }
        }
    }
    with patch("etl.nasdaq_tickers.requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_response
        mock_get.return_value.raise_for_status = lambda: None
        symbols = fetch_nasdaq_symbols()

    assert "AAPL" in symbols
    assert "MSFT" in symbols
    assert "BRK/A" not in symbols


def test_filter_dividend_payers_mocked():
    with patch(
        "etl.nasdaq_tickers.ticker_paid_dividend_last_year",
        side_effect=lambda s, year=None: s in ("AAPL", "MSFT"),
    ):
        result = filter_dividend_payers(["AAPL", "GOOG", "MSFT", "TSLA"])

    assert result == ["AAPL", "MSFT"]


def test_filter_dividend_payers_skips_errors():
    def side_effect(symbol, year=None):
        if symbol == "BAD":
            raise ValueError("API error")
        return symbol == "GOOD"

    with patch(
        "etl.nasdaq_tickers.ticker_paid_dividend_last_year",
        side_effect=side_effect,
    ):
        result = filter_dividend_payers(["BAD", "GOOD"])

    assert result == ["GOOD"]


def test_get_sample_tickers_mocked():
    with patch(
        "etl.nasdaq_tickers.fetch_nasdaq_symbols",
        return_value=[f"SYM{i}" for i in range(50)],
    ):
        with patch(
            "etl.nasdaq_tickers.filter_dividend_payers",
            return_value=[f"SYM{i}" for i in range(30)],
        ):
            tickers = get_sample_tickers(sample_size=25, seed=42)

    assert len(tickers) == 25


def test_get_sample_tickers_raises_when_insufficient():
    with patch("etl.nasdaq_tickers.fetch_nasdaq_symbols", return_value=["A"]):
        with patch("etl.nasdaq_tickers.filter_dividend_payers", return_value=["A"]):
            try:
                get_sample_tickers(sample_size=25)
                assert False, "Expected RuntimeError"
            except RuntimeError as exc:
                assert "Found only 1" in str(exc)
