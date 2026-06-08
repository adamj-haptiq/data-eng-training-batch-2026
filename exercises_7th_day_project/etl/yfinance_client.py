from datetime import date, timedelta

import pandas as pd
import yfinance as yf

from etl.config import current_year


def get_dividends_for_ticker(
    ticker: str, start_year: int | None = None, end_year: int | None = None
) -> pd.DataFrame:
    """Fetch dividend history for a ticker within a year range."""
    start_year = start_year or current_year() - 1
    end_year = end_year or current_year()

    start = date(start_year, 1, 1)
    end = date(end_year, 12, 31)

    dividends = yf.Ticker(ticker).dividends
    if dividends is None or dividends.empty:
        return pd.DataFrame(columns=["ticker", "ex_dividend_date", "dividend_amount"])

    df = dividends.reset_index()
    df.columns = ["ex_dividend_date", "dividend_amount"]
    df["ex_dividend_date"] = pd.to_datetime(df["ex_dividend_date"]).dt.tz_localize(None)
    df = df[
        (df["ex_dividend_date"].dt.date >= start)
        & (df["ex_dividend_date"].dt.date <= end)
    ]
    df["ticker"] = ticker
    return df[["ticker", "ex_dividend_date", "dividend_amount"]]


def get_price_history(
    ticker: str, start_date: date, end_date: date
) -> pd.DataFrame:
    """Fetch daily close prices for a ticker."""
    history = yf.Ticker(ticker).history(
        start=start_date.isoformat(),
        end=(end_date + timedelta(days=1)).isoformat(),
        auto_adjust=False,
    )
    if history.empty:
        return pd.DataFrame(columns=["ticker", "date", "close"])

    df = history.reset_index()
    date_col = "Date" if "Date" in df.columns else df.columns[0]
    df["date"] = pd.to_datetime(df[date_col]).dt.tz_localize(None)
    if "close" not in df.columns:
        df["close"] = df["Close"] if "Close" in df.columns else df.iloc[:, -1]
    df["ticker"] = ticker
    return df[["ticker", "date", "close"]]


def ticker_paid_dividend_last_year(ticker: str, year: int | None = None) -> bool:
    year = year or current_year() - 1
    dividends = get_dividends_for_ticker(ticker, start_year=year, end_year=year)
    return not dividends.empty


def build_dividend_events_for_tickers(
    tickers: list[str], lookback_years: int
) -> pd.DataFrame:
    """Build enriched dividend events for a list of tickers."""
    end_year = current_year()
    start_year = end_year - lookback_years
    start_date = date(start_year, 1, 1)
    end_date = date(end_year, 12, 31)

    all_dividends = []
    all_prices = []

    for ticker in tickers:
        dividends = get_dividends_for_ticker(ticker, start_year, end_year)
        if dividends.empty:
            continue
        all_dividends.append(dividends)

        prices = get_price_history(ticker, start_date, end_date)
        if not prices.empty:
            all_prices.append(prices)

    if not all_dividends:
        return pd.DataFrame(
            columns=[
                "ticker",
                "ex_dividend_date",
                "dividend_amount",
                "close_day_before",
                "close_on_ex_div",
                "price_drop",
            ]
        )

    dividends_df = pd.concat(all_dividends, ignore_index=True)
    prices_df = (
        pd.concat(all_prices, ignore_index=True)
        if all_prices
        else pd.DataFrame(columns=["ticker", "date", "close"])
    )

    from etl.price_analysis import enrich_dividend_events

    return enrich_dividend_events(dividends_df, prices_df)
