from datetime import date

import pandas as pd

from etl.config import MIN_EVENTS_FOR_SCORING


def compute_price_drop(close_day_before: float, close_on_ex_div: float) -> float:
    return close_day_before - close_on_ex_div


def enrich_dividend_events(
    dividends_df: pd.DataFrame, prices_df: pd.DataFrame
) -> pd.DataFrame:
    """Join dividend events with close prices and compute price_drop."""
    if dividends_df.empty:
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

    prices = prices_df.copy()
    prices["date"] = pd.to_datetime(prices["date"]).dt.date
    prices = prices.sort_values(["ticker", "date"])

    records = []
    for _, row in dividends_df.iterrows():
        ticker = row["ticker"]
        ex_date = row["ex_dividend_date"]
        if isinstance(ex_date, pd.Timestamp):
            ex_date = ex_date.date()

        ticker_prices = prices[prices["ticker"] == ticker]
        if ticker_prices.empty:
            continue

        on_ex_div = ticker_prices[ticker_prices["date"] == ex_date]
        before_ex_div = ticker_prices[ticker_prices["date"] < ex_date]
        if on_ex_div.empty or before_ex_div.empty:
            continue

        close_on_ex_div = float(on_ex_div.iloc[0]["close"])
        close_day_before = float(before_ex_div.iloc[-1]["close"])
        price_drop = compute_price_drop(close_day_before, close_on_ex_div)

        records.append(
            {
                "ticker": ticker,
                "ex_dividend_date": ex_date,
                "dividend_amount": float(row["dividend_amount"]),
                "close_day_before": close_day_before,
                "close_on_ex_div": close_on_ex_div,
                "price_drop": price_drop,
            }
        )

    return pd.DataFrame(records)


def filter_favorable_events(events_df: pd.DataFrame) -> pd.DataFrame:
    """Keep events where price drop was less than the dividend paid."""
    if events_df.empty:
        return events_df
    return events_df[events_df["price_drop"] < events_df["dividend_amount"]].copy()


def filter_favorable_tickers(events_df: pd.DataFrame) -> list[str]:
    favorable = filter_favorable_events(events_df)
    if favorable.empty:
        return []
    return sorted(favorable["ticker"].unique().tolist())


def score_tickers(events_df: pd.DataFrame, lookback_years: int) -> pd.DataFrame:
    """Compute consistency ratio per ticker over the lookback window."""
    if events_df.empty:
        return pd.DataFrame(
            columns=[
                "ticker",
                "total_events",
                "favorable_events",
                "consistency_ratio",
                "lookback_years",
            ]
        )

    cutoff = date.today().replace(year=date.today().year - lookback_years)
    filtered = events_df.copy()
    filtered["ex_dividend_date"] = pd.to_datetime(
        filtered["ex_dividend_date"]
    ).dt.date
    filtered = filtered[filtered["ex_dividend_date"] >= cutoff]

    records = []
    for ticker, group in filtered.groupby("ticker"):
        total_events = len(group)
        if total_events < MIN_EVENTS_FOR_SCORING:
            continue

        favorable_events = int((group["price_drop"] < group["dividend_amount"]).sum())
        consistency_ratio = favorable_events / total_events
        records.append(
            {
                "ticker": ticker,
                "total_events": total_events,
                "favorable_events": favorable_events,
                "consistency_ratio": consistency_ratio,
                "lookback_years": lookback_years,
            }
        )

    scores = pd.DataFrame(records)
    if scores.empty:
        return scores

    return scores.sort_values(
        ["consistency_ratio", "total_events"], ascending=[False, False]
    ).reset_index(drop=True)
