import random

import requests

from etl.config import (
    NASDAQ_SYMBOLS_URL,
    RANDOM_SEED,
    TICKER_SAMPLE_SIZE,
    current_year,
)
from etl.yfinance_client import ticker_paid_dividend_last_year


def fetch_nasdaq_symbols() -> list[str]:
    """Fetch Nasdaq-listed stock symbols from the Nasdaq API."""
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
    }
    response = requests.get(NASDAQ_SYMBOLS_URL, headers=headers, timeout=30)
    response.raise_for_status()
    data = response.json()
    table = data.get("data", {}).get("table", {})
    rows = table.get("rows", data.get("data", {}).get("rows", []))
    symbols = []
    for row in rows:
        symbol = row.get("symbol", "")
        if symbol and "^" not in symbol and "/" not in symbol:
            symbols.append(symbol.upper())
    return symbols


def filter_dividend_payers(
    symbols: list[str],
    year: int | None = None,
    max_candidates: int | None = None,
) -> list[str]:
    """Return symbols that paid at least one dividend in the given year."""
    year = year or current_year() - 1
    candidates = symbols[:max_candidates] if max_candidates else symbols
    dividend_payers = []
    for symbol in candidates:
        try:
            if ticker_paid_dividend_last_year(symbol, year=year):
                dividend_payers.append(symbol)
        except Exception:
            continue
    return dividend_payers


def sample_tickers(
    symbols: list[str],
    sample_size: int = TICKER_SAMPLE_SIZE,
    seed: int = RANDOM_SEED,
) -> list[str]:
    if len(symbols) <= sample_size:
        return sorted(symbols)
    rng = random.Random(seed)
    return sorted(rng.sample(symbols, sample_size))


def get_sample_tickers(
    sample_size: int = TICKER_SAMPLE_SIZE,
    seed: int = RANDOM_SEED,
) -> list[str]:
    """Get 25 random Nasdaq tickers that paid dividends last year."""
    nasdaq_symbols = fetch_nasdaq_symbols()
    dividend_payers = filter_dividend_payers(nasdaq_symbols, max_candidates=200)
    if len(dividend_payers) < sample_size:
        raise RuntimeError(
            f"Found only {len(dividend_payers)} dividend payers; need {sample_size}"
        )
    return sample_tickers(dividend_payers, sample_size=sample_size, seed=seed)
