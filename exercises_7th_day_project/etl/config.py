from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DB_PATH = DATA_DIR / "dev.duckdb"

RANDOM_SEED = 42
TICKER_SAMPLE_SIZE = 25
MIN_EVENTS_FOR_SCORING = 3
LOOKBACK_YEARS_INITIAL = 1
LOOKBACK_YEARS_PATTERN = 5

NASDAQ_SYMBOLS_URL = (
    "https://api.nasdaq.com/api/screener/stocks"
    "?tableonly=true&limit=10000&exchange=nasdaq"
)


def current_year() -> int:
    return date.today().year
