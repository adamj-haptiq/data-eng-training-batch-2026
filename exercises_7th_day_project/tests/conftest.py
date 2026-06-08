from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from etl.config import PROJECT_ROOT


@pytest.fixture
def sample_dividends_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": ["TICKA", "TICKA", "TICKB"],
            "ex_dividend_date": [date(2024, 3, 15), date(2024, 9, 15), date(2024, 6, 1)],
            "dividend_amount": [0.65, 0.65, 0.55],
        }
    )


@pytest.fixture
def sample_prices_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "ticker": ["TICKA", "TICKA", "TICKA", "TICKA", "TICKB", "TICKB", "TICKB"],
            "date": [
                date(2024, 3, 14),
                date(2024, 3, 15),
                date(2024, 9, 13),
                date(2024, 9, 15),
                date(2024, 5, 31),
                date(2024, 6, 1),
                date(2024, 11, 29),
            ],
            "close": [130.0, 129.8, 135.0, 134.7, 92.0, 91.8, 94.0],
        }
    )


@pytest.fixture
def temp_db_path(tmp_path) -> str:
    return str(tmp_path / "test.duckdb")


@pytest.fixture
def project_root() -> Path:
    return PROJECT_ROOT
