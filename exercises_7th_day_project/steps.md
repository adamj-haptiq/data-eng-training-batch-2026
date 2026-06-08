# Dividend Analysis Project — Step-by-Step Guide

## 1. Create environment

```bash
cd exercises_7th_day_project
uv venv .venv --python 3.12
source .venv/bin/activate
uv pip install -r requirements.txt
```

## 2. Create tables in the database

Tables are defined in `etl/db.py`:

- `raw_dividend_events` — dividend events with price-drop analysis
- `raw_ticker_scores` — per-ticker consistency scores

Run the Prefect flow (or ETL directly) to create and populate them:

```bash
python flow.py
# or: python -m etl.pipeline
```

## 3. Load 25 Nasdaq tickers with dividends (yfinance)

Fetches Nasdaq symbols and samples 25 dividend payers via yfinance:

```bash
python flow.py
```

## 7b. Prefect orchestration

The full pipeline is orchestrated by `flow.py`:

| Task | Steps | What it does |
|------|-------|--------------|
| `extract-and-load` | 2–6 | Python ETL, loads DuckDB raw tables |
| `validate-raw-data` | 10 | Great Expectations checkpoint on raw data |
| `transform-with-dbt` | 7, 9 | `dbt deps` + `dbt build` via `PrefectDbtRunner` |

Unit tests (steps 8 & 11) run separately via `make test`, not inside the Prefect flow.

```bash
python flow.py
```

On retry, the dbt task runs `dbt retry` instead of a full rebuild.

## 4. Build pandas DataFrame with ex_dividend and dividend_amount

See `etl/yfinance_client.py` and `etl/price_analysis.py`. Columns:

| Column | Description |
|--------|-------------|
| `ex_dividend_date` | Ex-dividend date |
| `dividend_amount` | Dividend paid per share |
| `close_day_before` | Close price day before ex-div |
| `close_on_ex_div` | Close price on ex-div date |
| `price_drop` | `close_day_before - close_on_ex_div` |

## 5. Find tickers where price drop < dividend amount

Logic in `etl/price_analysis.py` → `filter_favorable_tickers()`.

A favorable event means the stock dropped **less** than the dividend paid.

## 6. Check 5-year consistency pattern

For qualifying tickers, reload 5 years of history and compute:

```
consistency_ratio = favorable_events / total_events
```

Requires at least 3 events per ticker. See `score_tickers()` in `etl/price_analysis.py`.

## 7. dbt model — top 2 best tickers

```bash
cd dbt/dividend_project
dbt deps --profiles-dir .
dbt build --profiles-dir .
```

Models:

| Model | Layer | Purpose |
|-------|-------|---------|
| `stg_dividend_events` | staging | Clean raw events, cap to 5 years |
| `stg_ticker_scores` | staging | Ticker consistency scores |
| `int_ex_dividend_analysis` | intermediate | Add `is_favorable` flag |
| `mart_best_dividend_tickers` | mart | Top 2 tickers + dividend history |

## 8. Unit tests (pytest)

```bash
pytest --cov=etl --cov-report=term-missing
```

Tests live in `tests/unit/` and `tests/integration/`. No network calls — yfinance is mocked.

## 9. dbt_expectations tests

Configured in `dbt/dividend_project/models/schema.yml` and `packages.yml`.

Examples:

- `expect_column_values_to_not_be_null`
- `expect_compound_columns_to_be_unique`
- `expect_column_values_to_be_between`
- `expect_table_row_count_to_be_between`

Run via `dbt build`.

## 10. Great Expectations tests

```bash
python great_expectations/run_checkpoint.py
```

Validates `raw_dividend_events` via Pandas DataFrame (GX does not support DuckDB SQL natively).

Expectation suite: `great_expectations/expectations/raw_dividend_events.json`

## 11. Test coverage

```bash
pytest --cov=etl --cov-report=xml --cov-report=term-missing
python -m coverage report
```

Target: **≥80%** line coverage on `etl/` (enforced in `pyproject.toml`).

| Layer | Tool | What it measures |
|-------|------|------------------|
| Python ETL | pytest-cov | Line coverage of `etl/` |
| dbt models | dbt build | Data test pass/fail |
| Raw data | GX checkpoint | Expectation pass/fail |

## 12. CI/CD pipeline

### GitHub Actions (remote)

Workflow: `.github/workflows/dividend-analysis.yml`

Triggers on push/PR when files under `exercises_7th_day_project/` change.

| Job | Steps | What it validates |
|-----|-------|-------------------|
| `test` | pytest + coverage | Unit tests, ≥80% coverage on `etl/` |
| `pipeline` | `python flow.py` | ETL → GX → dbt (dbt_expectations included) |

Coverage artifact: `coverage.xml` (uploaded from the `test` job).

### Local

Run the same checks locally:

```bash
make ci-local   # make flow + make test
```

This runs the Prefect flow (ETL → GX → dbt), then pytest with coverage as a separate step. Coverage is written to `coverage.xml`.
