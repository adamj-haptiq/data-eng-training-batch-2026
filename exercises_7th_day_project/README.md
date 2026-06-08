# Dividend Analysis Project (Day 7)

## Architecture

```
Prefect flow (data pipeline)
    ├── extract-and-load      (yfinance → DuckDB)
    ├── validate-raw-data     (Great Expectations)
    └── transform-with-dbt    (dbt build + dbt_expectations)

pytest (separate)             (unit tests + coverage)
```

## Quickstart

```bash
cd exercises_7th_day_project
uv venv .venv --python 3.12
source .venv/bin/activate
uv pip install -r requirements.txt

# Run data pipeline via Prefect
make flow

# Run everything locally (pipeline + tests)
make ci-local
```

## CI/CD

GitHub Actions workflow [`.github/workflows/dividend-analysis.yml`](../.github/workflows/dividend-analysis.yml) runs on push/PR when `exercises_7th_day_project/` changes:

| Job | What it runs |
|-----|--------------|
| **test** | pytest + coverage (≥80% on `etl/`), uploads `coverage.xml` |
| **pipeline** | Prefect flow: ETL → Great Expectations → dbt |

## Project Structure

| Path | Purpose |
|------|---------|
| `flow.py` | Prefect flow: ETL → GX → dbt |
| `etl/` | Python ETL: fetch tickers, load dividends, score patterns |
| `dbt/dividend_project/` | dbt models and data tests |
| `great_expectations/` | GX expectation suite and CLI checkpoint |
| `tests/` | pytest unit and integration tests |

See [steps.md](steps.md) for the full walkthrough.
