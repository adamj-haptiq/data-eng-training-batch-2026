"""Prefect flow orchestrating the dividend analysis data pipeline."""

import os

from prefect import flow, get_run_logger, task
from prefect.runtime.flow_run import get_run_count
from prefect_dbt import PrefectDbtRunner
from prefect_dbt.core.settings import PrefectDbtSettings

from etl.config import DB_PATH, PROJECT_ROOT
from etl.gx_validation import run_validation
from etl.pipeline import run_pipeline

DBT_DIR = PROJECT_ROOT / "dbt" / "dividend_project"


@task(name="extract-and-load", retries=2, retry_delay_seconds=5)
def extract_and_load() -> None:
    """Run the Python ETL and load raw tables into DuckDB."""
    logger = get_run_logger()
    logger.info("Running ETL pipeline")
    run_pipeline()


@task(name="validate-raw-data", retries=1)
def validate_raw_data() -> bool:
    """Validate raw_dividend_events with Great Expectations."""
    logger = get_run_logger()
    success = run_validation()
    if not success:
        raise RuntimeError("Great Expectations validation failed")
    logger.info("Great Expectations validation passed")
    return success


@task(name="transform-with-dbt", retries=2, retry_delay_seconds=5)
def transform_with_dbt(commands: list[str] | None = None) -> None:
    """Run dbt deps/build (or retry on flow rerun)."""
    if commands is None:
        commands = ["deps", "build"]

    os.environ["DUCKDB_PATH"] = str(DB_PATH.resolve())

    settings = PrefectDbtSettings(
        project_dir=DBT_DIR,
        profiles_dir=DBT_DIR,
    )
    runner = PrefectDbtRunner(
        settings=settings,
        include_compiled_code=True,
    )

    if get_run_count() == 1:
        for command in commands:
            runner.invoke(command.split())
    else:
        runner.invoke(["retry"])


@flow(
    name="dividend-analysis-pipeline",
    description=(
        "Extract Nasdaq dividend data, validate with Great Expectations, "
        "and transform with dbt to find the best dividend tickers."
    ),
    retries=1,
)
def dividend_analysis_flow() -> None:
    extract_and_load()
    validate_raw_data()
    transform_with_dbt()


if __name__ == "__main__":
    dividend_analysis_flow()
