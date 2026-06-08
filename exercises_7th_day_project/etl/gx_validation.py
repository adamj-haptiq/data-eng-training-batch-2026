"""Great Expectations validation for raw_dividend_events."""

import json
from pathlib import Path

import duckdb
import great_expectations as gx
import pandas as pd

from etl.config import DB_PATH, PROJECT_ROOT

GX_ROOT = PROJECT_ROOT / "great_expectations"
SUITE_PATH = GX_ROOT / "expectations" / "raw_dividend_events.json"

EXPECTATION_TYPE_MAP = {
    "expect_table_columns_to_match_ordered_list": (
        gx.expectations.ExpectTableColumnsToMatchOrderedList
    ),
    "expect_column_values_to_not_be_null": gx.expectations.ExpectColumnValuesToNotBeNull,
    "expect_column_values_to_be_between": gx.expectations.ExpectColumnValuesToBeBetween,
    "expect_compound_columns_to_be_unique": (
        gx.expectations.ExpectCompoundColumnsToBeUnique
    ),
}


def load_dividend_events(db_path: Path | None = None) -> pd.DataFrame:
    path = db_path or DB_PATH
    if not path.exists():
        raise FileNotFoundError(
            f"DuckDB not found at {path}. Run the ETL pipeline first."
        )
    conn = duckdb.connect(str(path), read_only=True)
    try:
        return conn.execute("SELECT * FROM raw_dividend_events").fetchdf()
    finally:
        conn.close()


def build_expectation_suite(context, suite_name: str):
    with open(SUITE_PATH) as f:
        suite_data = json.load(f)

    suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
    for exp in suite_data["expectations"]:
        exp_type = exp["type"]
        exp_class = EXPECTATION_TYPE_MAP.get(exp_type)
        if exp_class is None:
            raise ValueError(f"Unsupported expectation type: {exp_type}")

        kwargs = exp["kwargs"].copy()
        if exp_type == "expect_column_values_to_be_between" and kwargs.pop(
            "strict_min", False
        ):
            kwargs["strict_min"] = True

        suite.add_expectation(exp_class(**kwargs))

    return suite


def run_validation(db_path: Path | None = None) -> bool:
    df = load_dividend_events(db_path)
    if df.empty:
        return False

    context = gx.get_context(mode="ephemeral")
    suite = build_expectation_suite(context, "raw_dividend_events")

    datasource = context.data_sources.add_pandas(name="duckdb_pandas")
    asset = datasource.add_dataframe_asset(name="raw_dividend_events")
    batch_def = asset.add_batch_definition_whole_dataframe("whole")
    batch_request = batch_def.build_batch_request(batch_parameters={"dataframe": df})

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )
    results = validator.validate()
    return results.success
