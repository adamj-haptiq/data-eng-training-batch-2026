import logging
from unittest.mock import MagicMock, patch

import pytest

from flow import dividend_analysis_flow, extract_and_load, transform_with_dbt, validate_raw_data


def test_extract_and_load_calls_pipeline():
    with patch("flow.get_run_logger", return_value=logging.getLogger("test")):
        with patch("flow.run_pipeline") as mock_pipeline:
            extract_and_load.fn()
    mock_pipeline.assert_called_once_with()


def test_validate_raw_data_raises_on_failure():
    with patch("flow.get_run_logger", return_value=logging.getLogger("test")):
        with patch("flow.run_validation", return_value=False):
            with pytest.raises(RuntimeError, match="Great Expectations validation failed"):
                validate_raw_data.fn()


def test_validate_raw_data_passes():
    with patch("flow.get_run_logger", return_value=logging.getLogger("test")):
        with patch("flow.run_validation", return_value=True):
            assert validate_raw_data.fn() is True


def test_transform_with_dbt_invokes_runner():
    mock_runner = MagicMock()
    with patch("flow.PrefectDbtRunner", return_value=mock_runner):
        with patch("flow.get_run_count", return_value=1):
            transform_with_dbt.fn()

    assert mock_runner.invoke.call_count == 2
    mock_runner.invoke.assert_any_call(["deps"])
    mock_runner.invoke.assert_any_call(["build"])


def test_transform_with_dbt_retries_on_rerun():
    mock_runner = MagicMock()
    with patch("flow.PrefectDbtRunner", return_value=mock_runner):
        with patch("flow.get_run_count", return_value=2):
            transform_with_dbt.fn()

    mock_runner.invoke.assert_called_once_with(["retry"])


def test_dividend_analysis_flow_orchestrates_tasks():
    with patch("flow.extract_and_load") as mock_etl:
        with patch("flow.validate_raw_data") as mock_gx:
            with patch("flow.transform_with_dbt") as mock_dbt:
                dividend_analysis_flow()

    mock_etl.assert_called_once_with()
    mock_gx.assert_called_once_with()
    mock_dbt.assert_called_once_with()
