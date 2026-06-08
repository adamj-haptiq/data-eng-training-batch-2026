.PHONY: setup etl flow test gx dbt ci-local coverage

VENV := .venv
PYTHON := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
DBT := $(VENV)/bin/dbt

setup:
	uv venv $(VENV) --python 3.12
	$(PIP) install -r requirements.txt

etl:
	$(PYTHON) -m etl.pipeline

flow:
	$(PYTHON) flow.py

test:
	$(PYTHON) -m pytest --cov=etl --cov-report=term-missing --cov-report=xml

gx:
	$(PYTHON) great_expectations/run_checkpoint.py

DBT_DIR := dbt/dividend_project

dbt:
	cd $(DBT_DIR) && ../../$(DBT) deps --profiles-dir . && ../../$(DBT) build --profiles-dir .

coverage: test
	@echo "--- Coverage summary ---"
	@$(PYTHON) -m coverage report

ci-local: flow test
	@echo "All CI steps passed."
