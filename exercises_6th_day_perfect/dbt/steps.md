1. create env
$ uv venv dbt-example --python 3.12
$ . dbt-example/bin/activate
$ uv pip install "dbt-core==1.10.2" "dbt-duckdb>=1.9.4,<2.0.0" "prefect-dbt>=0.7.0" tzdata

2. create project
$ dbt init --profiles-dir . my_prefect_dbt_project

3. fix a default model that references a non existing seed
$ cd my_prefect_dbt_project
$ cat > models/example/my_first_dbt_model.sql << 'EOF'
{{
    config(
        materialized='table'
    )
}}

with source_data as (
    select 1 as id
    union all
    select 2 as id
)

select *
from source_data
where id is not null
EOF
cat models/example/my_first_dbt_model.sql

4. create requirements
$ echo "prefect-dbt>=0.7.0" > requirements.txt
$ echo "dbt-duckdb>=1.9.4,<2.0.0" >> requirements.txt
$ cat requirements.txt

5. create flow
$ touch flow.py
from prefect import flow
from prefect.runtime.flow_run import get_run_count
from prefect_dbt import PrefectDbtRunner

@flow(
    description=(
        "Runs commands dbt deps then dbt build by default. "
        "Runs dbt retry if the flow is retrying."
    ),
    retries=2,
)
def dbt_flow(commands: list[str] | None = None):
    if commands is None:
        commands = ["deps", "build"]

    runner = PrefectDbtRunner(
        include_compiled_code=True,
    )

    if get_run_count() == 1:
        for command in commands:
            runner.invoke(command.split(" "))
    else:
        runner.invoke(["retry"])

if __name__ == "__main__":
    dbt_flow()

6. run flow
$ uv run flow.py
