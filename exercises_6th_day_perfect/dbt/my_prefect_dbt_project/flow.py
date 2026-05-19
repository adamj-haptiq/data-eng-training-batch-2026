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