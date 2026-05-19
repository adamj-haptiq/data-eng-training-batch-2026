from prefect import flow, get_run_logger, tags

@flow
def hello(name: str = "Marvin"):
    get_run_logger().info(f"Hello, {name}!")

if __name__ == "__main__":
    # Run the flow
    hello()  # Output: "Hello, Marvin!"

    # Run the flow with a different argument
    hello("Arthur")  # Output: "Hello, Arthur!"

    # Run the flow with a "local" tag for organization
    with tags("local"):
        hello()