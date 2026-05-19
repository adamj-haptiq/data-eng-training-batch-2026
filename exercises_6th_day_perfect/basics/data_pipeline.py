from prefect import flow, task, get_run_logger
import time
import random

@task(retries=2, retry_delay_seconds=1)
def fetch_data(source: str):
    """Simulate fetching data with potential failures"""
    logger = get_run_logger()
    logger.info(f"Fetching data from {source}")

    # Simulate network delay
    time.sleep(2)

    # Simulate occasional failures
    if random.random() < 0.3:  # 30% chance of failure
        raise Exception(f"Network error fetching from {source}")

    return f"Data from {source}"

@task
def process_data(data: str):
    """Process the fetched data"""
    logger = get_run_logger()
    logger.info(f"Processing: {data}")

    # Simulate processing time
    time.sleep(1)

    return f"Processed {data}"

@task
def save_data(processed_data: str, destination: str):
    """Save processed data"""
    logger = get_run_logger()
    logger.info(f"Saving {processed_data} to {destination}")

    return f"Saved to {destination}"

@flow(name="data-pipeline-{environment}")
def data_pipeline(environment: str = "local"):
    """Data pipeline that demonstrates local worker capabilities"""
    logger = get_run_logger()
    logger.info(f"Starting data pipeline in {environment} environment")

    # Fetch data from multiple sources
    sources = ["API", "Database", "File System"]
    processed_results = []

    for source in sources:
        try:
            data = fetch_data(source)
            processed = process_data(data)
            saved = save_data(processed, f"{environment}_storage")
            processed_results.append(saved)
        except Exception as e:
            logger.error(f"Failed to process {source}: {str(e)}")
            # Continue with other sources

    logger.info(f"Pipeline completed. Processed {len(processed_results)} sources")
    return processed_results

if __name__ == "__main__":
    # Run the pipeline locally
    result = data_pipeline("development")
    print(f"Pipeline result: {result}")