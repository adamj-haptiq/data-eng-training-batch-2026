from prefect import flow, task, get_run_logger
from prefect import runtime
import requests
import json
from datetime import datetime

@task
def fetch_data_with_context(source: str):
    """Fetch data with runtime context awareness"""
    logger = get_run_logger()

    # Access runtime information
    flow_name = runtime.flow_run.name
    task_name = runtime.task_run.name
    flow_parameters = runtime.flow_run.parameters

    logger.info(f"Fetching {source} data in flow '{flow_name}' with task '{task_name}'")
    logger.info(f"Flow parameters: {flow_parameters}")

    response = requests.get(f"https://jsonplaceholder.typicode.com/{source}")
    response.raise_for_status()
    return response.json()

@task
def process_data_with_context(data, data_type: str):
    """Process data with runtime context awareness"""
    logger = get_run_logger()

    # Access runtime information
    task_name = runtime.task_run.name
    flow_name = runtime.flow_run.name

    logger.info(f"Processing {len(data)} {data_type} items in task '{task_name}'")

    processed = []
    for i, item in enumerate(data):
        processed.append({
            "id": item["id"],
            "title": item.get("title", item.get("name", "Unknown")),
            "processed_by": task_name,
            "flow_name": flow_name,
            "processing_order": i + 1
        })

    return processed

@task
def save_results_with_context(processed_data, data_type: str):
    """Save results with runtime context metadata"""
    logger = get_run_logger()

    # Access runtime information
    flow_name = runtime.flow_run.name
    flow_id = runtime.flow_run.id
    deployment_name = runtime.deployment.name if runtime.deployment else "local"

    # Add runtime metadata to results
    results_with_metadata = {
        "data_type": data_type,
        "processed_count": len(processed_data),
        "flow_name": flow_name,
        "flow_id": str(flow_id),
        "deployment_name": deployment_name,
        "processing_timestamp": datetime.now().isoformat(),
        "data": processed_data
    }

    filename = f"{data_type}_{flow_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    with open(filename, 'w') as f:
        json.dump(results_with_metadata, f, indent=2)

    logger.info(f"Saved {len(processed_data)} {data_type} items to {filename}")
    return filename

@flow(name="context-aware-data-flow-{data_type}")
def context_aware_data_flow(data_type: str):
    """Context-aware data processing flow"""
    logger = get_run_logger()

    # Access flow-level runtime information
    flow_name = runtime.flow_run.name
    flow_id = runtime.flow_run.id
    deployment_name = runtime.deployment.name if runtime.deployment else "local"

    logger.info(f"Starting context-aware flow '{flow_name}' (ID: {flow_id})")
    logger.info(f"Deployment: {deployment_name}")
    logger.info(f"Processing data type: {data_type}")

    # Fetch data
    data = fetch_data_with_context(data_type)

    # Process data
    processed = process_data_with_context(data, data_type)

    # Save results
    filename = save_results_with_context(processed, data_type)

    return {
        "data_type": data_type,
        "processed_count": len(processed),
        "output_file": filename,
        "flow_name": flow_name,
        "flow_id": str(flow_id)
    }

if __name__ == "__main__":
    result = context_aware_data_flow("posts")
    print(f"Context-aware processing complete: {result}")