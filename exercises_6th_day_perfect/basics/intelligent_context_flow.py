from prefect import flow, task, get_run_logger
from prefect import runtime
import requests
import json
from datetime import datetime

@task
def intelligent_data_fetch(source: str, limit: int = None):
    """Intelligently fetch data based on runtime context"""
    logger = get_run_logger()

    # Access runtime information for intelligent decisions
    flow_name = runtime.flow_run.name
    flow_parameters = runtime.flow_run.parameters
    deployment_name = runtime.deployment.name if runtime.deployment else "local"

    # Determine fetch strategy based on context
    if deployment_name == "production":
        # Production: fetch more data, be more thorough
        actual_limit = limit or 100
        logger.info(f"Production mode: fetching up to {actual_limit} {source} items")
    elif "test" in flow_name.lower():
        # Test mode: fetch minimal data for speed
        actual_limit = min(limit or 10, 10)
        logger.info(f"Test mode: fetching {actual_limit} {source} items for speed")
    else:
        # Development mode: moderate amount
        actual_limit = limit or 50
        logger.info(f"Development mode: fetching {actual_limit} {source} items")

    # Fetch data with limit
    url = f"https://jsonplaceholder.typicode.com/{source}"
    if actual_limit:
        url += f"?_limit={actual_limit}"

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Add context metadata to each item
    for item in data:
        item["_runtime_metadata"] = {
            "fetched_by": runtime.task_run.name,
            "flow_name": flow_name,
            "deployment": deployment_name,
            "fetch_timestamp": datetime.now().isoformat()
        }

    return data

@task
def adaptive_processing(data, data_type: str):
    """Adapt processing based on runtime context"""
    logger = get_run_logger()

    # Access runtime information
    flow_name = runtime.flow_run.name
    deployment_name = runtime.deployment.name if runtime.deployment else "local"

    # Determine processing strategy based on context
    if deployment_name == "production":
        # Production: full processing with validation
        logger.info("Production processing: full validation and enrichment")
        processed = []
        for item in data:
            processed.append({
                "id": item["id"],
                "title": item.get("title", item.get("name", "Unknown")),
                "body": item.get("body", item.get("email", "")),
                "processed_by": runtime.task_run.name,
                "processing_mode": "production",
                "validation_status": "validated",
                "runtime_metadata": item.get("_runtime_metadata", {})
            })
    else:
        # Non-production: simplified processing
        logger.info("Non-production processing: simplified for speed")
        processed = []
        for item in data:
            processed.append({
                "id": item["id"],
                "title": item.get("title", item.get("name", "Unknown")),
                "processed_by": runtime.task_run.name,
                "processing_mode": "development",
                "runtime_metadata": item.get("_runtime_metadata", {})
            })

    return processed

@task
def context_aware_save(processed_data, data_type: str):
    """Save data with context-aware naming and metadata"""
    logger = get_run_logger()

    # Access runtime information
    flow_name = runtime.flow_run.name
    flow_id = runtime.flow_run.id
    deployment_name = runtime.deployment.name if runtime.deployment else "local"

    # Create context-aware filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{data_type}_{deployment_name}_{flow_name}_{timestamp}.json"

    # Create comprehensive metadata
    metadata = {
        "processing_summary": {
            "data_type": data_type,
            "processed_count": len(processed_data),
            "flow_name": flow_name,
            "flow_id": str(flow_id),
            "deployment_name": deployment_name,
            "processing_timestamp": datetime.now().isoformat()
        },
        "runtime_context": {
            "flow_parameters": runtime.flow_run.parameters,
            "flow_run_name": runtime.flow_run.name,
            "deployment_id": str(runtime.deployment.id) if runtime.deployment else None
        },
        "data": processed_data
    }

    with open(filename, 'w') as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Saved {len(processed_data)} {data_type} items to {filename}")
    logger.info(f"Deployment: {deployment_name}, Flow: {flow_name}")

    return filename

@flow(name="intelligent-context-flow-{data_type}")
def intelligent_context_flow(data_type: str, limit: int = None):
    """Intelligent workflow that adapts based on runtime context"""
    logger = get_run_logger()

    # Access and log runtime context
    flow_name = runtime.flow_run.name
    flow_id = runtime.flow_run.id
    deployment_name = runtime.deployment.name if runtime.deployment else "local"

    logger.info(f"Starting intelligent flow '{flow_name}' (ID: {flow_id})")
    logger.info(f"Deployment context: {deployment_name}")
    logger.info(f"Flow parameters: {runtime.flow_run.parameters}")

    # Intelligently fetch data based on context
    data = intelligent_data_fetch(data_type, limit)

    # Adapt processing based on context
    processed = adaptive_processing(data, data_type)

    # Save with context-aware metadata
    filename = context_aware_save(processed, data_type)

    return {
        "data_type": data_type,
        "processed_count": len(processed),
        "output_file": filename,
        "flow_name": flow_name,
        "deployment": deployment_name,
        "processing_mode": "production" if deployment_name == "production" else "development"
    }

if __name__ == "__main__":
    # Test with different contexts
    result = intelligent_context_flow("posts", limit=20)
    print(f"Intelligent processing complete: {result}")