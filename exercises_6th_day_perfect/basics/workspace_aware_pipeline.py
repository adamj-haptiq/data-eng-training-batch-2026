from prefect import flow, task, get_run_logger
from prefect import runtime
import requests
import json
from datetime import datetime

@task
def fetch_environment_config():
    """Fetch configuration based on current workspace"""
    logger = get_run_logger()

    # Access runtime information to determine environment
    deployment_name = runtime.deployment.name if runtime.deployment else "local"
    flow_name = runtime.flow_run.name

    # Simulate different configurations based on environment
    if "production" in deployment_name.lower():
        config = {
            "environment": "production",
            "api_url": "https://api.production.com",
            "log_level": "WARNING",
            "retry_attempts": 5,
            "timeout": 30
        }
    elif "staging" in deployment_name.lower():
        config = {
            "environment": "staging",
            "api_url": "https://api.staging.com",
            "log_level": "INFO",
            "retry_attempts": 3,
            "timeout": 15
        }
    else:
        config = {
            "environment": "development",
            "api_url": "https://jsonplaceholder.typicode.com",  # Test API
            "log_level": "DEBUG",
            "retry_attempts": 1,
            "timeout": 5
        }

    logger.info(f"Using {config['environment']} configuration")
    return config

@task
def fetch_data_with_config(config: dict):
    """Fetch data using environment-specific configuration"""
    logger = get_run_logger()

    logger.info(f"Fetching data from {config['api_url']}")
    logger.info(f"Using {config['log_level']} logging level")

    # Simulate API call with environment-specific timeout
    response = requests.get(f"{config['api_url']}/posts", timeout=config['timeout'])
    response.raise_for_status()

    data = response.json()
    logger.info(f"Fetched {len(data)} items from {config['environment']} environment")

    return data

@task
def process_data_with_config(data: list, config: dict):
    """Process data with environment-specific logic"""
    logger = get_run_logger()

    # Different processing based on environment
    if config['environment'] == 'production':
        # Production: full processing with validation
        processed = []
        for item in data[:10]:  # Limit for demo
            processed.append({
                "id": item["id"],
                "title": item["title"],
                "body": item["body"][:100],
                "environment": config['environment'],
                "processed_at": datetime.now().isoformat()
            })
        logger.info(f"Production processing: {len(processed)} items with full validation")

    elif config['environment'] == 'staging':
        # Staging: moderate processing
        processed = []
        for item in data[:5]:  # Fewer items for staging
            processed.append({
                "id": item["id"],
                "title": item["title"],
                "environment": config['environment'],
                "processed_at": datetime.now().isoformat()
            })
        logger.info(f"Staging processing: {len(processed)} items with moderate validation")

    else:
        # Development: minimal processing
        processed = []
        for item in data[:3]:  # Minimal items for dev
            processed.append({
                "id": item["id"],
                "title": item["title"][:50],  # Truncated for dev
                "environment": config['environment'],
                "processed_at": datetime.now().isoformat()
            })
        logger.info(f"Development processing: {len(processed)} items with minimal validation")

    return processed

@flow(name="workspace-aware-pipeline-{environment}")
def workspace_aware_pipeline(environment: str = "development"):
    """Pipeline that adapts based on workspace/environment"""
    logger = get_run_logger()
    logger.info(f"Starting workspace-aware pipeline for {environment}")

    # Get environment-specific configuration
    config = fetch_environment_config()

    # Fetch data with environment-specific settings
    data = fetch_data_with_config(config)

    # Process data with environment-specific logic
    processed = process_data_with_config(data, config)

    # Log summary
    logger.info(f"Pipeline completed in {config['environment']} environment")
    logger.info(f"Processed {len(processed)} items with {config['retry_attempts']} retry attempts")

    return {
        "environment": config['environment'],
        "processed_count": len(processed),
        "config": config,
        "results": processed
    }

if __name__ == "__main__":
    # Run in development mode
    result = workspace_aware_pipeline("development")
    print(f"Development pipeline result: {result['environment']} - {result['processed_count']} items")