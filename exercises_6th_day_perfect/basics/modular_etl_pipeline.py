from prefect import flow, task
import requests
import json

@task
def fetch_data(source: str):
    """Fetch data from different sources"""
    if source == "users":
        response = requests.get("https://jsonplaceholder.typicode.com/users")
    elif source == "posts":
        response = requests.get("https://jsonplaceholder.typicode.com/posts")
    else:
        raise ValueError(f"Unknown source: {source}")

    response.raise_for_status()
    return response.json()

@task
def process_data(data, data_type: str):
    """Process different types of data"""
    if data_type == "users":
        return [{"id": item["id"], "name": item["name"], "email": item["email"]} for item in data]
    elif data_type == "posts":
        return [{"id": item["id"], "title": item["title"], "body": item["body"][:100]} for item in data]
    else:
        raise ValueError(f"Unknown data type: {data_type}")

@task
def save_data(data, filename: str):
    """Save data to file"""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    return f"Saved {len(data)} records to {filename}"

@flow(name="etl-{data_type}")
def etl_subflow(data_type: str, source: str, filename: str):
    """Reusable ETL subflow for any data type"""
    # Extract
    raw_data = fetch_data(source)

    # Transform
    processed_data = process_data(raw_data, data_type)

    # Load
    result = save_data(processed_data, filename)

    return {
        "data_type": data_type,
        "records_processed": len(processed_data),
        "filename": filename,
        "status": "completed"
    }

@flow(name="modular-etl-pipeline")
def modular_etl_pipeline():
    """Modular ETL pipeline using subflows"""
    # Process users using subflow
    user_result = etl_subflow("users", "users", "users.json")

    # Process posts using subflow
    post_result = etl_subflow("posts", "posts", "posts.json")

    return {
        "user_etl": user_result,
        "post_etl": post_result,
        "overall_status": "completed"
    }

if __name__ == "__main__":
    result = modular_etl_pipeline()
    print(f"ETL Pipeline Results: {result}")