from prefect import flow, task
import requests

@task
def fetch_data(source: str):
    """Fetch data from API"""
    response = requests.get(f"https://jsonplaceholder.typicode.com/{source}")
    response.raise_for_status()
    return response.json()

@task
def process_data(data, data_type: str):
    """Process data"""
    return [{"id": item["id"], "title": item.get("title", item.get("name", "Unknown"))} for item in data]

@flow
def basic_data_flow(data_type: str):
    """Basic data processing flow"""
    data = fetch_data(data_type)
    processed = process_data(data, data_type)
    return processed

if __name__ == "__main__":
    result = basic_data_flow("users")
    print(f"Processed {len(result)} items")