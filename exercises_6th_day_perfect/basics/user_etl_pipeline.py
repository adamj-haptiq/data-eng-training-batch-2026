from prefect import flow, task
import pandas as pd
import requests

def name_etl_flow():
    """Generate dynamic flow name based on parameters"""
    from prefect.context import get_run_context

    flow_run = get_run_context().flow_run
    params = flow_run.parameters
    environment = params.get("environment", "dev")

    return f"user-etl-{environment}"

@flow(flow_run_name=name_etl_flow)
def user_etl_pipeline(environment: str = "dev"):
    """ETL pipeline for user data from JSONPlaceholder API"""

    # Extract users
    users = fetch_users()

    # Transform data
    processed_users = process_user_data(users)

    # Load data
    result = save_user_data(processed_users, environment)

    return result

@task(name="fetch-users-from-api")
def fetch_users():
    """Fetch users from JSONPlaceholder API"""
    response = requests.get("https://jsonplaceholder.typicode.com/users")
    response.raise_for_status()
    return response.json()

@task(name="process-user-data")
def process_user_data(users):
    """Transform user data into a clean format"""
    processed = []
    for user in users:
        processed.append({
            "id": user["id"],
            "name": user["name"],
            "email": user["email"],
            "city": user["address"]["city"],
            "company": user["company"]["name"]
        })
    return processed

@task(name="save-user-data")
def save_user_data(users, destination: str):
    """Save user data to CSV"""
    df = pd.DataFrame(users)
    filename = f"users_{destination}.csv"
    df.to_csv(filename, index=False)
    return f"Saved {len(users)} users to {filename}"

if __name__ == "__main__":
    result = user_etl_pipeline("production")
    print(f"ETL Result: {result}")