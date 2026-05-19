from prefect import flow, task
import requests

@task
def fetch_user_data(user_id: int):
    """Fetch user data from API"""
    response = requests.get(f"https://jsonplaceholder.typicode.com/users/{user_id}")
    response.raise_for_status()
    return response.json()

@task
def process_user_data(user_data):
    """Process user data"""
    return {
        "id": user_data["id"],
        "name": user_data["name"],
        "email": user_data["email"],
        "status": "processed"
    }

@flow
def simple_user_flow(user_id: int):
    """Simple user processing flow"""
    user_data = fetch_user_data(user_id)
    processed = process_user_data(user_data)
    return processed

if __name__ == "__main__":
    result = simple_user_flow(1)
    print(f"Processed user: {result}")