import requests
import time
from prefect import flow, task
from datetime import timedelta

def name_analytics_flow():
    """Generate dynamic flow name for user analytics"""
    from datetime import datetime
    return f"user-analytics-{datetime.now().strftime('%Y%m%d-%H%M')}"

@task(cache_key_fn=lambda user_id: f"user_{user_id}", cache_expiration=timedelta(minutes=30))
def fetch_user_details(user_id: int):
    """Fetch user details from JSONPlaceholder API with caching"""
    print(f"Fetching user {user_id} details...")

    response = requests.get(f"https://jsonplaceholder.typicode.com/users/{user_id}")
    response.raise_for_status()

    user_data = response.json()
    return {
        "id": user_data["id"],
        "name": user_data["name"],
        "email": user_data["email"],
        "company": user_data["company"]["name"]
    }

@flow(flow_run_name=name_analytics_flow, log_prints=True)
def user_analytics_flow(user_ids: list):
    """Analyze user data with intelligent caching"""
    print(f"Analyzing {len(user_ids)} users...")

    users = []
    for user_id in user_ids:
        user = fetch_user_details(user_id)
        users.append(user)

    # Calculate some analytics
    total_users = len(users)
    companies = set(user["company"] for user in users)

    return {
        "total_users": total_users,
        "unique_companies": len(companies),
        "companies": list(companies),
        "users": users
    }

if __name__ == "__main__":
    # Test with some repeated user IDs to see caching in action
    user_ids = [1, 2, 3, 1, 2, 4, 5, 1]  # Notice duplicates

    start_time = time.time()
    analytics = user_analytics_flow(user_ids)
    end_time = time.time()

    print(f"Analytics completed in {end_time - start_time:.2f} seconds")
    print(f"Total users: {analytics['total_users']}")
    print(f"Unique companies: {analytics['unique_companies']}")