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

@flow
def monolithic_etl():
    """Monolithic ETL workflow"""
    # Process users
    user_data = fetch_data("users")
    processed_users = process_data(user_data, "users")
    save_data(processed_users, "users.json")

    # Process posts
    post_data = fetch_data("posts")
    processed_posts = process_data(post_data, "posts")
    save_data(processed_posts, "posts.json")

    return "ETL completed"

if __name__ == "__main__":
    result = monolithic_etl()
    print(result)