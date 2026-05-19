from prefect import flow, task, get_run_logger
import requests

@task
def fetch_user_data(user_id: int):
    """Fetch user data with error handling"""
    logger = get_run_logger()

    try:
        response = requests.get(f"https://jsonplaceholder.typicode.com/users/{user_id}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch user {user_id}: {str(e)}")
        return None

@task
def process_user_data(user_data):
    """Process user data with validation"""
    logger = get_run_logger()

    if not user_data:
        logger.warning("No user data to process")
        return None

    return {
        "id": user_data["id"],
        "name": user_data["name"],
        "email": user_data["email"],
        "status": "processed"
    }

@task
def handle_missing_user(user_id: int):
    """Handle case when user doesn't exist"""
    logger = get_run_logger()
    logger.warning(f"User {user_id} not found, creating placeholder")
    return {
        "id": user_id,
        "name": "Unknown User",
        "email": "unknown@example.com",
        "status": "placeholder"
    }

@flow(name="resilient-user-flow-{user_id}")
def resilient_user_flow(user_id: int):
    """Resilient user processing with conditional logic"""
    logger = get_run_logger()
    logger.info(f"Processing user {user_id}")

    # Fetch user data
    user_data = fetch_user_data(user_id)

    # Conditional processing based on data availability
    if user_data:
        processed = process_user_data(user_data)
        logger.info(f"Successfully processed user {user_id}")
    else:
        processed = handle_missing_user(user_id)
        logger.warning(f"Used placeholder for user {user_id}")

    return processed

if __name__ == "__main__":
    # Test with valid user
    result1 = resilient_user_flow(1)
    print(f"Valid user: {result1}")

    # Test with invalid user (should trigger conditional logic)
    result2 = resilient_user_flow(999)
    print(f"Invalid user: {result2}")