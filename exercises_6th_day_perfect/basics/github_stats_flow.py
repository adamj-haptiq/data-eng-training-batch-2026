from prefect import flow, task, get_run_logger
import requests

def name_github_flow():
    """Generate dynamic flow name based on username parameter"""
    from prefect.context import get_run_context

    flow_run = get_run_context().flow_run
    params = flow_run.parameters
    username = params.get("username", "unknown")

    return f"github-stats-{username}"


@task(retries=3, retry_delay_seconds=60)
def get_github_user(username: str):
    """Fetch GitHub user information"""
    logger = get_run_logger()
    logger.info(f"Fetching GitHub user info for {username}")

    response = requests.get(f"https://api.github.com/users/{username}")
    response.raise_for_status()
    return response.json()

@task(retries=3, retry_delay_seconds=30)
def get_github_repos(username: str):
    """Fetch GitHub repositories"""
    logger = get_run_logger()
    logger.info(f"Fetching repositories for {username}")

    response = requests.get(f"https://api.github.com/users/{username}/repos")
    response.raise_for_status()
    return response.json()

@task
def calculate_repo_stats(repos):
    """Calculate repository statistics"""
    logger = get_run_logger()
    logger.info("Calculating repository statistics")

    if not repos:
        return {"total_repos": 0, "total_stars": 0, "avg_size": 0}

    total_stars = sum(repo.get("stargazers_count", 0) for repo in repos)
    total_size = sum(repo.get("size", 0) for repo in repos)

    stats = {
        "total_repos": len(repos),
        "total_stars": total_stars,
        "avg_size": total_size / len(repos) if repos else 0,
        "languages": list(set(repo.get("language", "Unknown") for repo in repos if repo.get("language")))
    }

    logger.info("Repository statistics calculated", extra=stats)
    return stats

@flow(flow_run_name=name_github_flow, log_prints=True)
def github_stats_flow(username: str = "PrefectHQ"):
    """Get GitHub statistics for a user/organization"""
    logger = get_run_logger()
    logger.info(f"Starting GitHub stats collection for {username}")

    try:
        # Get user info
        user_info = get_github_user(username)
        logger.info(f"Retrieved user info for {username}")

        # Get repositories
        repos = get_github_repos(username)
        logger.info(f"Found {len(repos)} repositories")

        # Calculate stats
        stats = calculate_repo_stats(repos)
        logger.info("Repository statistics calculated", extra={"total_repos": len(repos)})

        return {
            "username": username,
            "user_info": user_info,
            "repo_stats": stats
        }

    except Exception as e:
        logger.error(f"GitHub stats collection failed: {str(e)}", extra={"username": username})
        raise

if __name__ == "__main__":
    result = github_stats_flow("PrefectHQ")
    print(f"GitHub Stats: {result['repo_stats']}")