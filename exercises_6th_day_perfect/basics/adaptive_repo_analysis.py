from prefect import flow, task, get_run_logger
import requests

@task
def fetch_repository_info(owner: str, repo: str):
    """Fetch repository information"""
    logger = get_run_logger()

    try:
        response = requests.get(f"https://api.github.com/repos/{owner}/{repo}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch {owner}/{repo}: {str(e)}")
        return None

@task
def analyze_repository(repo_data):
    """Analyze repository based on its characteristics"""
    logger = get_run_logger()

    if not repo_data:
        return {"status": "not_found", "action": "skip"}

    stars = repo_data.get("stargazers_count", 0)
    forks = repo_data.get("forks_count", 0)
    language = repo_data.get("language", "Unknown")

    # Conditional analysis based on repository characteristics
    if stars > 1000:
        return {
            "status": "popular",
            "action": "detailed_analysis",
            "priority": "high",
            "stars": stars,
            "forks": forks,
            "language": language
        }
    elif stars > 100:
        return {
            "status": "moderate",
            "action": "standard_analysis",
            "priority": "medium",
            "stars": stars,
            "forks": forks,
            "language": language
        }
    else:
        return {
            "status": "new",
            "action": "basic_analysis",
            "priority": "low",
            "stars": stars,
            "forks": forks,
            "language": language
        }

@task
def detailed_analysis(repo_data):
    """Perform detailed analysis for popular repositories"""
    logger = get_run_logger()
    logger.info("Performing detailed analysis for popular repository")

    return {
        "analysis_type": "detailed",
        "recommendations": [
            "Monitor for security updates",
            "Consider enterprise features",
            "Track community engagement"
        ]
    }

@task
def standard_analysis(repo_data):
    """Perform standard analysis for moderate repositories"""
    logger = get_run_logger()
    logger.info("Performing standard analysis for moderate repository")

    return {
        "analysis_type": "standard",
        "recommendations": [
            "Monitor growth trends",
            "Check documentation quality"
        ]
    }

@task
def basic_analysis(repo_data):
    """Perform basic analysis for new repositories"""
    logger = get_run_logger()
    logger.info("Performing basic analysis for new repository")

    return {
        "analysis_type": "basic",
        "recommendations": [
            "Focus on initial development",
            "Build community presence"
        ]
    }

@flow(name="adaptive-repo-analysis-{owner}-{repo}")
def adaptive_repo_analysis(owner: str, repo: str):
    """Adaptive repository analysis with conditional logic"""
    logger = get_run_logger()
    logger.info(f"Starting adaptive analysis for {owner}/{repo}")

    # Fetch repository data
    repo_data = fetch_repository_info(owner, repo)

    # Analyze repository characteristics
    analysis_plan = analyze_repository(repo_data)

    # Execute different analysis based on repository status
    if analysis_plan["action"] == "detailed_analysis":
        result = detailed_analysis(repo_data)
    elif analysis_plan["action"] == "standard_analysis":
        result = standard_analysis(repo_data)
    elif analysis_plan["action"] == "basic_analysis":
        result = basic_analysis(repo_data)
    else:
        logger.warning(f"Skipping analysis for {owner}/{repo}")
        result = {"analysis_type": "skipped", "reason": "repository_not_found"}

    return {
        "repository": f"{owner}/{repo}",
        "analysis_plan": analysis_plan,
        "result": result
    }

if __name__ == "__main__":
    # Test with different types of repositories
    repos = [
        ("PrefectHQ", "prefect"),  # Popular
        ("pandas-dev", "pandas"),  # Popular
        ("your-username", "test-repo"),  # Likely not found
    ]

    for owner, repo in repos:
        result = adaptive_repo_analysis(owner, repo)
        print(f"\n{owner}/{repo}: {result['analysis_plan']['status']} - {result['result']['analysis_type']}")