import requests
import time
from prefect import flow, task

def name_sequential_flow():
    """Generate dynamic flow name for sequential analysis"""
    from datetime import datetime
    return f"sequential-repo-analysis-{datetime.now().strftime('%H%M%S')}"

@task
def fetch_github_repo_info(owner: str, repo: str):
    """Fetch GitHub repository information"""
    print(f"Fetching {owner}/{repo}...")

    response = requests.get(f"https://api.github.com/repos/{owner}/{repo}")
    response.raise_for_status()

    repo_data = response.json()
    return {
        "name": repo_data["name"],
        "stars": repo_data["stargazers_count"],
        "forks": repo_data["forks_count"],
        "language": repo_data["language"],
        "size": repo_data["size"]
    }

@flow(flow_run_name=name_sequential_flow, log_prints=True)
def sequential_repo_analysis(repos: list):
    """Analyze repositories sequentially (slow)"""
    results = []
    for owner, repo in repos:
        repo_info = fetch_github_repo_info(owner, repo)
        results.append(repo_info)

    return results

if __name__ == "__main__":
    repos = [
        ("PrefectHQ", "prefect"),
        ("pandas-dev", "pandas"),
        ("numpy", "numpy"),
        ("scikit-learn", "scikit-learn"),
        ("matplotlib", "matplotlib")
    ]

    start_time = time.time()
    results = sequential_repo_analysis(repos)
    end_time = time.time()

    print(f"Sequential analysis completed in {end_time - start_time:.2f} seconds")
    for repo in results:
        print(f"{repo['name']}: {repo['stars']} stars, {repo['language']}")