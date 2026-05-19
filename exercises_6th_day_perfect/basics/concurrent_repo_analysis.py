import asyncio
import time
import aiohttp
from prefect import flow, task

def name_concurrent_flow():
    """Generate dynamic flow name for concurrent analysis"""
    from datetime import datetime
    return f"concurrent-repo-analysis-{datetime.now().strftime('%H%M%S')}"

@task
async def fetch_github_repo_info(owner: str, repo: str):
    """Fetch GitHub repository information asynchronously"""
    print(f"Fetching {owner}/{repo}...")

    async with aiohttp.ClientSession() as session:
        async with session.get(f"https://api.github.com/repos/{owner}/{repo}") as response:
            response.raise_for_status()
            repo_data = await response.json()

    return {
        "name": repo_data["name"],
        "stars": repo_data["stargazers_count"],
        "forks": repo_data["forks_count"],
        "language": repo_data["language"],
        "size": repo_data["size"]
    }

@flow(flow_run_name=name_concurrent_flow, log_prints=True)
async def concurrent_repo_analysis(repos: list):
    """Analyze repositories concurrently (fast!)"""
    # Create tasks for all repositories
    tasks = [fetch_github_repo_info(owner, repo) for owner, repo in repos]

    # Run all tasks concurrently
    results = await asyncio.gather(*tasks)

    return results

if __name__ == "__main__":
    import aiohttp

    repos = [
        ("PrefectHQ", "prefect"),
        ("pandas-dev", "pandas"),
        ("numpy", "numpy"),
        ("scikit-learn", "scikit-learn"),
        ("matplotlib", "matplotlib")
    ]

    start_time = time.time()
    results = asyncio.run(concurrent_repo_analysis(repos))
    end_time = time.time()

    print(f"Concurrent analysis completed in {end_time - start_time:.2f} seconds")
    for repo in results:
        print(f"{repo['name']}: {repo['stars']} stars, {repo['language']}")