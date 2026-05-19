import asyncio
import time
import aiohttp
from prefect import flow, task
from datetime import timedelta

def name_combined_flow():
    """Generate dynamic flow name for combined analysis"""
    from datetime import datetime
    return f"combined-repo-analysis-{datetime.now().strftime('%Y%m%d-%H%M')}"

@task(cache_key_fn=lambda owner, repo: f"repo_{owner}_{repo}", cache_expiration=timedelta(hours=2))
async def fetch_github_repo_info(owner: str, repo: str):
    """Fetch GitHub repository information with caching and async"""
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

@flow(flow_run_name=name_combined_flow, log_prints=True)
async def combined_repo_analysis(repos: list):
    """Repository analysis with caching and concurrency"""
    # Create tasks for all repositories
    tasks = [fetch_github_repo_info(owner, repo) for owner, repo in repos]

    # Run all tasks concurrently (cached ones will be instant)
    results = await asyncio.gather(*tasks)

    # Calculate some analytics
    total_stars = sum(repo["stars"] for repo in results)
    languages = set(repo["language"] for repo in results if repo["language"])

    return {
        "repos": results,
        "total_stars": total_stars,
        "languages": list(languages),
        "count": len(results)
    }

if __name__ == "__main__":
    import aiohttp

    repos = [
        ("PrefectHQ", "prefect"),
        ("pandas-dev", "pandas"),
        ("numpy", "numpy"),
        ("scikit-learn", "scikit-learn"),
        ("matplotlib", "matplotlib"),
        ("PrefectHQ", "prefect"),  # Duplicate to test caching
        ("pandas-dev", "pandas")   # Duplicate to test caching
    ]

    start_time = time.time()
    results = asyncio.run(combined_repo_analysis(repos))
    end_time = time.time()

    print(f"Combined analysis completed in {end_time - start_time:.2f} seconds")
    print(f"Total stars across all repos: {results['total_stars']}")
    print(f"Languages used: {', '.join(results['languages'])}")
    print(f"Analyzed {results['count']} repositories")