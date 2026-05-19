from prefect import flow, task, get_run_logger
import requests
import json
from datetime import datetime

@task
def fetch_github_repos(owner: str):
    """Fetch repositories for a GitHub user/organization"""
    logger = get_run_logger()
    logger.info(f"Fetching repositories for {owner}")

    response = requests.get(f"https://api.github.com/users/{owner}/repos")
    response.raise_for_status()
    return response.json()

@task
def analyze_repository(repo_data):
    """Analyze a single repository"""
    return {
        "name": repo_data["name"],
        "stars": repo_data["stargazers_count"],
        "forks": repo_data["forks_count"],
        "language": repo_data["language"],
        "size": repo_data["size"],
        "created_at": repo_data["created_at"],
        "updated_at": repo_data["updated_at"]
    }

@flow(name="analyze-repo-{repo_name}")
def analyze_repository_subflow(repo_data):
    """Subflow to analyze a single repository"""
    logger = get_run_logger()
    logger.info(f"Analyzing repository: {repo_data['name']}")

    analysis = analyze_repository(repo_data)

    # Add some derived metrics
    analysis["popularity_score"] = analysis["stars"] + (analysis["forks"] * 2)
    analysis["age_days"] = (datetime.now() - datetime.fromisoformat(analysis["created_at"].replace('Z', '+00:00'))).days

    return analysis

@flow(name="analyze-owner-{owner}")
def analyze_owner_subflow(owner: str):
    """Subflow to analyze all repositories for an owner"""
    logger = get_run_logger()
    logger.info(f"Starting analysis for owner: {owner}")

    # Fetch all repositories
    repos = fetch_github_repos(owner)

    # Analyze each repository using subflows
    analyses = []
    for repo in repos:
        analysis = analyze_repository_subflow(repo)
        analyses.append(analysis)

    # Calculate summary statistics
    total_stars = sum(analysis["stars"] for analysis in analyses)
    total_forks = sum(analysis["forks"] for analysis in analyses)
    languages = list(set(analysis["language"] for analysis in analyses if analysis["language"]))

    return {
        "owner": owner,
        "total_repos": len(analyses),
        "total_stars": total_stars,
        "total_forks": total_forks,
        "languages": languages,
        "repositories": analyses
    }

@task
def save_analysis(analysis_data, filename: str):
    """Save analysis results to file"""
    with open(filename, 'w') as f:
        json.dump(analysis_data, f, indent=2)
    return f"Analysis saved to {filename}"

@flow(name="github-ecosystem-analysis")
def github_ecosystem_analysis(owners: list):
    """Main flow that analyzes multiple GitHub owners using subflows"""
    logger = get_run_logger()
    logger.info(f"Starting ecosystem analysis for {len(owners)} owners")

    # Analyze each owner using subflows
    owner_analyses = []
    for owner in owners:
        analysis = analyze_owner_subflow(owner)
        owner_analyses.append(analysis)

    # Save individual analyses
    for analysis in owner_analyses:
        filename = f"{analysis['owner']}_analysis.json"
        save_analysis(analysis, filename)

    # Calculate ecosystem summary
    total_repos = sum(analysis["total_repos"] for analysis in owner_analyses)
    total_stars = sum(analysis["total_stars"] for analysis in owner_analyses)
    all_languages = set()
    for analysis in owner_analyses:
        all_languages.update(analysis["languages"])

    ecosystem_summary = {
        "owners_analyzed": len(owners),
        "total_repositories": total_repos,
        "total_stars": total_stars,
        "languages_used": list(all_languages),
        "analysis_timestamp": datetime.now().isoformat()
    }

    # Save ecosystem summary
    save_analysis(ecosystem_summary, "ecosystem_summary.json")

    return ecosystem_summary

if __name__ == "__main__":
    owners = ["PrefectHQ", "pandas-dev", "numpy"]
    result = github_ecosystem_analysis(owners)
    print(f"Ecosystem Analysis Complete: {result}")