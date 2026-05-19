import requests
from prefect import flow, task

@task(retries=3, retry_delay_seconds=30)
def fetch_dev_articles(tag: str = "python", per_page: int = 5):
    """Fetch articles from Dev.to API with retry logic"""
    url = f"https://dev.to/api/articles"
    params = {"tag": tag, "per_page": per_page}

    response = requests.get(url, params=params)
    response.raise_for_status()  # This will raise an exception for HTTP errors
    return response.json()

@flow(name="dev-articles")
def dev_articles_flow(tag: str = "python"):
    """Flow to fetch and display Dev.to articles"""
    articles = fetch_dev_articles(tag)

    print(f"Found {len(articles)} articles")
    for article in articles:
        print(f"- {article['title']} by {article['user']['name']}")

    return articles

if __name__ == "__main__":
    dev_articles_flow("prefect")