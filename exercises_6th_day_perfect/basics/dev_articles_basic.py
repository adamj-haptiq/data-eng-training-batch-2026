import requests

def fetch_dev_articles(tag: str = "python"):
    """Fetch articles from Dev.to API"""
    url = f"https://dev.to/api/articles"
    params = {"tag": tag, "per_page": 5}

    response = requests.get(url, params=params)
    return response.json()

if __name__ == "__main__":
    articles = fetch_dev_articles("prefect")
    print(f"Found {len(articles)} articles about Prefect!")
    for article in articles:
        print(f"- {article['title']}")