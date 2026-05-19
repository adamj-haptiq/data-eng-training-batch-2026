from __future__ import annotations

import requests
from bs4 import BeautifulSoup
from prefect import flow, task

@task(retries=3, retry_delay_seconds=2)
def fetch_html(url: str) -> str:
    """Download page HTML (with retries).

    This is just a regular requests call - Prefect adds retry logic
    without changing how we write the code."""
    print(f"Fetching {url} …")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.text

@task
def parse_article(html: str) -> str:
    """Extract article text, skipping code blocks.

    Regular BeautifulSoup parsing with standard Python string operations.
    Prefect adds observability without changing the logic."""
    soup = BeautifulSoup(html, "html.parser")

    # Find main content - just regular BeautifulSoup
    article = soup.find("article") or soup.find("main")
    if not article:
        return ""

    # Standard Python all the way
    for code in article.find_all(["pre", "code"]):
        code.decompose()

    content = []
    for elem in article.find_all(["h1", "h2", "h3", "p", "ul", "ol", "li"]):
        text = elem.get_text().strip()
        if not text:
            continue

        if elem.name.startswith("h"):
            content.extend(["\n" + "=" * 80, text.upper(), "=" * 80 + "\n"])
        else:
            content.extend([text, ""])

    return "\n".join(content)

@flow(log_prints=True)
def scrape(urls: list[str] | None = None) -> None:
    """Scrape and print article content from URLs.

    A regular Python function that composes our tasks together.
    Prefect adds logging and dependency management automatically."""

    if urls:
        for url in urls:
            content = parse_article(fetch_html(url))
            print(content if content else "No article content found.")

if __name__ == "__main__":
    urls = [
        "https://www.prefect.io/blog/airflow-to-prefect-why-modern-teams-choose-prefect"
    ]
    scrape(urls=urls)