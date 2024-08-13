import asyncio
import time
from typing import Dict, List, Optional
from urllib.parse import quote_plus, urlparse

import httpx
from bs4 import BeautifulSoup
from curl_cffi import requests
from fake_headers import Headers
from langchain.pydantic_v1 import BaseModel
from langchain.tools import tool
from langchain_community.utilities import GoogleSerperAPIWrapper
from loguru import logger
from slugify import slugify  # type: ignore
from unstructured.partition.html import partition_html

# Import the wine_searcher functions
from core.wine.wine_searcher import (
    fetch_wine_data,
    parse_wine_searcher_wine,
)


class SearchResult(BaseModel):
    position: Optional[int] = None
    title: Optional[str] = None
    link: Optional[str] = None
    snippet: Optional[str] = None
    crawled_content: Optional[str] = None


class SearchResultsResponse(BaseModel):
    result: List[SearchResult]


def google(search_term, site=None):
    escaped_search_term = slugify(search_term, separator=" ")
    if site:
        escaped_search_term = "site: " + site + " " + escaped_search_term

    escaped_search_term = quote_plus(escaped_search_term)
    params = (
        ("q", escaped_search_term),
        ("oq", escaped_search_term),
        ("aqs", "chrome..69i57j69i60.1395j0j1"),
        ("sourceid", "chrome"),
    )
    resp = requests.get(
        "https://www.google.com/search",
        impersonate="chrome110",
        params=params,
        verify=False,
    )
    soup = BeautifulSoup(resp.text, "html.parser")
    links = []

    # Extract links from search results
    for item in soup.find_all("a"):
        href = item.get("href")
        if href and href.startswith("/url?q="):
            link = href.split("/url?q=")[1].split("&")[0]
            links.append(link)
    return links


skip_domains = ["klwines.com", "wine.com", "reddit.com", "benchmarkwine.com"]


def perform_search(query: str) -> List[Dict]:
    search = GoogleSerperAPIWrapper()

    start_time = time.time()
    search_result = search.results(query)
    search_latency = time.time() - start_time
    logger.info(f"search.results latency: {search_latency:.2f} seconds")

    if "organic" not in search_result:
        raise ValueError(f"Search failed, {search_result}")
    return search_result["organic"]


@tool
def search_tool(query: str, top_n: int = 3) -> SearchResultsResponse:
    """Perform a Google search and scrape the top N organic results. If the query is a wine name, suggest to add wine searcher in the query."""
    search_result = perform_search(query)

    # Pre-filter results to exclude specified domains
    filtered_search_result = [
        result
        for result in search_result
        if urlparse(result["link"]).netloc not in skip_domains
    ]

    # Keep only the highest-ranked link from each domain
    domain_to_result = {}
    for result in filtered_search_result:
        domain = urlparse(result["link"]).netloc
        if domain not in domain_to_result:
            domain_to_result[domain] = result

    # Get the top N results
    filtered_results = list(domain_to_result.values())[:top_n]
    link_to_result = {result["link"]: result for result in filtered_results}

    urls = list(link_to_result.keys())

    start_time = time.time()
    crawled_contents = asyncio.run(batch_crawl(urls))
    crawl_latency = time.time() - start_time
    logger.info(f"batch_crawl latency: {crawl_latency:.2f} seconds")

    # Map crawled content back to search results
    for i, doc in enumerate(crawled_contents):
        url = urls[i]
        if url in link_to_result:
            link_to_result[url]["crawled_content"] = doc

    organic_results = list(link_to_result.values())
    return SearchResultsResponse(result=organic_results)


def batch_search_wines(wine_names: List[str]) -> List[str]:
    """Search for wines by name and return the results."""
    return [
        wine_data
        for wine_name in wine_names
        if (wine_data := fetch_wine_data(wine_name))
    ]


def general_parse(html_content: str) -> str:
    """
    Converts HTML content to Markdown format.
    Args:
        html_content (str): The HTML content to convert.
    Returns:
        str: The converted Markdown content.
    """
    return extract_main_text(html_content)


def extract_main_text(html_content: str) -> str:
    # Parse the HTML content
    text_elements = [element.text for element in partition_html(text=html_content)]
    return " ".join(text_elements)


def extract_card_body_text(html_content: str) -> str:
    # Parse the HTML content
    soup = BeautifulSoup(html_content, "html.parser")

    # Find all elements with class "card-body"
    card_bodies = soup.find_all(class_="card-body")

    # Extract and concatenate the text content of each card body
    card_body_texts = [card_body.get_text(strip=True) for card_body in card_bodies]

    # Join the texts with a newline for better readability
    return "\n".join(card_body_texts)


async def fetch_and_process_page(client, url):
    if "wine-searcher.com/find/" in url:
        response = await client.get(url, headers=Headers(headers=True).generate())
        if response.status_code == 200:
            return parse_wine_searcher_wine(response.text)
        else:
            logger.error(f"Failed to fetch {url}: {response.status_code}")
    else:
        try:
            return partition_html(
                url=url,
                headers=Headers(headers=True).generate(),
                skip_headers_and_footers=True,
                chunking_strategy="basic",
                max_characters=50000,
            )[0].text
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {e}")
            return None


async def batch_crawl(links: List[str]):
    # Filter links by domain, keeping the highest-ranked one
    unique_links = {}
    for link in links:
        domain = urlparse(link).netloc
        # Check if the link is a file based on its extension
        if domain not in unique_links and not link.endswith(
            (".pdf", ".txt", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx")
        ):
            unique_links[domain] = link  # Keep the first occurrence (highest rank)

    filtered_links = list(unique_links.values())

    async with httpx.AsyncClient() as client:
        tasks = [fetch_and_process_page(client, link) for link in filtered_links]
        results = await asyncio.gather(*tasks)
    return results
