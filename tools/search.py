import asyncio
import json
import time
from typing import Dict, List, Optional
from urllib.parse import quote_plus, urljoin, urlparse

import httpx
import yaml  # type: ignore
from bs4 import BeautifulSoup
from curl_cffi import requests
from langchain.pydantic_v1 import BaseModel
from langchain.tools import tool
from langchain_community.utilities import ApifyWrapper, GoogleSerperAPIWrapper
from langchain_core.documents import Document
from loguru import logger
from slugify import slugify  # type: ignore
from unstructured.partition.html import partition_html

headers = {
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}


class SearchResult(BaseModel):
    position: Optional[int] = None
    title: Optional[str] = None
    link: Optional[str] = None
    snippet: Optional[str] = None
    crawled_content: Optional[str] = None


class SearchResultsResponse(BaseModel):
    result: List[SearchResult]


def my_dataset_mapping_function(dataset_item: Dict) -> Document:
    return Document(
        page_content=dataset_item.get("text", ""),
        metadata=dataset_item.get("metadata", ""),
    )


def fetch_crawler_results(apify: ApifyWrapper, urls: List[str]) -> List[str]:
    loader = apify.call_actor(
        actor_id="apify/website-content-crawler",
        run_input={
            "startUrls": [{"url": url} for url in urls],
            "maxCrawlDepth": 1,
            "maxCrawlPages": 10,
        },
        dataset_mapping_function=my_dataset_mapping_function,
    )
    documents = loader.load()
    return [doc.page_content for doc in documents]  # Return list of strings


@tool
def search_tool_deprecated(query: str, top_n: int = 3) -> SearchResultsResponse:
    """Perform a Google search and scrape the top N organic results."""
    apify = ApifyWrapper(apify_client=None, apify_client_async=None)
    search = GoogleSerperAPIWrapper()
    top_organic_results = search.results(query)
    if "organic_results" not in top_organic_results:
        raise ValueError(f"Search failed, {top_organic_results}")
    top_organic_results = top_organic_results["organic_results"][:top_n]
    link_to_result = {result["link"]: result for result in top_organic_results}

    loader = apify.call_actor(
        actor_id="apify/website-content-crawler",
        run_input={
            "startUrls": [{"url": url} for url in link_to_result.keys()],
            "maxCrawlDepth": 0,
            "maxCrawlPages": 10,
            "crawlerType": "cheerio",
        },
        dataset_mapping_function=my_dataset_mapping_function,
    )
    documents = loader.load()
    # Map scraped content back to search results
    for doc in documents:
        url = doc.metadata.get("canonicalUrl")
        if url in link_to_result:
            link_to_result[url]["crawled_content"] = doc.page_content

    organic_results = list(link_to_result.values())
    return SearchResultsResponse(result=organic_results)


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


@tool
def search_tool(query: str, top_n: int = 3) -> SearchResultsResponse:
    """Perform a Google search and scrape the top N organic results. If the query is a wine name, suggest to add wine searcher in the query."""
    search = GoogleSerperAPIWrapper()

    start_time = time.time()
    search_result = search.results(query)
    search_latency = time.time() - start_time
    logger.info(f"search.results latency: {search_latency:.2f} seconds")

    if "organic" not in search_result:
        raise ValueError(f"Search failed, {search_result}")
    search_result = search_result["organic"]

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


@tool
def fetch_wine_data(wine_name: str) -> str:
    """
    Fetches wine data from Wine-Searcher based on the provided wine name.
    Args:
        wine_name (str): The name of the wine to search for.
    Returns:
        str: The LD+JSON data converted to YAML format, or None if no data is found.
    """
    url = f"https://www.wine-searcher.com/find/{slugify(wine_name)}"

    client = httpx.Client(http2=True)
    response = client.get(url, headers=headers)
    return parse_wine_searcher_wine(response.text)


def parse_wine_searcher_wine(response_text: str) -> str:
    soup = BeautifulSoup(response_text, "html.parser")
    ld_json = soup.find("script", type="application/ld+json")
    if ld_json:
        json_data = json.loads(ld_json.string)

        fields_to_remove = ["@context", "@type", "itemCondition", "mpn", "sku"]
        for field in fields_to_remove:
            json_data.pop(field, None)

        if "image" in json_data:
            json_data["image"] = urljoin(
                "https://www.wine-searcher.com/", json_data["image"]
            )

        if "offers" in json_data:
            trimmed_offers = []
            for offer in json_data["offers"][:5]:
                trimmed_offer = {
                    "price": offer.get("price"),
                    "description": offer.get("description"),
                    "priceCurrency": offer.get("priceCurrency"),
                    "availability": offer.get("availability"),
                    "url": offer.get("url"),
                    "name": offer.get("name"),
                    "seller_name": offer.get("seller", {}).get("name"),
                    "address": offer.get("seller", {}).get("address", {}),
                }
                trimmed_offers.append(trimmed_offer)
            json_data["offers"] = trimmed_offers
        return yaml.dump(json_data)
    return ""


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
    response = await client.get(url, headers=headers)

    if response.status_code == 200:
        if "wine-searcher.com/find/" in url:
            return parse_wine_searcher_wine(response.text)
        else:
            return general_parse(response.text)
    else:
        logger.error(f"Failed to fetch {url}: {response.status_code}")
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
