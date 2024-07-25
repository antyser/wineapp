import asyncio
import httpx
from langchain_community.utilities import SerpAPIWrapper, ApifyWrapper
from langchain_core.documents import Document
from langchain.tools import tool
from typing import Dict, List, Optional, Any
from langchain.pydantic_v1 import BaseModel
from markdownify import markdownify as md
from slugify import slugify
import httpx
import json
from bs4 import BeautifulSoup
from slugify import slugify
from urllib.parse import urlparse
import yaml

from tools.wine_searcher_tool import parse_wine_searcher

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
    redirect_link: Optional[str] = None
    displayed_link: Optional[str] = None
    thumbnail: Optional[str] = None
    favicon: Optional[str] = None
    snippet: Optional[str] = None
    snippet_highlighted_words: Optional[List[str]] = None
    sitelinks_search_box: Optional[bool] = None
    sitelinks: Optional[Dict[str, Any]] = None
    rich_snippet: Optional[Dict[str, Any]] = None
    about_this_result: Optional[Dict[str, Any]] = None
    cached_page_link: Optional[str] = None
    related_pages_link: Optional[str] = None
    source: Optional[str] = None
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
    return documents


@tool
def search_tool_deprecated(query: str, top_n: int = 3) -> SearchResultsResponse:
    """Perform a Google search and scrape the top N organic results."""
    search = SerpAPIWrapper()
    apify = ApifyWrapper()

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

@tool
def search_tool(query: str, top_n: int = 3) -> SearchResultsResponse:
    """Perform a Google search and scrape the top N organic results."""
    search = SerpAPIWrapper()

    top_organic_results = search.results(query)
    if "organic_results" not in top_organic_results:
        raise ValueError(f"Search failed, {top_organic_results}")
    top_organic_results = top_organic_results["organic_results"][:top_n]
    link_to_result = {result["link"]: result for result in top_organic_results}

    urls = list(link_to_result.keys())
    crawled_contents = asyncio.run(batch_crawl(urls))

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
    return parse_wine_searcher(response.text)


def parse_wine_searcher(response_text: str) -> str:
    soup = BeautifulSoup(response_text, "html.parser")
    ld_json = soup.find("script", type="application/ld+json")
    if ld_json:
        json_data = json.loads(ld_json.string)
        return yaml.dump(json_data)
    return None

def general_parse(html_content: str) -> str:
    """
    Converts HTML content to Markdown format.
    Args:
        html_content (str): The HTML content to convert.
    Returns:
        str: The converted Markdown content.
    """
    return md(html_content)


async def fetch_and_process_page(client, url):
    response = await client.get(url, headers=headers)
    
    if "wine-searcher.com" in url:
        return parse_wine_searcher(response.text)
    else:
        return general_parse(response.text)
    

async def batch_crawl(links: List[str]):
    # Filter links by domain, keeping the highest-ranked one
    unique_links = {}
    for link in links:
        domain = urlparse(link).netloc
        if domain not in unique_links:
            unique_links[domain] = link  # Keep the first occurrence (highest rank)
    
    filtered_links = list(unique_links.values())

    async with httpx.AsyncClient() as client:
        tasks = [fetch_and_process_page(client, link) for link in filtered_links]
        results = await asyncio.gather(*tasks)
    return results
