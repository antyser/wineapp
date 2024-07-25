from langchain_community.utilities import SerpAPIWrapper, ApifyWrapper
from langchain_core.documents import Document
from langchain.tools import tool
from typing import Dict, List, Optional, Any
from langchain.pydantic_v1 import BaseModel


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
    crawled_metadata: Optional[Dict[str, Any]] = None


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
def search_tool(query: str, top_n: int = 3) -> SearchResultsResponse:
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
            link_to_result[url]["crawled_metadata"] = doc.metadata

    organic_results = list(link_to_result.values())
    return SearchResultsResponse(result=organic_results)
