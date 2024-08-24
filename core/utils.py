import asyncio
from typing import List, Optional

import httpx
from fake_headers import Headers
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)),
    reraise=True,
)
async def fetch_url(
    url: str, is_pro: bool, client: Optional[httpx.AsyncClient] = None
) -> httpx.Response:
    client_created = False
    if client is None:
        client = httpx.AsyncClient(http2=True, timeout=httpx.Timeout(60.0, read=60.0))
        client_created = True

    try:
        if is_pro:
            payload = {"api_key": "746d6963fe656750b2bfc9e615f93bb9", "url": url}
            response = await client.get("https://api.scraperapi.com/", params=payload)
        else:
            response = await client.get(
                url, headers=Headers(headers=True).generate(), follow_redirects=True
            )

        if response.status_code == 429:
            raise httpx.HTTPStatusError(
                "Rate limited", request=response.request, response=response
            )

        response.raise_for_status()
        return response
    finally:
        if client_created:
            await client.aclose()


async def fetch(urls: str | List[str], is_pro: bool = False) -> List[httpx.Response]:
    if isinstance(urls, str):
        urls = [urls]
    timeout = httpx.Timeout(60.0, read=60.0)
    semaphore = asyncio.Semaphore(5)
    async with httpx.AsyncClient(http2=True, timeout=timeout) as client:

        async def bounded_fetch(url: str) -> httpx.Response:
            async with semaphore:
                try:
                    return await fetch_url(url, is_pro, client)
                except Exception as e:
                    logger.error(f"Failed to fetch {url}: {e}")
                    return None

        responses = await asyncio.gather(*[bounded_fetch(url) for url in urls])
        return [r for r in responses if r is not None]
