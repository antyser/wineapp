import asyncio
import itertools
import os
from typing import List, Optional

import httpx
import orjson as json
from fake_headers import Headers
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.timer import timer


async def fetch_proxies(cnt: int = 1) -> List[str]:
    app_key = "1146132303828111360"
    app_secret = "Kn5WteOk"
    url = "https://api.xiaoxiangdaili.com/ip/get"
    params = {
        "appKey": app_key,
        "appSecret": app_secret,
        "cnt": cnt,
        "wt": "json",
        "method": "http",
    }

    async with httpx.AsyncClient() as client:
        response = await client.get(url, params=params)
        response.raise_for_status()
        data = json.loads(response.content)

    if data["code"] != 200:
        logger.error(f"Failed to fetch proxies: {data['msg']}")
        return []

    proxies = [f"{item['ip']}:{item['port']}" for item in data["data"]]
    logger.info(f"Fetched {proxies} proxies")
    return proxies


@retry(
    stop=stop_after_attempt(1),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((httpx.HTTPStatusError, httpx.RequestError)),
    reraise=True,
)
async def fetch_url(
    url: str, use_scraper_api: bool = False, proxy: Optional[str] = None
) -> httpx.Response:
    if proxy:
        proxies = {
            "http://": f"http://{proxy}",
            "https://": f"http://{proxy}",
        }
        client = httpx.AsyncClient(
            http2=True, timeout=httpx.Timeout(60.0, read=60.0), proxies=proxies
        )
    else:
        client = httpx.AsyncClient(http2=True, timeout=httpx.Timeout(60.0, read=60.0))

    try:
        if use_scraper_api:
            payload = {"api_key": os.getenv("SCRAPER_API_KEY"), "url": url}
            response = await client.get("https://api.scraperapi.com/", params=payload)

            if response.status_code == 500:
                logger.warning(
                    f"Received 500 error from scraper API for URL: {url}. Retrying..."
                )
                raise httpx.HTTPStatusError(
                    "Server error from scraper API",
                    request=response.request,
                    response=response,
                )
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
        await client.aclose()


@timer
async def fetch_v2(
    urls: str | List[str], use_proxy: bool = False
) -> List[httpx.Response]:
    # the proxy is not very reliable.
    if isinstance(urls, str):
        urls = [urls]
    semaphore = asyncio.Semaphore(5)

    async def bounded_fetch(url: str, proxy: Optional[str] = None) -> httpx.Response:
        async with semaphore:
            try:
                return await fetch_url(url, proxy=proxy)
            except Exception as e:
                logger.error(f"Failed to fetch {url}: {e}")
                return None

    if use_proxy:
        proxy_pool = (
            await fetch_proxies(min(5, len(urls))) if use_proxy else [] * len(urls)
        )
        proxy_cycle = itertools.cycle(proxy_pool)

        responses = await asyncio.gather(
            *[bounded_fetch(url, next(proxy_cycle)) for url in urls]
        )
    else:
        responses = await asyncio.gather(
            *[bounded_fetch(url, proxy=None) for url in urls]
        )
    return [r for r in responses if r is not None]


@timer
async def fetch(urls: str | List[str], is_pro: bool = False) -> List[httpx.Response]:
    if isinstance(urls, str):
        urls = [urls]
    semaphore = asyncio.Semaphore(5)

    async def bounded_fetch(url: str) -> httpx.Response:
        async with semaphore:
            try:
                return await fetch_url(url, is_pro)
            except Exception as e:
                logger.error(f"Failed to fetch {url}: {e}")
                return None

    responses = await asyncio.gather(*[bounded_fetch(url) for url in urls])
    return [r for r in responses if r is not None]
