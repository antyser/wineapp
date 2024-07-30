import argparse
import os
import random
import time
from urllib.parse import quote, urljoin, urlparse, urlunparse

import httpx
from bs4 import BeautifulSoup
from fake_headers import Headers
from loguru import logger
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from webdriver_manager.chrome import ChromeDriverManager

BASE_URL = "https://www.wine-searcher.com/regions"
OUTPUT_DIR = "ws"
URLS_FILE = "crawled_urls.txt"
UNCRAWLED_URLS_FILE = "uncrawled_urls.txt"

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "priority": "u=0, i",
    "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Google Chrome";v="126"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "none",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
}

PROXY = "http://1135478480335949824:CBtfFxU6@http-dynamic-S03.xiaoxiangdaili.com:10030"

logger.add("crawler.log", rotation="1 MB")


def save_html(url, content):
    parsed_url = urlparse(url)
    path = quote(parsed_url.path.strip("/"), safe="")
    file_path = os.path.join(OUTPUT_DIR, f"{path}.html")
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as file:
        file.write(content)
    logger.info(f"Saved HTML content for {url}")


def load_urls(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            return set(line.strip() for line in file)
    return set()


def save_url(file_path, url):
    with open(file_path, "a", encoding="utf-8") as file:
        file.write(url + "\n")
    logger.info(f"Saved URL: {url} to {file_path}")


def remove_url(file_path, url):
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as file:
            lines = file.readlines()
        with open(file_path, "w", encoding="utf-8") as file:
            for line in lines:
                if line.strip() != url:
                    file.write(line)


def is_single_level_path(url):
    parsed_url = urlparse(url)
    path = parsed_url.path.strip("/")
    return len(path.split("/")) == 1


def strip_fragment_and_query(url):
    parsed_url = urlparse(url)
    return urlunparse(parsed_url._replace(fragment="", query=""))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=20, max=100),
    retry=retry_if_exception_type(httpx.HTTPStatusError),
)
def fetch_url(client, url):
    headers = Headers(os="mac", headers=True).generate()
    response = client.get(url, headers=headers)
    if response.status_code == 403:
        logger.warning(f"Received 403 for {url}, retrying...")
        response.raise_for_status()
    return response


def fetch_url_with_selenium(url):
    options = webdriver.ChromeOptions()
    options.add_argument("--no-sandbox")
    driver = webdriver.Chrome(
        service=ChromeService(ChromeDriverManager().install()), options=options
    )
    driver.get(url)
    content = driver.page_source
    driver.quit()
    return content


def crawl(url, visited, uncrawled, use_browser):
    url = strip_fragment_and_query(url)
    if url in visited or not is_single_level_path(url):
        logger.info(f"Skipping already visited URL: {url}")
        return
    visited.add(url)
    save_url(URLS_FILE, url)
    remove_url(UNCRAWLED_URLS_FILE, url)

    if use_browser:
        try:
            response_text = fetch_url_with_selenium(url)
        except Exception as e:
            logger.error(f"Failed to retrieve {url} with Selenium: {e}")
            return
    else:
        headers = Headers(os="mac", headers=True).generate()
        client = httpx.Client(
            http2=True, headers=headers, proxies={"http://": PROXY, "https://": PROXY}
        )
        try:
            response = fetch_url(client, url)
        except httpx.HTTPStatusError as e:
            logger.error(f"Failed to retrieve {url} after retries: {e}")
            return
        response_text = response.text

    time.sleep(random.randint(15, 20))
    save_html(url, response_text)

    soup = BeautifulSoup(response_text, "html.parser")
    for link in soup.find_all("a", href=True):
        next_url = urljoin(BASE_URL, link["href"])
        next_url = strip_fragment_and_query(next_url)
        if (
            next_url.startswith(BASE_URL)
            and next_url not in visited
            and is_single_level_path(next_url)
        ):
            if next_url not in uncrawled:
                save_url(UNCRAWLED_URLS_FILE, next_url)
                uncrawled.add(next_url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crawl wine-searcher website.")
    parser.add_argument(
        "--use-browser", action="store_true", help="Use Selenium browser for crawling"
    )
    args = parser.parse_args()

    visited_urls = load_urls(URLS_FILE)
    uncrawled_urls = load_urls(UNCRAWLED_URLS_FILE)

    if not uncrawled_urls:
        uncrawled_urls.add(BASE_URL)

    logger.info("Starting crawl")
    while uncrawled_urls:
        url = uncrawled_urls.pop()
        crawl(url, visited_urls, uncrawled_urls, args.use_browser)
    logger.info("Crawl finished")
