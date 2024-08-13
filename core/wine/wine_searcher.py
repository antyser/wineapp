import json
import re
from typing import Optional
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup
from fake_headers import Headers
from loguru import logger
from lxml.html import fromstring
from slugify import slugify  # type: ignore

from core.wine.model import Offer, Wine


def compose_search_url(
    keyword: str,
    vintage: Optional[str | int] = "",
    include_auction: Optional[bool] = False,
) -> str:
    """
    Composes the URL for the Wine-Searcher API.

    Parameters:
    keyword (str): The keyword to search for.
    vintage (str, optional): The vintage to search for. Defaults to "".
    include_auction (bool, optional): Whether to include auctions in the search. Defaults to False.

    Returns:
    str: The composed URL.
    """
    # Extract vintage from the keyword if not provided
    if not vintage:
        match = re.search(r"(\d{4})", keyword)
        if match:
            vintage = match.group(1)
            # Remove the vintage from the keyword
            keyword = re.sub(r"(\d{4})", "", keyword).strip()

    url = f"https://www.wine-searcher.com/find/{keyword}/"
    if vintage:
        url += f"{vintage}/"
    if not include_auction:
        url += "-/-/ndbipe?Xsort_order=p"
    return url


def fetch_wine_data(wine_name: str) -> Optional[Wine]:
    """
    Fetches wine data from Wine-Searcher based on the provided wine name.
    Args:
        wine_name (str): The name of the wine to search for.
    Returns:
        str: The LD+JSON data converted to YAML format, or None if no data is found.
    """
    url = compose_search_url(slugify(wine_name))

    client = httpx.Client(http2=True)
    response = client.get(
        url, headers=Headers(headers=True).generate(), follow_redirects=True
    )
    return parse_wine(response.text)


def str_to_vintage(vintage_str: Optional[str]) -> int:
    if not vintage_str:
        return 1
    return 1 if vintage_str == "All" else int(vintage_str)


def extract_wine_info(wine_data: dict) -> str:
    """
    Extracts relevant wine information from a dictionary and converts it to plain text.

    Args:
        wine_data (dict): The wine data dictionary.

    Returns:
        str: The extracted wine information in plain text format.
    """
    name = wine_data.get("name", "N/A")
    description = wine_data.get("description", "N/A")
    country = wine_data.get("countryOfOrigin", {}).get("name", "N/A")
    vintage = wine_data.get("model", "N/A")
    brand = wine_data.get("brand", {}).get("name", "N/A")
    award = wine_data.get("award", "N/A")
    image_url = wine_data.get("image", "N/A")
    rating = wine_data.get("aggregateRating", {}).get("ratingValue", "N/A")
    rating_count = wine_data.get("aggregateRating", {}).get("ratingCount", "N/A")

    categories = ", ".join(
        [cat.get("name", "N/A") for cat in wine_data.get("category", [])]
    )

    reviews = wine_data.get("review", [])
    reviews_text = "\n".join(
        [
            f"Author: {review.get('author', {}).get('name', 'N/A')}, "
            f"Rating: {review.get('reviewRating', {}).get('ratingValue', 'N/A')}/100, "
            f"Review: {review.get('reviewBody', 'N/A')}"
            for review in reviews
        ]
    )

    return (
        f"Name: {name}\n"
        f"Description: {description}\n"
        f"Country of Origin: {country}\n"
        f"Vintage: {vintage}\n"
        f"Brand: {brand}\n"
        f"Award: {award}\n"
        f"Image URL: {image_url}\n"
        f"Rating: {rating} (based on {rating_count} reviews)\n"
        f"Categories: {categories}\n"
        f"Reviews:\n{reviews_text}"
    )


def extract_wine_data(response_text: str) -> dict:
    soup = BeautifulSoup(response_text, "html.parser")
    ld_json = soup.find("script", type="application/ld+json")
    if ld_json:
        json_data = json.loads(ld_json.string)
        return json_data
    return {}


def _extract_ld_json(root) -> dict:
    script_tag = root.xpath('//script[@type="application/ld+json"]')
    ld_json = script_tag[0].text if script_tag else None
    return json.loads(ld_json)


def parse_wine(html: str) -> Optional[Wine]:
    try:
        root = fromstring(html)
        ld_json = _extract_ld_json(root)
        wine_searcher_id = (
            int(root.xpath("//h1/@data-name-id")[0])
            if root.xpath("//h1/@data-name-id")
            else None
        )
        og_url = root.xpath('//meta[@property="og:url"]/@content')[0]
        match = re.search(r"/(\d{4})/", og_url)
        vintage_str = match.group(1) if match else None
        vintage = str_to_vintage(vintage_str) if vintage_str else 1
        display_name_short = ld_json["name"] if "name" in ld_json else None
        display_name_url = og_url
        region = (
            root.xpath('//meta[@name="productRegion"]/@content')[0]
            if root.xpath('//meta[@name="productRegion"]/@content')
            else None
        )
        origin = (
            root.xpath('//meta[@name="productOrigin"]/@content')[0]
            if root.xpath('//meta[@name="productOrigin"]/@content')
            else None
        )
        image = (
            root.xpath('//meta[@property="og:image"]/@content')[0]
            if root.xpath('//meta[@property="og:image"]/@content')
            else None
        )
        average_price = _extract_average_price(root)
        producer = (
            ld_json["brand"]["name"]
            if "brand" in ld_json and "name" in ld_json["brand"]
            else None
        )
        region_image = None
        grape_variety = None
        wine_type = None
        wine_style = None

        # Extract category information
        for category in ld_json.get("category", []):
            if category.get("disambiguatingDescription") == "Region":
                region = category.get("name")
                region_image = urljoin(
                    "https://www.wine-searcher.com/", category.get("image")
                )
            elif category.get("disambiguatingDescription") == "Grape Variety / Blend":
                grape_variety = category.get("name")
            elif category.get("disambiguatingDescription") == "Style":
                wine_type, wine_style = category.get("name").split(" - ")
        offers = []
        for offer in ld_json["offers"][:5]:
            seller = offer.get("seller", {})
            address = seller.get("address", {})
            offers.append(
                Offer(
                    price=float(offer.get("price", 0)),
                    description=offer.get("description"),
                    seller_name=seller.get("name"),
                    url=offer.get(
                        "url",
                    ),
                    name=offer.get("name"),
                    seller_address_region=address.get("addressRegion"),
                    seller_address_country=address.get("addressCountry", {}).get(
                        "name"
                    ),
                )
            )

        return Wine(
            id=str(f"{wine_searcher_id}_{vintage}"),
            wine_searcher_id=wine_searcher_id,
            vintage=vintage,
            name=display_name_short,
            url=display_name_url,
            description=ld_json.get("description"),
            region=region,
            region_image=region_image,
            origin=origin,
            grape_variety=grape_variety,
            image=image,
            producer=producer,
            average_price=average_price,
            min_price=min(offers, key=lambda x: x.price).price,
            wine_type=wine_type,
            wine_style=wine_style,
            offers=offers,
        )
    except Exception as e:
        logger.error(e)
        return None


def parse_float(value: str) -> Optional[float]:
    """
    Parses a string to a float.

    Parameters:
    value (str): The string to parse.

    Returns:
    Optional[float]: The parsed float or None if parsing fails.
    """
    try:
        return float(value)
    except ValueError:
        logger.error(f"Failed to parse float from value: {value}")
        return None


def _extract_average_price(root) -> Optional[float]:
    description_content = root.xpath('//meta[@name="description"]/@content')[0]
    average_price_str = description_content.split("$")[1].split("/")[0].strip()
    average_price_str = average_price_str.replace(",", "")
    return float(average_price_str)


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

        json_data["offers"] = json_data["offers"][:5]
        wine_info = extract_wine_info(json_data)
        return wine_info
    return ""
