import csv
import io
import json
import re
from typing import Dict, List, Optional

from loguru import logger
from lxml.html import fromstring

from core.timer import timer
from core.utils import fetch, fetch_url
from core.wine.model import Offer, Wine


def compose_search_url(
    keyword: str,
    vintage: Optional[str | int] = "",
    country: Optional[str] = "-",
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
        url += f"{country}/-/ndbipe?Xsort_order=p&Xcurrencycode=USD&Xsavecurrency=Y"
    return url


async def fetch_wine(wine_name: str, is_pro: bool = False) -> Optional[Wine]:
    url = compose_search_url(wine_name, country="usa")
    response = await fetch_url(url, is_pro)
    if response.status_code != 200:
        return None
    return parse_wine(response.text)


@timer
async def batch_fetch_wines(
    wine_names: List[str], is_pro: bool = False
) -> Dict[str, Optional[Wine]]:
    urls = [compose_search_url(wine_name, country="usa") for wine_name in wine_names]
    responses = await fetch(urls, is_pro)

    result = {}
    for wine_name, response in zip(wine_names, responses):
        if response and response.status_code == 200:
            wine = parse_wine(response.text)
            result[wine_name] = wine
            if wine is None:
                logger.error(f"Failed to parse wine: {wine_name}")
        else:
            result[wine_name] = None

    return result


def str_to_vintage(vintage_str: Optional[str]) -> int:
    if not vintage_str:
        return 1
    return 1 if vintage_str == "All" else int(vintage_str)


def _extract_ld_json(root) -> dict:
    script_tag = root.xpath('//script[@type="application/ld+json"]')
    ld_json = script_tag[0].text if script_tag else None
    if not ld_json:
        return {}
    return json.loads(ld_json)


def parse_wine(html: str) -> Optional[Wine]:
    # TODO: only works for USA because of the $ sign in the price
    try:
        root = fromstring(html)
        wine_searcher_id = (
            int(root.xpath("//h1/@data-name-id")[0])
            if root.xpath("//h1/@data-name-id")
            else None
        )
        name = (
            root.xpath("//h1/text()")[0].strip() if root.xpath("//h1/text()") else None
        )
        og_url = root.xpath('//meta[@property="og:url"]/@content')[0]
        match = re.search(r"/(\d{4})/", og_url)
        vintage_str = match.group(1) if match else None
        vintage = str_to_vintage(vintage_str) if vintage_str else 1
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

        grape_variety = (
            root.xpath('//meta[@name="productVarietal"]/@content')[0]
            if root.xpath('//meta[@name="productVarietal"]/@content')
            else None
        )
        wine_type = None
        wine_style = None
        style_element = root.xpath('//li[@class="product-details__styles"]/span/text()')
        if style_element:
            wine_type, wine_style = style_element[0].split(" - ", 1)
        region_image = (
            "https://www.wine-searcher.com"
            + root.xpath(f'//img[@alt="{region}"]/@data-src')[0]
            if root.xpath(f'//img[@alt="{region}"]/@data-src')
            else None
        )
        producer = (
            root.xpath('//a[@id="MoreProducerDetail"]/@title')[0].replace(
                "More information about ", ""
            )
            if root.xpath('//a[@id="MoreProducerDetail"]/@title')
            else None
        )
        offers = []

        ld_json = _extract_ld_json(root)
        if ld_json:
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
            name=name,
            url=display_name_url,
            description=ld_json.get("description"),
            region=region,
            region_image=region_image,
            origin=origin,
            grape_variety=grape_variety,
            image=image,
            producer=producer,
            average_price=average_price,
            min_price=min(offers, key=lambda x: x.price).price if offers else None,
            wine_type=wine_type,
            wine_style=wine_style,
            offers=offers,
        )
    except Exception as e:
        logger.error(f"Error in parse_wine: {e}")
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


def wines_to_csv(wines: List[Wine]) -> str:
    """Convert a list of Wine objects to a CSV string."""
    output = io.StringIO()
    writer = csv.writer(output)

    header = list(Wine.model_fields.keys())
    header.extend(["first_offer", "second_offer", "third_offer"])
    writer.writerow(header)

    # Write the wine data
    for wine in wines:
        row = [getattr(wine, field) for field in Wine.model_fields.keys()]
        offers = wine.offers[:3] if wine.offers else []
        for i in range(3):
            if i < len(offers):
                row.append(offers[i].model_dump())
            else:
                row.append(None)
        writer.writerow(row)
    return output.getvalue()
