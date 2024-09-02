import asyncio
import csv
import io
import os
import re
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import unquote

from loguru import logger
from lxml.html import fromstring

from core.timer import timer
from core.utils import fetch
from core.wines.model import Offer, Wine
from core.wines.service import save_wines_batch


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


@timer
async def batch_fetch_wines(
    wine_names: List[str], is_pro: bool = False
) -> Dict[str, Optional[Wine]]:
    urls = [compose_search_url(wine_name, country="usa") for wine_name in wine_names]
    responses = await fetch(urls, is_pro)

    result = {}
    wines_to_save = []
    for wine_name, response in zip(wine_names, responses):
        if response and response.status_code == 200:
            wine = parse_wine(response.text)
            result[wine_name] = wine
            if wine is None:
                logger.error(f"Failed to parse wine: {wine_name}")
            else:
                wines_to_save.append(wine)
        else:
            result[wine_name] = None

    # Schedule save_wines_batch to run in the background
    if wines_to_save:
        asyncio.create_task(save_wines_batch(wines_to_save))

    return result


def str_to_vintage(vintage_str: Optional[str]) -> int:
    if not vintage_str:
        return 1
    return 1 if vintage_str == "All" else int(vintage_str)


def safe_xpath_extract(root, xpath, default=None):
    """Safely extract value using XPath, returning default if not found."""
    try:
        result = root.xpath(xpath)
        return result[0].strip() if result else default
    except IndexError:
        return default


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


def extract_offers(root) -> Tuple[List[Offer], int, bool]:
    offers = []
    offers_count = 0
    search_expanded = False

    # Check if search was expanded
    expanded_info = root.xpath(
        '//div[@id="pjax-offers"]//div[contains(@class, "auto-expand-card")]'
    )
    if expanded_info:
        search_expanded = True
        return (
            [],
            0,
            search_expanded,
        )  # Return empty offers, 0 count, and True for search_expanded

    # If search was not expanded, extract offers count and offers
    offers_count_element = root.xpath(
        '//div[@id="pjax-offers"]/div[1]//span[@class="font-weight-bold"]/text()'
    )
    if offers_count_element:
        offers_count = int(offers_count_element[0].split()[0])

    offer_cards = root.xpath('//div[contains(@class, "offer-card__container")]')

    for offer_card in offer_cards:
        price_section = offer_card.xpath(
            './/div[contains(@class, "offer-card__price-section")]'
        )[0]
        price_detail = price_section.xpath(
            './/div[contains(@class, "price__detail_main")]'
        )[0]

        seller_name = safe_xpath_extract(
            offer_card, './/a[contains(@class, "offer-card__merchant-name")]/text()'
        )
        price_str = price_detail.text_content()
        price = (
            float(price_str.replace("$", "").replace(",", "")) if price_str else None
        )

        unit_price_detail = price_section.xpath(
            './/div[contains(@class, "price__detail_secondary")]'
        )
        if not unit_price_detail:
            unit_price = price
        else:
            unit_price_str = unit_price_detail[0].text_content()
            unit_price = (
                float(unit_price_str.split("/")[0].replace("$", "").replace(",", ""))
                if unit_price_str
                else None
            )

        encoded_url = safe_xpath_extract(
            offer_card, './/a[contains(@class, "col2")]/@href'
        )
        url = unquote(encoded_url) if encoded_url else None

        location = safe_xpath_extract(
            offer_card,
            './/div[contains(@class, "offer-card__location-address")]/text()',
        )
        seller_address_region = location.split(":")[-1].strip() if location else None

        country_flag = safe_xpath_extract(
            offer_card, './/svg[contains(@class, "offer-card__location-flag")]/@class'
        )
        seller_address_country = (
            country_flag.split()[-1].replace("icon-flag-", "").upper()
            if country_flag
            else None
        )

        description = safe_xpath_extract(
            offer_card, './/div[contains(@class, "mb-2 small d-full-card-only")]/text()'
        )

        offers.append(
            Offer(
                price=price,
                unit_price=unit_price,
                description=description,
                seller_name=seller_name,
                url=url,
                seller_address_region=seller_address_region,
                seller_address_country=seller_address_country,
            )
        )

    return offers, offers_count, search_expanded


def parse_wine(html: str) -> Optional[Wine]:
    try:
        root = fromstring(html)
        wine_searcher_id = safe_xpath_extract(root, "//h1/@data-name-id")
        wine_searcher_id = int(wine_searcher_id) if wine_searcher_id else None
        description = safe_xpath_extract(
            root, '//li[contains(@class, "product-details__description")]/p/text()'
        )
        name = safe_xpath_extract(root, "//h1/text()")
        og_url = safe_xpath_extract(root, '//meta[@property="og:url"]/@content')
        match = re.search(r"/(\d{4})/", og_url) if og_url else None
        vintage_str = match.group(1) if match else None
        vintage = str_to_vintage(vintage_str) if vintage_str else 1
        display_name_url = og_url

        region = safe_xpath_extract(root, '//meta[@name="productRegion"]/@content')
        origin = safe_xpath_extract(root, '//meta[@name="productOrigin"]/@content')
        image = safe_xpath_extract(root, '//meta[@property="og:image"]/@content')
        average_price = _extract_average_price(root)

        grape_variety = safe_xpath_extract(
            root, '//meta[@name="productVarietal"]/@content'
        )

        wine_type = None
        wine_style = None
        style_element = safe_xpath_extract(
            root, '//li[contains(@class, "product-details__styles")]/span/text()'
        )
        if style_element:
            wine_type, wine_style = style_element.split(" - ", 1)

        region_image = safe_xpath_extract(root, f'//img[@alt="{region}"]/@data-src')
        if region_image:
            region_image = "https://www.wine-searcher.com" + region_image

        producer = safe_xpath_extract(root, '//a[@id="MoreProducerDetail"]/@title')
        if producer:
            producer = producer.replace("More information about ", "")

        offers, offers_count, search_expanded = extract_offers(root)
        logger.info(f"Offers count: {offers_count}")
        logger.info(f"Search expended: {search_expanded}")

        return Wine(
            id=str(f"{wine_searcher_id}_{vintage}"),
            wine_searcher_id=wine_searcher_id,
            vintage=vintage,
            name=name,
            url=display_name_url,
            description=description,
            region=region,
            region_image=region_image,
            origin=origin,
            grape_variety=grape_variety,
            image=image,
            producer=producer,
            average_price=average_price,
            min_price=min(offers, key=lambda x: x.unit_price).unit_price
            if offers
            else None,
            wine_type=wine_type,
            wine_style=wine_style,
            offers=offers,
            offers_count=offers_count,
            search_expanded=search_expanded,
        )
    except Exception as e:
        logger.warning(f"Error in parse_wine: {e}")
        return None


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


async def process_wine_list(
    input_file: str, output_file: str, batch_size: int = 10, is_pro: bool = False
) -> None:
    """
    Process a list of wine names, fetch their details, and store them in CSV format.

    Args:
    input_file (str): Path to the input file containing wine names (one per line).
    output_file (str): Path to the output CSV file.
    batch_size (int): Number of wines to process in each batch.
    is_pro (bool): Whether to use the pro version of the wine searcher.
    """
    processed_wines: Set[str] = set()

    # If output file exists, read already processed wines
    if os.path.exists(output_file):
        with open(output_file, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            processed_wines = set(row["query"] for row in reader)

    with open(input_file, "r") as f:
        wine_names = [line.strip() for line in f if line.strip()]

    total_wines = len(wine_names)
    wines_to_process = [name for name in wine_names if name not in processed_wines]

    logger.info(f"Total wines: {total_wines}")
    logger.info(f"Wines to process: {len(wines_to_process)}")

    for i in range(0, len(wines_to_process), batch_size):
        batch = wines_to_process[i : i + batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}")

        try:
            results = await batch_fetch_wines(batch, is_pro)

            # Prepare data for CSV
            csv_data = []
            for wine_name, wine in results.items():
                if wine:
                    wine_dict = wine.model_dump()
                    wine_dict["query"] = wine_name  # Add query field
                    offers = wine.offers[:3] if wine.offers else []
                    for j in range(3):
                        if j < len(offers):
                            wine_dict[f"offer_{j+1}"] = offers[j].model_dump()
                        else:
                            wine_dict[f"offer_{j+1}"] = None
                    csv_data.append(wine_dict)

            # Append to CSV file
            file_exists = os.path.exists(output_file)
            with open(output_file, "a", newline="", encoding="utf-8") as csvfile:
                fieldnames = (
                    list(Wine.model_fields.keys())
                    + ["query"]
                    + [f"offer_{j+1}" for j in range(3)]
                )
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                if not file_exists:
                    writer.writeheader()

                for row in csv_data:
                    writer.writerow(row)

            processed_wines.update(batch)

        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            # Continue with the next batch

    logger.info("Wine processing completed")
