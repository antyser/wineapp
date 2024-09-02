from typing import List

from core.clients.supabase import get_client
from core.wines.model import Wine


async def save_wine(wine: Wine):
    client = await get_client()
    wine_data = wine.model_dump(exclude={"offers"})
    offers = wine.offers or []

    # Save the wine
    wine_result = await client.table("wines").upsert(wine_data).execute()

    # Prepare offers with wine_id
    offer_data = []
    for offer in offers:
        offer_dict = offer.model_dump()
        offer_dict["wine_id"] = wine.id
        offer_data.append(offer_dict)

    if offer_data:
        await client.table("offers").upsert(offer_data).execute()

    return wine_result


async def get_wine(id: str):
    client = await get_client()
    data = await client.table("wines").select("*").eq("id", id).execute()
    return data


async def get_wine_by_name(name: str):
    client = await get_client()
    data = await client.table("wines").select("*").eq("name", name).execute()
    return data


async def save_wines_batch(wines: List[Wine]):
    client = await get_client()

    unique_wines = {}  # type: ignore
    for wine in wines:
        if (
            wine.id not in unique_wines
            or wine.updated_at > unique_wines[wine.id].updated_at
        ):
            unique_wines[wine.id] = wine

    wine_data = [wine.model_dump(exclude={"offers"}) for wine in unique_wines.values()]
    offers_data = []

    for wine in unique_wines.values():
        if wine.offers:
            for offer in wine.offers:
                offer_dict = offer.model_dump()
                offer_dict["wine_id"] = wine.id
                offers_data.append(offer_dict)

    # Save wines
    wines_result = await client.table("wines").upsert(wine_data).execute()

    if offers_data:
        await client.table("offers").upsert(offers_data).execute()

    return wines_result
