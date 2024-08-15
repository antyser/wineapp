from core.clients.supabase import get_client
from core.wine.model import Wine


def save_wine(wine: Wine):
    client = get_client()
    wine_data = wine.model_dump()
    # Pop the offer fields if they exist
    wine_data.pop("offers", None)
    data = client.table("wines").upsert(wine_data).execute()
    return data


def get_wine(id: str):
    client = get_client()
    data = client.table("wines").select("*").eq("id", id).execute()
    return data


def get_wine_by_name(name: str):
    client = get_client()
    data = client.table("wines").select("*").eq("name", name).execute()
    return data
