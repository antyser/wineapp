from core.clients.supabase import supabase_client
from core.wine.model import Wine


def save_wine(wine: Wine):
    wine_data = wine.model_dump()
    # Pop the offer fields if they exist
    wine_data.pop("offers", None)
    data = supabase_client.table("wines").upsert(wine_data).execute()
    return data


def get_wine(id: str):
    data = supabase_client.table("wines").select("*").eq("id", id).execute()
    return data


def get_wine_by_name(name: str):
    data = supabase_client.table("wines").select("*").eq("name", name).execute()
    return data
