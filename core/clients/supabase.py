import os
from typing import Optional

from dotenv import load_dotenv
from supabase._async.client import AsyncClient as Client
from supabase._async.client import create_client

load_dotenv()

_client: Optional[Client] = None


def get_client() -> Client:
    global _client
    if _client is None:
        url: str = os.environ["SUPABASE_URL"]
        key: str = os.environ["SUPABASE_KEY"]
        _client = create_client(url, key)
    return _client
