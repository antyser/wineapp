from langchain.tools import tool
from langchain_core.runnables.config import RunnableConfig
from mem0 import MemoryClient

MEM0_API_KEY = "m0-d80l5YfLDuqlHY5pnMlo3gZnLswIgv21kNbWMDJE"
client = MemoryClient(api_key=MEM0_API_KEY)


@tool
def save_memory(message: str, config: RunnableConfig) -> str:
    """
    Save a message to the user's memory.

    Args:
        message (str): The message to be saved in the user's memory.
        config (RunnableConfig): Configuration object containing user-specific settings.
    """
    user_id = config.get("configurable", {}).get("user_id")
    if user_id:
        client.add(message, user_id=user_id)
        return "saved"
    else:
        raise ValueError("User ID not found in config")


@tool
def search_personal_memory(query: str, config: RunnableConfig) -> str:
    """
    Given a query, look up the relevant memory of the user.

    Args:
        query (str): The search query to look up in the user's memory.
        config (RunnableConfig): Configuration object containing user-specific settings.

    Returns:
        str: A string containing the relevant memories joined by newlines.
    """
    user_id = config.get("configurable", {}).get("user_id")
    if not user_id:
        raise ValueError("User ID not found in config")

    # Initialize the Mem0 client with the provided API key

    # Perform the search on the user's memory using the query
    resp = client.search(query, user_id=user_id)

    # Extract the 'memory' field from each result and join them with newlines
    return "\n".join([mem["memory"] for mem in resp])


def search_personal_memory_v2(query: str, user_id: str) -> str:
    """
    Given a query, look up the relevant memory of the user.

    Args:
        query (str): The search query to look up in the user's memory.
        config (RunnableConfig): Configuration object containing user-specific settings.

    Returns:
        str: A string containing the relevant memories joined by newlines.
    """
    if not user_id:
        raise ValueError("User ID not found in config")

    # Perform the search on the user's memory using the query
    resp = client.search(query, user_id=user_id)
    print(resp)
    # Extract the 'memory' field from each result and join them with newlines
    return "\n".join([mem["memory"] for mem in resp])
