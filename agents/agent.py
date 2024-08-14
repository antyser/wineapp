from typing import Optional

from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from tools.recommendation import (
    save_memory,
    search_personal_memory_v2,
)
from tools.search import search_tool

# parser = PydanticOutputParser(pydantic_object=Followups)


def create_agent(user_id: Optional[str] = None):
    search_memory = search_personal_memory_v2("preference", user_id) if user_id else ""
    model = ChatOpenAI(model="gpt-4o-mini", temperature=0.7, streaming=True)

    system_message = f"""Act as a knowledgeable sommelier. 
    Your job is to answer any questions related to wines include but not limit to wine basic information, regions, pairing, etc.
    - When given a wine name(s), find the relevant wine information including the wine name, retail price, links to purchase, wine region, winery, and wine critic reviews. Use the search tool and search by the wine name wine name + wine searcher as the search term.
    Otherwise, use the search tool with the query. Use the search tool no more than 5 times.
    - If you are provided with preference or fact, save it to memory.
    Do not restate or appreciate what I say.
    Always be as efficient as possible when providing information or making recommendations.
    Here are the user preference: {search_memory}. You should take it into consideration when providing information or making recommendations.
    """

    # tools = [TavilySearchResults(max_results=5, include_raw_content=True, include_images=True)]
    tools = [search_tool, save_memory]
    app = create_react_agent(model, tools, state_modifier=system_message, debug=True)
    return app
