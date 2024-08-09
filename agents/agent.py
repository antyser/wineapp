from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from tools.recommendation import save_memory, search_personal_memory
from tools.search import search_tool

# parser = PydanticOutputParser(pydantic_object=Followups)


def create_agent():
    model = ChatOpenAI(model="gpt-4o-mini", temperature=0.7, streaming=True)

    system_message = """Act as a knowledgeable sommelier. 
    Your job is to answer any questions related to wines include but not limit to wine basic information, regions, pairing, etc.
    - When given a wine name(s), find the relevant wine information including the wine name, retail price, links to purchase, wine region, winery, wine photo, and wine critic reviews. Use the search tool and search by the wine name wine name + wine searcher as the search term.
    Otherwise, use the search tool with the query you feel appropriate.
    - If you are provided with preference or fact, save it to memory.
    - If you are asked for a wine recommendation, looking up the personal memory for the preference if available. If not available, ask the users for clarification before giving an answer.
    Do not restate or appreciate what I say.
    Always be as efficient as possible when providing information or making recommendations.
    """

    # tools = [TavilySearchResults(max_results=5, include_raw_content=True, include_images=True)]
    tools = [search_tool, search_personal_memory, save_memory]
    app = create_react_agent(model, tools, state_modifier=system_message, debug=True)
    return app
