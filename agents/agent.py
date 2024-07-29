from langchain_community.tools.tavily_search import TavilySearchResults
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.prebuilt import create_react_agent

from tools.search import search_tool


def create_agent():
    model = ChatOpenAI(model="gpt-4o-mini", temperature=1, streaming=True)

    system_message = """Act as a knowledgeable wine assistant. 
    Your job is to answer any questions related to wines include but not limit to wine basic information, regions, pairing, etc.

    When given a wine name(s) or a wine image only, find the relevant wine information including the wine name, retail price, links to purchase, wine region, winery, wine photo, and wine critic reviews. Use the search tool and search by the wine name.

    If you are only given a wine name, use the search tool to find the wine relevant information. You may also include additional relevant responses to questions, remarks, or statements provided they are truthful.

    Do not restate or appreciate what I say.

    Always be as efficient as possible when providing information or making recommendations.

    If asked specific questions, feel free to use the search tool."""

    # tools = [TavilySearchResults(max_results=5, include_raw_content=True, include_images=True)]
    tools = [search_tool]
    app = create_react_agent(model, tools, state_modifier=system_message)
    return app


def test_agent():
    model = ChatOpenAI(model="gpt-4o-mini")

    system_message = """Act as a knowledgeable assistant."""

    tools = [
        TavilySearchResults(
            max_results=5, include_raw_content=True, include_images=True
        )
    ]
    # tools = [search_tool]
    memory = SqliteSaver.from_conn_string(":memory:")
    app = create_react_agent(
        model, tools, state_modifier=system_message, checkpointer=memory
    )
    return app
