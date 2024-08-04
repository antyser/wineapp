from typing import List, Optional

from langchain_community.tools.tavily_search import TavilySearchResults
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain_openai import ChatOpenAI
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.prebuilt import create_react_agent

from tools.search import search_tool


class Wine(BaseModel):
    name: str = Field(description="The name of the wine")
    image: Optional[str] = Field(description="The image of the wine")


class WineOutput(BaseModel):
    text: str = Field(description="The text output")
    wines: Optional[List[Wine]] = Field(
        description="The wines referred in the converstation"
    )


# parser = PydanticOutputParser(pydantic_object=Followups)


def create_agent():
    model = ChatOpenAI(model="gpt-4o-mini", temperature=0.7, streaming=True)

    system_message = """Act as a knowledgeable wine assistant. 
    Your job is to answer any questions related to wines include but not limit to wine basic information, regions, pairing, etc.

    When given a wine name(s) or a wine image only, find the relevant wine information including the wine name, retail price, links to purchase, wine region, winery, wine photo, and wine critic reviews. Use the search tool and search by the wine name.

    If you are only given a wine name, first try to use the search tool by querying wine name + wine searcher as the search term.

    Otherwise, use the search tool with the query you feel appropriate.

    Do not restate or appreciate what I say.

    Always be as efficient as possible when providing information or making recommendations.

    If the response text contains wines, put the wine name and image separately in the wines field. Output the information in the following JSON format:
    {
        "text": "The detailed output to address the question.",
        "wines(optional)": [
            {
                "name": "Wine Name",
                "image(optional)": "Image URL"
            },
        ]
    }


    """

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
