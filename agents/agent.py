from typing import List, Optional

from langchain_openai import ChatOpenAI
from langgraph.prebuilt import create_react_agent

from core.wine.model import Wine
from core.wine.wine_searcher import wines_to_csv
from tools.recommendation import save_memory, search_personal_memory_v2
from tools.search import batch_search_wines_tool


def somm_agent(user_id: Optional[str] = None, wines: Optional[List[Wine]] = None):
    wine_info = wines_to_csv(wines) if wines else ""
    search_memory = search_personal_memory_v2("preference", user_id) if user_id else ""
    model = ChatOpenAI(model="gpt-4o-mini", temperature=0.7, streaming=True)

    system_message = f"""You are an expert sommelier AI assistant. Your primary tasks are:

1. Provide insightful analysis and recommendations based on the given wine information.
2. Answer specific questions about wine characteristics, regions, and food pairings.
3. Offer personalized recommendations considering user preferences.

Guidelines:
- Reference the provided wine information. They are latest information online in CSV formt: <wine_info>{wine_info}</wine_info>
- Consider user preferences: <preference>{search_memory}</preference>
- Use the save_memory tool for new user preferences or facts.
- Be concise and avoid restating basic wine information already provided.
- Focus on unique insights, comparisons, and expert recommendations.

Response format:
- For recommendations: Suggest 1-2 options with brief, insightful explanations.
- For analysis: Provide unique perspectives or comparisons between wines.
- For food pairings: Suggest 1-2 specific dishes, explaining the pairing logic.

Key points:
- Emphasize expert insights not obvious from basic wine data.
- Tailor advice to user preferences when applicable.
- Be direct and efficient in your responses.
- If asked about unavailable information, clearly state so and offer related insights if possible.

Your goal is to provide expert, tailored advice that goes beyond the basic information already available to the user.
"""

    tools = [save_memory]
    app = create_react_agent(model, tools, state_modifier=system_message, debug=True)
    return app


def wine_search_agent(user_id: Optional[str] = None):
    search_memory = search_personal_memory_v2("preference", user_id) if user_id else ""
    model = ChatOpenAI(model="gpt-4o-mini", temperature=0.7, streaming=True)

    system_message = f"""Act as a knowledgeable sommelier. 
    Your are given one or more wine names, use the batch_search_wines tool to search for the wine information. 
    - Provide the compehensive wine name, usually a wine name include vintage, winery, region, and varietal.
    - If you are provided with preference or fact, save it to memory.
    Do not restate or appreciate what I say.
    Always be as efficient as possible when providing information or making recommendations.
    Here are the user preference: {search_memory}. You should take it into consideration when providing information or making recommendations.
    """

    tools = [batch_search_wines_tool]
    app = create_react_agent(model, tools, state_modifier=system_message, debug=True)
    return app
