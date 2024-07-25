from typing import List, Optional, Union
from langchain_core.messages import SystemMessage
from langgraph.prebuilt import create_react_agent
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from langchain_community.tools.tavily_search import TavilySearchResults
from langgraph.prebuilt import ToolExecutor, ToolInvocation
import io  # Added import for io
from PIL import Image
from agent import create_agent  # Added import for Image

from tools import search
from langgraph.checkpoint import MemorySaver

from fastapi import FastAPI, HTTPException
from langserve import add_routes
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from langchain_core.pydantic_v1 import BaseModel


class ChatInputType(BaseModel):
    input: List[Union[HumanMessage, AIMessage, SystemMessage]]


app = FastAPI(
    title="LangChain Server",
    version="1.0",
    description="A simple API server using Langchain's Runnable interfaces",
)

# Set all CORS enabled origins
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
)

agent = create_agent()
runnable = agent.with_types(input_type=ChatInputType, output_type=dict)

add_routes(app, runnable, path="/chat", playground_type="chat")

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="localhost", port=8000)
