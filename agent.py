import base64
import io
import operator
from typing import Annotated, Optional, Sequence, Dict, Any, TypedDict
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI
from langchain_community.tools.tavily_search import TavilySearchResults
from langchain_core.messages import BaseMessage, HumanMessage, ToolMessage
from langgraph.prebuilt import ToolExecutor, ToolInvocation
from langgraph.graph import StateGraph, START, END
import streamlit as st
from PIL import Image
from langchain_core.messages import HumanMessage

from tools import search_tool

class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], operator.add]


def create_workflow() -> StateGraph:

    # Initialize tools and model
    tools = [TavilySearchResults(max_results=5, include_raw_content=True, include_images=True)]
    # tools = [search_tool]
    tool_executor = ToolExecutor(tools)
    model = ChatOpenAI(model='gpt-4o-mini',temperature=1, streaming=True)
    model = model.bind_tools(tools)

    # Define the function that determines whether to continue or not
    def should_continue(state: AgentState) -> str:
        messages = state["messages"]
        last_message = messages[-1]
        return "end" if not last_message.tool_calls else "continue"

    # Define the function that calls the model
    def call_model(state: AgentState) -> Dict[str, Any]:
        messages = state["messages"]
        response = model.invoke(messages)
        return {"messages": [response]}

    # Define the function to execute tools
    def call_tool(state: AgentState) -> Dict[str, Any]:
        messages = state["messages"]
        last_message = messages[-1]
        tool_call = last_message.tool_calls[0]
        action = ToolInvocation(
            tool=tool_call["name"],
            tool_input=tool_call["args"],
        )
        response = tool_executor.invoke(action)
        function_message = ToolMessage(
            content=str(response), name=action.tool, tool_call_id=tool_call["id"]
        )
        return {"messages": [function_message]}

    # Define a new graph
    workflow = StateGraph(AgentState)

    # Define the two nodes we will cycle between
    workflow.add_node("agent", call_model)
    workflow.add_node("action", call_tool)

    # Set the entrypoint as `agent`
    workflow.add_edge(START, "agent")

    # Add a conditional edge
    workflow.add_conditional_edges(
        "agent",
        should_continue,
        {
            "continue": "action",
            "end": END,
        },
    )

    # Add a normal edge from `tools` to `agent`
    workflow.add_edge("action", "agent")

    # Compile the workflow
    return workflow.compile()


def build_human_message(text: str, image_bytes: Optional[bytes] = None) -> HumanMessage:
    # Create the base message content
    content = [{"type": "text", "text": text}]
    
    # If image_bytes is provided, encode it to base64 and add to content
    if image_bytes is not None:
        image_base64 = base64.b64encode(image_bytes).decode('utf-8')
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"},
        })
    
    # Create the HumanMessage with the constructed content
    message = HumanMessage(content=content)
    return message


def get_instruction(user_input, with_image=False):
    if not with_image:
        return f"""
        Your task is to find the wine(s) information (wine name, the retail price, links to purchase, wine region, winery, wine photo, and wine critic reviews).
        Extract the wine name from the user text input. The wine name has to be specific: usually it includes winery name, grape, region and vintage. Find the full wine name before sending it to search.
        If there's no wine name, ask the user to provide a wine name. If there are more than one, search each one by one.
        To run the search, you should use query as wine name + retail price and gather the information from it. 
        Here's the user text input: {user_input}"""
    else:
        return f"""
        Your task is to find the wine(s) information (wine name, the retail price, links to purchase, wine region, winery, wine photo, and wine critic reviews).
        Extract the wine name from the image. The wine name has to be specific: usually it includes winery name, grape, region and vintage. Find the full wine name before sending it to search.
        If there's no wine name, ask the user to provide a wine name. If there are more than one, search each one by one.
        To run the search, you should use query as wine name + retail price and gather the information from it. """


def main():
    # Load environment variables
    load_dotenv('.env')  # Load environment variables from a .env file

    # Create the workflow
    app = create_workflow()
    st.title("Wine Information Finder")

    # Initialize session state for messages
    if "messages" not in st.session_state:
        st.session_state["messages"] = [{"role": "assistant", "content": "Search by the wine name or upload the wine label to start."}]

    # Display chat messages
    for msg in st.session_state.messages:
        st.chat_message(msg["role"]).write(msg["content"])

    # Text input for wine query
    wine_query = st.text_input("Enter the wine name and details you want to find:")
    # Add file uploader for image
    uploaded_image = st.file_uploader("Upload an image of the wine label (optional)", type=["jpg", "jpeg", "png"])
    

    
    if st.button("Find Wine Information"):
        try:
            image_bytes = None
            if uploaded_image:
                # Process the uploaded image here
                image = Image.open(uploaded_image)
                st.image(image, caption="Uploaded Image", use_column_width=True)
                
                # Convert image to bytes
                img_byte_arr = io.BytesIO()
                image.save(img_byte_arr, format='PNG')
                image_bytes = img_byte_arr.getvalue()

            # Use the build_human_message function to create the message
            message = build_human_message(
                text=get_instruction(wine_query, image_bytes != None),
                image_bytes=image_bytes
            )

            # Append user message to session state
            st.session_state.messages.append({"role": "user", "content": wine_query})
            st.chat_message("user").write(wine_query)

            # Invoke the agent with the message
            with st.status('Running'):
                result = app.invoke({"messages": [message]})
            ai_message = result["messages"][-1].content
            st.session_state.messages.append({"role": "assistant", "content": ai_message})
            st.chat_message("assistant").write(ai_message)
        except Exception as e:
            st.error(f"Error: {e}")


def main_cmd(command: str, image_file: Optional[str] = None):
    # Load environment variables
    load_dotenv('.env')  # Load environment variables from a .env file

    # Create the workflow
    app = create_workflow()

    image_bytes = None
    if image_file:
        try:
            # Process the uploaded image file
            with Image.open(image_file) as image:
                # Convert image to bytes
                img_byte_arr = io.BytesIO()
                image.save(img_byte_arr, format='PNG')
                image_bytes = img_byte_arr.getvalue()
        except Exception as e:
            print(f"Error processing image: {e}")
            return

    # Use the build_human_message function to create the message
    message = build_human_message(
        text=get_instruction(command),
        image_bytes=image_bytes
    )

    # Invoke the agent with the message
    config = {"configurable": {"thread_id": "1"}}
    events = app.stream(
        {"messages": [message]}, config, stream_mode="values"
    )
    for event in events:
        if "messages" in event:
            event["messages"][-1].pretty_print()

if __name__ == "__main__":
    main()
