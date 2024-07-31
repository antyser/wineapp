import base64
import datetime
import io
from typing import Any, Dict, List, Optional, Union

import streamlit as st
from dotenv import load_dotenv  # Added import for load_dotenv
from langchain_core.messages import AIMessage, HumanMessage
from PIL import Image  # Added import for Image

from agents.agent import create_agent
from models import Message


def build_input_messages(
    text: Optional[str] = None,
    base64_image: Optional[str] = None,
    history: Optional[List[Message]] = None,
) -> List[Union[HumanMessage, AIMessage]]:
    # Create the base message content
    content: List[Union[str, Dict[str, Any]]] = []

    if text is not None:
        content.append({"type": "text", "text": text})

    if base64_image:
        content.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
            }
        )

    # Create the HumanMessage with the constructed content
    message = HumanMessage(content=content)

    history_messages = [
        (
            HumanMessage(content=msg.content)
            if msg.type == "human"
            else AIMessage(content=msg.content)
        )
        for msg in (history or [])
    ]

    return history_messages + [message]


def main():
    # Load environment variables
    load_dotenv(".env")  # Load environment variables from a .env file

    # Create the workflow
    app = create_agent()
    st.title("Wine Information Finder")

    # Initialize session state for messages
    if "messages" not in st.session_state:
        st.session_state["messages"] = [
            {
                "role": "assistant",
                "content": "Search by the wine name or upload the wine label to start.",
            }
        ]

    # Display chat messages
    for msg in st.session_state.messages:
        st.chat_message(msg["role"]).write(msg["content"])

    # Text input for wine query
    wine_query = st.chat_input("Enter the wine name and details you want to find:")

    # Add file uploader for image
    uploaded_image = st.file_uploader(
        "Upload an image of the wine label (optional)", type=["jpg", "jpeg", "png"]
    )
    config = {
        "configurable": {
            "thread_id": "1",
            "thread_ts": datetime.datetime.now(datetime.UTC),
        }
    }
    if st.button("Find Wine Information") or wine_query:
        try:
            image_bytes = None
            if uploaded_image:
                # Process the uploaded image here
                image = Image.open(uploaded_image)
                st.image(image, caption="Uploaded Image", use_column_width=True)

                # Convert image to bytes
                img_byte_arr = io.BytesIO()
                image.save(img_byte_arr, format="PNG")
                image_bytes = img_byte_arr.getvalue()

            # Use the build_human_message function to create the message
            message = build_input_messages(text=wine_query, base64_image=image_bytes)

            # Append user message to session state
            st.session_state.messages.append(
                {"role": "user", "content": wine_query or "Image uploaded"}
            )
            st.chat_message("user").write(wine_query or "Image uploaded")

            # Invoke the agent with the message
            with st.chat_message("assistant"):
                events = app.stream(
                    {"messages": [message]}, config, stream_mode="values"
                )
                for event in events:
                    if "messages" in event:
                        if event["messages"][-1].type == "ai":
                            st.write(event["messages"][-1].content)
                        event["messages"][-1].pretty_print()
            ai_message = event["messages"][-1].content
            st.session_state.messages.append(
                {"role": "assistant", "content": ai_message}
            )
        except Exception as e:
            st.error(f"Error: {e}")


def print_stream(stream):
    for s in stream:
        message = s["messages"][-1]
        if isinstance(message, tuple):
            print(message)
        else:
            message.pretty_print()


def main_cmd(command: Optional[str] = None, image_file: Optional[str] = None):
    # Load environment variables
    load_dotenv(".env")  # Load environment variables from a .env file

    agent = create_agent()
    print(agent.input_schema.schema())

    if image_file:
        try:
            # Process the uploaded image file
            with open(image_file, "rb") as image_file:  # type: ignore
                encoded_image = base64.b64encode(image_file.read()).decode("utf-8")  # type: ignore
        except Exception as e:
            print(f"Error processing image: {e}")
            return

    # Use the build_human_message function to create the message
    message = build_input_messages(text=command, base64_image=encoded_image)
    # Invoke the agent with the message
    config = {
        "configurable": {
            "thread_id": 1,
            "thread_ts": datetime.datetime.now(datetime.UTC),
        }
    }
    print_stream(
        agent.stream({"messages": message}, config=config, stream_mode="values")
    )


if __name__ == "__main__":
    # main()
    main_cmd("2010 opus one")
