import base64  # Added import for base64
import datetime
import io
from typing import Any, Dict, List, Optional, Union

import streamlit as st
from dotenv import load_dotenv  # Added import for load_dotenv
from langchain_core.messages import HumanMessage
from PIL import Image  # Added import for Image

from agents.agent import create_agent


def build_human_message(
    text: Optional[str] = None, image_bytes: Optional[bytes] = None
) -> HumanMessage:
    # Create the base message content
    content: List[Union[str, Dict[str, Any]]] = []

    # If text is provided, add it to the content
    if text is not None:
        content.append({"type": "text", "text": text})

    # If image_bytes is provided, encode it to base64 and add to content
    if image_bytes is not None:
        image_base64 = base64.b64encode(image_bytes).decode("utf-8")
        content.append(
            {
                "type": "image_url",
                "image_url": {"url": f"data:image/jpeg;base64,{image_base64}"},
            }
        )

    # Create the HumanMessage with the constructed content
    message = HumanMessage(content=content)
    return message


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
            message = build_human_message(text=wine_query, image_bytes=image_bytes)

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

    image_bytes = None
    if image_file:
        try:
            # Process the uploaded image file
            with Image.open(image_file) as image:
                # Convert image to bytes
                img_byte_arr = io.BytesIO()
                image.save(img_byte_arr, format="PNG")
                image_bytes = img_byte_arr.getvalue()
        except Exception as e:
            print(f"Error processing image: {e}")
            return

    # Use the build_human_message function to create the message
    message = build_human_message(text=command, image_bytes=image_bytes)
    print(message)
    # Invoke the agent with the message
    config = {
        "configurable": {
            "thread_id": 1,
            "thread_ts": datetime.datetime.now(datetime.UTC),
        }
    }
    print_stream(agent.stream(message, config=config, stream_mode="values"))


if __name__ == "__main__":
    # main()
    main_cmd("2010 opus one")
