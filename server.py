from fastapi import FastAPI
from loguru import logger

from agents.agent import create_agent
from main import build_input_messages
from models import ChatRequest, ChatResponse

app = FastAPI()


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    logger.info(f"Received request: {request.json()}")
    # Create the agent
    agent = create_agent()
    logger.info(request)
    # Build the human message
    input_messages = build_input_messages(
        text=request.text, image_bytes=request.image_bytes, history=request.history
    )

    # Configuration for the agent
    logger.info(input_messages)
    # Call the agent's stream function
    event = agent.invoke({"messages": input_messages}, stream_mode="values")
    # Collect the messages from the events
    messages = []
    print(event)
    if "messages" in event:
        for msg in event["messages"]:
            print(msg)
            print(msg.dict())
            messages.append(msg.dict())  # Convert message objects to dictionaries

    response = ChatResponse(messages=messages)
    logger.info(f"Response: {response.json()}")
    return response


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
