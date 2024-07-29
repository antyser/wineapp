from fastapi import FastAPI, HTTPException
from loguru import logger
from modal import App, Image, Secret, asgi_app

from agents.agent import create_agent
from llm.gen_followup import generate_followup_questions
from main import build_input_messages
from models import ChatRequest, ChatResponse, FollowupRequest, FollowupResponse

app = FastAPI()

modal_app = App(name="wineapp")

image = Image.debian_slim(python_version="3.12").poetry_install_from_file(
    "pyproject.toml", without=["dev"]
)
secret = Secret.from_dotenv(".env")


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


@app.post("/followups", response_model=FollowupResponse)
async def followups(request: FollowupRequest):
    try:
        followup_questions = generate_followup_questions(request.context, request.n)
        if followup_questions:
            response = FollowupResponse(questions=followup_questions)
            return response
        else:
            raise HTTPException(
                status_code=500, detail="Failed to generate follow-up questions"
            )
    except Exception as e:
        logger.error(f"Error generating follow-up questions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@modal_app.function(image=image, secrets=[secret])
@asgi_app()
def fastapi_app():
    return app


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
