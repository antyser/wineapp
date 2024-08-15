import time

import sentry_sdk
from fastapi import FastAPI, HTTPException
from loguru import logger
from sse_starlette.sse import EventSourceResponse

from agents.agent import create_agent
from core.users.service import delete_user
from llm.gen_followup import generate_followups
from llm.structure_wine import extact_wines
from main import build_input_messages
from models import (
    ChatRequest,
    ChatResponse,
    ExtractWineRequest,
    ExtractWineResponse,
    FollowupRequest,
    FollowupResponse,
)

sentry_sdk.init(
    dsn="https://a767b779feb7c6c6265dd37f1cebe3f1@o4507757109903360.ingest.us.sentry.io/4507757112066048",
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=1.0,
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=1.0,
)

app = FastAPI()


@app.post("/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    logger.info(f"Received request: {request.json()}")
    try:
        if (
            request.text
            and request.text.lower() == "hello"
            and not request.base64_image
        ):
            return ChatResponse(
                messages=["How can I assist you with wine information today?"]
            )
        agent = create_agent()
        logger.info(request)
        input_messages = build_input_messages(
            text=request.text,
            base64_image=request.base64_image,
            history=request.history,
        )

        logger.info(input_messages)
        event = agent.invoke({"messages": input_messages}, stream_mode="values")
        logger.info(event)
        message = event["messages"][-1].content
        try:
            wines = extact_wines(message)
            response = ChatResponse(messages=[message], wines=wines)
        except Exception as e:
            logger.error(f"Error parsing response: {e}")
            response = ChatResponse(messages=[message])
        logger.info(f"Response: {response.json()}")
        return response
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.post("/stream_chat")
async def stream_chat(request: ChatRequest):
    logger.info(f"Received request: {request.json()}")
    start_time = time.time()  # Start the timer

    try:
        agent = create_agent(request.user_id)
        logger.info(request)
        input_messages = build_input_messages(
            text=request.text,
            base64_image=request.base64_image,
            history=request.history,
        )

        logger.info(input_messages)

        async def event_stream():
            first_yield = True
            async for event in agent.astream_events(
                {"messages": input_messages},
                {"configurable": {"user_id": request.user_id}},
                stream_mode="values",
                version="v2",
            ):
                kind = event["event"]
                if kind == "on_chat_model_stream":
                    data = event["data"]["chunk"].content
                    if data:
                        if first_yield:
                            time_to_stream_start = time.time() - start_time
                            logger.info(
                                f"Time to start streaming: {time_to_stream_start:.2f} seconds"
                            )
                            first_yield = False
                        yield "^" + data
            yield "[DONE]"
            time_to_stream_end = time.time() - start_time
            logger.info(f"Time to end streaming: {time_to_stream_end:.2f} seconds")

        return EventSourceResponse(event_stream())
    except Exception as e:
        logger.error(f"Error in stream_chat endpoint: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.post("/followups", response_model=FollowupResponse)
async def followups(request: FollowupRequest):
    try:
        followups = generate_followups(request.context, request.n)
        logger.info(followups)
        return FollowupResponse(**followups)
    except Exception as e:
        logger.error(f"Error generating follow-up questions: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/extract_wine", response_model=ExtractWineResponse)
async def extract_wine(request: ExtractWineRequest):
    try:
        wines = extact_wines(request.message)
        logger.info(f"Extracted wines: {wines}")
        return ExtractWineResponse(wines=wines)
    except Exception as e:
        logger.error(f"Error extracting wines: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.delete("/user/{user_id}", status_code=204)
async def delete_user_endpoint(user_id: str):
    try:
        delete_user(user_id)
        return {"message": "User deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
