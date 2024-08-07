from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger

from agents.agent import create_agent
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
    try:
        if (
            request.text
            and request.text.lower() == "hello"
            and not request.base64_image
        ):

            async def hello_stream():
                yield "How can I assist you with wine information today?\n"

            return StreamingResponse(hello_stream(), media_type="text/plain")

        agent = create_agent()
        logger.info(request)
        input_messages = build_input_messages(
            text=request.text,
            base64_image=request.base64_image,
            history=request.history,
        )

        logger.info(input_messages)

        async def event_stream():
            async for event in agent.astream_events(
                {"messages": input_messages}, stream_mode="values", version="v2"
            ):
                kind = event["event"]
                if kind == "on_chat_model_stream":
                    content = event["data"]["chunk"].content
                    if content:
                        yield content
            yield "\n"

        return StreamingResponse(event_stream(), media_type="text/plain")
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
