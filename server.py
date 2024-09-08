import time

import orjson
import sentry_sdk
from fastapi import FastAPI, HTTPException
from loguru import logger
from sse_starlette.sse import EventSourceResponse

from agents.agent import somm_agent
from core.users.service import delete_user
from core.wines.wine_searcher import batch_fetch_wines
from llm.extract_wines import extract_wines, extract_wines_llm
from llm.gen_followup import generate_followups
from main import build_input_messages
from models import (
    ChatRequest,
    ExtractWineRequest,
    ExtractWineResponse,
    FollowupRequest,
    FollowupResponse,
)

sentry_sdk.init(
    dsn="https://a767b779feb7c6c6265dd37f1cebe3f1@o4507757109903360.ingest.us.sentry.io/4507757112066048",
    traces_sample_rate=1.0,
    profiles_sample_rate=1.0,
)

app = FastAPI()


@app.post("/stream_chat")
async def stream_chat(request: ChatRequest):
    start_time = time.time()
    if not request.text and not request.base64_image:
        raise HTTPException(
            status_code=400, detail="Either text or base64_image must be provided"
        )
    try:

        async def event_stream():
            try:
                result = extract_wines_llm(request.text, request.base64_image)
                wines = {}
                if result.has_wine:
                    wine_names = list(dict.fromkeys(result.dict().get("wines", [])))
                    wine_names_str = "\n".join([f"• {wine}" for wine in wine_names])
                    event = orjson.dumps(
                        {"msg": f"Searching wine information for:\n{wine_names_str}"}
                    ).decode("utf-8")
                    yield f"{event}\n\n"
                    for i in range(0, len(wine_names), 10):
                        batch = wine_names[i : i + 10]
                        logger.info(f"batch: {batch}")
                        wines_batch = await batch_fetch_wines(batch, is_pro=True)
                        wines.update(wines_batch)
                        wine_dicts = [
                            wine.model_dump()
                            for wine in wines_batch.values()
                            if wine is not None
                        ]
                        logger.info(f"wine_dicts: {wine_dicts}")
                        wine_json = orjson.dumps({"wines": wine_dicts}).decode("utf-8")
                        yield f"{wine_json}\n\n"
                    logger.info(
                        f"time to extract wines: {time.time() - start_time:.2f} seconds"
                    )

                if not result.need_further_action and wines:
                    return
                else:
                    agent = somm_agent(request.user_id, wines)
                    input_messages = build_input_messages(
                        text=request.text,
                        base64_image=request.base64_image,
                        history=request.history,
                    )

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
                                event = orjson.dumps({"msg": data}).decode("utf-8")
                                yield f"{event}\n\n"
                    time_to_stream_end = time.time() - start_time
                    logger.info(
                        f"Time to end streaming: {time_to_stream_end:.2f} seconds"
                    )
            except Exception as e:
                logger.error(f"Error when streaming: {e}")
                raise e
            finally:
                yield "[DONE]"

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
        if not request.message and not request.image_url:
            raise HTTPException(
                status_code=400, detail="Either message or image_url must be provided"
            )
        wines, _ = await extract_wines(request.message, request.image_url)
        logger.info(f"Extracted wines: {wines}")
        return ExtractWineResponse(wines=wines)
    except Exception as e:
        logger.error(f"Error extracting wines: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.delete("/user/{user_id}", status_code=204)
async def delete_user_endpoint(user_id: str):
    try:
        await delete_user(user_id)
        return {"message": "User deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting user: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)
