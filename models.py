from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class Message(BaseModel):
    type: str
    content: str


class ChatRequest(BaseModel):
    text: Optional[str] = None
    image_bytes: Optional[bytes] = None
    history: Optional[List[Message]] = None


class ChatResponse(BaseModel):
    messages: List[Dict]


class FollowupRequest(BaseModel):
    context: str
    n: int = 3


class FollowupResponse(BaseModel):
    questions: List[str] = Field(description="List of follow-up questions")
