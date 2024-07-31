from typing import List, Optional

from pydantic import BaseModel, Field


class Message(BaseModel):
    type: str
    content: str


class ChatRequest(BaseModel):
    text: Optional[str] = None
    base64_image: Optional[str] = None
    history: Optional[List[Message]] = None


class ChatResponse(BaseModel):
    messages: List[str]


class FollowupRequest(BaseModel):
    context: str
    n: int = 3


class FollowupResponse(BaseModel):
    questions: List[str] = Field(description="List of follow-up questions")
