from typing import Dict, List, Optional

from pydantic import BaseModel


class Message(BaseModel):
    type: str
    content: str


class ChatRequest(BaseModel):
    text: Optional[str] = None
    image_bytes: Optional[bytes] = None
    history: Optional[List[Message]] = None


class ChatResponse(BaseModel):
    messages: List[Dict]
