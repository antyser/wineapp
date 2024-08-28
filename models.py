from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from core.wine.model import Wine


class Message(BaseModel):
    type: str
    content: str


class ChatRequest(BaseModel):
    user_id: Optional[str] = None
    text: Optional[str] = None
    base64_image: Optional[str] = None
    history: Optional[List[Message]] = None


class ChatResponse(BaseModel):
    messages: List[str]
    wines: Optional[List[Wine]] = None


class FollowupRequest(BaseModel):
    context: str
    n: int = 3


class FollowupResponse(BaseModel):
    followups: List[str] = Field(description="List of follow-up questions")
    wines: Optional[List[Wine]] = Field(description="The wines referred in the context")


class ExtractWineRequest(BaseModel):
    message: Optional[str] = None
    image_url: Optional[str] = None


class ExtractWineResponse(BaseModel):
    wines: Dict[str, Optional[Wine]] = Field(
        description="a dictionary of wine names and their corresponding wines"
    )
