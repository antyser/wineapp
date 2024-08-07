from typing import List, Optional

from pydantic import BaseModel, Field


class Wine(BaseModel):
    name: str = Field(description="The name of the wine")
    image: Optional[str] = Field(description="The image of the wine")
    region: Optional[str] = Field(description="The region of the wine")
    producer: Optional[str] = Field(description="The producer of the wine")
    vintage: Optional[str] = Field(description="The vintage of the wine")
    type: Optional[str] = Field(description="The type of the wine")


class Message(BaseModel):
    type: str
    content: str


class ChatRequest(BaseModel):
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
    message: str


class ExtractWineResponse(BaseModel):
    wines: List[Wine]
