from typing import List, Optional

from pydantic import BaseModel


class Offer(BaseModel):
    price: float
    unit_price: float
    description: Optional[str] = None
    seller_name: Optional[str] = None
    url: Optional[str] = None
    name: Optional[str] = None
    seller_address_region: Optional[str] = None
    seller_address_country: Optional[str] = None


class Wine(BaseModel):
    id: str
    wine_searcher_id: int
    description: Optional[str] = None
    vintage: int
    name: Optional[str]
    url: Optional[str]
    region: Optional[str]
    region_image: Optional[str]
    origin: Optional[str]
    grape_variety: Optional[str]
    image: Optional[str]
    producer: Optional[str]
    average_price: Optional[float]
    min_price: Optional[float]
    wine_type: Optional[str]
    wine_style: Optional[str]
    offers: Optional[List[Offer]]
    offers_count: Optional[int] = None
