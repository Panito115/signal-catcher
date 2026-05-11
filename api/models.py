"""Pydantic models for the three ad event types ingested by the API."""

from typing import Any
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Nested models — Impression
# ---------------------------------------------------------------------------

class Advertiser(BaseModel):
    advertiser_id: str
    advertiser_name: str


class Campaign(BaseModel):
    campaign_id: str
    campaign_name: str


class Ad(BaseModel):
    ad_id: str
    ad_name: str
    ad_text: str
    ad_link: str
    ad_position: int
    ad_format: str


class AdEntry(BaseModel):
    advertiser: Advertiser
    campaign: Campaign
    ad: Ad


class ImpressionEvent(BaseModel):
    impression_id: str
    user_ip: str
    user_agent: str
    timestamp: str
    state: str
    search_keywords: str
    session_id: str
    ads: list[AdEntry]


# ---------------------------------------------------------------------------
# Nested models — Click
# ---------------------------------------------------------------------------

class ClickCoordinates(BaseModel):
    x: float
    y: float
    normalized_x: float
    normalized_y: float


class ClickedAd(BaseModel):
    ad_id: str
    ad_position: int
    click_coordinates: ClickCoordinates
    time_to_click: float


class ClickUserInfo(BaseModel):
    user_ip: str
    state: str
    session_id: str


class ClickEvent(BaseModel):
    click_id: str
    impression_id: str
    timestamp: str
    clicked_ad: ClickedAd
    user_info: ClickUserInfo


# ---------------------------------------------------------------------------
# Nested models — Conversion
# ---------------------------------------------------------------------------

class AttributionInfo(BaseModel):
    time_to_convert: int
    attribution_model: str


class ConversionUserInfo(BaseModel):
    user_ip: str
    state: str
    session_id: str


class ConversionEvent(BaseModel):
    conversion_id: str
    click_id: str
    impression_id: str
    timestamp: str
    conversion_type: str
    conversion_value: float
    conversion_currency: str
    conversion_attributes: dict[str, Any]
    attribution_info: AttributionInfo
    user_info: ConversionUserInfo
