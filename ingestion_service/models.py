"""
Pydantic models for the SocialSphere product event schema. Used by the
FastAPI ingestion service to validate every incoming event before it's
written to the bronze layer.
"""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field, field_validator, ConfigDict

VALID_EVENT_TYPES = {
    "user_signup",
    "user_login",
    "profile_view",
    "post_created",
    "post_liked",
    "post_shared",
    "comment_created",
    "video_viewed",
    "message_sent",
    "friend_request_sent",
    "ad_impression",
    "ad_click",
    "purchase_completed",
    "subscription_started",
    "subscription_cancelled",
    "app_error",
    "session_started",
    "session_ended",
}


class EventMetadata(BaseModel):
    model_config = ConfigDict(extra="allow")  # tolerate additional ad-hoc metadata fields

    experiment_group: Optional[str] = None
    referrer: Optional[str] = None


class ProductEvent(BaseModel):
    event_id: str = Field(..., min_length=1)
    user_id: str = Field(..., min_length=1)
    session_id: str = Field(..., min_length=1)
    event_type: str
    event_timestamp: datetime
    platform: str
    device_type: str
    country: str
    city: str
    app_version: str
    page: Optional[str] = None
    feature: Optional[str] = None
    post_id: Optional[str] = None
    creator_id: Optional[str] = None
    duration_seconds: Optional[int] = Field(default=None, ge=0)
    revenue: float = Field(default=0.0, ge=0.0)
    metadata: Optional[EventMetadata] = None

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, v: str) -> str:
        if v not in VALID_EVENT_TYPES:
            raise ValueError(f"event_type '{v}' is not a recognized event type")
        return v

    @field_validator("event_timestamp")
    @classmethod
    def validate_not_future(cls, v: datetime) -> datetime:
        # Allow a small clock-skew buffer (60s) but reject clearly-future events
        now = datetime.now(v.tzinfo) if v.tzinfo else datetime.utcnow()
        if v > now and (v - now).total_seconds() > 60:
            raise ValueError("event_timestamp cannot be in the future")
        return v


class EventBatch(BaseModel):
    events: list[ProductEvent] = Field(..., min_length=1, max_length=10_000)


class IngestResponse(BaseModel):
    message: str
    event_id: Optional[str] = None
    accepted: Optional[int] = None
    rejected: Optional[int] = None
