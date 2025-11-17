from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


class StartProducerRequest(BaseModel):
    producer_id: str = Field(..., description="Unique identifier for this producer instance")
    interval_seconds: float = Field(1.0, gt=0, description="Seconds between events")
    jitter_seconds: float = Field(
        0.5, ge=0, description="Maximum random jitter added to each interval"
    )
    payload_template: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional base payload that will be merged into each event",
    )
    topic: Optional[str] = Field(
        default=None, description="Optional topic/stream name; defaults to settings"
    )

    @validator("producer_id")
    def validate_id(cls, value: str) -> str:  # noqa: N805
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("producer_id cannot be empty")
        return cleaned


class ProducerStatus(BaseModel):
    producer_id: str
    running: bool
    interval_seconds: float
    jitter_seconds: float
    started_at: datetime
    last_sent_at: Optional[datetime]
    messages_sent: int
    topic: str
    recent_events: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Recent sample events mirrored for the UI stream",
    )


class ManualAlertRequest(BaseModel):
    message: str = Field(..., description="Human-readable alert body")
    severity: str = Field(
        "warning",
        description="Severity flag sent to Zerobus; e.g. info|warning|critical",
    )
    producer_id: Optional[str] = Field(
        default=None, description="Optional producer id to attribute the alert to"
    )
    topic: Optional[str] = Field(
        default=None, description="Optional topic/stream name; defaults to settings"
    )

