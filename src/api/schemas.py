from pydantic import BaseModel, ConfigDict
from uuid import UUID
from datetime import datetime


class EventCreate(BaseModel):
    endpoint_id: UUID
    payload: dict


class EventOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)  # lets you do EventOut.model_validate(orm_obj)
    id: UUID
    status: str
    attempts: int
    idempotency_key: str | None = None
    created_at: datetime


class AttemptOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    attempt_number: int
    status_code: int | None
    error: str | None
    duration_ms: int | None
    attempted_at: datetime


class EventDetail(EventOut):
    model_config = ConfigDict(from_attributes=True)
    attempts_log: list[AttemptOut]


class EndpointCreate(BaseModel):
    tenant_id: str
    url: str


class EndpointOut(BaseModel):
    id: UUID
    tenant_id: str
    url: str
    created_at: datetime
