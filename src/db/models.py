import uuid
import secrets
from datetime import datetime
from sqlalchemy import String, Integer, ForeignKey, JSON, DateTime, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Endpoint(Base):
    __tablename__ = "endpoints"
    id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[str] = mapped_column(String, index=True)
    url: Mapped[str] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    signing_secret: Mapped[str] = mapped_column(String, default=lambda: secrets.token_hex(32))

    events: Mapped[list["Event"]] = relationship(back_populates="endpoint")


class Event(Base):
    __tablename__ = "events"
    id: Mapped[uuid.UUID] = mapped_column(UUID, primary_key=True, default=uuid.uuid4)
    tenant_id: Mapped[str] = mapped_column(String, index=True)
    endpoint_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("endpoints.id"))
    payload: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    idempotency_key: Mapped[str | None] = mapped_column(String, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    endpoint: Mapped["Endpoint"] = relationship(back_populates="events")
    attempts_log: Mapped[list["DeliveryAttempt"]] = relationship(back_populates="event")


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempts"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("events.id"))
    attempt_number: Mapped[int] = mapped_column(Integer)
    status_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error: Mapped[str | None] = mapped_column(String, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    attempted_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    event: Mapped["Event"] = relationship(back_populates="attempts_log")
