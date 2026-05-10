from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status
from sqlalchemy.orm import Session, joinedload

from src.api.schemas import EventCreate, EventDetail, EventOut
from src.db.models import Endpoint, Event
from src.db.session import get_db

router = APIRouter(prefix="/webhooks", tags=["Webhooks"])


@router.post("/events", response_model=EventOut)
def create_event(
        body: EventCreate,
        response: Response,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        db: Session = Depends(get_db),
):
    # ── resolve tenant from endpoint ──────────────────────────────────────────
    # We look up the endpoint so we know which tenant owns this event.
    endpoint = db.get(Endpoint, body.endpoint_id)
    if endpoint is None:
        raise HTTPException(status_code=404, detail="endpoint not found")

    tenant_id = endpoint.tenant_id

    # ── idempotency check (Stripe-style) ──────────────────────────────────────
    # If the caller sends the same Idempotency-Key for the same tenant a second
    # time, we return the original event instead of creating a duplicate.
    if idempotency_key:
        existing = (
            db.query(Event)
            .filter(
                Event.tenant_id == tenant_id,
                Event.idempotency_key == idempotency_key,
            )
            .first()
        )
        if existing:
            response.status_code = status.HTTP_200_OK  # 200, not 202
            return existing

    # ── create new event ──────────────────────────────────────────────────────
    event = Event(
        tenant_id=tenant_id,
        endpoint_id=body.endpoint_id,
        payload=body.payload,
        idempotency_key=idempotency_key,  # None if header not sent
    )
    db.add(event)
    db.commit()
    db.refresh(event)

    response.status_code = status.HTTP_202_ACCEPTED
    return event


@router.get("/events/{event_id}", response_model=EventDetail)
def get_event(
        event_id: UUID,
        db: Session = Depends(get_db),
):
    # joinedload tells SQLAlchemy to fetch attempts_log in the same query
    # so we don't get lazy-load errors outside a session.
    event = (
        db.query(Event)
        .options(joinedload(Event.attempts_log))
        .filter(Event.id == event_id)
        .first()
    )
    if event is None:
        raise HTTPException(status_code=404, detail="event not found")
    return event


# NEW: replay a dead event
@router.post("/events/{event_id}/replay", response_model=EventOut)
def replay_event(event_id: UUID, db: Session = Depends(get_db)):
    event = db.get(Event, event_id)
    if event is None:
        raise HTTPException(404, "event not found")
    if event.status != "dead":
        raise HTTPException(409, f"event is '{event.status}', only 'dead' events can be replayed")
    event.status = "pending"
    event.attempts = 0
    event.next_attempt_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(event)
    return event


# NEW: list dead-letter queue
@router.get("/dlq", response_model=list[EventOut])
def list_dlq(limit: int = 50, db: Session = Depends(get_db)):
    return (
        db.query(Event)
        .filter(Event.status == "dead")
        .order_by(Event.created_at.desc())
        .limit(limit)
        .all()
    )
