from fastapi import APIRouter, status, Depends
from sqlalchemy.orm import Session

from src.db.models import Event
from src.api.schemas import *
from src.db.session import get_db

router = APIRouter(prefix="/webhooks", tags=["Webhooks"])


@router.post("/events", status_code=status.HTTP_202_ACCEPTED, response_model=EventOut)
def create_event(
        body: EventCreate,
        # idempotency_key: str | None = Header(default=None),
        db: Session = Depends(get_db),
):
    event = Event(
        tenant_id="...",
        endpoint_id=body.endpoint_id,
        payload=body.payload,
        # idempotency_key=idempotency_key,
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event
