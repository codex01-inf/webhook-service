from fastapi import APIRouter, status, Depends, HTTPException
from sqlalchemy.orm import Session

from src.db.models import Endpoint
from src.api.schemas import *
from src.db.session import get_db

router = APIRouter(prefix="/endpoint", tags=["Registration"])


@router.post("", status_code=status.HTTP_201_CREATED, response_model=EndpointOut)
def create_endpoint(
        body: EndpointCreate,
        db: Session = Depends(get_db)
):
    ep = Endpoint(tenant_id=body.tenant_id, url=str(body.url))
    db.add(ep)
    db.commit()
    db.refresh(ep)
    return ep


@router.post("/{endpoint_id}", response_model=EndpointOut)
def get_endpoint(
        endpoint_id: UUID,
        db: Session = Depends(get_db)
):
    ep = db.get(Endpoint, endpoint_id)

    if ep is None:
        raise HTTPException(status_code=404, detail="endpoint not found")
    return ep

