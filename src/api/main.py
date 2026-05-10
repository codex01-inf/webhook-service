from fastapi import FastAPI

from src.api.routes import events, endpoints
from src.db import models
from src.db.session import engine
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST, Gauge
from fastapi import Response
from sqlalchemy import text
from src.db.session import SessionLocal  # sync session

app = FastAPI()

# Create tables on startup
# @app.on_event("startup")
# def startup():
#     models.Base.metadata.create_all(bind=engine)


# Plug in the routers
app.include_router(events.router)
app.include_router(endpoints.router)

QUEUE_DEPTH = Gauge("webhook_queue_depth", "Pending events waiting for delivery")


@app.get("/")
async def root():
    return {"message": "mizu to gohan kudasai."}


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/metrics")
def metrics():
    with SessionLocal() as db:
        count = db.execute(
            text("SELECT count(*) FROM events WHERE status = 'pending'")
        ).scalar_one()
    QUEUE_DEPTH.set(count)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
