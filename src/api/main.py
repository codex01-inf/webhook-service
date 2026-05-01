from fastapi import FastAPI

from src.api.routes import events
from src.db import models
from src.db.session import engine

app = FastAPI()


# Create tables on startup
# @app.on_event("startup")
# def startup():
#     models.Base.metadata.create_all(bind=engine)


# Plug in the routers
app.include_router(events.router)


@app.get("/")
async def root():
    return {"message": "mizu to gohan kudasai."}


@app.get("/health")
def health():
    return {"status": "ok"}
