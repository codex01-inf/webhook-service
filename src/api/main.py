from fastapi import FastAPI

from src.api import models
from src.db.session import engine

app = FastAPI()


# Create tables on startup
@app.on_event("startup")
def startup():
    models.Base.metadata.create_all(bind=engine)


@app.get("/")
async def root():
    return {"message": "mizu to gohan kudasai."}
