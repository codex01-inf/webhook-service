import logging
from fastapi import FastAPI, Request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mock-receiver")

app = FastAPI(title="Mock Webhook Receiver")


@app.post("/webhook")
async def receive(request: Request):
    body = await request.json()
    log.info("received: %s", body)
    return {"received": True}
