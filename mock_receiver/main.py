import logging
import random
import asyncio
from fastapi import FastAPI, Request, responses

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mock-receiver")

app = FastAPI(title="Mock Webhook Receiver")


# @app.post("/webhook")
# async def receive(request: Request):
#     body = await request.json()
#     log.info("received: %s", body)
#     return {"received": True}
#


@app.post("/webhook")
async def receive(request: Request):
    body = await request.json()
    r = random.random()

    if r < 0.20:  # 20% → 500
        log.warning("simulating 500 for: %s", body)
        return responses.JSONResponse({"error": "internal"}, status_code=500)
    elif r < 0.30:  # 10% → 429
        log.warning("simulating 429 for: %s", body)
        return responses.JSONResponse({"error": "rate limited"}, status_code=429)
    elif r < 0.35:  # 5% → slow 200 (10s timeout test)
        log.warning("simulating slow response for: %s", body)
        await asyncio.sleep(10)
        log.info("slow received: %s", body)
        return {"received": True, "slow": True}
    else:  # 65% → normal 200
        log.info("received: %s", body)
        return {"received": True}
