import logging
import random
import asyncio
import hashlib, hmac, time
from fastapi import FastAPI, Request, responses

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("mock-receiver")

import os
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

app = FastAPI(title="Mock Webhook Receiver")


# @app.post("/webhook")
# async def receive(request: Request):
#     body = await request.json()
#     log.info("received: %s", body)
#     return {"received": True}
#


TIMESTAMP_WINDOW_S = 300   # reject signed requests whose timestamp drifts > 5 min


@app.post("/webhook")
async def receive(request: Request):
    ts = request.headers.get("X-Webhook-Timestamp", "")
    sig = request.headers.get("X-Webhook-Signature", "")
    body = await request.body()

    # verify signature + timestamp window if WEBHOOK_SECRET is configured
    if WEBHOOK_SECRET:
        if not ts:
            log.warning("missing X-Webhook-Timestamp header")
            return responses.JSONResponse({"error": "missing timestamp"}, status_code=400)
        try:
            skew = abs(int(time.time()) - int(ts))
        except (TypeError, ValueError):
            log.warning("invalid timestamp header: %r", ts)
            return responses.JSONResponse({"error": "invalid timestamp"}, status_code=400)
        if skew > TIMESTAMP_WINDOW_S:
            log.warning("timestamp out of window: skew=%ds (max=%ds)", skew, TIMESTAMP_WINDOW_S)
            return responses.JSONResponse({"error": "timestamp out of window"}, status_code=401)

        expected = "sha256=" + hmac.new(
            WEBHOOK_SECRET.encode(), f"{ts}.{body.decode()}".encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(sig, expected):
            log.warning("invalid signature — possible forgery or wrong secret")
            return responses.JSONResponse({"error": "invalid signature"}, status_code=401)
        log.info("signature + timestamp verified OK (skew=%ds)", skew)
    else:
        log.info("HMAC header present=%s sig_prefix=%s (no secret set, skipping verify)", bool(sig), sig[:20] if sig else "none")

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
