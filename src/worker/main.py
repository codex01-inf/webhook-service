import time
import asyncio
import logging
from datetime import datetime, timezone, timedelta
import random

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from prometheus_client import Counter, Histogram, start_http_server

from src.db.session import AsyncSessionLocal
from src.db.models import Event, Endpoint, DeliveryAttempt

DELIVERIES = Counter("webhook_deliveries_total", "Deliveries by outcome", ["status"])
DURATION = Histogram("webhook_delivery_duration_seconds", "HTTP delivery latency")

log = logging.getLogger("worker")

BATCH_SIZE = 50
MAX_ATTEMPTS = 3   # change to 8 for production
POLL_INTERVAL_S = 0.5
HTTP_TIMEOUT_S = 10
CONCURRENT_DELIVERIES = 20


async def claim_batch(session: AsyncSession) -> list[tuple[Event, str]]:
    now = datetime.now(timezone.utc)
    stmt = (
        select(Event, Endpoint.url)
        .join(Endpoint, Event.endpoint_id == Endpoint.id)
        .where(Event.status == "pending", Event.next_attempt_at <= now)
        .order_by(Event.next_attempt_at)
        .limit(BATCH_SIZE)
        .with_for_update(of=Event, skip_locked=True)  # only lock Event rows
    )
    rows = (await session.execute(stmt)).all()
    if not rows:
        return []

    event_ids = [e.id for e, _ in rows]
    await session.execute(
        update(Event).where(Event.id.in_(event_ids)).values(status="in_flight")
    )
    await session.commit()
    return [(e, url) for e, url in rows]


async def deliver_one(
        client: httpx.AsyncClient, event: Event, endpoint_url: str
) -> tuple[int | None, str | None, int]:
    """Returns (status_code, error_str, duration_ms)."""
    loop = asyncio.get_event_loop()
    start = loop.time()
    try:
        resp = await client.post(endpoint_url, json=event.payload, timeout=HTTP_TIMEOUT_S)
        return resp.status_code, None, int((loop.time() - start) * 1000)
    except httpx.RequestError as exc:
        return None, f"{type(exc).__name__}: {exc}", int((loop.time() - start) * 1000)


def next_attempt_delay(attempt: int) -> timedelta:
    # For production: base = min(60 * (2 ** attempt), 3600)
    # For quick local testing: short delays so you can see retries in seconds
    base = min(5 * (2 ** attempt), 30)   # 5s → 10s → 20s → capped 30s  (change to 60/3600 for prod)
    jitter = random.uniform(0.5, 1.5)
    return timedelta(seconds=base * jitter)


async def finalize(event, status_code, error, duration_ms):
    success = status_code is not None and 200 <= status_code < 300
    new_attempts = event.attempts + 1

    if success:
        new_status = "succeeded"
        updates = {"status": new_status, "attempts": new_attempts}
    elif new_attempts >= MAX_ATTEMPTS:
        new_status = "dead"
        updates = {"status": new_status, "attempts": new_attempts}
    else:
        new_status = "pending"  # schedule retry
        retry_at = datetime.now(timezone.utc) + next_attempt_delay(new_attempts)
        updates = {"status": new_status, "attempts": new_attempts, "next_attempt_at": retry_at}

    async with AsyncSessionLocal() as session:
        await session.execute(update(Event).where(Event.id == event.id).values(**updates))
        session.add(DeliveryAttempt(
            event_id=event.id,
            attempt_number=new_attempts,
            status_code=status_code,
            error=error,
            duration_ms=duration_ms,
        ))
        await session.commit()

    log.info(f"delivery done for {str(event.id)}, attempt={new_attempts}, status={new_status}, "
             f"status_code={status_code}, duration_ms={duration_ms}, error={error}")

    # prometheus
    DELIVERIES.labels(status=new_status).inc()
    DURATION.observe(duration_ms / 1000)


# async def finalize(
#         event: Event, status_code: int | None, error: str | None, duration_ms: int
# ):
#     success = status_code is not None and 200 <= status_code < 300
#     new_status = "succeeded" if success else "failed"
#
#     async with AsyncSessionLocal() as session:
#         await session.execute(
#             update(Event)
#             .where(Event.id == event.id)
#             .values(status=new_status, attempts=Event.attempts + 1)
#         )
#         session.add(DeliveryAttempt(
#             event_id=event.id,
#             attempt_number=event.attempts + 1,
#             status_code=status_code,
#             error=error,
#             duration_ms=duration_ms,
#         ))
#         await session.commit()


async def process_event(client: httpx.AsyncClient, event: Event, endpoint_url: str):
    status_code, error, duration_ms = await deliver_one(client, event, endpoint_url)
    await finalize(event, status_code, error, duration_ms)
    log.info(
        "delivered event_id=%s url=%s status=%s error=%s duration_ms=%d",
        event.id, endpoint_url, status_code, error, duration_ms,
    )


# ---------- main loop ----------

async def worker_loop():
    log.info("worker starting (batch=%d concurrency=%d)", BATCH_SIZE, CONCURRENT_DELIVERIES)
    sem = asyncio.Semaphore(CONCURRENT_DELIVERIES)

    async with httpx.AsyncClient() as client:
        while True:
            try:
                async with AsyncSessionLocal() as session:
                    claimed = await claim_batch(session)

                if not claimed:
                    await asyncio.sleep(POLL_INTERVAL_S)
                    continue

                async def bounded(event: Event, url: str):
                    async with sem:
                        await process_event(client, event, url)

                await asyncio.gather(*(bounded(e, u) for e, u in claimed))

            except Exception:
                log.exception("worker loop iteration failed")
                await asyncio.sleep(1)  # don't hot-loop on persistent errors


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    start_http_server(9100)   # Prometheus scrapes http://localhost:9100/metrics
    log.info("Prometheus metrics server started on :9100")
    asyncio.run(worker_loop())


if __name__ == "__main__":
    main()
