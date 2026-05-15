import json
import time
import asyncio
import logging
from datetime import datetime, timezone, timedelta
import hashlib, hmac
import random

import httpx
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from prometheus_client import Counter, Histogram, start_http_server

from src.worker.rate_limiter import allow
from src.db.session import AsyncSessionLocal
from src.db.models import Event, Endpoint, DeliveryAttempt
from src.worker.circuit_breaker import is_open, record_attempt

DELIVERIES = Counter("webhook_deliveries_total", "Deliveries by outcome", ["status", "tenant_id"])
DURATION = Histogram("webhook_delivery_duration_seconds", "HTTP delivery latency", ["tenant_id"])

log = logging.getLogger("worker")

BATCH_SIZE = 50
MAX_ATTEMPTS = 3  # change to 8 for production
POLL_INTERVAL_S = 0.5
HTTP_TIMEOUT_S = 10
CONCURRENT_DELIVERIES = 20


async def claim_batch(session: AsyncSession) -> list[tuple[Event, str, str]]:
    now = datetime.now(timezone.utc)
    # Step 1: pick one candidate event per tenant (no lock yet)
    subq = (
        select(Event.id)
        .where(Event.status == "pending", Event.next_attempt_at <= now)
        .distinct(Event.tenant_id)  # DISTINCT ON (tenant_id)
        .order_by(Event.tenant_id, Event.next_attempt_at)
        .limit(BATCH_SIZE)
        .subquery()
    )

    # Step 2: lock exactly those rows
    stmt = (
        select(Event, Endpoint.url, Endpoint.signing_secret)
        .join(Endpoint, Event.endpoint_id == Endpoint.id)
        .where(Event.id.in_(select(subq.c.id)))
        .with_for_update(of=Event, skip_locked=True)
    )
    rows = (await session.execute(stmt)).all()
    # print(f"HERE: {rows}")

    if not rows:
        return []

    event_ids = [e.id for e, _, __ in rows]
    await session.execute(
        update(Event).where(Event.id.in_(event_ids)).values(status="in_flight")
    )
    await session.commit()
    return [(e, url, secret) for e, url, secret in rows]


async def deliver_one(client, event, endpoint_url, signing_secret) -> tuple[int | None, str | None, int]:
    loop = asyncio.get_event_loop()
    start = loop.time()
    timestamp = str(int(time.time()))
    body = json.dumps(event.payload, separators=(",", ":"))
    # print(f"Inside deliver, sig being created and the secret being used={signing_secret}")
    # print(f"BODY in deliver: {body}")
    sig = hmac.new(
        signing_secret.encode(),
        f"{timestamp}.{body}".encode(),
        hashlib.sha256,
    ).hexdigest()
    headers = {
        "Content-Type": "application/json",
        "X-Webhook-Timestamp": timestamp,
        "X-Webhook-Signature": f"sha256={sig}",
    }
    try:
        resp = await client.post(endpoint_url, content=body, headers=headers, timeout=HTTP_TIMEOUT_S)
        return resp.status_code, None, int((loop.time() - start) * 1000)
    except httpx.RequestError as exc:
        print(f"ERROR BEFORE GOING IN MOCK")
        return None, f"{type(exc).__name__}: {exc}", int((loop.time() - start) * 1000)


def next_attempt_delay(attempt: int) -> timedelta:
    # For production: base = min(60 * (2 ** attempt), 3600)
    # For quick local testing: short delays so you can see retries in seconds
    base = min(5 * (2 ** attempt), 30)  # 5s → 10s → 20s → capped 30s  (change to 60/3600 for prod)
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
    DELIVERIES.labels(status=new_status, tenant_id=event.tenant_id).inc()
    DURATION.labels(tenant_id=event.tenant_id).observe(duration_ms / 1000)


async def process_event(client, event, endpoint_url, signing_secret):
    if await is_open(str(event.endpoint_id)):
        # Re-queue with 30s delay instead of delivering
        async with AsyncSessionLocal() as session:
            retry_at = datetime.now(timezone.utc) + timedelta(seconds=30)
            await session.execute(
                update(Event).where(Event.id == event.id)
                .values(status="pending", next_attempt_at=retry_at)
            )
            await session.commit()
        log.warning("circuit open for endpoint %s, re-queuing event %s", event.endpoint_id, event.id)
        return

    status_code, error, duration_ms = await deliver_one(client, event, endpoint_url, signing_secret)
    await record_attempt(str(event.endpoint_id), success=(status_code and 200 <= status_code < 300))
    await finalize(event, status_code, error, duration_ms)
    log.info(
        "delivered event_id=%s url=%s status=%s error=%s duration_ms=%d",
        event.id, endpoint_url, status_code, error, duration_ms,
    )


# ---------- main loop ----------

async def worker_loop():
    log.info("worker starting (batch=%d concurrency=%d)", BATCH_SIZE, CONCURRENT_DELIVERIES)

    # On startup, reset any events stuck in_flight from a previous crashed run
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            update(Event).where(Event.status == "in_flight")
            .values(status="pending", next_attempt_at=datetime.now(timezone.utc))
        )
        await session.commit()
        if result.rowcount:
            log.warning("reset %d stuck in_flight events to pending on startup", result.rowcount)

    sem = asyncio.Semaphore(CONCURRENT_DELIVERIES)

    async with httpx.AsyncClient() as client:
        while True:
            try:
                async with AsyncSessionLocal() as session:
                    claimed = await claim_batch(session)
                    # print(claimed)

                if not claimed:
                    await asyncio.sleep(POLL_INTERVAL_S)
                    continue

                async def bounded(event: Event, url: str, secret: str):
                    async with sem:
                        try:
                            if not await allow(str(event.tenant_id)):
                                # Bucket empty — push next_attempt_at forward 1 second
                                async with AsyncSessionLocal() as session:
                                    retry_at = datetime.now(timezone.utc) + timedelta(seconds=1)
                                    await session.execute(
                                        update(Event).where(Event.id == event.id)
                                        .values(status="pending", next_attempt_at=retry_at)
                                    )
                                    await session.commit()
                                return
                            await process_event(client, event, url, secret)
                        except Exception:
                            log.exception("unhandled error processing event %s, resetting to pending", event.id)
                            async with AsyncSessionLocal() as session:
                                await session.execute(
                                    update(Event).where(Event.id == event.id)
                                    .values(status="pending", next_attempt_at=datetime.now(timezone.utc) + timedelta(seconds=5))
                                )
                                await session.commit()

                await asyncio.gather(*(bounded(e, u, s) for e, u, s in claimed), return_exceptions=True)

            except Exception as e:
                log.exception(f"worker loop iteration failed: {e}")
                await asyncio.sleep(1)  # don't hot-loop on persistent errors


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    start_http_server(9100)  # Prometheus scrapes http://localhost:9100/metrics
    log.info("Prometheus metrics server started on :9100")
    asyncio.run(worker_loop())


if __name__ == "__main__":
    main()
