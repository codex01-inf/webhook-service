import time
import redis.asyncio as aioredis

from src.config import settings

WINDOW = 20  # last N attempts
TRIP_THRESHOLD = 0.5  # trip if >50% failures
OPEN_DURATION = 60  # stay open for 60s
HALF_OPEN_KEY = "cb:halfopen:{eid}"
ATTEMPTS_KEY = "cb:attempts:{eid}"  # Redis list of "1"=success "0"=fail

_redis = None


async def get_redis():
    global _redis
    if _redis is None:
        _redis = aioredis.from_url(settings.REDIS_URL)
    return _redis


async def is_open(endpoint_id: str) -> bool:
    r = await get_redis()
    # Check if breaker is open (key exists with TTL)
    return bool(await r.exists(f"cb:open:{endpoint_id}"))


async def record_attempt(endpoint_id: str, success: bool):
    r = await get_redis()
    key = ATTEMPTS_KEY.format(eid=endpoint_id)
    await r.lpush(key, "1" if success else "0")
    await r.ltrim(key, 0, WINDOW - 1)  # keep last 20 only

    if not success:
        recent = await r.lrange(key, 0, WINDOW - 1)
        failures = recent.count(b"0")
        if len(recent) >= WINDOW and failures / len(recent) > TRIP_THRESHOLD:
            # Trip the breaker
            await r.setex(f"cb:open:{endpoint_id}", OPEN_DURATION, "1")
            await r.delete(key)  # reset window
