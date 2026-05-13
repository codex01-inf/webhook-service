import redis.asyncio as aioredis

from src.config import settings

RATE_LIMIT = 100  # deliveries per second per tenant

# Lua script: atomic check-and-decrement token bucket
LUA_SCRIPT = """
local key = KEYS[1]
local cap = tonumber(ARGV[1])
local now = tonumber(ARGV[2])
local refill_rate = tonumber(ARGV[3])

local data = redis.call("HMGET", key, "tokens", "last_refill")
local tokens = tonumber(data[1]) or cap
local last = tonumber(data[2]) or now

local elapsed = now - last
local refill = elapsed * refill_rate
tokens = math.min(cap, tokens + refill)

if tokens >= 1 then
    tokens = tokens - 1
    redis.call("HMSET", key, "tokens", tokens, "last_refill", now)
    redis.call("EXPIRE", key, 60)
    return 1
else
    redis.call("HMSET", key, "tokens", tokens, "last_refill", now)
    redis.call("EXPIRE", key, 60)
    return 0
end
"""

_redis = None
_script = None


async def get_redis():
    global _redis, _script
    if _redis is None:
        _redis = aioredis.from_url(settings.REDIS_URL)
        _script = _redis.register_script(LUA_SCRIPT)
    return _redis, _script


async def allow(tenant_id: str) -> bool:
    import time
    r, script = await get_redis()
    result = await script(
        keys=[f"ratelimit:{tenant_id}"],
        args=[RATE_LIMIT, time.time(), RATE_LIMIT],
    )
    return bool(result)
