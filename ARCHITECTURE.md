# Architecture Decisions

A log of non-obvious design choices and the reasoning behind them. Written for the person who reads this in 12 months and asks *"why did we do that?"*

---

## ADR-001 — Postgres as queue, not Kafka

**Status:** Accepted

**Context.** Need a durable event store + a queue for the delivery worker. Two natural choices: (a) Postgres for both, (b) Postgres for storage + Kafka for queue.

**Decision.** Postgres for both. Use `SELECT ... FOR UPDATE SKIP LOCKED` for atomic claim.

**Rationale.**
- Target scale is ≤10k events/sec. A single Postgres instance handles this comfortably.
- Kafka adds operational surface area: a cluster to run, partitioning to manage, consumer group rebalancing to debug, schema-registry decisions to make.
- A single source of truth simplifies correctness reasoning: an event exists ⟺ a row exists. No "is it in the topic? is it in the DB? what if one but not the other?"
- The pattern is battle-tested. Stripe, Shopify, GitLab, and Sidekiq Pro all use Postgres or similar relational stores as queues for the middle scale band.

**Trade-offs accepted.**
- At higher throughput, the `next_attempt_at` index becomes a contention point. Mitigated by partial index (`WHERE status = 'pending'`) and small claim batches.
- No native fan-out — extra consumers require shared claim, not independent offsets.

**When to revisit.** Sustained throughput >10k events/sec, or when we need cross-system fan-out of the same event stream. Migration path: introduce Kafka as the dispatch layer downstream of Postgres, not as a replacement.

---

## ADR-002 — Atomic claim via SKIP LOCKED + status update

**Status:** Accepted

**Context.** Multiple worker processes need to consume events without double-processing.

**Decision.** Inside a single transaction:
```sql
BEGIN;
SELECT id FROM events
 WHERE status='pending' AND next_attempt_at <= now()
 ORDER BY next_attempt_at
 LIMIT 50
 FOR UPDATE SKIP LOCKED;

UPDATE events SET status='in_flight' WHERE id IN (...);
COMMIT;
```

**Rationale.** `SKIP LOCKED` makes the claim non-blocking across workers — each worker grabs disjoint rows. The status flip inside the same transaction means that once the transaction commits, no future `status='pending'` query will see these rows. We get atomic claim + isolation from each other.

**Trade-off accepted.** Worker crash between `in_flight` and final status update leaves rows stuck in `in_flight`. Mitigated by a sweeper that resets rows older than 5 min back to `pending`. A more sophisticated alternative would be lease-based claims with a `claimed_at` watchdog.

---

## ADR-003 — Full jitter for retry backoff

**Status:** Accepted

**Context.** Many events failing simultaneously (e.g., a customer's gateway briefly returns 503) all enter retry. Naive `2^attempt` spacing makes them retry in lockstep, creating a thundering herd on recovery.

**Decision.** Full jitter:
```python
delay = random.uniform(0, base * 2 ** attempt)
```

**Rationale.** AWS's analysis showed full jitter minimizes both retry concurrency and time-to-success vs. no jitter, equal jitter, or decorrelated jitter. Reference: AWS Architecture Blog "Exponential Backoff and Jitter".

**Trade-off accepted.** Slightly higher tail latency for any individual event vs. equal jitter. We optimize for fleet behavior under load, not single-event recovery time.

---

## ADR-004 — Per-tenant token buckets in Redis

**Status:** Accepted

**Context.** Multi-tenant system. Tenant A's surge should not delay tenant B's deliveries.

**Decision.** Each tenant has a Redis-backed token bucket (capacity 100 tokens, refill 100/sec — tune per plan). Check + decrement is a single Lua script for atomicity. If the bucket is empty, the worker re-queues the event with `next_attempt_at = now() + 1s` instead of consuming a worker slot.

**Rationale.**
- Redis Lua = atomic, no race condition between check and consume.
- Re-queue instead of block — workers stay free to process other tenants.
- Per-tenant state in Redis is shared across worker instances; in-memory wouldn't scale horizontally.

**Trade-off accepted.** Adds a Redis hop per delivery. Acceptable at sub-ms Redis latency; would not be acceptable at 100x scale.

---

## ADR-005 — Circuit breaker state in Redis, sliding-window failures

**Status:** Accepted

**Context.** A consistently-failing endpoint shouldn't burn worker capacity attempting deliveries that will fail.

**Decision.** Per-endpoint breaker stored in Redis with 3 states:
- **Closed** — normal operation.
- **Open** — trip when sliding-window failure rate > 50% over last 20 attempts. While open, workers skip the endpoint and re-queue events with 60s delay.
- **Half-open** — after 60s in open, let one test request through. Two consecutive successes close the breaker; one failure resets to open.

**Rationale.** Sliding-window over fixed-count gives a better signal than time-window during low-traffic endpoints (might have 1 attempt/min — time window would have stale state).

**Trade-off accepted.** Redis is a SPOF on the hot path. For the target scale, acceptable; at higher scale, switch to per-worker in-memory state synced via Redis pub/sub.

---

## ADR-006 — Separate `delivery_attempts` table

**Status:** Accepted

**Context.** Need attempt-by-attempt history (which status code, when, how long) for support, debugging, and analytics.

**Decision.** Each attempt writes one row to `delivery_attempts`. `events.attempts` is just a counter for retry-budget logic; the truth lives in the attempts log.

**Rationale.** Counters on the events table lose information. With `delivery_attempts`, ops engineers can answer: *"when did this endpoint start failing? what error did the receiver return on the third attempt? how long did the timeout take?"* — all questions that come up at 2 AM during an incident.

**Trade-off accepted.** Storage growth — ~1 row per delivery attempt. With 100M events/day and avg ~2 attempts each, that's 200M rows/day. Partition `delivery_attempts` by month or archive to S3 after 30 days.

---

## ADR-007 — HMAC signing with timestamp window

**Status:** Accepted

**Context.** Webhook receivers must verify that requests came from us (authenticity) and have not been replayed (freshness).

**Decision.**
- Compute `signature = HMAC-SHA256(shared_secret, timestamp + "." + body)`.
- Send headers `X-Webhook-Signature: <hex>` and `X-Webhook-Timestamp: <unix>`.
- Receivers reject requests where `|now - timestamp| > 300s`.

**Rationale.** Stripe-style. Timestamp prevents replay attacks within the bounded window. HMAC over `timestamp.body` (not just body) means an attacker can't replay the same signature with a fresh timestamp.

**Trade-off accepted.** Requires receivers to have approximately synchronized clocks (within 5 minutes). NTP makes this universal.

---

## ADR-008 — Idempotency contract (Stripe-style)

**Status:** Accepted

**Context.** Callers may retry POST `/events` after network errors. We must not double-enqueue.

**Decision.** If caller provides `Idempotency-Key`:
- First request creates the event and returns 202 with the event id.
- Any subsequent request with the same `(tenant_id, idempotency_key)` within 24h returns the **same event id** (HTTP 200, not 202).
- After 24h, the key is eligible for reuse.

**Rationale.** Stripe's contract is the de-facto standard for fintech APIs. The 24h window covers all realistic retry storms while keeping the unique index small enough to fit comfortably in memory.

**Trade-off accepted.** A unique partial index on `(tenant_id, idempotency_key)` adds modest write overhead and storage. The index is partial (`WHERE idempotency_key IS NOT NULL`) so events without a key are not indexed.

---

## ADR-009 — Fairness via round-robin claim, not weighted queues

**Status:** Accepted

**Context.** Tenant `noisy` enqueues 100k events; tenant `quiet` enqueues 10. With `ORDER BY next_attempt_at`, `noisy` dominates all worker batches.

**Decision.** Worker claim uses `SELECT DISTINCT ON (tenant_id) ... ORDER BY tenant_id, next_attempt_at` to pick at most one event per tenant per claim cycle. Combined with the per-tenant token bucket, this approximates fair round-robin.

**Rationale.** `DISTINCT ON` is idiomatic Postgres and runs efficiently with the right index (`events(tenant_id, next_attempt_at) WHERE status='pending'`). Alternative — hash-bucket workers by tenant — works but introduces hotspots if one bucket holds the noisy tenant.

**Trade-off accepted.** `DISTINCT ON` slightly increases query planning cost vs. raw `ORDER BY`. Mitigated by index. Could revisit at >10k tenants.

---

## Open questions / future ADRs

- Ordering guarantees per `(tenant, endpoint)` — currently best-effort; would need a dependency graph for strict.
- Multi-region active-active design.
- Whether to expose at-most-once delivery as an opt-in mode for non-idempotent receivers.
- How to handle webhook signing key rotation with zero-downtime overlap.