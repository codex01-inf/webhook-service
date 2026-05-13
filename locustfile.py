"""
Webhook delivery load test.

Simulates 10 tenants sending events.
tenant-01 points at a black-hole endpoint (noisy neighbor).
tenants 02-10 point at the healthy mock_receiver.

Goal: show that tenant-01's broken endpoint does NOT delay the other 9.

Run:
    locust -f locustfile.py --host=http://localhost:8000 --users=50 --spawn-rate=10
Then open: http://localhost:8089
"""

import json
import random
import uuid

from locust import HttpUser, between, task, events


# Load endpoint IDs created by seed_tenants.py
try:
    with open("endpoint_ids.json") as f:
        ENDPOINT_MAP = json.load(f)   # {"tenant-01": "uuid", ...}
    ENDPOINT_IDS = list(ENDPOINT_MAP.values())
    print(f"Loaded {len(ENDPOINT_IDS)} endpoints from endpoint_ids.json")
except FileNotFoundError:
    raise RuntimeError("Run seed_tenants.py first to create endpoints")


class WebhookSender(HttpUser):
    """Each simulated user picks a random endpoint and fires events."""
    wait_time = between(0.05, 0.2)   # 5–20 ms between requests per user

    @task
    def send_event(self):
        endpoint_id = random.choice(ENDPOINT_IDS)
        self.client.post(
            "/webhooks/events",
            json={
                "endpoint_id": endpoint_id,
                "payload": {
                    "order_id": str(uuid.uuid4()),
                    "amount": round(random.uniform(1, 500), 2),
                },
            },
            # unique idempotency key so every request creates a new event
            headers={"Idempotency-Key": str(uuid.uuid4())},
            name="/webhooks/events",   # group all in locust stats
        )


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    print("\n" + "="*60)
    print("Load test started")
    print(f"  Tenants: {len(ENDPOINT_IDS)}")
    print(f"  Noisy neighbor: tenant-01 → black hole")
    print(f"  Watch Grafana at http://localhost:3000")
    print(f"  Watch Prometheus at http://localhost:9090")
    print("="*60 + "\n")
