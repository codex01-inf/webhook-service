from locust import HttpUser, task, between
import uuid, random

ENDPOINT_IDS = []  # fill these after seeding endpoints


class WebhookUser(HttpUser):
    wait_time = between(0.01, 0.05)  # fire fast

    @task
    def send_event(self):
        endpoint_id = random.choice(ENDPOINT_IDS)
        self.client.post("/webhooks/events", json={
            "endpoint_id": endpoint_id,
            "payload": {"order_id": str(uuid.uuid4()), "amount": 99.99},
        })
