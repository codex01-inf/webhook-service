"""
Run this once before the load test to create 10 tenants.
9 tenants → healthy mock_receiver endpoint
1 tenant  → black hole (nothing listening) to simulate noisy neighbor

Usage:
    python seed_tenants.py
"""

import requests
import json

API = "http://localhost:8000"

tenants = [f"tenant-{i:02d}" for i in range(1, 11)]
GOOD_URL = "http://localhost:9000/webhook"
BAD_URL = "http://localhost:9999/dead-end"  # nothing listens here

endpoints = {}

print("Creating endpoints...")
for i, tenant in enumerate(tenants):
    url = BAD_URL if i == 0 else GOOD_URL  # tenant-01 is the noisy neighbor
    resp = requests.post(f"{API}/endpoint", json={"tenant_id": tenant, "url": url})
    resp.raise_for_status()
    data = resp.json()
    endpoints[tenant] = data["id"]
    label = "BAD (black hole)" if i == 0 else "GOOD"
    print(f"  {tenant} → {label} → endpoint_id={data['id']}")

# Save to file so locustfile.py can read it
with open("endpoint_ids.json", "w") as f:
    json.dump(endpoints, f, indent=2)

print("\nSaved to endpoint_ids.json")
print("Now run: locust -f locustfile.py --host=http://localhost:8000")
