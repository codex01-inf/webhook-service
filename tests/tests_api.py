# tests/test_api.py
def test_create_endpoint(client, db):
    resp = client.post("/endpoints", json={"tenant_id": "t1", "url": "http://example.com/wh"})
    assert resp.status_code == 201
    assert resp.json()["tenant_id"] == "t1"


def test_idempotency(client, db, endpoint):
    body = {"endpoint_id": str(endpoint.id), "payload": {"x": 1}}
    key = "test-key-1"
    r1 = client.post("/events", json=body, headers={"Idempotency-Key": key})
    r2 = client.post("/events", json=body, headers={"Idempotency-Key": key})
    assert r1.json()["id"] == r2.json()["id"]
    assert r1.status_code == 202 and r2.status_code == 200


def test_replay_dead_event(client, db, dead_event):
    resp = client.post(f"/events/{dead_event.id}/replay")
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"
