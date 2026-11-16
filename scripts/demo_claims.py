"""End-to-end demo for the change claim/acknowledgement workflow.

Usage:
    python scripts/demo_claims.py [BASE_URL] [--demo-expiry]

Creates an isolated demo port, uploads two batches of AIS points and then
drives two independent accounting consumers over the change feed:

* claim -> repeated claim (same token and frozen boundaries) -> acknowledge;
* new uploads that happen while a lease is open wait for the next batch;
* with --demo-expiry, a second consumer lets its lease lapse, re-claims the
  batch and shows that a late acknowledgement with the stale token is rejected
  (lease_expired / lease_superseded) while the cursor stays put.

Requires only the standard library so it can run against the container.
"""

from __future__ import annotations

import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone

BASE = (sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("-")
        else "http://localhost:8000").rstrip("/")
DEMO_EXPIRY = "--demo-expiry" in sys.argv
PORT_CODE = "CLAIM-" + uuid.uuid4().hex[:6].upper()
CONSUMER_A = "emission-accounting-a"
CONSUMER_B = "emission-accounting-b"


def request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def upload(rows: list[tuple[str, str]], key: str) -> None:
    boundary = "----demo" + uuid.uuid4().hex
    csv_body = "vessel,time,lat,lon\n" + "\n".join(
        f"{vessel},{ts},{lat},{lon}" for vessel, ts, lat, lon in rows
    ) + "\n"
    payload = (
        f"--{boundary}\r\n"
        'Content-Disposition: form-data; name="file"; filename="ais.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
        f"{csv_body}\r\n"
        f"--{boundary}--\r\n"
    ).encode()
    req = urllib.request.Request(
        f"{BASE}/ports/{PORT_CODE}/uploads",
        data=payload,
        method="POST",
        headers={
            "Content-Type": f"multipart/form-data; boundary={boundary}",
            "Idempotency-Key": key,
        },
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        assert response.status == 201


def stay(vessel: str, start_hour: int) -> list[tuple[str, str, float, float]]:
    return [
        (vessel, f"2026-01-01T{start_hour:02d}:{minute:02d}:00Z", 31.20, 121.50)
        for minute in (0, 10, 20, 30)
    ]


def claim(consumer: str, limit: int = 10) -> tuple[int, dict]:
    return request(
        "POST",
        f"/ports/{PORT_CODE}/changes/claims",
        {"consumer_id": consumer, "limit": limit},
    )


def acknowledge(consumer: str, token: str) -> tuple[int, dict]:
    return request(
        "POST",
        f"/ports/{PORT_CODE}/changes/acknowledgements",
        {"consumer_id": consumer, "claim_token": token},
    )


def state(consumer: str) -> dict:
    _, body = request(
        "GET", f"/ports/{PORT_CODE}/changes/consumers/{consumer}"
    )
    return body


def main() -> None:
    request(
        "POST",
        "/ports",
        {
            "code": PORT_CODE,
            "name": "领取确认演示港区",
            "min_lat": 31.0,
            "max_lat": 31.4,
            "min_lon": 121.2,
            "max_lon": 121.8,
        },
    )
    upload(stay("SHIP-A", 0), "claim-seed-1")
    print(f"demo port: {PORT_CODE}")

    # --- Consumer A: claim, replay, acknowledge ---------------------------
    status, first = claim(CONSUMER_A, limit=1)
    print(f"\n[1] claim ({status}): lease_id={first['lease']['lease_id']} "
          f"boundary=({first['lease']['cursor_from']},"
          f"{first['lease']['cursor_to']}) "
          f"expires_at={first['lease']['expires_at']}")
    print("    changes:", [c["cursor"] for c in first["changes"]],
          "has_more:", first["has_more"])

    # A second change lands while the lease is still open.
    upload(stay("SHIP-B", 2), "claim-seed-2")
    status, replay = claim(CONSUMER_A, limit=10)
    same_token = (
        replay["lease"]["claim_token"] == first["lease"]["claim_token"]
    )
    print(f"\n[2] repeated claim ({status}, replayed={replay['lease']['replayed']}):"
          f" same_token={same_token} same_boundary="
          f"{replay['lease']['cursor_to'] == first['lease']['cursor_to']} "
          f"changes={[c['cursor'] for c in replay['changes']]} "
          "(new upload waits for the next batch)")

    status, ack = acknowledge(CONSUMER_A, first["lease"]["claim_token"])
    print(f"\n[3] acknowledge ({status}): confirmed_cursor="
          f"{ack['confirmed_cursor']}")

    status, second = claim(CONSUMER_A)
    print(f"\n[4] next claim ({status}): now delivers the upload that waited: "
          f"changes={[c['cursor'] for c in second['changes']]}")
    acknowledge(CONSUMER_A, second["lease"]["claim_token"])
    print("    consumer A final state:", json.dumps(state(CONSUMER_A), ensure_ascii=False))

    # --- Consumer B: expiry, re-claim, stale token -------------------------
    if not DEMO_EXPIRY:
        print("\n(pass --demo-expiry to also exercise lease timeout and re-claim)")
        return

    upload(stay("SHIP-C", 4), "claim-seed-3")
    _, leased = claim(CONSUMER_B)
    old_token = leased["lease"]["claim_token"]
    expires_at = leased["lease"]["expires_at"]
    print(f"\n[5] consumer B claim: lease_id={leased['lease']['lease_id']} "
          f"expires_at={expires_at}")
    # Wait until the deadline actually passes (plus a small safety margin),
    # so the demo honours whatever lease TTL the service is configured with.
    deadline = datetime.fromisoformat(expires_at)
    wait_seconds = (deadline - datetime.now(timezone.utc)).total_seconds() + 0.2
    print(f"    waiting {max(wait_seconds, 0.2):.1f}s for the lease to expire ...")
    time.sleep(max(wait_seconds, 0.2))

    status, late = acknowledge(CONSUMER_B, old_token)
    print(f"\n[6] stale acknowledgement after timeout -> {status} "
          f"{late['error']['code']}; cursor stays at "
          f"{state(CONSUMER_B)['confirmed_cursor']}")

    status, reclaimed = claim(CONSUMER_B)
    new_token = reclaimed["lease"]["claim_token"]
    print(f"\n[7] re-claim ({status}): new lease_id="
          f"{reclaimed['lease']['lease_id']} token_changed={new_token != old_token}")

    status, stale = acknowledge(CONSUMER_B, old_token)
    print(f"[8] old token arrives late -> {status} {stale['error']['code']}")

    status, good = acknowledge(CONSUMER_B, new_token)
    print(f"[9] new token acknowledged ({status}): "
          f"confirmed_cursor={good['confirmed_cursor']}")
    print("    consumer B final state:", json.dumps(state(CONSUMER_B), ensure_ascii=False))


if __name__ == "__main__":
    main()
