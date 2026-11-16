"""Tests for the claim/acknowledge workflow on the change feed.

Covers the operational contract:
* new consumers start at cursor 0 and receive ordered, limited batches;
* a valid lease is replayed with the same token and frozen boundaries while
  uploads that happen in between wait for the next batch;
* only the current unexpired token advances the confirmed cursor;
* expired batches can be re-claimed and stale tokens cannot overtake them;
* two service instances / concurrent writers never issue different batches
  and never skip a change;
* progress is persisted, so a restarted consumer resumes from the last ack.
"""

from __future__ import annotations

import sqlite3
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import claims, db
from app.config import Settings
from app.main import create_app

PORT = {
    "code": "CNSHA-PD",
    "name": "上海港浦东",
    "min_lat": 31.0,
    "max_lat": 31.4,
    "min_lon": 121.2,
    "max_lon": 121.8,
}
INSIDE = (31.2, 121.5)


def utc(hour: int, minute: int = 0) -> str:
    return f"2026-01-01T{hour:02d}:{minute:02d}:00Z"


def csv_bytes(rows: list[tuple[str, str, float, float]]) -> bytes:
    lines = ["vessel,time,lat,lon"]
    lines += [f"{v},{t},{lat},{lon}" for v, t, lat, lon in rows]
    return ("\n".join(lines) + "\n").encode("utf-8")


def stay_rows(vessel: str, start_hour: int) -> list[tuple[str, str, float, float]]:
    return [
        (vessel, utc(start_hour, minute), *INSIDE) for minute in (0, 10, 20, 30)
    ]


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    settings = Settings(
        db_path=str(tmp_path / "claims.sqlite3"),
        change_lease_ttl_seconds=1,
    )
    app = create_app(settings)
    with TestClient(app) as test_client:
        yield test_client


@pytest.fixture()
def registered(client: TestClient) -> TestClient:
    response = client.post("/ports", json=PORT)
    assert response.status_code == 201, response.text
    return client


def upload(
    client: TestClient,
    rows: list[tuple[str, str, float, float]],
    key: str,
    *,
    port: str = PORT["code"],
) -> dict:
    response = client.post(
        f"/ports/{port}/uploads",
        files={"file": ("ais.csv", csv_bytes(rows), "text/csv")},
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 201, response.text
    return response.json()


def claim(
    client: TestClient,
    consumer: str,
    *,
    limit: int = 100,
    port: str = PORT["code"],
) -> dict:
    response = client.post(
        f"/ports/{port}/changes/claims",
        json={"consumer_id": consumer, "limit": limit},
    )
    assert response.status_code in (200, 201), response.text
    return response.json()


def acknowledge(
    client: TestClient, consumer: str, token: str, *, port: str = PORT["code"]
):
    return client.post(
        f"/ports/{port}/changes/acknowledgements",
        json={"consumer_id": consumer, "claim_token": token},
    )


def test_new_consumer_starts_at_zero_and_batches_are_ordered_and_limited(
    registered: TestClient,
) -> None:
    # Three events => three upsert changes with global seq 1..3.
    for index, vessel in enumerate(("SHIP-A", "SHIP-B", "SHIP-C")):
        upload(registered, stay_rows(vessel, index * 2), f"k-{vessel}")

    first = claim(registered, "emitter-1", limit=2)
    assert first["lease"]["replayed"] is False
    assert first["lease"]["cursor_from"] == 0
    assert [c["cursor"] for c in first["changes"]] == [1, 2]
    assert first["change_count"] == 2
    assert first["has_more"] is True
    assert first["next_cursor"] == 2

    # The outstanding lease keeps the same page even with a smaller limit.
    again = claim(registered, "emitter-1", limit=1)
    assert again["lease"]["lease_id"] == first["lease"]["lease_id"]
    assert again["lease"]["replayed"] is True
    assert [c["cursor"] for c in again["changes"]] == [1, 2]

    ack = acknowledge(registered, "emitter-1", first["lease"]["claim_token"])
    assert ack.status_code == 200, ack.text
    assert ack.json()["confirmed_cursor"] == 2

    second = claim(registered, "emitter-1", limit=2)
    assert second["lease"]["cursor_from"] == 2
    assert [c["cursor"] for c in second["changes"]] == [3]
    assert second["has_more"] is False
    assert acknowledge(
        registered, "emitter-1", second["lease"]["claim_token"]
    ).status_code == 200

    state = registered.get(
        f"/ports/{PORT['code']}/changes/consumers/emitter-1"
    ).json()
    assert state["confirmed_cursor"] == 3
    assert state["active_lease"] is None


def test_repeated_claim_returns_same_batch_while_new_uploads_wait(
    registered: TestClient,
) -> None:
    upload(registered, stay_rows("SHIP-A", 0), "k1")
    first = claim(registered, "emitter-1")
    boundary = first["lease"]["cursor_to"]
    token = first["lease"]["claim_token"]

    # A change uploaded while the lease is open must not join the batch.
    upload(registered, stay_rows("SHIP-B", 2), "k2")
    replay = claim(registered, "emitter-1")
    assert replay["lease"]["claim_token"] == token
    assert replay["lease"]["lease_id"] == first["lease"]["lease_id"]
    assert replay["lease"]["cursor_to"] == boundary
    assert replay["change_count"] == 1
    assert replay["has_more"] is True

    assert acknowledge(registered, "emitter-1", token).status_code == 200
    next_batch = claim(registered, "emitter-1")
    assert next_batch["lease"]["cursor_from"] == boundary
    assert next_batch["change_count"] == 1
    assert next_batch["changes"][0]["vessel_id"] == "SHIP-B"


def test_empty_claim_creates_no_lease_and_keeps_cursor(registered: TestClient) -> None:
    body = claim(registered, "emitter-empty")
    assert body["lease"] is None
    assert body["changes"] == []
    assert body["next_cursor"] == 0
    assert body["has_more"] is False

    state = registered.get(
        f"/ports/{PORT['code']}/changes/consumers/emitter-empty"
    ).json()
    assert state["confirmed_cursor"] == 0
    assert state["active_lease"] is None

    # Once changes arrive the same consumer still starts at cursor 0.
    upload(registered, stay_rows("SHIP-A", 0), "k1")
    batch = claim(registered, "emitter-empty")
    assert batch["lease"]["cursor_from"] == 0
    assert batch["change_count"] == 1


def test_invalid_unknown_and_foreign_tokens_are_rejected_without_progress(
    registered: TestClient,
) -> None:
    upload(registered, stay_rows("SHIP-A", 0), "k1")
    batch = claim(registered, "emitter-1")

    bad = acknowledge(registered, "emitter-1", "lc_does-not-exist-token")
    assert bad.status_code == 409
    assert bad.json()["error"]["code"] == "invalid_claim_token"

    # A token minted for another consumer must not confirm this lease.
    other = claim(registered, "emitter-2")
    foreign = acknowledge(
        registered, "emitter-1", other["lease"]["claim_token"]
    )
    assert foreign.status_code == 409
    assert foreign.json()["error"]["code"] == "invalid_claim_token"

    state = registered.get(
        f"/ports/{PORT['code']}/changes/consumers/emitter-1"
    ).json()
    assert state["confirmed_cursor"] == 0
    assert state["active_lease"]["lease_id"] == batch["lease"]["lease_id"]


def test_acknowledgement_is_idempotent(registered: TestClient) -> None:
    upload(registered, stay_rows("SHIP-A", 0), "k1")
    token = claim(registered, "emitter-1")["lease"]["claim_token"]
    first = acknowledge(registered, "emitter-1", token)
    second = acknowledge(registered, "emitter-1", token)
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()


def test_expired_lease_can_be_reclaimed_and_stale_token_is_superseded(
    registered: TestClient,
) -> None:
    upload(registered, stay_rows("SHIP-A", 0), "k1")
    first = claim(registered, "emitter-1")
    old_token = first["lease"]["claim_token"]

    # TTL is one second in this fixture.
    import time

    time.sleep(1.15)

    expired_ack = acknowledge(registered, "emitter-1", old_token)
    assert expired_ack.status_code == 409
    assert expired_ack.json()["error"]["code"] == "lease_expired"
    # Failed confirmation never moves the cursor.
    state = registered.get(
        f"/ports/{PORT['code']}/changes/consumers/emitter-1"
    ).json()
    assert state["confirmed_cursor"] == 0
    assert state["active_lease"] is None

    reclaimed = claim(registered, "emitter-1")
    assert reclaimed["lease"]["replayed"] is False
    assert reclaimed["lease"]["claim_token"] != old_token
    assert reclaimed["lease"]["cursor_from"] == 0
    assert reclaimed["change_count"] == 1

    # The old token arrives late: it must not jump over the new batch.
    stale = acknowledge(registered, "emitter-1", old_token)
    assert stale.status_code == 409
    assert stale.json()["error"]["code"] == "lease_superseded"
    state = registered.get(
        f"/ports/{PORT['code']}/changes/consumers/emitter-1"
    ).json()
    assert state["confirmed_cursor"] == 0

    good = acknowledge(
        registered, "emitter-1", reclaimed["lease"]["claim_token"]
    )
    assert good.status_code == 200
    assert good.json()["confirmed_cursor"] == 1


def test_expired_lease_state_transitions_are_persisted(
    registered: TestClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Drive the clock deterministically to check status transitions in SQLite."""
    upload(registered, stay_rows("SHIP-A", 0), "k1")
    frozen = datetime(2026, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    clock = {"now": frozen}
    monkeypatch.setattr(claims, "_utc_now", lambda: clock["now"])

    batch = claim(registered, "emitter-1")
    token = batch["lease"]["claim_token"]

    # Exactly at the deadline is treated as expired.
    clock["now"] = frozen + timedelta(milliseconds=1000)
    expired = acknowledge(registered, "emitter-1", token)
    assert expired.status_code == 409
    assert expired.json()["error"]["code"] == "lease_expired"

    reclaimed = claim(registered, "emitter-1")
    assert reclaimed["lease"]["claim_token"] != token
    db_path = registered.app.state.settings.db_path  # type: ignore[attr-defined]
    conn = sqlite3.connect(db_path)
    try:
        statuses = conn.execute(
            "SELECT id, status FROM change_leases ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    assert statuses == [(1, "superseded"), (2, "active")]


def test_consumers_are_independent_and_port_isolated(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "multi.sqlite3")
    app = create_app(Settings(db_path=db_path, change_lease_ttl_seconds=30))
    with TestClient(app) as c:
        for code, shift in (("PORT-A", 0.0), ("PORT-B", 5.0)):
            response = c.post(
                "/ports",
                json={
                    **PORT,
                    "code": code,
                    "min_lat": PORT["min_lat"] + shift,
                    "max_lat": PORT["max_lat"] + shift,
                },
            )
            assert response.status_code == 201
        rows_a = [
            ("SHIP-A", t, lat, lon)
            for (_v, t, lat, lon) in stay_rows("SHIP-A", 0)
        ]
        rows_b = [
            ("SHIP-A", t, lat + 5.0, lon)
            for (_v, t, lat, lon) in stay_rows("SHIP-A", 0)
        ]
        upload(c, rows_a, "a", port="PORT-A")
        upload(c, rows_b, "b", port="PORT-B")

        first = claim(c, "shared-name", port="PORT-A")
        second = claim(c, "shared-name", port="PORT-B")
        assert first["port_id"] != second["port_id"]
        assert first["lease"]["lease_id"] != second["lease"]["lease_id"]
        assert (
            first["changes"][0]["event"]["port_id"]
            != second["changes"][0]["event"]["port_id"]
        )

        # A token from PORT-B cannot acknowledge the PORT-A lease.
        cross = acknowledge(
            c,
            "shared-name",
            second["lease"]["claim_token"],
            port="PORT-A",
        )
        assert cross.status_code == 409
        assert cross.json()["error"]["code"] == "invalid_claim_token"

        assert (
            acknowledge(
                c, "shared-name", first["lease"]["claim_token"], port="PORT-A"
            ).status_code
            == 200
        )
        assert (
            acknowledge(
                c, "shared-name", second["lease"]["claim_token"], port="PORT-B"
            ).status_code
            == 200
        )


def test_two_service_instances_never_hand_out_different_batches(
    tmp_path: Path,
) -> None:
    """Concurrent claims through two instances serialise on the write lock."""
    db_path = str(tmp_path / "instances.sqlite3")
    settings = Settings(db_path=db_path, change_lease_ttl_seconds=30)
    app_one, app_two = create_app(settings), create_app(settings)
    with TestClient(app_one) as one, TestClient(app_two) as two:
        assert one.post("/ports", json=PORT).status_code == 201
        for vessel in ("SHIP-A", "SHIP-B", "SHIP-C"):
            upload(one, stay_rows(vessel, 0), f"k-{vessel}")

        results: list[dict] = []
        barrier = threading.Barrier(2)
        errors: list[BaseException] = []

        def worker(client: TestClient) -> None:
            try:
                barrier.wait(timeout=10)
                response = client.post(
                    f"/ports/{PORT['code']}/changes/claims",
                    json={"consumer_id": "emitter-x", "limit": 2},
                )
                assert response.status_code in (200, 201)
                results.append(response.json())
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(one,)),
            threading.Thread(target=worker, args=(two,)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)

        assert not errors, errors
        assert len(results) == 2
        lease_ids = {body["lease"]["lease_id"] for body in results}
        tokens = {body["lease"]["claim_token"] for body in results}
        boundaries = {
            (body["lease"]["cursor_from"], body["lease"]["cursor_to"])
            for body in results
        }
        assert len(lease_ids) == len(tokens) == len(boundaries) == 1
        assert results[0]["changes"] == results[1]["changes"]

        # The database holds exactly one active lease for the consumer.
        conn = sqlite3.connect(db_path)
        try:
            active = conn.execute(
                "SELECT COUNT(*) FROM change_leases WHERE status = 'active'"
            ).fetchone()[0]
            total = conn.execute("SELECT COUNT(*) FROM change_leases").fetchone()[0]
        finally:
            conn.close()
        assert active == 1
        assert total == 1


def test_service_layer_concurrent_claims_serialise_on_begin_immediate(
    tmp_path: Path,
) -> None:
    """Two raw connections (like two containers) claiming at the same time."""
    path = str(tmp_path / "raw.sqlite3")
    db.initialise(path)
    setup = db.connect(path)
    try:
        setup.execute(
            "INSERT INTO ports (code, name, min_lat_e6, max_lat_e6, min_lon_e6,"
            " max_lon_e6, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ("P", "p", 31_000_000, 31_400_000, 121_200_000, 121_800_000, "t"),
        )
        setup.execute(
            "INSERT INTO ingest_batches (port_id, upload_key, content_sha256,"
            " rows_received, rows_new, rows_duplicate, rows_outside_port,"
            " received_at) VALUES (1, 'k', 'h', 0, 0, 0, 0, 't')"
        )
        for seq in range(5):
            setup.execute(
                "INSERT INTO event_changes (batch_id, port_id, change_type,"
                " event_id, vessel_id, start_time, end_time, start_lat_e6,"
                " start_lon_e6, end_lat_e6, end_lon_e6, point_count,"
                " created_at) VALUES (1, 1, 'upsert', ?, 'V', ?, ?,"
                " 31200000, 121500000, 31200000, 121500000, 4, 't')",
                (seq + 1, seq * 600, seq * 600 + 1800),
            )
    finally:
        setup.close()

    barrier = threading.Barrier(2)
    answers: list[dict] = []
    errors: list[BaseException] = []

    def worker() -> None:
        conn = db.connect(path)
        try:
            barrier.wait(timeout=10)
            answers.append(
                claims.claim_changes(
                    conn,
                    port_code="P",
                    consumer_id="c",
                    limit=3,
                    ttl_seconds=30,
                )
            )
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)
        finally:
            conn.close()

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors, errors
    assert len(answers) == 2
    assert answers[0]["lease"]["lease_id"] == answers[1]["lease"]["lease_id"]
    assert answers[0]["lease"]["claim_token"] == answers[1]["lease"]["claim_token"]
    assert [c["cursor"] for c in answers[0]["changes"]] == [1, 2, 3]
    assert [c["cursor"] for c in answers[1]["changes"]] == [1, 2, 3]


def test_interleaved_upload_claim_ack_across_two_instances_loses_nothing(
    tmp_path: Path,
) -> None:
    """The headline topology: two instances interleave every operation."""
    db_path = str(tmp_path / "interleave.sqlite3")
    settings = Settings(db_path=db_path, change_lease_ttl_seconds=30)
    app_one, app_two = create_app(settings), create_app(settings)
    with TestClient(app_one) as one, TestClient(app_two) as two:
        assert one.post("/ports", json=PORT).status_code == 201
        consumer = "emitter-interleaved"
        seen: list[int] = []

        for round_index in range(3):
            vessel = f"SHIP-{round_index}"
            # Alternate which instance receives the upload.
            uploader = one if round_index % 2 == 0 else two
            upload(uploader, stay_rows(vessel, round_index * 2), f"k-{round_index}")

            claimer = two if round_index % 2 == 0 else one
            batch = claim(claimer, consumer)
            assert batch["lease"]["cursor_from"] == (
                seen[-1] if seen else 0
            )
            seen.extend(change["cursor"] for change in batch["changes"])

            acker = uploader  # ack through yet another instance than the claim
            ack = acknowledge(
                acker, consumer, batch["lease"]["claim_token"]
            )
            assert ack.status_code == 200, ack.text
            assert ack.json()["confirmed_cursor"] == seen[-1]

        # Every change was delivered exactly once, in order, none skipped.
        feed = one.get(
            f"/ports/{PORT['code']}/changes", params={"after": 0, "limit": 1000}
        ).json()
        all_cursors = [change["cursor"] for change in feed["changes"]]
        assert seen == all_cursors
        assert len(seen) == len(set(seen)) == 3


def test_restarted_consumer_resumes_from_last_acknowledged_cursor(
    tmp_path: Path,
) -> None:
    db_path = str(tmp_path / "restart.sqlite3")
    settings = Settings(db_path=db_path, change_lease_ttl_seconds=30)

    with TestClient(create_app(settings)) as first:
        assert first.post("/ports", json=PORT).status_code == 201
        upload(first, stay_rows("SHIP-A", 0), "k1")
        upload(first, stay_rows("SHIP-B", 2), "k2")
        batch = claim(first, "emitter-restart", limit=1)
        assert [c["cursor"] for c in batch["changes"]] == [1]
        assert acknowledge(
            first, "emitter-restart", batch["lease"]["claim_token"]
        ).status_code == 200

    # A brand-new process/app opens the same database file.
    with TestClient(create_app(settings)) as restarted:
        state = restarted.get(
            f"/ports/{PORT['code']}/changes/consumers/emitter-restart"
        ).json()
        assert state["confirmed_cursor"] == 1
        assert state["active_lease"] is None

        batch = claim(restarted, "emitter-restart", limit=10)
        assert batch["lease"]["cursor_from"] == 1
        assert [c["cursor"] for c in batch["changes"]] == [2]


def test_ack_failure_does_not_advance_cursor_and_batch_is_replayable(
    registered: TestClient,
) -> None:
    upload(registered, stay_rows("SHIP-A", 0), "k1")
    batch = claim(registered, "emitter-1")
    bad = acknowledge(registered, "emitter-1", "lc_wrong-wrong-wrong-token")
    assert bad.status_code == 409

    state = registered.get(
        f"/ports/{PORT['code']}/changes/consumers/emitter-1"
    ).json()
    assert state["confirmed_cursor"] == 0
    # The original lease is still current and can be replayed and confirmed.
    replay = claim(registered, "emitter-1")
    assert replay["lease"]["claim_token"] == batch["lease"]["claim_token"]
    assert (
        acknowledge(
            registered, "emitter-1", batch["lease"]["claim_token"]
        ).status_code
        == 200
    )


def test_claim_input_validation(registered: TestClient) -> None:
    bad_consumer = registered.post(
        f"/ports/{PORT['code']}/changes/claims",
        json={"consumer_id": "bad id!", "limit": 10},
    )
    assert bad_consumer.status_code == 422
    bad_limit = registered.post(
        f"/ports/{PORT['code']}/changes/claims",
        json={"consumer_id": "ok-id", "limit": 0},
    )
    assert bad_limit.status_code == 422
    short_token = registered.post(
        f"/ports/{PORT['code']}/changes/acknowledgements",
        json={"consumer_id": "ok-id", "claim_token": "x"},
    )
    assert short_token.status_code == 422


def test_claim_and_ack_on_unknown_port_are_404(client: TestClient) -> None:
    claim_response = client.post(
        "/ports/NOPE/changes/claims",
        json={"consumer_id": "c", "limit": 10},
    )
    assert claim_response.status_code == 404
    ack_response = client.post(
        "/ports/NOPE/changes/acknowledgements",
        json={"consumer_id": "c", "claim_token": "lc_whatever"},
    )
    assert ack_response.status_code == 404
    state_response = client.get("/ports/NOPE/changes/consumers/c")
    assert state_response.status_code == 404


def test_two_consumers_each_consume_the_full_stream(registered: TestClient) -> None:
    upload(registered, stay_rows("SHIP-A", 0), "k1")
    upload(registered, stay_rows("SHIP-B", 2), "k2")

    for consumer in ("emitter-a", "emitter-b"):
        batch = claim(registered, consumer, limit=1)
        assert [c["cursor"] for c in batch["changes"]] == [1]
        assert acknowledge(
            registered, consumer, batch["lease"]["claim_token"]
        ).status_code == 200
        batch = claim(registered, consumer, limit=1)
        assert [c["cursor"] for c in batch["changes"]] == [2]
        assert acknowledge(
            registered, consumer, batch["lease"]["claim_token"]
        ).status_code == 200

    # Their leases never interfere: two acknowledged leases per consumer.
    db_path = registered.app.state.settings.db_path  # type: ignore[attr-defined]
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT c.consumer_id, l.status, COUNT(*)
            FROM change_leases l JOIN change_consumers c
              ON c.id = l.consumer_pk_id
            GROUP BY c.consumer_id, l.status
            """
        ).fetchall()
    finally:
        conn.close()
    assert sorted(rows) == [
        ("emitter-a", "acknowledged", 2),
        ("emitter-b", "acknowledged", 2),
    ]
