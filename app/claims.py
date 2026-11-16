"""Claim/acknowledge workflow over the port change feed.

Each accounting process is a *consumer* identified by ``(port, consumer_id)``.
A consumer owns a ``confirmed_cursor`` (last ``seq`` it has acknowledged) and
at most one outstanding batch (lease).

Workflow
--------
* ``claim_changes`` hands out the next ordered page starting right after the
  confirmed cursor.  The batch carries an opaque ``claim_token`` and an
  ``expires_at`` deadline; the boundaries are frozen at claim time.
* Re-claiming while the lease is still valid returns the *same* lease id,
  token and boundaries (and the same immutable change rows); changes uploaded
  in the meantime wait for the next batch.
* A lease that is not acknowledged before its deadline may be re-claimed.  The
  old lease is marked ``superseded`` once a new batch is taken, so a late
  acknowledgement with the stale token is rejected and can never jump past the
  newer batch.
* ``acknowledge_changes`` only advances the confirmed cursor when presented
  with the token of the current, unexpired lease.  Every other outcome is
  rejected and leaves the cursor untouched, so a failed/partial confirm never
  loses progress and a restart can resume from the last confirmed cursor.

All three operations run inside ``BEGIN IMMEDIATE`` (see :mod:`app.db`), which
serialises writers across processes: two service instances sharing the SQLite
file can never emit two different batches for one consumer concurrently.
"""

from __future__ import annotations

import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from .db import read_snapshot, write_transaction
from .errors import AppError
from .service import _change_dict, get_port, utc_now_iso

TOKEN_PREFIX = "lc_"


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(value: datetime) -> str:
    # Millisecond precision keeps short-TTL expiry tests deterministic while
    # staying an ISO-8601 string that clients can parse directly.
    return value.isoformat(timespec="milliseconds")


def _parse_iso(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:  # pragma: no cover - we always write aware UTC
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _get_consumer(
    conn: sqlite3.Connection, port_id: int, consumer_id: str
) -> sqlite3.Row:
    row = conn.execute(
        "SELECT * FROM change_consumers WHERE port_id = ? AND consumer_id = ?",
        (port_id, consumer_id),
    ).fetchone()
    if row is None:
        raise AppError(
            404,
            "consumer_unknown",
            (
                f"consumer {consumer_id!r} has never claimed changes for this "
                "port; claim a batch first"
            ),
        )
    return row


def _lease_payload(
    row: sqlite3.Row, *, replayed: bool
) -> dict[str, Any]:
    return {
        "lease_id": row["id"],
        "claim_token": row["claim_token"],
        "cursor_from": row["cursor_from"],
        "cursor_to": row["cursor_to"],
        "status": row["status"],
        "expires_at": row["expires_at"],
        "replayed": replayed,
    }


def _load_lease_changes(
    conn: sqlite3.Connection, lease: sqlite3.Row
) -> list[dict[str, Any]]:
    # event_changes is append-only, so re-reading a frozen lease always yields
    # the exact rows that were handed out at the original claim.
    rows = conn.execute(
        """
        SELECT * FROM event_changes
        WHERE port_id = ? AND seq > ? AND seq <= ?
        ORDER BY seq
        """,
        (lease["port_id"], lease["cursor_from"], lease["cursor_to"]),
    ).fetchall()
    return [_change_dict(row) for row in rows]


def claim_changes(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    consumer_id: str,
    limit: int,
    ttl_seconds: int,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Claim the next ordered change batch for a consumer.

    Returns the lease (or ``None`` when nothing new is available) and the
    change rows covered by it.  A repeated claim within the validity window
    returns the original lease and boundaries with ``replayed=true``.
    """
    current_time = (now or _utc_now)()
    with write_transaction(conn):
        port = get_port(conn, port_code)
        moment = current_time
        stamp = utc_now_iso()

        consumer = conn.execute(
            """
            INSERT INTO change_consumers
                (port_id, consumer_id, confirmed_cursor, created_at, updated_at)
            VALUES (?, ?, 0, ?, ?)
            ON CONFLICT(port_id, consumer_id)
            DO UPDATE SET updated_at = excluded.updated_at
            RETURNING id, confirmed_cursor
            """,
            (port.id, consumer_id, stamp, stamp),
        ).fetchone()
        consumer_pk = consumer["id"]
        confirmed_cursor = int(consumer["confirmed_cursor"])

        active = conn.execute(
            "SELECT * FROM change_leases WHERE consumer_pk_id = ? "
            "AND status = 'active'",
            (consumer_pk,),
        ).fetchone()

        if active is not None:
            if _parse_iso(active["expires_at"]) > moment:
                # Valid outstanding lease: hand back the identical batch.
                # Newly uploaded changes are deliberately excluded and wait
                # for the next claim after this lease is settled or expires.
                changes = _load_lease_changes(conn, active)
                return {
                    "port_id": port.id,
                    "consumer_id": consumer_id,
                    "lease": _lease_payload(active, replayed=True),
                    "changes": changes,
                    "change_count": len(changes),
                    "next_cursor": active["cursor_to"],
                    "has_more": _has_more(conn, port.id, active["cursor_to"]),
                }
            # Deadline passed without acknowledgement: the batch can be
            # re-claimed.  Expire it now; a replacement claim below will mark
            # it superseded once the new lease exists.
            conn.execute(
                "UPDATE change_leases SET status = 'expired' WHERE id = ?",
                (active["id"],),
            )
            conn.execute(
                "UPDATE change_consumers SET active_lease_id = NULL WHERE id = ?",
                (consumer_pk,),
            )

        rows = conn.execute(
            """
            SELECT * FROM event_changes
            WHERE port_id = ? AND seq > ?
            ORDER BY seq
            LIMIT ?
            """,
            (port.id, confirmed_cursor, limit),
        ).fetchall()

        if not rows:
            # Nothing new: no lease is created, so the consumer simply polls
            # again later.  Its confirmed cursor is unchanged.
            return {
                "port_id": port.id,
                "consumer_id": consumer_id,
                "lease": None,
                "changes": [],
                "change_count": 0,
                "next_cursor": confirmed_cursor,
                "has_more": False,
            }

        cursor_to = int(rows[-1]["seq"])
        expires_at = _iso(moment + timedelta(seconds=ttl_seconds))
        token = TOKEN_PREFIX + secrets.token_urlsafe(24)
        lease_cursor = conn.execute(
            """
            INSERT INTO change_leases
                (port_id, consumer_pk_id, claim_token, cursor_from, cursor_to,
                 status, expires_at, created_at)
            VALUES (?, ?, ?, ?, ?, 'active', ?, ?)
            """,
            (
                port.id,
                consumer_pk,
                token,
                confirmed_cursor,
                cursor_to,
                expires_at,
                stamp,
            ),
        )
        lease_id = int(lease_cursor.lastrowid)

        # An expired lease replaced by this fresh one is explicitly recorded
        # as superseded, so its stale token can never pass the new batch.
        conn.execute(
            """
            UPDATE change_leases
            SET status = 'superseded'
            WHERE consumer_pk_id = ? AND status = 'expired'
            """,
            (consumer_pk,),
        )
        conn.execute(
            "UPDATE change_consumers SET active_lease_id = ?, updated_at = ? "
            "WHERE id = ?",
            (lease_id, stamp, consumer_pk),
        )

        lease = conn.execute(
            "SELECT * FROM change_leases WHERE id = ?", (lease_id,)
        ).fetchone()
        changes = [_change_dict(row) for row in rows]
        return {
            "port_id": port.id,
            "consumer_id": consumer_id,
            "lease": _lease_payload(lease, replayed=False),
            "changes": changes,
            "change_count": len(changes),
            "next_cursor": cursor_to,
            "has_more": _has_more(conn, port.id, cursor_to),
        }


def _has_more(conn: sqlite3.Connection, port_id: int, cursor_to: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM event_changes WHERE port_id = ? AND seq > ? LIMIT 1",
        (port_id, cursor_to),
    ).fetchone()
    return row is not None


def acknowledge_changes(
    conn: sqlite3.Connection,
    *,
    port_code: str,
    consumer_id: str,
    claim_token: str,
    now: Callable[[], datetime] | None = None,
) -> dict[str, Any]:
    """Acknowledge a claimed batch and advance the consumer cursor.

    Only the token of the current, unexpired lease succeeds.  Unknown tokens,
    expired/superseded leases and a lease whose deadline has just passed are
    all rejected without moving the confirmed cursor.
    """
    moment = (now or _utc_now)()
    with write_transaction(conn):
        port = get_port(conn, port_code)
        consumer = _get_consumer(conn, port.id, consumer_id)
        consumer_pk = consumer["id"]

        lease = conn.execute(
            "SELECT * FROM change_leases WHERE claim_token = ?",
            (claim_token,),
        ).fetchone()
        if lease is None or lease["consumer_pk_id"] != consumer_pk:
            # Do not reveal whether the token belongs to another consumer.
            raise AppError(
                409,
                "invalid_claim_token",
                "the claim token is not valid for this port and consumer",
            )

        if lease["status"] == "acknowledged":
            # Idempotent replay of a completed acknowledgement.
            return _ack_payload(port.id, consumer_id, lease, consumer)

        if lease["status"] == "superseded":
            raise AppError(
                409,
                "lease_superseded",
                (
                    "this batch has been superseded by a newer claim; the "
                    "stale token cannot advance the consumer cursor"
                ),
            )

        if lease["status"] == "expired" or (
            lease["status"] == "active"
            and _parse_iso(lease["expires_at"]) <= moment
        ):
            if lease["status"] == "active":
                conn.execute(
                    "UPDATE change_leases SET status = 'expired' WHERE id = ?",
                    (lease["id"],),
                )
                conn.execute(
                    "UPDATE change_consumers SET active_lease_id = NULL "
                    "WHERE id = ?",
                    (consumer_pk,),
                )
            raise AppError(
                409,
                "lease_expired",
                (
                    "the claim has expired without acknowledgement; re-claim "
                    "the batch and acknowledge it with the new token"
                ),
            )

        # Current, unexpired lease: this is the only path that moves the
        # confirmed cursor.  Lease state and consumer cursor commit together.
        stamp = utc_now_iso()
        conn.execute(
            """
            UPDATE change_leases
            SET status = 'acknowledged', acknowledged_at = ?
            WHERE id = ?
            """,
            (stamp, lease["id"]),
        )
        conn.execute(
            """
            UPDATE change_consumers
            SET confirmed_cursor = ?, active_lease_id = NULL, updated_at = ?
            WHERE id = ?
            """,
            (lease["cursor_to"], stamp, consumer_pk),
        )
        refreshed = conn.execute(
            "SELECT * FROM change_leases WHERE id = ?", (lease["id"],)
        ).fetchone()
        updated_consumer = conn.execute(
            "SELECT * FROM change_consumers WHERE id = ?", (consumer_pk,)
        ).fetchone()
        return _ack_payload(port.id, consumer_id, refreshed, updated_consumer)


def _ack_payload(
    port_id: int,
    consumer_id: str,
    lease: sqlite3.Row,
    consumer: sqlite3.Row,
) -> dict[str, Any]:
    return {
        "port_id": port_id,
        "consumer_id": consumer_id,
        "lease_id": lease["id"],
        "confirmed_cursor": consumer["confirmed_cursor"],
        "acknowledged_at": lease["acknowledged_at"],
    }


def get_consumer_state(
    conn: sqlite3.Connection, *, port_code: str, consumer_id: str
) -> dict[str, Any]:
    """Return the persisted progress of one consumer (for inspection/resume)."""
    with read_snapshot(conn):
        # A WAL snapshot gives a fully committed view without taking the write
        # lock and blocking concurrent claims or uploads.
        port = get_port(conn, port_code)
        consumer = _get_consumer(conn, port.id, consumer_id)
        active_lease = None
        if consumer["active_lease_id"] is not None:
            row = conn.execute(
                "SELECT * FROM change_leases WHERE id = ?",
                (consumer["active_lease_id"],),
            ).fetchone()
            # Only a lease that is still active and past no deadline is
            # outstanding; an expired one is awaiting re-claim, not usable.
            if row is not None and row["status"] == "active":
                if _parse_iso(row["expires_at"]) > _utc_now():
                    active_lease = _lease_payload(row, replayed=False)
        return {
            "port_id": port.id,
            "consumer_id": consumer_id,
            "confirmed_cursor": consumer["confirmed_cursor"],
            "active_lease": active_lease,
            "created_at": consumer["created_at"],
            "updated_at": consumer["updated_at"],
        }
