from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    import psycopg2
    from psycopg2.extras import Json
except Exception:  # pragma: no cover
    psycopg2 = None
    Json = None

_CONN = None


def init_db(dsn: Optional[str] = None) -> None:
    """Initialize global Postgres connection and ensure cache tables exist."""
    global _CONN
    dsn = dsn or os.getenv("POSTGRES_DSN", "")

    if not dsn:
        _CONN = None
        return

    if psycopg2 is None:
        raise RuntimeError('psycopg2 is not installed but POSTGRES_DSN was provided')


    _CONN = psycopg2.connect(dsn)
    _CONN.autocommit = True

    with _CONN.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS player_history_cache (
                puuid TEXT PRIMARY KEY,
                data JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS player_mastery_cache (
                puuid TEXT PRIMARY KEY,
                data JSONB NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )


def close_db() -> None:
    global _CONN
    if _CONN is not None:
        try:
            _CONN.close()
        finally:
            _CONN = None


def get_history(puuid: str) -> Optional[Dict[str, Any]]:
    if _CONN is None:
        return None
    with _CONN.cursor() as cur:
        cur.execute("SELECT data FROM player_history_cache WHERE puuid=%s", (puuid,))
        row = cur.fetchone()
        return row[0] if row else None


def upsert_history(puuid: str, data: Dict[str, Any]) -> None:
    if _CONN is None:
        return
    payload = dict(data)
    payload.setdefault("fetched_at", datetime.now(timezone.utc).isoformat())
    with _CONN.cursor() as cur:
        cur.execute(
            """
            INSERT INTO player_history_cache (puuid, data, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (puuid) DO UPDATE
            SET data = EXCLUDED.data, updated_at = NOW()
            """,
            (puuid, Json(payload)),
        )

"""
def get_mastery(puuid: str) -> Optional[Dict[str, Any]]:
    if _CONN is None:
        return None
    with _CONN.cursor() as cur:
        cur.execute("SELECT data FROM player_mastery_cache WHERE puuid=%s", (puuid,))
        row = cur.fetchone()
        return row[0] if row else None
"""

def upsert_mastery(puuid: str, data: Dict[str, Any]) -> None:
    if _CONN is None:
        return
    payload = dict(data)
    payload.setdefault("fetched_at", datetime.now(timezone.utc).isoformat())
    with _CONN.cursor() as cur:
        cur.execute(
            """
            INSERT INTO player_mastery_cache (puuid, data, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (puuid) DO UPDATE
            SET data = EXCLUDED.data, updated_at = NOW()
            """,
            (puuid, Json(payload)),
        )
