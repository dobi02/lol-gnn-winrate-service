"""Backfill actual game outcomes for prediction logs.

This DAG fills `prediction_request_log.actual_blue_win` using either:
  1) cached match JSON in Postgres `matches.match_json`, or
  2) Riot Match-V5 API (asia routing) when cache miss.

It is designed to be safe to run frequently (e.g., every 30 minutes).

Requirements/assumptions:
  - Airflow Postgres connection exists (conn id via Variable, default DATA_POSTGRES_CONNECTION)
  - prediction_request_log table exists (can be created manually or by API startup)
  - matches table stores match_id like "KR_1234567890" and match_json (Riot match-v5 payload)
  - Airflow Variable RIOT_API_KEY is set (only needed for cache-miss fetch)
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, Tuple

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable, dag, task


DEFAULT_PG_CONN_ID = "DATA_POSTGRES_CONNECTION"


def _get_pg_conn_id() -> str:
    # Project DAGs already use these Variable names in places.
    return Variable.get("lol_gnn_data_postgres_conn_id", default=DEFAULT_PG_CONN_ID)


def _match_id(platform_id: str, game_id: int) -> str:
    # Riot match-v5 matchId is like "KR_123".
    return f"{platform_id}_{game_id}"


def _extract_blue_win(match_payload: Dict[str, Any]) -> Optional[bool]:
    """Return True if teamId 100 won, False if lost, None if not determinable."""
    info = (match_payload or {}).get("info") or {}

    # Prefer `teams` array if present.
    teams = info.get("teams")
    if isinstance(teams, list):
        for t in teams:
            if isinstance(t, dict) and t.get("teamId") == 100:
                # Riot payloads vary by patch; use a few keys.
                if "win" in t:
                    return bool(t.get("win"))
                if "won" in t:
                    return bool(t.get("won"))
                if "teamWin" in t:
                    return bool(t.get("teamWin"))

    # Fallback: infer from participants if teams missing.
    participants = info.get("participants")
    if isinstance(participants, list) and participants:
        # Find any participant from team 100 and use their win flag.
        for p in participants:
            if isinstance(p, dict) and p.get("teamId") == 100 and "win" in p:
                return bool(p.get("win"))
    return None


def _fetch_match_from_riot(match_id: str, api_key: str) -> Optional[Dict[str, Any]]:
    url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}"
    headers = {"X-Riot-Token": api_key}
    # Timeouts are important to avoid hanging task workers.
    r = requests.get(url, headers=headers, timeout=(3.0, 15.0))
    if r.status_code == 200:
        return r.json()
    if r.status_code == 404:
        return None
    # 429/403 etc should be visible in logs.
    raise RuntimeError(f"Riot API error {r.status_code}: {r.text[:300]}")


@dag(
    dag_id="lol_backfill_actual_results",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="*/30 * * * *",  # every 30 minutes
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2},
    tags=["monitoring", "labels", "backfill"],
    doc_md=__doc__,
)
def backfill_actual_results_dag():
    @task
    def select_pending(limit: int = 200, min_age_minutes: int = 10) -> List[Dict[str, Any]]:
        """Select pending rows that likely have finished games."""
        hook = PostgresHook(postgres_conn_id=_get_pg_conn_id())
        sql = """
            SELECT DISTINCT platform_id, game_id
            FROM prediction_request_log
            WHERE actual_blue_win IS NULL
              AND game_id IS NOT NULL
              AND created_at < now() - (%s || ' minutes')::interval
            ORDER BY game_id ASC
            LIMIT %s;
        """
        rows = hook.get_records(sql, parameters=(min_age_minutes, limit))
        return [{"platform_id": r[0], "game_id": int(r[1])} for r in rows]

    @task
    def backfill(pendings: List[Dict[str, Any]], sleep_between_sec: float = 0.2) -> Dict[str, Any]:
        """Fill actual results using DB cache first, then Riot API."""
        hook = PostgresHook(postgres_conn_id=_get_pg_conn_id())
        api_key = None
        try:
            api_key = Variable.get("RIOT_API_KEY")
        except Exception:
            # Only needed on cache misses.
            api_key = None

        updated = 0
        skipped_unknown = 0
        cache_hits = 0
        cache_misses = 0
        errors = 0

        for item in pendings or []:
            platform_id = str(item.get("platform_id") or "KR")
            game_id = int(item.get("game_id"))
            mid = _match_id(platform_id, game_id)

            match_payload = None
            # 1) Try DB cache
            try:
                rec = hook.get_first(
                    "SELECT match_json FROM matches WHERE match_id = %s;",
                    parameters=(mid,),
                )
                if rec and rec[0] is not None:
                    # psycopg2 returns dict for JSONB in many setups; but may be str.
                    match_payload = rec[0]
                    if isinstance(match_payload, str):
                        match_payload = json.loads(match_payload)
                    cache_hits += 1
                else:
                    cache_misses += 1
            except Exception as e:
                errors += 1
                print(f"[WARN] DB cache lookup failed for {mid}: {e}")

            # 2) Cache miss -> Riot
            if match_payload is None:
                if not api_key:
                    print(f"[WARN] RIOT_API_KEY not set; cannot fetch match {mid}. Skipping.")
                    continue
                try:
                    match_payload = _fetch_match_from_riot(mid, api_key)
                except Exception as e:
                    errors += 1
                    print(f"[WARN] Riot fetch failed for {mid}: {e}")
                    continue

            if not match_payload:
                # 404 or empty
                continue

            blue_win = _extract_blue_win(match_payload)
            if blue_win is None:
                skipped_unknown += 1
                continue

            try:
                hook.run(
                    """
                    UPDATE prediction_request_log
                    SET actual_blue_win = %s,
                        actual_fetched_at = now()
                    WHERE platform_id = %s
                      AND game_id = %s
                      AND actual_blue_win IS NULL;
                    """,
                    parameters=(blue_win, platform_id, game_id),
                )
                updated += 1
            except Exception as e:
                errors += 1
                print(f"[WARN] Update failed for {platform_id}/{game_id}: {e}")

            if sleep_between_sec:
                time.sleep(sleep_between_sec)

        summary = {
            "pending": len(pendings or []),
            "updated": updated,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "skipped_unknown": skipped_unknown,
            "errors": errors,
        }
        print(f"Backfill summary: {summary}")
        return summary

    pendings = select_pending()
    backfill(pendings)


dag = backfill_actual_results_dag()
