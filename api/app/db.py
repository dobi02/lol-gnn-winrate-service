# app/db.py
from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psycopg2
from psycopg2.extras import Json, execute_batch

_CONN = None


def init_db(dsn: Optional[str] = None) -> None:
    global _CONN
    dsn = dsn or os.getenv("DATABASE_URL", "") or os.getenv("POSTGRES_DSN", "")
    if not dsn:
        _CONN = None
        return
    if psycopg2 is None:
        raise RuntimeError('psycopg2 is not installed')

    _CONN = psycopg2.connect(dsn)
    _CONN.autocommit = True


def close_db() -> None:
    global _CONN
    if _CONN is not None:
        _CONN.close()
        _CONN = None


def get_matches_by_ids(match_ids: List[str]) -> Dict[str, Dict[str, Any]]:
    """
    주어진 match_id 리스트 중에서 DB에 이미 존재하는 매치들을 찾아
    {match_id: match_json} 딕셔너리 형태로 반환합니다.
    """
    if _CONN is None or not match_ids:
        return {}

    # match_ids가 빈 리스트가 아닐 때만 쿼리 실행
    query = """
            SELECT match_id, match_json
            FROM matches
            WHERE match_id = ANY (%s) \
            """

    with _CONN.cursor() as cur:
        # PostgreSQL에서는 리스트를 ANY()로 넘기는 것이 효율적입니다.
        cur.execute(query, (match_ids,))
        rows = cur.fetchall()

        # { "KR_12345": {...json...}, "KR_67890": {...json...} }
        return {row[0]: row[1] for row in rows}


def save_match(match_detail: Dict[str, Any]) -> None:
    """
    Riot API에서 받아온 매치 JSON을 matches, summoners, participants 테이블에 저장합니다.
    """
    if _CONN is None:
        return

    meta = match_detail.get('metadata', {})
    info = match_detail.get('info', {})
    match_id = meta.get('matchId')

    if not match_id:
        return

    # 타임스탬프 변환
    game_start_ts = info.get('gameStartTimestamp', 0)
    game_start_at = datetime.fromtimestamp(game_start_ts / 1000, tz=timezone.utc)
    queue_id = info.get('queueId', 0)

    participants_data = info.get('participants', [])

    with _CONN.cursor() as cur:
        try:
            # 1. Matches 테이블 저장
            # 이미 있으면(Conflict) 아무것도 안 함(DO NOTHING)
            cur.execute("""
                        INSERT INTO matches (match_id, queue_id, match_json, game_start_at)
                        VALUES (%s, %s, %s, %s) ON CONFLICT (match_id) DO NOTHING
                        """, (match_id, queue_id, Json(match_detail), game_start_at))

            # 2. Participants & Summoners 저장
            participant_tuples = []

            for p in participants_data:
                p_puuid = p.get('puuid')
                # 봇전 등에서 puuid가 없는 경우가 간혹 있음
                if not p_puuid or p_puuid == "BOT":
                    continue

                # Summoner 테이블에 먼저 존재해야 FK 제약조건 만족
                cur.execute("""
                            INSERT INTO summoners (puuid)
                            VALUES (%s) ON CONFLICT (puuid) DO NOTHING
                            """, (p_puuid,))

                participant_tuples.append((
                    match_id,
                    p_puuid,
                    p.get('participantId', 0),
                    p.get('teamId', 0),
                    p.get('championId', 0)
                ))

            if participant_tuples:
                execute_batch(cur, """
                                   INSERT INTO participants (match_id, puuid, participant_no, team_id, champion_id)
                                   VALUES (%s, %s, %s, %s, %s) ON CONFLICT (match_id, puuid) DO NOTHING
                                   """, participant_tuples)

        except Exception as e:
            print(f"❌ DB Save Error ({match_id}): {e}")