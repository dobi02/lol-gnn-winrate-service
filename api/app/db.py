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

def create_log_tables() -> None:
    """
    로그 테이블 생성은 init_db와 분리.
    서버 startup(lifespan)에서 init_db() 다음에 한 번 호출하는 것을 권장.
    """
    if _CONN is None:
        return

    with _CONN.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS prediction_request_log (
              id                   BIGSERIAL PRIMARY KEY,
              created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),   -- 요청 시간

              trace_id             TEXT NOT NULL,                        -- 요청 추적(없으면 UUID 추천)
              endpoint             TEXT NOT NULL,
              platform_id          TEXT NOT NULL,
              game_id              BIGINT,

              success              BOOLEAN NOT NULL,                     -- 성공 여부
              status_code          INT NOT NULL,
              error_message        TEXT,

              latency_ms           INT NOT NULL,                         -- 처리 시간(ms)
              pred_blue_win_prob   DOUBLE PRECISION,                     -- 반환 결과(블루 승률)

              actual_blue_win      BOOLEAN,                              -- 실제 결과(나중에 batch update)
              actual_fetched_at    TIMESTAMPTZ                           -- 실제 결과 반영 시간
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_prl_created_at ON prediction_request_log (created_at DESC);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_prl_game_platform ON prediction_request_log (platform_id, game_id);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_prl_trace_id ON prediction_request_log (trace_id);"
        )


def insert_prediction_log(
    *,
    trace_id: str,
    endpoint: str,
    platform_id: str,
    game_id: Optional[int],
    success: bool,
    status_code: int,
    latency_ms: int,
    pred_blue_win_prob: Optional[float],
    error_message: Optional[str] = None,
) -> None:
    """
    요청 1건당 1 row INSERT.
    - 성공/실패 모두 기록 가능
    - actual_blue_win은 나중 batch가 업데이트
    """
    if _CONN is None:
        return

    # 너무 긴 에러메시지는 컷(선택)
    if error_message and len(error_message) > 2000:
        error_message = error_message[:2000]

    with _CONN.cursor() as cur:
        cur.execute(
            """
            INSERT INTO prediction_request_log (
              trace_id, endpoint, platform_id, game_id,
              success, status_code, error_message,
              latency_ms, pred_blue_win_prob
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
            """,
            (
                trace_id,
                endpoint,
                platform_id,
                game_id,
                success,
                status_code,
                error_message,
                latency_ms,
                pred_blue_win_prob,
            ),
        )


def update_actual_result(
    *,
    platform_id: str,
    game_id: int,
    actual_blue_win: bool,
) -> int:
    """
    나중에 batch 작업에서 실제 승패를 채우는 용도.
    해당 (platform_id, game_id)에 대해 업데이트된 row 수를 반환.
    """
    if _CONN is None:
        return 0

    with _CONN.cursor() as cur:
        cur.execute(
            """
            UPDATE prediction_request_log
            SET actual_blue_win = %s,
                actual_fetched_at = now()
            WHERE platform_id = %s
              AND game_id = %s
              AND actual_blue_win IS NULL;
            """,
            (actual_blue_win, platform_id, game_id),
        )
        return cur.rowcount
