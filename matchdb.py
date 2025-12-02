import sys
from pathlib import Path
from typing import List, Dict

from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

# Airflow 환경에서 src 모듈을 import하기 위해 경로 설정
sys.path.append("/opt/airflow/dags/git/repo")

# Riot API 클라이언트 경로
from src.collector.http_client import HttpClient, RiotAPI


PG_CONN_ID = "data_postgres_connection"


# ---------- DB helper ----------

def upsert_summoner(pg: PostgresHook, puuid: str, summoner_name: str | None = None,
                    summoner_id: str | None = None):
    sql = """
    INSERT INTO summoners (puuid, summoner_id, summoner_name)
    VALUES (%s, %s, %s)
    ON CONFLICT (puuid) DO UPDATE
    SET summoner_id   = COALESCE(EXCLUDED.summoner_id, summoners.summoner_id),
        summoner_name = COALESCE(EXCLUDED.summoner_name, summoners.summoner_name);
    """
    pg.run(sql, parameters=(puuid, summoner_id, summoner_name))


def upsert_match(pg: PostgresHook, match_json: Dict, is_root: bool):
    match_id = match_json["metadata"]["matchId"]
    queue_id = match_json["info"].get("queueId", 0)

    sql = """
    INSERT INTO matches (match_id, queue_id, match_json, is_root)
    VALUES (%s, %s, %s::jsonb, %s)
    ON CONFLICT (match_id) DO UPDATE
    SET queue_id   = EXCLUDED.queue_id,
        match_json = EXCLUDED.match_json,
        is_root    = matches.is_root OR EXCLUDED.is_root;
    """
    pg.run(sql, parameters=(match_id, queue_id, Json(match_json), is_root))


def upsert_participants(pg: PostgresHook, match_json: Dict):
    match_id = match_json["metadata"]["matchId"]
    participants = match_json["info"]["participants"]

    rows = []
    for p in participants:
        puuid = p["puuid"]
        participant_no = p["participantId"]
        team_id = p["teamId"]           # 100 / 200
        champion_id = p["championId"]

        rows.append(
            (match_id, puuid, participant_no, team_id, champion_id)
        )

        # summoners 테이블도 같이 upsert
        upsert_summoner(
            pg,
            puuid=puuid,
            summoner_name=p.get("summonerName"),
            summoner_id=p.get("summonerId"),
        )

    pg.insert_rows(
        table="participants",
        rows=rows,
        target_fields=[
            "match_id",
            "puuid",
            "participant_no",
            "team_id",
            "champion_id",
        ],
        commit_every=50,
        replace=True,
        replace_index=["match_id", "puuid"],
    )


def upsert_masteries(pg: PostgresHook, puuid: str, mastery_list: List[Dict]):
    """
    mastery_list: Riot API champion-mastery-v4 결과(JSON 배열)
    """
    rows = []
    for m in mastery_list:
        rows.append(
            (
                puuid,
                m["championId"],
                m.get("championLevel"),
                m.get("championPoints"),
                m.get("tokensEarned", 0),
            )
        )

    if not rows:
        return

    sql = """
    INSERT INTO champion_masteries (
      puuid, champion_id, champion_level,
      champion_points, tokens_earned
    )
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (puuid, champion_id) DO UPDATE
    SET champion_level  = EXCLUDED.champion_level,
        champion_points = EXCLUDED.champion_points,
        tokens_earned   = EXCLUDED.tokens_earned;
    """
    conn = pg.get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)


# ---------- DAG ----------

@dag(
    dag_id="riot_match_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,   # 나중에 자동화하려면 예: "0 * * * *" (매 시 정각)
    catchup=False,
    params={
        "root_match_id": "KR_7917227565",
        "per_player": 8,     # 최근 경기 수
    },
    max_active_runs=1,
)
def riot_match_to_postgres():

    @task
    def fetch_root_match(params) -> Dict:
        root_match_id: str = params["root_match_id"]

        http = HttpClient()
        api = RiotAPI(http)

        # Riot Match-v5: GET /lol/match/v5/matches/{matchId}
        root_match = api.match_by_id(root_match_id)
        return root_match

    @task
    def fetch_recent_matches(root_match: Dict, params) -> List[Dict]:
        per_player: int = params["per_player"]

        http = HttpClient()
        api = RiotAPI(http)

        puuids: List[str] = root_match["metadata"]["participants"]

        all_matches: Dict[str, Dict] = {}  # match_id 중복 방지

        # 10명 * per_player 경기씩 조회
        for puuid in puuids:
            # Riot Match-v5: GET /lol/match/v5/matches/by-puuid/{puuid}/ids?count=...
            match_ids = api.match_ids_by_puuid(
                puuid=puuid,
                count=per_player,
                queue=420,  # 솔랭만
            )
            for mid in match_ids:
                if mid in all_matches:
                    continue
                all_matches[mid] = api.match_by_id(mid)

        # root match도 포함해서 저장
        all_matches[root_match["metadata"]["matchId"]] = root_match

        return list(all_matches.values())

    @task
    def save_all_to_postgres(matches_json: List[Dict]):
        pg = PostgresHook(postgres_conn_id=PG_CONN_ID)
        http = HttpClient()
        api = RiotAPI(http)

        # root match는 is_root = True, 나머지는 False로 처리
        # root 여부는 "가장 처음" 매치로 간주
        if not matches_json:
            return

        root_id = matches_json[0]["metadata"]["matchId"]

        for match_json in matches_json:
            match_id = match_json["metadata"]["matchId"]
            is_root = (match_id == root_id)

            # 1) matches
            upsert_match(pg, match_json, is_root=is_root)

            # 2) participants + summoners
            upsert_participants(pg, match_json)

        # 3) mastery 정보 (puuid 기준으로 한 번씩 호출)
        puuid_set = {
            p["puuid"]
            for m in matches_json
            for p in m["info"]["participants"]
        }

        for puuid in puuid_set:
            mastery_list = api.get_champion_masteries(puuid)  # champion-mastery-v4
            upsert_masteries(pg, puuid, mastery_list)

    root_match = fetch_root_match()
    matches_json = fetch_recent_matches(root_match)
    save_all_to_postgres(matches_json)


dag = riot_match_to_postgres()