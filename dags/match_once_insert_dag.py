import sys
from typing import List, Dict
from datetime import datetime

from airflow.sdk import dag, task

# Airflow 환경에서 src 모듈을 import하기 위해 경로 설정
sys.path.append("/opt/airflow/dags/git/repo")

# Riot API 클라이언트 경로
from src.collector.http_client import HttpClient, RiotAPI
# [NEW] 새로 만든 Repository 경로
from src.repository.match_repository import MatchRepository

PG_CONN_ID = "data_postgres_connection"


@dag(
    dag_id="riot_match_to_postgres_once",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "root_match_id": "KR_7917227565",
        "per_player": 8,  # 최근 경기 수
    },
    max_active_runs=1,
)
def riot_match_to_postgres():
    @task
    def fetch_root_match(params) -> Dict:
        root_match_id: str = params["root_match_id"]
        http = HttpClient()
        api = RiotAPI(http)

        return api.match_by_id(root_match_id)

    @task
    def fetch_recent_matches(root_match: Dict, params) -> List[Dict]:
        per_player: int = params["per_player"]
        http = HttpClient()
        api = RiotAPI(http)

        puuids: List[str] = root_match["metadata"]["participants"]
        all_matches: Dict[str, Dict] = {}

        # 10명 * per_player 경기씩 조회
        for puuid in puuids:
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
        if not matches_json:
            return

        # [Refactored] Repository를 사용하여 저장 로직 수행
        repo = MatchRepository(conn_id=PG_CONN_ID)
        http = HttpClient()
        api = RiotAPI(http)

        root_id = matches_json[0]["metadata"]["matchId"]

        # 1. Match & Participant 저장
        for match_json in matches_json:
            match_id = match_json["metadata"]["matchId"]
            is_root = (match_id == root_id)

            repo.upsert_match(match_json, is_root=is_root)
            repo.upsert_participants(match_json)

        # 2. Mastery 저장
        puuid_set = {
            p["puuid"]
            for m in matches_json
            for p in m["info"]["participants"]
        }

        for puuid in puuid_set:
            mastery_list = api.get_champion_masteries(puuid)
            repo.upsert_masteries(puuid, mastery_list)

    # DAG Flow
    root_match = fetch_root_match()
    matches_json = fetch_recent_matches(root_match)
    save_all_to_postgres(matches_json)


dag = riot_match_to_postgres()