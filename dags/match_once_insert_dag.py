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
        repo = MatchRepository(conn_id=PG_CONN_ID)

        puuids: List[str] = root_match["metadata"]["participants"]
        all_match_ids: set[str] = set()

        # 1. 각 플레이어별로 최근 경기 ID 목록 수집 (중복 제거)
        for puuid in puuids:
            match_ids = api.match_ids_by_puuid(
                puuid=puuid,
                count=per_player,
                queue=420,  # 솔랭만
            )
            all_match_ids.update(match_ids)

        # 2. root match도 포함
        root_match_id = root_match["metadata"]["matchId"]
        all_match_ids.add(root_match_id)

        # 3. DB에 이미 저장된 경기 ID들 확인
        existing_match_ids = repo.get_existing_match_ids(list(all_match_ids))

        # 4. DB에 없는 경기 ID들만 필터링
        new_match_ids = all_match_ids - existing_match_ids

        print(f"총 {len(all_match_ids)}개 경기 중 {len(existing_match_ids)}개는 이미 저장됨")
        print(f"새로 조회할 경기: {len(new_match_ids)}개")

        # 5. 새로운 경기들만 상세 정보 조회
        all_matches: Dict[str, Dict] = {}

        # root match는 이미 상세 정보가 있으므로 바로 추가
        all_matches[root_match_id] = root_match

        # DB에 없는 경기들만 API로 상세 정보 조회
        for match_id in new_match_ids:
            if match_id == root_match_id:  # root match는 이미 처리됨
                continue
            match_detail = api.match_by_id(match_id)
            all_matches[match_id] = match_detail

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

        # 2. Mastery 저장 (TODO: get_champion_masteries 구현 필요)
        # 현재는 주석 처리하여 오류 방지
        # puuid_set = {
        #     p["puuid"]
        #     for m in matches_json
        #     for p in m["info"]["participants"]
        # }

        # for puuid in puuid_set:
        #     mastery_list = api.get_champion_masteries(puuid)
        #     repo.upsert_masteries(puuid, mastery_list)

    # DAG Flow
    root_match = fetch_root_match()
    matches_json = fetch_recent_matches(root_match)
    save_all_to_postgres(matches_json)


dag = riot_match_to_postgres()