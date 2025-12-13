"""Task functions for recent match collection DAG"""

from typing import List, Dict

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)  # src 디렉토리
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from collector.http_client import HttpClient, RiotAPI
from repository.match_repository import MatchRepository


def fetch_latest_non_root_match(**kwargs) -> Dict:
    """
    가장 최근의 is_root=False 매치를 조회합니다.

    Args:
        **kwargs: Airflow context variables

    Returns:
        Dict: 가장 최근 매치 데이터
    """
    # Initialize repository
    PG_CONN_ID = "data_postgres_connection"
    repo = MatchRepository(conn_id=PG_CONN_ID)

    # Get the latest non-root match
    match_id = repo.get_latest_non_root_match()

    if not match_id:
        raise ValueError("조회할 is_root=False 매치가 없습니다.")

    # Initialize API client to fetch match details
    http = HttpClient()
    api = RiotAPI(http)

    # Fetch match data
    match_data = api.match_by_id(match_id)

    print(f"가장 최근 매치 조회: {match_id}")
    return match_data


def fetch_recent_matches_by_players(**kwargs) -> List[Dict]:
    """
    각 플레이어별로 최근 9개의 매치 정보를 조회합니다.

    Args:
        **kwargs: Airflow context variables

    Returns:
        List[Dict]: 조회된 모든 매치 데이터 리스트
    """
    # Get parameters from Airflow context
    params = kwargs['params']
    per_player: int = params.get("per_player", 9)  # 기본값 9개

    # Get root match from previous task (XCom)
    ti = kwargs['ti']
    root_match = ti.xcom_pull(task_ids='fetch_latest_non_root_match')

    # Initialize clients
    http = HttpClient()
    api = RiotAPI(http)
    PG_CONN_ID = "data_postgres_connection"
    repo = MatchRepository(conn_id=PG_CONN_ID)

    # Collect participant PUUIDs
    puuids: List[str] = root_match["metadata"]["participants"]
    root_match_id = root_match["metadata"]["matchId"]
    all_match_ids: set[str] = set()
    all_match_ids.add(root_match_id)

    # 각 플레이어별로 최근 매치 9개 조회
    for puuid in puuids:
        match_ids = api.match_ids_by_puuid(
            puuid=puuid,
            count=per_player,
            queue=420,  # Solo queue only
        )

        print(f"플레이어 {puuid[:8]}...의 최근 {len(match_ids)}개 경기 조회")

        # 최근 경기에서 root match를 제외하고 추가
        filtered_matches = [mid for mid in match_ids if mid != root_match_id]
        all_match_ids.update(filtered_matches)

    # 기존 DB에 저장된 매치 확인
    existing_match_ids = repo.get_existing_match_ids(list(all_match_ids))

    # 새로 조회할 매치 필터링
    new_match_ids = all_match_ids - existing_match_ids

    print(f"총 {len(all_match_ids)}개 경기 중 {len(existing_match_ids)}개는 이미 저장됨")
    print(f"새로 조회할 경기: {len(new_match_ids)}개")

    # 새 매치만 상세 정보 조회
    all_matches: Dict[str, Dict] = {}

    # Root match는 이미 상세 정보 있으므로 바로 추가
    all_matches[root_match_id] = root_match

    # DB에 없는 매치들만 상세 정보 조회
    for match_id in new_match_ids:
        if match_id == root_match_id:  # Root match는 이미 처리됨
            continue
        match_detail = api.match_by_id(match_id)
        all_matches[match_id] = match_detail

    return list(all_matches.values())


def save_all_matches_to_postgres(**kwargs):
    """
    조회된 모든 매치를 PostgreSQL 데이터베이스에 저장합니다.

    Args:
        **kwargs: Airflow context variables
    """
    # Get matches from previous task (XCom)
    ti = kwargs['ti']
    matches_json = ti.xcom_pull(task_ids='fetch_recent_matches_by_players')

    if not matches_json:
        print("저장할 매치 데이터가 없습니다.")
        return

    # Initialize repository
    PG_CONN_ID = "data_postgres_connection"
    repo = MatchRepository(conn_id=PG_CONN_ID)

    # 모든 매치와 참가자 정보 저장 (is_root=False로 설정)
    for match_json in matches_json:
        repo.upsert_match(match_json, is_root=False)
        repo.upsert_participants(match_json)

    print(f"{len(matches_json)}개 경기 저장 완료 (is_root=False)")