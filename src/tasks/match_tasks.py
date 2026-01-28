"""Individual task functions for match processing DAG"""

from typing import List, Dict

import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)  # src 디렉토리
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from collector.http_client import HttpClient, RiotAPI
from repository.match_repository import MatchRepository


def fetch_root_match(**kwargs) -> Dict:
    """
    Fetch a single match by ID from Riot API

    Args:
        **kwargs: Airflow context variables

    Returns:
        Dict: Match data from Riot API
    """
    # Get parameters from Airflow context
    params = kwargs['params']
    root_match_id: str = params["root_match_id"]

    # Initialize API client
    http = HttpClient()
    api = RiotAPI(http)

    # Fetch match data
    return api.match_by_id(root_match_id)


def fetch_recent_matches(**kwargs) -> List[Dict]:
    """
    Fetch recent matches for all participants of a root match

    Args:
        **kwargs: Airflow context variables

    Returns:
        List[Dict]: List of match data
    """
    # Get parameters from Airflow context
    params = kwargs['params']
    per_player: int = params["per_player"]

    # Get root match from previous task (XCom)
    ti = kwargs['ti']
    root_match = ti.xcom_pull(task_ids='fetch_root_match')

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

    # 1. Collect match IDs for each player and filter matches before root match
    for puuid in puuids:
        match_ids = api.match_ids_by_puuid(
            puuid=puuid,
            count=100,
            queue=420,  # Solo queue only
        )
        # Find root match index in the match list
        if root_match_id in match_ids:
            root_index = match_ids.index(root_match_id)
        
            print(f"플레이어 {puuid[:8]}... (총 {len(match_ids)}개 경기 기록)")
            print(f"  - Root match {root_match_id}의 인덱스: {root_index}")
        
            # root match "이후"(더 과거) 경기들 선택
            start_index = root_index + 1
            end_index = min(root_index + int(per_player) + 1, len(match_ids))
            selected_matches = match_ids[start_index:end_index]
        
        else:
            # ✅ root match가 목록에 없으면: 최신 경기에서 per_player개 폴백
            print(f"[WARN] Root match {root_match_id} not found for puuid={puuid[:8]}... "
                  f"(count={len(match_ids)}). Fallback to most recent {per_player} matches.")
            selected_matches = match_ids[:int(per_player)]

        # Select matches before root match (older matches)
        # Take per_player matches from the ones before root match
        start_index = root_index+1
        end_index = min(root_index + int(per_player) + 1, 99)
        selected_matches = match_ids[start_index:end_index]

        all_match_ids.update(selected_matches)

    # 3. Check existing matches in database
    existing_match_ids = repo.get_existing_match_ids(list(all_match_ids))

    # 4. Filter only new match IDs
    new_match_ids = all_match_ids - existing_match_ids

    print(f"총 {len(all_match_ids)}개 경기 중 {len(existing_match_ids)}개는 이미 저장됨")
    print(f"새로 조회할 경기: {len(new_match_ids)}개")

    # 5. Fetch detailed information for new matches only
    all_matches: Dict[str, Dict] = {}

    # Root match already has detailed info, add directly
    all_matches[root_match_id] = root_match

    # Fetch details only for matches not in DB
    for match_id in new_match_ids:
        if match_id == root_match_id:  # Root match already processed
            continue
        match_detail = api.match_by_id(match_id)
        all_matches[match_id] = match_detail

    return list(all_matches.values())


def save_all_to_postgres(**kwargs):
    """
    Save matches and participants to PostgreSQL database

    Args:
        **kwargs: Airflow context variables
    """
    # Get matches from previous task (XCom)
    ti = kwargs['ti']
    matches_json = ti.xcom_pull(task_ids='fetch_recent_matches')

    if not matches_json:
        return

    # Initialize repository
    PG_CONN_ID = "data_postgres_connection"
    repo = MatchRepository(conn_id=PG_CONN_ID)

    root_id = matches_json[0]["metadata"]["matchId"]

    # 1. Save Match & Participant data
    for match_json in matches_json:
        match_id = match_json["metadata"]["matchId"]
        is_root = (match_id == root_id)

        repo.upsert_match(match_json, is_root=is_root)
        repo.upsert_participants(match_json)

    print(f"{len(matches_json)}개 경기 저장 완료")

def save_champion_mastery(**kwargs):
    ti = kwargs["ti"]
    matches_json = ti.xcom_pull(task_ids="fetch_recent_matches")

    if not matches_json:
        print("[save_champion_masteries] No matches_json. Skip.")
        return

    http = HttpClient()
    api = RiotAPI(http)

    PG_CONN_ID = "data_postgres_connection"
    repo = MatchRepository(conn_id=PG_CONN_ID)

    # 1) match에서 실제 사용한 (puuid, championId)만 수집
    pairs = set()
    for m in matches_json:
        for p in m["info"]["participants"]:
            puuid = p.get("puuid")
            champ = p.get("championId")
            if puuid and champ is not None:
                pairs.add((puuid, int(champ)))

    print(f"[save_champion_masteries] unique (puuid,champ) pairs = {len(pairs)}")

    # 2) (puuid, championId) 단건 mastery 조회 후 저장
    for puuid, champ_id in pairs:
        try:
            mastery = api.champion_mastery_by_puuid_and_champion(puuid, champ_id)
        except Exception as e:
            print(f"[WARN] mastery fetch failed puuid={puuid[:8]} champ={champ_id}: {e}")
            continue

        if not mastery:
            continue

        repo.upsert_mastery_one(puuid, champ_id, mastery)

    print("[save_champion_masteries] done")