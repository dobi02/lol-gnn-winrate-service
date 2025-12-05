"""Individual task functions for match processing DAG"""

from typing import List, Dict
from src.collector.http_client import HttpClient, RiotAPI
from src.repository.match_repository import MatchRepository


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
    all_match_ids: set[str] = set()

    # 1. Collect recent match IDs for each player (with deduplication)
    for puuid in puuids:
        match_ids = api.match_ids_by_puuid(
            puuid=puuid,
            count=per_player,
            queue=420,  # Solo queue only
        )
        all_match_ids.update(match_ids)

    # 2. Include root match
    root_match_id = root_match["metadata"]["matchId"]
    all_match_ids.add(root_match_id)

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

    # 2. Save Mastery data (TODO: implement get_champion_masteries)
    # Currently commented out to prevent errors
    # puuid_set = {
    #     p["puuid"]
    #     for m in matches_json
    #     for p in m["info"]["participants"]
    # }

    # for puuid in puuid_set:
    #     mastery_list = api.get_champion_masteries(puuid)
    #     repo.upsert_masteries(puuid, mastery_list)