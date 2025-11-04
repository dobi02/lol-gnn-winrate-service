import json
import os
import random
from collections import deque


from src.repository.riot_api_repository import RiotApiRepository
from src.repository.file_repository import FileRepository
from src.model.riot_dto import MatchDto, AccountDto

def load_known_puuids(file_repo: FileRepository) -> dict[str, dict]:
    """
    'known_puuids.json' 파일에서 알려진 PUUID 목록을 로드하는 함수.
    """
    known_puuids_file = os.path.join(file_repo.data_dir, "known_puuids.json")
    if os.path.exists(known_puuids_file):
        with open(known_puuids_file, 'r', encoding='utf-8') as f:
            return {acc['puuid']: acc for acc in json.load(f)}
    return {}

def save_known_puuids(file_repo: FileRepository, known_puuids: dict[str, dict]):
    """
    알려진 PUUID 목록을 'known_puuids.json' 파일에 저장하는 함수.
    """
    known_puuids_file = os.path.join(file_repo.data_dir, "known_puuids.json")
    with open(known_puuids_file, 'w', encoding='utf-8') as f:
        json.dump(list(known_puuids.values()), f, ensure_ascii=False, indent=4)
    print(f"--- Saved {len(known_puuids)} PUUIDs to {known_puuids_file} ---\n")


def get_sample_puuid(riot_api_repo: RiotApiRepository) -> str | None:
    """
    샘플 Riot ID를 사용하여 PUUID를 가져오는 함수.
    """
    # game_name = "드래곤무조건챙김"
    # tag_line = "KR1"
    game_name = "kiin"
    tag_line = "KR1"
    print(f"--- Retrieving PUUID for {game_name}#{tag_line} ---\n")
    puuid = riot_api_repo.get_puuid_by_riot_id(game_name, tag_line)
    if puuid:
        print(f"  Successfully retrieved PUUID: {puuid}\n")
    else:
        print(f"  Failed to retrieve PUUID for {game_name}#{tag_line}\n")
    return puuid

def get_match_ids_by_puuid(riot_api_repo: RiotApiRepository, puuid: str, count: int = 20) -> list[str] | None:
    """
    PUUID를 사용하여 매치 ID 목록을 가져오는 함수.
    """
    print(f"--- Testing get_match_ids_by_puuid with PUUID: {puuid} ---\n")
    match_ids = riot_api_repo.get_match_ids_by_puuid(puuid, count=count)
    if match_ids:
        print(f"  Found {len(match_ids)} match IDs: {match_ids[:5]}...\n")
    else:
        print("  No match IDs found.\n")
    return match_ids

def get_match_data(riot_api_repo: RiotApiRepository, match_id: str) -> MatchDto | None:
    """
    매치 ID를 사용하여 매치 상세 정보를 가져오는 함수.
    """
    print(f"--- Fetching match data for ID: {match_id} ---\n")
    match_data = riot_api_repo.get_match_by_id(match_id)
    if not match_data:
        print(f"  Failed to retrieve match data for ID: {match_id}\n")
    return match_data

def save_match_data_as_json(file_repo: FileRepository, match_id: str, match_data: MatchDto):
    """
    매치 데이터를 JSON 파일로 저장하는 함수.
    """
    print(f"--- Attempting to save match data for {match_id} ---\n") # Debug print
    file_repo.save_match_data_as_json(match_id, match_data.model_dump())
    print(f"--- Finished saving match data for {match_id} ---\n") # Debug print


def extract_and_save_new_puuids(riot_api_repo: RiotApiRepository, file_repo: FileRepository, match_data: MatchDto, known_puuids_data: dict[str, dict]) -> dict[str, dict]:
    """
    매치 데이터에서 참가자들의 PUUID를 추출하고,
    새로운 PUUID인 경우 해당 계정 정보를 가져와 전달받은 known_puuids_data를 업데이트합니다.
    """
    print("--- Extracting new PUUIDs from match data ---\n")
    new_puuids_found_in_match = 0

    for participant in match_data.info.participants:
        participant_puuid = participant.puuid
        if participant_puuid not in known_puuids_data:
            account_dto: AccountDto | None = riot_api_repo.get_account_by_puuid(participant_puuid)
            if account_dto:
                known_puuids_data[account_dto.puuid] = account_dto.model_dump()
                new_puuids_found_in_match += 1
                print(f"  Found and added new PUUID: {account_dto.gameName}#{account_dto.tagLine} ({account_dto.puuid})\n")
            else:
                print(f"  Could not retrieve account info for PUUID: {participant_puuid}\n")
    
    if new_puuids_found_in_match > 0:
        print(f"--- Extracted {new_puuids_found_in_match} new PUUIDs from match {match_data.metadata.matchId} ---\n")
    else:
        print(f"--- No new PUUIDs extracted from match {match_data.metadata.matchId} ---\n")
    
    return known_puuids_data


def discover_puuids_subflow(initial_puuid: str, riot_api_repo: RiotApiRepository, file_repo: FileRepository, known_puuids_data: dict[str, dict], max_matches_per_puuid: int = 3) -> dict[str, dict]:
    """
    주어진 initial_puuid의 최신 매치 데이터를 가져와 저장하고, 매치 참가자들의 PUUID를 추출하여
    전달받은 known_puuids_data를 업데이트합니다.
    """
    print(f"--- Starting PUUID Discovery Subflow (Initial PUUID: {initial_puuid}) ---\n")
    
    # Ensure initial PUUID's account data is saved if not already known
    if initial_puuid not in known_puuids_data:
        account_dto = riot_api_repo.get_account_by_puuid(initial_puuid)
        if account_dto:
            known_puuids_data[account_dto.puuid] = account_dto.model_dump()
            print(f"  Added initial PUUID to known accounts: {account_dto.gameName}#{account_dto.tagLine}\n")
        else:
            print(f"  Failed to get account data for initial PUUID: {initial_puuid}. Cannot proceed.\n")
            return known_puuids_data # Return current state if initial PUUID fails

    print(f"  Fetching {max_matches_per_puuid} matches for initial PUUID: {initial_puuid}\n")
    match_ids = get_match_ids_by_puuid(riot_api_repo, initial_puuid, count=max_matches_per_puuid)
    
    if match_ids:
        for match_id in match_ids:
            match_data = get_match_data(riot_api_repo, match_id)
            if match_data: # Check if match_data was successfully retrieved
                save_match_data_as_json(file_repo, match_id, match_data) # Save every fetched match
                # Extract and save new PUUIDs from this match
                known_puuids_data = extract_and_save_new_puuids(riot_api_repo, file_repo, match_data, known_puuids_data)
    else:
        print(f"  No match IDs found for initial PUUID: {initial_puuid}.\n")

    # The final save will be handled by the calling flow/task
    print(f"--- Finished PUUID Discovery Subflow for {initial_puuid}. Total unique PUUIDs in current run: {len(known_puuids_data)} ---\n")
    return known_puuids_data # Return the updated known_puuids_data


def discover_from_top_known_puuids(riot_api_repo: RiotApiRepository, file_repo: FileRepository, known_puuids_data: dict[str, dict], limit: int = 10, max_puuids_to_discover_per_seed: int = 5, max_matches_per_puuid: int = 3) -> dict[str, dict]:
    """
    data/known_puuids.json 파일의 상위 N개 항목에 대해 새로운 PUUID를 탐색하고
    전달받은 known_puuids_data를 업데이트합니다.
    """
    print(f"--- Starting discovery from top {limit} known PUUIDs ---\n")
    
    if not known_puuids_data:
        print("  No known PUUIDs found to start discovery from. Initializing empty.\n")
        # If no known PUUIDs, we can't discover from top, so return current empty state
        return known_puuids_data

    # Get top N PUUIDs (e.g., by arbitrary order, or could be based on last updated, etc.)
    top_puuids = list(known_puuids_data.keys())[:limit]

    for puuid_seed in top_puuids:
        print(f"--- Initiating discovery for seed PUUID: {puuid_seed} ---\n")
        known_puuids_data = discover_puuids_subflow( # Capture returned updated data
            initial_puuid=puuid_seed,
            riot_api_repo=riot_api_repo,
            file_repo=file_repo,
            known_puuids_data=known_puuids_data, # Pass current known_puuids_data
            max_matches_per_puuid=max_matches_per_puuid
        )
    
    print(f"--- Finished discovery from top {limit} known PUUIDs. Total unique PUUIDs: {len(known_puuids_data)} ---\n")
    return known_puuids_data # Return the final updated data


def discover_from_random_known_puuids(riot_api_repo: RiotApiRepository, file_repo: FileRepository,
                                        known_puuids_data: dict[str, dict], limit: int = 3,
                                        max_puuids_to_discover_per_seed: int = 5, max_matches_per_puuid: int = 3) -> \
dict[str, dict]:
    """
    data/known_puuids.json 파일의 랜덤 N개 항목에 대해 새로운 PUUID를 탐색하고
    전달받은 known_puuids_data를 업데이트합니다.
    """
    print(f"--- Starting discovery from {limit} random known PUUIDs ---\n")

    if not known_puuids_data:
        print("  No known PUUIDs found to start discovery from. Initializing empty.\n")
        return known_puuids_data

    # Get random N PUUIDs
    all_puuids = list(known_puuids_data.keys())
    if len(all_puuids) < limit:
        print(f"  Number of known PUUIDs ({len(all_puuids)}) is less than the limit ({limit}). Using all known PUUIDs.\n")
        random_puuids = all_puuids
    else:
        random_puuids = random.sample(all_puuids, limit)

    for puuid_seed in random_puuids:
        print(f"--- Initiating discovery for seed PUUID: {puuid_seed} ---\n")
        known_puuids_data = discover_puuids_subflow(  # Capture returned updated data
            initial_puuid=puuid_seed,
            riot_api_repo=riot_api_repo,
            file_repo=file_repo,
            known_puuids_data=known_puuids_data,  # Pass current known_puuids_data
            max_matches_per_puuid=max_matches_per_puuid
        )

    print(f"--- Finished discovery from {limit} random known PUUIDs. Total unique PUUIDs: {len(known_puuids_data)} ---\n")
    return known_puuids_data  # Return the final updated data


# --- Prefect Flow ---

def riot_data_pipeline(initial_game_name: str = "드래곤무조건챙김", initial_tag_line: str = "KR1",
                       max_puuids_to_discover_initial: int = 10, max_matches_per_puuid_initial: int = 3,
                       discover_from_top_count: int = 5, discover_from_random_count: int = 3): # New parameter for random discovery
    """
    Riot API에서 데이터를 가져오고 처리하는 Prefect 워크플로우.
    """
    print("--- Starting Riot Data Pipeline Flow ---\n")

    riot_api_repo = RiotApiRepository()
    file_repo = FileRepository()

    # Load known PUUIDs once at the beginning of the flow
    known_puuids_data = load_known_puuids(file_repo)

    # Get initial PUUID
    initial_puuid = get_sample_puuid(riot_api_repo)

    if initial_puuid:
        # Initial PUUID discovery
        known_puuids_data = discover_puuids_subflow( # Capture returned updated data
            initial_puuid=initial_puuid,
            riot_api_repo=riot_api_repo,
            file_repo=file_repo,
            known_puuids_data=known_puuids_data, # Pass current known_puuids_data
            max_matches_per_puuid=max_matches_per_puuid_initial
        )
        #
        # # Discover from top N known PUUIDs
        # known_puuids_data = discover_from_top_known_puuids_task( # Capture returned updated data
        #     riot_api_repo=riot_api_repo,
        #     file_repo=file_repo,
        #     known_puuids_data=known_puuids_data, # Pass current known_puuids_data
        #     limit=discover_from_top_count,
        #     max_puuids_to_discover_per_seed=max_puuids_to_discover_initial,
        #     max_matches_per_puuid=max_matches_per_puuid_initial
        # )

        # Discover from random N known PUUIDs
        known_puuids_data = discover_from_random_known_puuids(
            riot_api_repo=riot_api_repo,
            file_repo=file_repo,
            known_puuids_data=known_puuids_data,
            limit=discover_from_random_count,
            max_matches_per_puuid=max_matches_per_puuid_initial
        )
        
        # Final save of all known PUUIDs after all discovery processes
        save_known_puuids(file_repo, known_puuids_data)


    else:
        print("  Failed to retrieve initial PUUID, cannot proceed with pipeline.\n")
    print("--- Riot Data Pipeline Flow Finished ---\n")
