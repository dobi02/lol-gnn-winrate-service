import os
import random  # Mock 데이터를 위해 필요

# 실제로는 라이엇 API 키 등을 로드해야 함
RIOT_API_KEY = os.getenv("RIOT_API_KEY")


async def get_match_data_hybrid(summoner_name: str):
    """
    [하이브리드 데이터 수집 로직]
    1. Riot API로 현재 게임 정보 조회
    2. 참여자들의 최근 전적 조회 (DB 검색 -> 없으면 API 호출)
    3. 모델 입력 데이터로 변환
    """
    print(f"DEBUG: '{summoner_name}'의 데이터를 수집합니다...")

    # 1. 소환사 정보 조회 (Mock)
    summoner_info = get_summoner_by_name(summoner_name)
    puuid = summoner_info.get('puuid', 'mock_puuid')

    # (실제 로직에서는 여기서 현재 게임 정보를 가져와야 함)
    participant_puuids = ["puuid_1", "puuid_2"]  # 가상의 참여자 목록

    # 2. 각 참여자의 최근 전적 수집
    collected_matches = []

    for p_id in participant_puuids:
        # [에러 났던 부분] 이제 함수가 있으므로 에러 안 남!
        recent_match_ids = get_match_ids(p_id)

        for match_id in recent_match_ids:
            # DB 조회 시도
            match_data = db_query_match(match_id)

            if match_data:
                # DB에 있으면 사용
                collected_matches.append(match_data)
            else:
                # DB에 없으면 API 호출 후 저장
                # print(f"API Call needed for {match_id}")
                match_data = get_match_detail_api(match_id)
                save_match_to_db(match_data)
                collected_matches.append(match_data)

    # 3. 모델 입력 변환 (지금은 더미 데이터)
    # 실제로는 PyG Data 객체나 Tensor로 변환해야 함
    return [0.1, 0.2, 0.3]  # 가상의 텐서 데이터


# --- [Helper Functions (Mock)] ---
# 아래 함수들이 없어서 에러가 났었습니다.

def get_summoner_by_name(name):
    # (나중에) requests.get(RIOT_API_URL + ...)
    return {"puuid": "mock_puuid_123", "name": name}


def get_match_ids(puuid):
    """특정 유저의 최근 매치 ID 리스트 반환"""
    # (나중에) Riot API 호출
    return ["KR_1234567890", "KR_0987654321"]


def db_query_match(match_id):
    """DB에 매치 데이터가 있는지 확인"""
    # (나중에) SELECT * FROM matches WHERE match_id = ...
    # 테스트를 위해 랜덤하게 있다/없다 반환
    if random.random() > 0.5:
        return {"match_id": match_id, "data": "cached_data"}
    return None


def get_match_detail_api(match_id):
    """매치 상세 정보 API 호출"""
    return {"match_id": match_id, "gameDuration": 1500}


def save_match_to_db(data):
    """DB에 데이터 저장"""
    pass