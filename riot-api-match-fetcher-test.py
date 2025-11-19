from airflow.sdk import dag, task
from airflow.sdk import Variable
import pendulum
import requests
import json

# --- 설정 ---
# 테스트할 매치 ID (예: KR_1234567890)
# 실제로는 API 호출 결과나 XCom으로 동적으로 받을 수 있습니다.
TARGET_MATCH_ID = "KR_7917227262"

"""
GEMINI에 의해 작성된 코드입니다.
"""
@dag(
    dag_id="riot_api_match_fetcher_v1",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,  # 수동 실행
    catchup=False,
    tags=["riot", "api", "example"],
    doc_md="Airflow Variable을 이용해 Riot API Key를 가져오고 매치 정보를 조회하는 예제"
)
def riot_match_pipeline():
    @task
    def fetch_match_detail(match_id: str):
        # 1. Airflow Variable에서 API Key 가져오기
        # deserialize_json=True를 쓰면 JSON 객체로 바로 변환 가능 (여기선 문자열이므로 불필요)
        try:
            api_key = Variable.get("RIOT_API_KEY")
        except KeyError:
            print("Error: 'RIOT_API_KEY' variable not found in Airflow.")
            raise

        # 2. Riot API 요청 설정 (Asia 리전 기준)
        # Match V5 API: https://developer.riotgames.com/apis#match-v5
        url = f"https://asia.api.riotgames.com/lol/match/v5/matches/{match_id}"
        headers = {
            "X-Riot-Token": api_key
        }

        print(f"Fetching match info for: {match_id}")

        # 3. API 호출
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            data = response.json()

            # 게임 모드, 게임 시간 등 간단한 정보 로그 출력
            info = data.get('info', {})
            game_mode = info.get('gameMode', 'Unknown')
            game_duration = info.get('gameDuration', 0)

            print(f"Success! Game Mode: {game_mode}, Duration: {game_duration}s")
            return data
        elif response.status_code == 403:
            raise Exception("API Key expired or invalid (403 Forbidden). Please check your RIOT_API_KEY.")
        elif response.status_code == 404:
            print(f"Match {match_id} not found.")
            return None
        else:
            raise Exception(f"API Request failed: {response.status_code} - {response.text}")

    @task
    def process_match_data(match_data: dict):
        if not match_data:
            print("No data to process.")
            return

        # 예시: 데이터의 일부를 파싱하거나 저장하는 로직
        metadata = match_data.get('metadata', {})
        participants = metadata.get('participants', [])
        print(f"Match Participants PUUIDs: {participants}")

    # --- DAG 흐름 정의 ---
    # 매치 정보를 가져와서 -> 처리하는 순서
    data = fetch_match_detail(match_id=TARGET_MATCH_ID)
    process_match_data(data)


# DAG 인스턴스 생성
riot_match_pipeline()