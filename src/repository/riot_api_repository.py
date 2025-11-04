import requests
from src.core.config import RIOT_API_KEY
from src.model.riot_dto import MatchDto, AccountDto


class RiotApiRepository:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'): # Ensure __init__ runs only once
            if not RIOT_API_KEY:
                raise ValueError("RIOT_API_KEY not found in .env file")
            self.api_key = RIOT_API_KEY
            self.base_url = "https://asia.api.riotgames.com"
            self.initialized = True

    def get_puuid_by_riot_id(self, game_name: str, tag_line: str) -> str | None: # Corrected return type
        """
        Riot ID로 계정 정보를 조회합니다.
        """
        url = f"{self.base_url}/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
        headers = {
            "X-Riot-Token": self.api_key
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # 2xx 상태 코드가 아닐 경우 예외 발생
            print("Successfully received account data from Riot API.")
            return AccountDto.model_validate(response.json()).puuid
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while requesting Riot API: {e}")
            return None
        except Exception as e:
            print(f"An error occurred while parsing account data: {e}")
            return None

    def get_match_by_id(self, match_id: str) -> MatchDto | None:
        """
        matchId를 사용하여 매치 상세 정보를 가져옵니다.
        """
        url = f"{self.base_url}/lol/match/v5/matches/{match_id}"
        headers = {
            "X-Riot-Token": self.api_key
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            print(f"Successfully received match data for {match_id}.")
            # Pydantic 모델로 데이터 유효성 검사 및 변환
            return MatchDto.model_validate(response.json())
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while requesting match data: {e}")
            return None
        except Exception as e:
            print(f"An error occurred while parsing match data: {e}")
            return None

    def get_match_ids_by_puuid(self, puuid: str, count: int = 20) -> list[str] | None:
        """
        PUUID를 사용하여 매치 ID 목록을 가져옵니다.
        기본적으로 최신 20개의 매치 ID를 반환합니다.
        """
        url = f"{self.base_url}/lol/match/v5/matches/by-puuid/{puuid}/ids?count={count}"
        headers = {
            "X-Riot-Token": self.api_key
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            print(f"Successfully received match IDs for PUUID: {puuid}.")
            return response.json()  # This should be a list of strings
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while requesting match IDs: {e}")
            return None
        except Exception as e:
            print(f"An error occurred while parsing match IDs: {e}")
            return None

    def get_account_by_puuid(self, puuid: str) -> AccountDto | None:
        """
        PUUID를 사용하여 계정 정보를 조회합니다.
        """
        url = f"{self.base_url}/riot/account/v1/accounts/by-puuid/{puuid}"
        headers = {
            "X-Riot-Token": self.api_key
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            print(f"Successfully received account data for PUUID: {puuid}.")
            return AccountDto.model_validate(response.json())
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while requesting account data by PUUID: {e}")
            return None
        except Exception as e:
            print(f"An error occurred while parsing account data by PUUID: {e}")
            return None
