# collecotr/http_client.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import time
import requests

from . import settings
from .settings import get_riot_api_keys


class HttpClient:
    """
    Riot API 호출용 HTTP 클라이언트.
    - 여러 개의 API 키를 순환 사용
    - 429 (rate limit) → 다음 키로 교체 후 Retry-After 만큼 대기
    """

    def __init__(self):
        self._api_keys = None
        self._idx: int = 0  # 현재 사용 중인 키 인덱스

    def _get_api_keys(self):
        """런타임에 API 키 가져오기 (캐싱 포함)"""
        if self._api_keys is None:
            self._api_keys = get_riot_api_keys()
        return self._api_keys

    def get_headers(self):
        api_keys = self._get_api_keys()
        return {
            "X-Riot-Token": api_keys[self._idx]
        }

    @property
    def headers(self) -> Dict[str, str]:
        return self.get_headers()

    def _rotate_key(self) -> None:
        """다음 API 키로 교체."""
        if len(self._api_keys) > 1:
            self._idx = (self._idx + 1) % len(self._api_keys)

    def get_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 6,
    ) -> Any:
        """JSON 응답을 반환. 필요 시 재시도 & 키교체."""
        for i in range(max_retries):
            resp = requests.get(
                url,
                headers=self.headers,
                params=params,
                timeout=30,
            )

            # 정상
            if resp.status_code == 200:
                return resp.json()

            # 없는 리소스
            if resp.status_code == 404:
                return None

            # 레이트 리밋 → 키 교체 후 대기
            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", "2"))
                self._rotate_key()
                time.sleep(retry_after)
                continue

            # 서버 에러 → 지수 백오프
            if 500 <= resp.status_code < 600:
                time.sleep(1.5 ** (i + 1))
                continue

            # 그 외 (401/403 등) → 즉시 에러
            raise RuntimeError(
                f"HTTP {resp.status_code} {url}: {resp.text[:200]}"
            )

        raise RuntimeError(f"Max retries exceeded for URL: {url}")

class RiotAPI:
    """
    Riot Match V5 API 래퍼
    """

    def __init__(self, http: HttpClient):
        self.http = http

    def match_by_id(self, match_id: str) -> Optional[Dict[str, Any]]:
        headers = self.http.get_headers()
        url = f"{settings.BASE_REGIONAL}/lol/match/v5/matches/{match_id}"
        return self.http.get_json(url)

    def match_ids_by_puuid(
        self,
        puuid: str,
        count: int = 100,
        queue: Optional[int] = None,
    ) -> List[str]:
        url = f"{settings.BASE_REGIONAL}/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params: Dict[str, Any] = {"start": 0, "count": count}
        if queue is not None:
            params["queue"] = queue
        data = self.http.get_json(url, params=params) or []
        return list(data)
    
    def champion_masteries_by_puuid(self, puuid: str):
        url = f"{settings.BASE_PLATFORM}/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}"
        return self.http.get_json(url) or []
        
    def champion_mastery_by_puuid_and_champion(self, puuid: str, champion_id: int):
        url = f"{settings.BASE_PLATFORM}/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}/by-champion/{champion_id}"
        return self.http.get_json(url)  # dict 반환(없으면 404일 수 있음)
