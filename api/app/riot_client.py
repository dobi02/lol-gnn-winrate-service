from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests


class RiotAPIError(RuntimeError):
    def __init__(self, status_code: int, message: str, url: str):
        super().__init__(f"Riot API error {status_code}: {message} ({url})")
        self.status_code = status_code
        self.url = url


@dataclass(frozen=True)
class RiotRouting:
    platform: str  # e.g. "KR"
    platform_host: str  # e.g. "https://kr.api.riotgames.com"
    region_host: str  # e.g. "https://asia.api.riotgames.com"
    account_host: str  # same as region_host for account-v1 (americas/asia/europe)


def _routing_for_platform(platform_id: str) -> RiotRouting:
    """
    Minimal routing map.

    - platform_host: LoL platform routing (kr, na1, euw1, ...)
    - region_host: match-v5 routing cluster (asia, americas, europe)
    - account_host: account-v1 routing cluster (same as region_host)
    """
    p = (platform_id or "").upper()

    if p in ("KR", "JP1"):
        region = "asia"
    elif p in ("NA1", "BR1", "LA1", "LA2", "OC1"):
        region = "americas"
    elif p in ("EUW1", "EUN1", "TR1", "RU"):
        region = "europe"
    else:
        region = "asia"
        if not p:
            p = "KR"

    platform_host = f"https://{p.lower()}.api.riotgames.com"
    region_host = f"https://{region}.api.riotgames.com"
    return RiotRouting(platform=p, platform_host=platform_host, region_host=region_host, account_host=region_host)


class RiotClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout_s: float = 15.0,
        max_retries: int = 3,
        backoff_s: float = 2,
    ):
        self.api_key = api_key or os.getenv("RIOT_API_KEY", "")
        if not self.api_key:
            raise RuntimeError("RIOT_API_KEY is not set")
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self.backoff_s = backoff_s

    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> Any:
        headers = {"X-Riot-Token": self.api_key}
        last_exc: Optional[Exception] = None

        for attempt in range(self.max_retries + 1):
            try:
                r = requests.get(url, headers=headers, params=params, timeout=self.timeout_s)
                if r.status_code == 200:
                    return r.json()
                msg = r.text[:500]
                raise RiotAPIError(r.status_code, msg, url)
            except RiotAPIError as e:
                last_exc = e
                # 404 is a valid business case for "not in game", caller can decide.
                if attempt < self.max_retries and e.status_code in (429, 500, 502, 503, 504):
                    time.sleep(self.backoff_s * (2 ** attempt))
                    continue
                raise
            except Exception as e:
                last_exc = e
                if attempt < self.max_retries:
                    time.sleep(self.backoff_s * (2 ** attempt))
                    continue
                raise

        raise last_exc  # pragma: no cover

    # Account (Riot ID -> PUUID)
    def get_account_by_riot_id(self, platform_id: str, game_name: str, tag_line: str) -> Dict[str, Any]:
        routing = _routing_for_platform(platform_id)
        url = f"{routing.account_host}/riot/account/v1/accounts/by-riot-id/{game_name}/{tag_line}"
        return self._get(url)

    # Summoner (PUUID -> encryptedSummonerId)
    def get_summoner_by_puuid(self, platform_id: str, puuid: str) -> Dict[str, Any]:
        routing = _routing_for_platform(platform_id)
        url = f"{routing.platform_host}/lol/summoner/v4/summoners/by-puuid/{puuid}"
        return self._get(url)

    # Spectator (active game) by summonerId
    def get_active_game_by_summoner_id(self, platform_id: str, encrypted_summoner_id: str) -> Dict[str, Any]:
        routing = _routing_for_platform(platform_id)
        url = f"{routing.platform_host}/lol/spectator/v5/active-games/by-summoner/{encrypted_summoner_id}"
        return self._get(url)

    # Match v5 (history)
    def get_match_ids_by_puuid(self, platform_id: str, puuid: str, count: int = 20) -> List[str]:
        routing = _routing_for_platform(platform_id)
        url = f"{routing.region_host}/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = {"count": max(1, min(int(count), 100))}
        return self._get(url, params=params)

    def get_match_by_id(self, platform_id: str, match_id: str) -> Dict[str, Any]:
        routing = _routing_for_platform(platform_id)
        url = f"{routing.region_host}/lol/match/v5/matches/{match_id}"
        return self._get(url)

    # Champion mastery (by summoner id)
    # def get_champion_mastery_by_summoner_id(self, platform_id: str, encrypted_summoner_id: str) -> List[Dict[str, Any]]:
    #     routing = _routing_for_platform(platform_id)
    #     url = f"{routing.platform_host}/lol/champion-mastery/v4/champion-masteries/by-summoner/{encrypted_summoner_id}"
    #     return self._get(url)

    def get_active_game_by_puuid(self, platform_id: str, puuid: str) -> Dict[str, Any]:
        routing = _routing_for_platform(platform_id)
        url = f"{routing.platform_host}/lol/spectator/v5/active-games/by-summoner/{puuid}"
        return self._get(url)

    # def get_champion_mastery_by_puuid(self, platform_id: str, puuid: str) -> List[Dict[str, Any]]:
    #     routing = _routing_for_platform(platform_id)
    #     url = f"{routing.platform_host}/lol/champion-mastery/v4/champion-masteries/by-puuid/{puuid}"
    #     return self._get(url)


def parse_riot_id(riot_id: str) -> Tuple[str, str]:
    """Parse 'gameName#tagLine' into (game_name, tag_line)."""
    if not riot_id or "#" not in riot_id:
        raise ValueError("riot_id must be in 'gameName#tagLine' format")
    game_name, tag_line = riot_id.split("#", 1)
    game_name = game_name.strip()
    tag_line = tag_line.strip()
    if not game_name or not tag_line:
        raise ValueError("riot_id must be in 'gameName#tagLine' format")
    return game_name, tag_line
