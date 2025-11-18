import Storage
from typing import Any, Dict, List, Optional
import setting
import util
import time

class MatchCollector:
    def __init__(self, api: util.RiotAPI, storage: Storage):
        self.api = api
        self.storage = storage

    def _save_if_solo(self, match: Dict[str, Any]) -> Optional[str]:
        """솔랭(420)만 저장. 저장 시 matchId 반환, 아니면 None."""
        if not match:
            return None
        info = match.get("info") or {}
        mid  = (match.get("metadata") or {}).get("matchId")
        if not mid:
            return None
        if info.get("queueId") != setting.QUEUE_SOLO:
            return None
        # 수집 메타 부가
        match["_collector"] = {"matchId": mid, "collectedAt": int(time.time())}
        self.storage.save_match(mid, match)
        return mid

    def collect_from_riot_id(self, riot_id: str, count: int = 10) -> List[str]:
        """라이엇ID 한 명의 최근 솔랭 count경기 저장."""
        m = util.RIOT_ID_RE.match(riot_id)
        if not m:
            print(f"  [SKIP] Riot ID 형식 아님: {riot_id}")
            return []
        game, tag = m.group(1), m.group(2)
        acct = self.api.account_by_riot_id(game, tag)
        if not acct:
            print(f"  [MISS] Riot ID 조회 실패: {riot_id}")
            return []
        puuid = acct.get("puuid")
        if not puuid:
            print(f"  [MISS] Riot ID puuid 없음: {riot_id}")
            return []
        ids = self.api.match_ids_by_puuid(puuid, count=count, queue=setting.QUEUE_SOLO)
        saved: List[str] = []
        for mid in ids:
            if self.storage.has(mid):
                continue
            mjson = self.api.match_by_id(mid)
            if not mjson:
                continue
            if self._save_if_solo(mjson):
                saved.append(mid)
        return saved

    def collect_from_puuid(self, puuid: str, count: int = 10) -> List[str]:
        ids = self.api.match_ids_by_puuid(puuid, count=count, queue=setting.QUEUE_SOLO)
        saved: List[str] = []
        for mid in ids:
            if self.storage.has(mid):
                continue
            mjson = self.api.match_by_id(mid)
            if not mjson:
                continue
            if self._save_if_solo(mjson):
                saved.append(mid)
        return saved

    def collect_from_match_id(self, match_id: str, per_player: int = 10) -> List[str]:
        """
        매치ID가 '솔랭'이면:
          1) 그 매치 저장
          2) 참가자들의 PUUID로 각자 최근 솔랭 per_player 경기 추가 저장
        솔랭이 아니면 건너뜀.
        """
        mjson = self.api.match_by_id(match_id)
        if not mjson:
            print(f"  [MISS] 매치 조회 실패: {match_id}")
            return []
        saved: List[str] = []
        if self._save_if_solo(mjson):
            saved.append(match_id)
        else:
            print(f"  [SKIP] 솔랭 아님: {match_id}")
            return saved

        info = mjson.get("info") or {}
        participants = info.get("participants") or []
        for p in participants:
            puuid = p.get("puuid")
            if not puuid:
                continue
            ids = self.api.match_ids_by_puuid(puuid, count=per_player, queue=setting.QUEUE_SOLO)
            for mid in ids:
                if self.storage.has(mid):
                    continue
                m2 = self.api.match_by_id(mid)
                if not m2:
                    continue
                if self._save_if_solo(m2):
                    saved.append(mid)
        return saved