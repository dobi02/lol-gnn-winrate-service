# riot/match collector.py
from __future__ import annotations
from typing import Any, Dict, List, Optional
import time

from . import settings
from .http_client import RiotAPI
from .storage import Storage


class MatchCollector:
    """
    - 시작 매치 ID 하나를 기준으로
      1) 그 매치를 저장 (솔랭 420만)
      2) 참가자 10명의 최근 per_player 솔랭 매치 수집
    - 모든 매치는 JSON 파일 1개로 저장
    - 각 JSON에 `_collector` 메타데이터로 rootMatchId 기록
    """

    def __init__(self, api: RiotAPI, storage: Storage):
        self.api = api
        self.storage = storage

    # 내부 헬퍼: 솔랭일 때만 저장
    def _save_if_solo(
        self,
        match: Dict[str, Any],
        root_match_id: Optional[str] = None,
    ) -> Optional[str]:
        if not match:
            return None

        metadata = match.get("metadata") or {}
        info = match.get("info") or {}
        match_id = metadata.get("matchId")
        if not match_id:
            return None

        # 솔로랭크만 저장
        if info.get("queueId") != settings.QUEUE_SOLO:
            return None

        # 수집 메타정보 추가
        match["_collector"] = {
            "matchId": match_id,
            "rootMatchId": root_match_id or match_id,
            "collectedAt": int(time.time()),
        }

        self.storage.save_match(match_id, match)
        return match_id

    # Riot ID/PUUID용 함수는 필요하면 확장용으로 남겨둠
    def collect_from_puuid(self, puuid: str, count: int = 10) -> List[str]:
        saved: List[str] = []
        ids = self.api.match_ids_by_puuid(
            puuid,
            count=count,
            queue=settings.QUEUE_SOLO,
        )
        for mid in ids:
            if self.storage.has(mid):
                continue
            mjson = self.api.match_by_id(mid)
            if not mjson:
                continue
            if self._save_if_solo(mjson):
                saved.append(mid)
        return saved

    def collect_from_match_id(
        self,
        root_match_id: str,
        per_player: int = 10,
    ) -> List[str]:
        """
        root_match_id:
          - 이 매치가 솔랭이면 그 매치를 저장하고
          - 참가자 10명의 최근 per_player 솔랭 매치들을 추가로 저장
        반환값: 실제로 저장된 match_id 리스트
        """
        saved: List[str] = []

        root = self.api.match_by_id(root_match_id)
        if not root:
            print(f"[MISS] 매치 조회 실패: {root_match_id}")
            return saved

        # 1) 루트 매치 저장
        if self._save_if_solo(root, root_match_id=root_match_id):
            saved.append(root_match_id)
        else:
            print(f"[SKIP] 솔랭이 아니어서 루트 매치 스킵: {root_match_id}")
            return saved

        # 2) 참가자 10명의 최근 per_player 경기 수집
        info = root.get("info") or {}
        participants = info.get("participants") or []
        for p in participants:
            puuid = p.get("puuid")
            if not puuid:
                continue

            ids = self.api.match_ids_by_puuid(
                puuid,
                count=per_player,
                queue=settings.QUEUE_SOLO,
            )
            for mid in ids:
                if self.storage.has(mid):
                    continue
                mjson = self.api.match_by_id(mid)
                if not mjson:
                    continue
                if self._save_if_solo(mjson, root_match_id=root_match_id):
                    saved.append(mid)

        return saved