from __future__ import annotations

from typing import Any, Dict, List, Optional, Union
from pydantic import BaseModel

try:
    # Pydantic v2
    from pydantic import ConfigDict
    _V2 = True
except Exception:
    ConfigDict = None
    _V2 = False


class _Base(BaseModel):
    """
    Base model that tolerates extra fields coming from Riot API.
    This prevents breaking changes when Riot adds fields.
    """
    if _V2:
        model_config = ConfigDict(extra="allow")
    else:
        class Config:
            extra = "allow"


class SpectatorPerks(_Base):
    perkIds: List[int]
    perkStyle: int
    perkSubStyle: int


class SpectatorParticipant(_Base):
    # puuid can be null in spectator payloads
    puuid: Optional[str] = None
    teamId: int
    spell1Id: int
    spell2Id: int
    championId: int
    perks: Optional[SpectatorPerks] = None

    # Optional common fields (kept for completeness)
    summonerId: Optional[str] = None
    riotId: Optional[str] = None
    profileIconId: Optional[int] = None
    bot: Optional[bool] = None
    skinIndex: Optional[int] = None


class SpectatorBannedChampion(_Base):
    championId: int
    teamId: int
    pickTurn: int


class SpectatorGame(_Base):
    gameId: int
    platformId: str
    gameQueueConfigId: int
    participants: List[SpectatorParticipant]

    # Optional meta fields (may exist depending on endpoint/version)
    gameStartTime: Optional[int] = None
    gameLength: Optional[int] = None
    mapId: Optional[int] = None
    gameMode: Optional[str] = None
    gameType: Optional[str] = None
    bannedChampions: Optional[List[SpectatorBannedChampion]] = None


class EnrichmentPayload(_Base):
    """
    Optional enrichment payload for models that require history/mastery features.
    Keys are typically puuid -> data.
    """
    use_history: bool = False
    history_matches_by_puuid: Dict[str, Any] = {}
    champ_mastery_by_puuid: Dict[str, Any] = {}


class RequestContext(_Base):
    request_id: Optional[str] = None
    source: Optional[str] = "spectator"


class SpectatorPredictRequest(_Base):
    spectator: SpectatorGame
    enrichment: Optional[EnrichmentPayload] = None
    context: Optional[RequestContext] = None


class ModelMeta(_Base):
    experiment: str = "LoL_Win_Prediction_v1"
    run_name: str = "unknown"
    data_version: str = "unknown"
    artifact_dir: str = "artifacts"


class SpectatorPredictResponse(_Base):
    win_rate_team_100: float
    win_rate_team_200: float
    model: ModelMeta
    warnings: List[str] = []
