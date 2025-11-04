from pydantic import BaseModel
from typing import List


# NOTE: 전체 Match DTO는 매우 크므로, 주요 필드 위주로 일부만 정의합니다.

class ParticipantDto(BaseModel):
    puuid: str
    summonerName: str
    championName: str
    kills: int
    deaths: int
    assists: int
    win: bool

class InfoDto(BaseModel):
    gameCreation: int
    gameDuration: int
    gameMode: str
    participants: List[ParticipantDto]

class MetadataDto(BaseModel):
    matchId: str
    participants: List[str]  # List of participant PUUIDs

class MatchDto(BaseModel):
    metadata: MetadataDto
    info: InfoDto

class AccountDto(BaseModel):
    puuid: str
    gameName: str
    tagLine: str
