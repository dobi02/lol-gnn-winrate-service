from pydantic import BaseModel

class PredictionRequest(BaseModel):
    summoner_name: str

class PredictionResponse(BaseModel):
    summoner_name: str
    win_rate: float         # 0.0 ~ 1.0 (블루팀 승리 확률)
    team_color: str         # 'Blue' or 'Red'
    status: str             # 'Success' or 'Fail'