# collector/settings.py
from __future__ import annotations
import os

REGION = os.environ.get("RIOT_REGION", "asia")
BASE_REGIONAL = f"https://{REGION}.api.riotgames.com"

# Solo Rank Queue ID
QUEUE_SOLO = 420

# 여러 개의 API 키를 환경변수에서 읽기
# 예: RIOT_API_KEYS="RGAPI-aaa,RGAPI-bbb,RGAPI-ccc"
_raw_keys = os.environ.get("RIOT_API_KEYS") or os.environ.get("RIOT_API_KEY")
if not _raw_keys:
    raise SystemExit(
        "환경변수 RIOT_API_KEYS 또는 RIOT_API_KEY 가 없습니다.\n"
        "예: export RIOT_API_KEYS='RGAPI-aaa,RGAPI-bbb'"
    )

API_KEYS = [k.strip() for k in _raw_keys.split(",") if k.strip()]
if not API_KEYS:
    raise SystemExit("유효한 RIOT API 키가 없습니다.")