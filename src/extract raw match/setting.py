from __future__ import annotations
import os

# ----------------------------
# 설정/상수
# ----------------------------
REGIONAL = "asia"  # KR/JP/OC/SEA 대부분은 asia
BASE_REGIONAL = f"https://{REGIONAL}.api.riotgames.com"
QUEUE_SOLO = 420

API_KEY = os.environ.get("RIOT_API_KEY")
##API_KEY = "RGAPI-784cd20f-3728-4333-a918-e1e88fed4f3d"
if not API_KEY:
    raise SystemExit("환경변수 RIOT_API_KEY 가 없습니다. RGAPI-... 키를 설정하세요.")

HEADERS = {"X-Riot-Token": API_KEY}