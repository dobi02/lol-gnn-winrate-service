# collector/settings.py
from __future__ import annotations
import os

# Airflow Variable import - DAG 외부에서는 동작하지 않으므로 예외 처리
try:
    from airflow.models.variable import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

REGION = os.environ.get("RIOT_REGION", "asia")
BASE_REGIONAL = f"https://{REGION}.api.riotgames.com"

# Solo Rank Queue ID
QUEUE_SOLO = 420

def get_riot_api_keys():
    """
    Riot API 키를 여러 소스에서 가져오기
    우선순위: Airflow Variable > 환경변수 RIOT_API_KEYS > 환경변수 RIOT_API_KEY
    """
    # 1. Airflow Variable에서 가져오기 (DAG 내부에서만 동작)
    if AIRFLOW_AVAILABLE:
        try:
            # Airflow UI에서 Variables에 'riot_api_keys'로 등록된 값 가져오기
            # 예: "RGAPI-key1,RGAPI-key2,RGAPI-key3"
            api_keys_var = Variable.get("riot_api_keys", default_var=None)
            if api_keys_var:
                return [k.strip() for k in api_keys_var.split(",") if k.strip()]
        except Exception:
            # Airflow Variable 접근 실패 시 다음 방법으로 fallback
            pass

    # 2. 환경변수에서 가져오기
    _raw_keys = os.environ.get("RIOT_API_KEYS") or os.environ.get("RIOT_API_KEY")
    if _raw_keys:
        return [k.strip() for k in _raw_keys.split(",") if k.strip()]

    # 3. 키가 없는 경우
    raise SystemExit(
        "Riot API 키가 설정되지 않았습니다.\n"
        "Airflow UI에서 Variables에 'riot_api_keys'를 설정하거나\n"
        "환경변수 RIOT_API_KEYS 또는 RIOT_API_KEY를 설정해주세요.\n"
        "예: export RIOT_API_KEYS='RGAPI-aaa,RGAPI-bbb'"
    )

# API 키 가져오기
#API_KEYS = _get_riot_api_keys()

#if not API_KEYS:
#    raise SystemExit("유효한 RIOT API 키가 없습니다.")