# dags/matchdag.py
import os
import sys
from pathlib import Path

# Airflow 환경에서 src 모듈을 import하기 위해 경로 설정
# Docker 컨테이너 내에서 /opt/src 경로에 프로젝트 소스가 있다고 가정
sys.path.append("/opt/src")

from datetime import datetime
from airflow.sdk import dag, task
from src.collector.http_client import HttpClient, RiotAPI
from src.collector.storage import Storage
from src.collector.match_collector import MatchCollector

## export RIOT_API_KEYS="RGAPI-"
## export MATCH_DATA_DIR="/opt/airflow/data/matches"  # 환경 변수로 설정 가능
DATA_DIR = Path(os.environ.get("MATCH_DATA_DIR", "/opt/airflow/data/matches"))

@dag(
    dag_id="riot_match_collector",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # ✅ 한번만 수동 실행 / 스케줄 없음
    # 예: 매일 새벽 3시면 schedule="0 3 * * *"
    catchup=False,
    params={
        "root_match_id": "KR_7917227565",
        "per_player": 10,
    },
)
def riot_match_collector():

    @task
    def collect(**context):
        params = context["params"]
        root_match = params["root_match_id"]
        per_player = params["per_player"]

        storage = Storage(DATA_DIR)
        http = HttpClient()
        api = RiotAPI(http)
        collector = MatchCollector(api, storage)

        saved_ids = collector.collect_from_match_id(root_match, per_player)
        return {"saved_ids": saved_ids}

    collect()


dag = riot_match_collector()