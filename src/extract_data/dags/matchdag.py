# dags/matchdag.py
import sys
from pathlib import Path

sys.path.append("/opt/airflow/extract_data")

from datetime import datetime
from airflow.decorators import dag, task
from collector.http_client import HttpClient, RiotAPI
from collector.storage import Storage
from collector.match_collector import MatchCollector

## export RIOT_API_KEYS="RGAPI-"
DATA_DIR = Path("/opt/airflow/data/matches")

@dag(
    dag_id="riot_match_collector",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    params={
        "root_match_id": "KR_7917227262",
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