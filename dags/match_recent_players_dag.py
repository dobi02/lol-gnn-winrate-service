import sys
from datetime import datetime

from airflow.sdk import dag
from airflow.operators.python import PythonOperator

# Airflow 환경에서 src 모듈을 import하기 위해 경로 설정
sys.path.append("/opt/airflow/dags/git/repo")

# Import task functions
from src.tasks.recent_match_tasks import (
    fetch_latest_non_root_match,
    fetch_recent_matches_by_players,
    save_all_matches_to_postgres
)


@dag(
    dag_id="riot_match_recent_players_once",
    start_date=datetime(2024, 1, 1),
    schedule="* * */1 * *", #1일마다
    catchup=False,
    params={
        "per_player": 9,  # 각 플레이어별 조회할 최근 경기 수
    },
    tags=["lol", "match", "recent"],
)
def riot_match_recent_players():
    """
    DAG to collect recent matches from players:
    1. Fetch the latest non-root match from database
    2. Fetch recent 9 matches for each participant of that match
    3. Save all matches to PostgreSQL database with is_root=False
    """

    # Task 1: Fetch the latest non-root match
    fetch_match_task = PythonOperator(
        task_id="fetch_latest_non_root_match",
        python_callable=fetch_latest_non_root_match
    )

    # Task 2: Fetch recent matches for all participants
    fetch_matches_task = PythonOperator(
        task_id="fetch_recent_matches_by_players",
        python_callable=fetch_recent_matches_by_players
    )

    # Task 3: Save all matches to PostgreSQL
    save_to_postgres_task = PythonOperator(
        task_id="save_all_matches_to_postgres",
        python_callable=save_all_matches_to_postgres
    )

    # Define task dependencies
    fetch_match_task >> fetch_matches_task >> save_to_postgres_task


dag = riot_match_recent_players()