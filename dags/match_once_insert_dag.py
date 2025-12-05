import sys
from datetime import datetime

from airflow.sdk import dag
from airflow.operators.python import PythonOperator

# Airflow 환경에서 src 모듈을 import하기 위해 경로 설정
sys.path.append("/opt/airflow/dags/git/repo")

# Import task functions
from src.tasks.match_tasks import (
    fetch_root_match,
    fetch_recent_matches,
    save_all_to_postgres
)


@dag(
    dag_id="riot_match_to_postgres_once",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "root_match_id": "KR_7917227565",
        "per_player": 8,  # 최근 경기 수
    },
)
def riot_match_to_postgres():
    """
    DAG to fetch match data from Riot API and save to PostgreSQL.
    Uses PythonOperator with separate task function files for better testability.
    """

    # Task 1: Fetch root match
    fetch_root_task = PythonOperator(
        task_id="fetch_root_match",
        python_callable=fetch_root_match
    )

    # Task 2: Fetch recent matches for all participants
    fetch_matches_task = PythonOperator(
        task_id="fetch_recent_matches",
        python_callable=fetch_recent_matches
    )

    # Task 3: Save all matches to PostgreSQL
    save_to_postgres_task = PythonOperator(
        task_id="save_all_to_postgres",
        python_callable=save_all_to_postgres
    )

    # Define task dependencies
    fetch_root_task >> fetch_matches_task >> save_to_postgres_task


dag = riot_match_to_postgres()