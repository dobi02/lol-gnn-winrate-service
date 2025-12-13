import sys
from datetime import datetime

from airflow.sdk import dag
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.empty import EmptyOperator

# Airflow 환경에서 src 모듈을 import하기 위해 경로 설정
sys.path.append("/opt/airflow/dags/git/repo")

# Import task functions
from src.tasks.chain_tasks import (
    fetch_next_child_match,
    trigger_match_collection_with_new_root
)


class CheckForMatchesOperator(BaseBranchOperator):
    """
    Branch operator that checks if there are matches to process
    """
    def choose_branch(self, context):
        # Get match_id from previous task
        ti = context['ti']
        match_id = ti.xcom_pull(task_ids='fetch_next_child_match')

        # Return task_id to follow
        if match_id:
            return "trigger_match_collection"
        else:
            return "no_matches_to_process"


@dag(
    dag_id="riot_match_chain_collector",
    start_date=datetime(2024, 1, 1),
    schedule="*/2 * * * *",  # 2분마다 실행
    catchup=False,
    params={
        "per_player": 8,  # 자식 매치에서 수집할 경기 수
    },
)
def riot_match_chain_collector():
    """
    Simple match collection DAG that:
    1. Fetches a random non-root match from database
    2. Uses it as new root to trigger match collection
    3. Runs every 2 minutes
    """

    # Fetch next child match to use as root
    fetch_match_task = PythonOperator(
        task_id="fetch_next_child_match",
        python_callable=fetch_next_child_match
    )

    # Check if we have matches and branch accordingly
    check_branch = CheckForMatchesOperator(
        task_id="check_for_matches"
    )

    # Trigger match collection DAG with new root
    # trigger_collection_task = PythonOperator(
    #     task_id="trigger_match_collection",
    #     python_callable=trigger_match_collection_with_new_root
    # )

    trigger_collection_task = TriggerDagRunOperator(
        task_id="trigger_match_collection",
        trigger_dag_id="riot_match_to_postgres_once",
        conf={
            "root_match_id": "{{ ti.xcom_pull(task_ids='fetch_next_child_match') }}",
            "per_player": "{{ params.per_player }}"
        },
        wait_for_completion=False
    )

    # No matches to process - just log
    no_matches_task = EmptyOperator(
        task_id="no_matches_to_process"
    )

    # Define task dependencies
    fetch_match_task >> check_branch
    check_branch >> [trigger_collection_task, no_matches_task]


dag = riot_match_chain_collector()