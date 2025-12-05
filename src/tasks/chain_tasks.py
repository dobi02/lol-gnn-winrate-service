"""Task functions for match chain DAG"""

from typing import Optional

import sys
sys.path.append("/opt/airflow/dags/git/repo")

from src.repository.match_repository import MatchRepository



def fetch_next_child_match(**kwargs) -> Optional[str]:
    """
    Fetch a non-root match from database to use as new root for chain collection

    Args:
        **kwargs: Airflow context variables

    Returns:
        Optional[str]: Match ID to use as new root, or None if no matches available
    """
    # Initialize repository
    PG_CONN_ID = "data_postgres_connection"
    repo = MatchRepository(conn_id=PG_CONN_ID)

    # Get a random non-root match
    match_id = repo.get_random_non_root_match()

    if not match_id:
        print("처리할 자식 매치가 없습니다.")
        return None

    print(f"다음 자식 매치를 루트로 사용: {match_id}")
    return match_id


def trigger_match_collection_with_new_root(**kwargs) -> bool:
    """
    Trigger match_once_insert_dag with new root match ID

    Args:
        **kwargs: Airflow context variables

    Returns:
        bool: True if DAG was triggered successfully, False otherwise
    """
    # Get match_id from previous task (XCom)
    ti = kwargs['ti']
    match_id = ti.xcom_pull(task_ids='fetch_next_child_match')

    if not match_id:
        print("처리할 매치가 없어 DAG를 트리거하지 않습니다.")
        return False

    # Get configured per_player value
    params = kwargs['params']
    per_player = params.get('per_player', 8)

    # Use TriggerDagRunOperator to trigger match_once_insert_dag
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    trigger = TriggerDagRunOperator(
        task_id=f"trigger_match_collection_{match_id}",
        trigger_dag_id="riot_match_to_postgres_once",
        conf={
            "root_match_id": match_id,
            "per_player": per_player
        },
        wait_for_completion=False,
        deferrable=False
    )

    # Execute the trigger
    trigger.execute(context=kwargs)

    print(f"매치 {match_id}에 대한 수집 DAG를 트리거했습니다.")
    return True