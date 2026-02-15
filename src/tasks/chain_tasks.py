"""Task functions for match chain DAG"""

from typing import Optional
from airflow.models import Variable
import json
import sys
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)  # src directory
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from repository.match_repository import MatchRepository


def fetch_next_child_match(**kwargs) -> Optional[str]:
    MAX_BLOCKLIST_SIZE = 50
    PG_CONN_ID = "data_postgres_connection"
    repo = MatchRepository(conn_id=PG_CONN_ID)

    # 1) Load blocklist from Airflow Variable
    blocklist = Variable.get("failed_match_blocklist", default_var=[], deserialize_json=True)

    # Guard invalid variable type
    if not isinstance(blocklist, list):
        blocklist = []

    # If blocklist is full, reset it entirely
    if len(blocklist) >= MAX_BLOCKLIST_SIZE:
        print(f"Blocklist reached {MAX_BLOCKLIST_SIZE}. Resetting blocklist.")
        blocklist = []

    # 2) Load recent non-root candidates from DB
    candidates = repo.get_recent_non_root_matches(limit=20)

    if not candidates:
        print("No child matches to process in DB.")
        return None

    target_match_id = None

    # 3) Pick first candidate not in blocklist
    for match in candidates:
        if match not in blocklist:
            target_match_id = match
            break
        print(f"Skipping {match}: already in blocklist")

    if not target_match_id:
        print("All fetched candidates are in blocklist. Skipping this run.")
        return None

    print(f"Selected match: {target_match_id}")

    # 4) Add selected match to blocklist for de-dup between runs
    blocklist.append(target_match_id)

    # 5) Persist updated blocklist
    Variable.set("failed_match_blocklist", json.dumps(blocklist))

    return target_match_id


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
        print("No match to process; trigger is skipped.")
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

    print(f"Triggered collection DAG with match: {match_id}")
    return True
