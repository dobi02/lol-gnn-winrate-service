"""Task functions for match chain DAG"""

from typing import Optional
from airflow.models import Variable
import json
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
src_dir = os.path.dirname(current_dir)  # src 디렉토리
if src_dir not in sys.path:
    sys.path.insert(0, src_dir)

from repository.match_repository import MatchRepository


def fetch_next_child_match(**kwargs) -> Optional[str]:
    PG_CONN_ID = "data_postgres_connection"
    repo = MatchRepository(conn_id=PG_CONN_ID)

    # 1. 블랙리스트(최근 시도했으나 성공 여부가 불분명한 매치들) 로드
    # deserialize_json=True를 쓰면 자동으로 파이썬 리스트로 변환됩니다.
    blocklist = Variable.get("failed_match_blocklist", default_var=[], deserialize_json=True)

    # 만약 blocklist가 리스트가 아니면(초기화 문제 등) 빈 리스트로 방어 코드
    if not isinstance(blocklist, list):
        blocklist = []

    # 2. DB에서 넉넉하게 후보군을 가져옵니다 (예: 20개)
    candidates = repo.get_recent_non_root_matches(limit=20)

    if not candidates:
        print("DB에 처리할 자식 매치가 없습니다.")
        return None

    target_match_id = None

    # 3. 후보군 중에서 블랙리스트에 없는 가장 최신 매치 찾기
    for match in candidates:
        if match not in blocklist:
            target_match_id = match
            break
        else:
            print(f"매치 {match}는 블랙리스트에 있어 스킵합니다.")

    if not target_match_id:
        print("가져온 후보 매치들이 모두 블랙리스트에 있습니다. (일시적 처리 불가)")
        return None

    print(f"선택된 매치: {target_match_id}")

    # 4. 선택된 매치를 블랙리스트에 추가 (다음 실행 때 중복 방지)
    blocklist.append(target_match_id)

    # 5. 리스트가 무한정 커지지 않도록 최근 50개만 유지 (Rolling)
    if len(blocklist) > 50:
        blocklist = blocklist[-50:]  # 뒤에서부터 50개만 남김

    # 6. 업데이트된 블랙리스트 저장
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