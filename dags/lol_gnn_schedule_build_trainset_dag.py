import json
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import Variable, dag


def _resolve_calendar_path(host_training_dir: str, calendar_file: str) -> Path:
    primary = Path(host_training_dir) / calendar_file
    if primary.exists():
        return primary
    fallback = Path(__file__).resolve().parents[1] / "src" / "training" / calendar_file
    if fallback.exists():
        return fallback
    raise FileNotFoundError(f"Calendar file not found: {primary} (fallback: {fallback})")


def _parse_calendar_datetime(raw: str) -> datetime:
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    raise ValueError(f"Unsupported datetime format: {raw}")


def _is_offset_run_day(calendar_obj: dict, now_kst: datetime) -> bool:
    target_date = now_kst.date()
    windows = calendar_obj.get("windows", [])
    if not isinstance(windows, list) or not windows:
        return False

    for window in windows:
        start_raw = window.get("start_date")
        if not start_raw:
            continue
        start_date = _parse_calendar_datetime(str(start_raw)).date()
        if target_date == start_date + timedelta(days=1):
            return True
        if target_date == start_date + timedelta(days=3):
            return True
    return False


@dag(
    dag_id="lol_gnn_schedule_build_trainset",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule="0 5 * * *",  # daily 05:00 KST
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1},
    tags=["mlops", "gnn", "dataset", "scheduler"],
)
def lol_gnn_schedule_build_trainset():
    def _should_trigger_today(**_context) -> bool:
        now_kst = datetime.now(tz=pendulum.timezone("Asia/Seoul")).replace(tzinfo=None)
        calendar_dir = Variable.get(
            "lol_gnn_dataset_calendar_dir",
            default="/opt/airflow/dags/git/repo/src/training",
        )
        calendar_file = Variable.get("lol_gnn_dataset_calendar_file", default="dataset_calendar.json")
        calendar_path = _resolve_calendar_path(calendar_dir, calendar_file)
        with calendar_path.open("r", encoding="utf-8") as fp:
            calendar_obj = json.load(fp)

        should_trigger = _is_offset_run_day(calendar_obj, now_kst)
        print(f"Schedule trigger check: now_kst={now_kst}, should_trigger={should_trigger}")
        return bool(should_trigger)

    gate = ShortCircuitOperator(
        task_id="offset_gate",
        python_callable=_should_trigger_today,
    )

    trigger_build = TriggerDagRunOperator(
        task_id="trigger_build_trainset",
        trigger_dag_id=Variable.get("lol_gnn_schedule_target_dag_id", default="lol_gnn_build_trainset"),
        conf={},
        wait_for_completion=False,
        reset_dag_run=True,
    )

    gate >> trigger_build


dag = lol_gnn_schedule_build_trainset()
