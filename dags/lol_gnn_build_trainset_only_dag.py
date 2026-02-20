import json
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pendulum
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.sdk import Variable, dag, task

from docker.types import Mount


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


def _resolve_calendar_entry(calendar_obj: dict, now_utc: datetime) -> dict:
    # Backward-compatible simple mode: one fixed start_date + dataset_version.
    if calendar_obj.get("start_date") and calendar_obj.get("dataset_version"):
        return {
            "start_date": str(calendar_obj["start_date"]),
            "dataset_version": str(calendar_obj["dataset_version"]),
        }

    windows = calendar_obj.get("windows", [])
    if not isinstance(windows, list) or not windows:
        raise ValueError("Calendar JSON must include non-empty `windows` list")

    matched = []
    for window in windows:
        if "start_date" not in window or "dataset_version" not in window:
            raise ValueError("Each window must include `start_date` and `dataset_version`")
        start_dt = _parse_calendar_datetime(str(window["start_date"]))
        end_raw = window.get("end_date")
        end_dt = _parse_calendar_datetime(str(end_raw)) if end_raw else None
        # Select the latest window that already started.
        if start_dt <= now_utc:
            matched.append((start_dt, window))

    if not matched:
        raise ValueError("No calendar window matched current UTC datetime")

    matched.sort(key=lambda x: x[0], reverse=True)
    selected = matched[0][1]
    end_raw = selected.get("end_date")
    selected_end = _parse_calendar_datetime(str(end_raw)) if end_raw else None
    effective_end = min(now_utc, selected_end) if selected_end else now_utc
    return {
        "start_date": str(selected["start_date"]),
        "dataset_version": str(selected["dataset_version"]),
        "end_date": effective_end.strftime("%Y-%m-%d %H:%M:%S"),
    }


@dag(
    dag_id="lol_gnn_build_trainset_only",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "gnn", "dataset"],
    default_args={"retries": 1},
    params={
        "dataset_version": Param(
            default=None,
            type=["null", "string"],
            title="Dataset Version (Optional Override)",
            description="If provided, override calendar-selected version",
            examples=["26.08"],
        ),
        "start_date": Param(
            default=None,
            type=["null", "string"],
            title="Start Date (Optional Override)",
            description="If provided, override calendar-selected start datetime (KST), format: YYYY-MM-DD HH:MM:SS",
            examples=["2026-04-16 10:00:00"],
        ),
        "end_date": Param(
            default=None,
            type=["null", "string"],
            title="End Date (Optional Override)",
            description="If provided, override computed end datetime (KST), format: YYYY-MM-DD HH:MM:SS",
            examples=["2026-04-30 05:59:59"],
        ),
    },
)
def lol_gnn_build_trainset_only():
    @task(task_id="runtime_config")
    def runtime_config():
        minio_conn_id = Variable.get("lol_gnn_minio_conn_id", default="minio_default")
        data_pg_conn_id = Variable.get("lol_gnn_data_postgres_conn_id", default="data_postgres_connection")

        minio_conn = BaseHook.get_connection(minio_conn_id)
        pg_conn = BaseHook.get_connection(data_pg_conn_id)

        endpoint = "http://lol-minio:9000"
        if minio_conn.extra:
            try:
                extra = json.loads(minio_conn.extra)
                endpoint = extra.get("endpoint_url", endpoint)
            except Exception:
                endpoint = minio_conn.extra_dejson.get("endpoint_url", endpoint)

        return {
            "image": Variable.get("lol_gnn_pipeline_image", default="lol-gnn-pipeline:latest"),
            "network_mode": Variable.get("lol_gnn_pipeline_network", default="lol-gnn-winrate-service_lol-network"),
            "project_dir": Variable.get("lol_gnn_pipeline_project_dir", default="/workspace/src/training"),
            "host_training_dir": Variable.get(
                "lol_gnn_pipeline_training_host_dir",
                default="/home/dobi/lol-gnn-winrate-service/airflow/dags/git/repo/src/training",
            ),
            "calendar_dir": Variable.get("lol_gnn_dataset_calendar_dir", default="/opt/airflow/dags/git/repo/src/training"),
            "calendar_file": Variable.get("lol_gnn_dataset_calendar_file", default="dataset_calendar.json"),
            "train_config": Variable.get("lol_gnn_pipeline_train_config", default="config.json"),
            "minio_bucket": Variable.get("lol_gnn_dataset_bucket", default="mlflow"),
            "minio_prefix": Variable.get("lol_gnn_dataset_prefix", default="datasets/lol_gnn"),
            "db_user": pg_conn.login or "",
            "db_pass": pg_conn.password or "",
            "db_host": pg_conn.host or "data-postgres",
            "db_port": str(pg_conn.port or 5432),
            "db_name": pg_conn.schema or "data_db",
            "minio_endpoint": endpoint,
            "minio_access_key": minio_conn.login or "",
            "minio_secret_key": minio_conn.password or "",
        }

    cfg = runtime_config()

    @task(task_id="resolve_dataset_window")
    def resolve_dataset_window(cfg_dict: dict):
        now_kst = datetime.now(ZoneInfo("Asia/Seoul")).replace(tzinfo=None)
        calendar_path = _resolve_calendar_path(cfg_dict["calendar_dir"], cfg_dict["calendar_file"])
        with calendar_path.open("r", encoding="utf-8") as fp:
            calendar_obj = json.load(fp)
        return _resolve_calendar_entry(calendar_obj, now_kst)

    resolved = resolve_dataset_window(cfg)

    build_and_upload_dataset = DockerOperator(
        task_id="build_and_upload_dataset",
        image="{{ ti.xcom_pull(task_ids='runtime_config')['image'] }}",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode=Variable.get(
            "lol_gnn_pipeline_network",
            default="lol-gnn-winrate-service_lol-network",
        ),
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source=Variable.get(
                    "lol_gnn_pipeline_training_host_dir",
                    default="/home/dobi/lol-gnn-winrate-service/airflow/dags/git/repo/src/training",
                ),
                target="/workspace/src/training",
                type="bind",
            ),
        ],
        environment={
            "DB_USER": "{{ ti.xcom_pull(task_ids='runtime_config')['db_user'] }}",
            "DB_PASS": "{{ ti.xcom_pull(task_ids='runtime_config')['db_pass'] }}",
            "DB_HOST": "{{ ti.xcom_pull(task_ids='runtime_config')['db_host'] }}",
            "DB_PORT": "{{ ti.xcom_pull(task_ids='runtime_config')['db_port'] }}",
            "DB_NAME": "{{ ti.xcom_pull(task_ids='runtime_config')['db_name'] }}",
            "MINIO_ENDPOINT_URL": "{{ ti.xcom_pull(task_ids='runtime_config')['minio_endpoint'] }}",
            "MLFLOW_S3_ENDPOINT_URL": "{{ ti.xcom_pull(task_ids='runtime_config')['minio_endpoint'] }}",
            "AWS_ACCESS_KEY_ID": "{{ ti.xcom_pull(task_ids='runtime_config')['minio_access_key'] }}",
            "AWS_SECRET_ACCESS_KEY": "{{ ti.xcom_pull(task_ids='runtime_config')['minio_secret_key'] }}",
        },
        command=(
            "bash -lc '"
            "set -euo pipefail && "
            "DATASET_VERSION=\"{{ dag_run.conf.get('dataset_version') or params.dataset_version or ti.xcom_pull(task_ids='resolve_dataset_window')['dataset_version'] }}\" && "
            "START_DATE=\"{{ dag_run.conf.get('start_date') or params.start_date or ti.xcom_pull(task_ids='resolve_dataset_window')['start_date'] }}\" && "
            "END_DATE=\"{{ dag_run.conf.get('end_date') or params.end_date or ti.xcom_pull(task_ids='resolve_dataset_window')['end_date'] }}\" && "
            "test -n \"$DATASET_VERSION\" && "
            "test -n \"$START_DATE\" && "
            "test -n \"$END_DATE\" && "
            "cd {{ ti.xcom_pull(task_ids='runtime_config')['project_dir'] }} && "
            "python build_dataset.py "
            "--config {{ ti.xcom_pull(task_ids='runtime_config')['train_config'] }} "
            "--start-date \"$START_DATE\" "
            "--end-date \"$END_DATE\" "
            "--dataset-version \"$DATASET_VERSION\" "
            "--clear-output && "
            "python minio_dataset.py upload "
            "--config {{ ti.xcom_pull(task_ids='runtime_config')['train_config'] }} "
            "--bucket {{ ti.xcom_pull(task_ids='runtime_config')['minio_bucket'] }} "
            "--prefix {{ ti.xcom_pull(task_ids='runtime_config')['minio_prefix'] }} "
            "--dataset-version \"$DATASET_VERSION\""
            "'"
        ),
    )

    cfg >> resolved >> build_and_upload_dataset


dag = lol_gnn_build_trainset_only()

