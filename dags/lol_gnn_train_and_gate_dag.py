import json
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.hooks.base import BaseHook
from airflow.models.param import Param
from airflow.sdk import Variable, dag, task
from docker.types import Mount



@dag(
    dag_id="lol_gnn_train_and_gate",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "gnn", "train"],
    default_args={"retries": 1},
    params={
        "dataset_version": Param(
            default="",
            type="string",
            title="Dataset Version",
            description="Target dataset version to download/train/evaluate",
            examples=["15.4"],
        ),
    },
)
def lol_gnn_train_and_gate():
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
            "network_mode": Variable.get("lol_gnn_pipeline_network", default="bridge"),
            "project_dir": Variable.get("lol_gnn_pipeline_project_dir", default="/workspace/src/training"),
            "train_config": Variable.get("lol_gnn_pipeline_train_config", default="config.json"),
            "criteria_file": Variable.get("lol_gnn_pipeline_criteria_file", default="evaluation_criteria.json"),
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

    download_and_train = DockerOperator(
        task_id="download_and_train",
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
                source="/home/dobi/lol-gnn-winrate-service/airflow/dags/git/repo/src/training",
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
            "DATASET_VERSION=\"{{ dag_run.conf.get('dataset_version', params.dataset_version) }}\" && "
            "test -n \"$DATASET_VERSION\" && "
            "cd {{ ti.xcom_pull(task_ids='runtime_config')['project_dir'] }} && "
            "python minio_dataset.py download-latest "
            "--config {{ ti.xcom_pull(task_ids='runtime_config')['train_config'] }} "
            "--bucket {{ ti.xcom_pull(task_ids='runtime_config')['minio_bucket'] }} "
            "--prefix {{ ti.xcom_pull(task_ids='runtime_config')['minio_prefix'] }} "
            "--dataset-version \"$DATASET_VERSION\" "
            "--clear-output --write-dataset-name && "
            "python train.py "
            "--config {{ ti.xcom_pull(task_ids='runtime_config')['train_config'] }} "
            "--pipeline-run-id {{ run_id }} "
            "--run-id-output .mlflow_run_id"
            "'"
        ),
    )

    evaluate_gate = DockerOperator(
        task_id="evaluate_gate",
        retries=0,
        image="{{ ti.xcom_pull(task_ids='runtime_config')['image'] }}",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        email=["dobi0231@naver.com"],
        email_on_failure=True,
        network_mode=Variable.get(
            "lol_gnn_pipeline_network",
            default="lol-gnn-winrate-service_lol-network",
        ),
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/home/dobi/lol-gnn-winrate-service/airflow/dags/git/repo/src/training",
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
            "cd {{ ti.xcom_pull(task_ids='runtime_config')['project_dir'] }} && "
            "python evaluate_gate.py "
            "--config {{ ti.xcom_pull(task_ids='runtime_config')['train_config'] }} "
            "--criteria {{ ti.xcom_pull(task_ids='runtime_config')['criteria_file'] }} "
            "--pipeline-run-id {{ run_id }} "
            "--run-id-file .mlflow_run_id"
            "'"
        ),
    )

    cfg >> download_and_train >> evaluate_gate


dag = lol_gnn_train_and_gate()
