import json
from datetime import datetime

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.hooks.base import BaseHook
from airflow.sdk import Variable, dag, task


@dag(
    dag_id="lol_gnn_build_trainset",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["mlops", "gnn", "dataset"],
    default_args={"retries": 1},
    params={
        "dataset_version": "",
        "start_date": "",
        "end_date": "",
    },
)
def lol_gnn_build_trainset():
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

    build_and_upload_dataset = DockerOperator(
        task_id="build_and_upload_dataset",
        image="{{ ti.xcom_pull(task_ids='runtime_config')['image'] }}",
        api_version="auto",
        docker_url="unix://var/run/docker.sock",
        network_mode="{{ ti.xcom_pull(task_ids='runtime_config')['network_mode'] }}",
        auto_remove="success",
        mount_tmp_dir=False,
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
            "START_DATE=\"{{ dag_run.conf.get('start_date', params.start_date) }}\" && "
            "END_DATE=\"{{ dag_run.conf.get('end_date', params.end_date) }}\" && "
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

    cfg >> build_and_upload_dataset


dag = lol_gnn_build_trainset()
