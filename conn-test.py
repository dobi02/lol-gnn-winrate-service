from airflow import DAG
# 권장 경로로 PythonOperator 임포트
from airflow.providers.standard.operators.python import PythonOperator 
# BaseHook은 SDK로 이동했으나, 현재 경로도 사용 가능
from airflow.hooks.base import BaseHook 
import pendulum 
import logging

log = logging.getLogger(__name__)

def test_minio_connection():
    conn_id = 'MINIO_DEFAULT'
    try:
        conn = BaseHook.get_connection(conn_id)
        log.info(f"Connection '{conn_id}' retrieved successfully.")
        # conn 객체는 host, port 등의 속성을 가집니다.
        log.info(f"Host: {conn.host}, Port: {conn.port}, Login: {conn.login}")
        return f"MinIO 연결 성공: {conn.host}"
    except Exception as e:
        log.error(f"MinIO 연결 테스트 실패: {e}")
        raise

def test_postgres_connection():
    conn_id = 'DATA_POSTGRES_CONNECTION'
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id=conn_id)
        
        # PostgresHook에서 연결 세부 정보를 가져오는 올바른 방법:
        # hook.get_conn() 으로 세션을 얻거나, BaseHook으로 conn 객체를 가져옵니다.
        conn = BaseHook.get_connection(conn_id)
        host_name = conn.host # conn 객체에서 host 속성 사용
        
        hook.get_first("SELECT 1")
        
        log.info(f"Postgres 연결 성공, 쿼리 실행 완료.")
        return f"Postgres 연결 성공: {host_name}" 

    except Exception as e:
        log.error(f"Postgres 연결 테스트 실패: {e}")
        raise

with DAG(
    dag_id='connection_test_dag',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None, 
    catchup=False,
    tags=['test', 'connection'],
) as dag:
    
    test_minio_task = PythonOperator(
        task_id='test_minio_connection',
        python_callable=test_minio_connection,
    )

    test_postgres_task = PythonOperator(
        task_id='test_postgres_connection',
        python_callable=test_postgres_connection,
    )

    test_minio_task
    test_postgres_task
