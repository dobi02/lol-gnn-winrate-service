from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import logging
import pendulum
import json

@dag(
    dag_id='minio-crud-test',
    start_date=pendulum.datetime(2025, 11, 18, tz='UTC'),
    schedule=None, 
    catchup=False,
    tags=['test', 'connection'],
)
def conn_test(connection_id:str = 'MINIO_DEFAULT', bucket_name:str = 'test'):
    @task(task_id='create_bucket')
    def create_bucket(conn_id: str, bucket_name: str) -> None:
        logging.info(f'Using S3Hook with conn_id: {conn_id}')
        hook = S3Hook(aws_conn_id=conn_id)

        try:
            if not hook.check_for_bucket(bucket_name):
                hook.create_bucket(bucket_name=bucket_name)
                logging.info(f'Bucket {bucket_name} created to exist')
            else:
                logging.info(f"Bucket '{bucket_name}' already exists.")
        except Exception as e:
            logging.error(f'An error occured while creating bucket {bucket_name}')
            raise

    @task(task_id='upload_string_data')
    def upload_string_data(conn_id: str, bucket_name: str, object_key: str) -> str:
        logging.info(f'Using S3Hook with conn_id: {conn_id}')
        hook = S3Hook(aws_conn_id=conn_id)

        data_dict = {
            "id": 123,
            "name": "Airflow MinIO Test",
            "status": "success"
        }
        json_string = json.dumps(data_dict, ensure_ascii=False, indent=4)

        hook.load_string(
            string_data=json_string,
            key=object_key,
            bucket_name=bucket_name,
            replace=True,
        )

        return object_key

    @task(task_id='read_object')
    def read_object(conn_id: str, bucket_name: str, object_key: str) -> None:
        logging.info(f'Using S3Hook with conn_id: {conn_id}')
        hook = S3Hook(aws_conn_id=conn_id)

        logging.info(f"Reading object '{object_key}' from bucket '{bucket_name}'")
        content = hook.read_key(key=object_key, bucket_name=bucket_name)
        logging.info(f"Content: {content}")
        return content

    @task(task_id='delete_object')
    def delete_object(conn_id: str, bucket_name: str, object_key: str) -> None:
        logging.info(f'Using S3Hook with conn_id: {conn_id}')
        hook = S3Hook(aws_conn_id=conn_id)

        logging.info(f"Deleting object '{object_key}' from bucket '{bucket_name}'")
        hook.delete_objects(bucket=bucket_name, keys=object_key)

        if not hook.check_for_key(object_key, bucket_name):
            logging.info(f"Object '{object_key}' deleted successfully.")
        else:
            logging.error(f"Failed to delete object '{object_key}'.")

    @task(task_id='delete_bucket')
    def delete_bucket(conn_id: str, bucket_name: str):
        logging.info(f'Using S3Hook with conn_id: {conn_id}')
        hook = S3Hook(aws_conn_id=conn_id)

        logging.info(f"Deleting bucket: {bucket_name}")
        try:
            hook.delete_bucket(bucket_name)
            logging.info(f"Bucket '{bucket_name}' deleted.")
        except Exception as e:
            logging.warning(f"Could not delete bucket '{bucket_name}'. Ensure it is empty.")
            raise e

    create_bucket = create_bucket(connection_id, bucket_name)
    upload_string_data = upload_string_data(connection_id, bucket_name, "test.json")
    read_object = read_object(connection_id, bucket_name, upload_string_data)
    delete_object = delete_object(connection_id, bucket_name, upload_string_data)
    delete_bucket = delete_bucket(connection_id, bucket_name)

    create_bucket >> upload_string_data >> read_object >> delete_object >> delete_bucket



dag = conn_test("MINIO_DEFAULT", "test")
