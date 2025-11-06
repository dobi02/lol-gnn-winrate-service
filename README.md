# Data Science Platform using Docker

This project sets up a complete data science platform using Docker Compose, including environments for Data Engineering and Data Science, a data lake, and a workflow orchestrator.

## How to Run

To start all the services in the background, run the following command from the root of the project:

```bash
docker-compose up -d
```

To stop the services, use:

```bash
docker-compose down
```

## Service Access Guide

Here is a list of the services and how to access them:

| Service              | URL / Command                             | Credentials          |
| -------------------- | ----------------------------------------- | -------------------- |
| **Airflow UI**       | `http://localhost:8080`                   | `airflow` / `airflow`|
| **MinIO Console**    | `http://localhost:9001`                   | `minioadmin` / `minioadmin` |
| **JupyterLab**       | `http://localhost:8888`                   | `your_secret_token`  |
| **Data Engineer SSH**| `ssh de_user@localhost -p 2222`           | (SSH Key)            |
| **~~Data Scientist SSH~~**| `ssh jovyan@localhost -p 2223`            | (SSH Key)            |

---

## Data Engineer Guide

### 1. Connecting to the Environment

You can access the Data Engineer container via SSH. This environment is designed for developing and managing data pipelines.

-   **Port**: `2222`
-   **Username**: `de_user`

**Command:**
```bash
ssh de_user@localhost -p 2222
```
*Note: You will need to add your SSH public key to `ssh_keys/de_authorized_keys`.*

### 2. Deploying Airflow DAGs

The Data Engineer environment has its `/home/de_user/dags` directory mounted to the local `./airflow/dags` folder. This local folder is then directly mounted into the Airflow scheduler, webserver, and other components. This setup allows Data Engineers to develop and deploy DAGs directly within their isolated environment.

Simply create or modify your DAG Python files in `/home/de_user/dags` within your Data Engineer container (or by modifying the local `./airflow/dags` folder), and Airflow will automatically detect and deploy them.

1.  Access your Data Engineer environment via SSH.
2.  Create a new DAG file (e.g., `my_new_dag.py`) in the `/home/de_user/dags` directory inside your container.
3.  Airflow will automatically detect the new file and add it to its DAG list.
4.  You can view and manage the DAG from the **Airflow UI** at `http://localhost:8080`.

---

## Data Scientist Guide

### ~~1. Connecting to the Environment~~

~~You can access the Data Scientist container, which comes with JupyterLab and common data science libraries, via SSH.~~

-   **Port**: `2223`
-   **Username**: `jovyan`

**Command:**
```bash
ssh jovyan@localhost -p 2223
```
*Note: You will need to add your SSH public key to `ssh_keys/ds_authorized_keys`.*

### 2. Accessing JupyterLab

JupyterLab is running and can be accessed directly in your browser. The local `./ds_env/notebooks` directory is mounted as the workspace.

-   **URL**: `http://localhost:8888`
-   **Token**: `your_secret_token` (as defined in `docker-compose.yml`)

### 3. Accessing Data from the Data Lake (MinIO)

You can interact with the MinIO data lake using the `boto3` library in your Python scripts or notebooks. The MinIO service is available to the Data Scientist container at the address `http://minio:9000`.

Here is a sample Python snippet to connect to MinIO and list the available buckets:

```python
import boto3
from botocore.client import Config

# MinIO connection details
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'

# Initialize S3 client for MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=f'http://{MINIO_ENDPOINT}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4')
)

# --- Example: List buckets ---
try:
    response = s3_client.list_buckets()
    print("Successfully connected to MinIO. Buckets:")
    for bucket in response['Buckets']:
        print(f"- {bucket['Name']}")
except Exception as e:
    print(f"Error connecting to MinIO: {e}")

# --- Example: Download a file from a bucket ---
# try:
#     bucket_name = 'your-bucket-name'
#     object_name = 'path/to/your/file.csv'
#     local_file_path = 'downloaded_file.csv'
#
#     s3_client.download_file(bucket_name, object_name, local_file_path)
#     print(f"Successfully downloaded {object_name} to {local_file_path}")
#
# except Exception as e:
#     print(f"Error downloading file: {e}")

```
