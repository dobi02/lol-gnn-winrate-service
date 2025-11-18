# Data Science Platform using Docker

This project uses Docker Compose to set up a comprehensive data science platform, including Apache Airflow for workflow orchestration, MinIO as a data lake, JupyterLab for data science, and PostgreSQL databases for metadata and data storage.

Access to all services is managed by an Nginx reverse proxy.

## How to Run

Before starting, you must create a `.env` file by copying the example:

```bash
cp .env.example .env
```

To start all the services in the background, run the following command from the root of the project:

```bash
docker-compose up -d
```

To stop the services, use:

```bash
docker-compose down
```

## Service Access Guide

Here is a list of the services and how to access them in local:

| Service              | URL / Command                             | Default Credentials          |
| -------------------- | ----------------------------------------- | -------------------- |
| **Airflow UI**       | `http://localhost:8080`                   | `airflow` / `airflow`|
| **MinIO Console**    | `http://localhost:9001`                   | `minioadmin` / `minioadmin` |
| **JupyterLab**       | `http://localhost:8888`                   | `your_secret_token`  |
| ~~**Data Engineer SSH**~~| `ssh de_user@localhost -p 2222`           | (SSH Key)            |

or if you have domain, you can access them:

| Service              | URL / Command                             | Default Credentials          |
| -------------------- | ----------------------------------------- | -------------------- |
| **Airflow UI**       | `http://airflow.example.com`                   | `airflow` / `airflow`|
| **MinIO Console**    | `http://minio.example.com`                   | `minioadmin` / `minioadmin` |
| **JupyterLab**       | `http://jupyter.example.com`                   | `your_secret_token`  |

---

## Data Engineer Guide: Airflow DAG Deployment
DAGs are not deployed manually. Instead, the git-sync service automatically monitors and pulls DAG 

files from a specific Git repository.
- Repository: `https://github.com/dobi02/lol-gnn-winrate-service.git`
- Branch: `example/dags`
- Sync Interval: 60 seconds (by default)

To deploy or update a DAG:
1. Create or modify your `DAG.py` file.
2. Commit and push your changes to the `example/dags` branch of the repository above.
3. The git-sync service will automatically pull the changes into Airflow.
4. You can monitor the new DAGs in the Airflow UI.

---

## Data Scientist Guide
### Accessing JupyterLab
You can access the JupyterLab environment at http://localhost:8888 (or http://jupyter.${MY_DOMAIN}). You will need the JUPYTER_TOKEN set in your .env file to log in.

The local directory ./ds_env/notebooks is mounted into the container at /home/jovyan/work. Any notebooks you create here will persist on your host machine.

### Accessing the Data Lake (MinIO)
From within your Jupyter notebook, you can interact with the MinIO data lake. The MinIO service is available at the internal Docker network address `http://minio:9000`.

Here is a sample Python snippet to connect to MinIO and list buckets:
```python
import boto3
from botocore.client import Config

# MinIO connection details (from within the Docker network)
# Use the credentials you set in your .env file
MINIO_ENDPOINT = 'minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'  # Your .env MINIO_ROOT_USER
MINIO_SECRET_KEY = 'minioadmin'  # Your .env MINIO_ROOT_PASSWORD

# Initialize S3 client for MinIO
s3_client = boto3.client(
    's3',
    endpoint_url=f'http://{MINIO_ENDPOINT}',
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_key_MINIO_SECRET_KEY,
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

# --- Example: Download a file ---
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