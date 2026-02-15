import argparse
import json
import os
import shutil
from datetime import datetime, timezone

import boto3
from botocore.client import Config


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def resolve_root_dir(project_dir: str, cfg: dict) -> str:
    root_dir = cfg["paths"]["root_dir"]
    if os.path.isabs(root_dir):
        return root_dir
    return os.path.join(project_dir, root_dir)


def get_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT_URL") or os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://lol-minio:9000")
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("MINIO_ROOT_PASSWORD")

    if not access_key or not secret_key:
        raise ValueError("Missing MinIO credentials. Set AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
    )


def ensure_bucket(client, bucket: str):
    try:
        client.head_bucket(Bucket=bucket)
    except Exception:
        client.create_bucket(Bucket=bucket)


def upload_dataset(config_path: str, bucket: str, prefix: str, dataset_version: str) -> None:
    if not dataset_version or not dataset_version.strip():
        raise ValueError("dataset_version is required")

    project_dir = os.getcwd()
    cfg = load_config(config_path)
    root_dir = resolve_root_dir(project_dir, cfg)

    client = get_s3_client()
    ensure_bucket(client, bucket)

    dataset_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    version_prefix = f"{prefix.rstrip('/')}/version={dataset_version}"
    dataset_prefix = f"{version_prefix}/{dataset_id}"

    split_counts = {}
    for split in ("train", "val", "test"):
        split_dir = os.path.join(root_dir, split)
        if not os.path.isdir(split_dir):
            raise FileNotFoundError(f"Missing split directory: {split_dir}")

        pt_files = sorted([f for f in os.listdir(split_dir) if f.endswith(".pt")])
        if not pt_files:
            raise FileNotFoundError(f"No .pt files found in {split_dir}")

        split_counts[split] = len(pt_files)

        for name in pt_files:
            local_path = os.path.join(split_dir, name)
            object_key = f"{dataset_prefix}/{split}/{name}"
            client.upload_file(local_path, bucket, object_key)

    metadata = {
        "dataset_version": dataset_version,
        "dataset_id": dataset_id,
        "prefix": dataset_prefix,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "split_counts": split_counts,
    }

    metadata_bytes = json.dumps(metadata, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(Bucket=bucket, Key=f"{dataset_prefix}/metadata.json", Body=metadata_bytes, ContentType="application/json")
    client.put_object(
        Bucket=bucket,
        Key=f"{version_prefix}/latest.json",
        Body=metadata_bytes,
        ContentType="application/json",
    )

    print(f"Uploaded dataset to s3://{bucket}/{dataset_prefix}")


def download_latest_dataset(
    config_path: str,
    bucket: str,
    prefix: str,
    dataset_version: str,
    clear_output: bool,
    write_dataset_name: bool,
):
    if not dataset_version or not dataset_version.strip():
        raise ValueError("dataset_version is required")

    project_dir = os.getcwd()
    cfg = load_config(config_path)
    root_dir = resolve_root_dir(project_dir, cfg)

    client = get_s3_client()

    latest_key = f"{prefix.rstrip('/')}/version={dataset_version}/latest.json"
    latest_obj = client.get_object(Bucket=bucket, Key=latest_key)
    latest = json.loads(latest_obj["Body"].read().decode("utf-8"))
    dataset_prefix = latest["prefix"]

    if clear_output and os.path.isdir(root_dir):
        shutil.rmtree(root_dir)

    for split in ("train", "val", "test"):
        os.makedirs(os.path.join(root_dir, split), exist_ok=True)

    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=f"{dataset_prefix}/")

    downloaded = 0
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".pt"):
                continue

            rel = key[len(dataset_prefix) + 1 :]
            local_path = os.path.join(root_dir, rel)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            client.download_file(bucket, key, local_path)
            downloaded += 1

    if downloaded == 0:
        raise FileNotFoundError(f"No dataset chunk files found at s3://{bucket}/{dataset_prefix}")

    if write_dataset_name:
        if "mlflow" not in cfg:
            cfg["mlflow"] = {}
        cfg["mlflow"]["dataset_name"] = f"{latest.get('dataset_version', dataset_version)}:{latest.get('dataset_id', 'latest')}"
        cfg["mlflow"]["dataset_version"] = latest.get("dataset_version", dataset_version)
        cfg["mlflow"]["dataset_id"] = latest.get("dataset_id", "latest")
        cfg["mlflow"]["dataset_prefix"] = latest.get("prefix", "")
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=4)

    print(f"Downloaded {downloaded} chunks from s3://{bucket}/{dataset_prefix} into {root_dir}")


def parse_args():
    parser = argparse.ArgumentParser(description="Upload/download latest training dataset to/from MinIO")
    sub = parser.add_subparsers(dest="command", required=True)

    up = sub.add_parser("upload")
    up.add_argument("--config", default="config.json")
    up.add_argument("--bucket", default=os.getenv("MINIO_DATASET_BUCKET", "mlflow"))
    up.add_argument("--prefix", default=os.getenv("MINIO_DATASET_PREFIX", "datasets/lol_gnn"))
    up.add_argument("--dataset-version", required=True)

    down = sub.add_parser("download-latest")
    down.add_argument("--config", default="config.json")
    down.add_argument("--bucket", default=os.getenv("MINIO_DATASET_BUCKET", "mlflow"))
    down.add_argument("--prefix", default=os.getenv("MINIO_DATASET_PREFIX", "datasets/lol_gnn"))
    down.add_argument("--dataset-version", required=True)
    down.add_argument("--clear-output", action="store_true")
    down.add_argument("--write-dataset-name", action="store_true")

    return parser.parse_args()


def main():
    args = parse_args()

    if args.command == "upload":
        upload_dataset(
            config_path=args.config,
            bucket=args.bucket,
            prefix=args.prefix,
            dataset_version=args.dataset_version,
        )
    elif args.command == "download-latest":
        download_latest_dataset(
            config_path=args.config,
            bucket=args.bucket,
            prefix=args.prefix,
            dataset_version=args.dataset_version,
            clear_output=args.clear_output,
            write_dataset_name=args.write_dataset_name,
        )


if __name__ == "__main__":
    main()
