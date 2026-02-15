import argparse
import json
import os

import mlflow
import numpy as np
import torch
from torch_geometric.loader import DataLoader

from dataset import LoLChunkDataset
from metrics import calculate_metrics
from model import LoLGNN
from utils import load_config


def evaluate(model, loader, device, prefix="val"):
    model.eval()
    preds, targets = [], []

    with torch.no_grad():
        for batch in loader:
            batch = batch.to(device)
            out = model(batch.x_dict, batch.edge_index_dict, batch.batch_dict)

            prob = torch.sigmoid(out).view(-1).cpu().numpy()
            target = batch.y.view(-1).cpu().numpy()

            preds.extend(prob)
            targets.extend(target)

    preds = np.array(preds)
    targets = np.array(targets)

    return calculate_metrics(targets, preds, prefix=prefix)


def main(config_path: str = "config.json", pipeline_run_id: str = "", run_id_output: str = ""):
    config = load_config(config_path)

    mlflow.set_tracking_uri(config["mlflow"]["uri"])
    mlflow.set_experiment(config["mlflow"]["experiment_name"])

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"Using device: {device}")

    with mlflow.start_run(run_name=f"{config['mlflow']['run_name_prefix']}_train") as run:
        if "tags" in config["mlflow"]:
            mlflow.set_tags(config["mlflow"]["tags"])

        if pipeline_run_id:
            mlflow.set_tag("pipeline_run_id", pipeline_run_id)

        dataset_name = config["mlflow"].get("dataset_name", "unknown")
        mlflow.log_param("dataset_name", dataset_name)
        for key in ("dataset_version", "dataset_id", "dataset_prefix"):
            value = config["mlflow"].get(key)
            if value is not None and str(value) != "":
                mlflow.log_param(key, str(value))
        mlflow.log_params(config["train"])
        mlflow.log_params(config["data_dims"])

        train_ds = LoLChunkDataset(config=config, split="train")
        val_ds = LoLChunkDataset(config=config, split="val", shuffle=False)
        test_ds = LoLChunkDataset(config=config, split="test", shuffle=False)

        train_loader = DataLoader(train_ds, batch_size=config["train"]["batch_size"], num_workers=2)
        val_loader = DataLoader(val_ds, batch_size=config["train"]["batch_size"], num_workers=2)
        test_loader = DataLoader(test_ds, batch_size=config["train"]["batch_size"], num_workers=2)

        model = LoLGNN(config=config).to(device)
        optimizer = torch.optim.Adam(model.parameters(), lr=config["train"]["learning_rate"])
        criterion = torch.nn.BCEWithLogitsLoss()

        print(f"Run ID: {run.info.run_id} - Start Training...")

        best_val_loss = float("inf")

        for epoch in range(1, config["train"]["epochs"] + 1):
            model.train()
            total_loss = 0
            steps = 0

            for batch in train_loader:
                batch = batch.to(device)
                optimizer.zero_grad()

                out = model(batch.x_dict, batch.edge_index_dict, batch.batch_dict)
                loss = criterion(out, batch.y.view(-1, 1))

                loss.backward()
                optimizer.step()

                total_loss += loss.item()
                steps += 1

            avg_train_loss = total_loss / steps if steps > 0 else 0
            val_metrics = evaluate(model, val_loader, device)

            print(f"Epoch {epoch} | Train Loss: {avg_train_loss:.4f}")
            print(f"Val Metrics: {val_metrics}")

            mlflow.log_metric("train_loss", avg_train_loss, step=epoch)
            for k, v in val_metrics.items():
                mlflow.log_metric(k, v, step=epoch)

            if val_metrics["val_log_loss"] < best_val_loss:
                best_val_loss = val_metrics["val_log_loss"]
                torch.save(model.state_dict(), config["paths"]["model_save_path"])
                print(f"--> Model Saved (Val Log Loss: {best_val_loss:.4f})")

        print("\nTraining Complete. Starting Final Test Evaluation...")

        if os.path.exists(config["paths"]["model_save_path"]):
            checkpoint = torch.load(config["paths"]["model_save_path"])
            model.load_state_dict(checkpoint)
            print("Loaded Best Model for Testing.")
        else:
            print("Warning: No model checkpoint found. Using last epoch model.")

        test_metrics = evaluate(model, test_loader, device, prefix="test")
        print(f"Final Test Metrics: {test_metrics}")

        mlflow.log_metrics(test_metrics)
        mlflow.log_artifact(config["paths"]["model_save_path"], artifact_path="model/data")

        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(config, f, indent=4, ensure_ascii=False)

        mlflow.log_artifact(config_path, artifact_path="config")
        mlflow.log_artifact("model.py", artifact_path="code")
        mlflow.log_artifact("model_utils.py", artifact_path="code")

        run_id = run.info.run_id

    if run_id_output:
        with open(run_id_output, "w", encoding="utf-8") as f:
            f.write(run_id)

    print(f"Training Complete & Logged to MLflow. run_id={run_id}")


def parse_args():
    parser = argparse.ArgumentParser(description="Train LoL GNN and log to MLflow")
    parser.add_argument("--config", default="config.json", help="Path to training config")
    parser.add_argument("--pipeline-run-id", default="", help="External orchestration run id")
    parser.add_argument("--run-id-output", default="", help="File path to write MLflow run_id")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    main(config_path=args.config, pipeline_run_id=args.pipeline_run_id, run_id_output=args.run_id_output)
