import argparse
import json
import os
import shutil

import pandas as pd
import torch
from tqdm import tqdm

from database import DataLoader
from graph_builder import GraphFactory


def create_dirs(base_dir: str) -> None:
    for split in ("train", "val", "test"):
        os.makedirs(os.path.join(base_dir, split), exist_ok=True)


def save_chunk(graph_list, output_dir: str, split_type: str, chunk_index: int) -> None:
    if not graph_list:
        return
    file_name = f"{split_type}_chunk_{chunk_index}.pt"
    save_path = os.path.join(output_dir, split_type, file_name)
    torch.save(graph_list, save_path)


def load_builder_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config


def validate_builder_config(config: dict, config_path: str) -> None:
    required = ["paths"]
    missing = [k for k in required if k not in config]
    if missing:
        raise ValueError(f"Missing required keys in {config_path}: {missing}")
    if "root_dir" not in config["paths"]:
        raise ValueError(f"Missing required key in {config_path}: paths.root_dir")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build graph chunks for LoL GNN training")
    parser.add_argument("--config", default="config.json", help="Path to JSON config")
    parser.add_argument("--start-date", required=True, help="Dataset collection start datetime")
    parser.add_argument("--end-date", required=True, help="Dataset collection end datetime")
    parser.add_argument("--dataset-version", default="", help="Dataset version label (e.g., patch version)")
    parser.add_argument("--chunk-size", type=int, default=0, help="Chunk size override")
    parser.add_argument(
        "--clear-output",
        action="store_true",
        help="Delete existing output_dir before generating new chunks",
    )
    args = parser.parse_args()

    config = load_builder_config(args.config)

    config["start_date"] = args.start_date
    config["end_date"] = args.end_date
    if args.dataset_version:
        config["dataset_version"] = args.dataset_version

    validate_builder_config(config, args.config)

    output_dir = config["paths"]["root_dir"]
    if not os.path.isabs(output_dir):
        output_dir = os.path.join(os.getcwd(), output_dir)

    if args.clear_output and os.path.isdir(output_dir):
        shutil.rmtree(output_dir)

    create_dirs(output_dir)

    db_loader = DataLoader()
    graph_factory = GraphFactory(config, db_loader)

    print(f"Fetching matches from {config['start_date']} to {config['end_date']}...")
    df = db_loader.get_root_matches(config["start_date"], config["end_date"])

    if df.empty:
        print("No matches found.")
        return

    print("Sorting and splitting by time...")
    df["game_start_at"] = pd.to_datetime(df["game_start_at"])
    df = df.sort_values(by="game_start_at").reset_index(drop=True)

    total_count = len(df)
    split_ratio = config.get("split_ratio", {"train": 0.8, "val": 0.1, "test": 0.1})
    if abs((split_ratio["train"] + split_ratio["val"] + split_ratio["test"]) - 1.0) > 1e-9:
        raise ValueError(f"split_ratio must sum to 1.0, got {split_ratio}")

    train_idx = int(total_count * split_ratio["train"])
    val_idx = int(total_count * (split_ratio["train"] + split_ratio["val"]))

    splits = {
        "train": df.iloc[:train_idx],
        "val": df.iloc[train_idx:val_idx],
        "test": df.iloc[val_idx:],
    }

    chunk_size = int(args.chunk_size) if int(args.chunk_size) > 0 else int(config.get("chunk_size", 2048))
    total_stats = {"processed": 0, "skipped": 0}

    for split_type, split_df in splits.items():
        print(f"Processing {split_type} set ({len(split_df)} matches)...")

        buffer = []
        chunk_counter = 0

        for _, row in tqdm(split_df.iterrows(), total=len(split_df), desc=split_type):
            try:
                graph_data = graph_factory.create_graph(row)

                if graph_data is None:
                    total_stats["skipped"] += 1
                    continue

                buffer.append(graph_data)
                total_stats["processed"] += 1

                if len(buffer) >= chunk_size:
                    save_chunk(buffer, output_dir, split_type, chunk_counter)
                    buffer = []
                    chunk_counter += 1

            except Exception as exc:
                print(f"[Error] Match {row.get('match_id')}: {exc}")
                total_stats["skipped"] += 1

        if buffer:
            save_chunk(buffer, output_dir, split_type, chunk_counter)

    print(f"Done. Stats: {total_stats}")


if __name__ == "__main__":
    main()
