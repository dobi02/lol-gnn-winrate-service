from __future__ import annotations

import json
import os
from typing import Any, Dict


def _safe_read_json(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def load_artifacts_meta(artifact_dir: str) -> Dict[str, Any]:
    """
    Load minimal metadata for /meta endpoint.
    This is NOT responsible for loading model weights.
    """
    config_path_candidates = [
        os.path.join(artifact_dir, "config", "config.json"),
        os.path.join(artifact_dir, "config.json"),
        os.path.join("config", "config.json"),
        "config.json",
    ]
    config_data: Dict[str, Any] = {}
    for p in config_path_candidates:
        if os.path.exists(p):
            config_data = _safe_read_json(p)
            break

    meta = {
        "model": {
            "experiment": config_data.get("experiment_name", "LoL_Win_Prediction_v1"),
            "run_name": config_data.get("run_name", "unknown"),
            "data_version": config_data.get("data_version", "unknown"),
            "artifact_dir": artifact_dir,
        },
        "config_present": bool(config_data),
    }
    return meta
