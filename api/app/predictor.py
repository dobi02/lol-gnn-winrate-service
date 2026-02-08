from __future__ import annotations

import os
from typing import Any, Optional

class LoLPredictor:
    """
    Model inference wrapper.

    - Loads model weights from artifacts directory (model.pth)
    - Handles Batch size = 1 by wrapping into PyG Batch when available
    - Returns win rate (float) for team 100
    """

    def __init__(self, artifact_dir: str = "artifacts", device: str = "cpu"):
        self.artifact_dir = artifact_dir
        self.device = device
        self.model = self._load_model()

    def _load_model(self):
        # Skeleton:
        # - Real implementation should reconstruct architecture from config.json
        # - Then load state_dict from model.pth
        model_path_candidates = [
            os.path.join(self.artifact_dir, "model", "data", "model.pth"),
            os.path.join(self.artifact_dir, "model.pth"),
            os.path.join("model.pth"),
        ]
        model_path = next((p for p in model_path_candidates if os.path.exists(p)), None)

        try:
            import torch  # type: ignore
        except Exception:
            # No torch -> fallback to mock
            return _MockModel()

        if model_path is None:
            return _MockModel()

        # TODO(DS): replace with real model class and config-based init
        # For now, load a scripted/torch.nn.Module if provided
        try:
            obj = torch.load(model_path, map_location="cpu")
            if hasattr(obj, "eval"):
                obj.eval()
            return obj
        except Exception:
            return _MockModel()

    def predict_team100_win_rate(self, graph_data: Any) -> float:
        # If we're in mock mode
        if isinstance(self.model, _MockModel):
            return float(self.model.predict(graph_data))

        import torch  # type: ignore

        # Move model to device if possible
        try:
            self.model.to(self.device)
        except Exception:
            pass

        # Handle PyG Batch dimension for single sample
        batch_obj = graph_data
        try:
            from torch_geometric.data import Batch  # type: ignore
            batch_obj = Batch.from_data_list([graph_data])
        except Exception:
            # Not a PyG graph, try to add batch dimension if it's a tensor-like input
            if hasattr(graph_data, "unsqueeze"):
                batch_obj = graph_data.unsqueeze(0)

        # Move to device if supported
        try:
            batch_obj = batch_obj.to(self.device)
        except Exception:
            pass

        with torch.no_grad():
            out = self.model(batch_obj)

            # Common patterns:
            # - out is a single logit tensor shape [1] or [1,1]
            # - apply sigmoid to get probability
            if hasattr(torch, "sigmoid"):
                prob = torch.sigmoid(out)
            else:
                prob = out

            # Extract scalar
            if hasattr(prob, "item"):
                return float(prob.item())

            # Fallback conversion
            return float(prob)

class _MockModel:
    def predict(self, _input: Any) -> float:
        # deterministic-ish mock
        return 0.5
