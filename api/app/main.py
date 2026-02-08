from __future__ import annotations

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException

from .schemas import (
    SpectatorPredictRequest,
    SpectatorPredictResponse,
)
from .predictor import LoLPredictor
from .services import build_model_input_from_spectator
from .model_loader import load_artifacts_meta

# uvicorn api.app.main:app --host 0.0.0.0 --port 8000 <- 터미널 실행 코드

# Global model store
ml_models = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    artifact_dir = os.getenv("ARTIFACT_DIR", "artifacts")
    device = os.getenv("DEVICE", "cpu")  # keep CPU default for safety

    predictor = LoLPredictor(artifact_dir=artifact_dir, device=device)
    ml_models["predictor"] = predictor
    ml_models["meta"] = load_artifacts_meta(artifact_dir)

    print(f"✅ Predictor ready (artifact_dir={artifact_dir}, device={device})")
    yield
    ml_models.clear()


app = FastAPI(
    title="LoL GNN Winrate Service",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/health")
async def health():
    ready = "predictor" in ml_models
    return {"status": "ok" if ready else "not_ready"}


@app.get("/meta")
async def meta():
    if "meta" not in ml_models:
        raise HTTPException(status_code=503, detail="Model metadata is not ready")
    return ml_models["meta"]


@app.post("/predict/by-spectator", response_model=SpectatorPredictResponse)
async def predict_by_spectator(request: SpectatorPredictRequest):
    predictor: LoLPredictor = ml_models.get("predictor")
    if predictor is None:
        raise HTTPException(status_code=503, detail="Model is not ready")

    warnings = []

    # Build model input (Graph/Data) from spectator payload
    try:
        graph_obj, build_warnings = await build_model_input_from_spectator(
            spectator=request.spectator,
            enrichment=request.enrichment,
        )
        warnings.extend(build_warnings)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Preprocessing failed: {e}")

    # Predict
    try:
        win_rate_100 = predictor.predict_team100_win_rate(graph_obj)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Inference failed: {e}")

    # Ensure complementary probability
    win_rate_100 = float(win_rate_100)
    win_rate_200 = float(max(0.0, min(1.0, 1.0 - win_rate_100)))

    meta = ml_models.get("meta", {})
    model_meta = meta.get("model", meta) if isinstance(meta, dict) else {}

    return SpectatorPredictResponse(
        win_rate_team_100=win_rate_100,
        win_rate_team_200=win_rate_200,
        model=model_meta,
        warnings=warnings,
    )


@app.post("/predict")
async def predict_deprecated():
    raise HTTPException(
        status_code=410,
        detail="Deprecated. Use POST /predict/by-spectator with Riot spectator payload.",
    )
