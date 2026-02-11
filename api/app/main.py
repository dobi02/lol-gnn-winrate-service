from __future__ import annotations

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from typing import Any, Dict, List, Tuple, Optional

from .schemas import (
    SpectatorPredictRequest,
    SpectatorPredictResponse,
    DiscordPredictRequest,
    DiscordPredictResponse,
)
from .predictor import LoLPredictor
from .services import build_model_input_from_spectator, fetch_spectator_and_enrichment_from_riot_id
from .model_loader import load_artifacts_meta
from . import db

# Global model store
ml_models = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    artifact_dir = os.getenv("ARTIFACT_DIR", "artifacts")
    device = os.getenv("DEVICE", "cpu")  # keep CPU default for safety

    # Initialize Postgres cache (optional). Set POSTGRES_DSN to enable.
    db.init_db(os.getenv('POSTGRES_DSN'))

    predictor = LoLPredictor(artifact_dir=artifact_dir, device=device)
    ml_models["predictor"] = predictor
    ml_models["meta"] = load_artifacts_meta(artifact_dir)

    print(f"✅ Predictor ready (artifact_dir={artifact_dir}, device={device})")
    yield
    db.close_db()
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

def _map_upstream_errors_to_http(e: Exception) -> HTTPException:
    """1~4 단계에서 발생하는 업스트림(Riot/DB) 오류를 HTTPException으로 매핑."""
    msg = str(e)
    if "riot_id must be in" in msg:
        return HTTPException(status_code=422, detail=msg)
    if "Riot API error 404" in msg:
        return HTTPException(status_code=404, detail="Summoner is not currently in an active game")
    if "RIOT_API_KEY is not set" in msg:
        return HTTPException(status_code=500, detail="Server is missing RIOT_API_KEY")
    return HTTPException(status_code=502, detail=f"Upstream error: {msg}")


async def resolve_spectator_and_enrichment_or_raise(
    *,
    riot_id: str,
    platform_id: str,
    use_history: bool,
    history_count: int,
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    """
    (1~4) Discord 입력(riot_id)로부터:
      1) riot_id -> puuid
      2) puuid -> active game spectator payload
      3) DB cache for participant history/mastery
      4) Cache miss -> Riot fetch
    를 수행하고 (spectator_payload, enrichment, warnings)를 반환.
    실패 시 적절한 HTTPException을 raise.
    """
    try:
        spectator_payload, enrichment, warnings = await fetch_spectator_and_enrichment_from_riot_id(
            riot_id=riot_id,
            platform_id=platform_id,
            use_history=use_history,
            history_count=history_count,
        )
        return spectator_payload, enrichment, warnings
    except Exception as e:
        raise _map_upstream_errors_to_http(e)


async def predict_and_format_response_or_raise(
    *,
    predictor: "LoLPredictor",
    spectator_payload: Dict[str, Any],
    enrichment: Dict[str, Any],
    warnings: List[str],
    request_platform_id: str,
    meta: Any,
) -> "DiscordPredictResponse":
    """
    (5~6)
      5) Build model input and predict
      6) Return to bot (DiscordPredictResponse)
    를 수행.
    실패 시 적절한 HTTPException을 raise.
    """
    # 기존 파이프라인 재사용 (spectator -> graph)
    spectator_obj = SpectatorPredictRequest(spectator=spectator_payload, enrichment=enrichment).spectator
    graph_obj, more_warnings = await build_model_input_from_spectator(spectator_obj, enrichment=enrichment)
    warnings.extend(more_warnings)

    try:
        win100 = predictor.predict_team100_win_rate(graph_obj)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction failed: {type(e).__name__}: {e}")

    win100 = float(win100)
    win200 = float(max(0.0, min(1.0, 1.0 - win100)))

    return DiscordPredictResponse(
        win_rate_team_100=win100,
        win_rate_team_200=win200,
        model=meta,
        warnings=warnings,
        game_id=int(spectator_payload.get("gameId")) if spectator_payload.get("gameId") is not None else None,
        platform_id=str(spectator_payload.get("platformId")) if spectator_payload.get("platformId") is not None else request_platform_id,
    )


# ---- 기존 엔드포인트는 아래처럼 “두 함수”를 호출만 하도록 얇게 만들면 됩니다. ----
@app.post("/predict/from-discord", response_model=DiscordPredictResponse)
async def predict_from_discord(request: DiscordPredictRequest):
    predictor: LoLPredictor = ml_models.get("predictor")
    if predictor is None:
        raise HTTPException(status_code=503, detail="Model is not ready")

    # (1~4)
    spectator_payload, enrichment, warnings = await resolve_spectator_and_enrichment_or_raise(
        riot_id=request.riot_id,
        platform_id=request.platform_id,
        use_history=request.use_history,
        history_count=request.history_count,
    )

    # (5~6)
    meta = ml_models.get("meta")
    return await predict_and_format_response_or_raise(
        predictor=predictor,
        spectator_payload=spectator_payload,
        enrichment=enrichment,
        warnings=warnings,
        request_platform_id=request.platform_id,
        meta=meta,
    )
