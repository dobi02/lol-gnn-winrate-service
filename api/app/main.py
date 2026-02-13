from __future__ import annotations

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from typing import Any, Dict, List, Tuple, Optional
import traceback
from mlflow.tracking import MlflowClient

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


def get_production_run_id(experiment_name: str) -> str:
    """MLflow를 검색하여 status 태그가 production인 가장 최신 모델의 run_id를 찾습니다."""
    client = MlflowClient()

    # 1. 실험(Experiment) ID 찾기
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        raise ValueError(f"❌ MLflow에서 '{experiment_name}' 실험을 찾을 수 없습니다.")

    # 2. 조건(태그)으로 Run 검색 (최신순 정렬)
    # 검색 조건: tags.status 가 'production' 인 것
    query = "tags.status = 'production'"
    runs = client.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=query,
        order_by=["start_time DESC"],  # 가장 최근에 production으로 지정된 것 1개
        max_results=1
    )

    if not runs:
        raise ValueError(f"❌ '{experiment_name}' 실험에서 '{query}' 태그를 가진 모델이 없습니다.")

    # 3. 찾은 run_id 반환
    return runs[0].info.run_id


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 환경변수에서 설정값 가져오기
    device = os.getenv("DEVICE", "cpu")
    experiment_name = os.getenv("MLFLOW_EXPERIMENT_NAME", "LoL_Win_Prediction_v1")

    # (선택) DB 초기화
    # db.init_db(os.getenv('POSTGRES_DSN'))

    try:
        # ★ MLflow 검색을 통해 Production 모델의 run_id 획득 ★
        print(f"🔍 MLflow에서 '{experiment_name}'의 Production 모델을 검색 중...")
        run_id = get_production_run_id(experiment_name)
        print(f"🎯 Production 모델 발견! (Run ID: {run_id})")

        # 찾은 run_id를 Predictor에 넘겨서 다운로드 및 로드 수행
        predictor = LoLPredictor(run_id=run_id, device=device)
        ml_models["predictor"] = predictor

        ml_models["meta"] = {
            "model": {
                "experiment": experiment_name,
                "run_id": run_id,
            },
            "status": "production"
        }
        print(f"✅ Predictor 서빙 준비 완료")

    except Exception as e:
        print(f"🚨 모델 로드 실패: {e}")
        # 실패하더라도 서버는 띄우려면 여기서 멈추지 않고 Mock으로 넘기거나 에러 처리
        pass

    yield
    # db.close_db()
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

    print("❌ [DEBUG] Upstream Error Caught:")
    traceback.print_exc()

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
# @app.post("/predict/from-discord", response_model=DiscordPredictResponse)
# async def predict_from_discord(request: DiscordPredictRequest):
#     predictor: LoLPredictor = ml_models.get("predictor")
#     if predictor is None:
#         raise HTTPException(status_code=503, detail="Model is not ready")
#
#     # (1~4)
#     spectator_payload, enrichment, warnings = await resolve_spectator_and_enrichment_or_raise(
#         riot_id=request.riot_id,
#         platform_id=request.platform_id,
#         use_history=request.use_history,
#         history_count=request.history_count,
#     )
#
#     # (5~6)
#     meta = ml_models.get("meta")
#     return await predict_and_format_response_or_raise(
#         predictor=predictor,
#         spectator_payload=spectator_payload,
#         enrichment=enrichment,
#         warnings=warnings,
#         request_platform_id=request.platform_id,
#         meta=meta,
#     )


#디버깅용 코드
@app.post("/predict/from-discord", response_model=DiscordPredictResponse)
async def predict_from_discord(request: DiscordPredictRequest):
    print(f"🚀 [DEBUG] 1. 요청 시작: {request.riot_id}", flush=True)

    predictor: LoLPredictor = ml_models.get("predictor")
    if predictor is None:
        raise HTTPException(status_code=503, detail="Model is not ready")

    try:
        # (1~4) Riot API 호출 단계
        print("🚀 [DEBUG] 2. Riot API 데이터 조회 시작...", flush=True)
        spectator_payload, enrichment, warnings = await resolve_spectator_and_enrichment_or_raise(
            riot_id=request.riot_id,
            platform_id=request.platform_id,
            use_history=request.use_history,
            history_count=request.history_count,
        )
        print("✅ [DEBUG] 3. Riot API 데이터 조회 성공!", flush=True)

        # Spectator 확인
        print(f"   -> Spectator Keys: {list(spectator_payload.keys())}", flush=True)

        # ★★★ 여기가 의심 지점입니다 ★★★
        # enrichment가 None인지, dict인지 타입부터 확인합니다.
        print(f"🔍 [DEBUG] Enrichment 타입 확인: {type(enrichment)}", flush=True)
        print(f"🔍 [DEBUG] Enrichment 값 확인: {enrichment}", flush=True)

        # Pydantic 모델인지 확인 후 dict로 변환하여 로깅
        if hasattr(enrichment, "model_dump"):
            enrichment_dict = enrichment.model_dump()
            print(f"   -> Enrichment Keys: {list(enrichment_dict.keys())}", flush=True)
        elif isinstance(enrichment, dict):
            print(f"   -> Enrichment Keys: {list(enrichment.keys())}", flush=True)
        else:
            print(f"⚠️ [WARN] Enrichment가 알 수 없는 타입입니다: {type(enrichment)}", flush=True)

        # (5~6) 모델 입력 변환 및 추론 단계
        print("🚀 [DEBUG] 4. 모델 입력 데이터 변환 및 추론 시작...", flush=True)

        # 4-1. Spectator 객체 변환
        spectator_obj = SpectatorPredictRequest(spectator=spectator_payload, enrichment=enrichment).spectator
        print("   -> [4-1] Spectator 객체 변환 완료", flush=True)

        # 4-2. 그래프 변환
        graph_obj, more_warnings = await build_model_input_from_spectator(spectator_obj, enrichment=enrichment)
        print("   -> [4-2] 그래프(GNN Input) 변환 완료", flush=True)
        warnings.extend(more_warnings)

        # 4-3. 추론
        win100 = predictor.predict_team100_win_rate(graph_obj)
        print(f"   -> [4-3] 모델 추론 완료: WinRate100={win100}", flush=True)

        win100 = float(win100)
        win200 = float(max(0.0, min(1.0, 1.0 - win100)))

        meta = ml_models.get("meta")

        print("✅ [DEBUG] 5. 모든 과정 성공! 응답 반환", flush=True)
        return DiscordPredictResponse(
            win_rate_team_100=win100,
            win_rate_team_200=win200,
            model=meta,
            warnings=warnings,
            game_id=int(spectator_payload.get("gameId")) if spectator_payload.get("gameId") is not None else None,
            platform_id=str(spectator_payload.get("platformId")) if spectator_payload.get(
                "platformId") is not None else request.platform_id,
        )

    except HTTPException as http_exc:
        print(f"⚠️ [DEBUG] HTTP 예외 발생: {http_exc.detail}", flush=True)
        raise http_exc
    except Exception as e:
        # Traceback을 문자열로 받아서 강제로 찍어버립니다.
        error_msg = traceback.format_exc()
        print("\n🚨🚨🚨 [CRITICAL ERROR - FULL TRACEBACK] 🚨🚨🚨", flush=True)
        print(error_msg, flush=True)
        print("---------------------------------------------------------------", flush=True)
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")