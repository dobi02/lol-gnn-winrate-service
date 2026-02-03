from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from .schemas import PredictionRequest, PredictionResponse
from .model_loader import load_model
from .services import get_match_data_hybrid

# 전역 모델 저장소
ml_models = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 시작 시: 모델 로드 (지금은 Mock 클래스)
    ml_models["lol_gnn"] = load_model()
    print("✅ Model loaded successfully (Mock ver.)")
    yield
    # 종료 시: 정리
    ml_models.clear()


app = FastAPI(lifespan=lifespan)


@app.post("/predict", response_model=PredictionResponse)
async def predict_win_rate(request: PredictionRequest):
    if "lol_gnn" not in ml_models:
        raise HTTPException(status_code=503, detail="Model is not ready")

    try:
        # 1. 하이브리드 데이터 수집 및 전처리
        # (시간이 좀 걸리므로 비동기 처리가 매우 중요)
        model_input = await get_match_data_hybrid(request.summoner_name)

        # 2. 모델 추론
        # Mock 모델의 predict 호출
        win_prob = ml_models["lol_gnn"].predict(model_input)

        # 3. 결과 반환 (Blue팀 기준)
        return PredictionResponse(
            summoner_name=request.summoner_name,
            win_rate=win_prob,
            team_color="Blue",  # 로직에 따라 계산 필요
            status="Success"
        )

    except Exception as e:
        print(f"Error: {e}")
        raise HTTPException(status_code=500, detail=str(e))