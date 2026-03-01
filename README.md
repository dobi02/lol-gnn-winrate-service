# LOL GNN Winrate Service

이 저장소는 League of Legends 승률 예측 파이프라인을 위한 Docker 기반 실험/서빙 환경입니다.

## 1) 구성 요약

- **Airflow**: 수집/전처리/학습 파이프라인 오케스트레이션
- **MinIO**: MLflow artifact 및 데이터셋 저장소
- **MLflow**: experiment tracking / model registry
- **JupyterLab**: 분석 및 실험 환경
- **PostgreSQL**
  - Airflow metadata DB
  - 서비스/MLflow용 data DB
- **Redis**: API 캐시
- **FastAPI (`prediction-api`)**: 예측 API

핵심 오케스트레이션 파일: `docker-compose.yml`

## 2) 디렉토리

- `airflow/`: DAG, 로그, 플러그인
- `api/`: 예측 API(FastAPI)
- `dockerfile/`: 커스텀 이미지 정의(`jupyter`, `mlflow`, `train`, `de`)
- `notebooks/`: 실험 노트북/산출물
- `minio/data/`: MinIO 로컬 바인드 데이터
- `requirements/`: Python dependency 목록

## 3) 실행 전 준비

1. 환경 변수 파일 생성

```bash
cp .env.example .env
```

2. Linux에서 Docker 그룹 GID 설정(airflow가 docker.sock 접근 필요)

```bash
echo "DOCKER_GID=$(getent group docker | cut -d: -f3)" >> .env
```

3. 외부 네트워크 생성(없으면 compose 실행 실패)

```bash
docker network create proxy-net
```

## 4) 실행/중지

실행:

```bash
docker compose up -d
```

중지:

```bash
docker compose down
```

볼륨까지 삭제:

```bash
docker compose down -v
```

## 5) 접근 포인트

기본은 reverse proxy 경유 구성이며, 환경에 따라 localhost 포트가 열려 있지 않을 수 있습니다.

- Airflow: `https://airflow.${MY_DOMAIN}`
- MinIO Console: `https://minio.${MY_DOMAIN}`
- Jupyter: `https://jupyter.${MY_DOMAIN}`
- MLflow: `https://mlflow.${MY_DOMAIN}`
- Prediction API: `http://localhost:8000` (`prediction-api` 서비스 포트 매핑)

## 6) Prediction API 상세 (`prediction-api`)

### 역할

`prediction-api`는 Riot spectator payload를 입력받아 팀 100/200 승률을 반환합니다.
모델은 서버 시작 시 MLflow에서 `status=production` 태그가 붙은 최신 run을 찾아 로드합니다.

### API 컨테이너 빌드/실행

Compose 기준:

```bash
docker compose up -d prediction-api
```

단독 빌드:

```bash
docker build -f api/Dockerfile -t prediction-api:latest ./api
```

### 주요 환경 변수

`docker-compose.yml` 기준으로 API가 사용합니다.

- `DATABASE_URL`: Postgres DSN
- `REDIS_URL`: Redis URL
- `RIOT_API_KEY`: Riot API Key
- `MLFLOW_TRACKING_URI`: MLflow 서버 주소
- `MLFLOW_EXPERIMENT_NAME`: production run 탐색 대상 experiment
- `MLFLOW_ARTIFACT_CACHE_DIR`: 모델 artifact 캐시 경로
- `MLFLOW_S3_ENDPOINT_URL`: MinIO S3 endpoint
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`: MinIO 접근용
- `DEVICE`(옵션): 미지정 시 기본 `cpu`

### 엔드포인트

- `GET /health`
  - 모델 로딩 완료 시: `{"status":"ok"}`
  - 로딩 전/실패 시: `{"status":"not_ready"}`

- `GET /meta`
  - 현재 서빙 중 모델 정보(실험명, run_id 등) 반환
  - 모델 미준비 시 `503`

- `POST /predict/by-spectator`
  - spectator payload 기반 승률 추론 메인 엔드포인트
  - 입력 스키마 핵심:
    - `spectator.gameId`
    - `spectator.platformId`
    - `spectator.gameQueueConfigId`
    - `spectator.participants[]` (`teamId`, `spell1Id`, `spell2Id`, `championId`, `perks`)
    - `enrichment` (선택)
  - 응답:
    - `win_rate_team_100` (float)
    - `win_rate_team_200` (float)
    - `model` (experiment/run 메타)
    - `warnings` (기본값 처리 등 경고)

- `POST /predict`
  - Deprecated 엔드포인트
  - 항상 `410 Gone` 반환

참고: `DiscordPredictRequest/Response` 스키마는 코드에 존재하지만, 현재 활성 라우트는 `POST /predict/by-spectator`입니다.

### 요청/응답 예시

```bash
curl -X POST "http://localhost:8000/predict/by-spectator" \
  -H "Content-Type: application/json" \
  -d '{
    "spectator": {
      "gameId": 1234567890,
      "platformId": "KR",
      "gameQueueConfigId": 420,
      "participants": [
        {
          "teamId": 100,
          "spell1Id": 4,
          "spell2Id": 14,
          "championId": 157,
          "perks": {
            "perkIds": [8005, 9111, 9104, 8014, 8138, 8135, 5008, 5008, 5002],
            "perkStyle": 8000,
            "perkSubStyle": 8100
          }
        }
      ]
    }
  }'
```

## 7) `lol-gnn-pipeline` 이미지 재빌드 방법

최근 삭제한 `lol-gnn-pipeline:latest`는 `dockerfile/train.Dockerfile` 기반 수동 빌드 이미지입니다.

빌드:

```bash
docker build -f dockerfile/train.Dockerfile -t lol-gnn-pipeline:latest .
```

대화형 실행 예시(GPU):

```bash
docker run --rm -it \
  --gpus all \
  -v "$(pwd)":/workspace \
  -w /workspace \
  lol-gnn-pipeline:latest
```

compose 네트워크를 붙여 실행해야 할 경우:

```bash
docker run --rm -it \
  --gpus all \
  --network lol-gnn-winrate-service_lol-network \
  -v "$(pwd)":/workspace \
  -w /workspace \
  lol-gnn-pipeline:latest
```

이미지 삭제:

```bash
docker rmi lol-gnn-pipeline:latest
```

## 8) 참고

- DAG 동기화는 `git-sync` 컨테이너가 `example/dags` 브랜치를 주기적으로 pull 합니다.
- Jupyter 데이터는 `./notebooks -> /home/jovyan/work`로 마운트됩니다.
- `prediction-api`는 `./api/app`를 바인드 마운트하여 코드 변경이 즉시 반영됩니다.
