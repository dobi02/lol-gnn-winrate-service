FROM pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime

WORKDIR /app

# 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# torch Geometric 의존성 설치
RUN pip install --no-cache-dir \
    pyg-lib \
    torch-scatter \
    torch-sparse \
    torch-cluster \
    torch-spline-conv \
    -f https://data.pyg.org/whl/torch-2.8.0+cu129.html

# PyTorch Geometric 및 나머지 파이썬 패키지 설치
COPY requirements/serving_requirements.txt .
RUN pip install --no-cache-dir -r serving_requirements.txt

# 5. 소스 코드 복사
COPY shared_modules /app/shared_modules
COPY serving /app/serving

# 6. 실행 (GPU 사용 가능)
CMD ["uvicorn", "serving.main:app", "--host", "0.0.0.0", "--port", "8000"]