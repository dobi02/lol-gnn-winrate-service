# 1. Base Image: PyTorch 공식 Runtime 이미지
FROM pytorch/pytorch:2.8.0-cuda12.9-cudnn9-runtime

# 2. 시스템 패키지 설치
RUN apt-get update && apt-get install -y \
    git \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 3. 작업 디렉토리 설정
WORKDIR /app

# 4. PyTorch Geometric (PyG) 및 의존성 설치
RUN pip install --no-cache-dir \
    pyg_lib \
    torch_scatter \
    torch_sparse \
    torch_cluster \
    torch_spline_conv \
    torch_geometric \
    -f https://data.pyg.org/whl/torch-2.8.0+cu129.html

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# 실행 (Port 8000)
CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]