FROM nvidia/cuda:12.8.1-cudnn-devel-ubuntu24.04

ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    curl \
    git \
    build-essential \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && \
    apt-get install -y --no-install-recommends python3.13 python3.13-venv python3.13-dev && \
    ln -sf /usr/bin/python3.13 /usr/bin/python3 && \
    ln -sf /usr/bin/python3.13 /usr/bin/python && \
    python -m ensurepip --upgrade && \
    python -m pip install --upgrade pip setuptools wheel

WORKDIR /workspace

COPY requirements/train_requirements.txt /tmp/train_requirements.txt

RUN pip install --no-cache-dir \
    torch==2.8.0 \
    torchvision==0.23.0 \
    torchaudio==2.8.0 \
    --index-url https://download.pytorch.org/whl/cu128 && \
    pip install --no-cache-dir \
    pyg_lib torch_scatter torch_sparse torch_cluster torch_spline_conv \
    -f https://data.pyg.org/whl/torch-2.8.0+cu128.html && \
    pip install --no-cache-dir torch_geometric && \
    pip install --no-cache-dir --ignore-installed -r /tmp/train_requirements.txt

WORKDIR /workspace

CMD ["bash"]
