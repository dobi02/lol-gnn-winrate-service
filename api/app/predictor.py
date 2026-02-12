import os
import sys
import json
import torch
import mlflow.tracking
import tempfile

class LoLPredictor:
    def __init__(self, run_id: str, device: str = "cpu"):
        self.device = torch.device(device)
        client = mlflow.tracking.MlflowClient()

        # ---------------------------------------------------------
        # 1. Artifact 다운로드
        # ---------------------------------------------------------
        temp_dir = tempfile.mkdtemp()

        # ★ 수정된 부분: path="" 로 변경하여 전체 아티팩트 폴더를 temp_dir로 다운로드
        local_path = client.download_artifacts(run_id, path="", dst_path=temp_dir)

        # (2) 코드 파일 다운로드 및 경로 설정
        code_path = os.path.join(local_path, "code")

        # ---------------------------------------------------------
        # 2. 동적 임포트 (Dynamic Import)
        # ---------------------------------------------------------
        # 파이썬이 다운로드 받은 code 폴더를 바라보게 만듦
        if code_path not in sys.path:
            sys.path.append(code_path)

        # ★ 이제 마치 내 프로젝트에 있는 것처럼 import 가능!
        try:
            from model import LoLGNN  # 다운로드 받은 model.py에서 import
        except ImportError as e:
            raise RuntimeError(f"모델 코드를 불러올 수 없습니다: {e}")

        # ---------------------------------------------------------
        # 3. 모델 초기화
        # ---------------------------------------------------------
        config_path = os.path.join(local_path, "config", "config.json")
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        # 저장된 코드로 모델 객체 생성
        self.model = LoLGNN(self.config)

        # 가중치 로드
        weight_path = os.path.join(local_path, "model", "data", "model.pth")
        self.model.load_state_dict(torch.load(weight_path, map_location=self.device))
        self.model.to(self.device)
        self.model.eval()

        print("✅ MLflow에서 코드와 모델을 성공적으로 불러왔습니다.")

    def _load_model(self, artifact_dir):
        # 1. Config 로드 (하이퍼파라미터용)
        config_path = os.path.join(artifact_dir, "config", "config.json")
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
        except:
            config = {}  # Fallback

        model_path = os.path.join(artifact_dir, "model", "data", "model.pth")

        # 2. 모델 아키텍처 초기화 (User Model Spec에 맞게)
        # 예시: HeteroGNN 클래스 생성
        try:
            model = LoLGNN(
                hidden_channels=config.get("hidden_dim", 64),
                out_channels=1
            )

            # 3. 가중치 로드
            state_dict = torch.load(model_path, map_location=self.device)
            model.load_state_dict(state_dict)
            model.to(self.device)
            model.eval()
            return model
        except Exception as e:
            print(f"Model Load Failed: {e}")
            return None

    def predict(self, spectator_payload: dict, enrichment_payload: dict) -> float:
        if self.model is None:
            return 0.5

        # 1. 전처리 (JSON -> HeteroData)
        data = convert_spectator_to_graph(spectator_payload, enrichment_payload)

        # 2. Device 이동
        data = data.to(self.device)

        # 3. Batch Dimension 추가 (GNN 모델이 배치를 요구할 경우)
        # PyG의 경우 단일 그래프도 모델에 바로 넣을 수 있는 경우가 많지만,
        # Batch 객체로 만들어주는 것이 안전합니다.
        from torch_geometric.data import Batch
        batch = Batch.from_data_list([data])

        # 4. 추론
        with torch.no_grad():
            logits = self.model(batch.x_dict, batch.edge_index_dict)  # 모델 forward signature에 따라 다름
            # 또는 logits = self.model(batch) 

            prob = torch.sigmoid(logits).item()
            return prob