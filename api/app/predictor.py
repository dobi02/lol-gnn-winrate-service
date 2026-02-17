import os
import sys
import json
import shutil
import torch
import mlflow.tracking
from torch_geometric.data import Batch, HeteroData


class LoLPredictor:
    def __init__(self, run_id: str, device: str = "cpu"):
        self.device = torch.device(device)
        client = mlflow.tracking.MlflowClient()

        # ---------------------------------------------------------
        # 1. Artifact 캐시 확인 -> 없으면 다운로드
        # ---------------------------------------------------------
        cache_root = os.getenv("MLFLOW_ARTIFACT_CACHE_DIR", "/tmp/mlflow-artifacts")
        run_cache_root = os.path.join(cache_root, run_id)
        os.makedirs(run_cache_root, exist_ok=True)

        manifest_path = os.path.join(run_cache_root, "manifest.json")
        cached_local_path = ""
        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
                    cached_local_path = manifest.get("local_path", "")
            except Exception:
                cached_local_path = ""

        def _is_valid_artifact_dir(path: str) -> bool:
            if not path:
                return False
            config_ok = (
                os.path.exists(os.path.join(path, "config", "config.json"))
                or os.path.exists(os.path.join(path, "config.json"))
            )
            weight_ok = os.path.exists(os.path.join(path, "model", "data", "model.pth"))
            code_ok = os.path.isdir(os.path.join(path, "code"))
            return config_ok and weight_ok and code_ok

        local_path = ""
        candidate_paths = [
            cached_local_path,
            run_cache_root,
            os.path.join(run_cache_root, "artifacts"),
        ]
        for candidate in candidate_paths:
            if _is_valid_artifact_dir(candidate):
                local_path = candidate
                print(f"♻️ MLflow artifact cache hit: {local_path}")
                break

        if not local_path:
            local_path = client.download_artifacts(run_id, path="", dst_path=run_cache_root)
            with open(manifest_path, "w", encoding="utf-8") as f:
                json.dump({"run_id": run_id, "local_path": local_path}, f)
            print(f"⬇️ MLflow artifact downloaded: {local_path}")

        # 최근 1개(run_id)만 유지: 현재 run_id를 제외한 캐시 디렉터리 정리
        for entry in os.listdir(cache_root):
            entry_path = os.path.join(cache_root, entry)
            if entry == run_id:
                continue
            if os.path.isdir(entry_path):
                try:
                    shutil.rmtree(entry_path)
                    print(f"🧹 Removed old MLflow cache run: {entry}")
                except Exception as e:
                    print(f"⚠️ Failed to remove old cache run {entry}: {e}")

        code_path = os.path.join(local_path, "code")

        # ---------------------------------------------------------
        # 2. 동적 임포트 (Dynamic Import)
        # ---------------------------------------------------------
        # model.py와 model_utils.py가 있는 경로를 sys.path에 추가
        if code_path not in sys.path:
            sys.path.append(code_path)

        try:
            # model.py에서 LoLGNN 클래스 로드
            from model import LoLGNN
        except ImportError as e:
            raise RuntimeError(f"모델 코드를 불러올 수 없습니다. model_utils.py 등이 포함되었는지 확인해주세요: {e}")

        # ---------------------------------------------------------
        # 3. 모델 초기화
        # ---------------------------------------------------------
        config_path = os.path.join(local_path, "config", "config.json")
        try:
            with open(config_path, 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            # 경로가 다를 경우를 대비한 예비 경로 (artifacts 구조에 따라 다를 수 있음)
            config_path = os.path.join(local_path, "config.json")
            with open(config_path, 'r') as f:
                self.config = json.load(f)

        # 모델 인스턴스 생성
        self.model = LoLGNN(self.config)

        # 가중치 로드
        weight_path = os.path.join(local_path, "model", "data", "model.pth")
        self.model.load_state_dict(torch.load(weight_path, map_location=self.device))
        self.model.to(self.device)
        self.model.eval()

        print("✅ MLflow에서 코드와 모델을 성공적으로 불러왔습니다.")

    def predict_team100_win_rate(self, data: HeteroData) -> float:
        """
        API에서 호출하는 메서드입니다.
        model.py의 forward(x_dict, edge_index_dict, batch_dict) 시그니처에 맞춰 데이터를 가공합니다.
        """
        if self.model is None:
            return 0.5

        # 1. Device 이동
        data = data.to(self.device)

        # 2. Batch 객체 생성 (단일 그래프라도 Batch로 감싸야 batch vector가 생성됨)
        batch = Batch.from_data_list([data])

        # 3. 인자 준비 (model.py의 요구사항)
        # HeteroData Batch 객체는 x_dict, edge_index_dict 속성을 가집니다.
        x_dict = batch.x_dict
        edge_index_dict = batch.edge_index_dict

        # ★ 중요: batch_dict 생성 ★
        # model.py의 global_mean_pool(x_dict['player'], batch_dict['player'])를 위해 필요
        batch_dict = {}
        for node_type in batch.node_types:
            batch_dict[node_type] = batch[node_type].batch

        # 4. 추론
        with torch.no_grad():
            # forward(self, x_dict, edge_index_dict, batch_dict) 호출
            logits = self.model(x_dict, edge_index_dict, batch_dict)
            prob = torch.sigmoid(logits).item()

        return prob
