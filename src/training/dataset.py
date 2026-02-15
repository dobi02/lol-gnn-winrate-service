import torch
import glob
import os
import random
import math
from torch.utils.data import IterableDataset, get_worker_info
from torch_geometric.transforms import ToUndirected
from transforms import StreamerModeTransform

class LoLChunkDataset(IterableDataset):
    def __init__(self, config, split='train', shuffle=True):
        """
        Args:
            config (dict): utils.load_config()로 로드된 설정 딕셔너리
            split (str): 'train', 'val', 'test'
            shuffle (bool): 셔플 여부
        """
        super().__init__()
        self.config = config
        self.split = split
        self.shuffle = shuffle
        
        # [변경] JSON 구조에 맞춰 경로 접근 ('paths' 키 아래에 있음)
        self.root_dir = config['paths']['root_dir']
        
        # [변경] data_mode가 JSON 루트 혹은 특정 키에 있는지 확인 (기본값 local)
        self.mode = config.get('data_mode', 'local')
        
        # [변경] 스트리머 모드 설정 접근
        use_streamer = config.get('use_streamer_mode', False)
        self.streamer_transform = StreamerModeTransform() if use_streamer else None
        
        self.undirected_transform = ToUndirected()
        
        self.file_list = self._get_file_list()

    def _get_file_list(self):
        """파일 목록 조회"""
        if self.mode == 'local':
            path = os.path.join(self.root_dir, self.split, "*.pt")
            files = sorted(glob.glob(path))
            # 파일이 없는 경우 경고 메시지 출력 (디버깅용)
            if not files:
                print(f"[Warning] No files found in {path}")
            return files
            
        elif self.mode == 'minio':
            # Mockup: MinIO 로직
            print("Connecting to MinIO for file list... (Mockup)")
            return []
        return []

    def _load_chunk(self, path):
        """청크 파일 로드"""
        if self.mode == 'local':
            # 신뢰할 수 있는 데이터이므로 weights_only=False
            return torch.load(path, weights_only=False)
        elif self.mode == 'minio':
            return []

    def __iter__(self):
        worker_info = get_worker_info()
        files = self.file_list[:]

        if worker_info is not None:
            per_worker = int(math.ceil(len(files) / float(worker_info.num_workers)))
            iter_start = worker_info.id * per_worker
            iter_end = min(iter_start + per_worker, len(files))
            files = files[iter_start:iter_end]

        if self.shuffle:
            random.shuffle(files)

        for path in files:
            try:
                graph_list = self._load_chunk(path)
                
                for data in graph_list:
                    if self.streamer_transform:
                        data = self.streamer_transform(data)
                    
                    # 양방향 엣지 생성
                    data = self.undirected_transform(data)
                    
                    yield data
                    
            except Exception as e:
                print(f"[Warning] Failed to load {path}: {e}")
                continue
    
    def __len__(self):
        # 대략적인 크기 반환 (tqdm 등을 위해)
        return len(self.file_list) * 2000