import glob
import math
import os
import random

import torch
from torch.utils.data import IterableDataset, get_worker_info
from torch_geometric.transforms import ToUndirected

from transforms import StreamerModeTransform


class LoLChunkDataset(IterableDataset):
    def __init__(self, config, split='train', shuffle=True):
        super().__init__()
        self.config = config
        self.split = split
        self.shuffle = shuffle

        self.root_dir = config['paths']['root_dir']
        self.mode = config.get('data_mode', 'local')

        use_streamer = bool(config.get('use_streamer_mode', False))
        train_only_streamer = bool(config.get('train_only_streamer_mode', True))
        streamer_deterministic = bool(config.get('streamer_deterministic', False))
        streamer_seed = int(config.get('streamer_seed', config.get('train', {}).get('seed', 42)))

        enable_streamer = use_streamer and (self.split == 'train' or not train_only_streamer)
        self.streamer_transform = (
            StreamerModeTransform(deterministic=streamer_deterministic, seed=streamer_seed)
            if enable_streamer else None
        )

        self.undirected_transform = ToUndirected()
        self.file_list = self._get_file_list()

    def _get_file_list(self):
        if self.mode == 'local':
            path = os.path.join(self.root_dir, self.split, '*.pt')
            files = sorted(glob.glob(path))
            if not files:
                print(f"[Warning] No files found in {path}")
            return files

        if self.mode == 'minio':
            print('Connecting to MinIO for file list... (Mockup)')
            return []

        return []

    def _load_chunk(self, path):
        if self.mode == 'local':
            return torch.load(path, weights_only=False)
        if self.mode == 'minio':
            return []
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
                    data = self.undirected_transform(data)
                    yield data
            except Exception as exc:
                print(f"[Warning] Failed to load {path}: {exc}")
                continue

    def __len__(self):
        return len(self.file_list) * 2000
