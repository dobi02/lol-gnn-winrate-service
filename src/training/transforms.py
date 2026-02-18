import hashlib
import random

import torch


class StreamerModeTransform:
    def __init__(self, max_players=5, num_players=10, deterministic=False, seed=42):
        self.max_players = int(max_players)
        self.num_players = int(num_players)
        self.deterministic = bool(deterministic)
        self.seed = int(seed)

    def _sample_seed_from_data(self, data):
        player_x = data['player'].x.detach().cpu()
        signature_bytes = player_x.numpy().tobytes()
        digest = hashlib.blake2b(signature_bytes, digest_size=8).digest()
        graph_sig = int.from_bytes(digest, byteorder='little', signed=False)
        return self.seed ^ graph_sig

    def __call__(self, data):
        rng = random.Random(self._sample_seed_from_data(data)) if self.deterministic else random

        max_mask = min(self.max_players, self.num_players)
        num_to_mask = rng.randint(0, max_mask)
        if num_to_mask == 0:
            return data

        players_to_mask = torch.tensor(
            rng.sample(range(self.num_players), num_to_mask),
            dtype=torch.long,
        )

        edge_type = ('player', 'has_history', 'history')
        if edge_type in data.edge_index_dict:
            edge_index = data[edge_type].edge_index
            src_nodes = edge_index[0]
            keep_mask = ~torch.isin(src_nodes, players_to_mask)
            data[edge_type].edge_index = edge_index[:, keep_mask]

        return data
