# model.py
import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv

from model_utils import CategoricalEncoder


class LoLGNN(torch.nn.Module):
    """
    DAG-safe integrated architecture (fixed, no exp toggles):
    - Role-normalized history stats
    - Relation-attention message passing
    - Team-attention readout
    - Team aggregate scalar features
    """

    def __init__(self, config):
        super().__init__()
        self.config = config

        hidden_dim = config['train']['hidden_channels']
        self.dropout_rate = float(config['train'].get('dropout_rate', 0.0))

        player_dim = int(config['data_dims']['player_dim'])
        history_dim = int(config['data_dims']['history_dim']) + 11

        emb_conf = config['emb_configs']
        self.player_enc = CategoricalEncoder(
            input_dim=player_dim,
            cat_indices=[1, 2, 3, 4, 5],
            embedding_configs=emb_conf,
            out_channels=hidden_dim,
        )

        history_emb_conf = {
            0: (2000, 16),
            3: (10, 4),
        }
        self.history_enc = CategoricalEncoder(
            input_dim=history_dim,
            cat_indices=[0, 3],
            embedding_configs=history_emb_conf,
            out_channels=hidden_dim,
        )

        self.player_relations = [
            ('player', 'teammate', 'player'),
            ('player', 'enemy', 'player'),
            ('history', 'rev_has_history', 'player'),
        ]

        self.num_layers = 3
        self.player_rel_convs = nn.ModuleList()
        self.player_rel_logits = nn.ParameterList()
        for _ in range(self.num_layers):
            rel_convs = nn.ModuleDict()
            for rel in self.player_relations:
                rel_convs[self._rel_to_key(rel)] = SAGEConv(hidden_dim, hidden_dim)
            self.player_rel_convs.append(rel_convs)
            self.player_rel_logits.append(nn.Parameter(torch.zeros(len(self.player_relations))))

        self.team_attn_gate = nn.Linear(hidden_dim, 1)
        self.classifier = nn.Linear(hidden_dim * 4 + 6, 1)

    @staticmethod
    def _rel_to_key(rel):
        return f"{rel[0]}__{rel[1]}__{rel[2]}"

    def _get_has_history_edge_index(self, edge_index_dict):
        edge_key = ('player', 'has_history', 'history')
        rev_key = ('history', 'rev_has_history', 'player')
        if edge_key in edge_index_dict:
            return edge_index_dict[edge_key]
        if rev_key in edge_index_dict:
            rev = edge_index_dict[rev_key]
            return torch.stack([rev[1], rev[0]], dim=0)
        return None

    def _role_normalize_history_stats(self, history_raw):
        if history_raw.numel() == 0:
            return history_raw.new_zeros((0, 11))
        stats = history_raw[:, 4:15]
        roles = history_raw[:, 3].long().clamp_min(0)
        normalized = torch.zeros_like(stats)
        for role in roles.unique():
            mask = roles == role
            role_stats = stats[mask]
            mean = role_stats.mean(dim=0, keepdim=True)
            std = role_stats.std(dim=0, unbiased=False, keepdim=True).clamp_min(1e-6)
            normalized[mask] = (role_stats - mean) / std
        return normalized

    def _compute_player_champion_proxy(self, player_raw, history_raw, edge_index_dict):
        num_players = int(player_raw.size(0))
        device = player_raw.device
        wr = torch.full((num_players,), 0.5, device=device)
        games = torch.zeros((num_players,), device=device)

        has_history = self._get_has_history_edge_index(edge_index_dict)
        if has_history is None or history_raw is None or history_raw.numel() == 0:
            return wr, games

        p_idx = has_history[0].long()
        h_idx = has_history[1].long()
        valid = (h_idx >= 0) & (h_idx < history_raw.size(0)) & (p_idx >= 0) & (p_idx < num_players)
        if not valid.any():
            return wr, games

        p_idx = p_idx[valid]
        h_idx = h_idx[valid]
        player_champs = player_raw[:, 1].long().clamp_min(0)
        hist_champs = history_raw[:, 0].long().clamp_min(0)
        hist_wins = history_raw[:, 1].float().clamp(0.0, 1.0)

        same_champ = hist_champs[h_idx] == player_champs[p_idx]
        if not same_champ.any():
            return wr, games

        p_idx = p_idx[same_champ]
        h_idx = h_idx[same_champ]
        ones = torch.ones_like(p_idx, dtype=torch.float, device=device)
        wins = hist_wins[h_idx]

        games.index_add_(0, p_idx, ones)
        win_sum = torch.zeros((num_players,), device=device)
        win_sum.index_add_(0, p_idx, wins)

        global_wr = hist_wins.mean() if hist_wins.numel() > 0 else torch.tensor(0.5, device=device)
        prior_k = 3.0
        wr = (win_sum + prior_k * global_wr) / (games + prior_k)
        return wr, games

    def _team_attention_pool(self, player_h, team_ids, player_batch):
        num_graphs = int(player_batch.max().item()) + 1 if player_batch.numel() > 0 else 0
        hidden_dim = player_h.size(-1)
        device = player_h.device

        blue_vec = torch.zeros(num_graphs, hidden_dim, device=device)
        red_vec = torch.zeros(num_graphs, hidden_dim, device=device)

        for g in range(num_graphs):
            for team, out in ((0, blue_vec), (1, red_vec)):
                mask = (player_batch == g) & (team_ids == team)
                if not mask.any():
                    continue
                h = player_h[mask]
                logits = self.team_attn_gate(h).view(-1)
                weights = torch.softmax(logits, dim=0).unsqueeze(1)
                out[g] = (weights * h).sum(dim=0)
        return blue_vec, red_vec

    def _team_scalar_pool(self, values, team_ids, player_batch):
        num_graphs = int(player_batch.max().item()) + 1 if player_batch.numel() > 0 else 0
        device = values.device
        values = values.view(-1, 1)
        blue_sum = torch.zeros(num_graphs, 1, device=device)
        red_sum = torch.zeros(num_graphs, 1, device=device)
        blue_cnt = torch.zeros(num_graphs, 1, device=device)
        red_cnt = torch.zeros(num_graphs, 1, device=device)

        blue_mask = team_ids == 0
        red_mask = team_ids == 1
        if blue_mask.any():
            blue_sum.index_add_(0, player_batch[blue_mask], values[blue_mask])
            blue_cnt.index_add_(0, player_batch[blue_mask], torch.ones((blue_mask.sum(), 1), device=device))
        if red_mask.any():
            red_sum.index_add_(0, player_batch[red_mask], values[red_mask])
            red_cnt.index_add_(0, player_batch[red_mask], torch.ones((red_mask.sum(), 1), device=device))

        blue_mean = blue_sum / blue_cnt.clamp_min(1.0)
        red_mean = red_sum / red_cnt.clamp_min(1.0)
        return blue_mean, red_mean

    def _message_passing(self, x_dict, edge_index_dict):
        for layer_idx in range(self.num_layers):
            prev_player = x_dict['player']
            rel_convs = self.player_rel_convs[layer_idx]
            rel_weights = torch.softmax(self.player_rel_logits[layer_idx], dim=0)

            player_update = torch.zeros_like(prev_player)
            for ridx, rel in enumerate(self.player_relations):
                if rel not in edge_index_dict:
                    continue
                src_t, _, dst_t = rel
                if src_t not in x_dict or dst_t not in x_dict:
                    continue
                edge_index = edge_index_dict[rel]
                msg = rel_convs[self._rel_to_key(rel)]((x_dict[src_t], x_dict[dst_t]), edge_index)
                player_update = player_update + rel_weights[ridx] * msg

            x_dict['player'] = F.dropout(F.relu(player_update), p=self.dropout_rate, training=self.training)
        return x_dict

    def forward(self, x_dict, edge_index_dict, batch_dict):
        player_raw = x_dict['player']
        history_raw = x_dict.get('history', None)
        team_ids = player_raw[:, 0].long().clamp(0, 1)

        champ_wr, champ_games = self._compute_player_champion_proxy(player_raw, history_raw, edge_index_dict)

        x_dict['player'] = self.player_enc(player_raw)
        if history_raw is not None:
            history_norm = self._role_normalize_history_stats(history_raw)
            history_raw = torch.cat([history_raw, history_norm], dim=1)
            x_dict['history'] = self.history_enc(history_raw)

        x_dict = self._message_passing(x_dict, edge_index_dict)

        if 'player' not in x_dict:
            raise RuntimeError('Player features lost during convolution!')

        player_h = x_dict['player']
        player_batch = batch_dict['player']
        blue_h, red_h = self._team_attention_pool(player_h, team_ids, player_batch)

        h = torch.cat([blue_h, red_h, blue_h - red_h, torch.abs(blue_h - red_h)], dim=-1)
        blue_wr, red_wr = self._team_scalar_pool(champ_wr, team_ids, player_batch)
        blue_games, red_games = self._team_scalar_pool(champ_games, team_ids, player_batch)
        game_gap = blue_games - red_games
        wr_gap = blue_wr - red_wr
        team_extra = torch.cat([blue_wr, red_wr, blue_games, red_games, wr_gap, game_gap], dim=-1)
        h = torch.cat([h, team_extra], dim=-1)

        h = F.dropout(h, p=self.dropout_rate, training=self.training)
        return self.classifier(h)
