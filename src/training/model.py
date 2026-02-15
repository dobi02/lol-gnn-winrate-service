# model.py
import torch
import torch.nn.functional as F
from torch_geometric.nn import SAGEConv, HeteroConv, global_mean_pool
from model_utils import CategoricalEncoder
# [수정 1] config.py 직접 import 삭제 (의존성 제거)
# from config import CONFIG 


class LoLGNN(torch.nn.Module):
    def __init__(self, config):
        """
        Args:
            config (dict): config.json에서 로드된 설정 딕셔너리
        """
        super().__init__()
        self.config = config
        
        # [수정 2] JSON 구조에 맞춰 파라미터 추출
        # config['train']['hidden_channels'] 
        hidden_dim = config['train']['hidden_channels']
        
        # config['data_dims']['player_dim']
        player_dim = config['data_dims']['player_dim']
        
        # config['data_dims']['history_dim']
        history_dim = config['data_dims']['history_dim']

        # config['emb_configs'] (JSON 로드 시 String Key "1", "2"...)
        emb_conf = config['emb_configs']
        
        # -----------------------------------------------------------
        # 1. Player Node Encoder
        # -----------------------------------------------------------
        self.player_enc = CategoricalEncoder(
            input_dim=player_dim,
            cat_indices=[1, 2, 3, 4, 5],
            embedding_configs=emb_conf, # JSON Config 그대로 전달 (model_utils에서 처리함)
            out_channels=hidden_dim
        )
        
        # -----------------------------------------------------------
        # 2. History Node Encoder
        # -----------------------------------------------------------
        # History 노드의 임베딩 설정은 보통 고정값이거나 Player와 공유
        # 여기서는 원본 코드의 하드코딩 방식을 유지하되, 안전하게 int key 사용
        history_emb_conf = {
            0: (2000, 16), # Champion
            3: (10, 4)     # Position
        }
        
        self.history_enc = CategoricalEncoder(
            input_dim=history_dim,
            cat_indices=[0, 3],
            embedding_configs=history_emb_conf,
            out_channels=hidden_dim
        )

        # -----------------------------------------------------------
        # 3. GNN Layers
        # -----------------------------------------------------------
        self.convs = torch.nn.ModuleList()
        for _ in range(2): # 2 Layers
            conv = HeteroConv({
                ('player', 'teammate', 'player'): SAGEConv(hidden_dim, hidden_dim),
                ('player', 'enemy', 'player'): SAGEConv(hidden_dim, hidden_dim),
                ('history', 'rev_has_history', 'player'): SAGEConv(hidden_dim, hidden_dim),
            }, aggr='sum')
            self.convs.append(conv)

        self.classifier = torch.nn.Linear(hidden_dim, 1)

    def forward(self, x_dict, edge_index_dict, batch_dict):
        # 1. Encoding
        x_dict['player'] = self.player_enc(x_dict['player'])
        
        if 'history' in x_dict:
            x_dict['history'] = self.history_enc(x_dict['history'])
        
        # 2. Message Passing
        for conv in self.convs:
            out_dict = conv(x_dict, edge_index_dict)
            
            # History 노드 보존 (업데이트 안 됨)
            if 'history' in x_dict:
                out_dict['history'] = x_dict['history']
            
            x_dict = out_dict
            
            # Activation (Player만)
            if 'player' in x_dict:
                x_dict['player'] = F.relu(x_dict['player'])
        
        # 3. Readout
        if 'player' not in x_dict:
             raise RuntimeError("Player features lost during convolution!")

        h = global_mean_pool(x_dict['player'], batch_dict['player'])
        
        return self.classifier(h)