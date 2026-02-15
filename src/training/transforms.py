# transforms.py
import torch
import random

class StreamerModeTransform:
    def __init__(self, max_players=10):
        """
        스트리머 모드 시뮬레이션:
        랜덤하게 N명(0~10명)의 플레이어를 선택하여, 그들의 과거 전적 연결(Edge)을 제거합니다.
        """
        self.max_players = max_players

    def __call__(self, data):
        # 1. 몇 명을 가릴지 결정 (0명 ~ 10명 랜덤)
        num_to_mask = random.randint(0, self.max_players)
        
        if num_to_mask == 0:
            return data

        # 2. 가릴 플레이어 인덱스 랜덤 선택 (예: {0, 3, 9})
        players_to_mask = torch.tensor(
            random.sample(range(self.max_players), num_to_mask), 
            dtype=torch.long
        )

        # 3. 엣지 필터링 ('player' -> 'history')
        # 기존 엣지: [2, num_edges] (Row 0: Player Index, Row 1: History Index)
        edge_type = ('player', 'has_history', 'history')
        
        if edge_type in data.edge_index_dict:
            edge_index = data[edge_type].edge_index
            src_nodes = edge_index[0] # 출발점(Player Index)
            
            # 마스킹 대상에 포함되지 않는 엣지만 True (살려둘 엣지)
            # torch.isin을 사용하여 벡터 연산 (빠름)
            # ~ (Not) 연산으로 마스킹 된 애들을 False로 만듦
            mask = ~torch.isin(src_nodes, players_to_mask)
            
            # 엣지 갱신 (선택된 플레이어의 엣지는 사라짐)
            data[edge_type].edge_index = edge_index[:, mask]

        return data