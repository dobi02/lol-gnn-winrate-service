import torch
from torch_geometric.data import HeteroData
# [수정 1] import 이름 변경 (parse_player_features -> parse_all_player_features)
from features import parse_all_player_features, parse_history_features, get_label

class GraphFactory:
    def __init__(self, config, data_loader):
        self.config = config
        self.db = data_loader
        self.history_limit = (
            config.get('history_limit')
            or config.get('data_dims', {}).get('history_limit')
            or 8
        )

    def create_graph(self, root_match_row):
        match_id = root_match_row['match_id']
        match_json = root_match_row['match_json']
        game_time = root_match_row['game_start_at']
        
        # 참가자 정보 조회
        participants_df = self.db.get_participants(match_id)
        
        # 10명이 아니면 스킵 (데이터 정합성)
        if len(participants_df) != 10:
            return None

        data = HeteroData()

        # ==============================================================================
        # 1. Player Node 생성 (Batch Processing)
        # ==============================================================================
        data['player'].x = parse_all_player_features(match_json, participants_df)

        # ==============================================================================
        # 2. History Node 생성 & 연결 (개별 처리)
        # ==============================================================================
        all_history_features = []
        edge_src, edge_dst = [], [] # Player -> History
        global_history_idx = 0
        
        for player_idx, row in participants_df.iterrows():
            target_puuid = row['puuid']
            
            # DB 조회
            histories = self.db.get_player_history(target_puuid, game_time, limit=self.history_limit)
            
            for h_json in histories:
                # History 파싱
                h_feat = parse_history_features(h_json, target_puuid)
                
                # 파싱 실패(None) 시 건너뜀
                if h_feat is None:
                    continue
                
                all_history_features.append(h_feat)
                
                # Edge 연결 (현재 플레이어 인덱스 -> 현재 히스토리 인덱스)
                edge_src.append(player_idx)
                edge_dst.append(global_history_idx)
                global_history_idx += 1

        # History Tensor 할당
        if all_history_features:
            data['history'].x = torch.stack(all_history_features)
            data['player', 'has_history', 'history'].edge_index = torch.tensor(
                [edge_src, edge_dst], dtype=torch.long
            )
        else:
            # 전적이 하나도 없는 경우 (Empty Tensor) - shape 맞추기 주의
            # features.py의 HISTORY_DIM과 맞춰야 함 (15)
            HISTORY_DIM = 15 
            data['history'].x = torch.empty((0, HISTORY_DIM), dtype=torch.float)
            data['player', 'has_history', 'history'].edge_index = torch.empty((2, 0), dtype=torch.long)

        # 3. 팀/적 관계 설정
        self._build_player_relationships(data, participants_df)
        
        # 4. Label
        data.y = get_label(match_json)
        
        return data

    def _build_player_relationships(self, data, participants_df):
        """플레이어 간의 팀/적 관계 엣지 생성 함수"""
        team_100_idx = participants_df[participants_df['team_id'] == 100].index.tolist()
        team_200_idx = participants_df[participants_df['team_id'] == 200].index.tolist()
        
        team_src, team_dst = [], []
        enemy_src, enemy_dst = [], []

        # Teammate Edges
        for team_indices in [team_100_idx, team_200_idx]:
            for i in team_indices:
                for j in team_indices:
                    if i != j:
                        team_src.append(i)
                        team_dst.append(j)
        
        # Enemy Edges
        for i in team_100_idx:
            for j in team_200_idx:
                enemy_src.append(i)
                enemy_dst.append(j)
                enemy_src.append(j)
                enemy_dst.append(i)

        data['player', 'teammate', 'player'].edge_index = torch.tensor([team_src, team_dst], dtype=torch.long)
        data['player', 'enemy', 'player'].edge_index = torch.tensor([enemy_src, enemy_dst], dtype=torch.long)
