# app/preprocessing.py

import torch
import math
from typing import Any, Dict, List, Optional

# torch_geometric이 설치된 환경이어야 함
try:
    from torch_geometric.data import HeteroData
except ImportError:
    # Mock 환경 등을 위해 임시 처리
    HeteroData = Any


# ==============================================================================
# 1. Math Helpers (User provided)
# ==============================================================================
def symlog(x: float) -> float:
    if x > 0:
        return math.log1p(x)
    elif x < 0:
        return -math.log1p(-x)
    return 0.0


def log1p(x: float) -> float:
    return math.log1p(x)


# ==============================================================================
# 2. Parsing Functions (User provided & Adapted)
# ==============================================================================

def parse_spectator_player_features(spectator_participants: List[Dict[str, Any]]) -> torch.Tensor:
    """
    [Node Type 1: Player]
    Spectator V5 JSON의 participants 리스트를 받아 (10, 6) 텐서 반환.
    DataFrame 의존성을 제거하고 리스트를 직접 순회합니다.
    """
    PLAYER_DIM = 6
    features_batch = []

    # Spectator V5에서 participants는 10명의 리스트로 옵니다.
    # 순서는 [Blue 1~5, Red 1~5]로 보장된다고 가정합니다.

    for p_data in spectator_participants:
        f = []

        # [0] Team (100 -> 0.0, 200 -> 1.0)
        # Spectator JSON은 teamId가 int형
        f.append(0.0 if p_data.get('teamId') == 100 else 1.0)

        # [1] Champion ID
        f.append(float(p_data.get('championId', 0)))

        # [2, 3] Summoner Spells (spell1Id, spell2Id)
        f.append(float(p_data.get('spell1Id', 0)))
        f.append(float(p_data.get('spell2Id', 0)))

        # [4, 5] Runes (Perks)
        perks = p_data.get('perks', {})
        perk_ids = perks.get('perkIds', [])

        keystone_id = 0.0
        # Main Rune은 보통 첫 번째
        if perk_ids and len(perk_ids) > 0:
            keystone_id = float(perk_ids[0])

        sub_style_id = float(perks.get('perkSubStyle', 0))

        f.append(keystone_id)
        f.append(sub_style_id)

        if len(f) != PLAYER_DIM:
            # 안전장치: 차원이 안 맞으면 0으로 채움 (서빙 에러 방지)
            f = f[:PLAYER_DIM] + [0.0] * (PLAYER_DIM - len(f))

        features_batch.append(f)

    if not features_batch:
        return torch.zeros((0, PLAYER_DIM), dtype=torch.float)

    return torch.tensor(features_batch, dtype=torch.float)


def parse_history_features(history_match_json: Dict[str, Any], target_puuid: str) -> Optional[List[float]]:
    """
    [Node Type 2: History] Match V5 JSON 파싱
    """
    HISTORY_DIM = 15
    POSITION_MAP = {
        'TOP': 1.0, 'JUNGLE': 2.0, 'MIDDLE': 3.0, 'BOTTOM': 4.0, 'UTILITY': 5.0,
        '': 0.0, 'NONE': 0.0
    }

    info = history_match_json.get('info', {})

    # 1. 게임 정상 종료 여부 확인 (서빙 시에는 데이터 확보를 위해 조건 완화 고려 가능)
    if info.get('endOfGameResult') != 'GameComplete':
        return None

    # 2. 타겟 플레이어 찾기
    participants = info.get('participants', [])
    me = None
    for p in participants:
        if p.get('puuid') == target_puuid:
            me = p
            break

    if me is None:
        return None

    # 3. Feature Extraction
    features = []

    # [0-3] Meta
    features.append(float(me.get('championId', 0)))
    features.append(1.0 if me.get('win') else 0.0)
    features.append(0.0 if me.get('teamId') == 100 else 1.0)
    features.append(POSITION_MAP.get(me.get('teamPosition', ''), 0.0))

    # [4-14] Stats
    c = me.get('challenges', {})

    # Group A: Symlog
    diff_stats = [
        'earlyLaningPhaseGoldExpAdvantage', 'laningPhaseGoldExpAdvantage',
        'maxCsAdvantageOnLaneOpponent', 'maxLevelLeadLaneOpponent'
    ]
    for key in diff_stats:
        features.append(symlog(float(c.get(key, 0.0))))

    # Group B: Log1p
    large_positive_stats = [
        'goldPerMinute', 'damagePerMinute', 'visionScorePerMinute', 'kda'
    ]
    for key in large_positive_stats:
        val = float(c.get(key, 0.0))
        # API 구조상 kda가 challenges 밖(top-level)에 있을 수도 있음. 방어 로직.
        if key == 'kda' and val == 0.0 and 'kda' in me:
            val = float(me['kda'])
        features.append(log1p(max(0.0, val)))

    # Group C: Raw
    percentage_stats = [
        'teamDamagePercentage', 'damageTakenOnTeamPercentage', 'killParticipation'
    ]
    for key in percentage_stats:
        features.append(float(c.get(key, 0.0)))

    if len(features) != HISTORY_DIM:
        return None  # 차원 안맞으면 버림

    return features


# ==============================================================================
# 3. Graph Builder (Entry Point)
# ==============================================================================

def convert_spectator_to_graph(
        spectator_payload: Dict[str, Any],
        enrichment_payload: Optional[Dict[str, Any]] = None,
) -> HeteroData:
    """
    [서빙용 진입점]
    API에서 받은 JSON들을 조립하여 GNN 입력용 HeteroData 생성
    """
    data = HeteroData()
    participants = spectator_payload.get('participants', [])

    # ---------------------------------------------------------
    # 1. Player Node 생성
    # ---------------------------------------------------------
    data['player'].x = parse_spectator_player_features(participants)

    # ---------------------------------------------------------
    # 2. History Node 생성 & 연결
    # ---------------------------------------------------------
    all_history_features = []
    edge_src, edge_dst = [], []  # Player -> History
    global_history_idx = 0

    # enrichment에서 전적 데이터 가져오기
    # 구조: enrichment['history_matches_by_puuid'][puuid] = [MatchJSON1, MatchJSON2...]
    history_map = {}
    if enrichment_payload:
        history_map = enrichment_payload.get('history_matches_by_puuid', {})

    for p_idx, p_data in enumerate(participants):
        puuid = p_data.get('puuid')
        if not puuid:
            continue

        # 해당 유저의 과거 매치 리스트 (최대 20개 등 설정값에 따름)
        histories = history_map.get(puuid, [])
        if not isinstance(histories, list):
            histories = []

        for h_json in histories:
            # JSON이 아니면(ID 스트링이면) 스킵
            if not isinstance(h_json, dict):
                continue

            h_feat = parse_history_features(h_json, puuid)

            if h_feat is not None:
                all_history_features.append(h_feat)

                # Edge 연결
                edge_src.append(p_idx)
                edge_dst.append(global_history_idx)
                global_history_idx += 1

    # History Tensor 할당
    HISTORY_DIM = 15
    if all_history_features:
        data['history'].x = torch.tensor(all_history_features, dtype=torch.float)
        data['player', 'has_history', 'history'].edge_index = torch.tensor(
            [edge_src, edge_dst], dtype=torch.long
        )
    else:
        data['history'].x = torch.empty((0, HISTORY_DIM), dtype=torch.float)
        data['player', 'has_history', 'history'].edge_index = torch.empty((2, 0), dtype=torch.long)

    # ---------------------------------------------------------
    # 3. 관계(Edge) 설정 - GraphFactory 로직 이식
    # ---------------------------------------------------------
    team_100_idx = [i for i, p in enumerate(participants) if p.get('teamId') == 100]
    team_200_idx = [i for i, p in enumerate(participants) if p.get('teamId') == 200]

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
            enemy_src.append(j)  # 양방향
            enemy_dst.append(i)

    # Tensor 할당 (Empty 체크)
    if team_src:
        data['player', 'teammate', 'player'].edge_index = torch.tensor([team_src, team_dst], dtype=torch.long)
    else:
        data['player', 'teammate', 'player'].edge_index = torch.empty((2, 0), dtype=torch.long)

    if enemy_src:
        data['player', 'enemy', 'player'].edge_index = torch.tensor([enemy_src, enemy_dst], dtype=torch.long)
    else:
        data['player', 'enemy', 'player'].edge_index = torch.empty((2, 0), dtype=torch.long)

    return data