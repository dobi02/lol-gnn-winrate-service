from __future__ import annotations

from typing import Any, Dict, Optional

# NOTE:
# This is a SERVING-oriented, single-sample (atomic) preprocessing skeleton.
# Replace the TODO sections with the DS team's exact feature engineering logic
# using config.json feature ordering to avoid training-serving skew.

def convert_spectator_to_graph(
    spectator_payload: Dict[str, Any],
    enrichment_payload: Optional[Dict[str, Any]] = None,
):
    """
    [Serving Essential]
    Convert Riot Spectator (current game) JSON into a single model input object.

    Args:
        spectator_payload: spectator JSON (as dict)
        enrichment_payload: optional dict containing history/mastery etc.
    Returns:
        graph_object: either torch_geometric.data.Data (preferred) or a dict placeholder.
    """
    # Try to build a PyG graph if torch_geometric is installed
    try:
        import torch
        from torch_geometric.data import Data  # type: ignore
    except Exception:
        # Fallback: return a plain dict so the API can still run in mock mode
        return {
            "type": "graph_placeholder",
            "spectator": spectator_payload,
            "enrichment": enrichment_payload or {},
        }

    participants = spectator_payload.get("participants", [])
    banned = spectator_payload.get("bannedChampions", []) or []

    # TODO(DS): build node features according to config.json (ordered list)
    # For now, minimal node feature: [championId, spell1Id, spell2Id, perkStyle, perkSubStyle]
    x_list = []
    for p in participants:
        perks = p.get("perks") or {}
        x_list.append([
            float(p.get("championId", 0)),
            float(p.get("spell1Id", 0)),
            float(p.get("spell2Id", 0)),
            float(perks.get("perkStyle", 0)),
            float(perks.get("perkSubStyle", 0)),
        ])
    if not x_list:
        # Avoid empty tensor crashes
        x_list = [[0.0, 0.0, 0.0, 0.0, 0.0]]

    x = torch.tensor(x_list, dtype=torch.float)

    # TODO(DS): define edges (team graph / synergy edges / lane edges, etc.)
    # Skeleton: fully connect within each team (undirected)
    team_ids = [p.get("teamId", 0) for p in participants]
    edges_src = []
    edges_dst = []
    for i in range(len(team_ids)):
        for j in range(len(team_ids)):
            if i == j:
                continue
            if team_ids[i] == team_ids[j] and team_ids[i] in (100, 200):
                edges_src.append(i)
                edges_dst.append(j)

    if edges_src:
        edge_index = torch.tensor([edges_src, edges_dst], dtype=torch.long)
    else:
        edge_index = torch.empty((2, 0), dtype=torch.long)

    # Optional global features
    game_queue = float(spectator_payload.get("gameQueueConfigId", 0))
    platform_id = spectator_payload.get("platformId", "")
    # Encode platform_id as hash for now (TODO: use vocab in config)
    platform_hash = float(abs(hash(platform_id)) % 10_000)

    u = torch.tensor([game_queue, platform_hash], dtype=torch.float)

    graph = Data(x=x, edge_index=edge_index)
    graph.u = u  # custom attribute (PyG allows)
    graph.num_nodes = x.size(0)
    return graph
