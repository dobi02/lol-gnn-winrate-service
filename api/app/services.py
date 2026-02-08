from __future__ import annotations

from typing import List, Tuple, Optional

from .schemas import SpectatorGame, EnrichmentPayload
from .preprocessing import convert_spectator_to_graph


async def build_model_input_from_spectator(
    spectator: SpectatorGame,
    enrichment: Optional[EnrichmentPayload] = None,
) -> Tuple[object, List[str]]:
    """
    Convert Riot spectator payload into a single model input object (Graph/Data).

    Returns:
        (graph_obj, warnings)
    """
    warnings: List[str] = []

    # Detect missing puuid cases (common in some spectator payloads)
    missing = [p.championId for p in spectator.participants if not p.puuid]
    if missing:
        warnings.append(
            f"{len(missing)} participant(s) missing puuid; history/mastery features will be defaulted."
        )

    graph_obj = convert_spectator_to_graph(
        spectator_payload=spectator.model_dump() if hasattr(spectator, "model_dump") else spectator.dict(),
        enrichment_payload=(enrichment.model_dump() if (enrichment and hasattr(enrichment, "model_dump")) else (enrichment.dict() if enrichment else None)),
    )
    return graph_obj, warnings
