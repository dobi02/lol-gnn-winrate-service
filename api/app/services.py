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


from .riot_client import RiotClient, RiotAPIError, parse_riot_id
from . import db


async def fetch_spectator_and_enrichment_from_riot_id(
    riot_id: str,
    platform_id: str = "KR",
    use_history: bool = True,
    history_count: int = 8,
) -> Tuple[dict, EnrichmentPayload, List[str]]:
    """
    Implements the procedure:
      1) Riot ID -> PUUID
      2) PUUID -> summonerId -> active game (spectator payload)
      3) DB cache lookup for participants
      4) Cache miss -> Riot API fetch, then upsert cache
    Returns:
      spectator_payload(dict), enrichment_payload(EnrichmentPayload), warnings(list)
    """
    warnings: List[str] = []
    client = RiotClient()

    game_name, tag_line = parse_riot_id(riot_id)

    # 1) Riot ID -> PUUID
    acct = client.get_account_by_riot_id(platform_id=platform_id, game_name=game_name, tag_line=tag_line)
    puuid = acct.get("puuid")
    if not puuid:
        raise RuntimeError("Failed to resolve puuid from riot_id")

    # 2) active game
    try:
        spectator_payload = client.get_active_game_by_puuid(
            platform_id=platform_id, puuid=puuid
        )
    except RiotAPIError as e:
        if e.status_code == 404:
            raise e  # caller will convert to 404 Not in game
        raise

    enrichment = EnrichmentPayload(
        use_history=bool(use_history),
        history_matches_by_puuid={}
    )

    if not use_history:
        return spectator_payload, enrichment, warnings

    # 3-4) per participant cache
    participants = spectator_payload.get("participants", []) or []

    for p in participants:
        p_puuid = p.get("puuid")
        if not p_puuid:
            warnings.append("participant missing puuid; skip enrichment fetch")
            continue

        try:
            target_match_ids = client.get_match_ids_by_puuid(
                platform_id=platform_id,
                puuid=p_puuid,
                count=history_count
            )

            existing_matches_map = db.get_matches_by_ids(target_match_ids)

            final_match_list = []

            for match_id in target_match_ids:
                if match_id in existing_matches_map:
                    # Case A: DB에 있음 -> DB 데이터 사용 (API 호출 X)
                    final_match_list.append(existing_matches_map[match_id])
                else:
                    # Case B: DB에 없음 -> API 호출 (Heavy Fetch) & DB 저장
                    try:
                        match_detail = client.get_match_by_id(
                            platform_id=platform_id,
                            match_id=match_id
                        )

                        # 가져온 데이터 리스트에 추가
                        final_match_list.append(match_detail)

                        # 나중을 위해 DB에 저장 (비동기 처리하면 더 좋음)
                        db.save_match(match_detail)

                    except Exception as e:
                        warnings.append(f"Failed to fetch missing match {match_id}: {e}")

            enrichment.history_matches_by_puuid[p_puuid] = final_match_list
        except Exception as e:
            warnings.append(f"history fetch failed for puuid={p_puuid[:8]}...: {type(e).__name__}")




        # Mastery cache (optional; requires summonerId)
        # mastery = db.get_mastery(p_puuid)
        # if mastery is None:
        #     # try get summonerId from spectator participant payload first
        #     p_summoner_id = p.get("summonerId")
        #     if not p_summoner_id:
        #         # resolve via summoner-v4 by puuid
        #         try:
        #             s = client.get_summoner_by_puuid(platform_id=platform_id, puuid=p_puuid)
        #             p_summoner_id = s.get("id")
        #         except Exception:
        #             p_summoner_id = None
        #
        #     if p_summoner_id:
        #         try:
        #             mastery_list = client.get_champion_mastery_by_puuid(platform_id=platform_id, puuid=p_puuid)
        #             # compress to championId -> championPoints
        #             mastery = {
        #                 "champion_points": {str(m.get("championId")): m.get("championPoints") for m in mastery_list if m.get("championId") is not None}
        #             }
        #             db.upsert_mastery(p_puuid, mastery)
        #         except Exception as e:
        #             warnings.append(f"mastery fetch failed for puuid={p_puuid[:8]}...: {type(e).__name__}")
        #             mastery = {"champion_points": {}}
        #     else:
        #         mastery = {"champion_points": {}}
        #
        # enrichment.champ_mastery_by_puuid[p_puuid] = mastery

    return spectator_payload, enrichment, warnings
