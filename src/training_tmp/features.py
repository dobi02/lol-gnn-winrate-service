import numpy as np
import torch


def symlog(x: float) -> float:
    if x == 0:
        return 0.0
    return float(np.sign(x) * np.log1p(np.abs(x)))


def log1p(x: float) -> float:
    return float(np.log1p(x))


def parse_all_player_features(match_json, participants_df):
    player_dim = 6

    json_participants = match_json.get("info", {}).get("participants", [])
    p_dict = {p.get("puuid"): p for p in json_participants}

    features_batch = []
    for _, row in participants_df.iterrows():
        target_puuid = row["puuid"]
        p_data = p_dict.get(target_puuid)

        if p_data is None:
            features_batch.append(torch.zeros(player_dim, dtype=torch.float))
            continue

        f = []
        f.append(0.0 if p_data.get("teamId") == 100 else 1.0)
        f.append(float(p_data.get("championId", 0)))
        f.append(float(p_data.get("summoner1Id", 0)))
        f.append(float(p_data.get("summoner2Id", 0)))

        perks = p_data.get("perks", {})
        styles = perks.get("styles", [])

        keystone_id = 0.0
        sub_style_id = 0.0

        if len(styles) > 0:
            selections = styles[0].get("selections", [])
            if len(selections) > 0:
                keystone_id = float(selections[0].get("perk", 0))

        if len(styles) > 1:
            sub_style_id = float(styles[1].get("style", 0))

        f.append(keystone_id)
        f.append(sub_style_id)

        if len(f) != player_dim:
            raise ValueError(f"Player feature dim mismatch: expected {player_dim}, got {len(f)}")

        features_batch.append(torch.tensor(f, dtype=torch.float))

    if len(features_batch) != len(participants_df):
        raise ValueError("Player batch size mismatch")

    return torch.stack(features_batch)


def parse_history_features(history_match_json, target_puuid):
    history_dim = 15

    position_map = {
        "TOP": 1.0,
        "JUNGLE": 2.0,
        "MIDDLE": 3.0,
        "BOTTOM": 4.0,
        "UTILITY": 5.0,
        "": 0.0,
        "NONE": 0.0,
    }

    info = history_match_json.get("info", {})
    if info.get("endOfGameResult") != "GameComplete":
        return None

    participants = info.get("participants", [])
    me = None
    for p in participants:
        if p.get("puuid") == target_puuid:
            me = p
            break

    if me is None:
        return None

    features = []
    features.append(float(me.get("championId", 0)))
    features.append(1.0 if me.get("win") else 0.0)
    features.append(0.0 if me.get("teamId") == 100 else 1.0)
    features.append(position_map.get(me.get("teamPosition", ""), 0.0))

    c = me.get("challenges", {})

    for key in [
        "earlyLaningPhaseGoldExpAdvantage",
        "laningPhaseGoldExpAdvantage",
        "maxCsAdvantageOnLaneOpponent",
        "maxLevelLeadLaneOpponent",
    ]:
        features.append(symlog(float(c.get(key, 0.0))))

    for key in ["goldPerMinute", "damagePerMinute", "visionScorePerMinute", "kda"]:
        features.append(log1p(max(0.0, float(c.get(key, 0.0)))))

    for key in ["teamDamagePercentage", "damageTakenOnTeamPercentage", "killParticipation"]:
        features.append(float(c.get(key, 0.0)))

    if len(features) != history_dim:
        raise ValueError(f"History feature dim mismatch: expected {history_dim}, got {len(features)}")

    return torch.tensor(features, dtype=torch.float)


def get_label(match_json):
    teams = match_json.get("info", {}).get("teams", [])
    if not teams:
        return torch.tensor([0.0], dtype=torch.float)
    blue_team_win = teams[0].get("win", False)
    return torch.tensor([1.0 if blue_team_win else 0.0], dtype=torch.float)
