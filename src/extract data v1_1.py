"""
Riot ID 목록(txt) → puuid → match/timeline 수집
기본 지표 + 추가적인 계산 지표

준비:
  - 같은 디렉토리에 summoners.txt (한 줄에 하나씩 Riot ID, 예: Hide on bush#KR1)
  - API_KEY 설정(아래 상수 또는 환경변수 사용)

출력:
  /lol_summary_with_pre15.csv (여러 소환사 데이터 누적 저장)
"""
## import os
import time
import requests
import pandas as pd
from pathlib import Path

# ---------------------- 설정 ----------------------
API_KEY = "RGAPI-213e8361-9ddf-4e77-95aa-a3518ef71ef7" # 직접 키 입력
## os.environ.get("RIOT_API_KEY") or "PUT_YOUR_API_KEY_HERE"  # API 키 환경변수 저장 시 사용 코드
PLATFORM = "kr"             # kr, na1, euw1 ...
REGIONAL = "asia"           # americas|europe|asia|sea
COUNT = 30                  # 소환사별 가져올 경기 수
QUEUE = 420                 # 420=솔랭, 440=자랭
PLAYERS_TXT = "GNN project/Data collecting/playerlist.txt"

OUTDIR = Path("data"); OUTDIR.mkdir(parents=True, exist_ok=True)
CSV_PATH = OUTDIR / "lol_extracted_data_GEN.csv"

if not API_KEY or API_KEY == "PUT_YOUR_API_KEY_HERE":
    raise SystemExit("환경변수 RIOT_API_KEY 또는 코드 상단의 API_KEY를 설정하세요.")

HEADERS = {"X-Riot-Token": API_KEY}
BASE = {
    "platform": lambda p: f"https://{p}.api.riotgames.com",
    "regional": lambda r: f"https://{r}.api.riotgames.com",
}

# ------------------- 요청 유틸(백오프) -------------------
def get_json(url, params=None, max_retries=4, sleep_base=1.0):
    for i in range(max_retries):
        r = requests.get(url, headers=HEADERS, params=params, timeout=30)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:  # rate limit
            retry_after = float(r.headers.get("Retry-After", "2"))
            time.sleep(retry_after)
            continue
        if r.status_code >= 500:
            time.sleep(sleep_base * (i + 1))
            continue
        raise RuntimeError(f"HTTP {r.status_code} {url} -> {r.text[:200]}")
    raise RuntimeError(f"Max retries exceeded for {url}")

# ------------------ 계정/매치 API -------------------
def get_puuid_by_riot_id(regional, game_name, tag_line):
    url = BASE["regional"](regional) + f"/riot/account/v1/accounts/by-riot-id/{requests.utils.quote(game_name)}/{requests.utils.quote(tag_line)}"
    return get_json(url)["puuid"]

def get_match_ids(regional, puuid, count=20, queue=None):
    params = {"count": count}
    if queue is not None:
        params["queue"] = queue
    url = BASE["regional"](regional) + f"/lol/match/v5/matches/by-puuid/{puuid}/ids"
    return get_json(url, params=params)

def get_match(regional, match_id):
    url = BASE["regional"](regional) + f"/lol/match/v5/matches/{match_id}"
    return get_json(url)

def get_timeline(regional, match_id):
    url = BASE["regional"](regional) + f"/lol/match/v5/matches/{match_id}/timeline"
    return get_json(url)

# ------------------ 보조 유틸 -------------------
def minutes_from_match(info):
    dur = info.get("gameDuration")
    if dur is None:
        start = info.get("gameStartTimestamp")
        end = info.get("gameEndTimestamp")
        dur = (end - start) / 1000.0 if (start and end) else 0
    return max(dur / 60.0, 1e-9)

def participant_id_from_puuid(match, puuid):
    arr = match["metadata"]["participants"]  # index0→pid=1
    return arr.index(puuid) + 1

def team_kills(info, team_id):
    return sum(p.get("kills", 0) for p in info["participants"] if p.get("teamId") == team_id)

def get_frame_closest_to_ms(timeline, target_ms):
    frames = timeline["info"]["frames"]
    return min(frames, key=lambda fr: abs(fr["timestamp"] - target_ms))

def get_first_frame(timeline):
    return timeline["info"]["frames"][0]

def get_last_frame(timeline):
    return timeline["info"]["frames"][-1]

def extract_cs_gold_xp_from_frame(frame, pid):
    pf = frame["participantFrames"][str(pid)]
    minions = pf.get("minionsKilled", 0) or 0
    jungle  = pf.get("jungleMinionsKilled", 0) or 0
    total_cs = minions + jungle
    total_gold = pf.get("totalGold", 0) or 0
    xp = pf.get("xp", 0) or 0
    return total_cs, total_gold, xp

def find_lane_opponent_participant(info, me_part):
    my_team = me_part.get("teamId")
    my_pos = (me_part.get("teamPosition")
              or me_part.get("individualPosition")
              or me_part.get("lane"))
    if not my_pos or my_pos == "NONE":
        return None
    enemies = [p for p in info["participants"] if p.get("teamId") != my_team]
    for p in enemies:
        pos = p.get("teamPosition") or p.get("individualPosition") or p.get("lane")
        if pos == my_pos:
            return p
    # 간단 alias
    alias = {
        "BOTTOM": {"BOTTOM", "BOT"},
        "BOT": {"BOTTOM", "BOT"},
        "UTILITY": {"UTILITY", "SUPPORT"},
        "SUPPORT": {"UTILITY", "SUPPORT"},
        "MIDDLE": {"MIDDLE", "MID"},
        "MID": {"MIDDLE", "MID"},
        "TOP": {"TOP"},
        "JUNGLE": {"JUNGLE"},
    }
    cand_set = alias.get(my_pos, {my_pos})
    for p in enemies:
        pos = p.get("teamPosition") or p.get("individualPosition") or p.get("lane")
        if pos in cand_set:
            return p
    return None

def xp_gain_total(timeline, pid):
    try:
        frames = timeline["info"]["frames"]
        first = frames[0]["participantFrames"][str(pid)]["xp"]
        last = frames[-1]["participantFrames"][str(pid)]["xp"]
        return max(last - first, 0)
    except Exception:
        return None

# ------------------ 0–15 및 라인 격차 -------------------
def compute_pre15_and_lane_diff(match, timeline, puuid):
    info = match["info"]
    me_part = next(p for p in info["participants"] if p["puuid"] == puuid)
    pid_me = participant_id_from_puuid(match, puuid)

    opp_part = find_lane_opponent_participant(info, me_part)
    pid_opp = participant_id_from_puuid(match, opp_part["puuid"]) if opp_part else None

    f0 = get_first_frame(timeline)
    f15 = get_frame_closest_to_ms(timeline, 15 * 60 * 1000)

    cs0, gold0, xp0 = extract_cs_gold_xp_from_frame(f0, pid_me)
    cs15, gold15, xp15 = extract_cs_gold_xp_from_frame(f15, pid_me)
    my_cs_15   = max(cs15 - cs0, 0)
    my_gold_15 = max(gold15 - gold0, 0)
    my_xp_15   = max(xp15 - xp0, 0)

    lane_diff_cs_15 = lane_diff_gold_15 = lane_diff_xp_15 = None
    if pid_opp:
        o_cs0, o_gold0, o_xp0 = extract_cs_gold_xp_from_frame(f0, pid_opp)
        o_cs15, o_gold15, o_xp15 = extract_cs_gold_xp_from_frame(f15, pid_opp)
        opp_cs_15   = max(o_cs15 - o_cs0, 0)
        opp_gold_15 = max(o_gold15 - o_gold0, 0)
        opp_xp_15   = max(o_xp15 - o_xp0, 0)
        lane_diff_cs_15   = my_cs_15   - opp_cs_15
        lane_diff_gold_15 = my_gold_15 - opp_gold_15
        lane_diff_xp_15   = my_xp_15   - opp_xp_15

    return {
        "CS_0_15": my_cs_15,
        "Gold_0_15": my_gold_15,
        "XP_0_15": my_xp_15,
        "CS_per_min_0_15": round(my_cs_15 / 15.0, 2),
        "Gold_per_min_0_15": round(my_gold_15 / 15.0, 1),
        "XP_per_min_0_15": round(my_xp_15 / 15.0, 1),
        "LaneDiff_CS_0_15": lane_diff_cs_15,
        "LaneDiff_Gold_0_15": lane_diff_gold_15,
        "LaneDiff_XP_0_15": lane_diff_xp_15,
    }

# ------------------ 게임 종료 시점 격차 -------------------
def compute_end_lane_diff(match, timeline, puuid):
    """종료 프레임의 총합 기준으로 CS/Gold/XP 상대 차이(내 - 상대)"""
    info = match["info"]
    me_part = next(p for p in info["participants"] if p["puuid"] == puuid)
    pid_me = participant_id_from_puuid(match, puuid)

    opp_part = find_lane_opponent_participant(info, me_part)
    if not opp_part:
        return {"LaneDiff_CS_End": None, "LaneDiff_Gold_End": None, "LaneDiff_XP_End": None}

    pid_opp = participant_id_from_puuid(match, opp_part["puuid"])
    fend = get_last_frame(timeline)

    my_cs_end, my_gold_end, my_xp_end = extract_cs_gold_xp_from_frame(fend, pid_me)
    op_cs_end, op_gold_end, op_xp_end = extract_cs_gold_xp_from_frame(fend, pid_opp)

    return {
        "LaneDiff_CS_End":   my_cs_end   - op_cs_end,
        "LaneDiff_Gold_End": my_gold_end - op_gold_end,
        "LaneDiff_XP_End":   my_xp_end   - op_xp_end,
    }

# ------------------ 한 경기 요약 -------------------
def compute_row_from_match(match, timeline, puuid, riot_id_for_csv):
    info = match["info"]; meta = match["metadata"]
    pid = participant_id_from_puuid(match, puuid)
    me = next(p for p in info["participants"] if p["puuid"] == puuid)
    mins = minutes_from_match(info)

    champ = me.get("championName")
    kills, deaths, assists = me.get("kills", 0), me.get("deaths", 0), me.get("assists", 0)
    kda_ratio = (kills + assists) / max(1, deaths)
    gold_earned = me.get("goldEarned", 0)

    cs = me.get("totalMinionsKilled", 0) + me.get("neutralMinionsKilled", 0)
    cs_per_min = cs / mins
    gpm = gold_earned / mins

    vision_score = me.get("visionScore", 0)
    vpm = vision_score / mins

    xp_total_gain = xp_gain_total(timeline, pid)
    xpm = (xp_total_gain / mins) if xp_total_gain is not None else None

    dmg_to_champs = me.get("totalDamageDealtToChampions", 0)
    dpm = dmg_to_champs / mins

    dmg_taken = me.get("totalDamageTaken", 0)
    dmg_per_gold = (dmg_to_champs / gold_earned) if gold_earned > 0 else None

    dur_sec = int(round(mins * 60))
    game_time_str = f"{dur_sec//60}:{dur_sec%60:02d}"

    t_kills = team_kills(info, me.get("teamId"))
    kp = ((kills + assists) / t_kills) * 100 if t_kills > 0 else None

    base = {
        "summoner": riot_id_for_csv,
        "matchId": meta.get("matchId"),
        "champion": champ,
        "gameTime": game_time_str,
        "win": me.get("win"),
        "kills": kills, "deaths": deaths, "assists": assists,
        "KDA_ratio": round(kda_ratio, 2),
        "gold_earned": gold_earned,
        "CS_total": cs,
        "CS_per_min": round(cs_per_min, 2),
        "G_per_min": round(gpm, 1),
        "XP_per_min": round(xpm, 1) if xpm is not None else None,
        "Vision_total": vision_score,
        "Vision_per_min": round(vpm, 2),
        "Damage_to_Champions": dmg_to_champs,
        "Damage_per_min": round(dpm, 1),
        "Damage_Taken": dmg_taken,
        "Damage_per_Gold": round(dmg_per_gold, 3) if dmg_per_gold is not None else None,
        "Kill_Participation_%": round(kp, 1) if kp is not None else None,
    }

    base.update(compute_pre15_and_lane_diff(match, timeline, puuid))
    base.update(compute_end_lane_diff(match, timeline, puuid))
    return base

# ------------------ 텍스트 파일에서 소환사 읽기 -------------------
def load_summoners_from_txt(path=PLAYERS_TXT):
    names = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "#" not in s:
                print(f"[WARN] Riot ID 형식 아님(건너뜀): {s}")
                continue
            names.append(s)
    if not names:
        raise SystemExit("summoners.txt에서 Riot ID를 찾지 못했습니다. 예) Hide on bush#KR1")
    return names

# ------------------ 소환사별 수집 -------------------
def collect_for_summoner(riot_id, count=COUNT, queue=QUEUE):
    game_name, tag_line = riot_id.split("#", 1)
    puuid = get_puuid_by_riot_id(REGIONAL, game_name, tag_line)
    mids = get_match_ids(REGIONAL, puuid, count=count, queue=queue)

    rows = []
    for mid in mids:
        try:
            match = get_match(REGIONAL, mid)
            timeline = get_timeline(REGIONAL, mid)
            rows.append(compute_row_from_match(match, timeline, puuid, riot_id_for_csv=riot_id))
        except Exception as e:
            rows.append({"summoner": riot_id, "matchId": mid, "error": str(e)})
    return rows

# ------------------ 메인 -------------------
def main_from_txt():
    summoners = load_summoners_from_txt(PLAYERS_TXT)
    all_rows = []
    for rid in summoners:
        print(f"[INFO] Collecting for {rid} ...")
        all_rows.extend(collect_for_summoner(rid, count=COUNT, queue=QUEUE))

    df = pd.DataFrame(all_rows)
    df.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")
    print(f"[OK] Saved -> {CSV_PATH}")
    return df

if __name__ == "__main__":
    main_from_txt()
