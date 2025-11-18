import re
from pathlib import Path
from typing import List
import time
import setting
from typing import Any, Dict, Optional
import requests

RIOT_ID_RE = re.compile(r"^(.+?)#([A-Za-z0-9]+)$")
MATCH_ID_RE = re.compile(r"^[A-Z0-9]+_\d+$")
PUUID_RE    = re.compile(r"^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$", re.I)

class HttpClient:
    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None, max_retries: int = 6) -> Any:
        for i in range(max_retries):
            r = requests.get(url, headers=setting.HEADERS, params=params, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 404:
                # 없으면 None으로 반환해 상위에서 건너뛰기
                return None
            if r.status_code == 429:
                retry = float(r.headers.get("Retry-After", "2"))
                time.sleep(retry)
                continue
            if 500 <= r.status_code < 600:
                time.sleep(1.5 ** (i + 1))
                continue
            # 나머지(401/403 등)는 즉시 예외
            raise RuntimeError(f"HTTP {r.status_code} {url}: {r.text[:200]}")
        raise RuntimeError(f"Max retries exceeded: {url}")
    
class RiotAPI:
    def __init__(self, http: HttpClient):
        self.http = http

    # ---- 계정/식별자
    def account_by_riot_id(self, game_name: str, tag_line: str) -> Optional[Dict[str, Any]]:
        url = f"{setting.BASE_REGIONAL}/riot/account/v1/accounts/by-riot-id/{requests.utils.quote(game_name)}/{requests.utils.quote(tag_line)}"
        return self.http.get_json(url)

    # ---- 매치 목록/상세
    def match_ids_by_puuid(
        self, puuid: str, count: int = 10, queue: Optional[int] = setting.QUEUE_SOLO
    ) -> List[str]:
        url = f"{setting.BASE_REGIONAL}/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params: Dict[str, Any] = {"count": count}
        if queue is not None:
            params["queue"] = queue
        ids = self.http.get_json(url, params=params)
        return ids or []

    def match_by_id(self, match_id: str) -> Optional[Dict[str, Any]]:
        url = f"{setting.BASE_REGIONAL}/lol/match/v5/matches/{match_id}"
        return self.http.get_json(url)
        
def load_inputs(path: Path) -> List[str]:
    lines: List[str] = []
    if not path.exists():
        raise FileNotFoundError(f"입력 파일이 없습니다: {path}")
    for raw in path.read_text(encoding="utf-8").splitlines():
        s = raw.strip()
        if not s or s.startswith("#"):
            continue
        lines.append(s)
    return lines

def classify_token(token: str) -> str:
    if MATCH_ID_RE.match(token):
        return "match_id"
    if RIOT_ID_RE.match(token):
        return "riot_id"
    if PUUID_RE.match(token):
        return "puuid"
    return "unknown"