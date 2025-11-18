# collecotr/storage.py
from __future__ import annotations
from pathlib import Path
from typing import Dict, Any, Set
import json


class Storage:
    """
    매치 한 개당 JSON 파일 1개씩 저장.
    파일명: <match_id>.json
    """

    def __init__(self, out_dir: Path):
        self.out_dir = out_dir
        self.out_dir.mkdir(parents=True, exist_ok=True)
        self._seen: Set[str] = set()

    def has(self, match_id: str) -> bool:
        if match_id in self._seen:
            return True
        path = self.out_dir / f"{match_id}.json"
        if path.exists():
            self._seen.add(match_id)
            return True
        return False

    def save_match(self, match_id: str, data: Dict[str, Any]) -> None:
        if self.has(match_id):
            return
        path = self.out_dir / f"{match_id}.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self._seen.add(match_id)