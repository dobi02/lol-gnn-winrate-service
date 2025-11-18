from typing import Any, Dict
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Set

@dataclass
class SaveContext:
    out_dir: Path
    seen_ids: Set[str]  # 이미 저장한 매치ID 중복 방지

class Storage:
    def __init__(self, out_dir: Path):
        self.ctx = SaveContext(out_dir=out_dir, seen_ids=set())
        self.ctx.out_dir.mkdir(parents=True, exist_ok=True)

    def has(self, match_id: str) -> bool:
        return (self.ctx.out_dir / f"{match_id}.json").exists() or (match_id in self.ctx.seen_ids)

    def save_match(self, match_id: str, data: Dict[str, Any]) -> None:
        if self.has(match_id):
            return
        path = self.ctx.out_dir / f"{match_id}.json"
        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        self.ctx.seen_ids.add(match_id)