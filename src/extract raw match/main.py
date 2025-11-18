import time
from pathlib import Path
from Storage import Storage
import MatchCollector
import util

def main():
    # 출력 디렉토리 (시간 스탬프)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    out_dir = Path(f"GNN Project/data/{stamp}_matchdata")
    storage = Storage(out_dir)

    http = util.HttpClient()
    api = util.RiotAPI(http)
    collector = MatchCollector.MatchCollector(api, storage)

    # 입력 로드
    tokens = util.load_inputs(Path("GNN project/Data collecting/extract data v1_3/input.txt"))
    print(f"[INFO] 입력 {len(tokens)}건 처리 시작 → 저장 경로: {out_dir}")

    # 전체 수집
    total_saved = 0
    for tok in tokens:
        kind = util.classify_token(tok)
        try:
            if kind == "match_id":
                saved = collector.collect_from_match_id(tok, per_player=10)
            elif kind == "riot_id":
                saved = collector.collect_from_riot_id(tok, count=10)
            elif kind == "puuid":
                saved = collector.collect_from_puuid(tok, count=10)
            else:
                print(f"  [SKIP] 인식 불가: {tok}")
                continue
            total_saved += len(saved)
            print(f"  -> {tok} 처리 완료, 저장 {len(saved)}건 (누적 {total_saved})")
        except Exception as e:
            print(f"  [FAIL] {tok}: {e}")

    print(f"[DONE] 총 저장 매치 수: {total_saved} / 폴더: {out_dir.resolve()}")


if __name__ == "__main__":
    main()