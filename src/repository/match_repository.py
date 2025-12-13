from typing import List, Dict
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

class MatchRepository:
    def __init__(self, conn_id: str):
        self.pg = PostgresHook(postgres_conn_id=conn_id)

    def upsert_summoner(self, puuid: str, summoner_name: str | None = None, summoner_id: str | None = None):
        sql = """
        INSERT INTO summoners (puuid, summoner_id, summoner_name)
        VALUES (%s, %s, %s)
        ON CONFLICT (puuid) DO UPDATE
        SET summoner_id   = COALESCE(EXCLUDED.summoner_id, summoners.summoner_id),
            summoner_name = COALESCE(EXCLUDED.summoner_name, summoners.summoner_name);
        """
        self.pg.run(sql, parameters=(puuid, summoner_id, summoner_name))

    def upsert_match(self, match_json: Dict, is_root: bool):
        match_id = match_json["metadata"]["matchId"]
        queue_id = match_json["info"].get("queueId", 0)

        sql = """
        INSERT INTO matches (match_id, queue_id, match_json, is_root)
        VALUES (%s, %s, %s::jsonb, %s)
        ON CONFLICT (match_id) DO UPDATE
        SET queue_id   = EXCLUDED.queue_id,
            match_json = EXCLUDED.match_json,
            is_root    = matches.is_root OR EXCLUDED.is_root;
        """
        self.pg.run(sql, parameters=(match_id, queue_id, Json(match_json), is_root))

    def upsert_participants(self, match_json: Dict):
        match_id = match_json["metadata"]["matchId"]
        participants = match_json["info"]["participants"]

        rows = []
        for p in participants:
            puuid = p["puuid"]
            participant_no = p["participantId"]
            team_id = p["teamId"]
            champion_id = p["championId"]

            rows.append(
                (match_id, puuid, participant_no, team_id, champion_id)
            )

            # summoners 테이블도 같이 upsert (내부 메서드 호출)
            self.upsert_summoner(
                puuid=puuid,
                summoner_name=p.get("summonerName"),
                summoner_id=p.get("summonerId"),
            )

        self.pg.insert_rows(
            table="participants",
            rows=rows,
            target_fields=[
                "match_id",
                "puuid",
                "participant_no",
                "team_id",
                "champion_id",
            ],
            commit_every=50,
            replace=True,
            replace_index=["match_id", "puuid"],
        )

    def upsert_masteries(self, puuid: str, mastery_list: List[Dict]):
        """
        mastery_list: Riot API champion-mastery-v4 결과(JSON 배열)
        """
        rows = []
        for m in mastery_list:
            rows.append(
                (
                    puuid,
                    m["championId"],
                    m.get("championLevel"),
                    m.get("championPoints"),
                    m.get("tokensEarned", 0),
                )
            )

        if not rows:
            return

        sql = """
        INSERT INTO champion_masteries (
          puuid, champion_id, champion_level,
          champion_points, tokens_earned
        )
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (puuid, champion_id) DO UPDATE
        SET champion_level  = EXCLUDED.champion_level,
            champion_points = EXCLUDED.champion_points,
            tokens_earned   = EXCLUDED.tokens_earned;
        """
        conn = self.pg.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.executemany(sql, rows)

    def get_existing_match_ids(self, match_ids: List[str]) -> set[str]:
        """
        이미 DB에 저장된 match_id들을 반환
        """
        if not match_ids:
            return set()

        # match_ids 리스트를 PostgreSQL array로 변환
        match_ids_array = match_ids

        sql = """
        SELECT match_id
        FROM matches
        WHERE match_id = ANY(%s)
        """

        conn = self.pg.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, (match_ids_array,))
                existing_ids = {row[0] for row in cur.fetchall()}
                return existing_ids

    def get_random_non_root_match(self) -> str | None:
        """
        DB에서 is_root=False인 매치 중 하나를 랜덤하게 반환
        """
        sql = """
        SELECT match_id
        FROM matches
        WHERE is_root = false
        ORDER BY RANDOM()
        LIMIT 1
        """

        conn = self.pg.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                result = cur.fetchone()
                return result[0] if result else None

    def get_recent_non_root_matches(self, limit: int = 20) -> str | None:
        """
        DB에서 is_root=False인 매치 중 가장 최신 매치(game_start_at 기준)를 반환
        """
        sql = f"""
        SELECT match_id
        FROM matches
        WHERE is_root = false
        ORDER BY game_start_at DESC
        LIMIT {limit}
        """

        conn = self.pg.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                results = cur.fetchall()
                return [row[0] for row in results] if results else []

    def get_latest_non_root_match(self) -> str | None:
        """
        DB에서 is_root=False인 매치 중 가장 최신 매치 하나를 반환
        """
        sql = """
        SELECT match_id
        FROM matches
        WHERE is_root = false
        ORDER BY game_start_at DESC
        LIMIT 1
        """

        conn = self.pg.get_conn()
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                result = cur.fetchone()
                return result[0] if result else None