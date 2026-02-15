import os
import json
from sqlalchemy import create_engine, text
import pandas as pd

class DataLoader:
    def __init__(self):
        user = os.getenv('DB_USER')
        password = os.getenv('DB_PASS')
        host = os.getenv('DB_HOST', 'data-postgres')
        port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'data_db')

        db_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        self.engine = create_engine(db_url)
        print(f"✅ Connected to {host}:{port}/{db_name}")

    def get_root_matches(self, start_date, end_date):
        """학습 대상이 되는 Root Match 목록을 가져옵니다."""
        query = text("""
            SELECT match_id, match_json, game_start_at
            FROM public.matches
            WHERE is_root = true 
              AND game_start_at BETWEEN :start_date AND :end_date
        """)
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"start_date": start_date, "end_date": end_date})

    def get_participants(self, match_id):
        """특정 매치의 참가자 정보를 가져옵니다."""
        query = text("""
            SELECT p.puuid, p.team_id, p.champion_id, p.participant_no
            FROM public.participants p
            WHERE p.match_id = :match_id
            ORDER BY p.participant_no ASC
        """)
        with self.engine.connect() as conn:
            return pd.read_sql(query, conn, params={"match_id": match_id})

    def get_player_history(self, puuid, current_game_time, limit=8):
        """
        특정 플레이어의 과거 전적(match_json)을 가져옵니다.
        주의: 현재 게임(root match) 이전의 데이터만 가져옵니다.
        """
        query = text("""
            SELECT m.match_json
            FROM public.participants p
            JOIN public.matches m ON p.match_id = m.match_id
            WHERE p.puuid = :puuid
              AND m.game_start_at < :curr_time
            ORDER BY m.game_start_at DESC
            LIMIT :limit
        """)
        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={
                "puuid": puuid, 
                "curr_time": current_game_time, 
                "limit": limit
            })
            return df['match_json'].tolist()