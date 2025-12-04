from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum

# --- 설정 ---
# docker-compose.yml에 정의된 Connection ID 사용
CONNECTION_ID = "DATA_POSTGRES_CONNECTION"
TABLE_NAME = "game_stats_test"

"""
GEMINI에 의해 작성된 코드입니다.
"""
@dag(
    dag_id="postgres-crud-test",
    start_date=pendulum.datetime(2025, 11, 20, tz="Asia/Seoul"),
    schedule=None,
    catchup=False,
    tags=["postgres", "crud", "example"],
    doc_md="PostgresHook을 이용한 데이터베이스 CRUD 예제"
)
def postgres_crud_pipeline():
    # 1. 테이블 생성 (DDL)
    @task
    def create_table():
        hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
        sql = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id SERIAL PRIMARY KEY,
                player_name VARCHAR(50),
                champion VARCHAR(50),
                win_rate FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
        hook.run(sql)
        print(f"Table '{TABLE_NAME}' check/created successfully.")

    # 2. Create: 데이터 삽입 (Insert)
    @task
    def insert_data():
        hook = PostgresHook(postgres_conn_id=CONNECTION_ID)

        # 대량 삽입을 위한 데이터 리스트
        rows = [
            ("Faker", "Ahri", 65.5),
            ("Chovy", "Azir", 70.2),
            ("ShowMaker", "Sylas", 60.0),
            ("Zeka", "Yone", 55.5)
        ]

        # insert_rows 메서드는 Python 리스트를 효율적으로 Insert 해줍니다.
        hook.insert_rows(
            table=TABLE_NAME,
            rows=rows,
            target_fields=["player_name", "champion", "win_rate"]
        )
        print(f"Inserted {len(rows)} rows into {TABLE_NAME}.")

    # 3. Read: 데이터 조회 (Select)
    @task
    def read_data():
        hook = PostgresHook(postgres_conn_id=CONNECTION_ID)
        sql = f"SELECT * FROM {TABLE_NAME} ORDER BY id DESC;"

        # 쿼리 결과 가져오기
        records = hook.get_records(sql)

        print(f"--- Fetched Data from {TABLE_NAME} ---")
        for row in records:
            print(row)
        return records

    # 4. Update: 데이터 수정
    @task
    def update_data():
        hook = PostgresHook(postgres_conn_id=CONNECTION_ID)

        # Faker의 Ahri 승률을 업데이트
        # 파라미터 바인딩(%s)을 사용하여 SQL Injection 방지
        sql = f"UPDATE {TABLE_NAME} SET win_rate = %s WHERE player_name = %s"
        parameters = (100.0, "Faker")

        hook.run(sql, parameters=parameters)
        print("Updated Faker's win rate to 100.0")

    # 5. Delete: 데이터 삭제
    @task
    def delete_data():
        hook = PostgresHook(postgres_conn_id=CONNECTION_ID)

        # 승률이 60 미만인 데이터 삭제
        sql = f"DELETE FROM {TABLE_NAME} WHERE win_rate < %s"
        parameters = (60.0,)

        hook.run(sql, parameters=parameters)
        print("Deleted records with win_rate < 60.0")

    # --- DAG 흐름 정의 ---
    # 순차 실행: 테이블 생성 -> 삽입 -> 조회 -> 수정 -> (다시 조회 확인용) -> 삭제 -> (최종 조회)

    setup = create_table()
    create = insert_data()
    read_1 = read_data()
    update = update_data()
    read_2 = read_data()  # 수정 확인용
    delete = delete_data()
    read_3 = read_data()  # 삭제 확인용

    # 의존성 설정
    setup >> create >> read_1 >> update >> read_2 >> delete >> read_3

dag = postgres_crud_pipeline()