import requests
import json
import subprocess
from datetime import datetime
import os
from prefect import task, flow

# API 엔드포인트 및 데이터 저장 경로 정의
API_ENDPOINT = "https://jsonplaceholder.typicode.com/users"
RAW_DATA_PATH = "data\\raw"

@task
def fetch_data(url: str) -> list:
    """지정된 URL에서 데이터를 가져와 JSON으로 파싱한다."""
    print(f"Fetching data from {url}...")
    try:
        response = requests.get(url)
        response.raise_for_status()  # HTTP 에러 발생 시 예외 처리
        print("Data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        raise

@task
def save_data(data: list, path: str) -> str:
    """데이터를 타임스탬프가 포함된 JSON 파일로 저장한다."""
    os.makedirs(path, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"users_{timestamp}.json"
    file_path = os.path.join(path, file_name)
    
    print(f"Saving data to {file_path}...")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    print("Data saved successfully.")
    return file_path

def run_command(command: list):
    """주어진 셸 명령어를 실행하고 결과를 출력한다."""
    print(f"Running command: {' '.join(command)}")
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)

@task
def version_data(file_path: str):
    """DVC와 Git을 사용하여 데이터와 메타데이터를 버전 관리한다."""
    try:
        print("Versioning data with DVC and Git...")
        
        # 1. DVC로 데이터 파일 추적 및 캐싱
        run_command(["dvc", "add", file_path])
        
        # 2. DVC 원격 저장소로 데이터 푸시
        run_command(["dvc", "push"])
        
        # 3. Git으로.dvc 파일 및 설정 변경 사항 스테이징
        dvc_file = f"{file_path}.dvc"
        run_command(["git", "add", dvc_file, ".dvc\\config"])
        
        # 4. Git 커밋
        commit_message = f"feat: Add new user data from {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        run_command(["git", "commit", "-m", commit_message])
        
        # 5. Git 원격 저장소로 푸시
        run_command(["git", "push"])
        
        print("Data versioned successfully.")
    except subprocess.CalledProcessError as e:
        print(f"An error occurred during versioning: {e}")
        print(f"Stderr: {e.stderr}")
        raise

@flow(log_prints=True)
def data_collection_flow():
    """데이터 수집, 저장, 버전 관리를 위한 전체 워크플로우."""
    print("Data collection flow started.")
    raw_data = fetch_data(url=API_ENDPOINT)
    saved_file_path = save_data(data=raw_data, path=RAW_DATA_PATH)
    version_data(file_path=saved_file_path)
    print("Data collection flow finished successfully.")

# src/data_collection.py (마지막에 추가)

if __name__ == "__main__":
    data_collection_flow.serve(
        name="daily-user-data-collection",
        cron="0 8 * * *",  # 매일 오전 8시 0분에 실행
    )