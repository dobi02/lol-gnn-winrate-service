import json
import os

class FileRepository:
    def __init__(self):
        self.data_dir = "data"
        os.makedirs(self.data_dir, exist_ok=True)

    def save_match_data_as_json(self, match_id: str, raw_match_data: dict):
        """
        매치 데이터를 JSON 파일로 저장합니다.
        """
        file_path = os.path.join(self.data_dir, f"{match_id}.json")
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(raw_match_data, f, ensure_ascii=False, indent=4)
            print(f"Match data for {match_id} saved to {file_path}")
        except IOError as e:
            print(f"Error saving match data to {file_path}: {e}")
