import os
from dotenv import load_dotenv

# .env 파일에서 환경 변수를 로드합니다.
load_dotenv()

# API 키를 변수로 저장합니다.
RIOT_API_KEY = os.getenv("RIOT_API_KEY")
