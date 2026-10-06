"""환경 설정 모음
API 키 같은 비밀 값은 같은 폴더의 .env 파일에서 읽습니다. (양식: .env.example)"""
import os
from pathlib import Path


def _load_env_file(path):
    """.env 파일의 KEY=VALUE 줄을 환경변수로 등록 (별도 라이브러리 없이)
    이미 설정된 환경변수는 덮어쓰지 않음 → 서버에서 따로 지정한 값이 우선"""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.split(" #", 1)[0].strip().strip('"').strip("'")   # 줄 끝 주석·따옴표 제거
        os.environ.setdefault(key.strip(), value)


_load_env_file(Path(__file__).parent / ".env")

# API 키 (.env 에서 읽음)
AIR_KOREA_KEY = os.getenv("AIR_KOREA_KEY", "")
KMA_KEY = os.getenv("KMA_KEY", "")

if not AIR_KOREA_KEY or not KMA_KEY:
    print("[CONFIG] API 키가 없어요. .env 파일에 AIR_KOREA_KEY, KMA_KEY를 넣어 주세요 (.env.example 참고)")

# 위치 (인천 기본값)
FIXED_LAT = float(os.getenv("FIXED_LAT", "37.3392"))
FIXED_LON = float(os.getenv("FIXED_LON", "126.7366"))

# 자동 판단 주기 (초)
AUTO_INTERVAL_SECONDS = int(os.getenv("AUTO_INTERVAL_SECONDS", "600"))

# 로그 파일     
LOG_FILE = os.getenv("LOG_FILE", "logs/air_log.json")
MAX_LOG_COUNT = 120

# 실내 센서 로그 파일
INDOOR_LOG_FILE = os.getenv("INDOOR_LOG_FILE", "logs/indoor_log.json")

# 서버 포트
PORT = int(os.getenv("PORT", "5000"))