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

# ── 환기 주기 테스트 모드 ─────────────────────────────
# .env 에 VENT_CYCLE_TEST=1 을 넣으면 켜짐 (기본은 꺼짐 → 평소 동작 그대로)
# 켜면 자동 모드에서: 열어도 되는 상황 → OPEN초 열기 → CLOSE초 닫고 쉬기 → 지금 값으로 다시 판단 → 반복
VENT_CYCLE_TEST = os.getenv("VENT_CYCLE_TEST", "0") == "1"
VENT_CYCLE_OPEN_SECONDS = int(os.getenv("VENT_CYCLE_OPEN_SECONDS", "60"))
VENT_CYCLE_CLOSE_SECONDS = int(os.getenv("VENT_CYCLE_CLOSE_SECONDS", "30"))

# ── 데이터베이스 (MySQL) ─────────────────────────────
# 접속 정보는 .env 에 적습니다. DB_NAME 은 MySQL 안에 만든 데이터베이스 이름 (표는 서버가 자동으로 만듦)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME", "smartwindow")
DB_USER = os.getenv("DB_USER", "root")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
# (선택) 접속 주소를 통째로 지정. 지정하면 위 값 대신 이걸 씀. 예: sqlite:///test.db
DATABASE_URL = os.getenv("DATABASE_URL", "")

# 기록 보관 기간 (일). 지나면 자동으로 지움
INDOOR_KEEP_DAYS = int(os.getenv("INDOOR_KEEP_DAYS", "30"))   # 실내 센서 (1분마다)
AIR_KEEP_DAYS = int(os.getenv("AIR_KEEP_DAYS", "30"))         # 바깥 날씨·공기 (10분마다)
EVENT_KEEP_DAYS = int(os.getenv("EVENT_KEEP_DAYS", "90"))     # 활동 기록

# 예전 JSON 기록 파일 (DB 로 처음 한 번 옮길 때만 읽음)
LOG_FILE = os.getenv("LOG_FILE", "logs/air_log.json")
INDOOR_LOG_FILE = os.getenv("INDOOR_LOG_FILE", "logs/indoor_log.json")

# 서버 포트
PORT = int(os.getenv("PORT", "5000"))