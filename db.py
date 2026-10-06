"""데이터베이스 (MySQL) — 연결, 표 정의, 저장·조회 함수
app.py 는 여기 있는 함수만 부릅니다. 접속 정보는 .env 에서 읽습니다 (config.py 참고).

표 4개
  indoor_logs : 실내 센서 기록 (1분마다)
  air_logs    : 바깥 날씨·공기 + 그때의 자동 판단 (10분마다)
  events      : 창문 열림/닫힘, 모드·위치 변경 등 활동 기록
  settings    : 재시작해도 유지할 설정 (모드, 위치, 측정소)
"""
import json
import os
from datetime import datetime, timedelta

from sqlalchemy import DateTime, Float, Integer, String, Text, create_engine, delete, func, select
from sqlalchemy.engine import URL
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from config import (
    DATABASE_URL, DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER,
    INDOOR_KEEP_DAYS, AIR_KEEP_DAYS, EVENT_KEEP_DAYS,
)

TS_FORMAT = "%Y-%m-%d %H:%M:%S"     # API 가 주고받는 시각 문자열 (한국 시간)


# ── 연결 ───────────────────────────────────────────────────────────────────
def _make_engine():
    if DATABASE_URL:                                   # 통째로 지정한 경우 (테스트용 sqlite 등)
        args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
        return create_engine(DATABASE_URL, connect_args=args)
    url = URL.create(                                  # 항목별로 조립 → 비밀번호에 @ 같은 문자가 있어도 안전
        "mysql+pymysql", username=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT, database=DB_NAME, query={"charset": "utf8mb4"},
    )
    # pool_pre_ping: 오래 쉰 연결이 끊겼으면 다시 연결 / pool_recycle: MySQL 이 끊기 전에 미리 새로 맺음
    return create_engine(url, pool_pre_ping=True, pool_recycle=3600)


engine = _make_engine()


# ── 표 정의 ────────────────────────────────────────────────────────────────
class Base(DeclarativeBase):
    pass


class IndoorLog(Base):
    __tablename__ = "indoor_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime, index=True)          # 기록 시각 (한국 시간)
    co2: Mapped[float | None] = mapped_column(Float)
    pm25: Mapped[float | None] = mapped_column(Float)
    pm10: Mapped[float | None] = mapped_column(Float)
    temperature: Mapped[float | None] = mapped_column(Float)
    humidity: Mapped[float | None] = mapped_column(Float)


class AirLog(Base):
    __tablename__ = "air_logs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime, index=True)
    station: Mapped[str | None] = mapped_column(String(50))
    # 날씨 (기상청)
    temperature: Mapped[float | None] = mapped_column(Float)
    humidity: Mapped[float | None] = mapped_column(Float)
    wind_speed: Mapped[float | None] = mapped_column(Float)
    precipitation: Mapped[str | None] = mapped_column(String(20))
    sky: Mapped[str | None] = mapped_column(String(20))
    # 대기질 (에어코리아)
    pm25: Mapped[float | None] = mapped_column(Float)
    pm10: Mapped[float | None] = mapped_column(Float)
    o3: Mapped[float | None] = mapped_column(Float)
    no2: Mapped[float | None] = mapped_column(Float)
    measured_at: Mapped[str | None] = mapped_column(String(20))
    # 그때의 자동 판단
    window_status: Mapped[str | None] = mapped_column(String(10))
    rule: Mapped[str | None] = mapped_column(String(30))
    reason: Mapped[str | None] = mapped_column(String(255))


class Event(Base):
    __tablename__ = "events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(DateTime, index=True)
    kind: Mapped[str] = mapped_column(String(20))                        # window / timer / mode / location
    window_status: Mapped[str | None] = mapped_column(String(10))
    reason: Mapped[str | None] = mapped_column(String(255))
    source: Mapped[str | None] = mapped_column(String(20))               # auto / manual / timer / system


class Setting(Base):
    __tablename__ = "settings"
    name: Mapped[str] = mapped_column(String(50), primary_key=True)
    value: Mapped[str | None] = mapped_column(Text)                      # JSON 문자열


# ── 도우미 ─────────────────────────────────────────────────────────────────
def _parse_ts(text):
    return datetime.strptime(text, TS_FORMAT)


def _num(value):
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _cut(text, length=255):
    return None if text is None else str(text)[:length]


INDOOR_FIELDS = ("co2", "pm25", "pm10", "temperature", "humidity")


def _indoor_row(ts, indoor):
    indoor = indoor or {}
    return IndoorLog(ts=ts, **{k: _num(indoor.get(k)) for k in INDOOR_FIELDS})


def _air_row(ts, weather, air, decision):
    weather, air, decision = weather or {}, air or {}, decision or {}
    return AirLog(
        ts=ts, station=_cut(air.get("station"), 50),
        temperature=_num(weather.get("temperature")), humidity=_num(weather.get("humidity")),
        wind_speed=_num(weather.get("wind_speed")),
        precipitation=_cut(weather.get("precipitation"), 20), sky=_cut(weather.get("sky"), 20),
        pm25=_num(air.get("pm25")), pm10=_num(air.get("pm10")), o3=_num(air.get("o3")), no2=_num(air.get("no2")),
        measured_at=_cut(air.get("measured_at"), 20),
        window_status=_cut(decision.get("window_status"), 10), rule=_cut(decision.get("rule"), 30),
        reason=_cut(decision.get("reason")),
    )


def _event_row(ts, kind, reason, source, window_status):
    return Event(ts=ts, kind=_cut(kind, 20) or "", window_status=_cut(window_status, 10),
                 reason=_cut(reason), source=_cut(source, 20))


# ── 저장 ───────────────────────────────────────────────────────────────────
def add_indoor_log(ts, indoor):
    with Session(engine) as s:
        s.add(_indoor_row(ts, indoor))
        s.commit()


def add_air_log(ts, weather, air, decision):
    with Session(engine) as s:
        s.add(_air_row(ts, weather, air, decision))
        s.commit()


def add_event(ts, kind, reason, source, window_status=None):
    with Session(engine) as s:
        s.add(_event_row(ts, kind, reason, source, window_status))
        s.commit()


# ── 조회 (예전 JSON 방식과 같은 모양으로 돌려줌 → 화면 코드는 그대로) ──────
def get_indoor_logs(since):
    """since 이후의 실내 기록, 시간순. [{"timestamp": "...", "indoor": {...}}]"""
    with Session(engine) as s:
        rows = s.scalars(select(IndoorLog).where(IndoorLog.ts >= since).order_by(IndoorLog.ts)).all()
        return [{
            "timestamp": r.ts.strftime(TS_FORMAT),
            "indoor": {k: getattr(r, k) for k in INDOOR_FIELDS if getattr(r, k) is not None},
        } for r in rows]


def get_air_logs(since):
    """since 이후의 바깥 미세먼지 기록, 시간순. [{"timestamp", "pm25", "pm10"}]"""
    with Session(engine) as s:
        rows = s.execute(select(AirLog.ts, AirLog.pm25, AirLog.pm10)
                         .where(AirLog.ts >= since).order_by(AirLog.ts)).all()
        return [{"timestamp": ts.strftime(TS_FORMAT), "pm25": pm25, "pm10": pm10} for ts, pm25, pm10 in rows]


def get_events(limit=30):
    """최신순 활동 기록"""
    with Session(engine) as s:
        rows = s.scalars(select(Event).order_by(Event.ts.desc(), Event.id.desc()).limit(limit)).all()
        return [{"timestamp": r.ts.strftime(TS_FORMAT), "kind": r.kind, "window_status": r.window_status,
                 "reason": r.reason, "source": r.source} for r in rows]


# ── 설정 (재시작해도 유지) ─────────────────────────────────────────────────
def get_settings():
    with Session(engine) as s:
        return {r.name: json.loads(r.value) for r in s.scalars(select(Setting)).all() if r.value is not None}


def set_settings(**values):
    with Session(engine) as s:
        for name, value in values.items():
            s.merge(Setting(name=name, value=json.dumps(value, ensure_ascii=False)))
        s.commit()


# ── 오래된 기록 정리 ───────────────────────────────────────────────────────
def cleanup_old(now):
    """보관 기간이 지난 기록 삭제. 지운 건수를 돌려줌"""
    removed = 0
    with Session(engine) as s:
        for model, days in ((IndoorLog, INDOOR_KEEP_DAYS), (AirLog, AIR_KEEP_DAYS), (Event, EVENT_KEEP_DAYS)):
            removed += s.execute(delete(model).where(model.ts < now - timedelta(days=days))).rowcount or 0
        s.commit()
    return removed


# ── 시작할 때: 표 만들기 + 예전 JSON 기록 옮기기 ───────────────────────────
def _read_json_list(path):
    if not path or not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _import_json(session, model, path, make_row):
    """표가 비어 있을 때만 JSON 파일의 기록을 옮김 (JSON 파일은 그대로 둠)"""
    if session.scalar(select(func.count()).select_from(model)):
        return 0
    rows = []
    for item in _read_json_list(path):
        try:
            rows.append(make_row(_parse_ts(item["timestamp"]), item))
        except Exception:
            continue                                   # 형식이 깨진 기록은 건너뜀
    session.add_all(rows)
    return len(rows)


def init_db(indoor_json=None, air_json=None, events_json=None):
    """표가 없으면 만들고, 처음 한 번 예전 JSON 기록을 옮김. 옮긴 건수를 돌려줌"""
    Base.metadata.create_all(engine)
    with Session(engine) as s:
        moved = {
            "indoor_logs": _import_json(s, IndoorLog, indoor_json, lambda ts, it: _indoor_row(ts, it.get("indoor"))),
            "air_logs": _import_json(s, AirLog, air_json,
                                     lambda ts, it: _air_row(ts, it.get("weather"), it.get("air"), it.get("decision"))),
            "events": _import_json(s, Event, events_json,
                                   lambda ts, it: _event_row(ts, it.get("kind"), it.get("reason"),
                                                             it.get("source"), it.get("window_status"))),
        }
        s.commit()
    return moved


def count_rows():
    with Session(engine) as s:
        return {m.__tablename__: s.scalar(select(func.count()).select_from(m)) for m in (IndoorLog, AirLog, Event, Setting)}
