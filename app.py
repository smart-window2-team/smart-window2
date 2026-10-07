"""스마트 창문 서버 - 메인 진입점"""
from typing import Optional

from fastapi.staticfiles import StaticFiles

import json
import os
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Literal

import uvicorn
from datetime import datetime, timezone, timedelta

from config import (
    AUTO_INTERVAL_SECONDS,
    LOG_FILE,
    INDOOR_LOG_FILE,   # ★ 추가
    PORT,
    VENT_CYCLE_TEST,            # ★ 환기 주기 테스트 모드
    VENT_CYCLE_OPEN_SECONDS,
    VENT_CYCLE_CLOSE_SECONDS,
)
from external import get_air_quality, get_kma_weather, get_location, get_nearest_station
import db   # ★ 기록·설정 저장 (MySQL)

app = FastAPI(title="스마트 창문 서버", version="2.0.0")
# ★ 추가: static 폴더를 /static 경로로 노출
BASE_DIR = Path(__file__).parent
app.mount(
    "/static",
    StaticFiles(directory=BASE_DIR / "static", html=False, check_dir=False),
    name="static"
)


# ★ 화면 파일(HTML/CSS)은 브라우저가 매번 최신인지 확인하게 함
#   → 수정 후 강력 새로고침(Ctrl+F5) 안 해도 바로 반영 (안 바뀌었으면 저장본 사용이라 느려지지 않음)
@app.middleware("http")
async def no_stale_frontend(request, call_next):
    response = await call_next(request)
    if request.url.path == "/" or request.url.path.startswith("/static/"):
        response.headers["Cache-Control"] = "no-cache"
    return response

# ── 시간 (KST, tzdata 없어도 동작) ────────────────────────────────────────
try:
    from zoneinfo import ZoneInfo
    KST = ZoneInfo("Asia/Seoul")
except Exception:
    KST = timezone(timedelta(hours=9))


def now_str() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")


def now_local() -> datetime:
    """DB 에 넣는 시각: 한국 시간, 시간대 표시 없는 값 (서버가 어느 나라에 있든 같게)"""
    return datetime.now(KST).replace(tzinfo=None)


# ── 전역 상태 (스레드 안전) ────────────────────────────────────────────────
state_lock = threading.Lock()
stop_event = threading.Event()

# 상태 정의
state = {
    "mode": "auto",
    "window_status": "closed",
    "last_reason": "서버 시작",
    "source": "system",
    "weather": None,
    "air": None,
    "indoor": None,
    "station": None,
    "lat": None,
    "lon": None,
    "timer_end_at": None,        # ★ 추가: 만료 시각 (ISO string)
    "timer_duration": None,      # ★ 추가: 설정한 시간 (초)
    "decided_at": None,          # ★ 추가: 마지막 자동 판단 시각
    "last_rule": None,           # ★ 추가: 마지막 자동 판단 규칙 (대시보드 '쉬운 말' 이유 표시용)
    "vent_end_at": None,         # ★ 추가: 자동 '짧게 환기' 끝나는 시각 (ISO)
    "vent_rest_until": None,     # ★ 추가: 짧은 환기 후 쉬는 시간 끝 (ISO)
    "updated_at": now_str(),
}

# ── 로그 저장용 타이머 ─────────────────────────────
_last_indoor_log_time = None   # 마지막 저장 시각
LOG_INTERVAL_SECONDS = 60      # 1분에 한 번

# ── 판단 기준값 (decide_window / explain_window 공통) ──────────────────────
# 근거: CO₂ 1,000ppm = 실내공기질 관리법 유지기준 / 미세먼지 등급 = 환경부 예보 등급
WIND_CLOSE = 5.0                  # 이상이면 강풍 → 닫힘
CO2_VENT = 1000                   # 이상이면 환기 필요
CO2_URGENT = 2000                 # 이상이면 바깥이 매우나빠도 짧게 환기
PM10_BAD, PM25_BAD = 80, 35       # 초과면 바깥 '나쁨' (환경부 예보 등급)
PM10_VBAD, PM25_VBAD = 150, 75    # 초과면 바깥 '매우나쁨'
SHORT_VENT_SECONDS = 10 * 60      # 바깥이 나쁠 때 짧게 여는 시간
VENT_REST_SECONDS = 30 * 60       # 짧은 환기 뒤 다시 짧게 열지 않는 쉬는 시간


def outdoor_level(air):
    """바깥 대기질 → 'vbad' / 'bad' / 'ok' / None(정보 없음)"""
    pm10, pm25 = air.get("pm10"), air.get("pm25")
    if pm10 is None and pm25 is None:
        return None
    if (pm10 and pm10 > PM10_VBAD) or (pm25 and pm25 > PM25_VBAD):
        return "vbad"
    if (pm10 and pm10 > PM10_BAD) or (pm25 and pm25 > PM25_BAD):
        return "bad"
    return "ok"


def indoor_dirtier(indoor, air):
    """실내 미세먼지가 바깥보다 높은 항목 (name, 실내, 바깥) 또는 None"""
    for key, name in (("pm10", "PM10"), ("pm25", "PM2.5")):
        i_v, o_v = indoor.get(key), air.get(key)
        if i_v is not None and o_v is not None and i_v > o_v:
            return name, i_v, o_v
    return None


# ── 자동 판단 로직 ─────────────────────────────────────────────────────────
def decide_window(weather, air, indoor=None):
    """실외 기상/대기질 + 실내 센서를 종합해 open/closed 판단 (반쯤 열림 없음)
    rule: 어떤 규칙으로 결정됐는지 (대시보드 '판단 근거'·'쉬운 말' 이유 표시용)
    vent_short 규칙은 '짧게 열기' → 실제 열고 닫는 시간은 plan_window가 처리
    trigger: 환기가 필요한 이유 (co2 / indoor_pm)"""
    if not weather or not air:
        return {"window_status": "closed", "reason": "데이터 수신 실패 → 안전모드",
                "rule": "no_data"}

    wind = weather.get("wind_speed") or 0
    pty = weather.get("precipitation_code", 0)
    pm10 = air.get("pm10")
    pm25 = air.get("pm25")

    # // 실내 센서를 이용한 창문 열림 정의 부분 //
    indoor = indoor or {}
    co2 = indoor.get("co2")
    dirty = indoor_dirtier(indoor, air)
    level = outdoor_level(air)

    # 우선순위: 강수 > 강풍 > 바깥 미세먼지(위험 비교) > 환기 필요 > 양호
    if pty != 0:
        return {"window_status": "closed",
                "reason": f"강수({weather.get('precipitation')}) → 닫힘", "rule": "rain"}

    if wind >= WIND_CLOSE:
        return {"window_status": "closed",
                "reason": f"강풍({wind}m/s) → 닫힘", "rule": "wind"}

    # 바깥 매우나쁨: 미세먼지 위해성이 더 크므로 닫음. CO₂가 매우 높을 때만 짧게 환기
    if level == "vbad":
        if co2 is not None and co2 >= CO2_URGENT:
            return {"window_status": "open", "rule": "vent_short", "trigger": "co2",
                    "reason": f"바깥 매우나쁨(PM2.5 {pm25}) + CO₂ 매우 높음({co2}ppm) → 짧게 환기"}
        return {"window_status": "closed", "rule": "outdoor_vbad",
                "reason": f"대기질 매우나쁨(PM10={pm10}, PM2.5={pm25}) → 닫힘"}

    # 바깥 나쁨: 환기가 필요할 때만 짧게, 아니면 닫음
    if level == "bad":
        if co2 is not None and co2 >= CO2_VENT:
            return {"window_status": "open", "rule": "vent_short", "trigger": "co2",
                    "reason": f"바깥 나쁨(PM2.5 {pm25}) + CO₂ 높음({co2}ppm) → 짧게 환기"}
        if dirty:
            name, i_v, o_v = dirty
            return {"window_status": "open", "rule": "vent_short", "trigger": "indoor_pm",
                    "reason": f"바깥 나쁨 + 실내 {name}({i_v}) > 바깥({o_v}) → 짧게 환기"}
        return {"window_status": "closed", "rule": "outdoor_bad",
                "reason": f"대기질 나쁨(PM10={pm10}, PM2.5={pm25}) → 닫힘"}

    # 바깥 좋음·보통
    if co2 is not None and co2 >= CO2_VENT:
        return {"window_status": "open",
                "reason": f"CO₂ 높음({co2}ppm) → 환기 필수, 열림", "rule": "co2"}

    if dirty:
        name, i_v, o_v = dirty
        return {"window_status": "open", "rule": "indoor_pm",
                "reason": f"실내 {name}({i_v}) > 외부({o_v}) → 환기, 열림"}

    return {"window_status": "open", "reason": "열림", "rule": "good"}


def is_timed_open(decision):
    """시간을 정해 두고 여는 판단인가
    - 평소: 바깥이 나쁠 때의 '짧게 환기'(vent_short)만
    - 환기 주기 테스트 모드: 모든 '열기' 판단"""
    return decision["rule"] == "vent_short" or (VENT_CYCLE_TEST and decision["window_status"] == "open")


def vent_times():
    """(여는 시간, 닫고 쉬는 시간) 초"""
    if VENT_CYCLE_TEST:
        return VENT_CYCLE_OPEN_SECONDS, VENT_CYCLE_CLOSE_SECONDS
    return SHORT_VENT_SECONDS, VENT_REST_SECONDS


def plan_window(decision, now_dt, vent_end_at, vent_rest_until):
    """decide_window 결과에 '시간을 정해 열기' 규칙 적용
    - 환기 중이면 계속 열어둠
    - 환기를 마치고 쉬는 시간이면 닫아둠 (rule: vent_rest)
    vent_end_at / vent_rest_until: ISO 문자열 또는 None"""
    if not is_timed_open(decision):
        return decision
    end = datetime.fromisoformat(vent_end_at) if vent_end_at else None
    rest = datetime.fromisoformat(vent_rest_until) if vent_rest_until else None
    if end and now_dt < end:
        return decision
    if rest and now_dt < rest:
        return {**decision, "window_status": "closed", "rule": "vent_rest",
                "reason": "짧은 환기 후 쉬는 시간 → 닫힘"}
    return decision


PRECIP_KO = {"rain": "비", "rain_snow": "비/눈", "snow": "눈", "drizzle": "빗방울",
             "sleet": "진눈깨비", "flurry": "눈날림"}


def explain_window(weather, air, indoor, decision):
    """대시보드 '판단 근거' 체크리스트. decide_window와 같은 기준값 사용
    level: ok(문제 없음) / warn(주의) / close(닫을 이유) / open(열 이유) / none(데이터 없음)
    decisive: 이번 결정을 만든 항목"""
    weather, air, indoor = weather or {}, air or {}, indoor or {}
    rule = decision.get("rule")
    trigger = decision.get("trigger")
    short = rule in ("vent_short", "vent_rest")      # 바깥이 나빠서 짧게 환기하는 경우
    num = lambda v: f"{round(v):,}"
    checks = []

    def add(key, level, label, detail, decisive=False):
        checks.append({"key": key, "level": level, "label": label,
                       "detail": detail, "decisive": decisive})

    # 1) 강수
    if not weather:
        add("rain", "none", "날씨 정보를 못 받았어요", "")
    elif weather.get("precipitation_code", 0) != 0:
        add("rain", "close", "비·눈이 와요",
            PRECIP_KO.get(weather.get("precipitation"), "강수"), rule == "rain")
    else:
        add("rain", "ok", "비·눈이 안 와요", "")

    # 2) 바람
    wind = weather.get("wind_speed")
    if wind is None:
        add("wind", "none", "풍속 정보가 없어요", "")
    elif wind >= WIND_CLOSE:
        add("wind", "close", "바람이 강해요", f"{wind:.1f}m/s", rule == "wind")
    else:
        add("wind", "ok", "바람이 괜찮아요", f"{wind:.1f}m/s")

    # 3) 실내 CO2
    co2 = indoor.get("co2")
    co2_decisive = rule == "co2" or (short and trigger == "co2")
    if co2 is None:
        add("co2", "none", "실내 CO₂ 측정값이 없어요", "")
    elif co2 >= CO2_URGENT:
        add("co2", "open", "실내 CO₂가 매우 높아요", f"{num(co2)}ppm", co2_decisive)
    elif co2 >= CO2_VENT:
        add("co2", "open", "실내 CO₂가 높아요", f"{num(co2)}ppm", co2_decisive)
    else:
        add("co2", "ok", "실내 CO₂는 괜찮아요", f"{num(co2)}ppm")

    # 4) 실내 vs 바깥 미세먼지
    pairs = [(indoor.get("pm25"), air.get("pm25"), "PM2.5"),
             (indoor.get("pm10"), air.get("pm10"), "PM10")]
    pairs = [p for p in pairs if p[0] is not None and p[1] is not None]
    if not pairs:
        add("indoor_pm", "none", "실내·바깥 미세먼지를 비교할 수 없어요", "")
    else:
        dirty = [p for p in pairs if p[0] > p[1]]
        i_v, o_v, name = (dirty or pairs)[0]
        detail = f"{name} 실내 {num(i_v)} · 바깥 {num(o_v)}"
        if dirty:
            add("indoor_pm", "open", "실내가 바깥보다 탁해요", detail,
                rule == "indoor_pm" or (short and trigger == "indoor_pm"))
        else:
            add("indoor_pm", "ok", "실내가 바깥보다 깨끗해요", detail)

    # 5) 바깥 대기질
    pm10, pm25 = air.get("pm10"), air.get("pm25")
    level = outdoor_level(air)
    if level is None:
        add("outdoor", "none", "바깥 대기질 정보가 없어요", "")
    else:
        detail = f"PM2.5 {num(pm25) if pm25 is not None else '-'} · PM10 {num(pm10) if pm10 is not None else '-'}"
        if level == "vbad":
            add("outdoor", "close", "바깥 공기가 매우 나빠요", detail, rule == "outdoor_vbad" or short)
        elif level == "bad":
            add("outdoor", "warn", "바깥 공기가 나빠요", detail, rule == "outdoor_bad" or short)
        else:
            add("outdoor", "ok", "바깥 공기가 괜찮아요", detail)

    return checks


# ── 기록 저장 (DB) ─────────────────────────────────────────────────────────
# 예전에는 JSON 파일 전체를 읽고 다시 썼음 → 지금은 DB 에 한 줄씩 넣음 (db.py)
EVENT_JSON_FILE = os.path.join(os.path.dirname(LOG_FILE), "window_events.json")   # 예전 활동 기록 (이전용)


def _db_safe(what, func, *args, **kwargs):
    """DB 작업이 실패해도 창문 제어는 계속되게: 에러는 로그만 남기고 넘어감"""
    try:
        return func(*args, **kwargs)
    except Exception as exc:
        print(f"[DB] {what} 실패: {exc}")
        return None


def save_log(weather, air, decision):
    """바깥 날씨·공기와 그때의 자동 판단 기록 (자동 판단할 때마다)"""
    _db_safe("바깥 기록 저장", db.add_air_log, now_local(), weather, air, decision)


def save_indoor_log(indoor):
    """실내 센서 기록. 센서값은 5초마다 오지만 1분에 한 번만 저장"""
    global _last_indoor_log_time

    now_dt = datetime.now(KST)
    if _last_indoor_log_time is not None:
        if (now_dt - _last_indoor_log_time).total_seconds() < LOG_INTERVAL_SECONDS:
            return   # 1분 안 지남 → 스킵
    _last_indoor_log_time = now_dt

    _db_safe("실내 기록 저장", db.add_indoor_log, now_local(), indoor)


def add_event(kind, reason, source, window_status=None):
    """
    활동 기록 (대시보드 '활동 기록'용)
    kind: window(창문 상태 변경) / timer / mode / location
    source: auto / manual / timer / system
    """
    _db_safe("활동 기록 저장", db.add_event, now_local(), kind, reason, source, window_status)


def save_settings():
    """재시작해도 유지할 설정(모드·위치·측정소) 저장"""
    with state_lock:
        values = {k: state.get(k) for k in ("mode", "lat", "lon", "station")}
    _db_safe("설정 저장", db.set_settings, **values)


# ── 자동 사이클 (자동 모드일 때 주기적 실행) ───────────────────────────────
def run_auto_cycle():
    # ★ 사용자가 지도에서 설정한 위치 우선 사용
    with state_lock:
        lat = state.get("lat")
        lon = state.get("lon")

    if lat is None or lon is None:
        lat, lon = get_location()   # config 기본값

    station = state.get("station") or get_nearest_station(lat, lon)
    weather = get_kma_weather(lat, lon)
    air = get_air_quality(station)

    # ★ 실내 센서값도 판단에 반영
    with state_lock:
        indoor = state.get("indoor")
        prev_weather = state.get("weather")
        prev_air = state.get("air")

    # ★ API 일시 실패 시 직전 값 유지 (한 번 실패로 '데이터 없음'이 되지 않게)
    if weather is None and prev_weather:
        print("[AUTO] 날씨 조회 실패 → 직전 값 사용")
        weather = prev_weather
    if air is None and prev_air and prev_air.get("station") == station:
        print("[AUTO] 대기질 조회 실패 → 직전 값 사용")
        air = prev_air

    with state_lock:
        state["weather"] = weather
        state["air"] = air
        state["station"] = station
        state["lat"] = lat          # ★ 추가
        state["lon"] = lon          # ★ 추가

    decision = apply_auto_decision(weather, air, indoor)
    save_log(weather, air, decision)
    return decision


def apply_auto_decision(weather, air, indoor, quiet=False):
    """판단하고 그 결과를 상태에 반영
    quiet=True: 1초마다 다시 볼 때 사용. 바뀐 게 없으면 아무것도 건드리지 않음"""
    now_dt = datetime.now(KST)
    with state_lock:
        decision = plan_window(decide_window(weather, air, indoor), now_dt,
                               state.get("vent_end_at"), state.get("vent_rest_until"))
        prev_status = state["window_status"]
        timed = is_timed_open(decision)
        starting = timed and not state.get("vent_end_at")     # 시간 정해 열기를 새로 시작
        changed = starting or prev_status != decision["window_status"] or state.get("last_rule") != decision["rule"]
        if quiet and not changed:
            return decision

        # ★ 시간 정해 열기: 새로 시작할 때만 끝나는 시각을 정함 / 다른 판단이면 취소
        if timed:
            if starting:
                open_seconds, _ = vent_times()
                state["vent_end_at"] = (now_dt + timedelta(seconds=open_seconds)).isoformat()
        else:
            state["vent_end_at"] = None
        state["window_status"] = decision["window_status"]
        state["decided_at"] = now_str()
        state["last_rule"] = decision["rule"]
        state["last_reason"] = decision["reason"]
        state["source"] = "auto"
        state["updated_at"] = now_str()

    print(f"[AUTO {now_str()}] {decision['reason']}" + (f" → {vent_times()[0]}초 뒤 닫힘" if starting else ""))

    # 자동 판단은 상태가 바뀔 때만 기록 (10분마다 같은 기록이 쌓이지 않게)
    if prev_status != decision["window_status"]:
        add_event("window", decision["reason"], "auto", decision["window_status"])
    return decision


# ── 백그라운드 워커 ────────────────────────────────────────────────────────
def auto_worker():
    while not stop_event.is_set():
        with state_lock:
            mode = state["mode"]

        if mode == "auto":
            try:
                run_auto_cycle()
            except Exception as exc:
                print(f"[Worker] 사이클 실패: {exc}")

            # 다음 사이클까지 대기 (모드 바뀌면 즉시 빠져나감)
            for _ in range(AUTO_INTERVAL_SECONDS):
                if stop_event.is_set():
                    return
                with state_lock:
                    if state["mode"] != "auto":
                        break
                stop_event.wait(1)
        else:
            stop_event.wait(1)

def end_short_vent(now_dt):
    """자동 모드 '짧게 환기' 시간이 끝났으면 닫고 쉬는 시간 시작 (끝냈으면 True)"""
    with state_lock:
        vent_end = state.get("vent_end_at")
        if not vent_end or state["mode"] != "auto" or now_dt < datetime.fromisoformat(vent_end):
            return False
        state["window_status"] = "closed"
        state["vent_end_at"] = None
        rest_seconds = vent_times()[1]
        state["vent_rest_until"] = (now_dt + timedelta(seconds=rest_seconds)).isoformat()
        state["last_rule"] = "vent_rest"
        state["last_reason"] = "환기 끝 → 닫힘"
        state["source"] = "auto"
        state["updated_at"] = now_str()
    print(f"[AUTO {now_str()}] 환기 끝 → 닫힘 ({rest_seconds}초 쉬기)")
    add_event("window", "환기 끝 → 닫고 쉬는 중", "auto", "closed")
    return True


def recheck_in_cycle_test():
    """환기 주기 테스트 모드: 서버가 가진 지금 값으로 다시 판단 (외부 API 호출 없음)
    → 쉬는 시간이 끝나면 바로 다시 열고, 환기 중에 비가 오면 바로 닫음"""
    with state_lock:
        if state["mode"] != "auto":
            return
        weather, air, indoor = state.get("weather"), state.get("air"), state.get("indoor")
    if weather and air:        # 첫 자동 판단(날씨·공기 수신) 이후부터
        apply_auto_decision(weather, air, indoor, quiet=True)


_last_cleanup = datetime.now(KST)


def cleanup_daily():
    """하루에 한 번, 보관 기간이 지난 기록을 DB 에서 지움"""
    global _last_cleanup
    if (datetime.now(KST) - _last_cleanup).total_seconds() < 24 * 3600:
        return
    _last_cleanup = datetime.now(KST)
    removed = _db_safe("오래된 기록 정리", db.cleanup_old, now_local())
    if removed:
        print(f"[DB] 보관 기간이 지난 기록 {removed}건 정리")


def timer_worker():
    """수동 모드 타이머 / 자동 모드 '짧게 환기' 만료 체크 (1초마다)"""
    while not stop_event.is_set():
        try:
            cleanup_daily()
            end_short_vent(datetime.now(KST))
            if VENT_CYCLE_TEST:
                recheck_in_cycle_test()

            with state_lock:
                timer_end = state.get("timer_end_at")
                mode = state["mode"]
                window = state["window_status"]

            # 타이머 활성 + 수동 모드 + 창문 열림 상태일 때만 체크
            if timer_end and mode == "manual" and window == "open":
                end_dt = datetime.fromisoformat(timer_end)
                now_dt = datetime.now(KST)

                if now_dt >= end_dt:
                    # 타이머 만료 → 창문 닫음
                    print(f"[TIMER {now_str()}] 만료 → 창문 자동 닫음")
                    with state_lock:
                        state["window_status"] = "closed"
                        state["last_reason"] = "⏰ 타이머 만료로 자동 닫힘"
                        state["source"] = "timer"
                        state["timer_end_at"] = None
                        state["timer_duration"] = None
                        state["updated_at"] = now_str()
                    add_event("window", "타이머 만료로 자동 닫힘", "timer", "closed")
        except Exception as exc:
            print(f"[Timer] 체크 실패: {exc}")
            
        stop_event.wait(1)

# ── FastAPI 앱 ─────────────────────────────────────────────────────────────
# app = FastAPI(title="스마트 창문 서버", version="2.0.0")


# ── 요청 모델 ──────────────────────────────────────────────────────────────
class ModeRequest(BaseModel):
    mode: Literal["manual", "auto"]
    
class WindowRequest(BaseModel):
    status: Literal["open", "closed"]
    
class TimerRequest(BaseModel):
    """타이머 시작/연장 요청"""
    duration_seconds: int  # 0이면 무제한, >0이면 N초 후 자동 닫힘

class LocationRequest(BaseModel):
    lat: float
    lng: float

class SensorData(BaseModel):
    """라즈베리파이에서 보내는 실내 센서 데이터"""
    pm25: Optional[float] = None
    pm10: Optional[float] = None
    co2: Optional[float] = None
    temperature: Optional[float] = None
    humidity: Optional[float] = None

# ── 라우트 ─────────────────────────────────────────────────────────────────
# == async def -> 비동기 함수 ==
@app.get("/")
async def dashboard():
    path = Path(__file__).parent / "templates" / "dashboard.html"
    if not path.exists():
        raise HTTPException(404, "dashboard.html이 없습니다")
    return FileResponse(path)


@app.get("/api/status")
async def get_status():
    """현재 모든 상태 조회 (대시보드가 폴링)
    server_time: 대시보드가 '라즈베리파이 마지막 수신 후 몇 초' 계산에 사용 (서버 시계 기준)
    checks / suggested_status: '지금 값'으로 다시 계산한 판단 근거와 판단 결과
      → 마지막 자동 판단 이후 센서값이 바뀌었거나 수동 모드여도 항상 최신"""
    with state_lock:
        snap = dict(state)
    live = plan_window(decide_window(snap["weather"], snap["air"], snap["indoor"]),
                       datetime.now(KST), snap.get("vent_end_at"), snap.get("vent_rest_until"))
    snap["checks"] = explain_window(snap["weather"], snap["air"], snap["indoor"], live)
    snap["suggested_status"] = live["window_status"]
    snap["server_time"] = now_str()
    # 환기 남은 시간 / 쉬는 시간 남은 시간 (초). 해당 없으면 None
    now_dt = datetime.now(KST)
    remain = lambda iso: max(0, round((datetime.fromisoformat(iso) - now_dt).total_seconds())) if iso else None
    snap["vent_remaining"] = remain(snap.get("vent_end_at"))
    rest = remain(snap.get("vent_rest_until"))
    snap["vent_rest_remaining"] = rest if rest else None
    snap["vent_cycle_test"] = VENT_CYCLE_TEST
    return snap

@app.get("/api/command")
async def get_command():
    """
    라즈베리파이용 명령 엔드포인트.
    창문 = LED로 시각화 (open → LED ON, closed → LED OFF)
    """
    with state_lock:
        window = state["window_status"]
        return {
            "led": "on" if window == "open" else "off",   # ★ LED 명령
            "window": window,        # 참고용 (open/closed)
            "mode": state["mode"],
            "reason": state["last_reason"],
        }

@app.post("/api/mode")
async def set_mode(req: ModeRequest):
    """수동/자동 모드 전환"""
    with state_lock:
        changed = state["mode"] != req.mode
        state["mode"] = req.mode
        state["vent_end_at"] = None      # ★ 자동 '짧게 환기' 진행 중이었다면 취소
        state["updated_at"] = now_str()

    if changed:
        add_event("mode", "자동 모드로 전환" if req.mode == "auto" else "수동 모드로 전환", "manual")
        save_settings()

    # 자동 모드로 바꾸면 즉시 1번 실행
    if req.mode == "auto":
        try:
            run_auto_cycle()
        except Exception as exc:
            print(f"[Mode] 자동 모드 초기 실행 실패: {exc}")

    return {"ok": True, "mode": req.mode}


@app.post("/api/window")
async def control_window(req: WindowRequest):
    """수동 모드에서 창문 직접 제어"""
    with state_lock:
        if state["mode"] != "manual":
            raise HTTPException(409, "자동 모드에서는 수동 제어 불가")

        state["window_status"] = req.status
        state["last_reason"] = f"수동 제어: {req.status}"
        state["source"] = "manual"
        state["updated_at"] = now_str()

        # ★ 창문 닫으면 타이머도 정리
        if req.status == "closed":
            state["timer_end_at"] = None
            state["timer_duration"] = None

    print(f"[MANUAL {now_str()}] 창문 → {req.status}")
    add_event("window", "직접 열기" if req.status == "open" else "직접 닫기", "manual", req.status)
    return {"ok": True, "status": req.status}

# 라즈베리파이 데이터 받기
@app.post("/api/sensors")
async def receive_sensors(data: SensorData):
    received = {k: v for k, v in data.model_dump().items() if v is not None}

    if not received:
        raise HTTPException(400, "보낸 데이터가 비어있음")

    with state_lock:
        current = state.get("indoor") or {}
        current.update(received)
        current["updated_at"] = now_str()
        state["indoor"] = current
        snapshot = dict(current)

    print(f"[SENSOR {now_str()}] {received}")
    save_indoor_log(snapshot)   # ← 매번 호출되지만 내부에서 1분 체크
    return {"ok": True, "received": received}


# 히스토리 조회 (DB 에서 필요한 기간만 꺼냄)
@app.get("/api/indoor-history")
async def get_indoor_history(hours: int = 1):
    """
    실내 센서 히스토리 조회.
    hours: 몇 시간 전까지 (1/6/24)
    """
    since = now_local() - timedelta(hours=hours)
    logs = _db_safe("실내 기록 조회", db.get_indoor_logs, since) or []
    return {"logs": logs, "hours": hours}


@app.get("/api/air-history")
async def get_air_history(hours: int = 1):
    """바깥 대기질 히스토리 - 실내/바깥 비교 차트용"""
    since = now_local() - timedelta(hours=hours)
    logs = _db_safe("바깥 기록 조회", db.get_air_logs, since) or []
    return {"logs": logs, "hours": hours}


VENT_MIN_SECONDS = 180                    # 이보다 짧게 열린 건 환기 효과를 계산하지 않음
VENT_SAMPLE = timedelta(seconds=150)      # 열릴 때·닫힐 때 직전 2~3분의 평균으로 비교


def find_last_vent():
    """가장 최근 환기(창문이 열렸다 닫힌 구간)와 그 전후의 실내 공기. 없으면 None"""
    now = now_local()
    spans, start = [], None
    for ts, status in db.get_window_events(now - timedelta(hours=24)):
        if status != "closed":
            start = ts                    # 닫힘 기록 없이 또 열림 = 서버 재시작 → 새로 시작한 것으로 봄
        elif start is not None:
            spans.append((start, ts))
            start = None
    with state_lock:
        is_open = state["window_status"] == "open"
    if start is not None and is_open:
        spans.append((start, None))       # 지금 환기 중

    # 테스트 모드에서는 여는 시간이 짧으므로 기준도 거기에 맞춤
    min_seconds = min(VENT_MIN_SECONDS, VENT_CYCLE_OPEN_SECONDS) if VENT_CYCLE_TEST else VENT_MIN_SECONDS
    candidates = [(s, e) for s, e in spans if ((e or now) - s).total_seconds() >= min_seconds]
    pad = timedelta(seconds=30)
    for start, end in reversed(candidates[-5:]):
        stop = end or now
        before = db.get_indoor_avg(start - VENT_SAMPLE, start + pad)
        after = db.get_indoor_avg(stop - VENT_SAMPLE, stop + pad)
        if not before or not after:
            continue                      # 그때 센서 기록이 없음
        return {
            "start": start.strftime(db.TS_FORMAT),
            "end": end.strftime(db.TS_FORMAT) if end else None,
            "ongoing": end is None,
            "minutes": int((stop - start).total_seconds() // 60),
            "before": before,
            "after": after,
        }
    return None


@app.get("/api/vent-effect")
async def get_vent_effect():
    """마지막 환기 전후의 실내 공기 비교 (요약 화면용)"""
    return {"vent": _db_safe("환기 효과 조회", find_last_vent)}


@app.get("/api/events")
async def get_events(limit: int = 30):
    """활동 기록 (최신순)"""
    events = _db_safe("활동 기록 조회", db.get_events, limit) or []
    return {"events": events}

@app.post("/api/location")
async def set_location(req: LocationRequest):
    """지도에서 클릭한 위치로 변경"""
    # 한국 영역 검증
    if not (33 <= req.lat <= 39 and 124 <= req.lng <= 132):
        raise HTTPException(400, "한국 영역 좌표만 가능합니다")

    # 측정소 조회
    try:
        new_station = get_nearest_station(req.lat, req.lng)
    except Exception as exc:
        raise HTTPException(500, f"측정소 조회 실패: {exc}")

    if not new_station:
        raise HTTPException(404, "근처 측정소를 찾을 수 없습니다")

    with state_lock:
        state["lat"] = req.lat
        state["lon"] = req.lng
        state["station"] = new_station
        state["weather"] = None     # ★ 이전 위치 날씨가 새 위치 값처럼 남지 않게
        state["last_reason"] = f"위치 변경 → {new_station}"
        state["updated_at"] = now_str()

    print(f"[LOCATION {now_str()}] ({req.lat:.4f}, {req.lng:.4f}) → {new_station}")
    add_event("location", f"위치 변경 → {new_station}", "manual")
    save_settings()

    # 자동 모드면 즉시 새 데이터 가져오기
    with state_lock:
        mode = state["mode"]
    if mode == "auto":
        try:
            run_auto_cycle()
        except Exception as exc:
            print(f"[Location] 자동 사이클 실패: {exc}")

    return {
        "ok": True,
        "lat": req.lat,
        "lng": req.lng,
        "station": new_station,
        "message": f"측정소가 {new_station}으로 변경되었습니다",
    }

# 타이머 관련
@app.post("/api/window/timer")
async def set_timer(req: TimerRequest):
    """
    수동 모드에서 창문 열고 타이머 시작.
    duration_seconds=0이면 무제한 열기.
    """
    with state_lock:
        if state["mode"] != "manual":
            raise HTTPException(409, "타이머는 수동 모드에서만 사용 가능")

        # 창문 열기
        state["window_status"] = "open"
        state["source"] = "manual"
        state["updated_at"] = now_str()

        if req.duration_seconds > 0:
            # 타이머 설정
            end_dt = datetime.now(KST) + timedelta(seconds=req.duration_seconds)
            state["timer_end_at"] = end_dt.isoformat()
            state["timer_duration"] = req.duration_seconds
            minutes = req.duration_seconds // 60
            state["last_reason"] = f"⏰ 수동 열기 ({minutes}분 후 자동 닫힘)"
            print(f"[TIMER {now_str()}] 시작: {minutes}분")
        else:
            # 무제한 열기
            state["timer_end_at"] = None
            state["timer_duration"] = None
            state["last_reason"] = "수동 열기 (계속)"

    reason = (f"직접 열기 · {req.duration_seconds // 60}분 뒤 자동 닫힘"
              if req.duration_seconds > 0 else "직접 열기 · 계속 열어둠")
    add_event("window", reason, "manual", "open")
    return {"ok": True}


@app.post("/api/window/timer/cancel")
async def cancel_timer():
    """타이머만 취소 (창문은 계속 열림 상태 유지)"""
    with state_lock:
        if state.get("timer_end_at") is None:
            raise HTTPException(409, "활성 타이머가 없습니다")

        state["timer_end_at"] = None
        state["timer_duration"] = None
        state["last_reason"] = "타이머 취소 (계속 열림)"
        state["updated_at"] = now_str()

    add_event("timer", "타이머 끔 · 계속 열어둠", "manual")
    return {"ok": True}


@app.post("/api/window/timer/extend")
async def extend_timer(req: TimerRequest):
    """타이머 연장 (남은 시간에 추가)"""
    with state_lock:
        if state.get("timer_end_at") is None:
            raise HTTPException(409, "활성 타이머가 없습니다")

        # 현재 만료시각에 시간 추가
        end_dt = datetime.fromisoformat(state["timer_end_at"])
        new_end = end_dt + timedelta(seconds=req.duration_seconds)
        state["timer_end_at"] = new_end.isoformat()
        state["timer_duration"] = (state["timer_duration"] or 0) + req.duration_seconds
        minutes = req.duration_seconds // 60
        state["last_reason"] = f"⏰ 타이머 {minutes}분 연장됨"
        state["updated_at"] = now_str()

    add_event("timer", f"타이머 {minutes}분 연장", "manual")
    return {"ok": True}

# ── 시작 시 초기화 + 워커 ──────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    # ★ DB 준비: 표가 없으면 만들고, 예전 JSON 기록이 있으면 처음 한 번 옮김
    try:
        moved = db.init_db(INDOOR_LOG_FILE, LOG_FILE, EVENT_JSON_FILE)
        if any(moved.values()):
            print(f"[DB] 예전 JSON 기록을 옮겼어요: {moved}")
        print(f"[DB] 연결됨 · 기록 수: {db.count_rows()}")
        removed = db.cleanup_old(now_local())
        if removed:
            print(f"[DB] 보관 기간이 지난 기록 {removed}건 정리")

        # 지난번에 쓰던 모드·위치 복원
        saved = db.get_settings()
        with state_lock:
            for key in ("mode", "lat", "lon", "station"):
                if saved.get(key) is not None:
                    state[key] = saved[key]
        if saved:
            print(f"[DB] 저장된 설정 복원: {saved}")
    except Exception as exc:
        print(f"[DB] ⚠ 연결 실패 → 기록 없이 동작합니다. .env 의 DB_* 값을 확인하세요: {exc}")

    # 측정소 미리 조회 (저장된 위치가 없을 때만, 실패해도 무시)
    if not state.get("station"):
        try:
            lat, lon = get_location()
            state["station"] = get_nearest_station(lat, lon)
            print(f"[INIT] 측정소: {state['station']}")
        except Exception as exc:
            print(f"[INIT] 측정소 조회 실패: {exc}")

    # 백그라운드 워커 시작
    threading.Thread(target=auto_worker, daemon=True).start()
    threading.Thread(target=timer_worker, daemon=True).start()
    print("[INIT] 워커 시작")
    if VENT_CYCLE_TEST:
        print(f"[INIT] ★ 환기 주기 테스트 모드: {VENT_CYCLE_OPEN_SECONDS}초 열기 → {VENT_CYCLE_CLOSE_SECONDS}초 닫기 반복")


@app.on_event("shutdown")
async def shutdown():
    stop_event.set()


if __name__ == "__main__":
    print("=" * 50)
    print(f"스마트 창문 서버: http://127.0.0.1:{PORT}")
    print(f"API 문서: http://127.0.0.1:{PORT}/docs")
    print("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=PORT)