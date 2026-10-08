"""기상청 + 에어코리아 API 호출"""
import math
import time
from datetime import datetime, timedelta, timezone
import requests

from config import AIR_KOREA_KEY, KMA_KEY, FIXED_LAT, FIXED_LON


KST = timezone(timedelta(hours=9))   # 서버가 어느 나라 시간으로 설정돼 있든 한국 시간 기준으로 조회


def _safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_json(url, params, retries=3, timeout=4):
    """공공데이터포털 GET + JSON 파싱.
    포털이 가끔 504(SERVICETIMEOUT_ERROR)나 타임아웃을 내서 몇 번 재시도함"""
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
            if attempt < retries - 1:
                time.sleep(0.5)
    raise last_exc


def get_location():
    """일단 고정 좌표 반환 (나중에 IP 기반 추가 가능)"""
    return FIXED_LAT, FIXED_LON


def _latlon_to_tm(lat, lon):
    """위경도 → TM좌표 (에어코리아 측정소 조회용, 간이 변환)"""
    # 정확도 떨어지지만 측정소 조회용으론 충분
    # 더 정확히 하려면 pyproj 사용
    K = 6371.00877
    LAT_TO_KM = 111.32
    LON_TO_KM = 88.74  # 한국 기준 평균
    tm_x = (lon - 127.0) * LON_TO_KM * 1000 + 200000
    tm_y = (lat - 38.0) * LAT_TO_KM * 1000 + 500000
    return int(tm_x), int(tm_y)


def get_nearest_station(lat, lon):
    """가장 가까운 대기질 측정소"""
    try:
        tm_x, tm_y = _latlon_to_tm(lat, lon)
        url = "http://apis.data.go.kr/B552584/MsrstnInfoInqireSvc/getNearbyMsrstnList"
        params = {
            "serviceKey": AIR_KOREA_KEY,
            "returnType": "json",
            "tmX": tm_x,
            "tmY": tm_y,
            "ver": "1.1",
        }
        items = _get_json(url, params)["response"]["body"]["items"]
        return items[0]["stationName"] if items else None
    except Exception as exc:
        print(f"[AirKorea] 측정소 조회 실패: {exc}")
        return None


def get_air_quality(station_name):
    """측정소의 대기질 데이터"""
    if not station_name:
        return None
    try:
        url = "http://apis.data.go.kr/B552584/ArpltnInforInqireSvc/getMsrstnAcctoRltmMesureDnsty"
        params = {
            "serviceKey": AIR_KOREA_KEY,
            "returnType": "JSON",
            "numOfRows": 1,
            "pageNo": 1,
            "stationName": station_name,
            "dataTerm": "DAILY",
            "ver": "1.5",
        }
        item = _get_json(url, params)["response"]["body"]["items"][0]
        return {
            "station": station_name,
            "measured_at": item.get("dataTime"),
            "pm10": _safe_float(item.get("pm10Value")),
            "pm25": _safe_float(item.get("pm25Value")),
            "o3": _safe_float(item.get("o3Value")),
            "no2": _safe_float(item.get("no2Value")),
        }
    except Exception as exc:
        print(f"[AirKorea] 조회 실패: {exc}")
        return None


def _latlon_to_grid(lat, lon):
    """위경도 → 기상청 격자좌표 (Lambert Conformal Conic)"""
    RE, GRID = 6371.00877, 5.0
    SLAT1, SLAT2, OLON, OLAT, XO, YO = 30.0, 60.0, 126.0, 38.0, 43, 136
    DEGRAD = math.pi / 180.0
    re = RE / GRID
    slat1, slat2 = SLAT1 * DEGRAD, SLAT2 * DEGRAD
    olon, olat = OLON * DEGRAD, OLAT * DEGRAD
    sn = math.log(math.cos(slat1) / math.cos(slat2)) / math.log(
        math.tan(math.pi * 0.25 + slat2 * 0.5)
        / math.tan(math.pi * 0.25 + slat1 * 0.5)
    )
    sf = (math.tan(math.pi * 0.25 + slat1 * 0.5) ** sn) * math.cos(slat1) / sn
    ro = re * sf / (math.tan(math.pi * 0.25 + olat * 0.5) ** sn)
    ra = re * sf / (math.tan(math.pi * 0.25 + lat * DEGRAD * 0.5) ** sn)
    theta = lon * DEGRAD - olon
    if theta > math.pi:
        theta -= 2.0 * math.pi
    if theta < -math.pi:
        theta += 2.0 * math.pi
    theta *= sn
    return (
        int(ra * math.sin(theta) + XO + 0.5),
        int(ro - ra * math.cos(theta) + YO + 0.5),
    )


def get_kma_weather(lat, lon):
    """기상청 초단기예보 (기존 실황값 + 하늘상태 SKY 포함)"""
    try:
        nx, ny = _latlon_to_grid(lat, lon)
        now = datetime.now(KST)
        # 초단기예보는 매시 45분에 발표됨 → 45분 이전이면 한 시간 전 발표분 사용
        if now.minute >= 45:
            base_dt = now
        else:
            base_dt = now - timedelta(hours=1)
        base_date = base_dt.strftime("%Y%m%d")
        base_time = base_dt.strftime("%H30")  # 예보 base_time은 'HH30'

        url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtFcst"
        params = {
            "serviceKey": KMA_KEY,
            "pageNo": 1,
            "numOfRows": 60,          # 항목 여러 개 × 여러 시각 → 넉넉히
            "dataType": "JSON",
            "base_date": base_date,
            "base_time": base_time,
            "nx": nx,
            "ny": ny,
        }
        items = _get_json(url, params)["response"]["body"]["items"]["item"]

        # 예보는 여러 시각이 내려옴 → 가장 가까운(가장 빠른) fcstTime 한 개만 사용
        nearest = min(item["fcstTime"] for item in items)
        values = {
            item["category"]: item["fcstValue"]
            for item in items if item["fcstTime"] == nearest
        }

        pty_code = int(values.get("PTY", 0))
        pty_map = {
            0: "none", 1: "rain", 2: "rain_snow", 3: "snow",
            5: "drizzle", 6: "sleet", 7: "flurry",
        }
        sky_code = int(values.get("SKY", 0))
        sky_map = {1: "clear", 3: "cloudy", 4: "overcast"}
        return {
            "temperature": _safe_float(values.get("T1H")),
            "humidity": _safe_float(values.get("REH")),
            "wind_speed": _safe_float(values.get("WSD")),
            "precipitation_code": pty_code,
            "precipitation": pty_map.get(pty_code, "unknown"),
            "sky_code": sky_code,                      # ← 추가
            "sky": sky_map.get(sky_code, "unknown"),   # ← 추가
        }
    except Exception as exc:
        print(f"[KMA] 조회 실패: {exc}")
        return None
