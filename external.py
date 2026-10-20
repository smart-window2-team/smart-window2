"""기상청 + 에어코리아 API 호출"""
import math
import time
from concurrent.futures import ThreadPoolExecutor
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


KMA_URL = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0"
PTY_MAP = {0: "none", 1: "rain", 2: "rain_snow", 3: "snow",
           5: "drizzle", 6: "sleet", 7: "flurry"}
SKY_MAP = {1: "clear", 3: "cloudy", 4: "overcast"}
WIND_LOOKAHEAD = timedelta(hours=1)   # 바람은 지금부터 이 시간 안의 예보까지 같이 봄 (세지기 전에 미리 닫기)


def _kma_items(service, base_dt, base_time, nx, ny, rows):
    params = {
        "serviceKey": KMA_KEY,
        "pageNo": 1,
        "numOfRows": rows,
        "dataType": "JSON",
        "base_date": base_dt.strftime("%Y%m%d"),
        "base_time": base_time,
        "nx": nx,
        "ny": ny,
    }
    return _get_json(f"{KMA_URL}/{service}", params)["response"]["body"]["items"]["item"]


def _kma_observed(nx, ny, now):
    """초단기실황 = 매시 정각에 실제로 잰 값 (40분쯤 뒤에 나옴)
    돌려주는 값: {"T1H": "19.8", "WSD": "3.2", ...}, 관측 시각"""
    base = now - timedelta(minutes=40)
    items = _kma_items("getUltraSrtNcst", base, base.strftime("%H00"), nx, ny, rows=20)
    return {item["category"]: item["obsrValue"] for item in items}, base.strftime("%H:00")


def _kma_forecast(nx, ny, now):
    """초단기예보 = 앞으로 6시간 예상값 (매시 45분에 새로 나옴)
    돌려주는 값: [(예보 시각, {"T1H": "20", "WSD": "5", ...}), ...] 시간순"""
    base = now if now.minute >= 45 else now - timedelta(hours=1)
    items = _kma_items("getUltraSrtFcst", base, base.strftime("%H30"), nx, ny, rows=60)
    slots = {}
    for item in items:
        # 날짜까지 같이 봐야 함 (시각만 보면 밤에는 자정 예보를 '가장 가까운 시각'으로 잘못 고름)
        at = datetime.strptime(item["fcstDate"] + item["fcstTime"], "%Y%m%d%H%M").replace(tzinfo=KST)
        slots.setdefault(at, {})[item["category"]] = item["fcstValue"]
    return sorted(slots.items())


def _code(value):
    number = _safe_float(value)
    return int(number) if number is not None else 0


def get_kma_weather(lat, lon):
    """기상청 날씨. 실제 관측(초단기실황)과 예보(초단기예보)를 함께 써서 항목마다 나은 쪽을 고름
    - 기온·습도: 관측 (관측이 없으면 예보)
    - 비: 관측이나 가장 가까운 예보 중 하나라도 비면 비
    - 풍속: 관측과 앞으로 1시간 안의 예보 중 가장 센 값 (관측은 최대 1시간 늦게 나와서 예보로 보완)
    - 하늘: 예보 (관측에는 하늘 항목이 없음)
    둘 중 하나가 실패해도 나머지로 동작, 둘 다 실패하면 None"""
    nx, ny = _latlon_to_grid(lat, lon)
    now = datetime.now(KST)

    # 실황과 예보를 동시에 조회
    with ThreadPoolExecutor(max_workers=2) as pool:
        obs_job = pool.submit(_kma_observed, nx, ny, now)
        fcst_job = pool.submit(_kma_forecast, nx, ny, now)
        try:
            obs, observed_at = obs_job.result()
        except Exception as exc:
            print(f"[KMA] 실황 조회 실패 → 예보만 사용: {exc}")
            obs, observed_at = {}, None
        try:
            slots = fcst_job.result()
        except Exception as exc:
            print(f"[KMA] 예보 조회 실패 → 실황만 사용: {exc}")
            slots = []

    if not obs and not slots:
        print("[KMA] 조회 실패: 실황·예보 모두 없음")
        return None

    nearest = slots[0][1] if slots else {}                          # 가장 가까운 예보
    soon = [v for at, v in slots if at <= now + WIND_LOOKAHEAD] or ([nearest] if nearest else [])

    pick = lambda key: _safe_float(obs.get(key)) if _safe_float(obs.get(key)) is not None else _safe_float(nearest.get(key))
    winds = [w for w in [_safe_float(obs.get("WSD"))] + [_safe_float(v.get("WSD")) for v in soon] if w is not None]
    pty_code = _code(obs.get("PTY")) or _code(nearest.get("PTY"))
    sky_code = _code(nearest.get("SKY"))

    return {
        "temperature": pick("T1H"),
        "humidity": pick("REH"),
        "wind_speed": max(winds) if winds else None,
        "precipitation_code": pty_code,
        "precipitation": PTY_MAP.get(pty_code, "unknown"),
        "sky_code": sky_code,
        "sky": SKY_MAP.get(sky_code, "unknown"),
        # 참고용 (판단 근거·기록에서 어디서 온 값인지 볼 수 있게)
        "wind_observed": _safe_float(obs.get("WSD")),
        "observed_at": observed_at,
    }
