import json, os
import time
import requests
from datetime import datetime, timedelta, timezone, time as dtime
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, jsonify, request
from flask_compress import Compress
from flask_cors import CORS

# =====================================================
# CONFIG
# =====================================================
COMPANY_FILE = "companies_list.json"

GROWW_URL = (
    "https://groww.in/v1/api/charting_service/v2/charting_service/v2/chart/"
    "delayed/exchange/NSE/segment/CASH"
)

INTERVAL_MINUTES = 3
MAX_WORKERS = 100
MAX_RETRIES = 3
TIMEOUT = 20

TOTAL_BATCHES = 10
BATCH_NO = int(os.getenv("BATCH_NUM", 1))

IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN = dtime(9, 0)
MARKET_CLOSE = dtime(15, 30)

MAX_LOOKBACK_DAYS = 7

# =====================================================
# FLASK
# =====================================================
app = Flask(__name__)
Compress(app)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# =====================================================
# TIME HELPERS
# =====================================================
def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def market_range_for_date(date_str: str):
    d = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=IST)
    start = d.replace(hour=9, minute=0, second=0, microsecond=0)
    end = d.replace(hour=15, minute=30, second=0, microsecond=0)
    return to_ms(start), to_ms(end)


def effective_trade_date(requested_date: str | None):
    now = datetime.now(IST)

    if requested_date:
        base_date = datetime.strptime(requested_date, "%Y-%m-%d")
    else:
        if now.time() < MARKET_OPEN:
            base_date = now - timedelta(days=1)
        else:
            base_date = now

    for i in range(MAX_LOOKBACK_DAYS):
        check_date = (base_date - timedelta(days=i)).strftime("%Y-%m-%d")
        start_ms, end_ms = market_range_for_date(check_date)

        try:
            with open(COMPANY_FILE) as f:
                sample_symbol = json.load(f)[0].split("__")[0].strip()

            candles = fetch_candles(sample_symbol, start_ms, end_ms)
            if candles:
                return check_date, i > 0
        except Exception:
            pass

    return base_date.strftime("%Y-%m-%d"), True

# =====================================================
# FETCH LOGIC
# =====================================================
def fetch_candles(symbol: str, start_ms: int, end_ms: int):
    url = f"{GROWW_URL}/{symbol}"

    params = {
        "intervalInMinutes": INTERVAL_MINUTES,
        "startTimeInMillis": start_ms,
        "endTimeInMillis": end_ms,
    }

    headers = {
        "accept": "application/json, text/plain, */*",
        "x-app-id": "growwWeb",
        "x-platform": "web",
        "x-device-type": "charts",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=TIMEOUT)
            r.raise_for_status()
            return r.json().get("candles", [])
        except Exception:
            if attempt == MAX_RETRIES:
                return []
            time.sleep(1)

# =====================================================
# HOME
# =====================================================
@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "ok",
        "message": "Server running fine 🚀",
        "time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"),
    })

# =====================================================
# API
# =====================================================
@app.route("/api/live-candles", methods=["GET"])
def live_candles():
    requested_date = request.args.get("date")
    latest = request.args.get("latest", "false").lower() == "true"

    fetched_date, is_fallback = effective_trade_date(requested_date)
    start_ms, end_ms = market_range_for_date(fetched_date)

    with open(COMPANY_FILE) as f:
        companies = json.load(f)

    total_items = len(companies)
    batch_size = max(1, total_items // TOTAL_BATCHES)

    batch_no = min(BATCH_NO, (total_items + batch_size - 1) // batch_size)
    start_idx = (batch_no - 1) * batch_size
    end_idx = min(start_idx + batch_size, total_items)

    batch = companies[start_idx:end_idx]
    results = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                fetch_candles,
                item.split("__")[0].strip(),
                start_ms,
                end_ms
            ): item.split("__")[0].strip()
            for item in batch
        }

        for future in as_completed(futures):
            symbol = futures[future]
            candles = future.result()

            if latest:
                last_4 = candles[-4:] if len(candles) >= 4 else candles
                labeled = []

                for ts, o, h, l, c, v in last_4:
                    start = datetime.fromtimestamp(ts / 1000, IST)
                    end = start + timedelta(minutes=INTERVAL_MINUTES)

                    labeled.append({
                        "start_time": start.strftime("%H:%M:%S"),
                        "end_time": end.strftime("%H:%M:%S"),
                        "open": o,
                        "high": h,
                        "low": l,
                        "close": c,
                        "volume": v,
                    })

                results[symbol] = {"candles": labeled} if labeled else None
            else:
                results[symbol] = candles

    return jsonify({
        "mode": "latest" if latest else "full",
        "requested_date": requested_date,
        "fetched_date": fetched_date,
        "is_fallback": is_fallback,
        "batch_no": batch_no,
        "interval_minutes": INTERVAL_MINUTES,
        "count": len(results),
        "data": results,
    })

# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
