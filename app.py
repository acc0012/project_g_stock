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
    "https://groww.in/v1/api/charting_service/v2/chart/"
    "delayed/exchange/NSE/segment/CASH"
)

INTERVAL_MINUTES = 3          # candle interval from Groww
LATEST_WINDOW_MINUTES = 5     # 👈 NEW LOGIC: last 5 minutes

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


def candle_time_range(candles):
    """
    Groww candle timestamps are in SECONDS
    """
    if not candles:
        return None, None

    start_ts = candles[0][0]
    end_ts = candles[-1][0]

    return (
        datetime.fromtimestamp(start_ts, IST).strftime("%H:%M:%S"),
        datetime.fromtimestamp(end_ts, IST).strftime("%H:%M:%S"),
    )


def effective_trade_date(requested_date: str | None):
    now = datetime.now(IST)

    if requested_date:
        base_date = datetime.strptime(requested_date, "%Y-%m-%d")
    else:
        base_date = now - timedelta(days=1) if now.time() < MARKET_OPEN else now

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
# API – SYMBOL LIST
# =====================================================
@app.route("/api/symbols", methods=["GET"])
def get_symbols():
    try:
        with open(COMPANY_FILE) as f:
            companies = json.load(f)

        symbols = sorted({
            item.split("__")[0].strip()
            for item in companies
            if item and "__" in item
        })

        return jsonify({
            "status": "ok",
            "count": len(symbols),
            "symbols": symbols
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# =====================================================
# API – LIVE CANDLES (NEW LOGIC)
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

    candles_needed = max(1, LATEST_WINDOW_MINUTES // INTERVAL_MINUTES)

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

            if latest and candles:
                results[symbol] = candles[-candles_needed:]
            else:
                results[symbol] = candles

    all_candles = [c for v in results.values() for c in v]
    start_time, end_time = candle_time_range(all_candles)

    return jsonify({
        "mode": "latest" if latest else "full",
        "latest_window_minutes": LATEST_WINDOW_MINUTES if latest else None,
        "requested_date": requested_date,
        "fetched_date": fetched_date,
        "is_fallback": is_fallback,
        "batch_no": batch_no,
        "interval_minutes": INTERVAL_MINUTES,
        "count": len(results),
        "start_time": start_time,
        "end_time": end_time,
        "data": results,
    })

# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
