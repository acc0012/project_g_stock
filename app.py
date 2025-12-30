import json,os
import time
import requests
from datetime import datetime, timedelta, timezone, time as dtime
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, jsonify, request
from flask_compress import Compress
from flask_cors import CORS   # ✅ ADDED

# =====================================================
# CONFIG
# =====================================================
COMPANY_FILE = "companies_list.json"

GROWW_URL = (
    "https://groww.in/v1/api/charting_service/v2/chart/"
    "delayed/exchange/NSE/segment/CASH"
)

INTERVAL_MINUTES = 3
MAX_WORKERS = 100
MAX_RETRIES = 3
TIMEOUT = 20

TOTAL_BATCHES = 10
BATCH_NO = int(os.getenv("BATCH_NUM",1))        # 👈 CHANGE THIS (1-based index)

IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN = dtime(9, 0)
MARKET_CLOSE = dtime(15, 30)

# =====================================================
# FLASK
# =====================================================
app = Flask(__name__)
Compress(app)
CORS(app, resources={r"/api/*": {"origins": "*"}})   # ✅ ADDED

# =====================================================
# TIME HELPERS
# =====================================================
def to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def market_range_for_date(date_str: str | None):
    now = datetime.now(IST)

    if date_str:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        start = d.replace(hour=9, minute=0, tzinfo=IST)
        end = d.replace(hour=15, minute=30, tzinfo=IST)
        return to_ms(start), to_ms(end)

    start = now.replace(hour=9, minute=0, second=0, microsecond=0)

    if now.time() < MARKET_CLOSE:
        end = now
    else:
        end = now.replace(hour=15, minute=30, second=0, microsecond=0)

    return to_ms(start), to_ms(end)

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
            r = requests.get(
                url,
                params=params,
                headers=headers,
                timeout=TIMEOUT,
            )
            r.raise_for_status()

            data = r.json()
            return data.get("candles", [])

        except Exception:
            if attempt == MAX_RETRIES:
                return []

            time.sleep(1)
# =====================================================
# HOME / HEALTH CHECK
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
    date = request.args.get("date")  # YYYY-MM-DD optional

    with open(COMPANY_FILE, "r") as f:
        data = json.load(f)

    total_items = len(data)
    batch_size = max(1, total_items // TOTAL_BATCHES)

    max_batch_no = (total_items + batch_size - 1) // batch_size
    batch_no = min(BATCH_NO, max_batch_no)

    start_idx = (batch_no - 1) * batch_size
    end_idx = min(start_idx + batch_size, total_items)

    batch = data[start_idx:end_idx]

    start_ms, end_ms = market_range_for_date(date)

    results = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {}

        for item in batch:
            symbol = item.split("__")[0].strip()
            futures[
                executor.submit(fetch_candles, symbol, start_ms, end_ms)
            ] = symbol

        for future in as_completed(futures):
            symbol = futures[future]
            results[symbol] = future.result()

    return jsonify({
        "batch_no": batch_no,
        "total_batches": max_batch_no,
        "date": date or datetime.now(IST).strftime("%Y-%m-%d"),
        "start_ms": start_ms,
        "end_ms": end_ms,
        "interval_minutes": INTERVAL_MINUTES,
        "count": len(results),
        "data": results,
    })

# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, threaded=True)
