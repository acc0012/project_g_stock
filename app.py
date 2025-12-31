import json, os, time, asyncio
from datetime import datetime, timedelta, timezone, time as dtime

import aiohttp
from flask import Flask, jsonify
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

SIGNALS_URL = "https://project-get-entry.vercel.app/api/signals"

INTERVAL_MINUTES = 3
MAX_WORKERS = 100
TIMEOUT = 20

TOTAL_BATCHES = 10
BATCH_NO = int(os.getenv("BATCH_NUM", 1))

IST = timezone(timedelta(hours=5, minutes=30))
MARKET_OPEN = dtime(9, 0)

# =====================================================
# FLASK
# =====================================================
app = Flask(__name__)
Compress(app)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# =====================================================
# TIME HELPERS
# =====================================================
def to_ms(dt):
    return int(dt.timestamp() * 1000)

def market_start_today():
    now = datetime.now(IST)
    return to_ms(now.replace(hour=9, minute=0, second=0, microsecond=0))

# =====================================================
# ASYNC FETCH
# =====================================================
async def fetch_candles(session, symbol, start_ms, end_ms):
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

    try:
        async with session.get(url, params=params, headers=headers, timeout=TIMEOUT) as r:
            if r.status == 200:
                data = await r.json()
                return symbol, data.get("candles", [])
    except Exception:
        pass

    return symbol, []

async def fetch_all_candles(symbols, start_ms, end_ms):
    connector = aiohttp.TCPConnector(limit=MAX_WORKERS)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [
            fetch_candles(session, s, start_ms, end_ms)
            for s in symbols
        ]
        return await asyncio.gather(*tasks)

# =====================================================
# SIGNALS FETCH
# =====================================================
def fetch_signals():
    import requests
    try:
        r = requests.get(SIGNALS_URL, timeout=20)
        r.raise_for_status()
        return r.json().get("data", [])
    except Exception:
        return []

# =====================================================
# ANALYSIS LOGIC
# =====================================================
def analyze_trade(candles, signal):
    open_price = signal["open"]
    target = signal["target"]
    stoploss = signal["stoploss"]

    entered = False
    entry_time = None

    for ts, o, h, l, c, v in candles:
        t = datetime.fromtimestamp(ts, IST).strftime("%H:%M:%S")

        if not entered and h > open_price:
            entered = True
            entry_time = t

        if entered:
            if h >= target:
                return "EXITED_TARGET", entry_time, t, target
            if l <= stoploss:
                return "EXITED_SL", entry_time, t, stoploss

    if entered:
        return "ENTERED", entry_time, None, None

    return "NOT_ENTERED", None, None, None

# =====================================================
# ROUTES
# =====================================================
@app.route("/")
def home():
    return jsonify({
        "status": "ok",
        "time": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    })

@app.route("/api/analyze-signals")
def analyze_signals():
    start_clock = time.perf_counter()

    # ---------------------------
    # Load signals
    # ---------------------------
    signals = fetch_signals()
    signal_map = {s["symbol"]: s for s in signals}

    # ---------------------------
    # Load batch symbols
    # ---------------------------
    with open(COMPANY_FILE) as f:
        companies = json.load(f)

    symbols = [c.split("__")[0].strip() for c in companies if "__" in c]

    total = len(symbols)
    batch_size = max(1, total // TOTAL_BATCHES)

    batch_no = min(BATCH_NO, (total + batch_size - 1) // batch_size)
    start_i = (batch_no - 1) * batch_size
    end_i = min(start_i + batch_size, total)

    batch_symbols = [s for s in symbols[start_i:end_i] if s in signal_map]

    # ---------------------------
    # Candle range (last 45 mins)
    # ---------------------------
    now = datetime.now(IST)
    end_ms = to_ms(now)
    start_ms = to_ms(now - timedelta(minutes=45))

    # ---------------------------
    # Async candle fetch
    # ---------------------------
    candle_results = asyncio.run(
        fetch_all_candles(batch_symbols, start_ms, end_ms)
    )

    results = {}

    for sym, candles in candle_results:
        sig = signal_map[sym]
        status, entry_t, exit_t, hit = analyze_trade(candles, sig)

        results[sym] = {
            **sig,
            "status": status,
            "entry_time": entry_t,
            "exit_time": exit_t,
            "hit": hit
        }

    elapsed = time.perf_counter() - start_clock

    return jsonify({
        "status": "ok",
        "batch_no": batch_no,
        "count": len(results),
        "response_time": {
            "seconds": round(elapsed, 3),
            "milliseconds": int(elapsed * 1000)
        },
        "data": results
    })

# =====================================================
# RUN
# =====================================================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
