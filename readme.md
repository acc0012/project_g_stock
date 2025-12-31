# 📊 Groww Live Candle API (Flask)

# A lightweight Flask API to fetch **live & historical intraday candle data**
from the **Groww Charting API** for NSE cash stocks.

# This service is optimized for:
- Intraday trading dashboards
- React / frontend consumption
- Railway / server deployments
- Batch-based stock processing

# ---

# ## 🚀 Features

# - Fetch **full-day intraday candles**
- Fetch **latest 5-minute window candles**
- Automatic **holiday & weekend fallback**
- Parallel API fetching using ThreadPoolExecutor
- Batch-wise stock processing (scalable)
- IST-aware market timings
- Simple REST APIs (JSON only)

# ---

# ## 🗂 Project Structure

# project_get_stock/
│
├── app.py # Main Flask API server
├── companies_list.json # Stock symbol master list
├── README.md # Documentation

# 
---

# ## 📦 Requirements

# - Python 3.10+
- Flask
- requests
- flask-compress
- flask-cors

# Install dependencies:

# ```bash
pip install flask requests flask-compress flask-cors

# ▶️ Running the Server
python app.py

# 
Server starts at:

# http://localhost:5000

# ⏰ Market Configuration

# Market Open: 09:00 IST

# Market Close: 15:30 IST

# Candle Interval: 3 minutes

# Latest Window: Last 5 minutes

# Timezone: IST (UTC+5:30)

# 📘 API Endpoints
1️⃣ Health Check

# Request

# GET /

# 
Response

# {
  "status": "ok",
  "message": "Server running fine 🚀",
  "time": "2025-01-01 10:15:22"
}

# 2️⃣ Get All Stock Symbols

# Returns all NSE symbols from companies_list.json.

# Request

# GET /api/symbols

# 
Response

# {
  "status": "ok",
  "count": 3,
  "symbols": ["AXISBANK", "INFY", "TITAN"]
}

# 3️⃣ Fetch Full Day Candles (Batch-wise)

# Returns all intraday candles for the selected batch.

# Request

# GET /api/live-candles

# 
Response (sample)

# {
  "mode": "full",
  "fetched_date": "2025-01-01",
  "interval_minutes": 3,
  "count": 25,
  "start_time": "09:00:00",
  "end_time": "15:27:00",
  "data": {
    "TITAN": [
      [1735711800, 3520, 3530, 3510, 3525, 12000],
      ...
    ]
  }
}

# 4️⃣ Fetch Latest 5-Minute Candles (NEW LOGIC)

# Automatically calculates required candles based on interval.

# Request

# GET /api/live-candles?latest=true

# 
Response

# {
  "mode": "latest",
  "latest_window_minutes": 5,
  "interval_minutes": 3,
  "start_time": "13:42:00",
  "end_time": "13:45:00",
  "data": {
    "INFY": [
      [1735738320, 1562, 1564, 1560, 1563, 8200],
      [1735738500, 1563, 1565, 1562, 1564, 9100]
    ]
  }
}

# 5️⃣ Fetch Candles for a Specific Date

# If data is unavailable (holiday/weekend), API auto-falls back.

# Request

# GET /api/live-candles?date=2024-12-25

# 
Response

# {
  "requested_date": "2024-12-25",
  "fetched_date": "2024-12-24",
  "is_fallback": true
}

# 🔁 Batch Processing Logic

# Stocks are divided into batches for scalability.

# Environment variable:

# BATCH_NUM=1

# 
Example:

# TOTAL_BATCHES = 10

# If 1000 stocks → 100 per batch

# 🧠 Candle Timestamp Format

# Groww API candle format:

# [
  timestamp_in_seconds,
  open,
  high,
  low,
  close,
  volume
]

# 
Converted internally to IST for display.

# 🛡 Reliability Features

# Retry logic (3 attempts)

# Timeout protection

# Holiday/weekend fallback

# Safe parallel execution

# CORS enabled for frontend apps

# 🌍 Deployment Ready

# This app works perfectly with:

# Railway

# Render

# VPS

# Docker

# GitHub Actions

# Just expose port 5000.

# 📌 Example CURL Commands
curl http://localhost:5000/api/symbols

# curl "http://localhost:5000/api/live-candles?latest=true"

# curl "http://localhost:5000/api/live-candles?date=2025-01-01"
