from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime
from typing import Optional

# Load CSV once at startup
df = pd.read_csv("q-fastapi-timeseries-cache.csv", parse_dates=["timestamp"])

# Initialize FastAPI app
app = FastAPI(title="IoT Sensor Analytics Platform")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Simple cache dictionary
cache = {}

@app.get("/stats")
def get_stats(
    response: Response,
    location: Optional[str] = None,
    sensor: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
):
    # Build cache key
    key = (location, sensor, start_date, end_date)

    # Return cached result if exists
    if key in cache:
        response.headers["X-Cache"] = "HIT"
        return cache[key]
    response.headers["X-Cache"] = "MISS"

    # Filter the dataframe
    filtered = df.copy()
    if location:
        filtered = filtered[filtered["location"] == location]
    if sensor:
        filtered = filtered[filtered["sensor"] == sensor]
    if start_date:
        filtered = filtered[filtered["timestamp"] >= pd.to_datetime(start_date)]
    if end_date:
        filtered = filtered[filtered["timestamp"] <= pd.to_datetime(end_date)]

    # Compute stats
    if filtered.empty:
        result = {"stats": {"count": 0, "avg": None, "min": None, "max": None}}
    else:
        result = {
            "stats": {
                "count": int(filtered["value"].count()),
                "avg": round(filtered["value"].mean(), 2),
                "min": round(filtered["value"].min(), 2),
                "max": round(filtered["value"].max(), 2),
            }
        }

    # Cache result
    cache[key] = result
    return result
