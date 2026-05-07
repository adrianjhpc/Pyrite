import os
import time
import json
import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Grab the DB URL from Docker Compose, default to localhost for local testing
DB_URL = os.getenv("DB_URL", "http://localhost:8428")

@app.get("/api/time")
async def get_time():
    return {"current_db_time": time.time()}

@app.get("/api/topology")
async def get_topology():
    try:
        with open("cluster_topology.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"hardware_blueprint": {}, "topology": [], "metadata": {"program": "Live Run", "system_name": "Cluster"}}

@app.get("/api/events")
async def get_events(start: float, end: float):
    start_ms = int(start * 1000)
    end_ms = int(end * 1000)
    query_url = f"{DB_URL}/api/v1/export"
    params = {"match[]": "mpi_call", "start": start_ms, "end": end_ms}

    formatted_events = []

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(query_url, params=params, timeout=5.0)
            if response.status_code != 200:
                return []

            lines = response.text.strip().split('\n')
            for line in lines:
                if not line: continue
                db_event = json.loads(line)
                
                metric = db_event.get("metric", {})
                values = db_event.get("values", [])
                timestamps = db_event.get("timestamps", [])

                for i in range(len(values)):
                    event = {
                        "time": timestamps[i] / 1000.0, 
                        "message_type": int(metric.get("type", 0)),
                        "call": int(metric.get("type", 0)), 
                        "sender": int(metric.get("sender", metric.get("sender1", 0))),
                        "receiver": int(metric.get("receiver", metric.get("receiver1", 0)))
                    }

                    if "bytes2" in metric:
                        event["is_large"] = 1
                        event["bytes"] = int(metric.get("bytes1", 0))
                        event["count"] = int(metric.get("count1", 0))
                        event["bytes2"] = int(metric.get("bytes2", 0))
                        event["count2"] = int(metric.get("count2", 0))
                    else:
                        event["is_large"] = 0
                        event["bytes"] = int(metric.get("bytes", 0))
                        event["count"] = int(metric.get("count", 0))

                    formatted_events.append(event)

        except Exception as e:
            print(f"Database query failed: {e}")
            return []

    return formatted_events
