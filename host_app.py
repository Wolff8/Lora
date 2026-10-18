#!/usr/bin/env python3
"""
Železniški Telemetrijski API Host App (FastAPI + TCP MQTT + MÁV EMMA)
Zagon: uvicorn host_app:app --host 0.0.0.0 --port 8000
"""

import asyncio
import json
import math
import time
import threading
from collections import deque
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
import paho.mqtt.client as mqtt
import urllib.request

# --- INICIALIZACIJA FASTAPI APLIKACIJE ---
app = FastAPI(
    title="RailFreight SI - Telemetry API Host",
    description="REST in pretočni API za spremljanje železniškega koridorja Proga 41 (Lipovci - Hodoš) in mejnih tokov.",
    version="1.0.0"
)

# Omogočimo CORS za vse odjemalce
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- GEODETSKA MREŽA PROGE 41 ---
LINE_41_WAYPOINTS = [
    {"name": "Postaja Lipovci (izogibališče)", "km": 19.21, "lat": 46.62934, "lon": 16.19792},
    {"name": "Nivojski prehod Lipovci-Bratonci", "km": 20.45, "lat": 46.63581, "lon": 16.19245},
    {"name": "Nivojski prehod Rakičan", "km": 21.80, "lat": 46.64320, "lon": 16.18520},
    {"name": "Uvozni signal Murska Sobota jug", "km": 23.40, "lat": 46.65480, "lon": 16.17420},
    {"name": "ŽP Murska Sobota (Glavna)", "km": 24.30, "lat": 46.66250, "lon": 16.16850},
    {"name": "Industrijski tir M. Sobota sever", "km": 25.60, "lat": 46.67340, "lon": 16.16280},
    {"name": "Postaja Puconci", "km": 29.80, "lat": 46.70420, "lon": 16.15540},
    {"name": "Postajališče Dankovci", "km": 34.10, "lat": 46.73890, "lon": 16.16780},
    {"name": "Postaja Mačkovci", "km": 38.20, "lat": 46.77210, "lon": 16.17240},
    {"name": "Predor Cankova/Mačkovci", "km": 40.50, "lat": 46.79120, "lon": 16.17980},
    {"name": "Postajališče Petrovci", "km": 44.90, "lat": 46.82450, "lon": 16.19210},
    {"name": "Postaja Hodoš (Tir 2/3 tovorni)", "km": 52.80, "lat": 46.89240, "lon": 16.32890},
    {"name": "Državna meja SI/HU (Hodoš meja)", "km": 54.10, "lat": 46.89910, "lon": 16.34580}
]

# --- GLOBALNO STANJE IN MEDKORILNIKI ---
class SystemState:
    mqtt_connected: bool = False
    mqtt_packets_received: int = 0
    start_time: float = time.time()
    recent_rail_hits: deque = deque(maxlen=100)       # Zadnjih 100 dogodkov ob tirih
    latest_mav_trains: List[Dict[str, Any]] = []      # Zadnji podatki MÁV
    sse_subscribers: List[asyncio.Queue] = []         # Čakalne vrste za živi tok

state = SystemState()

# --- MATEMATIČNI FILTER RAZDALJE DO TIRA ---
def distance_point_to_segment(p_lat, p_lon, a_lat, a_lon, b_lat, b_lon):
    lat_mid = math.radians((a_lat + b_lat) / 2.0)
    cos_lat = math.cos(lat_mid)
    
    px, py = p_lon * 111320.0 * cos_lat, p_lat * 110540.0
    ax, ay = a_lon * 111320.0 * cos_lat, a_lat * 110540.0
    bx, by = b_lon * 111320.0 * cos_lat, b_lat * 110540.0

    dx, dy = bx - ax, by - ay
    if dx == 0 and dy == 0:
        return math.hypot(px - ax, py - ay), 0.0

    t = ((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)
    t = max(0.0, min(1.0, t))
    proj_x, proj_y = ax + t * dx, ay + t * dy
    return math.hypot(px - proj_x, py - proj_y), t

def check_rail_proximity(lat: float, lon: float, max_dist_m: float = 120.0):
    min_dist = float("inf")
    closest_hit = None

    for i in range(len(LINE_41_WAYPOINTS) - 1):
        wp_a, wp_b = LINE_41_WAYPOINTS[i], LINE_41_WAYPOINTS[i + 1]
        dist, t = distance_point_to_segment(lat, lon, wp_a["lat"], wp_a["lon"], wp_b["lat"], wp_b["lon"])
        if dist < min_dist:
            min_dist = dist
            est_km = wp_a["km"] + t * (wp_b["km"] - wp_a["km"])
            closest_hit = {
                "distance_m": round(dist, 1),
                "est_km": round(est_km, 2),
                "segment": f"{wp_a['name']} → {wp_b['name']}",
                "ref_station": wp_a["name"]
            }

    if min_dist <= max_dist_m:
        return closest_hit
    return None

def publish_event(event_type: str, payload: dict):
    """Pošlje dogodek vsem aktivnim SSE poslušalcem."""
    msg = {"event": event_type, "data": payload, "timestamp": time.time()}
    for queue in state.sse_subscribers[:]:
        try:
            queue.put_nowait(msg)
        except Exception:
            state.sse_subscribers.remove(queue)

# --- 1. TCP MQTT ZALEDNI ODJEMALEC (paho-mqtt) ---
def mqtt_thread_worker():
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            state.mqtt_connected = True
            client.subscribe("si/meshnet/slovenia/#")
            client.subscribe("si/meshnet/#")
            client.subscribe("msh/EU_868/#")
        else:
            state.mqtt_connected = False

    def on_disconnect(client, userdata, rc):
        state.mqtt_connected = False

    def on_message(client, userdata, msg):
        state.mqtt_packets_received += 1
        try:
            payload_raw = msg.payload.decode("utf-8", errors="ignore")
            data = json.loads(payload_raw)
            p = data.get("payload", data)
            
            lat = p.get("latitude") or (p.get("latitude_i", 0) / 1e7)
            lon = p.get("longitude") or (p.get("longitude_i", 0) / 1e7)
            speed = p.get("ground_speed", 0)
            node_id = str(data.get("from", data.get("sender", "Neznano")))

            if lat != 0 and lon != 0:
                hit = check_rail_proximity(lat, lon, max_dist_m=120.0)
                if hit:
                    record = {
                        "node_id": node_id,
                        "lat": lat,
                        "lon": lon,
                        "speed_kmh": round(speed * 3.6, 1) if speed < 100 else speed,
                        "distance_to_track_m": hit["distance_m"],
                        "rail_km": hit["est_km"],
                        "segment": hit["segment"],
                        "source": "TCP_MQTT_1883",
                        "time": time.strftime("%H:%M:%S")
                    }
                    state.recent_rail_hits.appendleft(record)
                    publish_event("rail_hit", record)
        except Exception:
            pass

    while True:
        try:
            client = mqtt.Client(client_id=f"HostAppBridge_{int(time.time())}", protocol=mqtt.MQTTv311)
            client.username_pw_set("slovenia", "meshnet-si-slovenia")
            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            client.on_message = on_message
            client.connect("mqtt.meshnet.si", 1883, keepalive=60)
            client.loop_forever()
        except Exception:
            state.mqtt_connected = False
            time.sleep(5)

# --- 2. MÁV EMMA GRAPHQL ZALEDNI ZAJEMALEC ---
def mav_thread_worker():
    url = "https://emma.mav.hu/otp2-backend/otp/routers/default/index/graphql"
    query = """
    {
      vehiclePositions(
        swLat: 46.70, swLon: 16.20,
        neLat: 47.05, neLon: 16.95,
        modes: [RAIL]
      ) {
        vehicleId
        lat
        lon
        speed
        heading
        label
      }
    }
    """
    req_body = json.dumps({"query": query}).encode("utf-8")
    headers = {"Content-Type": "application/json", "Origin": "https://emma.mav.hu"}

    while True:
        try:
            req = urllib.request.Request(url, data=req_body, headers=headers, method="POST")
            with urllib.request.urlopen(req, timeout=10) as resp:
                if resp.status == 200:
                    res = json.loads(resp.read().decode("utf-8"))
                    vehicles = res.get("data", {}).get("vehiclePositions", [])
                    trains = []
                    for v in vehicles:
                        trains.append({
                            "vehicle_id": v.get("vehicleId"),
                            "label": v.get("label") or "MÁV Vlak",
                            "lat": v.get("lat"),
                            "lon": v.get("lon"),
                            "speed": v.get("speed", 0),
                            "heading": v.get("heading", 0),
                            "timestamp": time.time()
                        })
                    state.latest_mav_trains = trains
                    if trains:
                        publish_event("mav_update", {"trains": trains, "count": len(trains)})
        except Exception:
            pass
        time.sleep(15)

# Zagon zalednih niti ob zagonu aplikacije
@app.on_event("startup")
def startup_event():
    t1 = threading.Thread(target=mqtt_thread_worker, daemon=True)
    t1.start()
    t2 = threading.Thread(target=mav_thread_worker, daemon=True)
    t2.start()

# --- MODELI PODATKOV ZA API (Pydantic) ---
class IngestTelemetryModel(BaseModel):
    node_id: str
    lat: float
    lon: float
    speed: Optional[float] = 0.0
    battery: Optional[int] = None
    sensor_note: Optional[str] = "Wayside LoRa"

# --- 3. DEFINICIJA API KONČNIH TOČK (ENDPOINTS) ---

@app.get("/api/v1/health", tags=["Sistem"])
def get_health():
    """Diagnostika stanja: stanje TCP MQTT, prejeti paketi in čas delovanja."""
    return {
        "status": "online",
        "uptime_seconds": round(time.time() - state.start_time, 1),
        "tcp_mqtt_connected": state.mqtt_connected,
        "mqtt_packets_received": state.mqtt_packets_received,
        "active_sse_clients": len(state.sse_subscribers),
        "cached_rail_hits": len(state.recent_rail_hits),
        "active_border_mav_trains": len(state.latest_mav_trains)
    }

@app.get("/api/v1/rail/hits", tags=["Proga 41"])
def get_rail_hits(limit: int = 20):
    """Vrne zadnje zaznane pakete/vozlišča ob tirih proge 41 (privzeto zadnjih 20)."""
    hits = list(state.recent_rail_hits)[:limit]
    return {
        "count": len(hits),
        "data": hits
    }

@app.get("/api/v1/mav/border", tags=["Mejni promet"])
def get_mav_border_trains():
    """Vrne trenutno aktivne vlake v madžarskem mejnem koridorju (MÁV EMMA)."""
    return {
        "source": "MÁV EMMA GraphQL",
        "sector": "Hodoš - Bajánsenye - Őriszentpéter",
        "count": len(state.latest_mav_trains),
        "trains": state.latest_mav_trains
    }

@app.post("/api/v1/telemetry/ingest", tags=["Senzorika ob progi"])
def ingest_telemetry(payload: IngestTelemetryModel):
    """
    Sprejemni endpoint za obtirne senzorje (ESP32, LoRa prehod ali ročni vnos).
    Samodejno preveri odmik od proge 41 in ob zadetku sproži alarm v sistemu.
    """
    hit = check_rail_proximity(payload.lat, payload.lon, max_dist_m=150.0)
    result = {
        "node_id": payload.node_id,
        "lat": payload.lat,
        "lon": payload.lon,
        "is_on_track": hit is not None,
        "proximity": hit
    }
    
    if hit:
        record = {
            "node_id": payload.node_id,
            "lat": payload.lat,
            "lon": payload.lon,
            "speed_kmh": payload.speed,
            "distance_to_track_m": hit["distance_m"],
            "rail_km": hit["est_km"],
            "segment": hit["segment"],
            "source": f"HTTP_INGEST ({payload.sensor_note})",
            "time": time.strftime("%H:%M:%S")
        }
        state.recent_rail_hits.appendleft(record)
        publish_event("rail_hit", record)
        
    return JSONResponse(status_code=status.HTTP_201_CREATED, content=result)

@app.get("/api/v1/rail/stream", tags=["Pretok v živo"])
async def stream_rail_telemetry(request: Request):
    """
    Živi pretočni kanal (Server-Sent Events - SSE).
    Vsak odjemalec (brskalnik, dashboard) prejme dogodek takoj, ko se zgodi.
    """
    queue = asyncio.Queue()
    state.sse_subscribers.append(queue)

    async def event_generator():
        try:
            # Pošlji začetni pozdrav
            yield f"event: connected\ndata: {json.dumps({'message': 'Povezan na RailFreight API SSE'})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                event = await queue.get()
                yield f"event: {event['event']}\ndata: {json.dumps(event['data'])}\n\n"
        finally:
            if queue in state.sse_subscribers:
                state.sse_subscribers.remove(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*"
        }
    )

if __name__ == "__main__":
    import uvicorn
    print("ZAGANJAM TELEMETRIJSKI FASTAPI HOST APP NA http://0.0.0.0:8000")
    print("Interaktivna Swagger dokumentacija: http://localhost:8000/docs")
    uvicorn.run("host_app:app", host="0.0.0.0", port=8000, reload=False)
