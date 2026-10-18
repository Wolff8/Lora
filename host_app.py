#!/usr/bin/env python3
"""
RailFreight SI - Celotni zajem brez filtrov z geofencingom 100 m ob progi 41
FastAPI + TCP MQTT (1883) + MÁV EMMA + Univerzalni SSE pretočni strežnik
"""

import asyncio
import json
import math
import re
import time
import threading
from collections import deque
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse, RedirectResponse
from pydantic import BaseModel
import paho.mqtt.client as mqtt
import urllib.request

app = FastAPI(
    title="RailFreight SI - 100m Corridor Sniffer",
    description="Zajem vseh surovih radijskih paketov z alarmom ob tirih proge 41 (toleranca 100 m).",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- GEODETSKA MREŽA PROGE 41 (OS TIRA) ---
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

# --- GLOBALNO STANJE ---
class SystemState:
    mqtt_connected: bool = False
    mqtt_packets_received: int = 0
    start_time: float = time.time()
    recent_raw_packets: deque = deque(maxlen=50)      # Zadnjih 50 surovih paketov brez filtrov
    recent_rail_hits: deque = deque(maxlen=100)       # Zadetki znotraj 100 m od tira
    latest_mav_trains: List[Dict[str, Any]] = []
    sse_subscribers: List[asyncio.Queue] = []

state = SystemState()

# --- MATEMATIČNI FILTER 100 METROV OD TIRA ---
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

def check_rail_corridor_100m(lat: float, lon: float):
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

    # Striktna meja: točno 100 metrov
    if min_dist <= 100.0:
        return closest_hit
    return None

# --- UNIVERZALNO ISKANJE KOORDINAT V KATEREMKOLI PAKETU ---
def find_coordinates(data_obj, raw_text=""):
    lat, lon = None, None

    def search_dict(d):
        nonlocal lat, lon
        if not isinstance(d, dict):
            return
        for k, v in d.items():
            k_lower = k.lower()
            if k_lower in ("latitude", "lat") and isinstance(v, (int, float)):
                lat = float(v)
            elif k_lower in ("longitude", "lon", "lng") and isinstance(v, (int, float)):
                lon = float(v)
            elif k_lower in ("latitude_i", "lat_i") and isinstance(v, int):
                lat = v / 1e7
            elif k_lower in ("longitude_i", "lon_i") and isinstance(v, int):
                lon = v / 1e7
            elif isinstance(v, dict):
                search_dict(v)

    if isinstance(data_obj, dict):
        search_dict(data_obj)

    # Regex rezerva, če je JSON poškodovan ali nestandarden
    if lat is None and raw_text:
        m_lati = re.search(r'"latitude_i"\s*:\s*(-?\d+)', raw_text)
        m_loni = re.search(r'"longitude_i"\s*:\s*(-?\d+)', raw_text)
        if m_lati and m_loni:
            lat = int(m_lati.group(1)) / 1e7
            lon = int(m_loni.group(1)) / 1e7
        else:
            m_lat = re.search(r'"(?:lat|latitude)"\s*:\s*(-?\d+\.\d+)', raw_text)
            m_lon = re.search(r'"(?:lon|longitude|lng)"\s*:\s*(-?\d+\.\d+)', raw_text)
            if m_lat and m_lon:
                lat = float(m_lat.group(1))
                lon = float(m_lon.group(1))

    return lat, lon

def publish_event(event_type: str, payload: dict):
    msg = {"event": event_type, "data": payload, "timestamp": time.time()}
    for queue in state.sse_subscribers[:]:
        try:
            queue.put_nowait(msg)
        except Exception:
            state.sse_subscribers.remove(queue)

# --- 1. TCP MQTT KLIENT (BREZ FILTROV) ---
def mqtt_thread_worker():
    def on_connect(client, userdata, flags, rc, properties=None):
        if rc == 0:
            state.mqtt_connected = True
            # Naročnina na VSE teme brez filtrov
            client.subscribe("#")
        else:
            state.mqtt_connected = False

    def on_disconnect(client, userdata, rc):
        state.mqtt_connected = False

    def on_message(client, userdata, msg):
        state.mqtt_packets_received += 1
        raw_payload = msg.payload.decode("utf-8", errors="ignore")
        topic = msg.topic
        
        parsed_json = None
        try:
            parsed_json = json.loads(raw_payload)
        except Exception:
            parsed_json = None

        node_id = "Neznano"
        if isinstance(parsed_json, dict):
            node_id = str(parsed_json.get("from") or parsed_json.get("sender") or parsed_json.get("id") or "Node")

        # 1. Pošlji surovi paket v živo vsem naročnikom (brez filtrov)
        raw_entry = {
            "id": state.mqtt_packets_received,
            "topic": topic,
            "node_id": node_id,
            "payload_snippet": raw_payload[:180],
            "time": time.strftime("%H:%M:%S")
        }
        state.recent_raw_packets.appendleft(raw_entry)
        publish_event("raw_packet", raw_entry)

        # 2. Poišči koordinate kjerkoli v paketu
        lat, lon = find_coordinates(parsed_json, raw_payload)
        
        # 3. Edini filter: ali je znotraj 100 m od proge 41
        if lat is not None and lon is not None and lat != 0 and lon != 0:
            hit = check_rail_corridor_100m(lat, lon)
            if hit:
                record = {
                    "node_id": node_id,
                    "lat": lat,
                    "lon": lon,
                    "distance_to_track_m": hit["distance_m"],
                    "rail_km": hit["est_km"],
                    "segment": hit["segment"],
                    "ref_station": hit["ref_station"],
                    "topic": topic,
                    "time": time.strftime("%H:%M:%S")
                }
                state.recent_rail_hits.appendleft(record)
                publish_event("rail_hit", record)

    while True:
        try:
            client = mqtt.Client(client_id=f"Host100m_{int(time.time())}", protocol=mqtt.MQTTv311)
            client.username_pw_set("slovenia", "meshnet-si-slovenia")
            client.on_connect = on_connect
            client.on_disconnect = on_disconnect
            client.on_message = on_message
            client.connect("mqtt.meshnet.si", 1883, keepalive=60)
            client.loop_forever()
        except Exception:
            state.mqtt_connected = False
            time.sleep(5)

# --- 2. MÁV EMMA GRAPHQL NALOGA ---
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

@app.on_event("startup")
def startup_event():
    threading.Thread(target=mqtt_thread_worker, daemon=True).start()
    threading.Thread(target=mav_thread_worker, daemon=True).start()

# --- 3. ENDPOINTI ---

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse(url="/docs")

@app.get("/api/v1/health", tags=["Sistem"])
def get_health():
    return {
        "status": "online",
        "uptime_seconds": round(time.time() - state.start_time, 1),
        "tcp_mqtt_connected": state.mqtt_connected,
        "mqtt_packets_received": state.mqtt_packets_received,
        "active_sse_clients": len(state.sse_subscribers),
        "cached_rail_hits_100m": len(state.recent_rail_hits),
        "recent_raw_packets_count": len(state.recent_raw_packets),
        "active_border_mav_trains": len(state.latest_mav_trains)
    }

@app.get("/api/v1/raw/packets", tags=["Surovi podatki"])
def get_raw_packets(limit: int = 50):
    """Vrne zadnje surove pakete brez kakršnihkoli filtrov."""
    return {
        "total_received": state.mqtt_packets_received,
        "count": min(limit, len(state.recent_raw_packets)),
        "data": list(state.recent_raw_packets)[:limit]
    }

@app.get("/api/v1/rail/hits", tags=["Proga 41 (100 m)"])
def get_rail_hits(limit: int = 20):
    """Vrne izključno pakete, ki so bili zaznani točno v pasu 100 m od osi tira."""
    hits = list(state.recent_rail_hits)[:limit]
    return {
        "filter": "max_distance_100m",
        "count": len(hits),
        "data": hits
    }

@app.get("/api/v1/rail/stream", tags=["Pretok v živo"])
async def stream_telemetry(request: Request):
    """SSE tok vseh surovih paketov in alarmov 100m koridorja."""
    queue = asyncio.Queue()
    state.sse_subscribers.append(queue)

    async def event_generator():
        try:
            yield f"event: connected\ndata: {json.dumps({'status': 'Povezan na 100m progovni tok'})}\n\n"
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
    uvicorn.run("host_app:app", host="0.0.0.0", port=8000, reload=False)
