"""
flow_physicist.py ─ Agent 2
Listens for: "live_iot_stream", "mock_surge"
Outputs: Flux scalars to DB + "critical_status" events to bus.

Thresholds are loaded dynamically from `stadium_thresholds` table so
admins can tune them via the dashboard without restarting the server.
"""
from __future__ import annotations
import asyncio
import time
from typing import Dict, Any

from backend.database import get_conn

_bus: Dict[str, asyncio.Queue] = {}
_node_capacities: Dict[str, Dict[str, int]] = {}   # {sid: {node_id: capacity}}
_latest_metrics: Dict[str, Dict[str, Any]] = {}     # {sid: {node_id: metric}}
STATUS = {"state": "IDLE"}

# Default fallback thresholds (used if no DB row exists yet)
DEFAULT_THRESHOLDS = {
    "EMPTY":    0.15,
    "NORMAL":   0.40,
    "BUSY":     0.55,
    "CRITICAL": 0.75,
}

# Cache thresholds per stadium; refresh every N ticks
_threshold_cache: Dict[str, Dict[str, float]] = {}
_threshold_last_refresh: Dict[str, float] = {}
THRESHOLD_TTL = 15.0   # seconds between DB reads


def init(bus: Dict[str, asyncio.Queue], capacities: Dict[str, Dict[str, int]]):
    global _bus, _node_capacities
    _bus = bus
    _node_capacities = capacities


def _load_thresholds(stadium_id: str) -> Dict[str, float]:
    """Return thresholds for a stadium, refreshing from DB if TTL expired."""
    now = time.monotonic()
    if now - _threshold_last_refresh.get(stadium_id, 0) < THRESHOLD_TTL:
        return _threshold_cache.get(stadium_id, DEFAULT_THRESHOLDS)

    try:
        conn = get_conn()
        row = conn.execute(
            "SELECT * FROM stadium_thresholds WHERE stadium_id=?", (stadium_id,)
        ).fetchone()
        conn.close()
        if row:
            t = {
                "EMPTY":    row["empty_threshold"],
                "NORMAL":   row["green_threshold"],
                "BUSY":     row["busy_threshold"],
                "CRITICAL": row["critical_threshold"],
            }
        else:
            t = DEFAULT_THRESHOLDS.copy()
    except Exception:
        t = DEFAULT_THRESHOLDS.copy()

    _threshold_cache[stadium_id] = t
    _threshold_last_refresh[stadium_id] = now
    return t


def calculate_flux(occupancy: int, capacity: int, flow_rate: float) -> float:
    """J = ρ · v  where ρ = occupancy/capacity, v = flow_rate (normalized)."""
    if capacity <= 0:
        return 0.0
    rho = min(occupancy / capacity, 1.0)
    v = min(flow_rate / max(capacity, 1), 1.0)
    return round(rho * v, 4)


def classify_status(density: float, thresholds: Dict[str, float]) -> str:
    if density < thresholds["EMPTY"]:
        return "EMPTY"
    elif density < thresholds["NORMAL"]:
        return "GREEN"
    elif density < thresholds["BUSY"]:
        return "AMBER"
    elif density < thresholds["CRITICAL"]:
        return "CRITICAL"
    else:
        return "CHOKE"


def mm1_wait_time(mu: float, lam: float) -> float:
    """W = 1 / (μ − λ), capped for stability."""
    if mu <= lam or mu <= 0:
        return 99.0   # effectively infinite wait
    return round(1.0 / (mu - lam), 2)


async def _persist_metric(stadium_id: str, node_id: str, occ: int, flux: float, status: str):
    conn = get_conn()
    conn.execute(
        "INSERT INTO flow_metrics (node_id,stadium_id,current_occupancy,flux_value,status) VALUES (?,?,?,?,?)",
        (node_id, stadium_id, occ, flux, status),
    )
    conn.commit()
    conn.close()


async def process_iot_event(event: Dict[str, Any]):
    sid = event["stadium_id"]
    occupancy_map: Dict[str, int] = event["occupancy"]
    caps = _node_capacities.get(sid, {})
    thresholds = _load_thresholds(sid)

    metrics = {}
    critical_nodes = []

    for node_id, occ in occupancy_map.items():
        cap = caps.get(node_id, 1000)
        rho = min(occ / cap, 1.0) if cap > 0 else 0.0
        flow_rate = occ * 0.8   # estimate
        flux = calculate_flux(occ, cap, flow_rate)
        status = classify_status(rho, thresholds)

        metrics[node_id] = {
            "node_id": node_id,
            "capacity": cap,
            "current_occupancy": occ,
            "density": round(rho, 4),
            "flux": flux,
            "status": status,
        }

        # Persist to time-series (every other tick to avoid DB flood)
        if event.get("tick", 0) % 2 == 0:
            await _persist_metric(sid, node_id, occ, flux, status)

        if status in ("CRITICAL", "CHOKE"):
            critical_nodes.append(node_id)

    _latest_metrics[sid] = metrics

    # Broadcast heatmap update (include live thresholds so UI can read them)
    await _bus["ws_broadcast"].put({
        "type": "heatmap_update",
        "stadium_id": sid,
        "metrics": metrics,
        "thresholds": thresholds,
        "timestamp": event.get("timestamp", time.time()),
        "sim_time": event.get("sim_time"),
        "state": event.get("state", "UNKNOWN"),
        "time_mult": event.get("time_mult", 1.0)
    })

    # Trigger AI Brain for critical nodes
    if critical_nodes:
        await _bus["critical_status"].put({
            "stadium_id": sid,
            "critical_nodes": critical_nodes,
            "metrics": metrics,
        })


async def run():
    STATUS["state"] = "ACTIVE"
    while True:
        event = await _bus["live_iot_stream"].get()
        await process_iot_event(event)


def get_latest_metrics(stadium_id: str) -> Dict[str, Any]:
    return _latest_metrics.get(stadium_id, {})


def get_thresholds(stadium_id: str) -> Dict[str, float]:
    """Expose cached thresholds for the REST endpoint."""
    return _load_thresholds(stadium_id)
