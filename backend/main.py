"""
main.py ─ FastAPI application entry point.

Endpoints:
  POST  /auth/employee/login
  POST  /auth/attendee/login
  GET   /stadiums/{sid}/layout
  POST  /stadiums/{sid}/layout
  POST  /stadiums/{sid}/surge
  GET   /stadiums/{sid}/flow-history
  GET   /stadiums/{sid}/tickets
  PATCH /stadiums/{sid}/tickets/{tid}/enter
  POST  /stadiums/{sid}/image-to-json
  GET   /agents/status
  WS    /ws
"""
from __future__ import annotations
import asyncio
import os
import uuid
from pathlib import Path
from typing import Dict, Any, List

from fastapi import (
    FastAPI, WebSocket, WebSocketDisconnect,
    Depends, HTTPException, File, UploadFile, Path as FPath
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

from backend.database import init_db, seed_data, get_conn
from pydantic import BaseModel
from backend.models import (
    EmployeeLoginRequest, AttendeeLoginRequest, TokenResponse,
    LayoutPayload, SurgeRequest, ThresholdUpdate
)

class ScheduleRequest(BaseModel):
    start_time: float
    end_time: float
    deviation_minutes: float

class MockTimeRequest(BaseModel):
    multiplier: float
from backend.auth import (
    authenticate_employee, authenticate_attendee,
    create_access_token, require_employee, require_admin, get_current_user
)
from backend.tools.mcp_tools import save_user_layout, fetch_historical_flow
from backend.tools.mock_iot import init_iot, inject_surge, iot_loop, reset_occupancy, set_schedule, set_time_multiplier
from backend.tools.image_processor import process_stadium_image

import backend.agents.spatial_architect as spatial
import backend.agents.flow_physicist as physicist
import backend.agents.ai_brain as brain
import backend.agents.ux_concierge as concierge


# ── App Setup ──────────────────────────────────────────────────────────────────
app = FastAPI(title="Nexus Arena OS", version="3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIR = Path(__file__).parent.parent / "frontend"
app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


# ── Message Bus ────────────────────────────────────────────────────────────────
bus: Dict[str, asyncio.Queue] = {
    "user_input_layout": asyncio.Queue(),
    "live_iot_stream":   asyncio.Queue(),
    "mock_surge":        asyncio.Queue(),
    "critical_status":   asyncio.Queue(),
    "ws_broadcast":      asyncio.Queue(),
}


# ── WebSocket Manager ──────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws) if hasattr(self.active, "discard") else None
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)


manager = ConnectionManager()


async def broadcast_loop():
    """Drain ws_broadcast queue into all WebSocket clients."""
    while True:
        msg = await bus["ws_broadcast"].get()
        await manager.broadcast(msg)


# ── Startup ────────────────────────────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    init_db()
    seed_data()

    # Load node capacities from DB for agents
    conn = get_conn()
    stadiums = conn.execute("SELECT id FROM stadiums").fetchall()
    stadium_nodes: Dict[str, Dict[str, int]] = {}
    node_meta: Dict[str, Dict[str, Any]] = {}

    for s in stadiums:
        sid = s["id"]
        nodes = conn.execute("SELECT * FROM nodes WHERE stadium_id=?", (sid,)).fetchall()
        caps = {n["id"]: n["capacity"] for n in nodes}
        meta = {n["id"]: {"label": n["label"], "type": n["type"], "capacity": n["capacity"]} for n in nodes}
        stadium_nodes[sid] = caps
        node_meta.update(meta)
    conn.close()

    # Init agents
    spatial.init(bus)
    physicist.init(bus, stadium_nodes)
    brain.init(bus, node_meta)
    concierge.init(bus)
    init_iot(bus, stadium_nodes)

    # Launch background tasks
    asyncio.create_task(spatial.run())
    asyncio.create_task(physicist.run())
    asyncio.create_task(brain.run())
    asyncio.create_task(concierge.run())
    asyncio.create_task(iot_loop())
    asyncio.create_task(broadcast_loop())

    print("\n=== NEXUS ARENA OS — ONLINE ===")
    print("=== http://localhost:8000    ===\n")


# ── HTML Pages ─────────────────────────────────────────────────────────────────
@app.get("/")
async def serve_landing():
    return FileResponse(str(FRONTEND_DIR / "landing.html"))

@app.get("/login")
async def serve_login():
    return FileResponse(str(FRONTEND_DIR / "login.html"))

@app.get("/dashboard")
async def serve_dashboard():
    return FileResponse(str(FRONTEND_DIR / "index.html"))

@app.get("/builder")
async def serve_builder():
    return FileResponse(str(FRONTEND_DIR / "builder.html"))

@app.get("/monitor")
async def serve_monitor():
    return FileResponse(str(FRONTEND_DIR / "monitor.html"))

@app.get("/analytics")
async def serve_analytics():
    return FileResponse(str(FRONTEND_DIR / "analytics.html"))

@app.get("/concierge")
async def serve_concierge():
    return FileResponse(str(FRONTEND_DIR / "concierge.html"))

@app.get("/attendee")
async def serve_attendee():
    return FileResponse(str(FRONTEND_DIR / "attendee.html"))


# ── Auth Endpoints ─────────────────────────────────────────────────────────────
@app.post("/auth/employee/login", response_model=TokenResponse)
async def employee_login(req: EmployeeLoginRequest):
    emp = authenticate_employee(req.email, req.password)
    if not emp:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_access_token({
        "sub": emp["id"], "role": emp["role"],
        "name": emp["name"], "stadium_id": emp["stadium_id"],
    })
    return TokenResponse(access_token=token, role=emp["role"], name=emp["name"], stadium_id=emp["stadium_id"])


@app.post("/auth/attendee/login", response_model=TokenResponse)
async def attendee_login(req: AttendeeLoginRequest):
    ticket = authenticate_attendee(req.ticket_code)
    if not ticket:
        raise HTTPException(status_code=401, detail="Invalid ticket code")
    token = create_access_token({
        "sub": ticket["id"], "role": "attendee",
        "name": ticket["holder_name"], "ticket_code": ticket["ticket_code"],
        "gate_assigned": ticket["gate_assigned"], "stadium_id": ticket["stadium_id"],
        "seat_section": ticket["seat_section"], "seat_number": ticket["seat_number"],
    })
    return TokenResponse(access_token=token, role="attendee", name=ticket["holder_name"], stadium_id=ticket["stadium_id"])


# ── Stadium Endpoints ──────────────────────────────────────────────────────────
@app.get("/stadiums/{sid}/layout")
async def get_layout(sid: str, _=Depends(require_employee)):
    conn = get_conn()
    stadium = conn.execute("SELECT * FROM stadiums WHERE id=?", (sid,)).fetchone()
    if not stadium:
        raise HTTPException(404, "Stadium not found")
    nodes = [dict(r) for r in conn.execute("SELECT * FROM nodes WHERE stadium_id=?", (sid,)).fetchall()]
    edges = [dict(r) for r in conn.execute("SELECT * FROM edges WHERE stadium_id=?", (sid,)).fetchall()]
    conn.close()
    return {"stadium": dict(stadium), "nodes": nodes, "edges": edges}


@app.post("/stadiums/{sid}/layout")
async def post_layout(sid: str, payload: LayoutPayload, user=Depends(require_admin)):
    nodes = [n.dict() for n in payload.nodes]
    edges = [e.dict() for e in payload.edges]
    result = await save_user_layout(sid, payload.venue_name, nodes, edges)

    # Notify Spatial Architect
    await bus["user_input_layout"].put({
        "stadium_id": sid,
        "venue_name": payload.venue_name,
        "nodes": nodes,
        "edges": edges,
    })
    return result


@app.post("/stadiums/{sid}/surge")
async def inject_surge_endpoint(sid: str, req: SurgeRequest, _=Depends(require_employee)):
    inject_surge(req.node_id, req.magnitude)
    await bus["ws_broadcast"].put({
        "type": "agent_event",
        "agent": "PHYSICIST",
        "message": f"Manual surge injected at {req.node_id}: +{req.magnitude} people/min.",
        "severity": "WARN",
    })
    return {"success": True, "node_id": req.node_id, "magnitude": req.magnitude}


@app.post("/stadiums/{sid}/reset")
async def reset_system_endpoint(sid: str, _=Depends(require_admin)):
    reset_occupancy(sid)
    # Give a tiny pause to clear out any residual surges
    await asyncio.sleep(0.5) 
    await bus["ws_broadcast"].put({
        "type": "agent_event",
        "agent": "SYS",
        "message": "Global reset initiated. Occupancy cleared and clock reset.",
        "severity": "INFO",
    })
    return {"success": True}

@app.post("/stadiums/{sid}/schedule")
async def set_schedule_endpoint(sid: str, req: ScheduleRequest, _=Depends(require_admin)):
    set_schedule(sid, req.start_time, req.end_time, req.deviation_minutes)
    await bus["ws_broadcast"].put({
        "type": "agent_event",
        "agent": "SYS",
        "message": f"Event schedule updated. Deviation: {req.deviation_minutes}m.",
        "severity": "INFO",
    })
    return {"success": True}

@app.post("/stadiums/{sid}/mock-time")
async def set_mock_time_endpoint(sid: str, req: MockTimeRequest, _=Depends(require_admin)):
    set_time_multiplier(sid, req.multiplier)
    return {"success": True, "multiplier": req.multiplier}


@app.get("/stadiums/{sid}/flow-history")
async def flow_history(
    sid: str,
    node_id: str | None = None,
    limit: int = 200,
    _=Depends(require_employee),
):
    data = await fetch_historical_flow(sid, node_id=node_id, limit=limit)
    return {"data": data}


@app.get("/stadiums/{sid}/metrics")
async def current_metrics(sid: str):
    """Public-ish endpoint — returns current heatmap scalars (employee only in UI)."""
    metrics = physicist.get_latest_metrics(sid)
    return {"metrics": metrics}


@app.get("/stadiums/{sid}/thresholds")
async def get_thresholds(sid: str, _=Depends(require_employee)):
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM stadium_thresholds WHERE stadium_id=?", (sid,)
    ).fetchone()
    conn.close()
    if row:
        return dict(row)
    # If no row yet, return defaults
    return {
        "stadium_id": sid,
        "empty_threshold":    0.15,
        "green_threshold":    0.40,
        "busy_threshold":     0.55,
        "critical_threshold": 0.75,
    }


@app.patch("/stadiums/{sid}/thresholds")
async def patch_thresholds(sid: str, payload: ThresholdUpdate, _=Depends(require_admin)):
    """Update per-stadium density thresholds. Admin only."""
    updates = {k: v for k, v in payload.dict().items() if v is not None}
    if not updates:
        raise HTTPException(400, "No valid threshold fields provided.")
    # Read current to fill missing values for ordering check
    conn = get_conn()
    existing = conn.execute(
        "SELECT * FROM stadium_thresholds WHERE stadium_id=?", (sid,)
    ).fetchone()
    conn.close()
    base = dict(existing) if existing else {
        "empty_threshold": 0.15, "green_threshold": 0.40,
        "busy_threshold": 0.55, "critical_threshold": 0.75,
    }
    merged = {**base, **updates}
    e = merged["empty_threshold"]; g = merged["green_threshold"]
    b = merged["busy_threshold"];  c = merged["critical_threshold"]
    if not (e < g < b < c):
        raise HTTPException(400, "Thresholds must be in ascending order (empty < green < busy < critical).")
    set_clause = ", ".join(f"{k}=?" for k in updates)
    conn = get_conn()
    conn.execute("INSERT OR IGNORE INTO stadium_thresholds (stadium_id) VALUES (?)", (sid,))
    conn.execute(
        f"UPDATE stadium_thresholds SET {set_clause}, updated_at=datetime('now') WHERE stadium_id=?",
        list(updates.values()) + [sid],
    )
    conn.commit()
    conn.close()
    # Invalidate physicist cache so next tick reloads from DB
    import backend.agents.flow_physicist as _fp
    _fp._threshold_last_refresh[sid] = 0
    return {"success": True, "updated": updates}


@app.get("/stadiums/{sid}/tickets")
async def list_tickets(sid: str, _=Depends(require_employee)):
    conn = get_conn()
    tickets = [dict(r) for r in conn.execute(
        "SELECT * FROM tickets WHERE stadium_id=?", (sid,)
    ).fetchall()]
    conn.close()
    return {"tickets": tickets}


@app.patch("/stadiums/{sid}/tickets/{tid}/enter")
async def mark_entered(sid: str, tid: str, _=Depends(require_employee)):
    import datetime
    conn = get_conn()
    conn.execute(
        "UPDATE tickets SET entry_status='entered', entry_time=datetime('now') WHERE id=? AND stadium_id=?",
        (tid, sid),
    )
    conn.commit()
    conn.close()
    return {"success": True}


@app.get("/stadiums/list")
async def list_stadiums(_=Depends(require_employee)):
    conn = get_conn()
    rows = [dict(r) for r in conn.execute("SELECT * FROM stadiums").fetchall()]
    conn.close()
    return {"stadiums": rows}


# ── Image → JSON ───────────────────────────────────────────────────────────────
@app.post("/stadiums/image-to-json")
async def image_to_json(file: UploadFile = File(...), _=Depends(require_employee)):
    content = await file.read()
    result = await process_stadium_image(content)
    return result


# ── Agent Status ───────────────────────────────────────────────────────────────
@app.get("/agents/status")
async def agent_status():
    return {
        "spatial_architect": spatial.STATUS,
        "flow_physicist":    physicist.STATUS,
        "ai_brain":          brain.STATUS,
        "ux_concierge":      concierge.STATUS,
    }


# ── Attendee Endpoint ──────────────────────────────────────────────────────────
@app.get("/attendee/me")
async def attendee_me(user=Depends(get_current_user)):
    if user.get("role") != "attendee":
        raise HTTPException(403, "Attendee access only")

    sid = user.get("stadium_id")
    gate = user.get("gate_assigned")

    # Latest nudge for this ticket
    conn = get_conn()
    ticket_id = user.get("sub")
    nudge = conn.execute(
        "SELECT * FROM ai_directives WHERE target_id=? AND directive_type='attendee' ORDER BY timestamp DESC LIMIT 1",
        (ticket_id,),
    ).fetchone()

    # Current gate metrics
    metrics = physicist.get_latest_metrics(sid) if sid else {}
    gate_info = metrics.get(gate, {})
    conn.close()

    return {
        "name": user.get("name"),
        "ticket_code": user.get("ticket_code"),
        "gate_assigned": gate,
        "seat_section": user.get("seat_section"),
        "seat_number": user.get("seat_number"),
        "gate_status": gate_info.get("status", "UNKNOWN"),
        "gate_density": gate_info.get("density", 0),
        "wait_time": nudge["wait_time"] if nudge else None,
        "nudge": dict(nudge) if nudge else None,
    }


# ── AI Directives ──────────────────────────────────────────────────────────────
@app.get("/directives/recent")
async def recent_directives(stadium_id: str = "nexus-grand-01", limit: int = 50, _=Depends(require_employee)):
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM ai_directives WHERE stadium_id=? ORDER BY timestamp DESC LIMIT ?",
        (stadium_id, limit),
    ).fetchall()
    conn.close()
    return {"directives": [dict(r) for r in rows]}


# ── WebSocket ──────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            # Keep connection alive; client can send pings
            data = await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)
