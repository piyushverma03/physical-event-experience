"""
mock_iot.py ─ Simulates sensor streams (2-second tick).

Maintains in-memory occupancy state per node and pushes to the message bus.
Now implements an Event Schedule Finite State Machine (LOCKED -> INGRESS -> FULL -> EGRESS).
"""
from __future__ import annotations
import asyncio
import random
import time
from typing import Dict, Any

# Shared occupancy state: {stadium_id: {node_id: current_occupancy}}
_occupancy: Dict[str, Dict[str, int]] = {}

# Node Metadata: {stadium_id: {node_id: {"type": ..., "capacity": ...}}}
_node_meta: Dict[str, Dict[str, Any]] = {}

# Schedules: {stadium_id: {"start": epoch, "end": epoch, "dev": minutes, "mult": float, "sim_time": float}}
_schedules: Dict[str, Dict[str, float]] = {}

# Manual surge overrides: {node_id: extra_arrivals_per_tick}
_surges: Dict[str, int] = {}

# Reference to the app's message bus
_bus: Dict[str, asyncio.Queue] = {}


def init_iot(bus: Dict[str, asyncio.Queue], stadium_nodes: Dict[str, Dict[str, Any]]):
    """Pass the message bus and initial node capacities."""
    global _bus, _occupancy, _node_meta, _schedules
    _bus = bus
    for sid, nodes in stadium_nodes.items():
        _occupancy[sid] = {}
        _node_meta[sid] = {}
        for nid, cap in nodes.items():
            _occupancy[sid][nid] = 0
            # Rough guess of node type based on ID or we default to 'gate'
            type_str = 'gate'
            if 'LN' in nid or 'LS' in nid or 'LE' in nid or 'LW' in nid or 'Lobby' in nid:
                type_str = 'lobby'
            elif 'FL' in nid or 'Floor' in nid or 'Seat' in nid:
                type_str = 'floor'
            
            _node_meta[sid][nid] = {"type": type_str, "capacity": cap}
            
        _schedules[sid] = {
            "start": time.time() + 86400, # default tomorrow
            "end": time.time() + 86400 + 7200, 
            "dev": 30.0, # minutes
            "mult": 1.0,
            "sim_time": time.time()
        }


def inject_surge(node_id: str, magnitude: int):
    """Inject a manual surge at a specific node (people/tick)."""
    _surges[node_id] = magnitude

def clear_surge(node_id: str):
    _surges.pop(node_id, None)

def reset_occupancy(stadium_id: str):
    """Zeroes out all occupancy and clears all active surges for a clean demo reset."""
    if stadium_id in _occupancy:
        for nid in _occupancy[stadium_id]:
            _occupancy[stadium_id][nid] = 0
            _surges.pop(nid, None)
        _schedules[stadium_id]["mult"] = 1.0
        _schedules[stadium_id]["sim_time"] = time.time()

def set_schedule(stadium_id: str, start: float, end: float, dev: float):
    if stadium_id in _schedules:
        # User passes dev in minutes, schedule stores it directly
        _schedules[stadium_id]["start"] = start
        _schedules[stadium_id]["end"] = end
        _schedules[stadium_id]["dev"] = dev

def set_time_multiplier(stadium_id: str, mult: float):
     if stadium_id in _schedules:
         _schedules[stadium_id]["mult"] = mult

def get_stadium_state(sid: str) -> str:
    sched = _schedules.get(sid)
    if not sched: return "LOCKED"
    
    t = sched["sim_time"]
    start = sched["start"]
    end = sched["end"]
    dev_sec = sched["dev"] * 60

    if t < (start - dev_sec):
        return "LOCKED"
    elif t >= (start - dev_sec) and t < start:
        return "INGRESS"
    elif t >= start and t < end:
        return "FULL"
    elif t >= end and t < (end + 7200): # 2 hours of egress allowance
        return "EGRESS"
    else:
        return "LOCKED"

async def iot_loop():
    """Main 2-second simulation tick."""
    tick = 0
    t_last = time.time()
    
    while True:
        await asyncio.sleep(2)
        tick += 1
        
        t_now = time.time()
        real_dt = t_now - t_last
        t_last = t_now

        for sid, nodes in _occupancy.items():
            sched = _schedules.get(sid)
            if not sched: continue

            # Advance sim clock
            dt = real_dt * sched["mult"]
            sched["sim_time"] += dt
            
            state = get_stadium_state(sid)
            
            # Global capacity tracking
            floor_nodes = [n for n, meta in _node_meta[sid].items() if meta["type"] == 'floor']
            gate_nodes = [n for n, meta in _node_meta[sid].items() if meta["type"] == 'gate']
            lobby_nodes = [n for n, meta in _node_meta[sid].items() if meta["type"] == 'lobby']
            
            total_floor_occ = sum(nodes[n] for n in floor_nodes)
            total_floor_cap = sum(_node_meta[sid][n]["capacity"] for n in floor_nodes)
            
            is_floor_full = total_floor_occ >= total_floor_cap

            batch = {}
            for nid, occ in nodes.items():
                meta = _node_meta[sid][nid]
                ntype = meta["type"]
                cap = meta["capacity"]
                
                arrivals = 0
                departures = 0

                # -- STATE MACHINE LOGIC --
                if state == "LOCKED":
                    # Everyone leaves immediately if locked
                    departures = int(occ * 0.2) + 5
                
                elif state == "INGRESS":
                    if not is_floor_full:
                        if ntype == 'gate':
                            arrivals = int(random.uniform(20, 80) * (dt/2.0))
                            departures = int(random.uniform(15, 60) * (dt/2.0))
                        elif ntype == 'lobby':
                            arrivals = int(random.uniform(20, 60) * (dt/2.0))
                            departures = int(random.uniform(15, 50) * (dt/2.0))
                        elif ntype == 'floor':
                            arrivals = int(random.uniform(40, 150) * (dt/2.0))
                            # Floor keeps filling, no departures
                    else:
                        # Floor is full, gates/lobbies empty out
                        if ntype != 'floor':
                            departures = int(occ * 0.1) + 10
                
                elif state == "FULL":
                    # Event is happening. Gates and lobbies clear out. Floor stays full.
                    if ntype == 'floor':
                        if occ < cap: arrivals = int(random.uniform(10, 30) * (dt/2.0))
                        departures = int(random.uniform(0, 10) * (dt/2.0))
                    else:
                        departures = int(occ * 0.2) + 10
                
                elif state == "EGRESS":
                    # Event ends. Floor empties, rushing the lobbies and gates
                    if ntype == 'floor':
                        departures = int(random.uniform(100, 300) * (dt/2.0))
                    elif ntype == 'lobby':
                        # Surge from floor
                        arrivals = int(random.uniform(50, 150) * (dt/2.0))
                        departures = int(random.uniform(40, 100) * (dt/2.0))
                    elif ntype == 'gate':
                        # Huge surge at gates leaving
                        arrivals = int(random.uniform(80, 200) * (dt/2.0))
                        departures = int(random.uniform(30, 80) * (dt/2.0))

                # Apply manual surge if active (overrides state)
                surge = _surges.get(nid, 0)
                if surge > 0:
                    _surges[nid] = max(0, int(surge * 0.95))
                    arrivals += surge

                new_occ = max(0, occ + arrivals - departures)
                # Cap the maximum locally (except during manual surge to allow red)
                if surge == 0 and ntype == 'floor' and new_occ > cap:
                    new_occ = cap
                    
                nodes[nid] = new_occ
                batch[nid] = new_occ

            # Push to message bus
            if "live_iot_stream" in _bus:
                await _bus["live_iot_stream"].put({
                    "stadium_id": sid,
                    "occupancy": batch,
                    "tick": tick,
                    "timestamp": t_now,
                    "sim_time": sched["sim_time"],
                    "state": state,
                    "time_mult": sched["mult"]
                })
