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

import math

def get_stadium_state(sid: str) -> str:
    # Deprecated since we use the organic pipeline, returning generic active state
    return "ORGANIC FLOW"

async def iot_loop():
    """Main 2-second simulation tick driven by dynamic organic pipeline math."""
    tick = 0
    t_last = time.time()
    
    # Internal scenario scale clock for the environment agent
    scenario_clock = 0.0
    
    while True:
        await asyncio.sleep(2)
        tick += 1
        
        t_now = time.time()
        real_dt = t_now - t_last
        t_last = t_now

        for sid, nodes in _occupancy.items():
            sched = _schedules.get(sid)
            if not sched: continue

            # Advance sim clock (fast-forwarding affects the organic cycle)
            mult = sched.get("mult", 1.0)
            dt = real_dt * mult
            scenario_clock += dt
            sched["sim_time"] += dt
            
            # 1. Environment Agent (Dynamic Pressure Map)
            # A sinusoidal wave simulation: Period = ~15 sim-minutes
            # +ve = Global Influx (Arrivals)
            # -ve = Global Outflux (Departures)
            env_pressure = math.sin(scenario_clock / 150.0)
            is_ingress = env_pressure >= 0
            
            # Segregate nodes
            floor_nodes = [n for n, meta in _node_meta[sid].items() if meta["type"] == 'floor']
            gate_nodes = [n for n, meta in _node_meta[sid].items() if meta["type"] == 'gate']
            lobby_nodes = [n for n, meta in _node_meta[sid].items() if meta["type"] == 'lobby']
            
            # Track deltas per node to ensure perfect mass conservation (no magically appearing people inside)
            deltas = {nid: 0 for nid in nodes.keys()}
            
            # 2. EXTERNAL BOUNDARY CONDITIONS
            if is_ingress:
                # People arrive at the gates from outside the stadium
                global_arrivals = int(env_pressure * random.uniform(80, 200) * (dt/2.0))
                for _ in range(global_arrivals):
                    deltas[random.choice(gate_nodes)] += 1
            else:
                # People depart the stadium through the gates
                global_pull = int(abs(env_pressure) * random.uniform(100, 250) * (dt/2.0))
                for _ in range(global_pull):
                    g = random.choice(gate_nodes)
                    if (nodes[g] + deltas[g]) > 0:
                        deltas[g] -= 1

            # 3. INTERNAL PIPELINE KINEMATICS (Fluid Transfer)
            # Max node transfer rate per 2-second nominal window
            transfer_rate = 120 * (dt/2.0) 

            if is_ingress:
                # Flow: Gates -> Lobbies -> Floor
                for g in gate_nodes:
                    avail = min(nodes[g] + deltas[g], int(transfer_rate * random.uniform(0.6, 1.2)))
                    if avail > 0:
                        deltas[g] -= avail
                        # Dump flow into lobbies
                        for _ in range(avail):
                            deltas[random.choice(lobby_nodes)] += 1
                            
                for l in lobby_nodes:
                    avail = min(nodes[l] + deltas[l], int(transfer_rate * random.uniform(0.8, 1.4)))
                    if avail > 0:
                        deltas[l] -= avail
                        for _ in range(avail):
                            deltas[random.choice(floor_nodes)] += 1
            else:
                # Flow: Floor -> Lobbies -> Gates
                for f in floor_nodes:
                    avail = min(nodes[f] + deltas[f], int(transfer_rate * random.uniform(1.2, 2.5)))
                    if avail > 0:
                        deltas[f] -= avail
                        for _ in range(avail):
                            deltas[random.choice(lobby_nodes)] += 1
                            
                for l in lobby_nodes:
                    avail = min(nodes[l] + deltas[l], int(transfer_rate * random.uniform(0.8, 1.5)))
                    if avail > 0:
                        deltas[l] -= avail
                        for _ in range(avail):
                            deltas[random.choice(gate_nodes)] += 1

            # 4. MANUAL OVERRIDES / SURGES 
            for nid, s in _surges.items():
                if s > 0:
                    _surges[nid] = max(0, int(s * 0.95)) # Surgeon decay
                    deltas[nid] += s

            # Apply all verified transfer math
            batch = {}
            for nid in nodes.keys():
                new_occ = max(0, nodes[nid] + deltas[nid])
                nodes[nid] = new_occ
                batch[nid] = new_occ

            ui_state = "FLUX: INGRESS" if is_ingress else "FLUX: EGRESS"

            # Push to unified message bus
            if "live_iot_stream" in _bus:
                await _bus["live_iot_stream"].put({
                    "stadium_id": sid,
                    "occupancy": batch,
                    "tick": tick,
                    "timestamp": t_now,
                    "sim_time": sched["sim_time"],
                    "state": ui_state,
                    "time_mult": mult
                })
