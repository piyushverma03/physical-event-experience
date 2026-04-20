import sqlite3
import os
import json
import uuid
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "nexus.db"


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    os.makedirs(DB_PATH.parent, exist_ok=True)
    conn = get_conn()
    c = conn.cursor()

    # ── Storage A: Graph / Relational ─────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS stadiums (
            id            TEXT PRIMARY KEY,
            venue_name    TEXT NOT NULL,
            user_id       TEXT,
            total_capacity INTEGER DEFAULT 0,
            created_at    TEXT DEFAULT (datetime('now'))
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS nodes (
            id         TEXT,
            stadium_id TEXT NOT NULL,
            label      TEXT NOT NULL,
            type       TEXT DEFAULT 'gate',
            capacity   INTEGER DEFAULT 0,
            coord_x    REAL DEFAULT 0,
            coord_y    REAL DEFAULT 0,
            coord_z    REAL DEFAULT 0,
            PRIMARY KEY (id, stadium_id),
            FOREIGN KEY (stadium_id) REFERENCES stadiums(id) ON DELETE CASCADE
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS edges (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            stadium_id TEXT NOT NULL,
            from_id    TEXT NOT NULL,
            to_id      TEXT NOT NULL,
            max_flow   INTEGER DEFAULT 1000,
            distance   REAL DEFAULT 100,
            FOREIGN KEY (stadium_id) REFERENCES stadiums(id) ON DELETE CASCADE
        )
    """)

    # ── Storage B: Time-Series Flow Metrics ────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS flow_metrics (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp         TEXT DEFAULT (datetime('now')),
            node_id           TEXT NOT NULL,
            stadium_id        TEXT NOT NULL,
            current_occupancy INTEGER DEFAULT 0,
            flow_rate         REAL DEFAULT 0,
            flux_value        REAL DEFAULT 0,
            status            TEXT DEFAULT 'GREEN'
        )
    """)

    # ── Employees ──────────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            id            TEXT PRIMARY KEY,
            name          TEXT NOT NULL,
            email         TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            stadium_id    TEXT,
            role          TEXT DEFAULT 'staff',
            created_at    TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (stadium_id) REFERENCES stadiums(id)
        )
    """)

    # ── Tickets / Users ────────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS tickets (
            id             TEXT PRIMARY KEY,
            ticket_code    TEXT UNIQUE NOT NULL,
            holder_name    TEXT NOT NULL,
            holder_contact TEXT,
            stadium_id     TEXT,
            gate_assigned  TEXT,
            seat_section   TEXT,
            seat_number    TEXT,
            entry_status   TEXT DEFAULT 'pending',
            entry_time     TEXT,
            FOREIGN KEY (stadium_id) REFERENCES stadiums(id)
        )
    """)

    # ── AI Directives Log ─────────────────────────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS ai_directives (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp      TEXT DEFAULT (datetime('now')),
            stadium_id     TEXT,
            directive_type TEXT,
            target_id      TEXT,
            node_id        TEXT,
            message        TEXT,
            severity       TEXT DEFAULT 'INFO',
            alt_gate       TEXT,
            wait_time      REAL,
            acknowledged   INTEGER DEFAULT 0
        )
    """)

    # ── Per-Stadium Thresholds ──────────────────────────────────
    c.execute("""
        CREATE TABLE IF NOT EXISTS stadium_thresholds (
            stadium_id           TEXT PRIMARY KEY,
            empty_threshold      REAL DEFAULT 0.15,
            green_threshold      REAL DEFAULT 0.40,
            busy_threshold       REAL DEFAULT 0.55,
            critical_threshold   REAL DEFAULT 0.75,
            venue_capacity_limit INTEGER DEFAULT 75000,
            updated_at           TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (stadium_id) REFERENCES stadiums(id) ON DELETE CASCADE
        )
    """)
    # Migrate: add venue_capacity_limit if column is missing (existing DB)
    try:
        c.execute("ALTER TABLE stadium_thresholds ADD COLUMN venue_capacity_limit INTEGER DEFAULT 75000")
    except Exception:
        pass  # Column already exists

    conn.commit()
    conn.close()
    print("[DB] Schema initialized.")


def seed_data():
    """Seed default stadium, employees, and tickets on first run."""
    conn = get_conn()
    c = conn.cursor()

    # ── Stadium & Graph ────────────────────────────────────────────────────────
    c.execute("SELECT COUNT(*) FROM stadiums")
    if c.fetchone()[0] == 0:
        fake_path = Path(__file__).parent / "data" / "fake_stadium.json"
        with open(fake_path, encoding="utf-8") as f:
            stadium = json.load(f)

        sid = "nexus-grand-01"
        c.execute(
            "INSERT OR IGNORE INTO stadiums (id, venue_name, total_capacity) VALUES (?,?,?)",
            (sid, stadium["venue_name"], stadium["total_capacity"]),
        )
        for node in stadium["nodes"]:
            c.execute(
                "INSERT OR IGNORE INTO nodes (id,stadium_id,label,type,capacity,coord_x,coord_y,coord_z) VALUES (?,?,?,?,?,?,?,?)",
                (node["id"], sid, node["label"], node["type"], node["capacity"],
                 node["coord_x"], node["coord_y"], node["coord_z"]),
            )
        for edge in stadium["edges"]:
            c.execute(
                "INSERT INTO edges (stadium_id,from_id,to_id,max_flow,distance) VALUES (?,?,?,?,?)",
                (sid, edge["from_id"], edge["to_id"], edge["max_flow"], edge["distance"]),
            )

        # Seed tickets from fake_stadium.json
        for tkt in stadium.get("demo_tickets", []):
            c.execute(
                "INSERT OR IGNORE INTO tickets (id,ticket_code,holder_name,holder_contact,stadium_id,gate_assigned,seat_section,seat_number) VALUES (?,?,?,?,?,?,?,?)",
                (str(uuid.uuid4()), tkt["ticket_code"], tkt["holder_name"],
                 tkt["holder_contact"], sid, tkt["gate_assigned"],
                 tkt["seat_section"], tkt["seat_number"]),
            )
        # Seed default thresholds row
        c.execute(
            "INSERT OR IGNORE INTO stadium_thresholds (stadium_id) VALUES (?)",
            (sid,),
        )
        print(f"[DB] Seeded '{stadium['venue_name']}' ({len(stadium['nodes'])} nodes, {len(stadium['edges'])} edges, {len(stadium.get('demo_tickets',[]))} tickets).")

    # ── Employees ──────────────────────────────────────────────────────────────
    c.execute("SELECT COUNT(*) FROM employees")
    if c.fetchone()[0] == 0:
        from passlib.context import CryptContext
        pwd_ctx = CryptContext(schemes=["bcrypt"], deprecated="auto")

        defaults = [
            (str(uuid.uuid4()), "Admin Nexus",    "admin@nexus.com",  pwd_ctx.hash("NexusAdmin123"), "nexus-grand-01", "admin"),
            (str(uuid.uuid4()), "Staff Member 1", "staff1@nexus.com", pwd_ctx.hash("Staff123"),      "nexus-grand-01", "staff"),
        ]
        for row in defaults:
            c.execute(
                "INSERT OR IGNORE INTO employees (id,name,email,password_hash,stadium_id,role) VALUES (?,?,?,?,?,?)",
                row,
            )
        print("[DB] Seeded employees — admin@nexus.com / NexusAdmin123")

    conn.commit()
    conn.close()


def reset_demo(stadium_id: str):
    """Clear all runtime metrics and directives — for prototype reset."""
    conn = get_conn()
    conn.execute("DELETE FROM flow_metrics   WHERE stadium_id=?", (stadium_id,))
    conn.execute("DELETE FROM ai_directives  WHERE stadium_id=?", (stadium_id,))
    conn.commit()
    conn.close()
    print(f"[DB] Demo reset for {stadium_id}")
