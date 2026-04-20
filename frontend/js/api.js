/* api.js ─ Shared API client + WebSocket manager */

const BASE = '';
let _ws = null;
const _wsHandlers = [];

export const API = {
  /* ── Auth helpers ─────────────────────────────────────────────────────── */
  token()    { return localStorage.getItem('nexus_token'); },
  role()     { return localStorage.getItem('nexus_role'); },
  name()     { return localStorage.getItem('nexus_name'); },
  stadiumId(){ return localStorage.getItem('nexus_stadium_id') || 'nexus-grand-01'; },

  headers() {
    const t = this.token();
    return {
      'Content-Type': 'application/json',
      ...(t ? { 'Authorization': `Bearer ${t}` } : {}),
    };
  },

  async _fetch(method, path, body) {
    const res = await fetch(BASE + path, {
      method,
      headers: this.headers(),
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
    });
    if (res.status === 401) { this.logout(); return null; }
    return res.ok ? res.json() : Promise.reject(await res.json());
  },

  get(path)             { return this._fetch('GET',   path); },
  post(path, body)      { return this._fetch('POST',  path, body); },
  patch(path)           { return this._fetch('PATCH', path); },
  patchBody(path, body) { return this._fetch('PATCH', path, body); },

  /* ── Auth ─────────────────────────────────────────────────────────────── */
  async loginEmployee(email, password) {
    const data = await this.post('/auth/employee/login', { email, password });
    if (data) this._storeAuth(data);
    return data;
  },

  async loginAttendee(ticket_code) {
    const data = await this.post('/auth/attendee/login', { ticket_code });
    if (data) this._storeAuth(data);
    return data;
  },

  _storeAuth(data) {
    localStorage.setItem('nexus_token',      data.access_token);
    localStorage.setItem('nexus_role',       data.role);
    localStorage.setItem('nexus_name',       data.name);
    localStorage.setItem('nexus_stadium_id', data.stadium_id || 'nexus-grand-01');
  },

  logout() {
    ['nexus_token','nexus_role','nexus_name','nexus_stadium_id'].forEach(k => localStorage.removeItem(k));
    window.location.href = '/login';
  },

  /* ── Stadium ──────────────────────────────────────────────────────────── */
  getLayout(sid)       { return this.get(`/stadiums/${sid}/layout`); },
  getMetrics(sid)      { return this.get(`/stadiums/${sid}/metrics`); },
  getTickets(sid)      { return this.get(`/stadiums/${sid}/tickets`); },
  getFlowHistory(sid, node, limit=200) {
    const q = new URLSearchParams();
    if (node) q.set('node_id', node);
    q.set('limit', limit);
    return this.get(`/stadiums/${sid}/flow-history?${q}`);
  },
  postLayout(sid, payload) { return this.post(`/stadiums/${sid}/layout`, payload); },
  postSurge(sid, node_id, magnitude) {
    return this.post(`/stadiums/${sid}/surge`, { node_id, magnitude });
  },
  postReset(sid) { return this.post(`/stadiums/${sid}/reset`, {}); },
  postSchedule(sid, start_time, end_time, deviation_minutes) {
    return this.post(`/stadiums/${sid}/schedule`, { start_time, end_time, deviation_minutes });
  },
  postMockTime(sid, multiplier) {
    return this.post(`/stadiums/${sid}/mock-time`, { multiplier });
  },
  markEntered(sid, tid) { return this.patch(`/stadiums/${sid}/tickets/${tid}/enter`); },

  /* ── Thresholds ────────────────────────────────────────────────────────── */
  getThresholds(sid)           { return this.get(`/stadiums/${sid}/thresholds`); },
  patchThresholds(sid, updates){ return this.patchBody(`/stadiums/${sid}/thresholds`, updates); },

  /* ── Agents / Directives ──────────────────────────────────────────────── */
  getAgentStatus()     { return this.get('/agents/status'); },
  getDirectives(sid, limit=50) {
    return this.get(`/directives/recent?stadium_id=${sid}&limit=${limit}`);
  },

  /* ── Attendee ─────────────────────────────────────────────────────────── */
  getAttendeMe() { return this.get('/attendee/me'); },

  /* ── Image → JSON ─────────────────────────────────────────────────────── */
  async uploadStadiumImage(file) {
    const form = new FormData();
    form.append('file', file);
    const res = await fetch(BASE + '/stadiums/image-to-json', {
      method: 'POST',
      headers: { 'Authorization': `Bearer ${this.token()}` },
      body: form,
    });
    return res.ok ? res.json() : Promise.reject(await res.json());
  },

  /* ── WebSocket ────────────────────────────────────────────────────────── */
  connectWS() {
    if (_ws && _ws.readyState === WebSocket.OPEN) return;
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    _ws = new WebSocket(`${protocol}//${window.location.host}/ws`);

    _ws.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      _wsHandlers.forEach(h => h(msg));
    };

    _ws.onclose = () => {
      setTimeout(() => this.connectWS(), 3000);  // auto-reconnect
    };

    // Keep-alive ping
    setInterval(() => {
      if (_ws && _ws.readyState === WebSocket.OPEN) _ws.send('ping');
    }, 20000);
  },

  onWS(handler) { _wsHandlers.push(handler); },
};

/* ── Toast notifications ──────────────────────────────────────────────────── */
export function showToast(title, body, type = 'info', duration = 5000) {
  let container = document.getElementById('toast-container');
  if (!container) {
    container = document.createElement('div');
    container.id = 'toast-container';
    document.body.appendChild(container);
  }
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `
    <div>
      <div class="toast-title">${title}</div>
      ${body ? `<div class="toast-body">${body}</div>` : ''}
    </div>`;
  container.appendChild(toast);
  setTimeout(() => toast.remove(), duration);
}

/* ── Format helpers ───────────────────────────────────────────────────────── */
export function fmtTime(isoOrEpoch) {
  const d = typeof isoOrEpoch === 'number'
    ? new Date(isoOrEpoch * 1000)
    : new Date(isoOrEpoch);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

export function statusClass(status) {
  const map = {
    GREEN:    'status-green',
    AMBER:    'status-amber',
    BUSY:     'status-amber',
    CRITICAL: 'status-red',
    CHOKE:    'status-choke',
    EMPTY:    'status-empty',
  };
  return map[status] || 'status-empty';
}

/* Density → colour with reduced sensitivity
   green < 55%  |  amber 55–75%  |  red > 75%  */
export function densityColor(density, thresholds) {
  const busy = thresholds?.busy_threshold     ?? 0.55;
  const crit = thresholds?.critical_threshold ?? 0.75;
  if (density < busy) return '#22D3A5';   // --green
  if (density < crit) return '#FB923C';   // --amber
  return '#FF4D6D';                       // --red
}
