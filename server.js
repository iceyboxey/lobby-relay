// Tiny relay server for the Animal Company lobby multiplayer.
// Run: npm install && npm start   (PORT env var is used if set)
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_CLIENTS = 200, MAX_PER_IP = 6;
const server = http.createServer((req, res) => { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('lobby relay ok, players: ' + wss.clients.size); });
const wss = new WebSocketServer({ server, maxPayload: 2048 });
const state = new Map();           // id -> last valid presence
const ipCount = new Map();
const items = new Map();           // id -> {id,type,x,y,z,yaw,m,h,on}
let itemSeq = 1;
let shop = null;                   // {x,y,z} set by any player pressing B
const MAX_ITEMS = 120;

const num = v => typeof v === 'number' && isFinite(v) && Math.abs(v) < 1e6 ? v : null;
function clean(p) {
  if (!p || typeof p !== 'object') return null;
  const x = num(p.x), y = num(p.y), z = num(p.z), yaw = num(p.yaw);
  if (x === null || y === null || z === null) return null;
  const hv = h => Array.isArray(h) && h.length === 3 && h.every(isNum) ? h.map(n => Math.round(n * 100) / 100) : undefined;
  const q4 = a => Array.isArray(a) && a.length === 4 && a.every(isNum) ? a.map(n => Math.round(n * 1000) / 1000) : undefined;
  return { x, y, z, yaw: yaw || 0, vr: p.vr === 1 ? 1 : 0, hh: isNum(p.hh) ? Math.round(p.hh * 100) / 100 : undefined, hl: hv(p.hl), hr: hv(p.hr), rq: q4(p.rq), m: String(p.m || '').slice(0, 4), n: String(p.n || 'Player').replace(/[^\w \-.]/g, '').slice(0, 16) || 'Player' };
}
const isNum = v => typeof v === 'number' && isFinite(v) && Math.abs(v) < 1e6;
function itemOut(it) { return { id: it.id, type: it.type, x: it.x, y: it.y, z: it.z, yaw: it.yaw, m: it.m, h: it.h, on: it.on }; }
function send(ws, o) { if (ws.readyState === 1) ws.send(JSON.stringify(o)); }
function broadcast(o, except) { const s = JSON.stringify(o); for (const c of wss.clients) if (c !== except && c.readyState === 1) c.send(s); }

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const n = (ipCount.get(ip) || 0) + 1;
  if (wss.clients.size > MAX_CLIENTS || n > MAX_PER_IP) { ws.close(1013); return; }
  ipCount.set(ip, n);
  ws.id = crypto.randomBytes(6).toString('hex'); ws.alive = true; ws.tokens = 40; ws.lastRefill = Date.now();
  send(ws, { t: 'hello', id: ws.id, peers: [...state].map(([id, p]) => ({ id, p })), items: [...items.values()].map(itemOut), shop });
  ws.on('pong', () => ws.alive = true);
  ws.on('message', data => {
    const now = Date.now(); ws.tokens = Math.min(40, ws.tokens + (now - ws.lastRefill) / 1000 * 25); ws.lastRefill = now;
    if (ws.tokens < 1) return; ws.tokens--;                       // rate limit ~25 msgs/s
    let m; try { m = JSON.parse(data); } catch { return; }
    if (m.t === 'p') { const p = clean(m.p); if (!p) return; state.set(ws.id, p); broadcast({ t: 's', id: ws.id, p }, ws); return; }
    if (m.t === 'chat') {
      const now2 = Date.now(); if (now2 - (ws.lastChat || 0) < 700) return; ws.lastChat = now2;
      const text = String(m.text || '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 120); if (!text) return;
      const n = (state.get(ws.id) || {}).n || 'Player';
      broadcast({ t: 'chat', id: ws.id, n, text }); return;
    }
    if (m.t === 'shop') { if (![m.x, m.y, m.z].every(isNum)) return; shop = { x: m.x, y: m.y, z: m.z }; broadcast({ t: 'shop', x: m.x, y: m.y, z: m.z }); return; }
    if (m.t === 'ispawn') {
      if (!['stick', 'flashlight'].includes(m.type) || ![m.x, m.y, m.z, m.yaw].every(isNum)) return;
      if (items.size >= MAX_ITEMS) { const old = items.keys().next().value; items.delete(old); broadcast({ t: 'idel', id: old }); }
      const it = { id: 'i' + (itemSeq++), type: m.type, x: m.x, y: m.y, z: m.z, yaw: m.yaw, m: String(m.m || '').slice(0, 4), h: null, on: true };
      items.set(it.id, it); broadcast({ t: 'item', it: itemOut(it) }); return;
    }
    const it = items.get(m.id); if (!it) return;
    if (m.t === 'igrab') {
      if (it.h) return;
      for (const o of items.values()) if (o.h === ws.id) return;      // one item at a time
      it.h = ws.id; broadcast({ t: 'item', it: itemOut(it) });
    } else if (m.t === 'idrop') {
      if (it.h !== ws.id || ![m.x, m.y, m.z, m.yaw].every(isNum)) return;
      Object.assign(it, { x: m.x, y: m.y, z: m.z, yaw: m.yaw, m: String(m.m || '').slice(0, 4), h: null }); broadcast({ t: 'item', it: itemOut(it) });
    } else if (m.t === 'itog') {
      if (it.h !== ws.id) return; it.on = !!m.on; broadcast({ t: 'item', it: itemOut(it) });
    }
  });
  ws.on('close', () => {
    const last = state.get(ws.id);
    for (const it of items.values()) if (it.h === ws.id) { it.h = null; if (last) { it.x = last.x; it.y = last.y; it.z = last.z; it.m = last.m; } broadcast({ t: 'item', it: itemOut(it) }); }
    state.delete(ws.id); broadcast({ t: 'l', id: ws.id });
    const c = (ipCount.get(ip) || 1) - 1; if (c <= 0) ipCount.delete(ip); else ipCount.set(ip, c);
  });
});
setInterval(() => { for (const c of wss.clients) { if (!c.alive) { c.terminate(); continue; } c.alive = false; c.ping(); } }, 30000);
server.listen(PORT, () => console.log('relay listening on ' + PORT));
