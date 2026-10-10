// Tiny relay server for the Animal Company lobby multiplayer.
// Run: npm install && npm start   (PORT env var is used if set)
const http = require('http');
const { WebSocketServer } = require('ws');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_CLIENTS = 200, MAX_PER_IP = 6;
const server = http.createServer((req, res) => { res.writeHead(200, { 'content-type': 'text/plain' }); res.end('lobby relay ok, players: ' + wss.clients.size); });
const wss = new WebSocketServer({ server, maxPayload: 8192 });
const state = new Map();           // id -> last valid presence
const ipCount = new Map();
const items = new Map();           // id -> {id,type,x,y,z,yaw,m,h,hd,on,w}
let itemSeq = 1;
let shop = null;                   // {x,y,z} set by any player pressing B
const MAX_ITEMS = 220;

// ---- game tables -------------------------------------------------------------
const WEAPON = { stick: 15, bat: 30 };                                         // damage per hit
const SELL = { can: 3, banana: 6, book: 12, duck: 10, gem: 30, trophy: 60, goldbanana: 120 };
const SELL_WEIGHT = { can: 30, banana: 25, duck: 15, book: 15, gem: 8, trophy: 4, goldbanana: 1 };
const SHOP_TYPES = ['stick', 'bat', 'flashlight', 'lantern', 'glowstick'];
const ALLOWED = new Set([...SHOP_TYPES, ...Object.keys(SELL)]);
const TOGGLE = new Set(['flashlight', 'lantern']);
const RESPAWN_MS = 90000;
const slots = [];                  // world item spawn points {m,x,y,z,yaw}
const seeded = new Set();          // maps that already got their world items

function pickSellType() {
  let tot = 0; for (const k in SELL_WEIGHT) tot += SELL_WEIGHT[k];
  let r = Math.random() * tot; for (const k in SELL_WEIGHT) { r -= SELL_WEIGHT[k]; if (r <= 0) return k; }
  return 'can';
}
const num = v => typeof v === 'number' && isFinite(v) && Math.abs(v) < 1e6 ? v : null;
const isNum = v => typeof v === 'number' && isFinite(v) && Math.abs(v) < 1e6;
function clean(p) {
  if (!p || typeof p !== 'object') return null;
  const x = num(p.x), y = num(p.y), z = num(p.z), yaw = num(p.yaw);
  if (x === null || y === null || z === null) return null;
  const hv = h => Array.isArray(h) && h.length === 3 && h.every(isNum) ? h.map(n => Math.round(n * 100) / 100) : undefined;
  const q4 = a => Array.isArray(a) && a.length === 4 && a.every(isNum) ? a.map(n => Math.round(n * 1000) / 1000) : undefined;
  return { x, y, z, yaw: yaw || 0, vr: p.vr === 1 ? 1 : 0, hh: isNum(p.hh) ? Math.round(p.hh * 100) / 100 : undefined, hl: hv(p.hl), hr: hv(p.hr), rq: q4(p.rq), lq: q4(p.lq), m: String(p.m || '').slice(0, 4), n: String(p.n || 'Player').replace(/[^\w \-.]/g, '').slice(0, 16) || 'Player' };
}
function itemOut(it) { return { id: it.id, type: it.type, x: it.x, y: it.y, z: it.z, yaw: it.yaw, m: it.m, h: it.h, hd: it.hd, on: it.on }; }
function send(ws, o) { if (ws.readyState === 1) ws.send(JSON.stringify(o)); }
function broadcast(o, except) { const s = JSON.stringify(o); for (const c of wss.clients) if (c !== except && c.readyState === 1) c.send(s); }
function byId(id) { for (const c of wss.clients) if (c.id === id) return c; return null; }

function addItem(o) {
  if (items.size >= MAX_ITEMS) {                                   // evict the oldest player-made item first
    let old = null; for (const it of items.values()) if (it.w < 0 && !it.h) { old = it.id; break; }
    if (old === null) old = items.keys().next().value;
    items.delete(old); broadcast({ t: 'idel', id: old });
  }
  const it = Object.assign({ id: 'i' + (itemSeq++), yaw: 0, m: 'm1', h: null, hd: null, on: true, w: -1 }, o);
  items.set(it.id, it); broadcast({ t: 'item', it: itemOut(it) }); return it;
}
function spawnSlot(i) {
  const s = slots[i]; if (!s) return;
  for (const it of items.values()) if (it.w === i) return;          // slot already filled
  addItem({ type: pickSellType(), x: s.x, y: s.y, z: s.z, yaw: s.yaw, m: s.m, w: i });
}
function releaseHeld(id, at) {
  for (const it of items.values()) if (it.h === id) {
    it.h = null; it.hd = null; if (at) { it.x = at.x; it.y = at.y; it.z = at.z; it.m = at.m; }
    broadcast({ t: 'item', it: itemOut(it) });
  }
}

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const n = (ipCount.get(ip) || 0) + 1;
  if (wss.clients.size > MAX_CLIENTS || n > MAX_PER_IP) { ws.close(1013); return; }
  ipCount.set(ip, n);
  ws.id = crypto.randomBytes(6).toString('hex'); ws.alive = true; ws.tokens = 40; ws.lastRefill = Date.now();
  ws.hp = 100; ws.hitAt = 0; ws.protect = 0;
  send(ws, { t: 'hello', id: ws.id, peers: [...state].map(([id, p]) => ({ id, p })), items: [...items.values()].map(itemOut), shop, seeded: [...seeded] });
  ws.on('pong', () => ws.alive = true);
  ws.on('message', data => {
    const now = Date.now(); ws.tokens = Math.min(40, ws.tokens + (now - ws.lastRefill) / 1000 * 25); ws.lastRefill = now;
    if (ws.tokens < 1) return; ws.tokens--;                       // rate limit ~25 msgs/s
    let m; try { m = JSON.parse(data); } catch { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'p') { const p = clean(m.p); if (!p) return; state.set(ws.id, p); broadcast({ t: 's', id: ws.id, p }, ws); return; }
    if (m.t === 'chat') {
      const now2 = Date.now(); if (now2 - (ws.lastChat || 0) < 700) return; ws.lastChat = now2;
      const text = String(m.text || '').replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, 120); if (!text) return;
      const nm = (state.get(ws.id) || {}).n || 'Player';
      broadcast({ t: 'chat', id: ws.id, n: nm, text }); return;
    }
    if (m.t === 'shop') { if (![m.x, m.y, m.z].every(isNum)) return; shop = { x: m.x, y: m.y, z: m.z }; broadcast({ t: 'shop', x: m.x, y: m.y, z: m.z }); return; }
    if (m.t === 'heal') { ws.hp = 100; send(ws, { t: 'hp', hp: 100 }); return; }
    if (m.t === 'seedworld') {                                      // first client to load a map supplies its spawn points
      const mp = String(m.m || '').slice(0, 4); if (!mp || seeded.has(mp) || !Array.isArray(m.list)) return;
      seeded.add(mp); broadcast({ t: 'seeded', m: mp });
      for (const s of m.list.slice(0, 30)) { if (!s || ![s.x, s.y, s.z].every(isNum)) continue; slots.push({ m: mp, x: s.x, y: s.y, z: s.z, yaw: isNum(s.yaw) ? s.yaw : 0 }); spawnSlot(slots.length - 1); }
      return;
    }
    if (m.t === 'hit') {                                            // melee: server checks weapon, reach, cooldown
      const v = byId(m.id); if (!v || v === ws) return;
      if (now - ws.hitAt < 350 || now < v.protect) return;
      let dmg = 0; for (const it of items.values()) if (it.h === ws.id && WEAPON[it.type]) dmg = Math.max(dmg, WEAPON[it.type]);
      if (!dmg) return;
      const a = state.get(ws.id), b = state.get(v.id); if (!a || !b || a.m !== b.m) return;
      if (Math.hypot(a.x - b.x, a.z - b.z) > 4.5 || Math.abs(a.y - b.y) > 4) return;
      ws.hitAt = now; v.hp -= dmg;
      broadcast({ t: 'hit', id: v.id, by: ws.id, hp: Math.max(0, v.hp) });
      if (v.hp <= 0) { v.hp = 100; v.protect = now + 3000; releaseHeld(v.id, b); broadcast({ t: 'ko', id: v.id, by: ws.id }); send(v, { t: 'hp', hp: 100 }); }
      else send(v, { t: 'hp', hp: v.hp });
      return;
    }
    if (m.t === 'ispawn') {
      if (!ALLOWED.has(m.type) || ![m.x, m.y, m.z, m.yaw].every(isNum)) return;
      addItem({ type: m.type, x: m.x, y: m.y, z: m.z, yaw: m.yaw, m: String(m.m || '').slice(0, 4) }); return;
    }
    const it = items.get(m.id); if (!it) return;
    if (m.t === 'igrab') {
      if (it.h) return;
      const hd = m.hand === 'r' ? 'r' : 'l';
      for (const o of items.values()) if (o.h === ws.id && o.hd === hd) return;   // that hand is full
      it.h = ws.id; it.hd = hd; broadcast({ t: 'item', it: itemOut(it) });
    } else if (m.t === 'idrop') {
      if (it.h !== ws.id || ![m.x, m.y, m.z, m.yaw].every(isNum)) return;
      Object.assign(it, { x: m.x, y: m.y, z: m.z, yaw: m.yaw, m: String(m.m || '').slice(0, 4), h: null, hd: null }); broadcast({ t: 'item', it: itemOut(it) });
    } else if (m.t === 'itog') {
      if (it.h !== ws.id || !TOGGLE.has(it.type)) return; it.on = !!m.on; broadcast({ t: 'item', it: itemOut(it) });
    } else if (m.t === 'isell') {
      const val = SELL[it.type]; if (!val || (it.h && it.h !== ws.id)) return;
      items.delete(it.id); broadcast({ t: 'idel', id: it.id });
      send(ws, { t: 'sold', type: it.type, coins: val });
      if (it.w >= 0) { const i = it.w; setTimeout(() => spawnSlot(i), RESPAWN_MS); }
    }
  });
  ws.on('close', () => {
    const last = state.get(ws.id);
    releaseHeld(ws.id, last);
    state.delete(ws.id); broadcast({ t: 'l', id: ws.id });
    const c = (ipCount.get(ip) || 1) - 1; if (c <= 0) ipCount.delete(ip); else ipCount.set(ip, c);
  });
});
setInterval(() => { for (const c of wss.clients) if (c.hp < 100) { c.hp = Math.min(100, c.hp + 3); send(c, { t: 'hp', hp: c.hp }); } }, 1000);   // slow health regen
setInterval(() => { for (const c of wss.clients) { if (!c.alive) { c.terminate(); continue; } c.alive = false; c.ping(); } }, 30000);
server.listen(PORT, () => console.log('relay listening on ' + PORT));
