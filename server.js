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

const num = v => typeof v === 'number' && isFinite(v) && Math.abs(v) < 1e6 ? v : null;
function clean(p) {
  if (!p || typeof p !== 'object') return null;
  const x = num(p.x), y = num(p.y), z = num(p.z), yaw = num(p.yaw);
  if (x === null || y === null || z === null) return null;
  return { x, y, z, yaw: yaw || 0, m: String(p.m || '').slice(0, 4), n: String(p.n || 'Player').replace(/[^\w \-.]/g, '').slice(0, 16) || 'Player' };
}
function send(ws, o) { if (ws.readyState === 1) ws.send(JSON.stringify(o)); }
function broadcast(o, except) { const s = JSON.stringify(o); for (const c of wss.clients) if (c !== except && c.readyState === 1) c.send(s); }

wss.on('connection', (ws, req) => {
  const ip = (req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  const n = (ipCount.get(ip) || 0) + 1;
  if (wss.clients.size > MAX_CLIENTS || n > MAX_PER_IP) { ws.close(1013); return; }
  ipCount.set(ip, n);
  ws.id = crypto.randomBytes(6).toString('hex'); ws.alive = true; ws.tokens = 40; ws.lastRefill = Date.now();
  send(ws, { t: 'hello', id: ws.id, peers: [...state].map(([id, p]) => ({ id, p })) });
  ws.on('pong', () => ws.alive = true);
  ws.on('message', data => {
    const now = Date.now(); ws.tokens = Math.min(40, ws.tokens + (now - ws.lastRefill) / 1000 * 25); ws.lastRefill = now;
    if (ws.tokens < 1) return; ws.tokens--;                       // rate limit ~25 msgs/s
    let m; try { m = JSON.parse(data); } catch { return; }
    if (m.t !== 'p') return;
    const p = clean(m.p); if (!p) return;
    state.set(ws.id, p); broadcast({ t: 's', id: ws.id, p }, ws);
  });
  ws.on('close', () => {
    state.delete(ws.id); broadcast({ t: 'l', id: ws.id });
    const c = (ipCount.get(ip) || 1) - 1; if (c <= 0) ipCount.delete(ip); else ipCount.set(ip, c);
  });
});
setInterval(() => { for (const c of wss.clients) { if (!c.alive) { c.terminate(); continue; } c.alive = false; c.ping(); } }, 30000);
server.listen(PORT, () => console.log('relay listening on ' + PORT));
