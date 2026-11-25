// Copyright 2026 Anthropic PBC
// SPDX-License-Identifier: Apache-2.0

// Bot relay — lets a Managed Agent in Anthropic's cloud reach a
// participant's local bot WITHOUT the participant exposing anything.
//
//   participant bot  ──(outbound WebSocket)──▶  event server  ◀──(MCP over HTTPS)──  CMA
//
// The bot dials OUT to /relay/ws and registers with a participant name,
// a secret key, and its MCP tool list. The event server then exposes
// that bot's tools at /p/<key>/mcp. Tool calls are forwarded over the
// WebSocket as JSON and the result is returned to the MCP caller.
//
// Why this exists: per-participant cloudflared quick-tunnels fail at
// venues — Cloudflare rate-limits tunnel creation per source IP, and
// 30+ participants behind one conference NAT all count as one IP.
// With the relay, participants make only outbound HTTPS/WSS connections
// (indistinguishable from normal web traffic), and the event server is
// the single public endpoint.

import { WebSocketServer } from 'ws';
import { timingSafeEqual } from 'node:crypto';

const CALL_TIMEOUT_MS = 150_000;   // > the longest bot action (go_near can dig ~90s)
const HEARTBEAT_MS = 25_000;
const REGISTER_TIMEOUT_MS = 10_000;  // unregistered sockets get this long, then dropped
const STALE_BOT_MS = 60 * 60_000;    // forget disconnected registrations after 1h
const MAX_WS_PAYLOAD = 1024 * 1024;  // 1 MiB — tool results are small JSON
// Hard ceiling on concurrent sockets: ~100 expected bots with 5x headroom.
// Prevents connection-flood exhaustion of fds/heap on a single instance —
// the 10s register timeout alone still allows a steady unauthenticated
// population without this cap.
const MAX_WS_CONNECTIONS = 500;

// Retained relay state is bounded independently of WebSocket message size.
// A normal bot registers nine small tools and makes one call at a time; these
// ceilings leave ample event headroom without allowing public-key holders to
// turn registration or timeout caches into unbounded storage.
export const RELAY_LIMITS = Object.freeze({
  registrations: 200,
  toolsPerRegistration: 32,
  toolBytesPerRegistration: 64 * 1024,
  pendingCalls: 500,
  pendingCallsPerBot: 4,
  keyChars: 128,
  participantChars: 64,
});

const RELAY_KEY_RE = /^[A-Za-z0-9_-]+$/;

function isBoundedString(value, maxChars, maxBytes = maxChars * 4) {
  return typeof value === 'string'
    && value.length > 0
    && [...value].length <= maxChars
    && Buffer.byteLength(value, 'utf8') <= maxBytes;
}

function serializedBytes(value) {
  try {
    return Buffer.byteLength(JSON.stringify(value), 'utf8');
  } catch {
    return Infinity;
  }
}

function toolsAreValid(tools) {
  if (!Array.isArray(tools) || tools.length > RELAY_LIMITS.toolsPerRegistration) return false;
  if (serializedBytes(tools) > RELAY_LIMITS.toolBytesPerRegistration) return false;
  return tools.every((tool) => tool !== null
    && typeof tool === 'object'
    && !Array.isArray(tool)
    && isBoundedString(tool.name, 64)
    && (tool.description === undefined
      || (typeof tool.description === 'string' && Buffer.byteLength(tool.description, 'utf8') <= 4_096))
    && tool.inputSchema !== null
    && typeof tool.inputSchema === 'object'
    && !Array.isArray(tool.inputSchema));
}

function keysEqual(a, b) {
  const ba = Buffer.from(String(a ?? ''));
  const bb = Buffer.from(String(b ?? ''));
  return ba.length === bb.length && timingSafeEqual(ba, bb);
}

// Per-IP limit on WebSocket UPGRADE attempts (the express rate-limit
// middleware never sees upgrades). Sized for venue NAT: a whole room of
// bots reconnecting after a wifi blip must fit in one window.
const UPGRADES_PER_MIN_PER_IP = 120;

export class Relay {
  constructor({ workshopKey = '', trustProxy = 1 } = {}) {
    this.trustProxy = trustProxy;
    this._upgradeBuckets = new Map(); // ip -> {windowStart, count}
    // When set, register messages must carry the matching workshop_key —
    // the same shared secret bots already use for leaderboard POSTs. This
    // keeps drive-by connections from squatting registrations or filling
    // the bots map with junk. Empty = open (local dev).
    this.workshopKey = workshopKey;
    this.bots = new Map();     // key -> {ws, participant, tools, connectedAt, lastSeen, calls}
    this.pending = new Map();  // callId -> {resolve, timer, key, ws}
    this._nextCallId = 1;
  }

  // Attach the WebSocket endpoint to an existing http.Server.
  attach(httpServer, path = '/relay/ws') {
    const wss = new WebSocketServer({ noServer: true, maxPayload: MAX_WS_PAYLOAD });
    httpServer.on('upgrade', (req, socket, head) => {
      const { pathname } = new URL(req.url, 'http://x');
      if (pathname !== path) { socket.destroy(); return; }
      if (wss.clients.size >= MAX_WS_CONNECTIONS) { socket.destroy(); return; }
      if (!this._upgradeAllowed(req)) { socket.destroy(); return; }
      wss.handleUpgrade(req, socket, head, (ws) => wss.emit('connection', ws, req));
    });

    wss.on('connection', (ws) => {
      ws.isAlive = true;
      // A socket that connects but never (successfully) registers holds a
      // connection slot for nothing — drop it. Legit bots register
      // immediately on open.
      const regTimer = setTimeout(() => {
        if (!ws._relayKey) ws.terminate();
      }, REGISTER_TIMEOUT_MS);
      regTimer.unref();
      ws.on('pong', () => { ws.isAlive = true; });
      ws.on('message', (data) => {
        let msg;
        try { msg = JSON.parse(data.toString()); } catch { return; }
        this._onMessage(ws, msg);
      });
      ws.on('close', () => {
        clearTimeout(regTimer);
        this._onClose(ws);
      });
      ws.on('error', () => { /* close handler runs after */ });
    });

    // Heartbeat: drop connections that miss two pings (venue wifi is lossy;
    // a half-open TCP connection would otherwise black-hole tool calls).
    // Same sweep forgets registrations whose bot has been gone >1h — the
    // tools cache exists to bridge reconnects, not to grow forever.
    this._heartbeat = setInterval(() => {
      for (const ws of wss.clients) {
        if (!ws.isAlive) { ws.terminate(); continue; }
        ws.isAlive = false;
        ws.ping();
      }
      const cutoff = Date.now() - STALE_BOT_MS;
      for (const [key, bot] of this.bots) {
        if (!bot.ws && (bot.lastSeen || 0) < cutoff) this.bots.delete(key);
      }
    }, HEARTBEAT_MS);
    this._heartbeat.unref();
    return wss;
  }

  // Client IP for upgrade requests, honoring the same proxy-trust model as
  // the HTTP side: with a fronting proxy, the platform-appended (rightmost)
  // X-Forwarded-For entry is the client; without one, the socket address.
  _clientIp(req) {
    if (this.trustProxy > 0) {
      const xff = String(req.headers['x-forwarded-for'] || '');
      const parts = xff.split(',').map((s) => s.trim()).filter(Boolean);
      if (parts.length) return parts[Math.max(0, parts.length - this.trustProxy)];
    }
    return req.socket?.remoteAddress || 'unknown';
  }

  _upgradeAllowed(req) {
    const ip = this._clientIp(req);
    const now = Date.now();
    let b = this._upgradeBuckets.get(ip);
    if (!b || now - b.windowStart >= 60_000) {
      b = { windowStart: now, count: 0 };
      this._upgradeBuckets.set(ip, b);
    }
    if (this._upgradeBuckets.size > 10_000) this._upgradeBuckets.clear();
    return ++b.count <= UPGRADES_PER_MIN_PER_IP;
  }

  _onMessage(ws, msg) {
    if (!msg || typeof msg !== 'object' || Array.isArray(msg)) return;
    if (msg.type === 'register') {
      const { participant, key, tools } = msg;
      if (this.workshopKey && !keysEqual(msg.workshop_key, this.workshopKey)) {
        ws.send(JSON.stringify({
          type: 'error',
          error: 'invalid workshop key — set LEADERBOARD_KEY to the value from .env.event',
        }));
        ws.close();
        return;
      }
      // A socket has exactly one immutable registration. Without this check,
      // one connection can leave arbitrarily many key -> socket entries whose
      // close handler can only discover the last key.
      if (ws._relayKey) {
        ws.send(JSON.stringify({ type: 'error', error: 'socket is already registered' }));
        ws.close(1008, 'duplicate registration');
        return;
      }
      if (!isBoundedString(key, RELAY_LIMITS.keyChars, RELAY_LIMITS.keyChars)
          || key.length < 8
          || !RELAY_KEY_RE.test(key)) {
        ws.send(JSON.stringify({
          type: 'error',
          error: 'register requires an 8-128 character key using letters, digits, _ or -',
        }));
        ws.close();
        return;
      }
      if (!isBoundedString(participant, RELAY_LIMITS.participantChars, 256)) {
        ws.send(JSON.stringify({ type: 'error', error: 'participant must be a string of at most 64 characters' }));
        ws.close();
        return;
      }
      if (!toolsAreValid(tools)) {
        ws.send(JSON.stringify({ type: 'error', error: 'tool list is invalid or exceeds relay limits' }));
        ws.close();
        return;
      }
      // One bot per key: a re-registration (bot restart, network blip,
      // second machine) supersedes the old connection.
      const existing = this.bots.get(key);
      if (!existing && this.bots.size >= RELAY_LIMITS.registrations) {
        ws.send(JSON.stringify({ type: 'error', error: 'relay registration limit reached' }));
        ws.close(1013, 'registration limit reached');
        return;
      }
      if (existing?.ws && existing.ws !== ws && existing.ws.readyState === existing.ws.OPEN) {
        existing.ws.close(4000, 'superseded by new registration');
      }
      ws._relayKey = key;
      this.bots.set(key, {
        ws,
        participant,
        tools,
        connectedAt: existing?.ws === ws ? existing.connectedAt : Date.now(),
        lastSeen: Date.now(),
        calls: existing?.calls || 0,
      });
      ws.send(JSON.stringify({ type: 'registered', mcp_path: `/p/${key}/mcp` }));
      console.log(`[relay] registered '${this.bots.get(key).participant}' (${key.slice(0, 8)}…, ${this.bots.get(key).tools.length} tools)`);
      return;
    }
    if (msg.type === 'result') {
      const pending = this.pending.get(msg.id);
      if (!pending || pending.ws !== ws) return; // late or wrong-socket result — drop
      this.pending.delete(msg.id);
      clearTimeout(pending.timer);
      const bot = ws._relayKey && this.bots.get(ws._relayKey);
      if (bot) bot.lastSeen = Date.now();
      pending.resolve(msg.ok
        ? { ok: true, result: msg.result }
        : { ok: false, error: msg.error || 'tool call failed' });
      return;
    }
    if (msg.type === 'pong') {
      const bot = ws._relayKey && this.bots.get(ws._relayKey);
      if (bot) bot.lastSeen = Date.now();
    }
  }

  _onClose(ws) {
    const key = ws._relayKey;
    if (!key) return;
    const bot = this.bots.get(key);
    // Only forget the bot if THIS socket is still its current one
    // (a reconnect may have superseded it already).
    if (bot && bot.ws === ws) {
      // Keep the registration (tools list) so list_tools still answers
      // while the bot reconnects, but mark it offline.
      bot.ws = null;
      console.log(`[relay] '${bot.participant}' disconnected (${key.slice(0, 8)}…)`);
    }
    // Fail any in-flight calls for this key — better an immediate error
    // (the agent retries) than a 150s hang.
    for (const [id, p] of this.pending) {
      if (p.ws === ws) {
        this.pending.delete(id);
        clearTimeout(p.timer);
        p.resolve({ ok: false, error: 'bot disconnected mid-call — it should reconnect within seconds; retry' });
      }
    }
  }

  // ── API used by the MCP layer ───────────────────────────────────────
  isConnected(key) {
    const bot = this.bots.get(key);
    return !!(bot && bot.ws && bot.ws.readyState === bot.ws.OPEN);
  }

  // True if this key has (or recently had) a registration. Registrations
  // are workshop-key-gated, so "known" implies an authenticated bot put
  // it here — safe to use as a rate-limit identity.
  isKnown(key) {
    return this.bots.has(key);
  }

  tools(key) {
    return this.bots.get(key)?.tools || null;
  }

  participantName(key) {
    return this.bots.get(key)?.participant || null;
  }

  call(key, name, args, timeoutMs = CALL_TIMEOUT_MS) {
    const bot = this.bots.get(key);
    if (!bot || !bot.ws || bot.ws.readyState !== bot.ws.OPEN) {
      return Promise.resolve({
        ok: false,
        error: 'bot is not connected to the relay. On the participant machine, '
          + 'check that the bot is running (./setup.sh) and can reach the event server.',
      });
    }
    if (this.pending.size >= RELAY_LIMITS.pendingCalls) {
      return Promise.resolve({ ok: false, error: 'relay is at its global in-flight call limit; retry shortly' });
    }
    let callsForBot = 0;
    for (const pending of this.pending.values()) {
      if (pending.key === key) callsForBot++;
    }
    if (callsForBot >= RELAY_LIMITS.pendingCallsPerBot) {
      return Promise.resolve({ ok: false, error: 'bot is at its in-flight call limit; retry shortly' });
    }
    const id = `c${this._nextCallId++}`;
    bot.calls++;
    bot.lastCallAt = Date.now();
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        resolve({ ok: false, error: `tool call timed out after ${Math.round(timeoutMs / 1000)}s` });
      }, timeoutMs);
      this.pending.set(id, { resolve, timer, key, ws: bot.ws });
      bot.ws.send(JSON.stringify({ type: 'call', id, name, args }), (err) => {
        if (err) {
          this.pending.delete(id);
          clearTimeout(timer);
          resolve({ ok: false, error: `relay send failed: ${err.message}` });
        }
      });
    });
  }

  // ── admin / status ──────────────────────────────────────────────────
  status() {
    return [...this.bots.entries()].map(([key, b]) => ({
      key_prefix: key.slice(0, 8),
      participant: b.participant,
      connected: !!(b.ws && b.ws.readyState === b.ws.OPEN),
      connected_at: new Date(b.connectedAt).toISOString(),
      last_seen: b.lastSeen ? new Date(b.lastSeen).toISOString() : null,
      calls: b.calls,
      tools: b.tools.length,
    }));
  }
}
