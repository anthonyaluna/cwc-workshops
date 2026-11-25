// Copyright 2026 Anthropic PBC
// SPDX-License-Identifier: Apache-2.0

// Leaderboard state machine for the Agent Battle event server.
//
// Pure in-memory state with debounced disk snapshots. All scoring rules
// live here:
//   - achievements (diamond_1, diamond_2, …) are the source of truth for
//     diamond counts; each POST is gated by run_elapsed_ms <= 305s
//   - /cost posts carry tokens/turns and pick the tiebreaker run
//   - ranking = best-run diamonds DESC, that run's tokens ASC
//   - a "session window" gates all writes; outside it, posts get 403

import { readFileSync, statSync } from 'node:fs';
import { mkdir, rename, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';

import { isClean } from './moderation.mjs';

// Legacy milestone/quest points kept for back-compat with older bots that
// still post wooden_pickaxe etc. They don't affect Agent Battle ranking.
const TASK_POINTS = {
  wooden_pickaxe: 200, stone_pickaxe: 200, furnace: 200, iron_ingot: 400,
  iron_pickaxe: 200, diamond: 200,
  first_block: 100, chat_to_player: 100, meet_a_friend: 100,
  home_builder: 100, light_it_up: 100, deep_diver: 100,
};
const MILESTONE_IDS = new Set([
  'wooden_pickaxe', 'stone_pickaxe', 'furnace', 'iron_ingot',
  'iron_pickaxe', 'diamond',
]);
const DIAMOND_RE = /^diamond_([1-9]\d{0,2})$/;

// These ceilings are deliberately above the documented 100-participant
// event, while making the maximum retained board and snapshot size finite.
// All attacker-controlled strings are rejected before a Map/array allocation.
export const BOARD_LIMITS = Object.freeze({
  participants: 200,
  achievementsPerParticipant: 256,
  runsPerParticipant: 32,
  narrations: 500,
  participantChars: 64,
  runIdChars: 64,
  narrationTextChars: 500,
  snapshotBytes: 8 * 1024 * 1024,
});

const MAX_PARTICIPANT_BYTES = 256;
const MAX_RUN_ID_BYTES = 256;
const MAX_NARRATION_TEXT_BYTES = 2_000;
const MAX_DIAMOND_NUMBER = 999;
const MAX_SESSION_SECONDS = 86_400;
const NARRATION_KINDS = new Set(['chat', 'thought']);

function isPlainObject(value) {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isBoundedString(value, maxChars, maxBytes) {
  return typeof value === 'string'
    && value.length > 0
    && [...value].length <= maxChars
    && Buffer.byteLength(value, 'utf8') <= maxBytes;
}

function isParticipantName(value) {
  return isBoundedString(
    value,
    BOARD_LIMITS.participantChars,
    MAX_PARTICIPANT_BYTES,
  );
}

function isRunId(value) {
  return isBoundedString(value, BOARD_LIMITS.runIdChars, MAX_RUN_ID_BYTES);
}

function isTimestamp(value) {
  return typeof value === 'string'
    && value.length <= 40
    && Number.isFinite(Date.parse(value));
}

function isFiniteNumber(value, max) {
  return typeof value === 'number'
    && Number.isFinite(value)
    && value >= 0
    && value <= max;
}

function snapshotError(message) {
  throw new Error(`invalid board snapshot: ${message}`);
}

// 5 min + 5s grace; bot.js stamps run_elapsed_ms on every achievement.
export const MAX_RUN_MS = 305_000;

function pointsFor(id) {
  if (Object.hasOwn(TASK_POINTS, id)) return TASK_POINTS[id];
  const diamond = DIAMOND_RE.exec(id);
  if (diamond && Number(diamond[1]) <= MAX_DIAMOND_NUMBER) return 0;
  return undefined; // unknown → rejected
}

export class Board {
  constructor(snapshotPath) {
    this.snapshotPath = snapshotPath;
    this.participants = new Map(); // name -> participant record
    this.narrations = [];          // {participant, name, kind, text, ts}
    this.session = null;           // {opened_at, closed_at, duration_seconds}
    this._saveTimer = null;
    this._savePromise = Promise.resolve();
    this._load();
  }

  // ── persistence ───────────────────────────────────────────────────
  _load() {
    if (!this.snapshotPath) return;
    try {
      if (statSync(this.snapshotPath).size > BOARD_LIMITS.snapshotBytes) {
        snapshotError(`file exceeds ${BOARD_LIMITS.snapshotBytes} bytes`);
      }
      const state = this._decodeSnapshot(JSON.parse(readFileSync(this.snapshotPath, 'utf8')));
      this.participants = state.participants;
      this.narrations = state.narrations;
      this.session = state.session;
      console.log(`[board] restored ${this.participants.size} participants from ${this.snapshotPath}`);
    } catch (e) {
      if (e.code !== 'ENOENT') console.log('[board] snapshot restore failed:', e.message);
    }
  }

  save() {
    if (!this.snapshotPath || this._saveTimer) return;
    this._saveTimer = setTimeout(() => {
      this._saveTimer = null;
      void this.flush();
    }, 500);
  }

  // Queue atomic asynchronous writes so participant requests never block on
  // disk I/O. The state itself is hard-bounded before this serialization.
  flush() {
    if (!this.snapshotPath) return this._savePromise;
    if (this._saveTimer) {
      clearTimeout(this._saveTimer);
      this._saveTimer = null;
    }
    let text;
    try {
      text = JSON.stringify(this.toJSON());
      if (Buffer.byteLength(text, 'utf8') > BOARD_LIMITS.snapshotBytes) {
        throw new Error(`snapshot exceeds ${BOARD_LIMITS.snapshotBytes} bytes`);
      }
    } catch (e) {
      console.log('[board] snapshot serialization failed:', e.message);
      return this._savePromise;
    }

    const writeSnapshot = async () => {
      const temporaryPath = `${this.snapshotPath}.tmp`;
      try {
        await mkdir(dirname(this.snapshotPath), { recursive: true });
        await writeFile(temporaryPath, text);
        await rename(temporaryPath, this.snapshotPath);
      } catch (e) {
        console.log('[board] snapshot save failed:', e.message);
      }
    };
    this._savePromise = this._savePromise.then(writeSnapshot, writeSnapshot);
    return this._savePromise;
  }

  toJSON() {
    return {
      participants: [...this.participants.values()].map((p) => ({
        ...p,
        completed_tasks: Object.fromEntries(p.completed_tasks),
        runs: Object.fromEntries(p.runs),
      })),
      narrations: this.narrations.slice(-200),
      session: this.session,
    };
  }

  restore(json) {
    const state = this._decodeSnapshot(json);
    this.participants = state.participants;
    this.narrations = state.narrations;
    this.session = state.session;
    this.save();
  }

  _decodeSnapshot(json) {
    if (!isPlainObject(json)) snapshotError('root must be an object');
    const rawParticipants = json.participants ?? [];
    const rawNarrations = json.narrations ?? [];
    if (!Array.isArray(rawParticipants)
        || rawParticipants.length > BOARD_LIMITS.participants) {
      snapshotError(`participants must contain at most ${BOARD_LIMITS.participants} entries`);
    }
    if (!Array.isArray(rawNarrations)
        || rawNarrations.length > BOARD_LIMITS.narrations) {
      snapshotError(`narrations must contain at most ${BOARD_LIMITS.narrations} entries`);
    }

    const participants = new Map();
    for (const raw of rawParticipants) {
      if (!isPlainObject(raw) || !isParticipantName(raw.name) || raw.id !== raw.name) {
        snapshotError('participant identity is invalid');
      }
      if (participants.has(raw.name)) snapshotError('participant names must be unique');

      // Old snapshots stored completed_tasks as an array (Set); current
      // snapshots store an object (id -> ISO timestamp).
      const taskEntries = Array.isArray(raw.completed_tasks)
        ? raw.completed_tasks.map((id) => [id, null])
        : isPlainObject(raw.completed_tasks ?? {})
          ? Object.entries(raw.completed_tasks ?? {})
          : snapshotError('completed_tasks must be an object or legacy array');
      if (taskEntries.length > BOARD_LIMITS.achievementsPerParticipant) {
        snapshotError('participant achievement quota exceeded');
      }
      const completedTasks = new Map();
      for (const [id, timestamp] of taskEntries) {
        if (typeof id !== 'string' || pointsFor(id) === undefined
            || (timestamp !== null && !isTimestamp(timestamp))) {
          snapshotError('achievement entry is invalid');
        }
        completedTasks.set(id, timestamp);
      }

      if (!isPlainObject(raw.runs ?? {})) snapshotError('runs must be an object');
      const runEntries = Object.entries(raw.runs ?? {});
      if (runEntries.length > BOARD_LIMITS.runsPerParticipant) {
        snapshotError('participant run quota exceeded');
      }
      const runs = new Map();
      for (const [runId, run] of runEntries) {
        if (!isRunId(runId) || !isPlainObject(run)
            || !isFiniteNumber(run.tokens, 1_000_000_000)
            || !isFiniteNumber(run.turns, 1_000_000)
            || !isFiniteNumber(run.diamonds, 1_000_000)
            || !isTimestamp(run.updated_at)) {
          snapshotError('run entry is invalid');
        }
        runs.set(runId, {
          tokens: run.tokens,
          turns: run.turns,
          diamonds: run.diamonds,
          updated_at: run.updated_at,
        });
      }

      const lastActivity = raw.last_activity ?? null;
      if (lastActivity !== null && !isTimestamp(lastActivity)) {
        snapshotError('last_activity is invalid');
      }
      const participant = {
        id: raw.name,
        name: raw.name,
        completed_tasks: completedTasks,
        runs,
        last_activity: lastActivity,
      };
      if (raw.live_run !== undefined) {
        if (!isPlainObject(raw.live_run)
            || !isFiniteNumber(raw.live_run.at, Number.MAX_SAFE_INTEGER)
            || !isFiniteNumber(raw.live_run.remaining_ms, MAX_RUN_MS)) {
          snapshotError('live_run is invalid');
        }
        participant.live_run = {
          at: raw.live_run.at,
          remaining_ms: raw.live_run.remaining_ms,
        };
      }
      participants.set(raw.name, participant);
    }

    const narrations = rawNarrations.map((raw) => {
      if (!isPlainObject(raw)
          || !isParticipantName(raw.participant)
          || raw.name !== raw.participant
          || !NARRATION_KINDS.has(raw.kind)
          || !isBoundedString(
            raw.text,
            BOARD_LIMITS.narrationTextChars,
            MAX_NARRATION_TEXT_BYTES,
          )
          || !isFiniteNumber(raw.ts, Number.MAX_SAFE_INTEGER)) {
        snapshotError('narration entry is invalid');
      }
      return {
        participant: raw.participant,
        name: raw.name,
        kind: raw.kind,
        text: raw.text,
        ts: raw.ts,
      };
    });

    let session = null;
    if (json.session !== undefined && json.session !== null) {
      const raw = json.session;
      if (!isPlainObject(raw)
          || !isTimestamp(raw.opened_at)
          || (raw.closed_at !== null && !isTimestamp(raw.closed_at))
          || !isFiniteNumber(raw.duration_seconds, MAX_SESSION_SECONDS)
          || raw.duration_seconds < 1) {
        snapshotError('session is invalid');
      }
      session = {
        opened_at: raw.opened_at,
        closed_at: raw.closed_at,
        duration_seconds: raw.duration_seconds,
      };
    }
    return { participants, narrations, session };
  }

  // ── session window ────────────────────────────────────────────────
  // Until a session is first opened, the window is treated as always-
  // open so local/solo testing needs no admin steps. Once opened, the
  // window is enforced strictly.
  sessionWindow() {
    const s = this.session;
    if (!s) return { open: true, devAlwaysOpen: true };
    const opened = new Date(s.opened_at).getTime();
    const closes = s.closed_at
      ? new Date(s.closed_at).getTime()
      : opened + (s.duration_seconds || 2700) * 1000;
    const now = Date.now();
    // Strict `<`: an explicit close means closed from that instant — a
    // write landing in the same millisecond as closeSession() must not
    // slip in (this was an observable race under test).
    const open = now >= opened && now < closes;
    return {
      open,
      opened_at: s.opened_at,
      closes_at: new Date(closes).toISOString(),
      remaining_seconds: open ? Math.max(0, Math.floor((closes - now) / 1000)) : 0,
    };
  }

  openSession(durationSeconds) {
    const requested = Math.round(Number(durationSeconds));
    this.session = {
      opened_at: new Date().toISOString(),
      closed_at: null,
      duration_seconds: Number.isFinite(requested) && requested > 0
        ? Math.min(requested, MAX_SESSION_SECONDS)
        : 1800,
    };
    this.save();
    return this.session;
  }

  closeSession() {
    if (!this.session || this.session.closed_at) return null;
    this.session.closed_at = new Date().toISOString();
    this.save();
    return this.session;
  }

  // Clear all scores/chat. Session is also cleared so the next event
  // starts from "window not yet opened".
  reset() {
    const n = this.participants.size;
    this.participants = new Map();
    this.narrations = [];
    this.session = null;
    this.save();
    return { cleared_participants: n };
  }

  // ── participants ──────────────────────────────────────────────────
  participant(rawName) {
    const name = rawName;
    if (!this.participants.has(name)) {
      if (this.participants.size >= BOARD_LIMITS.participants) {
        throw new Error('participant quota exceeded');
      }
      this.participants.set(name, {
        id: name,
        name,
        completed_tasks: new Map(), // id -> ISO ts of first achievement
        runs: new Map(),            // run_id -> {tokens, turns, diamonds, updated_at}
        last_activity: null,
      });
    }
    return this.participants.get(name);
  }

  // ── writes (called from authenticated routes) ─────────────────────
  recordAchievement({ participant, id, run_elapsed_ms }) {
    if (!participant || !id) {
      return { status: 400, body: { error: 'participant and id required' } };
    }
    if (!isParticipantName(participant) || typeof id !== 'string') {
      return { status: 400, body: { error: 'participant must be a string of at most 64 characters' } };
    }
    if (!isClean(participant)) {
      return { status: 422, body: { error: 'participant name rejected' } };
    }
    if (!this.sessionWindow().open) {
      return { status: 403, body: { error: 'session not open' } };
    }
    // Hard 5-min run cap, enforced server-side. null/missing = old bot;
    // reject so participants must update.
    if (!isFiniteNumber(run_elapsed_ms, MAX_RUN_MS)) {
      return {
        status: 403,
        body: { error: `run_elapsed_ms ${run_elapsed_ms} outside 0..${MAX_RUN_MS}` },
      };
    }
    const points = pointsFor(id);
    if (points === undefined) {
      return { status: 400, body: { error: `unknown achievement: ${id}` } };
    }
    const current = this.participants.get(participant);
    if (!current && this.participants.size >= BOARD_LIMITS.participants) {
      return { status: 429, body: { error: 'participant limit reached' } };
    }
    if (current && !current.completed_tasks.has(id)
        && current.completed_tasks.size >= BOARD_LIMITS.achievementsPerParticipant) {
      return { status: 429, body: { error: 'achievement limit reached for participant' } };
    }
    const p = this.participant(participant);
    p.completed_tasks.set(id, new Date().toISOString());
    p.last_activity = new Date().toISOString();
    // Track the live run's clock so the cast view can show per-participant
    // time remaining.
    p.live_run = { at: Date.now(), remaining_ms: MAX_RUN_MS - run_elapsed_ms };
    this.save();
    return { status: 200, body: { ok: true, participant_id: p.id, task_id: id, points } };
  }

  recordCost({ participant, tokens, turns, diamonds = 0, run_id }) {
    if (!participant || tokens === undefined || turns === undefined) {
      return { status: 400, body: { error: 'participant, tokens, turns required' } };
    }
    if (!isParticipantName(participant)) {
      return { status: 400, body: { error: 'participant must be a string of at most 64 characters' } };
    }
    if (!isClean(participant)) {
      return { status: 422, body: { error: 'participant name rejected' } };
    }
    if (!isFiniteNumber(tokens, 1_000_000_000)
        || !isFiniteNumber(turns, 1_000_000)
        || !isFiniteNumber(diamonds, 1_000_000)) {
      return { status: 400, body: { error: 'tokens, turns, and diamonds must be finite non-negative numbers' } };
    }
    if (!this.sessionWindow().open) {
      return { status: 403, body: { error: 'session not open' } };
    }
    const rid = run_id || 'default';
    if (!isRunId(rid)) {
      return { status: 400, body: { error: 'run_id must be a string of at most 64 characters' } };
    }
    const current = this.participants.get(participant);
    if (!current && this.participants.size >= BOARD_LIMITS.participants) {
      return { status: 429, body: { error: 'participant limit reached' } };
    }
    if (current && !current.runs.has(rid)
        && current.runs.size >= BOARD_LIMITS.runsPerParticipant) {
      return { status: 429, body: { error: 'run limit reached for participant' } };
    }
    const p = this.participant(participant);
    // Mark live on /cost too — achievements only fire on diamond pickup,
    // so a participant who's running but hasn't found one yet would
    // otherwise show as idle on the cast view.
    p.last_activity = new Date().toISOString();
    // diamonds is self-reported (participant owns the bot AND the harness).
    // Real integrity = top-3 verify.py replay. This monotonic clamp just
    // makes the trivial curl cheat show as a slow climb instead of a spike.
    const prev = p.runs.get(rid);
    const prevD = prev?.diamonds ?? 0;
    const reqD = Math.max(0, Math.round(diamonds));
    const clampedD = Math.max(prevD, Math.min(reqD, prevD + 10));
    p.runs.set(rid, {
      tokens: Math.round(tokens),
      turns: Math.round(turns),
      diamonds: clampedD,
      updated_at: new Date().toISOString(),
    });
    this.save();
    return { status: 200, body: { ok: true, diamonds: clampedD, clamped: clampedD !== reqD } };
  }

  recordNarration({ participant, kind, text }) {
    if (!participant || !text) {
      return { status: 400, body: { error: 'participant and text required' } };
    }
    if (!isParticipantName(participant)
        || !isBoundedString(
          text,
          BOARD_LIMITS.narrationTextChars,
          MAX_NARRATION_TEXT_BYTES,
        )) {
      return { status: 400, body: { error: 'participant or narration text is too large' } };
    }
    const narrationKind = kind || 'chat';
    if (!NARRATION_KINDS.has(narrationKind)) {
      return { status: 400, body: { error: 'kind must be "chat" or "thought"' } };
    }
    if (!isClean(participant) || !isClean(text)) {
      return { status: 422, body: { error: 'content rejected' } };
    }
    // Note: deliberately does NOT create a participant record — chat alone
    // shouldn't add rows to the board (kept the ticker usable pre-window
    // without letting it pollute the standings).
    const name = participant;
    this.narrations.push({
      participant: name,
      name,
      kind: narrationKind,
      text,
      ts: Date.now(),
    });
    if (this.narrations.length > BOARD_LIMITS.narrations) {
      this.narrations.splice(0, this.narrations.length - BOARD_LIMITS.narrations);
    }
    this.save();
    return { status: 200, body: { ok: true } };
  }

  // ── reads ─────────────────────────────────────────────────────────
  getNarrations({ since = 0, limit = 200 } = {}) {
    const lim = Math.min(limit, 500);
    return this.narrations.filter((n) => n.ts > since).slice(-lim);
  }

  leaderboard() {
    const w = this.sessionWindow();
    const inWindow = (ts) => w.devAlwaysOpen
      || (ts && ts >= w.opened_at && ts <= w.closes_at);
    return [...this.participants.values()].map((p) => {
      const tasks = [...p.completed_tasks.entries()]
        .filter(([, ts]) => inWindow(ts))
        .map(([id]) => id);
      const pts = tasks.reduce((s, t) => s + (pointsFor(t) || 0), 0);
      // Best single run within the session window: max diamonds, then min tokens.
      let best = null;
      let latest = null;
      for (const [rid, r] of p.runs) {
        if (!inWindow(r.updated_at)) continue;
        if (!latest || r.updated_at > latest.updated_at) latest = { run_id: rid, ...r };
        if (!best
            || r.diamonds > best.diamonds
            || (r.diamonds === best.diamonds && r.tokens < best.tokens)) {
          best = { run_id: rid, ...r };
        }
      }
      // diamond_N achievements are server-gated (rejected past 305s), so
      // max(N) over them is the trusted best-run count. The /cost
      // 'diamonds' field is self-reported and NOT time-gated.
      const diamondNs = tasks
        .map((t) => { const m = /^diamond_(\d+)$/.exec(t); return m ? +m[1] : 0; })
        .filter((n) => n > 0);
      const diamondsCount = diamondNs.length ? Math.max(...diamondNs) : 0;
      return {
        id: p.id,
        name: p.name,
        achievement_points: pts,
        tokens: best?.tokens ?? 0,
        turns: best?.turns ?? 0,
        diamonds_count: diamondsCount,
        best_run_id: best?.run_id ?? null,
        latest_run: latest,
        runs_count: p.runs.size,
        run_remaining_s: (() => {
          if (!p.live_run) return null;
          const left = Math.round((p.live_run.remaining_ms - (Date.now() - p.live_run.at)) / 1000);
          return left > 0 ? left : null;
        })(),
        milestones: tasks.filter((t) => MILESTONE_IDS.has(t)).length,
        quests: tasks.filter((t) => !MILESTONE_IDS.has(t) && !DIAMOND_RE.test(t)).length,
        last_activity: p.last_activity,
      };
    }).sort((a, b) => {
      // Agent Battle: best-run diamonds desc, then that run's tokens asc.
      if (b.diamonds_count !== a.diamonds_count) return b.diamonds_count - a.diamonds_count;
      return a.tokens - b.tokens;
    });
  }
}
