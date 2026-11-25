// Copyright 2026 Anthropic PBC
// SPDX-License-Identifier: Apache-2.0

import assert from 'node:assert/strict';
import test from 'node:test';

import { Board, BOARD_LIMITS } from '../lib/board.mjs';
import { Relay, RELAY_LIMITS } from '../lib/relay.mjs';

function relaySocket() {
  return {
    OPEN: 1,
    readyState: 1,
    sent: [],
    closed: [],
    send(payload, callback) {
      this.sent.push(JSON.parse(payload));
      callback?.();
    },
    close(code, reason) {
      this.readyState = 3;
      this.closed.push({ code, reason });
    },
  };
}

function register(relay, ws, overrides = {}) {
  relay._onMessage(ws, {
    type: 'register',
    participant: 'alice',
    key: 'relay-key-123456',
    tools: [{ name: 'get_state', description: 'state', inputSchema: { type: 'object' } }],
    ...overrides,
  });
}

test('board rejects attacker fields before retaining them', () => {
  const board = new Board(null);

  const longName = board.recordCost({
    participant: 'x'.repeat(BOARD_LIMITS.participantChars + 1),
    tokens: 1,
    turns: 1,
    run_id: 'r1',
  });
  assert.equal(longName.status, 400);
  assert.equal(board.participants.size, 0);

  const longDiamond = board.recordAchievement({
    participant: 'alice',
    id: `diamond_${'1'.repeat(100)}`,
    run_elapsed_ms: 1,
  });
  assert.equal(longDiamond.status, 400);
  assert.equal(board.participants.size, 0);

  const inheritedAchievement = board.recordAchievement({
    participant: 'alice',
    id: 'toString',
    run_elapsed_ms: 1,
  });
  assert.equal(inheritedAchievement.status, 400);
  assert.equal(board.participants.size, 0);

  const longKind = board.recordNarration({
    participant: 'alice',
    kind: 'x'.repeat(100_000),
    text: 'hello',
  });
  assert.equal(longKind.status, 400);
  assert.equal(board.narrations.length, 0);

  const ordinaryNarration = board.recordNarration({
    participant: 'alice',
    kind: 'thought',
    text: 'checking the next move',
  });
  assert.equal(ordinaryNarration.status, 200);
  assert.equal(board.narrations[0].kind, 'thought');
});

test('board quotas preserve updates to existing entries', () => {
  const board = new Board(null);
  for (let i = 1; i <= BOARD_LIMITS.achievementsPerParticipant; i++) {
    assert.equal(board.recordAchievement({
      participant: 'alice',
      id: `diamond_${i}`,
      run_elapsed_ms: i,
    }).status, 200);
  }
  assert.equal(board.recordAchievement({
    participant: 'alice',
    id: `diamond_${BOARD_LIMITS.achievementsPerParticipant + 1}`,
    run_elapsed_ms: 1,
  }).status, 429);
  assert.equal(board.recordAchievement({
    participant: 'alice',
    id: 'diamond_1',
    run_elapsed_ms: 2,
  }).status, 200);

  for (let i = 0; i < BOARD_LIMITS.runsPerParticipant; i++) {
    assert.equal(board.recordCost({
      participant: 'alice', tokens: i, turns: i, run_id: `run-${i}`,
    }).status, 200);
  }
  assert.equal(board.recordCost({
    participant: 'alice', tokens: 1, turns: 1, run_id: 'one-run-too-many',
  }).status, 429);
  assert.equal(board.recordCost({
    participant: 'alice', tokens: 2, turns: 2, run_id: 'run-0',
  }).status, 200);
  const row = board.leaderboard()[0];
  assert.equal(row.runs_count, BOARD_LIMITS.runsPerParticipant);
  assert.ok(row.latest_run);
  assert.equal('runs' in row, false);
  assert.equal('completed_tasks' in row, false);
});

test('board caps global participants and narration history', () => {
  const board = new Board(null);
  for (let i = 0; i < BOARD_LIMITS.participants; i++) {
    assert.equal(board.recordCost({
      participant: `p${i}`, tokens: i, turns: i, run_id: 'default',
    }).status, 200);
  }
  assert.equal(board.recordCost({
    participant: 'one-too-many', tokens: 1, turns: 1, run_id: 'default',
  }).status, 429);
  assert.equal(board.recordCost({
    participant: 'p0', tokens: 2, turns: 2, run_id: 'default',
  }).status, 200);

  for (let i = 0; i <= BOARD_LIMITS.narrations; i++) {
    assert.equal(board.recordNarration({
      participant: 'p0', kind: 'chat', text: `line ${i}`,
    }).status, 200);
  }
  assert.equal(board.narrations.length, BOARD_LIMITS.narrations);
  assert.equal(board.narrations[0].text, 'line 1');
});

test('snapshot restore cannot bypass board quotas', () => {
  const board = new Board(null);
  const participants = Array.from({ length: BOARD_LIMITS.participants + 1 }, (_, i) => ({
    id: `p${i}`,
    name: `p${i}`,
    completed_tasks: {},
    runs: {},
    last_activity: null,
  }));
  assert.throws(
    () => board.restore({ participants, narrations: [], session: null }),
    /participant/,
  );
  assert.equal(board.participants.size, 0);
});

test('relay accepts one bounded registration per socket', () => {
  const relay = new Relay();
  const first = relaySocket();
  register(relay, first);
  assert.equal(relay.bots.size, 1);
  assert.equal(relay.tools('relay-key-123456').length, 1);

  register(relay, first, { key: 'second-key-12345' });
  assert.equal(first.sent.at(-1).type, 'error');
  assert.equal(relay.bots.size, 1);
  assert.equal(relay.isKnown('second-key-12345'), false);

  relay._onClose(first);
  const reconnect = relaySocket();
  register(relay, reconnect);
  assert.equal(relay.isConnected('relay-key-123456'), true);
  assert.equal(relay.tools('relay-key-123456').length, 1);
});

test('relay rejects oversized tools before allocation', () => {
  const relay = new Relay();
  const ws = relaySocket();
  register(relay, ws, {
    tools: [{
      name: 'get_state',
      inputSchema: { type: 'object', description: 'x'.repeat(RELAY_LIMITS.toolBytesPerRegistration) },
    }],
  });
  assert.equal(ws.sent.at(-1).type, 'error');
  assert.equal(relay.bots.size, 0);
});

test('relay close cleans only that socket calls', async () => {
  const relay = new Relay();
  const oldSocket = relaySocket();
  register(relay, oldSocket);
  const oldCall = relay.call('relay-key-123456', 'get_state', {}, 60_000);

  const replacement = relaySocket();
  register(relay, replacement);
  const replacementCall = relay.call('relay-key-123456', 'get_state', {}, 60_000);

  relay._onClose(oldSocket);
  assert.match((await oldCall).error, /disconnected/);
  assert.equal(relay.pending.size, 1);
  assert.equal(relay.isConnected('relay-key-123456'), true);

  relay._onClose(replacement);
  assert.match((await replacementCall).error, /disconnected/);
  assert.equal(relay.pending.size, 0);
});

test('relay caps registrations and pending calls', async () => {
  const relay = new Relay();
  for (let i = 0; i < RELAY_LIMITS.registrations; i++) {
    register(relay, relaySocket(), {
      participant: `p${i}`,
      key: `key-${String(i).padStart(8, '0')}`,
    });
  }
  const overflow = relaySocket();
  register(relay, overflow, { key: 'overflow-key-123' });
  assert.equal(overflow.sent.at(-1).type, 'error');
  assert.equal(relay.bots.size, RELAY_LIMITS.registrations);

  const key = 'key-00000000';
  const ws = relay.bots.get(key).ws;
  const pending = Array.from(
    { length: RELAY_LIMITS.pendingCallsPerBot },
    () => relay.call(key, 'get_state', {}, 60_000),
  );
  const refused = await relay.call(key, 'get_state', {}, 60_000);
  assert.match(refused.error, /in-flight call limit/);
  assert.equal(relay.pending.size, RELAY_LIMITS.pendingCallsPerBot);

  relay._onClose(ws);
  await Promise.all(pending);
  assert.equal(relay.pending.size, 0);
});
