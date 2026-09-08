// sab-pipe-host-gate.test.mjs — the HOST GATE: one outstanding `Atomics.waitAsync`
// however many rings one non-blocking host pumps.
//
// Why it exists. A host that drives N sessions used to park each ring's
// `readAsync` on that ring's own SEQ, so the agent held N outstanding async
// waiters at once. On WebKit that shape intermittently freezes the WHOLE agent
// for about a second (measured, Safari 26.6.2: ~1 freeze per 500 round trips
// with four rings, none with one), which is why `readAsync` now parks on a
// shared host gate and re-tests. The property that must not regress is the one
// the fix is FOR: however many rings are waiting, this agent has exactly ONE
// live wait on the gate.
//
// Everything here runs on one thread — the producer writes from the same agent
// — so nothing blocks and no worker is needed.
//
// Run: node wasm/test/sab-pipe-host-gate.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { SabPipe } from '../sab-pipe.js';

const enc = new TextEncoder();
const read = async (pipe, max = 64) => {
  const buf = new Uint8Array(max);
  const n = await pipe.readAsync(buf, max);
  return new TextDecoder().decode(buf.subarray(0, n));
};

test('a gated ring carries its host gate across a descriptor', () => {
  const hostGate = SabPipe.createHostGate();
  const pipe = SabPipe.create(1 << 10, { hostGate });
  const rebuilt = SabPipe.from(pipe.descriptor());
  assert.equal(rebuilt.hostGateSab, hostGate, 'the descriptor must carry the gate by reference');
  assert.equal(SabPipe.from(SabPipe.create(1 << 10).descriptor()).hostGateSab, null);
});

test('a write bumps the host gate as well as SEQ', () => {
  const hostGate = SabPipe.createHostGate();
  const view = new Int32Array(hostGate);
  const pipe = SabPipe.create(1 << 10, { hostGate });
  assert.equal(Atomics.load(view, 0), 0);
  pipe.write(enc.encode('a'));
  assert.equal(Atomics.load(view, 0), 1, 'the producer must announce on the host gate');
});

test('N rings waiting on one host gate hold ONE live wait, and all of them wake', async () => {
  const hostGate = SabPipe.createHostGate();
  const pipes = Array.from({ length: 4 }, () => SabPipe.create(1 << 10, { hostGate }));
  const reads = pipes.map((pipe) => read(pipe));
  // Let every reader reach its park before anything is written.
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(SabPipe._hostGateWaits.size, 1, 'four parked readers, one live wait');

  pipes.forEach((pipe, index) => pipe.write(enc.encode(`ring${index}`)));
  assert.deepEqual(await Promise.all(reads), ['ring0', 'ring1', 'ring2', 'ring3']);
});

test('a ring without a host gate still parks on its own SEQ', async () => {
  const pipe = SabPipe.create(1 << 10);
  const pending = read(pipe);
  await new Promise((resolve) => setTimeout(resolve, 10));
  assert.equal(SabPipe._hostGateWaits.size, 0, 'an ungated ring must not touch the shared map');
  pipe.write(enc.encode('plain'));
  assert.equal(await pending, 'plain');
});

test('a byte that lands between the gate read and the readiness test is not slept through', async () => {
  // The SEQ-first discipline, on the host gate: `readAsync` reads the gate BEFORE it tests the
  // ring, so a write in that window makes the park return at once. Writing before the reader
  // ever parks is the strongest form of that window.
  const hostGate = SabPipe.createHostGate();
  const pipe = SabPipe.create(1 << 10, { hostGate });
  pipe.write(enc.encode('early'));
  assert.equal(await read(pipe), 'early');
  assert.equal(SabPipe._hostGateWaits.size, 0, 'a ring with bytes ready must not park at all');
});

test('EOF is delivered to a parked gated reader', async () => {
  const hostGate = SabPipe.createHostGate();
  const pipe = SabPipe.create(1 << 10, { hostGate });
  const pending = pipe.readAsync(new Uint8Array(8), 8);
  await new Promise((resolve) => setTimeout(resolve, 10));
  pipe.close();
  assert.equal(await pending, 0, 'a closed, drained ring reads 0');
});
