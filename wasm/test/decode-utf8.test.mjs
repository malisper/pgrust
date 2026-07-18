// decode-utf8.test.mjs — regression test for public issue #34 (garbled Chinese
// column headers on the web demo).
//
// The wasm module's stdout arrives in the worker as byte chunks whose
// boundaries can fall in the MIDDLE of a multi-byte UTF-8 character. The old
// code decoded each chunk with a one-shot TextDecoder, so a character split
// across two chunks became U+FFFD replacement garbage. decodeUtf8Chunks must
// round-trip such splits exactly.
//
// Run: node wasm/test/decode-utf8.test.mjs
import test from 'node:test';
import assert from 'node:assert/strict';
import { decodeUtf8Chunks } from '../format.js';

const enc = new TextEncoder();

// A header-like line matching the issue's shape: Chinese column names in
// single-user printtup output. Every Chinese character is 3 bytes in UTF-8.
const CJK = '\t 1: 中文列标题 = "商品名称"\t(typeid = 25, len = -1)';

test('CJK string split at EVERY byte offset round-trips', () => {
  const bytes = enc.encode(CJK);
  assert.ok(bytes.length > CJK.length, 'test string must be multi-byte');
  for (let cut = 0; cut <= bytes.length; cut++) {
    const got = decodeUtf8Chunks([bytes.subarray(0, cut), bytes.subarray(cut)]);
    assert.equal(got, CJK, `split at byte ${cut} garbled the output`);
  }
});

test('every 3-way split of a 4-byte-emoji + CJK mix round-trips', () => {
  const s = '数据🐘库';
  const bytes = enc.encode(s);
  for (let a = 0; a <= bytes.length; a++) {
    for (let b = a; b <= bytes.length; b++) {
      const got = decodeUtf8Chunks([
        bytes.subarray(0, a),
        bytes.subarray(a, b),
        bytes.subarray(b),
      ]);
      assert.equal(got, s, `splits at bytes ${a},${b} garbled the output`);
    }
  }
});

test('the old one-shot-per-chunk pattern DOES garble a mid-character split (bug exists without the fix)', () => {
  const bytes = enc.encode('中');
  const oneShot = (chunks) => chunks.map((b) => new TextDecoder().decode(b)).join('');
  const garbled = oneShot([bytes.subarray(0, 1), bytes.subarray(1)]);
  assert.notEqual(garbled, '中');
  assert.ok(garbled.includes('�'));
  assert.equal(decodeUtf8Chunks([bytes.subarray(0, 1), bytes.subarray(1)]), '中');
});

test('empty and trivial inputs', () => {
  assert.equal(decodeUtf8Chunks([]), '');
  assert.equal(decodeUtf8Chunks([new Uint8Array(0)]), '');
  assert.equal(decodeUtf8Chunks([enc.encode('plain ascii')]), 'plain ascii');
});

test('genuinely truncated output flushes the dangling bytes as U+FFFD', () => {
  // If the stream really ends mid-character (crash mid-write), the final
  // flush must surface lossy replacement, not silently drop bytes.
  const bytes = enc.encode('中').subarray(0, 2); // 2 of 3 bytes
  assert.equal(decodeUtf8Chunks([bytes]), '�');
});
