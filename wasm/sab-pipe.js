// sab-pipe.js — a single-producer/single-consumer byte ring over a
// SharedArrayBuffer, with a REAL blocking read (Atomics.wait) on the consumer
// side and an async (Atomics.waitAsync) read for the agent that may not block
// — the Node/browser main thread.
//
// Why this exists (spike/wasip1-threads, checkpoint (a)): the wasm32-wasip1
// guest's between-statements stdin read is a plain blocking `read(0)`
// (pqcomm_stdio::secure_read -> WASI fd_read). The shipped single-threaded
// demo makes that read suspend with JSPI (wasm/wiresession.js). The threads
// build does not need JSPI at all: the guest runs on a spawned Worker, and a
// Worker is allowed to call Atomics.wait — so fd_read can BLOCK exactly the
// way it blocks under wasmtime's host pipe. No JSPI anywhere in this file or
// its callers.
//
// Layout (one SharedArrayBuffer per direction):
//   Int32Array header, 4 slots:
//     [0] READ  — bytes consumed  (free-running int32, wraps)
//     [1] WRITE — bytes produced  (free-running int32, wraps)
//     [2] CLOSED— 1 once the producer will never write again
//     [3] SEQ   — bumped on every state change; the futex word both sides wait
//                 on (waiting on READ/WRITE directly would race a wrap).
//   byte payload of `capacity` bytes, capacity a power of two.
//
// READ/WRITE are free-running int32 counters; the ring is full iff
// (WRITE - READ) === capacity and empty iff they are equal. Both advance with
// Atomics.add, so they wrap modulo 2^32, and every difference is taken as
// `(w - r) | 0` — ToInt32 makes the subtraction wrap the same way, so the
// occupancy stays exact across the wrap with no rebase and no lock.
//
// SPSC is the contract: exactly one agent writes and exactly one reads a given
// pipe. In the threads spike, stdin is driver->guest and stdout is
// guest->driver, and only the wire-session thread does wire I/O.

const HDR_I32 = 4;
const H_READ = 0;
const H_WRITE = 1;
const H_CLOSED = 2;
const H_SEQ = 3;

export class SabPipe {
  // capacity must be a power of two.
  static create(capacity = 1 << 20) {
    if ((capacity & (capacity - 1)) !== 0) {
      throw new Error(`SabPipe capacity must be a power of two, got ${capacity}`);
    }
    const sab = new SharedArrayBuffer(HDR_I32 * 4 + capacity);
    return new SabPipe(sab, capacity);
  }

  // Rehydrate the same pipe in another agent from the transferred descriptor.
  static from(desc) {
    return new SabPipe(desc.sab, desc.capacity);
  }

  constructor(sab, capacity) {
    this.sab = sab;
    this.capacity = capacity;
    this.mask = capacity - 1;
    this.hdr = new Int32Array(sab, 0, HDR_I32);
    this.buf = new Uint8Array(sab, HDR_I32 * 4, capacity);
  }

  // Structured-cloneable handle (SharedArrayBuffer is cloned by reference).
  descriptor() {
    return { sab: this.sab, capacity: this.capacity };
  }

  _bump() {
    Atomics.add(this.hdr, H_SEQ, 1);
    Atomics.notify(this.hdr, H_SEQ);
  }

  get closed() {
    return Atomics.load(this.hdr, H_CLOSED) === 1;
  }

  available() {
    return (Atomics.load(this.hdr, H_WRITE) - Atomics.load(this.hdr, H_READ)) | 0;
  }

  // Producer side's twin of available(): how many bytes fit right now.
  room() {
    return this.capacity - this.available();
  }

  close() {
    Atomics.store(this.hdr, H_CLOSED, 1);
    this._bump();
  }

  // Producer side. Blocks (Atomics.wait) only if the ring is FULL, which is
  // the faithful pipe behaviour; `block: false` short-writes instead and
  // returns the count actually enqueued.
  write(bytes, { block = true } = {}) {
    let off = 0;
    while (off < bytes.length) {
      const seq = Atomics.load(this.hdr, H_SEQ);
      const r = Atomics.load(this.hdr, H_READ);
      const w = Atomics.load(this.hdr, H_WRITE);
      const room = this.capacity - ((w - r) | 0);
      if (room === 0) {
        if (!block) return off;
        // Only a Worker (or Node's main thread) may block here; the browser
        // page never writes enough at once to fill a 1MiB ring.
        Atomics.wait(this.hdr, H_SEQ, seq);
        continue;
      }
      const n = Math.min(room, bytes.length - off);
      const start = w & this.mask;
      const first = Math.min(n, this.capacity - start);
      this.buf.set(bytes.subarray(off, off + first), start);
      if (n > first) this.buf.set(bytes.subarray(off + first, off + n), 0);
      Atomics.add(this.hdr, H_WRITE, n);
      off += n;
      this._bump();
    }
    return off;
  }

  // Consumer side, BLOCKING. Copies at most `maxLen` bytes into u8 at offset 0
  // and returns the count; returns 0 only at EOF (producer closed and drained).
  // Blocks in Atomics.wait while the ring is empty and open — this is the call
  // the guest's WASI fd_read(0) lands on.
  readInto(u8, maxLen) {
    const want = Math.min(maxLen, u8.length);
    if (want <= 0) return 0;
    for (;;) {
      const seq = Atomics.load(this.hdr, H_SEQ);
      const r = Atomics.load(this.hdr, H_READ);
      const w = Atomics.load(this.hdr, H_WRITE);
      const avail = (w - r) | 0;
      if (avail > 0) return this._drain(u8, want, r, avail);
      if (this.closed) return 0;
      Atomics.wait(this.hdr, H_SEQ, seq);
    }
  }

  // Consumer side, BOUNDED blocking readiness wait — the poll(2) half of the
  // blocking read above. Returns true once at least one byte is readable or
  // the producer closed, false when `timeoutMs` elapsed first. This is what
  // the host's `poll_oneoff` lands on when the guest polls fd 0 with a
  // deadline (a `WaitLatch`-style "data or timer, whichever first"); without
  // it the only honest answers would be "spin" or "block forever".
  // Blocks in Atomics.wait, so only an agent allowed to block may call it.
  waitReadable(timeoutMs) {
    const deadline = Date.now() + Math.max(0, timeoutMs);
    for (;;) {
      // Load SEQ FIRST: any state change after this point bumps it, so the
      // Atomics.wait below cannot sleep through a write that lands in the
      // window between the check and the wait (it returns 'not-equal').
      const seq = Atomics.load(this.hdr, H_SEQ);
      if (this.available() > 0 || this.closed) return true;
      const rem = deadline - Date.now();
      if (rem <= 0) return false;
      Atomics.wait(this.hdr, H_SEQ, seq, rem);
    }
  }

  // Producer side, BOUNDED blocking readiness wait — waitReadable's mirror,
  // and what the host's `poll_oneoff` lands on for an FD_WRITE subscription
  // (pqcomm_hostpipes::secure_write polls POLLOUT with a 100ms interrupt
  // bound before every write). Returns true once at least one byte fits or
  // the pipe is closed (a closed pipe is "ready" so the caller's write can
  // surface EPIPE instead of parking), false when `timeoutMs` elapsed first.
  // Same SEQ-first discipline as waitReadable: a state change in the window
  // between the check and the wait makes Atomics.wait return 'not-equal'.
  waitWritable(timeoutMs) {
    const deadline = Date.now() + Math.max(0, timeoutMs);
    for (;;) {
      const seq = Atomics.load(this.hdr, H_SEQ);
      if (this.room() > 0 || this.closed) return true;
      const rem = deadline - Date.now();
      if (rem <= 0) return false;
      Atomics.wait(this.hdr, H_SEQ, seq, rem);
    }
  }

  // Consumer side, NON-blocking: -1 = would block, 0 = EOF, >0 = byte count.
  readIntoNow(u8, maxLen) {
    const want = Math.min(maxLen, u8.length);
    if (want <= 0) return 0;
    const r = Atomics.load(this.hdr, H_READ);
    const avail = (Atomics.load(this.hdr, H_WRITE) - r) | 0;
    if (avail > 0) return this._drain(u8, want, r, avail);
    return this.closed ? 0 : -1;
  }

  _drain(u8, want, r, avail) {
    const n = Math.min(want, avail);
    const start = r & this.mask;
    const first = Math.min(n, this.capacity - start);
    u8.set(this.buf.subarray(start, start + first), 0);
    if (n > first) u8.set(this.buf.subarray(0, n - first), first);
    Atomics.add(this.hdr, H_READ, n);
    this._bump();
    return n;
  }

  // Consumer side for an agent that MUST NOT block (the Node/browser main
  // thread driving the pipes). Resolves with the byte count (0 = EOF).
  // Atomics.waitAsync where available; a 1ms poll otherwise.
  async readAsync(u8, maxLen) {
    for (;;) {
      const n = this.readIntoNow(u8, maxLen);
      if (n >= 0) return n;
      const seq = Atomics.load(this.hdr, H_SEQ);
      if (typeof Atomics.waitAsync === 'function') {
        const res = Atomics.waitAsync(this.hdr, H_SEQ, seq, 50);
        if (res.async) await res.value;
      } else {
        await new Promise((r) => setTimeout(r, 1));
      }
    }
  }

  // Convenience: one chunk out of the pipe, or null at EOF.
  async readChunk(maxLen = 65536) {
    const u8 = new Uint8Array(maxLen);
    const n = await this.readAsync(u8, maxLen);
    return n === 0 ? null : u8.subarray(0, n);
  }
}

export default SabPipe;
