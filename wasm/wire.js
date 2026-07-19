// wire.js — pgwire (PostgreSQL frontend/backend protocol 3.0) frame codec for
// the browser/Node host side of the `postgres --stdio-wire` transport.
//
// This is the JS port of the host half of wasm/pgwire_stdio_driver.py (the
// wasmtime-proven wire driver): frontend message encoders, an incremental
// backend-message reader, a structured per-message parser for the UI, and the
// EXACT canonicalization the python driver prints — the wire e2e byte-compares
// canonical transcripts between the native --stdio-wire session (python
// driver) and the wasm-under-JS session (this codec), so canonMessage() must
// stay in lockstep with pgwire_stdio_driver.py canon().
//
// Environment-agnostic: no DOM, no Node APIs — worker.js, run-node-wire.mjs,
// and the e2e harnesses all import it.

const enc = new TextEncoder();
const dec = new TextDecoder('utf-8', { fatal: false });

// ---- frontend message encoders ----------------------------------------------

export function encodeStartup(params) {
  const parts = [];
  for (const [k, v] of Object.entries(params)) {
    parts.push(enc.encode(k), new Uint8Array([0]), enc.encode(v), new Uint8Array([0]));
  }
  parts.push(new Uint8Array([0]));
  let bodyLen = 4; // protocol version
  for (const p of parts) bodyLen += p.length;
  const buf = new Uint8Array(4 + bodyLen);
  const view = new DataView(buf.buffer);
  view.setInt32(0, 4 + bodyLen, false);   // length includes itself
  view.setInt32(4, 196608, false);        // protocol 3.0
  let off = 8;
  for (const p of parts) { buf.set(p, off); off += p.length; }
  return buf;
}

export function encodeQuery(sql) {
  const body = enc.encode(sql);
  const buf = new Uint8Array(1 + 4 + body.length + 1);
  buf[0] = 0x51; // 'Q'
  new DataView(buf.buffer).setInt32(1, 4 + body.length + 1, false);
  buf.set(body, 5);
  buf[5 + body.length] = 0;
  return buf;
}

export const TERMINATE = (() => {
  const buf = new Uint8Array(5);
  buf[0] = 0x58; // 'X'
  new DataView(buf.buffer).setInt32(1, 4, false);
  return buf;
})();

// ---- incremental backend-message reader -------------------------------------
// Feed raw stdout bytes as they arrive; next() yields complete { t, body }
// messages (t = one-char type, body = Uint8Array without the length header).

export class WireReader {
  constructor() {
    this.chunks = [];
    this.len = 0;
  }

  feed(bytes) {
    if (bytes && bytes.length) { this.chunks.push(bytes); this.len += bytes.length; }
  }

  _compact() {
    if (this.chunks.length <= 1) return;
    const all = new Uint8Array(this.len);
    let off = 0;
    for (const c of this.chunks) { all.set(c, off); off += c.length; }
    this.chunks = [all];
  }

  get buffered() { return this.len; }

  next() {
    if (this.len < 5) return null;
    this._compact();
    const buf = this.chunks[0] || new Uint8Array(0);
    const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
    const length = view.getInt32(1, false);
    if (this.len < 1 + length) return null;
    const t = String.fromCharCode(buf[0]);
    const body = buf.slice(5, 1 + length);
    const rest = buf.subarray(1 + length);
    this.chunks = rest.length ? [rest] : [];
    this.len = rest.length;
    return { t, body };
  }
}

// ---- structured parser (what the UI consumes) -------------------------------

function cstr(body, off) {
  let end = off;
  while (end < body.length && body[end] !== 0) end++;
  return [dec.decode(body.subarray(off, end)), end + 1];
}

// Returns a tagged object per message type; unknown types pass through raw.
export function parseMessage(t, body) {
  const view = new DataView(body.buffer, body.byteOffset, body.byteLength);
  switch (t) {
    case 'R': return { t, code: view.getInt32(0, false) };
    case 'S': {
      const [name, off] = cstr(body, 0);
      const [value] = cstr(body, off);
      return { t, name, value };
    }
    case 'K': return { t };
    case 'Z': return { t, status: dec.decode(body) };
    case 'T': {
      const ncols = view.getInt16(0, false);
      const columns = [];
      let off = 2;
      for (let i = 0; i < ncols; i++) {
        const [name, o2] = cstr(body, off);
        // tableoid(4) attnum(2) typoid(4) typlen(2) atttypmod(4) format(2)
        const typeid = new DataView(body.buffer, body.byteOffset + o2 + 6, 4).getInt32(0, false);
        columns.push({ name, typeid });
        off = o2 + 18;
      }
      return { t, columns };
    }
    case 'D': {
      const ncols = view.getInt16(0, false);
      const values = [];
      let off = 2;
      for (let i = 0; i < ncols; i++) {
        const vlen = view.getInt32(off, false);
        off += 4;
        if (vlen === -1) { values.push(null); continue; }
        values.push(dec.decode(body.subarray(off, off + vlen)));
        off += vlen;
      }
      return { t, values };
    }
    case 'C': {
      const [tag] = cstr(body, 0);
      return { t, tag };
    }
    case 'E': case 'N': {
      const fields = {};
      let off = 0;
      while (off < body.length && body[off] !== 0) {
        const code = String.fromCharCode(body[off]);
        const [val, o2] = cstr(body, off + 1);
        fields[code] = val;
        off = o2;
      }
      return { t, fields, severity: fields.S || '?', message: fields.M || '?' };
    }
    default: return { t, raw: body };
  }
}

// ---- canonicalization (MUST match pgwire_stdio_driver.py canon()) -----------

export function canonMessage(t, body) {
  const m = parseMessage(t, body);
  switch (t) {
    case 'R': return `R auth=${m.code}`;
    case 'S': return `S ${m.name}=${m.value}`;
    case 'K': return 'K <pid+cancelkey redacted>';
    case 'Z': return `Z ${m.status}`;
    case 'T': return `T cols=${m.columns.map((c) => c.name).join(',')}`;
    case 'D': return `D ${m.values.map((v) => (v === null ? 'NULL' : v)).join('|')}`;
    case 'C': return `C ${m.tag}`;
    case 'E': case 'N': return `${t} ${m.severity}: ${m.message}`;
    default: return `${t} <${body.length} bytes>`;
  }
}
