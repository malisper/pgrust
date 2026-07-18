#!/usr/bin/env python3
"""pgwire-over-stdio host driver (wasm/p5-net-seam).

Spawns a postgres --stdio-wire process (native binary, or postgres.wasm under
wasmtime), pumps frontend pgwire bytes into its stdin and reads backend pgwire
bytes from its stdout — the host side of the §2.4 stdio transport provider.

Protocol exercised: StartupMessage(3.0) -> AuthenticationOk -> ParameterStatus*
-> BackendKeyData -> ReadyForQuery, then simple-query cycles (Q), then
Terminate (X) and a clean-exit check.

Output: a canonical transcript on stdout, one line per backend message, with
the volatile bytes redacted (BackendKeyData carries the pid + a random cancel
key) so native and wasm transcripts diff clean. Exit 0 = every expectation
met; the transcript still prints on failure for diagnosis.

Usage: pgwire_stdio_driver.py --sql FILE -- <server argv...>
  FILE holds one SQL statement per line ('--' comment lines skipped).
"""

import argparse
import struct
import subprocess
import sys

# ---------- wire helpers ----------


def startup_message(params: dict) -> bytes:
    body = struct.pack("!i", 196608)  # protocol 3.0
    for k, v in params.items():
        body += k.encode() + b"\0" + v.encode() + b"\0"
    body += b"\0"
    return struct.pack("!i", len(body) + 4) + body


def query_message(sql: str) -> bytes:
    body = sql.encode() + b"\0"
    return b"Q" + struct.pack("!i", len(body) + 4) + body


TERMINATE = b"X" + struct.pack("!i", 4)


class Wire:
    def __init__(self, proc):
        self.proc = proc

    def read_exact(self, n: int) -> bytes:
        buf = b""
        while len(buf) < n:
            chunk = self.proc.stdout.read(n - len(buf))
            if not chunk:
                raise EOFError(f"server EOF after {len(buf)}/{n} bytes")
            buf += chunk
        return buf

    def read_message(self):
        t = self.read_exact(1)
        (length,) = struct.unpack("!i", self.read_exact(4))
        body = self.read_exact(length - 4)
        return t.decode(), body

    def send(self, b: bytes):
        self.proc.stdin.write(b)
        self.proc.stdin.flush()


# ---------- canonicalization ----------


def cstr(body: bytes, off: int):
    end = body.index(b"\0", off)
    return body[off:end].decode(errors="replace"), end + 1


def canon(t: str, body: bytes) -> str:
    if t == "R":
        (code,) = struct.unpack("!i", body[:4])
        return f"R auth={code}"
    if t == "S":
        name, off = cstr(body, 0)
        val, _ = cstr(body, off)
        return f"S {name}={val}"
    if t == "K":
        return "K <pid+cancelkey redacted>"
    if t == "Z":
        return f"Z {body.decode()}"
    if t == "T":
        (ncols,) = struct.unpack("!h", body[:2])
        names = []
        off = 2
        for _ in range(ncols):
            name, off = cstr(body, off)
            off += 18  # tableoid(4) attnum(2) typoid(4) typlen(2) atttypmod(4) format(2)
            names.append(name)
        return f"T cols={','.join(names)}"
    if t == "D":
        (ncols,) = struct.unpack("!h", body[:2])
        vals = []
        off = 2
        for _ in range(ncols):
            (vlen,) = struct.unpack("!i", body[off : off + 4])
            off += 4
            if vlen == -1:
                vals.append("NULL")
            else:
                vals.append(body[off : off + vlen].decode(errors="replace"))
                off += vlen
        return f"D {('|'.join(vals))}"
    if t == "C":
        tag, _ = cstr(body, 0)
        return f"C {tag}"
    if t in ("E", "N"):
        fields = {}
        off = 0
        while off < len(body) and body[off : off + 1] != b"\0":
            code = body[off : off + 1].decode()
            val, off = cstr(body, off + 1)
            fields[code] = val
        sev = fields.get("S", "?")
        msg = fields.get("M", "?")
        return f"{t} {sev}: {msg}"
    return f"{t} <{len(body)} bytes>"


# ---------- driver ----------


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sql", required=True)
    ap.add_argument("--user", default="postgres")
    ap.add_argument("--database", default="postgres")
    ap.add_argument("--stderr", default=None, help="file for server stderr")
    ap.add_argument("server_argv", nargs=argparse.REMAINDER)
    args = ap.parse_args()

    argv = args.server_argv
    if argv and argv[0] == "--":
        argv = argv[1:]
    if not argv:
        print("no server argv", file=sys.stderr)
        return 2

    stmts = []
    with open(args.sql) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("--"):
                stmts.append(line)

    errlog = open(args.stderr, "wb") if args.stderr else sys.stderr.buffer
    proc = subprocess.Popen(
        argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errlog
    )
    w = Wire(proc)
    transcript = []
    failures = []

    def note(line):
        transcript.append(line)
        print(line)

    try:
        w.send(startup_message({"user": args.user, "database": args.database}))

        # Handshake: R(0) ... S* ... K ... Z(I)
        saw_auth_ok = saw_key = False
        while True:
            t, body = w.read_message()
            note(canon(t, body))
            if t == "R":
                (code,) = struct.unpack("!i", body[:4])
                saw_auth_ok = code == 0
            elif t == "K":
                saw_key = True
            elif t == "E":
                failures.append("error during handshake")
                break
            elif t == "Z":
                break
        if not saw_auth_ok:
            failures.append("no AuthenticationOk in handshake")
        if not saw_key:
            failures.append("no BackendKeyData in handshake")

        # Simple-query cycles.
        for sql in stmts:
            note(f">>> Q {sql}")
            w.send(query_message(sql))
            while True:
                t, body = w.read_message()
                note(canon(t, body))
                if t == "Z":
                    break

        note(">>> X")
        w.send(TERMINATE)
        leftover = proc.stdout.read()
        if leftover:
            failures.append(f"{len(leftover)} unexpected bytes after Terminate")
        rc = proc.wait(timeout=120)
        note(f"=== exit {rc}")
        if rc != 0:
            failures.append(f"server exit code {rc}")
    except Exception as e:  # EOFError, BrokenPipeError, timeout
        failures.append(f"driver exception: {e}")
        proc.kill()
        proc.wait()

    if failures:
        for f_ in failures:
            print(f"DRIVER-FAIL: {f_}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
