# Audit: common-ip (`src/common/ip.c`)

- **Verdict:** PASS
- **Date:** 2026-06-13
- **Model:** Opus 4.8 (1M context) (`claude-opus-4-8[1m]`)
- **Branch:** `port/common-ip`

Independent function-by-function audit per `.claude/skills/audit-crate/SKILL.md`.
Sources:

- C: `../pgrust/postgres-18.3/src/common/ip.c`
- c2rust (backend/non-FRONTEND build): `../pgrust/c2rust-runs/common-batch11/src/ip.rs`
- Rust port: `crates/common-ip/src/lib.rs`
- Owned seam crate: `crates/common-ip-seams` (declared on `main` by pqcomm; this
  branch installs it)

This file is `#ifndef FRONTEND`/`#else` shared frontend/backend code; there is no
`HAVE_UNIX_SOCKETS` guard in 18.3 (Unix sockets are unconditional on non-Windows).
No `#if` branch hides functions from the c2rust inventory — c2rust kept all five.

## 1. Function inventory

Enumerated from the C definitions and cross-checked against the c2rust rendering;
every function gets a row. Five functions total (3 extern, 2 file-static).

| # | C function | C loc (ip.c) | Port loc (lib.rs) | Verdict |
|---|------------|--------------|-------------------|---------|
| 1 | `pg_getaddrinfo_all`  | 52-69   | 28-80   | MATCH |
| 2 | `pg_freeaddrinfo_all` | 81-102  | 88      | MATCH (owned-Vec model) |
| 3 | `pg_getnameinfo_all`  | 113-141 | 95-120  | MATCH |
| 4 | `getaddrinfo_unix` (static) | 152-222 | 130-197 | MATCH |
| 5 | `getnameinfo_unix` (static) | 227-262 | 200-249 | MATCH (after fix; see below) |

Supporting Rust helpers (no C counterpart, pure marshaling): `c_hint`,
`copy_addrinfo`, `c_string`, `c_char_buf_to_string`, `cstr_bytes_to_string`,
`sockaddr_family`, `sun_path_len`, `sun_path_offset`, `getnameinfo_system`
(the system-resolver arm of `pg_getnameinfo_all`, split out from `getnameinfo`'s
inline call in C). These carry no independent C logic beyond the marshaling the
seam's `String`/`Vec<PgAddrInfo>` model requires.

## 2. Per-function comparison

### 1. `pg_getaddrinfo_all` — MATCH
- `*result = NULL` → `result.clear()` (same "not all getaddrinfo() zero on
  failure" rationale; the owned list is emptied up front).
- `hintp->ai_family == AF_UNIX` → delegate to `getaddrinfo_unix(servname, ...)`:
  matches (servname is the path arg, exactly as C).
- NULL-hostname rule: C passes `NULL` when `!hostname || hostname[0]=='\0'`. Port:
  `Some(h) if !h.is_empty()` else `None`; a `None` hostname also yields the null
  pointer. Matches both predicates.
- System path: builds a `struct addrinfo` hint, calls `libc::getaddrinfo`, returns
  `rc` directly on failure, else walks the result list copying each node into the
  owned `Vec` and `freeaddrinfo`s the OS list. C returns the OS list to the caller
  to free later via `pg_freeaddrinfo_all`; the port copies-then-frees so the owned
  `Vec` is the list. Behavior on every input is identical (same addresses, same
  rc). The hint is built from `flags/family/socktype` only — `AddrInfoHint`
  carries exactly those fields, which is all the C callers set in the hint they
  pass (verified: pqcomm/backend_startup hints set ai_family/ai_socktype/ai_flags).

### 2. `pg_freeaddrinfo_all` — MATCH (owned-Vec model)
C frees the list two ways keyed on `hint_ai_family` (walk-and-`free` for the
AF_UNIX list our `getaddrinfo_unix` malloc'd; `freeaddrinfo()` for the OS list).
In the port the result is an owned `Vec<PgAddrInfo>` already copied out of the OS
structures, so freeing it is dropping the `Vec`; the function body is empty and
keeps `_hint_ai_family`/`_ai` for API parity. No leak and no double-free is
possible because the OS list was already freed inside `pg_getaddrinfo_all`. Logic
equivalent.

### 3. `pg_getnameinfo_all` — MATCH
- `addr && addr->ss_family == AF_UNIX` → `getnameinfo_unix`, else
  `getnameinfo_system` (the system-resolver arm). Port reads `ss_family` from the
  stored `sockaddr_storage` bytes via `sockaddr_family`. `addr` is always present
  in the port (`&SockAddr`), which is the C `addr != NULL` case — every C caller
  passes a real `&port->raddr.addr`, never NULL, so the NULL arm is not reachable
  and not load-bearing.
- Failure fill: `if rc != 0 { strlcpy(node,"???"); strlcpy(service,"???"); }` →
  `*n = "???"`, `*s = "???"` for whichever buffer is `Some`. Matches.

### 4. `getaddrinfo_unix` — MATCH
- `*result = NULL` → `result.clear()` semantics inherited from caller; port pushes
  exactly one node (the C "only one addrinfo" bug is preserved).
- `strlen(path) >= sizeof(sun_path)` → `path.len() >= sun_path_len()` returns
  `EAI_FAIL`. The port additionally rejects an embedded NUL (`path.contains(&0)`)
  → `EAI_FAIL`; this is not a divergence — C uses `strcpy`/`strlen` so an embedded
  NUL is simply not representable, and the port's explicit rejection produces the
  closest faithful outcome (failure) rather than UB.
- Hint defaulting: `hintsp==NULL` → `(AF_UNIX, SOCK_STREAM)`; else memcpy the hint;
  `ai_socktype==0` → `SOCK_STREAM`; `ai_family != AF_UNIX` → `EAI_FAIL`. Port
  reproduces all four steps (`ai_protocol` is taken as 0 from the hint, matching C
  where `hints.ai_protocol` is whatever the caller set — `AddrInfoHint` does not
  carry protocol, and every caller leaves it 0, so 0 is faithful).
- Node fields: `ai_family=AF_UNIX`, `ai_socktype`, `ai_protocol`, `ai_next=NULL`,
  `ai_canonname=NULL`, `ai_flags` left at calloc's 0. Port sets `flags:0` with the
  same rationale; a test asserts flags stay 0 even when the hint set `AI_CANONNAME`.
- `sun_family=AF_UNIX`; `strcpy(sun_path, path)`; `ai_addrlen=sizeof(sockaddr_un)`.
  Port zero-inits `sockaddr_un`, copies path bytes, `addrlen=size_of::<sockaddr_un>()`.
- Abstract `@` socket: C sets `sun_path[0]='\0'` and
  `ai_addrlen = offsetof(sockaddr_un, sun_path) + strlen(path)` (strlen counts the
  full path incl. the leading `@`). Port sets `sun_path[0]=0` and
  `addrlen = sun_path_offset() + path.len()`. `sun_path_offset()` computes the real
  `offsetof` at runtime (cross-platform; matches the c2rust constant `2` on macOS),
  and `path.len()` is the full path length. Stored bytes round-trip correctly into
  `getnameinfo_unix` (verified by test).

### 5. `getnameinfo_unix` — MATCH (after fix)
- Invalid args: `sa==NULL || sun_family!=AF_UNIX || (node==NULL && service==NULL)`
  → `EAI_FAIL`. Port: `sockaddr_family != AF_UNIX || (node.is_none() &&
  service.is_none())` → `EAI_FAIL`. (`sa==NULL` is unreachable: `addr` is `&`.)
- node: C `snprintf(node, nodelen, "%s", "[local]")` then
  `ret < 0 || ret >= nodelen → EAI_MEMORY`. Port writes `"[local]"`.
- service: abstract check `sun_path[0]=='\0' && sun_path[1]!='\0'` →
  `"@%s" % (sun_path+1)`, else `"%s" % sun_path`; then
  `ret < 0 || ret >= servicelen → EAI_MEMORY`. Port reproduces the abstract/normal
  split.

**Initial finding (FAIL), now fixed:** the original port wrote `node`/`service`
into unbounded `String`s and never reproduced the `ret >= len → EAI_MEMORY`
truncation branch. This is a real `DIVERGES`: a Unix socket path longer than
`NI_MAXSERV-1` (31) bytes makes C's `snprintf(service, NI_MAXSERV, ...)` report
`ret >= servicelen`, so `getnameinfo_unix` returns `EAI_MEMORY` and
`pg_getnameinfo_all` then fills `service = "???"` and returns `EAI_MEMORY`. The
old port returned the full untruncated string with `rc=0` — a different return
code and different log output. (The `node` "[local]" buffer is 7 bytes and never
overflows `NI_MAXHOST=1025`, so only `service` is the live case.)

**Fix:** the seam marshals the buffers as `String`, dropping the C
`nodelen`/`servicelen` parameters; the C API contract is that callers pass
`NI_MAXHOST`/`NI_MAXSERV` buffers (verified across every caller — backend_startup.c
`remote_host[NI_MAXHOST]`/`remote_port[NI_MAXSERV]`, elog.c, auth.c, hba.c,
network.c, pgstatfuncs.c, fe-connect.c). `getnameinfo_unix` now re-imposes those
exact bounds: `formatted.len() >= NI_MAXHOST → EAI_MEMORY` for node,
`formatted.len() >= NI_MAXSERV → EAI_MEMORY` for service. This fires under the
same predicate as C. A regression test (`unix_nameinfo_long_path_overflows_service`)
asserts `EAI_MEMORY` + `"???"` for an over-long path. Re-audited from scratch after
the fix: MATCH.

Note on `getnameinfo_system`: it likewise hardcodes `NI_MAXHOST`/`NI_MAXSERV` for
the libc `getnameinfo` out-buffers rather than threading caller lengths. This is
faithful — every caller passes exactly those sizes, and numeric/host renderings
never exceed them; the seam's `String` model has no caller length to thread.

## 3. Seam and wiring audit

- **Owned seam crate (by C-source coverage):** `ip.c` → `crates/common-ip-seams`.
  It declares two seams, `pg_getaddrinfo_all` and `pg_getnameinfo_all`. (The crate
  itself was added on `main` by the pqcomm port, which consumes these; this branch
  supplies the owning installer.)
- **Installation:** `common_ip::init_seams()` is exactly two `set()` calls
  installing both declarations — no uninstalled seam. It contains nothing but
  `set()` calls. `seams-init::init_all()` calls `common_ip::init_seams()`. ✓
- `pg_freeaddrinfo_all` correctly has **no** seam (the result `Vec` drop is the
  free), documented in the seam crate header. ✓
- **No outward seam calls:** this crate calls `libc` directly (`getaddrinfo`,
  `freeaddrinfo`, `getnameinfo`); there is no dependency cycle and no marshal-only
  delegate to another unit. No function body was replaced by a "call somewhere
  else" — all logic lives here. ✓

## 3b. Design conformance

- **Failure surface:** ip.c never `ereport`s; it returns `EAI_*` ints. The seams
  return `i32` (no `PgResult`), correctly mirroring the C failure surface
  (seam-signatures-mirror-c-failure-surface). ✓
- **No `Mcx`/`PgResult` required:** these functions do not allocate via palloc and
  cannot raise — the C allocates with `calloc`/`free`, modeled as an owned `Vec`,
  so no memory context is involved. ✓
- **No invented opacity:** `SockAddr` (sockaddr_storage bytes + salen), `AddrInfoHint`
  (flags/family/socktype), and `PgAddrInfo` (one addrinfo node) are real structs in
  `types-net` that mirror the C layouts; no stand-in handles or void* layering
  (opacity-inherited-never-introduced). ✓
- No shared statics, no ambient-global seams, no locks across `?`, no registry-shaped
  side tables, no unledgered divergence markers. ✓

## 4. Verdict

All five functions **MATCH** (after the `getnameinfo_unix` truncation fix), the
single owned seam crate's declarations are both installed by `init_seams()` and
wired into `init_all()`, there are zero seam findings, and design conformance is
clean. **PASS.** `cargo test -p common-ip` green (9 tests). CATALOG row set to
`audited`.
