# Audit: backend-tcop-backend-startup

**Verdict: PASS** — every function MATCH; zero seam findings; design-conformant.
Date: 2026-06-12. Model: Opus 4.8 (1M context, `claude-opus-4-8[1m]`).

Unit: `backend-tcop-backend-startup` (`src/backend/tcop/backend_startup.c`, PG 18.3, 1115 lines)
Crate: `crates/backend-tcop-backend-startup`
Compared against: C ground truth (`../pgrust/postgres-18.3/src/backend/tcop/backend_startup.c`)
and the c2rust rendering (`../pgrust/c2rust-runs/backend-tcop-backend-startup/src/backend_startup.rs`).

Independent `/audit-crate` pass (re-derived from the C and headers, not from the
port's self-review). Findings were fixed in-branch and the affected functions
re-audited from scratch; see the Findings section.

## Independent re-audit (2026-06-12)

Re-derived all 11 functions from the C and headers without trusting the port's
comments. Confirmed: cancel-key length handling (`pktlen < 8` → COMMERROR,
`len = pktlen-8`, `0 < len <= 256`, `SendCancelRequest(be32(backendPID),
&cancelAuthCode[..len])`) matches c2rust lines 2577-2668; the SSL/GSS
negotiation recursion, EINTR-retry write loop with `%m`/saved-errno threading,
and man-in-the-middle FATAL all match; the v3 name/value scan, terminator
`offset != len-1` FATAL, NAMEDATALEN truncation, and walsender db-clear match;
the `compat_options[8]`/`options[5]` tables transcribe value-for-value against
`backend_startup.h`; `ssl_negotiation_disabled` is `!LoadedSSL || AF_UNIX` (no
`ssl_in_use`), and `ProcessStartupPacket` adds `|| ssl_in_use` separately while
`ProcessSSLStartup` does not — matching the two distinct C predicates (lines 433
vs 585). Seam crate `backend-tcop-backend-startup-seams` owns exactly
`backend_main` + `set_conn_timing_child`, both installed by a `set()`-only
`init_seams()` wired into `seams-init` (line 71); neighbor seam crates
(`common-string-seams`, `backend-utils-adt-scalar-seams`) correctly not
installed here. `cargo check` + `cargo test` (15 passed) clean. Verdict stands:
**PASS**.

## Function inventory and verdicts

Every top-level and static function defined in `backend_startup.c` (11 total):

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `BackendMain` (76) | `backend_main` (lib.rs:134) | MATCH | Reads `BackendStartupData`, creates the backend TopMemoryContext, `BackendInitialize`, `InitProcess` (proc seam), reads `MyProcPort` names, `PostgresMain` hand-off (postgres seam). `#ifdef EXEC_BACKEND` SSL reinit not ported (fork target). Also seeds `conn_timing.socket_create/.fork_start` from the payload — that assignment is `launch_backend.c`'s in C; harmless duplication, also exposed properly via the `set_conn_timing_child` inward seam. |
| `BackendInitialize` (141) | `backend_initialize`/`_inner` (lib.rs:192) | MATCH | Full body verified line-for-line: ReserveExternalFD, PreAuthDelay sleep (`* 1000000L`), ClientAuthInProgress, pq_init, whereToSendOutput=DestRemote, empty remote_host/port, SIGTERM arm, InitializeTimeouts, StartupBlockSig mask, pg_getnameinfo_all + WARNING, remote save, LOG_CONNECTION_RECEIPT branch (port-present vs not), reverse-lookup numeric guard (`strspn` over both charsets `< len`), RegisterTimeout + enable_timeout_after (`* 1000`), SSL+startup-packet, CAC switch, disable_timeout, BlockSig mask, check_on_shmem_exit_lists_are_empty, proc_exit(0) on non-OK, ps title. USE_INJECTION_POINTS block not ported (compiled out). FATAL path returned as `Err`, driven through the report/`proc_exit(1)` cycle by the wrapper. |
| `ProcessSSLStartup` (401) | `process_ssl_startup` (lib.rs:452) | MATCH | startmsgread/peekbyte/endmsgread; EOF→ERROR; `!= 0x16`→OK; USE_SSL arms (secure_open_server, ALPN COMMERROR, Trace accept) gated on `ssl_supported()`; non-SSL build and all failures take the `reject:` label. |
| `ProcessStartupPacket` (492) | `process_startup_packet` (lib.rs:523) | MATCH | 1-then-3 length read with the partial-packet COMMERROR (only when `!ssl_done && !gss_done`), `len -= 4`, bounds `< sizeof(ProtocolVersion) || > MAX`, palloc-equivalent via pq_getbytes, CANCEL/SSL/GSS dispatch (true recursion, EINTR retry loop, man-in-the-middle buffered-data FATAL), `FrontendProtocol = Min(proto, LATEST)`, major-range FATAL, v3 name/value scan (database/user/options/replication-hybrid/`_pq_.`/generic+application_name), terminator `offset != len-1` FATAL, NegotiateProtocolVersion trigger, user-name FATAL, db defaults to user, NAMEDATALEN truncation, MyBackendType, non-db walsender db-clear. Buffer is exactly `len` bytes; the C trailing-NUL is the slice boundary — `cstr_len`/scan never read past `len`, behavior identical. |
| `ProcessCancelRequestPacket` (875) | `process_cancel_request_packet` (lib.rs:902) | MATCH | `pktlen < 8`→COMMERROR return; `len = pktlen-8`; `len == 0 || len > 256`→COMMERROR return; big-endian backendPID at offset 4; cancelAuthCode = `pkt[8..8+len]`; `SendCancelRequest` (direct dep, slice carries length). |
| `SendNegotiateProtocolVersion` (917) | `send_negotiate_protocol_version` (lib.rs:938) | MATCH | Built in-crate via direct `backend-libpq-pqformat`: beginmessage('v'), sendint32(FrontendProtocol), sendint32(count), per-option sendstring (sends bytes + NUL), endmessage. |
| `process_startup_packet_die` (947) | `process_startup_packet_die` (lib.rs:968) | MATCH | `_exit(1)` → `std::process::exit(1)`. |
| `StartupPacketTimeoutHandler` (957) | `startup_packet_timeout_handler` (lib.rs:974) | MATCH | `_exit(1)`. |
| `validate_log_connections_options` (976) | same name (lib.rs:993) | MATCH | compat_options[8] + options[5] tables transcribed value-for-value (verified against `backend_startup.h`); first-token compat-must-be-alone rule; `goto next` modeled with labeled `continue`; `Err(detail)` carries the verbatim `GUC_check_errdetail` text. Unit-tested. |
| `check_log_connections` (1068) | `check_log_connections` (lib.rs:1068) | MATCH | SplitIdentifierString via varlena seam (`None`→invalid-syntax detail); validation in the helper; returns `Ok(Ok(flags))`/`Ok(Err(detail))`/`Err(oom)`. The C `guc_malloc(*extra)` store is the GUC machinery's job; the flags are returned for it. |
| `assign_log_connections` (1112) | `assign_log_connections` (lib.rs:1090) | MATCH | `log_connections = *((int *) extra)`. |

Globals owned by this unit (backend_startup.c:46-58), all per-backend → `thread_local!` in `globals.rs`:
- `Trace_connection_negotiation` (46), `log_connections` (47), `conn_timing` (58, `ready_for_use = TIMESTAMP_MINUS_INFINITY = i64::MIN + 1`, others 0).
- `log_connections_string` (48) — the raw GUC string the GUC machinery owns; only fed back through `check_log_connections`. No in-crate consumer; correctly not materialized.

## Constants / parity (verified against headers, not memory)

- `pqcomm.h`: PG_PROTOCOL(m,n)=`(m<<16)|n`, EARLIEST=3.0, LATEST=3.2, MAX_STARTUP_PACKET_LENGTH=10000, CANCEL=1234.5678, SSL=1234.5679, GSS=1234.5680. MsgType=ProtocolVersion (4 bytes) ⇒ offsetof(cancelAuthCode)=8.
- `c.h`: STATUS_OK=0, STATUS_ERROR=-1. `pg_config_manual.h`: NAMEDATALEN=64. `storage/proc.h`: PGPROC_MAX_CACHED_SUBXIDS=64.
- `backend_startup.h`: LOG_CONNECTION_RECEIPT/AUTH/AUTHZ/SETUP = 1/2/4/8, ON=7, ALL=15. `protocol.h`: PqMsg_NegotiateProtocolVersion='v'.
- Cancel-key bounds `0 < len <= 256` match c2rust line 2621. All SQLSTATEs/errmsg/errdetail/errhint strings reproduced verbatim.

## Seam / wiring audit

Owned seam crate (by C-source coverage, `backend_startup.c` ⇒ `backend-tcop-backend-startup-seams`): both declarations — `backend_main`, `set_conn_timing_child` — are installed by `init_seams()`, which is `set()`-only, and `seams-init::init_all()` calls it (seams-init/src/lib.rs:69). No uninstalled owned seam.

`common-string-seams` (`common/string.c`) and `backend-utils-adt-scalar-seams` (`bool.c`) were created by this port but are owned by unported neighbors; per AGENTS.md they are correctly NOT installed here and panic until their owners land. Extended shared seam crates (pqcomm/be-secure/timeout/ps-status/postmaster/proc/postgres/walsender/xlog/xlogrecovery/ipc/init-small/init-miscinit/misc-more) belong to their respective units. Every outward seam call inspected is a thin marshal+delegate; no branching/computation lives in a seam path.

## Findings (fixed in-branch, then re-audited)

1. `write_negotiation_byte` (the `secure_write != 1` loop, both SSL and GSS):
   the COMMERROR message dropped the C format's `%m`, and the failing write's
   errno was never threaded into the report — so the SQLSTATE from
   `errcode_for_socket_access()` came off the stale ambient errno and the errno
   text was absent. **DIVERGES.** Fixed: capture `e` from
   `SockError::Errno(e)`, `.with_saved_errno(e)` before
   `errcode_for_socket_access()`/`errmsg`, and restore the `%m` in the message
   (`"failed to send %s negotiation response: %m"`). The error infra expands
   `%m` only when `saved_errno` is `Some`, and `errcode_for_socket_access`
   reads `saved_errno` first, so both the SQLSTATE and the message now match C.
   Re-audited: MATCH.

2. `gai_strerror` helper (used by the `pg_getnameinfo_all() failed: %s` WARNING):
   the stub returned `"EAI error {code}"` instead of the libc text. **DIVERGES**
   (message content). Fixed to call `libc::gai_strerror` and render the real
   string, matching C. Re-audited: MATCH.

## Design conformance

- ERROR/FATAL C ereports → `Err(PgError)`; BackendInitialize's wrapper drives the FATAL report cycle then `proc_exit(1)`. COMMERROR/LOG/WARNING use `.finish(loc)` (emit + continue, returns Ok below ERROR).
- Allocating paths take `Mcx` (`pq_getbytes` → `PgVec<'mcx>`, `pg_clean_ascii` seam → `PgString`, negotiate buffer in the threaded `Mcx`); no ambient current-context.
- Per-backend C globals are `thread_local!`, never shared statics. `MyProcPort` reached through the `with_my_proc_port` callback (no `&'static mut`); `MyClientSocket` read via getter and passed explicitly.
- No locks/pins held across `?`; no resource registry; no unledgered divergence markers.

## Deliberate non-ports (compiled out on this target, not logic gaps)

- `BackendMain` `#ifdef EXEC_BACKEND` SSL reinit — not this repo's model.
- `BackendInitialize` `#ifdef USE_INJECTION_POINTS` hooks — compiled out by default.
- `#ifdef USE_SSL`/`#ifdef ENABLE_GSS` bodies — gated on `ssl_supported()`/`gss_supported()` seams (false until the TLS/GSS units land); the control flow around them is fully present.

## Gate

- `cargo check -p backend-tcop-backend-startup -p backend-tcop-backend-startup-seams -p common-string-seams -p backend-utils-adt-scalar-seams` — clean.
- `cargo test -p backend-tcop-backend-startup` — 15 passed.

## Verdict

**PASS** — every function MATCH after the two `%m`/errno findings were fixed
and re-audited; zero outstanding seam findings.
