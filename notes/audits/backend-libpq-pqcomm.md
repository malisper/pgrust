# Audit: backend-libpq-pqcomm

- Unit: `backend-libpq-pqcomm` (`src/backend/libpq/pqcomm.c`, postgres-18.3, 2088 lines)
- Port: `crates/backend-libpq-pqcomm` (`src/lib.rs`, `src/config.rs`), plus new support
  crates `types-net`, `backend-libpq-be-secure-seams`,
  `backend-storage-ipc-waiteventset-seams`, `backend-utils-init-miscinit-seams`,
  `common-ip-seams`, `port-noblock-seams`, `port-pgsleep-seams`.
- References: C source above; c2rust rendering
  `pgrust/c2rust-runs/backend-libpq-pqcomm/src/pqcomm.rs` (macOS build: no
  `WIN32`, no `ENABLE_GSS`, no `TCP_USER_TIMEOUT`, `PG_TCP_KEEPALIVE_IDLE` =
  `TCP_KEEPALIVE`).
- Audit independence: inventory re-derived from the C file; every function read
  in all three forms (C, c2rust, port).

## Function inventory and verdicts

| # | C function (pqcomm.c) | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|
| 1 | `pq_init` (173) | `pq_init` (215) | MATCH | Same order: Port fill, getsockname FATAL, TCP_NODELAY/SO_KEEPALIVE FATALs (skipped for AF_UNIX), keepalive/user-timeout best-effort sets, buffer init, `on_proc_exit(socket_close)`, `pg_set_noblock` FATAL, `fcntl(FD_CLOEXEC)` elog-FATAL (`errmsg_internal`), FeBeWaitSet creation with WRITEABLE/LATCH/PM_DEATH in order, position asserts. WIN32 SO_SNDBUF block correctly absent (outside build config). |
| 2 | `socket_comm_reset` (333) | `socket_comm_reset` (382) | MATCH | Resets only `PqCommBusy`. |
| 3 | `socket_close` (348) | `socket_close` (389) | MATCH | NULL-MyProcPort no-op; `secure_close`; `sock = PGINVALID_SOCKET`. GSS block absent from build (c2rust confirms). |
| 4 | `ListenServerPort` (417) | `ListenServerPort` (421) | MATCH | AF_UNIX path-length check vs `sizeof(sun_path)` with same message; `Lock_AF_UNIX`; getaddrinfo failure both message variants; per-addr loop with skip-unix-unless-asked, MAXLISTEN break, familyDesc/addrDesc, socket/FD_CLOEXEC(FATAL)/SO_REUSEADDR(non-unix)/IPV6_V6ONLY/bind(EADDRINUSE hints, unix vs tcp variant)/Setup_AF_UNIX(break)/listen(`MaxConnections*2`)/log lines; `STATUS_ERROR` iff nothing added. `UNIXSOCK_PATH` format `"%s/.s.PGSQL.%d"` verified against `libpq/pqcomm.h`. |
| 5 | `Lock_AF_UNIX` (684) | `Lock_AF_UNIX` (678) | MATCH | `@`-abstract shortcut; `CreateSocketLockFile` via miscinit seam (FATAL inside → `Err`); `unlink`; path remembered in `SOCK_PATHS`. |
| 6 | `Setup_AF_UNIX` (719) | `Setup_AF_UNIX` (744) | MATCH (after fix) | `@`-abstract shortcut; numeric-gid vs `getgrnam` per `strtoul`+`*endptr=='\0'`; LOG+STATUS_ERROR paths for missing group / chown / chmod, `(uid_t)-1` owner. Fix round 1: `parse_strtoul_full` now clamps to `ULONG_MAX` on overflow (strtoul/ERANGE semantics) and uses the C-locale `isspace` set incl. `\v`. |
| 7 | `AcceptConnection` (793) | `AcceptConnection` (812) | MATCH | accept; on failure LOG (`errcode_for_socket_access`) + `pg_usleep(100000)` + STATUS_ERROR (sock = PGINVALID_SOCKET as accept's -1 would leave it). |
| 8 | `TouchSocketFiles` (829) | `TouchSocketFiles` (851) | MATCH | `utime(path, NULL)`, errors ignored. |
| 9 | `RemoveSocketFiles` (847) | `RemoveSocketFiles` (863) | MATCH | unlink each, errors ignored, list cleared. |
| 10 | `socket_set_nonblocking` (880) | `socket_set_nonblocking` (882) | MATCH | ERROR `08003` "there is no client connection" when MyProcPort NULL; else sets `noblock`. |
| 11 | `pq_recvbuf` (897) | `pq_recvbuf` (901) | MATCH | Left-justify/reset of recv cursors; blocking mode; `errno = 0`; EINTR-continue; COMMERROR (socket errcode) only when errno != 0; EOF on r<=0; length bump on success. |
| 12 | `pq_getbyte` (963) | `pq_getbyte` (965) | MATCH | Assert reading; refill loop; returns unsigned byte, advances. |
| 13 | `pq_peekbyte` (982) | `pq_peekbyte` (985) | MATCH | Same without advance. |
| 14 | `pq_getbyte_if_available` (1003) | `pq_getbyte_if_available` (1005) | MATCH | Buffered fast path; nonblocking; `errno = 0`; EAGAIN/EWOULDBLOCK/EINTR → 0; other errno != 0 → COMMERROR; r==0 → EOF; byte stored only on success (C writes `*c` via secure_read directly; identical observable results since C's failed read does not produce a defined `*c` either). |
| 15 | `pq_getbytes` (1062) | `pq_getbytes` (1060) | MATCH | `len` is the slice length; refill + bounded copy loop. |
| 16 | `pq_discardbytes` (1097) | `pq_discardbytes` (1092) | MATCH | Same loop without the copy. |
| 17 | `pq_buffer_remaining_data` (1127) | `pq_buffer_remaining_data` (1119) | MATCH | Assert + difference. |
| 18 | `pq_startmsgread` (1141) | `pq_startmsgread` (1130) | MATCH | FATAL `08P01` "protocol synchronization was lost" if already reading (c2rust confirms level 22 / 08P01); then sets flag. |
| 19 | `pq_endmsgread` (1165) | `pq_endmsgread` (1143) | MATCH | Assert + clear. |
| 20 | `pq_is_reading_msg` (1181) | `pq_is_reading_msg` (1149) | MATCH | — |
| 21 | `pq_getmessage` (1203) | `pq_getmessage` (1183) | MATCH (after fix) | reset; 4-byte length read (COMMERROR "unexpected EOF within message length word"); `pg_ntoh32`; `len < 4 || len > maxlen` → COMMERROR "invalid message length"; enlarge with stringinfo's `MaxAllocSize` (0x3fffffff) check, message and detail texts matched; PG_CATCH = discard + COMMERROR "incomplete message from client" + clear flag + rethrow; body read EOF → COMMERROR "incomplete message from client"; flag cleared on success. Fix round 1: an `Err` from `pq_discardbytes` inside the catch now propagates immediately (C longjmp out of PG_CATCH) instead of being swallowed. |
| 22 | `internal_putbytes` (1277) | `internal_putbytes` (1254) | MATCH | flush-when-full; large-message direct `internal_flush_buffer(s, &start=0, &len)` path including C's quirk that a would-block partial leaves `len` unchanged (unreachable: blocking mode); bounded buffer copy otherwise. |
| 23 | `socket_flush` (1327) | `socket_flush` (1310) | MATCH | Busy no-op; busy set before nonblocking(false)+flush; busy cleared on both 0/EOF returns; stays set on `Err` (C longjmp), cleared later by `socket_comm_reset`. |
| 24 | `internal_flush` (1349) | `internal_flush` (1329) | MATCH | Delegates over `PqSendBuffer`/`PqSendStart`/`PqSendPointer`; cursors written back on `Err` too. Buffer is moved out for the call; safe because every caller holds `PqCommBusy`, so re-entrant putmessage/flush are suppressed exactly as in C. |
| 25 | `internal_flush_buffer` (1362) | `internal_flush_buffer` (1353) | MATCH | EINTR-continue; EAGAIN/EWOULDBLOCK → 0; duplicate-suppressed COMMERROR via thread-local `LAST_REPORTED_SEND_ERRNO` (C function-static); drop buffer, `ClientConnectionLost = 1`, `InterruptPending = 1` (seam setters), EOF; errno-latch reset after successful send; cursors zeroed on completion. |
| 26 | `socket_flush_if_writable` (1435) | `socket_flush_if_writable` (1403) | MATCH | Quick-exit when empty; busy no-op; nonblocking(true) before busy set (same ordering as C). |
| 27 | `socket_is_send_pending` (1461) | `socket_is_send_pending` (1430) | MATCH | — |
| 28 | `socket_putmessage` (1491) | `socket_putmessage` (1444) | MATCH | Assert msgtype; busy suppress; type byte, `pg_hton32(len+4)`, body; `goto fail` clears busy (Ok path), `Err` leaves it set. |
| 29 | `socket_putmessage_noblock` (1524) | `socket_putmessage_noblock` (1477) | MATCH | `required = PqSendPointer + 1 + 4 + len`; grow-only repalloc; re-dispatch through `pq_putmessage` (PqCommMethods) as the C macro does; Assert res == 0. |
| 30 | `pq_putmessage_v2` (1561) | `pq_putmessage_v2` (1502) | MATCH | Type byte + raw body, no length word; same busy handling. |
| 31 | `pq_setkeepaliveswin32` (1593) | — | N/A | `#if defined(WIN32) && defined(SIO_KEEPALIVE_VALS)` only; outside all supported targets (c2rust confirms absent). |
| 32 | `pq_getkeepalivesidle` (1632) | `pq_getkeepalivesidle` (1656) | MATCH | NULL/AF_UNIX → 0; cached `keepalives_idle`; probe default via `getsockopt(PG_TCP_KEEPALIVE_IDLE)` (TCP_KEEPALIVE on darwin / TCP_KEEPIDLE elsewhere, matching the C `#elif` ladder for the supported targets), LOG + `-1` on failure. |
| 33 | `pq_setkeepalivesidle` (1667) | `pq_setkeepalivesidle` (1680) | MATCH | NULL/AF_UNIX → OK; equal-value shortcut; probe default (`<= 0` guard, unknown-default rules); 0 → default; setsockopt LOG+ERROR; store. |
| 34 | `pq_getkeepalivesinterval` (1717) | `pq_getkeepalivesinterval` (1714) | MATCH | Same shape with `TCP_KEEPINTVL`. |
| 35 | `pq_setkeepalivesinterval` (1752) | `pq_setkeepalivesinterval` (1738) | MATCH | — |
| 36 | `pq_getkeepalivescount` (1801) | `pq_getkeepalivescount` (1772) | MATCH | Same shape with `TCP_KEEPCNT`. |
| 37 | `pq_setkeepalivescount` (1831) | `pq_setkeepalivescount` (1796) | MATCH | — |
| 38 | `pq_gettcpusertimeout` (1876) | `pq_gettcpusertimeout` (1831/1856) | MATCH | Linux cfg = the `#ifdef TCP_USER_TIMEOUT` arm; other targets return 0 (the `#else` arm, matching the c2rust build). |
| 39 | `pq_settcpusertimeout` (1906) | `pq_settcpusertimeout` (1862/1897) | MATCH | Linux arm full; non-linux arm keeps the NULL/AF_UNIX precheck then LOG "setsockopt(TCP_USER_TIMEOUT) not supported" + STATUS_ERROR for nonzero, exactly as C/c2rust. |
| 40 | `assign_tcp_keepalives_idle` (1954) | (1919) | MATCH | `pq_setkeepalivesidle(newval, MyProcPort)`, result ignored. |
| 41 | `show_tcp_keepalives_idle` (1974) | (1926) | MATCH | `%d` of `pq_getkeepalivesidle(MyProcPort)`. |
| 42 | `assign_tcp_keepalives_interval` (1987) | (1935) | MATCH | — |
| 43 | `show_tcp_keepalives_interval` (1997) | (1942) | MATCH | — |
| 44 | `assign_tcp_keepalives_count` (2010) | (1951) | MATCH | — |
| 45 | `show_tcp_keepalives_count` (2020) | (1958) | MATCH | — |
| 46 | `assign_tcp_user_timeout` (2033) | (1967) | MATCH | — |
| 47 | `show_tcp_user_timeout` (2043) | (1974) | MATCH | — |
| 48 | `pq_check_connection` (2056) | `pq_check_connection` (1988) | MATCH | ModifyWaitEvent → WL_SOCKET_CLOSED; zero-timeout WaitEventSetWait over `FeBeWaitSetNEvents` events; CLOSED → false; LATCH_SET → ResetLatch(MyLatch) + retry; else true. |

Data/global items: `PqCommSocketMethods`/`PqCommMethods` vtable ported as
`PQ_COMM_SOCKET_METHODS` + thread-local pointer with `set_pq_comm_methods`
(pqmq swap point); the `pq_flush`/`pq_putmessage`/... dispatch macros from
`libpq/libpq.h` ported as the dispatch functions; `FeBeWaitSet` as a
thread-local handle; file-statics (`PqSendBuffer`/sizes/cursors, `PqRecvBuffer`,
`PqCommBusy`, `PqCommReadingMsg`, `sock_paths`, `last_reported_send_errno`) as
thread-locals per the repo's backend-global convention. GUC storage
(`Unix_socket_permissions` = 0777 boot default per guc_tables.c,
`Unix_socket_group` = "", `tcp_keepalives_*`/`tcp_user_timeout` = 0) lives in
`config.rs` with public setters for the future GUC unit.

Constants verified against headers: `PQ_SEND/RECV_BUFFER_SIZE` 8192;
`FeBeWaitSetSocketPos/LatchPos/NEvents` 0/1/3 (libpq.h); `WL_*` bits
(waiteventset.h, incl. non-Windows `WL_SOCKET_CONNECTED`/`ACCEPT` aliases and
`WL_SOCKET_CLOSED` = 1<<7); `MaxAllocSize` 0x3fffffff; severities LOG=15,
COMMERROR=LOG_SERVER_ONLY=16, FATAL=22 (matches c2rust's rendered levels);
sqlstates 08003, 08P01, 53200, 54000 (errcodes.txt); STATUS_OK/STATUS_ERROR
0/-1, PGINVALID_SOCKET -1 (c.h/port.h); UNIXSOCK_PATH format and
UNIXSOCK_PATH_BUFLEN = sizeof(sun_path) (pqcomm.h).

Accepted repo-convention divergences (not findings): `PgResult` instead of
sigsetjmp for ERROR/FATAL; allocator OOM `errdetail` lacks the memory-context
name (`"Failed on request of size N."`), matching the audited elog-port
precedent; `Assert` → `debug_assert!`; no NLS `_()` wrappers.

## Fix rounds

Round 1 (committed with this audit):

1. `parse_strtoul_full` (`Setup_AF_UNIX` helper): overflow now clamps to
   `ULONG_MAX` like `strtoul` (was: wrapping accumulate), and leading
   whitespace uses the C-locale `isspace` set including `\v` (was:
   `is_ascii_whitespace`, which excludes `\v`). Tests extended.
2. `pq_getmessage`: an error raised by `pq_discardbytes` inside the PG_CATCH
   equivalent now propagates immediately (C: longjmp out of the catch block),
   instead of being swallowed in favor of the original enlarge error.

Both functions re-audited from scratch after the fix: MATCH.

## Seam audit

Inward (owned by this unit, `crates/backend-libpq-pqcomm-seams`):
`pq_putmessage`, `pq_putmessage_v2`, `pq_flush` — all three installed by
`backend_libpq_pqcomm::init_seams()`, which contains only `set()` calls;
`seams-init::init_all()` calls it. Consumer (`backend-utils-error`
`send_message_to_frontend`) call sites updated for the reconciled
`PgResult<i32>` signatures, still result-ignoring as in C. No `set()` of these
seams anywhere else. OK.

Outward (consumed):

| Seam | Owner (unported unit) | Shape |
|---|---|---|
| `with_my_proc_port`, `set_client_connection_lost`, `set_interrupt_pending`, `max_connections` | globals.c (`backend-utils-init-small`) | global read/write marshals only |
| `secure_read`, `secure_write`, `secure_close` | be-secure.c | one call + result passthrough; errno-based protocol preserved |
| `create_wait_event_set`, `add_wait_event_to_set`, `modify_wait_event`, `wait_event_set_wait` | waiteventset.c | thin; `attach_my_latch: bool` marshals the `MyLatch`/NULL latch argument; resowner fixed to NULL as at the only C call site |
| `reset_latch_my_latch` | latch.c | thin |
| `on_proc_exit` | ipc.c | registration passthrough |
| `create_socket_lock_file` | miscinit.c | thin |
| `pg_getaddrinfo_all`, `pg_getnameinfo_all` | common/ip.c | owned-`Vec<PgAddrInfo>` marshal; `pg_freeaddrinfo_all` = drop |
| `pg_set_noblock` | port/noblock.c | thin |
| `pg_usleep` | port/pgsleep.c | thin |

No branching, node construction, or computation in any seam path; every seam
declaration crate contains only `seam!` declarations. No function body in this
unit was replaced by a seam call. OK.

## Build / tests

`cargo build --workspace` clean; `cargo test --workspace` green
(backend-libpq-pqcomm: 13 unit tests incl. framing, buffer enlargement,
strtoul semantics, busy-flag, maxlen rejection).

## Verdict

**PASS** (after fix round 1). All 47 in-scope functions MATCH (1 Windows-only
function correctly out of scope); zero seam findings.
