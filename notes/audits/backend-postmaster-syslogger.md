# Audit: backend-postmaster-syslogger

- **Unit**: `backend-postmaster-syslogger` (`src/backend/postmaster/syslogger.c`, PostgreSQL 18.3)
- **Port**: `crates/backend-postmaster-syslogger` (`src/lib.rs`, `src/config.rs`, `src/tests.rs`)
- **Seam crate owned by this unit**: `crates/backend-postmaster-syslogger-seams`
- **New seam/type crates introduced by this port** (owners not yet ported):
  `backend-storage-ipc-waiteventset-seams`, `backend-storage-file-fd-seams`,
  `backend-postmaster-launch-backend-seams`, `backend-timezone-pgtz-seams`,
  `backend-timezone-strftime-seams`, `backend-utils-misc-more-seams`,
  `port-pqsignal-seams`, `common-file-perm-seams`, `types-pgtime`, `types-signal`
- **C cross-check**: `/Users/malisper/workspace/work/pgrust/c2rust-runs/backend-postmaster-syslogger/src/syslogger.rs`
  (14 function definitions; matches the C inventory below — the WIN32
  `pipeThread` and EXEC_BACKEND `syslogger_fdget`/`syslogger_fdopen` are absent
  from the c2rust build, as expected for the non-WIN32, non-EXEC_BACKEND
  configuration this repo targets)
- **Audit date**: 2026-06-12. Re-derived independently from the C source and
  the c2rust rendering; the port's own comments were not trusted.

## Function inventory and verdicts

| # | C function (syslogger.c) | C lines | Port location (lib.rs) | Verdict | Notes |
|---|---|---|---|---|---|
| 1 | `SysLoggerMain` | 164–587 | `SysLoggerMain` (248–546) | MATCH | Full main loop verified branch-by-branch: redirection_done `/dev/null` re-point (close-then-dup2 ordering, close even on failed open), write-end pipe close + `-1` reset, full signal table (SIGHUP→config-reload handler, INT/TERM/QUIT/ALRM/PIPE/USR2→SIG_IGN, USR1→sigUsr1Handler, SIGCHLD→SIG_DFL), `sigprocmask(SIG_SETMASK, &UnBlockSig)`, last-file-name recompute from `first_syslogger_file_time` (csv/json conditional on file non-null), pstrdup'd current params, `set_next_rotation_time` + `update_metainfo_datafile`, `whereToSendOutput = DestNone`, WES with latch + pipe-read events, ResetLatch-first loop, SIGHUP block (dir/filename change → rotation_requested + MakePGDirectory; csv/json on-off mismatch; rotation-age change → reset time only; rotation_disabled re-enable; unconditional metainfo rewrite), age rotation (`now >= next_rotation_time`), size rotation gated on `!rotation_requested` with per-dest `ftello >= Log_RotationSize * 1024` (i64 math), forced all-dest rotation when both triggers zero, timeout clamp `INT_MAX/1000` with stale-`now` reuse exactly as C, `read()` EINTR/LOG-ereport (errcode_for_socket_access)/`continue`-on-data/EOF→flush, DEBUG1 "logger shutting down" → `proc_exit(0)` without closing syslogFile. Sanctioned divergences only: `MemoryContextDelete(PostmasterContext)` not reproduced (mcx RAII, documented), `MyBackendType = B_LOGGER` ports as the globals seam + elog's `am_syslogger` mirror, `pg_noreturn` ports as `PgResult` (ERROR-capable callees propagate `Err` instead of longjmp). |
| 2 | `SysLogger_Start` | 592–789 | `SysLogger_Start` (563–699) | MATCH | Pipe created once (`syslogPipe[0] < 0`), FATAL with errcode_for_socket_access on `pipe()` failure; `MakePGDirectory` errors ignored; `first_syslogger_file_time = time(NULL)`; initial stderr file always opened with `allow_errors=false` (FATAL); csv/json files opened iff `Log_destination` bit set; `postmaster_child_launch(B_LOGGER, child_slot, NULL, 0)`; `-1` → LOG "could not fork system logger: %m" + return 0; first-time redirection: breadcrumb LOG+errhint, fflush(stdout)/dup2/FATAL, fflush(stderr)/dup2/FATAL, close write end + `-1`, `redirection_done = true`; postmaster closes all three FILE handles (csv/json conditionally). `fflush` is realized on the Rust `std::io` handles — correct in this codebase, where all stdout/stderr writes (elog's `write_stderr` included) go through `std::io`, so the analogous buffers are the ones flushed. `Assert(Logging_collector)` → `debug_assert!`. |
| 3 | `process_pipe_input` (static) | 878–1033 | `process_pipe_input` (712–834) | MATCH | Loop bound `count >= offsetof(PipeProtoHeader, data) + 1` = 10 (header retained unprocessed at exactly 9 bytes — covered by a unit test); header parse = native-endian `u16 len`/`i32 pid`/`u8 flags` at offsets 2/4/8 (verified against `PipeProtoHeader` layout); validity predicate identical (`nuls`, `0 < len <= PIPE_MAX_PAYLOAD`, `pid != 0`, `pg_number_of_ones[dest_flags] == 1` → `count_ones() == 1`); break on incomplete chunk; dest selection ordering stderr→csv→json; per-pid list at `pid % NBUFFER_LISTS` (negative pid: C out-of-bounds UB → Rust index panic, acceptable UB→defined); non-final: append-to-existing / first-free-slot / push-new, exactly C's single-pass existing-then-free scan semantics; final: append+write+mark-free vs single-chunk direct write; non-protocol path scans from `chunklen=1` for the next NUL and dumps to stderr dest; left-justify via `copy_within` under `count > 0 && cursor != start`. |
| 4 | `flush_pipe_input` (static) | 1041–1077 | `flush_pipe_input` (840–866) | MATCH | All 256 lists scanned in order; active buffers (pid != 0) written to `LOG_DESTINATION_STDERR` and marked free; residual buffer bytes flushed as-is; `*bytes_in_logbuffer = 0`. Collect-then-write keeps C's output order (`write_syslogger_file` never touches the lists). |
| 5 | `write_syslogger_file` (exported) | 1092–1128 | `write_syslogger_file` (880–915) | MATCH | Destination fallback chain csv→json→syslogFile with `&` bit tests and open-file checks identical; `fwrite` count mismatch reported via `write_stderr("could not write to log file: %m\n")` (never ereport — recursion), `%m` pre-expanded as elog's `write_stderr` printf would. C `buffer`/`count` folded into `&[u8]`. Null `syslogFile` (C segfault) surfaces as the failed-write report — UB→defined. Installed into the seam (see seam audit). |
| 6 | `logfile_open` (static) | 1218–1253 | `logfile_open` (924–956) | MATCH | umask `~(Log_file_mode \| S_IWUSR) & (S_IRWXU\|S_IRWXG\|S_IRWXO)` set around `fopen`, restored after; success → `setvbuf(_IOLBF)`; failure → ereport LOG-or-FATAL (errcode_for_file_access, filename in message) then errno restored to the fopen errno (`set_errno`) so the caller's ENFILE/EMFILE test sees the right value; returns the (possibly null) handle. FATAL diverges in elog, matching C's no-return. |
| 7 | `logfile_rotate_dest` (static) | 1263–1357 | `logfile_rotate_dest` (962–1052) | MATCH | Turned-off non-stderr destination → close file, clear name, return true; skip when `!time_based_rotation && (size_rotation_for & target_dest) == 0`; ext None/".csv"/".json"; truncate-vs-append predicate (`Log_truncate_on_rotation && time_based_rotation && last != NULL && strcmp != 0`) reproduced including the name-inequality; open failure: errno read right after (preserved by `logfile_open`), non-ENFILE/EMFILE → LOG "disabling automatic rotation..." + `rotation_disabled = true`, return false; success: close old handle, store new file + name. The C `char **`/`FILE **` out-params are the `Slot` accessor pair — behaviorally identical. |
| 8 | `logfile_rotate` (static) | 1362–1400 | `logfile_rotate` (1056–1106) | MATCH | Clears `rotation_requested`; `fntime` = planned rotation time when time-based else `time(NULL)`; stderr→csv→json with early return on false; then `update_metainfo_datafile()` and `set_next_rotation_time()`. |
| 9 | `logfile_getname` (static) | 1411–1436 | `logfile_getname` (1114–1149) | MATCH (after fix) | `"%s/"` prefix with MAXPGPATH (=1024, verified) snprintf truncation; `pg_strftime(pattern, pg_localtime(&t, log_timezone))` via the pgtz/strftime seams with the C `MAXPGPATH - len` cap applied caller-side (NUL byte reserved, char-boundary safe); suffix replaces a trailing ".log" (`len > 4` + byte compare) and is `strlcpy`-capped at `MAXPGPATH - len`. **Initial port DIVERGED**: the ".log" test used a `String` slice `&filename[len - 4..]`, which panics when `len - 4` splits a multibyte UTF-8 character (reachable via a multibyte `log_filename` GUC) where C's `strcmp` byte-compares; fixed to `filename.as_bytes()[len - 4..] == *b".log"` (ASCII match ⇒ `len - 4` is a boundary ⇒ `truncate` safe) and re-audited from scratch. Remaining note: on >1023-byte renders C's `pg_strftime` overflow leaves an indeterminate, possibly unterminated buffer (UB on the subsequent `strlen`); the port truncates deterministically — UB→defined. |
| 10 | `set_next_rotation_time` (static) | 1441–1466 | `set_next_rotation_time` (1149–1168) | MATCH | Early return when `Log_RotationAge <= 0`; `rotinterval = Log_RotationAge * SECS_PER_MINUTE` (no overflow: GUC max is `INT_MAX/SECS_PER_MINUTE`); `now += tm_gmtoff; now -= now % rotinterval; now += rotinterval; now -= tm_gmtoff` in `pg_time_t` (i64) arithmetic, matching C's promotion. |
| 11 | `update_metainfo_datafile` (static) | 1476–1562 | `update_metainfo_datafile` (1175–1283) | MATCH | No stderr/csv/json destination → unlink `current_logfiles` (LOG unless ENOENT) and return; otherwise `umask(pg_mode_mask)` around `fopen(LOG_METAINFO_DATAFILE_TMP, "w")` (restored), `setvbuf(_IOLBF)`, open failure → LOG + return; three `"<label> <name>\n"` lines gated on `last_*_file_name && (Log_destination & bit)` in stderr/csvlog/jsonlog order, short write → LOG + fclose + return; fclose; `rename` tmp→final, failure → LOG. `fprintf < 0` ports as `fwrite != len` — same error predicate. Constants `"current_logfiles"`/`".tmp"` verified against syslogger.h. |
| 12 | `CheckLogrotateSignal` (exported) | 1573–1582 | `CheckLogrotateSignal` (1292–1296) | MATCH | `stat("logrotate") == 0`. |
| 13 | `RemoveLogrotateSignalFiles` (exported) | 1587–1591 | `RemoveLogrotateSignalFiles` (1300–1305) | MATCH | `unlink("logrotate")`, result ignored. |
| 14 | `sigUsr1Handler` (static) | 1594–1599 | `sigUsr1Handler` (1309–1312) | MATCH | Sets `rotation_requested`, `SetLatch(MyLatch)` via the latch seam. |
| — | `pipeThread` (WIN32 only) | 1139–1208 | not ported | N/A | WIN32-only; outside this repo's build config (absent from c2rust output too). |
| — | `syslogger_fdget`/`syslogger_fdopen` (EXEC_BACKEND only) | 800–846 | not ported | N/A | EXEC_BACKEND-only; outside the build config. |

## Globals / GUCs (`config.rs`)

Boot values verified against the C initializers (syslogger.c:70–76, 114):
`Logging_collector=false`, `Log_RotationAge=HOURS_PER_DAY*MINS_PER_HOUR=1440`,
`Log_RotationSize=10*1024`, `Log_directory="log"`,
`Log_filename="postgresql-%Y-%m-%d_%H%M%S.log"` (guc_tables.c boot values for
the two strings, since C inits them NULL and GUC fills them),
`Log_truncate_on_rotation=false`, `Log_file_mode=S_IRUSR|S_IWUSR=0o600`,
`syslogPipe={-1,-1}`. All private statics present as thread-locals with the C
initial values; `buffer_lists` lazily sized to `NBUFFER_LISTS=256`.

## Constants verified against headers

- `PIPE_CHUNK_SIZE`: syslogger.h = `PIPE_BUF` clamped to 64K → 4096 (Linux) /
  512 (macOS/BSD); port cfg matches; c2rust (macOS run) shows
  `READ_BUF_SIZE = 1024 = 2*512`, agreeing.
- `PIPE_HEADER_SIZE = offsetof(PipeProtoHeader, data) = 9` (2+2+4+1, char[]
  flexible member, no padding); matches elog's `write_pipe_chunks` constant.
- Flag bits `PIPE_PROTO_IS_LAST=0x01`, `DEST_STDERR=0x10`, `DEST_CSVLOG=0x20`,
  `DEST_JSONLOG=0x40` — syslogger.h:63–67. ✓
- `LOG_DESTINATION_STDERR/CSVLOG/JSONLOG = 1/8/16` — elog.h:498–502. ✓
- `B_LOGGER = 17` — miscadmin.h BackendType enum, counted. ✓
- `WAIT_EVENT_SYSLOGGER_MAIN = PG_WAIT_ACTIVITY + 13 = 83886093` — confirmed
  against the c2rust constant. ✓
- `WL_LATCH_SET = 1<<0`, `WL_SOCKET_READABLE = 1<<1` — waiteventset.h. ✓
- `MAXPGPATH = 1024`, `PGINVALID_SOCKET = -1`, `LOGROTATE_SIGNAL_FILE =
  "logrotate"`, `LOG_METAINFO_DATAFILE(_TMP)`. ✓
- `HOURS_PER_DAY=24`, `MINS_PER_HOUR=60`, `SECS_PER_MINUTE=60` (datetime.h). ✓

## Seam audit

**Owned seam (`backend-postmaster-syslogger-seams`)**: one declaration,
`write_syslogger_file(data: &[u8], dest: i32)` — signature reconciled against
the C `(const char *buffer, int count, int destination)` (count folded into the
slice, infallible as in C). Installed by this crate's `init_seams()` (a single
`set()` call, nothing else), which `seams-init::init_all()` invokes
(seams-init/src/lib.rs:16). No `set()` of this seam anywhere else in the tree.
The consumer (elog's `send_message_to_server_log` when `am_syslogger`) calls it
as thin marshal+delegate. ✓

**Outward seam calls** (all owners unported; direct deps would be impossible or
cyclic — elog⇄syslogger is the canonical cycle, broken by the owned seam):

| Seam | C call | Thin? |
|---|---|---|
| `backend-storage-ipc-waiteventset-seams::{create_wait_event_set, add_wait_event_to_set, wait_event_set_wait}` | `CreateWaitEventSet(NULL,2)`, `AddWaitEventToSet`, `WaitEventSetWait` | ✓ opaque u64 set token; `MyLatch` resolved owner-side via `attach_my_latch` flag; occurred-event masks returned (`rc==1 && events==WL_SOCKET_READABLE` reproduced exactly) |
| `backend-storage-file-fd-seams::make_pg_directory` | `MakePGDirectory(Log_directory)` ×2, result ignored as in C | ✓ |
| `backend-postmaster-launch-backend-seams::postmaster_child_launch` | `postmaster_child_launch(B_LOGGER, child_slot, NULL, 0, NULL)` | ✓ |
| `backend-timezone-pgtz-seams::pg_localtime_log_timezone` | `pg_localtime(&t, log_timezone)` (global resolved by owner) | ✓ `None` = C NULL (C dereferences unchecked → port `expect`s loudly) |
| `backend-timezone-strftime-seams::pg_strftime` | `pg_strftime(buf, max, fmt, tm)` | ✓ owned-String marshal; the C `maxsize` cap is applied caller-side byte-for-byte |
| `backend-utils-misc-more-seams::init_ps_display` | `init_ps_display(NULL)` | ✓ |
| `port-pqsignal-seams::pqsignal` | `pqsignal(...)` ×9 (src/port/pqsignal.c owner) | ✓ `SigDisposition` carries SIG_DFL/SIG_IGN/handler |
| `common-file-perm-seams::pg_mode_mask` | `umask(pg_mode_mask)` global read | ✓ |
| `backend-utils-misc-guc-seams::process_config_file` | `ProcessConfigFile(PGC_SIGHUP)` | ✓ (pre-existing crate) |
| `backend-storage-ipc-latch-seams::{reset_latch_my_latch, set_latch_my_latch}` | `ResetLatch(MyLatch)`, `SetLatch(MyLatch)` | ✓ (pre-existing crate, extended) |
| `backend-storage-ipc-seams::proc_exit` | `proc_exit(0)` | ✓ |
| `backend-utils-init-small-seams::{my_start_time, set_my_backend_type}` | `MyStartTime`, `MyBackendType = B_LOGGER` global accesses | ✓ |

No branching, node construction, or computation found in any seam path. Direct
(non-seam) deps — `backend-utils-error` (ereport/write_stderr/log_destination/
redirection_done mirrors), `backend-postmaster-interrupt`
(ConfigReloadPending/SignalHandlerForConfigReload), `backend-libpq-pqsignal`
(UnBlockSig) — are acyclic and correct.

## Findings and fixes

1. **`logfile_getname` — DIVERGES (fixed)**: `&filename[len - 4..] == ".log"`
   panics on a non-char-boundary slice for multibyte `log_filename` values
   where the C `strcmp` simply compares bytes. Fixed to a byte-slice compare
   (`filename.as_bytes()`); re-audited the function from scratch → MATCH.

No other findings. Fix round count: 1.

## Verdict

**PASS** — all 14 in-config functions MATCH (one after the fix above); the
owned seam is installed by `init_seams()` and wired into
`seams-init::init_all()`; every outward seam call is thin marshal+delegate to
an unported owner. Workspace builds clean; all tests pass (including the
crate's pipe-protocol and truncation tests).

Spot-checks re-derived in full detail before sign-off: `process_pipe_input`
(header layout/offsets against `PipeProtoHeader`, slot-reuse semantics),
`SysLoggerMain` timeout/rotation interplay (stale-`now` reuse, INT_MAX clamp),
`logfile_open`/`logfile_rotate_dest` errno preservation contract, and the
`WAIT_EVENT_SYSLOGGER_MAIN` value against the c2rust constant.
