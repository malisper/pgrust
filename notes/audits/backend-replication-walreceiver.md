# Audit: backend-replication-walreceiver

- **Date:** 2026-06-12
- **Model:** Opus 4.8
- **Verdict:** PASS
- **Branch:** port/backend-replication-walreceiver
- **C source:** `src/backend/replication/walreceiver.c` (1522 lines)
- **Port:** `crates/backend-replication-walreceiver/src/lib.rs`

Independent re-audit per `.claude/skills/audit-crate/SKILL.md`. The function
inventory was rebuilt from the C source and cross-checked against
`pgrust/c2rust-runs/backend-replication-walreceiver/src/walreceiver.rs`. Every
function definition in the translation unit (15 total; the two `static`
declarations at C lines 138/140 are forward prototypes of the definitions at
819/890, not separate functions) was compared C ⇄ c2rust ⇄ Rust for control
flow, constants, error paths, and design conformance. The c2rust file also
inlined header static-inline helpers (`XLogFileName`, `TLHistoryFileName`,
`set_ps_display`, the `pq_send*`/`pq_getmsg*` encoders, `Int*GetDatum`, atomics);
those are ported as in-crate helpers and verified separately below.

## Per-function table

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `WalReceiverMain` | 152 | `wal_receiver_main` / `wal_receiver_main_inner` 292/303 | MATCH | Full state machine + streaming loop. STOPPING falls through to STOPPED (broadcast CV + proc_exit(1)); STARTING is the usual path; WAITING/STREAMING/RESTARTING ⇒ PANIC. `now` computed before the closure (no lock dependency). Both `rc & WL_LATCH_SET` and `rc & WL_TIMEOUT` checked as independent `if`s (matches C). Inner receive loop processes `buf[0]`/`&buf[1..len]` exactly as C `buf[0]`/`&buf[1]`,`len-1`. Archive-mode branch (`!= ARCHIVE_MODE_ALWAYS`→ForceDone else Notify) matches. `proc_exit`/`my_proc_pid` passed explicitly (no-ambient-global). Top-level error ⇒ panic (no Rust caller to unwind to), faithful to `-> !` child-launch contract. |
| `WalRcvWaitForStartPosition` | 645 | 754 | MATCH | STREAMING→WAITING transition under lock; non-STREAMING ⇒ proc_exit(0) on STOPPING else FATAL. Inner loop: assert(RESTARTING\|WAITING\|STOPPING); RESTARTING adopts receiveStart/TLI→STREAMING→break; STOPPING ⇒ proc_exit(1) (C `exit(1)`); WaitLatch with WL_LATCH_SET\|WL_EXIT_ON_PM_DEATH. `update_process_title` PS-display tail matches. |
| `WalRcvFetchTimeLineHistoryFiles` | 725 | 846 | MATCH | `for tli in first..=last`; skips tli==1; `existsTimeLineHistory` check; filename sanity (PROTOCOL_VIOLATION on mismatch); write file; archive-mode branch. `wrconn` read from FileState. |
| `WalRcvDie` | 781 | `wal_rcv_die_callback` 897 / `WalRcvDie` 903 | MATCH | C registers `PointerGetDatum(&startpointTLI)`; port reads the live TLI from FileState (faithful read-latest of the pointer). assert(TLI!=0); flush(dying=true); state asserts + STOPPED/pid=0/procno=INVALID/ready=false under lock; CV broadcast; disconnect if conn; WakeupRecovery. |
| `XLogWalRcvProcessMsg` | 819 | 938 | MATCH | 'w': hdrlen=24, `len<hdr`⇒ERROR PROTOCOL_VIOLATION, parse dataStart/walEnd/sendTime, ProcessWalSndrMessage, write payload. 'k': hdrlen=17, `len!=hdr`⇒ERROR, parse + optional immediate reply. default⇒ERROR with type. |
| `XLogWalRcvWrite` | 890 | 991 | MATCH | assert(tli!=0); segment close-on-boundary, create-on-need (XLByteToSeg/XLogFileInit), startoff/segbytes clamp, pgstat I/O timing bracket, pwrite, `byteswritten<=0`⇒PANIC (errcode_for_file_access, filename), recptr/nbytes/buf advance, LogstreamResult.Write, set_written_upto, trailing close-on-boundary. |
| `XLogWalRcvFlush` | 985 | 1089 | MATCH | assert(tli!=0); `Flush<Write` guard; issue_xlog_fsync; Flush=Write; shmem flushedUpto/latestChunkStart/receivedTLI update under lock; WakeupRecovery + cascade walsnd wakeup; PS display; `!dying`⇒reply+HSFeedback. |
| `XLogWalRcvClose` | 1040 | 1129 | MATCH | asserts; flush(false); filename; close⇒PANIC on error; archive-mode branch; recvFile=-1. |
| `XLogWalRcvSendReply` | 1092 | 1172 | MATCH | `!force && interval<=0`⇒return; now; `!force && reply_write==Write && reply_flush==Flush && now<wakeup[REPLY]`⇒return; ComputeNextWakeup(REPLY); build 'r' message (write/flush/apply/now/requestReply byte) big-endian; DEBUG2 elog; walrcv_send. Function-local statics writePtr/flushPtr→FileState. |
| `XLogWalRcvSendHSFeedback` | 1161 | 1245 | MATCH | `(interval<=0 \|\| !hsfb) && !primary_has_standby_xmin`⇒return; now; `!immed && now<wakeup[HSFEEDBACK]`⇒return; ComputeNextWakeup(HSFEEDBACK); `!HotStandbyActive`⇒return; xmin/catalog_xmin from horizons or Invalid; epoch adjust (`nextXid<xmin`/`<catalog_xmin` decrement); DEBUG2; build 'h' message; walrcv_send; set primary_has_standby_xmin. Function-local static→FileState (init true). |
| `ProcessWalSndrMessage` | 1257 | 1331 | MATCH | shmem latestWalEnd/latestWalEndTime/lastMsgSendTime/lastMsgReceiptTime under lock; DEBUG2-gated trace with apply-delay==-1 branch. |
| `WalRcvComputeNextWakeup` | 1309 | 1372 | MATCH | TERMINATE/PING use `wal_receiver_timeout` (ms; PING is `/2` integer division); HSFEEDBACK/REPLY use `wal_receiver_status_interval` (seconds); each `<=0`⇒TIMESTAMP_INFINITY. No default arm. |
| `WalRcvForceReply` | 1350 | 1413 | MATCH | set_force_reply (with barrier in seam); read procno under lock; SetLatch if procno valid. |
| `WalRcvGetStateString` | 1368 | 1426 | MATCH | Exhaustive 6-variant match → identical strings. C's trailing `return "UNKNOWN"` is dead (enum exhaustive); Rust match covers all variants. |
| `pg_stat_get_wal_receiver` | 1393 | 1443 | MATCH | Snapshot WalRcv under lock; `pid==0 \|\| !ready`⇒NULL (`Ok(None)`); read writtenUpto lock-free; privilege gate (`has_privs_of_role(GetUserId, ROLE_PG_READ_ALL_STATS=3375)`) returns pid-only tuple; field-by-field NULL/value selection ported 1:1. fmgr/Datum/tuple layer is the project-wide systemic deferral — returns structured `WalReceiverActivity`; the NULL-vs-value branching (the actual logic) is fully present. |

### Inlined header helpers (verified in-crate)

`XLogFileName`, `TLHistoryFileName` (format strings `%08X%08X%08X` / `%08X.history`),
`XLogSegmentsPerXLogId`, `XLogSegmentOffset`, `XLByteToSeg`, `XLByteInSeg`,
`XLogRecPtrIsInvalid`, `TimestampTzPlusMilliseconds`/`Seconds`,
`TransactionIdIsValid`, `Xid/EpochFromFullTransactionId`, `pq_sendbyte/int32/int64`
(big-endian), `pq_getmsgint64`, `lsn_fmt` (`%X/%X`), `pg_close`, `pg_pwrite`:
all match C macro/inline semantics.

## Constants (verified against headers)

- `MAXCONNINFO = 1024` (`replication/walreceiver.h:37`) ✓
- `NAMEDATALEN = 64` (`pg_config_manual.h:29`) ✓
- `ROLE_PG_READ_ALL_STATS = 3375` (`pg_authid.dat:52`) ✓
- Wakeup enum order TERMINATE=0/PING=1/REPLY=2/HSFEEDBACK=3, `NUM_WALRCV_WAKEUPS=4`
  (`walreceiver.c:120-124`) ✓ — `wakeup_reason_from_index` maps 0..3 correctly.
- `WalRcvState` 6-variant order (`walreceiver.h:47-53`) ✓
- `TIMESTAMP_INFINITY = i64::MAX`, `InvalidXLogRecPtr=0`, `InvalidTransactionId=0`,
  `PGINVALID_SOCKET=-1`, latch flags `WL_LATCH_SET=1<<0`/`WL_SOCKET_READABLE=1<<1`/
  `WL_TIMEOUT=1<<3`/`WL_EXIT_ON_PM_DEATH=1<<5` ✓
- Message header lengths: 'w' = 24 (3×int64), 'k' = 17 (2×int64 + char) ✓
- SQLSTATEs: CONNECTION_FAILURE, OBJECT_NOT_IN_PREREQUISITE_STATE,
  PROTOCOL_VIOLATION; severities ERROR/FATAL/PANIC/LOG/DEBUG1/DEBUG2 all match. ✓

## Seam audit

The process-global statics (`recvFile`, `recvFileTLI`, `recvSegNo`,
`LogstreamResult`, `wakeup[]`, the two function-local statics) and the
file-scope GUCs (`wal_receiver_status_interval`, `wal_receiver_timeout`,
`hot_standby_feedback`) live in a `thread_local` `FileState` — faithful since
the walreceiver is one single-threaded daemon per process; not a shared static.

Outward seams are all justified by real cross-unit boundaries and are thin
marshal+delegate (argument conversion, one call, result conversion), no
branching/node-construction/computation in any seam path:

- `walreceiverfuncs-seams` (`with_walrcv`, `set/get_written_upto`,
  `set/take_force_reply`, `wal_rcv_stopped_cv_broadcast`,
  `get_replication_apply_delay/transfer_latency`): `walreceiverfuncs.c` genuinely
  owns the `WalRcvData` shmem control block + the apply-delay/latency helpers
  (unported `todo`). `with_walrcv` brackets the caller's closure with the
  spinlock; ALL `switch(walRcvState)` and state-transition branching stays
  in-crate inside the closures (verified: the STOPPING/STOPPED/STARTING/PANIC
  arm, the WAITING/RESTARTING transitions, the flush shmem update, and the
  pg_stat snapshot all branch in-crate). `take_force_reply` is read-clear-barrier
  on an owned shmem field whose access discipline belongs to the owner — thin.
- `libpqwalreceiver-seams`: the libpq replication function table
  (`load_libpqwalreceiver`, `walrcv_connect/get_conninfo/get_senderinfo/
  identify_system/readtimelinehistoryfile/create_slot/get_backend_pid/
  startstreaming/receive/send/endstreaming/disconnect`) — a separate unit
  (`load_file("libpqwalreceiver")`).
- Acyclic direct deps (no cycle, imported directly, not seamed):
  `backend_postmaster_interrupt` (ConfigReloadPending),
  `backend_libpq_pqsignal`/`port_pqsignal_seams`, `backend_utils_init_small_seams`
  (MyProcPid/MyProcNumber), the in-crate `signal_handler_for_config_reload`.
- The remaining seams (xlog, xlogrecovery, xlogarchive, timeline, varsup,
  procarray, walsender, timestamp, ps_status, acl, guc_file, guc_tables, latch,
  ipc, pgstat_wal/io, waitevent, auxprocess, tcop, procsignal, miscinit) each
  delegate one owner function.

`init_seams()` contains exactly one `set()` (`wal_receiver_main`) and is called
by `seams-init::init_all()`. No `set()` outside the owner; no uninstalled
declared seam (the unit declares only `wal_receiver_main`).

### Minor observations (non-blocking)

- `pgstat_prepare_io_time(track_wal_io_timing)` — the C bool GUC argument is
  erased at the seam (`pgstat_prepare_io_time() -> instr_time`). The GUC choice
  is part of the pgstat-io subsystem the owner manages; the seam is WAL-write
  specific. Behavior only gates whether I/O timing is recorded. Acceptable.
- `pgstat_count_io_op_time` bakes `IOOBJECT_WAL/IOCONTEXT_NORMAL/IOOP_WRITE/cnt=1`
  into the owner (correct — those constants belong to pgstat-io). The Rust clamps
  `byteswritten.max(0)`; the only path with `byteswritten<=0` PANICs immediately
  after, so the clamp is observationally inert.

## Design conformance

- No invented opacity; types live in `types-walreceiver`/`types-wal`/`types-pgstat`.
- Allocating boundaries (`pg_stat_get_wal_receiver`, message builders) return
  `PgResult`/owned values; `ereport(ERROR/FATAL/PANIC)` modeled as `Err(PgError)`
  with matching SQLSTATE/severity, propagated with `?`.
- Per-backend globals modeled as `thread_local` FileState, not shared statics.
- No ambient-global seams (MyProcPid/MyProcNumber passed explicitly).
- No locks held across `?` (the spinlock is bracketed within `with_walrcv`
  closures that do not `?`).
- No registry-shaped side tables; no unledgered divergence markers.

## Result

All 15 functions + inlined helpers MATCH. Zero MISSING/PARTIAL/DIVERGES, zero
seam findings. **PASS.** CATALOG.tsv row set to `audited`.
