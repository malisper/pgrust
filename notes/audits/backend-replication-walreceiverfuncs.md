# Audit: backend-replication-walreceiverfuncs

- **Date:** 2026-06-13
- **Model:** Claude Fable 5 (Opus 4.8 1M)
- **Verdict:** PASS (independent re-audit)
- **C source:** `src/backend/replication/walreceiverfuncs.c` (PostgreSQL 18.3, 409 lines)
- **Completeness oracle:** `../pgrust/c2rust-runs/backend-replication-walreceiverfuncs/src/walreceiverfuncs.rs`
- **Port:** `crates/backend-replication-walreceiverfuncs/src/lib.rs`
- **Owned seam crate:** `crates/backend-replication-walreceiverfuncs-seams`

## Function inventory

The C file defines 10 external functions plus the file-scope global `WalRcv`.
The c2rust run additionally renders the inlined spinlock/atomic primitives
(`pg_atomic_read_u64{,_impl}`, `pg_atomic_init_u64{,_impl}`, `tas`, `s_lock`)
the preprocessor folded in from `atomics.h` / `s_lock.h`; these are the
shmem synchronization model, not walreceiverfuncs logic, and are subsumed by
the owned `WalRcvShared` (`Mutex<WalRcvData>` + `AtomicU64` + `AtomicI32`).

| # | C function | C lines | Port location | Verdict | Notes |
|---|-----------|---------|---------------|---------|-------|
| 1 | `WalRcvShmemSize` | 43-51 | lib.rs:140 | MATCH | `add_size(0, size_of::<WalRcvShared>())`; `add_size` checked-add with the C overflow `ereport(ERROR)` text. `WalRcvShared` (guarded + 2 atomics) is the full-struct analog; size is informational (owned singleton, not a real shmem alloc). |
| 2 | `WalRcvShmemInit` | 54-72 | lib.rs:161 | MATCH | `OnceLock::get_or_init` = ShmemInitStruct + `!found` first-time init. Default zero-state == MemSet 0 (state=STOPPED, atomics 0); explicit re-inits replicated: CV init, writtenUpto store 0, procno = INVALID_PROC_NUMBER. SpinLockInit == fresh Mutex. |
| 3 | `WalRcvRunning` | 75-120 | lib.rs:192 | MATCH | Read state+startTime under lock, release; STARTING + `(now-startTime) > WALRCV_STARTUP_TIMEOUT` re-check under re-acquired lock, set STOPPED + broadcast; return `state != STOPPED`. |
| 4 | `WalRcvStreaming` | 126-172 | lib.rs:237 | MATCH | Same timeout machinery as #3; returns true for STREAMING\|STARTING\|RESTARTING via `matches!`. |
| 5 | `ShutdownWalRcv` | 178-230 | lib.rs:281 | MATCH | `switch(walRcvState)` arms exact; STREAMING/WAITING/RESTARTING -> STOPPING then fall-through to read `pid` (replicated by setting walrcvpid in that arm AND the STOPPING arm). stopped-broadcast, conditional kill(SIGTERM), CV prepare / sleep-while-WalRcvRunning / cancel loop. |
| 6 | `RequestXLogStreaming` | 245-321 | lib.rs:344 | MATCH | XLogSegmentOffset segment-start snap; Assert -> debug_assert; conninfo strlcpy/clear; slotname persistent-vs-temp branch (`slotname != NULL && slotname[0] != '\0'`) exact; STOPPED->STARTING+launch else RESTARTING; startTime set; first-startup-or-TLI-change init of flushedUpto/receivedTLI/latestChunkStart; receiveStart/TLI set; launch -> PMSIGNAL_START_WALRECEIVER else SetLatch when procno != INVALID_PROC_NUMBER. |
| 7 | `GetWalRcvFlushRecPtr` | 331-346 | lib.rs:435 | MATCH | flushedUpto under lock; optional latestChunkStart / receivedTLI out-params via `Option<&mut>`. |
| 8 | `GetWalRcvWriteRecPtr` | 352-358 | lib.rs:458 | MATCH | lock-free atomic read of writtenUpto. |
| 9 | `GetReplicationApplyDelay` | 364-388 | lib.rs:470 | MATCH | receivePtr (locked) vs GetXLogReplayRecPtr(NULL) seam; `== -> return 0`; chunkReplayStartTime seam, `==0 -> -1`; else TimestampDifferenceMilliseconds(chunk, GetCurrentTimestamp()). |
| 10 | `GetReplicationTransferLatency` | 394-408 | lib.rs:503 | MATCH | lastMsgSendTime/lastMsgReceiptTime under lock; TimestampDifferenceMilliseconds. |

Constants verified against headers: `WALRCV_STARTUP_TIMEOUT = 10`
(walreceiverfuncs.c:40), `MAXCONNINFO = 1024`, `NAMEDATALEN = 64`,
`WalRcvState` discriminants 0..5 in declared order, `XLogSegmentOffset` =
`ptr & (segsz - 1)`.

## Locking model

The C struct's single `slock_t mutex` guards the data fields plus `startTime`.
The port carries `start_time` as a separate `Mutex<pg_time_t>` from
`guarded: Mutex<WalRcvData>`. In every read site (`WalRcvRunning`,
`WalRcvStreaming`) the port holds `guarded` while reading `start_time`, and in
the one writer (`RequestXLogStreaming`) it writes `start_time` while holding
`guarded` — acquisition order is consistent (guarded -> start_time everywhere),
so the (state, startTime) snapshot is atomic w.r.t. the writer.
Behavior-preserving against the single C spinlock; no deadlock.

## Seam audit

Owned seam crate (by C-source coverage = walreceiverfuncs.c):
`backend-replication-walreceiverfuncs-seams`. 11 declarations.

`init_seams()` installs 10: `wal_rcv_shmem_size`, `wal_rcv_shmem_init`,
`with_walrcv`, `set_written_upto`, `get_written_upto`, `set_force_reply`,
`take_force_reply`, `wal_rcv_stopped_cv_broadcast`,
`get_replication_apply_delay`, `get_replication_transfer_latency`. It also
installs `walreceiver-seams::get_wal_rcv_flush_rec_ptr` (the (lsn,tli) form for
xlog checkpoint / walsummarizer) — correct, this crate is the real owner.

The 11th decl, `xlog_request_wal_receiver_reply`, is mis-homed in this seams
crate but maps to `xlogrecovery.c`'s `XLogRequestWalReceiverReply`, NOT
walreceiverfuncs.c. By the ownership-is-by-C-source-coverage rule it is not this
unit's seam to install; xlogrecovery (still in_progress) owns it. The
`every_declared_seam_is_installed_by_its_owner` recurrence guard passes, so it
is not flagged as an owned-but-uninstalled seam.

Seam accessors (`with_walrcv`, `set/get_written_upto`, `set/take_force_reply`,
`wal_rcv_stopped_cv_broadcast`) are all thin marshal+delegate over the owned
block; no branching/computation in any seam path. The state-machine
`switch(walRcvState)` logic lives in this crate, not behind a seam — MATCH, not
MISSING.

Outward seam calls (`xlog::wal_segment_size`,
`xlogrecovery::{get_xlog_replay_rec_ptr, get_current_chunk_replay_start_time}`,
`timestamp::{get_current_timestamp, timestamp_difference_milliseconds}`,
`pmsignal::SendPostmasterSignal`, `latch::set_latch_for_procno`) are each a
single delegate into the real owner — genuine cross-unit dependencies.

## Gates

- `cargo check --workspace`: clean (Finished, 0 errors).
- `cargo test --workspace`: 0 genuine failures; only the known 2 timeout flakes
  (process exit 144, no FAILED / panicked lines).
- `cargo test -p backend-replication-walreceiverfuncs`: 4 passed.
- `seams-init` recurrence guard: both
  `every_seam_installing_crate_is_wired_into_init_all` and
  `every_declared_seam_is_installed_by_its_owner` pass. `init_seams()` is wired
  into `seams_init::init_all()` (seams-init/src/lib.rs:98).

## Verdict: PASS

Every function MATCH; no MISSING/PARTIAL/DIVERGES; no own-logic stubs; zero seam
findings; wiring + guard green.

---

## Follow-up (2026-06-15): streaming-control seams installed

- **Model:** Claude Opus 4.8 (1M)
- **Verdict:** PASS

The 6 streaming-control seams the recovery page-read driver
(`xlogrecovery.c WaitForWALToBecomeAvailable`, `pageread.rs`) reaches were
previously declared-but-uninstalled and sat in
`seams-init::CONTRACT_RECONCILE_PENDING`. They are now installed with real
bodies; the allowlist entries were removed.

### walreceiverfuncs.c-owned (installed by this crate's `init_seams()`)

| Seam | Body | Verdict |
|------|------|---------|
| `wal_rcv_streaming` | `WalRcvStreaming` (lib.rs) | MATCH — direct install |
| `request_xlog_streaming` | `request_xlog_streaming_seam` → `RequestXLogStreaming` | MATCH — adapter maps `&str` conninfo/slotname to `Some(bytes)`; a non-NULL empty C string copies 0 bytes through `strlcpy`, reproduced by the `Some(b"")` arm |
| `get_wal_rcv_flush_rec_ptr_full` | `get_wal_rcv_flush_rec_ptr_full_seam` → `GetWalRcvFlushRecPtr` | MATCH — out-params returned as `(flushedUpto, latestChunkStart, receiveTLI)` tuple |
| `shutdown_wal_rcv` (NEW decl) | `ShutdownWalRcv` (void seam, `.expect` on `PgResult`) | MATCH — inner walreceiverfuncs.c routine, called by xlog's `XLogShutdownWalRcv` |

### xlog.c-owned (installed by `backend-access-transam-xlog::init_seams()`)

`XLogShutdownWalRcv`, `SetInstallXLogFileSegmentActive`,
`ResetInstallXLogFileSegmentActive` are xlog.c functions touching the
xlog-owned `XLogCtl->InstallXLogFileSegmentActive` flag under `ControlFileLock`.
They are ported in `backend-access-transam-xlog/src/write.rs` and installed from
xlog's `init_seams` (the real owner), even though the seams were declared in
this `-seams` crate (consumed by the recovery driver). `IsInstallXLogFileSegmentActive`
(xlog-seams) was also ported and installed in the same pass.

| C function (xlog.c) | Port | Verdict |
|---------------------|------|---------|
| `SetInstallXLogFileSegmentActive` (9554) | `write::SetInstallXLogFileSegmentActive` | MATCH — `LWLockAcquire(ControlFileLock, LW_EXCLUSIVE)`, set flag true, release |
| `ResetInstallXLogFileSegmentActive` (9563) | `write::ResetInstallXLogFileSegmentActive` | MATCH — same, flag false |
| `IsInstallXLogFileSegmentActive` (9571) | `write::IsInstallXLogFileSegmentActive` | MATCH — `LW_SHARED`, read flag, release |
| `XLogShutdownWalRcv` (9546) | `write::XLogShutdownWalRcv` | MATCH — `ShutdownWalRcv()` (via `shutdown_wal_rcv` seam) then `ResetInstallXLogFileSegmentActive()` |

The deferred-driver stub for `XLogShutdownWalRcv` was removed from xlog's
`xlog_driver_deferred!` list and replaced by the real `write::` function.

### Gates

- `cargo check --workspace`: clean.
- `cargo test -p no-todo-guard -p seams-init`: pass (both recurrence guards green;
  the new `shutdown_wal_rcv` seam is installed by its owner).
- `cargo test -p backend-replication-walreceiverfuncs`: 4 passed.
- `cargo test -p backend-access-transam-xlog --lib`: 37 passed.
