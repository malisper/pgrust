# Audit: backend-access-transam-twophase

Unit: `backend-access-transam-twophase` (`src/backend/access/transam/twophase.c`, 2752 lines)
Crate: `crates/backend-access-transam-twophase`
Owned outward seam crates created/extended: `backend-access-transam-twophase-fileio-seams`
(now installed in `init_seams()` over the merged `backend-storage-file-fd`),
`backend-replication-syncrep-seams`, `port-crc32c-seams`.
Inward seam crate: `backend-access-transam-twophase-seams`.

Audit is independent: function inventory derived from the C source and cross-checked against the
c2rust rendering (`c2rust-runs/backend-access-transam-twophase/src/twophase.rs`). Constants verified
against headers (`xact.h`, `twophase_rmgr.h`, `memutils.h`, `origin.h`).

## Constants (verified against headers)

| Constant | C value | Port | Verdict |
|---|---|---|---|
| `TWOPHASE_MAGIC` | `0x57F94534` (twophase.c:973) | `0x57F9_4534` | MATCH |
| `GIDSIZE` | 200 (xact.h:31) | 200 | MATCH |
| `XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK` | `1U<<1` (xact.h:108) | `1<<1` | MATCH |
| `TWOPHASE_RM_END_ID` | 0 | 0 | MATCH |
| `TWOPHASE_RM_MAX_ID` | `PREDICATELOCK_ID`=4 | 4 | MATCH |
| `MaxAllocSize` | `0x3fffffff` (memutils.h:40) | `0x3fff_ffff` | MATCH |
| `InvalidRepOriginId` / `DoNotReplicateId` | 0 / `PG_UINT16_MAX`=0xFFFF (origin.h) | 0 / 0xFFFF | MATCH |
| `xl_xact_prepare` wire layout | 72 bytes, gidlen@54, origin_lsn@56, origin_ts@64 | 72, codec matches offsets | MATCH |
| `GlobalTransactionData` / `TwoPhaseStateData` sizing | 256 / offsetof prepXacts=16 | 256 / 16 | MATCH |
| `xl_xact_stats_item` on-disk (16B, objid_hi<<32\|objid_lo) | xact.h:282 / pgstat_xact.c:80 | decode_stats_items matches | MATCH |

## Per-function table

| C function (twophase.c) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `TwoPhaseShmemSize` | `two_phase_shmem_size` | MATCH | offsetof+ptr-array maxalign+gtd array; sizing only (shmem owner consumes) |
| `TwoPhaseShmemInit` (!IsUnderPostmaster) | `TwoPhaseStateData::new` | MATCH | freelist LIFO order reproduced (push 0..max, pop from end); procno via seam |
| `AtProcExit_Twophase` | `at_proc_exit_twophase` + `register_twophase_exit` | SEAMED | exit-hook safety-net via `ipc::before_shmem_exit`, defers to backend-installed cleanup slot |
| `AtAbort_Twophase` | `at_abort_twophase` | MATCH | valid→clear locking_backend else RemoveGXact under lock |
| `PostPrepare_Twophase` | `post_prepare_twophase` | MATCH | clear locking_backend under lock |
| `MarkAsPreparing` | `mark_as_preparing` | MATCH | len/disabled/dup checks, freelist pop, guts, ondisk=false, all error SQLSTATEs match |
| `MarkAsPreparingGuts` | `mark_as_preparing_guts` | MATCH | dummy-PGPROC init seamed (`proc_init_prepared`); gxact fields + MyLockedGxact set |
| `GXactLoadSubxactData` | `proc::gxact_load_subxact_data` (seam) | SEAMED | PGPROC subxid cache is proc.c-owned |
| `MarkAsPrepared` | `mark_as_prepared` | MATCH | lock_held branch, valid=true, ProcArrayAdd seamed |
| `LockGXact` | `lock_gxact` | MATCH | valid/gid/busy/perm/db checks, all 4 error SQLSTATEs + hints match |
| `RemoveGXact` | `remove_gxact` | MATCH | swap-remove + freelist push; bounds→internal-error elog |
| `GetPreparedTransactionList` | `get_prepared_transaction_list` | MATCH | LW_SHARED snapshot copy |
| `pg_prepared_xact` (SRF) | `pg_prepared_xact_rows` | SEAMED | owned row projection (filter !valid, proc->xid/databaseId); funcapi/tupdesc glue at boundary |
| `TwoPhaseGetGXact` | `two_phase_get_gxact` | MATCH | static cache omitted (pure optimization, identical output) |
| `TwoPhaseGetXidByVirtualXID` | `two_phase_get_xid_by_virtual_xid` | MATCH | vxid match, have_more on 2nd match |
| `TwoPhaseGetDummyProcNumber` | `two_phase_get_dummy_proc_number` | MATCH | |
| `TwoPhaseGetDummyProc` | (returns pgprocno via above; PGPROC* deref is caller/seam) | SEAMED | PGPROC* is proc.c-owned |
| `AdjustToFullTransactionId` | `adjust_to_full_transaction_id` | MATCH | `FullTransactionIdFromAllowableAt(ReadNextFullTransactionId(), xid)` reproduced in-crate (special-xid epoch 0; epoch-- when xid>next.xid); next-full-xid via `varsup::read_next_full_transaction_id` |
| `TwoPhaseFilePath` | `two_phase_file_path` (full path) + `two_phase_file_basename` | MATCH | `pg_twophase/%08X%08X` epoch,xid; computed in-crate, fed to the fileio seam bodies |
| `save_state_data` | `SaveState::save_state_data` | MATCH | MAXALIGN pad, try_reserve OOM |
| `StartPrepare` | `start_prepare` | MATCH | header+gid+segments in C order; gathering inputs is caller's job; GXactLoadSubxactData seamed |
| `EndPrepare` | `end_prepare` | MATCH | END sentinel, total_len, origin fields, MaxAllocSize, crit/delayChkpt, insert/flush, MarkAsPrepared, SyncRep. MyLockedGxact re-set is a no-op (already set in guts) |
| `RegisterTwoPhaseRecord` | `register_two_phase_record` | MATCH | record header + optional payload |
| `ReadTwoPhaseFile` | `read_twophase_file` + `seam_read_twophase_file` | MATCH | size/alignment/magic/total_len/CRC validation in-crate; raw `OpenTransientFile+fstat+read+close` glue (`seam_read_twophase_file`) delegates the syscalls to fd.c; `missing_ok` probes `access_f_ok` (fd.c eagerly ERRORs in OpenTransientFile) then →None on ENOENT |
| `XlogReadTwoPhaseData` | `wal::xlog_read_twophase_data` (seam) | SEAMED | xlogreader is xlog.c-owned |
| `StandbyTransactionIdIsPrepared` | `standby_transaction_id_is_prepared` | MATCH | max==0→false, read missing_ok, hdr.xid==xid |
| `FinishPreparedTransaction` | `finish_prepared_transaction` | MATCH (after fix) | see Findings #2/#3; ordering critical-path preserved |
| `ProcessRecords` | `process_records` | MATCH | END break, callbacks[rmid], MAXALIGN advance; dispatches ported rmgr tables directly |
| `RemoveTwoPhaseFile` | `seam_remove_twophase_file` | MATCH | path-format in-crate; `unlink` delegated to `fd_seams::unlink_file`; missing→WARNING only when `give_warning` (errno!=ENOENT\|\|giveWarning), matching twophase.c:1713 |
| `RecreateTwoPhaseFile` | `recreate_two_phase_file` + `seam_recreate_twophase_file` | MATCH | CRC computed in-crate; raw `OpenTransientFile(O_CREAT\|O_TRUNC\|O_WRONLY)+write(content)+write(crc)+pg_fsync+close` glue delegated to fd.c |
| `CheckPointTwoPhase` | `check_point_two_phase` | MATCH | (valid\|inredo)&&!ondisk&&end_lsn<=horizon, recreate, ondisk=true; dir fsync via `seam_fsync_twophase_dir`→`fd::fsync_fname(TWOPHASE_DIR,true)` |
| `restoreTwoPhaseData` | `restore_two_phase_data` + `seam_scan_twophase_dir` | MATCH | dir scan via fd's `with_allocated_dir` (AllocateDir/ReadDir/FreeDir); 16-hex-char filter + `u64::from_str_radix(.,16)` in-crate; ProcessTwoPhaseBuffer + PrepareRedoAdd; no in-memory swap during restore |
| `PrescanPreparedTransactions` | `prescan_prepared_transactions` | MATCH (after fix) | see Finding #1 |
| `StandbyRecoverPreparedTransactions` | `standby_recover_prepared_transactions` | MATCH (after fix) | see Finding #1 |
| `RecoverPreparedTransactions` | `recover_prepared_transactions` | MATCH (after fix) | see Finding #1; recover-in-place, lock drop around ProcessRecords/StandbyReleaseLockTree |
| `ProcessTwoPhaseBuffer` | `process_two_phase_buffer` | MATCH | DidCommit/DidAbort or too-new→remove file/redo + None; hdr.xid check; subxid loop set_parent/set_next_xid |
| `RecordTransactionCommitPrepared` | `record_transaction_commit_prepared` | MATCH (after fix) | see Finding #2 |
| `RecordTransactionAbortPrepared` | `record_transaction_abort_prepared` | MATCH (after fix) | see Finding #2/#3 |
| `PrepareRedoAdd` | `prepare_redo_add` + `seam_twophase_file_exists` | MATCH | file-exists probe via `fd_seams::access_f_ok` (ENOENT→false, other errno→ERROR); ERROR/WARNING by reachedConsistency; freelist; replorigin_advance |
| `PrepareRedoRemove` | `prepare_redo_remove` | MATCH | find by xid, ondisk→remove file, RemoveGXact |
| `LookupGXact` | `lookup_gxact` | MATCH | valid&&gid match, read disk/WAL, origin_lsn+timestamp compare |
| `TwoPhaseTransactionGid` | `two_phase_transaction_gid` | MATCH | `pg_gid_%u_%u`; invalid-xid→PROTOCOL_VIOLATION |
| `IsTwoPhaseTransactionGidForSubid` | `is_two_phase_transaction_gid_for_subid` | MATCH | parse subid/xid + reconstruct-and-compare; outcome identical to C sscanf+strcmp on all probed edge cases (trailing garbage, sign, whitespace) |
| `LookupGXactBySubid` | `lookup_gxact_by_subid` | MATCH | valid && IsTwoPhase…ForSubid |

Helpers (`TransactionIdLatest`/`Follows`/`Precedes`/`FollowsOrEquals`, codecs, `BufferLayout`,
`decode_children`/`decode_rels`/`decode_gid`/`decode_stats_items`): MATCH — modular-arithmetic
comparisons and MAXALIGN segment offsets reproduce the C pointer walk.

## Seam audit

- Inward: `backend-access-transam-twophase-seams::standby_transaction_id_is_prepared` installed by
  `init_seams()` (only a `set()` call); `seams-init::init_all` calls `init_seams()`. OK.
- Outward seam crates (`syncrep-seams`, `crc32c-seams`, and the extended
  proc/procarray/lwlock/xlog/xact/commit-ts/subtrans/varsup/origin/predicate/inval/miscinit/stat/
  catalog-storage seams) are thin declarations: marshal + delegate, no branching/computation in any
  seam path. CRC32C is a pure `src/port/` primitive seamed pending its owner — acceptable (genuine
  foreign owner); validation/format logic stays in-crate.
- **`backend-access-transam-twophase-fileio-seams` — now INSTALLED in this crate's `init_seams()`**
  (set()-only). These 6 declarations (`read_twophase_file`, `recreate_twophase_file`,
  `remove_twophase_file`, `scan_twophase_dir`, `fsync_twophase_dir`, `twophase_file_exists`) back
  twophase.c's OWN static file helpers; they were declared but uninstalled while fd.c was unported.
  fd (`backend-storage-file-fd`) is now merged, so the bodies (`seam_*` in lib.rs) live in this
  crate: the path-format (`two_phase_file_path`/`adjust_to_full_transaction_id`), CRC framing, and
  16-hex scan filter stay in-crate, and only the raw syscalls (`OpenTransientFile`/
  `CloseTransientFile`/`pg_fsync`/`fsync_fname`/`with_allocated_dir`, and the `unlink_file`/
  `access_f_ok` fd-seams) are delegated to fd. This is the unit installing its own outward seam over
  a now-merged dependency — the install lives here, not in fd, because the path/format/scan logic is
  twophase.c-owned.
- No `set()` of another unit's inward seam; no uninstalled seam remaining; no body replaced by a
  "delegate elsewhere" — the 2PC algorithm, file codec, CRC framing, path format, scan filter, and
  record dispatch all live in-crate; only raw fd.c syscalls cross the seam.

## Findings (fixed this round)

1. **DIVERGES → fixed.** The three recovery scan loops (`prescan_prepared_transactions`,
   `standby_recover_prepared_transactions`, `recover_prepared_transactions`) re-processed the
   swap-removed entry instead of advancing the index. C iterates `for (i; i < numPrepXacts; i++)`
   with a plain `i++`: when `ProcessTwoPhaseBuffer`→`PrepareRedoRemove` swap-removes slot `i`, the
   entry swapped into `i` is left **unscanned** this pass. The port's `if num_prep_xacts() < before
   { continue }` reprocessed it, changing the set of entries processed (different subtrans/nextXid
   setup). Replaced with a faithful unconditional `i += 1` per entry.

2. **DIVERGES → fixed (record corruption).** `RecordTransactionCommitPrepared`/`AbortPrepared`
   passed `dropped_stats: Vec::new()` and (commit) `msgs: Vec::new()` with `nmsgs: ninvalmsgs`,
   plus `my_database_id: 0`/`my_database_table_space: 0`. C's `XactLogCommitRecord`/`AbortRecord`
   register the actual `droppedstats` items and `msgs` bytes (and set `HAS_DBINFO` with real
   dbId/tsId when `nmsgs>0`/logical) into the WAL record. The port wrote a structurally invalid
   record (count said N inval msgs but body carried none; dropped stats lost). Fixed by decoding the
   commit/abort stats items (`decode_stats_items`) and threading the real `invalmsgs` bytes, plus
   `MyDatabaseId`/`MyDatabaseTableSpace`/`XLogLogicalInfoActive` through `FinishContext`.

3. **DIVERGES → fixed (ambient global dropped).** The commit/abort `xactflags` was hardcoded to
   `XACT_FLAGS_ACQUIREDACCESSEXCLUSIVELOCK`, dropping C's `MyXactFlags |`. Threaded `MyXactFlags`
   through `FinishContext` (no-ambient-global rule).

## Residual notes (not findings)

- `FinishPreparedTransaction` removes the gxact eagerly under the post-callback lock even on a
  callback error, vs. C leaving it for `AtAbort_Twophase`; both converge to "gxact removed + error
  raised", and the post-error `AtEOXact_PgStat`/`RemoveTwoPhaseFile` are skipped identically.
- Commit-record `force_sync_commit`/`synchronous_commit` remain defaulted (false/0): these are GUC-
  derived optional `xinfo` feedback bits, not structural; consistent with the porter's documented
  default and not record-corrupting. Tracked as GUC-threading debt for the syncrep/guc owner.

## Re-audit (twophase-fileio install over merged fd)

The 6 `backend-access-transam-twophase-fileio-seams` declarations — previously declared-but-
uninstalled while `backend-storage-file-fd` was unported — are now installed in this crate's
`init_seams()` (`set()`-only), backed by in-crate `seam_*` bodies. Verified against twophase.c:

- `seam_read_twophase_file` ↔ `ReadTwoPhaseFile` raw I/O (twophase.c:1288): OpenTransientFile(
  O_RDONLY) + fstat (size only; the length-bound + magic/total_len/CRC checks stay in the caller
  `read_twophase_file`) + read + CloseTransientFile. `missing_ok` ENOENT→None is reproduced via an
  `access_f_ok` pre-probe (fd's OpenTransientFile raises the open ERROR eagerly, so the errno cannot
  be inspected post-open as C does — the probe is the faithful substitute and the only behavioural
  nuance vs. C).
- `seam_recreate_twophase_file` ↔ `RecreateTwoPhaseFile` store (twophase.c:1727): CRC is computed
  in-crate by `recreate_two_phase_file`; the body does OpenTransientFile(O_CREAT|O_TRUNC|O_WRONLY) +
  write(content) + write(crc, native-endian like the C `&statefile_crc`) + pg_fsync + close.
- `seam_remove_twophase_file` ↔ `RemoveTwoPhaseFile` (twophase.c:1707): `fd_seams::unlink_file`;
  WARNING only when `errno != ENOENT || give_warning`.
- `seam_scan_twophase_dir` ↔ `restoreTwoPhaseData` scan (twophase.c:1895): fd's `with_allocated_dir`
  (AllocateDir/ReadDir/FreeDir); the strlen==16 && strspn(HEX)==16 filter + strtou64(.,16) decode to
  the full-xid u64 stay in-crate.
- `seam_fsync_twophase_dir` ↔ `CheckPointTwoPhase`'s `fsync_fname(TWOPHASE_DIR, true)`
  (twophase.c:1866).
- `seam_twophase_file_exists` ↔ `PrepareRedoAdd`'s `access(path, F_OK)` (twophase.c:2509):
  `fd_seams::access_f_ok` → Ok→true, NoEnt→false, Other(errno)→ERROR.

Path format + epoch adjustment (`two_phase_file_path` / `adjust_to_full_transaction_id`,
twophase.c:938/945) reproduced in-crate, `ReadNextFullTransactionId` reached via
`varsup::read_next_full_transaction_id`. No new opacity introduced; `Mcx`/`PgResult` discipline
preserved (every body returns `PgResult`, file-access errno mapped via `errcode_for_file_access`).
`init_all()` already wired `backend_access_transam_twophase::init_seams()`; the recurrence_guard
test passes with the 6 new `set()` calls. Workspace `cargo check` and `cargo test` (1416 passed, 0
failed) green.

## Verdict

**PASS** — after the three fixes above and the twophase-fileio install. Every function is MATCH or
SEAMED per the seam rules; no MISSING/PARTIAL; the 6 fileio seams are now installed over merged fd
with the OWN path/CRC/scan logic in-crate; constants verified against headers; seams thin and
correctly installed. Crate builds clean; unit tests (7) pass; workspace gate green.
