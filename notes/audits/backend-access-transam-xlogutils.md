# Audit: backend-access-transam-xlogutils

- **Verdict: PASS**
- Date: 2026-06-12
- Model: Claude Opus 4.8 (1M context)
- C source: `src/backend/access/transam/xlogutils.c` (PostgreSQL 18.3)
- c2rust: `c2rust-runs/backend-access-transam-xlogutils/src/xlogutils.rs`
- Port: `crates/backend-access-transam-xlogutils/src/lib.rs`
- Branch: `port/backend-access-transam-xlogutils` (off main `d78910eb`)

This audit is independent of the port: every function was re-derived from the
C and cross-checked against the c2rust rendering. All 21 functions in
xlogutils.c are present and `MATCH`; seams + design conformance are clean.

## Function inventory (enumerated from C; cross-checked vs c2rust)

| # | C function (loc) | Port location | Verdict | Notes |
|---|---|---|---|---|
| 1 | `report_invalid_page` (85) | `report_invalid_page` | MATCH | `elog(elevel, ...)` → `elog`; "is uninitialized" / "does not exist" branch on `present`; path via `relpathperm`/`relpathbackend(loc, INVALID_PROC_NUMBER, forkno)` = C `GetRelationPath(...,-1,forkno)`. C uses `elog` (errmsg_internal); port `elog` is the same no-translate/no-errcode surface. |
| 2 | `log_invalid_page` (100) | `log_invalid_page` | MATCH | reachedConsistency block (WARNING report + `ignore_invalid_pages?WARNING:PANIC` "WAL contains references to invalid pages") → `message_level_is_interesting(DEBUG1)` report → HASH_ENTER keep-existing (`or_insert(present)`). `ignore_invalid_pages` read in-crate (owner is xlogutils.c). |
| 3 | `forget_invalid_pages` (165) | `forget_invalid_pages` | MATCH | filter `locator==&&forkno==&&blkno>=minblkno`; per-removed DEBUG2 "has been dropped"; missing-after-found → `elog(ERROR,"hash table corrupted")`. Two-phase collect-then-remove avoids iterator-invalidation; behavior identical (set membership, no order dependence). |
| 4 | `forget_invalid_pages_db` (196) | `forget_invalid_pages_db` | MATCH | filter `locator.dbOid==dbid`; same DEBUG2 + corruption-check structure. |
| 5 | `XLogHaveInvalidPages` (224) | `XLogHaveInvalidPages` | MATCH | `invalid_page_tab != NULL && hash_get_num_entries > 0` → `is_some_and(!is_empty())`. |
| 6 | `XLogCheckInvalidPages` (234) | `XLogCheckInvalidPages` | MATCH | NULL table → return; iterate WARNING-report all, `foundone`; if found `ignore_invalid_pages?WARNING:PANIC`; destroy table + null global (`take()` then drop). |
| 7 | `XLogReadBufferForRedo` (303) | `XLogReadBufferForRedo` | MATCH | delegates to Extended with RBM_NORMAL, get_cleanup_lock=false. Returns `(action, buf)`; C's `Buffer *buf` out-param becomes the tuple's 2nd element (matches main's inward `xlog_read_buffer_for_redo` seam shape). |
| 8 | `XLogInitBufferForRedo` (315) | `XLogInitBufferForRedo` | MATCH | Extended with RBM_ZERO_AND_LOCK, returns buf. |
| 9 | `XLogReadBufferForRedoExtended` (340) | `XLogReadBufferForRedoExtended` | MATCH | lsn=EndRecPtr; bogus block_id → PANIC; WILL_INIT vs zeromode cross-check (both PANIC sites); FPI-apply branch (read ZERO_AND_[CLEANUP_]LOCK, RestoreBlockImage→ERROR/INTERNAL on fail, PageSetLSN unless PageIsNew, MarkBufferDirty, FlushOneBuffer if INIT_FORKNUM → BLK_RESTORED); else branch (read mode, if valid lock unless zeromode, LSN compare → BLK_DONE/BLK_NEEDS_REDO; else BLK_NOTFOUND). Page ops cross by Buffer id (no Page pointer). |
| 10 | `XLogReadBufferExtended` (460) | `XLogReadBufferExtended` | MATCH (see seam note) | `Assert(blkno != P_NEW)`; recent-buffer fast path + smgropen/smgrcreate/smgrnblocks + ReadBufferWithoutRelcache-vs-ExtendBufferedRelTo are the bufmgr/smgr substrate behind `xlog_read_buffer_extended` (returns InvalidBuffer for the RBM_NORMAL/NO_LOG missing-page case). The xlogutils-owned decisions stay in-crate: RBM_NORMAL missing → `log_invalid_page(...,false)`; RBM_NORMAL PageIsNew → ReleaseBuffer + `log_invalid_page(...,true)` + InvalidBuffer. |
| 11 | `CreateFakeRelcacheEntry` (571) | `CreateFakeRelcacheEntry` | MATCH (SEAMED) | C palloc0s a `FakeRelCacheEntryData` (RelationData + FormData_pg_class), sets rd_locator/rd_backend=INVALID_PROC_NUMBER/relpersistence=PERMANENT/name=relNumber/lockRelId/non-pinned smgropen. This is relcache substrate (RelationData allocation + SMgrRelation); routed to `relcache-seams::create_fake_relcache_entry(mcx, rlocator)`. Body is not "logic moved elsewhere" — it is allocation of an owner's type + owner's smgropen, correctly the owner's. |
| 12 | `FreeFakeRelcacheEntry` (618) | `FreeFakeRelcacheEntry` | MATCH (SEAMED) | C `pfree(fakerel)`; routed to `relcache-seams::free_fake_relcache_entry` (takes ownership). |
| 13 | `XLogDropRelation` (630) | `XLogDropRelation` | MATCH | `forget_invalid_pages(rlocator, forknum, 0)`. |
| 14 | `XLogDropDatabase` (641) | `XLogDropDatabase` | MATCH | `smgrdestroyall()` (smgr-seams) then `forget_invalid_pages_db(dbid)`. |
| 15 | `XLogTruncateRelation` (660) | `XLogTruncateRelation` | MATCH | `forget_invalid_pages(rlocator, forkNum, nblocks)`. |
| 16 | `XLogReadDetermineTimeline` (707) | `XLogReadDetermineTimeline` | MATCH | lastReadPage = ws_segno*ws_segsize + segoff (u64 promotion of C int ws_segsize); three early-return guards incl. `Min(wantLength, XLOG_BLCKSZ-1)` and the historical-segment division check; final block: tliOfPointInHistory (sets currTLI), tliSwitchPoint (sets currTLIValidUntil + nextTLI), DEBUG3 "switched to timeline %u valid until %X/%X". readTimeLineHistory + list_free_deep are inside the timeline-seam owner. |
| 17 | `wal_segment_open` (806) | `wal_segment_open` | MATCH | XLogFilePath(tli, segno, ws_segsize); BasicOpenFile O_RDONLY|PG_BINARY; fd>=0 → set ws_file; errno==ENOENT → "already been removed", else "could not open file ...: %m"; both errcode_for_file_access + saved errno. |
| 18 | `wal_segment_close` (831) | `wal_segment_close` | MATCH | close + ws_file=-1, via `reader_close_ws_file`. |
| 19 | `read_local_xlog_page` (845) | `read_local_xlog_page` | MATCH | guts(..., wait_for_wal=true). |
| 20 | `read_local_xlog_page_no_wait` (857) | `read_local_xlog_page_no_wait` | MATCH | guts(..., wait_for_wal=false). |
| 21 | `read_local_xlog_page_guts` (869) | `read_local_xlog_page_guts` | MATCH | loc=targetPagePtr+reqLen; loop: !RecoveryInProgress → GetFlushRecPtr(&currTLI), else GetXLogReplayRecPtr(&currTLI); DetermineTimeline; if currTLI matches: loc<=read_upto break / !wait_for_wal set end_of_wal break / CHECK_FOR_INTERRUPTS + pg_usleep(1000); else historical: read_upto=currTLIValidUntil, tli=currTLI, break. count: XLOG_BLCKSZ / -1 / read_upto-targetPagePtr. WALRead → on fail WALReadRaiseError. Returns count. `targetRecPtr` unused (also unused in C path). |
| 22 | `WALReadRaiseError` (1011) | `WALReadRaiseError` | MATCH | XLogFileName(ws_tli, ws_segno, wal_segment_size); wre_read<0 → file-access ERROR + saved errno + "...: %m"; wre_read==0 → ERRCODE_DATA_CORRUPTED + "read %d of %d"; else (partial) no-op Ok. |

(22 rows: 21 functions + the static `read_local_xlog_page_guts` helper enumerated
separately for clarity.)

### Spot-recheck of MATCH verdicts (auditor self-check)

- #2 `log_invalid_page`: re-derived branch order against C lines 116-160 and
  c2rust 1586+ — reachedConsistency → DEBUG1 → HASH_ENTER keep-existing.
  Confirmed `or_insert(present)` reproduces "if found, leave present as it was".
- #16 `XLogReadDetermineTimeline`: re-derived all three guards + endOfSegment
  arithmetic against C 713-797 and c2rust 2361+. `((wantPage/seg)+1)*seg-1`
  and the `/ws_segsize` comparisons match.
- #21 `read_local_xlog_page_guts`: re-derived count branches and the
  GetFlush/GetReplay split against c2rust 2565+. The TLI out-param is preserved
  via the two-element returns of `get_flush_rec_ptr` / `get_xlog_replay_rec_ptr_tli`.
- #22 `WALReadRaiseError`: re-derived the two ereport sites and the implicit
  partial-read no-op against C 1018-1033.

## Seam audit

**Owned seam crates (by C-source coverage):** `backend-access-transam-xlogutils-seams`
is the only `X-seams` mapping to this unit's sole C file (`xlogutils.c`).

All 7 of its declarations are installed by the crate's `init_seams()`
(set()-only), which `seams-init::init_all()` invokes:

- `standby_state`, `set_standby_state` — `standbyState` global (xlogutils.c).
- `in_recovery`, `set_in_recovery` — `InRecovery` global (xlogutils.c).
- `ignore_invalid_pages`, `set_ignore_invalid_pages` — GUC global (xlogutils.c).
- `xlog_read_buffer_for_redo` — the inward redo-fetcher seam main already
  declared; installed as a thin delegate to `XLogReadBufferForRedo`.

No uninstalled owned seam; no `set()` outside the owner; `init_seams()` is
nothing but `set()` calls.

**Outward seam calls** — each is a real cross-crate dependency (cycle or
unported owner) and is thin marshal + delegate, no branching/computation in the
seam path:

- `common-relpath-seams::relpathbackend` (new crate; relpath.c unported).
- `xlog-seams::{recovery_in_progress, get_flush_rec_ptr, wal_read,
  wal_segment_size}` — get_flush_rec_ptr (owner xlog.c) and wal_read added;
  the pre-existing settled `wal_segment_size() -> i32` and
  `recovery_in_progress()` were reused unchanged (not re-forked).
- `xlogrecovery-seams::{reached_consistency, get_xlog_replay_rec_ptr_tli}` —
  the TLI-returning replay-ptr added to the proper owner (xlogrecovery.c);
  main's settled NULL-TLI `get_xlog_replay_rec_ptr` on xlog-seams left untouched.
- `timeline-seams::{tli_of_point_in_history, tli_switch_point}` (+ TimelineSwitch)
  — readTimeLineHistory + list_free_deep stay inside the timeline owner.
- `xlogreader-seams` block-tag/flags/image accessors + reader seg/TLI/readLen/
  segoff/ws_file/private accessors (+ XLogBlockTag) — the trimmed shared
  `XLogReaderState` (main) exposes only ReadRecPtr/EndRecPtr/record, so the
  seg/TLI/private fields are correctly reached through the reader owner.
- `bufmgr-seams` redo-buffer primitives (`xlog_read_buffer_extended`,
  `page_is_new`/`page_set_lsn`/`page_get_lsn`, `flush_one_buffer`,
  `lock_buffer_exclusive`/`lock_buffer_for_cleanup`); reused existing
  infallible `mark_buffer_dirty`/`release_buffer`.
- `smgr-seams::smgrdestroyall`, `fd-seams::basic_open_file`,
  `relcache-seams::{create,free}_fake_relcache_entry`.
- direct: `postgres-seams::check_for_interrupts`, `pgsleep-seams::pg_usleep`.

Note on #10: the C `XLogReadBufferExtended` body's smgr/buffer operations are
condensed into one `xlog_read_buffer_extended` bufmgr seam. This is bufmgr/smgr
substrate (smgropen/smgrcreate/smgrnblocks/ReadBufferWithoutRelcache/
ExtendBufferedRelTo/ReadRecentBuffer), genuinely the bufmgr+smgr owners' work,
and avoids inventing an `SMgrRelation` opacity ahead of the unported owner. The
xlogutils-owned decisions — the RBM_NORMAL/NO_LOG `log_invalid_page` calls and
the PageIsNew check — remain in-crate. This is `SEAMED` substrate, not
`MISSING` logic.

## Design conformance

- **Invented opacity (types.md 6-7):** none. No `type X = Oid/usize/uN` stand-in;
  `ReadBufferMode` is the real C enum (added to types-storage), `XLogBlockTag`/
  `TimelineSwitch`/`WalReadErrorInfo` are real struct shapes; `RelFileLocator`/
  `RelationData`/`XLogReaderState` are real types. No `&[u8]` blob for a typed
  thing.
- **Allocating fns/seams take Mcx + return PgResult:** `CreateFakeRelcacheEntry`
  takes `mcx::Mcx` + `PgResult`. `relpathbackend`/`reader_errormsg_buf`/`wal_read`
  return owned `String`/`Vec` used only for error-message text / the WAL byte
  copy at a return-or-Err site — the C allocates in CurrentMemoryContext there;
  acceptable per the design-debt carve-out for message/return-site allocation.
- **Shared statics for per-backend globals:** none. `InRecovery`,
  `standbyState`, `ignore_invalid_pages`, `invalid_page_tab` are all
  `thread_local` (C file-statics). No `Atomic`/`Mutex`/`OnceCell`/`lazy_static`
  in the crate (the test file's atomics are test scaffolding only).
- **Ambient-global seams:** the xlogutils-owned globals are exposed via
  read+write seam pairs on the owner's seam crate (parameter for writes), not
  zero-arg ambient getters fabricated for foreign globals. The reused
  `reached_consistency`/`reader_*` getters belong to their owners.
- **Locks held across `?`:** none. Buffer locks acquired in
  XLogReadBufferForRedoExtended are released by the redo caller (as in C — the
  function deliberately returns a locked buffer); no lock is acquired-and-leaked
  within a single function across a fallible call.
- **Registry-shaped side tables:** the invalid-page table is the C
  `invalid_page_tab` HTAB modelled as a per-backend BTreeMap (keyed lookup +
  filtered removal + unordered dump only; order not observable). Not a global
  registry.
- **Unledgered divergence markers:** none. No "for now"/"hack"/"TODO"/"FIXME"
  added by the port; the C's own algorithm comments are retained.
- `unwrap`/`unreachable!`: the single `unreachable!("elog(PANIC) returned Ok")`
  follows an `elog(PANIC)` that does not return in C — a correct
  control-flow-never-reached marker, not a stand-in for an error path. All
  FATAL/PANIC/ERROR C sites are `Err(PgError)` or `elog(...)?`.

## Gate

- `cargo check --workspace`: clean (only pre-existing unrelated warnings in
  backend-catalog-pg-depend / backend-access-common-printtup).
- `cargo test --workspace`: clean. Crate's 13 unit tests pass
  (`--test-threads=1`).
