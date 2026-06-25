# Audit: backend-utils-cache-relmapper

Independent audit of `crates/backend-utils-cache-relmapper` against
`src/backend/utils/cache/relmapper.c` (+ `src/include/utils/relmapper.h`) and
the c2rust rendering `c2rust-runs/backend-utils-cache-relmapper/src/relmapper.rs`.

`c_sources`: `relmapper.c`, `relmapper.h`. Note `relmap_desc`/`relmap_identify`
live in `relmapdesc.c` (a separate unit, already owned by
`backend-access-rmgrdesc-small`) and are correctly **not** in this crate.

## Constants (verified against C headers, not memory)

| Constant | C value | Port | Verdict |
|---|---|---|---|
| `RELMAPPER_FILEMAGIC` | `0x592717` (relmapper.c:73) | `0x0059_2717` | MATCH |
| `MAX_MAPPINGS` | `64` (relmapper.c:81) | `64` | MATCH |
| `XLOG_RELMAP_UPDATE` | `0x00` (relmapper.h:25) | `0x00` | MATCH |
| `MinSizeOfRelmapUpdate` | `offsetof(xl_relmap_update,data)` = 12 | header encode is 12 bytes (dbid@0,tsid@4,nbytes@8) | MATCH |
| `sizeof(RelMapFile)` | 4+4+64*8+4 = 524 | `SIZEOF_RELMAPFILE` = 524 (test) | MATCH |
| `offsetof(RelMapFile,crc)` | 520 | `OFFSETOF_RELMAPFILE_CRC` = 520 (test) | MATCH |
| `GLOBALTABLESPACE_OID` | `1664` (pg_tablespace.dat) | `1664` | MATCH |
| `RM_RELMAP_ID` | rmgrlist.h entry 7 | `RM_RELMAP_ID = 7` (types-wal) | MATCH |
| `WAIT_EVENT_RELATION_MAP_{READ,REPLACE,WRITE}` | PG_WAIT_IO+{40,41,42} = 167772200/201/202 | same (types-pgstat) | MATCH |
| `ENOSPC` substitution | `errno = ENOSPC` on zero-errno write | `ENOSPC = 28` | MATCH |

## Function inventory and verdicts

| C function (relmapper.c) | Port (lib.rs) | Verdict | Notes |
|---|---|---|---|
| `RelationMapOidToFilenumber` (165) | `RelationMapOidToFilenumber` | MATCH | active-then-main, shared/local split; `lookup_oid` mirrors the inner loop; returns `InvalidRelFileNumber`. |
| `RelationMapFilenumberToOid` (218) | `RelationMapFilenumberToOid` | MATCH | reverse lookups in same order; `InvalidOid` default. |
| `RelationMapOidToFilenumberForDatabase` (265) | same | MATCH | `read_relmap_file(...,false,ERROR)` then linear scan; `InvalidRelFileNumber` default. |
| `RelationMapCopy` (292) | same | MATCH | read src, LWLock EXCLUSIVE, `write_relmap_file(true,false,false,...)`, unlock. send_sinval=false, preserve_files=false as in C. |
| `RelationMapUpdateMap` (325) | same | MATCH | bootstrap→permanent; else nestlevel>1 / parallel errors (same messages), immediate→active / pending; `apply_map_update(...,true)`. |
| `apply_map_update` (383) | same | MATCH | replace-in-place; `!add_okay`→error; `>= MAX_MAPPINGS`→"ran out of space"; append + bump count. |
| `merge_map_updates` (416) | same | MATCH | bulk apply loop. |
| `RelationMapRemoveMapping` (438) | same | MATCH | collapse `mappings[last]` into the hole, decrement; not-found→error msg with oid. |
| `RelationMapInvalidate` (468) | same | MATCH | reload only if magic==FILEMAGIC, per shared/local. |
| `RelationMapInvalidateAll` (490) | same | MATCH | reload both if valid. |
| `AtCCI_RelationMap` (504) | same | MATCH | merge pending→active for shared then local, zero pending counts. (C void; merge can elog ERROR → port returns `PgResult`, seam reconciled.) |
| `AtEOXact_RelationMap` (541) | same | MATCH | commit&!parallel: assert no pending, perform_relmap_update for shared/local then zero; else drop all four. Asserts mirrored as debug_assert. |
| `AtPrepare_RelationMap` (588) | same | MATCH | any of the four nonzero → `ereport(ERROR, FEATURE_NOT_SUPPORTED)`, same message/SQLSTATE. |
| `CheckPointRelationMap` (611) | same | MATCH | lock SHARED (exclusive=false) then unlock. |
| `RelationMapFinishBootstrap` (625) | same | MATCH | assert bootstrap + no pending; lock EXCLUSIVE; write shared (global/InvalidOid/GLOBALTABLESPACE) and local (DatabasePath/MyDatabaseId/MyDatabaseTableSpace), write_wal=send_sinval=preserve=false; unlock. Lock released on every path. |
| `RelationMapInitialize` (651) | same | MATCH | zero magic + all counts. |
| `RelationMapInitializePhase2` (671) | same | MATCH | bootstrap→noop; else load shared. |
| `RelationMapInitializePhase3` (692) | same | MATCH | bootstrap→noop; else load local. |
| `EstimateRelationMapSpace` (713) | same | MATCH | `2 * sizeof(RelMapFile)`. |
| `SerializeRelationMap` (724) | same | MATCH | copies active shared+local into the owned `SerializedActiveRelMaps` (C writes into the DSM buffer; the DSM-marshal layer places the owned value). |
| `RestoreRelationMap` (741) | same | MATCH | any of four nonzero→"parallel worker has existing mappings"; else copy active shared+local in. |
| `load_relmap_file` (765) | same | MATCH | shared→"global"; local→DatabasePath; FATAL elevel. |
| `read_relmap_file` (784) | same | MATCH | lock (unless held); open/read/close as fd seam unit; the open/read/close-failure, short-read (DATA_CORRUPTED), magic/num_mappings, and CRC-mismatch reports all reproduced with C message text/SQLSTATE; lock released before validation as in C. |
| `write_relmap_file` (889) | same | MATCH | assert lock-held-exclusive; magic+bogus-count check; CRC fill; temp write (fd seam) with errno→ereport incl. ENOSPC fallback; WAL leg in START/END_CRIT_SECTION (xl_relmap_update header + image, XLogInsert(RM_RELMAP_ID,XLOG_RELMAP_UPDATE), XLogFlush); durable_rename; conditional sinval; preserve_files loop building RelFileLocator{tsid,dbid,filenumber}. Ordering preserved. |
| `perform_relmap_update` (1039) | same | MATCH | lock EXCLUSIVE; reload target (lock_held=true); copy to newmap; merge with `allowSystemTableMods`; write_relmap_file(true,true,true,...); copy back; unlock on every path. |
| `relmap_redo` (1096) | `relmap_redo` | MATCH | `info = XLogRecGetInfo & ~XLR_INFO_MASK`; assert no block refs; XLOG_RELMAP_UPDATE→decode xl_relmap_update header, nbytes!=sizeof→PANIC, decode image, GetDatabasePath, lock EXCLUSIVE, write_relmap_file(false,true,false,...), unlock, dbpath dropped (C pfree); else→PANIC "unknown op code". |

c2rust cross-check: the c2rust unit contains exactly these functions plus the
patched raw-pointer accessors `relmap_mapping`/`relmap_mapping_mut` (a c2rust
codegen artifact to avoid `panic_bounds_check`, not C functions) — no C function
is missing from the port.

## Byte-image fidelity

`encode_relmapfile`/`decode_relmapfile` produce the exact native-endian
`sizeof(RelMapFile)` image C `read()`/`write()`/`memcpy`/CRC use (all fields are
4-byte ints; `RelMapping` is `{u32,u32}`, no padding). `relmapfile_crc` computes
`INIT/COMP/FIN_CRC32C` over the leading `offsetof(crc)` bytes. Round-trip and CRC
tests pass. The image handed to the temp-write seam and to the WAL register call
is the same bytes (matching C's single `newmap` source). MATCH.

## Seam audit

Owned seam crate: `backend-utils-cache-relmapper-seams`. All 5 declarations
(`relation_map_filenumber_to_oid`, `relmap_redo`, `at_cci_relation_map`,
`at_eoxact_relation_map`, `at_prepare_relation_map`) are installed by
`init_seams()`, which contains only `set()` calls. `seams-init::init_all()`
calls `backend_utils_cache_relmapper::init_seams()`. No uninstalled seam, no
`set()` outside the owner.

Seam-signature reconciliation: `at_cci_relation_map` was declared `-> ()` but
the owned `AtCCI_RelationMap` can `elog(ERROR)` via the merge; per AGENTS.md
"seam declarations are not frozen", the declaration was changed to
`-> PgResult<()>` and the single existing caller (`xact` `AtCCI_LocalCache`)
updated to `?`. No adapter/second seam.

Outward calls — each is a genuinely-external primitive owned by another unit
(direct deps would not all cycle, but the established per-owner scheme routes
these; all are thin marshal+delegate, panic until owner lands):

- `lwlock-seams`: `lock_relation_mapping` / `unlock_relation_mapping` /
  `relation_mapping_lock_held_by_me_exclusive` (RelationMappingLock, a named
  main LWLock; mirrors the existing `TwoPhaseStateLock` named-lock pattern).
- `fd-seams`: `relmap_read_file` / `relmap_write_temp` / `relmap_durable_rename`
  (+ `RelmapReadOutcome`/`RelmapWriteOutcome` outcome types). The fd (held
  resource) stays inside the owner; only the validated-bytes/errno outcome
  crosses, so the CRC/magic/error-message algorithm stays in-crate.
- `port-crc32c-seams`: `comp_crc32c` (COMP_CRC32C primitive).
- `xloginsert-seams`: `xlog_insert` (begin+register+insert) — the WAL record is
  built in-crate (`encode_xl_relmap_update` + image fragments).
- `xlog-seams`: `xlog_flush`.
- `miscinit-seams`: `start_crit_section` / `end_crit_section` /
  `is_bootstrap_processing_mode`.
- `inval-seams`: `cache_invalidate_relmap`.
- `catalog-storage-seams`: `relation_preserve_storage`.
- `catalog-catalog-seams`: `get_database_path`.
- `xact-seams`: `get_current_transaction_nest_level` / `is_in_parallel_mode`.
- `init-small-seams`: `database_path` / `my_database_id` /
  `my_database_table_space`.
- `guc-seams`: `allow_system_table_mods`.
- `backend-utils-error-seams`: `sqlstate_for_file_access` (new; the
  `errcode_for_file_access` errno→SQLSTATE switch stays single-sourced in the
  elog owner — relmapper does not duplicate the table).

No branching/computation lives in any seam path; all algorithm logic is in the
crate body.

## Design conformance

- Per-backend state (`shared_map`/`local_map`/`active_*`/`pending_*`): C
  file-statics with no shmem → `thread_local!` (not a shared static). PASS.
- No invented opacity: `RelMapFile`/`RelMapping` are the real C structs;
  `RelFileLocator` is the shared `types_storage` type; outcome enums are typed,
  not `&[u8]` blobs (the one `&[u8]` is the genuine serialized map image C writes
  verbatim). PASS.
- Allocating seams: `relmap_read_file` returns owned bytes only for a transient
  validation buffer (not allocated in a caller context); `database_path` /
  `get_database_path` return owned `String` for transiently-used paths the
  caller drops (C `pfree`), `-> PgResult` to carry OOM. Filenames are built with
  `format!` because the C counterpart is non-allocating `snprintf` into a
  `char[MAXPGPATH]` stack buffer. PASS.
- No locks held across `?` without release: every `read_relmap_file` /
  `write_relmap_file` / `perform_relmap_update` / `RelationMapCopy` /
  `RelationMapFinishBootstrap` / `relmap_redo` path releases the lock on both the
  success and the error return (the lock seams are explicit acquire/release,
  tracked in DESIGN_DEBT alongside TwoPhaseStateLock until lwlock hands back a
  guard for named locks). PASS.
- FATAL/PANIC error paths are `Err(PgError)` at that level (`elog_panic`,
  `read_relmap_file` FATAL elevel). PASS.
- No registry-shaped side tables, no unledgered divergence markers. PASS.

## Verdict: PASS

Every C function is MATCH; all owned seams installed; zero seam findings; design
rules satisfied. 16 unit tests pass; `cargo check -p backend-utils-cache-relmapper
-p backend-utils-cache-relmapper-seams -p seams-init` is clean.
