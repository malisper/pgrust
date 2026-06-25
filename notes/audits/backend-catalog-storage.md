# Audit: backend-catalog-storage

Source: `src/backend/catalog/storage.c` (+ `md.c` `DropRelationFiles`,
`relcache.c` `RelationSetNewRelfilenumber` legs). Independent function-by-function
re-derivation from the C; not from the port's comments.

## Function inventory + verdicts

| C function (storage.c) | Port | Verdict |
|---|---|---|
| AddPendingSync (:86, static) | add_pending_sync | MATCH |
| RelationCreateStorage (:122) | RelationCreateStorage | MATCH |
| log_smgrcreate (:187) | log_smgrcreate | MATCH (byte layout verified) |
| RelationDropStorage (:207) | relation_drop_storage | MATCH (re-modeled: rlocator+backend) |
| RelationPreserveStorage (:252) | RelationPreserveStorage | MATCH |
| RelationTruncate (:289) | RelationTruncate | MATCH (smgr cache reset dropped — justified) |
| RelationPreTruncate (:450) | RelationPreTruncate | MATCH |
| RelationCopyStorage (:478) | RelationCopyStorage | MATCH |
| RelFileLocatorSkippingWAL (:573) | rel_file_locator_skipping_wal | MATCH |
| EstimatePendingSyncsSpace (:587) | EstimatePendingSyncsSpace | MATCH |
| SerializePendingSyncs (:600) | SerializePendingSyncs | MATCH (slice interface) |
| RestorePendingSyncs (:651) | RestorePendingSyncs | MATCH |
| smgrDoPendingDeletes (:673) | smgr_do_pending_deletes | MATCH |
| smgrDoPendingSyncs (:741) | smgr_do_pending_syncs | MATCH |
| smgrGetPendingDeletes (:893) | smgr_get_pending_deletes | MATCH |
| PostPrepare_smgr (:934) | post_prepare_smgr | MATCH |
| AtSubCommit_smgr (:955) | at_subcommit_smgr | MATCH |
| AtSubAbort_smgr (:975) | at_subabort_smgr | MATCH |
| smgr_redo (:981) | smgr_redo | MATCH |
| DropRelationFiles (md.c:1601, isRedo=false) | drop_relation_files | MATCH |

Wrapper seams (composed from storage.c + relcache.c legs): smgr_unlink_relation_now,
relation_create_storage_main_fork, relation_set_new_filelocator_storage,
update_pg_class_relfilenumber — all MATCH.

## Key verifications

- Constants verified against C headers: XLOG_SMGR_CREATE=0x10, XLOG_SMGR_TRUNCATE=0x20,
  SMGR_TRUNCATE_HEAP/VM/FSM=0x1/0x2/0x4, ALL=0x7, RM_SMGR_ID=2, XLR_SPECIAL_REL_UPDATE=0x01,
  XLR_INFO_MASK=0x0F, RELPERSISTENCE p/u/t, RELKIND_SEQUENCE='S', RelationRelationId=1259.
- xl_smgr_create (16 bytes: spc@0,db@4,rel@8,fork@12) and xl_smgr_truncate
  (20 bytes: blkno@0,spc@4,db@8,rel@12,flags@16) byte serialization matches both
  the C struct layout and the types-wal decoders.
- smgrDoPendingDeletes nest-level drain: ALL entries >= nestLevel removed; only
  atCommit==isCommit unlinked. Two-pass (collect to_delete, retain nest<level) is faithful.
- smgrDoPendingSyncs wal_skip comparison, is_truncated handling, INIT_FORKNUM assert,
  at-commit-delete removal, 0..=MAX_FORKNUM iteration all match.
- RelationTruncate DELAY_CHKPT_START|COMPLETE bracket, crit section, WAL-before-truncate.

## Accepted deviations (not findings)

1. RelationTruncate drops the C `smgr_targblock`/`smgr_cached_nblocks[*]` reset
   (C :304-307): this repo's smgr is value-keyed (`smgropen` returns a fresh
   `SMgrRelationData` snapshot keyed on `RelFileLocatorBackend`; there is no
   persistent per-handle cache to invalidate).
2. `AssertPendingSyncs_RelationCache()` (C :762) omitted: entirely inside
   `#ifdef USE_ASSERT_CHECKING`, no production behavior.
3. `update_pg_class_relfilenumber` folds the SearchSysCacheLockedCopy1/UnlockTuple/
   heap_freetuple lifecycle into the owned-copy + syscache/indexing owner seams
   (the project value-model; same pattern backend-commands-cluster uses). Calls the
   uninstalled `search_syscache_copy_pg_class` + `catalog_tuple_update_pg_class`
   seams (correct owners: syscache / indexing #304) — latent-panic until ported.

## Seams / wiring

- Owned inward seams (backend-catalog-storage-seams): all 15 installed by
  `init_seams()`, which is wired into `seams-init::init_all()`.
- Added seams to neighbor owners: proc `set_delay_chkpt_complete` (installed),
  bulkwrite `smgr_bulk_start_smgr` (installed), pgstat
  `pgstat_prepare_report_checksum_failure` / `pgstat_report_checksum_failures_in_db`
  (owner unported — declared, latent-panic, correct per discipline).
- All outward calls are thin marshal+delegate; no logic in seam paths.

## Verdict: PASS
