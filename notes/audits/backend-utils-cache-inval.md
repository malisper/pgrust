# Audit: backend-utils-cache-inval

- **Verdict: PASS**
- Date: 2026-06-12
- Model: Opus 4.8 (claude-opus-4-8[1m])
- C sources: `src/backend/utils/cache/inval.c` (+ `storage/sinval.h`, `utils/inval.h`)
- c2rust: `c2rust-runs/backend-utils-cache-inval/src/inval.rs`
- Port: `crates/backend-utils-cache-inval/src/{lib,msgs,registration,local_list,cache_invalidate,at_eoxact}.rs`

This is an ASSEMBLE-stage audit of the five family bodies merged onto the
decomposition scaffold. Two findings were fixed in this round (see below) and
the affected functions re-derived from scratch before signing off.

## Function inventory (49 C function definitions in inval.c)

### Subgroup / group helpers — `msgs.rs`
| C func (line) | Port | Verdict |
|---|---|---|
| AddInvalidationMessage (320) | add_invalidation_message | MATCH — PgVec push; C maxmsgs/repalloc growth = Vec capacity; first-alloc Assert = `nextindex==len` debug_assert |
| AppendInvalidationMessageSubGroup (360) | append_invalidation_message_sub_group | MATCH — adjacency assert + SetSubGroupToFollow |
| ProcessMessageSubGroup / Multi (macros 384/402) | process_message_sub_group / process_invalidation_messages_multi | MATCH — Multi only fires func when n>0 |
| AddCatcacheInvalidationMessage (425) | add_catcache_invalidation_message | MATCH — `id < CHAR_MAX` debug_assert; VALGRIND no-op (no union padding) |
| AddCatalogInvalidationMessage (453) | add_catalog_invalidation_message | MATCH |
| AddRelcacheInvalidationMessage (471) | add_relcache_invalidation_message | MATCH — dedup scan (relId match or InvalidOid); id check subsumed by enum variant |
| AddRelsyncInvalidationMessage (505) | add_relsync_invalidation_message | MATCH |
| AddSnapshotInvalidationMessage (533) | add_snapshot_invalidation_message | MATCH — dedup on relId only |
| AppendInvalidationMessages (560) | append_invalidation_messages | MATCH |
| ProcessInvalidationMessages (574) | process_invalidation_messages_group | MATCH — catcache first |
| ProcessInvalidationMessagesMulti (586) | process_invalidation_messages_multi | MATCH |

### Register / Prepare — `registration.rs`
| C func (line) | Port | Verdict |
|---|---|---|
| RegisterCatcacheInvalidation (604) | register_catcache_invalidation | MATCH |
| RegisterCatalogInvalidation (621) | register_catalog_invalidation | MATCH |
| RegisterRelcacheInvalidation (632) | register_relcache_invalidation | MATCH — GetCurrentCommandId(true) via xact seam; init-file flag via RelationIdIsInInitFile seam; `relId==InvalidOid || ...` |
| RegisterRelsyncInvalidation (660) | register_relsync_invalidation | MATCH |
| RegisterSnapshotInvalidation (672) | register_snapshot_invalidation | MATCH |
| PrepareInvalidationState (682) | prepare_invalidation_state | MATCH — parent chain modelled as stack; subxact-unprocessed-msgs ERROR; SetGroupToFollow plumbing; first-txn array reset |
| PrepareInplaceInvalidationState (752) | prepare_inplace_invalidation_state | MATCH |

### Local list / accept — `local_list.rs`
| C func (line) | Port | Verdict |
|---|---|---|
| InvalidateSystemCachesExtended (785) | InvalidateSystemCachesExtended | MATCH — snapshot/reset/relcache then 3 callback loops (snapshotted to avoid re-entrant borrow) |
| LocalExecuteInvalidationMessage (823) | LocalExecuteInvalidationMessage | MATCH — full id dispatch; smgr backend = `(hi<<16)|lo`; relmap/snapshot dbId arms; the C `elog(FATAL, unrecognized id)` is structurally unreachable (exhaustive enum) |
| InvalidateSystemCaches (916) | InvalidateSystemCaches | MATCH |
| AcceptInvalidationMessages (930) | AcceptInvalidationMessages | MATCH — ReceiveSharedInvalidMessages seam with error capture; DISCARD_CACHES recursion-depth guard mirrored via thread_local |

### CacheInvalidate* + callbacks — `cache_invalidate.rs`
| C func (line) | Port | Verdict |
|---|---|---|
| CacheInvalidateHeapTupleCommon (1436) | cache_invalidate_heap_tuple_common | MATCH — bootstrap/IsCatalog/IsToast short-circuits; RelationInvalidatesSnapshotsOnly branch; PrepareToInvalidateCacheTuple replayed through RegisterCatcacheInvalidation; pg_class/attribute/index/constraint relcache-definer dispatch incl. CONSTRAINT_FOREIGN+OidIsValid(conrelid) else-return |
| CacheInvalidateHeapTuple (1571) | CacheInvalidateHeapTuple | MATCH |
| CacheInvalidateHeapTupleInplace (1593) | CacheInvalidateHeapTupleInplace | MATCH (newtuple=None, inplace=true) |
| CacheInvalidateCatalog (1612) | CacheInvalidateCatalog | MATCH |
| CacheInvalidateRelcache (1635) | CacheInvalidateRelcache | MATCH — `rd_rel->relisshared` resolved via IsSharedRelation(relid) (trimmed RelationData lacks rd_rel; behaviorally identical) |
| CacheInvalidateRelcacheAll (1658) | CacheInvalidateRelcacheAll | MATCH |
| CacheInvalidateRelcacheByTuple (1669) | CacheInvalidateRelcacheByTuple | MATCH |
| CacheInvalidateRelcacheByRelid (1691) | CacheInvalidateRelcacheByRelid | MATCH — RELOID lookup via seam; not-found ERROR "cache lookup failed for relation %u" |
| CacheInvalidateRelSync (1712) | CacheInvalidateRelSync | MATCH |
| CacheInvalidateRelSyncAll (1724) | CacheInvalidateRelSyncAll | MATCH |
| CacheInvalidateSmgr (1755) | CacheInvalidateSmgr | MATCH — MAX_BACKENDS_BITS<=23 static assert; hi/lo split; sent immediately |
| CacheInvalidateRelmap (1789) | CacheInvalidateRelmap | MATCH |
| CacheRegisterSyscacheCallback (1816) | CacheRegisterSyscacheCallback | MATCH (after fix) — link-chain head/append; FATAL on bad id / out-of-slots |
| CacheRegisterRelcacheCallback (1858) | CacheRegisterRelcacheCallback | MATCH (after fix) — FATAL on out-of-slots |
| CacheRegisterRelSyncCallback (1879) | CacheRegisterRelSyncCallback | MATCH (after fix) — FATAL on out-of-slots |
| CallSyscacheCallbacks (1898) | CallSyscacheCallbacks | MATCH — ERROR on bad id; link walk `i = link-1`; callbacks snapshotted before invoke |
| CallRelSyncCallbacks (1920) | CallRelSyncCallbacks | MATCH |

### End-of-xact / inplace / 2PC — `at_eoxact.rs`
| C func (line) | Port | Verdict |
|---|---|---|
| PostPrepare_Inval (993) | PostPrepare_Inval | MATCH |
| xactGetCommittedInvalidationMessages (1011) | xactGetCommittedInvalidationMessages | MATCH — order Prior:Cat, Cur:Cat, Prior:Rel, Cur:Rel; top-of-stack assert; nummsgs assert |
| inplaceGetInvalidationMessages (1087) | inplaceGetInvalidationMessages | MATCH |
| ProcessCommittedInvalidationMessages (1134) | ProcessCommittedInvalidationMessages | MATCH — nmsgs<=0 early return; DatabasePath set/use/clear dance via seams; pre/send/post |
| AtEOXact_Inval (1198) | AtEOXact_Inval | MATCH — commit: pre, Append, MultiSend, post; abort: local-process Prior; reset state |
| PreInplace_Inval (1249) | PreInplace_Inval | MATCH |
| AtInplace_Inval (1262) | AtInplace_Inval | MATCH — MultiSend then post; clear inplace |
| ForgetInplace_Inval (1285) | ForgetInplace_Inval | MATCH — clears inplace; rolls dense-array tail back to preserve nextmsg==len |
| AtEOSubXact_Inval (1310) | AtEOSubXact_Inval | MATCH — inplace clear policy; level bail; commit lazy-parent `my_level--` vs append-up-and-pop (parent-adjacency predicate equals C `parent==NULL || parent->my_level < my_level-1`); abort local-process + pop |
| CommandEndInvalidationMessages (1409) | CommandEndInvalidationMessages | MATCH — local-process Current; XLogLogicalInfoActive -> LogLogicalInvalidations; Append Current into Prior |
| LogLogicalInvalidations (1939) | LogLogicalInvalidations | MATCH — nmsgs>0 guard; xl_xact_invals header (MinSizeOfXactInvals = sizeof(int)); per-subgroup XLogRegisterData only when non-empty; XLogInsert(RM_XACT_ID, XLOG_XACT_INVALIDATIONS) |

### Shared state accessor — `lib.rs`
| Item | Verdict |
|---|---|
| with_state (lazy thread_local InvalState) | MATCH (after fix) — McxOwned::try_new builds "CacheInvalidation" ctx with both InvalMessageArrays, the (sub)txn stack, inplace slot, three callback tables; syscache_callback_links zero-init (0=no entry); infallible -> R with expect() on one-time build, matching C statics resident for backend life |

## Constants verified field-by-field vs headers
- SHAREDINVAL{CATALOG,RELCACHE,SMGR,RELMAP,SNAPSHOT,RELSYNC}_ID = -1..-6 (sinval.h) — match.
- RELATION/ATTRIBUTE/INDEX/CONSTRAINT_RELATION_ID = 1259/1249/2610/2606 (pg_class/attribute/index/constraint.h) — match.
- CONSTRAINT_FOREIGN = 'f' (pg_constraint.h) — match (encoded in pg_constraint_fk_target seam contract).
- MAX_SYSCACHE_CALLBACKS=64, MAX_RELCACHE_CALLBACKS=10, MAX_RELSYNC_CALLBACKS=10 (inval.c) — match.
- SYS_CACHE_SIZE=85 (== ported syscache cacheinfo length / SysCacheSize) — match.
- MAX_BACKENDS_BITS=18 (<=23) — match.
- sizeof(SharedInvalidationMessage)=16 (union; types-storage wire size) — match.

## Seam audit
Owned seam crate: `crates/backend-utils-cache-inval-seams` (inval.c is the sole C
file). 14 declarations. The 11 that map to inval.c functions are all installed by
this crate's `init_seams()` (cache_register_syscache/relcache_callback,
accept_invalidation_messages, command_end_invalidation_messages,
at_eoxact_inval, at_eosubxact_inval, post_prepare_inval, log_logical_invalidations,
invalidate_system_caches, process_committed_invalidation_messages,
xact_get_committed_invalidation_messages). The remaining 3
(relcache_init_file_pre/post_invalidate -> relcache.c, send_shared_invalid_messages
-> sinval.c) are NOT inval.c functions; they belong to other owners and are
forward-declared here only — uninstalled/panic-until-owner-lands, which is correct.
`init_seams()` is `set()`-only and is wired in `seams-init`.

Outward seams used by the unit are thin marshal+delegate to other-owner C
functions: catalog probes (IsCatalogRelation/IsToastRelation/IsSharedRelation/
RelationInvalidatesSnapshotsOnly), catcache (ResetCatalogCachesExt,
SysCacheInvalidate, CatalogCacheFlushCatalog, PrepareToInvalidateCacheTuple),
relcache (RelationCacheInvalidate[Entry], RelationIdIsInInitFile,
init-file pre/post), relmapper, snapmgr (InvalidateCatalogSnapshot), smgr
(smgrreleaserellocator), sinval (Send/Receive), xact (GetCurrentCommandId,
GetCurrentTransactionNestLevel), xlog (XLogLogicalInfoActive), xloginsert
(XLogInsert), miscinit (MyDatabaseId, IsBootstrapProcessingMode,
Set/Clear DatabasePath). No branching/computation observed in seam paths; the
PrepareToInvalidateCacheTuple callback model is replayed locally (logic stays in
inval). No logic was relocated out of inval into a seam.

## Design conformance
- opacity: InvalState, InvalMessageArray, InvalidationMsgsGroup,
  Trans/InvalidationInfo are real field-for-field structs; no opaque handles/ids.
- Allocating fns/seams take Mcx and return PgResult; per-backend state is one
  `thread_local! McxOwned<InvalStateTy>` (no shared statics, no ambient getter).
- No locks held across `?`; no registry-shaped side tables; no unledgered
  divergence markers.

## Findings fixed this round (re-audited clean)
1. **with_state was `todo!()`** (lib.rs) — the scaffold left the central shared
   accessor unimplemented; every family routes through it, so this was MISSING
   own logic. Implemented the lazy McxOwned build (see table). Re-derived: MATCH.
2. **FATAL severity downgraded to ERROR** (cache_invalidate.rs) — the four C
   `elog(FATAL)` sites in CacheRegisterSyscacheCallback (invalid id / out of
   slots) and CacheRegister{Relcache,RelSync}Callback (out of slots) used
   `PgError::error` (ERROR). Changed to `PgError::new(FATAL, ...)` to match C
   severity and repo convention. CallSyscacheCallbacks correctly remains ERROR.
   Re-derived: MATCH.

## Gate
`cargo check --workspace` clean; `cargo test --workspace` 721 result-ok, 0 failed.
No `todo!()`/`unimplemented!()` remain in own logic.
