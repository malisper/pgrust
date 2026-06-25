# Audit: pgstat F0 (types + dshash-registry model + KindInfo + count-seam sig-widen)

Stage: pgstat-tower F0-types-sigwiden. Scope: types-only + decl-widen; installs
ZERO seams (correct for a types+decl family — the guard stays green).

## Type set added (field-for-field vs C headers)

`crates/types-pgstat/src/activity_pgstat.rs` (vs `src/include/pgstat.h`):

| C type | Rust | parity notes |
|---|---|---|
| `TrackFunctionsLevel` (38) | enum | OFF/PL/ALL, order significant ✓ |
| `PgStat_FetchConsistency` (45) | enum ✓ |
| `SessionEndType` (53) | enum ✓ |
| `IOObject` (273) | enum; `IOOBJECT_NUM_TYPES` ✓ |
| `IOContext` (282) | re-export of `types_storage::buf::IOContext` (no dup); `IOCONTEXT_NUM_TYPES` ✓ |
| `IOOp` (302) | enum; `IOOP_NUM_TYPES`; `pgstat_is_ioop_tracked_in_bytes` ✓ |
| `PgStat_FunctionCounts` (81) | numcalls/total_time/self_time ✓ |
| `PgStat_FunctionCallUsage` (91) | C `fs *` pointer→`tracking:bool` (owner resolves the back-ptr); other 3 instr_time fields ✓ |
| `PgStat_BackendSubEntry` (108) | conflict_count[CONFLICT_NUM_TYPES] ✓ |
| `PgStat_TableCounts` (137) | all 13 counters + truncdropped, `#[repr(C)]`, is_all_zeros ✓ |
| `PgStat_TableStatus` (174) | `relation:Relation` DROPPED (keystone divergence; OID-keyed); `trans *`→`Option<usize>` index into xact stack ✓ |
| `PgStat_BktypeIO`/`PgStat_PendingIO`/`PgStat_IO` (323/330/337) | 3-D arrays `[IOOBJECT][IOCONTEXT][IOOP]`, `stats[BACKEND_NUM_TYPES]` ✓ |
| `PgStat_StatDBEntry` (343) | all 33 fields, order ✓ |
| `PgStat_StatFuncEntry` (381) | ✓ |
| `PgStat_StatReplSlotEntry` (389) | ✓ |
| `PgStat_SLRUStats` (402) | ✓ |
| `PgStat_StatSubEntry` (414) | conflict_count[CONFLICT_NUM_TYPES] ✓ |
| `PgStat_StatTabEntry` (422) | all 30 fields, order ✓ |
| `PgStat_WalCounters`/`PgStat_WalStats` (467/479) | ✓ |
| `PgStat_Backend`/`PgStat_BackendPending` (489/500) | ✓ |
| `PGSTAT_FILE_FORMAT_ID` (215) = 0x01A5BCB7 ✓ |

`crates/types-pgstat/src/pgstat_internal.rs` (vs `src/include/utils/pgstat_internal.h`):

| C type | Rust | parity notes |
|---|---|---|
| `PgStat_HashKey` (52) | kind/dboid/objid ✓ |
| `PgStatShared_Common` (121) | magic + real LWLock ✓ |
| `PgStatShared_HashEntry` (65) | dropped, refcount/generation `AtomicU32`, body `dsa_pointer` ✓ |
| `PgStatShared_{Database,Relation,Function,Subscription,ReplSlot,Backend}` (423-457) | header + stats ✓ |
| `PgStatShared_{IO,SLRU,Wal}` (389-411) | locks[BACKEND_NUM_TYPES] / lock + stats[SLRU_NUM_ELEMENTS] ✓ |
| `PgStat_KindInfo` (202) | the 10 SCALAR descriptor fields (bitfields → bool); the fn-ptr callbacks are OWNER-INTERNAL (NOT here) per the plan — avoids a PgResult/owner dep. The built-in table is C's static array, a faithful model not an invented registry ✓ |
| `PgStat_ShmemControl` (466) | raw_dsa_area `dsa_pointer`, hash_handle `dshash_table_handle`, is_shutdown, gc_request_count `AtomicU64`, 6 fixed kinds, custom_data[PGSTAT_KIND_CUSTOM_SIZE] ✓ |
| `PgStat_Snapshot` (510) | mode/timestamp/fixed_valid + 6 fixed + custom_valid/custom_data; C `stats *` simplehash + `context` MemoryContext are owner-internal lifecycle (NOT here) ✓ |
| `slru_names[]` / `SLRU_NUM_ELEMENTS` (327/338) = 8, "other" last ✓ |

`PgStat_EntryRef` (135) and `PgStat_LocalState` (548) are owner-internal
(nothing outside reads their fields) — NOT modeled, per the plan.

## Count-seam signature widening (RISK 1 decision)

The C count macros gate on `pgstat_should_count_relation(rel)` =
`rel->pgstat_info != NULL ? true : (rel->pgstat_enabled ? assoc(),true : false)`.
Our `Relation` carries `pgstat_enabled` but no `pgstat_info` back-pointer (the
pending-entry link is OID-keyed inside pgstat). DECISION: widen each count seam
to `(relid, pgstat_enabled, ...)` — narrowest faithful capability; the owner
replicates the gate. Widened (in `backend-utils-activity-pgstat-seams`):
pgstat_count_{index_tuples,heap_fetch,index_scan,heap_scan,heap_getnext,
heap_insert,heap_delete,heap_update}, pgstat_update_heap_dead_tuples.

To carry `pgstat_enabled` to the call sites, added the field to the trimmed
`RelationData` (types-rel) field-for-field (`utils/rel.h pgstat_enabled`), the
relcache builder copies it from the canonical entry, 7 test constructors set
false.

## Call sites updated (~14 across 9 crates)

heapam scan(4)/insert/update/delete/inplace, pruneheap, gist_scan(2),
spgscan, hashsearch, gin-ginscan, brin-scan, indexam(3). All read
`<rel>.pgstat_enabled` off the Relation already in scope. (matview's seams live
in its own deps-seams crate delegating to pgstat — out of scope, untouched.)

## Seams installed: ZERO (correct). Guard: green.
no-todo-guard ✓, seams-init ✓ (2/2), workspace check ✓, touched-crate tests ✓.
Only flake: `backend-access-hashfunc::text_deterministic_hashes_image`
(strxfrm/ICU-locale-dependent; passes in isolation + on clean base; unrelated
to this change).
