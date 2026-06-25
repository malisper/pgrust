# Audit: contrib-amcheck-verify-common

C source: `contrib/amcheck/verify_common.c` (191 LOC).
Port: `crates/contrib-amcheck-verify-common/src/lib.rs`.

## Function inventory

| C function | C loc | Port loc | Verdict | Notes |
|---|---|---|---|---|
| `amcheck_index_mainfork_expected` (static) | verify_common.c:35 | `amcheck_index_mainfork_expected` | MATCH | `relpersistence != UNLOGGED \|\| !RecoveryInProgress()` → true; else NOTICE (ERRCODE_READ_ONLY_SQL_TRANSACTION) via `ereport_msg` and return false. |
| `amcheck_lock_relation_and_check` | verify_common.c:59 | `amcheck_lock_relation_and_check` | MATCH | IndexGetRelation(true) → table_open(heapid) → save userid/sec/nestlevel + SetUserIdAndSecContext(relowner, sec\|SECURITY_RESTRICTED_OPERATION) + NewGUCNestLevel; index_open; re-check heapid==IndexGetRelation(false) else ERRCODE_UNDEFINED_TABLE; index_checkable→callback(readonly=lockmode==ShareLock); AtEOXact_GUC(false)+SetUserIdAndSecContext(restore)+close index then heap. Error from index_checkable/callback unwinds (cleanup reached only on success), matching C's reliance on xact abort. |
| `index_checkable` | verify_common.c:158 | `index_checkable` | MATCH | relkind!=INDEX\|\|relam!=am_id → FEATURE_NOT_SUPPORTED with amname (am_id) errmsg + amname (rel's am) errdetail via syscache search_am_name; RELATION_IS_OTHER_TEMP → FEATURE_NOT_SUPPORTED; !rd_index->indisvalid → FEATURE_NOT_SUPPORTED "Index is not valid."; return amcheck_index_mainfork_expected. |

`RELATION_IS_OTHER_TEMP` is `relpersistence==TEMP && !rd_islocaltemp`, read via the relcache `rd_islocaltemp` seam (relcache-owned).

## Seam audit

Owned seam crate: `amcheck-verify-common-seams` (maps to verify_common.c).
Its single declaration `amcheck_lock_relation_and_check` is installed by this
crate's `init_seams()` (only `set()` calls), which `seams-init::init_all()`
invokes. PASS.

Outward calls are all thin delegates across genuine cycles, via owner seam
crates: catalog-index `index_get_relation`, table `table_open`, indexam
`index_open`, miscinit `get/set_user_id_and_sec_context`, guc
`new_guc_nest_level`/`at_eoxact_guc`, xlog `recovery_in_progress`, syscache
`search_am_name`, relcache `rd_islocaltemp`, elog `ereport_msg`. No logic in any
seam path.

## Findings fixed during audit

1. **Callback error-path cleanup (DIVERGES → fixed).** Initial port captured the
   callback `PgResult` and ran AtEOXact_GUC/SetUserIdAndSecContext/close even on
   callback error. C reaches the inline cleanup only on the success path (an
   error ereports and the transaction abort does the cleanup). Changed to
   `?`-propagate index_checkable + callback so cleanup runs only on success.
2. **RELATION_IS_OTHER_TEMP (DIVERGES → fixed).** Initial port approximated it as
   `relpersistence==TEMP && rd_backend != INVALID_PROC_NUMBER`, which
   misclassifies a *local* temp (whose `rd_backend` is this backend's number) as
   other-temp. Switched to the faithful `relpersistence==TEMP && !rd_islocaltemp`
   via the relcache `rd_islocaltemp` seam.

## Design conformance

No invented opacity, no shared statics, no ambient-global getter seams, no locks
held across `?` (Relation::close is the armed-closer guard). The fresh
`MemoryContext::new` arena for the relation opens is the established pattern for
a no-`mcx` seam owner (rmgr). PASS.

## Verdict: PASS
