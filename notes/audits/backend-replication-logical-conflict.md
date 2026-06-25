# Audit: backend-replication-logical-conflict

- Unit: `backend-replication-logical-conflict` (`src/backend/replication/logical/conflict.c`, PostgreSQL 18.3)
- Port: `crates/backend-replication-logical-conflict` (+ `crates/types-replication`,
  new seam crates `backend-access-index-indexam-seams`, `backend-catalog-index-seams`,
  `backend-executor-execReplication-seams`, and new declarations in six existing seam crates)
- C source: `../pgrust/postgres-18.3/src/backend/replication/logical/conflict.c` (505 lines)
- c2rust: `../pgrust/c2rust-runs/backend-replication-logical-conflict/src/conflict.rs`
- Audited at: branch `port/backend-replication-logical-conflict`, commit `c6269a3`
- Auditor basis: independent re-derivation from the C source and headers, cross-checked
  against the c2rust rendering function by function.

## Function inventory and verdicts

The C file defines exactly 7 functions (3 extern, 4 static) plus one static data
table. The c2rust rendering contains the same 7 (`conflict.rs` lines 2755, 2777,
2866, 2884, 2894, 3059, 3211) — no preprocessor-conditional functions exist.

| C function (conflict.c) | Port location (crates/backend-replication-logical-conflict/src/lib.rs) | Verdict | Notes |
|---|---|---|---|
| `ConflictTypeNames[]` (l.26) | `CONFLICT_TYPE_NAMES` (l.59) | MATCH | Designated initializers reproduced index-by-discriminant; all 7 strings verified byte-equal against the C; unit-tested (tests.rs). |
| `GetTupleTransactionInfo` (l.61) | `GetTupleTransactionInfo` (l.97) | MATCH | `slot_getsysattr(MinTransactionIdAttributeNumber=-2, sysattr.h verified)` via execTuples seam; `*xmin` assigned before the track check, exactly as C; `!track_commit_timestamp` → `(InvalidRepOriginId, 0, false)`; otherwise `TransactionIdGetCommitTsData` via commit-ts seam returning `(found, ts, nodeid)`. GUC global passed as explicit param per AGENTS.md. `Assert(!isnull)` → `debug_assert`. |
| `ReportApplyConflict` (l.102) | `ReportApplyConflict` (l.141) | MATCH | `foreach_ptr` over conflicttuples → slice iteration in order; `pgstat_report_subscription_conflict(MySubscription->oid→subid param, type)` fires before the ereport, as in C; errmsg format `conflict detected on relation "%s.%s": conflict=%s` byte-equal, with `get_namespace_name` NULL rendered "(null)" (PG vsnprintf behavior); `errdetail_internal("%s", err_detail)`; `errcode_apply_conflict`. `finish` drives the full ereport cycle (non-error elevel returns Ok, ERROR+ returns Err) — verified against `ThrowErrorData`/`errstart`. |
| `InitConflictIndexes` (l.137) | `InitConflictIndexes` (l.197) | MATCH | Loop `0..ri_NumIndices`; NULL index slot → `continue`; `!ii_Unique` → continue; `!rd_index->indimmediate` → continue; `lappend_oid(RelationGetRelid)` → fallible `try_reserve`+push of `rd_id`; result stored unconditionally (NIL → None). |
| `errcode_apply_conflict` (l.167) | `errcode_apply_conflict` (l.255) | MATCH | INSERT_EXISTS/UPDATE_EXISTS/MULTIPLE_UNIQUE_CONFLICTS → 23505 (`ERRCODE_UNIQUE_VIOLATION`), the four others → 40001 (`ERRCODE_T_R_SERIALIZATION_FAILURE`); both verified against errcodes.txt and `types-error` `make_sqlstate` values. The C `Assert(false); return 0` tail is unreachable in Rust (total match over the enum). Unit-tested. |
| `errdetail_apply_conflict` (l.197) | `errdetail_apply_conflict` (l.275) | MATCH | All five switch arms with the exact `if (localts)` / `localorigin == InvalidRepOriginId` / `replorigin_by_oid(.., true, ..)` decision tree per arm; all 12 message format strings verified byte-equal to the C `_()` literals; `%u` on u32 == `{}`; `timestamptz_to_str` via adt-timestamp seam; `get_rel_name` NULL → "(null)" matching PG vsnprintf. `Assert(err_detail.len > 0)` → debug_assert. val_desc appended as `"\n%s"`; blank-line separator `if (err_msg->len > 0)` appended before, then err_detail — order identical. `CheckRelationOidLockedByMe` half of the Assert is cassert-only (absent from c2rust) and elided; `OidIsValid` half kept as debug_assert. |
| `build_tuple_value_details` (l.317) | `build_tuple_value_details` (l.440) | MATCH | Key block gated on the three unique-violation types; localslot block (`modifiedCols = NULL`, maxfieldlen 64); remoteslot block with `bms_union(ExecGetInsertedCols, ExecGetUpdatedCols)` (direct execUtils dep + nodes-core seam); searchslot block: `GetRelationIdentityOrPK` via execReplication seam, valid → `build_index_value_desc(searchslot, replica_index)`, else full-tuple description; the 8 capitalized/lowercase "; "-joined variants ("Key %s", "[Ee]xisting local row %s", "[Rr]emote row %s", "[Rr]eplica identity [full ]%s") all byte-equal; empty → NULL/None; trailing '.' appended. `Assert(searchslot || localslot || remoteslot)` and `Assert(type != CT_INSERT_EXISTS)` → debug_asserts. |
| `build_index_value_desc` (l.460) | `build_index_value_desc` (l.581) | MATCH | NULL slot → None before anything else; `index_open(indexoid, NoLock)` via indexam seam; `TTS_IS_VIRTUAL(slot)` (checks the *original* slot, as C does) → `table_slot_create(localrel, &estate->es_tupleTable)` (direct tableam dep, slot registered in the estate pool) then `ExecCopySlot` via execTuples seam; `GetPerTupleExprContext(estate)` (create-if-absent macro) → `MakePerTupleExprContext`, whose Rust port (execUtils l.366) is verified to be the same create-once-and-return; `ecxt_scantuple = tableslot` set before `FormIndexDatum(BuildIndexInfo(indexDesc), ...)` (catalog-index seams, INDEX_MAX_KEYS=32 verified) then `BuildIndexValueDescription` (genam seam); `index_close(NoLock)` on success, drop on the error path mirrors the C resowner abort cleanup. |

## Types audit

- `ConflictType` (`types-replication/src/conflict.rs`): discriminants 0..6 verified
  against `replication/conflict.h` declaration order (they index pgstat counters and
  the name table); `CONFLICT_NUM_TYPES = 7` matches `CT_MULTIPLE_UNIQUE_CONFLICTS + 1`.
- `ConflictTupleInfo`: all five header fields present (`slot`, `indexoid`, `xmin`,
  `origin`, `ts`) with faithful type mappings (`TupleTableSlot*` → `Option<SlotId>`).
- `IndexInfo` (types-nodes, trimmed to `ii_Unique`), `ResultRelInfo` index fields
  (`ri_NumIndices`, `ri_IndexRelationDescs`, `ri_IndexRelationInfo`,
  `ri_onConflictArbiterIndexes`), `FormData_pg_index` (trimmed to `indimmediate`),
  `relnamespace` on `FormData_pg_class`: real trimmed types per types.md rules 6-7,
  no invented opacity; docs/types.md updated.
- `MinTransactionIdAttributeNumber = -2` verified against `access/sysattr.h`.

## Seam audit

Outward seams (this unit calls; owners unported, so the declarations are
uninstalled-and-panicking until the owners land — sanctioned):

| Seam | Crate | Thin? | Justified? |
|---|---|---|---|
| `slot_getsysattr`, `exec_copy_slot` | backend-executor-execTuples-seams | yes (marshal+call) | execTuples unported |
| `exec_build_slot_value_description` | backend-executor-execMain-seams (new decl) | yes; `Mcx` in, `'mcx` out | execMain unported |
| `transaction_id_get_commit_ts_data` | backend-access-transam-commit-ts-seams (new decl) | yes | commit_ts unported |
| `replorigin_by_oid` | backend-replication-logical-origin-seams (new decl) | yes; `Mcx` for the name copy | origin unported |
| `timestamptz_to_str` | backend-utils-adt-timestamp-seams (new decl) | yes; `Mcx` for the copy | adt/timestamp unported |
| `build_index_value_description` | backend-access-index-genam-seams (new decl) | yes; `Mcx` in, `'mcx` out | genam unported |
| `pgstat_report_subscription_conflict` | backend-utils-activity-stat-seams (new decl) | yes | pgstat_subscription unported |
| `index_open` | backend-access-index-indexam-seams (new crate) | yes; close = `Relation::close` | indexam unported |
| `build_index_info`, `form_index_datum` | backend-catalog-index-seams (new crate) | yes | catalog/index unported |
| `get_relation_identity_or_pk` | backend-executor-execReplication-seams (new crate) | yes | execReplication unported |

Direct deps (no seam, already-ported crates): `backend-executor-execUtils`
(`ExecGetInsertedCols`/`ExecGetUpdatedCols`/`MakePerTupleExprContext`),
`backend-access-table-tableam` (`table_slot_create`), `backend-nodes-core-seams`
(`bms_union`), `backend-utils-cache-lsyscache-seams`
(`get_rel_name`/`get_namespace_name`), `backend-utils-error`.

- No `set()` calls exist in the consumer crate or the new seam crates — installation
  correctly deferred to the owners' `init_seams()`.
- No inward seams: no other crate calls conflict.c entry points yet (consumers can
  direct-dep); `init_seams()` is empty by design and is wired into
  `seams-init::init_all()` (alphabetical position correct).
- No logic in seam paths: every seam call is argument conversion, one call, result
  conversion.

## Design conformance

- Per-backend globals (`track_commit_timestamp`, `MySubscription->oid`) taken as
  explicit parameters, not getter seams (AGENTS.md neighbor table). PASS.
- Neighbor types defined real-and-trimmed in `types-*`, values verified against
  headers; no Oid/blob stand-ins. PASS.
- Allocating seams carry `Mcx<'mcx>` and return `'mcx` data; `PgResult` throughout. PASS.
- Fallible allocation: `InitConflictIndexes` uses `try_reserve` + `mcx.oom(...)`.
  Error-message `String`s feed the ereport builder (which stores `String`),
  consistent with repo-wide error construction. PASS.
- No shared statics, no registry side tables, no locks across `?`. PASS.
- `cargo check --workspace` clean; crate tests (6) pass.

## Spot-check of MATCH verdicts

Re-derived in full detail: `errdetail_apply_conflict` (all 12 format strings
compared character-by-character against the C literals; the
`localts → localorigin → replorigin_by_oid` decision tree traced per arm) and
`build_index_value_desc` (against both the C and the c2rust rendering, confirming
the virtual-slot check is on the original `slot`, the scantuple assignment ordering,
and the GetPerTupleExprContext macro equivalence to the ported
`MakePerTupleExprContext`).

## Verdict

**PASS** — all 7 functions + the name table MATCH; zero seam findings; zero design
findings.
