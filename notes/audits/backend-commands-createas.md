# Audit: backend-commands-createas (commands/createas.c)

C source: `../pgrust/postgres-18.3/src/backend/commands/createas.c` (637 LOC).
c2rust: `../pgrust/c2rust-runs/backend-commands-createas/src/createas.rs`.
Port: `crates/backend-commands-createas/src/lib.rs` (+ owned seam crate
`crates/backend-commands-createas-seams`).

Audit performed independently from the C + headers; constants re-derived.

## Function inventory + verdicts

| C function (line) | Port location | Verdict | Notes |
|---|---|---|---|
| `create_ctas_internal` (81) | `create_ctas_internal` | MATCH (catalog leg SEAMED) | Computes `is_matview = into->viewQuery != NULL`, `relkind = MATVIEW/RELATION` in-crate (faithful). The DefineRelation + CCI + TOAST-options + matview StoreViewQuery sequence — no createas-observable intermediate state, shares `create->options`/new OID, and touches the unported tablecmds/view + `Datum`-varlena reloptions — crosses one `create_ctas_relation` seam (mirrors src-idiomatic's `create_relation`). |
| `create_ctas_nodata` (154) | `create_ctas_nodata` | MATCH | Non-junk-tle loop; colName override cursor with "too few OK / too many error" (`ERRCODE_SYNTAX_ERROR`); `exprType/Typmod/Collation(tle->expr)` via the bundled `expr_type_info` nodeFuncs seam; `build_coldef_checked` per column; final `create_ctas_internal`. |
| `ExecCreateTableAs` (222) | `ExecCreateTableAs` | MATCH (rewrite/plan/run leg SEAMED) | castNode(Query)/into; `CreateTableAsRelExists` early-return InvalidObjectAddress; `CreateIntoRelDestReceiver`; jumble+post_parse preamble (seam, model-split); CMD_UTILITY+ExecuteStmt → `execute_query` seam, reladdr read-back; matview forces `skipData=true`, `do_refresh=!skipData`; skipData branch → `create_ctas_nodata` (+ `RefreshMatViewByOid` when do_refresh); else branch → `run_ctas_executor` seam (rewrite→plan→snapshot→QueryDesc→run→SetQueryCompletion→teardown), reladdr read-back. |
| `GetIntoRelEFlags` (374) | `GetIntoRelEFlags` + `get_into_rel_eflags_seam` | MATCH | `flags=0; if skipData flags |= EXEC_FLAG_WITH_NO_DATA (0x0040, verified executor.h:71)`. Inward seam impl over trimmed `parsestmt::IntoClause` (reads only skipData; consumed by merged prepare/explain). |
| `CreateTableAsRelExists` (392) | `CreateTableAsRelExists` | MATCH | `RangeVarGetCreationNamespace(into->rel)` (direct namespace call; node→access RangeVar field copy); `get_relname_relid` seam; `OidIsValid` → if `!if_not_exists` ERROR `ERRCODE_DUPLICATE_TABLE` (42P07, verified) else `ObjectAddressSet(RelationRelationId=1259, oldrelid)` + `checkMembershipInCurrentExtension` + NOTICE skipping + return true; else false. |
| `CreateIntoRelDestReceiver` (439) | `CreateIntoRelDestReceiver` | MATCH | `palloc0(DR_intorel)` + 4 callback assignments + `mydest=DestIntoRel` + `self->into` → registers a real `ReceiverVtable {rStartup,receiveSlot,rShutdown}` keyed `CommandDest::IntoRel` into the tcop-dest router (mirrors copyto's `CreateCopyDestReceiver`). `self->into` is threaded via `receiver_setup_run` by the run driver (the receiver-creation site has no arena), documented. rDestroy not in the router vtable (owner teardown path). |
| `intorel_startup` (458) | `intorel_startup` | MATCH | `Assert(into != NULL)`; is_matview; column loop over `typeinfo->natts` with `TupleDescAttr`/`NameStr(attname)` defaults vs colNames override (too-many → SYNTAX_ERROR); `create_ctas_internal`; `table_open(addr, AccessExclusiveLock)`; `check_enable_rls(...)==RLS_ENABLED` → ERROR `ERRCODE_FEATURE_NOT_SUPPORTED` (0A000); matview+!skipData → `SetMatViewPopulatedState(oid,true)`; fill output_cid=`GetCurrentCommandId(true)`, ti_options=`TABLE_INSERT_SKIP_FSM (0x0002, verified)`, bistate=`GetBulkInsertState()`/NULL; reladdr saved. The `Assert(RelationGetTargetBlock==InvalidBlockNumber)` is a debug-only Assert reading `rd_smgr->smgr_targblock` (absent from trimmed relcache) — elided with a documented note (no logic depends on it). |
| `intorel_receive` (582) | `intorel_receive` | MATCH | `if !into->skipData` → `table_tuple_insert(mcx, &rel, slot, output_cid, ti_options, bistate)` (reached via the #333 mcx-vtable); return true. Slot-type comment preserved. |
| `intorel_shutdown` (613) | `intorel_shutdown` | MATCH (finish_bulk_insert SEAMED) | `if !into->skipData` → `FreeBulkInsertState(bistate)` (direct heapam) + `table_finish_bulk_insert(rel, ti_options)` (seam — AM `finish_bulk_insert` vtable slot unported); `table_close(rel, NoLock)`; `myState->rel = NULL` (state ptr unbound). |
| `intorel_destroy` (633) | `intorel_destroy` | MATCH | `pfree(self)`: registry slot + arena state reclaimed by context reset; documented no-op, not wired to the router vtable (which has no rDestroy slot). |

## Seam audit

Owned seam crate: `backend-commands-createas-seams`. All declarations installed
by `backend_commands_createas::init_seams()` (wired into `seams-init::init_all`):

INWARD (this crate installs the real fn):
- `get_into_rel_eflags`, `exec_create_table_as`, `create_table_as_rel_exists`,
  `create_into_rel_dest_receiver` — all `set()` in `init_seams()`. ✓

OUTWARD/declared-here (panic until owner lands — owner not complete, guard-exempt):
- `jumble_and_post_analyze` — JumbleQuery/post_parse_analyze_hook operate on the
  trimmed `portalcmds::Query`, incompatible with the CTAS `copy_query::Query`;
  owners reconcile the model. Justified bundle (cross-model).
- `create_ctas_relation` — DefineRelation (tablecmds.c, todo) + StoreViewQuery
  (view.c, absent) + TOAST `Datum`-varlena; no observable intermediate state.
- `run_ctas_executor` — QueryRewrite/pg_plan_query take the handle Query; the
  CTAS Query is the canonical arena Query (incompatible models). The prompt's
  sanctioned `pg_plan_query` panic. The receiver itself is fully real.
- `execute_query` — prepare.c's ExecuteQuery (cycle: prepare→createas-seams);
  prepare installs when it wires the cross-model bridge.
- `table_finish_bulk_insert` — tableam AM `finish_bulk_insert` vtable slot
  unported; declared consumer-side (createas owner not complete → guard-exempt),
  panics until the AM slot lands.

Direct (cycle-free) callees: makefuncs (`make_column_def`), tcop-dest
(`register_dest_receiver`/`ReceiverVtable`), table-table (`table_open`/`_close`),
table-tableam (`table_tuple_insert`), heap-heapam (`Get/FreeBulkInsertState`),
namespace (`RangeVarGetCreationNamespace`), pg-depend
(`checkMembershipInCurrentExtension`), xact (`GetCurrentCommandId`). Outward
seams used for cycle/ported-but-on-the-shim owners: nodeFuncs, lsyscache,
format-type, misc-more (rls), matview (Refresh/SetPopulated).

No branching/node-construction inside any seam path on this side; seams are thin
marshal+delegate. `init_seams()` contains only `set()` calls.

## Design conformance

- No invented opacity: `DestReceiverHandle`/`ParamListInfoHandle` are inherited
  unported-subsystem handles; `ObjectAddress`/`Relation<'mcx>`/`QueryCompletion`
  are real typed values. ✓
- Allocating paths carry `Mcx` + return `PgResult`; column/attr lists use
  `mcx::vec_with_capacity_in`/`alloc_in` (fallible). ✓
- Per-backend `DR_intorel` registry is `thread_local` (per-backend C global
  model), not a shared static. The raw-pointer state bind mirrors copyto's
  proven `DR_copy.cstate` alias; backing store is arena-allocated via
  `mcx::leak_in` (lives for the query, as C `palloc`). ✓
- No locks held across `?` (table_open's RAII close is the `Relation` Drop;
  shutdown closes explicitly). ✓
- Error sites map to matching SQLSTATE/severity (DUPLICATE_TABLE,
  INDETERMINATE_COLLATION, SYNTAX_ERROR, FEATURE_NOT_SUPPORTED; NOTICE skipping).
  ✓

## Constants re-derived (vs headers)

- `EXEC_FLAG_WITH_NO_DATA = 0x0040` — executor.h:71 ✓
- `TABLE_INSERT_SKIP_FSM = 0x0002` — tableam.h:258 ✓
- `RELATION_RELATION_ID = 1259` — pg_class ✓
- `RELKIND_RELATION = 'r'` / `RELKIND_MATVIEW = 'm'` — typed re-exports ✓
- ERRCODEs 42P07 / 42P22 / 42601 / 0A000 — types-error ✓

## Verdict: PASS

Every C function MATCH or SEAMED per the rules (panics reach only unported
callees / cross-model owners; no createas-own logic is absent or approximated).
Zero seam findings. Gates: `cargo check --workspace` green; seams-init
recurrence guards (both) green; no-todo guard green.
