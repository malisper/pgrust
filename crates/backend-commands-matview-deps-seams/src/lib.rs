//! Outward (frontier) seam declarations for `backend-commands-matview`.
//!
//! `matview.c` is a thin driver over a wide set of subsystems. The relcache /
//! table-AM / index-AM read-bundle the C inspects off its open `Relation`s is
//! NOT here any more: the matview/index/transient relations are opened as real
//! [`types_rel::Relation`] values (via the canonical
//! `backend_access_table_table::table_open` /
//! `backend_access_index_indexam::index_open`) and their `rd_rel`/`rd_att`
//! fields are read directly in the ported body. `RelationGetIndexList`,
//! `CheckTableNotInUse`, the pgstat counters, and `GetDefaultTablespace` are
//! likewise reached through their real ported owners.
//!
//! What remains here is the genuine frontier into still-unported owners (GUC /
//! security context, SPI, planner / executor, snapshot, cluster heap-swap,
//! rangevar resolution, the pg_class populated-state update), plus the three
//! reads that bottom out off the trimmed relcache carrier and so cannot be read
//! off the open `Relation`:
//!
//!   - `matview_rule_info` / `matview_data_query` — the matview's `rd_rules`
//!     rewrite-rule shape and stored `dataQuery` (`RelationData` does not model
//!     `rd_rules`; the RuleLock-carrier keystone — see rewriteHandler).
//!   - `index_usability_info` / `index_match_merge_quals` — the index's full
//!     `indkey[]` array, the `RelationGetIndexPredicate == NIL` test, and the
//!     `rd_indextuple` opclass reads (the trimmed `FormData_pg_index` projection
//!     carries only `indkey0`; the opclass/ruleutils side is unported). These
//!     take the real index/matview `Relation` (no OID re-resolution); the owner
//!     reads the off-carrier fields from the open handle.
//!
//! Where the real subsystem can `ereport(ERROR)`, the seam returns
//! [`PgResult`]; otherwise it returns a bare value.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;
use types_matview::{
    IndexUsabilityInfo, MatViewRuleInfo, MatchMergeQual, PlannedStmtHandle, QueryDescHandle,
    QueryHandle,
};
use types_rel::Relation;

// --- matview rewrite-rule shape (RuleLock-carrier keystone) --------------------

seam_core::seam!(
    /// The `matviewRel->rd_rules->...` rewrite-rule shape `RefreshMatViewByOid`
    /// branches on (matview.c 211-262). `RelationData` does not model `rd_rules`
    /// (the RuleLock-carrier keystone), so the relcache owner reports it from the
    /// open handle, keyed by the matview OID. See [`MatViewRuleInfo`].
    pub fn matview_rule_info(rel: Oid) -> PgResult<MatViewRuleInfo>
);
seam_core::seam!(
    /// `linitial_node(Query, rule->actions)` — the matview's stored `dataQuery`,
    /// reached off `rd_rules` (the same RuleLock keystone).
    pub fn matview_data_query(rel: Oid) -> PgResult<QueryHandle>
);
seam_core::seam!(
    /// Read the `pg_index` fields `is_usable_unique_index` inspects that are NOT
    /// on the trimmed relcache projection — the full `indkey[]` array and
    /// `RelationGetIndexPredicate(indexRel) == NIL` (the carrier-widen keystone).
    /// The predicate logic stays in-crate. Takes the real open index relation.
    /// See [`IndexUsabilityInfo`].
    pub fn index_usability_info(index_rel: &Relation<'_>) -> PgResult<IndexUsabilityInfo>
);

// --- pg_class populated-state update + command counter -------------------------

seam_core::seam!(
    /// `table_open(RelationRelationId, RowExclusiveLock)` ->
    /// `SearchSysCacheCopy1(RELOID, relid)` -> set `relispopulated = newstate` ->
    /// `CatalogTupleUpdate` -> `heap_freetuple` -> `table_close`. One atomic
    /// pg_class update; returns `false` when the syscache lookup failed (the
    /// in-crate caller raises `cache lookup failed for relation %u`).
    pub fn update_pg_class_populated(relid: Oid, newstate: bool) -> PgResult<bool>
);
seam_core::seam!(
    /// `CommandCounterIncrement()`.
    pub fn command_counter_increment() -> PgResult<()>
);

// --- ExecRefreshMatView name resolution ----------------------------------------

seam_core::seam!(
    /// `RangeVarGetRelidExtended(relation, lockmode, 0,
    /// RangeVarCallbackMaintainsTable, NULL)`. The RangeVar is identified by its
    /// resolved schema/relation names; the maintains-table callback is folded in.
    pub fn rangevar_get_relid_extended(
        schemaname: Option<String>,
        relname: String,
        lockmode: i32,
    ) -> PgResult<Oid>
);

// --- userid / security context / GUC nest level --------------------------------

seam_core::seam!(
    /// `GetUserIdAndSecContext(&save_userid, &save_sec_context)`.
    pub fn get_user_id_and_sec_context() -> PgResult<(Oid, i32)>
);
seam_core::seam!(
    /// `SetUserIdAndSecContext(userid, sec_context)`.
    pub fn set_user_id_and_sec_context(userid: Oid, sec_context: i32) -> PgResult<()>
);
seam_core::seam!(
    /// `NewGUCNestLevel()`.
    pub fn new_guc_nest_level() -> PgResult<i32>
);
// (RestrictSearchPath re-homed to backend-utils-misc-guc-seams — it is guc.c's
// function (guc.c:2246) — and installed by the merged guc owner. matview calls
// it there.)
seam_core::seam!(
    /// `AtEOXact_GUC(isCommit, nestLevel)` (called with `(false, save_nestlevel)`).
    pub fn at_eoxact_guc(is_commit: bool, nest_level: i32) -> PgResult<()>
);

// --- table maintenance / tablespace / new heap ---------------------------------

seam_core::seam!(
    /// `GetDefaultTablespace(relpersistence, false)`.
    pub fn get_default_tablespace(relpersistence: i8) -> PgResult<Oid>
);
seam_core::seam!(
    /// `make_new_heap(matviewOid, tableSpace, relam, relpersistence,
    /// ExclusiveLock)` -> OID of the new transient heap.
    pub fn make_new_heap(
        matview_oid: Oid,
        table_space: Oid,
        relam: Oid,
        relpersistence: i8,
    ) -> PgResult<Oid>
);
seam_core::seam!(
    /// `finish_heap_swap(matviewOid, OIDNewHeap, false, false, true, true,
    /// RecentXmin, ReadNextMultiXactId(), relpersistence)`. The runtime reads
    /// `RecentXmin` / `ReadNextMultiXactId()` itself.
    pub fn finish_heap_swap(
        matview_oid: Oid,
        oid_new_heap: Oid,
        relpersistence: i8,
    ) -> PgResult<()>
);

// --- refresh_matview_datafill: rewrite / plan / executor / snapshot ------------

seam_core::seam!(
    /// `copyObject(query)` -> `AcquireRewriteLocks(copied, true, false)` ->
    /// `QueryRewrite(copied)`; returns the rewritten list's length and, when 1,
    /// the single rewritten Query.
    pub fn rewrite_data_query(query: QueryHandle) -> PgResult<(i32, QueryHandle)>
);
seam_core::seam!(
    /// `CHECK_FOR_INTERRUPTS()`.
    pub fn check_for_interrupts() -> PgResult<()>
);
seam_core::seam!(
    /// `pg_plan_query(query, queryString, CURSOR_OPT_PARALLEL_OK, NULL)`.
    pub fn pg_plan_query(query: QueryHandle, query_string: String) -> PgResult<PlannedStmtHandle>
);
// NOTE: `push_copied_snapshot_and_bump` was re-homed to
// `backend-utils-time-snapmgr-seams` (its true C owner is
// `utils/time/snapmgr.c`); matview now calls it through that crate.
seam_core::seam!(
    /// `CreateQueryDesc(plan, queryString, GetActiveSnapshot(), InvalidSnapshot,
    /// dest, NULL, NULL, 0)`.
    pub fn create_query_desc(
        plan: PlannedStmtHandle,
        query_string: String,
        dest: types_nodes::parsestmt::DestReceiverHandle,
    ) -> PgResult<QueryDescHandle>
);
seam_core::seam!(
    /// `ExecutorStart(queryDesc, 0)`.
    pub fn executor_start(query_desc: QueryDescHandle) -> PgResult<()>
);
seam_core::seam!(
    /// `ExecutorRun(queryDesc, ForwardScanDirection, 0)`.
    pub fn executor_run(query_desc: QueryDescHandle) -> PgResult<()>
);
seam_core::seam!(
    /// `queryDesc->estate->es_processed`.
    pub fn query_desc_es_processed(query_desc: QueryDescHandle) -> PgResult<u64>
);
seam_core::seam!(
    /// `ExecutorFinish(queryDesc); ExecutorEnd(queryDesc); FreeQueryDesc(queryDesc);`
    pub fn executor_finish_end_free(query_desc: QueryDescHandle) -> PgResult<()>
);
// NOTE: `pop_active_snapshot` is owned by snapmgr.c and already declared in
// `backend-utils-time-snapmgr-seams`; matview calls it through that crate.

// --- transientrel_* DestReceiver: the table-AM bulk-insert flush ---------------
//
// The `DR_transientrel` receiver itself is owned in-crate by
// `backend-commands-matview` and registered into the `backend-tcop-dest`
// value-router (mirroring `createas.c`'s `DR_intorel`): `transientrel_startup`
// reaches `table_open`/`GetBulkInsertState`, `transientrel_receive` reaches
// `table_tuple_insert`, and `transientrel_shutdown` reaches
// `FreeBulkInsertState`/`table_close` directly. The single dep left on the
// frontier is the table-AM `finish_bulk_insert` vtable slot (unported in the
// heap AM handler), exactly as `createas.c` carries it.

seam_core::seam!(
    /// `table_finish_bulk_insert(rel, options)` (tableam.h inline) — flush any
    /// remaining buffered tuples for a bulk insert and, for
    /// `TABLE_INSERT_SKIP_WAL`, fsync the relation. Called by
    /// `transientrel_shutdown`. The heap AM's `finish_bulk_insert` vtable slot is
    /// not yet ported, so this stays on the frontier and panics until it lands
    /// (mirror-PG-and-panic), matching `backend-commands-createas`.
    pub fn table_finish_bulk_insert<'mcx>(
        rel: &types_rel::Relation<'mcx>,
        options: i32,
    ) -> PgResult<()>
);

// --- refresh_by_match_merge: SPI + ruleutils -----------------------------------

seam_core::seam!(
    /// `SPI_connect()`.
    pub fn spi_connect() -> PgResult<()>
);
seam_core::seam!(
    /// `SPI_finish()` -> result code (`SPI_OK_FINISH` on success).
    pub fn spi_finish() -> PgResult<i32>
);
seam_core::seam!(
    /// `SPI_exec(querytext, 0)` -> SPI result code.
    pub fn spi_exec(query: String) -> PgResult<i32>
);
seam_core::seam!(
    /// `SPI_execute(querytext, read_only, tcount)` -> SPI result code.
    pub fn spi_execute(query: String, read_only: bool, tcount: i64) -> PgResult<i32>
);
seam_core::seam!(
    /// `SPI_processed` — rows produced by the last `SPI_execute`.
    pub fn spi_processed() -> PgResult<u64>
);
seam_core::seam!(
    /// `SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1)` — the first
    /// column of the first row, for the duplicate-row `errdetail`.
    pub fn spi_getvalue_first() -> PgResult<String>
);
seam_core::seam!(
    /// Resolve the per-key-column equality quals for one usable unique index
    /// (matview.c 741-817). For each key column: `indclass` ->
    /// `SearchSysCache1(CLAOID, opclass)` -> `opcfamily`/`opcintype` ->
    /// `get_opfamily_member_for_cmptype(.., COMPARE_EQ)` -> [`MatchMergeQual`].
    /// Reads off the index's raw `rd_indextuple` + the matview tuple descriptor,
    /// so it takes the real open index/matview relations. Errors surface here;
    /// quals are returned in index-key order.
    pub fn index_match_merge_quals(
        index_rel: &Relation<'_>,
        matview_rel: &Relation<'_>,
    ) -> PgResult<Vec<MatchMergeQual>>
);
seam_core::seam!(
    /// `generate_operator_clause(&buf, leftop, leftType, op, rightop, rightType)`
    /// -> the appended clause text (the in-crate code controls placement / the
    /// leading `" AND "`).
    pub fn generate_operator_clause(qual: MatchMergeQual) -> PgResult<String>
);
