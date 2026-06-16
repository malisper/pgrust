//! Outward (frontier) seam declarations for `backend-commands-matview`.
//!
//! `matview.c` is a thin driver over a wide set of subsystems — the relcache,
//! the table-AM, the lock manager, the pg_class catalog, SPI, the planner, the
//! executor, the snapshot manager, cluster's heap-swap, command/GUC/security
//! context, transam/multixact, pgstat, and the lsyscache/ruleutils helpers —
//! and ALL of them are still unported. Every call out of matview crosses one of
//! the seams declared here; until each owner lands, the seam panics with its
//! path (mirror-PG-and-panic). The matview-shaped read-bundles
//! (`matview_rel_info`, `index_usability_info`, `index_match_merge_quals`,
//! `update_pg_class_populated`) mirror the inline relcache/catalog reads the C
//! does — no single ported owner exposes them yet, so they live on matview's
//! frontier and the real owners install them when they arrive.
//!
//! Where the real subsystem can `ereport(ERROR)`, the seam returns
//! [`PgResult`]; otherwise it returns a bare value.

#![allow(non_snake_case)]

use types_core::primitive::Oid;
use types_error::PgResult;
use types_matview::{
    IndexUsabilityInfo, MatViewRelInfo, MatchMergeQual, PlannedStmtHandle, QueryDescHandle,
    QueryHandle,
};

// --- relcache / table-AM / lock ------------------------------------------------

seam_core::seam!(
    /// `table_open(oid, lockmode)` -> the opened relation, modeled by its OID
    /// (the relcache owner re-resolves the descriptor for the read seams).
    pub fn table_open(oid: Oid, lockmode: i32) -> PgResult<Oid>
);
seam_core::seam!(
    /// `table_close(rel, lockmode)`.
    pub fn table_close(rel: Oid, lockmode: i32) -> PgResult<()>
);
seam_core::seam!(
    /// `index_open(indexoid, lockmode)` -> opened index relation OID.
    pub fn index_open(indexoid: Oid, lockmode: i32) -> PgResult<Oid>
);
seam_core::seam!(
    /// `index_close(indexRel, lockmode)`.
    pub fn index_close(index_rel: Oid, lockmode: i32) -> PgResult<()>
);
seam_core::seam!(
    /// `RelationGetIndexList(rel)` — the OID list of the relation's indexes
    /// (iterated in-crate; the in-crate `list_free` is the owned `Vec` drop).
    pub fn relation_get_index_list(rel: Oid) -> PgResult<Vec<Oid>>
);
seam_core::seam!(
    /// `relation->rd_rel->relkind` for the `SetMatViewPopulatedState` assert.
    pub fn relation_get_relkind(rel: Oid) -> PgResult<i8>
);
seam_core::seam!(
    /// `RelationGetRelid(relation)`.
    pub fn relation_get_relid(rel: Oid) -> PgResult<Oid>
);
seam_core::seam!(
    /// `RelationGetRelationName(rel)` — the bare relation name (for the
    /// match-merge error messages, which use the unqualified name).
    pub fn relation_get_relname(rel: Oid) -> PgResult<String>
);
seam_core::seam!(
    /// `quote_qualified_identifier(get_namespace_name(RelationGetNamespace(rel)),
    /// RelationGetRelationName(rel))` — the schema-qualified name for the SQL.
    pub fn quote_qualified_relname(rel: Oid) -> PgResult<String>
);
seam_core::seam!(
    /// `RelationGetNumberOfAttributes(rel)` (`relnatts`).
    pub fn relation_num_attrs(rel: Oid) -> PgResult<i16>
);
seam_core::seam!(
    /// Read the relcache fields + first-rewrite-rule shape `RefreshMatViewByOid`
    /// branches on (matview.c 184-283; see [`MatViewRelInfo`]).
    pub fn matview_rel_info(rel: Oid) -> PgResult<MatViewRelInfo>
);
seam_core::seam!(
    /// `linitial_node(Query, rule->actions)` — the matview's stored `dataQuery`.
    pub fn matview_data_query(rel: Oid) -> PgResult<QueryHandle>
);
seam_core::seam!(
    /// Read the `pg_index` relcache fields `is_usable_unique_index` inspects (the
    /// predicate logic stays in-crate; see [`IndexUsabilityInfo`]).
    pub fn index_usability_info(index_rel: Oid) -> PgResult<IndexUsabilityInfo>
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
    /// `CheckTableNotInUse(rel, stmt_kind)`.
    pub fn check_table_not_in_use(rel: Oid, stmt_kind: String) -> PgResult<()>
);
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

// --- pgstat --------------------------------------------------------------------

seam_core::seam!(
    /// `pgstat_count_truncate(matviewRel)`.
    pub fn pgstat_count_truncate(rel: Oid) -> PgResult<()>
);
seam_core::seam!(
    /// `pgstat_count_heap_insert(matviewRel, n)`.
    pub fn pgstat_count_heap_insert(rel: Oid, n: u64) -> PgResult<()>
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
seam_core::seam!(
    /// `PushCopiedSnapshot(GetActiveSnapshot()); UpdateActiveSnapshotCommandId();`
    pub fn push_copied_snapshot_and_bump() -> PgResult<()>
);
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
seam_core::seam!(
    /// `PopActiveSnapshot()`.
    pub fn pop_active_snapshot() -> PgResult<()>
);

// --- transientrel_* DestReceiver: the table-AM bulk-insert flush ---------------
//
// The `DR_transientrel` receiver itself is now owned in-crate by
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
    /// Errors surface here; quals are returned in index-key order.
    pub fn index_match_merge_quals(
        index_rel: Oid,
        matview_rel: Oid,
    ) -> PgResult<Vec<MatchMergeQual>>
);
seam_core::seam!(
    /// `generate_operator_clause(&buf, leftop, leftType, op, rightop, rightType)`
    /// -> the appended clause text (the in-crate code controls placement / the
    /// leading `" AND "`).
    pub fn generate_operator_clause(qual: MatchMergeQual) -> PgResult<String>
);
