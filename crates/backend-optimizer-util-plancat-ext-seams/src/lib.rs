#![forbid(unsafe_code)]
#![allow(non_snake_case)]

//! Outward seam declarations for the not-yet-ported externals that
//! `optimizer/util/plancat.c` calls and that are not already declared by another
//! `-seams` crate.
//!
//! These belong to several distinct owners (rewriteManip.c `ChangeVarNodes`,
//! relcache.c index-info detoasting `RelationGetIndexExpressions`/`Predicate`/
//! `AttOptions`/`ParallelWorkers`/`PartitionKey`/`PartitionQual`, the index AM
//! routine vtable + `index_can_return`/`amgettreeheight`, the table-AM
//! `table_relation_estimate_size` dispatch + `scan_*` capability probes, the
//! FDW `GetForeignServerIdByRelId`/`GetFdwRoutineForRelation`, the syscache
//! reads for `pg_proc`/`pg_statistic_ext`/`pg_statistic_ext_data` +
//! `statext_is_kind_built`, the catalog scans `RelationGetStatExtList`/
//! `RelationGetFKeyList`/`get_constraint_index`, the node ops `copyObject`/
//! `stringToNode`/`expandRTE`/`expand_generated_columns_in_expr`/
//! `expression_planner`/`fix_opfuncids`/`pull_varattnos`/`equal`, the
//! arena-level qual transforms `eval_const_expressions`/`canonicalize_qual`/
//! `make_ands_implicit`/`contain_mutable_functions`/`predicate_refuted_by`, the
//! `PartitionDirectory` infrastructure, the `fmgr_info_copy` + `OidFunctionCall*`
//! function-manager calls, the trigger-descriptor + generated-column reads, and
//! the transaction/recovery/GUC state probes) that are not ported (or whose
//! relevant surface is not declared) at the point plancat.c lands.
//!
//! They are homed here in a single consumer-side seam crate with NO owner
//! directory, so each call panics loudly until the real owner lands ("mirror PG
//! and panic"); the owning crates install their own once ported. The
//! `every_declared_seam_is_installed_by_its_owner` guard skips this crate
//! because no `backend-optimizer-util-plancat-ext` owner directory exists.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use types_core::primitive::{AttrNumber, BlockNumber, Index, Oid};
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{CmdType, NodeId, PlannerInfo, RelId, Relids};

/* ==========================================================================
 * Per-index raw catalog descriptor (relcache index-info detoast).
 *
 * `get_relation_info`'s index loop reads a large amount of catalog state that
 * the trimmed `types_rel::Relation`'s `rd_index`/`rd_indam`/`rd_op*` do not
 * carry in a planner-ready form: `indisvalid`/`indcheckxmin`/`indexrelid`/
 * `indnatts`/the full `indkey.values[]`/`indisexclusion`, the full
 * `IndexAmRoutine` capability vtable, and per-column opfamily/opcintype/
 * collation/indoption arrays. This descriptor mirrors exactly the fields the C
 * reads from `index_open(indexoid, lmode)` + `indexRelation->rd_index` +
 * `->rd_indam` + `->rd_opfamily`/`->rd_opcintype`/`->rd_indcollation`/
 * `->rd_indoption`, so that the IndexOptInfo construction logic runs in plancat
 * with real planner logic over real catalog data.
 * ======================================================================== */

/// One index's raw catalog data, as `get_relation_info` reads it from the
/// relcache (mirrors `index_open` + `rd_index`/`rd_indam`/`rd_op*`).
#[derive(Clone, Debug, Default)]
pub struct IndexCatInfo {
    /// `index->indexrelid` — the index relation OID.
    pub indexrelid: Oid,
    /// `RelationGetForm(indexRelation)->reltablespace`.
    pub reltablespace: Oid,
    /// `indexRelation->rd_rel->relam`.
    pub relam: Oid,
    /// `indexRelation->rd_rel->relkind == RELKIND_PARTITIONED_INDEX`.
    pub is_partitioned: bool,
    /// `index->indisvalid`.
    pub indisvalid: bool,
    /// `index->indcheckxmin` (HOT xmin recheck needed).
    pub indcheckxmin: bool,
    /// when `indcheckxmin`, whether `HeapTupleHeaderGetXmin(rd_indextuple) <
    /// TransactionXmin` (i.e. the index is already usable).
    pub indcheckxmin_passes: bool,
    /// `index->indnatts` (`info->ncolumns`).
    pub indnatts: i32,
    /// `index->indnkeyatts` (`info->nkeycolumns`).
    pub indnkeyatts: i32,
    /// `index->indkey.values[0..ncolumns]` (0 = expression column).
    pub indkey: Vec<i32>,
    /// `index->indisunique`.
    pub indisunique: bool,
    /// `index->indisexclusion`.
    pub indisexclusion: bool,
    /// `index->indnullsnotdistinct`.
    pub indnullsnotdistinct: bool,
    /// `index->indimmediate`.
    pub indimmediate: bool,
    /// per-key `indexRelation->rd_opfamily[i]` (length `nkeycolumns`).
    pub opfamily: Vec<Oid>,
    /// per-key `indexRelation->rd_opcintype[i]`.
    pub opcintype: Vec<Oid>,
    /// per-key `indexRelation->rd_indcollation[i]`.
    pub indcollation: Vec<Oid>,
    /// per-key `indexRelation->rd_indoption[i]`.
    pub indoption: Vec<i16>,
    /// per-column `index_can_return(indexRelation, i + 1)` (length `ncolumns`).
    pub canreturn: Vec<bool>,
    /// opclass options (`RelationGetIndexAttOptions(indexRelation, true)`); the
    /// planner only carries presence — modeled as `true` when any column has
    /// non-default opclass options.
    pub has_opclassoptions: bool,
    /* rd_indam capability flags (NULL for partitioned indexes) */
    pub amcanorder: bool,
    pub amcanorderbyop: bool,
    pub amoptionalkey: bool,
    pub amsearcharray: bool,
    pub amsearchnulls: bool,
    pub amcanparallel: bool,
    /// `amroutine->amgettuple != NULL`.
    pub amhasgettuple: bool,
    /// `amroutine->amgetbitmap != NULL && rel->rd_tableam->scan_bitmap_next_tuple
    /// != NULL` — the table-AM half of this AND is supplied as
    /// `table_has_scan_bitmap`.
    pub amhasgetbitmap: bool,
    /// `amroutine->ammarkpos != NULL && amroutine->amrestrpos != NULL`.
    pub amcanmarkpos: bool,
    /// `amroutine->amgettreeheight != NULL`.
    pub amhasgettreeheight: bool,
}

/// One cached foreign-key, as `get_relation_foreign_keys` reads it from
/// `RelationGetFKeyList` (`ForeignKeyCacheInfo`).
#[derive(Clone, Debug, Default)]
pub struct CachedFkInfo {
    /// `cachedfk->conrelid` — the referencing (FK) table OID.
    pub conrelid: Oid,
    /// `cachedfk->confrelid` — the referenced (PK) table OID.
    pub confrelid: Oid,
    /// `cachedfk->conenforced`.
    pub conenforced: bool,
    /// `cachedfk->nkeys`.
    pub nkeys: i32,
    /// `cachedfk->conkey[0..nkeys]`.
    pub conkey: Vec<AttrNumber>,
    /// `cachedfk->confkey[0..nkeys]`.
    pub confkey: Vec<AttrNumber>,
    /// `cachedfk->conpfeqop[0..nkeys]`.
    pub conpfeqop: Vec<Oid>,
}

/// The portion of a relation's `rd_att->constr` `get_relation_constraints` reads
/// for one check constraint that has been fully validated.
#[derive(Clone, Debug, Default)]
pub struct CheckConstraintInfo {
    /// `constr->check[i].ccbin` — the serialized constraint expression.
    pub ccbin: String,
    /// `constr->check[i].ccnoinherit`.
    pub ccnoinherit: bool,
}

/* ==========================================================================
 * Seam declarations.
 * ======================================================================== */

/* ---- rewriteManip.c -------------------------------------------------- */

seam_core::seam!(
    /// `ChangeVarNodes((Node *) exprs, 1, varno, 0)` (rewriteManip.c) — re-stamp
    /// every Var of relid `1` in the arena-resident node list to `varno`,
    /// returning the (possibly new) node handles. Used to fix index expressions /
    /// predicates / constraint expressions / partition exprs to the parent rel's
    /// varno.
    pub fn change_var_nodes(
        root: &mut PlannerInfo,
        nodes: &[NodeId],
        rt_index: i32,
        new_index: i32,
    ) -> Vec<NodeId>
);

/* ---- relcache.c index-info detoasting -------------------------------- */

seam_core::seam!(
    /// Open the index `indexoid` with `lmode` and extract everything
    /// `get_relation_info` reads from it into a planner-ready [`IndexCatInfo`].
    /// The table-AM half of `amhasgetbitmap` is supplied separately via
    /// `table_has_scan_bitmap`. `Err` carries the `index_open` ereport.
    pub fn get_index_cat_info(indexoid: Oid, lmode: i32) -> PgResult<IndexCatInfo>
);
seam_core::seam!(
    /// `RelationGetIndexExpressions(indexRelation)` (relcache.c) — the index's
    /// expression columns as fresh arena node handles, in indkey order.
    pub fn get_index_expressions(root: &mut PlannerInfo, indexoid: Oid) -> PgResult<Vec<NodeId>>
);
seam_core::seam!(
    /// `RelationGetIndexPredicate(indexRelation)` (relcache.c) — the partial
    /// index predicate as fresh arena node handles (empty if not partial).
    pub fn get_index_predicate(root: &mut PlannerInfo, indexoid: Oid) -> PgResult<Vec<NodeId>>
);
seam_core::seam!(
    /// `amroutine->amgettreeheight(indexRelation)` (index AM) — the index tree
    /// height; only called when `IndexCatInfo::amhasgettreeheight` is true.
    pub fn index_get_tree_height(indexoid: Oid) -> PgResult<i32>
);
seam_core::seam!(
    /// `RelationGetNumberOfBlocks(indexRelation)` (bufmgr.c) for an index.
    pub fn index_number_of_blocks(indexoid: Oid) -> PgResult<BlockNumber>
);
seam_core::seam!(
    /// `RelationGetParallelWorkers(relation, -1)` (rel.h) — the parallel_workers
    /// reloption, or -1 if not set.
    pub fn relation_parallel_workers(relid: Oid) -> PgResult<i32>
);
seam_core::seam!(
    /// `relation->rd_tableam->scan_bitmap_next_tuple != NULL` — whether the
    /// relation's table AM supports bitmap scans (the table-AM half of
    /// `info->amhasgetbitmap`).
    pub fn table_has_scan_bitmap(relid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `relation->rd_tableam->scan_set_tidrange != NULL &&
    /// scan_getnextslot_tidrange != NULL` — whether the table AM supports TID
    /// range scans (sets `AMFLAG_HAS_TID_RANGE`).
    pub fn table_has_tid_range(relid: Oid) -> PgResult<bool>
);

/* ---- table-AM size estimation --------------------------------------- */

seam_core::seam!(
    /// `table_relation_estimate_size(rel, attr_widths, &pages, &tuples,
    /// &allvisfrac)` (tableam.h dispatch) for a `RELKIND_HAS_TABLE_AM` relation.
    /// `attr_widths` (when supplied) is the `[0..=natts]` cache the callback
    /// fills. Returns `(pages, tuples, allvisfrac)`; `Err` carries the smgr/
    /// syscache ereports.
    pub fn table_relation_estimate_size(
        relid: Oid,
        attr_widths: Option<&mut [i32]>,
    ) -> PgResult<(BlockNumber, f64, f64)>
);

/* ---- foreign tables (fdwapi.h / foreign.c) -------------------------- */

seam_core::seam!(
    /// `GetForeignServerIdByRelId(RelationGetRelid(relation))` (foreign.c).
    pub fn get_foreign_server_id_by_rel_id(relid: Oid) -> PgResult<Oid>
);
seam_core::seam!(
    /// `GetFdwRoutineForRelation(relation, true) != NULL` (foreign.c) — whether
    /// the foreign table has an FDW routine (the planner stores presence in
    /// `RelOptInfo::has_fdwroutine`).
    pub fn rel_has_fdwroutine(relid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `restrict_nonsystem_relation_kind & RESTRICT_RELKIND_FOREIGN_TABLE`
    /// (guc) — whether access to non-system foreign tables is restricted.
    pub fn foreign_table_access_restricted() -> bool
);

/* ---- syscache: pg_proc cost/rows support (selfuncs.c surface) ------- */

/* ---- selectivity fmgr dispatch (plancat.c bodies) ------------------- */

seam_core::seam!(
    /// `DatumGetFloat8(OidFunctionCall4Coll(oprrest, inputcollid, root,
    /// operatorid, args, varRelid))` (fmgr.c): invoke a restriction-selectivity
    /// estimator. `args` are arena node handles. `Err` carries the fmgr ereport.
    pub fn call_oprrest<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        oprrest: Oid,
        operatorid: Oid,
        args: &[NodeId],
        inputcollid: Oid,
        var_relid: i32,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// `DatumGetFloat8(OidFunctionCall5Coll(oprjoin, inputcollid, root,
    /// operatorid, args, jointype, sjinfo))` (fmgr.c): invoke a
    /// join-selectivity estimator. `sjinfo` is passed by its node handle.
    pub fn call_oprjoin<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        oprjoin: Oid,
        operatorid: Oid,
        args: &[NodeId],
        inputcollid: Oid,
        jointype: i16,
        sjinfo: Option<&types_pathnodes::SpecialJoinInfo>,
    ) -> PgResult<f64>
);
seam_core::seam!(
    /// The `function_selectivity` body's `SupportRequestSelectivity` dispatch
    /// over `get_func_support(funcid)` — returns `Some(sel)` on a successful
    /// support-function reply, `None` to fall back to the historical 0.3333333.
    pub fn call_func_selectivity_support<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        funcid: Oid,
        args: &[NodeId],
        inputcollid: Oid,
        is_join: bool,
        var_relid: i32,
        jointype: i16,
        sjinfo: Option<&types_pathnodes::SpecialJoinInfo>,
    ) -> PgResult<Option<f64>>
);

/* ---- syscache: extended statistics --------------------------------- */

seam_core::seam!(
    /// `RelationGetStatExtList(relation)` (relcache.c) — OIDs of the relation's
    /// statistics objects.
    pub fn get_stat_ext_list(relid: Oid) -> PgResult<Vec<Oid>>
);
seam_core::seam!(
    /// For one `pg_statistic_ext` row (`statOid`), return the covered-column
    /// attnums (`stxkeys`) and the const-folded, opfuncid-fixed expression list
    /// (`stxexprs`) as fresh arena node handles. Mirrors the
    /// `SearchSysCache1(STATEXTOID)` + `eval_const_expressions` + `fix_opfuncids`
    /// body of `get_relation_statistics`. `Err` carries the cache-lookup elog.
    pub fn get_stat_ext_keys_exprs(
        root: &mut PlannerInfo,
        stat_oid: Oid,
    ) -> PgResult<(Vec<i32>, Vec<NodeId>)>
);
seam_core::seam!(
    /// `get_relation_statistics_worker(stainfos, rel, statOid, inh, keys, exprs)`
    /// inner half: for the `pg_statistic_ext_data` row keyed by `(statOid, inh)`,
    /// return the `stxdinherit` flag and which statistics kinds are built (in the
    /// fixed order NDISTINCT, DEPENDENCIES, MCV, EXPRESSIONS), or `None` if no
    /// such data row exists. Mirrors `SearchSysCache2(STATEXTDATASTXOID)` +
    /// `statext_is_kind_built`.
    pub fn get_stat_ext_data_kinds(
        stat_oid: Oid,
        inh: bool,
    ) -> PgResult<Option<StatExtDataKinds>>
);

/// The built-kinds reply for one `pg_statistic_ext_data` row.
#[derive(Clone, Debug, Default)]
pub struct StatExtDataKinds {
    /// `dataForm->stxdinherit`.
    pub stxdinherit: bool,
    /// kinds built, each in `STATS_EXT_*` order; the `char kind` of the
    /// resulting `StatisticExtInfo`s.
    pub kinds: Vec<i8>,
}

/* ---- get_relation_constraints helpers ------------------------------ */

seam_core::seam!(
    /// The relation's validated, enforced check constraints (`rd_att->constr->
    /// check[i]` where `ccvalid`), as deserialized [`CheckConstraintInfo`]s, in
    /// catalog order. Empty if the relation has no `TupleConstr`.
    pub fn get_check_constraints(relid: Oid) -> PgResult<Vec<CheckConstraintInfo>>
);
seam_core::seam!(
    /// `constr->has_not_null` (rd_att->constr) for the relation.
    pub fn relation_has_not_null(relid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `stringToNode(ccbin)` then const-fold + canonicalize-qual + ChangeVarNodes
    /// + make_ands_implicit a single check-constraint string into arena clause
    /// handles, with Vars stamped to `varno`. Mirrors the per-constraint body of
    /// `get_relation_constraints`. `Err` carries the parse/fold ereport.
    pub fn process_check_constraint(
        root: &mut PlannerInfo,
        ccbin: &str,
        varno: i32,
    ) -> PgResult<Vec<NodeId>>
);
seam_core::seam!(
    /// `expand_generated_columns_in_expr((Node *) result, relation, varno)`
    /// (rewriteHandler.c) over the arena clause list.
    pub fn expand_generated_columns_in_expr(
        root: &mut PlannerInfo,
        nodes: &[NodeId],
        relid: Oid,
        varno: i32,
    ) -> PgResult<Vec<NodeId>>
);

/* ---- relation_excluded_by_constraints qual ops --------------------- */

seam_core::seam!(
    /// `contain_mutable_functions((Node *) clause)` (clauses.c) over an arena
    /// node handle.
    pub fn contain_mutable_functions(root: &PlannerInfo, node: NodeId) -> PgResult<bool>
);
seam_core::seam!(
    /// `predicate_refuted_by(predicate_clauses, restriction_clauses, weak)`
    /// (predtest.c) over arena clause handles. Not declared by predtest-seams
    /// (which only declares `predicate_implied_by`).
    pub fn predicate_refuted_by(
        root: &PlannerInfo,
        predicate_clauses: &[NodeId],
        restriction_clauses: &[NodeId],
        weak: bool,
    ) -> bool
);
seam_core::seam!(
    /// Is the arena node a `Const` whose value is NULL or boolean false?
    /// (`IsA(clause, Const) && (constisnull || !DatumGetBool(constvalue))`) —
    /// the constant-FALSE-or-NULL restriction test.
    pub fn const_is_false_or_null(root: &PlannerInfo, node: NodeId) -> bool
);

/* ---- build_physical_tlist -------------------------------------------- */

seam_core::seam!(
    /// `expandRTE(rte, varno, 0, VAR_RETURNING_DEFAULT, -1, true, NULL,
    /// &colvars)` (parse_relation.c) for the non-relation RTE kinds: returns the
    /// per-column Var nodes as arena handles, OR `None` if a non-Var (dropped
    /// column) is encountered and the caller must punt. Mirrors the
    /// build_physical_tlist loop over `colvars`.
    pub fn expand_rte_physical_tlist<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rti: Index,
    ) -> PgResult<Option<Vec<NodeId>>>
);
seam_core::seam!(
    /// The subquery RTE's targetlist for build_physical_tlist: each entry is
    /// `(makeVarFromTargetEntry(varno, tle), tle->resno, tle->resjunk)`. Returns
    /// the constructed Var handle + resno + resjunk per output column.
    pub fn subquery_physical_tlist<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
        rti: Index,
    ) -> PgResult<Vec<(NodeId, AttrNumber, bool)>>
);

/* ---- triggers / generated columns (has_*_triggers etc.) ------------- */

seam_core::seam!(
    /// `has_row_triggers` trigdesc probe (commands/trigger.c): whether the
    /// relation has a row-level trigger for `event`.
    pub fn relation_has_row_triggers(relid: Oid, event: CmdType) -> PgResult<bool>
);
seam_core::seam!(
    /// `has_transition_tables` trigdesc probe.
    pub fn relation_has_transition_tables(relid: Oid, event: CmdType) -> PgResult<bool>
);
seam_core::seam!(
    /// `tupdesc->constr && tupdesc->constr->has_generated_stored` for the
    /// relation (`has_stored_generated_columns`).
    pub fn relation_has_stored_generated_columns(relid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// `get_dependent_generated_columns` body: for each STORED GENERATED column
    /// whose defining expression (`pull_varattnos`) overlaps `target_cols`,
    /// return its `adnum - FirstLowInvalidHeapAttributeNumber` offset attno. The
    /// caller folds these into a `Relids`.
    pub fn dependent_generated_columns(
        relid: Oid,
        target_cols: &Relids,
    ) -> PgResult<Vec<i32>>
);

/* ---- partitioning (set_relation_partition_info) -------------------- */

seam_core::seam!(
    /// `set_relation_partition_info(root, rel, relation)` (plancat.c) — sets
    /// `rel->part_scheme`/`boundinfo`/`nparts`/`partexprs`/`nullable_partexprs`/
    /// `partition_qual` for an inheritance-parent partitioned table, creating the
    /// `PartitionDirectory` on `root->glob` if needed and finding/creating the
    /// shared `PartitionScheme` on `root->part_schemes`. Bundled because it spans
    /// the unported `CreatePartitionDirectory`/`PartitionDirectoryLookup`,
    /// `RelationGetPartitionKey`/`Qual`, `fmgr_info_copy`, `copyObject`,
    /// `expression_planner` and the lifetime-free `PartitionScheme`/`boundinfo`
    /// modelling.
    pub fn set_relation_partition_info(
        root: &mut PlannerInfo,
        rel: RelId,
        relid: Oid,
    ) -> PgResult<()>
);
seam_core::seam!(
    /// `set_baserel_partition_constraint(relation, rel)` (plancat.c) — set
    /// `rel->partition_qual` from `RelationGetPartitionQual` (const-folded via
    /// `expression_planner`, Vars stamped to `rel->relid`). Used by
    /// `get_relation_constraints` when `include_partition` and the rel is a
    /// partition.
    pub fn set_baserel_partition_constraint(root: &mut PlannerInfo, rel: RelId) -> PgResult<()>
);

/* ---- transaction / recovery / catalog state ------------------------ */

seam_core::seam!(
    /// `IgnoreSystemIndexes` (guc) `&& IsSystemRelation(relation)` (catalog.c)
    /// for the relation — whether to ignore its indexes.
    pub fn ignore_system_indexes_for(relid: Oid) -> PgResult<bool>
);

/* ==========================================================================
 * Additional descriptors + seams for the remaining unported reads.
 * ======================================================================== */

/// One `InferenceElem` of an `ON CONFLICT` inference specification.
#[derive(Clone, Debug, Default)]
pub struct InferenceElemInfo {
    /// `elem->expr` — the inference expression as an arena node handle.
    pub expr: NodeId,
    /// `elem->infercollid`.
    pub infercollid: Oid,
    /// `elem->inferopclass`.
    pub inferopclass: Oid,
}

/// The `root->parse->onConflict` data `infer_arbiter_indexes` reads.
#[derive(Clone, Debug, Default)]
pub struct OnConflictInfo {
    /// `onconflict->arbiterElems`.
    pub arbiter_elems: Vec<InferenceElemInfo>,
    /// `onconflict->constraint`.
    pub constraint: Oid,
    /// `onconflict->action == ONCONFLICT_UPDATE`.
    pub action_is_update: bool,
    /// `(List *) onconflict->arbiterWhere` — arena clause handles.
    pub arbiter_where: Vec<NodeId>,
}

/// The per-index catalog data `infer_arbiter_indexes` reads from `index_open`.
#[derive(Clone, Debug, Default)]
pub struct InferIndexInfo {
    /// `idxForm->indexrelid`.
    pub indexrelid: Oid,
    /// `idxForm->indisvalid`.
    pub indisvalid: bool,
    /// `idxForm->indisunique`.
    pub indisunique: bool,
    /// `idxForm->indisexclusion`.
    pub indisexclusion: bool,
    /// `idxForm->indnkeyatts`.
    pub indnkeyatts: i32,
    /// `idxRel->rd_index->indkey.values[0..indnkeyatts]`.
    pub indkey: Vec<AttrNumber>,
    /// `RelationGetIndexExpressions(idxRel)` — arena handles (pre-ChangeVarNodes).
    pub idx_exprs: Vec<NodeId>,
    /// `RelationGetIndexPredicate(idxRel)` — arena handles (pre-ChangeVarNodes).
    pub idx_predicate: Vec<NodeId>,
}

seam_core::seam!(
    /// `root->parse->onConflict` (`infer_arbiter_indexes`). `None` when the
    /// query has no `ON CONFLICT` clause. Resolves the owned `OnConflictExpr`
    /// through the planner-run query store (`run.resolve(root.parse)`) and interns
    /// the arbiter element / arbiter-WHERE sub-trees into `root`'s node arena, so
    /// the returned [`InferenceElemInfo::expr`] / [`OnConflictInfo::arbiter_where`]
    /// are arena [`NodeId`] handles the consumer reads via `root.node(..)`.
    pub fn parse_onconflict<'mcx>(
        run: &PlannerRun<'mcx>,
        root: &mut PlannerInfo,
    ) -> PgResult<Option<OnConflictInfo>>
);
seam_core::seam!(
    /// `root->parse->resultRelation` (parsetree) — the result RT index. Resolved
    /// through the planner-run query store (`run.resolve(root.parse)`), so the
    /// resolver threads as a parameter like the `rte_*` projections.
    pub fn parse_result_relation<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo) -> i32
);
seam_core::seam!(
    /// `root->simple_rte_array[rti]->rellockmode` (rte) — the lock mode for the
    /// RTE at the given RT index. Resolved through the planner-run RTE store
    /// (`planner_rt_fetch(run, root, rti)`) exactly as the other `rte_*`
    /// projections are.
    pub fn rte_rellockmode<'mcx>(run: &PlannerRun<'mcx>, root: &PlannerInfo, rti: Index) -> i32
);
seam_core::seam!(
    /// `RelationGetIndexList(relation)` returned as a lifetime-free `Vec<Oid>`
    /// (relcache.c) — the relation's index OIDs.
    pub fn relation_get_index_list_oids(relid: Oid) -> PgResult<Vec<Oid>>
);
seam_core::seam!(
    /// `RelationGetFKeyList(relation)` (relcache.c) — the relation's cached
    /// foreign keys.
    pub fn get_relation_fkey_list(relid: Oid) -> PgResult<Vec<CachedFkInfo>>
);
seam_core::seam!(
    /// `index_open(indexoid, rellockmode)` + the `idxForm`/expr/predicate reads
    /// `infer_arbiter_indexes` needs (with the index left closed at return).
    pub fn get_infer_index_info(
        root: &mut PlannerInfo,
        indexoid: Oid,
        rellockmode: i32,
    ) -> PgResult<InferIndexInfo>
);
seam_core::seam!(
    /// `infer_collation_opclass_match(elem, idxRel, idxExprs)` (plancat.c) — the
    /// per-element collation/opclass match test against an opened index, given
    /// the (varno-stamped) index expressions. Returns true when the element has
    /// no collation/opclass requirement or at least one indexed attribute
    /// satisfies it. Opens the index by OID.
    pub fn infer_collation_opclass_match(
        root: &PlannerInfo,
        indexoid: Oid,
        elem: &InferenceElemInfo,
        idx_exprs: &[NodeId],
    ) -> PgResult<bool>
);
seam_core::seam!(
    /// `equal(a, b)` (nodes/equalfuncs.c) over two arena node handles.
    pub fn node_equal(root: &PlannerInfo, a: NodeId, b: NodeId) -> bool
);
seam_core::seam!(
    /// `SystemAttributeDefinition(indexkey)` (catalog/heap.c) for a negative
    /// index key (system column): returns `(atttypid, atttypmod, attcollation)`.
    pub fn system_attribute_definition(attno: i32) -> PgResult<(Oid, i32, Oid)>
);
seam_core::seam!(
    /// `SystemAttributeByName(attname)` (catalog/heap.c): if `attname` names a
    /// system column, return its `attnum` (a negative number); else `None`.
    /// Consumed by `specialAttNum` (parser/parse_relation.c).
    pub fn system_attribute_by_name(attname: &str) -> PgResult<Option<i32>>
);
seam_core::seam!(
    /// `estimate_rel_size(indexRelation, NULL, &pages, &tuples, &allvisfrac)`
    /// for a partial index in `get_relation_info` — the index variant of size
    /// estimation (opens the index by OID).
    pub fn table_relation_estimate_size_for_index(
        indexoid: Oid,
    ) -> PgResult<(BlockNumber, f64, f64)>
);
seam_core::seam!(
    /// `get_function_rows(root, funcid, node)` over a by-value SRF node — the
    /// clauses-seams contract (no `root`, no arena handle).
    pub fn get_function_rows_by_node(funcid: Oid, node: &Expr) -> PgResult<f64>
);
seam_core::seam!(
    /// `relation->rd_rel->relispartition` for the relation.
    pub fn relation_is_partition(relid: Oid) -> PgResult<bool>
);
seam_core::seam!(
    /// The relation's NOT NULL columns (`attnullability == ATTNULLABLE_VALID &&
    /// !attisdropped`) as `(attno, atttypid, atttypmod, attcollation)` tuples, in
    /// attno order — the data `get_relation_constraints` needs to build the
    /// `col IS NOT NULL` NullTests.
    pub fn not_null_attnums(relid: Oid) -> PgResult<Vec<(AttrNumber, Oid, i32, Oid)>>
);
