#![forbid(unsafe_code)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_else_if)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::too_many_arguments)]

//! Safe-Rust port of `src/backend/optimizer/util/plancat.c` (postgres-18.3):
//! routines for accessing the system catalogs on behalf of the planner —
//! `get_relation_info` and friends.
//!
//! # Arena model
//!
//! The C pointer graph is modelled over the
//! [`PlannerInfo`](::pathnodes::PlannerInfo) arena. `RelOptInfo`s are
//! [`RelId`] handles into `rel_arena`; expression/`TargetEntry`/
//! `ForeignKeyOptInfo`/`StatisticExtInfo` nodes are [`NodeId`] handles into
//! `node_arena` (resolved via `root.node`/`targetentry`/`foreign_key`/
//! `statistic_ext`). The two seams this unit OWNS (`estimate_rel_size` and
//! `get_rel_data_width`, declared in `backend-optimizer-util-plancat-seams`) take
//! an opened `rel::Relation`/`Oid` and are installed by [`init_seams`].
//!
//! Externals owned by other (sometimes unported) units cross through their
//! `*-seams` crates; the genuinely-absent ones (rewriteManip `ChangeVarNodes`,
//! the relcache index-info detoasting, the table-AM size dispatch, the FDW
//! probes, the syscache stat/proc reads, the `PartitionDirectory`
//! infrastructure, the fmgr selectivity calls, …) are declared in
//! `backend-optimizer-util-plancat-ext-seams` and panic until their owner lands
//! ("mirror PG and panic"). There is no `extern "C"`, no raw pointers.

extern crate alloc;

use alloc::vec::Vec;

use ::types_core::primitive::{AttrNumber, BlockNumber, Index, Oid};
use ::types_error::PgResult;
use ::nodes::primnodes::{Expr, NullTest, Var};
use ::pathnodes::planner_run::PlannerRun;
use pathnodes::{
    ForeignKeyOptInfo, IndexOptInfo, NodeId, PlannerInfo, RelId, Relids, StatisticExtInfo,
    TargetEntryNode, CMD_DELETE, CMD_INSERT, CMD_MERGE, CMD_UPDATE,
};

use plancat_ext_seams as ext;
use relnode_seams as bms;
use predtest_seams as predtest;
use rte_seams as rte;
use lsyscache_seams as lsyscache;
use syscache_seams as syscache_seams;

/* --------------------------------------------------------------------------
 * Constants mirrored from C headers (not present in the trimmed types crates).
 * ------------------------------------------------------------------------ */

const InvalidOid: Oid = 0;

const RELKIND_RELATION: u8 = b'r';
const RELKIND_TOASTVALUE: u8 = b't';
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_INDEX: u8 = b'i';
const RELKIND_FOREIGN_TABLE: u8 = b'f';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';
const RELKIND_SEQUENCE: u8 = b'S';

const RELPERSISTENCE_PERMANENT: u8 = b'p';

/// `ATTNULLABLE_VALID` / `ATTNULLABLE_UNKNOWN` (`access/tupdesc.h`).
const ATTNULLABLE_VALID: i8 = b'v' as i8;
const ATTNULLABLE_UNKNOWN: i8 = b'u' as i8;

/// `FirstLowInvalidHeapAttributeNumber` (access/sysattr.h) = -7.
const FirstLowInvalidHeapAttributeNumber: i32 = -7;

/// `FirstNormalObjectId` (access/transam.h).
const FirstNormalObjectId: Oid = 16384;

/// `BTREE_AM_OID` (catalog/pg_am_d.h).
const BTREE_AM_OID: Oid = 403;

/// `AMFLAG_HAS_TID_RANGE` (optimizer/plancat.h).
const AMFLAG_HAS_TID_RANGE: u32 = 0x01;

/// `COMPARE_LT` (`CompareType`, cmptype.h).
const COMPARE_LT: i32 = 1;

/// `INDOPTION_DESC` / `INDOPTION_NULLS_FIRST` (catalog/pg_index.h).
const INDOPTION_DESC: i16 = 0x0001;
const INDOPTION_NULLS_FIRST: i16 = 0x0002;

/// `BLCKSZ` (pg_config.h).
const BLCKSZ: i32 = ::types_core::primitive::BLCKSZ as i32;
/// `SizeOfPageHeaderData` (storage/bufpage.h) = 24.
const SizeOfPageHeaderData: i32 = 24;
/// `SizeofHeapTupleHeader` (access/htup_details.h) = 23.
const SizeofHeapTupleHeader: usize = 23;
/// `sizeof(ItemIdData)` (storage/itemid.h) = 4.
const SizeofItemIdData: i64 = 4;

/// `MaxAllocSize` (utils/memutils.h) = `0x3fffffff`.
const MaxAllocSize: i64 = 0x3fffffff;

/// `NoLock` (storage/lockdefs.h).
const NoLock: i32 = 0;

/// `RELOPT_BASEREL` (pathnodes.h `RelOptKind`).
use pathnodes::{RELOPT_BASEREL, RELOPT_OTHER_MEMBER_REL, RTE_RELATION};

/// constraint_exclusion GUC values (utils/guc.h).
const CONSTRAINT_EXCLUSION_OFF: i32 = 0;
const CONSTRAINT_EXCLUSION_ON: i32 = 1;
const CONSTRAINT_EXCLUSION_PARTITION: i32 = 2;

/// GUC `conf->variable` backing storage owned by this unit.
///
/// Mirrors the C global `int constraint_exclusion = CONSTRAINT_EXCLUSION_PARTITION;`
/// declared at `optimizer/util/plancat.c:58` (its enum value is read directly
/// from the GUC slot in `relation_excluded_by_constraints`, not the ControlFile).
/// Each backend owns its own copy, so this is a backend-private `thread_local!`
/// `Cell`, like `globals.c` scalars. The getter/setter are installed into the
/// GUC engine's `constraint_exclusion` slot from [`init_seams`].
mod guc_backing {
    use std::cell::Cell;

    thread_local! {
        /// `int constraint_exclusion = CONSTRAINT_EXCLUSION_PARTITION;`
        static CONSTRAINT_EXCLUSION: Cell<i32> =
            const { Cell::new(super::CONSTRAINT_EXCLUSION_PARTITION) };
    }

    #[inline]
    pub fn constraint_exclusion() -> i32 {
        CONSTRAINT_EXCLUSION.with(Cell::get)
    }

    #[inline]
    pub fn set_constraint_exclusion(value: i32) {
        CONSTRAINT_EXCLUSION.with(|c| c.set(value));
    }
}

/// `RELKIND_HAS_TABLE_AM(relkind)` (catalog/pg_class.h).
#[inline]
fn relkind_has_table_am(relkind: u8) -> bool {
    relkind == RELKIND_RELATION || relkind == RELKIND_TOASTVALUE || relkind == RELKIND_MATVIEW
}

/// `clamp_width_est` (costsize.c) — force a tuple-width estimate to a sane int32.
/// Inlined to avoid a costsize crate dependency cycle.
#[inline]
fn clamp_width_est(tuple_width: i64) -> i32 {
    if tuple_width > MaxAllocSize {
        return MaxAllocSize as i32;
    }
    tuple_width as i32
}

/* ==========================================================================
 * makeVar / makeTargetEntry helpers (nodes/makefuncs.c).
 *
 * These are small constructors; the repo's makefuncs-seams crate does not expose
 * `makeVar`/`makeTargetEntry`, so they are built inline against the arena.
 * ======================================================================== */

/// `makeVar(varno, varattno, vartype, vartypmod, varcollid, varlevelsup)`
/// (makefuncs.c) — build a `Var` `Expr` value.
fn make_var<'mcx>(
    varno: i32,
    varattno: AttrNumber,
    vartype: Oid,
    vartypmod: i32,
    varcollid: Oid,
    varlevelsup: Index,
) -> Expr<'mcx> {
    Expr::Var(Var {
        varno,
        varattno,
        vartype,
        vartypmod,
        varcollid,
        varnullingrels: Default::default(),
        varlevelsup,
        varnosyn: 0,
        varattnosyn: 0,
        varreturningtype: Default::default(),
        location: -1,
    })
}

/// `makeTargetEntry(expr, resno, resname, resjunk)` (makefuncs.c) — build a
/// `TargetEntryNode` over the arena `expr` handle.
fn make_target_entry(
    expr: NodeId,
    resno: AttrNumber,
    resname: Option<alloc::string::String>,
    resjunk: bool,
) -> TargetEntryNode {
    TargetEntryNode {
        expr,
        resno,
        resname,
        ressortgroupref: 0,
        resorigtbl: InvalidOid,
        resorigcol: 0,
        resjunk,
    }
}

/* ==========================================================================
 * get_relation_info
 * ======================================================================== */

/// `get_relation_info(root, relationObjectId, inhparent, rel)` (plancat.c) —
/// retrieve catalog information for a given relation into the `RelOptInfo`.
pub fn get_relation_info<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    relation_object_id: Oid,
    inhparent: bool,
    rel: RelId,
) -> PgResult<()> {
    let varno: Index = root.rel(rel).relid;

    // We need not lock the relation since it was already locked.
    let relcx = mcx::MemoryContext::new("get_relation_info relcache");
    let relation = table::table_open(relcx.mcx(), relation_object_id, NoLock)?;

    // C tests `if (!relation->rd_tableam)`: relations without a table AM can be
    // used only for the special-cased foreign/partitioned relkinds. The repo's
    // `Relation` does not expose `rd_tableam` directly here, so we mirror which
    // relkinds the relcache actually assigns an `rd_tableam` to. That set is the
    // `RELKIND_HAS_TABLE_AM` macro PLUS sequences: the relcache populates
    // `rd_tableam` for RELKIND_SEQUENCE (a sequence is a scannable heap; e.g.
    // `SELECT * FROM a_sequence` is valid), even though sequences are not in the
    // macro. Missing that arm rejected scans of sequences with a spurious
    // "cannot open relation".
    let relkind = relation.rd_rel.relkind;
    let has_table_am = relkind_has_table_am(relkind) || relkind == RELKIND_SEQUENCE;
    if !has_table_am
        && !(relkind == RELKIND_FOREIGN_TABLE || relkind == RELKIND_PARTITIONED_TABLE)
    {
        return Err(::types_error::PgError::error(alloc::format!(
            "cannot open relation \"{}\"",
            relation.rd_rel.relname.as_str()
        )));
    }

    // Temporary and unlogged relations are inaccessible during recovery.
    if relation.rd_rel.relpersistence != RELPERSISTENCE_PERMANENT
        && transam_xlog_seams::recovery_in_progress::call()
    {
        return Err(::types_error::PgError::error(
            "cannot access temporary or unlogged relations during recovery",
        ));
    }

    {
        let r = root.rel_mut(rel);
        r.min_attr = (FirstLowInvalidHeapAttributeNumber + 1) as AttrNumber;
        r.max_attr = relation.rd_att.natts as AttrNumber;
        r.reltablespace = relation.rd_rel.reltablespace;

        debug_assert!(r.max_attr >= r.min_attr);
        let span = (r.max_attr - r.min_attr + 1) as usize;
        r.attr_needed = alloc::vec![None; span];
        r.attr_widths = alloc::vec![0i32; span];
    }

    // Record which columns are defined as NOT NULL. Left unpopulated for
    // non-partitioned inheritance parents.
    if !inhparent || relkind == RELKIND_PARTITIONED_TABLE {
        for i in 0..relation.rd_att.natts as usize {
            let attr = relation.rd_att.compact_attr(i);
            debug_assert!(attr.attnullability != ATTNULLABLE_UNKNOWN);
            if attr.attnullability == ATTNULLABLE_VALID {
                // Per RemoveAttributeById(), dropped columns have attnotnull
                // unset, so we needn't check attisdropped here.
                debug_assert!(!attr.attisdropped);
                let nn = bms::relids_add_member::call(
                    root.rel_mut(rel).notnullattnums.take(),
                    (i + 1) as i32,
                );
                root.rel_mut(rel).notnullattnums = nn;
            }
        }
    }

    // Estimate relation size --- unless it's an inheritance parent.
    if !inhparent {
        // estimate_rel_size(relation, rel->attr_widths - rel->min_attr, ...):
        // the C passes a base-shifted pointer so that attr_widths[attno] indexes
        // by 1-based attno. We pass the [0..=natts] cache directly to the
        // table-AM estimator and let it fill attr_widths[attno].
        let min_attr = root.rel(rel).min_attr;
        let mut widths = core::mem::take(&mut root.rel_mut(rel).attr_widths);
        let (pages, tuples, allvisfrac) =
            estimate_rel_size_impl(&relation, Some(&mut widths), min_attr)?;
        let r = root.rel_mut(rel);
        r.attr_widths = widths;
        r.pages = pages;
        r.tuples = tuples;
        r.allvisfrac = allvisfrac;
    }

    // Retrieve the parallel_workers reloption, or -1 if not set.
    root.rel_mut(rel).rel_parallel_workers = ext::relation_parallel_workers::call(relation_object_id)?;

    // Make list of indexes.
    let hasindex = if (inhparent && relkind != RELKIND_PARTITIONED_TABLE)
        || ext::ignore_system_indexes_for::call(relation_object_id)?
    {
        false
    } else {
        relation.rd_rel.relhasindex
    };

    let mut indexinfos: Vec<IndexOptInfo> = Vec::new();
    if hasindex {
        let indexoidlist = ext::relation_get_index_list_oids::call(
            relation_object_id,
        )?;

        // For each index, get the same lock the executor will need.
        // C: `lmode = root->simple_rte_array[varno]->rellockmode`
        // (`planner_rt_fetch(varno, root)->rellockmode`).
        let lmode = ::pathnodes::planner_run::planner_rt_fetch(run, root, varno).rellockmode;

        // table-AM bitmap capability (the table-AM half of amhasgetbitmap).
        let table_bitmap = ext::table_has_scan_bitmap::call(relation_object_id)?;

        for &indexoid in indexoidlist.iter() {
            let cat = ext::get_index_cat_info::call(indexoid, lmode)?;

            // Ignore invalid indexes.
            if !cat.indisvalid {
                continue;
            }

            // If valid but not yet usable, ignore it and mark the plan transient.
            if cat.indcheckxmin && !cat.indcheckxmin_passes {
                if let Some(glob) = root.glob.as_mut() {
                    glob.transient_plan = true;
                }
                continue;
            }

            let ncolumns = cat.indnatts;
            let nkeycolumns = cat.indnkeyatts;

            let mut info = IndexOptInfo {
                indexoid: cat.indexrelid,
                reltablespace: cat.reltablespace,
                rel: Some(rel),
                ncolumns,
                nkeycolumns,
                indexkeys: cat.indkey.clone(),
                indexcollations: cat.indcollation.clone(),
                opfamily: cat.opfamily.clone(),
                opcintype: cat.opcintype.clone(),
                canreturn: cat.canreturn.clone(),
                relam: cat.relam,
                ..Default::default()
            };

            if !cat.is_partitioned {
                // We copy just the fields we need, not all of rd_indam.
                info.amcanorderbyop = cat.amcanorderbyop;
                info.amoptionalkey = cat.amoptionalkey;
                info.amsearcharray = cat.amsearcharray;
                info.amsearchnulls = cat.amsearchnulls;
                info.amcanparallel = cat.amcanparallel;
                info.amhasgettuple = cat.amhasgettuple;
                info.amhasgetbitmap = cat.amhasgetbitmap && table_bitmap;
                info.amcanmarkpos = cat.amcanmarkpos;
                // amcostestimate is a function pointer in C; presence is asserted
                // (Assert(info->amcostestimate != NULL)). The planner reaches it
                // only via the cost seams, so no field is carried here.

                // Fetch the ordering information for the index, if any.
                if info.relam == BTREE_AM_OID {
                    // btree: use its opfamily OIDs directly as sort opfamilies.
                    debug_assert!(cat.amcanorder);
                    info.sortopfamily = info.opfamily.clone();
                    info.reverse_sort = alloc::vec![false; nkeycolumns as usize];
                    info.nulls_first = alloc::vec![false; nkeycolumns as usize];
                    for i in 0..nkeycolumns as usize {
                        let opt = cat.indoption[i];
                        info.reverse_sort[i] = (opt & INDOPTION_DESC) != 0;
                        info.nulls_first[i] = (opt & INDOPTION_NULLS_FIRST) != 0;
                    }
                } else if cat.amcanorder {
                    // Map this index's "<" operators into btree.
                    info.sortopfamily = alloc::vec![InvalidOid; nkeycolumns as usize];
                    info.reverse_sort = alloc::vec![false; nkeycolumns as usize];
                    info.nulls_first = alloc::vec![false; nkeycolumns as usize];
                    let mut failed = false;
                    for i in 0..nkeycolumns as usize {
                        let opt = cat.indoption[i];
                        info.reverse_sort[i] = (opt & INDOPTION_DESC) != 0;
                        info.nulls_first[i] = (opt & INDOPTION_NULLS_FIRST) != 0;

                        let ltopr = lsyscache::get_opfamily_member_for_cmptype::call(
                            info.opfamily[i],
                            info.opcintype[i],
                            info.opcintype[i],
                            COMPARE_LT,
                        )?;
                        let mut mapped = false;
                        if ltopr != InvalidOid {
                            if let Some((opfamily, opcintype, cmptype)) =
                                lsyscache::get_ordering_op_properties::call(ltopr)?
                            {
                                if opcintype == info.opcintype[i] && cmptype == COMPARE_LT {
                                    info.sortopfamily[i] = opfamily;
                                    mapped = true;
                                }
                            }
                        }
                        if !mapped {
                            // Fail ... quietly treat index as unordered.
                            info.sortopfamily = Vec::new();
                            info.reverse_sort = Vec::new();
                            info.nulls_first = Vec::new();
                            failed = true;
                            break;
                        }
                    }
                    let _ = failed;
                } else {
                    info.sortopfamily = Vec::new();
                    info.reverse_sort = Vec::new();
                    info.nulls_first = Vec::new();
                }
            } else {
                info.amcanorderbyop = false;
                info.amoptionalkey = false;
                info.amsearcharray = false;
                info.amsearchnulls = false;
                info.amcanparallel = false;
                info.amhasgettuple = false;
                info.amhasgetbitmap = false;
                info.amcanmarkpos = false;
                info.sortopfamily = Vec::new();
                info.reverse_sort = Vec::new();
                info.nulls_first = Vec::new();
            }

            // Fetch the index expressions and predicate, if any; re-stamp varno.
            info.indexprs = ext::get_index_expressions::call(run.mcx(), root, indexoid)?;
            info.indpred = ext::get_index_predicate::call(run.mcx(), root, indexoid)?;
            if !info.indexprs.is_empty() && varno != 1 {
                info.indexprs = ext::change_var_nodes::call(run.mcx(), root, &info.indexprs, 1, varno as i32);
            }
            if !info.indpred.is_empty() && varno != 1 {
                info.indpred = ext::change_var_nodes::call(run.mcx(), root, &info.indpred, 1, varno as i32);
            }

            // Build targetlist using the completed indexprs data.
            info.indextlist = build_index_tlist(root, &info, &relation)?;

            info.indrestrictinfo = Vec::new(); // set later, in indxpath.c
            info.predOK = false; // set later, in indxpath.c
            info.unique = cat.indisunique;
            info.nullsnotdistinct = cat.indnullsnotdistinct;
            info.immediate = cat.indimmediate;
            info.hypothetical = false;

            // Estimate the index size.
            if !cat.is_partitioned {
                if info.indpred.is_empty() {
                    info.pages = ext::index_number_of_blocks::call(indexoid)?;
                    info.tuples = root.rel(rel).tuples;
                } else {
                    let (pages, tuples, _allvisfrac) =
                        ext::table_relation_estimate_size_for_index::call(indexoid)?;
                    info.pages = pages;
                    info.tuples = tuples;
                    if info.tuples > root.rel(rel).tuples {
                        info.tuples = root.rel(rel).tuples;
                    }
                }

                // Get tree height while we have the index open.
                if cat.amhasgettreeheight {
                    info.tree_height = ext::index_get_tree_height::call(indexoid)?;
                } else {
                    info.tree_height = -1;
                }
            } else {
                info.pages = 0;
                info.tuples = 0.0;
                info.tree_height = -1;
            }

            // We've historically used lcons() here (prepend).
            indexinfos.insert(0, info);
        }
    }

    root.rel_mut(rel).indexlist = indexinfos;

    let statlist = get_relation_statistics(run, root, rel, relation_object_id)?;
    root.rel_mut(rel).statlist = statlist;

    // Grab foreign-table info using the relcache.
    if relkind == RELKIND_FOREIGN_TABLE {
        // Check if access to foreign tables is restricted.
        if ext::foreign_table_access_restricted::call() {
            debug_assert!(relation_object_id >= FirstNormalObjectId);
            return Err(::types_error::PgError::error(
                "access to non-system foreign table is restricted",
            ));
        }
        let serverid = ext::get_foreign_server_id_by_rel_id::call(relation_object_id)?;
        let has_fdw = ext::rel_has_fdwroutine::call(relation_object_id)?;
        let r = root.rel_mut(rel);
        r.serverid = serverid;
        r.has_fdwroutine = has_fdw;
    } else {
        let r = root.rel_mut(rel);
        r.serverid = InvalidOid;
        r.has_fdwroutine = false;
        r.fdwroutine = None;
    }

    // Collect info about relation's foreign keys, if relevant.
    get_relation_foreign_keys(run, root, rel, relation_object_id, inhparent)?;

    // Collect info about functions implemented by the rel's table AM.
    if has_table_am && ext::table_has_tid_range::call(relation_object_id)? {
        root.rel_mut(rel).amflags |= AMFLAG_HAS_TID_RANGE;
    }

    // Collect info about relation's partitioning scheme, if any.
    if inhparent && relkind == RELKIND_PARTITIONED_TABLE {
        ext::set_relation_partition_info::call(run.mcx(), root, rel, relation_object_id)?;
    }

    // `table_close(relation, NoLock)` — a single unpin. The `relation` guard's
    // Drop is `relation_close(rel, NoLock)`, so consuming it here is the one
    // close (an additional by-OID `relation_close::call` would double-unpin and
    // drive `rd_refcnt` below the caller's outstanding pins — the inheritance
    // expansion path, which holds its own parent open across `get_relation_info`,
    // exposes that underflow).
    relation.close(NoLock)?;

    // (The C get_relation_info_hook is a plugin hook; no plugin hook surface in
    // this port.)

    Ok(())
}

/* ==========================================================================
 * get_relation_foreign_keys
 * ======================================================================== */

/// `get_relation_foreign_keys(root, rel, relation, inhparent)` (plancat.c) —
/// create `ForeignKeyOptInfo`s for relevant FKs and append to `root->fkey_list`.
fn get_relation_foreign_keys<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    relation_object_id: Oid,
    inhparent: bool,
) -> PgResult<()> {
    let rtable_len = rte::parse_rtable_len::call(run, root);

    // If it's not a baserel, we don't care about its FKs. Also skip if the query
    // references only a single relation.
    if root.rel(rel).reloptkind != RELOPT_BASEREL || rtable_len < 2 {
        return Ok(());
    }

    // If it's the parent of an inheritance tree, ignore its FKs.
    if inhparent {
        return Ok(());
    }

    let cachedfkeys =
        ext::get_relation_fkey_list::call(relation_object_id)?;

    let con_relid = root.rel(rel).relid;
    let rti_count = rtable_len as Index;

    for cachedfk in cachedfkeys.iter() {
        debug_assert_eq!(cachedfk.conrelid, relation_object_id);

        // Skip constraints currently not enforced.
        if !cachedfk.conenforced {
            continue;
        }

        // Scan to find other RTEs matching confrelid.
        for rti in 1..=rti_count {
            // Ignore if not the correct table.
            if rte::rte_rtekind::call(run, root, rti) != RTE_RELATION
                || rte::rte_relid::call(run, root, rti) != cachedfk.confrelid
            {
                continue;
            }
            // Ignore if it's an inheritance parent.
            if rte::rte_inh::call(run, root, rti) {
                continue;
            }
            // Ignore self-referential FKs.
            if rti == con_relid {
                continue;
            }

            // OK, make an entry.
            let nkeys = cachedfk.nkeys;
            let info = ForeignKeyOptInfo {
                con_relid,
                ref_relid: rti,
                nkeys,
                conkey: cachedfk.conkey.clone(),
                confkey: cachedfk.confkey.clone(),
                conpfeqop: cachedfk.conpfeqop.clone(),
                // zeroed/NIL by palloc0; filled by match_foreign_keys_to_quals:
                nmatched_ec: 0,
                nconst_ec: 0,
                nmatched_rcols: 0,
                nmatched_ri: 0,
                eclass: alloc::vec![None; nkeys as usize],
                fk_eclass_member: alloc::vec![None; nkeys as usize],
                rinfos: alloc::vec![alloc::vec::Vec::new(); nkeys as usize],
            };
            let nid = root.alloc_foreign_key(info);
            root.fkey_list.push(nid);
        }
    }

    Ok(())
}

/* ==========================================================================
 * infer_arbiter_indexes
 * ======================================================================== */

/// `infer_arbiter_indexes(root)` (plancat.c) — determine the unique indexes used
/// to arbitrate speculative insertion. Returns the matched index OIDs.
pub fn infer_arbiter_indexes<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
) -> PgResult<Vec<Oid>> {
    // Iteration state.
    let mut index_oid_from_constraint: Oid = InvalidOid;

    // Quickly return NIL for ON CONFLICT DO NOTHING without an inference spec or
    // named constraint.
    let onconflict = ext::parse_onconflict::call(run, root)?;
    let onconflict = match onconflict {
        Some(oc) => oc,
        None => return Ok(Vec::new()),
    };
    if onconflict.arbiter_elems.is_empty() && onconflict.constraint == InvalidOid {
        return Ok(Vec::new());
    }

    let varno = ext::parse_result_relation::call(run, root) as Index;
    let relid = rte::rte_relid::call(run, root, varno);
    let rellockmode = ext::rte_rellockmode::call(run, root, varno);

    // Build normalized/BMS representation of plain indexed attributes plus a
    // separate list of expression items.
    let mut infer_attrs: Relids = None;
    let mut infer_elems: Vec<NodeId> = Vec::new();
    for elem in onconflict.arbiter_elems.iter() {
        if !root.node(elem.expr).is_var() {
            infer_elems.push(elem.expr);
            continue;
        }
        let attno = match root.node(elem.expr) {
            Expr::Var(v) => v.varattno,
            _ => unreachable!(),
        };
        if attno == 0 {
            return Err(::types_error::PgError::error(
                "whole row unique index inference specifications are not supported",
            ));
        }
        infer_attrs = bms::relids_add_member::call(
            infer_attrs.take(),
            attno as i32 - FirstLowInvalidHeapAttributeNumber,
        );
    }

    // Lookup named constraint's index.
    if onconflict.constraint != InvalidOid {
        index_oid_from_constraint =
            lsyscache_seams::get_constraint_index::call(onconflict.constraint)?;
        if index_oid_from_constraint == InvalidOid {
            return Err(::types_error::PgError::error(
                "constraint in ON CONFLICT clause has no associated index",
            ));
        }
    }

    let mut results: Vec<Oid> = Vec::new();

    let index_list =
        ext::relation_get_index_list_oids::call(relid)?;

    for &indexoid in index_list.iter() {
        let idx = ext::get_infer_index_info::call(run.mcx(), root, indexoid, rellockmode)?;

        if !idx.indisvalid {
            continue;
        }

        // "ON constraint_name" variant.
        if index_oid_from_constraint == idx.indexrelid {
            if idx.indisexclusion && onconflict.action_is_update {
                return Err(::types_error::PgError::error(
                    "ON CONFLICT DO UPDATE not supported with exclusion constraints",
                ));
            }
            results.push(idx.indexrelid);
            return Ok(results);
        } else if index_oid_from_constraint != InvalidOid {
            continue;
        }

        // Conventional inference: skip non-unique indexes.
        if !idx.indisunique {
            continue;
        }
        // WITHOUT OVERLAPS "unique" constraints are exclusion constraints; skip.
        if idx.indisexclusion {
            continue;
        }

        // Build BMS representation of plain (non-expression) index attrs.
        let mut indexed_attrs: Relids = None;
        for &attno in idx.indkey.iter().take(idx.indnkeyatts as usize) {
            if attno != 0 {
                indexed_attrs = bms::relids_add_member::call(
                    indexed_attrs.take(),
                    attno as i32 - FirstLowInvalidHeapAttributeNumber,
                );
            }
        }
        // Non-expression attributes must match.
        if !bms::relids_equal::call(&indexed_attrs, &infer_attrs) {
            continue;
        }

        // Expression attributes (if any) must match.
        let mut idx_exprs = idx.idx_exprs.clone();
        if !idx_exprs.is_empty() && varno != 1 {
            idx_exprs = ext::change_var_nodes::call(run.mcx(), root, &idx_exprs, 1, varno as i32);
        }

        let mut matched = true;
        for elem in onconflict.arbiter_elems.iter() {
            // Ensure collation/opclass aspects match.
            if !ext::infer_collation_opclass_match::call(root, indexoid, elem, &idx_exprs)? {
                matched = false;
                break;
            }
            // Plain Vars don't factor into the expression-element count.
            if root.node(elem.expr).is_var() {
                continue;
            }
            // Element expression must appear in cataloged index definition (or
            // it specified its own collation/opclass).
            if elem.infercollid != InvalidOid
                || elem.inferopclass != InvalidOid
                || list_member_node(root, &idx_exprs, elem.expr)
            {
                continue;
            }
            matched = false;
            break;
        }
        if !matched {
            continue;
        }

        // Ensure index has no cataloged expressions missing from inferElems.
        if !list_difference_empty(root, &idx_exprs, &infer_elems) {
            continue;
        }

        // Partial index: predicate must be implied by the ON CONFLICT WHERE.
        let mut pred_exprs = idx.idx_predicate.clone();
        if !pred_exprs.is_empty() && varno != 1 {
            pred_exprs = ext::change_var_nodes::call(run.mcx(), root, &pred_exprs, 1, varno as i32);
        }
        let arbiter_where = onconflict.arbiter_where.clone();
        if !predtest::predicate_implied_by::call(root, &pred_exprs, &arbiter_where, false) {
            continue;
        }

        results.push(idx.indexrelid);
    }

    if results.is_empty() {
        return Err(::types_error::PgError::error(
            "there is no unique or exclusion constraint matching the ON CONFLICT specification",
        ));
    }

    Ok(results)
}

/// `list_member(idxExprs, elem->expr)` — node-equality membership over the arena
/// (clauses.c `equal`).
fn list_member_node(root: &PlannerInfo, list: &[NodeId], target: NodeId) -> bool {
    list.iter().any(|&n| ext::node_equal::call(root, n, target))
}

/// `list_difference(idxExprs, inferElems) == NIL` — true when every member of
/// `a` appears (by node-equality) in `b`.
fn list_difference_empty(root: &PlannerInfo, a: &[NodeId], b: &[NodeId]) -> bool {
    a.iter().all(|&x| b.iter().any(|&y| ext::node_equal::call(root, x, y)))
}

/* ==========================================================================
 * estimate_rel_size / get_rel_data_width (the two OWNED seams) +
 * get_relation_data_width
 * ======================================================================== */

/// `estimate_rel_size(rel, attr_widths, &pages, &tuples, &allvisfrac)`
/// (plancat.c). `attr_widths` (when supplied) is the `[0..=natts]` cache; the
/// `min_attr` shift mirrors the C `rel->attr_widths - rel->min_attr` base.
fn estimate_rel_size_impl(
    rel: &rel::Relation<'_>,
    attr_widths: Option<&mut [i32]>,
    min_attr: AttrNumber,
) -> PgResult<(BlockNumber, f64, f64)> {
    let relkind = rel.rd_rel.relkind;

    if relkind_has_table_am(relkind) {
        ext::table_relation_estimate_size::call(rel.rd_id, attr_widths)
    } else if relkind == RELKIND_INDEX {
        // It has storage, ok to call the smgr.
        let mut curpages = ext::index_number_of_blocks::call(rel.rd_id)?;

        // Report estimated # pages (the ORIGINAL count, before the metapage
        // discount below — C sets `*pages = curpages` here).
        let pages = curpages;

        // Quick exit if rel is clearly empty.
        if curpages == 0 {
            return Ok((pages, 0.0, 0.0));
        }

        // Coerce values in pg_class to more desirable types.
        let mut relpages = rel.rd_rel.relpages as BlockNumber;
        let reltuples = rel.rd_rel.reltuples as f64;
        let relallvisible = rel.rd_rel.relallvisible as BlockNumber;

        // Discount the metapage while estimating the number of tuples.
        if relpages > 0 {
            curpages -= 1;
            relpages -= 1;
        }

        let density: f64;
        if reltuples >= 0.0 && relpages > 0 {
            density = reltuples / relpages as f64;
        } else {
            // No data: estimate tuple width from attribute datatypes.
            let mut tuple_width = get_rel_data_width_impl(rel, attr_widths, min_attr)? as i64;
            tuple_width += maxalign(SizeofHeapTupleHeader) as i64;
            tuple_width += SizeofItemIdData;
            // note: integer division is intentional here
            density = ((BLCKSZ - SizeOfPageHeaderData) as i64 / tuple_width) as f64;
        }
        let tuples = rint(density * curpages as f64);

        let allvisfrac = if relallvisible == 0 || curpages == 0 {
            0.0
        } else if relallvisible as f64 >= curpages as f64 {
            1.0
        } else {
            relallvisible as f64 / curpages as f64
        };

        Ok((pages, tuples, allvisfrac))
    } else {
        // Just use whatever's in pg_class (foreign tables, sequences, ...).
        Ok((
            rel.rd_rel.relpages as BlockNumber,
            rel.rd_rel.reltuples as f64,
            0.0,
        ))
    }
}

/// `get_rel_data_width(rel, attr_widths)` (plancat.c) — estimate the average
/// width of (the data part of) the relation's tuples.
///
/// The C passes a base-shifted pointer `attr_widths - rel->min_attr` so that the
/// per-attribute cache is keyed by 1-based `attno`. This repo's value model has
/// no pointer arithmetic, so the cache array is the unshifted
/// `RelOptInfo::attr_widths` (length `max_attr - min_attr + 1`) and the entry for
/// `attno` lives at `attr_widths[attno - min_attr]` — exactly the index costsize
/// (`set_rel_width`) reads/writes. `min_attr` is `FirstLowInvalidHeapAttributeNumber
/// + 1` for any table.
fn get_rel_data_width_impl(
    rel: &rel::Relation<'_>,
    mut attr_widths: Option<&mut [i32]>,
    min_attr: AttrNumber,
) -> PgResult<i32> {
    let mut tuple_width: i64 = 0;
    let natts = rel.rd_att.natts;

    for i in 1..=natts {
        let att = rel.rd_att.attr((i - 1) as usize);
        if att.attisdropped {
            continue;
        }

        let ndx = (i as i32 - min_attr as i32) as usize;

        // Use previously cached data, if any.
        if let Some(aw) = attr_widths.as_deref() {
            if aw[ndx] > 0 {
                tuple_width += aw[ndx] as i64;
                continue;
            }
        }

        // This should match set_rel_width() in costsize.c.
        let mut item_width = lsyscache::get_attavgwidth::call(rel.rd_id, i as AttrNumber)?;
        if item_width <= 0 {
            item_width = lsyscache::get_typavgwidth::call(att.atttypid, att.atttypmod)?;
            debug_assert!(item_width > 0);
        }
        if let Some(aw) = attr_widths.as_deref_mut() {
            aw[ndx] = item_width;
        }
        tuple_width += item_width as i64;
    }

    Ok(clamp_width_est(tuple_width))
}

/// `get_relation_data_width(relid, attr_widths)` (plancat.c) — external API: open
/// the relcache entry, then `get_rel_data_width`.
pub fn get_relation_data_width(relid: Oid, attr_widths: &[i32], min_attr: AttrNumber) -> PgResult<i32> {
    let relcx = mcx::MemoryContext::new("get_relation_data_width relcache");
    let relation = table::table_open(relcx.mcx(), relid, NoLock)?;
    // C: `get_rel_data_width(relation, attr_widths)` is handed the caller's
    // `rel->attr_widths - rel->min_attr` base-shifted pointer (costsize.c:6330)
    // and indexes it `attr_widths[attno]` (1-based). The value model passes the
    // unshifted `rel->attr_widths` and the caller's `min_attr`, and the callee
    // indexes `attr_widths[attno - min_attr]`. A NULL `attr_widths` (C) is an
    // EMPTY slice here -> None (rather than wrapping an empty Vec in Some, which
    // would index out of bounds). A non-empty caller buffer is threaded through
    // and updated in place.
    let mut widths = attr_widths.to_vec();
    let cache: Option<&mut [i32]> = if widths.is_empty() {
        None
    } else {
        Some(&mut widths)
    };
    let result = get_rel_data_width_impl(&relation, cache, min_attr)?;
    // relation_close(relation, NoLock): close the RAII handle directly. A by-OID
    // close here plus the handle's Drop would double-decrement the pin.
    relation.close(NoLock)?;
    Ok(result)
}

/// `MAXALIGN(len)` (c.h) over `MAXIMUM_ALIGNOF == 8`.
#[inline]
fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

/// `rint` — round to nearest, ties to even (libm semantics), matching
/// costsize.c's helper.
#[inline]
fn rint(x: f64) -> f64 {
    let r = x.round_ties_even();
    if r == 0.0 {
        0.0_f64.copysign(x)
    } else {
        r
    }
}

/* ==========================================================================
 * get_relation_constraints / relation_excluded_by_constraints
 * ======================================================================== */

/// `get_relation_constraints(root, relationObjectId, rel, include_noinherit,
/// include_notnull, include_partition)` (plancat.c) — the relation's applicable
/// constraint expressions, canonicalized and Var-stamped to `rel->relid`.
fn get_relation_constraints<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    relation_object_id: Oid,
    rel: RelId,
    include_noinherit: bool,
    include_notnull: bool,
    include_partition: bool,
) -> PgResult<Vec<NodeId>> {
    let varno = root.rel(rel).relid;
    let mut result: Vec<NodeId> = Vec::new();

    // The check loop + NOT NULL block are both gated on `rd_att->constr != NULL`
    // in C; the ext-seam reads validated check constraints (empty iff no
    // TupleConstr / none validated), and `relation_has_not_null` returns
    // `constr && constr->has_not_null`.
    let checks = ext::get_check_constraints::call(relation_object_id)?;

    for chk in checks.iter() {
        // Ignore NO INHERIT unless told that's safe.
        if chk.ccnoinherit && !include_noinherit {
            continue;
        }
        // stringToNode + eval_const_expressions + canonicalize_qual +
        // ChangeVarNodes + make_ands_implicit, appended to result.
        let mut items =
            ext::process_check_constraint::call(run.mcx(), root, &chk.ccbin, varno as i32)?;
        result.append(&mut items);
    }

    // Add NOT NULL constraints in expression form, if requested.
    if include_notnull && ext::relation_has_not_null::call(relation_object_id)? {
        let notnull_attrs = ext::not_null_attnums::call(relation_object_id)?;
        for (attno, typid, typmod, collation) in notnull_attrs.iter().copied() {
            // ntest->arg = (Expr *) makeVar(varno, i, ...); argisrow=false.
            let var = make_var(varno as i32, attno, typid, typmod, collation, 0);
            let ntest = Expr::NullTest(NullTest {
                arg: Some(alloc::boxed::Box::new(var)),
                nulltesttype: ::nodes::primnodes::NullTestType::IS_NOT_NULL,
                argisrow: false,
                // get_relation_constraints sets ntest->location = -1.
                location: -1,
            });
            let nid = root.alloc_node(ntest);
            result.push(nid);
        }
    }

    // Add partitioning constraints, if requested.
    if include_partition && ext::relation_is_partition::call(relation_object_id)? {
        ext::set_baserel_partition_constraint::call(run.mcx(), root, rel, relation_object_id)?;
        let mut pq = root.rel(rel).partition_qual.clone();
        result.append(&mut pq);
    }

    // Expand virtual generated columns in the constraint expressions.
    if !result.is_empty() {
        result = ext::expand_generated_columns_in_expr::call(
            root,
            &result,
            relation_object_id,
            varno as i32,
        )?;
    }

    Ok(result)
}

/// `relation_excluded_by_constraints(root, rel, rte)` (plancat.c) — detect
/// whether the relation need not be scanned.
pub fn relation_excluded_by_constraints<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> PgResult<bool> {
    let mut include_partition = false;

    debug_assert!(is_simple_rel(root, rel));

    // No base restriction clauses ⇒ no hope.
    if root.rel(rel).baserestrictinfo.is_empty() {
        return Ok(false);
    }

    // Detect constant-FALSE-or-NULL restriction clauses, regardless of the GUC.
    let baserestrict: Vec<NodeId> = root
        .rel(rel)
        .baserestrictinfo
        .iter()
        .map(|&ri| root.rinfo(ri).clause)
        .collect();
    for &clause in baserestrict.iter() {
        if ext::const_is_false_or_null::call(root, clause) {
            return Ok(true);
        }
    }

    // Skip further tests depending on constraint_exclusion.
    let ce = ::guc_tables::vars::constraint_exclusion.read();
    let reloptkind = root.rel(rel).reloptkind;
    match ce {
        CONSTRAINT_EXCLUSION_OFF => return Ok(false),
        CONSTRAINT_EXCLUSION_PARTITION => {
            if reloptkind == RELOPT_OTHER_MEMBER_REL {
                // appendrel member, process it
            } else {
                return Ok(false);
            }
        }
        CONSTRAINT_EXCLUSION_ON => {
            if reloptkind == RELOPT_BASEREL {
                include_partition = true;
            }
        }
        _ => {}
    }

    // Check for self-contradictory restriction clauses (immutable only).
    let mut safe_restrictions: Vec<NodeId> = Vec::new();
    for &clause in baserestrict.iter() {
        if !ext::contain_mutable_functions::call(root, clause)? {
            safe_restrictions.push(clause);
        }
    }
    // Weak refutation (restriction vs restriction).
    if ext::predicate_refuted_by::call(root, &safe_restrictions, &safe_restrictions, true) {
        return Ok(true);
    }

    // Only plain relations have constraints.
    if rte::rte_rtekind::call(run, root, rti) != RTE_RELATION {
        return Ok(false);
    }

    let rte_inh = rte::rte_inh::call(run, root, rti);
    let rte_relkind = rte::rte_relkind::call(run, root, rti) as u8;

    // NO INHERIT constraints only when scanning just this table.
    let include_noinherit = !rte_inh;
    // attnotnull constraints as NO INHERIT unless partitioned.
    let include_notnull = !rte_inh || rte_relkind == RELKIND_PARTITIONED_TABLE;

    let relid = rte::rte_relid::call(run, root, rti);
    let constraint_pred = get_relation_constraints(
        run,
        root,
        relid,
        rel,
        include_noinherit,
        include_notnull,
        include_partition,
    )?;

    // Ignore mutable constraints.
    let mut safe_constraints: Vec<NodeId> = Vec::new();
    for &pred in constraint_pred.iter() {
        if !ext::contain_mutable_functions::call(root, pred)? {
            safe_constraints.push(pred);
        }
    }

    // Strong refutation vs the full baserestrictinfo.
    if ext::predicate_refuted_by::call(root, &safe_constraints, &baserestrict, false) {
        return Ok(true);
    }

    Ok(false)
}

/// `IS_SIMPLE_REL(rel)` — `reloptkind == RELOPT_BASEREL || RELOPT_OTHER_MEMBER_REL`.
fn is_simple_rel(root: &PlannerInfo, rel: RelId) -> bool {
    let k = root.rel(rel).reloptkind;
    k == RELOPT_BASEREL || k == RELOPT_OTHER_MEMBER_REL
}

/* ==========================================================================
 * build_physical_tlist / build_index_tlist
 * ======================================================================== */

/// `build_physical_tlist(root, rel)` (plancat.c) — a targetlist of exactly the
/// relation's user attributes, in order, or NIL when there are dropped/missing
/// columns.
pub fn build_physical_tlist<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
) -> PgResult<Vec<NodeId>> {
    let varno = root.rel(rel).relid;
    let rtekind = rte::rte_rtekind::call(run, root, varno);

    match rtekind {
        RTE_RELATION => {
            // Assume we already have adequate lock.
            let relid = rte::rte_relid::call(run, root, varno);
            let relcx = mcx::MemoryContext::new("build_physical_tlist relcache");
            let relation = table::table_open(relcx.mcx(), relid, NoLock)?;
            let numattrs = relation.rd_att.natts;
            let mut tlist: Vec<NodeId> = Vec::new();
            for attrno in 1..=numattrs {
                let att = relation.rd_att.attr((attrno - 1) as usize);
                if att.attisdropped || att.atthasmissing {
                    // found a dropped or missing col, so punt
                    tlist = Vec::new();
                    break;
                }
                let var = make_var(
                    varno as i32,
                    attrno as AttrNumber,
                    att.atttypid,
                    att.atttypmod,
                    att.attcollation,
                    0,
                );
                let vid = root.alloc_node(var);
                let te = make_target_entry(vid, attrno as AttrNumber, None, false);
                let tid = root.alloc_targetentry(te);
                tlist.push(tid);
            }
            // `relation_close(relation, NoLock)` — one unpin. Consume the
            // handle so its `Drop` (also `relation_close(rel, NoLock)`) does not
            // fire a SECOND unpin: a by-OID `relation_close::call` here PLUS the
            // guard's Drop would double-decrement `rd_refcnt`, driving it below
            // the caller's outstanding pins.
            relation.close(NoLock)?;
            Ok(tlist)
        }
        crate::RTE_SUBQUERY => {
            let cols = ext::subquery_physical_tlist::call(run, root, varno)?;
            let mut tlist: Vec<NodeId> = Vec::new();
            for (vid, resno, resjunk) in cols.into_iter() {
                let te = make_target_entry(vid, resno, None, resjunk);
                tlist.push(root.alloc_targetentry(te));
            }
            Ok(tlist)
        }
        RTE_FUNCTION | RTE_TABLEFUNC | RTE_VALUES | RTE_CTE | RTE_NAMEDTUPLESTORE | RTE_RESULT => {
            let colvars = ext::expand_rte_physical_tlist::call(run, root, varno)?;
            match colvars {
                None => Ok(Vec::new()), // a non-Var (dropped col) ⇒ punt
                Some(vars) => {
                    let mut tlist: Vec<NodeId> = Vec::new();
                    for vid in vars.into_iter() {
                        let varattno = match root.node(vid) {
                            Expr::Var(v) => v.varattno,
                            _ => 0,
                        };
                        let te = make_target_entry(vid, varattno, None, false);
                        tlist.push(root.alloc_targetentry(te));
                    }
                    Ok(tlist)
                }
            }
        }
        other => Err(::types_error::PgError::error(alloc::format!(
            "unsupported RTE kind {} in build_physical_tlist",
            other
        ))),
    }
}

/// `build_index_tlist(root, index, heapRelation)` (plancat.c) — a targetlist
/// representing the columns of the index.
fn build_index_tlist(
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    heap_relation: &rel::Relation<'_>,
) -> PgResult<Vec<NodeId>> {
    let varno = match index.rel {
        Some(rid) => root.rel(rid).relid,
        None => 0,
    };
    let mut tlist: Vec<NodeId> = Vec::new();
    let mut indexpr_pos = 0usize;

    for i in 0..index.ncolumns as usize {
        let indexkey = index.indexkeys[i];
        let indexvar: NodeId;

        if indexkey != 0 {
            // Simple column.
            let (atttypid, atttypmod, attcollation) = if indexkey < 0 {
                // SystemAttributeDefinition(indexkey).
                let def = ext::system_attribute_definition::call(indexkey)?;
                (def.0, def.1, def.2)
            } else {
                let att = heap_relation.rd_att.attr((indexkey - 1) as usize);
                (att.atttypid, att.atttypmod, att.attcollation)
            };
            let var = make_var(
                varno as i32,
                indexkey as AttrNumber,
                atttypid,
                atttypmod,
                attcollation,
                0,
            );
            indexvar = root.alloc_node(var);
        } else {
            // Expression column.
            if indexpr_pos >= index.indexprs.len() {
                return Err(::types_error::PgError::error(alloc::string::String::from("wrong number of index expressions")));
            }
            indexvar = index.indexprs[indexpr_pos];
            indexpr_pos += 1;
        }

        let te = make_target_entry(indexvar, (i + 1) as AttrNumber, None, false);
        tlist.push(root.alloc_targetentry(te));
    }

    if indexpr_pos != index.indexprs.len() {
        return Err(::types_error::PgError::error(alloc::string::String::from("wrong number of index expressions")));
    }

    Ok(tlist)
}

/* ==========================================================================
 * get_relation_statistics / get_relation_statistics_worker
 * ======================================================================== */

/// `get_relation_statistics(rel, relation)` (plancat.c) — the relation's
/// extended-statistics metadata (`StatisticExtInfo`s) as arena node handles.
fn get_relation_statistics<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    relation_object_id: Oid,
) -> PgResult<Vec<NodeId>> {
    let varno = root.rel(rel).relid;
    let statoidlist = ext::get_stat_ext_list::call(relation_object_id)?;
    let mut stainfos: Vec<NodeId> = Vec::new();

    for &stat_oid in statoidlist.iter() {
        // Build the covered-column keys + const-folded expressions (the
        // SearchSysCache1(STATEXTOID) + eval_const_expressions + fix_opfuncids +
        // ChangeVarNodes body).
        let (key_attnums, exprs) =
            ext::get_stat_ext_keys_exprs::call(run.mcx(), root, stat_oid, varno as i32)?;
        let mut keys: Relids = None;
        for &k in key_attnums.iter() {
            keys = bms::relids_add_member::call(keys.take(), k);
        }

        // Extract for both stxdinherit values (true then false).
        get_relation_statistics_worker(root, &mut stainfos, rel, stat_oid, true, &keys, &exprs)?;
        get_relation_statistics_worker(root, &mut stainfos, rel, stat_oid, false, &keys, &exprs)?;
    }

    Ok(stainfos)
}

/// `get_relation_statistics_worker(stainfos, rel, statOid, inh, keys, exprs)`
/// (plancat.c) — add one `StatisticExtInfo` per built kind for the data row.
fn get_relation_statistics_worker(
    root: &mut PlannerInfo,
    stainfos: &mut Vec<NodeId>,
    rel: RelId,
    stat_oid: Oid,
    inh: bool,
    keys: &Relids,
    exprs: &[NodeId],
) -> PgResult<()> {
    let data = ext::get_stat_ext_data_kinds::call(stat_oid, inh)?;
    let data = match data {
        Some(d) => d,
        None => return Ok(()),
    };

    for &kind in data.kinds.iter() {
        let info = StatisticExtInfo {
            stat_oid,
            inherit: data.stxdinherit,
            rel: Some(rel),
            kind,
            keys: bms::relids_copy::call(keys),
            exprs: exprs.to_vec(),
        };
        let nid = root.alloc_statistic_ext(info);
        stainfos.push(nid);
    }

    Ok(())
}

/* ==========================================================================
 * restriction_selectivity / join_selectivity / function_selectivity
 * ======================================================================== */

/// `restriction_selectivity(root, operatorid, args, inputcollid, varRelid)`
/// (plancat.c).
pub fn restriction_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operatorid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    var_relid: i32,
) -> PgResult<f64> {
    let oprrest = lsyscache::get_oprrest::call(operatorid)?;
    // Missing oprrest ⇒ 0.5.
    if oprrest == InvalidOid {
        return Ok(0.5);
    }
    let result = ext::call_oprrest::call(run, root, oprrest, operatorid, args, inputcollid, var_relid)?;
    if !(0.0..=1.0).contains(&result) {
        return Err(::types_error::PgError::error(alloc::format!(
            "invalid restriction selectivity: {}",
            result
        )));
    }
    Ok(result)
}

/// `join_selectivity(root, operatorid, args, inputcollid, jointype, sjinfo)`
/// (plancat.c). `sjinfo` is passed by its arena node handle (`None` = NULL).
pub fn join_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operatorid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    jointype: i16,
    sjinfo: Option<&::pathnodes::SpecialJoinInfo>,
) -> PgResult<f64> {
    let oprjoin = lsyscache::get_oprjoin::call(operatorid)?;
    if oprjoin == InvalidOid {
        return Ok(0.5);
    }
    let result = ext::call_oprjoin::call(run, root, oprjoin, operatorid, args, inputcollid, jointype, sjinfo)?;
    if !(0.0..=1.0).contains(&result) {
        return Err(::types_error::PgError::error(alloc::format!(
            "invalid join selectivity: {}",
            result
        )));
    }
    Ok(result)
}

/// `function_selectivity(root, funcid, args, inputcollid, is_join, varRelid,
/// jointype, sjinfo)` (plancat.c).
pub fn function_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    funcid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    is_join: bool,
    var_relid: i32,
    jointype: i16,
    sjinfo: Option<&::pathnodes::SpecialJoinInfo>,
) -> PgResult<f64> {
    let prosupport = lsyscache::get_func_support::call(funcid)?;
    // No support function ⇒ historical default 0.3333333.
    if prosupport == InvalidOid {
        return Ok(0.3333333);
    }
    let sresult = ext::call_func_selectivity_support::call(
        run, root, funcid, args, inputcollid, is_join, var_relid, jointype, sjinfo,
    )?;
    match sresult {
        None => Ok(0.3333333),
        Some(sel) => {
            if !(0.0..=1.0).contains(&sel) {
                return Err(::types_error::PgError::error(alloc::format!(
                    "invalid function selectivity: {}",
                    sel
                )));
            }
            Ok(sel)
        }
    }
}

/* ==========================================================================
 * add_function_cost / get_function_rows
 * ======================================================================== */

/// `add_function_cost(root, funcid, node, &cost)` (plancat.c) — returns the
/// `(startup, per_tuple)` cost to *add* (the caller accumulates).
///
/// C:
///   proctup = SearchSysCache1(PROCOID, funcid);
///   procform = GETSTRUCT(proctup);
///   if (OidIsValid(procform->prosupport)) {
///       SupportRequestCost req; ... OidFunctionCall1(prosupport, &req); ...
///       cost->startup += req.startup;
///       cost->per_tuple += req.per_tuple;
///   } else {
///       cost->per_tuple += procform->procost * cpu_operator_cost;
///   }
pub fn add_function_cost(
    _root: Option<&PlannerInfo>,
    funcid: Oid,
    _node: Option<NodeId>,
) -> PgResult<(f64, f64)> {
    let form = syscache_seams::proc_cost_rows::call(funcid)?;

    if form.prosupport != InvalidOid {
        // OidFunctionCall1(prosupport, SupportRequestCost{root, funcid, node}).
        // The decomposed dispatch rides the clauses support-cost table. This
        // `(root, funcid, NodeId)` entry has only the arena-handle node, not a
        // resolvable by-value Expr, so the dispatch passes `None`; a cost kernel
        // that needs the node declines, falling back on `procost` (the C "no
        // support function, or it failed" outcome). The by-node entry
        // `seam_add_function_cost_by_node` carries the real node.
        if let Some((startup, per_tuple)) =
            clauses_seams::call_support_cost::call(
                form.prosupport,
                funcid,
                None,
            )?
        {
            return Ok((startup, per_tuple));
        }
        // by-OID dispatch missed; retry by the support function's `prosrc`
        // symbol for a dynamically-OID'd support function (see
        // `call_support_rows_by_prosrc`).
        {
            let scratch = mcx::MemoryContext::new("add_function_cost prosrc");
            let prosrc: Option<alloc::string::String> =
                lsyscache::get_func_prosrc::call(scratch.mcx(), form.prosupport)?
                    .map(|s| s.as_str().to_string());
            if let Some(prosrc) = prosrc {
                if let Some((startup, per_tuple)) =
                    clauses_seams::call_support_cost_by_symbol::call(
                        &prosrc,
                        funcid,
                        None,
                    )?
                {
                    return Ok((startup, per_tuple));
                }
            }
        }
    }

    // cost->per_tuple += procform->procost * cpu_operator_cost;
    // The `cpu_operator_cost` GUC global is owned by costsize.c; read it through
    // its installed costsize seam (the raw GUC-slot path is unwired tree-wide,
    // costsize keeps the value as a const and exposes it via this seam).
    let cpu_operator_cost = costsize_seams::cpu_operator_cost::call();
    Ok((0.0, form.procost as f64 * cpu_operator_cost))
}

/// `get_function_rows(root, funcid, node)` (plancat.c).
///
/// C:
///   proctup = SearchSysCache1(PROCOID, funcid);
///   procform = GETSTRUCT(proctup);
///   Assert(procform->proretset);     /* else caller error */
///   if (OidIsValid(procform->prosupport)) { ... SupportRequestRows ... }
///   else result = procform->prorows;
pub fn get_function_rows(
    _root: &PlannerInfo,
    funcid: Oid,
    _node: Option<NodeId>,
) -> PgResult<f64> {
    let form = syscache_seams::proc_cost_rows::call(funcid)?;

    // Assert(procform->proretset);
    debug_assert!(form.proretset);

    // OidFunctionCall1(prosupport, SupportRequestRows{...}) when prosupport is
    // valid. This `(root, funcid, NodeId)` entry has only the arena-handle node,
    // not a resolvable by-value FuncExpr, so it cannot run a node-reading
    // support kernel; it falls back on `pg_proc.prorows` (the C "no support
    // function, or it failed" outcome). The by-value `seam_get_function_rows_by_node`
    // entry carries the real node and runs the support-rows dispatch.
    let _ = _node;

    // result = procform->prorows;
    Ok(form.prorows as f64)
}

/* ==========================================================================
 * has_unique_index / has_row_triggers / has_transition_tables /
 * has_stored_generated_columns / get_dependent_generated_columns
 * ======================================================================== */

/// `has_unique_index(rel, attno)` (plancat.c) — is there a single-column unique
/// index on the attribute?
pub fn has_unique_index(root: &PlannerInfo, rel: RelId, attno: AttrNumber) -> bool {
    for index in root.rel(rel).indexlist.iter() {
        if index.unique
            && index.nkeycolumns == 1
            && index.indexkeys.first().copied() == Some(attno as i32)
            && (index.indpred.is_empty() || index.predOK)
        {
            return true;
        }
    }
    false
}

/// `has_row_triggers(root, rti, event)` (plancat.c).
pub fn has_row_triggers<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    rti: Index,
    event: ::pathnodes::CmdType,
) -> PgResult<bool> {
    let relid = rte::rte_relid::call(run, root, rti);
    match event {
        CMD_INSERT | CMD_UPDATE | CMD_DELETE => ext::relation_has_row_triggers::call(relid, event),
        CMD_MERGE => Ok(false),
        other => Err(::types_error::PgError::error(alloc::format!(
            "unrecognized CmdType: {}",
            other
        ))),
    }
}

/// `has_transition_tables(root, rti, event)` (plancat.c).
pub fn has_transition_tables<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    rti: Index,
    event: ::pathnodes::CmdType,
) -> PgResult<bool> {
    debug_assert_eq!(rte::rte_rtekind::call(run, root, rti), RTE_RELATION);
    // Foreign tables cannot have transition tables.
    if rte::rte_relkind::call(run, root, rti) as u8 == RELKIND_FOREIGN_TABLE {
        return Ok(false);
    }
    let relid = rte::rte_relid::call(run, root, rti);
    match event {
        CMD_INSERT | CMD_UPDATE | CMD_DELETE => {
            ext::relation_has_transition_tables::call(relid, event)
        }
        CMD_MERGE => Ok(false),
        other => Err(::types_error::PgError::error(alloc::format!(
            "unrecognized CmdType: {}",
            other
        ))),
    }
}

/// `has_stored_generated_columns(root, rti)` (plancat.c).
pub fn has_stored_generated_columns<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    rti: Index,
) -> PgResult<bool> {
    let relid = rte::rte_relid::call(run, root, rti);
    ext::relation_has_stored_generated_columns::call(relid)
}

/// `get_dependent_generated_columns(root, rti, target_cols)` (plancat.c).
pub fn get_dependent_generated_columns<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &PlannerInfo,
    rti: Index,
    target_cols: &Relids,
) -> PgResult<Relids> {
    let relid = rte::rte_relid::call(run, root, rti);
    let attnums = ext::dependent_generated_columns::call(relid, target_cols)?;
    let mut dependent: Relids = None;
    for a in attnums {
        dependent = bms::relids_add_member::call(dependent.take(), a);
    }
    Ok(dependent)
}

/* ==========================================================================
 * Seam installation.
 * ======================================================================== */

/// Install the seams this unit OWNS.
pub fn init_seams() {
    // The two seams declared in `backend-optimizer-util-plancat-seams`.
    plancat_seams::estimate_rel_size::set(seam_estimate_rel_size);
    plancat_seams::get_rel_data_width::set(seam_get_rel_data_width);

    // `estimate_rel_size(indexRelation, NULL, ...)` — the index variant of size
    // estimation get_relation_info uses for a partial/expression index. Owned
    // here because the RELKIND_INDEX branch of estimate_rel_size lives in
    // plancat.c (indexes do not go through the table-AM dispatch).
    ext::table_relation_estimate_size_for_index::set(seam_table_relation_estimate_size_for_index);

    // Seams other crates declared awaiting plancat's logic.
    relnode_ext_seams::get_relation_info::set(seam_get_relation_info);
    allpaths::seams::relation_excluded_by_constraints::set(
        seam_relation_excluded_by_constraints,
    );
    // `const_is_false_or_null` — the inline const-FALSE/NULL restriction test in
    // C's `relation_excluded_by_constraints`. Homed in the plancat-ext stub
    // because it resolves the arena `NodeId` to a `Const`; plancat owns the
    // call site, so it installs the body. C:
    //   IsA(clause, Const) && (((Const *) clause)->constisnull ||
    //                          !DatumGetBool(((Const *) clause)->constvalue))
    ext::const_is_false_or_null::set(|root, node| match root.node(node) {
        ::nodes::primnodes::Expr::Const(c) => c.constisnull || !c.constvalue.as_bool(),
        _ => false,
    });

    clauses_seams::get_function_rows::set(seam_get_function_rows);
    // `get_function_rows` over a by-value SRF node (the clauses-seams contract:
    // no `root`, no arena handle). plancat's `get_function_rows` body consults
    // `pg_proc.prorows` / a `SupportRequestRows` support function; this is the
    // node-by-value shape `seam_get_function_rows` delegates to.
    ext::get_function_rows_by_node::set(seam_get_function_rows_by_node);
    costsize_seams::add_function_cost::set(seam_add_function_cost);
    costsize_seams::get_relation_data_width::set(
        seam_get_relation_data_width,
    );
    path_small_seams::restriction_selectivity::set(seam_restriction_selectivity);
    path_small_seams::join_selectivity::set(seam_join_selectivity);
    path_small_seams::function_selectivity::set(seam_function_selectivity);

    // GUC variable backing storage owned by plancat.c (`int constraint_exclusion`
    // at line 58), an enum int read directly from the GUC slot by
    // `relation_excluded_by_constraints` — never from the ControlFile.
    {
        use guc_tables::{vars, GucVarAccessors};
        vars::constraint_exclusion.install(GucVarAccessors {
            get: guc_backing::constraint_exclusion,
            set: guc_backing::set_constraint_exclusion,
        });
    }
}

fn seam_estimate_rel_size(
    rel: &rel::Relation<'_>,
) -> PgResult<(BlockNumber, f64, f64)> {
    estimate_rel_size_impl(rel, None, 0)
}

/// `estimate_rel_size(indexRelation, NULL, &pages, &tuples, &allvisfrac)`
/// (plancat.c) for an index relation opened by OID — the index variant of size
/// estimation used by `get_relation_info` for a partial/expression index (its
/// `indpred` is non-empty, so the plain `index_number_of_blocks` shortcut is not
/// taken). C opens the index with `index_open` and calls the SAME
/// `estimate_rel_size` body; the `RELKIND_INDEX` branch of
/// [`estimate_rel_size_impl`] supplies the metapage-discounted page/tuple/
/// allvisfrac math. `attr_widths` is `NULL` (C passes NULL here), so the
/// no-stats sub-branch estimates the index tuple width from the index relation's
/// own attribute datatypes.
fn seam_table_relation_estimate_size_for_index(
    indexoid: Oid,
) -> PgResult<(BlockNumber, f64, f64)> {
    // `index_open(indexoid, NoLock)` — project the index relcache entry to a
    // borrowed `Relation` value (no close authority; the caller in
    // get_relation_info owns the lock/open lifecycle). Mirrors the
    // heapam-handler table-AM estimator's open-by-OID pattern.
    let scratch = mcx::MemoryContext::new("table_relation_estimate_size_for_index");
    let data = relcache_seams::relation_id_get_relation::call(
        scratch.mcx(),
        indexoid,
    )?
    .expect("table_relation_estimate_size_for_index: index must exist in relcache");
    let rel = rel::Relation::open(data, None);

    // `estimate_rel_size(rel, NULL, ...)` — relkind is RELKIND_INDEX, so the
    // impl takes its index branch. The index branch only reaches
    // get_rel_data_width_impl when reltuples < 0; an index's attrs are all
    // positive attnos, so the `i - min_attr` cache index lands the same with a
    // base of FirstLowInvalidHeapAttributeNumber+1 as C's
    // `attr_widths - rel->min_attr`.
    let min_attr = (FirstLowInvalidHeapAttributeNumber + 1) as AttrNumber;
    let result = estimate_rel_size_impl(&rel, None, min_attr);

    // `relation_id_get_relation` took a fresh pin; release it explicitly
    // (`index_close(indexRelation, NoLock)`), like the table estimator. Without
    // this the pin leaks once per planning of any query touching a partial/
    // expression index, surfacing later as CheckTableNotInUse refusing a
    // same-session DROP/TRUNCATE.
    relcache_seams::relation_close::call(indexoid)?;

    result
}

fn seam_get_rel_data_width(rel: Oid, attr_widths: Option<&mut [i32]>) -> PgResult<i32> {
    let relcx = mcx::MemoryContext::new("get_rel_data_width relcache");
    let relation = table::table_open(relcx.mcx(), rel, NoLock)?;
    let min_attr = (FirstLowInvalidHeapAttributeNumber + 1) as AttrNumber;
    let result = get_rel_data_width_impl(&relation, attr_widths, min_attr)?;
    // relation_close(relation, NoLock): close the RAII handle directly. A by-OID
    // close here plus the handle's Drop would double-decrement the pin.
    relation.close(NoLock)?;
    Ok(result)
}

fn seam_get_relation_info<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    relation_object_id: Oid,
    inhparent: bool,
    rel: RelId,
) -> PgResult<()> {
    get_relation_info(run, root, relation_object_id, inhparent, rel)
}

fn seam_relation_excluded_by_constraints<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    rel: RelId,
    rti: Index,
) -> bool {
    match relation_excluded_by_constraints(run, root, rel, rti) {
        Ok(b) => b,
        Err(e) => panic!("relation_excluded_by_constraints: {e:?}"),
    }
}

fn seam_get_function_rows<'mcx>(funcid: Oid, node: &Expr<'mcx>) -> PgResult<f64> {
    // The clauses-seams contract passes the SRF node by value (`&Expr`) without a
    // PlannerInfo. plancat's `get_function_rows` body consults `pg_proc.prorows`
    // and a `SupportRequestRows` support function; with no `root` and no arena
    // handle, dispatch goes through the support layer keyed on the by-value node.
    ext::get_function_rows_by_node::call(funcid, node)
}

/// `get_function_rows(root, funcid, node)` (plancat.c) over a by-value SRF node.
/// C: read `pg_proc.prorows`, asserting `proretset`; the `prosupport` path runs
/// a `SupportRequestRows` support function (unported tree-wide; mirror-and-panic
/// — unreachable for current query paths where `prosupport == InvalidOid`).
fn seam_get_function_rows_by_node<'mcx>(funcid: Oid, node: &Expr<'mcx>) -> PgResult<f64> {
    let form = syscache_seams::proc_cost_rows::call(funcid)?;
    debug_assert!(form.proretset);
    if form.prosupport != InvalidOid {
        // OidFunctionCall1(prosupport, SupportRequestRows{root, funcid, node}).
        // The decomposed dispatch rides the clauses support-rows table over the
        // by-value (const-folded) FuncExpr `node`. On a successful estimate,
        // return it; otherwise (no kernel / non-constant args) fall back on
        // `pg_proc.prorows`.
        if let Some(rows) =
            clauses_seams::call_support_rows::call(form.prosupport, funcid, node)?
        {
            return Ok(rows);
        }
        // The by-OID dispatch missed; if `prosupport` is a dynamically-OID'd
        // support function (e.g. `... SUPPORT test_support_func`), resolve its
        // `prosrc` symbol and retry the symbol-keyed dispatch — the faithful
        // counterpart of fmgr running `OidFunctionCall1(prosupport, &req)` over
        // a C-language function resolved by its `prosrc` symbol.
        if let Some(rows) = call_support_rows_by_prosrc(form.prosupport, funcid, node)? {
            return Ok(rows);
        }
    }
    // No support function, or it declined, so rely on prorows.
    Ok(form.prorows as f64)
}

/// Resolve `prosupport`'s `prosrc` symbol and run the symbol-keyed
/// `SupportRequestRows` dispatch. `Ok(None)` when the OID has no `prosrc` or no
/// kernel is registered under its symbol.
fn call_support_rows_by_prosrc<'mcx>(
    prosupport: Oid,
    funcid: Oid,
    node: &Expr<'mcx>,
) -> PgResult<Option<f64>> {
    let scratch = mcx::MemoryContext::new("call_support_rows_by_prosrc");
    let prosrc: Option<alloc::string::String> =
        lsyscache::get_func_prosrc::call(scratch.mcx(), prosupport)?
            .map(|s| s.as_str().to_string());
    let Some(prosrc) = prosrc else {
        return Ok(None);
    };
    clauses_seams::call_support_rows_by_symbol::call(&prosrc, funcid, node)
}

fn seam_add_function_cost(
    root: Option<&PlannerInfo>,
    funcid: Oid,
    node: Option<NodeId>,
) -> (f64, f64) {
    match add_function_cost(root, funcid, node) {
        Ok(c) => c,
        Err(e) => panic!("add_function_cost: {e:?}"),
    }
}

fn seam_get_relation_data_width(reloid: Oid, attr_widths: &[i32], min_attr: i16) -> u32 {
    match get_relation_data_width(reloid, attr_widths, min_attr as AttrNumber) {
        Ok(w) => w as u32,
        Err(e) => panic!("get_relation_data_width: {e:?}"),
    }
}

fn seam_restriction_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operatorid: Oid,
    args: &[Expr],
    inputcollid: Oid,
    var_relid: i32,
) -> PgResult<f64> {
    let arg_ids = intern_args(root, args, run.mcx())?;
    restriction_selectivity(run, root, operatorid, &arg_ids, inputcollid, var_relid)
}

fn seam_join_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operatorid: Oid,
    args: &[Expr],
    inputcollid: Oid,
    jointype: ::pathnodes::JoinType,
    sjinfo: Option<&::pathnodes::SpecialJoinInfo>,
) -> PgResult<f64> {
    let arg_ids = intern_args(root, args, run.mcx())?;
    join_selectivity(run, root, operatorid, &arg_ids, inputcollid, jointype as i16, sjinfo)
}

fn seam_function_selectivity<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    funcid: Oid,
    args: &[Expr],
    inputcollid: Oid,
    is_join: bool,
    var_relid: i32,
    jointype: ::pathnodes::JoinType,
    sjinfo: Option<&::pathnodes::SpecialJoinInfo>,
) -> PgResult<f64> {
    let arg_ids = intern_args(root, args, run.mcx())?;
    function_selectivity(
        run, root, funcid, &arg_ids, inputcollid, is_join, var_relid, jointype as i16, sjinfo,
    )
}

/// Intern a borrowed `args` slice into arena node handles. Deep-copy via
/// `clone_in` (a selectivity clause arg may be an Aggref, whose context-allocated
/// TargetEntry args a bare derived `.clone()` panics on).
fn intern_args<'mcx>(
    root: &mut PlannerInfo,
    args: &[Expr],
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<Vec<NodeId>> {
    let mut out = Vec::with_capacity(args.len());
    for e in args {
        let cloned = e.clone_in(mcx)?;
        out.push(root.alloc_node(cloned));
    }
    Ok(out)
}

// RTE-kind constants used by build_physical_tlist (parsenodes.h `RTEKind`;
// `rte_rtekind` returns the matching numeric discriminant).
const RTE_SUBQUERY: u32 = 1;
const RTE_FUNCTION: u32 = 3;
const RTE_TABLEFUNC: u32 = 4;
const RTE_VALUES: u32 = 5;
const RTE_CTE: u32 = 6;
const RTE_NAMEDTUPLESTORE: u32 = 7;
const RTE_RESULT: u32 = 8;
