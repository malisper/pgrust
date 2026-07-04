//! plancat.c slice: get_relation_info for plain heap relations with btree
//! indexes, estimate_rel_size, has_unique_index, restriction_selectivity.

use std::cell::{Cell, RefCell};

use mcx::{vec_from_elem_in, PgVec};
use types_core::{BlockNumber, Oid, BTREE_AM_OID};
use types_error::PgResult;
use types_pathnodes::{IndexOptInfo, NodeId, RelId};
use types_rel::{NoLock, Relation, RELKIND_RELATION};
use types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
use types_tuple::tupdesc::{ATTNULLABLE_UNKNOWN, ATTNULLABLE_VALID};

use crate::relnode::{relids_singleton, relids_union};
use crate::run::PlannerRun;

const INDOPTION_DESC: i16 = 1 << 0;
const INDOPTION_NULLS_FIRST: i16 = 1 << 1;
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_TOASTVALUE: u8 = b't';
const RELKIND_SEQUENCE: u8 = b'S';
pub(crate) const AMFLAG_HAS_TID_RANGE: u32 = 1 << 0;

fn relkind_has_table_am(relkind: u8) -> bool {
    matches!(relkind, RELKIND_RELATION | RELKIND_MATVIEW | RELKIND_TOASTVALUE)
}

pub fn get_relation_info<'mcx>(
    run: &mut PlannerRun<'mcx>,
    relation_object_id: Oid,
    inhparent: bool,
    rel: RelId,
) -> PgResult<()> {
    let mcx = run.mcx;
    let varno = run.root.rel(rel).relid;

    let relation = table::table_open(mcx, relation_object_id, NoLock)?;
    let relkind = relation.rd_rel.relkind;
    if !(relkind_has_table_am(relkind)
        || relkind == RELKIND_SEQUENCE
        || relkind == types_rel::RELKIND_PARTITIONED_TABLE)
    {
        panic!("get_relation_info (plancat.c): relkind {relkind}; M2 foreign lane");
    }
    // C's !RelationIsPermanent && RecoveryInProgress guard: no hot-standby
    // sessions exist, so the recovery arm is compile-time false.


    let natts = relation.rd_att.natts;
    {
        let r = run.root.rel_mut(rel);
        r.min_attr = (FirstLowInvalidHeapAttributeNumber + 1) as i16;
        r.max_attr = natts as i16;
        r.reltablespace = relation.rd_rel.reltablespace;
        debug_assert!(r.max_attr >= r.min_attr);
        let span = (r.max_attr - r.min_attr + 1) as usize;
        r.attr_needed = PgVec::new_in(mcx);
        for _ in 0..span {
            r.attr_needed.push(None);
        }
        r.attr_widths = vec_from_elem_in(mcx, 0i32, span);
    }

    // C leaves notnullattnums unpopulated for traditional inheritance parents.
    if !inhparent || relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        for i in 0..natts as usize {
            let attr = relation.rd_att.compact_attr(i);
            debug_assert!(attr.attnullability != ATTNULLABLE_UNKNOWN);
            if attr.attnullability == ATTNULLABLE_VALID {
                debug_assert!(!attr.attisdropped);
                let nn = relids_singleton(mcx, (i + 1) as u32);
                let cur = run.root.rel_mut(rel).notnullattnums.take();
                run.root.rel_mut(rel).notnullattnums = relids_union(mcx, &cur, &nn);
            }
        }
    }

    // An inheritance parent's size is the appendrel's, computed in
    // set_append_rel_size; pages/tuples stay zero here.
    if !inhparent {
        let min_attr = run.root.rel(rel).min_attr;
        let empty = PgVec::new_in(mcx);
        let mut widths = core::mem::replace(&mut run.root.rel_mut(rel).attr_widths, empty);
        let (pages, tuples, allvisfrac) =
            estimate_rel_size(&relation, Some(&mut widths), min_attr)?;
        let r = run.root.rel_mut(rel);
        r.attr_widths = widths;
        r.pages = pages;
        r.tuples = tuples;
        r.allvisfrac = allvisfrac;
    }

    run.root.rel_mut(rel).rel_parallel_workers = relation.get_parallel_workers(-1);

    // A partitioned parent keeps its (partitioned) indexes in indexlist for
    // uniqueness proofs; a traditional inheritance parent keeps none.
    let hasindex = if inhparent && relkind != types_rel::RELKIND_PARTITIONED_TABLE {
        false
    } else {
        relation.rd_rel.relhasindex
    };
    let mut indexinfos: PgVec<'mcx, &'mcx IndexOptInfo<'mcx>> = PgVec::new_in(mcx);
    if hasindex {
        let indexoidlist =
            relcache_seams::relation_get_index_list::call(mcx, relation_object_id)?;
        let lmode = run.rte(varno as usize).rellockmode;

        for &indexoid in indexoidlist.iter() {
            let index_rel = indexam::index_open(mcx, indexoid, lmode)?;
            let ind = index_rel
                .rd_index
                .as_ref()
                .expect("index relation carries rd_index");

            if !ind.indisvalid {
                indexam::index_close(index_rel, NoLock)?;
                continue;
            }
            // indcheckxmin gate: M2 concurrent-build lane (Form lacks it).

            let is_partitioned_index =
                index_rel.rd_rel.relkind == types_rel::RELKIND_PARTITIONED_INDEX;
            assert!(
                index_rel.rd_rel.relkind == types_rel::RELKIND_INDEX || is_partitioned_index,
                "get_relation_info (plancat.c): unexpected index relkind"
            );
            let relam = index_rel.rd_rel.relam;
            let am_is_btree = relam == BTREE_AM_OID;
            let am_is_gin = relam == types_core::GIN_AM_OID;
            let am_is_gist = relam == types_core::GIST_AM_OID;
            let am_is_brin = relam == types_core::BRIN_AM_OID;
            let am_is_spgist = relam == types_core::SPGIST_AM_OID;
            if !am_is_btree
                && !am_is_gin
                && !am_is_gist
                && !am_is_brin
                && !am_is_spgist
                && relam != types_core::HASH_AM_OID
            {
                panic!("get_relation_info (plancat.c): index AM {relam}; M2 index-AM lane");
            }
            let ncolumns = ind.indnatts as i32;
            let nkeycolumns = ind.indnkeyatts as i32;
            let mut info = IndexOptInfo::new(mcx);
            info.indexoid = ind.indexrelid;
            info.reltablespace = index_rel.rd_rel.reltablespace;
            info.rel = Some(rel);
            info.ncolumns = ncolumns;
            info.nkeycolumns = nkeycolumns;
            for i in 0..ncolumns as usize {
                info.indexkeys.push(ind.indkey[i] as i32);
                info.indexcollations.push(
                    index_rel.rd_indcollation.get(i).copied().unwrap_or(0),
                );
            }
            for i in 0..nkeycolumns as usize {
                info.opfamily.push(index_rel.rd_opfamily[i]);
                info.opcintype.push(index_rel.rd_opcintype[i]);
                info.canreturn.push(match index_rel.rd_rel.relam {
                    BTREE_AM_OID => btcanreturn(),
                    types_core::GIST_AM_OID => gist::gistcanreturn(&index_rel, i as i32 + 1),
                    types_core::SPGIST_AM_OID => {
                        spgist::spgcanreturn(&index_rel, i as i32 + 1)?
                    }
                    _ => false,
                });
            }
            info.relam = relam;
            // Per-AM IndexAmRoutine flags (bt/hash/gin/gist/brin handlers);
            // a partitioned index has no AM (C NULLifies these fields).
            if !is_partitioned_index {
                info.amcanorderbyop = am_is_gist || am_is_spgist;
                info.amoptionalkey =
                    am_is_btree || am_is_gin || am_is_gist || am_is_spgist || am_is_brin;
                info.amsearcharray = am_is_btree;
                info.amsearchnulls = am_is_btree || am_is_gist || am_is_spgist || am_is_brin;
                info.amcanparallel = am_is_btree;
                info.amhasgettuple = !am_is_gin && !am_is_brin;
                info.amhasgetbitmap = true;
                info.amcanmarkpos = am_is_btree;

                // amcanorder arm: a non-ordering AM leaves the sort vectors
                // empty (C's NULL sortopfamily).
                if am_is_btree {
                    for i in 0..nkeycolumns as usize {
                        let opt = index_rel.rd_indoption[i];
                        info.sortopfamily.push(info.opfamily[i]);
                        info.reverse_sort.push(opt & INDOPTION_DESC != 0);
                        info.nulls_first.push(opt & INDOPTION_NULLS_FIRST != 0);
                    }
                }
            }

            // RelationGetIndexExpressions/Predicate + ChangeVarNodes(1, varno):
            // parsed from the Form's nodeToString sources (pg_index.rs note).
            if let Some(src) = ind.indexprs_src.as_ref() {
                let node = readfuncs::stringToNode(mcx, src.as_str())?;
                let list = node.as_list().expect("indexprs is a List");
                for e in list.iter() {
                    let e = clauses::eval_const_expressions(mcx, e)?;
                    if varno != 1 {
                        change_var_nodes(e, varno as i32);
                    }
                    info.indexprs.push(run.intern_expr(e));
                }
            }
            if let Some(src) = ind.indpred_src.as_ref() {
                let node = readfuncs::stringToNode(mcx, src.as_str())?;
                let folded = clauses::eval_const_expressions(mcx, node)?;
                let canon = crate::prepqual::canonicalize_qual(mcx, folded, false)?;
                let implicit = clauses::make_ands_implicit(mcx, Some(canon))?;
                for e in implicit.iter() {
                    if varno != 1 {
                        change_var_nodes(e, varno as i32);
                    }
                    info.indpred.push(run.intern_expr(e));
                }
            }

            // build_index_tlist (plancat.c); system attrs are unreachable in
            // an index key.
            let mut indexpr_next = 0usize;
            for i in 0..ncolumns as usize {
                let indexkey = info.indexkeys[i];
                let expr = if indexkey != 0 {
                    assert!(indexkey > 0, "build_index_tlist: system-attribute index key");
                    let att = relation.rd_att.attrs[indexkey as usize - 1];
                    types_nodes::Node::mk_var(
                        mcx,
                        varno as i32,
                        indexkey as i16,
                        att.atttypid,
                        att.atttypmod,
                        att.attcollation,
                        0,
                    )?
                } else {
                    let id = *info
                        .indexprs
                        .get(indexpr_next)
                        .expect("wrong number of index expressions");
                    indexpr_next += 1;
                    *run.root.expr_node(id)
                };
                let tle =
                    types_nodes::Node::mk_target_entry(mcx, expr, (i + 1) as i16, None, false)?;
                info.indextlist.push(run.intern_expr(tle));
            }
            assert!(
                indexpr_next == info.indexprs.len(),
                "wrong number of index expressions"
            );

            info.indrestrictinfo = RefCell::new(PgVec::new_in(mcx));
            info.predOK = Cell::new(false);
            info.unique = ind.indisunique;
            info.nullsnotdistinct = ind.indnullsnotdistinct;
            info.immediate = ind.indimmediate;
            info.hypothetical = false;

            if is_partitioned_index {
                info.pages = 0;
                info.tuples = 0.0;
                info.tree_height = Cell::new(-1);
            } else {
                if info.indpred.is_empty() {
                    info.pages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
                        &index_rel,
                        types_core::ForkNumber::MAIN_FORKNUM,
                    )?;
                    info.tuples = run.root.rel(rel).tuples;
                } else {
                    let (pages, tuples, _) = estimate_rel_size(&index_rel, None, 1)?;
                    info.pages = pages;
                    info.tuples = tuples.min(run.root.rel(rel).tuples);
                }
                info.tree_height = Cell::new(if am_is_btree {
                    nbtree::bt_getrootheight(&index_rel)?
                } else {
                    -1
                });
            }
            if am_is_gin && !is_partitioned_index {
                let gs = gin::ginGetStats(&index_rel)?;
                info.gin_stats = Some(types_pathnodes::GinIndexStats {
                    pending_pages: gs.nPendingPages,
                    total_pages: gs.nTotalPages,
                    entry_pages: gs.nEntryPages,
                    data_pages: gs.nDataPages,
                    entries: gs.nEntries,
                    version: gs.ginVersion,
                });
            }

            indexam::index_close(index_rel, NoLock)?;
            indexinfos.insert(0, &*mcx::forget_box_in(mcx, info)?);
        }
    }
    run.root.rel_mut(rel).indexlist = indexinfos;

    crate::extended_stats::get_relation_statistics(run, rel, relation.rd_id)?;

    {
        let r = run.root.rel_mut(rel);
        r.serverid = 0;
        r.has_fdwroutine = false;
        // Heap AM always provides scan_bitmap/scan_tid_range.
        r.amflags |= AMFLAG_HAS_TID_RANGE;
    }

    // Divergence: get_relation_foreign_keys is skipped (RelationGetFKeyList
    // unported), so fkey_list stays empty and join size estimation uses
    // fkselec = 1.0 even where C would match FK constraints. Estimate-only:
    // affects plan choice, never results. The plancat FK unit owns the fix.
    debug_assert!(run.root.fkey_list.is_empty());

    relation.close(NoLock)?;
    Ok(())
}
fn btcanreturn() -> bool {
    true
}

// ChangeVarNodes (rewriteManip.c), rt_index 1 arm over freshly parsed index
// expression trees (exclusively owned, so in-place mutation is safe).
fn change_var_nodes(node: types_nodes::Node<'_>, new_varno: i32) {
    use types_nodes::NodeTag;
    let walk_list = |l: &types_nodes::NodeList<'_>| {
        for e in l {
            change_var_nodes(e, new_varno);
        }
    };
    match node.node_tag() {
        NodeTag::T_Var => {
            // SAFETY: tree is freshly parsed and exclusively owned here.
            unsafe {
                node.with_mut::<types_nodes::primnodes::Var, _>(|v| {
                    if v.varno == 1 && v.varlevelsup == 0 {
                        v.varno = new_varno;
                    }
                })
            }
            .expect("Var");
        }
        NodeTag::T_Const | NodeTag::T_Param => {}
        NodeTag::T_OpExpr => walk_list(&node.as_op_expr().unwrap().args),
        NodeTag::T_DistinctExpr => walk_list(&node.as_distinct_expr().unwrap().args),
        NodeTag::T_FuncExpr => walk_list(&node.as_func_expr().unwrap().args),
        NodeTag::T_BoolExpr => walk_list(&node.as_bool_expr().unwrap().args),
        NodeTag::T_ScalarArrayOpExpr => {
            walk_list(&node.as_scalar_array_op_expr().unwrap().args)
        }
        NodeTag::T_RelabelType => {
            change_var_nodes(node.as_relabel_type().unwrap().arg, new_varno)
        }
        NodeTag::T_NullTest => {
            change_var_nodes(node.as_null_test().unwrap().arg.expect("NullTest.arg"), new_varno)
        }
        NodeTag::T_BooleanTest => change_var_nodes(
            node.as_boolean_test().unwrap().arg.expect("BooleanTest.arg"),
            new_varno,
        ),
        NodeTag::T_CoalesceExpr => walk_list(&node.as_coalesce_expr().unwrap().args),
        NodeTag::T_ArrayExpr => walk_list(&node.as_array_expr().unwrap().elements),
        NodeTag::T_RowExpr => walk_list(&node.as_row_expr().unwrap().args),
        NodeTag::T_List => walk_list(node.as_list().unwrap()),
        other => panic!("ChangeVarNodes (rewriteManip.c): {other:?}; unported lane"),
    }
}

const HEAP_OVERHEAD_BYTES_PER_TUPLE: usize = 24 + 4;
const HEAP_USABLE_BYTES_PER_PAGE: usize = 8192 - 24;

// estimate_rel_size (plancat.c), table-AM arm -> (pages, tuples, allvisfrac).
pub fn estimate_rel_size(
    rel: &Relation<'_>,
    attr_widths: Option<&mut [i32]>,
    min_attr: i16,
) -> PgResult<(BlockNumber, f64, f64)> {
    let relkind = rel.rd_rel.relkind;
    if !relkind_has_table_am(relkind) {
        if relkind == types_rel::RELKIND_INDEX {
            let reported_pages = bufmgr_seams::relation_get_number_of_blocks_in_fork::call(
                rel,
                types_core::ForkNumber::MAIN_FORKNUM,
            )?;
            if reported_pages == 0 {
                return Ok((0, 0.0, 0.0));
            }
            let mut curpages = reported_pages;
            let mut relpages = rel.rd_rel.relpages as BlockNumber;
            let reltuples = rel.rd_rel.reltuples as f64;
            let relallvisible = rel.rd_rel.relallvisible as BlockNumber;
            // Discount the metapage (OK for btree/hash/GIN, suspect for GiST).
            if relpages > 0 {
                curpages -= 1;
                relpages -= 1;
            }
            let density = if reltuples >= 0.0 && relpages > 0 {
                reltuples / relpages as f64
            } else {
                let tuple_width = get_rel_data_width(rel, None, 1)? as usize
                    + HEAP_OVERHEAD_BYTES_PER_TUPLE;
                (HEAP_USABLE_BYTES_PER_PAGE / tuple_width) as f64
            };
            let tuples = (density * curpages as f64).round_ties_even();
            let allvisfrac = if relallvisible == 0 || curpages == 0 {
                0.0
            } else if relallvisible as f64 >= curpages as f64 {
                1.0
            } else {
                relallvisible as f64 / curpages as f64
            };
            return Ok((reported_pages, tuples, allvisfrac));
        }
        if relkind == RELKIND_SEQUENCE || relkind == types_rel::RELKIND_PARTITIONED_TABLE {
            // C final else arm: just use whatever's in pg_class (partitioned
            // tables are storageless; reached with ONLY / zero partitions).
            return Ok((rel.rd_rel.relpages as BlockNumber, rel.rd_rel.reltuples as f64, 0.0));
        }
        panic!("estimate_rel_size (plancat.c): relkind {relkind}; M2 lane");
    }
    let mut pages: BlockNumber = 0;
    let mut tuples = 0.0f64;
    let mut allvisfrac = 0.0f64;
    tableam::table_relation_estimate_size(
        rel,
        HEAP_OVERHEAD_BYTES_PER_TUPLE,
        HEAP_USABLE_BYTES_PER_PAGE,
        |aw| get_rel_data_width(rel, aw, min_attr),
        attr_widths,
        &mut pages,
        &mut tuples,
        &mut allvisfrac,
    )?;
    Ok((pages, tuples, allvisfrac))
}

// get_rel_data_width (plancat.c); attr_widths[attno - min_attr] is the cache.
pub fn get_rel_data_width(
    rel: &Relation<'_>,
    mut attr_widths: Option<&mut [i32]>,
    min_attr: i16,
) -> PgResult<i32> {
    let mut tuple_width: i64 = 0;
    for i in 1..=rel.rd_att.natts {
        let att = rel.rd_att.attr((i - 1) as usize);
        if att.attisdropped {
            continue;
        }
        let ndx = (i - min_attr as i32) as usize;
        if let Some(aw) = attr_widths.as_deref() {
            if aw[ndx] > 0 {
                tuple_width += aw[ndx] as i64;
                continue;
            }
        }
        let mut item_width = lsyscache::get_attavgwidth(rel.rd_id, i as i16)?;
        if item_width <= 0 {
            item_width = lsyscache::get_typavgwidth(att.atttypid, att.atttypmod)?;
            debug_assert!(item_width > 0);
        }
        if let Some(aw) = attr_widths.as_deref_mut() {
            aw[ndx] = item_width;
        }
        tuple_width += item_width as i64;
    }
    Ok(crate::costsize::clamp_width_est(tuple_width))
}

// has_unique_index (plancat.c).
pub fn has_unique_index(run: &PlannerRun<'_>, rel: RelId, attno: i16) -> bool {
    for index in run.root.rel(rel).indexlist.iter() {
        if index.unique
            && index.nkeycolumns == 1
            && index.indexkeys[0] == attno as i32
            && (index.indpred.is_empty() || index.predOK.get())
        {
            return true;
        }
    }
    false
}

// restriction_selectivity (plancat.c): closed-set oprrest dispatch.
pub fn restriction_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operatorid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    varrelid: i32,
) -> PgResult<f64> {
    const F_EQSEL: Oid = 101;
    let oprrest = lsyscache::get_oprrest(operatorid)?;
    if oprrest == 0 {
        return Ok(0.5);
    }
    const F_NEQSEL: Oid = 102;
    const F_SCALARLTSEL: Oid = 103;
    const F_SCALARGTSEL: Oid = 104;
    const F_SCALARLESEL: Oid = 336;
    const F_SCALARGESEL: Oid = 337;
    const F_ICLIKESEL: Oid = 1814;
    const F_ICNLIKESEL: Oid = 1815;
    const F_REGEXEQSEL: Oid = 1818;
    const F_LIKESEL: Oid = 1819;
    const F_ICREGEXEQSEL: Oid = 1820;
    const F_REGEXNESEL: Oid = 1821;
    const F_NLIKESEL: Oid = 1822;
    const F_ICREGEXNESEL: Oid = 1823;
    const F_PREFIXSEL: Oid = 3437;
    use crate::like_support::PatternType;
    const F_MATCHINGSEL: Oid = 5040;
    // geo_selfuncs.c constants
    const F_AREASEL: Oid = 139;
    const F_POSITIONSEL: Oid = 1300;
    const F_CONTSEL: Oid = 1302;
    let result = match oprrest {
        F_AREASEL => 0.005,
        F_POSITIONSEL => 0.1,
        F_CONTSEL => 0.001,
        F_EQSEL => crate::selfuncs::eqsel(run, operatorid, args, varrelid, inputcollid)?,
        F_MATCHINGSEL => {
            crate::selfuncs::matchingsel(run, operatorid, args, varrelid, inputcollid)?
        }
        F_NEQSEL => crate::selfuncs::neqsel(run, operatorid, args, varrelid, inputcollid)?,
        F_SCALARLTSEL | F_SCALARGTSEL | F_SCALARLESEL | F_SCALARGESEL => {
            let isgt = oprrest == F_SCALARGTSEL || oprrest == F_SCALARGESEL;
            let iseq = oprrest == F_SCALARLESEL || oprrest == F_SCALARGESEL;
            crate::selfuncs::scalarineqsel_wrapper(
                run, operatorid, args, varrelid, inputcollid, isgt, iseq,
            )?
        }
        F_REGEXEQSEL | F_ICREGEXEQSEL | F_LIKESEL | F_ICLIKESEL | F_PREFIXSEL | F_REGEXNESEL
        | F_ICREGEXNESEL | F_NLIKESEL | F_ICNLIKESEL => {
            let (ptype, negate) = match oprrest {
                F_REGEXEQSEL => (PatternType::Regex, false),
                F_ICREGEXEQSEL => (PatternType::RegexIc, false),
                F_LIKESEL => (PatternType::Like, false),
                F_ICLIKESEL => (PatternType::LikeIc, false),
                F_PREFIXSEL => (PatternType::Prefix, false),
                F_REGEXNESEL => (PatternType::Regex, true),
                F_ICREGEXNESEL => (PatternType::RegexIc, true),
                F_NLIKESEL => (PatternType::Like, true),
                _ => (PatternType::LikeIc, true),
            };
            crate::like_support::patternsel(
                run, operatorid, args, varrelid, inputcollid, ptype, negate,
            )?
        }
        3169 => crate::rangetypes_selfuncs::rangesel(run, operatorid, args, varrelid)?,
        4243 => crate::multirangetypes_selfuncs::multirangesel(run, operatorid, args, varrelid)?,
        3560 => crate::network_selfuncs::networksel(run, operatorid, args, varrelid)?,
        other => panic!(
            "restriction_selectivity (plancat.c): oprrest {other}; M2 selfuncs lane"
        ),
    };
    if !(0.0..=1.0).contains(&result) {
        panic!("invalid restriction selectivity: {result}");
    }
    Ok(result)
}

// join_selectivity (plancat.c): closed-set oprjoin dispatch. The scalar
// inequality estimators return DEFAULT_INEQ_SEL with no arg inspection.
pub fn join_selectivity<'mcx>(
    run: &mut PlannerRun<'mcx>,
    operatorid: Oid,
    args: &[NodeId],
    inputcollid: Oid,
    jointype: types_pathnodes::JoinType,
    sjinfo: Option<&types_pathnodes::SpecialJoinInfo<'mcx>>,
) -> PgResult<f64> {
    const F_EQJOINSEL: Oid = 105;
    const F_SCALARLTJOINSEL: Oid = 107;
    const F_SCALARGTJOINSEL: Oid = 108;
    const F_SCALARLEJOINSEL: Oid = 386;
    const F_SCALARGEJOINSEL: Oid = 398;
    const F_AREAJOINSEL: Oid = 140;
    const F_POSITIONJOINSEL: Oid = 1301;
    const F_CONTJOINSEL: Oid = 1303;
    const DEFAULT_INEQ_SEL: f64 = 0.3333333333333333;
    let oprjoin = lsyscache::get_oprjoin(operatorid)?;
    if oprjoin == 0 {
        return Ok(0.5);
    }
    let result = match oprjoin {
        F_EQJOINSEL => {
            crate::selfuncs::eqjoinsel(run, operatorid, args, jointype, sjinfo, inputcollid)?
        }
        F_SCALARLTJOINSEL | F_SCALARGTJOINSEL | F_SCALARLEJOINSEL | F_SCALARGEJOINSEL => {
            DEFAULT_INEQ_SEL
        }
        // patternjoinsel (like_support.c) punts for all pattern types.
        1816 | 1824 | 1825 | 1826 | 3438 => crate::selfuncs::DEFAULT_MATCH_SEL,
        1817 | 1827 | 1828 | 1829 => 1.0 - crate::selfuncs::DEFAULT_MATCH_SEL,
        F_AREAJOINSEL => 0.005,
        F_POSITIONJOINSEL => 0.1,
        F_CONTJOINSEL => 0.001,
        106 => crate::selfuncs::neqjoinsel(run, operatorid, args, jointype, sjinfo, inputcollid)?,
        3561 => crate::network_selfuncs::networkjoinsel(run, operatorid, args, sjinfo)?,
        // matchingjoinsel (selfuncs.c) punts.
        5041 => crate::selfuncs::DEFAULT_MATCHING_SEL,
        other => panic!("join_selectivity (plancat.c): oprjoin {other}; M2 selfuncs lane"),
    };
    if !(0.0..=1.0).contains(&result) {
        panic!("invalid join selectivity: {result}");
    }
    Ok(result)
}

// function_selectivity (plancat.c). The in-core SupportRequestSelectivity
// providers (like_regex_support, ts match) are unwired; loud until a query
// reaches one.
pub fn function_selectivity(funcid: Oid) -> PgResult<f64> {
    let shape = syscache_seams::pg_proc_cost_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
    if shape.prosupport != 0 {
        panic!(
            "function_selectivity (plancat.c): SupportRequestSelectivity for prosupport {}; \
             M2 lane",
            shape.prosupport
        );
    }
    Ok(0.3333333)
}

// add_function_cost (plancat.c). DIVERGENCE: callers don't thread the calling
// node, so the support request carries node=None (in-core cost-support
// functions all tolerate that and fall back to procost).
pub fn add_function_cost(funcid: Oid, cost: &mut types_pathnodes::QualCost) -> PgResult<()> {
    let shape = syscache_seams::pg_proc_cost_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
    if shape.prosupport != 0 {
        let mut req = types_nodes::supportnodes::SupportRequestCost::new(funcid, None);
        let addr = core::ptr::from_mut(&mut req) as usize;
        let result =
            fmgr_core::oid_function_call1_coll(shape.prosupport, 0, datum::Datum::from_usize(addr))?;
        if result.as_usize() == addr {
            cost.startup += req.startup;
            cost.per_tuple += req.per_tuple;
            return Ok(());
        }
    }
    cost.per_tuple += shape.procost as f64 * crate::gucs::cpu_operator_cost();
    Ok(())
}

// get_function_rows (plancat.c); root is not threaded (support functions on
// this lane read only Const args).
pub fn get_function_rows(funcid: Oid, node: Option<types_nodes::Node<'_>>) -> PgResult<f64> {
    let shape = syscache_seams::pg_proc_cost_shape::call(funcid)?
        .unwrap_or_else(|| panic!("cache lookup failed for function {funcid}"));
    if shape.prosupport != 0 {
        let mut req = types_nodes::supportnodes::SupportRequestRows::new(funcid, node);
        let addr = core::ptr::from_mut(&mut req) as usize;
        let result =
            fmgr_core::oid_function_call1_coll(shape.prosupport, 0, datum::Datum::from_usize(addr))?;
        if result.as_usize() == addr {
            return Ok(req.rows);
        }
    }
    Ok(shape.prorows as f64)
}

// infer_arbiter_indexes (plancat.c): plain-Var inference elements matched
// against unique, valid, non-partial, non-expression btree indexes. ON
// CONSTRAINT, expression/COLLATE/opclass elements, and arbiter WHERE are loud.
pub fn infer_arbiter_indexes<'mcx>(
    run: &crate::run::PlannerRun<'mcx>,
    oc: &types_nodes::primnodes::OnConflictExpr<'mcx>,
) -> PgResult<types_nodes::list::OidList<'mcx>> {
    let mcx = run.mcx;
    let mut results = types_nodes::list::OidList::nil();
    if oc.arbiterElems.is_nil() && oc.constraint == 0 {
        return Ok(results);
    }
    if oc.constraint != 0 {
        panic!("infer_arbiter_indexes (plancat.c): ON CONSTRAINT arbiter; M2 upsert lane");
    }
    if oc.arbiterWhere.is_some() {
        panic!("infer_arbiter_indexes (plancat.c): arbiter WHERE; M2 partial-index lane");
    }

    let parse = run.parse();
    let rte = run.rte(parse.resultRelation as usize);
    let mut infer_attrs: Vec<i16> = Vec::new();
    for elem_node in &oc.arbiterElems {
        let elem = elem_node.as_inference_elem().expect("arbiterElems cell");
        if elem.infercollid != 0 || elem.inferopclass != 0 {
            panic!("infer_arbiter_indexes (plancat.c): COLLATE/opclass element; M2 upsert lane");
        }
        let var = elem
            .expr
            .and_then(|e| e.as_var())
            .unwrap_or_else(|| {
                panic!("infer_arbiter_indexes (plancat.c): expression element; M2 upsert lane")
            });
        if var.varattno == 0 {
            return Err(Box::new(
                types_error::PgError::error(
                    "whole row unique index inference specifications are not supported",
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if !infer_attrs.contains(&var.varattno) {
            infer_attrs.push(var.varattno);
        }
    }
    infer_attrs.sort_unstable();

    let relation = table::table_open(mcx, rte.relid, NoLock)?;
    let indexoidlist = relcache_seams::relation_get_index_list::call(mcx, rte.relid)?;
    for &indexoid in indexoidlist.iter() {
        let idx_rel = indexam::index_open(mcx, indexoid, rte.rellockmode)?;
        let ind = idx_rel.rd_index.as_ref().expect("index relation carries rd_index");
        let matches = ind.indisvalid && ind.indisunique && !ind.indisexclusion && {
            let mut indexed_attrs: Vec<i16> = Vec::new();
            let mut has_expr_col = false;
            for natt in 0..ind.indnkeyatts as usize {
                let attno = ind.indkey[natt];
                if attno == 0 {
                    has_expr_col = true;
                } else if !indexed_attrs.contains(&attno) {
                    indexed_attrs.push(attno);
                }
            }
            indexed_attrs.sort_unstable();
            // Expression columns can't be matched without expression elements,
            // and a partial index's predicate is never implied by the absent
            // (loud) arbiter WHERE: both fall through to no-match, as in C.
            !has_expr_col && !ind.has_indpred && indexed_attrs == infer_attrs
        };
        if matches {
            results.lappend(mcx, ind.indexrelid)?;
        }
        indexam::index_close(idx_rel, NoLock)?;
    }
    table::table_close(relation, NoLock)?;

    if results.is_nil() {
        return Err(Box::new(
            types_error::PgError::error(
                "there is no unique or exclusion constraint matching the ON CONFLICT specification",
            )
            .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_REFERENCE),
        ));
    }
    Ok(results)
}

// get_relation_constraints (plancat.c) for the constraint-exclusion refutation
// leg. include_partition is loud: partition_qual/set_baserel_partition_constraint
// are unported (reachable only under constraint_exclusion=on on a directly
// named partition).
pub fn get_relation_constraints<'mcx>(
    run: &mut PlannerRun<'mcx>,
    relation_object_id: Oid,
    rel: RelId,
    include_noinherit: bool,
    include_notnull: bool,
    include_partition: bool,
) -> PgResult<PgVec<'mcx, types_nodes::Node<'mcx>>> {
    let mcx = run.mcx;
    let varno = run.root.rel(rel).relid;
    let mut result: PgVec<'mcx, types_nodes::Node<'mcx>> = PgVec::new_in(mcx);

    let relation = table::table_open(mcx, relation_object_id, NoLock)?;
    if let Some(constr) = relation.rd_att.constr.as_deref() {
        for check in constr.check.iter() {
            if !check.ccvalid {
                continue;
            }
            debug_assert!(check.ccenforced);
            if check.ccnoinherit && !include_noinherit {
                continue;
            }
            let ccbin = check.ccbin.as_ref().expect("CHECK constraint has ccbin");
            let cexpr = readfuncs::stringToNode(mcx, ccbin.as_str())?;
            let cexpr = clauses::eval_const_expressions(mcx, cexpr)?;
            let cexpr = crate::prepqual::canonicalize_qual(mcx, cexpr, true)?;
            if varno != 1 {
                change_var_nodes(cexpr, varno as i32);
            }
            let implicit = clauses::make_ands_implicit(mcx, Some(cexpr))?;
            for item in implicit.iter() {
                result.push(item);
            }
        }
        if include_notnull && constr.has_not_null {
            let natts = relation.rd_att.natts;
            for i in 1..=natts {
                let att = &relation.rd_att.compact_attrs[(i - 1) as usize];
                if att.attnullability == ATTNULLABLE_VALID && !att.attisdropped {
                    let wholeatt = relation.rd_att.attrs[(i - 1) as usize];
                    let var = types_nodes::Node::mk_var(
                        mcx,
                        varno as i32,
                        i as i16,
                        wholeatt.atttypid,
                        wholeatt.atttypmod,
                        wholeatt.attcollation,
                        0,
                    )?;
                    // argisrow=false is correct even for a composite column
                    // (attnotnull is IS DISTINCT FROM NULL there, not SQL-spec).
                    let ntest = types_nodes::Node::mk(
                        mcx,
                        types_nodes::primnodes::NullTest {
                            arg: Some(var),
                            nulltesttype: types_nodes::primnodes::NullTestType::IS_NOT_NULL,
                            argisrow: false,
                            location: -1,
                        },
                    )?;
                    result.push(ntest);
                }
            }
        }
        if constr.has_generated_virtual {
            for item in result.iter_mut() {
                *item = crate::prepjointree::expand_generated_columns_in_expr(
                    mcx,
                    *item,
                    &relation,
                    varno as i32,
                )?;
            }
        }
    }
    assert!(
        !(include_partition && relation.rd_rel.relispartition),
        "get_relation_constraints (plancat.c): partition constraint under \
         constraint_exclusion=on; set_baserel_partition_constraint unported"
    );
    relation.close(NoLock)?;
    Ok(result)
}
