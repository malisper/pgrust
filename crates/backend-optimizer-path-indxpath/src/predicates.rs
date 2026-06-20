//! Partial-index predicate tests + index-only-scan check (indxpath.c).

use alloc::vec::Vec;

use mcx::{Mcx, PgBox};
use types_nodes::bitmapset::Bitmapset;
use types_nodes::primnodes::Expr;
use types_pathnodes::{IndexOptInfo, NodeId, PlannerInfo, RelId};

use backend_nodes_core::bitmapset::{bms_add_member, bms_is_subset, bms_union};
use backend_optimizer_path_costsize_seams::enable_indexonlyscan;
use backend_optimizer_util_predtest_seams::predicate_implied_by;
use backend_optimizer_util_restrictinfo_seams as restrictinfo;
use backend_optimizer_util_var_seams::pull_varattnos;

use crate::matchers::{match_boolean_index_clause, IsBooleanOpfamily};
use crate::util::{FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER, RELOPT_OTHER_MEMBER_REL};

/// `check_index_only(rel, index)` (indxpath.c:2229) — can an index-only scan be
/// used for this index? `mcx` is threaded for the `pull_varattnos` / `bms_*`
/// allocations (var.c's collector + nodes-core bitmapset).
pub fn check_index_only(
    mcx: Mcx<'_>,
    root: &PlannerInfo,
    rel: RelId,
    index: &IndexOptInfo,
) -> Result<bool, types_error::PgError> {
    // Index-only scans must be enabled.
    if !enable_indexonlyscan::call() {
        return Ok(false);
    }

    let relinfo = root.rel(rel);
    let relid_idx = relinfo.relid;

    // First, identify all the attributes needed for joins or final output, from
    // the rel's targetlist. attrs_used accumulates across the reltarget exprs and
    // each indrestrictinfo clause.
    let mut attrs_used: Option<PgBox<Bitmapset>> = None;
    let reltarget = relinfo
        .reltarget
        .as_ref()
        .expect("RelOptInfo without reltarget");
    for &expr_id in &reltarget.exprs {
        let expr = root.node(expr_id);
        let got = pull_varattnos::call(mcx, expr, relid_idx)?;
        attrs_used = bms_union(mcx, attrs_used.as_deref(), got.as_deref())?;
    }

    // Add all attributes used by restriction clauses (the predicate-pruned set).
    let indrestrictinfo = index.indrestrictinfo.clone();
    for ric in indrestrictinfo {
        let clause_id = root.rinfo(ric).clause;
        let clause = root.node(clause_id);
        let got = pull_varattnos::call(mcx, clause, relid_idx)?;
        attrs_used = bms_union(mcx, attrs_used.as_deref(), got.as_deref())?;
    }

    // Construct a bitmapset of columns the index can return in an index-only scan.
    let mut index_canreturn_attrs: Option<PgBox<Bitmapset>> = None;
    let ncolumns = index.ncolumns as usize;
    for i in 0..ncolumns {
        let attno = index.indexkeys[i];
        // For the moment, ignore index expressions.
        if attno == 0 {
            continue;
        }
        if index.canreturn[i] {
            let x = attno - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER;
            index_canreturn_attrs = Some(bms_add_member(mcx, index_canreturn_attrs, x)?);
        }
    }

    // Do we have all the necessary attributes?
    Ok(bms_is_subset(
        attrs_used.as_deref(),
        index_canreturn_attrs.as_deref(),
    ))
}

/// `check_index_predicates(root, rel)` (indxpath.c:3943) — set the
/// predicate-derived `predOK` / `indrestrictinfo` fields for each index of the
/// relation.
pub fn check_index_predicates<'mcx>(
    _mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    run: &types_pathnodes::planner_run::PlannerRun<'mcx>,
    rel: RelId,
) -> Result<(), types_error::PgError> {
    // Initialize indrestrictinfo to baserestrictinfo and detect partial indexes.
    let baserestrictinfo = root.rel(rel).baserestrictinfo.clone();
    let mut have_partial = false;
    let nindexes = root.rel(rel).indexlist.len();
    for i in 0..nindexes {
        root.rel_mut(rel).indexlist[i].indrestrictinfo = baserestrictinfo.clone();
        if !root.rel(rel).indexlist[i].indpred.is_empty() {
            have_partial = true;
        }
    }
    if !have_partial {
        return Ok(());
    }

    // Construct a list of clauses we can assume true for proving the index(es)
    // usable: the rel's restriction clauses, any movable join clauses, plus
    // EC-derivable join clauses.
    let mut clauselist: Vec<NodeId> = Vec::new();
    for &ric in &baserestrictinfo {
        clauselist.push(root.rinfo(ric).clause);
    }

    // Scan the rel's join clauses for movable ones.
    let joininfo = root.rel(rel).joininfo.clone();
    for rinfo in joininfo {
        if !restrictinfo::join_clause_is_movable_to::call(root, rinfo, rel) {
            continue;
        }
        clauselist.push(root.rinfo(rinfo).clause);
    }

    // Add on any equivalence-derivable join clauses. otherrels =
    // all_query_rels - (child-rel parents | rel->relids), minus nulling_relids.
    let relids = root.rel(rel).relids.clone();
    let reloptkind = root.rel(rel).reloptkind;
    let otherrels = if reloptkind == RELOPT_OTHER_MEMBER_REL {
        let parents = restrictinfo::find_childrel_parents::call(root, rel);
        bms_difference_relids(&root.all_query_rels, &parents)
    } else {
        bms_difference_relids(&root.all_query_rels, &relids)
    };
    // C 4005: otherrels = bms_del_members(otherrels, rel->nulling_relids) —
    // mustn't consider clauses only computable after outer joins that null the
    // rel.
    let nulling_relids = root.rel(rel).nulling_relids.clone();
    let otherrels = bms_difference_relids(&otherrels, &nulling_relids);

    if !relids_is_empty(&otherrels) {
        let joinrelids = bms_union_relids(&relids, &otherrels);
        // equivclass.c owns generate_join_implied_equalities; C passes NULL for
        // sjinfo here (check_index_predicates, indxpath.c:4010).
        let generated =
            backend_optimizer_path_equivclass_seams::generate_join_implied_equalities::call(
                root, run, joinrelids, otherrels, rel, None,
            )?;
        for ri in generated {
            clauselist.push(root.rinfo(ri).clause);
        }
    }

    // Is the rel a target relation of UPDATE/DELETE/MERGE/SELECT FOR UPDATE?
    // C 4029: bms_is_member(rel->relid, root->all_result_relids) ||
    //         get_plan_rowmark(root->rowMarks, rel->relid) != NULL.
    let relid_idx = root.rel(rel).relid;
    let is_target_rel = crate::util::relids_is_member(relid_idx as i32, &root.all_result_relids)
        || restrictinfo::has_plan_rowmark::call(root, relid_idx);

    // Now try to prove each index predicate true.
    let nindexes = root.rel(rel).indexlist.len();
    for i in 0..nindexes {
        if root.rel(rel).indexlist[i].indpred.is_empty() {
            continue; // ignore non-partial indexes here
        }

        if !root.rel(rel).indexlist[i].predOK {
            let indpred = root.rel(rel).indexlist[i].indpred.clone();
            let ok = predicate_implied_by::call(root, &indpred, &clauselist, false);
            root.rel_mut(rel).indexlist[i].predOK = ok;
        }

        // If rel is an update target, leave indrestrictinfo as set above.
        if is_target_rel {
            continue;
        }

        // If index is !amoptionalkey, also leave indrestrictinfo as set above.
        if !root.rel(rel).indexlist[i].amoptionalkey {
            continue;
        }

        // Else compute indrestrictinfo as the non-implied quals.
        let indpred = root.rel(rel).indexlist[i].indpred.clone();
        let mut kept: Vec<types_pathnodes::RinfoId> = Vec::new();
        for &ric in &baserestrictinfo {
            let clause_id = root.rinfo(ric).clause;
            // predicate_implied_by() assumes first arg is immutable. Borrow the
            // clause for the read-only mutability test (a derived `.clone()`
            // panics on an owned-subtree child).
            let clause: &Expr = root.node(clause_id);
            let mutable =
                backend_optimizer_util_clauses::contain_mutable_functions(Some(clause))?;
            if mutable
                || !predicate_implied_by::call(root, &[clause_id], &indpred, false)
            {
                kept.push(ric);
            }
        }
        root.rel_mut(rel).indexlist[i].indrestrictinfo = kept;
    }

    Ok(())
}

/// `indexcol_is_bool_constant_for_query(root, index, indexcol)`
/// (indxpath.c:4362) — is the index column constrained to a constant boolean
/// value by the query's WHERE clauses?
pub fn indexcol_is_bool_constant_for_query(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    index: &IndexOptInfo,
    indexcol: i32,
) -> bool {
    let indexcol = indexcol as usize;

    // If the index isn't boolean, we can't possibly get a match.
    if !IsBooleanOpfamily(index.opfamily[indexcol]) {
        return false;
    }

    // Check each restriction clause for the index's rel.
    let rel = index.rel.expect("IndexOptInfo without rel");
    let baserestrictinfo = root.rel(rel).baserestrictinfo.clone();
    for rinfo in baserestrictinfo {
        // As in match_clause_to_indexcol, never match pseudoconstants to indexes.
        if root.rinfo(rinfo).pseudoconstant {
            continue;
        }
        // See if we can match the clause's expression to the index column.
        if match_boolean_index_clause(mcx, root, rinfo, indexcol, index)
            .expect("match_boolean_index_clause")
            .is_some()
        {
            return true;
        }
    }

    false
}

/* ---- small Relids helpers over the planner `Relids` (Vec<u64> bitmapset) ---- */

fn relids_is_empty(a: &types_pathnodes::Relids) -> bool {
    match a {
        None => true,
        Some(b) => b.words.iter().all(|w| *w == 0),
    }
}

/// `bms_difference(a, b)` over the planner `Relids`.
fn bms_difference_relids(
    a: &types_pathnodes::Relids,
    b: &types_pathnodes::Relids,
) -> types_pathnodes::Relids {
    let aw = match a {
        None => return None,
        Some(x) => &x.words,
    };
    let bw: &[u64] = match b {
        None => &[],
        Some(x) => &x.words,
    };
    let mut out: Vec<u64> = Vec::with_capacity(aw.len());
    for (i, &w) in aw.iter().enumerate() {
        let mask = bw.get(i).copied().unwrap_or(0);
        out.push(w & !mask);
    }
    while out.last() == Some(&0) {
        out.pop();
    }
    if out.is_empty() {
        None
    } else {
        Some(alloc::boxed::Box::new(types_pathnodes::Bitmapset { words: out }))
    }
}

/// `bms_union(a, b)` over the planner `Relids`.
fn bms_union_relids(
    a: &types_pathnodes::Relids,
    b: &types_pathnodes::Relids,
) -> types_pathnodes::Relids {
    let aw: &[u64] = match a {
        None => &[],
        Some(x) => &x.words,
    };
    let bw: &[u64] = match b {
        None => &[],
        Some(x) => &x.words,
    };
    let n = aw.len().max(bw.len());
    if n == 0 {
        return None;
    }
    let mut out: Vec<u64> = Vec::with_capacity(n);
    for i in 0..n {
        out.push(aw.get(i).copied().unwrap_or(0) | bw.get(i).copied().unwrap_or(0));
    }
    while out.last() == Some(&0) {
        out.pop();
    }
    if out.is_empty() {
        None
    } else {
        Some(alloc::boxed::Box::new(types_pathnodes::Bitmapset { words: out }))
    }
}
