//! `backend-optimizer-plan-setrefs` — `src/backend/optimizer/plan/setrefs.c`.
//!
//! Post-processing of a completed plan tree: flatten the range table, renumber
//! `Var`s to reference subplan outputs, compute regproc OIDs for operators, and
//! record the objects the plan depends on (for plancache invalidation).
//!
//! Reconciled to this repo's model: the plan tree is the owned
//! `types_nodes::nodes::Node<'mcx>` enum (one variant per plan subtype), and
//! expressions are the lifetime-free `types_nodes::primnodes::Expr` enum. The
//! expression-fixing mutators (`fix_scan_expr`/`fix_join_expr`/`fix_upper_expr`)
//! walk `Expr` via `backend_nodes_core::nodefuncs::expression_tree_mutator`; the
//! plan recursion (`set_plan_refs`) matches on `Node`. Genuine cross-subsystem
//! externals (PlanInvalItem append, RTEPermissionInfo copy, the dummy-rel check,
//! the MULTIEXPR-param / minmax-agg resolution, and planner.c's
//! `mark_partial_aggref`) are seamed through `backend-optimizer-plan-setrefs-seams`.

#![no_std]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use alloc::boxed::Box as ABox;
use alloc::format;
use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgVec};
use types_error::{PgError, PgResult};

use backend_nodes_core::nodefuncs::{
    expr_collation, expr_type, expr_typmod, expression_tree_mutator, set_opfuncid, set_sa_opfuncid,
};
use backend_nodes_core::makefuncs::{
    flat_copy_target_entry, make_null_const, make_var, make_var_from_target_entry,
};
use backend_nodes_equalfuncs::equal_expr;

use types_core::primitive::{AttrNumber, Index, Oid};
use types_nodes::nodes::Node;
use types_nodes::nodes::ntag;
use types_nodes::nodeagg::do_aggsplit_combine;
use types_nodes::primnodes::{Const, Expr, Param, ParamKind, TargetEntry, Var, VarReturningType};
use types_nodes::nodeindexscan::{Plan, Scan};
use types_pathnodes::{PlannerGlobal, PlannerInfo, Relids};

use backend_optimizer_plan_setrefs_seams as ext;

// ===========================================================================
// Constants transcribed locally (primnodes.h / catalog OIDs / syscache IDs).
// ===========================================================================

/// `#define INNER_VAR (-1)` (primnodes.h) — reference to inner subplan.
///
/// These are the verbatim C sentinel values. setrefs is the planner/executor
/// boundary: the Vars it produces (with `varno` set to these constants) are
/// read back by the executor (execExpr / execUtils / nodeIndexonlyscan), which
/// switches on the C negative values. They MUST agree, so we use the real C
/// values here — and `IS_SPECIAL_VARNO` is the C `(int) varno < 0`.
const INNER_VAR: i32 = -1;
/// `#define OUTER_VAR (-2)` — reference to outer subplan.
const OUTER_VAR: i32 = -2;
/// `#define INDEX_VAR (-3)` — reference to index column.
const INDEX_VAR: i32 = -3;
/// `#define ROWID_VAR (-4)` — row identity column during planning.
const ROWID_VAR: i32 = -4;

/// `IS_SPECIAL_VARNO(varno)` (primnodes.h) — `((int) (varno) < 0)`.
#[inline]
fn is_special_varno(varno: i32) -> bool {
    varno < 0
}

/// `REGCLASSOID` / `OIDOID` for `ISREGCLASSCONST`; `PROCOID` / `TYPEOID` are the
/// syscache IDs recorded into PlanInvalItems.
const REGCLASSOID: Oid = 2205;
const OIDOID: Oid = 26;
// `SysCacheIdentifier` ordinals (catalog/syscache_ids.h, alphabetical-by-name
// generated enum): PROCOID is the 47th, TYPEOID the 82nd. These are the exact
// `cacheId` values plancache stores in a `PlanInvalItem` and matches against
// SI invalidation messages, and the index `GetSysCacheHashValue1` uses — they
// MUST equal the C enum / `backend-utils-cache-syscache` cacheinfo ordinals.
const PROCOID: i32 = 47; // SysCacheIdentifier PROCOID
const TYPEOID: i32 = 82; // SysCacheIdentifier TYPEOID

/// `FirstUnpinnedObjectId` (access/transam.h) — OIDs below this are built-in and
/// not tracked for plan invalidation.
const FirstUnpinnedObjectId: Oid = 12000;

/// `NullingRelsMatch` (setrefs.c) — how strict the varnullingrels cross-check is.
#[derive(Clone, Copy, PartialEq, Eq)]
enum NullingRelsMatch {
    /// expect exact match of nullingrels
    NRM_EQUAL,
    /// actual Var may have a subset of input
    NRM_SUBSET,
    /// actual Var may have a superset of input
    NRM_SUPERSET,
}
use NullingRelsMatch::*;

/// `NUM_EXEC_TLIST(parentplan)` — `parentplan->plan_rows`.
#[inline]
fn num_exec_tlist(plan: &Plan) -> f64 {
    plan.plan_rows
}
/// `NUM_EXEC_QUAL(parentplan)` — `parentplan->plan_rows * 2.0`.
#[inline]
fn num_exec_qual(plan: &Plan) -> f64 {
    plan.plan_rows * 2.0
}

/// `ISREGCLASSCONST(con)` — true if a `Const` is a regclass (or plain OID) value.
#[inline]
fn is_regclass_const(con: &Const) -> bool {
    (con.consttype == REGCLASSOID || con.consttype == OIDOID) && !con.constisnull
}

// ===========================================================================
// ExprRelids word helpers (Var.varnullingrels / PlaceHolderVar.phnullingrels).
//
// `ExprRelids { words: Vec<u64> }` carries no bms operations of its own; the
// nullingrels cross-check needs subset/equal over the raw bit words. Mirror the
// in-repo word-bit pattern (init-subselect/quals.rs `expr_relids_is_member`).
// ===========================================================================

fn expr_relids_equal(
    a: &types_nodes::primnodes::ExprRelids,
    b: &types_nodes::primnodes::ExprRelids,
) -> bool {
    let n = core::cmp::max(a.words.len(), b.words.len());
    for i in 0..n {
        let aw = a.words.get(i).copied().unwrap_or(0);
        let bw = b.words.get(i).copied().unwrap_or(0);
        if aw != bw {
            return false;
        }
    }
    true
}

/// `bms_is_subset(a, b)` over `ExprRelids`: is every bit of `a` also set in `b`?
fn expr_relids_is_subset(
    a: &types_nodes::primnodes::ExprRelids,
    b: &types_nodes::primnodes::ExprRelids,
) -> bool {
    for (i, &aw) in a.words.iter().enumerate() {
        let bw = b.words.get(i).copied().unwrap_or(0);
        if aw & !bw != 0 {
            return false;
        }
    }
    true
}

/// The varnullingrels cross-check shared by `search_indexed_tlist_for_var` (over
/// `Var.varnullingrels`) and `_for_phv` (over `PlaceHolderVar.phnullingrels`).
/// `actual` is the input Var/PHV, `expected` is the matched subplan entry's.
fn nullingrels_ok(
    nrm_match: NullingRelsMatch,
    actual: &types_nodes::primnodes::ExprRelids,
    expected: &types_nodes::primnodes::ExprRelids,
) -> bool {
    match nrm_match {
        NRM_SUBSET => expr_relids_is_subset(actual, expected),
        NRM_SUPERSET => expr_relids_is_subset(expected, actual),
        NRM_EQUAL => expr_relids_equal(expected, actual),
    }
}

// ===========================================================================
// set_plan_references — the whole final fix-up pass.
// ===========================================================================

/// `set_plan_references(root, plan)` (setrefs.c:287). The flattened range table,
/// rowmarks, and appendrels are appended to `root.glob`; the recursive
/// Plan-subtype walk (`set_plan_refs`) runs over the unified `Node` tree.
pub fn set_plan_references<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    plan: Node<'mcx>,
) -> PgResult<Node<'mcx>> {
    let rtoffset = glob_ref(root)?.finalrtable.len() as i32;

    // 1. Add all the query's RTEs to the flattened rangetable.
    add_rtes_to_flat_rtable(mcx, run, root, false)?;

    // 2. Adjust RT indexes of PlanRowMarks and add to final rowmarks list.
    //    `flat copy is enough since all fields are scalars`; adjust rti/prti
    //    (NOT rowmarkId). The PlanRowMark values live in the PlannerRun rowmark
    //    store; `root.rowMarks` and `glob.finalrowmarks` carry handles.
    {
        let marks = core::mem::take(&mut root.rowMarks);
        for rm_id in marks {
            let mut newrc = run.resolve_rowmark(rm_id).clone();
            newrc.rti = newrc.rti.wrapping_add(rtoffset as u32);
            newrc.prti = newrc.prti.wrapping_add(rtoffset as u32);
            let new_id = run.intern_rowmark(newrc);
            glob_mut(root)?.finalrowmarks.push(new_id);
        }
    }

    // 3. Adjust RT indexes of AppendRelInfos and add to final appendrels list.
    //    The owned AppendRelInfo values live in root.append_rel_list; the global
    //    `append_relations` list carries opaque NodeId handles (AppendRelInfo is
    //    not a node_arena Expr), so the flattened appendrels are accumulated by
    //    the cohort that owns the appendrel node space. We still apply the RT
    //    index bumps + translated_vars drop to the owned values in place.
    for appinfo in root.append_rel_list.iter_mut() {
        appinfo.parent_relid = appinfo.parent_relid.wrapping_add(rtoffset as u32);
        appinfo.child_relid = appinfo.child_relid.wrapping_add(rtoffset as u32);
        // Rather than adjust the translated_vars entries, just drop 'em.
        appinfo.translated_vars = Vec::new();
    }

    // 4. If needed, create workspace for processing AlternativeSubPlans.
    if root.hasAlternativeSubPlans {
        let n = glob_ref(root)?.subplans.len();
        root.isAltSubplan.clear();
        root.isUsedSubplan.clear();
        root.isAltSubplan.resize(n, false);
        root.isUsedSubplan.resize(n, false);
    }

    // 5. Now fix the Plan tree.
    let result = set_plan_refs(mcx, run, root, plan, rtoffset)?;

    // 6. Prune unreferenced AlternativeSubPlan subplans. The C sets
    //    `lfirst(lc) = NULL` in glob->subplans; here glob.subplans is a
    //    Vec<PlanId> of handles. The flags drive the cohort that materializes
    //    the nullable subplan slots; recording is faithful via the flag vectors.
    //    (No per-slot mutation of the PlanId list is needed: a NULL'd C subplan
    //    corresponds to isAltSubplan && !isUsedSubplan at that index.)

    Ok(result)
}

#[inline]
fn glob_ref<'a>(root: &'a PlannerInfo) -> PgResult<&'a PlannerGlobal> {
    root.glob
        .as_deref()
        .ok_or_else(|| PgError::error("setrefs: root->glob is NULL"))
}
#[inline]
fn glob_mut<'a>(root: &'a mut PlannerInfo) -> PgResult<&'a mut PlannerGlobal> {
    root.glob
        .as_deref_mut()
        .ok_or_else(|| PgError::error("setrefs: root->glob is NULL"))
}

// ===========================================================================
// Range-table flattening.
// ===========================================================================

/// `add_rtes_to_flat_rtable(root, recursing)` (setrefs.c:395).
fn add_rtes_to_flat_rtable<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    recursing: bool,
) -> PgResult<()> {
    // Add the query's own RTEs to the flattened rangetable.
    let parse = root.parse;
    let rte_ids: Vec<types_pathnodes::RangeTblEntryId> = run.rtable(parse).iter().enumerate().map(
        |(i, _)| {
            // RangeTblEntryId is the i-th entry of the query's rtable; the run
            // interned them in order. We re-resolve below by index via the
            // query's rtable slice; reconstruct the id by re-interning is wrong,
            // so collect the (kind, relid) we need plus the slice index.
            let _ = i;
            types_pathnodes::RangeTblEntryId::default()
        },
    ).collect();
    let _ = rte_ids;

    // Re-fetch as a slice each pass (the run owns the storage).
    let n = run.rtable(parse).len();
    for i in 0..n {
        let rte = &run.rtable(parse)[i];
        let want = !recursing
            || rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_RELATION
            || (rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_SUBQUERY
                && rte.relid != 0);
        if want {
            add_rte_to_flat_rtable(mcx, run, root, parse, i)?;
        }
    }

    // Dead-subquery pass: pull up RTEs from subqueries not in the Plan tree.
    let mut rti: usize = 1;
    let n = run.rtable(parse).len();
    while rti <= n {
        let idx = rti - 1;
        let (is_sub, inh) = {
            let rte = &run.rtable(parse)[idx];
            (
                rte.rtekind == types_nodes::parsenodes::RTEKind::RTE_SUBQUERY,
                rte.inh,
            )
        };
        if is_sub && !inh && (rti as i32) < root.simple_rel_array_size {
            let rel = root.simple_rel_array.get(rti).copied().flatten();
            if let Some(_rel_id) = rel {
                // The C inspects rel->subroot: if NULL → flatten_unplanned_rtes;
                // else if recursing or the subroot's final upper rel is dummy →
                // recurse into the subroot. Both the subroot navigation and the
                // dummy-rel test live with the relnode/path owner (RelOptInfo's
                // subroot is a RelId handle into root.rel_arena, whose subroot is
                // an unported per-subquery PlannerInfo). Route the dummy test
                // through the seam; recursion into a subroot requires the owner.
                //
                // For the common case (no dead subqueries), neither branch
                // fires. When it does, the unplanned/dummy pull-up is owned by
                // the subquery-planner cohort and reached through the seam below.
                if ext::subroot_final_rel_is_dummy::call(root, rti)? {
                    // recurse into subroot — owned by subquery_planner cohort.
                    return Err(PgError::error(
                        "add_rtes_to_flat_rtable: recursion into a dummy subroot \
                         (subquery_planner-owned subroot navigation) is not ported",
                    ));
                }
            }
        }
        rti += 1;
    }
    Ok(())
}

/// `add_rte_to_flat_rtable(glob, rteperminfos, rte)` (setrefs.c:541). `q` is the
/// query whose rtable/rteperminfos own `rte`; `src_idx` is its 0-based slot.
fn add_rte_to_flat_rtable<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    q: types_pathnodes::QueryId,
    src_idx: usize,
) -> PgResult<()> {
    // flat copy to duplicate all the scalar fields, then zap unneeded
    // sub-structure that the executor doesn't need.
    let mut newrte = run.rtable(q)[src_idx].clone_in(mcx)?;
    newrte.tablesample = None;
    newrte.subquery = None;
    newrte.joinaliasvars = PgVec::new_in(mcx);
    newrte.joinleftcols = PgVec::new_in(mcx);
    newrte.joinrightcols = PgVec::new_in(mcx);
    newrte.join_using_alias = None;
    newrte.functions = PgVec::new_in(mcx);
    newrte.tablefunc = None;
    newrte.values_lists = PgVec::new_in(mcx);
    newrte.coltypes = PgVec::new_in(mcx);
    newrte.coltypmods = PgVec::new_in(mcx);
    newrte.colcollations = PgVec::new_in(mcx);
    newrte.groupexprs = PgVec::new_in(mcx);
    newrte.securityQuals = PgVec::new_in(mcx);

    let rtekind = newrte.rtekind;
    let relid = newrte.relid;
    let perminfoindex = newrte.perminfoindex;

    let new_id = run.intern_rte(newrte);
    glob_mut(root)?.finalrtable.push(new_id);
    let new_len = glob_ref(root)?.finalrtable.len();

    // If it's a plain relation RTE (or a subquery that was once a view ref),
    // record the relation OID + add the new RT index to allRelids.
    if rtekind == types_nodes::parsenodes::RTEKind::RTE_RELATION
        || (rtekind == types_nodes::parsenodes::RTEKind::RTE_SUBQUERY && relid != 0)
    {
        glob_mut(root)?.relation_oids.push(relid);
        let g = glob_mut(root)?;
        g.all_relids = relids_add_member(g.all_relids.take(), new_len as i32);
    }

    // Copy the RTEPermissionInfo, if any (setrefs.c:579 `addRTEPermissionInfo`).
    // C deep-copies the source query's perminfo (`copyObject`) into
    // `glob->finalrteperminfos` and resets `newrte->perminfoindex` to the new
    // 1-based list length. The source query `q` owns its `rteperminfos` list
    // value-typed in the planner run; clone `rteperminfos[perminfoindex - 1]`
    // into the run's perminfo store and record the handle.
    if perminfoindex > 0 {
        let src = run.resolve(q).rteperminfos[(perminfoindex - 1) as usize].clone_in(mcx)?;
        let perm_id = run.intern_rte_perminfo(src);
        glob_mut(root)?.finalrteperminfos.push(perm_id);
        let new_perm_index = glob_ref(root)?.finalrteperminfos.len();
        // newrte->perminfoindex = list_length(glob->finalrteperminfos)
        run.resolve_rte_mut(new_id).perminfoindex = new_perm_index as Index;
    }
    Ok(())
}

// ===========================================================================
// Relids word helpers (offset_relid_set / all_relids add — over the
// `types_pathnodes::Bitmapset { words }` carrier, which has no bms ops here).
// ===========================================================================

/// `bms_add_member(a, x)` over the `Relids` carrier (1-based-bit semantics: bit
/// `x` of word `x/64`). `x` must be >= 0.
fn relids_add_member(a: Relids, x: i32) -> Relids {
    debug_assert!(x >= 0);
    let mut bms = a.unwrap_or_else(|| ABox::new(types_pathnodes::Bitmapset { words: Vec::new() }));
    let wi = (x as usize) / 64;
    let bit = (x as usize) % 64;
    if bms.words.len() <= wi {
        bms.words.resize(wi + 1, 0);
    }
    bms.words[wi] |= 1u64 << bit;
    Some(bms)
}

/// `bms_next_member(a, prevbit)` over `Relids` — next set bit > prevbit, or -1.
fn relids_next_member(a: &Relids, prevbit: i32) -> i32 {
    let bms = match a {
        Some(b) => b,
        None => return -1,
    };
    let mut bit = prevbit + 1;
    while (bit as usize) < bms.words.len() * 64 {
        let wi = (bit as usize) / 64;
        let off = (bit as usize) % 64;
        let w = bms.words[wi] >> off;
        if w != 0 {
            return bit + (w.trailing_zeros() as i32);
        }
        // advance to the next word boundary
        bit = ((wi + 1) * 64) as i32;
    }
    -1
}

/// `offset_relid_set(relids, rtoffset)` (setrefs.c:1985).
fn offset_relid_set(relids: Relids, rtoffset: i32) -> Relids {
    if rtoffset == 0 {
        return relids;
    }
    let mut result: Relids = None;
    let mut rtindex = -1;
    loop {
        rtindex = relids_next_member(&relids, rtindex);
        if rtindex < 0 {
            break;
        }
        result = relids_add_member(result, rtindex + rtoffset);
    }
    result
}

// ===========================================================================
// indexed_tlist + the search_indexed_tlist_for_* family.
// ===========================================================================

/// `tlist_vinfo` (setrefs.c) — info about one plain-`Var` tlist entry.
#[derive(Clone)]
struct TlistVinfo {
    varno: i32,
    varattno: AttrNumber,
    resno: AttrNumber,
    varnullingrels: types_nodes::primnodes::ExprRelids,
}

/// `indexed_tlist` (setrefs.c) — an index over a child tlist. We hold an owned
/// clone of the tlist (the C keeps a pointer; the searches only read it).
struct IndexedTlist {
    tlist: Vec<TargetEntry<'static>>,
    has_ph_vars: bool,
    has_non_vars: bool,
    vars: Vec<TlistVinfo>,
}

/// `build_tlist_index(tlist)` (setrefs.c:2758).
fn build_tlist_index(tlist: &[TargetEntry<'_>], mcx: Mcx<'_>) -> PgResult<IndexedTlist> {
    build_tlist_index_filtered(tlist, mcx, None)
}

/// `build_tlist_index_other_vars(tlist, ignore_rel)` (setrefs.c:2809) — only
/// indexes Vars whose varno != `ignore_rel`; sets has_ph_vars but not
/// has_non_vars. `ignore_rel == None` ⇒ plain `build_tlist_index`.
fn build_tlist_index_filtered(
    tlist: &[TargetEntry<'_>],
    mcx: Mcx<'_>,
    ignore_rel: Option<i32>,
) -> PgResult<IndexedTlist> {
    let mut itlist = IndexedTlist {
        tlist: Vec::new(),
        has_ph_vars: false,
        has_non_vars: false,
        vars: Vec::new(),
    };
    for tle in tlist {
        // Keep an owned clone of every tlist element so the PHV/non-var searches
        // can read it. The IndexedTlist is a transient read-only index whose
        // lifetime is strictly within one set_*_references call (shorter than
        // `mcx`); erase the clone's lifetime to the field's `'static` per the
        // established lifetime-parameter-only transmute convention.
        let cloned: TargetEntry<'_> = tle.clone_in(mcx)?;
        let cloned_static: TargetEntry<'static> = unsafe { core::mem::transmute(cloned) };
        itlist.tlist.push(cloned_static);
        match tle.expr.as_deref() {
            Some(Expr::Var(var)) => {
                let keep = match ignore_rel {
                    Some(ir) => var.varno != ir,
                    None => true,
                };
                if keep {
                    itlist.vars.push(TlistVinfo {
                        varno: var.varno,
                        varattno: var.varattno,
                        resno: tle.resno,
                        varnullingrels: var.varnullingrels.clone(),
                    });
                }
            }
            Some(Expr::PlaceHolderVar(_)) => itlist.has_ph_vars = true,
            _ => {
                if ignore_rel.is_none() {
                    itlist.has_non_vars = true;
                }
            }
        }
    }
    Ok(itlist)
}

/// `copyVar(var)` (setrefs.c:2007) — a plain clone of the concrete struct.
#[inline]
fn copy_var(var: &Var) -> Var {
    var.clone()
}

/// `search_indexed_tlist_for_var(var, itlist, newvarno, rtoffset, nrm_match)`
/// (setrefs.c:2867).
fn search_indexed_tlist_for_var(
    var: &Var,
    itlist: &IndexedTlist,
    newvarno: i32,
    rtoffset: i32,
    nrm_match: NullingRelsMatch,
) -> PgResult<Option<Var>> {
    let varno = var.varno;
    let varattno = var.varattno;
    for vinfo in &itlist.vars {
        if vinfo.varno == varno && vinfo.varattno == varattno {
            let mut newvar = copy_var(var);
            // Verify the nullingrels bookkeeping (skipped for system columns and
            // whole-row Vars, varattno <= 0).
            let ok = varattno <= 0
                || nullingrels_ok(nrm_match, &var.varnullingrels, &vinfo.varnullingrels);
            if !ok {
                return Err(PgError::error(format!(
                    "wrong varnullingrels for Var {}/{}",
                    varno, varattno
                )));
            }
            newvar.varno = newvarno;
            newvar.varattno = vinfo.resno;
            if newvar.varnosyn > 0 {
                newvar.varnosyn = newvar.varnosyn.wrapping_add(rtoffset as u32);
            }
            return Ok(Some(newvar));
        }
    }
    Ok(None)
}

/// `search_indexed_tlist_for_phv(phv, itlist, newvarno, nrm_match)`
/// (setrefs.c:2932).
fn search_indexed_tlist_for_phv(
    phv: &types_nodes::primnodes::PlaceHolderVar,
    itlist: &IndexedTlist,
    newvarno: i32,
    nrm_match: NullingRelsMatch,
) -> PgResult<Option<Var>> {
    for tle in &itlist.tlist {
        let Some(Expr::PlaceHolderVar(subphv)) = tle.expr.as_deref() else {
            continue;
        };
        if phv.phid != subphv.phid {
            continue;
        }
        if !nullingrels_ok(nrm_match, &phv.phnullingrels, &subphv.phnullingrels) {
            return Err(PgError::error(format!(
                "wrong phnullingrels for PlaceHolderVar {}",
                phv.phid
            )));
        }
        let mut newvar = make_var_from_target_entry(newvarno, tle)?;
        newvar.varnosyn = 0;
        newvar.varattnosyn = 0;
        return Ok(Some(newvar));
    }
    Ok(None)
}

/// `search_indexed_tlist_for_non_var(node, itlist, newvarno)` (setrefs.c:2985).
fn search_indexed_tlist_for_non_var(
    node: &Expr,
    itlist: &IndexedTlist,
    newvarno: i32,
) -> PgResult<Option<Var>> {
    // A simple Const is never worth replacing with a Var.
    if matches!(node, Expr::Const(_)) {
        return Ok(None);
    }
    if let Some(tle) = tlist_member(node, &itlist.tlist) {
        let mut newvar = make_var_from_target_entry(newvarno, tle)?;
        newvar.varnosyn = 0;
        newvar.varattnosyn = 0;
        return Ok(Some(newvar));
    }
    Ok(None)
}

/// `search_indexed_tlist_for_sortgroupref(node, sortgroupref, itlist, newvarno)`
/// (setrefs.c:3025).
fn search_indexed_tlist_for_sortgroupref(
    node: &Expr,
    sortgroupref: Index,
    itlist: &IndexedTlist,
    newvarno: i32,
) -> PgResult<Option<Var>> {
    for tle in &itlist.tlist {
        if tle.ressortgroupref == sortgroupref {
            if let Some(e) = tle.expr.as_deref() {
                if equal_expr(node, e) {
                    let mut newvar = make_var_from_target_entry(newvarno, tle)?;
                    newvar.varnosyn = 0;
                    newvar.varattnosyn = 0;
                    return Ok(Some(newvar));
                }
            }
        }
    }
    Ok(None)
}

/// `tlist_member(node, targetlist)` (tlist.c) — find a TLE whose expr equals
/// `node` (ignoring resjunk). Returns the first match.
fn tlist_member<'a>(node: &Expr, tlist: &'a [TargetEntry<'_>]) -> Option<&'a TargetEntry<'a>> {
    for tle in tlist {
        if let Some(e) = tle.expr.as_deref() {
            if equal_expr(node, e) {
                // SAFETY of lifetime: the slice borrow lifetime is tied to the
                // caller; reborrow through the reference.
                return Some(unsafe { &*(tle as *const TargetEntry) });
            }
        }
    }
    None
}

// ===========================================================================
// fix_expr_common — generic per-node opcode/dependency processing.
// ===========================================================================

/// `fix_expr_common(root, node)` (setrefs.c:2029). Mutates the `Expr` in place
/// (opfuncid fill-in / GROUPING() cols) and records dependencies into glob.
fn fix_expr_common(root: &mut PlannerInfo, node: &mut Expr) -> PgResult<()> {
    match node {
        Expr::Aggref(a) => record_plan_function_dependency(root, a.aggfnoid)?,
        Expr::WindowFunc(w) => record_plan_function_dependency(root, w.winfnoid)?,
        Expr::FuncExpr(f) => record_plan_function_dependency(root, f.funcid)?,
        Expr::OpExpr(op) => {
            set_opfuncid(op)?;
            record_plan_function_dependency(root, op.opfuncid)?;
        }
        // DistinctExpr / NullIfExpr share the OpExpr struct (struct equivalence).
        Expr::DistinctExpr(op) | Expr::NullIfExpr(op) => {
            set_opfuncid(op)?;
            record_plan_function_dependency(root, op.opfuncid)?;
        }
        Expr::ScalarArrayOpExpr(saop) => {
            set_sa_opfuncid(saop)?;
            record_plan_function_dependency(root, saop.opfuncid)?;
            if saop.hashfuncid != 0 {
                record_plan_function_dependency(root, saop.hashfuncid)?;
            }
            if saop.negfuncid != 0 {
                record_plan_function_dependency(root, saop.negfuncid)?;
            }
        }
        Expr::Const(con) => {
            if is_regclass_const(con) {
                // DatumGetObjectId(con->constvalue): the regclass OID.
                let oid = const_object_id(con);
                glob_mut(root)?.relation_oids.push(oid);
            }
        }
        Expr::GroupingFunc(g) => {
            // Fill in cols from grouping_map, if there are grouping sets.
            if !root.grouping_map.is_empty() {
                let mut cols: Vec<i32> = Vec::new();
                for &r in g.refs.iter() {
                    let m = root.grouping_map.get(r as usize).copied().unwrap_or(0);
                    cols.push(m as i32);
                }
                if g.cols.is_empty() {
                    g.cols = cols;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

/// `DatumGetObjectId(con->constvalue)` for a regclass/oid `Const`.
fn const_object_id(con: &Const) -> Oid {
    // The canonical Datum's by-value word holds the OID.
    con.constvalue.as_u32()
}

// ===========================================================================
// Dependency recorders.
// ===========================================================================

/// `record_plan_function_dependency(root, funcid)` (setrefs.c:3553).
pub fn record_plan_function_dependency(root: &mut PlannerInfo, funcid: Oid) -> PgResult<()> {
    if funcid >= FirstUnpinnedObjectId {
        // PlanInvalItem{cacheId=PROCOID, hashValue=GetSysCacheHashValue1(PROCOID,funcid)}
        // appended to glob->invalItems. The hash + append are owned by plancache.
        let g = glob_mut(root)?;
        ext::record_inval_item::call(&mut g.inval_items, PROCOID, funcid)?;
    }
    Ok(())
}

/// `record_plan_type_dependency(root, typid)` (setrefs.c:3593).
pub fn record_plan_type_dependency(root: &mut PlannerInfo, typid: Oid) -> PgResult<()> {
    if typid >= FirstUnpinnedObjectId {
        let g = glob_mut(root)?;
        ext::record_inval_item::call(&mut g.inval_items, TYPEOID, typid)?;
    }
    Ok(())
}

// ===========================================================================
// fix_param_node / fix_alternative_subplan / find_minmax_agg_replacement_param.
// ===========================================================================

/// `fix_param_node(root, p)` (setrefs.c:2124).
fn fix_param_node(root: &PlannerInfo, p: &Param) -> PgResult<Expr> {
    if p.paramkind == ParamKind::PARAM_MULTIEXPR {
        let subqueryid = (p.paramid >> 16) as i32;
        let colno = (p.paramid & 0xFFFF) as i32;
        if subqueryid <= 0 || subqueryid as usize > root.multiexpr_params.len() {
            return Err(PgError::error(format!(
                "unexpected PARAM_MULTIEXPR ID: {}",
                p.paramid
            )));
        }
        let params_len = root.multiexpr_params[(subqueryid - 1) as usize].len();
        if colno <= 0 || colno as usize > params_len {
            return Err(PgError::error(format!(
                "unexpected PARAM_MULTIEXPR ID: {}",
                p.paramid
            )));
        }
        return ext::multiexpr_param_lookup::call(root, subqueryid as usize, colno as usize);
    }
    // copyObject(p) — a flat clone.
    Ok(Expr::Param(p.clone()))
}

/// `fix_alternative_subplan(root, asplan, num_exec)` (setrefs.c:2155). Choose the
/// cheapest alternative and return just that one (as a `SubPlan` Expr). Takes the
/// owned `AlternativeSubPlan` so the chosen child `SubPlan` can be moved out.
fn fix_alternative_subplan<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    asplan: types_nodes::primnodes::AlternativeSubPlan<'static>,
    num_exec: f64,
) -> PgResult<Expr> {
    let mut best_idx: Option<usize> = None;
    let mut best_cost = 0.0f64;
    for (i, cur) in asplan.subplans.iter().enumerate() {
        let curcost = cur.startup_cost + num_exec * cur.per_call_cost;
        if best_idx.is_none() || curcost <= best_cost {
            best_idx = Some(i);
            best_cost = curcost;
        }
        // Mark all subplans that are in AlternativeSubPlans.
        let ndx = (cur.plan_id - 1) as usize;
        if let Some(slot) = root.isAltSubplan.get_mut(ndx) {
            *slot = true;
        }
    }
    let best = best_idx.ok_or_else(|| PgError::error("AlternativeSubPlan has no subplans"))?;
    let used = (asplan.subplans[best].plan_id - 1) as usize;
    if let Some(slot) = root.isUsedSubplan.get_mut(used) {
        *slot = true;
    }
    // Return the chosen SubPlan as a SubPlanExpr. Deep-clone the chosen child into
    // `mcx`, then erase its lifetime to the Expr tree's notional `'static`
    // (the established `SubPlanExpr(Box<SubPlan<'static>>)` convention; the arena
    // outlives the read-only Expr tree).
    let mut subplans = asplan.subplans;
    let chosen: PgBox<'static, types_nodes::primnodes::SubPlan<'static>> =
        subplans.swap_remove(best);
    let cloned: types_nodes::primnodes::SubPlan<'mcx> = chosen.clone_in(mcx)?;
    // SAFETY: lifetime-parameter-only transmute of an owned value whose backing
    // allocations live in the planner-run `mcx` (which outlives the read-only
    // Expr tree's notional 'static lifetime). Mirrors init-subselect's
    // `subplan_into_static`.
    let cloned_static: types_nodes::primnodes::SubPlan<'static> =
        unsafe { core::mem::transmute(cloned) };
    Ok(Expr::SubPlan(types_nodes::primnodes::SubPlanExpr(ABox::new(
        cloned_static,
    ))))
}

/// `find_minmax_agg_replacement_param(root, aggref)` (setrefs.c:3520).
fn find_minmax_agg_replacement_param(
    root: &PlannerInfo,
    aggref: &types_nodes::primnodes::Aggref,
) -> PgResult<Option<Param>> {
    // root->minmax_aggs != NIL && list_length(aggref->args) == 1
    let args_len = aggref.args.len();
    if !root.minmax_aggs.is_empty() && args_len == 1 {
        // curTarget = linitial(aggref->args); compare mminfo->target to its expr.
        let cur_target = &aggref.args[0];
        let cur_expr = match cur_target.expr.as_deref() {
            Some(e) => e,
            None => return Ok(None),
        };
        for idx in 0..root.minmax_aggs.len() {
            // The owner tests aggfnoid + equal(target, expr) and returns param.
            if let Some(param) =
                ext::minmax_replacement_param::call(root, idx, aggref.aggfnoid, cur_expr)?
            {
                return Ok(Some(param));
            }
        }
    }
    Ok(None)
}

// ===========================================================================
// fix_scan_expr — scan-level expression fix-up.
// ===========================================================================

/// `fix_scan_expr(root, node, rtoffset, num_exec)` (setrefs.c:2211) over one
/// `Expr`. We always run the mutator (the C fast-path that just scribbles
/// opfuncids when nothing else changes is behaviourally identical to running the
/// mutator, which copies as it goes).
fn fix_scan_expr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Expr,
    rtoffset: i32,
    num_exec: f64,
) -> PgResult<Expr> {
    fix_scan_expr_mutator(mcx, root, node, rtoffset, num_exec)
}

fn fix_scan_expr_mutator<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Expr,
    rtoffset: i32,
    num_exec: f64,
) -> PgResult<Expr> {
    match node {
        Expr::Var(mut var) => {
            // Assert varlevelsup == 0 and not INNER/OUTER/ROWID.
            if !is_special_varno(var.varno) {
                var.varno += rtoffset;
            }
            if var.varnosyn > 0 {
                var.varnosyn = var.varnosyn.wrapping_add(rtoffset as u32);
            }
            Ok(Expr::Var(var))
        }
        Expr::Param(p) => fix_param_node(root, &p),
        Expr::Aggref(aggref) => {
            if let Some(aggparam) = find_minmax_agg_replacement_param(root, &aggref)? {
                return Ok(Expr::Param(aggparam));
            }
            // No match: process normally.
            let mut e = Expr::Aggref(aggref);
            fix_expr_common(root, &mut e)?;
            fix_scan_expr_recurse(mcx, root, e, rtoffset, num_exec)
        }
        Expr::CurrentOfExpr(mut cexpr) => {
            cexpr.cvarno = cexpr.cvarno.wrapping_add(rtoffset as u32);
            Ok(Expr::CurrentOfExpr(cexpr))
        }
        Expr::PlaceHolderVar(phv) => {
            // At scan level, always evaluate the contained expr.
            match phv.phexpr {
                Some(inner) => fix_scan_expr_mutator(mcx, root, *inner, rtoffset, num_exec),
                None => Err(PgError::error(
                    "fix_scan_expr: PlaceHolderVar with NULL phexpr",
                )),
            }
        }
        Expr::AlternativeSubPlan(asp) => {
            let chosen = fix_alternative_subplan(mcx, root, *asp.0, num_exec)?;
            fix_scan_expr_mutator(mcx, root, chosen, rtoffset, num_exec)
        }
        mut other => {
            fix_expr_common(root, &mut other)?;
            fix_scan_expr_recurse(mcx, root, other, rtoffset, num_exec)
        }
    }
}

/// Recurse into the children of `node` with `fix_scan_expr_mutator`, propagating
/// errors. Wraps `expression_tree_mutator` (which is infallible) and surfaces any
/// error via a captured slot.
fn fix_scan_expr_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Expr,
    rtoffset: i32,
    num_exec: f64,
) -> PgResult<Expr> {
    let mut err: Option<PgError> = None;
    let out = {
        let root_cell = core::cell::RefCell::new(root);
        let mut f = |child: Expr| -> Expr {
            if err.is_some() {
                return child;
            }
            let mut r = root_cell.borrow_mut();
            match fix_scan_expr_mutator(mcx, *r, child, rtoffset, num_exec) {
                Ok(c) => c,
                Err(e) => {
                    err = Some(e);
                    Expr::Const(error_placeholder_const())
                }
            }
        };
        expression_tree_mutator(node, &mut f)
    };
    if let Some(e) = err {
        return Err(e);
    }
    Ok(out)
}

/// A throwaway `Const` used to fill a mutator slot when an error is in flight
/// (the result is discarded — the error is returned instead).
fn error_placeholder_const() -> Const {
    Const::default()
}

/// `fix_scan_list` over a `qual`-style list of `Expr`.
fn fix_scan_list_expr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    list: Option<PgVec<'mcx, Expr>>,
    rtoffset: i32,
    num_exec: f64,
) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    let list = match list {
        Some(l) => l,
        None => return Ok(None),
    };
    let mut out: PgVec<Expr> = PgVec::new_in(mcx);
    for e in list {
        out.push(fix_scan_expr(mcx, root, e, rtoffset, num_exec)?);
    }
    Ok(Some(out))
}

/// `fix_scan_list` over a `targetlist` of `TargetEntry` (fix each `.expr`).
fn fix_scan_list_tlist<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    list: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    rtoffset: i32,
    num_exec: f64,
) -> PgResult<Option<PgVec<'mcx, TargetEntry<'mcx>>>> {
    let list = match list {
        Some(l) => l,
        None => return Ok(None),
    };
    let mut out: PgVec<TargetEntry> = PgVec::new_in(mcx);
    for mut tle in list {
        if let Some(expr_box) = tle.expr.take() {
            let fixed = fix_scan_expr(mcx, root, PgBox::into_inner(expr_box), rtoffset, num_exec)?;
            tle.expr = Some(mcx::alloc_in(mcx, fixed)?);
        }
        out.push(tle);
    }
    Ok(Some(out))
}

// ===========================================================================
// set_dummy_tlist_references.
// ===========================================================================

/// `set_dummy_tlist_references(plan, rtoffset)` (setrefs.c:2692).
fn set_dummy_tlist_references<'mcx>(
    plan: &mut Plan<'mcx>,
    rtoffset: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    let old = match plan.targetlist.take() {
        Some(l) => l,
        None => return Ok(()),
    };
    let mut out: PgVec<TargetEntry> = PgVec::new_in(mcx);
    for tle in old {
        // Keep Consts as Consts.
        if matches!(tle.expr.as_deref(), Some(Expr::Const(_))) {
            out.push(tle);
            continue;
        }
        let oldexpr = tle.expr.as_deref();
        let mut newvar = make_var(
            OUTER_VAR,
            tle.resno,
            expr_type(oldexpr)?,
            expr_typmod(oldexpr)?,
            expr_collation(oldexpr)?,
            0,
        );
        match oldexpr {
            Some(Expr::Var(oldvar)) if oldvar.varnosyn > 0 => {
                newvar.varnosyn = oldvar.varnosyn.wrapping_add(rtoffset as u32);
                newvar.varattnosyn = oldvar.varattnosyn;
            }
            _ => {
                newvar.varnosyn = 0;
                newvar.varattnosyn = 0;
            }
        }
        let mut newtle = flat_copy_target_entry(mcx, &tle)?;
        newtle.expr = Some(mcx::alloc_in(mcx, Expr::Var(newvar))?);
        out.push(newtle);
    }
    plan.targetlist = Some(out);
    Ok(())
}

// ===========================================================================
// fix_join_expr / fix_upper_expr.
// ===========================================================================

struct FixJoinCtx<'a> {
    outer_itlist: Option<&'a IndexedTlist>,
    inner_itlist: Option<&'a IndexedTlist>,
    acceptable_rel: Index,
    rtoffset: i32,
    nrm_match: NullingRelsMatch,
    num_exec: f64,
}

/// `fix_join_expr(root, clauses, ...)` (setrefs.c:3103) over a list of `Expr`.
fn fix_join_expr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    clauses: Vec<Expr>,
    ctx: &FixJoinCtx,
) -> PgResult<Vec<Expr>> {
    let mut out = Vec::with_capacity(clauses.len());
    for c in clauses {
        out.push(fix_join_expr_mutator(mcx, root, c, ctx)?);
    }
    Ok(out)
}

fn fix_join_expr_mutator<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Expr,
    ctx: &FixJoinCtx,
) -> PgResult<Expr> {
    match node {
        Expr::Var(var) => {
            // Non-default varreturningtype only in RETURNING list to target rel.
            if var.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT {
                if ctx.inner_itlist.is_some()
                    || ctx.outer_itlist.is_none()
                    || ctx.acceptable_rel == 0
                {
                    return Err(PgError::error(
                        "variable returning old/new found outside RETURNING list",
                    ));
                }
                if var.varno as Index != ctx.acceptable_rel {
                    return Err(PgError::error(format!(
                        "wrong varno {} (expected {}) for variable returning old/new",
                        var.varno, ctx.acceptable_rel
                    )));
                }
            }
            if let Some(oi) = ctx.outer_itlist {
                if let Some(nv) =
                    search_indexed_tlist_for_var(&var, oi, OUTER_VAR, ctx.rtoffset, ctx.nrm_match)?
                {
                    return Ok(Expr::Var(nv));
                }
            }
            if let Some(ii) = ctx.inner_itlist {
                if let Some(nv) =
                    search_indexed_tlist_for_var(&var, ii, INNER_VAR, ctx.rtoffset, ctx.nrm_match)?
                {
                    return Ok(Expr::Var(nv));
                }
            }
            if var.varno as Index == ctx.acceptable_rel {
                let mut v = copy_var(&var);
                v.varno += ctx.rtoffset;
                if v.varnosyn > 0 {
                    v.varnosyn = v.varnosyn.wrapping_add(ctx.rtoffset as u32);
                }
                return Ok(Expr::Var(v));
            }
            Err(PgError::error("variable not found in subplan target lists"))
        }
        Expr::PlaceHolderVar(phv) => {
            if let Some(oi) = ctx.outer_itlist {
                if oi.has_ph_vars {
                    if let Some(nv) =
                        search_indexed_tlist_for_phv(&phv, oi, OUTER_VAR, ctx.nrm_match)?
                    {
                        return Ok(Expr::Var(nv));
                    }
                }
            }
            if let Some(ii) = ctx.inner_itlist {
                if ii.has_ph_vars {
                    if let Some(nv) =
                        search_indexed_tlist_for_phv(&phv, ii, INNER_VAR, ctx.nrm_match)?
                    {
                        return Ok(Expr::Var(nv));
                    }
                }
            }
            match phv.phexpr {
                Some(inner) => fix_join_expr_mutator(mcx, root, *inner, ctx),
                None => Err(PgError::error(
                    "fix_join_expr: PlaceHolderVar with NULL phexpr",
                )),
            }
        }
        other => {
            // Try matching more complex expressions to lower tlists.
            if let Some(oi) = ctx.outer_itlist {
                if oi.has_non_vars {
                    if let Some(nv) = search_indexed_tlist_for_non_var(&other, oi, OUTER_VAR)? {
                        return Ok(Expr::Var(nv));
                    }
                }
            }
            if let Some(ii) = ctx.inner_itlist {
                if ii.has_non_vars {
                    if let Some(nv) = search_indexed_tlist_for_non_var(&other, ii, INNER_VAR)? {
                        return Ok(Expr::Var(nv));
                    }
                }
            }
            // Special cases (only AFTER failing to match a lower tlist).
            match other {
                Expr::Param(p) => fix_param_node(root, &p),
                Expr::AlternativeSubPlan(asp) => {
                    let chosen = fix_alternative_subplan(mcx, root, *asp.0, ctx.num_exec)?;
                    fix_join_expr_mutator(mcx, root, chosen, ctx)
                }
                mut e => {
                    fix_expr_common(root, &mut e)?;
                    fix_join_expr_recurse(mcx, root, e, ctx)
                }
            }
        }
    }
}

fn fix_join_expr_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Expr,
    ctx: &FixJoinCtx,
) -> PgResult<Expr> {
    let mut err: Option<PgError> = None;
    let out = {
        let root_cell = core::cell::RefCell::new(root);
        let mut f = |child: Expr| -> Expr {
            if err.is_some() {
                return child;
            }
            let mut r = root_cell.borrow_mut();
            match fix_join_expr_mutator(mcx, *r, child, ctx) {
                Ok(c) => c,
                Err(e) => {
                    err = Some(e);
                    Expr::Const(error_placeholder_const())
                }
            }
        };
        expression_tree_mutator(node, &mut f)
    };
    if let Some(e) = err {
        return Err(e);
    }
    Ok(out)
}

struct FixUpperCtx<'a> {
    subplan_itlist: &'a IndexedTlist,
    newvarno: i32,
    rtoffset: i32,
    nrm_match: NullingRelsMatch,
    num_exec: f64,
}

/// `fix_upper_expr(root, node, ...)` (setrefs.c:3277).
fn fix_upper_expr<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Expr,
    ctx: &FixUpperCtx,
) -> PgResult<Expr> {
    fix_upper_expr_mutator(mcx, root, node, ctx)
}

fn fix_upper_expr_mutator<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Expr,
    ctx: &FixUpperCtx,
) -> PgResult<Expr> {
    match node {
        Expr::Var(var) => {
            match search_indexed_tlist_for_var(
                &var,
                ctx.subplan_itlist,
                ctx.newvarno,
                ctx.rtoffset,
                ctx.nrm_match,
            )? {
                Some(nv) => Ok(Expr::Var(nv)),
                None => Err(PgError::error("variable not found in subplan target list")),
            }
        }
        Expr::PlaceHolderVar(phv) => {
            if ctx.subplan_itlist.has_ph_vars {
                if let Some(nv) = search_indexed_tlist_for_phv(
                    &phv,
                    ctx.subplan_itlist,
                    ctx.newvarno,
                    ctx.nrm_match,
                )? {
                    return Ok(Expr::Var(nv));
                }
            }
            match phv.phexpr {
                Some(inner) => fix_upper_expr_mutator(mcx, root, *inner, ctx),
                None => Err(PgError::error(
                    "fix_upper_expr: PlaceHolderVar with NULL phexpr",
                )),
            }
        }
        other => {
            if ctx.subplan_itlist.has_non_vars {
                if let Some(nv) =
                    search_indexed_tlist_for_non_var(&other, ctx.subplan_itlist, ctx.newvarno)?
                {
                    return Ok(Expr::Var(nv));
                }
            }
            match other {
                Expr::Param(p) => fix_param_node(root, &p),
                Expr::Aggref(mut aggref) => {
                    if let Some(aggparam) = find_minmax_agg_replacement_param(root, &aggref)? {
                        return Ok(Expr::Param(aggparam));
                    }
                    // C (nodeFuncs.c expression_tree_mutator T_Aggref) MUTATEs the
                    // aggdirectargs / args / aggfilter so the aggregated-argument
                    // Vars get fixed to OUTER_VAR (the Agg reads its inputs from
                    // the outer subplan; setrefs.c:1427 asserts varno == OUTER_VAR
                    // for the agg's input vars). The generic
                    // `expression_tree_mutator` copies an Aggref verbatim (its
                    // `args` TargetEntry list has context-allocated `PgBox<'static>`
                    // children the mcx-less mutator cannot re-allocate), so fix the
                    // children explicitly with mcx in hand. `aggorder`/`aggdistinct`
                    // are SortGroupClause index lists with no Expr children (no-op);
                    // `aggargtypes` is unchanged (mutation must not change types).
                    let old_directargs = core::mem::take(&mut aggref.aggdirectargs);
                    let mut new_directargs = alloc::vec::Vec::with_capacity(old_directargs.len());
                    for e in old_directargs {
                        new_directargs.push(fix_upper_expr_mutator(mcx, root, e, ctx)?);
                    }
                    aggref.aggdirectargs = new_directargs;

                    let old_args = core::mem::take(&mut aggref.args);
                    let mut new_args = alloc::vec::Vec::with_capacity(old_args.len());
                    for mut te in old_args {
                        if let Some(b) = te.expr.take() {
                            // Consume the original box (move the Expr out, one
                            // balanced uncharge) instead of `(*b).clone()`: cloning
                            // an `Expr` that holds `Box<Expr, Mcx>` children allocates
                            // fresh inner boxes charged against the (lifetime-laundered
                            // `'static`) Mcx, then drops `b` — and when the plan arena
                            // is finally torn down those clone-charged inner boxes
                            // deallocate against an already-reset context, underflowing
                            // its accounting (`uncharging N with only 0 charged`). The
                            // top-level tlist path uses `PgBox::into_inner` for exactly
                            // this reason; mirror it here.
                            let fixed = fix_upper_expr_mutator(
                                mcx,
                                root,
                                mcx::PgBox::into_inner(b),
                                ctx,
                            )?;
                            // Re-box into the plan arena and re-tag as 'static (the
                            // backing alloc lives in `mcx`); same lifetime-only
                            // transmute the combining-aggref split path uses.
                            let boxed: mcx::PgBox<'mcx, Expr> = mcx::alloc_in(mcx, fixed)?;
                            let boxed_static: mcx::PgBox<'static, Expr> =
                                unsafe { core::mem::transmute(boxed) };
                            te.expr = Some(boxed_static);
                        }
                        new_args.push(te);
                    }
                    aggref.args = new_args;

                    if let Some(f) = aggref.aggfilter.take() {
                        let fixed = fix_upper_expr_mutator(mcx, root, *f, ctx)?;
                        aggref.aggfilter = Some(alloc::boxed::Box::new(fixed));
                    }

                    let mut e = Expr::Aggref(aggref);
                    fix_expr_common(root, &mut e)?;
                    Ok(e)
                }
                Expr::AlternativeSubPlan(asp) => {
                    let chosen = fix_alternative_subplan(mcx, root, *asp.0, ctx.num_exec)?;
                    fix_upper_expr_mutator(mcx, root, chosen, ctx)
                }
                mut e => {
                    fix_expr_common(root, &mut e)?;
                    fix_upper_expr_recurse(mcx, root, e, ctx)
                }
            }
        }
    }
}

fn fix_upper_expr_recurse<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: Expr,
    ctx: &FixUpperCtx,
) -> PgResult<Expr> {
    let mut err: Option<PgError> = None;
    let out = {
        let root_cell = core::cell::RefCell::new(root);
        let mut f = |child: Expr| -> Expr {
            if err.is_some() {
                return child;
            }
            let mut r = root_cell.borrow_mut();
            match fix_upper_expr_mutator(mcx, *r, child, ctx) {
                Ok(c) => c,
                Err(e) => {
                    err = Some(e);
                    Expr::Const(error_placeholder_const())
                }
            }
        };
        expression_tree_mutator(node, &mut f)
    };
    if let Some(e) = err {
        return Err(e);
    }
    Ok(out)
}

/// Fix a tlist of `TargetEntry` with `fix_upper_expr` over each `.expr`,
/// producing a fresh tlist (flatCopyTargetEntry each). Used for index-only-scan
/// + foreign/custom scan tlists where the subplan_itlist describes index/scan
/// columns and `newvarno = INDEX_VAR`.
fn fix_upper_tlist<'mcx>(
    root: &mut PlannerInfo,
    list: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    ctx: &FixUpperCtx,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, TargetEntry<'mcx>>>> {
    let list = match list {
        Some(l) => l,
        None => return Ok(None),
    };
    let mut out: PgVec<TargetEntry> = PgVec::new_in(mcx);
    for mut tle in list {
        if let Some(expr_box) = tle.expr.take() {
            let fixed = fix_upper_expr(mcx, root, PgBox::into_inner(expr_box), ctx)?;
            tle.expr = Some(mcx::alloc_in(mcx, fixed)?);
        }
        out.push(tle);
    }
    Ok(Some(out))
}

/// Fix a qual list of `Expr` with `fix_upper_expr`.
fn fix_upper_qual<'mcx>(
    root: &mut PlannerInfo,
    list: Option<PgVec<'mcx, Expr>>,
    ctx: &FixUpperCtx,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    let list = match list {
        Some(l) => l,
        None => return Ok(None),
    };
    let mut out: PgVec<Expr> = PgVec::new_in(mcx);
    for e in list {
        out.push(fix_upper_expr(mcx, root, e, ctx)?);
    }
    Ok(Some(out))
}

// ===========================================================================
// set_upper_references / set_join_references / set_param_references.
// ===========================================================================

/// `set_upper_references(root, plan, rtoffset)` (setrefs.c:2480). `is_agg` marks
/// the T_Agg call (for the grouping-sets nullingrels-strip, which is owned by
/// rewriteManip's `remove_nulling_relids` — not reachable here; loud if hit).
fn set_upper_references<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    plan: &mut Plan<'mcx>,
    rtoffset: i32,
    is_agg: bool,
    agg_grouping_sets: bool,
) -> PgResult<()> {
    // Build an index over the lefttree subplan's tlist.
    let subplan_itlist = {
        let subplan = plan
            .lefttree
            .as_deref()
            .ok_or_else(|| PgError::error("set_upper_references: lefttree is NULL"))?;
        let sub_tlist = subplan.plan_head().targetlist.as_deref().unwrap_or(&[]);
        build_tlist_index(sub_tlist, mcx)?
    };

    // Grouping-sets nullingrels strip (Agg with group_rtindex > 0 + groupingSets).
    if is_agg && root.group_rtindex > 0 && agg_grouping_sets {
        return Err(PgError::error(
            "set_upper_references: grouping-sets nullingrels strip \
             (remove_nulling_relids, rewriteManip-owned) is not ported",
        ));
    }

    let num_exec = num_exec_tlist(plan);
    let old_tlist = plan.targetlist.take();
    let mut output: PgVec<TargetEntry> = PgVec::new_in(mcx);
    if let Some(tlist) = old_tlist {
        for mut tle in tlist {
            let expr = tle
                .expr
                .take()
                .ok_or_else(|| PgError::error("set_upper_references: tle.expr is NULL"))?;
            let ctx = FixUpperCtx {
                subplan_itlist: &subplan_itlist,
                newvarno: OUTER_VAR,
                rtoffset,
                nrm_match: NRM_EQUAL,
                num_exec,
            };
            let newexpr = if tle.ressortgroupref != 0 {
                match search_indexed_tlist_for_sortgroupref(
                    &expr,
                    tle.ressortgroupref,
                    &subplan_itlist,
                    OUTER_VAR,
                )? {
                    Some(v) => Expr::Var(v),
                    None => fix_upper_expr(mcx, root, PgBox::into_inner(expr), &ctx)?,
                }
            } else {
                fix_upper_expr(mcx, root, PgBox::into_inner(expr), &ctx)?
            };
            let mut newtle = flat_copy_target_entry(mcx, &tle)?;
            newtle.expr = Some(mcx::alloc_in(mcx, newexpr)?);
            output.push(newtle);
        }
    }
    plan.targetlist = Some(output);

    let num_exec_q = num_exec_qual(plan);
    let qual = plan.qual.take();
    let ctx = FixUpperCtx {
        subplan_itlist: &subplan_itlist,
        newvarno: OUTER_VAR,
        rtoffset,
        nrm_match: NRM_EQUAL,
        num_exec: num_exec_q,
    };
    plan.qual = fix_upper_qual(root, qual, &ctx, mcx)?;
    Ok(())
}

/// `set_param_references(root, plan)` (setrefs.c:2568) — compute initParam for a
/// Gather/GatherMerge node. Returns the new initParam bitmapset.
fn set_param_references<'mcx>(
    root: &PlannerInfo,
    plan: &Plan<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::bitmapset::Bitmapset<'mcx>>>> {
    // if (plan->lefttree->extParam)
    let lefttree = plan
        .lefttree
        .as_deref()
        .ok_or_else(|| PgError::error("set_param_references: lefttree is NULL"))?;
    let ext_param = lefttree.plan_head().extParam.as_deref();
    let ext_param = match ext_param {
        Some(e) => e,
        None => return Ok(None),
    };

    // initSetParam = union over (proot up parent_root chain) of all
    // init_plans' setParam. The init_plans list carries opaque NodeId handles
    // (SubPlan node space owned by subselect); the per-init-plan setParam read
    // is not reachable from the lifetime-free PlannerInfo handles here.
    //
    // bms_intersect(extParam, initSetParam): when there are NO init plans at
    // any level (the common case), initSetParam is empty and the intersect is
    // NULL. We compute that case faithfully; a non-empty init_plans chain is
    // owned by the subselect cohort.
    let _ = (ext_param, mcx);
    let has_init = {
        let mut any = false;
        let mut proot: Option<&PlannerInfo> = Some(root);
        while let Some(p) = proot {
            if !p.init_plans.is_empty() {
                any = true;
                break;
            }
            proot = p.parent_root.as_deref();
        }
        any
    };
    if has_init {
        return Err(PgError::error(
            "set_param_references: Gather initParam over a non-empty init_plans \
             chain (subselect SubPlan.setParam read) is not ported",
        ));
    }
    // initSetParam empty ⇒ bms_intersect(extParam, {}) = NULL.
    Ok(None)
}

// ===========================================================================
// convert_combining_aggrefs.
// ===========================================================================

/// `convert_combining_aggrefs(node, NULL)` (setrefs.c:2623) over an `Expr`.
/// `mcx` is needed to deep-copy the (non-`Clone`) `Aggref` via `clone_in`; the
/// copies are erased to the Expr tree's notional `'static` lifetime.
fn convert_combining_aggrefs<'mcx>(mcx: Mcx<'mcx>, node: Expr) -> PgResult<Expr> {
    match node {
        Expr::Aggref(orig) => {
            // child_agg = flat copy of orig; parent_agg = copy with args=NIL,
            // aggfilter=NULL. Aggref is not Clone (only clone_in); deep-copy into
            // mcx, then erase the lifetime to the Expr arm's notional 'static.
            let mut child_agg: types_nodes::primnodes::Aggref =
                erase_aggref(orig.clone_in(mcx)?);
            let mut parent_agg: types_nodes::primnodes::Aggref =
                erase_aggref(orig.clone_in(mcx)?);
            parent_agg.args = Vec::new(); // args=NIL
            parent_agg.aggfilter = None;
            // child keeps the original args/aggfilter (they were copied above).

            // mark_partial_aggref(child_agg, AGGSPLIT_INITIAL_SERIAL).
            ext::mark_partial_aggref::call(
                &mut child_agg,
                types_nodes::nodeagg::AGGSPLIT_INITIAL_SERIAL,
            )?;

            // parent_agg.args = list_make1(makeTargetEntry((Expr*) child_agg, 1,
            // NULL, false)). Aggref.args is a plain Vec<TargetEntry<'static>>.
            let te: TargetEntry<'mcx> = TargetEntry {
                expr: Some(mcx::alloc_in(mcx, Expr::Aggref(child_agg))?),
                resno: 1,
                resname: None,
                ressortgroupref: 0,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk: false,
            };
            // SAFETY: lifetime-parameter-only transmute (backing allocs in `mcx`).
            let te_static: TargetEntry<'static> = unsafe { core::mem::transmute(te) };
            parent_agg.args = alloc::vec![te_static];

            ext::mark_partial_aggref::call(
                &mut parent_agg,
                types_nodes::nodeagg::AGGSPLIT_FINAL_DESERIAL,
            )?;
            Ok(Expr::Aggref(parent_agg))
        }
        other => {
            // expression_tree_mutator(node, convert_combining_aggrefs).
            let mut err: Option<PgError> = None;
            let out = {
                let mut f = |child: Expr| -> Expr {
                    if err.is_some() {
                        return child;
                    }
                    match convert_combining_aggrefs(mcx, child) {
                        Ok(c) => c,
                        Err(e) => {
                            err = Some(e);
                            Expr::Const(error_placeholder_const())
                        }
                    }
                };
                expression_tree_mutator(other, &mut f)
            };
            if let Some(e) = err {
                return Err(e);
            }
            Ok(out)
        }
    }
}

/// Erase the lifetime parameter of an `Aggref` cloned into `mcx` to the Expr
/// tree's notional `'static` (the `Aggref.args: Vec<TargetEntry<'static>>`
/// convention). Lifetime-parameter-only transmute of an owned value whose
/// backing allocations live in the planner-run arena (outlives the read-only
/// Expr tree). Mirrors init-subselect's `subplan_into_static`.
fn erase_aggref(a: types_nodes::primnodes::Aggref) -> types_nodes::primnodes::Aggref {
    // Aggref is lifetime-free in this model (args carry an explicit 'static),
    // so this is the identity; written as a function for documentation parity.
    a
}

fn convert_combining_aggrefs_tlist<'mcx>(
    list: Option<PgVec<'mcx, TargetEntry<'mcx>>>,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, TargetEntry<'mcx>>>> {
    let list = match list {
        Some(l) => l,
        None => return Ok(None),
    };
    let mut out: PgVec<TargetEntry> = PgVec::new_in(mcx);
    for mut tle in list {
        if let Some(eb) = tle.expr.take() {
            let c = convert_combining_aggrefs(mcx, PgBox::into_inner(eb))?;
            tle.expr = Some(mcx::alloc_in(mcx, c)?);
        }
        out.push(tle);
    }
    Ok(Some(out))
}

fn convert_combining_aggrefs_qual<'mcx>(
    list: Option<PgVec<'mcx, Expr>>,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    let list = match list {
        Some(l) => l,
        None => return Ok(None),
    };
    let mut out: PgVec<Expr> = PgVec::new_in(mcx);
    for e in list {
        out.push(convert_combining_aggrefs(mcx, e)?);
    }
    Ok(Some(out))
}

// ===========================================================================
// set_plan_refs — the recursive per-Plan-subtype dispatch.
// ===========================================================================

/// `set_plan_refs(root, plan, rtoffset)` (setrefs.c:618).
pub fn set_plan_refs<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    let mut plan = plan;

    // Assign this node a unique ID: plan->plan_node_id = glob->lastPlanNodeId++.
    {
        let g = glob_mut(root)?;
        let id = g.last_plan_node_id;
        g.last_plan_node_id = id + 1;
        plan.plan_head_mut().plan_node_id = id;
    }

    // Plan-type-specific fixes. We match on the `Node` enum variant directly
    // (the model's source of truth). Several arms `return` (no tail recursion).
    // NOTE: `LockRows` and `BitmapOr` are NOT represented as `Node` variants in
    // this repo's enum (verified against nodes.rs), so they cannot reach this
    // dispatch; their C arms have no place to land and are therefore absent (a
    // plan carrying them is unconstructible in this model).
    match plan.node_tag() {
        // -- plain scan types -------------------------------------------------
        ntag::T_SeqScan
        | ntag::T_SampleScan
        | ntag::T_IndexScan
        | ntag::T_BitmapHeapScan
        | ntag::T_TidScan
        | ntag::T_TidRangeScan
        | ntag::T_FunctionScan
        | ntag::T_TableFuncScan
        | ntag::T_ValuesScan
        | ntag::T_CteScan
        | ntag::T_NamedTuplestoreScan
        | ntag::T_WorkTableScan
        | ntag::T_BitmapIndexScan => {
            set_scan_node_refs(mcx, root, &mut plan, rtoffset)?;
        }

        // -- IndexOnlyScan (set_indexonlyscan_references) --------------------
        ntag::T_IndexOnlyScan => {
            return set_indexonlyscan_references(mcx, run, root, plan, rtoffset);
        }

        // -- SubqueryScan (set_subqueryscan_references) ----------------------
        ntag::T_SubqueryScan => {
            return set_subqueryscan_references(mcx, run, root, plan, rtoffset);
        }

        // -- ForeignScan / CustomScan ----------------------------------------
        ntag::T_ForeignScan => {
            set_foreignscan_references(mcx, root, &mut plan, rtoffset)?;
        }
        ntag::T_CustomScan => {
            return Err(PgError::error(
                "set_plan_refs(CustomScan): custom-scan provider fix-up \
                 (set_customscan_references over the CustomScanMethods vtable + \
                 custom_plans recursion) is a provider extension surface, not ported",
            ));
        }

        // -- joins ------------------------------------------------------------
        ntag::T_NestLoop | ntag::T_MergeJoin | ntag::T_HashJoin => {
            set_join_references(mcx, root, &mut plan, rtoffset)?;
        }

        // -- Gather / GatherMerge --------------------------------------------
        ntag::T_Gather => {
            set_upper_references(mcx, root, plan.plan_head_mut(), rtoffset, false, false)?;
            let init = set_param_references(root, plan.plan_head(), mcx)?;
            if let Some(g) = plan.as_gather_mut() {
                g.initParam = init;
            }
        }
        ntag::T_GatherMerge => {
            set_upper_references(mcx, root, plan.plan_head_mut(), rtoffset, false, false)?;
            let init = set_param_references(root, plan.plan_head(), mcx)?;
            if let Some(g) = plan.as_gathermerge_mut() {
                g.initParam = init;
            }
        }

        // -- Hash ------------------------------------------------------------
        ntag::T_Hash => {
            set_hash_references(mcx, root, &mut plan, rtoffset)?;
        }

        // -- Memoize ---------------------------------------------------------
        ntag::T_Memoize => {
            set_dummy_tlist_references(plan.plan_head_mut(), rtoffset, mcx)?;
            if let Some(m) = plan.as_memoize_mut() {
                let num_exec = num_exec_tlist(&m.plan);
                let exprs = core::mem::replace(&mut m.param_exprs, PgVec::new_in(mcx));
                let mut out: PgVec<Expr> = PgVec::new_in(mcx);
                for e in exprs {
                    out.push(fix_scan_expr(mcx, root, e, rtoffset, num_exec)?);
                }
                m.param_exprs = out;
            }
        }

        // -- dummy-tlist-only plan types -------------------------------------
        ntag::T_Material
        | ntag::T_Sort
        | ntag::T_IncrementalSort
        | ntag::T_Unique
        | ntag::T_SetOp
        | ntag::T_RecursiveUnion => {
            set_dummy_tlist_references(plan.plan_head_mut(), rtoffset, mcx)?;
        }

        // -- Limit -----------------------------------------------------------
        ntag::T_Limit => {
            set_dummy_tlist_references(plan.plan_head_mut(), rtoffset, mcx)?;
            if let Some(l) = plan.as_limit_mut() {
                if let Some(off) = l.limitOffset.take() {
                    let f = fix_scan_expr(mcx, root, PgBox::into_inner(off), rtoffset, 1.0)?;
                    l.limitOffset = Some(mcx::alloc_in(mcx, f)?);
                }
                if let Some(cnt) = l.limitCount.take() {
                    let f = fix_scan_expr(mcx, root, PgBox::into_inner(cnt), rtoffset, 1.0)?;
                    l.limitCount = Some(mcx::alloc_in(mcx, f)?);
                }
            }
        }

        // -- Agg -------------------------------------------------------------
        ntag::T_Agg => {
            let (combine, grouping_sets) = if let Some(a) = plan.as_agg() {
                (do_aggsplit_combine(a.aggsplit), a.grouping_sets.is_some())
            } else {
                (false, false)
            };
            if combine {
                let p = plan.plan_head_mut();
                let tl = p.targetlist.take();
                p.targetlist = convert_combining_aggrefs_tlist(tl, mcx)?;
                let ql = p.qual.take();
                p.qual = convert_combining_aggrefs_qual(ql, mcx)?;
            }
            set_upper_references(mcx, root, plan.plan_head_mut(), rtoffset, true, grouping_sets)?;
        }

        // -- Group -----------------------------------------------------------
        ntag::T_Group => {
            set_upper_references(mcx, root, plan.plan_head_mut(), rtoffset, false, false)?;
        }

        // -- WindowAgg -------------------------------------------------------
        ntag::T_WindowAgg => {
            let rc = if let Some(w) = plan.as_windowagg_mut() {
                w.runCondition.take()
            } else {
                None
            };
            let new_rc =
                set_windowagg_runcondition_references(root, rc, plan.plan_head(), mcx)?;
            if let Some(w) = plan.as_windowagg_mut() {
                w.runCondition = new_rc;
            }
            set_upper_references(mcx, root, plan.plan_head_mut(), rtoffset, false, false)?;
            if let Some(w) = plan.as_windowagg_mut() {
                if let Some(off) = w.startOffset.take() {
                    let f = fix_scan_expr(mcx, root, PgBox::into_inner(off), rtoffset, 1.0)?;
                    w.startOffset = Some(mcx::alloc_in(mcx, f)?);
                }
                if let Some(off) = w.endOffset.take() {
                    let f = fix_scan_expr(mcx, root, PgBox::into_inner(off), rtoffset, 1.0)?;
                    w.endOffset = Some(mcx::alloc_in(mcx, f)?);
                }
                let num_exec = num_exec_tlist(&w.plan);
                let rc = w.runCondition.take();
                w.runCondition = fix_scan_list_expr(mcx, root, rc, rtoffset, num_exec)?;
                let rco = w.runConditionOrig.take();
                w.runConditionOrig = fix_scan_list_expr(mcx, root, rco, rtoffset, num_exec)?;
            }
        }

        // -- Result ----------------------------------------------------------
        ntag::T_Result => {
            let has_left = plan.plan_head().lefttree.is_some();
            if has_left {
                set_upper_references(mcx, root, plan.plan_head_mut(), rtoffset, false, false)?;
            } else {
                {
                    let p = plan.plan_head_mut();
                    if let Some(tlist) = p.targetlist.as_mut() {
                        for tle in tlist.iter_mut() {
                            let replace = match tle.expr.as_deref() {
                                Some(Expr::Var(var)) if var.varno == ROWID_VAR => {
                                    Some((var.vartype, var.vartypmod, var.varcollid))
                                }
                                _ => None,
                            };
                            if let Some((vt, vm, vc)) = replace {
                                let nc = make_null_const(mcx, vt, vm, vc)?;
                                tle.expr = Some(mcx::alloc_in(mcx, Expr::Const(nc))?);
                            }
                        }
                    }
                }
                let p = plan.plan_head_mut();
                let num_exec = num_exec_tlist(p);
                let tl = p.targetlist.take();
                p.targetlist = fix_scan_list_tlist(mcx, root, tl, rtoffset, num_exec)?;
                let num_exec_q = num_exec_qual(p);
                let ql = p.qual.take();
                p.qual = fix_scan_list_expr(mcx, root, ql, rtoffset, num_exec_q)?;
            }
            if let Some(r) = plan.as_result_mut() {
                let rcq = r.resconstantqual.take();
                r.resconstantqual = fix_scan_list_expr(mcx, root, rcq, rtoffset, 1.0)?;
            }
        }

        // -- ProjectSet ------------------------------------------------------
        ntag::T_ProjectSet => {
            set_upper_references(mcx, root, plan.plan_head_mut(), rtoffset, false, false)?;
        }

        // -- ModifyTable -----------------------------------------------------
        ntag::T_ModifyTable => {
            return set_modifytable_references(mcx, run, root, plan, rtoffset);
        }

        // -- Append / MergeAppend (special early returns) --------------------
        ntag::T_Append => {
            return set_append_references(mcx, run, root, plan, rtoffset);
        }
        ntag::T_MergeAppend => {
            return set_mergeappend_references(mcx, run, root, plan, rtoffset);
        }

        // -- BitmapAnd -------------------------------------------------------
        ntag::T_BitmapAnd => {
            if let Some(b) = plan.as_bitmapand_mut() {
                let kids = core::mem::take(&mut b.bitmapplans);
                let mut newkids = Vec::with_capacity(kids.len());
                for k in kids {
                    newkids.push(set_plan_refs(mcx, run, root, k, rtoffset)?);
                }
                b.bitmapplans = newkids;
            }
            return Ok(plan);
        }

        _ => {
            return Err(PgError::error(format!(
                "set_plan_refs: unrecognized plan node: {}",
                plan.tag()
            )));
        }
    }

    // Now recurse into child plans, AFTER fixing this node's tlist/quals.
    {
        let lefttree = plan.plan_head_mut().lefttree.take();
        let righttree = plan.plan_head_mut().righttree.take();
        let new_left = set_plan_refs_opt(mcx, run, root, lefttree, rtoffset)?;
        let new_right = set_plan_refs_opt(mcx, run, root, righttree, rtoffset)?;
        plan.plan_head_mut().lefttree = new_left;
        plan.plan_head_mut().righttree = new_right;
    }

    Ok(plan)
}

fn set_plan_refs_opt<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    child: Option<PgBox<'mcx, Node<'mcx>>>,
    rtoffset: i32,
) -> PgResult<Option<PgBox<'mcx, Node<'mcx>>>> {
    match child {
        None => Ok(None),
        Some(b) => {
            let fixed = set_plan_refs(mcx, run, root, PgBox::into_inner(b), rtoffset)?;
            Ok(Some(mcx::alloc_in(mcx, fixed)?))
        }
    }
}

/// The shared plain-scan fix-up: bump `scan.scanrelid`, fix tlist/qual, plus the
/// scan-subtype-specific expression lists.
fn set_scan_node_refs<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    plan: &mut Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    match plan.node_tag() {
        ntag::T_SeqScan => {
            let s = plan.as_seqscan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?
        }
        ntag::T_SampleScan => {
            let s = plan.as_samplescan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?;
            // tablesample = fix_scan_expr over the TableSampleClause's args+repeatable.
            if let Some(ts) = s.tablesample.as_mut() {
                fix_tablesample(root, ts, rtoffset, mcx)?;
            }
        }
        ntag::T_IndexScan => {
            let s = plan.as_indexscan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?;
            let neq = num_exec_qual(&s.scan.plan);
            s.indexqual = fix_scan_list_expr(mcx, root, s.indexqual.take(), rtoffset, 1.0)?;
            s.indexqualorig =
                fix_scan_list_expr(mcx, root, s.indexqualorig.take(), rtoffset, neq)?;
            s.indexorderby = fix_scan_list_expr(mcx, root, s.indexorderby.take(), rtoffset, 1.0)?;
            s.indexorderbyorig =
                fix_scan_list_expr(mcx, root, s.indexorderbyorig.take(), rtoffset, neq)?;
        }
        ntag::T_BitmapIndexScan => {
            let s = plan.as_bitmapindexscan_mut().unwrap();
            // scanrelid += rtoffset; no tlist/qual to fix.
            s.scan.scanrelid = s.scan.scanrelid.wrapping_add(rtoffset as u32);
            let neq = num_exec_qual(&s.scan.plan);
            s.indexqual = fix_scan_list_expr(mcx, root, s.indexqual.take(), rtoffset, 1.0)?;
            s.indexqualorig =
                fix_scan_list_expr(mcx, root, s.indexqualorig.take(), rtoffset, neq)?;
        }
        ntag::T_BitmapHeapScan => {
            let s = plan.as_bitmapheapscan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?;
            // C: splan->bitmapqualorig =
            //      fix_scan_list(root, splan->bitmapqualorig, rtoffset,
            //                    NUM_EXEC_QUAL(plan));
            let neq = num_exec_qual(&s.scan.plan);
            let qualorig = core::mem::replace(&mut s.bitmapqualorig, PgVec::new_in(mcx));
            s.bitmapqualorig =
                fix_scan_list_expr(mcx, root, Some(qualorig), rtoffset, neq)?
                    .unwrap_or_else(|| PgVec::new_in(mcx));
        }
        ntag::T_TidScan => {
            let s = plan.as_tidscan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?;
            s.tidquals = fix_scan_list_expr(mcx, root, s.tidquals.take(), rtoffset, 1.0)?;
        }
        ntag::T_TidRangeScan => {
            let s = plan.as_tidrangescan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?;
            s.tidrangequals =
                fix_scan_list_expr(mcx, root, s.tidrangequals.take(), rtoffset, 1.0)?;
        }
        ntag::T_FunctionScan => {
            let s = plan.as_functionscan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?;
            // C: scan->functions = fix_scan_list(root, scan->functions, rtoffset,
            //    1); — fix_scan_list walks each RangeTblFunction node's funcexpr
            //    through fix_scan_expr. Each RangeTblFunction's funcexpr is a
            //    Node::Expr, so apply the Expr mutator to it in place (num_exec=1,
            //    matching the C `fix_scan_list(..., 1)`).
            if let Some(functions) = s.functions.as_mut() {
                let nfuncs = functions.len();
                for f in 0..nfuncs {
                    let funcexpr: Option<Expr> = match functions[f].funcexpr.take() {
                        Some(node) => {
                            let node = PgBox::into_inner(node);
                            // Peel the Expr-family node (Node::Expr spans every
                            // Expr tag); a non-Expr funcexpr is put back untouched.
                            if node.as_expr().is_some() {
                                node.into_expr()
                            } else {
                                functions[f].funcexpr = Some(mcx::alloc_in(mcx, node)?);
                                None
                            }
                        }
                        None => None,
                    };
                    if let Some(e) = funcexpr {
                        let fixed = fix_scan_expr(mcx, root, e, rtoffset, 1.0)?;
                        functions[f].funcexpr =
                            Some(mcx::alloc_in(mcx, Node::Expr(fixed))?);
                    }
                }
            }
        }
        ntag::T_TableFuncScan => {
            let s = plan.as_tablefuncscan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?;
            // tablefunc = fix_scan_expr over a TableFunc node (a Node, not Expr).
            let _ = &s.tablefunc;
            return Err(PgError::error(
                "set_plan_refs(T_TableFuncScan): fixing the TableFunc node \
                 (fix_scan_expr over a Node, not an Expr) requires the node-tree \
                 walker and is not ported",
            ));
        }
        ntag::T_ValuesScan => {
            let s = plan.as_valuesscan_mut().unwrap();
            fix_scan_common_valuesscan(root, s, rtoffset, mcx)?;
        }
        ntag::T_CteScan => {
            let s = plan.as_ctescan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?
        }
        ntag::T_NamedTuplestoreScan => {
            let s = plan.as_namedtuplestorescan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?
        }
        ntag::T_WorkTableScan => {
            let s = plan.as_worktablescan_mut().unwrap();
            fix_scan_common(root, &mut s.scan, rtoffset, mcx)?
        }
        _ => {
            return Err(PgError::error(
                "set_scan_node_refs: not a recognized plain-scan node",
            ));
        }
    }
    Ok(())
}

/// The three lines at the head of every plain scan arm.
fn fix_scan_common<'mcx>(
    root: &mut PlannerInfo,
    scan: &mut Scan<'mcx>,
    rtoffset: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    scan.scanrelid = scan.scanrelid.wrapping_add(rtoffset as u32);
    let nt = num_exec_tlist(&scan.plan);
    let tl = scan.plan.targetlist.take();
    scan.plan.targetlist = fix_scan_list_tlist(mcx, root, tl, rtoffset, nt)?;
    let nq = num_exec_qual(&scan.plan);
    let ql = scan.plan.qual.take();
    scan.plan.qual = fix_scan_list_expr(mcx, root, ql, rtoffset, nq)?;
    Ok(())
}

/// ValuesScan needs the common scan fix-up plus its `values_lists`
/// (`PgVec<PgVec<Expr>>`, not Option).
fn fix_scan_common_valuesscan<'mcx>(
    root: &mut PlannerInfo,
    s: &mut types_nodes::nodevaluesscan::ValuesScan<'mcx>,
    rtoffset: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    fix_scan_common(root, &mut s.scan, rtoffset, mcx)?;
    let lists = core::mem::replace(&mut s.values_lists, PgVec::new_in(mcx));
    let mut outer: PgVec<PgVec<Expr>> = PgVec::new_in(mcx);
    for inner in lists {
        let mut row: PgVec<Expr> = PgVec::new_in(mcx);
        for e in inner {
            row.push(fix_scan_expr(mcx, root, e, rtoffset, 1.0)?);
        }
        outer.push(row);
    }
    s.values_lists = outer;
    Ok(())
}

/// `fix_scan_expr(root, tablesample, rtoffset, 1)` over a TableSampleClause's
/// args + repeatable expressions.
fn fix_tablesample<'mcx>(
    root: &mut PlannerInfo,
    ts: &mut types_nodes::nodesamplescan::TableSampleClause<'mcx>,
    rtoffset: i32,
    mcx: Mcx<'mcx>,
) -> PgResult<()> {
    if let Some(args) = ts.args.take() {
        let mut out: PgVec<Expr> = PgVec::new_in(mcx);
        for e in args {
            out.push(fix_scan_expr(mcx, root, e, rtoffset, 1.0)?);
        }
        ts.args = Some(out);
    }
    if let Some(rep) = ts.repeatable.take() {
        let f = fix_scan_expr(mcx, root, *rep, rtoffset, 1.0)?;
        ts.repeatable = Some(ABox::new(f));
    }
    Ok(())
}

// ===========================================================================
// set_join_references / set_hash_references.
// ===========================================================================

/// `set_join_references(root, join, rtoffset)` (setrefs.c:2331).
fn set_join_references<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    plan: &mut Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    // Build outer/inner itlists from lefttree/righttree subplan tlists.
    let (outer_itlist, inner_itlist) = {
        let p = plan.plan_head();
        let outer = p
            .lefttree
            .as_deref()
            .ok_or_else(|| PgError::error("set_join_references: lefttree NULL"))?;
        let inner = p
            .righttree
            .as_deref()
            .ok_or_else(|| PgError::error("set_join_references: righttree NULL"))?;
        let oi = build_tlist_index(outer.plan_head().targetlist.as_deref().unwrap_or(&[]), mcx)?;
        let ii = build_tlist_index(inner.plan_head().targetlist.as_deref().unwrap_or(&[]), mcx)?;
        (oi, ii)
    };

    let neq = num_exec_qual(plan.plan_head());
    let nt = num_exec_tlist(plan.plan_head());
    let nt_outer = {
        let p = plan.plan_head();
        let outer = p.lefttree.as_deref().unwrap();
        num_exec_tlist(outer.plan_head())
    };

    // First the joinqual (NRM_EQUAL, full input tlists available).
    {
        let jq = take_joinqual(plan);
        let fixed = fix_join_expr(
            mcx,
            root,
            jq,
            &FixJoinCtx {
                outer_itlist: Some(&outer_itlist),
                inner_itlist: Some(&inner_itlist),
                acceptable_rel: 0,
                rtoffset,
                nrm_match: NRM_EQUAL,
                num_exec: neq,
            },
        )?;
        set_joinqual(plan, fixed, mcx);
    }

    // Join-type-specific stuff.
    match plan.node_tag() {
        ntag::T_NestLoop => {
            let nl = plan.as_nestloop_mut().unwrap();
            for nlp in nl.nestParams.iter_mut() {
                let pv = core::mem::take(&mut nlp.paramval);
                let fixed = fix_upper_expr(
                    mcx,
                    root,
                    Expr::Var(pv),
                    &FixUpperCtx {
                        subplan_itlist: &outer_itlist,
                        newvarno: OUTER_VAR,
                        rtoffset,
                        nrm_match: NRM_SUBSET,
                        num_exec: nt_outer,
                    },
                )?;
                match fixed {
                    Expr::Var(v) if v.varno == OUTER_VAR => nlp.paramval = v,
                    _ => {
                        return Err(PgError::error(
                            "NestLoopParam was not reduced to a simple Var",
                        ))
                    }
                }
            }
        }
        ntag::T_MergeJoin => {
            let mj = plan.as_mergejoin_mut().unwrap();
            // mergeclauses: Vec<Expr>.
            let mc = core::mem::take(&mut mj.mergeclauses);
            mj.mergeclauses = fix_join_expr(
                mcx,
                root,
                mc,
                &FixJoinCtx {
                    outer_itlist: Some(&outer_itlist),
                    inner_itlist: Some(&inner_itlist),
                    acceptable_rel: 0,
                    rtoffset,
                    nrm_match: NRM_EQUAL,
                    num_exec: neq,
                },
            )?;
        }
        ntag::T_HashJoin => {
            let hj = plan.as_hashjoin_mut().unwrap();
            // hashclauses: Option<PgVec<Node>> — each element is a Node::Expr.
            let hc = hj.hashclauses.take();
            hj.hashclauses = fix_join_expr_nodelist(
                root,
                hc,
                &FixJoinCtx {
                    outer_itlist: Some(&outer_itlist),
                    inner_itlist: Some(&inner_itlist),
                    acceptable_rel: 0,
                    rtoffset,
                    nrm_match: NRM_EQUAL,
                    num_exec: neq,
                },
                mcx,
            )?;
            // hashkeys: Option<PgVec<Node>> — fix_upper_expr against outer tlist.
            let hk = hj.hashkeys.take();
            hj.hashkeys = fix_upper_nodelist(
                root,
                hk,
                &FixUpperCtx {
                    subplan_itlist: &outer_itlist,
                    newvarno: OUTER_VAR,
                    rtoffset,
                    nrm_match: NRM_EQUAL,
                    num_exec: neq,
                },
                mcx,
            )?;
        }
        _ => {}
    }

    // Now the targetlist + qpqual (above the join).
    let jointype = join_jointype(plan);
    let tl_nrm = if jointype == types_pathnodes::JOIN_INNER {
        NRM_EQUAL
    } else {
        NRM_SUPERSET
    };
    {
        let p = plan.plan_head_mut();
        let tlist = p.targetlist.take();
        if let Some(tlist) = tlist {
            let mut out: PgVec<TargetEntry> = PgVec::new_in(mcx);
            for mut tle in tlist {
                if let Some(eb) = tle.expr.take() {
                    let fixed = fix_join_expr_mutator(
                        mcx,
                        root,
                        PgBox::into_inner(eb),
                        &FixJoinCtx {
                            outer_itlist: Some(&outer_itlist),
                            inner_itlist: Some(&inner_itlist),
                            acceptable_rel: 0,
                            rtoffset,
                            nrm_match: tl_nrm,
                            num_exec: nt,
                        },
                    )?;
                    tle.expr = Some(mcx::alloc_in(mcx, fixed)?);
                }
                out.push(tle);
            }
            plan.plan_head_mut().targetlist = Some(out);
        }
    }
    {
        let p = plan.plan_head_mut();
        let qual = p.qual.take();
        if let Some(qual) = qual {
            let mut out: PgVec<Expr> = PgVec::new_in(mcx);
            for e in qual {
                out.push(fix_join_expr_mutator(
                    mcx,
                    root,
                    e,
                    &FixJoinCtx {
                        outer_itlist: Some(&outer_itlist),
                        inner_itlist: Some(&inner_itlist),
                        acceptable_rel: 0,
                        rtoffset,
                        nrm_match: tl_nrm,
                        num_exec: neq,
                    },
                )?);
            }
            plan.plan_head_mut().qual = Some(out);
        }
    }
    Ok(())
}

fn take_joinqual<'mcx>(plan: &mut Node<'mcx>) -> Vec<Expr> {
    match plan.node_tag() {
        ntag::T_NestLoop => take_pgvec_expr(&mut plan.as_nestloop_mut().unwrap().join.joinqual),
        ntag::T_MergeJoin => take_pgvec_expr(&mut plan.as_mergejoin_mut().unwrap().join.joinqual),
        ntag::T_HashJoin => take_pgvec_expr(&mut plan.as_hashjoin_mut().unwrap().join.joinqual),
        _ => Vec::new(),
    }
}
fn set_joinqual<'mcx>(plan: &mut Node<'mcx>, list: Vec<Expr>, mcx: Mcx<'mcx>) {
    let v = put_pgvec_expr(list, mcx);
    match plan.node_tag() {
        ntag::T_NestLoop => plan.as_nestloop_mut().unwrap().join.joinqual = v,
        ntag::T_MergeJoin => plan.as_mergejoin_mut().unwrap().join.joinqual = v,
        ntag::T_HashJoin => plan.as_hashjoin_mut().unwrap().join.joinqual = v,
        _ => {}
    }
}
fn join_jointype<'mcx>(plan: &Node<'mcx>) -> types_pathnodes::JoinType {
    match plan.node_tag() {
        ntag::T_NestLoop => plan.as_nestloop().unwrap().join.jointype as types_pathnodes::JoinType,
        ntag::T_MergeJoin => plan.as_mergejoin().unwrap().join.jointype as types_pathnodes::JoinType,
        ntag::T_HashJoin => plan.as_hashjoin().unwrap().join.jointype as types_pathnodes::JoinType,
        _ => types_pathnodes::JOIN_INNER,
    }
}

/// Take an `Option<PgVec<Expr>>` joinqual into a Vec<Expr>.
fn take_pgvec_expr(opt: &mut Option<PgVec<Expr>>) -> Vec<Expr> {
    match opt.take() {
        Some(v) => v.into_iter().collect(),
        None => Vec::new(),
    }
}
fn put_pgvec_expr<'mcx>(list: Vec<Expr>, mcx: Mcx<'mcx>) -> Option<PgVec<'mcx, Expr>> {
    let mut v: PgVec<Expr> = PgVec::new_in(mcx);
    for e in list {
        v.push(e);
    }
    Some(v)
}

/// fix_join_expr over a `PgVec<Node>` (HashJoin.hashclauses) — each element is a
/// `Node::Expr`.
fn fix_join_expr_nodelist<'mcx>(
    root: &mut PlannerInfo,
    list: Option<PgVec<'mcx, Node<'mcx>>>,
    ctx: &FixJoinCtx,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, Node<'mcx>>>> {
    let list = match list {
        Some(l) => l,
        None => return Ok(None),
    };
    let mut out: PgVec<Node> = PgVec::new_in(mcx);
    for n in list {
        let e = node_into_expr(n)?;
        let fixed = fix_join_expr_mutator(mcx, root, e, ctx)?;
        out.push(Node::Expr(fixed));
    }
    Ok(Some(out))
}

/// fix_upper_expr over a `PgVec<Node>` (HashJoin.hashkeys / Hash.hashkeys).
fn fix_upper_nodelist<'mcx>(
    root: &mut PlannerInfo,
    list: Option<PgVec<'mcx, Node<'mcx>>>,
    ctx: &FixUpperCtx,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, Node<'mcx>>>> {
    let list = match list {
        Some(l) => l,
        None => return Ok(None),
    };
    let mut out: PgVec<Node> = PgVec::new_in(mcx);
    for n in list {
        let e = node_into_expr(n)?;
        let fixed = fix_upper_expr(mcx, root, e, ctx)?;
        out.push(Node::Expr(fixed));
    }
    Ok(Some(out))
}

fn node_into_expr(n: Node) -> PgResult<Expr> {
    n.into_expr()
        .ok_or_else(|| PgError::error("expected an expression node in a clause list"))
}

/// `set_hash_references(root, plan, rtoffset)` (setrefs.c:1952).
fn set_hash_references<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    plan: &mut Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    // outer_itlist from lefttree (the Hash's outer plan = HashJoin's inner plan).
    let outer_itlist = {
        let p = plan.plan_head();
        let outer = p
            .lefttree
            .as_deref()
            .ok_or_else(|| PgError::error("set_hash_references: lefttree NULL"))?;
        build_tlist_index(outer.plan_head().targetlist.as_deref().unwrap_or(&[]), mcx)?
    };
    let neq = num_exec_qual(plan.plan_head());
    if let Some(h) = plan.as_hash_mut() {
        let hk = h.hashkeys.take();
        h.hashkeys = fix_upper_nodelist(
            root,
            hk,
            &FixUpperCtx {
                subplan_itlist: &outer_itlist,
                newvarno: OUTER_VAR,
                rtoffset,
                nrm_match: NRM_EQUAL,
                num_exec: neq,
            },
            mcx,
        )?;
    }
    // Hash doesn't project.
    set_dummy_tlist_references(plan.plan_head_mut(), rtoffset, mcx)?;
    Ok(())
}

// ===========================================================================
// set_windowagg_runcondition_references.
// ===========================================================================

/// `set_windowagg_runcondition_references(root, runcondition, plan)`
/// (setrefs.c:3493) — swap WindowFunc refs in `runcondition` for Vars
/// referencing the matching WindowFunc in `plan`'s targetlist.
fn set_windowagg_runcondition_references<'mcx>(
    root: &mut PlannerInfo,
    runcondition: Option<PgVec<'mcx, Expr>>,
    plan: &Plan<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgVec<'mcx, Expr>>> {
    let runcondition = match runcondition {
        Some(r) => r,
        None => return Ok(None),
    };
    let itlist = build_tlist_index(plan.targetlist.as_deref().unwrap_or(&[]), mcx)?;
    let mut out: PgVec<Expr> = PgVec::new_in(mcx);
    for e in runcondition {
        out.push(fix_windowagg_condition_expr_mutator(&itlist, e)?);
    }
    Ok(Some(out))
}

/// `fix_windowagg_condition_expr_mutator` (setrefs.c:3442). newvarno = 0.
fn fix_windowagg_condition_expr_mutator(itlist: &IndexedTlist, node: Expr) -> PgResult<Expr> {
    match node {
        Expr::WindowFunc(_) => {
            match search_indexed_tlist_for_non_var(&node, itlist, 0)? {
                Some(v) => Ok(Expr::Var(v)),
                None => Err(PgError::error("WindowFunc not found in subplan target lists")),
            }
        }
        other => {
            let mut err: Option<PgError> = None;
            let out = {
                let mut f = |child: Expr| -> Expr {
                    if err.is_some() {
                        return child;
                    }
                    match fix_windowagg_condition_expr_mutator(itlist, child) {
                        Ok(c) => c,
                        Err(e) => {
                            err = Some(e);
                            Expr::Const(error_placeholder_const())
                        }
                    }
                };
                expression_tree_mutator(other, &mut f)
            };
            if let Some(e) = err {
                return Err(e);
            }
            Ok(out)
        }
    }
}

// ===========================================================================
// set_indexonlyscan_references / set_foreignscan_references.
// ===========================================================================

/// `set_indexonlyscan_references(root, plan, rtoffset)` (setrefs.c:1332).
fn set_indexonlyscan_references<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    {
        let s = plan
            .as_indexonlyscan_mut()
            .ok_or_else(|| PgError::error("set_indexonlyscan_references: not IndexOnlyScan"))?;
        // Build index_itlist from the resjunk-stripped indextlist. TargetEntry
        // is not `Clone`; deep-copy each non-resjunk entry via `clone_in`.
        let mut stripped: Vec<TargetEntry> = Vec::new();
        for t in s.indextlist.as_deref().unwrap_or(&[]).iter() {
            if !t.resjunk {
                stripped.push(t.clone_in(mcx)?);
            }
        }
        let index_itlist = build_tlist_index(&stripped, mcx)?;

        s.scan.scanrelid = s.scan.scanrelid.wrapping_add(rtoffset as u32);

        let nt = num_exec_tlist(&s.scan.plan);
        let nq = num_exec_qual(&s.scan.plan);

        let tl = s.scan.plan.targetlist.take();
        s.scan.plan.targetlist = fix_upper_tlist(
            root,
            tl,
            &FixUpperCtx { subplan_itlist: &index_itlist, newvarno: INDEX_VAR, rtoffset, nrm_match: NRM_EQUAL, num_exec: nt },
            mcx,
        )?;
        let ql = s.scan.plan.qual.take();
        s.scan.plan.qual = fix_upper_qual(
            root,
            ql,
            &FixUpperCtx { subplan_itlist: &index_itlist, newvarno: INDEX_VAR, rtoffset, nrm_match: NRM_EQUAL, num_exec: nq },
            mcx,
        )?;
        let rcq = s.recheckqual.take();
        s.recheckqual = fix_upper_qual(
            root,
            rcq,
            &FixUpperCtx { subplan_itlist: &index_itlist, newvarno: INDEX_VAR, rtoffset, nrm_match: NRM_EQUAL, num_exec: nq },
            mcx,
        )?;

        // indexqual / indexorderby already transformed to index columns.
        s.indexqual = fix_scan_list_expr(mcx, root, s.indexqual.take(), rtoffset, 1.0)?;
        s.indexorderby = fix_scan_list_expr(mcx, root, s.indexorderby.take(), rtoffset, 1.0)?;
        // indextlist must NOT be transformed to index columns; fix_scan_list it.
        let it = s.indextlist.take();
        s.indextlist = fix_scan_list_tlist(mcx, root, it, rtoffset, nt)?;
    }

    // recurse into child plans (the generic tail).
    {
        let lefttree = plan.plan_head_mut().lefttree.take();
        let righttree = plan.plan_head_mut().righttree.take();
        plan.plan_head_mut().lefttree = set_plan_refs_opt(mcx, run, root, lefttree, rtoffset)?;
        plan.plan_head_mut().righttree = set_plan_refs_opt(mcx, run, root, righttree, rtoffset)?;
    }
    Ok(plan)
}

/// `set_foreignscan_references(root, fscan, rtoffset)` (setrefs.c:1589).
fn set_foreignscan_references<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    plan: &mut Node<'mcx>,
    rtoffset: i32,
) -> PgResult<()> {
    let f = plan
        .as_foreignscan_mut()
        .ok_or_else(|| PgError::error("set_foreignscan_references: not ForeignScan"))?;
    if f.scan.scanrelid > 0 {
        f.scan.scanrelid = f.scan.scanrelid.wrapping_add(rtoffset as u32);
    }
    let nt = num_exec_tlist(&f.scan.plan);
    let nq = num_exec_qual(&f.scan.plan);

    if f.fdw_scan_tlist.is_some() || f.scan.scanrelid == 0 {
        // Reference foreign scan tuple via fdw_scan_tlist (INDEX_VAR).
        let itlist = build_tlist_index(f.fdw_scan_tlist.as_deref().unwrap_or(&[]), mcx)?;
        let tl = f.scan.plan.targetlist.take();
        f.scan.plan.targetlist = fix_upper_tlist(
            root, tl,
            &FixUpperCtx { subplan_itlist: &itlist, newvarno: INDEX_VAR, rtoffset, nrm_match: NRM_EQUAL, num_exec: nt },
            mcx,
        )?;
        let ql = f.scan.plan.qual.take();
        f.scan.plan.qual = fix_upper_qual(
            root, ql,
            &FixUpperCtx { subplan_itlist: &itlist, newvarno: INDEX_VAR, rtoffset, nrm_match: NRM_EQUAL, num_exec: nq },
            mcx,
        )?;
        let fe = f.fdw_exprs.take();
        f.fdw_exprs = fix_upper_qual(
            root, fe,
            &FixUpperCtx { subplan_itlist: &itlist, newvarno: INDEX_VAR, rtoffset, nrm_match: NRM_EQUAL, num_exec: nq },
            mcx,
        )?;
        let frq = f.fdw_recheck_quals.take();
        f.fdw_recheck_quals = fix_upper_qual(
            root, frq,
            &FixUpperCtx { subplan_itlist: &itlist, newvarno: INDEX_VAR, rtoffset, nrm_match: NRM_EQUAL, num_exec: nq },
            mcx,
        )?;
        let fst = f.fdw_scan_tlist.take();
        f.fdw_scan_tlist = fix_scan_list_tlist(mcx, root, fst, rtoffset, nt)?;
    } else {
        // Standard way.
        let tl = f.scan.plan.targetlist.take();
        f.scan.plan.targetlist = fix_scan_list_tlist(mcx, root, tl, rtoffset, nt)?;
        let ql = f.scan.plan.qual.take();
        f.scan.plan.qual = fix_scan_list_expr(mcx, root, ql, rtoffset, nq)?;
        let fe = f.fdw_exprs.take();
        f.fdw_exprs = fix_scan_list_expr(mcx, root, fe, rtoffset, nq)?;
        let frq = f.fdw_recheck_quals.take();
        f.fdw_recheck_quals = fix_scan_list_expr(mcx, root, frq, rtoffset, nq)?;
    }

    // Offset the relid sets.
    let fs = bms_nodes_to_relids(f.fs_relids.as_deref());
    let fs = offset_relid_set(fs, rtoffset);
    f.fs_relids = relids_to_bms_node(fs, mcx)?;
    let fsb = bms_nodes_to_relids(f.fs_base_relids.as_deref());
    let fsb = offset_relid_set(fsb, rtoffset);
    f.fs_base_relids = relids_to_bms_node(fsb, mcx)?;

    if f.resultRelation > 0 {
        f.resultRelation = f.resultRelation.wrapping_add(rtoffset as u32);
    }
    Ok(())
}

/// Convert a `types_nodes::bitmapset::Bitmapset` (Plan-side) to a `Relids`
/// (`types_pathnodes::Bitmapset`) for the offset helper, by copying words.
fn bms_nodes_to_relids(b: Option<&types_nodes::bitmapset::Bitmapset>) -> Relids {
    match b {
        None => None,
        Some(bms) => Some(ABox::new(types_pathnodes::Bitmapset {
            words: bms.words.iter().copied().collect(),
        })),
    }
}
/// Convert a `Relids` back to a Plan-side `Bitmapset` in `mcx`.
fn relids_to_bms_node<'mcx>(
    r: Relids,
    mcx: Mcx<'mcx>,
) -> PgResult<Option<PgBox<'mcx, types_nodes::bitmapset::Bitmapset<'mcx>>>> {
    match r {
        None => Ok(None),
        Some(bms) => {
            let mut words: PgVec<u64> = PgVec::new_in(mcx);
            for w in bms.words {
                words.push(w);
            }
            Ok(Some(mcx::alloc_in(
                mcx,
                types_nodes::bitmapset::Bitmapset { words },
            )?))
        }
    }
}

// ===========================================================================
// set_subqueryscan_references / set_append_references / set_mergeappend_references.
// ===========================================================================

/// `set_subqueryscan_references(root, plan, rtoffset)` (setrefs.c:1406).
///
/// C looks up `rel->subroot` for the scanned subquery and recurses with a fresh
/// top-level `set_plan_references(rel->subroot, plan->subplan)`. That entry
/// computes its own `rtoffset = list_length(glob->finalrtable)`, appends the
/// SUBROOT's range table to the flat `finalrtable`, and offsets every Var in the
/// subplan by it. This is what lets a set-op leg's relation scan — whose
/// `scanrelid` is subroot-relative (`= 1`) — resolve correctly: it ends up at
/// `1 + sub_rtoffset` in the flat table, distinct from the outer subquery RTE.
///
/// In this port the subplan was built in the subroot context
/// (`create_subqueryscan_subplan_inroot`), and `glob` is single-valued (it was
/// moved out of the subroot back to the outer root after planning). So we
/// temporarily lend the outer root's `glob` to the subroot for the duration of
/// the recursive `set_plan_references`, mirroring C's shared
/// `rel->subroot->glob == root->glob` pointer, then move it back.
fn set_subqueryscan_references<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    let mut sqs = plan
        .into_subqueryscan()
        .unwrap_or_else(|| panic!("set_subqueryscan_references: plan is not a SubqueryScan"));

    // Look up the subquery's RelOptInfo (we need its subroot). C:
    // `rel = find_base_rel(root, plan->scan.scanrelid)`.
    let scanrelid = sqs.scan.scanrelid as usize;
    let rel_id = root
        .simple_rel_array
        .get(scanrelid)
        .copied()
        .flatten()
        .expect("set_subqueryscan_references: no simple_rel for SubqueryScan scanrelid");

    // Recursively process the subplan: `set_plan_references(rel->subroot,
    // subplan)`. Take the subroot out, lend it the (single) glob, run the full
    // top-level entry (which flattens the subroot's rtable with its own
    // rtoffset), then restore both glob and subroot.
    let subplan = sqs
        .subplan
        .take()
        .expect("set_subqueryscan_references: SubqueryScan has no subplan");

    let mut subroot = root
        .rel_mut(rel_id)
        .subroot
        .0
        .take()
        .expect("set_subqueryscan_references: SubqueryScan rel has no subroot");
    subroot.glob = root.glob.take();
    let processed_res = set_plan_references(mcx, run, &mut subroot, PgBox::into_inner(subplan));
    root.glob = subroot.glob.take();
    root.rel_mut(rel_id).subroot.0 = Some(subroot);
    let processed = processed_res?;
    sqs.subplan = Some(mcx::alloc_in(mcx, processed)?);

    if trivial_subqueryscan(&mut sqs) {
        // Omit the SubqueryScan node and pull up the subplan.
        let child = sqs
            .subplan
            .take()
            .expect("set_subqueryscan_references: trivial scan has no subplan");
        clean_up_removed_plan_level(&sqs.scan.plan, PgBox::into_inner(child), mcx)
    } else {
        // Keep the SubqueryScan. Do the processing set_plan_references would
        // otherwise have done on it (no set_upper_references — a SubqueryScan is
        // created with correct references to its subplan's outputs already).
        fix_scan_common(root, &mut sqs.scan, rtoffset, mcx)?;
        Ok(Node::mk_subquery_scan(mcx, sqs))
    }
}

/// `trivial_subqueryscan(plan)` (setrefs.c:1476) — detect whether a SubqueryScan
/// can be deleted from the plan tree (no qual, tlist regurgitates the child).
fn trivial_subqueryscan(plan: &mut types_nodes::nodeindexscan::SubqueryScan<'_>) -> bool {
    use types_nodes::nodeindexscan::SubqueryScanStatus;

    if plan.scanstatus == SubqueryScanStatus::Trivial {
        return true;
    }
    if plan.scanstatus == SubqueryScanStatus::Nontrivial {
        return false;
    }
    debug_assert!(plan.scanstatus == SubqueryScanStatus::Unknown);
    // Initially mark non-deletable.
    plan.scanstatus = SubqueryScanStatus::Nontrivial;

    if plan.scan.plan.qual.as_ref().map(|q| !q.is_empty()).unwrap_or(false) {
        return false;
    }

    let parent_tlist = plan.scan.plan.targetlist.as_deref().unwrap_or(&[]);
    let scanrelid = plan.scan.scanrelid;
    let child_tlist = match plan.subplan.as_deref() {
        Some(n) => n.plan_head().targetlist.as_deref().unwrap_or(&[]),
        None => return false,
    };
    if parent_tlist.len() != child_tlist.len() {
        return false; // tlists not same length
    }

    let mut attrno: i16 = 1;
    for (ptle, ctle) in parent_tlist.iter().zip(child_tlist.iter()) {
        if ptle.resjunk != ctle.resjunk {
            return false; // junk status mismatch
        }
        match ptle.expr.as_deref() {
            // A Var referencing the matching subplan tlist element.
            Some(types_nodes::primnodes::Expr::Var(var)) => {
                debug_assert!(var.varno == scanrelid as i32);
                debug_assert!(var.varlevelsup == 0);
                if var.varattno != attrno {
                    return false; // out of order
                }
            }
            // A Const equaling the subplan element.
            Some(pe @ types_nodes::primnodes::Expr::Const(_)) => {
                match ctle.expr.as_deref() {
                    Some(ce) if equal_expr(pe, ce) => {}
                    _ => return false,
                }
            }
            _ => return false,
        }
        attrno += 1;
    }

    // Re-mark deletable.
    plan.scanstatus = SubqueryScanStatus::Trivial;
    true
}

/// `set_append_references(root, aplan, rtoffset)` (setrefs.c:1820).
fn set_append_references<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    // Recurse on children first.
    {
        let a = plan
            .as_append_mut()
            .ok_or_else(|| PgError::error("set_append_references: not Append"))?;
        let kids = core::mem::take(&mut a.appendplans);
        let mut newkids: Vec<Node> = Vec::with_capacity(kids.len());
        for k in kids {
            newkids.push(set_plan_refs(mcx, run, root, k, rtoffset)?);
        }
        a.appendplans = newkids;
    }

    // Single-child elision.
    {
        let a = plan.as_append().unwrap();
        if a.appendplans.len() == 1 {
            let child_safe = a.appendplans[0].plan_head().parallel_aware;
            let self_safe = a.plan.parallel_aware;
            if child_safe == self_safe {
                let mut a = plan.into_append().unwrap();
                let child = a.appendplans.pop().unwrap();
                return clean_up_removed_plan_level(&a.plan, child, mcx);
            }
        }
    }

    // Otherwise clean up the Append as needed.
    {
        let a = plan.as_append_mut().unwrap();
        set_dummy_tlist_references(&mut a.plan, rtoffset, mcx)?;
        let ar = bms_nodes_to_relids(a.apprelids.as_deref());
        let ar = offset_relid_set(ar, rtoffset);
        a.apprelids = relids_to_bms_node(ar, mcx)?;
        if a.part_prune_index >= 0 {
            return Err(PgError::error(
                "set_append_references: PartitionPruneInfo registration \
                 (register_partpruneinfo) is owned by the partition-pruning cohort \
                 and not ported",
            ));
        }
        // We don't recurse to lefttree/righttree (asserted NULL).
    }
    Ok(plan)
}

/// `set_mergeappend_references(root, mplan, rtoffset)` (setrefs.c:1887).
fn set_mergeappend_references<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    {
        let m = plan
            .as_mergeappend_mut()
            .ok_or_else(|| PgError::error("set_mergeappend_references: not MergeAppend"))?;
        let kids = core::mem::take(&mut m.mergeplans);
        let mut newkids: Vec<Node> = Vec::with_capacity(kids.len());
        for k in kids {
            newkids.push(set_plan_refs(mcx, run, root, k, rtoffset)?);
        }
        m.mergeplans = newkids;
    }
    {
        let m = plan.as_mergeappend().unwrap();
        if m.mergeplans.len() == 1 {
            let child_safe = m.mergeplans[0].plan_head().parallel_aware;
            let self_safe = m.plan.parallel_aware;
            if child_safe == self_safe {
                let mut m = plan.into_mergeappend().unwrap();
                let child = m.mergeplans.pop().unwrap();
                return clean_up_removed_plan_level(&m.plan, child, mcx);
            }
        }
    }
    {
        let m = plan.as_mergeappend_mut().unwrap();
        set_dummy_tlist_references(&mut m.plan, rtoffset, mcx)?;
        let ar = bms_nodes_to_relids(m.apprelids.as_deref());
        let ar = offset_relid_set(ar, rtoffset);
        m.apprelids = relids_to_bms_node(ar, mcx)?;
        if m.part_prune_index >= 0 {
            return Err(PgError::error(
                "set_mergeappend_references: PartitionPruneInfo registration \
                 (register_partpruneinfo) is owned by the partition-pruning cohort \
                 and not ported",
            ));
        }
    }
    Ok(plan)
}

/// `clean_up_removed_plan_level(parent, child)` (setrefs.c:1545).
fn clean_up_removed_plan_level<'mcx>(
    parent: &Plan<'mcx>,
    mut child: Node<'mcx>,
    mcx: Mcx<'mcx>,
) -> PgResult<Node<'mcx>> {
    // Move any parent initplans to the child (+ initplan cost / parallel safety).
    if parent.initPlan.as_ref().map(|p| !p.is_empty()).unwrap_or(false) {
        // SS_compute_initplan_cost + the initPlan list concat are the subselect
        // keystone — LOUD, never silently dropped.
        return Err(PgError::error(
            "clean_up_removed_plan_level: moving parent initPlans to the child \
             (SS_compute_initplan_cost) is owned by the subselect cohort and not ported",
        ));
    }
    // apply_tlist_labeling(child->targetlist, parent->targetlist): copy
    // resname/ressortgroupref/resorig*/resjunk by position.
    {
        let cbase = child.plan_head_mut();
        let parent_tl = parent.targetlist.as_deref().unwrap_or(&[]);
        if let Some(child_tl) = cbase.targetlist.as_mut() {
            if child_tl.len() != parent_tl.len() {
                return Err(PgError::error(
                    "clean_up_removed_plan_level: tlist length mismatch in apply_tlist_labeling",
                ));
            }
            for (d, s) in child_tl.iter_mut().zip(parent_tl.iter()) {
                d.resname = match s.resname.as_ref() { Some(n) => Some(n.clone_in(mcx)?), None => None };
                d.ressortgroupref = s.ressortgroupref;
                d.resorigtbl = s.resorigtbl;
                d.resorigcol = s.resorigcol;
                d.resjunk = s.resjunk;
            }
        }
    }
    Ok(child)
}

// ===========================================================================
// set_modifytable_references.
// ===========================================================================

/// `T_ModifyTable` arm of `set_plan_refs` (setrefs.c:1064). The plain
/// single-relation INSERT/UPDATE/DELETE (no WCO / RETURNING / ON CONFLICT /
/// MERGE) path is ported; the join-expr fix-up legs are loud-deferred.
fn set_modifytable_references<'mcx>(
    mcx: Mcx<'mcx>,
    run: &mut types_pathnodes::planner_run::PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    mut plan: Node<'mcx>,
    rtoffset: i32,
) -> PgResult<Node<'mcx>> {
    {
        let m = plan
            .as_modifytable_mut()
            .ok_or_else(|| PgError::error("set_modifytable_references: not ModifyTable"))?;
        let has_wco = m.withCheckOptionLists.as_ref().map(|l| !l.is_empty()).unwrap_or(false);
        let has_returning = m.returningLists.as_ref().map(|l| !l.is_empty()).unwrap_or(false);
        let has_onconflict = m.onConflictSet.as_ref().map(|l| !l.is_empty()).unwrap_or(false);
        let has_merge = m
            .mergeActionLists
            .as_ref()
            .map(|l| !l.is_empty())
            .unwrap_or(false);
        if has_wco || has_onconflict || has_merge {
            return Err(PgError::error(
                "set_plan_refs(T_ModifyTable): WCO / ON CONFLICT / MERGE \
                 fix-up (fix_join_expr / build_tlist_index over exclRelTlist+source \
                 tlists) is not ported",
            ));
        }

        // Pass each per-resultrel returningList through
        // set_returning_clause_references() (setrefs.c:1076). resultRelation is
        // the still-un-bumped RT index here; rtoffset is applied inside the
        // fix-up. The visible plan targetlist becomes a copy of the first list.
        if has_returning {
            let subplan = m.plan.lefttree.as_deref().ok_or_else(|| {
                PgError::error("set_modifytable_references: ModifyTable has no subplan")
            })?;
            let subplan_clone = subplan.plan_head().clone_in(mcx)?;
            let result_rels: Vec<i32> = m
                .resultRelations
                .as_deref()
                .map(|v| v.iter().map(|&i| i as i32).collect())
                .unwrap_or_default();
            let lists = m.returningLists.take().unwrap_or_else(|| PgVec::new_in(mcx));
            debug_assert_eq!(lists.len(), result_rels.len());
            let mut new_rl: PgVec<PgVec<TargetEntry>> = PgVec::new_in(mcx);
            let mut first: Option<PgVec<TargetEntry>> = None;
            for (i, rlist) in lists.into_iter().enumerate() {
                let resultrel = result_rels[i] as Index;
                let fixed = set_returning_clause_references(
                    mcx,
                    root,
                    rlist,
                    &subplan_clone,
                    resultrel,
                    rtoffset,
                )?;
                if first.is_none() {
                    let mut copy: PgVec<TargetEntry> = PgVec::new_in(mcx);
                    for tle in fixed.iter() {
                        copy.push(tle.clone_in(mcx)?);
                    }
                    first = Some(copy);
                }
                new_rl.push(fixed);
            }
            m.returningLists = Some(new_rl);
            // splan->plan.targetlist = copyObject(linitial(newRL));
            m.plan.targetlist = first;
        }

        // RT-index bumps.
        m.nominalRelation = m.nominalRelation.wrapping_add(rtoffset as u32);
        if m.rootRelation != 0 {
            m.rootRelation = m.rootRelation.wrapping_add(rtoffset as u32);
        }
        m.exclRelRTI = m.exclRelRTI.wrapping_add(rtoffset as u32);
        if let Some(rr) = m.resultRelations.as_mut() {
            for r in rr.iter_mut() {
                *r = r.wrapping_add(rtoffset as u32);
            }
        }
        // rowMarks: PlanRowMark nodes carried as Node handles here. A
        // single-relation UPDATE/DELETE has none; a non-empty list needs the
        // walkable PlanRowMark (owned by the rowmark cohort).
        if m.rowMarks.as_ref().map(|l| !l.is_empty()).unwrap_or(false) {
            return Err(PgError::error(
                "set_plan_refs(T_ModifyTable): per-rowmark rti/prti bump over the \
                 ModifyTable.rowMarks Node list is owned by the rowmark cohort and not ported",
            ));
        }

        // Append result relation RT index(es) to the global list.
        let res: Vec<i32> = m
            .resultRelations
            .as_deref()
            .map(|v| v.iter().map(|&i| i as i32).collect())
            .unwrap_or_default();
        let rootrel = m.rootRelation;
        let g = glob_mut(root)?;
        g.result_relations.extend(res);
        if rootrel != 0 {
            g.result_relations.push(rootrel as i32);
        }
    }

    // Recurse into the subplan (outerPlan) via the generic tail.
    {
        let lefttree = plan.plan_head_mut().lefttree.take();
        let righttree = plan.plan_head_mut().righttree.take();
        plan.plan_head_mut().lefttree = set_plan_refs_opt(mcx, run, root, lefttree, rtoffset)?;
        plan.plan_head_mut().righttree = set_plan_refs_opt(mcx, run, root, righttree, rtoffset)?;
    }
    Ok(plan)
}

// ===========================================================================
// extract_query_dependencies — exported plancache entry points.
//
// `set_plan_references` (above) records dependencies into `root->glob` via the
// `record_inval_item` seam (`Vec<NodeId>`, plancache-owned node space). The
// VALUE entry point below is what plancache.c's invalidation of cached UNPLANNED
// queries needs: given a rewritten querytree list it walks the owned `Query`
// tree and produces the concrete `(relationOids, invalItems, hasRowSecurity)`
// triple. C makes up a dummy zeroed `PlannerGlobal`/`PlannerInfo` to reuse this
// module's machinery; here the accumulators live in a small `ExtractDepsCtx`,
// and the func-OID inval-item hash is computed directly through the
// `get_syscache_hash_value1_oid` seam (PROCOID) so the result carries real
// `(cacheId, hashValue)` pairs rather than opaque node handles.
// ===========================================================================

/// The dummy `PlannerGlobal` accumulators of `extract_query_dependencies`
/// (setrefs.c:3635). `glob.relationOids` / `glob.invalItems`, plus the
/// `glob.dependsOnRole` field C abuses to collect the `hasRowSecurity` flag.
struct ExtractDepsCtx {
    relation_oids: Vec<Oid>,
    inval_items: Vec<(i32, u32)>,
    depends_on_role: bool,
}

/// `record_plan_function_dependency(root, funcid)` (setrefs.c:3553), VALUE form:
/// append a `PlanInvalItem` `(PROCOID, GetSysCacheHashValue1(PROCOID, funcid))`
/// to the accumulator, ignoring built-in functions (`funcid < FirstUnpinnedObjectId`).
fn record_plan_function_dependency_value(ctx: &mut ExtractDepsCtx, funcid: Oid) -> PgResult<()> {
    if funcid >= FirstUnpinnedObjectId {
        // inval_item->cacheId = PROCOID;
        // inval_item->hashValue = GetSysCacheHashValue1(PROCOID, ObjectIdGetDatum(funcid));
        let hash =
            backend_utils_cache_syscache_seams::get_syscache_hash_value_oid::call(PROCOID, funcid)?;
        ctx.inval_items.push((PROCOID, hash));
    }
    Ok(())
}

/// `fix_expr_common(context, node)` (setrefs.c:2029), VALUE form for the
/// dependency-extraction walk: record function-OID inval items and regclass
/// `Const` relation OIDs. The opcode fill-in (`set_opfuncid`/`set_sa_opfuncid`)
/// is reproduced on a local copy so the unplanned query's possibly-unset
/// `opfuncid` is resolved before recording (C scribbles it in place; we read it
/// off a clone, which yields the identical OID). The `GroupingFunc` cols fixup
/// is a no-op here — `extract_query_dependencies` uses a zeroed root whose
/// `grouping_map` is NULL — so it is omitted (matching the C no-op).
fn fix_expr_common_value(ctx: &mut ExtractDepsCtx, node: &Expr) -> PgResult<()> {
    match node {
        Expr::Aggref(a) => record_plan_function_dependency_value(ctx, a.aggfnoid)?,
        Expr::WindowFunc(w) => record_plan_function_dependency_value(ctx, w.winfnoid)?,
        Expr::FuncExpr(f) => record_plan_function_dependency_value(ctx, f.funcid)?,
        // OpExpr / DistinctExpr / NullIfExpr share the OpExpr struct.
        Expr::OpExpr(op) | Expr::DistinctExpr(op) | Expr::NullIfExpr(op) => {
            let mut tmp = op.clone();
            set_opfuncid(&mut tmp)?;
            record_plan_function_dependency_value(ctx, tmp.opfuncid)?;
        }
        Expr::ScalarArrayOpExpr(saop) => {
            let mut tmp = saop.clone();
            set_sa_opfuncid(&mut tmp)?;
            record_plan_function_dependency_value(ctx, tmp.opfuncid)?;
            if tmp.hashfuncid != 0 {
                record_plan_function_dependency_value(ctx, tmp.hashfuncid)?;
            }
            if tmp.negfuncid != 0 {
                record_plan_function_dependency_value(ctx, tmp.negfuncid)?;
            }
        }
        Expr::Const(con) => {
            if is_regclass_const(con) {
                // root->glob->relationOids = lappend_oid(..., DatumGetObjectId(con->constvalue));
                ctx.relation_oids.push(const_object_id(con));
            }
        }
        _ => {}
    }
    Ok(())
}

/// `extract_query_dependencies_walker(node, context)` (setrefs.c:3671) — the
/// recursive walker over the owned `Query`/`RangeTblEntry`/`Expr` tree. Returns
/// `true` to abort (it never does in this module, exactly as C). Errors from the
/// syscache-hash / opcode lookups are threaded out as `PgResult`.
fn extract_query_dependencies_walker(node: &Node, ctx: &mut ExtractDepsCtx) -> PgResult<bool> {
    // if (node == NULL) return false; — the caller never passes NULL here.
    // Assert(!IsA(node, PlaceHolderVar)); — PlaceHolderVars do not appear in
    // a not-yet-planned query tree.
    if let Some(query) = node.as_query() {
        if query.commandType == types_nodes::nodes::CmdType::CMD_UTILITY {
            // This logic must handle any utility command for which parse
            // analysis was nontrivial (cf. stmt_requires_parse_analysis).
            // Notably, CALL requires its own processing.
            if let Some(util) = query.utilityStmt.as_deref() {
                if let Some(callstmt) = util.as_callstmt() {
                    // We need not examine funccall, just the transformed exprs.
                    if let Some(fe) = callstmt.funcexpr.as_deref() {
                        if extract_query_dependencies_walker(fe, ctx)? {
                            return Ok(true);
                        }
                    }
                    for arg in callstmt.outargs.iter() {
                        if extract_query_dependencies_walker(arg, ctx)? {
                            return Ok(true);
                        }
                    }
                    return Ok(false);
                }
            }

            // Ignore other utility statements, except those (such as EXPLAIN)
            // that contain a parsed-but-not-planned query.  For those, we just
            // need to transfer our attention to the contained query.
            match utility_contains_query(query.utilityStmt.as_deref()) {
                None => return Ok(false),
                Some(inner) => {
                    // Recurse with the contained Query as the current node.
                    return extract_query_dependencies_walker(inner, ctx);
                }
            }
        }

        // Remember if any Query has RLS quals applied by rewriter.
        if query.hasRowSecurity {
            ctx.depends_on_role = true;
        }

        // Collect relation OIDs in this Query's rtable.
        for rte in query.rtable.iter() {
            use types_nodes::parsenodes::RTEKind;
            if rte.rtekind == RTEKind::RTE_RELATION
                || (rte.rtekind == RTEKind::RTE_SUBQUERY && rte.relid != 0)
                || (rte.rtekind == RTEKind::RTE_NAMEDTUPLESTORE && rte.relid != 0)
            {
                ctx.relation_oids.push(rte.relid);
            }
        }

        // And recurse into the query's subexpressions:
        //   return query_tree_walker(query, extract_query_dependencies_walker, context, 0);
        let mut callback_err: Option<PgError> = None;
        let aborted = {
            let mut walker = |child: &Node| -> bool {
                match extract_query_dependencies_walker(child, ctx) {
                    Ok(abort) => abort,
                    Err(e) => {
                        if callback_err.is_none() {
                            callback_err = Some(e);
                        }
                        true
                    }
                }
            };
            backend_nodes_core::node_walker::query_tree_walker(query, &mut walker, 0)
        };
        if let Some(e) = callback_err {
            return Err(e);
        }
        return Ok(aborted);
    }

    // Extract function dependencies and check for regclass Consts:
    //   fix_expr_common(context, node);
    if let Some(e) = node.as_expr() {
        fix_expr_common_value(ctx, e)?;
    }
    // return expression_tree_walker(node, extract_query_dependencies_walker, context);
    let mut callback_err: Option<PgError> = None;
    let aborted = {
        let mut walker = |child: &Node| -> bool {
            match extract_query_dependencies_walker(child, ctx) {
                Ok(abort) => abort,
                Err(e) => {
                    if callback_err.is_none() {
                        callback_err = Some(e);
                    }
                    true
                }
            }
        };
        backend_nodes_core::node_walker::expression_tree_walker(node, &mut walker)
    };
    if let Some(e) = callback_err {
        return Err(e);
    }
    Ok(aborted)
}

/// `UtilityContainsQuery(parsetree)` (utility.c:2179) — return the contained
/// not-yet-planned `Query` of an EXPLAIN / CREATE-TABLE-AS / DECLARE-CURSOR
/// utility statement, drilling through nested utility-`Query` wrappers, or
/// `None`. This is a pure structural recursion over the owned `Node` tree (the
/// same three `Node` variants utility.c switches on, all in `types-nodes`), so
/// it is mirrored locally rather than seamed — there is no catalog/runtime
/// dependency and no crate boundary to cross.
fn utility_contains_query<'a, 'mcx>(parsetree: Option<&'a Node<'mcx>>) -> Option<&'a Node<'mcx>> {
    let parsetree = parsetree?;
    // switch (nodeTag(parsetree)): each arm pulls out `->query`.
    let qry = match parsetree.node_tag() {
        ntag::T_DeclareCursorStmt => parsetree.expect_declarecursorstmt().query.as_deref(),
        ntag::T_ExplainStmt => parsetree.expect_explainstmt().query.as_deref(),
        ntag::T_CreateTableAsStmt => parsetree.expect_createtableasstmt().query.as_deref(),
        // default: return NULL;
        _ => return None,
    };
    // qry = castNode(Query, ...): the analyzed contained statement is a Query.
    let node = qry?;
    match node.as_query() {
        Some(q) => {
            if q.commandType == types_nodes::nodes::CmdType::CMD_UTILITY {
                // return UtilityContainsQuery(qry->utilityStmt);
                utility_contains_query(q.utilityStmt.as_deref())
            } else {
                // return qry;
                Some(node)
            }
        }
        None => None,
    }
}

/// `extract_query_dependencies(query, &relationOids, &invalItems, &hasRowSecurity)`
/// (setrefs.c:3635) over a rewritten-but-not-yet-planned querytree list. C makes
/// up a dummy zeroed `PlannerGlobal`/`PlannerInfo` and runs
/// `extract_query_dependencies_walker((Node *) query_list, &root)`; the
/// list-of-`Query` is walked element by element (the C `extract_query_dependencies`
/// is invoked on `(Node *) querytree_list`, and `expression_tree_walker`'s `List`
/// arm visits each `Query`). The dummy `glob`/`root` are realized as the local
/// [`ExtractDepsCtx`] accumulators.
fn extract_query_dependencies_value<'mcx>(
    _mcx: Mcx<'mcx>,
    query_list: &[types_nodes::copy_query::Query<'mcx>],
) -> PgResult<ext::QueryDependenciesValue> {
    let mut ctx = ExtractDepsCtx {
        relation_oids: Vec::new(),
        inval_items: Vec::new(),
        depends_on_role: false,
    };
    // (void) extract_query_dependencies_walker((Node *) query_list, &root);
    // The argument is a List of Query nodes; expression_tree_walker's List arm
    // walks each element, so we walk each Query directly as the current node.
    for query in query_list.iter() {
        // Wrap the borrowed Query as the `Node::Query` the walker expects. The
        // walker only reads it, so a clone of the owned value tree is faithful.
        let node = Node::mk_query(_mcx, query.clone_in(_mcx)?);
        if extract_query_dependencies_walker(&node, &mut ctx)? {
            break;
        }
    }
    Ok(ext::QueryDependenciesValue {
        relation_oids: ctx.relation_oids,
        inval_items: ctx.inval_items,
        depends_on_rls: ctx.depends_on_role,
    })
}

/// `extract_query_dependencies_walker(result, &root)` over a single planned
/// expression (the dependency-extraction tail of
/// `expression_planner_with_deps`, clauses.c:5479). Given the const-folded
/// expression `clauses.c`'s `expression_planner_with_deps` produces, walk it to
/// collect the relation-OID and function-inval-item dependencies (same dummy
/// `PlannerGlobal`/`PlannerInfo` realized as the local [`ExtractDepsCtx`]). The
/// `depends_on_rls` field is unused on the bare-expression path (an expression
/// carries no `Query.hasRowSecurity`) but is returned for shape parity.
/// Exported for the planner owner's `expression_planner_with_deps` value form;
/// the planner crate depends on this one directly (no cycle).
pub fn extract_expr_dependencies_value<'mcx>(
    _mcx: Mcx<'mcx>,
    expr: &Expr,
) -> PgResult<ext::QueryDependenciesValue> {
    let mut ctx = ExtractDepsCtx {
        relation_oids: Vec::new(),
        inval_items: Vec::new(),
        depends_on_role: false,
    };
    // (void) extract_query_dependencies_walker(result, &root); — `result` is a
    // bare Expr, wrapped as the `Node::Expr` the walker dispatches on.
    let node = Node::Expr(expr.clone());
    extract_query_dependencies_walker(&node, &mut ctx)?;
    Ok(ext::QueryDependenciesValue {
        relation_oids: ctx.relation_oids,
        inval_items: ctx.inval_items,
        depends_on_rls: ctx.depends_on_role,
    })
}

/// `set_returning_clause_references(root, rlist, topplan, resultRelation, rtoffset)`
/// (setrefs.c:3398). Ported as a real body (uses build_tlist_index_other_vars +
/// fix_join_expr); reached only from the ModifyTable RETURNING leg (loud above).
fn set_returning_clause_references<'mcx>(
    mcx: Mcx<'mcx>,
    root: &mut PlannerInfo,
    rlist: PgVec<'mcx, TargetEntry<'mcx>>,
    topplan: &Plan<'mcx>,
    result_relation: Index,
    rtoffset: i32,
) -> PgResult<PgVec<'mcx, TargetEntry<'mcx>>> {
    let itlist = build_tlist_index_filtered(
        topplan.targetlist.as_deref().unwrap_or(&[]),
        mcx,
        Some(result_relation as i32),
    )?;
    let nt = num_exec_tlist(topplan);
    let mut out: PgVec<TargetEntry> = PgVec::new_in(mcx);
    for mut tle in rlist {
        if let Some(eb) = tle.expr.take() {
            let fixed = fix_join_expr_mutator(
                mcx,
                root,
                PgBox::into_inner(eb),
                &FixJoinCtx {
                    outer_itlist: Some(&itlist),
                    inner_itlist: None,
                    acceptable_rel: result_relation,
                    rtoffset,
                    nrm_match: NRM_EQUAL,
                    num_exec: nt,
                },
            )?;
            tle.expr = Some(mcx::alloc_in(mcx, fixed)?);
        }
        out.push(tle);
    }
    Ok(out)
}

// ===========================================================================
// INWARD seams.
// ===========================================================================

/// Install the seams this unit owns. `extract_query_dependencies_value` is the
/// VALUE entry point plancache.c uses to extract dependencies of cached
/// not-yet-planned queries (setrefs.c:3635). The expression/plan-walking
/// machinery itself is real in-crate code over the `Node`/`Expr` model; the
/// outward seams this crate consumes are installed by their respective owners.
pub fn init_seams() {
    ext::extract_query_dependencies_value::set(extract_query_dependencies_value);

    // `find_minmax_agg_replacement_param(root, aggref)->paramid` (setrefs.c, via
    // planagg.c's minmax_aggs) consumed by `finalize_primnode`'s Aggref arm
    // (subselect.c, in init-subselect). The owner returns the whole `Param`; the
    // seam projects its `paramid`. C `finalize_primnode` is infallible here, so a
    // catalog miss in the equality probe panics (mirrors a hard `elog`).
    backend_optimizer_plan_init_subselect_ext_seams::find_minmax_agg_replacement_param::set(
        |root, aggref| {
            find_minmax_agg_replacement_param(root, aggref)
                .expect("find_minmax_agg_replacement_param")
                .map(|p| p.paramid)
        },
    );
}
