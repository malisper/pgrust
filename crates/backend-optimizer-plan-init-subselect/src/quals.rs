//! QUALIFICATIONS (initsplan.c) — `distribute_qual_to_rels`,
//! `check_redundant_nullability_qual`, `add_base_clause_to_rel`,
//! `expr_is_nonnullable`, `restriction_is_always_true`,
//! `restriction_is_always_false`, `distribute_restrictinfo_to_rels`,
//! `process_implied_equality`, `build_implied_join_equality`,
//! `get_join_domain_min_rels`.
//!
//! # Model reconciliation (read before editing)
//!
//! C passes the bare parse-tree `Node *clause` plus a `JoinTreeItem *jtitem`
//! into `distribute_qual_to_rels`. This repo carries qual clauses as owned,
//! lifetime-free [`Expr`] conjuncts (see `jointree.rs`), and the transient
//! `JoinTreeItem` arena is the `item_list: &mut Vec<JoinTreeItem>` indexed by a
//! [`JtId`] = `usize` (lib.rs). So every entry point that reads the parse tree
//! takes `run: &PlannerRun<'_>` alongside `&mut PlannerInfo`, matching the
//! LOCKED convention used throughout `jointree.rs`.
//!
//! `distribute_qual_to_rels` returns `()` (it is driven from `jointree.rs`'s
//! `distribute_quals_to_rels` loop, which ignores any result, mirroring C's
//! void return); inner seam calls that return [`PgResult`] are `.expect()`ed
//! where C `elog(ERROR)`s. `process_implied_equality`/
//! `build_implied_join_equality`/`distribute_restrictinfo_to_rels` are reached
//! from the EquivalenceClass machinery and the installed eqext seams declare
//! them returning [`PgResult`], so they `?`-propagate.
//!
//! `RestrictInfo.clause`/`orclause` are arena handles ([`NodeId`]); resolve via
//! `root.node(id)`. The `orclause` is a `BoolExpr` whose `args` are
//! `Expr::RestrictInfo(RinfoRef)`; recursion over an OR resolves each child via
//! `RinfoId::from(rinforef)` and `root.rinfo(id)` and calls the borrowed-
//! `RestrictInfo` helper.

extern crate alloc;

use alloc::vec::Vec;

use types_core::primitive::{Index, Oid};
use types_error::PgResult;
use types_nodes::primnodes::{Const, Expr, NullTest, NullTestType, OR_EXPR};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    OuterJoinClauseInfo, PlannerInfo, Relids, RestrictInfo, RinfoId, SpecialJoinInfo, JOIN_ANTI,
    JOIN_FULL,
};

use backend_optimizer_util_relnode_seams as bms;
use backend_optimizer_path_equivclass_ext_seams as eqext;
use backend_optimizer_plan_init_subselect_ext_seams as initext;

use backend_nodes_core::makefuncs::make_opclause;

use crate::{JoinTreeItem, JtId, BMS_MULTIPLE, BMS_SINGLETON, BOOLOID};

/// `RELKIND_PARTITIONED_TABLE` (pg_class.h) — `'p'`.
const RELKIND_PARTITIONED_TABLE: i8 = b'p' as i8;
/// `InvalidOid`.
const INVALID_OID: Oid = 0;

/// `pull_var_clause` flag set used for join-clause var collection:
/// `PVC_RECURSE_AGGREGATES | PVC_RECURSE_WINDOWFUNCS | PVC_INCLUDE_PLACEHOLDERS`.
const PVC_JOINCLAUSE_FLAGS: i32 = 0x0002 | 0x0008 | 0x0010;

/// Deep-copy a qual `Expr` value into the planner arena (C: `copyObject` /
/// pointer reuse). The derived `Expr::clone()` panics on owned-subtree variants
/// (SubLink/SubPlan/Aggref) whose children only deep-copy via `clone_in`; route
/// every qual copy through `Expr::clone_in`.
fn copy_clause_in(run: &PlannerRun<'_>, expr: &Expr) -> Expr {
    expr.clone_in(run.mcx())
        .unwrap_or_else(|e| panic!("copy_clause_in: clone_in: {e:?}"))
}

/// `distribute_qual_to_rels` (initsplan.c:2545).
///
/// Add clause information to the baserestrictinfo/joininfo lists of the rels
/// mentioned in the clause (building a [`RestrictInfo`]), or — if it's a
/// mergejoinable equality — feed it to the EquivalenceClass machinery. Quals
/// may instead be postponed (lateral references or non-degenerate outer-join
/// clauses with a `postponed_to` sink).
#[allow(clippy::too_many_arguments)]
pub fn distribute_qual_to_rels<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    clause: &Expr,
    item_list: &mut Vec<JoinTreeItem>,
    jti: JtId,
    sjinfo: Option<&SpecialJoinInfo>,
    security_level: u32,
    qualscope: &Relids,
    ojscope: &Relids,
    outerjoin_nonnullable: &Relids,
    incompatible_relids: &Relids,
    allow_equivalence: bool,
    has_clone: bool,
    is_clone: bool,
    postponed_to: Option<JtId>,
) {
    let mut pseudoconstant = false;

    // Retrieve all relids mentioned within the clause.
    let mut relids = eqext::pull_varnos::call(root, clause);

    // In ordinary SQL a WHERE/JOIN-ON clause can't reference rels outside its
    // syntactic scope; but a pulled-up LATERAL subquery can. When the clause
    // contains Vars outside qualscope, locate the nearest parent join level
    // that includes all required rels and postpone the clause to that level.
    if !bms::relids_is_subset::call(&relids, qualscope) {
        debug_assert!(root.hasLateralRTEs); // shouldn't happen otherwise
        debug_assert!(sjinfo.is_none()); // mustn't postpone past outer join
        let mut pitem = item_list[jti].jti_parent;
        while let Some(p) = pitem {
            if bms::relids_is_subset::call(&relids, &item_list[p].qualscope) {
                item_list[p].lateral_clauses.push(copy_clause_in(run, clause));
                return;
            }
            // We should not be postponing any quals past an outer join.
            debug_assert!(item_list[p].sjinfo.is_none());
            pitem = item_list[p].jti_parent;
        }
        panic!("failed to postpone qual containing lateral reference");
    }

    // If it's an outer-join clause, also check that relids is a subset of
    // ojscope. (This should not fail if the syntactic scope check passed.)
    if !bms::relids_is_empty::call(ojscope) && !bms::relids_is_subset::call(&relids, ojscope) {
        panic!("JOIN qualification cannot refer to other relations");
    }

    // If the clause is variable-free, our normal heuristic for pushing it down
    // to just the mentioned rels doesn't work, because there are none.
    if bms::relids_is_empty::call(&relids) {
        if !bms::relids_is_empty::call(ojscope) {
            // clause is attached to outer join, eval it there
            relids = bms::relids_copy::call(ojscope);
            // mustn't use as gating qual, so don't mark pseudoconstant
        } else if eqext::contain_volatile_functions::call(clause) {
            // eval at original syntactic level
            relids = bms::relids_copy::call(qualscope);
            // again, can't mark pseudoconstant
        } else {
            // If we are in the top-level join domain, push the qual to the top
            // of the plan tree. Otherwise, be conservative and eval it at the
            // original syntactic level.
            let jdomain = item_list[jti].jdomain;
            if jdomain == 0 {
                relids = bms::relids_copy::call(&root.join_domains[jdomain].jd_relids);
            } else {
                relids = bms::relids_copy::call(qualscope);
            }
            // mark as gating qual
            pseudoconstant = true;
            // tell createplan.c to check for gating quals
            root.hasPseudoConstantQuals = true;
        }
    }

    // Check whether clause application must be delayed by outer-join
    // considerations. (See the long C comment for the is_pushed_down rules.)
    let is_pushed_down;
    let maybe_equivalence;
    let maybe_outer_join;
    if bms::relids_overlap::call(&relids, outerjoin_nonnullable) {
        // The qual is attached to an outer join and mentions (some of the) rels
        // on the nonnullable side, so it's not degenerate. If the caller wants
        // to postpone handling such clauses, add it to the postponed list.
        if let Some(target) = postponed_to {
            item_list[target].oj_joinclauses.push(copy_clause_in(run, clause));
            return;
        }
        is_pushed_down = false;
        maybe_equivalence = false;
        maybe_outer_join = true;
        // Force the qual to be evaluated exactly at the outer-join level.
        debug_assert!(!bms::relids_is_empty::call(ojscope));
        relids = bms::relids_copy::call(ojscope);
        debug_assert!(!pseudoconstant);
    } else {
        // Normal qual clause or degenerate outer-join clause. Either way, mark
        // it as pushed-down.
        is_pushed_down = true;
        // It's possible that this is an IS NULL clause that's redundant with a
        // lower antijoin; if so we can just discard it.
        if check_redundant_nullability_qual(root, clause) {
            return;
        }
        // Feed qual to the equivalence machinery, if allowed by caller.
        maybe_equivalence = allow_equivalence;
        // Since it doesn't mention the LHS, it's certainly not useful as a
        // set-aside OJ clause, even if it's in an OJ.
        maybe_outer_join = false;
    }

    // Build the RestrictInfo node itself.
    let mut restrictinfo = eqext::make_restrictinfo::call(
        run.mcx(),
        root,
        copy_clause_in(run, clause),
        is_pushed_down,
        has_clone,
        is_clone,
        pseudoconstant,
        security_level,
        bms::relids_copy::call(&relids),
        bms::relids_copy::call(incompatible_relids),
        bms::relids_copy::call(outerjoin_nonnullable),
    )
    .expect("make_restrictinfo");

    // If it's a join clause, add vars used in the clause to targetlists of
    // their relations, so they will be emitted by the scan nodes (else they
    // won't be available at the join node!).
    if bms::relids_membership::call(&relids) == BMS_MULTIPLE {
        let vars = eqext::pull_var_clause::call(clause, PVC_JOINCLAUSE_FLAGS);
        let where_needed = if is_clone {
            bms::relids_intersect::call(&relids, &root.all_baserels)
        } else {
            bms::relids_copy::call(&relids)
        };
        crate::targetlist::add_vars_to_targetlist(root, vars, where_needed)
            .expect("add_vars_to_targetlist");
    }

    // We check mergejoinability of every clause, not only join clauses, because
    // we want to know about equivalences between vars of the same relation, or
    // between vars and consts.
    crate::mergehash::check_mergejoinable(root, restrictinfo).expect("check_mergejoinable");

    // If it is a true equivalence clause, send it to the EC machinery. We do
    // *not* attach it directly to any restriction or join lists.
    let has_mergeopfamilies = !root.rinfo(restrictinfo).mergeopfamilies.is_empty();
    if has_mergeopfamilies {
        if maybe_equivalence {
            let jdomain = item_list[jti].jdomain;
            let jdomain_relids = bms::relids_copy::call(&root.join_domains[jdomain].jd_relids);
            let (kept, ri) = eqext_process_equivalence(root, run, restrictinfo, jdomain_relids);
            if kept {
                return;
            }
            // EC may have replaced the RestrictInfo node (the `newri` case);
            // continue with the possibly-modified handle.
            restrictinfo = ri;
            // EC rejected it, so set left_ec/right_ec the hard way ...
            if !root.rinfo(restrictinfo).mergeopfamilies.is_empty() {
                // EC might have changed this
                backend_optimizer_path_pathkeys::initialize_mergeclause_eclasses(root, restrictinfo);
            }
            // ... and fall through to distribute_restrictinfo_to_rels.
        } else if maybe_outer_join && root.rinfo(restrictinfo).can_join {
            // we need to set up left_ec/right_ec the hard way
            backend_optimizer_path_pathkeys::initialize_mergeclause_eclasses(root, restrictinfo);
            // now see if it should go to any outer-join lists
            let sj = sjinfo.expect("maybe_outer_join implies sjinfo present");
            let left_relids = bms::relids_copy::call(&root.rinfo(restrictinfo).left_relids);
            let right_relids = bms::relids_copy::call(&root.rinfo(restrictinfo).right_relids);
            if bms::relids_is_subset::call(&left_relids, outerjoin_nonnullable)
                && !bms::relids_overlap::call(&right_relids, outerjoin_nonnullable)
            {
                // we have outervar = innervar
                push_oj_clause_info(root, restrictinfo, sj, OjList::Left);
                return;
            }
            if bms::relids_is_subset::call(&right_relids, outerjoin_nonnullable)
                && !bms::relids_overlap::call(&left_relids, outerjoin_nonnullable)
            {
                // we have innervar = outervar
                push_oj_clause_info(root, restrictinfo, sj, OjList::Right);
                return;
            }
            if sj.jointype == JOIN_FULL {
                // FULL JOIN (above tests cannot match in this case)
                push_oj_clause_info(root, restrictinfo, sj, OjList::Full);
                return;
            }
            // nope, so fall through to distribute_restrictinfo_to_rels
        } else {
            // we still need to set up left_ec/right_ec
            backend_optimizer_path_pathkeys::initialize_mergeclause_eclasses(root, restrictinfo);
        }
    }

    // No EC special case applies, so push it into the clause lists.
    distribute_restrictinfo_to_rels(run, root, restrictinfo)
        .expect("distribute_restrictinfo_to_rels");
}

/// `process_equivalence(root, &restrictinfo, jdomain)` (equivclass.c) via the
/// installed eqext seam. The C contract takes `RestrictInfo **` and may replace
/// the node; the seam returns `(kept_in_ec, possibly-modified rinfo)`. The
/// possibly-modified handle is irrelevant to the caller here (the arena entry is
/// mutated in place), so only the `kept` flag is consumed.
fn eqext_process_equivalence<'mcx>(
    root: &mut PlannerInfo,
    run: &PlannerRun<'mcx>,
    restrictinfo: RinfoId,
    jdomain: Relids,
) -> (bool, RinfoId) {
    backend_optimizer_path_equivclass_seams::process_equivalence::call(
        root, run, restrictinfo, jdomain,
    )
    .expect("process_equivalence")
}

/// Which set-aside outer-join clause list `push_oj_clause_info` appends to.
enum OjList {
    Left,
    Right,
    Full,
}

/// `lappend(root->{left,right,full}_join_clauses, ojcinfo)` — record a set-aside
/// outer-join clause (a mergejoinable clause not absorbed as an EC).
fn push_oj_clause_info(
    root: &mut PlannerInfo,
    rinfo: RinfoId,
    sjinfo: &SpecialJoinInfo,
    which: OjList,
) {
    let ojcinfo = OuterJoinClauseInfo {
        rinfo,
        sjinfo: sjinfo.clone(),
    };
    match which {
        OjList::Left => root.left_join_clauses.push(ojcinfo),
        OjList::Right => root.right_join_clauses.push(ojcinfo),
        OjList::Full => root.full_join_clauses.push(ojcinfo),
    }
}

/// `check_redundant_nullability_qual` (initsplan.c:2935).
///
/// Check whether `clause` is an IS NULL qual that is redundant with a lower
/// JOIN_ANTI join (in which case `distribute_qual_to_rels` throws it away).
fn check_redundant_nullability_qual(root: &PlannerInfo, clause: &Expr) -> bool {
    // Check for IS NULL, and identify the Var forced to NULL.
    let forced_null_var = match initext::find_forced_null_var_expr::call(clause) {
        None => return false,
        Some(v) => v,
    };
    let var = match forced_null_var.as_var() {
        Some(v) => v,
        None => return false,
    };

    // If the Var isn't nulled by anything, there's no point searching.
    if var.varnullingrels.words.is_empty() {
        return false;
    }

    for sjinfo in &root.join_info_list {
        // This test will not succeed if sjinfo->ojrelid is zero (possible for an
        // antijoin converted from a semijoin); but in that case the Var couldn't
        // have come from its nullable side.
        if sjinfo.jointype == JOIN_ANTI
            && sjinfo.ojrelid != 0
            && expr_relids_is_member(sjinfo.ojrelid as i32, &var.varnullingrels)
        {
            return true;
        }
    }
    false
}

/// `add_base_clause_to_rel` (initsplan.c:2980).
///
/// Add `restrictinfo` as a baserestrictinfo to the base relation `relid`,
/// applying the constant-TRUE / constant-FALSE pre-checks where allowed.
fn add_base_clause_to_rel<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    relid: i32,
    mut restrictinfo: RinfoId,
) -> PgResult<()> {
    let rel_id = bms::find_base_rel::call(root, relid);
    // rte = root->simple_rte_array[relid]; we only need its inh/relkind.
    let (_rtekind, rte_inh, rte_relkind) = initext::rte_kind_inh_relkind::call(run, root, relid);

    debug_assert!(
        bms::relids_membership::call(&root.rinfo(restrictinfo).required_relids) == BMS_SINGLETON
    );

    // For inheritance parent tables we must record the RestrictInfo as is (so
    // apply_child_basequals sees the original), except for partitioned tables,
    // which always get the constant-TRUE/FALSE transformations.
    if !rte_inh || rte_relkind == RELKIND_PARTITIONED_TABLE {
        // Don't add the clause if it is always true. (C calls
        // restriction_is_always_true(root, restrictinfo) — the RestrictInfo
        // form, which sees the has_clone/is_clone guard and the orclause.)
        if restriction_is_always_true_ri(root, root.rinfo(restrictinfo)) {
            return Ok(());
        }

        // Substitute the origin qual with constant-FALSE if it is provably
        // always false. We keep the same rinfo_serial (same condition in
        // practice) and reset the last_rinfo_serial counter.
        if restriction_is_always_false_ri(root, root.rinfo(restrictinfo)) {
            let save_rinfo_serial = root.rinfo(restrictinfo).rinfo_serial;
            let save_last_rinfo_serial = root.last_rinfo_serial;

            let is_pushed_down = root.rinfo(restrictinfo).is_pushed_down;
            let has_clone = root.rinfo(restrictinfo).has_clone;
            let is_clone = root.rinfo(restrictinfo).is_clone;
            let pseudoconstant = root.rinfo(restrictinfo).pseudoconstant;
            let required = bms::relids_copy::call(&root.rinfo(restrictinfo).required_relids);
            let incompat = bms::relids_copy::call(&root.rinfo(restrictinfo).incompatible_relids);
            let outer = bms::relids_copy::call(&root.rinfo(restrictinfo).outer_relids);

            let false_const = eqext::make_bool_const::call(false, false);
            restrictinfo = eqext::make_restrictinfo::call(
                run.mcx(),
                root,
                false_const,
                is_pushed_down,
                has_clone,
                is_clone,
                pseudoconstant,
                0, // security_level
                required,
                incompat,
                outer,
            )?;
            root.rinfo_mut(restrictinfo).rinfo_serial = save_rinfo_serial;
            root.last_rinfo_serial = save_last_rinfo_serial;
        }
    }

    // Add clause to rel's restriction list, and update security level info.
    let sec = root.rinfo(restrictinfo).security_level;
    let rel = root.rel_mut(rel_id);
    rel.baserestrictinfo.push(restrictinfo);
    rel.baserestrict_min_security = rel.baserestrict_min_security.min(sec);
    Ok(())
}

/// `expr_is_nonnullable` (initsplan.c:3055).
///
/// True if a simple Var that is defined NOT NULL and not nulled by any outer
/// joins; only simple Vars are checked.
fn expr_is_nonnullable(root: &PlannerInfo, expr: &Expr) -> bool {
    // For now only check simple Vars.
    let var = match expr.as_var() {
        Some(v) => v,
        None => return false,
    };

    // Could the Var be nulled by any outer joins?
    if !var.varnullingrels.words.is_empty() {
        return false;
    }

    // System columns cannot be NULL.
    if var.varattno < 0 {
        return true;
    }

    // Is the column defined NOT NULL?
    let rel_id = bms::find_base_rel::call(root, var.varno);
    if var.varattno > 0
        && bms::relids_is_member::call(var.varattno as i32, &root.rel(rel_id).notnullattnums)
    {
        return true;
    }

    false
}

/// `restriction_is_always_true` (initsplan.c:3091).
///
/// Public entry over a borrowed [`Expr`] (the EC/orclauses callers pass a
/// clause). Resolves to the borrowed-[`RestrictInfo`] body indirectly; here the
/// caller already has the clause [`Expr`] and there is no RestrictInfo wrapper,
/// so we replicate the NullTest arm directly and the OR arm via the embedded
/// RestrictInfo children.
pub fn restriction_is_always_true(root: &PlannerInfo, clause: &Expr) -> bool {
    // The borrowed-Expr entry models the clause directly (no has_clone/is_clone
    // flags available); the RestrictInfo-aware body lives in
    // `restriction_is_always_true_ri`, which is what add_base_clause_to_rel and
    // the OR recursion use. For a bare clause we evaluate the NullTest/OR arms
    // identically with the clone guard treated as "not a clone".
    restriction_is_always_true_clause(root, clause)
}

/// NullTest + OR body of [`restriction_is_always_true`] over a bare clause
/// [`Expr`] (no clone flags).
fn restriction_is_always_true_clause(root: &PlannerInfo, clause: &Expr) -> bool {
    // Check for NullTest qual.
    if let Expr::NullTest(nt) = clause {
        return nulltest_always_true(root, nt);
    }
    // If it's an OR, check its sub-clauses.
    if let Expr::BoolExpr(be) = clause {
        if be.boolop == OR_EXPR {
            for orarg in &be.args {
                if let Expr::RestrictInfo(rinforef) = orarg {
                    let subri = RinfoId::from(*rinforef);
                    if restriction_is_always_true_ri(root, root.rinfo(subri)) {
                        return true;
                    }
                }
                // non-RestrictInfo args are ignored (C `continue`)
            }
        }
    }
    false
}

/// [`restriction_is_always_true`] over a borrowed [`RestrictInfo`] (the
/// `RinfoId`-resolving form used by `add_base_clause_to_rel` and the OR
/// recursion). This is the faithful C body, including the clone guard.
fn restriction_is_always_true_ri(root: &PlannerInfo, ri: &RestrictInfo) -> bool {
    // For a clone clause we can't reliably determine non-nullability.
    if ri.has_clone || ri.is_clone {
        return false;
    }

    let clause = root.node(ri.clause);

    // Check for NullTest qual.
    if let Expr::NullTest(nt) = clause {
        return nulltest_always_true(root, nt);
    }

    // If it's an OR, check its sub-clauses: any always-true branch makes the
    // whole condition true.
    if restriction_is_or_clause(root, ri) {
        if let Some(orid) = ri.orclause {
            if let Expr::BoolExpr(be) = root.node(orid) {
                debug_assert!(be.boolop == OR_EXPR);
                for orarg in &be.args {
                    if let Expr::RestrictInfo(rinforef) = orarg {
                        let subri = RinfoId::from(*rinforef);
                        if restriction_is_always_true_ri(root, root.rinfo(subri)) {
                            return true;
                        }
                    }
                    // not a RestrictInfo → continue
                }
            }
        }
    }

    false
}

/// `restriction_is_always_true(root, restrictinfo)` (initsplan.c:3091) over a
/// `RinfoId` — the faithful C entry point used by `add_join_clause_to_rels` /
/// `add_base_clause_to_rel`, which pass the whole RestrictInfo (so the
/// `has_clone`/`is_clone` guard and the `orclause` OR-recursion both apply,
/// unlike the bare-clause [`restriction_is_always_true`] entry).
pub fn restriction_is_always_true_for(root: &PlannerInfo, ri: RinfoId) -> bool {
    restriction_is_always_true_ri(root, root.rinfo(ri))
}

/// `restriction_is_always_false(root, restrictinfo)` (initsplan.c:3156) over a
/// `RinfoId` (see [`restriction_is_always_true_for`]).
pub fn restriction_is_always_false_for(root: &PlannerInfo, ri: RinfoId) -> bool {
    restriction_is_always_false_ri(root, root.rinfo(ri))
}

/// The IS_NOT_NULL/argisrow arm shared by the bare-clause and RestrictInfo
/// `restriction_is_always_true` bodies.
fn nulltest_always_true(root: &PlannerInfo, nt: &NullTest) -> bool {
    // is this NullTest an IS_NOT_NULL qual?
    if nt.nulltesttype != NullTestType::IS_NOT_NULL {
        return false;
    }
    // Empty rows can appear NULL in some contexts and NOT NULL in others, so
    // avoid this optimization for row expressions.
    if nt.argisrow {
        return false;
    }
    match nt.arg.as_deref() {
        Some(arg) => expr_is_nonnullable(root, arg),
        None => false,
    }
}

/// `restriction_is_always_false` (initsplan.c:3156).
///
/// Public entry over a borrowed [`Expr`] (mirrors
/// [`restriction_is_always_true`]).
pub fn restriction_is_always_false(root: &PlannerInfo, clause: &Expr) -> bool {
    restriction_is_always_false_clause(root, clause)
}

/// NullTest + OR body of [`restriction_is_always_false`] over a bare clause.
fn restriction_is_always_false_clause(root: &PlannerInfo, clause: &Expr) -> bool {
    if let Expr::NullTest(nt) = clause {
        return nulltest_always_false(root, nt);
    }
    if let Expr::BoolExpr(be) = clause {
        if be.boolop == OR_EXPR {
            // We only return true when ALL of the OR branches are always false,
            // and every branch must be a RestrictInfo.
            for orarg in &be.args {
                match orarg {
                    Expr::RestrictInfo(rinforef) => {
                        let subri = RinfoId::from(*rinforef);
                        if !restriction_is_always_false_ri(root, root.rinfo(subri)) {
                            return false;
                        }
                    }
                    _ => return false,
                }
            }
            return true;
        }
    }
    false
}

/// [`restriction_is_always_false`] over a borrowed [`RestrictInfo`] (the
/// faithful C body, including the clone guard and the all-branches OR rule).
fn restriction_is_always_false_ri(root: &PlannerInfo, ri: &RestrictInfo) -> bool {
    if ri.has_clone || ri.is_clone {
        return false;
    }

    let clause = root.node(ri.clause);

    if let Expr::NullTest(nt) = clause {
        return nulltest_always_false(root, nt);
    }

    // If it's an OR, return true only if ALL branches are RestrictInfo AND
    // always false.
    if restriction_is_or_clause(root, ri) {
        if let Some(orid) = ri.orclause {
            if let Expr::BoolExpr(be) = root.node(orid) {
                debug_assert!(be.boolop == OR_EXPR);
                for orarg in &be.args {
                    match orarg {
                        Expr::RestrictInfo(rinforef) => {
                            let subri = RinfoId::from(*rinforef);
                            if !restriction_is_always_false_ri(root, root.rinfo(subri)) {
                                return false;
                            }
                        }
                        _ => return false,
                    }
                }
                return true;
            }
        }
    }

    false
}

/// The IS_NULL/argisrow arm shared by the bare-clause and RestrictInfo
/// `restriction_is_always_false` bodies.
fn nulltest_always_false(root: &PlannerInfo, nt: &NullTest) -> bool {
    // is this NullTest an IS_NULL qual?
    if nt.nulltesttype != NullTestType::IS_NULL {
        return false;
    }
    if nt.argisrow {
        return false;
    }
    match nt.arg.as_deref() {
        Some(arg) => expr_is_nonnullable(root, arg),
        None => false,
    }
}

/// `restriction_is_or_clause(restrictinfo)` (restrictinfo.c) — `restrictinfo`'s
/// `orclause` is non-null. Routed through the path-small seam (mirrors C's
/// `restrictinfo->orclause != NULL`).
fn restriction_is_or_clause(root: &PlannerInfo, ri: &RestrictInfo) -> bool {
    let _ = root;
    ri.orclause.is_some()
}

/// `distribute_restrictinfo_to_rels` (initsplan.c:3227).
///
/// Push a completed [`RestrictInfo`] into the proper restriction or join clause
/// list(s).
pub fn distribute_restrictinfo_to_rels<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    restrictinfo: RinfoId,
) -> PgResult<()> {
    let relids = bms::relids_copy::call(&root.rinfo(restrictinfo).required_relids);

    if !bms::relids_is_empty::call(&relids) {
        if let Some(relid) = bms::relids_get_singleton_member::call(&relids) {
            // There is only one relation participating, so it is a restriction
            // clause for that relation.
            add_base_clause_to_rel(run, root, relid, restrictinfo)?;
        } else {
            // The clause is a join clause (more than one rel in its relid set).
            // Check for hashjoinable operators. (We don't bother setting the
            // hashjoin info except in true join clauses.)
            crate::mergehash::check_hashjoinable(root, restrictinfo)?;
            // Likewise, check suitability for a Memoize node.
            crate::mergehash::check_memoizable(root, restrictinfo);
            // Add clause to the join lists of all the relevant relations.
            backend_optimizer_util_joininfo::add_join_clause_to_rels(
                run,
                root,
                restrictinfo,
                &relids,
            )?;
        }
    } else {
        // clause references no rels: shouldn't get here if callers are working.
        panic!("cannot cope with variable-free clause");
    }
    Ok(())
}

/// `process_implied_equality` (initsplan.c:3312).
///
/// Create a RestrictInfo saying "item1 op item2" and push it into the
/// appropriate lists. Returns the generated RestrictInfo, or `None` if
/// `both_const` and the clause reduced to constant TRUE.
#[allow(clippy::too_many_arguments)]
pub fn process_implied_equality<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    opno: Oid,
    collation: Oid,
    item1: &Expr,
    item2: &Expr,
    qualscope: &Relids,
    security_level: Index,
    both_const: bool,
) -> PgResult<Option<RinfoId>> {
    let mut pseudoconstant = false;

    // Build the new clause. Copy to ensure it shares no substructure with the
    // originals (necessary in case there are subselects in there).
    let mut clause = make_opclause(
        opno,
        BOOLOID,     // opresulttype
        false,       // opretset
        copy_clause_in(run, item1), // copyObject(item1)
        Some(copy_clause_in(run, item2)), // copyObject(item2)
        INVALID_OID, // opcollid
        collation,   // inputcollid
    );

    // If both constant, try to reduce to a boolean constant.
    if both_const {
        clause = initext::eval_const_expressions_expr::call(run.mcx(), clause)?;

        // If we produced const TRUE, just drop the clause.
        if let Expr::Const(c) = &clause {
            debug_assert!(c.consttype == BOOLOID);
            if !c.constisnull && const_get_bool(c) {
                return Ok(None);
            }
        }
    }

    // The rest is a very cut-down version of distribute_qual_to_rels.
    let mut relids = eqext::pull_varnos::call(root, &clause);
    debug_assert!(bms::relids_is_subset::call(&relids, qualscope));

    // If the clause is variable-free, apply it as a gating qual at the
    // appropriate level (see get_join_domain_min_rels).
    if bms::relids_is_empty::call(&relids) {
        relids = get_join_domain_min_rels(root, qualscope);
        pseudoconstant = true;
        root.hasPseudoConstantQuals = true;
    }

    // Build the RestrictInfo node itself.
    let restrictinfo = eqext::make_restrictinfo::call(
        run.mcx(),
        root,
        copy_clause_in(run, &clause),
        true,  // is_pushed_down
        false, // !has_clone
        false, // !is_clone
        pseudoconstant,
        security_level,
        bms::relids_copy::call(&relids),
        None, // incompatible_relids
        None, // outer_relids
    )?;

    // If it's a join clause, add vars used in the clause to targetlists.
    if bms::relids_membership::call(&relids) == BMS_MULTIPLE {
        let vars = eqext::pull_var_clause::call(&clause, PVC_JOINCLAUSE_FLAGS);
        crate::targetlist::add_vars_to_targetlist(root, vars, bms::relids_copy::call(&relids))?;
    }

    // Check mergejoinability (usually succeeds, since op came from an EC).
    crate::mergehash::check_mergejoinable(root, restrictinfo)?;

    // We don't do initialize_mergeclause_eclasses(); the caller handles that.
    // It's okay to call distribute_restrictinfo_to_rels() before that happens.
    distribute_restrictinfo_to_rels(run, root, restrictinfo)?;

    Ok(Some(restrictinfo))
}

/// `build_implied_join_equality` (initsplan.c:3456).
///
/// Build a RestrictInfo for a derived equality. Overlaps
/// [`process_implied_equality`] but must not push the RestrictInfo into the
/// joininfo tree.
#[allow(clippy::too_many_arguments)]
pub fn build_implied_join_equality<'mcx>(
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    opno: Oid,
    collation: Oid,
    item1: &Expr,
    item2: &Expr,
    qualscope: &Relids,
    security_level: Index,
) -> PgResult<RinfoId> {
    // Build the new clause. Copy to ensure no shared substructure.
    let clause = make_opclause(
        opno,
        BOOLOID, // opresulttype
        false,   // opretset
        copy_clause_in(run, item1),
        Some(copy_clause_in(run, item2)),
        INVALID_OID, // opcollid
        collation,   // inputcollid
    );

    // Build the RestrictInfo node itself.
    let restrictinfo = eqext::make_restrictinfo::call(
        run.mcx(),
        root,
        clause,
        true,  // is_pushed_down
        false, // !has_clone
        false, // !is_clone
        false, // pseudoconstant
        security_level,
        bms::relids_copy::call(qualscope), // required_relids
        None,                              // incompatible_relids
        None,                              // outer_relids
    )?;

    // Set mergejoinability/hashjoinability flags.
    crate::mergehash::check_mergejoinable(root, restrictinfo)?;
    crate::mergehash::check_hashjoinable(root, restrictinfo)?;
    crate::mergehash::check_memoizable(root, restrictinfo);

    Ok(restrictinfo)
}

/// `get_join_domain_min_rels` (initsplan.c:3524).
///
/// Identify the appropriate join level for derived quals belonging to the join
/// domain with the given relids: strip any lower outer joins that could commute
/// out, unless this is the top-level join domain.
fn get_join_domain_min_rels(root: &PlannerInfo, domain_relids: &Relids) -> Relids {
    let mut result = bms::relids_copy::call(domain_relids);

    // Top-level join domain?
    if bms::relids_equal::call(&result, &root.all_query_rels) {
        return result;
    }

    // Look for lower outer joins that could potentially commute out.
    // (Clone the join_info_list scope-relevant fields up front to avoid holding
    // a borrow of root.join_info_list across the mutating bms calls.)
    let lefts: Vec<(i32, Relids)> = root
        .join_info_list
        .iter()
        .filter(|sj| sj.jointype == types_pathnodes::JOIN_LEFT)
        .map(|sj| (sj.ojrelid as i32, bms::relids_copy::call(&sj.syn_righthand)))
        .collect();
    for (ojrelid, syn_righthand) in lefts {
        if bms::relids_is_member::call(ojrelid, &result) {
            result = del_member(result, ojrelid);
            result = bms::relids_difference::call(&result, &syn_righthand);
        }
    }
    result
}

/// `bms_del_member(a, x)` — relnode-seams has no del_member; mirror it as
/// `a \ {x}` (matching the `del_member` helper in `jointree.rs`).
fn del_member(a: Relids, x: i32) -> Relids {
    let single = bms::relids_make_singleton::call(x);
    bms::relids_difference::call(&a, &single)
}

/// `bms_is_member(x, varnullingrels)` over an [`ExprRelids`] bit storage.
/// Bit `x` lives in `words[x / 64]` at offset `x % 64` (the planner
/// `bitmapword` layout, `BITS_PER_WORD = 64`).
fn expr_relids_is_member(x: i32, set: &types_nodes::primnodes::ExprRelids) -> bool {
    if x < 0 {
        return false;
    }
    let wnum = (x as usize) / 64;
    let off = (x as usize) % 64;
    match set.words.get(wnum) {
        Some(w) => (w >> off) & 1 != 0,
        None => false,
    }
}

/// `DatumGetBool(constvalue)` — any nonzero word reads as true.
fn const_get_bool(c: &Const) -> bool {
    c.constvalue.as_bool()
}
