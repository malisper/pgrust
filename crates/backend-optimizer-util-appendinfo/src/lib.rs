#![forbid(unsafe_code)]
#![allow(non_snake_case)]

//! `optimizer/util/appendinfo.c` — routines for mapping between append parent(s)
//! and their children, plus row-identity variable management.
//!
//! This unit translates parent-relation `Var`s (and rtindexes appearing outside
//! Vars) into the corresponding child references when expanding inheritance /
//! UNION-ALL / partitioned appendrels.
//!
//! ## Model notes
//!
//! * The optimizer carries expressions as [`NodeId`] handles into
//!   [`PlannerInfo::node_arena`]. The Var-translation mutator therefore resolves
//!   a handle to an owned lifetime-free [`Expr`] value, rewrites it, and re-interns
//!   it — the value-tree analogue of the C `copyObject` + `expression_tree_mutator`.
//! * `Relids` set algebra runs through the relnode/pathnode bms seams (relnode is
//!   the bms owner; calling it directly would cycle).
//! * Two sub-branches of the mutator genuinely need `root->parse` (the opaque
//!   [`types_pathnodes::QueryId`]) which the `adjust_appendrel_attrs_*` seam
//!   contract drops: the UNION-ALL whole-row→`RowExpr` expansion (needs the parent
//!   RTE's `eref->colnames`). That branch is unreachable for inheritance (per the
//!   C comment) and for non-inherited queries; it returns a loud `Err` naming the
//!   dropped-`run` keystone if ever reached.

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::{String, ToString};
use alloc::vec::Vec;

use backend_nodes_core::makefuncs;
use backend_nodes_core::nodefuncs;

use mcx::Mcx;
use types_core::primitive::{AttrNumber, Index, Oid};
use types_error::{PgError, PgResult};
use types_nodes::nodes::CmdType;
use types_nodes::primnodes::etag;
use types_nodes::primnodes::{
    CoercionForm, Const, ConvertRowtypeExpr, Expr, Var, VarReturningType,
};
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    AppendRelInfo, NodeId, PlannerInfo, RelId, Relids, RestrictInfo, RinfoId,
    RowIdentityVarInfo,
};

use backend_nodes_equalfuncs_seams as equalfuncs;
use backend_optimizer_util_appendinfo_ext_seams as fdw;
use backend_optimizer_util_relnode_ext_seams as relnode_ext;
use backend_optimizer_util_relnode_seams as bms;
use backend_optimizer_plan_small_seams as initsplan;
use backend_utils_cache_lsyscache_seams as lsyscache;
use backend_utils_cache_syscache_seams as syscache;

/// `ROWID_VAR` (primnodes.h) — the special varno for row identity Vars during
/// planning.
const ROWID_VAR: i32 = -4;

/// `SelfItemPointerAttributeNumber` (sysattr.h) — the CTID system column.
const SELF_ITEM_POINTER_ATTRIBUTE_NUMBER: AttrNumber = -1;
/// `InvalidAttrNumber` (attnum.h).
const INVALID_ATTR_NUMBER: AttrNumber = 0;
/// `TIDOID` / `RECORDOID` (pg_type.dat).
const TIDOID: Oid = 27;
const RECORDOID: Oid = 2249;
const INVALID_OID: Oid = 0;

/// `RELKIND_*` (pg_class.h).
const RELKIND_RELATION: u8 = b'r';
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';
const RELKIND_FOREIGN_TABLE: u8 = b'f';

/// `NoLock` (lockdefs.h).
const NO_LOCK: i32 = 0;

type Relation<'mcx> = types_rel::RelationData<'mcx>;

/// `adjust_appendrel_attrs_context` (appendinfo.c). Carries the appinfo array and
/// (because the mutator can `elog(ERROR)`) a sticky error slot, mirroring the
/// owned-tree mutator convention.
struct AdjustContext<'a> {
    root: &'a PlannerInfo,
    appinfos: &'a [AppendRelInfo],
    /// First error raised inside the mutator (the C `elog(ERROR)` longjmp).
    err: Option<PgError>,
    /// Fresh nodes to intern after the walk (the mutator needs `&PlannerInfo`
    /// for `translated_vars`/`row_identity_vars` resolution, so it cannot also
    /// hold the `&mut` needed to `alloc_node`; deferred allocations are applied
    /// by the driver).
    _phantom: core::marker::PhantomData<&'a ()>,
}

/* ==========================================================================
 * make_append_rel_info / make_inh_translation_list
 * ======================================================================== */

/// `make_append_rel_info(parentrel, childrel, parentRTindex, childRTindex)`
/// (appendinfo.c) — build an `AppendRelInfo` for the parent/child pair.
///
/// The translated Vars (`translated_vars`) are interned as arena [`NodeId`]
/// handles, so `root` is threaded (the C list holds raw `Var *` in
/// CurrentMemoryContext; the arena is this model's planner memory context). The
/// sole caller is inherit.c's `expand_inherited_rtentry`, which owns `root`.
pub fn make_append_rel_info(
    root: &mut PlannerInfo,
    parentrel: &Relation,
    childrel: &Relation,
    parent_rt_index: Index,
    child_rt_index: Index,
) -> PgResult<AppendRelInfo> {
    let mut appinfo = AppendRelInfo {
        parent_relid: parent_rt_index,
        child_relid: child_rt_index,
        parent_reltype: parentrel.rd_rel.reltype,
        child_reltype: childrel.rd_rel.reltype,
        ..Default::default()
    };
    make_inh_translation_list(root, parentrel, childrel, child_rt_index, &mut appinfo)?;
    appinfo.parent_reloid = parentrel.rd_id;
    Ok(appinfo)
}

/// `make_inh_translation_list(oldrelation, newrelation, newvarno, appinfo)`
/// (appendinfo.c) — build the list of parent→child Var translations and the
/// reverse-translation array.
fn make_inh_translation_list(
    root: &mut PlannerInfo,
    oldrelation: &Relation,
    newrelation: &Relation,
    newvarno: Index,
    appinfo: &mut AppendRelInfo,
) -> PgResult<()> {
    let old_tupdesc = &oldrelation.rd_att;
    let new_tupdesc = &newrelation.rd_att;
    let new_relid = newrelation.rd_id;
    let oldnatts = old_tupdesc.natts as usize;
    let newnatts = new_tupdesc.natts as usize;

    let same_relation = oldrelation.rd_id == newrelation.rd_id;

    let mut vars: Vec<Option<Var>> = Vec::new();
    // Initialize reverse-translation array with all entries zero.
    appinfo.num_child_cols = newnatts as i32;
    let mut pcolnos: Vec<i16> = alloc::vec![0i16; newnatts];

    let mut new_attno: usize = 0;

    for old_attno in 0..oldnatts {
        let att = old_tupdesc.attr(old_attno);
        if att.attisdropped {
            // Just put NULL into this list entry.
            vars.push(None);
            continue;
        }
        let attname = String::from_utf8_lossy(att.attname.name_str()).into_owned();
        let atttypid = att.atttypid;
        let atttypmod = att.atttypmod;
        let attcollation = att.attcollation;

        // When generating the translation list for the parent table of an
        // inheritance set, no need to search for matches.
        if same_relation {
            vars.push(Some(makefuncs::make_var(
                newvarno as i32,
                (old_attno + 1) as AttrNumber,
                atttypid,
                atttypmod,
                attcollation,
                0,
            )));
            pcolnos[old_attno] = (old_attno + 1) as i16;
            continue;
        }

        // Otherwise search for the matching column by name, trying the column
        // that immediately follows the last match first, then syscache.
        let matched_att_typid;
        let matched_att_typmod;
        let matched_att_collation;
        let need_search = if new_attno >= newnatts {
            true
        } else {
            let cand = new_tupdesc.attr(new_attno);
            cand.attisdropped
                || String::from_utf8_lossy(cand.attname.name_str()) != attname
        };
        if need_search {
            match syscache::search_syscache_attname::call(new_relid, &attname)? {
                Some((attnum, _atttypid)) => {
                    new_attno = (attnum - 1) as usize;
                    debug_assert!(new_attno < newnatts);
                }
                None => {
                    return Err(PgError::error(format!(
                        "could not find inherited attribute \"{}\" of relation \"{}\"",
                        attname,
                        newrelation.name()
                    )));
                }
            }
        }
        {
            let cand = new_tupdesc.attr(new_attno);
            matched_att_typid = cand.atttypid;
            matched_att_typmod = cand.atttypmod;
            matched_att_collation = cand.attcollation;
        }

        // Found it, check type and collation match.
        if atttypid != matched_att_typid || atttypmod != matched_att_typmod {
            return Err(PgError::error(format!(
                "attribute \"{}\" of relation \"{}\" does not match parent's type",
                attname,
                newrelation.name()
            )));
        }
        if attcollation != matched_att_collation {
            return Err(PgError::error(format!(
                "attribute \"{}\" of relation \"{}\" does not match parent's collation",
                attname,
                newrelation.name()
            )));
        }

        vars.push(Some(makefuncs::make_var(
            newvarno as i32,
            (new_attno + 1) as AttrNumber,
            atttypid,
            atttypmod,
            attcollation,
            0,
        )));
        pcolnos[new_attno] = (old_attno + 1) as i16;
        new_attno += 1;
    }

    appinfo.parent_colnos = pcolnos;
    // Intern each translated Var (or a NULL element = NodeId::default() (0) for a
    // dropped parent column) into the node arena.
    appinfo.translated_vars = Vec::with_capacity(vars.len());
    for v in vars {
        let handle = match v {
            Some(var) => root.alloc_node(Expr::Var(var)),
            None => NodeId::default(),
        };
        appinfo.translated_vars.push(handle);
    }
    Ok(())
}

/* ==========================================================================
 * adjust_appendrel_attrs (+ mutator)
 * ======================================================================== */

/// `adjust_appendrel_attrs(root, node, nappinfos, appinfos)` (appendinfo.c) —
/// copy `node` translating parent Vars/rtindexes to the corresponding child.
pub fn adjust_appendrel_attrs(
    root: &mut PlannerInfo,
    node: Expr,
    appinfos: &[AppendRelInfo],
) -> PgResult<Expr> {
    debug_assert!(!appinfos.is_empty());
    let mut pending: Vec<Expr> = Vec::new();
    let result = {
        let mut ctx = AdjustContext {
            root,
            appinfos,
            err: None,
            _phantom: core::marker::PhantomData,
        };
        let out = adjust_appendrel_attrs_mutator(node, &mut ctx, &mut pending);
        if let Some(e) = ctx.err.take() {
            return Err(e);
        }
        out
    };
    // No deferred interning is produced by the expressible branches (Var/
    // RestrictInfo/CurrentOfExpr/PlaceHolderVar translate in place over the owned
    // value tree); `pending` stays empty.
    debug_assert!(pending.is_empty());
    Ok(result)
}

/// `adjust_appendrel_attrs_mutator(node, context)` (appendinfo.c). Operates over
/// the owned lifetime-free [`Expr`] value tree (the value analogue of the C
/// copy-and-mutate). On a translation error it records into `context.err` and
/// returns the partially-built node (the driver surfaces the error).
fn adjust_appendrel_attrs_mutator(
    node: Expr,
    context: &mut AdjustContext,
    pending: &mut Vec<Expr>,
) -> Expr {
    let appinfos = context.appinfos;

    match node.expr_tag() {
        etag::T_Var => {
            let mut var = node.expect_into_var();
            if var.varlevelsup != 0 {
                return Expr::Var(var); // no changes needed
            }

            let appinfo = appinfos
                .iter()
                .find(|ai| var.varno == ai.parent_relid as i32);

            if let Some(appinfo) = appinfo {
                var.varno = appinfo.child_relid as i32;
                // It's now a generated Var, so drop any syntactic labeling.
                var.varnosyn = 0;
                var.varattnosyn = 0;
                if var.varattno > 0 {
                    if var.varattno as usize > appinfo.translated_vars.len() {
                        context.err = Some(PgError::error(format!(
                            "attribute {} of relation \"{}\" does not exist",
                            var.varattno,
                            rel_name_or_unknown(context.root, appinfo.parent_reloid)
                        )));
                        return Expr::Var(var);
                    }
                    let handle = appinfo.translated_vars[(var.varattno - 1) as usize];
                    // A NULL element (dropped parent column) is NodeId::default().
                    if handle == NodeId::default() {
                        context.err = Some(PgError::error(format!(
                            "attribute {} of relation \"{}\" does not exist",
                            var.varattno,
                            rel_name_or_unknown(context.root, appinfo.parent_reloid)
                        )));
                        return Expr::Var(var);
                    }
                    let newnode = context.root.node(handle).clone();
                    if let Expr::Var(mut newvar) = newnode {
                        newvar.varreturningtype = var.varreturningtype;
                        newvar.varnullingrels = expr_relids_add_all(
                            newvar.varnullingrels,
                            &var.varnullingrels,
                        );
                        return Expr::Var(newvar);
                    } else {
                        if var.varreturningtype != VarReturningType::VAR_RETURNING_DEFAULT {
                            context.err = Some(PgError::error(
                                "failed to apply returningtype to a non-Var",
                            ));
                            return newnode;
                        }
                        if !var.varnullingrels.words.is_empty() {
                            context.err = Some(PgError::error(
                                "failed to apply nullingrels to a non-Var",
                            ));
                            return newnode;
                        }
                        return newnode;
                    }
                } else if var.varattno == 0 {
                    // Whole-row Var.
                    if appinfo.child_reltype != INVALID_OID {
                        debug_assert_eq!(var.vartype, appinfo.parent_reltype);
                        if appinfo.parent_reltype != appinfo.child_reltype {
                            let mut r = ConvertRowtypeExpr {
                                arg: None,
                                resulttype: appinfo.parent_reltype,
                                convertformat: CoercionForm::COERCE_IMPLICIT_CAST,
                                location: -1,
                            };
                            // Make sure the Var node has the right type ID, too.
                            var.vartype = appinfo.child_reltype;
                            r.arg = Some(Box::new(Expr::Var(var)));
                            return Expr::ConvertRowtypeExpr(r);
                        }
                        // parent_reltype == child_reltype: fall through, Var kept.
                    } else {
                        // UNION-ALL whole-row: build a RowExpr from translated_vars
                        // using the parent RTE's colnames. This needs
                        // root->parse->rtable (the opaque QueryId), which the
                        // adjust_appendrel_attrs seam contract drops.
                        context.err = Some(PgError::error(
                            "adjust_appendrel_attrs: UNION-ALL whole-row Var → RowExpr \
                             needs root->parse->rtable colnames (dropped-run keystone; \
                             unreachable for inheritance/non-inherited queries)",
                        ));
                        return Expr::Var(var);
                    }
                }
                // system attributes don't need any other translation
                Expr::Var(var)
            } else if var.varno == ROWID_VAR {
                // ROWID_VAR placeholder: if we've reached a leaf target rel, we can
                // translate to a specific instantiation.
                let leaf_result_relids = &context.root.leaf_result_relids;
                let mut leaf_relid: i32 = 0;
                for ai in appinfos {
                    if bms::relids_is_member::call(ai.child_relid as i32, leaf_result_relids) {
                        if leaf_relid != 0 {
                            context.err =
                                Some(PgError::error("cannot translate to multiple leaf relids"));
                            return Expr::Var(var);
                        }
                        leaf_relid = ai.child_relid as i32;
                    }
                }

                if leaf_relid != 0 {
                    let ridinfo_handle =
                        context.root.row_identity_vars[(var.varattno - 1) as usize];
                    let ridinfo = context.root.rowidvar(ridinfo_handle);
                    if bms::relids_is_member::call(leaf_relid, &ridinfo.rowidrels) {
                        // Substitute the Var given in the RowIdentityVarInfo.
                        let mut newvar = ridinfo.rowidvar.clone();
                        newvar.varno = leaf_relid;
                        debug_assert!(newvar.varnullingrels.words.is_empty());
                        newvar.varnosyn = 0;
                        newvar.varattnosyn = 0;
                        return Expr::Var(newvar);
                    } else {
                        // This leaf rel can't return the desired value, so
                        // substitute a NULL of the correct type.
                        match make_null_const(var.vartype, var.vartypmod, var.varcollid) {
                            Ok(c) => return Expr::Const(c),
                            Err(e) => {
                                context.err = Some(e);
                                return Expr::Var(var);
                            }
                        }
                    }
                }
                Expr::Var(var)
            } else {
                Expr::Var(var)
            }
        }
        etag::T_CurrentOfExpr => {
            let mut cexpr = node.expect_into_currentofexpr();
            for ai in appinfos {
                if cexpr.cvarno == ai.parent_relid {
                    cexpr.cvarno = ai.child_relid;
                    break;
                }
            }
            Expr::CurrentOfExpr(cexpr)
        }
        etag::T_PlaceHolderVar => {
            let mut phv = node.expect_into_placeholdervar();
            // Copy the PlaceHolderVar node with correct mutation of subnodes.
            if let Some(inner) = phv.phexpr.take() {
                let mutated = adjust_appendrel_attrs_mutator(*inner, context, pending);
                phv.phexpr = Some(Box::new(mutated));
            }
            // Now fix PlaceHolderVar's relid sets.
            if phv.phlevelsup == 0 {
                phv.phrels = adjust_child_relids_expr(&phv.phrels, appinfos);
                // we needn't touch phnullingrels
            }
            Expr::PlaceHolderVar(phv)
        }
        // Generic recursion for all other expression node types.
        _ => {
            let mut local_err: Option<PgError> = None;
            let result = nodefuncs::expression_tree_mutator(node, &mut |child: Expr| {
                if local_err.is_some() || context.err.is_some() {
                    return child;
                }
                let out = adjust_appendrel_attrs_mutator(child, context, pending);
                if let Some(e) = context.err.take() {
                    local_err = Some(e);
                }
                out
            });
            if let Some(e) = local_err {
                context.err = Some(e);
            }
            result
        }
    }
}

/// `(RestrictInfo *) adjust_appendrel_attrs_mutator` (appendinfo.c) for a
/// `RestrictInfo`. Returns the translated `RestrictInfo` value; the driver
/// interns its clause/orclause and stores it.
fn adjust_restrictinfo(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    oldinfo_id: RinfoId,
    appinfos: &[AppendRelInfo],
) -> PgResult<RinfoId> {
    // Copy all flat-copiable fields (notably including rinfo_serial).
    let mut newinfo: RestrictInfo = root.rinfo(oldinfo_id).clone();

    // Recursively fix the clause itself. Deep-copy via `clone_in` into the
    // planner arena `mcx` (the value is moved into `adjust_appendrel_attrs` and
    // the translated node is interned back into `root`'s node arena, so its
    // owned `PgBox`/`PgVec` children must outlive the whole run — a transient
    // context would dangle). A derived `.clone()` panics on an owned-subtree
    // child such as a `SubLink`/`SubPlan` correlated-subquery operand.
    let clause_expr = root.node(newinfo.clause).clone_in(mcx)?;
    let new_clause = adjust_appendrel_attrs(root, clause_expr, appinfos)?;
    newinfo.clause = root.alloc_node(new_clause);

    // and the modified version, if an OR clause.
    if let Some(orclause_id) = newinfo.orclause {
        let or_expr = root.node(orclause_id).clone_in(mcx)?;
        let new_or = adjust_appendrel_attrs(root, or_expr, appinfos)?;
        newinfo.orclause = Some(root.alloc_node(new_or));
    }

    // adjust relid sets too.
    newinfo.clause_relids = adjust_child_relids(&newinfo.clause_relids, appinfos);
    newinfo.required_relids = adjust_child_relids(&newinfo.required_relids, appinfos);
    newinfo.outer_relids = adjust_child_relids(&newinfo.outer_relids, appinfos);
    newinfo.left_relids = adjust_child_relids(&newinfo.left_relids, appinfos);
    newinfo.right_relids = adjust_child_relids(&newinfo.right_relids, appinfos);

    // Reset cached derivative fields.
    newinfo.eval_cost.startup = -1.0;
    newinfo.norm_selec = -1.0;
    newinfo.outer_selec = -1.0;
    newinfo.left_em = None;
    newinfo.right_em = None;
    newinfo.scansel_cache = Vec::new();
    newinfo.left_bucketsize = -1.0;
    newinfo.right_bucketsize = -1.0;
    newinfo.left_mcvfreq = -1.0;
    newinfo.right_mcvfreq = -1.0;

    Ok(root.alloc_rinfo(newinfo))
}

/* ==========================================================================
 * adjust_appendrel_attrs_multilevel
 * ======================================================================== */

/// `adjust_appendrel_attrs_multilevel(root, node, childrel, parentrel)`
/// (appendinfo.c) — apply Var translations down through (possibly multiple)
/// inheritance levels.
pub fn adjust_appendrel_attrs_multilevel(
    root: &mut PlannerInfo,
    node: Expr,
    childrel: RelId,
    parentrel: RelId,
) -> PgResult<Expr> {
    let mut node = node;
    // Recurse if immediate parent is not the top parent.
    let immediate_parent = root.rel(childrel).parent;
    if immediate_parent != Some(parentrel) {
        match immediate_parent {
            Some(p) => {
                node = adjust_appendrel_attrs_multilevel(root, node, p, parentrel)?;
            }
            None => {
                return Err(PgError::error("childrel is not a child of parentrel"));
            }
        }
    }
    // Now translate for this child.
    let child_relids = root.rel(childrel).relids.clone();
    let appinfos = find_appinfos_by_relids(root, &child_relids)?;
    adjust_appendrel_attrs(root, node, &appinfos)
}

/* ==========================================================================
 * adjust_child_relids / adjust_child_relids_multilevel
 * ======================================================================== */

/// `adjust_child_relids(relids, nappinfos, appinfos)` (appendinfo.c) —
/// substitute child relids for parent relids in a `Relids` set.
pub fn adjust_child_relids(relids: &Relids, appinfos: &[AppendRelInfo]) -> Relids {
    let mut result: Option<Relids> = None;
    for appinfo in appinfos {
        // Remove parent, add child.
        if bms::relids_is_member::call(appinfo.parent_relid as i32, relids) {
            // Make a copy if we are changing the set.
            if result.is_none() {
                result = Some(bms::relids_copy::call(relids));
            }
            let r = result.take().unwrap();
            let r = relids_del_member(r, appinfo.parent_relid as i32);
            let r = bms::relids_add_member::call(r, appinfo.child_relid as i32);
            result = Some(r);
        }
    }
    match result {
        Some(r) => r,
        None => bms::relids_copy::call(relids),
    }
}

/// `adjust_child_relids` over an [`ExprRelids`] word set (PlaceHolderVar.phrels).
fn adjust_child_relids_expr(
    relids: &types_nodes::primnodes::ExprRelids,
    appinfos: &[AppendRelInfo],
) -> types_nodes::primnodes::ExprRelids {
    use backend_rewrite_core::relids as er;
    let mut result: Option<types_nodes::primnodes::ExprRelids> = None;
    for appinfo in appinfos {
        if er::is_member(appinfo.parent_relid as i32, relids) {
            if result.is_none() {
                result = Some(er::copy(relids));
            }
            let r = result.take().unwrap();
            let r = er::del_member(r, appinfo.parent_relid as i32);
            let r = er::add_member(r, appinfo.child_relid as i32);
            result = Some(r);
        }
    }
    result.unwrap_or_else(|| er::copy(relids))
}

/// `adjust_child_relids_multilevel(root, relids, childrel, parentrel)`
/// (appendinfo.c) — substitute child relids for parent relids, possibly across
/// multiple inheritance levels.
pub fn adjust_child_relids_multilevel(
    root: &mut PlannerInfo,
    relids: &Relids,
    childrel: RelId,
    parentrel: RelId,
) -> PgResult<Relids> {
    // If the given relids set doesn't contain any of the parent relids, it
    // will remain unchanged.
    let parent_relids = root.rel(parentrel).relids.clone();
    if !bms::relids_overlap::call(relids, &parent_relids) {
        return Ok(bms::relids_copy::call(relids));
    }

    let mut relids_owned = bms::relids_copy::call(relids);

    // Recurse if immediate parent is not the top parent.
    let immediate_parent = root.rel(childrel).parent;
    if immediate_parent != Some(parentrel) {
        match immediate_parent {
            Some(p) => {
                relids_owned = adjust_child_relids_multilevel(root, &relids_owned, p, parentrel)?;
            }
            None => {
                return Err(PgError::error("childrel is not a child of parentrel"));
            }
        }
    }

    // Now translate for this child.
    let child_relids = root.rel(childrel).relids.clone();
    let appinfos = find_appinfos_by_relids(root, &child_relids)?;
    Ok(adjust_child_relids(&relids_owned, &appinfos))
}

/* ==========================================================================
 * adjust_inherited_attnums (+ _multilevel)
 * ======================================================================== */

/// `adjust_inherited_attnums(attnums, context)` (appendinfo.c) — translate an
/// integer list of attribute numbers from parent to child.
pub fn adjust_inherited_attnums(
    root: &PlannerInfo,
    attnums: &[AttrNumber],
    context: &AppendRelInfo,
) -> PgResult<Vec<AttrNumber>> {
    // This should only happen for an inheritance case, not UNION ALL.
    debug_assert!(context.parent_reloid != INVALID_OID);

    let mut result = Vec::with_capacity(attnums.len());
    for &parentattno in attnums {
        // Look up the translation of this column: it must be a Var.
        if parentattno <= 0 || parentattno as usize > context.translated_vars.len() {
            return Err(PgError::error(format!(
                "attribute {} of relation \"{}\" does not exist",
                parentattno,
                rel_name_or_unknown(root, context.parent_reloid)
            )));
        }
        let handle = context.translated_vars[(parentattno - 1) as usize];
        if handle == NodeId::default() {
            return Err(PgError::error(format!(
                "attribute {} of relation \"{}\" does not exist",
                parentattno,
                rel_name_or_unknown(root, context.parent_reloid)
            )));
        }
        match root.node(handle).as_var() {
            Some(childvar) => result.push(childvar.varattno),
            None => {
                return Err(PgError::error(format!(
                    "attribute {} of relation \"{}\" does not exist",
                    parentattno,
                    rel_name_or_unknown(root, context.parent_reloid)
                )));
            }
        }
    }
    Ok(result)
}

/// `adjust_inherited_attnums_multilevel(root, attnums, child_relid,
/// top_parent_relid)` (appendinfo.c).
pub fn adjust_inherited_attnums_multilevel(
    root: &PlannerInfo,
    attnums: &[AttrNumber],
    child_relid: Index,
    top_parent_relid: Index,
) -> PgResult<Vec<AttrNumber>> {
    let appinfo = root
        .append_rel_array
        .get(child_relid as usize)
        .and_then(|o| o.as_ref())
        .ok_or_else(|| {
            PgError::error(format!(
                "child rel {} not found in append_rel_array",
                child_relid
            ))
        })?
        .clone();

    let attnums_owned: Vec<AttrNumber>;
    let attnums_ref: &[AttrNumber] = if appinfo.parent_relid != top_parent_relid {
        attnums_owned = adjust_inherited_attnums_multilevel(
            root,
            attnums,
            appinfo.parent_relid,
            top_parent_relid,
        )?;
        &attnums_owned
    } else {
        attnums
    };

    adjust_inherited_attnums(root, attnums_ref, &appinfo)
}

/* ==========================================================================
 * get_translated_update_targetlist
 * ======================================================================== */

/// `get_translated_update_targetlist(root, relid, &processed_tlist,
/// &update_colnos)` (appendinfo.c) — the UPDATE processed_tlist (and optionally
/// update column numbers) translated to a child target relation.
///
/// Returns `(processed_tlist_handles, update_colnos)`. `update_colnos` is `None`
/// when the caller doesn't want it.
pub fn get_translated_update_targetlist(
    mcx: Mcx<'_>,
    run: &PlannerRun<'_>,
    root: &mut PlannerInfo,
    relid: Index,
    want_update_colnos: bool,
) -> PgResult<(Vec<NodeId>, Option<Vec<AttrNumber>>)> {
    let parse = run.resolve(root.parse);
    debug_assert_eq!(parse.commandType, CmdType::CMD_UPDATE);
    let result_relation = parse.resultRelation as Index;

    if relid == result_relation {
        // Non-inheritance case: copy the processed_tlist (caller may scribble).
        let tlist = copy_targetentry_handles(mcx, root, &root.processed_tlist.clone())?;
        let colnos = if want_update_colnos {
            Some(root.update_colnos.clone())
        } else {
            None
        };
        Ok((tlist, colnos))
    } else {
        debug_assert!(bms::relids_is_member::call(
            relid as i32,
            &root.all_result_relids
        ));
        let childrel = relnode_find_base_rel(root, relid as i32);
        let parentrel = relnode_find_base_rel(root, result_relation as i32);

        // Translate the processed_tlist (a list of TargetEntry handles).
        let tlist = adjust_targetlist_multilevel(
            mcx,
            root,
            &root.processed_tlist.clone(),
            childrel,
            parentrel,
        )?;

        let colnos = if want_update_colnos {
            Some(adjust_inherited_attnums_multilevel(
                root,
                &root.update_colnos.clone(),
                relid,
                result_relation,
            )?)
        } else {
            None
        };
        Ok((tlist, colnos))
    }
}

/* ==========================================================================
 * find_appinfos_by_relids
 * ======================================================================== */

/// `find_appinfos_by_relids(root, relids, &nappinfos)` (appendinfo.c) — the
/// `AppendRelInfo`s for the base relations listed in `relids` (a freshly-owned
/// vector; outer-join indexes are silently ignored).
pub fn find_appinfos_by_relids(
    root: &PlannerInfo,
    relids: &Relids,
) -> PgResult<Vec<AppendRelInfo>> {
    let mut appinfos: Vec<AppendRelInfo> =
        Vec::with_capacity(bms::relids_num_members::call(relids) as usize);

    let mut i: i32 = -1;
    loop {
        i = bms::relids_next_member::call(relids, i);
        if i < 0 {
            break;
        }
        let appinfo = root
            .append_rel_array
            .get(i as usize)
            .and_then(|o| o.as_ref());
        match appinfo {
            Some(ai) => appinfos.push(ai.clone()),
            None => {
                // Probably i is an OJ index, but let's check.
                if relnode_ext_find_base_rel_ignore_join_is_null(root, i) {
                    continue;
                }
                return Err(PgError::error(format!(
                    "child rel {} not found in append_rel_array",
                    i
                )));
            }
        }
    }
    Ok(appinfos)
}

/* ==========================================================================
 * ROW-IDENTITY VARIABLE MANAGEMENT
 * ======================================================================== */

/// `add_row_identity_var(root, orig_var, rtindex, rowid_name)` (appendinfo.c) —
/// register a row-identity column to be used in UPDATE/DELETE/MERGE.
///
/// `result_relation` is `root->parse->resultRelation` (the opaque `QueryId`
/// resolves only through the caller's `run`, so it is threaded in).
fn add_row_identity_var(
    root: &mut PlannerInfo,
    orig_var: Var,
    rtindex: Index,
    rowid_name: &str,
    result_relation: Index,
) -> PgResult<()> {
    // For now, the argument must be just a Var of the given rtindex.
    debug_assert_eq!(orig_var.varno, rtindex as i32);
    debug_assert_eq!(orig_var.varlevelsup, 0);
    debug_assert!(orig_var.varnullingrels.words.is_empty());

    if rtindex == result_relation {
        let expr_id = root.alloc_node(Expr::Var(orig_var));
        let tle = types_pathnodes::TargetEntryNode {
            expr: expr_id,
            resno: (root.processed_tlist.len() + 1) as AttrNumber,
            resname: Some(rowid_name.to_string()),
            ressortgroupref: 0,
            resorigtbl: INVALID_OID,
            resorigcol: 0,
            resjunk: true,
        };
        let tle_id = root.alloc_targetentry(tle);
        root.processed_tlist.push(tle_id);
        return Ok(());
    }

    // Otherwise, rtindex should reference a leaf target relation.
    debug_assert!(bms::relids_is_member::call(
        rtindex as i32,
        &root.leaf_result_relids
    ));
    debug_assert!(root
        .append_rel_array
        .get(rtindex as usize)
        .map(|o| o.is_some())
        .unwrap_or(false));

    // Find a matching RowIdentityVarInfo, or make one. To allow using equal() to
    // match the vars, change varno to ROWID_VAR, leaving all else alone.
    let mut rowid_var = orig_var.clone();
    rowid_var.varno = ROWID_VAR;

    // Look for an existing row-id column of the same name.
    for &handle in &root.row_identity_vars.clone() {
        let (name_matches, var_matches) = {
            let ridinfo = root.rowidvar(handle);
            (
                ridinfo.rowidname == rowid_name,
                equalfuncs::equal_expr::call(
                    &Expr::Var(rowid_var.clone()),
                    &Expr::Var(ridinfo.rowidvar.clone()),
                ),
            )
        };
        if !name_matches {
            continue;
        }
        if var_matches {
            // Found a match; record that rtindex needs it too.
            let ridinfo = root.rowidvar_mut(handle);
            ridinfo.rowidrels = bms::relids_add_member::call(
                core::mem::take(&mut ridinfo.rowidrels),
                rtindex as i32,
            );
            return Ok(());
        } else {
            return Err(PgError::error(format!(
                "conflicting uses of row-identity name \"{}\"",
                rowid_name
            )));
        }
    }

    // No request yet, so add a new RowIdentityVarInfo.
    let rowidwidth = lsyscache::get_typavgwidth::call(
        nodefuncs::expr_type(Some(&Expr::Var(rowid_var.clone())))?,
        nodefuncs::expr_typmod(Some(&Expr::Var(rowid_var.clone())))?,
    )?;
    let ridinfo = RowIdentityVarInfo {
        rowidvar: rowid_var.clone(),
        rowidwidth,
        rowidname: rowid_name.to_string(),
        rowidrels: bms::relids_make_singleton::call(rtindex as i32),
    };
    let ridinfo_id = root.alloc_rowidvar(ridinfo);
    root.row_identity_vars.push(ridinfo_id);

    // Change rowid_var into a reference to this row_identity_vars entry.
    rowid_var.varattno = root.row_identity_vars.len() as AttrNumber;

    // Push the ROWID_VAR reference variable into processed_tlist.
    let expr_id = root.alloc_node(Expr::Var(rowid_var));
    let tle = types_pathnodes::TargetEntryNode {
        expr: expr_id,
        resno: (root.processed_tlist.len() + 1) as AttrNumber,
        resname: Some(rowid_name.to_string()),
        ressortgroupref: 0,
        resorigtbl: INVALID_OID,
        resorigcol: 0,
        resjunk: true,
    };
    let tle_id = root.alloc_targetentry(tle);
    root.processed_tlist.push(tle_id);
    Ok(())
}

/// `add_row_identity_columns(root, rtindex, target_rte, target_relation)`
/// (appendinfo.c) — add the core row-identity columns for the target relation.
fn add_row_identity_columns(
    root: &mut PlannerInfo,
    rtindex: Index,
    command_type: CmdType,
    relid: Oid,
    relkind: u8,
    has_delete_row_trigger: bool,
    result_relation: Index,
) -> PgResult<()> {
    debug_assert!(
        command_type == CmdType::CMD_UPDATE
            || command_type == CmdType::CMD_DELETE
            || command_type == CmdType::CMD_MERGE
    );

    if relkind == RELKIND_RELATION
        || relkind == RELKIND_MATVIEW
        || relkind == RELKIND_PARTITIONED_TABLE
    {
        // Emit CTID so the executor can find the row to merge/update/delete.
        let var = makefuncs::make_var(
            rtindex as i32,
            SELF_ITEM_POINTER_ATTRIBUTE_NUMBER,
            TIDOID,
            -1,
            INVALID_OID,
            0,
        );
        add_row_identity_var(root, var, rtindex, "ctid", result_relation)?;
    } else if relkind == RELKIND_FOREIGN_TABLE {
        // Let the foreign table's FDW add whatever junk TLEs it wants.
        if fdw::fdw_has_add_foreign_update_targets::call(relid)? {
            fdw::fdw_add_foreign_update_targets::call(root, rtindex as u32, relid, command_type)?;
        }

        // For UPDATE (or with delete-row triggers) the FDW must fetch a whole-row
        // Var so the executor can build the complete new / old tuple.
        if command_type == CmdType::CMD_UPDATE || has_delete_row_trigger {
            let var = makefuncs::make_var(
                rtindex as i32,
                INVALID_ATTR_NUMBER,
                RECORDOID,
                -1,
                INVALID_OID,
                0,
            );
            add_row_identity_var(root, var, rtindex, "wholerow", result_relation)?;
        }
    }
    Ok(())
}

/// `distribute_row_identity_vars(root)` (appendinfo.c) — after row-identity
/// columns are identified, make sure they are generated by all target relations.
pub fn distribute_row_identity_vars(
    mcx: Mcx<'_>,
    run: &PlannerRun<'_>,
    root: &mut PlannerInfo,
) -> PgResult<()> {
    let (command_type, result_relation, target_rte_relid, target_rte_inh) = {
        let parse = run.resolve(root.parse);
        let result_relation = parse.resultRelation as Index;
        let (relid, inh) = if command_is_modify(parse.commandType) {
            let rte = parse
                .rtable
                .get((result_relation - 1) as usize)
                .ok_or_else(|| {
                    PgError::error("distribute_row_identity_vars: result relation out of range")
                })?;
            (rte.relid, rte.inh)
        } else {
            (INVALID_OID, false)
        };
        (parse.commandType, result_relation, relid, inh)
    };

    // Nothing to do if this isn't an inherited UPDATE/DELETE/MERGE.
    if !command_is_modify(command_type) {
        debug_assert!(root.row_identity_vars.is_empty());
        return Ok(());
    }
    if !target_rte_inh {
        debug_assert!(root.row_identity_vars.is_empty());
        return Ok(());
    }

    // Edge case: constraint exclusion suppressed every leaf relation. Re-open the
    // top result relation and add the row identity columns it would have used.
    if root.row_identity_vars.is_empty() {
        let target_relation =
            backend_access_table_table_seams::table_open::call(mcx, target_rte_relid, NO_LOCK)?;
        let relkind = target_relation.rd_rel.relkind;
        let has_delete_row_trigger = relation_has_delete_row_trigger(&target_relation);
        add_row_identity_columns(
            root,
            result_relation,
            command_type,
            target_rte_relid,
            relkind,
            has_delete_row_trigger,
            result_relation,
        )?;
        // C: `table_close(target_relation, NoLock)` — consume the owning carrier
        // opened above so its single relcache-pin release runs here and its Drop
        // closer is disarmed. Using the by-OID `relation_close(target_rte_relid)`
        // instead decremented the relcache refcount but left `target_relation`
        // armed, so its end-of-scope Drop double-released the pin (rd_refcnt
        // underflow at xact end for an inherited UPDATE/DELETE — e.g. a FOR
        // UPDATE cursor over an inheritance parent).
        target_relation.close(NO_LOCK)?;
        // Re-run build_base_rel_tlists to propagate the added columns.
        initsplan::build_base_rel_tlists::call(root, run);
        // There are no ROWID_VAR Vars in this case, so we're done.
        return Ok(());
    }

    // Dig through processed_tlist to find the ROWID_VAR reference Vars and copy
    // them into the reltarget of the topmost target relation.
    let target_rel = relnode_find_base_rel(root, result_relation as i32);

    let tlist = root.processed_tlist.clone();
    for tle_id in tlist {
        let expr_id = root.targetentry(tle_id).expr;
        let var_opt = root
            .node(expr_id)
            .as_var()
            .filter(|v| v.varno == ROWID_VAR)
            .cloned();
        if let Some(v) = var_opt {
            let copy_id = root.alloc_node(Expr::Var(v));
            if let Some(rt) = root.rel_mut(target_rel).reltarget.as_mut() {
                rt.exprs.push(copy_id);
            }
        }
    }
    Ok(())
}

/* ==========================================================================
 * Small helpers
 * ======================================================================== */

fn command_is_modify(ct: CmdType) -> bool {
    ct == CmdType::CMD_UPDATE || ct == CmdType::CMD_DELETE || ct == CmdType::CMD_MERGE
}

/// `((RowIdentityVarInfo *) list_nth(root->row_identity_vars, n))->rowidwidth`
/// (preptlist.c) — the cached width of the n'th (0-based) `RowIdentityVarInfo`.
fn row_identity_var_rowidwidth(root: &PlannerInfo, n: i32) -> i32 {
    let handle = root.row_identity_vars[n as usize];
    root.rowidvar(handle).rowidwidth
}

/// The C `get_rel_name(relid)` is used only inside these `elog(ERROR,
/// "attribute %d of relation \"%s\"")` internal-corruption messages, where C
/// itself tolerates a NULL name. `get_rel_name` needs an `Mcx` that the mcx-free
/// mutator/attnum seam contract does not carry, so render the OID into the
/// message instead (error text only — never on a success path).
fn rel_name_or_unknown(_root: &PlannerInfo, relid: Oid) -> String {
    alloc::format!("OID {}", relid)
}

/// `makeNullConst(consttype, consttypmod, constcollid)` (makefuncs.c) over the
/// value tree (no `Mcx` needed: a null `Const` never inspects `constvalue`).
fn make_null_const(consttype: Oid, consttypmod: i32, constcollid: Oid) -> PgResult<Const> {
    let (typ_len, typ_byval) = lsyscache::get_typlenbyval::call(consttype)?;
    Ok(Const {
        consttype,
        consttypmod,
        constcollid,
        constlen: typ_len as i32,
        // A null Const's value is never inspected (constisnull = true).
        constvalue: types_tuple::backend_access_common_heaptuple::Datum::ByVal(0),
        constisnull: true,
        constbyval: typ_byval,
        location: -1,
    })
}

/// `relation->trigdesc && (trig_delete_after_row || trig_delete_before_row)`.
fn relation_has_delete_row_trigger(rel: &Relation) -> bool {
    match rel.rd_trigdesc.as_ref() {
        Some(td) => td.trig_delete_after_row || td.trig_delete_before_row,
        None => false,
    }
}

/// `find_base_rel(root, relid)` through the relnode seam (relnode is the bms /
/// base-rel owner; a direct call would cycle).
fn relnode_find_base_rel(root: &PlannerInfo, relid: i32) -> RelId {
    bms::find_base_rel::call(root, relid)
}

/// `find_base_rel_ignore_join(root, relid) == NULL` (relnode.c). The real
/// provider needs `run` (it reads the RTE kind to debug-assert an OJ index), but
/// the `find_appinfos_by_relids` seam contract does not carry `run`. The only
/// observable distinction the caller needs is OJ-vs-base-rel, which the
/// `simple_rel_array` slot already encodes: an in-range `None` slot is the
/// outer-join index (`find_base_rel_ignore_join` returns NULL → the caller skips
/// it); an in-range `Some` slot is a base rel (returns non-NULL → the caller
/// raises the "child rel not found in append_rel_array" error); an out-of-range
/// index is the C `find_base_rel_ignore_join` terminal `elog`/panic.
fn relnode_ext_find_base_rel_ignore_join_is_null(root: &PlannerInfo, relid: i32) -> bool {
    match root.simple_rel_array.get(relid as usize) {
        Some(Some(_)) => false,
        Some(None) => true,
        None => panic!("no relation entry for relid {}", relid),
    }
}

/// `del_member` over a `Relids` set. relnode-seams lacks a singular del_member,
/// so delete the single member via the pathnode `del_members` seam with a
/// singleton (semantically identical).
fn relids_del_member(relids: Relids, x: i32) -> Relids {
    let singleton = bms::relids_make_singleton::call(x);
    backend_optimizer_util_pathnode_seams::relids_del_members::call(relids, &singleton)
}

/// `copyObject((List *) root->processed_tlist)` — duplicate a TargetEntry handle
/// list, deep-copying each TargetEntry node into a fresh arena handle.
fn copy_targetentry_handles(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    handles: &[NodeId],
) -> PgResult<Vec<NodeId>> {
    let mut out = Vec::with_capacity(handles.len());
    for &h in handles {
        let te = root.targetentry(h).clone();
        // Deep-copy via `clone_in` — the derived `Expr::clone` panics on an
        // owned-subtree child.
        let expr = root.node(te.expr).clone_in(mcx)?;
        let expr_id = root.alloc_node(expr);
        let new_te = types_pathnodes::TargetEntryNode {
            expr: expr_id,
            ..te
        };
        out.push(root.alloc_targetentry(new_te));
    }
    Ok(out)
}

/// Translate a TargetEntry handle list down through inheritance levels (the
/// `(List *) adjust_appendrel_attrs_multilevel(root, processed_tlist, ...)` of
/// `get_translated_update_targetlist`).
fn adjust_targetlist_multilevel(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    handles: &[NodeId],
    childrel: RelId,
    parentrel: RelId,
) -> PgResult<Vec<NodeId>> {
    let mut out = Vec::with_capacity(handles.len());
    for &h in handles {
        let te = root.targetentry(h).clone();
        // Deep-copy via `clone_in` — the derived `Expr::clone` panics on an
        // owned-subtree child.
        let expr = root.node(te.expr).clone_in(mcx)?;
        let new_expr = adjust_appendrel_attrs_multilevel(root, expr, childrel, parentrel)?;
        let expr_id = root.alloc_node(new_expr);
        let new_te = types_pathnodes::TargetEntryNode {
            expr: expr_id,
            ..te
        };
        out.push(root.alloc_targetentry(new_te));
    }
    Ok(out)
}

/// Merge `b`'s members into `a` (ExprRelids), used to fold a Var's nullingrels
/// into a translated Var. `Var.varnullingrels` is an [`ExprRelids`].
fn expr_relids_add_all(
    a: types_nodes::primnodes::ExprRelids,
    b: &types_nodes::primnodes::ExprRelids,
) -> types_nodes::primnodes::ExprRelids {
    backend_rewrite_core::relids::union(&a, b)
}

/* ==========================================================================
 * Seam installation
 * ======================================================================== */

/// Install every inward seam this unit owns.
pub fn init_seams() {
    use backend_optimizer_util_appendinfo_seams as ai;

    ai::find_appinfos_by_relids::set(find_appinfos_by_relids);
    ai::adjust_child_relids::set(adjust_child_relids);
    ai::adjust_appendrel_attrs_restrictlist::set(seam_adjust_appendrel_attrs_restrictlist);
    ai::distribute_row_identity_vars::set(distribute_row_identity_vars);
    ai::add_row_identity_columns::set(add_row_identity_columns);

    // relnode-ext-homed appendinfo seams (this unit is their C-source owner).
    relnode_ext::adjust_appendrel_attrs_node::set(seam_adjust_appendrel_attrs_node);
    relnode_ext::row_identity_var_rowidwidth::set(row_identity_var_rowidwidth);

    // equivclass-ext-homed appendinfo seams (this unit is their C-source owner).
    // The `Vec<RelId>` carrier names the child rels whose AppendRelInfos drive
    // the parent→child Var translation; resolve each through append_rel_array
    // (find_appinfos_by_relids over the child's relids).
    use backend_optimizer_path_equivclass_ext_seams as eq_ext;
    eq_ext::adjust_appendrel_attrs::set(seam_adjust_appendrel_attrs);
    eq_ext::adjust_appendrel_attrs_multilevel::set(|root, node, child_rel, top_parent| {
        let parentrel = top_parent.ok_or_else(|| {
            PgError::error("adjust_appendrel_attrs_multilevel: top_parent is NULL")
        })?;
        adjust_appendrel_attrs_multilevel(root, node, child_rel, parentrel)
    });
}

/// `adjust_appendrel_attrs(root, (Node *) node, nappinfos, appinfos)` — the
/// single-expression equivclass/allpaths consumer. The seam carries the child
/// rels as `Vec<RelId>`; resolve their AppendRelInfos and translate.
fn seam_adjust_appendrel_attrs(
    root: &mut PlannerInfo,
    node: Expr,
    child_rels: Vec<RelId>,
) -> PgResult<Expr> {
    let mut appinfos: Vec<AppendRelInfo> = Vec::with_capacity(child_rels.len());
    for cr in child_rels {
        let relids = root.rel(cr).relids.clone();
        appinfos.extend(find_appinfos_by_relids(root, &relids)?);
    }
    adjust_appendrel_attrs(root, node, &appinfos)
}

/// `(List *) adjust_appendrel_attrs(root, restrictlist, nappinfos, appinfos)` —
/// the RestrictInfo-list specialization joinrels/allpaths consume.
fn seam_adjust_appendrel_attrs_restrictlist(
    mcx: Mcx<'_>,
    root: &mut PlannerInfo,
    restrictlist: &[RinfoId],
    appinfos: &[AppendRelInfo],
) -> PgResult<Vec<RinfoId>> {
    let mut out = Vec::with_capacity(restrictlist.len());
    for &ri in restrictlist {
        out.push(adjust_restrictinfo(mcx, root, ri, appinfos)?);
    }
    Ok(out)
}

/// `adjust_appendrel_attrs(root, (Node *) node, nappinfos, appinfos)` for one
/// expression node (build_child_join_reltarget's per-expr translation).
fn seam_adjust_appendrel_attrs_node(
    root: &mut PlannerInfo,
    node: Expr,
    appinfos: &[AppendRelInfo],
) -> PgResult<Expr> {
    adjust_appendrel_attrs(root, node, appinfos)
}
