//! `backend/optimizer/prep/prepagg.c` — preprocessing of aggregate function
//! calls (transition-state sharing and cost accounting).
//!
//! 1:1 port of PostgreSQL 18.3 `prepagg.c` (695 lines) over this repo's
//! lifetime-free owned `Expr`/`Aggref` model and the `PlannerInfo` arena world.
//! All seven functions are present:
//!
//!   * the two public entry points `preprocess_aggrefs` / `get_agg_clause_costs`
//!     (`optimizer/prep.h`), declared as inward seams and installed here, plus
//!   * the file-local `preprocess_aggref`, `preprocess_aggrefs_walker`,
//!     `find_compatible_agg`, `find_compatible_trans`, and `GetAggInitVal`.
//!
//! ## Carrier model — `AggInfo.aggrefs` aliases interned `Aggref`s
//!
//! In C, `AggInfo.aggrefs` is a `List *` of *pointers* to live in-tree `Aggref`s
//! that share a state value; `preprocess_aggref` writes `aggref->aggno` /
//! `aggtransno` / `aggtranstype` back into those very nodes, and
//! `find_compatible_agg` / `get_agg_clause_costs` re-read `linitial(aggrefs)`.
//!
//! Here `PlannerInfo.agginfos` / `aggtransinfos` are `Vec<NodeId>` of handles
//! into `PlannerInfo.node_arena`
//! ([`ArenaNode::AggInfo`](::pathnodes::ArenaNode::AggInfo) /
//! [`AggTransInfo`](::pathnodes::ArenaNode::AggTransInfo)), and
//! `AggInfo.aggrefs` is a `Vec<NodeId>` of handles into the same arena
//! ([`ArenaNode::Expr`](::pathnodes::ArenaNode::Expr)`(Expr::Aggref)`). The
//! producer deep-clones each canonical `Aggref` into the arena (via
//! `Expr::clone_in` — keystone #280; a shallow `Aggref::clone()` panics on its
//! context-allocated `TargetEntry`-list children) with its `aggno` / `aggtransno`
//! / `aggtranstype` already filled, and stores the handle. Reading
//! `PlannerInfo::node` then yields the one shared `Aggref` exactly as C reads
//! the `Aggref *`. This is the carrier decision documented on
//! [`::pathnodes::AggInfo`].
//!
//! The C *also* mutates the `Aggref` in the **source** expression tree in place;
//! the walker entry here takes the clause by shared `&Expr` (it is reached from
//! the still-unported planner, which holds the live tree), so the in-place
//! source mutation is the planner driver's responsibility once it threads a
//! mutable tree. The de-dup/cost results live in the arena, which is what every
//! downstream prep/plan reader consults. No `Aggref` data is lost: the interned
//! copy is a full deep clone.
//!
//! ## Boundaries (seam-and-panic into unported owners)
//!
//! Genuinely-external reads cross focused seams: the `pg_aggregate` catalog read
//! + polymorphic transtype resolution + `GetAggInitVal` (all bundled into
//! `get_agg_catalog_info`), and `datumIsEqual` over the canonical `Datum` word
//! (`datum_is_equal`) — all owned by the syscache / aggregate-IO / datum layers,
//! declared in `backend-optimizer-prep-prepagg-seams`. The cost helpers
//! `add_function_cost` / `cost_qual_eval_walker` cross the already-declared
//! costsize seams (owner installs them when costsize lands). `equal()` over the
//! `args` / `aggorder` / `aggdistinct` / `aggdirectargs` lists crosses the
//! installed equalfuncs seams; `get_aggregate_argtypes` /
//! `agg_args_support_sendreceive` cross the installed parse_agg seams.

#![no_std]
#![allow(non_snake_case)]
// Project-wide error contract is the un-boxed `PgResult`.
#![allow(clippy::result_large_err)]
// `find_compatible_agg`'s comparison chain mirrors C's `if (a != b || ...)`.
#![allow(clippy::nonminimal_bool)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use alloc::vec::Vec;

use prepagg_seams as seam;

use ::types_core::catalog::INTERNALOID;
use ::types_core::primitive::{Oid, Size};
use ::datum::datum::Datum;
use ::types_error::PgResult;
use ::nodes::nodeagg::{
    AggSplit, AGGSPLITOP_COMBINE, AGGSPLITOP_DESERIALIZE, AGGSPLITOP_SERIALIZE, AGGSPLITOP_SKIPFINAL,
};
use ::nodes::primnodes::{Aggref, Expr};
use ::pathnodes::{
    AggClauseCosts, AggInfo, AggTransInfo, NodeId, PlannerInfo, QualCost,
};

#[cfg(test)]
extern crate std;
#[cfg(test)]
mod tests;

// ---------------------------------------------------------------------------
// Constants from PostgreSQL headers not (yet) in the types crates.
// ---------------------------------------------------------------------------

/// `AGGMODIFY_READ_WRITE` (catalog/pg_aggregate.h) — finalfn may modify state.
const AGGMODIFY_READ_WRITE: i8 = b'w' as i8;

/// `F_ARRAY_AGG_SERIALIZE` (utils/fmgroids.h, PG 18) — `array_agg_serialize`.
const F_ARRAY_AGG_SERIALIZE: Oid = 6294;
/// `F_ARRAY_AGG_DESERIALIZE` (utils/fmgroids.h, PG 18) — `array_agg_deserialize`.
const F_ARRAY_AGG_DESERIALIZE: Oid = 6295;
/// `F_ARRAY_APPEND` (utils/fmgroids.h) — `array_append`.
const F_ARRAY_APPEND: Oid = 378;

/// `ALLOCSET_SMALL_INITSIZE` (utils/memutils.h).
const ALLOCSET_SMALL_INITSIZE: Size = 1024;
/// `ALLOCSET_DEFAULT_INITSIZE` (utils/memutils.h).
const ALLOCSET_DEFAULT_INITSIZE: Size = 8 * 1024;

/// `MAXIMUM_ALIGNOF` (pg_config.h) on the supported platforms.
const MAXIMUM_ALIGNOF: i32 = 8;

/// `MAXALIGN(LEN)` (c.h) — round `len` up to `MAXIMUM_ALIGNOF`.
#[inline]
const fn MAXALIGN(len: i32) -> i32 {
    (len + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

/// `sizeof(void *)` — the per-allocation overhead term (`2 * sizeof(void*)`).
const SIZEOF_VOID_P: Size = core::mem::size_of::<usize>();

// ---------------------------------------------------------------------------
// `DO_AGGSPLIT_*` predicates (nodes/nodes.h) over the `AggSplit` bitmask.
// ---------------------------------------------------------------------------

#[inline]
fn DO_AGGSPLIT_COMBINE(as_: AggSplit) -> bool {
    (as_ & AGGSPLITOP_COMBINE) != 0
}
#[inline]
fn DO_AGGSPLIT_SERIALIZE(as_: AggSplit) -> bool {
    (as_ & AGGSPLITOP_SERIALIZE) != 0
}
#[inline]
fn DO_AGGSPLIT_DESERIALIZE(as_: AggSplit) -> bool {
    (as_ & AGGSPLITOP_DESERIALIZE) != 0
}
#[inline]
fn DO_AGGSPLIT_SKIPFINAL(as_: AggSplit) -> bool {
    (as_ & AGGSPLITOP_SKIPFINAL) != 0
}

#[inline]
fn OidIsValid(oid: Oid) -> bool {
    oid != 0
}

// ===========================================================================
// preprocess_aggrefs / preprocess_aggrefs_walker (prepagg.c:109 / :343)
// ===========================================================================

/// `preprocess_aggrefs(root, clause)` (prepagg.c:109) — walk `clause`, running
/// [`preprocess_aggref`] on every `Aggref` to set up transition-state sharing
/// and record the aggregates into `root`, AND writing the assigned
/// `aggno`/`aggtransno`/`aggtranstype` back onto the live `Aggref` node (the C
/// mutates the source tree in place; the plan's tlist Aggrefs must carry these
/// for `ExecInitAgg`).
///
/// `clause` is the live source-tree `Expr` (e.g. a `processed_tlist` entry's
/// expr or the `havingQual`), mutated in place.
pub fn preprocess_aggrefs<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    clause: &mut Expr,
) -> PgResult<()> {
    preprocess_aggrefs_walker(mcx, root, clause)?;
    Ok(())
}

/// `preprocess_aggrefs_walker(node, root)` (prepagg.c:343) — on an `Aggref`,
/// run [`preprocess_aggref`] (which catalogs the agg and returns its assigned
/// numbers), write those numbers back onto the live node, and stop descending
/// (the parser guaranteed no nested aggregates in the args/direct-args/filter);
/// else recurse into the node's `Expr` children in place.
fn preprocess_aggrefs_walker<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    node: &mut Expr,
) -> PgResult<bool> {
    if node.is_aggref() {
        let nums = preprocess_aggref(mcx, root, node)?;
        // aggref->aggno = aggno; aggref->aggtransno = aggtransno;
        // aggref->aggtranstype = aggtranstype; (the live-node mutation C does).
        if let Some(a) = node.as_aggref_mut() {
            a.aggno = nums.aggno;
            a.aggtransno = nums.aggtransno;
            a.aggtranstype = nums.aggtranstype;
        }

        /*
         * We assume that the parser checked that there are no aggregates (of
         * this level anyway) in the aggregated arguments, direct arguments, or
         * filter clause.  Hence, we need not recurse into any of them.
         */
        return Ok(false);
    }
    // Assert(!IsA(node, SubLink));
    debug_assert!(!node.is_sublink());

    // Recurse into the node's immediate Expr children in place. (C uses
    // expression_tree_walker; here we mutate each child so the write-back lands
    // on the live tree.)
    for child in expr_children_mut(node) {
        if preprocess_aggrefs_walker(mcx, root, child)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Set `aggpresorted = true` on every `Aggref` in `node`'s tree whose `aggno`
/// is in `aggnos`. Used by `adjust_group_pathkeys_for_groupagg` (planner.c) to
/// mark the live processed-tlist / HAVING-qual `Aggref`s the executor reads,
/// mirroring C's in-place mutation of the shared `Aggref` pointers.
pub fn mark_aggrefs_presorted(node: &mut Expr, aggnos: &[i32]) {
    if let Some(a) = node.as_aggref_mut() {
        if aggnos.contains(&a.aggno) {
            a.aggpresorted = true;
        }
        // Parser guarantees no nested same-level aggregates inside an Aggref's
        // own args/filter, so do not descend.
        return;
    }
    for child in expr_children_mut(node) {
        mark_aggrefs_presorted(child, aggnos);
    }
}

/// The immediate `Expr` children of `node` that may contain `Aggref`s, as
/// mutable references — the in-place analogue of `expression_tree_walker`'s
/// child recursion for the node types a planner tlist / HAVING qual can carry.
/// (The parser guarantees no nested same-level aggregates inside an `Aggref`'s
/// own args/filter, so `Aggref` is handled by the caller and not descended.)
fn expr_children_mut<'a, 'mcx>(node: &'a mut Expr<'mcx>) -> alloc::vec::Vec<&'a mut Expr<'mcx>> {
    let mut out: alloc::vec::Vec<&'a mut Expr<'mcx>> = alloc::vec::Vec::new();
    // The child-recursion arms of `expression_tree_walker` (nodeFuncs.c), one per
    // node type the walker descends into. Mechanically transcribed from
    // expression_tree_walker_impl; childless leaf types (Var/Const/Param/
    // CaseTestExpr/SQLValueFunction/CoerceToDomainValue/SetToDefault/CurrentOfExpr/
    // NextValueExpr/MergeSupportFunc) fall through to the no-op arm. Missing an arm
    // silently strands `Aggref`s nested under that node with `aggno = -1`, which the
    // executor then indexes as `peraggs[usize::MAX]`.
    match node {
        // Aggref: the caller (`preprocess_aggrefs_walker`/`mark_aggrefs_presorted`)
        // numbers it and stops before this is reached, so this arm is normally
        // dead; kept faithful for any other walker use. `expression_tree_walker`
        // descends aggdirectargs/args/aggfilter; aggorder/aggdistinct hold
        // SortGroupClause leaves (no Expr children).
        Expr::Aggref(a) => {
            out.extend(a.aggdirectargs.iter_mut());
            out.extend(a.args.iter_mut().filter_map(|te| te.expr.as_deref_mut()));
            if let Some(f) = a.aggfilter.as_deref_mut() {
                out.push(f);
            }
        }
        Expr::GroupingFunc(g) => out.extend(g.args.iter_mut()),
        // A WindowFunc's argument (and FILTER) may carry a plain Aggref, as in
        // `SUM(SUM(x)) OVER (...)`: the inner `SUM(x)` is an ordinary aggregate
        // computed by the Agg node feeding the WindowAgg, and it must be numbered
        // here (else `aggno` stays -1 and `ExecInitAgg` indexes `peraggs[-1]`).
        Expr::WindowFunc(w) => {
            out.extend(w.args.iter_mut());
            if let Some(f) = w.aggfilter.as_deref_mut() {
                out.push(f);
            }
            out.extend(w.runCondition.iter_mut());
        }
        Expr::SubscriptingRef(s) => {
            out.extend(s.refupperindexpr.iter_mut().filter_map(|e| e.as_mut()));
            out.extend(s.reflowerindexpr.iter_mut().filter_map(|e| e.as_mut()));
            if let Some(e) = s.refexpr.as_deref_mut() {
                out.push(e);
            }
            if let Some(e) = s.refassgnexpr.as_deref_mut() {
                out.push(e);
            }
        }
        Expr::FuncExpr(f) => out.extend(f.args.iter_mut()),
        Expr::NamedArgExpr(n) => {
            if let Some(arg) = n.arg.as_deref_mut() {
                out.push(arg);
            }
        }
        Expr::OpExpr(o) | Expr::DistinctExpr(o) | Expr::NullIfExpr(o) => {
            out.extend(o.args.iter_mut())
        }
        Expr::ScalarArrayOpExpr(s) => out.extend(s.args.iter_mut()),
        Expr::BoolExpr(b) => out.extend(b.args.iter_mut()),
        // `expression_tree_walker` descends into a SubLink's `testexpr` (which is
        // this query level's expression tree and may carry an Aggref, e.g.
        // `agg(x) = ANY (SELECT ...)`); the `subselect` Query is not descended (our
        // walker has no Query arm, matching C's no-op Query case here).
        Expr::SubLink(sl) => {
            if let Some(t) = sl.testexpr.as_deref_mut() {
                out.push(t);
            }
        }
        // SubPlan/AlternativeSubPlan do not occur in a tlist at preprocess_aggrefs
        // time (sublinks become subplans during path/plan generation), but the
        // walker covers them for completeness: testexpr + args, never the Plan.
        Expr::SubPlan(sp) => {
            let sp = &mut *sp.0;
            if let Some(t) = sp.testexpr.as_deref_mut() {
                out.push(t);
            }
            for a in sp.args.iter_mut() {
                out.push(&mut **a);
            }
        }
        Expr::AlternativeSubPlan(asp) => {
            for sp in asp.0.subplans.iter_mut() {
                let sp = &mut **sp;
                if let Some(t) = sp.testexpr.as_deref_mut() {
                    out.push(t);
                }
                for a in sp.args.iter_mut() {
                    out.push(&mut **a);
                }
            }
        }
        Expr::FieldSelect(f) => {
            if let Some(arg) = f.arg.as_deref_mut() {
                out.push(arg);
            }
        }
        Expr::FieldStore(f) => {
            if let Some(arg) = f.arg.as_deref_mut() {
                out.push(arg);
            }
            out.extend(f.newvals.iter_mut());
        }
        Expr::RelabelType(r) => {
            if let Some(arg) = r.arg.as_deref_mut() {
                out.push(arg);
            }
        }
        Expr::CoerceViaIO(c) => {
            if let Some(arg) = c.arg.as_deref_mut() {
                out.push(arg);
            }
        }
        Expr::ArrayCoerceExpr(a) => {
            if let Some(arg) = a.arg.as_deref_mut() {
                out.push(arg);
            }
            if let Some(e) = a.elemexpr.as_deref_mut() {
                out.push(e);
            }
        }
        Expr::ConvertRowtypeExpr(c) => {
            if let Some(arg) = c.arg.as_deref_mut() {
                out.push(arg);
            }
        }
        Expr::CollateExpr(c) => {
            if let Some(arg) = c.arg.as_deref_mut() {
                out.push(arg);
            }
        }
        Expr::CaseExpr(c) => {
            if let Some(arg) = c.arg.as_deref_mut() {
                out.push(arg);
            }
            for w in c.args.iter_mut() {
                if let Some(e) = w.expr.as_deref_mut() {
                    out.push(e);
                }
                if let Some(r) = w.result.as_deref_mut() {
                    out.push(r);
                }
            }
            if let Some(d) = c.defresult.as_deref_mut() {
                out.push(d);
            }
        }
        Expr::ArrayExpr(a) => out.extend(a.elements.iter_mut()),
        Expr::RowExpr(r) => out.extend(r.args.iter_mut()),
        Expr::RowCompareExpr(r) => {
            out.extend(r.largs.iter_mut());
            out.extend(r.rargs.iter_mut());
        }
        Expr::CoalesceExpr(c) => out.extend(c.args.iter_mut()),
        Expr::MinMaxExpr(m) => out.extend(m.args.iter_mut()),
        Expr::XmlExpr(x) => {
            out.extend(x.named_args.iter_mut());
            out.extend(x.args.iter_mut());
        }
        Expr::JsonValueExpr(jve) => {
            if let Some(r) = jve.raw_expr.as_deref_mut() {
                out.push(r);
            }
            if let Some(f) = jve.formatted_expr.as_deref_mut() {
                out.push(f);
            }
        }
        // SQL/JSON constructor (`JSON_OBJECT`/`JSON_ARRAY`/`JSON_OBJECTAGG`/
        // `JSON_ARRAYAGG`/…): for the AGG constructor forms the underlying `Aggref`
        // rides `func`, so this arm is what lets `preprocess_aggref` number that
        // nested aggregate.
        Expr::JsonConstructorExpr(ctor) => {
            out.extend(ctor.args.iter_mut());
            if let Some(f) = ctor.func.as_deref_mut() {
                out.push(f);
            }
            if let Some(co) = ctor.coercion.as_deref_mut() {
                out.push(co);
            }
        }
        Expr::JsonIsPredicate(j) => {
            if let Some(e) = j.expr.as_deref_mut() {
                out.push(e);
            }
        }
        Expr::JsonExpr(j) => {
            if let Some(e) = j.formatted_expr.as_deref_mut() {
                out.push(e);
            }
            if let Some(p) = j.path_spec.as_deref_mut() {
                out.push(p);
            }
            out.extend(j.passing_values.iter_mut());
            if let Some(b) = j.on_empty.as_deref_mut() {
                if let Some(e) = b.expr.as_deref_mut() {
                    out.push(e);
                }
            }
            if let Some(b) = j.on_error.as_deref_mut() {
                if let Some(e) = b.expr.as_deref_mut() {
                    out.push(e);
                }
            }
        }
        Expr::NullTest(n) => {
            if let Some(arg) = n.arg.as_deref_mut() {
                out.push(arg);
            }
        }
        Expr::BooleanTest(b) => {
            if let Some(arg) = b.arg.as_deref_mut() {
                out.push(arg);
            }
        }
        Expr::CoerceToDomain(c) => {
            if let Some(arg) = c.arg.as_deref_mut() {
                out.push(arg);
            }
        }
        Expr::ReturningExpr(r) => {
            if let Some(e) = r.retexpr.as_deref_mut() {
                out.push(e);
            }
        }
        _ => {}
    }
    out
}

/// `preprocess_aggref(aggref, root)` (prepagg.c:115) — the per-aggregate de-dup
/// / state-sharing decision. Resolves the transition type, computes `aggno` /
/// `aggtransno`, accumulates `AggInfo`/`AggTransInfo` into the arena, and stores
/// the resulting (fully-filled) `Aggref` as an interned arena node referenced by
/// `AggInfo.aggrefs`.
fn preprocess_aggref<'mcx>(
    mcx: mcx::Mcx<'mcx>,
    root: &mut PlannerInfo,
    aggref_node: &Expr,
) -> PgResult<AggrefNumbers> {
    debug_assert!(aggref_node.is_aggref());

    // Working copy of the node (the C mutates the live `Aggref` in place; here the
    // arena copy this becomes is the canonical shared node — see the crate docs).
    // A shallow `Aggref::clone()` panics on the context-allocated children, so
    // deep-clone the whole `Expr::Aggref` node via `Expr::clone_in` (keystone
    // #280). Keep it as an `Expr` so it can be re-wrapped for the
    // `contain_volatile_functions` walk and interned by value at the end.
    let mut working: Expr = aggref_node.clone_in(mcx)?;
    debug_assert!(working.is_aggref());

    // Assert(aggref->agglevelsup == 0);
    debug_assert!(as_aggref(&working).agglevelsup == 0);

    /* extract argument types (ignoring any ORDER BY expressions) */
    // get_aggregate_argtypes(aggref, inputTypes) — parse_agg.c (installed seam).
    let input_types =
        parse_agg_seams::get_aggregate_argtypes::call(mcx, as_aggref(&working))?;
    let input_types: &[Oid] = &input_types;

    /*
     * Fetch info about the aggregate from pg_aggregate (tuple pinned across the
     * reads) + resolve the possibly-polymorphic transition type.
     */
    let aggform =
        seam::get_agg_catalog_info::call(mcx, as_aggref(&working).aggfnoid, input_types)?;

    let aggtransfn = aggform.aggtransfn;
    let aggfinalfn = aggform.aggfinalfn;
    let aggcombinefn = aggform.aggcombinefn;
    let aggserialfn = aggform.aggserialfn;
    let aggdeserialfn = aggform.aggdeserialfn;
    let aggtransspace = aggform.aggtransspace;
    let aggtranstype = aggform.aggtranstype;
    as_aggref_mut(&mut working).aggtranstype = aggtranstype;

    /*
     * If transition state is of same type as first aggregated input, assume
     * it's the same typmod (same width) as well.  This works for cases like
     * MAX/MIN and is probably somewhat reasonable otherwise.
     */
    let mut aggtranstypmod: i32 = -1;
    {
        let aggref = as_aggref(&working);
        if !aggref.args.is_empty() {
            // ((TargetEntry *) linitial(aggref->args))->expr
            let tle = &aggref.args[0];
            let expr = tle.expr.as_deref();
            if aggtranstype == nodes_core::nodefuncs::expr_type(expr)? {
                aggtranstypmod = nodes_core::nodefuncs::expr_typmod(expr)?;
            }
        }
    }

    /*
     * If finalfn is marked read-write, we can't share transition states; but it
     * is okay to share states for AGGMODIFY_SHAREABLE aggs.
     */
    let shareable = aggform.aggfinalmodify != AGGMODIFY_READ_WRITE;

    /* get info about the output value's datatype */
    // get_typlenbyval(aggref->aggtype, ...) — results unused by prepagg.c (it
    // calls only for the side-effect of a valid-type check), so discard them.
    let _ = lsyscache::type_::get_typlenbyval(as_aggref(&working).aggtype)?;

    /* get initial value */
    // The catalog owner already ran GetAggInitVal (getTypeInputInfo +
    // OidInputFunctionCall) while the AGGFNOID tuple was pinned, with the
    // resolved aggtranstype, and allocated the result in `mcx`. So
    // `aggform.agginitval` is the resolved transition Datum directly (C does
    // this inline in preprocess_aggref).
    let initValueIsNull = aggform.agginitval_isnull;
    let initValue: Datum = if initValueIsNull {
        Datum::null()
    } else {
        aggform.agginitval
    };
    // Flat byte image of a by-reference init value (None for by-value or NULL);
    // carries the bytes C's `datumIsEqual` dereferences (see `AggCatalogInfo`).
    let initValueImage: Option<Vec<u8>> = if initValueIsNull {
        None
    } else {
        aggform.agginitval_image.clone()
    };

    /*
     * 1. See if this is identical to another aggregate function call that we've
     * seen already.
     */
    let mut same_input_transnos: Vec<i32> = Vec::new();
    let aggno = find_compatible_agg(root, &working, &mut same_input_transnos)?;
    let transno: i32;
    if aggno != -1 {
        // agginfo = list_nth_node(AggInfo, root->agginfos, aggno);
        let agginfo_id = root.agginfos[aggno as usize];
        transno = root.agg_info(agginfo_id).transno;
        // Fill in the Aggref fields on the canonical copy before interning it
        // (aggtranstype was set above; aggno/aggtransno here) — prepagg.c:339.
        {
            let aggref = as_aggref_mut(&mut working);
            aggref.aggno = aggno;
            aggref.aggtransno = transno;
        }
        // agginfo->aggrefs = lappend(agginfo->aggrefs, aggref);
        let aggref_id = root.alloc_node(working);
        root.agg_info_mut(agginfo_id).aggrefs.push(aggref_id);
    } else {
        // makeNode(AggInfo)
        let mut agginfo = AggInfo {
            finalfn_oid: aggfinalfn,
            // agginfo->aggrefs = list_make1(aggref);  (interned below)
            aggrefs: Vec::new(),
            shareable,
            transno: 0,
        };

        let aggno_new = root.agginfos.len() as i32;

        /*
         * Count it, and check for cases requiring ordered input.  Note that
         * ordered-set aggs always have nonempty aggorder.  Any ordered-input
         * case also defeats partial aggregation.
         */
        if !as_aggref(&working).aggorder.is_empty()
            || !as_aggref(&working).aggdistinct.is_empty()
        {
            root.numOrderedAggs += 1;
            root.hasNonPartialAggs = true;
        }

        let (transtypeLen, transtypeByVal) =
            lsyscache::type_::get_typlenbyval(aggtranstype)?;

        /*
         * 2. See if this aggregate can share transition state with another
         * aggregate that we've initialized already.
         */
        let mut transno_found = find_compatible_trans(
            root,
            shareable,
            aggtransfn,
            aggtranstype,
            transtypeLen as i32,
            transtypeByVal,
            aggcombinefn,
            aggserialfn,
            aggdeserialfn,
            initValue,
            initValueIsNull,
            initValueImage.as_deref(),
            &same_input_transnos,
        )?;
        if transno_found == -1 {
            // makeNode(AggTransInfo)
            //
            // transinfo->args = aggref->args;  transinfo->aggfilter =
            // aggref->aggfilter;  carried as arena handles (deep-cloned TLEs /
            // Expr) in the same id-space as the rest of the planner nodes.
            let mut args_ids: Vec<NodeId> = Vec::with_capacity(as_aggref(&working).args.len());
            // Clone each TargetEntry into the arena. (Resolve the TLE refs anew
            // each iteration to avoid holding a `&working` borrow across the
            // `&mut root` arena allocation.)
            let n_args = as_aggref(&working).args.len();
            for i in 0..n_args {
                let te_node = {
                    let tle = &as_aggref(&working).args[i];
                    clone_targetentry_into_arena(root, tle, mcx)?
                };
                args_ids.push(te_node);
            }
            let aggfilter_id: Option<NodeId> = match as_aggref(&working).aggfilter.as_deref() {
                Some(f) => {
                    let cloned = f.clone_in(mcx)?;
                    Some(root.alloc_node(cloned))
                }
                None => None,
            };

            let transinfo = AggTransInfo {
                args: args_ids,
                aggfilter: aggfilter_id,
                transfn_oid: aggtransfn,
                combinefn_oid: aggcombinefn,
                serialfn_oid: aggserialfn,
                deserialfn_oid: aggdeserialfn,
                aggtranstype,
                aggtranstypmod,
                transtypeLen: transtypeLen as i32,
                transtypeByVal,
                aggtransspace,
                initValue,
                initValueIsNull,
                initValueImage,
            };

            transno_found = root.aggtransinfos.len() as i32;
            let transinfo_id = root.alloc_agg_trans_info(transinfo);
            root.aggtransinfos.push(transinfo_id);

            /*
             * Check whether partial aggregation is feasible, unless we already
             * found out that we can't do it.
             */
            if !root.hasNonPartialAggs {
                let pertrans = root.agg_trans_info(transinfo_id);
                let combinefn_oid = pertrans.combinefn_oid;
                let pt_aggtranstype = pertrans.aggtranstype;
                let pt_serialfn = pertrans.serialfn_oid;
                let pt_deserialfn = pertrans.deserialfn_oid;

                /*
                 * If there is no combine function, then partial aggregation is
                 * not possible.
                 */
                if !OidIsValid(combinefn_oid) {
                    root.hasNonPartialAggs = true;
                }
                /*
                 * If we have any aggs with transtype INTERNAL then we must check
                 * whether they have serialization/deserialization functions; if
                 * not, we can't serialize partial-aggregation results.
                 */
                else if pt_aggtranstype == INTERNALOID {
                    if !OidIsValid(pt_serialfn) || !OidIsValid(pt_deserialfn) {
                        root.hasNonSerialAggs = true;
                    }

                    /*
                     * array_agg_serialize / array_agg_deserialize use the
                     * aggregated non-byval input type's send/receive functions,
                     * which may be missing; if so we must not allow the
                     * aggregate's serial/deserial functions to be used.
                     */
                    if pt_serialfn == F_ARRAY_AGG_SERIALIZE
                        || pt_deserialfn == F_ARRAY_AGG_DESERIALIZE
                    {
                        let supported =
                            parse_agg_seams::agg_args_support_sendreceive::call(
                                as_aggref(&working),
                            )?;
                        if !supported {
                            root.hasNonSerialAggs = true;
                        }
                    }
                }
            }
        }
        agginfo.transno = transno_found;
        transno = transno_found;

        // Fill in the Aggref fields on the canonical copy before interning it
        // (aggtranstype was set above; aggno is this new entry's index) —
        // prepagg.c:339.
        {
            let aggref = as_aggref_mut(&mut working);
            aggref.aggno = aggno_new;
            aggref.aggtransno = transno;
        }

        // agginfo->aggrefs = list_make1(aggref): intern the canonical Aggref and
        // store its handle.
        let aggref_id = root.alloc_node(working);
        agginfo.aggrefs.push(aggref_id);

        // root->agginfos = lappend(root->agginfos, agginfo);
        let agginfo_id = root.alloc_agg_info(agginfo);
        root.agginfos.push(agginfo_id);
        debug_assert!(aggno_new == (root.agginfos.len() as i32) - 1);
    }

    /*
     * The Aggref's aggno/aggtransno/aggtranstype have been filled in on the
     * canonical interned copy above (the arena form `AggInfo.aggrefs` references,
     * which every downstream prep/plan reader consults). The C also writes them
     * back into the live source-tree node in place; we return the assigned
     * numbers so the caller (`preprocess_aggrefs_walker`) can mutate the live
     * tlist/havingQual `Aggref` node — without this, the plan's tlist Aggrefs
     * keep aggno = aggtransno = -1, and `ExecInitAgg` (which sizes peraggs from
     * `max(aggno)+1`) reads a -1 index.
     */
    Ok(AggrefNumbers {
        aggno: match aggno {
            -1 => {
                // aggno was -1 from find_compatible_agg → a new agginfo was made;
                // its index is the one just appended.
                (root.agginfos.len() as i32) - 1
            }
            n => n,
        },
        aggtransno: transno,
        aggtranstype,
    })
}

/// The planner-visible numbers `preprocess_aggref` assigns to an `Aggref`
/// (`aggref->aggno`, `aggref->aggtransno`, `aggref->aggtranstype`), returned so
/// the caller can write them onto the live source-tree node (C mutates the
/// `Aggref` in place; here the live node is a distinct arena copy).
#[derive(Clone, Copy)]
struct AggrefNumbers {
    aggno: i32,
    aggtransno: i32,
    aggtranstype: Oid,
}

// ===========================================================================
// find_compatible_agg (prepagg.c:378)
// ===========================================================================

/// `find_compatible_agg(root, newagg, &same_input_transnos)` (prepagg.c:378) —
/// search the previously-seen aggregates for one with the same inputs; return
/// its `aggno` (or -1). Side effect: collect the transnos of existing shareable
/// aggs with matching inputs into `same_input_transnos`.
fn find_compatible_agg(
    root: &PlannerInfo,
    newagg_node: &Expr,
    same_input_transnos: &mut Vec<i32>,
) -> PgResult<i32> {
    same_input_transnos.clear();

    let newagg = as_aggref(newagg_node);

    /* we mustn't reuse the aggref if it contains volatile function calls */
    // contain_volatile_functions((Node *) newagg)
    if clauses::contain_volatile_functions(Some(newagg_node))? {
        return Ok(-1);
    }

    /*
     * Search through the list of already seen aggregates.  ...
     */
    let mut aggno: i32 = -1;
    for &agginfo_id in root.agginfos.iter() {
        aggno += 1;

        let agginfo = root.agg_info(agginfo_id);
        // existingRef = linitial(agginfo->aggrefs);
        let existing_ref = root.node(agginfo.aggrefs[0]).expect_aggref();

        /* all of the following must be the same or it's no match */
        if newagg.inputcollid != existing_ref.inputcollid
            || newagg.aggtranstype != existing_ref.aggtranstype
            || newagg.aggstar != existing_ref.aggstar
            || newagg.aggvariadic != existing_ref.aggvariadic
            || newagg.aggkind != existing_ref.aggkind
            || !equalfuncs_seams::equal_targetentry_list::call(
                &newagg.args,
                &existing_ref.args,
            )
            || !equalfuncs_seams::equal_sortgroupclause_list::call(
                &newagg.aggorder,
                &existing_ref.aggorder,
            )
            || !equalfuncs_seams::equal_sortgroupclause_list::call(
                &newagg.aggdistinct,
                &existing_ref.aggdistinct,
            )
            || !equal_opt_expr(
                newagg.aggfilter.as_deref(),
                existing_ref.aggfilter.as_deref(),
            )
        {
            continue;
        }

        /* if it's the same aggregate function then report exact match */
        if newagg.aggfnoid == existing_ref.aggfnoid
            && newagg.aggtype == existing_ref.aggtype
            && newagg.aggcollid == existing_ref.aggcollid
            && equalfuncs_seams::equal_expr_list::call(
                &newagg.aggdirectargs,
                &existing_ref.aggdirectargs,
            )
        {
            // list_free(*same_input_transnos); *same_input_transnos = NIL;
            same_input_transnos.clear();
            return Ok(aggno);
        }

        /*
         * Not identical, but it had the same inputs.  If the final function
         * permits sharing, return its transno to the caller.
         */
        if agginfo.shareable {
            // *same_input_transnos = lappend_int(*same_input_transnos, agginfo->transno);
            same_input_transnos.push(agginfo.transno);
        }
    }

    Ok(-1)
}

// ===========================================================================
// find_compatible_trans (prepagg.c:456)
// ===========================================================================

/// `find_compatible_trans(...)` (prepagg.c:456) — search the candidate transnos
/// for a per-Trans struct with the same transition function and initial
/// condition. Returns the matching transno or -1.
fn find_compatible_trans(
    root: &PlannerInfo,
    shareable: bool,
    aggtransfn: Oid,
    aggtranstype: Oid,
    transtypeLen: i32,
    transtypeByVal: bool,
    aggcombinefn: Oid,
    aggserialfn: Oid,
    aggdeserialfn: Oid,
    initValue: Datum,
    initValueIsNull: bool,
    initValueImage: Option<&[u8]>,
    transnos: &[i32],
) -> PgResult<i32> {
    /* If this aggregate can't share transition states, give up */
    if !shareable {
        return Ok(-1);
    }

    for &transno in transnos.iter() {
        // pertrans = list_nth_node(AggTransInfo, root->aggtransinfos, transno);
        let pertrans = root.agg_trans_info(root.aggtransinfos[transno as usize]);

        /*
         * if the transfns or transition state types are not the same then the
         * state can't be shared.
         */
        if aggtransfn != pertrans.transfn_oid || aggtranstype != pertrans.aggtranstype {
            continue;
        }

        /*
         * The serialization and deserialization functions must match, if
         * present, ...
         */
        if aggserialfn != pertrans.serialfn_oid || aggdeserialfn != pertrans.deserialfn_oid {
            continue;
        }

        /*
         * Combine function must also match.
         */
        if aggcombinefn != pertrans.combinefn_oid {
            continue;
        }

        /*
         * Check that the initial condition matches, too.
         */
        if initValueIsNull && pertrans.initValueIsNull {
            return Ok(transno);
        }

        // C compares the two init values with `datumIsEqual(v1, v2, byVal,
        // len)`. For a by-value transtype we compare the bare `Datum` words
        // through the datum-layer seam. For a by-reference transtype the bare
        // word is a `0` placeholder (the lifetime-free `AggTransInfo` cannot
        // carry the by-ref word), but we carry the flat varlena byte image
        // (`initValueImage`) instead — and `datumIsEqual` for a by-ref value is
        // a memcmp of the VARSIZE bytes, i.e. exactly a byte-equality of those
        // flat images. So compare the images directly here.
        if !initValueIsNull && !pertrans.initValueIsNull {
            let equal = if transtypeByVal {
                seam::datum_is_equal::call(
                    initValue,
                    pertrans.initValue,
                    transtypeByVal,
                    transtypeLen,
                )?
            } else {
                match (initValueImage, pertrans.initValueImage.as_deref()) {
                    (Some(a), Some(b)) => a == b,
                    // If either image is missing we cannot prove equality;
                    // decline to share (conservative — never merges distinct
                    // transition states).
                    _ => false,
                }
            };
            if equal {
                return Ok(transno);
            }
        }
    }
    Ok(-1)
}

// ===========================================================================
// GetAggInitVal (prepagg.c:520)
// ===========================================================================
//
// `GetAggInitVal(textInitVal, transtype)` (getTypeInputInfo +
// TextDatumGetCString + OidInputFunctionCall) is folded into the
// `get_agg_catalog_info` boundary seam: the catalog owner runs it while the
// AGGFNOID tuple is pinned (with the resolved aggtranstype) and returns the
// resolved init-value Datum directly. See `preprocess_aggref` above.

// ===========================================================================
// get_agg_clause_costs (prepagg.c:558)
// ===========================================================================

/// `get_agg_clause_costs(root, aggsplit, costs)` (prepagg.c:558) — accumulate
/// the planned aggregates' execution-cost estimates into `*costs` for the given
/// split mode. NOTE that the costs are ADDED to those already in `costs`, so the
/// caller is responsible for zeroing the struct initially.
pub fn get_agg_clause_costs(
    root: &PlannerInfo,
    aggsplit: AggSplit,
    costs: &mut AggClauseCosts,
) -> PgResult<()> {
    for &transinfo_id in root.aggtransinfos.iter() {
        let transinfo = root.agg_trans_info(transinfo_id);
        let transfn_oid = transinfo.transfn_oid;
        let combinefn_oid = transinfo.combinefn_oid;
        let deserialfn_oid = transinfo.deserialfn_oid;
        let serialfn_oid = transinfo.serialfn_oid;
        let transtypeByVal = transinfo.transtypeByVal;
        let aggtransspace = transinfo.aggtransspace;
        let aggtranstype = transinfo.aggtranstype;
        let aggtranstypmod = transinfo.aggtranstypmod;

        /*
         * Add the appropriate component function execution costs to appropriate
         * totals.
         */
        if DO_AGGSPLIT_COMBINE(aggsplit) {
            /* charge for combining previously aggregated states */
            add_function_cost(root, combinefn_oid, &mut costs.transCost);
        } else {
            add_function_cost(root, transfn_oid, &mut costs.transCost);
        }
        if DO_AGGSPLIT_DESERIALIZE(aggsplit) && OidIsValid(deserialfn_oid) {
            add_function_cost(root, deserialfn_oid, &mut costs.transCost);
        }
        if DO_AGGSPLIT_SERIALIZE(aggsplit) && OidIsValid(serialfn_oid) {
            add_function_cost(root, serialfn_oid, &mut costs.finalCost);
        }

        /*
         * These costs are incurred only by the initial aggregate node, so we
         * mustn't include them again at upper levels.
         */
        if !DO_AGGSPLIT_COMBINE(aggsplit) {
            /* add the input expressions' cost to per-input-row costs */
            // cost_qual_eval_node(&argcosts, (Node *) transinfo->args, root);
            let argcosts = cost_qual_eval_targetentry_list(root, &transinfo.args);
            costs.transCost.startup += argcosts.startup;
            costs.transCost.per_tuple += argcosts.per_tuple;

            /*
             * Add any filter's cost to per-input-row costs.
             */
            if let Some(aggfilter) = transinfo.aggfilter {
                let argcosts = cost_qual_eval_one(root, aggfilter);
                costs.transCost.startup += argcosts.startup;
                costs.transCost.per_tuple += argcosts.per_tuple;
            }
        }

        /*
         * If the transition type is pass-by-value then it doesn't add anything
         * to the required size of the hashtable.  If it is pass-by-reference
         * then we have to add the estimated size of the value itself, plus
         * palloc overhead.
         */
        if !transtypeByVal {
            let mut avgwidth: i32;

            /* Use average width if aggregate definition gave one */
            if aggtransspace > 0 {
                avgwidth = aggtransspace;
            } else if transfn_oid == F_ARRAY_APPEND {
                /*
                 * If the transition function is array_append(), it'll use an
                 * expanded array as transvalue, which will occupy at least
                 * ALLOCSET_SMALL_INITSIZE and possibly more.
                 */
                avgwidth = ALLOCSET_SMALL_INITSIZE as i32;
            } else {
                avgwidth =
                    lsyscache::type_::get_typavgwidth(aggtranstype, aggtranstypmod)?;
            }

            avgwidth = MAXALIGN(avgwidth);
            costs.transitionSpace += avgwidth as Size + 2 * SIZEOF_VOID_P;
        } else if aggtranstype == INTERNALOID {
            /*
             * INTERNAL transition type is a special case: although INTERNAL is
             * pass-by-value, it's almost certainly being used as a pointer to
             * some large data structure.
             */
            if aggtransspace > 0 {
                costs.transitionSpace += aggtransspace as Size;
            } else {
                costs.transitionSpace += ALLOCSET_DEFAULT_INITSIZE;
            }
        }
    }

    for &agginfo_id in root.agginfos.iter() {
        let agginfo = root.agg_info(agginfo_id);
        let finalfn_oid = agginfo.finalfn_oid;
        // aggref = linitial(agginfo->aggrefs);
        let aggref = root.node(agginfo.aggrefs[0]).expect_aggref();

        /*
         * Add the appropriate component function execution costs to appropriate
         * totals.
         */
        if !DO_AGGSPLIT_SKIPFINAL(aggsplit) && OidIsValid(finalfn_oid) {
            add_function_cost(root, finalfn_oid, &mut costs.finalCost);
        }

        /*
         * If there are direct arguments, treat their evaluation cost like the
         * cost of the finalfn.
         */
        if !aggref.aggdirectargs.is_empty() {
            // cost_qual_eval_node(&argcosts, (Node *) aggref->aggdirectargs, root).
            // aggdirectargs are inline `Expr`s in the Aggref, not arena handles;
            // the C costs them via the same walker. Collect the cost over the
            // owned exprs (clone each into the arena to obtain a NodeId the
            // walker seam can dereference — behaviour-preserving, no mutation).
            let argcosts = cost_qual_eval_inline_exprs(root, &aggref.aggdirectargs);
            costs.finalCost.startup += argcosts.startup;
            costs.finalCost.per_tuple += argcosts.per_tuple;
        }
    }

    Ok(())
}

// ===========================================================================
// Cost helpers (costsize.c seams)
// ===========================================================================

/// `add_function_cost(root, funcid, NULL, cost)` (costsize.c) — accumulate the
/// function's `procost` into `cost` (prepagg always passes node == NULL).
#[inline]
fn add_function_cost(root: &PlannerInfo, funcid: Oid, cost: &mut QualCost) {
    let (startup, per_tuple) =
        costsize_seams::add_function_cost::call(Some(root), funcid, None);
    cost.startup += startup;
    cost.per_tuple += per_tuple;
}

/// `cost_qual_eval_node(&cost, (Node *) node, root)` over one arena node handle
/// — the costsize single-node walker.
#[inline]
fn cost_qual_eval_one(root: &PlannerInfo, node: NodeId) -> QualCost {
    let (startup, per_tuple) =
        costsize_seams::cost_qual_eval_walker::call(root, node);
    QualCost { startup, per_tuple }
}

/// `cost_qual_eval_node(&cost, (Node *) transinfo->args, root)` over the
/// `AggTransInfo.args` `TargetEntry` list (arena handles). `cost_qual_eval`'s
/// `List` arm sums `cost_qual_eval_walker` over each element; a `TargetEntry`'s
/// cost is the cost of its `expr`. The walker seam addresses nodes by arena
/// handle, so each TLE handle is resolved to its `expr` handle first (the
/// `TargetEntry` wrapper itself adds no cost).
fn cost_qual_eval_targetentry_list(root: &PlannerInfo, list: &[NodeId]) -> QualCost {
    let mut total = QualCost::default();
    for &te_id in list.iter() {
        let expr_id = root.targetentry(te_id).expr;
        let c = cost_qual_eval_one(root, expr_id);
        total.startup += c.startup;
        total.per_tuple += c.per_tuple;
    }
    total
}

/// `cost_qual_eval_node(&cost, (Node *) aggref->aggdirectargs, root)` over the
/// inline `Expr` list. `aggdirectargs` are inline `Expr`s in the `Aggref` (not
/// arena handles), so they cross the by-value costsize seam (the same single-node
/// walker, value form) rather than being re-interned through a shared `&root`.
fn cost_qual_eval_inline_exprs(root: &PlannerInfo, list: &[Expr]) -> QualCost {
    let mut total = QualCost::default();
    for expr in list.iter() {
        let (startup, per_tuple) =
            joininfo_ext_seams::cost_qual_eval_node_expr::call(root, expr);
        total.startup += startup;
        total.per_tuple += per_tuple;
    }
    total
}

// ===========================================================================
// equal() / clone helpers
// ===========================================================================

/// `equal(a, b)` for two optional expression nodes (NULL ↔ NULL is equal).
fn equal_opt_expr(a: Option<&Expr>, b: Option<&Expr>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => equalfuncs_seams::equal_expr::call(a, b),
        _ => false,
    }
}

/// View an `Expr::Aggref` node as `&Aggref` (the working node is always an
/// `Aggref` — built by `clone_in` of an `Expr::Aggref`).
#[inline]
fn as_aggref<'a, 'mcx>(node: &'a Expr<'mcx>) -> &'a Aggref<'mcx> {
    node.expect_aggref()
}

/// View an `Expr::Aggref` node as `&mut Aggref`.
#[inline]
fn as_aggref_mut<'a, 'mcx>(node: &'a mut Expr<'mcx>) -> &'a mut Aggref<'mcx> {
    node.as_aggref_mut()
        .expect("prepagg working node is always Expr::Aggref")
}

/// Deep-clone a `TargetEntry` into the arena, returning its `NodeId` handle (the
/// `AggTransInfo.args` element form). Mirrors how `processed_tlist` interns TLEs.
fn clone_targetentry_into_arena<'mcx>(
    root: &mut PlannerInfo,
    tle: &::nodes::primnodes::TargetEntry<'_>,
    mcx: mcx::Mcx<'mcx>,
) -> PgResult<NodeId> {
    let expr_src = tle.expr.as_deref().expect(
        "prepagg: TargetEntry with NULL expr in Aggref.args (parser bug)",
    );
    let expr_clone = expr_src.clone_in(mcx)?;
    let expr_id = root.alloc_node(expr_clone);
    let te_node = ::pathnodes::TargetEntryNode {
        expr: expr_id,
        resno: tle.resno,
        resname: tle
            .resname
            .as_ref()
            .map(|s| alloc::string::String::from(s.as_str())),
        ressortgroupref: tle.ressortgroupref,
        resorigtbl: tle.resorigtbl,
        resorigcol: tle.resorigcol,
        resjunk: tle.resjunk,
    };
    Ok(root.alloc_targetentry(te_node))
}

// ===========================================================================
// seam wiring
// ===========================================================================

/// Install the seams this unit owns. Wired into the central init sequence.
pub fn init_seams() {
    seam::preprocess_aggrefs::set(preprocess_aggrefs);
    seam::get_agg_clause_costs::set(get_agg_clause_costs);
}
