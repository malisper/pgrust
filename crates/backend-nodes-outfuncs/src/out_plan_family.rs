//! `_out<Type>` writers for the out_plan_family node arms (the plannodes.h plan
//! / scan / join node family). Each writer mirrors its `outfuncs.funcs.c` body
//! field-for-field, with EXACT C token names (including the dotted supertype
//! prefix, e.g. `:scan.plan.targetlist`). `try_out` returns `true` iff it
//! claimed and wrote `node`.
//!
//! ## Flattened supertype emission
//!
//! C inlines the `Plan` / `Scan` / `Join` supertype fields with a dotted token
//! prefix per node (e.g. `_outSeqScan` writes `:scan.plan.disabled_nodes`,
//! `_outAppend` writes `:plan.disabled_nodes`). [`out_plan_fields`] /
//! [`out_scan_fields`] / [`out_join_fields`] take the literal prefix so each
//! node passes the exact C string.
//!
//! ## Field-type coverage
//!
//! A handful of nodes carry a child whose type this family cannot serialize
//! field-for-field (a sub-struct that is not reachable as a `Node` here, or a
//! supertype field the repo struct trims). Those nodes `mirror-pg-and-panic`
//! with a precise reason rather than emit a corrupt partial dump.

use alloc::string::String;
use core::fmt::Write as _;

use types_nodes::jointype::Join;
use types_nodes::nodeindexscan::{Plan, Scan};
use types_nodes::nodes::Node;
use types_nodes::primnodes::{Expr, TargetEntry};

use crate::{
    out_expr, out_node_inner, write_bitmapset_opt_field, write_bool_field, write_enum_field,
    write_float_field, write_int_field, write_long_field, write_oid_field, write_string_field,
    write_uint64_field, write_uint_field,
};

// ---------------------------------------------------------------------------
// Local list / array helpers (the lib.rs `write_node_list_field` and
// `out_targetentry` are private; this family re-derives the few it needs,
// matching `outfuncs.c`'s `_outList` / `_outTargetEntry` / `writeXxxCols`
// formats exactly).
// ---------------------------------------------------------------------------

/// `_outTargetEntry` (outfuncs.funcs.c) — the framed `{TARGETENTRY ...}` body
/// (re-derived here; lib.rs's copy is private).
fn out_te(buf: &mut String, node: &TargetEntry<'_>, write_loc: bool) {
    buf.push_str("TARGETENTRY");
    buf.push_str(" :expr ");
    match node.expr.as_deref() {
        None => buf.push_str("<>"),
        Some(e) => out_expr(buf, e, write_loc),
    }
    write_int_field(buf, "resno", node.resno as i32);
    write_string_field(buf, "resname", node.resname.as_ref().map(|s| s.as_str()));
    write_uint_field(buf, "ressortgroupref", node.ressortgroupref);
    write_oid_field(buf, "resorigtbl", node.resorigtbl);
    write_int_field(buf, "resorigcol", node.resorigcol as i32);
    write_bool_field(buf, "resjunk", node.resjunk);
}

/// `WRITE_NODE_FIELD` over a `List *` of `TargetEntry` (C: `outNode` of the
/// `List`). `None`/NIL → `<>`; otherwise `({TARGETENTRY ...} ...)`.
fn write_te_list(buf: &mut String, name: &str, list: Option<&[TargetEntry<'_>]>, write_loc: bool) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(elems) => {
            buf.push('(');
            let mut first = true;
            for te in elems {
                if !first {
                    buf.push(' ');
                }
                first = false;
                buf.push('{');
                out_te(buf, te, write_loc);
                buf.push('}');
            }
            buf.push(')');
        }
    }
}

/// `WRITE_NODE_FIELD` over a `List *` of `Expr` (C: `outNode` of the `List`).
/// `None`/NIL → `<>`; otherwise `({...} ...)`.
fn write_expr_opt_list(buf: &mut String, name: &str, list: Option<&[Expr]>, write_loc: bool) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(elems) => {
            buf.push('(');
            let mut first = true;
            for e in elems {
                if !first {
                    buf.push(' ');
                }
                first = false;
                out_expr(buf, e, write_loc);
            }
            buf.push(')');
        }
    }
}

/// `WRITE_NODE_FIELD` over a `List *` of `Node` (C: `outNode` of the `List`).
/// `None`/NIL → `<>`; otherwise `({...} ...)`.
fn write_node_opt_list(buf: &mut String, name: &str, list: Option<&[Node<'_>]>, write_loc: bool) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(elems) => {
            buf.push('(');
            let mut first = true;
            for n in elems {
                if !first {
                    buf.push(' ');
                }
                first = false;
                out_node_inner(buf, n, write_loc);
            }
            buf.push(')');
        }
    }
}

/// A non-optional `Vec<Node>` child list (the repo carries some plan-child
/// lists as a bare `Vec<Node>` — a populated list is `(...)`; the C planner
/// always builds these non-NIL).
fn write_node_vec(buf: &mut String, name: &str, list: &[Node<'_>], write_loc: bool) {
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    let mut first = true;
    for n in list {
        if !first {
            buf.push(' ');
        }
        first = false;
        out_node_inner(buf, n, write_loc);
    }
    buf.push(')');
}

/// `WRITE_ATTRNUMBER_ARRAY` → `writeAttrNumberCols`: `( v0 v1 ...)` (a leading
/// space before each `%d`), or `<>` for a NULL array.
fn write_attrnumber_array(buf: &mut String, name: &str, arr: &[i16]) {
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    for v in arr {
        let _ = write!(buf, " {}", v);
    }
    buf.push(')');
}

/// `WRITE_OID_ARRAY` → `writeOidCols`: `( v0 v1 ...)` (`%u`), or `<>`.
fn write_oid_array(buf: &mut String, name: &str, arr: &[u32]) {
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    for v in arr {
        let _ = write!(buf, " {}", v);
    }
    buf.push(')');
}

/// `WRITE_NODE_FIELD` over a `List *` of `Oid` (an `OidList`): the `(o v1 v2 ...)`
/// form. An empty list is `(o)`; the planner always builds these non-NIL for
/// the nodes that carry them (HashJoin hashoperators/hashcollations).
fn write_oidlist(buf: &mut String, name: &str, arr: &[u32]) {
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    buf.push('o');
    for v in arr {
        let _ = write!(buf, " {}", v);
    }
    buf.push(')');
}

/// `WRITE_BOOL_ARRAY` → `writeBoolCols`: `( t f ...)` (`booltostr`), or `<>`.
fn write_bool_array(buf: &mut String, name: &str, arr: &[bool]) {
    let _ = write!(buf, " :{} ", name);
    buf.push('(');
    for v in arr {
        let _ = write!(buf, " {}", if *v { "true" } else { "false" });
    }
    buf.push(')');
}

// ---------------------------------------------------------------------------
// Flattened supertype emitters. `prefix` is the literal dotted token prefix
// (e.g. `scan.plan.`, `plan.`, `join.plan.`) so each node passes its exact C
// string.
// ---------------------------------------------------------------------------

/// The flattened `Plan` supertype fields, in `_outPlan` order. `prefix` is the
/// dotted token prefix (e.g. `scan.plan.` for a SeqScan, `plan.` for an
/// Append).
fn out_plan_fields(buf: &mut String, plan: &Plan<'_>, prefix: &str, write_loc: bool) {
    let p = |f: &str| {
        let mut s = String::with_capacity(prefix.len() + f.len());
        s.push_str(prefix);
        s.push_str(f);
        s
    };
    write_int_field(buf, &p("disabled_nodes"), plan.disabled_nodes);
    write_float_field(buf, &p("startup_cost"), plan.startup_cost);
    write_float_field(buf, &p("total_cost"), plan.total_cost);
    write_float_field(buf, &p("plan_rows"), plan.plan_rows);
    write_int_field(buf, &p("plan_width"), plan.plan_width);
    write_bool_field(buf, &p("parallel_aware"), plan.parallel_aware);
    write_bool_field(buf, &p("parallel_safe"), plan.parallel_safe);
    write_bool_field(buf, &p("async_capable"), plan.async_capable);
    write_int_field(buf, &p("plan_node_id"), plan.plan_node_id);
    write_te_list(buf, &p("targetlist"), plan.targetlist.as_deref(), write_loc);
    write_expr_opt_list(buf, &p("qual"), plan.qual.as_deref(), write_loc);
    // lefttree / righttree: single child `Plan *` (any plan-node `Node`).
    let _ = write!(buf, " :{} ", p("lefttree"));
    match plan.lefttree.as_deref() {
        None => buf.push_str("<>"),
        Some(n) => out_node_inner(buf, n, write_loc),
    }
    let _ = write!(buf, " :{} ", p("righttree"));
    match plan.righttree.as_deref() {
        None => buf.push_str("<>"),
        Some(n) => out_node_inner(buf, n, write_loc),
    }
    // initPlan: `List *` of `SubPlan` (an Expr). The C `WRITE_NODE_FIELD`
    // renders NIL as `<>`; a populated list recurses through `out_expr`, which
    // panics for `SubPlan` until that Expr writer lands (mirror-pg-and-panic —
    // initPlan is empty in the common case).
    let _ = write!(buf, " :{} ", p("initPlan"));
    match &plan.initPlan {
        None => buf.push_str("<>"),
        Some(list) if list.is_empty() => buf.push_str("<>"),
        Some(_) => panic!(
            "outPlan: initPlan carries SubPlan; the {{SUBPLAN ...}} Expr writer is \
             not ported into this enum's serialization stage yet (out_expr panics on \
             Expr::SubPlan). initPlan is empty in the common stored-plan case; a \
             non-empty list is unmodeled here (mirror-pg-and-panic)"
        ),
    }
    write_bitmapset_opt_field(buf, &p("extParam"), plan.extParam.as_deref());
    write_bitmapset_opt_field(buf, &p("allParam"), plan.allParam.as_deref());
}

/// The flattened `Scan` supertype: `out_plan_fields` with prefix `<prefix>plan.`
/// then `:<prefix>scanrelid`.
fn out_scan_fields(buf: &mut String, scan: &Scan<'_>, prefix: &str, write_loc: bool) {
    let mut plan_prefix = String::with_capacity(prefix.len() + 5);
    plan_prefix.push_str(prefix);
    plan_prefix.push_str("plan.");
    out_plan_fields(buf, &scan.plan, &plan_prefix, write_loc);
    let mut srel = String::with_capacity(prefix.len() + 9);
    srel.push_str(prefix);
    srel.push_str("scanrelid");
    write_uint_field(buf, &srel, scan.scanrelid);
}

/// The flattened `Join` supertype (`_out*Join`'s common prologue): the plan
/// fields with prefix `<prefix>plan.`, then `jointype`/`inner_unique`/`joinqual`.
fn out_join_fields(buf: &mut String, join: &Join<'_>, prefix: &str, write_loc: bool) {
    let mut plan_prefix = String::with_capacity(prefix.len() + 5);
    plan_prefix.push_str(prefix);
    plan_prefix.push_str("plan.");
    out_plan_fields(buf, &join.plan, &plan_prefix, write_loc);
    let f = |name: &str| {
        let mut s = String::with_capacity(prefix.len() + name.len());
        s.push_str(prefix);
        s.push_str(name);
        s
    };
    write_enum_field(buf, &f("jointype"), join.jointype as i32);
    write_bool_field(buf, &f("inner_unique"), join.inner_unique);
    write_expr_opt_list(buf, &f("joinqual"), join.joinqual.as_deref(), write_loc);
}

// ---------------------------------------------------------------------------
// Per-node `_out<Type>` bodies.
// ---------------------------------------------------------------------------

fn out_seqscan(buf: &mut String, n: &types_nodes::nodeseqscan::SeqScan<'_>, wl: bool) {
    buf.push_str("SEQSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
}

/// `WRITE_NODE_FIELD(functions)` over the `List *` of framed `RangeTblFunction`
/// (C: `outNode` of the list → `({RANGETBLFUNCTION ...} ...)`). Each element is
/// the parse-family-owned `_outRangeTblFunction`, framed by `{`/`}`. A `None`
/// (C `NIL`) list renders `<>` (`outNode(NULL)`).
fn write_rtf_list_field(
    buf: &mut String,
    name: &str,
    list: Option<&[types_nodes::rawnodes::RangeTblFunction<'_>]>,
    wl: bool,
) {
    let _ = write!(buf, " :{} ", name);
    match list {
        None => buf.push_str("<>"),
        Some(elems) => {
            buf.push('(');
            let mut first = true;
            for rtf in elems {
                if !first {
                    buf.push(' ');
                }
                first = false;
                crate::framed(buf, |b| crate::out_parse_family::out_range_tbl_function(b, rtf, wl));
            }
            buf.push(')');
        }
    }
}

/// `_outFunctionScan` (outfuncs.funcs.c): `WRITE_SCAN_FIELDS()`, then the
/// `functions` node list and the `funcordinality` flag.
fn out_functionscan(buf: &mut String, n: &types_nodes::nodefunctionscan::FunctionScan<'_>, wl: bool) {
    buf.push_str("FUNCTIONSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_rtf_list_field(buf, "functions", n.functions.as_deref(), wl);
    write_bool_field(buf, "funcordinality", n.funcordinality);
}

fn out_material(buf: &mut String, n: &types_nodes::nodeforeigncustom::Material<'_>, wl: bool) {
    buf.push_str("MATERIAL");
    out_plan_fields(buf, &n.plan, "plan.", wl);
}

fn out_projectset(buf: &mut String, n: &types_nodes::nodeprojectset::ProjectSet<'_>, wl: bool) {
    buf.push_str("PROJECTSET");
    out_plan_fields(buf, &n.plan, "plan.", wl);
}

fn out_result(buf: &mut String, n: &types_nodes::noderesult::Result<'_>, wl: bool) {
    buf.push_str("RESULT");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_expr_opt_list(buf, "resconstantqual", n.resconstantqual.as_deref(), wl);
}

fn out_append(buf: &mut String, n: &types_nodes::nodeappend::Append<'_>, wl: bool) {
    buf.push_str("APPEND");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_bitmapset_opt_field(buf, "apprelids", n.apprelids.as_deref());
    write_node_vec(buf, "appendplans", &n.appendplans, wl);
    write_int_field(buf, "nasyncplans", n.nasyncplans);
    write_int_field(buf, "first_partial_plan", n.first_partial_plan);
    write_int_field(buf, "part_prune_index", n.part_prune_index);
}

fn out_bitmapand(buf: &mut String, n: &types_nodes::nodebitmapand::BitmapAnd<'_>, wl: bool) {
    buf.push_str("BITMAPAND");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_node_vec(buf, "bitmapplans", &n.bitmapplans, wl);
}

fn out_gather(buf: &mut String, n: &types_nodes::nodegather::Gather<'_>, wl: bool) {
    buf.push_str("GATHER");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_int_field(buf, "num_workers", n.num_workers);
    write_int_field(buf, "rescan_param", n.rescan_param);
    write_bool_field(buf, "single_copy", n.single_copy);
    write_bool_field(buf, "invisible", n.invisible);
    write_bitmapset_opt_field(buf, "initParam", n.initParam.as_deref());
}

fn out_gathermerge(buf: &mut String, n: &types_nodes::nodegathermerge::GatherMerge<'_>, wl: bool) {
    buf.push_str("GATHERMERGE");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_int_field(buf, "num_workers", n.num_workers);
    write_int_field(buf, "rescan_param", n.rescan_param);
    write_int_field(buf, "numCols", n.numCols);
    write_attrnumber_array(buf, "sortColIdx", &n.sortColIdx);
    write_oid_array(buf, "sortOperators", &n.sortOperators);
    write_oid_array(buf, "collations", &n.collations);
    write_bool_array(buf, "nullsFirst", &n.nullsFirst);
    write_bitmapset_opt_field(buf, "initParam", n.initParam.as_deref());
}

fn out_mergeappend(buf: &mut String, n: &types_nodes::nodemergeappend::MergeAppend<'_>, wl: bool) {
    buf.push_str("MERGEAPPEND");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_bitmapset_opt_field(buf, "apprelids", n.apprelids.as_deref());
    write_node_vec(buf, "mergeplans", &n.mergeplans, wl);
    write_int_field(buf, "numCols", n.numCols);
    write_attrnumber_array(buf, "sortColIdx", &n.sortColIdx);
    write_oid_array(buf, "sortOperators", &n.sortOperators);
    write_oid_array(buf, "collations", &n.collations);
    write_bool_array(buf, "nullsFirst", &n.nullsFirst);
    write_int_field(buf, "part_prune_index", n.part_prune_index);
}

fn out_recursiveunion(
    buf: &mut String,
    n: &types_nodes::noderecursiveunion::RecursiveUnion<'_>,
    wl: bool,
) {
    buf.push_str("RECURSIVEUNION");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_int_field(buf, "wtParam", n.wtParam);
    write_int_field(buf, "numCols", n.numCols);
    write_attrnumber_array(buf, "dupColIdx", &n.dupColIdx);
    write_oid_array(buf, "dupOperators", &n.dupOperators);
    write_oid_array(buf, "dupCollations", &n.dupCollations);
    write_long_field(buf, "numGroups", n.numGroups);
}

fn out_group(buf: &mut String, n: &types_nodes::nodegroup::Group<'_>, wl: bool) {
    buf.push_str("GROUP");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_int_field(buf, "numCols", n.numCols);
    write_attrnumber_array(buf, "grpColIdx", &n.grpColIdx);
    write_oid_array(buf, "grpOperators", &n.grpOperators);
    write_oid_array(buf, "grpCollations", &n.grpCollations);
}

fn out_setop(buf: &mut String, n: &types_nodes::nodesetop::SetOp<'_>, wl: bool) {
    buf.push_str("SETOP");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_enum_field(buf, "cmd", n.cmd as i32);
    write_enum_field(buf, "strategy", n.strategy as i32);
    write_int_field(buf, "numCols", n.numCols);
    write_attrnumber_array(buf, "cmpColIdx", &n.cmpColIdx);
    write_oid_array(buf, "cmpOperators", &n.cmpOperators);
    write_oid_array(buf, "cmpCollations", &n.cmpCollations);
    write_bool_array(buf, "cmpNullsFirst", &n.cmpNullsFirst);
    write_long_field(buf, "numGroups", n.numGroups);
}

fn out_unique(buf: &mut String, n: &types_nodes::nodeunique::Unique<'_>, wl: bool) {
    buf.push_str("UNIQUE");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_int_field(buf, "numCols", n.numCols);
    write_attrnumber_array(buf, "uniqColIdx", opt_slice(&n.uniqColIdx));
    write_oid_array(buf, "uniqOperators", opt_slice(&n.uniqOperators));
    write_oid_array(buf, "uniqCollations", opt_slice(&n.uniqCollations));
}

fn out_sort(buf: &mut String, n: &types_nodes::nodesort::Sort<'_>, wl: bool) {
    buf.push_str("SORT");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_int_field(buf, "numCols", n.numCols);
    write_attrnumber_array(buf, "sortColIdx", &n.sortColIdx);
    write_oid_array(buf, "sortOperators", &n.sortOperators);
    write_oid_array(buf, "collations", &n.collations);
    write_bool_array(buf, "nullsFirst", &n.nullsFirst);
}

fn out_incrementalsort(
    buf: &mut String,
    n: &types_nodes::nodeincrementalsort::IncrementalSort<'_>,
    wl: bool,
) {
    buf.push_str("INCREMENTALSORT");
    // Flattened `Sort sort` supertype: plan fields with prefix `sort.plan.`,
    // then the sort-key arrays with prefix `sort.`.
    out_plan_fields(buf, &n.sort.plan, "sort.plan.", wl);
    write_int_field(buf, "sort.numCols", n.sort.numCols);
    write_attrnumber_array(buf, "sort.sortColIdx", &n.sort.sortColIdx);
    write_oid_array(buf, "sort.sortOperators", &n.sort.sortOperators);
    write_oid_array(buf, "sort.collations", &n.sort.collations);
    write_bool_array(buf, "sort.nullsFirst", &n.sort.nullsFirst);
    write_int_field(buf, "nPresortedCols", n.nPresortedCols);
}

fn out_limit(buf: &mut String, n: &types_nodes::nodelimit::Limit<'_>, wl: bool) {
    buf.push_str("LIMIT");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    let _ = write!(buf, " :limitOffset ");
    match n.limitOffset.as_deref() {
        None => buf.push_str("<>"),
        Some(e) => out_expr(buf, e, wl),
    }
    let _ = write!(buf, " :limitCount ");
    match n.limitCount.as_deref() {
        None => buf.push_str("<>"),
        Some(e) => out_expr(buf, e, wl),
    }
    write_enum_field(buf, "limitOption", n.limitOption as i32);
    write_int_field(buf, "uniqNumCols", n.uniqNumCols);
    write_attrnumber_array(buf, "uniqColIdx", opt_slice(&n.uniqColIdx));
    write_oid_array(buf, "uniqOperators", opt_slice(&n.uniqOperators));
    write_oid_array(buf, "uniqCollations", opt_slice(&n.uniqCollations));
}

fn out_agg(buf: &mut String, n: &types_nodes::nodeagg::Agg<'_>, wl: bool) {
    buf.push_str("AGG");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_enum_field(buf, "aggstrategy", n.aggstrategy as i32);
    write_enum_field(buf, "aggsplit", n.aggsplit as i32);
    write_int_field(buf, "numCols", n.num_cols);
    write_attrnumber_array(buf, "grpColIdx", opt_slice(&n.grp_col_idx));
    write_oid_array(buf, "grpOperators", opt_slice(&n.grp_operators));
    write_oid_array(buf, "grpCollations", opt_slice(&n.grp_collations));
    write_long_field(buf, "numGroups", n.num_groups);
    write_uint64_field(buf, "transitionSpace", n.transition_space);
    write_bitmapset_opt_field(buf, "aggParams", n.agg_params.as_deref());
    // groupingSets: `List *` of `IntList` (each an int list). NIL → `<>`.
    let _ = write!(buf, " :groupingSets ");
    match &n.grouping_sets {
        None => buf.push_str("<>"),
        Some(sets) => {
            buf.push('(');
            let mut first = true;
            for gs in sets.iter() {
                if !first {
                    buf.push(' ');
                }
                first = false;
                buf.push('(');
                buf.push('i');
                for v in gs.iter() {
                    let _ = write!(buf, " {}", v);
                }
                buf.push(')');
            }
            buf.push(')');
        }
    }
    // chain: `List *` of `Agg`. NIL → `<>`; else `({AGG ...} ...)`.
    let _ = write!(buf, " :chain ");
    match &n.chain {
        None => buf.push_str("<>"),
        Some(list) => {
            buf.push('(');
            let mut first = true;
            for a in list.iter() {
                if !first {
                    buf.push(' ');
                }
                first = false;
                buf.push('{');
                out_agg(buf, a, wl);
                buf.push('}');
            }
            buf.push(')');
        }
    }
}

fn out_nestloop(buf: &mut String, n: &types_nodes::nodenestloop::NestLoop<'_>, wl: bool) {
    buf.push_str("NESTLOOP");
    out_join_fields(buf, &n.join, "join.", wl);
    // nestParams: `List *` of `NestLoopParam` nodes. NestLoopParam is not a
    // serialization `Node` arm in this enum, so a populated list cannot be
    // emitted faithfully; the planner always sets this for parameterized
    // nestloops. mirror-pg-and-panic on a non-empty list, `<>` when empty.
    if n.nestParams.is_empty() {
        buf.push_str(" :nestParams <>");
    } else {
        panic!(
            "_outNestLoop: nestParams carries NestLoopParam, which is not a \
             serialization Node arm in this enum (no _outNestLoopParam writer); \
             field unmodeled for non-empty lists"
        );
    }
}

fn out_mergejoin(buf: &mut String, n: &types_nodes::nodemergejoin::MergeJoin<'_>, wl: bool) {
    buf.push_str("MERGEJOIN");
    out_join_fields(buf, &n.join, "join.", wl);
    write_bool_field(buf, "skip_mark_restore", n.skip_mark_restore);
    write_expr_opt_list(buf, "mergeclauses", Some(&n.mergeclauses), wl);
    write_oid_array(buf, "mergeFamilies", &n.mergeFamilies);
    write_oid_array(buf, "mergeCollations", &n.mergeCollations);
    write_bool_array(buf, "mergeReversals", &n.mergeReversals);
    write_bool_array(buf, "mergeNullsFirst", &n.mergeNullsFirst);
}

fn out_hashjoin(buf: &mut String, n: &types_nodes::nodehashjoin::HashJoin<'_>, wl: bool) {
    buf.push_str("HASHJOIN");
    out_join_fields(buf, &n.join, "join.", wl);
    write_node_opt_list(buf, "hashclauses", n.hashclauses.as_deref(), wl);
    // hashoperators / hashcollations: C `WRITE_NODE_FIELD` over an `OidList`
    // (NOT a count-paired OID array) → the `(o v1 v2 ...)` form.
    write_oidlist(buf, "hashoperators", &n.hashoperators);
    write_oidlist(buf, "hashcollations", &n.hashcollations);
    write_node_opt_list(buf, "hashkeys", n.hashkeys.as_deref(), wl);
}

fn out_hash(buf: &mut String, n: &types_nodes::nodehashjoin::Hash<'_>, wl: bool) {
    buf.push_str("HASH");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_node_opt_list(buf, "hashkeys", n.hashkeys.as_deref(), wl);
    write_oid_field(buf, "skewTable", n.skewTable);
    write_int_field(buf, "skewColumn", n.skewColumn as i32);
    write_bool_field(buf, "skewInherit", n.skewInherit);
    write_float_field(buf, "rows_total", n.rows_total);
}

fn out_memoize(buf: &mut String, n: &types_nodes::nodememoize::Memoize<'_>, wl: bool) {
    buf.push_str("MEMOIZE");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_int_field(buf, "numKeys", n.numKeys);
    write_oid_array(buf, "hashOperators", &n.hashOperators);
    write_oid_array(buf, "collations", &n.collations);
    write_expr_opt_list(buf, "param_exprs", Some(&n.param_exprs), wl);
    write_bool_field(buf, "singlerow", n.singlerow);
    write_bool_field(buf, "binary_mode", n.binary_mode);
    write_uint_field(buf, "est_entries", n.est_entries);
    write_bitmapset_opt_field(buf, "keyparamids", n.keyparamids.as_deref());
}

fn out_indexscan(buf: &mut String, n: &types_nodes::nodeindexscan::IndexScan<'_>, wl: bool) {
    buf.push_str("INDEXSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_oid_field(buf, "indexid", n.indexid);
    write_expr_opt_list(buf, "indexqual", n.indexqual.as_deref(), wl);
    write_expr_opt_list(buf, "indexqualorig", n.indexqualorig.as_deref(), wl);
    write_expr_opt_list(buf, "indexorderby", n.indexorderby.as_deref(), wl);
    write_expr_opt_list(buf, "indexorderbyorig", n.indexorderbyorig.as_deref(), wl);
    // indexorderbyops: `List *` of `Oid` — C `WRITE_NODE_FIELD` of an OidList
    // → `(o ...)` / `<>`.
    let _ = write!(buf, " :indexorderbyops ");
    match &n.indexorderbyops {
        None => buf.push_str("<>"),
        Some(v) => {
            buf.push('(');
            buf.push('o');
            for x in v.iter() {
                let _ = write!(buf, " {}", x);
            }
            buf.push(')');
        }
    }
    write_enum_field(buf, "indexorderdir", n.indexorderdir as i32);
}

fn out_indexonlyscan(
    buf: &mut String,
    n: &types_nodes::nodeindexonlyscan::IndexOnlyScan<'_>,
    wl: bool,
) {
    buf.push_str("INDEXONLYSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_oid_field(buf, "indexid", n.indexid);
    write_expr_opt_list(buf, "indexqual", n.indexqual.as_deref(), wl);
    write_expr_opt_list(buf, "recheckqual", n.recheckqual.as_deref(), wl);
    write_expr_opt_list(buf, "indexorderby", n.indexorderby.as_deref(), wl);
    write_te_list(buf, "indextlist", n.indextlist.as_deref(), wl);
    write_enum_field(buf, "indexorderdir", n.indexorderdir as i32);
}

fn out_bitmapindexscan(
    buf: &mut String,
    n: &types_nodes::nodebitmapindexscan::BitmapIndexScan<'_>,
    wl: bool,
) {
    buf.push_str("BITMAPINDEXSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_oid_field(buf, "indexid", n.indexid);
    write_bool_field(buf, "isshared", n.isshared);
    write_expr_opt_list(buf, "indexqual", n.indexqual.as_deref(), wl);
    write_expr_opt_list(buf, "indexqualorig", n.indexqualorig.as_deref(), wl);
}

fn out_tidscan(buf: &mut String, n: &types_nodes::nodeindexscan::TidScan<'_>, wl: bool) {
    buf.push_str("TIDSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_expr_opt_list(buf, "tidquals", n.tidquals.as_deref(), wl);
}

fn out_tidrangescan(
    buf: &mut String,
    n: &types_nodes::nodetidrangescan::TidRangeScan<'_>,
    wl: bool,
) {
    buf.push_str("TIDRANGESCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_expr_opt_list(buf, "tidrangequals", n.tidrangequals.as_deref(), wl);
}

fn out_subqueryscan(
    buf: &mut String,
    n: &types_nodes::nodeindexscan::SubqueryScan<'_>,
    wl: bool,
) {
    buf.push_str("SUBQUERYSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    let _ = write!(buf, " :subplan ");
    match n.subplan.as_deref() {
        None => buf.push_str("<>"),
        Some(p) => out_node_inner(buf, p, wl),
    }
    write_enum_field(buf, "scanstatus", n.scanstatus as i32);
}

fn out_worktablescan(
    buf: &mut String,
    n: &types_nodes::nodeworktablescan::WorkTableScan<'_>,
    wl: bool,
) {
    buf.push_str("WORKTABLESCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_int_field(buf, "wtParam", n.wtParam);
}

fn out_ctescan(buf: &mut String, n: &types_nodes::nodectescan::CteScan<'_>, wl: bool) {
    buf.push_str("CTESCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_int_field(buf, "ctePlanId", n.ctePlanId);
    write_int_field(buf, "cteParam", n.cteParam);
}

fn out_namedtuplestorescan(
    buf: &mut String,
    n: &types_nodes::nodenamedtuplestorescan::NamedTuplestoreScan<'_>,
    wl: bool,
) {
    buf.push_str("NAMEDTUPLESTORESCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_string_field(buf, "enrname", n.enrname.as_ref().map(|s| s.as_str()));
}

fn out_valuesscan(buf: &mut String, n: &types_nodes::nodevaluesscan::ValuesScan<'_>, wl: bool) {
    buf.push_str("VALUESSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    // values_lists: `List *` of (`List *` of Expr). C `WRITE_NODE_FIELD` →
    // `(( {..} ..) ( ..))`. The repo carries it as a non-optional PgVec of
    // PgVec; the planner always sets it. A `List` of sub-lists is rendered
    // `(` <sublist> ... `)`; each sublist is an `_outList` `( {..} ..)`.
    let _ = write!(buf, " :values_lists ");
    buf.push('(');
    let mut first = true;
    for sub in n.values_lists.iter() {
        if !first {
            buf.push(' ');
        }
        first = false;
        buf.push('(');
        let mut sf = true;
        for e in sub.iter() {
            if !sf {
                buf.push(' ');
            }
            sf = false;
            out_expr(buf, e, wl);
        }
        buf.push(')');
    }
    buf.push(')');
}

fn out_foreignscan(
    buf: &mut String,
    n: &types_nodes::nodeforeigncustom::ForeignScan<'_>,
    wl: bool,
) {
    buf.push_str("FOREIGNSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_enum_field(buf, "operation", n.operation as i32);
    write_uint_field(buf, "resultRelation", n.resultRelation);
    write_oid_field(buf, "checkAsUser", n.checkAsUser);
    write_oid_field(buf, "fs_server", n.fs_server);
    write_expr_opt_list(buf, "fdw_exprs", n.fdw_exprs.as_deref(), wl);
    // fdw_private: `List *` of Node (carried as NodePtr boxes). NIL → `<>`.
    let _ = write!(buf, " :fdw_private ");
    match &n.fdw_private {
        None => buf.push_str("<>"),
        Some(list) => {
            buf.push('(');
            let mut first = true;
            for np in list.iter() {
                if !first {
                    buf.push(' ');
                }
                first = false;
                out_node_inner(buf, np, wl);
            }
            buf.push(')');
        }
    }
    write_te_list(buf, "fdw_scan_tlist", n.fdw_scan_tlist.as_deref(), wl);
    write_expr_opt_list(buf, "fdw_recheck_quals", n.fdw_recheck_quals.as_deref(), wl);
    write_bitmapset_opt_field(buf, "fs_relids", n.fs_relids.as_deref());
    write_bitmapset_opt_field(buf, "fs_base_relids", n.fs_base_relids.as_deref());
    write_bool_field(buf, "fsSystemCol", n.fsSystemCol);
}

/// Optional-`PgVec` → `&[]` (empty) or its slice; used where the repo stores an
/// array column as `Option<PgVec<_>>` but C always emits the bare `( ...)` over
/// `numCols` (never `<>` here — the count-paired arrays are always present when
/// numCols > 0, and an empty slice yields `( )` matching len 0).
fn opt_slice<'a, T>(o: &'a Option<mcx::PgVec<'_, T>>) -> &'a [T] {
    match o {
        Some(v) => v.as_slice(),
        None => &[],
    }
}

/// Dispatch the out_plan_family `Node` arms this module owns.
pub(crate) fn try_out(buf: &mut String, node: &Node<'_>, wl: bool) -> bool {
    match node {
        Node::SeqScan(n) => crate::framed(buf, |b| out_seqscan(b, n, wl)),
        Node::Material(n) => crate::framed(buf, |b| out_material(b, n, wl)),
        Node::ProjectSet(n) => crate::framed(buf, |b| out_projectset(b, n, wl)),
        Node::Result(n) => crate::framed(buf, |b| out_result(b, n, wl)),
        Node::Append(n) => crate::framed(buf, |b| out_append(b, n, wl)),
        Node::BitmapAnd(n) => crate::framed(buf, |b| out_bitmapand(b, n, wl)),
        Node::Gather(n) => crate::framed(buf, |b| out_gather(b, n, wl)),
        Node::GatherMerge(n) => crate::framed(buf, |b| out_gathermerge(b, n, wl)),
        Node::MergeAppend(n) => crate::framed(buf, |b| out_mergeappend(b, n, wl)),
        Node::RecursiveUnion(n) => crate::framed(buf, |b| out_recursiveunion(b, n, wl)),
        Node::Group(n) => crate::framed(buf, |b| out_group(b, n, wl)),
        Node::SetOp(n) => crate::framed(buf, |b| out_setop(b, n, wl)),
        Node::Unique(n) => crate::framed(buf, |b| out_unique(b, n, wl)),
        Node::Sort(n) => crate::framed(buf, |b| out_sort(b, n, wl)),
        Node::IncrementalSort(n) => crate::framed(buf, |b| out_incrementalsort(b, n, wl)),
        Node::Limit(n) => crate::framed(buf, |b| out_limit(b, n, wl)),
        Node::Agg(n) => crate::framed(buf, |b| out_agg(b, n, wl)),
        Node::NestLoop(n) => crate::framed(buf, |b| out_nestloop(b, n, wl)),
        Node::MergeJoin(n) => crate::framed(buf, |b| out_mergejoin(b, n, wl)),
        Node::HashJoin(n) => crate::framed(buf, |b| out_hashjoin(b, n, wl)),
        Node::Hash(n) => crate::framed(buf, |b| out_hash(b, n, wl)),
        Node::Memoize(n) => crate::framed(buf, |b| out_memoize(b, n, wl)),
        Node::IndexScan(n) => crate::framed(buf, |b| out_indexscan(b, n, wl)),
        Node::IndexOnlyScan(n) => crate::framed(buf, |b| out_indexonlyscan(b, n, wl)),
        Node::BitmapIndexScan(n) => crate::framed(buf, |b| out_bitmapindexscan(b, n, wl)),
        Node::TidScan(n) => crate::framed(buf, |b| out_tidscan(b, n, wl)),
        Node::TidRangeScan(n) => crate::framed(buf, |b| out_tidrangescan(b, n, wl)),
        Node::SubqueryScan(n) => crate::framed(buf, |b| out_subqueryscan(b, n, wl)),
        Node::WorkTableScan(n) => crate::framed(buf, |b| out_worktablescan(b, n, wl)),
        Node::CteScan(n) => crate::framed(buf, |b| out_ctescan(b, n, wl)),
        Node::NamedTuplestoreScan(n) => crate::framed(buf, |b| out_namedtuplestorescan(b, n, wl)),
        Node::ValuesScan(n) => crate::framed(buf, |b| out_valuesscan(b, n, wl)),
        Node::ForeignScan(n) => crate::framed(buf, |b| out_foreignscan(b, n, wl)),

        // ---- mirror-pg-and-panic: field-type unmodeled in this family ----
        Node::ModifyTable(_) => panic!(
            "_outModifyTable: not serialized — the node carries nested non-Node \
             list-of-lists (updateColnosLists/withCheckOptionLists/returningLists/\
             mergeActionLists/mergeJoinConditions) and MergeAction children, none \
             of which are reachable through this family's outNode dispatch"
        ),
        Node::WindowAgg(_) => panic!(
            "_outWindowAgg: not serialized — the repo WindowAgg struct trims the \
             `winname` field that C's _outWindowAgg writes (WRITE_STRING_FIELD(winname)); \
             field unmodeled"
        ),
        Node::TableFuncScan(_) => panic!(
            "_outTableFuncScan: not serialized — `tablefunc` is carried as a bare \
             TableFunc struct, whose framed `{{TABLEFUNC ...}}` writer lives in another \
             node family (not reachable as a Node here)"
        ),
        Node::FunctionScan(n) => crate::framed(buf, |b| out_functionscan(b, n, wl)),
        Node::SampleScan(_) => panic!(
            "_outSampleScan: not serialized — `tablesample` is a bare \
             TableSampleClause whose framed writer lives in another node family \
             (not reachable as a Node here)"
        ),
        Node::CustomScan(_) => panic!(
            "_outCustomScan: not serialized — `custom_private` and `methods` are \
             opaque provider pointers (the C `:methods` token reads \
             node->methods->CustomName from a static provider table)"
        ),
        _ => return false,
    }
    let _ = wl; // each writer threads wl directly
    true
}
