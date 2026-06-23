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

use ::nodes::jointype::Join;
use ::nodes::nodeindexscan::{Plan, Scan};
use ::nodes::nodes::{ntag, Node};
use ::nodes::primnodes::{Expr, TargetEntry};

use crate::{
    out_expr, out_node_inner, write_bitmapset_opt_field, write_bool_field, write_enum_field,
    write_expr_field, write_float_field, write_int_field, write_long_field, write_oid_field,
    write_string_field, write_uint64_field, write_uint_field,
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
    // initPlan: `List *` of `SubPlan`. The C `WRITE_NODE_FIELD(initPlan)` calls
    // `outNode` on the List, rendering NIL as `<>` and a populated list as
    // `({SUBPLAN ...} {SUBPLAN ...} ...)` (each element framed by `out_subplan`).
    let _ = write!(buf, " :{} ", p("initPlan"));
    match &plan.initPlan {
        None => buf.push_str("<>"),
        Some(list) if list.is_empty() => buf.push_str("<>"),
        Some(list) => {
            buf.push('(');
            let mut first = true;
            for sp in list.iter() {
                if !first {
                    buf.push(' ');
                }
                first = false;
                crate::framed(buf, |b| crate::out_expr_family::out_subplan(b, sp, write_loc));
            }
            buf.push(')');
        }
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

fn out_seqscan(buf: &mut String, n: &::nodes::nodeseqscan::SeqScan<'_>, wl: bool) {
    buf.push_str("SEQSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
}

/// `_outSampleScan` (outfuncs.funcs.c) — `Scan scan` base then
/// `WRITE_NODE_FIELD(tablesample)` over the `TableSampleClause *` (its framed
/// `{TABLESAMPLECLAUSE ...}` writer lives in the parse family). A `NULL`
/// `tablesample` renders `<>` (`outNode(NULL)`).
fn out_samplescan(buf: &mut String, n: &::nodes::nodesamplescan::SampleScan<'_>, wl: bool) {
    buf.push_str("SAMPLESCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    buf.push_str(" :tablesample ");
    match n.tablesample.as_deref() {
        None => buf.push_str("<>"),
        Some(ts) => {
            crate::framed(buf, |b| crate::out_parse_family::out_table_sample_clause(b, ts, wl))
        }
    }
}

/// `WRITE_NODE_FIELD(functions)` over the `List *` of framed `RangeTblFunction`
/// (C: `outNode` of the list → `({RANGETBLFUNCTION ...} ...)`). Each element is
/// the parse-family-owned `_outRangeTblFunction`, framed by `{`/`}`. A `None`
/// (C `NIL`) list renders `<>` (`outNode(NULL)`).
fn write_rtf_list_field(
    buf: &mut String,
    name: &str,
    list: Option<&[::nodes::rawnodes::RangeTblFunction<'_>]>,
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
fn out_functionscan(buf: &mut String, n: &::nodes::nodefunctionscan::FunctionScan<'_>, wl: bool) {
    buf.push_str("FUNCTIONSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_rtf_list_field(buf, "functions", n.functions.as_deref(), wl);
    write_bool_field(buf, "funcordinality", n.funcordinality);
}

/// `_outTableFuncScan` — scan fields then `WRITE_NODE_FIELD(tablefunc)`. The
/// `tablefunc` child is a `TableFunc` node framed by the out_parse_family writer.
fn out_tablefuncscan(
    buf: &mut String,
    n: &::nodes::nodetablefuncscan::TableFuncScan<'_>,
    wl: bool,
) {
    buf.push_str("TABLEFUNCSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    let _ = write!(buf, " :tablefunc ");
    crate::framed(buf, |b| {
        crate::out_parse_family::out_table_func(b, &n.tablefunc, wl)
    });
}

fn out_material(buf: &mut String, n: &::nodes::nodeforeigncustom::Material<'_>, wl: bool) {
    buf.push_str("MATERIAL");
    out_plan_fields(buf, &n.plan, "plan.", wl);
}

fn out_projectset(buf: &mut String, n: &::nodes::nodeprojectset::ProjectSet<'_>, wl: bool) {
    buf.push_str("PROJECTSET");
    out_plan_fields(buf, &n.plan, "plan.", wl);
}

fn out_result(buf: &mut String, n: &::nodes::noderesult::Result<'_>, wl: bool) {
    buf.push_str("RESULT");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_expr_opt_list(buf, "resconstantqual", n.resconstantqual.as_deref(), wl);
}

fn out_append(buf: &mut String, n: &::nodes::nodeappend::Append<'_>, wl: bool) {
    buf.push_str("APPEND");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_bitmapset_opt_field(buf, "apprelids", n.apprelids.as_deref());
    write_node_vec(buf, "appendplans", &n.appendplans, wl);
    write_int_field(buf, "nasyncplans", n.nasyncplans);
    write_int_field(buf, "first_partial_plan", n.first_partial_plan);
    write_int_field(buf, "part_prune_index", n.part_prune_index);
}

fn out_bitmapand(buf: &mut String, n: &::nodes::nodebitmapand::BitmapAnd<'_>, wl: bool) {
    buf.push_str("BITMAPAND");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_node_vec(buf, "bitmapplans", &n.bitmapplans, wl);
}

fn out_gather(buf: &mut String, n: &::nodes::nodegather::Gather<'_>, wl: bool) {
    buf.push_str("GATHER");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_int_field(buf, "num_workers", n.num_workers);
    write_int_field(buf, "rescan_param", n.rescan_param);
    write_bool_field(buf, "single_copy", n.single_copy);
    write_bool_field(buf, "invisible", n.invisible);
    write_bitmapset_opt_field(buf, "initParam", n.initParam.as_deref());
}

fn out_gathermerge(buf: &mut String, n: &::nodes::nodegathermerge::GatherMerge<'_>, wl: bool) {
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

fn out_mergeappend(buf: &mut String, n: &::nodes::nodemergeappend::MergeAppend<'_>, wl: bool) {
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
    n: &::nodes::noderecursiveunion::RecursiveUnion<'_>,
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

fn out_group(buf: &mut String, n: &::nodes::nodegroup::Group<'_>, wl: bool) {
    buf.push_str("GROUP");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_int_field(buf, "numCols", n.numCols);
    write_attrnumber_array(buf, "grpColIdx", &n.grpColIdx);
    write_oid_array(buf, "grpOperators", &n.grpOperators);
    write_oid_array(buf, "grpCollations", &n.grpCollations);
}

fn out_setop(buf: &mut String, n: &::nodes::nodesetop::SetOp<'_>, wl: bool) {
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

fn out_unique(buf: &mut String, n: &::nodes::nodeunique::Unique<'_>, wl: bool) {
    buf.push_str("UNIQUE");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_int_field(buf, "numCols", n.numCols);
    write_attrnumber_array(buf, "uniqColIdx", opt_slice(&n.uniqColIdx));
    write_oid_array(buf, "uniqOperators", opt_slice(&n.uniqOperators));
    write_oid_array(buf, "uniqCollations", opt_slice(&n.uniqCollations));
}

fn out_sort(buf: &mut String, n: &::nodes::nodesort::Sort<'_>, wl: bool) {
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
    n: &::nodes::nodeincrementalsort::IncrementalSort<'_>,
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

fn out_limit(buf: &mut String, n: &::nodes::nodelimit::Limit<'_>, wl: bool) {
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

fn out_agg(buf: &mut String, n: &::nodes::nodeagg::Agg<'_>, wl: bool) {
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

/// `_outNestLoopParam` (outfuncs.funcs.c) — the framed `{NESTLOOPPARAM ...}`
/// body. `NestLoopParam` is carried as a typed struct (not a `Node` enum arm),
/// so it is serialized directly here rather than dispatched through `outNode`;
/// the parent `_outNestLoop` emits the `:nestParams (...)` list of these framed
/// forms by hand, which is byte-identical to C's `WRITE_NODE_FIELD(nestParams)`
/// (`outNode` of the `List` → `({NESTLOOPPARAM ...} ...)`).
fn out_nestloopparam(buf: &mut String, p: &::nodes::nodenestloop::NestLoopParam, wl: bool) {
    buf.push_str("NESTLOOPPARAM");
    write_int_field(buf, "paramno", p.paramno);
    // WRITE_NODE_FIELD(paramval): typed `Var *` but may transiently hold a
    // PlaceHolderVar during plan creation, so emit the generic node form.
    buf.push_str(" :paramval ");
    crate::out_expr(buf, &p.paramval, wl);
}

fn out_nestloop(buf: &mut String, n: &::nodes::nodenestloop::NestLoop<'_>, wl: bool) {
    buf.push_str("NESTLOOP");
    out_join_fields(buf, &n.join, "join.", wl);
    // WRITE_NODE_FIELD(nestParams): `List *` of `NestLoopParam` nodes. NIL (the
    // empty vec) renders `<>` (outNode of a NULL List); otherwise the bare
    // `({NESTLOOPPARAM ...} ...)` list form (`_outList` for T_List).
    buf.push_str(" :nestParams ");
    if n.nestParams.is_empty() {
        buf.push_str("<>");
    } else {
        buf.push('(');
        let mut first = true;
        for p in &n.nestParams {
            if !first {
                buf.push(' ');
            }
            first = false;
            buf.push('{');
            out_nestloopparam(buf, p, wl);
            buf.push('}');
        }
        buf.push(')');
    }
}

fn out_mergejoin(buf: &mut String, n: &::nodes::nodemergejoin::MergeJoin<'_>, wl: bool) {
    buf.push_str("MERGEJOIN");
    out_join_fields(buf, &n.join, "join.", wl);
    write_bool_field(buf, "skip_mark_restore", n.skip_mark_restore);
    write_expr_opt_list(buf, "mergeclauses", Some(&n.mergeclauses), wl);
    write_oid_array(buf, "mergeFamilies", &n.mergeFamilies);
    write_oid_array(buf, "mergeCollations", &n.mergeCollations);
    write_bool_array(buf, "mergeReversals", &n.mergeReversals);
    write_bool_array(buf, "mergeNullsFirst", &n.mergeNullsFirst);
}

fn out_hashjoin(buf: &mut String, n: &::nodes::nodehashjoin::HashJoin<'_>, wl: bool) {
    buf.push_str("HASHJOIN");
    out_join_fields(buf, &n.join, "join.", wl);
    write_node_opt_list(buf, "hashclauses", n.hashclauses.as_deref(), wl);
    // hashoperators / hashcollations: C `WRITE_NODE_FIELD` over an `OidList`
    // (NOT a count-paired OID array) → the `(o v1 v2 ...)` form.
    write_oidlist(buf, "hashoperators", &n.hashoperators);
    write_oidlist(buf, "hashcollations", &n.hashcollations);
    write_node_opt_list(buf, "hashkeys", n.hashkeys.as_deref(), wl);
}

fn out_hash(buf: &mut String, n: &::nodes::nodehashjoin::Hash<'_>, wl: bool) {
    buf.push_str("HASH");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_node_opt_list(buf, "hashkeys", n.hashkeys.as_deref(), wl);
    write_oid_field(buf, "skewTable", n.skewTable);
    write_int_field(buf, "skewColumn", n.skewColumn as i32);
    write_bool_field(buf, "skewInherit", n.skewInherit);
    write_float_field(buf, "rows_total", n.rows_total);
}

fn out_memoize(buf: &mut String, n: &::nodes::nodememoize::Memoize<'_>, wl: bool) {
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

fn out_indexscan(buf: &mut String, n: &::nodes::nodeindexscan::IndexScan<'_>, wl: bool) {
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
    n: &::nodes::nodeindexonlyscan::IndexOnlyScan<'_>,
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
    n: &::nodes::nodebitmapindexscan::BitmapIndexScan<'_>,
    wl: bool,
) {
    buf.push_str("BITMAPINDEXSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_oid_field(buf, "indexid", n.indexid);
    write_bool_field(buf, "isshared", n.isshared);
    write_expr_opt_list(buf, "indexqual", n.indexqual.as_deref(), wl);
    write_expr_opt_list(buf, "indexqualorig", n.indexqualorig.as_deref(), wl);
}

fn out_bitmapheapscan(
    buf: &mut String,
    n: &::nodes::nodebitmapheapscan::BitmapHeapScan<'_>,
    wl: bool,
) {
    buf.push_str("BITMAPHEAPSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    // C: WRITE_NODE_FIELD(bitmapqualorig). An empty List prints as `<>`.
    let bqo: Option<&[Expr]> = if n.bitmapqualorig.is_empty() {
        None
    } else {
        Some(n.bitmapqualorig.as_slice())
    };
    write_expr_opt_list(buf, "bitmapqualorig", bqo, wl);
}

fn out_tidscan(buf: &mut String, n: &::nodes::nodeindexscan::TidScan<'_>, wl: bool) {
    buf.push_str("TIDSCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_expr_opt_list(buf, "tidquals", n.tidquals.as_deref(), wl);
}

fn out_tidrangescan(
    buf: &mut String,
    n: &::nodes::nodetidrangescan::TidRangeScan<'_>,
    wl: bool,
) {
    buf.push_str("TIDRANGESCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_expr_opt_list(buf, "tidrangequals", n.tidrangequals.as_deref(), wl);
}

fn out_subqueryscan(
    buf: &mut String,
    n: &::nodes::nodeindexscan::SubqueryScan<'_>,
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
    n: &::nodes::nodeworktablescan::WorkTableScan<'_>,
    wl: bool,
) {
    buf.push_str("WORKTABLESCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_int_field(buf, "wtParam", n.wtParam);
}

fn out_ctescan(buf: &mut String, n: &::nodes::nodectescan::CteScan<'_>, wl: bool) {
    buf.push_str("CTESCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_int_field(buf, "ctePlanId", n.ctePlanId);
    write_int_field(buf, "cteParam", n.cteParam);
}

fn out_namedtuplestorescan(
    buf: &mut String,
    n: &::nodes::nodenamedtuplestorescan::NamedTuplestoreScan<'_>,
    wl: bool,
) {
    buf.push_str("NAMEDTUPLESTORESCAN");
    out_scan_fields(buf, &n.scan, "scan.", wl);
    write_string_field(buf, "enrname", n.enrname.as_ref().map(|s| s.as_str()));
}

fn out_valuesscan(buf: &mut String, n: &::nodes::nodevaluesscan::ValuesScan<'_>, wl: bool) {
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
    n: &::nodes::nodeforeigncustom::ForeignScan<'_>,
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

/// `_outWindowAgg` (outfuncs.funcs.c) — writes every field in struct order.
fn out_windowagg(buf: &mut String, n: &::nodes::nodewindowagg::WindowAgg<'_>, wl: bool) {
    buf.push_str("WINDOWAGG");
    out_plan_fields(buf, &n.plan, "plan.", wl);
    write_string_field(buf, "winname", n.winname.as_ref().map(|s| s.as_str()));
    write_uint_field(buf, "winref", n.winref);
    write_int_field(buf, "partNumCols", n.partNumCols);
    write_attrnumber_array(buf, "partColIdx", opt_slice(&n.partColIdx));
    write_oid_array(buf, "partOperators", opt_slice(&n.partOperators));
    write_oid_array(buf, "partCollations", opt_slice(&n.partCollations));
    write_int_field(buf, "ordNumCols", n.ordNumCols);
    write_attrnumber_array(buf, "ordColIdx", opt_slice(&n.ordColIdx));
    write_oid_array(buf, "ordOperators", opt_slice(&n.ordOperators));
    write_oid_array(buf, "ordCollations", opt_slice(&n.ordCollations));
    write_int_field(buf, "frameOptions", n.frameOptions);
    write_expr_field(buf, "startOffset", n.startOffset.as_deref(), wl);
    write_expr_field(buf, "endOffset", n.endOffset.as_deref(), wl);
    write_expr_opt_list(buf, "runCondition", n.runCondition.as_deref(), wl);
    write_expr_opt_list(buf, "runConditionOrig", n.runConditionOrig.as_deref(), wl);
    write_oid_field(buf, "startInRangeFunc", n.startInRangeFunc);
    write_oid_field(buf, "endInRangeFunc", n.endInRangeFunc);
    write_oid_field(buf, "inRangeColl", n.inRangeColl);
    write_bool_field(buf, "inRangeAsc", n.inRangeAsc);
    write_bool_field(buf, "inRangeNullsFirst", n.inRangeNullsFirst);
    write_bool_field(buf, "topWindow", n.topWindow);
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
    match node.node_tag() {
        ntag::T_SeqScan => { let n = node.expect_seqscan(); crate::framed(buf, |b| out_seqscan(b, n, wl)) },
        ntag::T_Material => { let n = node.expect_material(); crate::framed(buf, |b| out_material(b, n, wl)) },
        ntag::T_ProjectSet => { let n = node.expect_projectset(); crate::framed(buf, |b| out_projectset(b, n, wl)) },
        ntag::T_Result => { let n = node.expect_result(); crate::framed(buf, |b| out_result(b, n, wl)) },
        ntag::T_Append => { let n = node.expect_append(); crate::framed(buf, |b| out_append(b, n, wl)) },
        ntag::T_BitmapAnd => { let n = node.expect_bitmapand(); crate::framed(buf, |b| out_bitmapand(b, n, wl)) },
        ntag::T_Gather => { let n = node.expect_gather(); crate::framed(buf, |b| out_gather(b, n, wl)) },
        ntag::T_GatherMerge => { let n = node.expect_gathermerge(); crate::framed(buf, |b| out_gathermerge(b, n, wl)) },
        ntag::T_MergeAppend => { let n = node.expect_mergeappend(); crate::framed(buf, |b| out_mergeappend(b, n, wl)) },
        ntag::T_RecursiveUnion => { let n = node.expect_recursiveunion(); crate::framed(buf, |b| out_recursiveunion(b, n, wl)) },
        ntag::T_Group => { let n = node.expect_group(); crate::framed(buf, |b| out_group(b, n, wl)) },
        ntag::T_SetOp => { let n = node.expect_setop(); crate::framed(buf, |b| out_setop(b, n, wl)) },
        ntag::T_Unique => { let n = node.expect_unique(); crate::framed(buf, |b| out_unique(b, n, wl)) },
        ntag::T_Sort => { let n = node.expect_sort(); crate::framed(buf, |b| out_sort(b, n, wl)) },
        ntag::T_IncrementalSort => { let n = node.expect_incrementalsort(); crate::framed(buf, |b| out_incrementalsort(b, n, wl)) },
        ntag::T_Limit => { let n = node.expect_limit(); crate::framed(buf, |b| out_limit(b, n, wl)) },
        ntag::T_Agg => { let n = node.expect_agg(); crate::framed(buf, |b| out_agg(b, n, wl)) },
        ntag::T_NestLoop => { let n = node.expect_nestloop(); crate::framed(buf, |b| out_nestloop(b, n, wl)) },
        ntag::T_MergeJoin => { let n = node.expect_mergejoin(); crate::framed(buf, |b| out_mergejoin(b, n, wl)) },
        ntag::T_HashJoin => { let n = node.expect_hashjoin(); crate::framed(buf, |b| out_hashjoin(b, n, wl)) },
        ntag::T_Hash => { let n = node.expect_hash(); crate::framed(buf, |b| out_hash(b, n, wl)) },
        ntag::T_Memoize => { let n = node.expect_memoize(); crate::framed(buf, |b| out_memoize(b, n, wl)) },
        ntag::T_IndexScan => { let n = node.expect_indexscan(); crate::framed(buf, |b| out_indexscan(b, n, wl)) },
        ntag::T_IndexOnlyScan => { let n = node.expect_indexonlyscan(); crate::framed(buf, |b| out_indexonlyscan(b, n, wl)) },
        ntag::T_BitmapIndexScan => { let n = node.expect_bitmapindexscan(); crate::framed(buf, |b| out_bitmapindexscan(b, n, wl)) },
        ntag::T_BitmapHeapScan => { let n = node.expect_bitmapheapscan(); crate::framed(buf, |b| out_bitmapheapscan(b, n, wl)) },
        ntag::T_TidScan => { let n = node.expect_tidscan(); crate::framed(buf, |b| out_tidscan(b, n, wl)) },
        ntag::T_TidRangeScan => { let n = node.expect_tidrangescan(); crate::framed(buf, |b| out_tidrangescan(b, n, wl)) },
        ntag::T_SubqueryScan => { let n = node.expect_subqueryscan(); crate::framed(buf, |b| out_subqueryscan(b, n, wl)) },
        ntag::T_WorkTableScan => { let n = node.expect_worktablescan(); crate::framed(buf, |b| out_worktablescan(b, n, wl)) },
        ntag::T_CteScan => { let n = node.expect_ctescan(); crate::framed(buf, |b| out_ctescan(b, n, wl)) },
        ntag::T_NamedTuplestoreScan => { let n = node.expect_namedtuplestorescan(); crate::framed(buf, |b| out_namedtuplestorescan(b, n, wl)) },
        ntag::T_ValuesScan => { let n = node.expect_valuesscan(); crate::framed(buf, |b| out_valuesscan(b, n, wl)) },
        ntag::T_ForeignScan => { let n = node.expect_foreignscan(); crate::framed(buf, |b| out_foreignscan(b, n, wl)) },

        // ---- mirror-pg-and-panic: field-type unmodeled in this family ----
        ntag::T_ModifyTable => panic!(
            "_outModifyTable: not serialized — `rowMarks` is `List *` of \
             `PlanRowMark` (C `WRITE_NODE_FIELD(rowMarks)`), but `PlanRowMark` is \
             NOT a `Node` enum variant and has no `_outPlanRowMark`/`_readPlanRowMark` \
             writer in this model (it exists only as a typed struct in \
             `nodelockrows`), AND the carrier types it as `PgVec<PgBox<Node>>` which \
             cannot hold one — so the field can neither be written nor round-tripped. \
             Prereq keystone: promote `PlanRowMark` to a serializable `Node` variant \
             (with its `_out`/`_read`) and re-type `ModifyTable.rowMarks` to a typed \
             `PgVec<PlanRowMark>`; the remaining fields (incl. the \
             updateColnosLists/withCheckOptionLists/returningLists/mergeActionLists/\
             mergeJoinConditions list-of-lists) are all modeled and writable"
        ),
        ntag::T_WindowAgg => { let n = node.expect_windowagg(); crate::framed(buf, |b| out_windowagg(b, n, wl)) },
        ntag::T_TableFuncScan => { let n = node.expect_tablefuncscan(); crate::framed(buf, |b| out_tablefuncscan(b, n, wl)) },
        ntag::T_FunctionScan => { let n = node.expect_functionscan(); crate::framed(buf, |b| out_functionscan(b, n, wl)) },
        ntag::T_SampleScan => { let n = node.expect_samplescan(); crate::framed(buf, |b| out_samplescan(b, n, wl)) },
        ntag::T_CustomScan => panic!(
            "_outCustomScan: not serialized — `custom_private` and `methods` are \
             opaque provider pointers (the C `:methods` token reads \
             node->methods->CustomName from a static provider table)"
        ),
        _ => return false,
    }
    let _ = wl; // each writer threads wl directly
    true
}
