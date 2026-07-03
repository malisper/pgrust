// explain.c plan display. Divergence: C walks the PlanState tree (it needs
// instrumentation and run-time subplan state); ANALYZE and SubPlan display are
// loud here, so the walk reads the sealed Plan tree directly.
#![allow(non_snake_case)]

use core::fmt::Write;

use mcx::{Mcx, PgString, PgVec};
use types_error::PgResult;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::NodeList;
use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
use types_nodes::plannodes::{Plan, PlannedStmt};
use types_nodes::primnodes::Const;
use types_nodes::{Node, NodeTag};

use crate::format::{
    append, ExplainCloseGroup, ExplainOpenGroup, ExplainPropertyBool, ExplainPropertyInteger,
    ExplainPropertyList,
};
use crate::options::str_in;
use crate::state::{ExplainState, EXPLAIN_FORMAT_TEXT};

const BOOLOID: u32 = 16;
const INT4OID: u32 = 23;
const UNKNOWNOID: u32 = 705;
const NUMERICOID: u32 = 1700;

#[cold]
#[inline(never)]
fn node_gap(c_fn: &str, what: &str) -> ! {
    panic!("{c_fn} (explain.c): {what}")
}

fn plan_of(node: Node<'_>) -> &Plan<'_> {
    node.as_plan().unwrap_or_else(|| {
        node_gap(
            "ExplainNode",
            &format!("{:?} plan vocabulary unported (M2+ plan lanes)", node.node_tag()),
        )
    })
}

pub fn ExplainPrintPlan<'mcx>(
    mcx: Mcx<'mcx>,
    es: &mut ExplainState<'mcx>,
    pstmt: &'mcx PlannedStmt<'mcx>,
) -> PgResult<()> {
    es.pstmt = Some(pstmt);
    es.rtable = Some(&pstmt.rtable);
    let root = pstmt.planTree.expect("ExplainPrintPlan: PlannedStmt without planTree");
    let mut rels_used = Bitmapset::empty();
    ExplainPreScanNode(mcx, root, &mut rels_used)?;
    es.rtable_names = select_rtable_names_for_explain(mcx, &pstmt.rtable, &rels_used)?;
    // deparse_context_for_plan_tree / printed_subplans: every consumer
    // (Var/subplan deparse) is loud below.
    es.rtable_size = pstmt.rtable.len() as i32;
    for rte in pstmt.rtable.iter() {
        if rte.as_range_tbl_entry().expect("rtable holds RTEs").rtekind == RTEKind::RTE_GROUP {
            es.rtable_size -= 1;
            break;
        }
    }
    // Gather-invisible skip: Gather vocabulary is unported; plan_of is loud.
    ExplainNode(root, None, None, es)?;
    if es.settings {
        node_gap(
            "ExplainPrintSettings",
            "SETTINGS needs get_explain_guc_options (guc lane)",
        );
    }
    if es.verbose
        && pstmt.queryId != 0
        && guc_tables::backing::compute_query_id() != guc_tables::consts::COMPUTE_QUERY_ID_REGRESS
    {
        ExplainPropertyInteger("Query Identifier", None, pstmt.queryId, es);
    }
    Ok(())
}

fn ExplainPreScanNode<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    rels_used: &mut Bitmapset<'mcx>,
) -> PgResult<()> {
    if node.node_tag() == NodeTag::T_SeqScan {
        rels_used.add_member(mcx, node.as_seq_scan().unwrap().scan.scanrelid as i32)?;
    }
    let plan = plan_of(node);
    if !plan.initPlan.is_nil() {
        node_gap("ExplainPreScanNode", "initPlan walk needs PlanState (SubPlan lane)");
    }
    if let Some(l) = plan.lefttree {
        ExplainPreScanNode(mcx, l, rels_used)?;
    }
    if let Some(r) = plan.righttree {
        ExplainPreScanNode(mcx, r, rels_used)?;
    }
    Ok(())
}

// ruleutils.c set_rtable_names slice, hosted here until ruleutils lands;
// its CATALOG row stays todo and points here.
fn select_rtable_names_for_explain<'mcx>(
    mcx: Mcx<'mcx>,
    rtable: &NodeList<'mcx>,
    rels_used: &Bitmapset<'mcx>,
) -> PgResult<PgVec<'mcx, Option<&'mcx str>>> {
    let mut names: PgVec<'mcx, Option<&'mcx str>> = PgVec::new_in(mcx);
    for (i, rte_node) in rtable.iter().enumerate() {
        let rte = rte_node.as_range_tbl_entry().expect("rtable holds RTEs");
        let rtindex = i as i32 + 1;
        let refname: Option<&'mcx str> = if !rels_used.is_member(rtindex) {
            None
        } else if let Some(alias) = rte.alias {
            alias.aliasname
        } else if rte.rtekind == RTEKind::RTE_RELATION {
            match lsyscache::get_rel_name(mcx, rte.relid)? {
                Some(name) => Some(str_in(mcx, name.as_str())?),
                None => None,
            }
        } else if rte.rtekind == RTEKind::RTE_JOIN {
            None
        } else {
            rte.eref.expect("RTE without eref").aliasname
        };
        if let Some(name) = refname {
            if names.iter().any(|n| *n == Some(name)) {
                node_gap(
                    "set_rtable_names",
                    "duplicate refname needs the \"_%d\" unique-ifier (ruleutils lane)",
                );
            }
        }
        names.push(refname);
    }
    Ok(names)
}

fn plan_is_disabled(node: Node<'_>) -> bool {
    let plan = plan_of(node);
    if plan.disabled_nodes == 0 {
        return false;
    }
    // Append/MergeAppend/SubqueryScan/CustomScan child sums: vocabulary
    // unported, plan_of already panicked on those tags.
    let mut child_disabled = 0;
    if let Some(l) = plan.lefttree {
        child_disabled += plan_of(l).disabled_nodes;
    }
    if let Some(r) = plan.righttree {
        child_disabled += plan_of(r).disabled_nodes;
    }
    plan.disabled_nodes > child_disabled
}

pub fn ExplainNode<'mcx>(
    node: Node<'mcx>,
    relationship: Option<&str>,
    plan_name: Option<&str>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let plan = plan_of(node);
    let save_indent = es.indent;

    let pname = match node.node_tag() {
        NodeTag::T_Result => "Result",
        NodeTag::T_SeqScan => "Seq Scan",
        NodeTag::T_FunctionScan => "Function Scan",
        NodeTag::T_Sort => "Sort",
        NodeTag::T_Limit => "Limit",
        t => node_gap("ExplainNode", &format!("{t:?} display arm unported (M2+ plan lanes)")),
    };

    ExplainOpenGroup("Plan", if relationship.is_some() { None } else { Some("Plan") }, true, es);

    if es.format != EXPLAIN_FORMAT_TEXT {
        crate::format::nontext_gap(es, "ExplainNode");
    }
    if let Some(name) = plan_name {
        crate::format::ExplainIndentText(es);
        append!(es, "{name}\n");
        es.indent += 1;
    }
    if es.indent != 0 {
        crate::format::ExplainIndentText(es);
        append!(es, "->  ");
        es.indent += 2;
    }
    if plan.parallel_aware {
        append!(es, "Parallel ");
    }
    if plan.async_capable {
        append!(es, "Async ");
    }
    append!(es, "{pname}");
    es.indent += 1;

    if node.node_tag() == NodeTag::T_SeqScan {
        ExplainScanTarget(node.as_seq_scan().unwrap().scan.scanrelid, es)?;
    }
    if let Some(fs) = node.as_function_scan() {
        ExplainFunctionTarget(fs, es)?;
    }

    if es.costs {
        append!(
            es,
            "  (cost={:.2}..{:.2} rows={:.0} width={})",
            plan.startup_cost,
            plan.total_cost,
            plan.plan_rows,
            plan.plan_width
        );
    }

    // C reads planstate->instrument; the walk here is over the sealed Plan
    // tree, so per-node Instrumentation comes from the executor keyed by
    // plan_node_id (the fetch also runs C's forced InstrEndLoop).
    let instrument = if es.qd.is_null() {
        None
    } else {
        execmain_seams::query_desc_instrument::call(es.qd, plan.plan_node_id)
    };
    match instrument {
        Some(i) if es.analyze && i.nloops > 0.0 => {
            let nloops = i.nloops;
            let startup_ms = 1000.0 * i.startup / nloops;
            let total_ms = 1000.0 * i.total / nloops;
            let rows = i.ntuples / nloops;
            append!(es, " (actual ");
            if es.timing {
                append!(es, "time={startup_ms:.3}..{total_ms:.3} ");
            }
            append!(es, "rows={rows:.2} loops={nloops:.0})");
        }
        _ if es.analyze => append!(es, " (never executed)"),
        _ => {}
    }
    append!(es, "\n");

    let isdisabled = plan_is_disabled(node);
    if isdisabled {
        ExplainPropertyBool("Disabled", isdisabled, es);
    }

    if es.verbose {
        show_plan_tlist(node, es)?;
    }

    match node.node_tag() {
        NodeTag::T_SeqScan | NodeTag::T_FunctionScan => {
            show_scan_qual(&plan.qual, "Filter", es)?;
            filtered_count_gap(&plan.qual, es);
        }
        NodeTag::T_Result => {
            if let Some(q) = node.as_result().unwrap().resconstantqual {
                show_one_time_filter(q, es)?;
            }
            show_scan_qual(&plan.qual, "Filter", es)?;
            filtered_count_gap(&plan.qual, es);
        }
        NodeTag::T_Sort => {
            show_sort_keys(node, es)?;
            if es.analyze {
                node_gap(
                    "show_sort_info",
                    "Sort Method display needs tuplesort_get_stats (sort instrumentation lane)",
                );
            }
        }
        // Limit shows nothing extra without ANALYZE.
        NodeTag::T_Limit => {}
        _ => unreachable!(),
    }

    if es.buffers {
        if let Some(i) = &instrument {
            crate::show_buffer_usage(es, &i.bufusage);
        }
    }

    if !plan.initPlan.is_nil() {
        node_gap("ExplainNode", "InitPlan display needs PlanState (SubPlan lane)");
    }
    let haschildren = plan.lefttree.is_some() || plan.righttree.is_some();
    if haschildren {
        ExplainOpenGroup("Plans", Some("Plans"), false, es);
    }
    if let Some(l) = plan.lefttree {
        ExplainNode(l, Some("Outer"), None, es)?;
    }
    if let Some(r) = plan.righttree {
        ExplainNode(r, Some("Inner"), None, es)?;
    }
    if haschildren {
        ExplainCloseGroup("Plans", Some("Plans"), false, es);
    }

    es.indent = save_indent;
    ExplainCloseGroup("Plan", if relationship.is_some() { None } else { Some("Plan") }, true, es);
    Ok(())
}

fn show_plan_tlist<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let plan = plan_of(node);
    if plan.targetlist.is_nil() {
        return Ok(());
    }
    // Append/MergeAppend/RecursiveUnion/ForeignScan suppression arms: those
    // tags already panic in plan_of.
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1;
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for tle_node in plan.targetlist.iter() {
        let tle = tle_node.as_target_entry().expect("targetlist holds TargetEntries");
        result.push(deparse_expression_minimal(mcx, tle.expr, useprefix)?);
    }
    ExplainPropertyList("Output", &result, es);
    Ok(())
}

// show_sort_keys -> show_sort_group_keys (explain.c), Var-only sort keys.
fn show_sort_keys<'mcx>(node: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let sort = node.as_sort().expect("Sort node");
    if sort.numCols <= 0 {
        return Ok(());
    }
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let mut result: PgVec<'mcx, PgString<'mcx>> = PgVec::new_in(mcx);
    for keyno in 0..sort.numCols as usize {
        let resno = sort.sortColIdx[keyno];
        let tle = get_tle_by_resno(&sort.plan.targetlist, resno)
            .unwrap_or_else(|| node_gap("show_sort_group_keys", "no tlist entry for key column"));
        let mut buf = PgString::new_in(mcx);
        deparse_plan_var(sort.plan.lefttree, tle.expr, useprefix, es, &mut buf)?;
        show_sortorder_options(
            &mut buf,
            tle.expr,
            sort.sortOperators[keyno],
            sort.collations[keyno],
            sort.nullsFirst[keyno],
        )?;
        result.push(buf);
    }
    ExplainPropertyList("Sort Key", &result, es);
    Ok(())
}

fn get_tle_by_resno<'a, 'mcx>(
    tlist: &'a NodeList<'mcx>,
    resno: i16,
) -> Option<&'mcx types_nodes::primnodes::TargetEntry<'mcx>> {
    tlist
        .iter()
        .map(|n| n.as_target_entry().expect("targetlist holds TargetEntries"))
        .find(|tle| tle.resno == resno)
}

// get_rule_expr/get_variable (ruleutils.c) reduced to the plan-tree Var walk:
// OUTER_VAR resolves through the child tlist, base Vars through eref colnames.
fn deparse_plan_var<'mcx>(
    child: Option<Node<'mcx>>,
    expr: Node<'mcx>,
    useprefix: bool,
    es: &ExplainState<'mcx>,
    buf: &mut PgString<'mcx>,
) -> PgResult<()> {
    let Some(var) = expr.as_var() else {
        node_gap(
            "deparse_expression",
            &format!("{:?} deparse unported (ruleutils lane)", expr.node_tag()),
        );
    };
    if var.varno == types_nodes::primnodes::OUTER_VAR {
        let child = child.unwrap_or_else(|| node_gap("get_variable", "OUTER_VAR without child"));
        let child_plan = plan_of(child);
        let tle = get_tle_by_resno(&child_plan.targetlist, var.varattno)
            .unwrap_or_else(|| node_gap("get_variable", "bogus varattno for OUTER_VAR"));
        return deparse_plan_var(child_plan.lefttree, tle.expr, useprefix, es, buf);
    }
    if var.varno <= 0 || var.varno as usize > es.rtable_size as usize {
        node_gap("get_variable", "INNER_VAR/INDEX_VAR deparse unported (ruleutils lane)");
    }
    let rte: &RangeTblEntry<'_> = es
        .rtable
        .expect("deparse before ExplainPrintPlan")
        .nth(var.varno as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable holds RTEs");
    let eref = rte.eref.expect("RTE without eref");
    if useprefix {
        let refname = es.rtable_names[var.varno as usize - 1]
            .or(eref.aliasname)
            .expect("deparsed Var's RTE has a refname");
        buf.try_push_str(quote_identifier(refname))?;
        buf.try_push('.')?;
    }
    debug_assert!(var.varattno > 0, "system/whole-row Var deparse is a loud upstream lane");
    let colname = eref
        .colnames
        .nth(var.varattno as usize - 1)
        .as_string()
        .expect("eref colnames hold String nodes")
        .sval;
    buf.try_push_str(quote_identifier(colname))?;
    Ok(())
}

// show_sortorder_options (explain.c); USING and COLLATE arms are loud.
fn show_sortorder_options(
    buf: &mut PgString<'_>,
    sortexpr: Node<'_>,
    sort_operator: types_core::primitive::Oid,
    collation: types_core::primitive::Oid,
    nulls_first: bool,
) -> PgResult<()> {
    let sortcoltype = execscan_expr_type(sortexpr);
    let typentry = typcache::lookup_type_cache(
        sortcoltype,
        typcache::TYPECACHE_LT_OPR | typcache::TYPECACHE_GT_OPR,
    )?;
    if collation != types_core::primitive::InvalidOid
        && collation != lsyscache::typ::get_typcollation(sortcoltype)?
    {
        node_gap("show_sortorder_options", "COLLATE needs generate_collation_name");
    }
    let reverse = if sort_operator == typentry.lt_opr() {
        false
    } else if sort_operator == typentry.gt_opr() {
        buf.try_push_str(" DESC")?;
        true
    } else {
        node_gap("show_sortorder_options", "USING <op> needs get_opname + opfamily probe");
    };
    if nulls_first != reverse {
        buf.try_push_str(if nulls_first { " NULLS FIRST" } else { " NULLS LAST" })?;
    }
    Ok(())
}

// exprType over the sort-key shapes this display lane can carry.
fn execscan_expr_type(node: Node<'_>) -> types_core::primitive::Oid {
    match node.as_var() {
        Some(v) => v.vartype,
        None => node_gap("exprType", "non-Var sort key (nodeFuncs lane)"),
    }
}

// show_upper_qual on Result.resconstantqual: C stores it as a bare expression
// (not an implicit-AND list).
fn show_one_time_filter<'mcx>(qual: Node<'mcx>, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let s = deparse_expression_minimal(mcx, qual, useprefix)?;
    crate::format::ExplainPropertyText("One-Time Filter", s.as_str(), es);
    Ok(())
}

// show_instrumentation_count's nfiltered read: the executor never counts
// qual-filtered tuples (InstrCountFiltered, execScan.c), so printing would be
// silently wrong whenever a filter removed rows.
fn filtered_count_gap(qual: &NodeList<'_>, es: &ExplainState<'_>) {
    if es.analyze && !qual.is_nil() {
        node_gap(
            "show_instrumentation_count",
            "Rows Removed by Filter needs nfiltered counting (InstrCountFiltered, execScan.c)",
        );
    }
}

fn show_scan_qual<'mcx>(
    qual: &NodeList<'mcx>,
    qlabel: &str,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    if qual.is_nil() {
        return Ok(());
    }
    if qual.len() > 1 {
        node_gap("show_qual", "multi-item qual needs make_ands_explicit (ruleutils lane)");
    }
    let mcx = es.str.allocator();
    let useprefix = es.rtable_size > 1 || es.verbose;
    let s = deparse_expression_minimal(mcx, qual.nth(0), useprefix)?;
    crate::format::ExplainPropertyText(qlabel, s.as_str(), es);
    Ok(())
}

// ruleutils.c deparse_expression slice: Const deparse is complete; every
// other node tag is loud. Its CATALOG row stays todo and points here.
fn deparse_expression_minimal<'mcx>(
    mcx: Mcx<'mcx>,
    node: Node<'mcx>,
    _useprefix: bool,
) -> PgResult<PgString<'mcx>> {
    let Some(c) = node.as_const() else {
        node_gap(
            "deparse_expression",
            &format!("{:?} deparse unported (ruleutils lane)", node.node_tag()),
        );
    };
    let mut buf = PgString::new_in(mcx);
    get_const_expr(c, &mut buf, 0)?;
    Ok(buf)
}

pub(crate) fn get_const_expr(c: &Const, buf: &mut PgString<'_>, showtype: i32) -> PgResult<()> {
    if c.constisnull {
        buf.try_push_str("NULL")?;
        if showtype >= 0 {
            let t = format_type::format_type_with_typemod(c.consttype, c.consttypmod)?;
            write!(buf, "::{t}").expect("PgString write");
            get_const_collation(c, buf)?;
        }
        return Ok(());
    }

    let (typoutput, _typisvarlena) = lsyscache::typ::getTypeOutputInfo(c.consttype)?;
    let mut finfo = fmgr_seams::fmgr_info::call(typoutput)?;
    let mut fcinfo = types_fmgr::LocalFcinfo::<1>::fresh(types_core::primitive::InvalidOid);
    fcinfo.set_arg(0, c.constvalue);
    let out = finfo.invoke(&mut fcinfo)?;
    // SAFETY: text output fns return a NUL-terminated cstring datum (the
    // contract C's DatumGetCString trusts).
    let extval = unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) };
    let extval = core::str::from_utf8(extval.to_bytes()).expect("typoutput yields server encoding");

    let mut needlabel = false;
    match c.consttype {
        // Negative INT4 deparses as '-nnn'::integer so it re-parses as a
        // constant, not constant-plus-operator (INT_MIN breaks the paren
        // form).
        INT4OID => {
            if !extval.starts_with('-') {
                buf.try_push_str(extval)?;
            } else {
                write!(buf, "'{extval}'").expect("PgString write");
                needlabel = true;
            }
        }
        NUMERICOID => {
            if extval.as_bytes()[0].is_ascii_digit() && extval.contains(['e', 'E', '.']) {
                buf.try_push_str(extval)?;
            } else {
                write!(buf, "'{extval}'").expect("PgString write");
                needlabel = true;
            }
        }
        BOOLOID => buf.try_push_str(if extval == "t" { "true" } else { "false" })?,
        _ => simple_quote_literal(buf, extval)?,
    }

    if showtype < 0 {
        return Ok(());
    }

    match c.consttype {
        BOOLOID | UNKNOWNOID => needlabel = false,
        INT4OID => {}
        NUMERICOID => needlabel |= c.consttypmod >= 0,
        _ => needlabel = true,
    }
    if needlabel || showtype > 0 {
        let t = format_type::format_type_with_typemod(c.consttype, c.consttypmod)?;
        write!(buf, "::{t}").expect("PgString write");
    }
    get_const_collation(c, buf)?;
    Ok(())
}

fn get_const_collation(c: &Const, _buf: &mut PgString<'_>) -> PgResult<()> {
    if c.constcollid != types_core::primitive::InvalidOid {
        let typcollation = lsyscache::typ::get_typcollation(c.consttype)?;
        if c.constcollid != typcollation {
            node_gap(
                "get_const_collation",
                "COLLATE deparse needs generate_collation_name (ruleutils lane)",
            );
        }
    }
    Ok(())
}

fn simple_quote_literal(buf: &mut PgString<'_>, val: &str) -> PgResult<()> {
    let std_strings = guc_tables::vars::standard_conforming_strings.read();
    buf.try_push('\'')?;
    for ch in val.chars() {
        if ch == '\'' || (ch == '\\' && !std_strings) {
            buf.try_push(ch)?;
        }
        buf.try_push(ch)?;
    }
    buf.try_push('\'')?;
    Ok(())
}

fn ExplainScanTarget(scanrelid: types_core::Index, es: &mut ExplainState<'_>) -> PgResult<()> {
    ExplainTargetRel(scanrelid, es)
}

// ExplainTargetRel's T_FunctionScan arm: objectname is the function's name
// when the RTE holds exactly one FuncExpr item.
fn ExplainFunctionTarget<'mcx>(
    fs: &types_nodes::plannodes::FunctionScan<'mcx>,
    es: &mut ExplainState<'mcx>,
) -> PgResult<()> {
    let mcx = es.str.allocator();
    let rti = fs.scan.scanrelid;
    let rte: &RangeTblEntry<'_> = es
        .rtable
        .expect("ExplainFunctionTarget before ExplainPrintPlan")
        .nth(rti as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable holds RTEs");
    debug_assert_eq!(rte.rtekind, RTEKind::RTE_FUNCTION);
    let mut objectname = None;
    if rte.functions.len() == 1 {
        let rtfunc = rte
            .functions
            .nth(0)
            .as_range_tbl_function()
            .expect("functions cell");
        if let Some(fe) = rtfunc.funcexpr.and_then(|n| n.as_func_expr()) {
            objectname = lsyscache::get_func_name(mcx, fe.funcid)?;
        }
    }
    let objectname = objectname.as_ref().map(|s| s.as_str());
    if es.verbose {
        node_gap(
            "ExplainTargetRel",
            "VERBOSE schema qualification needs get_namespace_name_or_temp (lsyscache lane)",
        );
    }
    let refname = es.rtable_names[rti as usize - 1]
        .or_else(|| rte.eref.expect("RTE without eref").aliasname)
        .expect("scan RTE has a refname");
    append!(es, " on");
    if let Some(obj) = objectname {
        append!(es, " {}", quote_identifier(obj));
    }
    if objectname != Some(refname) {
        append!(es, " {}", quote_identifier(refname));
    }
    Ok(())
}

fn ExplainTargetRel<'mcx>(rti: types_core::Index, es: &mut ExplainState<'mcx>) -> PgResult<()> {
    let mcx = es.str.allocator();
    let rte: &RangeTblEntry<'_> = es
        .rtable
        .expect("ExplainTargetRel before ExplainPrintPlan")
        .nth(rti as usize - 1)
        .as_range_tbl_entry()
        .expect("rtable holds RTEs");
    debug_assert_eq!(rte.rtekind, RTEKind::RTE_RELATION);
    let objectname = lsyscache::get_rel_name(mcx, rte.relid)?;
    let objectname = objectname.as_ref().map(|s| s.as_str());
    if es.verbose {
        node_gap(
            "ExplainTargetRel",
            "VERBOSE schema qualification needs get_namespace_name_or_temp (lsyscache lane)",
        );
    }
    let refname = es.rtable_names[rti as usize - 1]
        .or_else(|| rte.eref.expect("RTE without eref").aliasname)
        .expect("scan RTE has a refname");
    append!(es, " on");
    if let Some(obj) = objectname {
        append!(es, " {}", quote_identifier(obj));
    }
    if objectname != Some(refname) {
        append!(es, " {}", quote_identifier(refname));
    }
    Ok(())
}

// ruleutils.c quote_identifier slice: bare-safe identifiers pass through,
// anything that C would quote is loud.
fn quote_identifier(ident: &str) -> &str {
    let bytes = ident.as_bytes();
    let safe = !bytes.is_empty()
        && (bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|&b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        && !crate::gucs::quote_all_identifiers()
        && {
            let kwnum = keywords::ScanKeywordLookup(bytes, &keywords::ScanKeywords);
            kwnum < 0
                || keywords::ScanKeywordCategories[kwnum as usize]
                    == keywords::KeywordCategory::Unreserved
        };
    if !safe {
        node_gap("quote_identifier", "quoted-identifier form unported (ruleutils lane)");
    }
    ident
}
