//! allpaths.c subquery qual-pushdown slice: subquery_is_pushdown_safe,
//! qual_is_pushdown_safe, subquery_push_qual, find_window_run_conditions,
//! remove_unused_subquery_outputs.

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_nodes::parsenodes::{Query, RangeTblEntry, SetOperation};
use types_nodes::primnodes::{FromExpr, OpExpr, TargetEntry};
use types_nodes::{Bitmapset, Node, NodeList, NodeTag};
use types_pathnodes::{RelId, RinfoId, VOLATILITY_NOVOLATILE, VOLATILITY_VOLATILE};

use crate::run::PlannerRun;

const UNSAFE_HAS_VOLATILE_FUNC: u8 = 1 << 0;
const UNSAFE_HAS_SET_FUNC: u8 = 1 << 1;
const UNSAFE_NOTIN_DISTINCTON_CLAUSE: u8 = 1 << 2;
const UNSAFE_NOTIN_PARTITIONBY_CLAUSE: u8 = 1 << 3;
const UNSAFE_TYPE_MISMATCH: u8 = 1 << 4;

const FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER: i32 = -7;

// unsafe_flags is indexed by 1-based resno; slot 0 is C's unused pad.
struct PushdownSafetyInfo<'mcx> {
    unsafe_flags: PgVec<'mcx, u8>,
    unsafe_volatile: bool,
    unsafe_leaky: bool,
}

enum PushdownSafe {
    Unsafe,
    Safe,
    WindowclauseRuncond,
}

// The qual-pushdown block of set_subquery_pathlist (allpaths.c). The safety
// analysis walks the shared original (it is read-only and the copy is
// content-identical at this point); pushes mutate only sub_parse.
pub(crate) fn pushdown_quals_into_subquery<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    rti: usize,
    rte: &'mcx RangeTblEntry<'mcx>,
    orig: &'mcx Query<'mcx>,
    sub_parse: &mut Query<'mcx>,
    run_cond_attrs: &mut Bitmapset<'mcx>,
) -> PgResult<()> {
    if run.root.rel(rel).baserestrictinfo.is_empty() {
        return Ok(());
    }
    let mcx = run.mcx;
    let mut safety = PushdownSafetyInfo {
        unsafe_flags: mcx::vec_from_elem_in(mcx, 0u8, orig.targetList.len() + 1),
        unsafe_volatile: false,
        unsafe_leaky: rte.security_barrier,
    };
    if !subquery_is_pushdown_safe(mcx, orig, orig, &mut safety)? {
        return Ok(());
    }

    let rinfos = crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(rel).baserestrictinfo);
    let mut upperrestrictlist: PgVec<'mcx, RinfoId> = PgVec::new_in(mcx);
    for &rid in rinfos.iter() {
        // Pseudoconstant clauses stay above as gating quals.
        if run.root.rinfo(rid).pseudoconstant {
            upperrestrictlist.push(rid);
            continue;
        }
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        match qual_is_pushdown_safe(run, orig, rti, rid, clause, &safety)? {
            PushdownSafe::Safe => {
                subquery_push_qual(mcx, sub_parse, rte, rti, clause)?;
            }
            PushdownSafe::WindowclauseRuncond => {
                if !sub_parse.hasWindowFuncs
                    || check_and_push_window_quals(run, sub_parse, clause, run_cond_attrs)?
                {
                    upperrestrictlist.push(rid);
                }
            }
            PushdownSafe::Unsafe => {
                upperrestrictlist.push(rid);
            }
        }
    }
    run.root.rel_mut(rel).baserestrictinfo = upperrestrictlist;
    Ok(())
}

fn subquery_is_pushdown_safe<'mcx>(
    mcx: Mcx<'mcx>,
    subquery: &'mcx Query<'mcx>,
    top: &'mcx Query<'mcx>,
    safety: &mut PushdownSafetyInfo<'mcx>,
) -> PgResult<bool> {
    if subquery.limitOffset.is_some() || subquery.limitCount.is_some() {
        return Ok(false);
    }
    if !subquery.groupClause.is_nil() && !subquery.groupingSets.is_nil() {
        return Ok(false);
    }
    if !subquery.distinctClause.is_nil() || subquery.hasWindowFuncs || subquery.hasTargetSRFs {
        safety.unsafe_volatile = true;
    }
    if subquery.setOperations.is_none() {
        check_output_expressions(mcx, subquery, safety)?;
    }
    if core::ptr::eq(subquery, top) {
        if let Some(setop) = subquery.setOperations {
            if !recurse_pushdown_safe(mcx, setop, top, safety)? {
                return Ok(false);
            }
        }
    } else {
        // Setop component must not have more components (too weird).
        if subquery.setOperations.is_some() {
            return Ok(false);
        }
        let topop = top
            .setOperations
            .expect("setop component under a setop top")
            .as_set_operation_stmt()
            .expect("topquery setOperations is a SetOperationStmt");
        compare_tlist_datatypes(&subquery.targetList, &topop.colTypes, safety);
    }
    Ok(true)
}

fn recurse_pushdown_safe<'mcx>(
    mcx: Mcx<'mcx>,
    set_op: Node<'mcx>,
    top: &'mcx Query<'mcx>,
    safety: &mut PushdownSafetyInfo<'mcx>,
) -> PgResult<bool> {
    match set_op.node_tag() {
        NodeTag::T_RangeTblRef => {
            let rtindex = set_op.as_range_tbl_ref().unwrap().rtindex;
            let rte = top
                .rtable
                .nth(rtindex as usize - 1)
                .as_range_tbl_entry()
                .expect("rtable cell is a RangeTblEntry");
            let subquery = rte.subquery.expect("setop component has a subquery");
            subquery_is_pushdown_safe(mcx, subquery, top, safety)
        }
        NodeTag::T_SetOperationStmt => {
            let op = set_op.as_set_operation_stmt().unwrap();
            if op.op == SetOperation::SETOP_EXCEPT {
                return Ok(false);
            }
            if !recurse_pushdown_safe(mcx, op.larg.expect("setop larg"), top, safety)? {
                return Ok(false);
            }
            recurse_pushdown_safe(mcx, op.rarg.expect("setop rarg"), top, safety)
        }
        other => panic!("recurse_pushdown_safe (allpaths.c): unrecognized node {other:?}"),
    }
}

fn check_output_expressions<'mcx>(
    mcx: Mcx<'mcx>,
    subquery: &'mcx Query<'mcx>,
    safety: &mut PushdownSafetyInfo<'mcx>,
) -> PgResult<()> {
    // Grouping Vars hide the underlying grouping expressions (which may be
    // volatile or set-returning); expand them before inspection. Lower-level
    // references never expand to volatile/SRF expressions (such subqueries
    // are never pulled up), so no recursive expansion is needed.
    let mut flattened: PgVec<'mcx, Node<'mcx>> = PgVec::new_in(mcx);
    if subquery.hasGroupRTE {
        for tle_node in subquery.targetList.iter() {
            let tle = tle_node.as_target_entry().expect("tlist cell is a TargetEntry");
            flattened.push(vars::flatten_group_exprs(mcx, subquery, tle.expr)?);
        }
    }

    for (i, tle_node) in subquery.targetList.iter().enumerate() {
        let tle = tle_node.as_target_entry().expect("tlist cell is a TargetEntry");
        if tle.resjunk {
            continue;
        }
        let resno = tle.resno as usize;
        let texpr = if subquery.hasGroupRTE { flattened[i] } else { tle.expr };

        if subquery.hasTargetSRFs
            && safety.unsafe_flags[resno] & UNSAFE_HAS_SET_FUNC == 0
            && coerce::expression_returns_set(texpr)
        {
            safety.unsafe_flags[resno] |= UNSAFE_HAS_SET_FUNC;
            continue;
        }
        if safety.unsafe_flags[resno] & UNSAFE_HAS_VOLATILE_FUNC == 0
            && clauses::contain_volatile_functions(texpr)?
        {
            safety.unsafe_flags[resno] |= UNSAFE_HAS_VOLATILE_FUNC;
            continue;
        }
        if subquery.hasDistinctOn
            && safety.unsafe_flags[resno] & UNSAFE_NOTIN_DISTINCTON_CLAUSE == 0
            && !target_is_in_sort_list(tle, &subquery.distinctClause)
        {
            safety.unsafe_flags[resno] |= UNSAFE_NOTIN_DISTINCTON_CLAUSE;
            continue;
        }
        // C tests the DISTINCT-ON bit here, not the PARTITION-BY bit.
        if subquery.hasWindowFuncs
            && safety.unsafe_flags[resno] & UNSAFE_NOTIN_DISTINCTON_CLAUSE == 0
            && !target_is_in_all_partition_lists(tle, subquery)
        {
            safety.unsafe_flags[resno] |= UNSAFE_NOTIN_PARTITIONBY_CLAUSE;
            continue;
        }
    }
    Ok(())
}

// Only typmod difference allowed between setop input and output is specific
// vs -1, which needs no coercion, so types alone decide safety.
fn compare_tlist_datatypes<'mcx>(
    tlist: &NodeList<'mcx>,
    col_types: &types_nodes::OidList<'mcx>,
    safety: &mut PushdownSafetyInfo<'mcx>,
) {
    let mut col_iter = col_types.iter();
    for tle_node in tlist.iter() {
        let tle = tle_node.as_target_entry().expect("tlist cell is a TargetEntry");
        if tle.resjunk {
            continue;
        }
        let Some(ct) = col_iter.next() else {
            panic!("wrong number of tlist entries")
        };
        if crate::costsize::expr_type_typmod(tle.expr).0 != ct {
            safety.unsafe_flags[tle.resno as usize] |= UNSAFE_TYPE_MISMATCH;
        }
    }
    assert!(col_iter.next().is_none(), "wrong number of tlist entries");
}

fn target_is_in_all_partition_lists(tle: &TargetEntry<'_>, query: &Query<'_>) -> bool {
    for wc_node in query.windowClause.iter() {
        let wc = wc_node.as_window_clause().expect("windowClause cell is a WindowClause");
        if !target_is_in_sort_list(tle, &wc.partitionClause) {
            return false;
        }
    }
    true
}

// targetIsInSortList (parse_clause.c), InvalidOid-sortop arm.
fn target_is_in_sort_list(tle: &TargetEntry<'_>, sort_list: &NodeList<'_>) -> bool {
    let tle_ref = tle.ressortgroupref;
    if tle_ref == 0 {
        return false;
    }
    sort_list.iter().any(|n| {
        n.as_sort_group_clause().expect("sortlist cell is a SortGroupClause").tleSortGroupRef
            == tle_ref
    })
}

fn qual_is_pushdown_safe<'mcx>(
    run: &mut PlannerRun<'mcx>,
    subquery: &'mcx Query<'mcx>,
    rti: usize,
    rid: RinfoId,
    clause: Node<'mcx>,
    safety: &PushdownSafetyInfo<'mcx>,
) -> PgResult<PushdownSafe> {
    let mut safe = PushdownSafe::Safe;

    if clauses::contain_subplans(clause)? {
        return Ok(PushdownSafe::Unsafe);
    }
    if safety.unsafe_volatile && rinfo_contains_volatile(run, rid)? {
        return Ok(PushdownSafe::Unsafe);
    }
    if safety.unsafe_leaky && clauses::contain_leaked_vars(clause)? {
        return Ok(PushdownSafe::Unsafe);
    }

    let vars_list = vars::pull_var_clause(run.mcx, clause, vars::PVC_INCLUDE_PLACEHOLDERS)?;
    for v in vars_list.iter() {
        // PlaceHolderVars punt (C's XXX arm).
        let Some(var) = v.as_var() else {
            safe = PushdownSafe::Unsafe;
            break;
        };
        // Lateral references punt: subquery_push_qual can't convert them
        // into outer references.
        if var.varno != rti as i32 {
            safe = PushdownSafe::Unsafe;
            break;
        }
        debug_assert!(var.varattno >= 0, "subqueries have no system columns");
        if var.varattno == 0 {
            safe = PushdownSafe::Unsafe;
            break;
        }
        let flags = safety.unsafe_flags[var.varattno as usize];
        if flags != 0 {
            if flags
                & (UNSAFE_HAS_VOLATILE_FUNC
                    | UNSAFE_HAS_SET_FUNC
                    | UNSAFE_NOTIN_DISTINCTON_CLAUSE
                    | UNSAFE_TYPE_MISMATCH)
                != 0
            {
                safe = PushdownSafe::Unsafe;
                break;
            }
            // UNSAFE_NOTIN_PARTITIONBY_CLAUSE is ok for run conditions; keep
            // scanning for an outright-unsafe Var.
            safe = PushdownSafe::WindowclauseRuncond;
        }
    }

    // Check point 6: past a grouping layer (DISTINCT/DISTINCT ON, window
    // PARTITION BY, or a grouping set operation), the clause must not apply
    // a different equivalence relation to a grouping column than the
    // grouping uses.
    if matches!(safe, PushdownSafe::Safe)
        && (subquery.hasWindowFuncs
            || !subquery.distinctClause.is_nil()
            || subquery.setOperations.map_or(false, setop_has_grouping))
    {
        let conflict = clauses::expression_has_grouping_conflict(clause, &mut |var| {
            if var.varlevelsup != 0 {
                return Ok(types_core::InvalidOid);
            }
            let eqop = subquery_column_grouping_eqop(subquery, var.varattno)?;
            // qual_is_pushdown_safe ensures any level-0 subquery Var that
            // reaches us references a grouping column.
            debug_assert!(eqop != types_core::InvalidOid);
            Ok(eqop)
        })?;
        if conflict {
            safe = PushdownSafe::Unsafe;
        }
    }

    Ok(safe)
}

// subquery_column_grouping_eqop (allpaths.c): the eqop the subquery groups the
// output column under via distinctClause, every window's partitionClause, or
// a grouping set-op node; InvalidOid when the column is not grouping-relevant.
fn subquery_column_grouping_eqop(
    subquery: &Query<'_>,
    attno: i16,
) -> PgResult<types_core::Oid> {
    if attno <= 0 || attno as usize > subquery.targetList.len() {
        return Ok(types_core::InvalidOid);
    }
    let tle = subquery
        .targetList
        .nth(attno as usize - 1)
        .as_target_entry()
        .expect("tlist cell is a TargetEntry");

    for n in &subquery.distinctClause {
        let sgc = n.as_sort_group_clause().expect("distinctClause cell");
        if sgc.tleSortGroupRef == tle.ressortgroupref {
            return Ok(sgc.eqop);
        }
    }

    if subquery.hasWindowFuncs && !subquery.windowClause.is_nil() {
        let mut eqop = types_core::InvalidOid;
        let mut in_all_windows = true;
        for wc_node in &subquery.windowClause {
            let wc = wc_node.as_window_clause().expect("windowClause cell");
            match wc.partitionClause.iter().find_map(|n| {
                let sgc = n.as_sort_group_clause().expect("partitionClause cell");
                (sgc.tleSortGroupRef == tle.ressortgroupref).then_some(sgc.eqop)
            }) {
                Some(e) => eqop = e,
                None => {
                    in_all_windows = false;
                    break;
                }
            }
        }
        if in_all_windows {
            return Ok(eqop);
        }
    }

    if subquery.setOperations.is_some() {
        return Ok(setop_column_grouping_eqop(subquery.setOperations, attno));
    }

    Ok(types_core::InvalidOid)
}

// setop_column_grouping_eqop (allpaths.c): groupClauses is positional, element
// N-1 for output column N (makeSortGroupClauseForSetOp); an entirely-UNION-ALL
// tree yields InvalidOid.
fn setop_column_grouping_eqop(setop: Option<Node<'_>>, attno: i16) -> types_core::Oid {
    let Some(op) = setop.and_then(|n| n.as_set_operation_stmt()) else {
        return types_core::InvalidOid;
    };
    if !op.groupClauses.is_nil() && attno >= 1 && attno as usize <= op.groupClauses.len() {
        return op
            .groupClauses
            .nth(attno as usize - 1)
            .as_sort_group_clause()
            .expect("setop groupClauses cell")
            .eqop;
    }
    let eqop = setop_column_grouping_eqop(op.larg, attno);
    if eqop != types_core::InvalidOid {
        return eqop;
    }
    setop_column_grouping_eqop(op.rarg, attno)
}

// setop_has_grouping (allpaths.c).
fn setop_has_grouping(setop: Node<'_>) -> bool {
    let Some(op) = setop.as_set_operation_stmt() else {
        return false;
    };
    !op.groupClauses.is_nil()
        || op.larg.map_or(false, setop_has_grouping)
        || op.rarg.map_or(false, setop_has_grouping)
}

// contain_volatile_functions((Node *) rinfo): the walker's RestrictInfo arm
// answers from rinfo->has_volatile and caches a computed verdict.
fn rinfo_contains_volatile(run: &mut PlannerRun<'_>, rid: RinfoId) -> PgResult<bool> {
    match run.root.rinfo(rid).has_volatile {
        VOLATILITY_NOVOLATILE => Ok(false),
        VOLATILITY_VOLATILE => Ok(true),
        _ => {
            let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
            let volatil = clauses::contain_volatile_functions(clause)?;
            run.root.rinfo_mut(rid).has_volatile =
                if volatil { VOLATILITY_VOLATILE } else { VOLATILITY_NOVOLATILE };
            Ok(volatil)
        }
    }
}

fn subquery_push_qual<'mcx>(
    mcx: Mcx<'mcx>,
    subquery: &mut Query<'mcx>,
    rte: &'mcx RangeTblEntry<'mcx>,
    rti: usize,
    qual: Node<'mcx>,
) -> PgResult<()> {
    if let Some(setop) = subquery.setOperations {
        return recurse_push_qual(mcx, setop, subquery, rte, rti, qual);
    }

    // Replace Vars (subquery outputs) with copies of the tlist expressions;
    // the copy also gives each setop component its own qual tree. Uplevel
    // Vars were already turned into Params.
    let qual_copy = rewrite_manip::copy_node(mcx, qual)?;
    let mut has_sublinks = subquery.hasSubLinks;
    let new_qual = rewrite_manip::ReplaceVarsFromTargetList(
        mcx,
        qual_copy,
        rti as i32,
        0,
        rte,
        &subquery.targetList,
        subquery.resultRelation,
        rewrite_manip::ReplaceVarsNoMatchOption::ReportError,
        Some(&mut has_sublinks),
    )?;
    subquery.hasSubLinks = has_sublinks;

    // Grouping/aggregation: the qual refers to group-result rows, so HAVING.
    if subquery.hasAggs
        || !subquery.groupClause.is_nil()
        || !subquery.groupingSets.is_nil()
        || subquery.havingQual.is_some()
    {
        subquery.havingQual =
            Some(rewrite_manip::make_and_qual(mcx, subquery.havingQual, new_qual)?);
    } else {
        let jt = subquery.jointree.expect("subquery has a jointree");
        let quals = rewrite_manip::make_and_qual(mcx, jt.quals, new_qual)?;
        subquery.jointree = Some(mcx::alloc_leak_in(
            mcx,
            FromExpr { fromlist: jt.fromlist.clone_in(mcx)?, quals: Some(quals) },
        )?);
    }
    Ok(())
}

fn recurse_push_qual<'mcx>(
    mcx: Mcx<'mcx>,
    set_op: Node<'mcx>,
    top: &mut Query<'mcx>,
    rte: &'mcx RangeTblEntry<'mcx>,
    rti: usize,
    qual: Node<'mcx>,
) -> PgResult<()> {
    match set_op.node_tag() {
        NodeTag::T_RangeTblRef => {
            let rtindex = set_op.as_range_tbl_ref().unwrap().rtindex;
            let idx = rtindex as usize - 1;
            let subrte = top
                .rtable
                .nth(idx)
                .as_range_tbl_entry()
                .expect("rtable cell is a RangeTblEntry");
            // The component Query is shared with the original parsetree:
            // push into a copy and swap a rebuilt RTE into the copied rtable.
            let mut subq = crate::subselect::query_cells_copy(
                mcx,
                subrte.subquery.expect("setop component has a subquery"),
            )?;
            subquery_push_qual(mcx, &mut subq, rte, rti, qual)?;
            let new_rte = rte_copy_with_subquery(mcx, subrte, mcx::alloc_leak_in(mcx, subq)?)?;
            top.rtable.as_mut_slice()[idx] = new_rte;
            Ok(())
        }
        NodeTag::T_SetOperationStmt => {
            let op = set_op.as_set_operation_stmt().unwrap();
            recurse_push_qual(mcx, op.larg.expect("setop larg"), top, rte, rti, qual)?;
            recurse_push_qual(mcx, op.rarg.expect("setop rarg"), top, rte, rti, qual)
        }
        other => panic!("recurse_push_qual (allpaths.c): unrecognized node {other:?}"),
    }
}

// Struct-level RTE copy (sub-nodes stay shared) carrying a replaced subquery.
fn rte_copy_with_subquery<'mcx>(
    mcx: Mcx<'mcx>,
    rte: &RangeTblEntry<'mcx>,
    subquery: &'mcx Query<'mcx>,
) -> PgResult<Node<'mcx>> {
    Node::mk(
        mcx,
        RangeTblEntry {
            alias: rte.alias,
            eref: rte.eref,
            rtekind: rte.rtekind,
            relid: rte.relid,
            inh: rte.inh,
            relkind: rte.relkind,
            rellockmode: rte.rellockmode,
            perminfoindex: rte.perminfoindex,
            tablesample: rte.tablesample,
            subquery: Some(subquery),
            security_barrier: rte.security_barrier,
            jointype: rte.jointype,
            joinmergedcols: rte.joinmergedcols,
            joinaliasvars: rte.joinaliasvars.clone_in(mcx)?,
            joinleftcols: rte.joinleftcols.clone_in(mcx)?,
            joinrightcols: rte.joinrightcols.clone_in(mcx)?,
            join_using_alias: rte.join_using_alias,
            functions: rte.functions.clone_in(mcx)?,
            funcordinality: rte.funcordinality,
            tablefunc: rte.tablefunc,
            values_lists: rte.values_lists.clone_in(mcx)?,
            ctename: rte.ctename,
            ctelevelsup: rte.ctelevelsup,
            self_reference: rte.self_reference,
            coltypes: rte.coltypes.clone_in(mcx)?,
            coltypmods: rte.coltypmods.clone_in(mcx)?,
            colcollations: rte.colcollations.clone_in(mcx)?,
            enrname: rte.enrname,
            enrtuples: rte.enrtuples,
            groupexprs: rte.groupexprs.clone_in(mcx)?,
            lateral: rte.lateral,
            inFromCl: rte.inFromCl,
            securityQuals: rte.securityQuals.clone_in(mcx)?,
        },
    )
}

// check_and_push_window_quals (allpaths.c): returns whether the caller must
// keep the original qual.
fn check_and_push_window_quals<'mcx>(
    run: &mut PlannerRun<'mcx>,
    subquery: &mut Query<'mcx>,
    clause: Node<'mcx>,
    run_cond_attrs: &mut Bitmapset<'mcx>,
) -> PgResult<bool> {
    let Some(opexpr) = clause.as_op_expr() else {
        return Ok(true);
    };
    if opexpr.args.len() != 2 {
        return Ok(true);
    }
    // Only strict operators can serve: NULL'd stale window results must
    // filter out at the top-level WindowAgg.
    let opfuncid = nodes_core::set_opfuncid(opexpr)?;
    if !lsyscache::func_strict(opfuncid)? {
        return Ok(true);
    }
    for (argidx, wfunc_left) in [(0usize, true), (1usize, false)] {
        let arg = opexpr.args.nth(argidx);
        let Some(var) = arg.as_var() else { continue };
        if var.varattno <= 0 {
            continue;
        }
        let mut keep_original = true;
        if find_window_run_conditions(
            run,
            subquery,
            var.varattno,
            opexpr,
            wfunc_left,
            &mut keep_original,
            run_cond_attrs,
        )? {
            return Ok(keep_original);
        }
    }
    Ok(true)
}

// find_window_run_conditions (allpaths.c). The subquery tlist cells are ours
// to write but the entries are shared with the original parse tree, so a
// matched WindowFunc is replaced whole (rebuilt entry) instead of mutated.
fn find_window_run_conditions<'mcx>(
    run: &mut PlannerRun<'mcx>,
    subquery: &mut Query<'mcx>,
    attno: i16,
    opexpr: &OpExpr<'mcx>,
    wfunc_left: bool,
    keep_original: &mut bool,
    run_cond_attrs: &mut Bitmapset<'mcx>,
) -> PgResult<bool> {
    use types_nodes::primnodes::{
        RelabelType, SupportRequestWFuncMonotonic, WindowFunc, WindowFuncRunCondition,
        MONOTONICFUNC_BOTH, MONOTONICFUNC_DECREASING, MONOTONICFUNC_INCREASING,
        MONOTONICFUNC_NONE,
    };
    let mcx = run.mcx;
    *keep_original = true;

    let tle = subquery
        .targetList
        .nth(attno as usize - 1)
        .as_target_entry()
        .expect("tlist cell is a TargetEntry");
    let mut relabels: PgVec<'mcx, &'mcx RelabelType<'mcx>> = PgVec::new_in(mcx);
    let mut w = tle.expr;
    while let Some(r) = w.as_relabel_type() {
        relabels.push(r);
        w = r.arg;
    }
    let Some(wfunc) = w.as_window_func() else {
        return Ok(false);
    };
    if clauses::contain_subplans(w)? {
        return Ok(false);
    }
    let prosupport = lsyscache::get_func_support(wfunc.winfnoid)?;
    if prosupport == 0 {
        return Ok(false);
    }
    let otherexpr = opexpr.args.nth(if wfunc_left { 1 } else { 0 });
    if !clauses::is_pseudo_constant_clause(otherexpr)? {
        return Ok(false);
    }

    let wclause = subquery
        .windowClause
        .nth(wfunc.winref as usize - 1)
        .as_window_clause()
        .expect("windowClause cell");
    let mut req = SupportRequestWFuncMonotonic {
        tag: NodeTag::T_SupportRequestWFuncMonotonic,
        order_clause_empty: wclause.orderClause.is_nil(),
        frame_options: wclause.frameOptions,
        winfnoid: wfunc.winfnoid,
        agg_has_filter: wfunc.aggfilter.is_some(),
        monotonic: MONOTONICFUNC_NONE,
    };
    let res = fmgr_core::oid_function_call1_coll(
        prosupport,
        0,
        datum::Datum::from_usize(&mut req as *mut _ as usize),
    )?;
    if res.as_usize() == 0 || req.monotonic == MONOTONICFUNC_NONE {
        return Ok(false);
    }

    let mut runoperator = 0;
    let mut have_run_condition = false;
    let opinfos = lsyscache::get_op_index_interpretation(mcx, opexpr.opno)?;
    for opinfo in opinfos.iter() {
        match opinfo.cmptype {
            lsyscache::COMPARE_LT | lsyscache::COMPARE_LE => {
                if (wfunc_left && req.monotonic & MONOTONICFUNC_INCREASING != 0)
                    || (!wfunc_left && req.monotonic & MONOTONICFUNC_DECREASING != 0)
                {
                    *keep_original = false;
                    have_run_condition = true;
                    runoperator = opexpr.opno;
                }
                break;
            }
            lsyscache::COMPARE_GT | lsyscache::COMPARE_GE => {
                if (wfunc_left && req.monotonic & MONOTONICFUNC_DECREASING != 0)
                    || (!wfunc_left && req.monotonic & MONOTONICFUNC_INCREASING != 0)
                {
                    *keep_original = false;
                    have_run_condition = true;
                    runoperator = opexpr.opno;
                }
                break;
            }
            lsyscache::COMPARE_EQ => {
                if req.monotonic & MONOTONICFUNC_BOTH == MONOTONICFUNC_BOTH {
                    *keep_original = false;
                    have_run_condition = true;
                    runoperator = opexpr.opno;
                    break;
                }
                let newcmptype = if req.monotonic & MONOTONICFUNC_INCREASING != 0 {
                    if wfunc_left {
                        lsyscache::COMPARE_LE
                    } else {
                        lsyscache::COMPARE_GE
                    }
                } else if wfunc_left {
                    lsyscache::COMPARE_GE
                } else {
                    lsyscache::COMPARE_LE
                };
                *keep_original = true;
                have_run_condition = true;
                runoperator = lsyscache::get_opfamily_member_for_cmptype(
                    opinfo.opfamily_id,
                    opinfo.oplefttype,
                    opinfo.oprighttype,
                    newcmptype,
                )?;
                break;
            }
            _ => {}
        }
    }
    if !have_run_condition {
        return Ok(false);
    }

    // C copyObject's otherexpr and the WindowFunc; both trees are read-only
    // from here, so the sub-nodes stay shared.
    let wfuncrc = Node::mk(
        mcx,
        WindowFuncRunCondition {
            opno: runoperator,
            inputcollid: opexpr.inputcollid,
            wfunc_left,
            arg: otherexpr,
        },
    )?;
    let mut run_condition = wfunc.runCondition.clone_in(mcx)?;
    run_condition.lappend(mcx, wfuncrc)?;
    let new_wfunc = Node::mk(
        mcx,
        WindowFunc {
            winfnoid: wfunc.winfnoid,
            wintype: wfunc.wintype,
            wincollid: wfunc.wincollid,
            inputcollid: wfunc.inputcollid,
            args: wfunc.args.clone_in(mcx)?,
            aggfilter: wfunc.aggfilter,
            runCondition: run_condition,
            winref: wfunc.winref,
            winstar: wfunc.winstar,
            winagg: wfunc.winagg,
            location: wfunc.location,
        },
    )?;
    let mut expr = new_wfunc;
    for r in relabels.iter().rev() {
        expr = Node::mk(
            mcx,
            RelabelType {
                arg: expr,
                resulttype: r.resulttype,
                resulttypmod: r.resulttypmod,
                resultcollid: r.resultcollid,
                relabelformat: r.relabelformat,
                location: r.location,
            },
        )?;
    }
    let new_tle = Node::mk(
        mcx,
        TargetEntry {
            expr,
            resno: tle.resno,
            resname: tle.resname,
            ressortgroupref: tle.ressortgroupref,
            resorigtbl: tle.resorigtbl,
            resorigcol: tle.resorigcol,
            resjunk: tle.resjunk,
        },
    )?;
    subquery.targetList.as_mut_slice()[attno as usize - 1] = new_tle;

    run_cond_attrs.add_member(mcx, attno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER)?;
    Ok(true)
}

// remove_unused_subquery_outputs (allpaths.c): NULL-out subquery outputs the
// upper query never reads. extra_used_attrs carries attnos consumed by
// WindowAgg run conditions. Entries are replaced whole (TargetEntry nodes
// are shared with the original parsetree; only the copied list cells are
// ours to write).
pub(crate) fn remove_unused_subquery_outputs<'mcx>(
    run: &mut PlannerRun<'mcx>,
    rel: RelId,
    subquery: &mut Query<'mcx>,
    extra_used_attrs: Bitmapset<'mcx>,
) -> PgResult<()> {
    let mcx = run.mcx;
    if subquery.setOperations.is_some() {
        return Ok(());
    }
    // Plain DISTINCT uses every output column in the distinctClause.
    if !subquery.distinctClause.is_nil() && !subquery.hasDistinctOn {
        return Ok(());
    }

    let relid = run.root.rel(rel).relid as i32;
    let mut attrs_used = extra_used_attrs;
    let exprs = crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel_reltarget(rel).exprs);
    for &eid in exprs.iter() {
        vars::pull_varattnos(mcx, *run.root.expr_node(eid), relid, &mut attrs_used)?;
    }
    let rids = crate::relnode::pgvec_clone_shallow(mcx, &run.root.rel(rel).baserestrictinfo);
    for &rid in rids.iter() {
        let clause = *run.root.expr_node(run.root.rinfo(rid).clause);
        vars::pull_varattnos(mcx, clause, relid, &mut attrs_used)?;
    }
    if attrs_used.is_member(0 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER) {
        return Ok(());
    }

    for i in 0..subquery.targetList.len() {
        let tle = subquery
            .targetList
            .nth(i)
            .as_target_entry()
            .expect("tlist cell is a TargetEntry");
        if tle.ressortgroupref != 0 || tle.resjunk {
            continue;
        }
        if attrs_used.is_member(tle.resno as i32 - FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER) {
            continue;
        }
        // SRFs change the row count; volatile expressions have visible
        // side effects.
        if subquery.hasTargetSRFs && coerce::expression_returns_set(tle.expr) {
            continue;
        }
        if clauses::contain_volatile_functions(tle.expr)? {
            continue;
        }

        // NULL constant of the same exposed type, in case something looks at
        // the subquery's result rowtype.
        let (consttype, consttypmod) = crate::costsize::expr_type_typmod(tle.expr);
        let constcollid = crate::pathkeys::expr_collation(tle.expr);
        let (typlen, typbyval) = lsyscache::get_typlenbyval(consttype)?;
        let null_const = Node::mk(
            mcx,
            types_nodes::primnodes::Const {
                consttype,
                consttypmod,
                constcollid,
                constlen: typlen as i32,
                constvalue: datum::Datum::null(),
                constisnull: true,
                constbyval: typbyval,
                location: -1,
            },
        )?;
        let new_tle = Node::mk(
            mcx,
            TargetEntry {
                expr: null_const,
                resno: tle.resno,
                resname: tle.resname,
                ressortgroupref: tle.ressortgroupref,
                resorigtbl: tle.resorigtbl,
                resorigcol: tle.resorigcol,
                resjunk: tle.resjunk,
            },
        )?;
        subquery.targetList.as_mut_slice()[i] = new_tle;
    }
    Ok(())
}
