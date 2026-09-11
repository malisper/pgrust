//! outfuncs.funcs.c writers for the trees the debug_print_* GUCs dump
//! (tcop/postgres.c pg_parse_query / pg_rewrite_query / pg_plan_query via
//! print.c elog_node_display): the raw-statement family (RAWSTMT,
//! SELECTSTMT, RESTARGET, COLUMNREF, ...) and the plan-tree family
//! (PLANNEDSTMT and the Plan node subclasses). Field order and spelling
//! follow the generated outfuncs.funcs.c (gen_node_support.pl over
//! parsenodes.h / plannodes.h); the Plan/Scan/Join base prefixes are
//! written by shared helpers exactly as the generator inlines them.
//!
//! Tags without a writer here fall through to outNode's typed refusal in
//! lib.rs; the debug_print_* callers turn that refusal into a LOG line
//! rather than an aborted statement.

use super::*;
use types_nodes::parsenodes::{
    DeallocateStmt, ExecuteStmt, PrepareStmt, TransactionStmt, VariableSetStmt, VariableShowStmt,
    WithClause,
};
use types_nodes::plannodes::{
    Agg, Append, BitmapAnd, BitmapHeapScan, BitmapIndexScan, BitmapOr, CteScan, FunctionScan,
    Gather, GatherMerge, Group, Hash, HashJoin, IncrementalSort, IndexOnlyScan, IndexScan, Join,
    Limit, LockRows, Material, Memoize, MergeAppend, MergeJoin, ModifyTable, NestLoop,
    NestLoopParam, Plan, PlanInvalItem, PlanRowMark, PlannedStmt, ProjectSet, RecursiveUnion,
    Result as ResultPlan, Scan, SeqScan, SetOp, Sort, SubqueryScan, TidRangeScan, TidScan, Unique,
    ValuesScan, WindowAgg, WorkTableScan,
};
use types_nodes::rawnodes::{
    A_ArrayExpr, A_Indices, A_Indirection, A_Star, ColumnRef, DeleteStmt, DistinctClause, FuncCall,
    InferClause, InsertStmt, LockingClause, MultiAssignRef, OnConflictClause, ParamRef,
    RangeFunction, RangeSubselect, RawStmt, ResTarget, ReturningClause, ReturningOption,
    SelectStmt, SortBy, TypeCast, UpdateStmt, WindowDef,
};

/// outNode arms for the tags this module writes. `None` means "not one of
/// ours" so lib.rs can fall through to its typed refusal.
pub(super) fn try_out_node(out: &mut PgString<'_>, node: Node<'_>) -> Option<PgResult<()>> {
    macro_rules! arm {
        ($ty:ty, $f:ident) => {
            $f(out, node.as_variant::<$ty>().expect(stringify!($ty)))
        };
    }
    let mut handled = true;
    let r = (|| -> PgResult<()> {
        match node.node_tag() {
            // --- raw parse trees ---
            NodeTag::T_RawStmt => arm!(RawStmt, out_raw_stmt),
            NodeTag::T_SelectStmt => arm!(SelectStmt, out_select_stmt),
            NodeTag::T_ResTarget => arm!(ResTarget, out_res_target),
            NodeTag::T_ColumnRef => arm!(ColumnRef, out_column_ref),
            NodeTag::T_A_Star => {
                let _ = node.as_variant::<A_Star>().expect("A_Star");
                w!(out, "{{A_STAR}}");
                Ok(())
            }
            NodeTag::T_ParamRef => {
                let p = node.as_variant::<ParamRef>().expect("ParamRef");
                w!(
                    out,
                    "{{PARAMREF :number {} :location {}}}",
                    p.number,
                    loc(p.location)
                );
                Ok(())
            }
            NodeTag::T_SortBy => arm!(SortBy, out_sort_by),
            NodeTag::T_FuncCall => arm!(FuncCall, out_func_call),
            NodeTag::T_TypeCast => arm!(TypeCast, out_type_cast),
            NodeTag::T_InsertStmt => arm!(InsertStmt, out_insert_stmt),
            NodeTag::T_UpdateStmt => arm!(UpdateStmt, out_update_stmt),
            NodeTag::T_DeleteStmt => arm!(DeleteStmt, out_delete_stmt),
            NodeTag::T_WithClause => arm!(WithClause, out_with_clause),
            NodeTag::T_LockingClause => arm!(LockingClause, out_locking_clause),
            NodeTag::T_RangeSubselect => arm!(RangeSubselect, out_range_subselect),
            NodeTag::T_A_Indirection => arm!(A_Indirection, out_a_indirection),
            NodeTag::T_A_Indices => arm!(A_Indices, out_a_indices),
            NodeTag::T_A_ArrayExpr => arm!(A_ArrayExpr, out_a_array_expr),
            NodeTag::T_WindowDef => arm!(WindowDef, out_window_def),
            NodeTag::T_TransactionStmt => arm!(TransactionStmt, out_transaction_stmt),
            NodeTag::T_VariableSetStmt => arm!(VariableSetStmt, out_variable_set_stmt),
            NodeTag::T_VariableShowStmt => {
                let s = node
                    .as_variant::<VariableShowStmt>()
                    .expect("VariableShowStmt");
                w!(out, "{{VARIABLESHOWSTMT :name ");
                out_str(out, s.name);
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_PrepareStmt => {
                let p = node.as_variant::<PrepareStmt>().expect("PrepareStmt");
                w!(out, "{{PREPARESTMT :name ");
                out_str(out, p.name);
                w!(out, " :argtypes ");
                out_list(out, &p.argtypes)?;
                w!(out, " :query ");
                out_opt_node(out, p.query)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_ExecuteStmt => {
                let e = node.as_variant::<ExecuteStmt>().expect("ExecuteStmt");
                w!(out, "{{EXECUTESTMT :name ");
                out_str(out, e.name);
                w!(out, " :params ");
                out_list(out, &e.params)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_DeallocateStmt => {
                let d = node.as_variant::<DeallocateStmt>().expect("DeallocateStmt");
                w!(out, "{{DEALLOCATESTMT :name ");
                out_str(out, d.name);
                w!(out, " :isall ");
                out_bool(out, d.isall);
                w!(out, " :location {}}}", loc(d.location));
                Ok(())
            }
            NodeTag::T_RangeFunction => arm!(RangeFunction, out_range_function),
            NodeTag::T_MultiAssignRef => arm!(MultiAssignRef, out_multi_assign_ref),
            NodeTag::T_OnConflictClause => arm!(OnConflictClause, out_on_conflict_clause),
            NodeTag::T_InferClause => arm!(InferClause, out_infer_clause),
            NodeTag::T_ReturningClause => arm!(ReturningClause, out_returning_clause),
            NodeTag::T_ReturningOption => {
                let r = node
                    .as_variant::<ReturningOption>()
                    .expect("ReturningOption");
                w!(out, "{{RETURNINGOPTION :option {} :value ", r.option as i32);
                out_str(out, r.value);
                w!(out, " :location {}}}", loc(r.location));
                Ok(())
            }
            // --- plan trees ---
            NodeTag::T_PlannedStmt => arm!(PlannedStmt, out_planned_stmt),
            NodeTag::T_Result => arm!(ResultPlan, out_result),
            NodeTag::T_ProjectSet => {
                let p = node.as_variant::<ProjectSet>().expect("ProjectSet");
                w!(out, "{{PROJECTSET");
                out_plan_base(out, &p.plan, "plan.")?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_ModifyTable => arm!(ModifyTable, out_modify_table),
            NodeTag::T_Append => arm!(Append, out_append),
            NodeTag::T_MergeAppend => arm!(MergeAppend, out_merge_append),
            NodeTag::T_RecursiveUnion => arm!(RecursiveUnion, out_recursive_union),
            NodeTag::T_BitmapAnd => {
                let b = node.as_variant::<BitmapAnd>().expect("BitmapAnd");
                w!(out, "{{BITMAPAND");
                out_plan_base(out, &b.plan, "plan.")?;
                w!(out, " :bitmapplans ");
                out_list(out, &b.bitmapplans)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_BitmapOr => {
                let b = node.as_variant::<BitmapOr>().expect("BitmapOr");
                w!(out, "{{BITMAPOR");
                out_plan_base(out, &b.plan, "plan.")?;
                w!(out, " :isshared ");
                out_bool(out, b.isshared);
                w!(out, " :bitmapplans ");
                out_list(out, &b.bitmapplans)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_SeqScan => {
                let s = node.as_variant::<SeqScan>().expect("SeqScan");
                w!(out, "{{SEQSCAN");
                out_scan_base(out, &s.scan)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_IndexScan => arm!(IndexScan, out_index_scan),
            NodeTag::T_IndexOnlyScan => arm!(IndexOnlyScan, out_index_only_scan),
            NodeTag::T_BitmapIndexScan => arm!(BitmapIndexScan, out_bitmap_index_scan),
            NodeTag::T_BitmapHeapScan => {
                let s = node.as_variant::<BitmapHeapScan>().expect("BitmapHeapScan");
                w!(out, "{{BITMAPHEAPSCAN");
                out_scan_base(out, &s.scan)?;
                w!(out, " :bitmapqualorig ");
                out_list(out, &s.bitmapqualorig)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_TidScan => {
                let s = node.as_variant::<TidScan>().expect("TidScan");
                w!(out, "{{TIDSCAN");
                out_scan_base(out, &s.scan)?;
                w!(out, " :tidquals ");
                out_list(out, &s.tidquals)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_TidRangeScan => {
                let s = node.as_variant::<TidRangeScan>().expect("TidRangeScan");
                w!(out, "{{TIDRANGESCAN");
                out_scan_base(out, &s.scan)?;
                w!(out, " :tidrangequals ");
                out_list(out, &s.tidrangequals)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_SubqueryScan => {
                let s = node.as_variant::<SubqueryScan>().expect("SubqueryScan");
                w!(out, "{{SUBQUERYSCAN");
                out_scan_base(out, &s.scan)?;
                w!(out, " :subplan ");
                out_opt_node(out, s.subplan)?;
                w!(out, " :scanstatus {}}}", s.scanstatus);
                Ok(())
            }
            NodeTag::T_FunctionScan => {
                let s = node.as_variant::<FunctionScan>().expect("FunctionScan");
                w!(out, "{{FUNCTIONSCAN");
                out_scan_base(out, &s.scan)?;
                w!(out, " :functions ");
                out_list(out, &s.functions)?;
                w!(out, " :funcordinality ");
                out_bool(out, s.funcordinality);
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_ValuesScan => {
                let s = node.as_variant::<ValuesScan>().expect("ValuesScan");
                w!(out, "{{VALUESSCAN");
                out_scan_base(out, &s.scan)?;
                w!(out, " :values_lists ");
                out_list(out, &s.values_lists)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_CteScan => {
                let s = node.as_variant::<CteScan>().expect("CteScan");
                w!(out, "{{CTESCAN");
                out_scan_base(out, &s.scan)?;
                w!(
                    out,
                    " :ctePlanId {} :cteParam {}}}",
                    s.ctePlanId,
                    s.cteParam
                );
                Ok(())
            }
            NodeTag::T_WorkTableScan => {
                let s = node.as_variant::<WorkTableScan>().expect("WorkTableScan");
                w!(out, "{{WORKTABLESCAN");
                out_scan_base(out, &s.scan)?;
                w!(out, " :wtParam {}}}", s.wtParam);
                Ok(())
            }
            NodeTag::T_NestLoop => {
                let j = node.as_variant::<NestLoop>().expect("NestLoop");
                w!(out, "{{NESTLOOP");
                out_join_base(out, &j.join)?;
                w!(out, " :nestParams ");
                out_list(out, &j.nestParams)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_MergeJoin => arm!(MergeJoin, out_merge_join),
            NodeTag::T_HashJoin => arm!(HashJoin, out_hash_join),
            NodeTag::T_Material => {
                let m = node.as_variant::<Material>().expect("Material");
                w!(out, "{{MATERIAL");
                out_plan_base(out, &m.plan, "plan.")?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_Memoize => arm!(Memoize, out_memoize),
            NodeTag::T_Sort => {
                let s = node.as_variant::<Sort>().expect("Sort");
                w!(out, "{{SORT");
                out_plan_base(out, &s.plan, "plan.")?;
                out_sort_fields(out, s, "");
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_IncrementalSort => {
                let s = node
                    .as_variant::<IncrementalSort>()
                    .expect("IncrementalSort");
                w!(out, "{{INCREMENTALSORT");
                out_plan_base(out, &s.sort.plan, "sort.plan.")?;
                out_sort_fields(out, &s.sort, "sort.");
                w!(out, " :nPresortedCols {}}}", s.nPresortedCols);
                Ok(())
            }
            NodeTag::T_Group => {
                let g = node.as_variant::<Group>().expect("Group");
                w!(out, "{{GROUP");
                out_plan_base(out, &g.plan, "plan.")?;
                w!(out, " :numCols {} :grpColIdx ", g.numCols);
                out_attrnumber_array(out, g.grpColIdx);
                w!(out, " :grpOperators ");
                out_oid_array(out, g.grpOperators);
                w!(out, " :grpCollations ");
                out_oid_array(out, g.grpCollations);
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_Agg => arm!(Agg, out_agg),
            NodeTag::T_WindowAgg => arm!(WindowAgg, out_window_agg),
            NodeTag::T_Unique => {
                let u = node.as_variant::<Unique>().expect("Unique");
                w!(out, "{{UNIQUE");
                out_plan_base(out, &u.plan, "plan.")?;
                w!(out, " :numCols {} :uniqColIdx ", u.numCols);
                out_attrnumber_array(out, u.uniqColIdx);
                w!(out, " :uniqOperators ");
                out_oid_array(out, u.uniqOperators);
                w!(out, " :uniqCollations ");
                out_oid_array(out, u.uniqCollations);
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_Gather => {
                let g = node.as_variant::<Gather>().expect("Gather");
                w!(out, "{{GATHER");
                out_plan_base(out, &g.plan, "plan.")?;
                w!(
                    out,
                    " :num_workers {} :rescan_param {} :single_copy ",
                    g.num_workers,
                    g.rescan_param
                );
                out_bool(out, g.single_copy);
                w!(out, " :invisible ");
                out_bool(out, g.invisible);
                w!(out, " :initParam ");
                out_bitmapset(out, &g.initParam);
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_GatherMerge => {
                let g = node.as_variant::<GatherMerge>().expect("GatherMerge");
                w!(out, "{{GATHERMERGE");
                out_plan_base(out, &g.plan, "plan.")?;
                w!(
                    out,
                    " :num_workers {} :rescan_param {} :numCols {} :sortColIdx ",
                    g.num_workers,
                    g.rescan_param,
                    g.numCols
                );
                out_attrnumber_array(out, g.sortColIdx);
                w!(out, " :sortOperators ");
                out_oid_array(out, g.sortOperators);
                w!(out, " :collations ");
                out_oid_array(out, g.collations);
                w!(out, " :nullsFirst ");
                out_bool_array(out, g.nullsFirst);
                w!(out, " :initParam ");
                out_bitmapset(out, &g.initParam);
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_Hash => {
                let h = node.as_variant::<Hash>().expect("Hash");
                w!(out, "{{HASH");
                out_plan_base(out, &h.plan, "plan.")?;
                w!(out, " :hashkeys ");
                out_list(out, &h.hashkeys)?;
                w!(
                    out,
                    " :skewTable {} :skewColumn {} :skewInherit ",
                    h.skewTable,
                    h.skewColumn
                );
                out_bool(out, h.skewInherit);
                w!(out, " :rows_total ");
                out_double(out, h.rows_total);
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_SetOp => arm!(SetOp, out_set_op),
            NodeTag::T_LockRows => {
                let l = node.as_variant::<LockRows>().expect("LockRows");
                w!(out, "{{LOCKROWS");
                out_plan_base(out, &l.plan, "plan.")?;
                w!(out, " :rowMarks ");
                out_list(out, &l.rowMarks)?;
                w!(out, " :epqParam {}}}", l.epqParam);
                Ok(())
            }
            NodeTag::T_Limit => arm!(Limit, out_limit),
            NodeTag::T_NestLoopParam => {
                let p = node.as_variant::<NestLoopParam>().expect("NestLoopParam");
                w!(out, "{{NESTLOOPPARAM :paramno {} :paramval ", p.paramno);
                out_node(out, p.paramval)?;
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_PlanRowMark => {
                let r = node.as_variant::<PlanRowMark>().expect("PlanRowMark");
                w!(
                    out,
                    "{{PLANROWMARK :rti {} :prti {} :rowmarkId {} :markType {} :allMarkTypes {} \
                 :strength {} :waitPolicy {} :isParent ",
                    r.rti,
                    r.prti,
                    r.rowmarkId,
                    r.markType as u32,
                    r.allMarkTypes,
                    r.strength as u32,
                    r.waitPolicy as u32
                );
                out_bool(out, r.isParent);
                w!(out, "}}");
                Ok(())
            }
            NodeTag::T_PlanInvalItem => {
                let i = node.as_variant::<PlanInvalItem>().expect("PlanInvalItem");
                w!(
                    out,
                    "{{PLANINVALITEM :cacheId {} :hashValue {}}}",
                    i.cacheId,
                    i.hashValue
                );
                Ok(())
            }
            _ => {
                handled = false;
                Ok(())
            }
        }
    })();
    if handled {
        Some(r)
    } else {
        None
    }
}

// ---------------------------------------------------------------- arrays

// WRITE_SCALAR_ARRAY (outfuncs.c:233-246): "(" + " item"... + ")" for a
// non-NULL pointer, "<>" for NULL. A zero-length slice here stands for C's
// NULL pointer (the planner leaves numCols == 0 arrays unallocated).
fn out_attrnumber_array(out: &mut PgString<'_>, a: &[i16]) {
    if a.is_empty() {
        w!(out, "<>");
        return;
    }
    w!(out, "(");
    for v in a {
        w!(out, " {v}");
    }
    w!(out, ")");
}

fn out_oid_array(out: &mut PgString<'_>, a: &[types_core::Oid]) {
    if a.is_empty() {
        w!(out, "<>");
        return;
    }
    w!(out, "(");
    for v in a {
        w!(out, " {v}");
    }
    w!(out, ")");
}

fn out_bool_array(out: &mut PgString<'_>, a: &[bool]) {
    if a.is_empty() {
        w!(out, "<>");
        return;
    }
    w!(out, "(");
    for v in a {
        w!(out, " {}", if *v { "true" } else { "false" });
    }
    w!(out, ")");
}

// ------------------------------------------------------------ plan bases

// The Plan prefix every _out<PlanNode> inlines (plannodes.h Plan fields).
// `pfx` is the embedded-struct path the generator spells into the field
// names: "plan." for direct Plan subclasses, "scan.plan." / "join.plan." /
// "sort.plan." for the second-level bases.
fn out_plan_base(out: &mut PgString<'_>, p: &Plan<'_>, pfx: &str) -> PgResult<()> {
    w!(out, " :{pfx}disabled_nodes {} :{pfx}startup_cost ", p.disabled_nodes);
    out_double(out, p.startup_cost);
    w!(out, " :{pfx}total_cost ");
    out_double(out, p.total_cost);
    w!(out, " :{pfx}plan_rows ");
    out_double(out, p.plan_rows);
    w!(out, " :{pfx}plan_width {} :{pfx}parallel_aware ", p.plan_width);
    out_bool(out, p.parallel_aware);
    w!(out, " :{pfx}parallel_safe ");
    out_bool(out, p.parallel_safe);
    w!(out, " :{pfx}async_capable ");
    out_bool(out, p.async_capable);
    w!(out, " :{pfx}plan_node_id {} :{pfx}targetlist ", p.plan_node_id);
    out_list(out, &p.targetlist)?;
    w!(out, " :{pfx}qual ");
    out_list(out, &p.qual)?;
    w!(out, " :{pfx}lefttree ");
    out_opt_node(out, p.lefttree)?;
    w!(out, " :{pfx}righttree ");
    out_opt_node(out, p.righttree)?;
    w!(out, " :{pfx}initPlan ");
    out_list(out, &p.initPlan)?;
    w!(out, " :{pfx}extParam ");
    out_bitmapset(out, &p.extParam);
    w!(out, " :{pfx}allParam ");
    out_bitmapset(out, &p.allParam);
    Ok(())
}

fn out_scan_base(out: &mut PgString<'_>, s: &Scan<'_>) -> PgResult<()> {
    out_plan_base(out, &s.plan, "scan.plan.")?;
    w!(out, " :scan.scanrelid {}", s.scanrelid);
    Ok(())
}

fn out_join_base(out: &mut PgString<'_>, j: &Join<'_>) -> PgResult<()> {
    out_plan_base(out, &j.plan, "join.plan.")?;
    w!(out, " :join.jointype {} :join.inner_unique ", j.jointype as u32);
    out_bool(out, j.inner_unique);
    w!(out, " :join.joinqual ");
    out_list(out, &j.joinqual)
}

fn out_sort_fields(out: &mut PgString<'_>, s: &Sort<'_>, pfx: &str) {
    w!(out, " :{pfx}numCols {} :{pfx}sortColIdx ", s.numCols);
    out_attrnumber_array(out, s.sortColIdx);
    w!(out, " :{pfx}sortOperators ");
    out_oid_array(out, s.sortOperators);
    w!(out, " :{pfx}collations ");
    out_oid_array(out, s.collations);
    w!(out, " :{pfx}nullsFirst ");
    out_bool_array(out, s.nullsFirst);
}

// ------------------------------------------------------------ plan nodes

pub(super) fn out_planned_stmt(out: &mut PgString<'_>, p: &PlannedStmt<'_>) -> PgResult<()> {
    w!(
        out,
        "{{PLANNEDSTMT :commandType {} :queryId {} :planId {} :hasReturning ",
        p.commandType as u32,
        // SAFETY: a dump reads the statement's own queryId; the only writer
        // (the ProcessUtility hook) runs before or after, never concurrently.
        unsafe { p.queryId.get() },
        p.planId
    );
    out_bool(out, p.hasReturning);
    w!(out, " :hasModifyingCTE ");
    out_bool(out, p.hasModifyingCTE);
    w!(out, " :canSetTag ");
    out_bool(out, p.canSetTag);
    w!(out, " :transientPlan ");
    out_bool(out, p.transientPlan);
    w!(out, " :dependsOnRole ");
    out_bool(out, p.dependsOnRole);
    w!(out, " :parallelModeNeeded ");
    out_bool(out, p.parallelModeNeeded);
    w!(out, " :jitFlags {} :planTree ", p.jitFlags);
    out_opt_node(out, p.planTree)?;
    w!(out, " :partPruneInfos ");
    out_list(out, &p.partPruneInfos)?;
    w!(out, " :rtable ");
    out_list(out, &p.rtable)?;
    w!(out, " :unprunableRelids ");
    out_bitmapset(out, &p.unprunableRelids);
    w!(out, " :permInfos ");
    out_list(out, &p.permInfos)?;
    w!(out, " :resultRelations ");
    out_int_list(out, &p.resultRelations);
    w!(out, " :appendRelations ");
    out_list(out, &p.appendRelations)?;
    w!(out, " :subplans ");
    out_opt_list(out, &p.subplans)?;
    w!(out, " :rewindPlanIDs ");
    out_bitmapset(out, &p.rewindPlanIDs);
    w!(out, " :rowMarks ");
    out_list(out, &p.rowMarks)?;
    w!(out, " :relationOids ");
    out_oid_list(out, &p.relationOids);
    w!(out, " :invalItems ");
    out_list(out, &p.invalItems)?;
    w!(out, " :paramExecTypes ");
    out_oid_list(out, &p.paramExecTypes);
    w!(out, " :utilityStmt ");
    out_opt_node(out, p.utilityStmt)?;
    w!(
        out,
        " :stmt_location {} :stmt_len {}}}",
        loc(p.stmt_location),
        loc(p.stmt_len)
    );
    Ok(())
}

fn out_result(out: &mut PgString<'_>, r: &ResultPlan<'_>) -> PgResult<()> {
    w!(out, "{{RESULT");
    out_plan_base(out, &r.plan, "plan.")?;
    w!(out, " :resconstantqual ");
    out_opt_node(out, r.resconstantqual)?;
    w!(out, "}}");
    Ok(())
}

fn out_modify_table(out: &mut PgString<'_>, m: &ModifyTable<'_>) -> PgResult<()> {
    w!(out, "{{MODIFYTABLE");
    out_plan_base(out, &m.plan, "plan.")?;
    w!(out, " :operation {} :canSetTag ", m.operation as u32);
    out_bool(out, m.canSetTag);
    w!(
        out,
        " :nominalRelation {} :rootRelation {} :partColsUpdated ",
        m.nominalRelation,
        m.rootRelation
    );
    out_bool(out, m.partColsUpdated);
    w!(out, " :resultRelations ");
    out_int_list(out, &m.resultRelations);
    w!(out, " :updateColnosLists ");
    out_list(out, &m.updateColnosLists)?;
    w!(out, " :withCheckOptionLists ");
    out_list(out, &m.withCheckOptionLists)?;
    w!(out, " :returningOldAlias ");
    out_str(out, m.returningOldAlias);
    w!(out, " :returningNewAlias ");
    out_str(out, m.returningNewAlias);
    w!(out, " :returningLists ");
    out_list(out, &m.returningLists)?;
    w!(out, " :fdwPrivLists ");
    out_list(out, &m.fdwPrivLists)?;
    w!(out, " :fdwDirectModifyPlans ");
    out_bitmapset(out, &m.fdwDirectModifyPlans);
    w!(out, " :rowMarks ");
    out_list(out, &m.rowMarks)?;
    w!(
        out,
        " :epqParam {} :onConflictAction {} :arbiterIndexes ",
        m.epqParam,
        m.onConflictAction
    );
    out_oid_list(out, &m.arbiterIndexes);
    w!(out, " :onConflictSet ");
    out_list(out, &m.onConflictSet)?;
    w!(out, " :onConflictCols ");
    out_int_list(out, &m.onConflictCols);
    w!(out, " :onConflictWhere ");
    out_opt_node(out, m.onConflictWhere)?;
    w!(out, " :exclRelRTI {} :exclRelTlist ", m.exclRelRTI);
    out_list(out, &m.exclRelTlist)?;
    w!(out, " :mergeActionLists ");
    out_list(out, &m.mergeActionLists)?;
    w!(out, " :mergeJoinConditions ");
    out_list(out, &m.mergeJoinConditions)?;
    w!(out, "}}");
    Ok(())
}

fn out_append(out: &mut PgString<'_>, a: &Append<'_>) -> PgResult<()> {
    w!(out, "{{APPEND");
    out_plan_base(out, &a.plan, "plan.")?;
    w!(out, " :apprelids ");
    out_bitmapset(out, &a.apprelids);
    w!(out, " :appendplans ");
    out_list(out, &a.appendplans)?;
    w!(
        out,
        " :nasyncplans {} :first_partial_plan {} :part_prune_index {}}}",
        a.nasyncplans,
        a.first_partial_plan,
        a.part_prune_index
    );
    Ok(())
}

fn out_merge_append(out: &mut PgString<'_>, a: &MergeAppend<'_>) -> PgResult<()> {
    w!(out, "{{MERGEAPPEND");
    out_plan_base(out, &a.plan, "plan.")?;
    w!(out, " :apprelids ");
    out_bitmapset(out, &a.apprelids);
    w!(out, " :mergeplans ");
    out_list(out, &a.mergeplans)?;
    w!(out, " :numCols {} :sortColIdx ", a.numCols);
    out_attrnumber_array(out, a.sortColIdx);
    w!(out, " :sortOperators ");
    out_oid_array(out, a.sortOperators);
    w!(out, " :collations ");
    out_oid_array(out, a.collations);
    w!(out, " :nullsFirst ");
    out_bool_array(out, a.nullsFirst);
    w!(out, " :part_prune_index {}}}", a.part_prune_index);
    Ok(())
}

fn out_recursive_union(out: &mut PgString<'_>, r: &RecursiveUnion<'_>) -> PgResult<()> {
    w!(out, "{{RECURSIVEUNION");
    out_plan_base(out, &r.plan, "plan.")?;
    w!(
        out,
        " :wtParam {} :numCols {} :dupColIdx ",
        r.wtParam,
        r.numCols
    );
    out_attrnumber_array(out, r.dupColIdx);
    w!(out, " :dupOperators ");
    out_oid_array(out, r.dupOperators);
    w!(out, " :dupCollations ");
    out_oid_array(out, r.dupCollations);
    w!(out, " :numGroups {}}}", r.numGroups);
    Ok(())
}

fn out_index_scan(out: &mut PgString<'_>, s: &IndexScan<'_>) -> PgResult<()> {
    w!(out, "{{INDEXSCAN");
    out_scan_base(out, &s.scan)?;
    w!(out, " :indexid {} :indexqual ", s.indexid);
    out_list(out, &s.indexqual)?;
    w!(out, " :indexqualorig ");
    out_list(out, &s.indexqualorig)?;
    w!(out, " :indexorderby ");
    out_list(out, &s.indexorderby)?;
    w!(out, " :indexorderbyorig ");
    out_list(out, &s.indexorderbyorig)?;
    w!(out, " :indexorderbyops ");
    out_oid_list(out, &s.indexorderbyops);
    w!(out, " :indexorderdir {}}}", s.indexorderdir);
    Ok(())
}

fn out_index_only_scan(out: &mut PgString<'_>, s: &IndexOnlyScan<'_>) -> PgResult<()> {
    w!(out, "{{INDEXONLYSCAN");
    out_scan_base(out, &s.scan)?;
    w!(out, " :indexid {} :indexqual ", s.indexid);
    out_list(out, &s.indexqual)?;
    w!(out, " :recheckqual ");
    out_list(out, &s.recheckqual)?;
    w!(out, " :indexorderby ");
    out_list(out, &s.indexorderby)?;
    w!(out, " :indextlist ");
    out_list(out, &s.indextlist)?;
    w!(out, " :indexorderdir {}}}", s.indexorderdir);
    Ok(())
}

fn out_bitmap_index_scan(out: &mut PgString<'_>, s: &BitmapIndexScan<'_>) -> PgResult<()> {
    w!(out, "{{BITMAPINDEXSCAN");
    out_scan_base(out, &s.scan)?;
    w!(out, " :indexid {} :isshared ", s.indexid);
    out_bool(out, s.isshared);
    w!(out, " :indexqual ");
    out_list(out, &s.indexqual)?;
    w!(out, " :indexqualorig ");
    out_list(out, &s.indexqualorig)?;
    w!(out, "}}");
    Ok(())
}

fn out_merge_join(out: &mut PgString<'_>, j: &MergeJoin<'_>) -> PgResult<()> {
    w!(out, "{{MERGEJOIN");
    out_join_base(out, &j.join)?;
    w!(out, " :skip_mark_restore ");
    out_bool(out, j.skip_mark_restore);
    w!(out, " :mergeclauses ");
    out_list(out, &j.mergeclauses)?;
    w!(out, " :mergeFamilies ");
    out_oid_array(out, j.mergeFamilies);
    w!(out, " :mergeCollations ");
    out_oid_array(out, j.mergeCollations);
    w!(out, " :mergeReversals ");
    out_bool_array(out, j.mergeReversals);
    w!(out, " :mergeNullsFirst ");
    out_bool_array(out, j.mergeNullsFirst);
    w!(out, "}}");
    Ok(())
}

fn out_hash_join(out: &mut PgString<'_>, j: &HashJoin<'_>) -> PgResult<()> {
    w!(out, "{{HASHJOIN");
    out_join_base(out, &j.join)?;
    w!(out, " :hashclauses ");
    out_list(out, &j.hashclauses)?;
    w!(out, " :hashoperators ");
    out_oid_list(out, &j.hashoperators);
    w!(out, " :hashcollations ");
    out_oid_list(out, &j.hashcollations);
    w!(out, " :hashkeys ");
    out_list(out, &j.hashkeys)?;
    w!(out, "}}");
    Ok(())
}

fn out_memoize(out: &mut PgString<'_>, m: &Memoize<'_>) -> PgResult<()> {
    w!(out, "{{MEMOIZE");
    out_plan_base(out, &m.plan, "plan.")?;
    w!(out, " :numKeys {} :hashOperators ", m.numKeys);
    out_oid_array(out, m.hashOperators);
    w!(out, " :collations ");
    out_oid_array(out, m.collations);
    w!(out, " :param_exprs ");
    out_list(out, &m.param_exprs)?;
    w!(out, " :singlerow ");
    out_bool(out, m.singlerow);
    w!(out, " :binary_mode ");
    out_bool(out, m.binary_mode);
    w!(out, " :est_entries {} :keyparamids ", m.est_entries);
    out_bitmapset(out, &m.keyparamids);
    w!(out, "}}");
    Ok(())
}

fn out_agg(out: &mut PgString<'_>, a: &Agg<'_>) -> PgResult<()> {
    w!(out, "{{AGG");
    out_plan_base(out, &a.plan, "plan.")?;
    w!(
        out,
        " :aggstrategy {} :aggsplit {} :numCols {} :grpColIdx ",
        a.aggstrategy,
        a.aggsplit,
        a.numCols
    );
    // make_agg's arrays come from extract_grouping_cols/ops/collations, which
    // palloc even a zero-length result: C prints "()" here, never "<>".
    if a.numCols == 0 {
        w!(out, "() :grpOperators () :grpCollations ()");
    } else {
        out_attrnumber_array(out, a.grpColIdx);
        w!(out, " :grpOperators ");
        out_oid_array(out, a.grpOperators);
        w!(out, " :grpCollations ");
        out_oid_array(out, a.grpCollations);
    }
    w!(
        out,
        " :numGroups {} :transitionSpace {} :aggParams ",
        a.numGroups,
        a.transitionSpace
    );
    out_bitmapset(out, &a.aggParams);
    w!(out, " :groupingSets ");
    out_list(out, &a.groupingSets)?;
    w!(out, " :chain ");
    out_list(out, &a.chain)?;
    w!(out, "}}");
    Ok(())
}

fn out_window_agg(out: &mut PgString<'_>, w: &WindowAgg<'_>) -> PgResult<()> {
    w!(out, "{{WINDOWAGG");
    out_plan_base(out, &w.plan, "plan.")?;
    w!(out, " :winname ");
    out_str(out, w.winname);
    w!(
        out,
        " :winref {} :partNumCols {} :partColIdx ",
        w.winref,
        w.partNumCols
    );
    out_attrnumber_array(out, w.partColIdx);
    w!(out, " :partOperators ");
    out_oid_array(out, w.partOperators);
    w!(out, " :partCollations ");
    out_oid_array(out, w.partCollations);
    w!(out, " :ordNumCols {} :ordColIdx ", w.ordNumCols);
    out_attrnumber_array(out, w.ordColIdx);
    w!(out, " :ordOperators ");
    out_oid_array(out, w.ordOperators);
    w!(out, " :ordCollations ");
    out_oid_array(out, w.ordCollations);
    w!(out, " :frameOptions {} :startOffset ", w.frameOptions);
    out_opt_node(out, w.startOffset)?;
    w!(out, " :endOffset ");
    out_opt_node(out, w.endOffset)?;
    w!(out, " :runCondition ");
    out_list(out, &w.runCondition)?;
    w!(out, " :runConditionOrig ");
    out_list(out, &w.runConditionOrig)?;
    w!(
        out,
        " :startInRangeFunc {} :endInRangeFunc {} :inRangeColl {} :inRangeAsc ",
        w.startInRangeFunc,
        w.endInRangeFunc,
        w.inRangeColl
    );
    out_bool(out, w.inRangeAsc);
    w!(out, " :inRangeNullsFirst ");
    out_bool(out, w.inRangeNullsFirst);
    w!(out, " :topWindow ");
    out_bool(out, w.topWindow);
    w!(out, "}}");
    Ok(())
}

fn out_set_op(out: &mut PgString<'_>, s: &SetOp<'_>) -> PgResult<()> {
    w!(out, "{{SETOP");
    out_plan_base(out, &s.plan, "plan.")?;
    w!(
        out,
        " :cmd {} :strategy {} :numCols {} :cmpColIdx ",
        s.cmd,
        s.strategy,
        s.numCols
    );
    out_attrnumber_array(out, s.cmpColIdx);
    w!(out, " :cmpOperators ");
    out_oid_array(out, s.cmpOperators);
    w!(out, " :cmpCollations ");
    out_oid_array(out, s.cmpCollations);
    w!(out, " :cmpNullsFirst ");
    out_bool_array(out, s.cmpNullsFirst);
    w!(out, " :numGroups {}}}", s.numGroups);
    Ok(())
}

fn out_limit(out: &mut PgString<'_>, l: &Limit<'_>) -> PgResult<()> {
    w!(out, "{{LIMIT");
    out_plan_base(out, &l.plan, "plan.")?;
    w!(out, " :limitOffset ");
    out_opt_node(out, l.limitOffset)?;
    w!(out, " :limitCount ");
    out_opt_node(out, l.limitCount)?;
    w!(
        out,
        " :limitOption {} :uniqNumCols {} :uniqColIdx ",
        l.limitOption as u32,
        l.uniqNumCols
    );
    out_attrnumber_array(out, l.uniqColIdx);
    w!(out, " :uniqOperators ");
    out_oid_array(out, l.uniqOperators);
    w!(out, " :uniqCollations ");
    out_oid_array(out, l.uniqCollations);
    w!(out, "}}");
    Ok(())
}

// -------------------------------------------------------- raw statements

fn out_raw_stmt(out: &mut PgString<'_>, r: &RawStmt<'_>) -> PgResult<()> {
    w!(out, "{{RAWSTMT :stmt ");
    out_opt_node(out, r.stmt)?;
    w!(
        out,
        " :stmt_location {} :stmt_len {}}}",
        loc(r.stmt_location),
        loc(r.stmt_len)
    );
    Ok(())
}

// _outSelectStmt: larg/rarg are SelectStmt pointers written through outNode
// in C; here they are direct references, so the writer recurses itself.
fn out_select_stmt(out: &mut PgString<'_>, s: &SelectStmt<'_>) -> PgResult<()> {
    stack_depth_core::check_stack_depth()?;
    w!(out, "{{SELECTSTMT :distinctClause ");
    match &s.distinctClause {
        // C: NIL / list_make1(NIL) (one NULL cell) / the DISTINCT ON exprs.
        DistinctClause::None => w!(out, "<>"),
        DistinctClause::All => w!(out, "(<>)"),
        DistinctClause::On(l) => out_list(out, l)?,
    }
    w!(out, " :intoClause ");
    out_opt_node(out, s.intoClause)?;
    w!(out, " :targetList ");
    out_list(out, &s.targetList)?;
    w!(out, " :fromClause ");
    out_list(out, &s.fromClause)?;
    w!(out, " :whereClause ");
    out_opt_node(out, s.whereClause)?;
    w!(out, " :groupClause ");
    out_list(out, &s.groupClause)?;
    w!(out, " :groupDistinct ");
    out_bool(out, s.groupDistinct);
    w!(out, " :havingClause ");
    out_opt_node(out, s.havingClause)?;
    w!(out, " :windowClause ");
    out_list(out, &s.windowClause)?;
    w!(out, " :valuesLists ");
    out_list(out, &s.valuesLists)?;
    w!(out, " :sortClause ");
    out_list(out, &s.sortClause)?;
    w!(out, " :limitOffset ");
    out_opt_node(out, s.limitOffset)?;
    w!(out, " :limitCount ");
    out_opt_node(out, s.limitCount)?;
    w!(
        out,
        " :limitOption {} :lockingClause ",
        s.limitOption as u32
    );
    out_list(out, &s.lockingClause)?;
    w!(out, " :withClause ");
    out_opt_node(out, s.withClause)?;
    w!(out, " :op {} :all ", s.op as u32);
    out_bool(out, s.all);
    w!(out, " :larg ");
    match s.larg {
        None => w!(out, "<>"),
        Some(l) => out_select_stmt(out, l)?,
    }
    w!(out, " :rarg ");
    match s.rarg {
        None => w!(out, "<>"),
        Some(r) => out_select_stmt(out, r)?,
    }
    w!(out, "}}");
    Ok(())
}

fn out_res_target(out: &mut PgString<'_>, r: &ResTarget<'_>) -> PgResult<()> {
    w!(out, "{{RESTARGET :name ");
    out_str(out, r.name);
    w!(out, " :indirection ");
    out_list(out, &r.indirection)?;
    w!(out, " :val ");
    out_opt_node(out, r.val)?;
    w!(out, " :location {}}}", loc(r.location));
    Ok(())
}

fn out_column_ref(out: &mut PgString<'_>, c: &ColumnRef<'_>) -> PgResult<()> {
    w!(out, "{{COLUMNREF :fields ");
    out_list(out, &c.fields)?;
    w!(out, " :location {}}}", loc(c.location));
    Ok(())
}

fn out_sort_by(out: &mut PgString<'_>, s: &SortBy<'_>) -> PgResult<()> {
    w!(out, "{{SORTBY :node ");
    out_opt_node(out, s.node)?;
    w!(
        out,
        " :sortby_dir {} :sortby_nulls {} :useOp ",
        s.sortby_dir as u32,
        s.sortby_nulls as u32
    );
    out_list(out, &s.useOp)?;
    w!(out, " :location {}}}", loc(s.location));
    Ok(())
}

fn out_func_call(out: &mut PgString<'_>, f: &FuncCall<'_>) -> PgResult<()> {
    w!(out, "{{FUNCCALL :funcname ");
    out_list(out, &f.funcname)?;
    w!(out, " :args ");
    out_list(out, &f.args)?;
    w!(out, " :agg_order ");
    out_list(out, &f.agg_order)?;
    w!(out, " :agg_filter ");
    out_opt_node(out, f.agg_filter)?;
    w!(out, " :over ");
    out_opt_node(out, f.over)?;
    w!(out, " :agg_within_group ");
    out_bool(out, f.agg_within_group);
    w!(out, " :agg_star ");
    out_bool(out, f.agg_star);
    w!(out, " :agg_distinct ");
    out_bool(out, f.agg_distinct);
    w!(out, " :func_variadic ");
    out_bool(out, f.func_variadic);
    w!(
        out,
        " :funcformat {} :location {}}}",
        f.funcformat as u32,
        loc(f.location)
    );
    Ok(())
}

fn out_type_cast(out: &mut PgString<'_>, t: &TypeCast<'_>) -> PgResult<()> {
    w!(out, "{{TYPECAST :arg ");
    out_opt_node(out, t.arg)?;
    w!(out, " :typeName ");
    out_opt_node(out, t.typeName)?;
    w!(out, " :location {}}}", loc(t.location));
    Ok(())
}

fn out_insert_stmt(out: &mut PgString<'_>, s: &InsertStmt<'_>) -> PgResult<()> {
    w!(out, "{{INSERTSTMT :relation ");
    out_opt_node(out, s.relation)?;
    w!(out, " :cols ");
    out_list(out, &s.cols)?;
    w!(out, " :selectStmt ");
    out_opt_node(out, s.selectStmt)?;
    w!(out, " :onConflictClause ");
    out_opt_node(out, s.onConflictClause)?;
    w!(out, " :returningClause ");
    out_opt_node(out, s.returningClause)?;
    w!(out, " :withClause ");
    out_opt_node(out, s.withClause)?;
    w!(out, " :override {}}}", s.r#override as u32);
    Ok(())
}

fn out_update_stmt(out: &mut PgString<'_>, s: &UpdateStmt<'_>) -> PgResult<()> {
    w!(out, "{{UPDATESTMT :relation ");
    out_opt_node(out, s.relation)?;
    w!(out, " :targetList ");
    out_list(out, &s.targetList)?;
    w!(out, " :whereClause ");
    out_opt_node(out, s.whereClause)?;
    w!(out, " :fromClause ");
    out_list(out, &s.fromClause)?;
    w!(out, " :returningClause ");
    out_opt_node(out, s.returningClause)?;
    w!(out, " :withClause ");
    out_opt_node(out, s.withClause)?;
    w!(out, "}}");
    Ok(())
}

fn out_delete_stmt(out: &mut PgString<'_>, s: &DeleteStmt<'_>) -> PgResult<()> {
    w!(out, "{{DELETESTMT :relation ");
    out_opt_node(out, s.relation)?;
    w!(out, " :usingClause ");
    out_list(out, &s.usingClause)?;
    w!(out, " :whereClause ");
    out_opt_node(out, s.whereClause)?;
    w!(out, " :returningClause ");
    out_opt_node(out, s.returningClause)?;
    w!(out, " :withClause ");
    out_opt_node(out, s.withClause)?;
    w!(out, "}}");
    Ok(())
}

fn out_with_clause(out: &mut PgString<'_>, c: &WithClause<'_>) -> PgResult<()> {
    w!(out, "{{WITHCLAUSE :ctes ");
    out_list(out, &c.ctes)?;
    w!(out, " :recursive ");
    out_bool(out, c.recursive);
    w!(out, " :location {}}}", loc(c.location));
    Ok(())
}

fn out_locking_clause(out: &mut PgString<'_>, c: &LockingClause<'_>) -> PgResult<()> {
    w!(out, "{{LOCKINGCLAUSE :lockedRels ");
    out_list(out, &c.lockedRels)?;
    w!(
        out,
        " :strength {} :waitPolicy {}}}",
        c.strength as u32,
        c.waitPolicy as u32
    );
    Ok(())
}

fn out_range_subselect(out: &mut PgString<'_>, r: &RangeSubselect<'_>) -> PgResult<()> {
    w!(out, "{{RANGESUBSELECT :lateral ");
    out_bool(out, r.lateral);
    w!(out, " :subquery ");
    out_opt_node(out, r.subquery)?;
    w!(out, " :alias ");
    out_opt_alias(out, r.alias)?;
    w!(out, "}}");
    Ok(())
}

fn out_a_indirection(out: &mut PgString<'_>, a: &A_Indirection<'_>) -> PgResult<()> {
    w!(out, "{{A_INDIRECTION :arg ");
    out_opt_node(out, a.arg)?;
    w!(out, " :indirection ");
    out_list(out, &a.indirection)?;
    w!(out, "}}");
    Ok(())
}

fn out_a_indices(out: &mut PgString<'_>, a: &A_Indices<'_>) -> PgResult<()> {
    w!(out, "{{A_INDICES :is_slice ");
    out_bool(out, a.is_slice);
    w!(out, " :lidx ");
    out_opt_node(out, a.lidx)?;
    w!(out, " :uidx ");
    out_opt_node(out, a.uidx)?;
    w!(out, "}}");
    Ok(())
}

fn out_a_array_expr(out: &mut PgString<'_>, a: &A_ArrayExpr<'_>) -> PgResult<()> {
    w!(out, "{{A_ARRAYEXPR :elements ");
    out_list(out, &a.elements)?;
    w!(
        out,
        " :list_start {} :list_end {} :location {}}}",
        loc(a.list_start),
        loc(a.list_end),
        loc(a.location)
    );
    Ok(())
}

fn out_window_def(out: &mut PgString<'_>, w: &WindowDef<'_>) -> PgResult<()> {
    w!(out, "{{WINDOWDEF :name ");
    out_str(out, w.name);
    w!(out, " :refname ");
    out_str(out, w.refname);
    w!(out, " :partitionClause ");
    out_list(out, &w.partitionClause)?;
    w!(out, " :orderClause ");
    out_list(out, &w.orderClause)?;
    w!(out, " :frameOptions {} :startOffset ", w.frameOptions);
    out_opt_node(out, w.startOffset)?;
    w!(out, " :endOffset ");
    out_opt_node(out, w.endOffset)?;
    w!(out, " :location {}}}", loc(w.location));
    Ok(())
}

fn out_transaction_stmt(out: &mut PgString<'_>, t: &TransactionStmt<'_>) -> PgResult<()> {
    w!(out, "{{TRANSACTIONSTMT :kind {} :options ", t.kind as u32);
    out_list(out, &t.options)?;
    w!(out, " :savepoint_name ");
    out_str(out, t.savepoint_name);
    w!(out, " :gid ");
    out_str(out, t.gid);
    w!(out, " :chain ");
    out_bool(out, t.chain);
    w!(out, " :location {}}}", loc(t.location));
    Ok(())
}

fn out_variable_set_stmt(out: &mut PgString<'_>, v: &VariableSetStmt<'_>) -> PgResult<()> {
    w!(out, "{{VARIABLESETSTMT :kind {} :name ", v.kind as u32);
    out_str(out, v.name);
    w!(out, " :args ");
    out_list(out, &v.args)?;
    w!(out, " :jumble_args ");
    out_bool(out, v.jumble_args);
    w!(out, " :is_local ");
    out_bool(out, v.is_local);
    w!(out, " :location {}}}", loc(v.location));
    Ok(())
}

fn out_range_function(out: &mut PgString<'_>, r: &RangeFunction<'_>) -> PgResult<()> {
    w!(out, "{{RANGEFUNCTION :lateral ");
    out_bool(out, r.lateral);
    w!(out, " :ordinality ");
    out_bool(out, r.ordinality);
    w!(out, " :is_rowsfrom ");
    out_bool(out, r.is_rowsfrom);
    w!(out, " :functions ");
    out_list(out, &r.functions)?;
    w!(out, " :alias ");
    out_opt_alias(out, r.alias)?;
    w!(out, " :coldeflist ");
    out_list(out, &r.coldeflist)?;
    w!(out, "}}");
    Ok(())
}

fn out_multi_assign_ref(out: &mut PgString<'_>, m: &MultiAssignRef<'_>) -> PgResult<()> {
    w!(out, "{{MULTIASSIGNREF :source ");
    out_opt_node(out, m.source)?;
    w!(out, " :colno {} :ncolumns {}}}", m.colno, m.ncolumns);
    Ok(())
}

fn out_on_conflict_clause(out: &mut PgString<'_>, c: &OnConflictClause<'_>) -> PgResult<()> {
    w!(
        out,
        "{{ONCONFLICTCLAUSE :action {} :infer ",
        c.action as u32
    );
    out_opt_node(out, c.infer)?;
    w!(out, " :targetList ");
    out_list(out, &c.targetList)?;
    w!(out, " :whereClause ");
    out_opt_node(out, c.whereClause)?;
    w!(out, " :location {}}}", loc(c.location));
    Ok(())
}

fn out_infer_clause(out: &mut PgString<'_>, c: &InferClause<'_>) -> PgResult<()> {
    w!(out, "{{INFERCLAUSE :indexElems ");
    out_list(out, &c.indexElems)?;
    w!(out, " :whereClause ");
    out_opt_node(out, c.whereClause)?;
    w!(out, " :conname ");
    out_str(out, c.conname);
    w!(out, " :location {}}}", loc(c.location));
    Ok(())
}

fn out_returning_clause(out: &mut PgString<'_>, r: &ReturningClause<'_>) -> PgResult<()> {
    w!(out, "{{RETURNINGCLAUSE :options ");
    out_list(out, &r.options)?;
    w!(out, " :exprs ");
    out_list(out, &r.exprs)?;
    w!(out, "}}");
    Ok(())
}
