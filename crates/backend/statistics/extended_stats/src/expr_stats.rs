//! The CREATE STATISTICS expression-statistics build leg of `extended_stats.c`:
//! `stxexprs` decode (`decode_stxexprs`), the per-row expression evaluation that
//! `make_build_data` performs for expression columns
//! (`eval_exprs_into_build_data`), `build_expr_data` / `compute_expr_stats`, and
//! `serialize_expr_stats`.
//!
//! Faithful to extended_stats.c (PG18.3): the column path lives in the parent
//! module; this module adds the expression handling. `examine_expression` is
//! owned by `analyze.c` (it shares `examine_attribute`'s internals — the
//! built-in custom-typanalyze dispatch and `std_typanalyze`) and reached through
//! the `examine_expression` seam.

extern crate alloc;

use alloc::string::String;
use alloc::vec::Vec;

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::{PgError, PgResult};
use nodes::primnodes::Expr;
use rel::Relation;
use statistics::{
    AnalyzeAttrFetchFunc, VacAttrStats, STATISTIC_NUM_SLOTS,
    Anum_pg_statistic_starelid, Anum_pg_statistic_staattnum, Anum_pg_statistic_stainherit,
    Anum_pg_statistic_stanullfrac, Anum_pg_statistic_stawidth, Anum_pg_statistic_stadistinct,
    Anum_pg_statistic_stakind1, Anum_pg_statistic_staop1, Anum_pg_statistic_stacoll1,
    Anum_pg_statistic_stanumbers1, Anum_pg_statistic_stavalues1, Natts_pg_statistic,
    StatisticRelationId,
};
use types_tuple::heaptuple::{Datum, FormedTuple};

use table::{table_close, table_open};
use table_tableam::table_slot_create;
use execExpr_seams as expr_seam;
use execTuples_seams as slot_seam;
use execUtils_seams as exec_util_seam;
use arrayfuncs::construct::construct_md_array_values;
use scalar_datum_core::datum_copy_v;
use attoptcache::get_attribute_options;
use lsyscache_seams::get_rel_type_id;
use types_storage::lock::RowExclusiveLock;

use commands_analyze_seams as analyze;

const FLOAT4OID: Oid = 700;
/// `InvalidAttrNumber` (access/attnum.h).
const INVALID_ATTR_NUMBER: i32 = 0;

/// `TextDatumGetCString(datum)` over a stored `pg_node_tree`/`text` by-ref Datum,
/// then `stringToNode` → `eval_const_expressions` → `fix_opfuncids`
/// (extended_stats.c:483-507). Decodes the `stxexprs` `List*` into an owned
/// `Vec<Expr>` allocated in `mcx`.
pub fn decode_stxexprs<'mcx>(mcx: Mcx<'mcx>, exprs_string: &str) -> PgResult<Vec<Expr<'mcx>>> {
    // exprs = (List *) stringToNode(exprsString);
    let node = read_seams::string_to_node::call(mcx, exprs_string)?;
    let node = mcx::PgBox::into_inner(node);

    let mut out: Vec<Expr<'mcx>> = Vec::new();
    match node.into_list() {
        Some(elems) => {
            for elem in elems {
                let inner = mcx::PgBox::into_inner(elem);
                if let Some(e) = inner.into_expr() {
                    out.push(e);
                }
            }
        }
        None => {
            // Defensive: a bare expression node (not wrapped in a List).
            let node = read_seams::string_to_node::call(mcx, exprs_string)?;
            if let Some(e) = mcx::PgBox::into_inner(node).into_expr() {
                out.push(e);
            }
        }
    }

    // Run each expression through eval_const_expressions + fix_opfuncids. The C
    // feeds the whole List to eval_const_expressions/fix_opfuncids; the owned
    // per-Expr seams do the same element-wise.
    let mut result: Vec<Expr<'mcx>> = Vec::with_capacity(out.len());
    for e in out {
        let mut e =
            init_subselect_ext_seams::eval_const_expressions_expr::call(
                mcx, e,
            )?;
        // fix_opfuncids((Node *) exprs);
        nodes_core::nodefuncs::fix_opfuncids(&mut e)?;
        result.push(e);
    }
    Ok(result)
}

/// `examine_expression(expr, stattarget)` (extended_stats.c:604) via the analyze
/// seam (the typanalyze dispatch lives in analyze.c).
pub fn examine_expression<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    expr: &Expr,
    stattarget: i32,
) -> PgResult<Option<VacAttrStats<'mcx>>> {
    analyze::examine_expression::call(mcx, onerel, expr, stattarget)
}

/// The expression-column half of `make_build_data` (extended_stats.c:2553-2609):
/// evaluate `exprs` for each sampled row, appending one column of
/// `(values, nulls)` per expression to the build data. The regular-attribute
/// columns are filled by the parent module before this; `numrows`/`rows`/`rel`
/// match the C arguments.
///
/// Returns `(values, nulls)` where each outer element is one expression column
/// of length `numrows`.
#[allow(clippy::type_complexity)]
pub fn eval_exprs_into_build_data<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    exprs: &[Expr],
    numrows: i32,
    rows: &[FormedTuple<'mcx>],
) -> PgResult<(Vec<Vec<Datum<'mcx>>>, Vec<Vec<bool>>)> {
    let nexprs = exprs.len();

    // estate = CreateExecutorState(); econtext = GetPerTupleExprContext(estate);
    let mut estate = exec_util_seam::create_executor_state::call(mcx)?;
    let econtext = exec_util_seam::get_per_tuple_expr_context::call(&mut estate)?;

    // slot = MakeSingleTupleTableSlot(RelationGetDescr(rel), &TTSOpsHeapTuple);
    let slot_data = table_slot_create(mcx, rel)?;
    let slot = estate.push_slot_data(slot_data)?;
    // econtext->ecxt_scantuple = slot;
    estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

    // exprstates = ExecPrepareExprList(stat->exprs, estate);
    let mut exprstates = expr_seam::exec_prepare_expr_list::call(exprs, &mut estate)?;

    // result columns: one per expression.
    let mut values: Vec<Vec<Datum<'mcx>>> = (0..nexprs)
        .map(|_| Vec::with_capacity(numrows as usize))
        .collect();
    let mut nulls: Vec<Vec<bool>> = (0..nexprs)
        .map(|_| Vec::with_capacity(numrows as usize))
        .collect();

    for i in 0..numrows as usize {
        // ResetExprContext(econtext);
        exec_util_seam::reset_expr_context::call(&mut estate, econtext)?;

        // ExecStoreHeapTuple(rows[i], slot, false);
        let row_copy = rows[i].clone_in(mcx)?;
        slot_seam::exec_force_store_formed_heap_tuple::call(&mut estate, slot, row_copy, false)?;

        // foreach(lc, exprstates): datum = ExecEvalExpr(exprstate, ...);
        for (j, st) in exprstates.iter_mut().enumerate() {
            let (datum, isnull) =
                expr_seam::exec_eval_expr_switch_context::call(st, econtext, &mut estate)?;
            if isnull {
                values[j].push(Datum::null());
                nulls[j].push(true);
            } else {
                values[j].push(datum);
                nulls[j].push(false);
            }
        }
    }

    // ExecDropSingleTupleTableSlot(slot); FreeExecutorState(estate);
    // (slot is owned by estate; freeing the executor state drops it.)
    expr_seam::free_executor_state::call(estate)?;

    Ok((values, nulls))
}

/// `compute_expr_stats(onerel, exprdata, nexprs, rows, numrows)`
/// (extended_stats.c:2087) — evaluate each expression over the sampled rows and
/// run its `compute_stats` callback. The `vacstats` are the per-expression
/// `VacAttrStats` built by `build_expr_data` (`examine_expression`); on return
/// each carries its computed pg_statistic slots (`stats_valid`).
pub fn compute_expr_stats<'mcx>(
    mcx: Mcx<'mcx>,
    onerel: &Relation<'mcx>,
    exprs: &[Expr],
    vacstats: &mut [VacAttrStats<'mcx>],
    rows: &[FormedTuple<'mcx>],
    numrows: i32,
) -> PgResult<()> {
    for (ind, expr) in exprs.iter().enumerate() {
        // estate = CreateExecutorState(); econtext = GetPerTupleExprContext(estate);
        let mut estate = exec_util_seam::create_executor_state::call(mcx)?;
        let econtext = exec_util_seam::get_per_tuple_expr_context::call(&mut estate)?;

        // exprstate = ExecPrepareExpr((Expr *) expr, estate);
        let expr_one = [expr.clone_in(mcx)?];
        let mut exprstate = expr_seam::exec_prepare_expr_list::call(&expr_one, &mut estate)?;

        // slot = MakeSingleTupleTableSlot(RelationGetDescr(onerel), &TTSOpsHeapTuple);
        let slot_data = table_slot_create(mcx, onerel)?;
        let slot = estate.push_slot_data(slot_data)?;
        estate.ecxt_mut(econtext).ecxt_scantuple = Some(slot);

        // exprvals = palloc(numrows * sizeof(Datum)); exprnulls = ...;
        let mut exprvals: Vec<Datum<'mcx>> = Vec::with_capacity(numrows as usize);
        let mut exprnulls: Vec<bool> = Vec::with_capacity(numrows as usize);

        let mut tcnt = 0i32;
        for i in 0..numrows as usize {
            // ResetExprContext(econtext);
            exec_util_seam::reset_expr_context::call(&mut estate, econtext)?;

            // ExecStoreHeapTuple(rows[i], slot, false);
            let row_copy = rows[i].clone_in(mcx)?;
            slot_seam::exec_force_store_formed_heap_tuple::call(
                &mut estate,
                slot,
                row_copy,
                false,
            )?;

            // datum = ExecEvalExprSwitchContext(exprstate, ...);
            let (datum, isnull) =
                expr_seam::exec_eval_expr_switch_context::call(&mut exprstate[0], econtext, &mut estate)?;
            if isnull {
                exprvals.push(Datum::null());
                exprnulls.push(true);
            } else {
                // exprvals[tcnt] = datumCopy(datum, typbyval, typlen);
                let typ = vacstats[ind]
                    .attrtype
                    .as_ref()
                    .expect("examine_expression sets attrtype");
                let copied = datum_copy_v(mcx, &datum, typ.typbyval, typ.typlen as i32)?;
                exprvals.push(copied);
                exprnulls.push(false);
            }
            tcnt += 1;
        }

        // if (tcnt > 0) { ... stats->compute_stats(stats, expr_fetch_func, tcnt, tcnt); ... }
        if tcnt > 0 {
            // aopt = get_attribute_options(onerel->rd_id, stats->tupattnum);
            // tupattnum == InvalidAttrNumber for an expression -> typically None.
            let aopt = get_attribute_options(onerel.rd_id, vacstats[ind].tupattnum)?;

            let stats = &mut vacstats[ind];
            stats.exprvals = exprvals;
            stats.exprnulls = exprnulls;
            stats.rowstride = 1;
            let compute = stats
                .compute_stats
                .expect("examine_expression guarantees compute_stats is set");
            // compute_stats(stats, expr_fetch_func, tcnt, tcnt)
            compute(stats, expr_fetch_func, tcnt, tcnt as f64);

            // If the n_distinct option is specified, it overrides the above.
            if let Some(aopt) = aopt {
                if aopt.n_distinct != 0.0 {
                    stats.stadistinct = aopt.n_distinct as f32;
                }
            }
        }

        // ExecDropSingleTupleTableSlot(slot); FreeExecutorState(estate);
        expr_seam::free_executor_state::call(estate)?;
    }

    Ok(())
}

/// `expr_fetch_func(stats, rownum, isNull)` (extended_stats.c:2228). The
/// `exprvals`/`exprnulls` are already de-strided per column (`rowstride == 1`).
fn expr_fetch_func<'mcx>(stats: &VacAttrStats<'mcx>, rownum: i32, is_null: &mut bool) -> Datum<'mcx> {
    let i = (rownum * stats.rowstride) as usize;
    *is_null = stats.exprnulls[i];
    stats.exprvals[i].clone()
}

/// `serialize_expr_stats(exprdata, nexprs)` (extended_stats.c:2271) — form one
/// `pg_statistic` row per expression and pack them into a `pg_statistic[]`
/// composite-type array `Datum` (`stxdexpr`). `Ok(None)` is never returned: the
/// array always has `nexprs` elements (an unanalyzable expression contributes a
/// NULL element, matching C's `accumArrayResult(astate, 0, true, ...)`).
pub fn serialize_expr_stats<'mcx>(
    mcx: Mcx<'mcx>,
    vacstats: &[VacAttrStats<'mcx>],
) -> PgResult<Datum<'mcx>> {
    let sd = table_open(mcx, StatisticRelationId, RowExclusiveLock)?;

    // typOid = get_rel_type_id(StatisticRelationId);
    let typ_oid = get_rel_type_id::call(StatisticRelationId)?;
    if typ_oid == Oid::from(0u32) {
        table_close(sd, RowExclusiveLock)?;
        return Err(PgError::error(
            "relation \"pg_statistic\" does not have a composite type".to_string(),
        ));
    }

    let mut elems: Vec<Datum<'mcx>> = Vec::with_capacity(vacstats.len());
    let mut elem_nulls: Vec<bool> = Vec::with_capacity(vacstats.len());

    for stats in vacstats {
        if !stats.stats_valid {
            // accumArrayResult(astate, (Datum) 0, true /*isnull*/, typOid, ...)
            elems.push(Datum::null());
            elem_nulls.push(true);
            continue;
        }

        let mut values: Vec<Datum<'mcx>> = (0..Natts_pg_statistic).map(|_| Datum::null()).collect();
        let mut nulls = [false; Natts_pg_statistic];

        values[Anum_pg_statistic_starelid - 1] = Datum::from_oid(Oid::from(0u32));
        values[Anum_pg_statistic_staattnum - 1] = Datum::from_i16(INVALID_ATTR_NUMBER as i16);
        values[Anum_pg_statistic_stainherit - 1] = Datum::from_bool(false);
        values[Anum_pg_statistic_stanullfrac - 1] = Datum::from_f32(stats.stanullfrac);
        values[Anum_pg_statistic_stawidth - 1] = Datum::from_i32(stats.stawidth);
        values[Anum_pg_statistic_stadistinct - 1] = Datum::from_f32(stats.stadistinct);

        let mut i = Anum_pg_statistic_stakind1 - 1;
        for k in 0..STATISTIC_NUM_SLOTS {
            values[i] = Datum::from_i16(stats.stakind[k]);
            i += 1;
        }
        i = Anum_pg_statistic_staop1 - 1;
        for k in 0..STATISTIC_NUM_SLOTS {
            values[i] = Datum::from_oid(stats.staop[k]);
            i += 1;
        }
        i = Anum_pg_statistic_stacoll1 - 1;
        for k in 0..STATISTIC_NUM_SLOTS {
            values[i] = Datum::from_oid(stats.stacoll[k]);
            i += 1;
        }
        i = Anum_pg_statistic_stanumbers1 - 1;
        for k in 0..STATISTIC_NUM_SLOTS {
            let nnum = stats.numnumbers[k];
            if nnum > 0 {
                let numdatums: Vec<Datum<'mcx>> = (0..nnum as usize)
                    .map(|n| Datum::from_f32(stats.stanumbers[k][n]))
                    .collect();
                // construct_array_builtin(numdatums, nnum, FLOAT4OID): float4 is
                // pass-by-value, length 4, 'i' alignment.
                let arry = arrayfuncs::construct::construct_array_values(
                    mcx, &numdatums[..], FLOAT4OID, 4, true, b'i',
                )?;
                values[i] = Datum::ByRef(arry);
            } else {
                nulls[i] = true;
                values[i] = Datum::null();
            }
            i += 1;
        }
        i = Anum_pg_statistic_stavalues1 - 1;
        for k in 0..STATISTIC_NUM_SLOTS {
            if stats.numvalues[k] > 0 {
                let arry = arrayfuncs::construct::construct_array_values(
                    mcx,
                    &stats.stavalues[k][..],
                    stats.statypid[k],
                    stats.statyplen[k] as i32,
                    stats.statypbyval[k],
                    stats.statypalign[k] as u8,
                )?;
                values[i] = Datum::ByRef(arry);
            } else {
                nulls[i] = true;
                values[i] = Datum::null();
            }
            i += 1;
        }

        // stup = heap_form_tuple(RelationGetDescr(sd), values, nulls);
        let stup = heaptuple::heap_form_tuple(mcx, &sd.rd_att, &values, &nulls)
            .map_err(|e| PgError::error(format!("serialize_expr_stats: heap_form_tuple: {e:?}")))?;
        // heap_copy_tuple_as_datum(stup, RelationGetDescr(sd)) -> composite Datum.
        let composite =
            heaptuple::heap_copy_tuple_as_datum(mcx, &stup, &sd.rd_att)?;
        elems.push(Datum::Composite(composite));
        elem_nulls.push(false);
    }

    table_close(sd, RowExclusiveLock)?;

    // makeArrayResult(astate, ...): a 1-D pg_statistic[] array. The composite
    // rowtype is a varlena (pass-by-reference, typlen -1, 'd' alignment).
    let nelems = elems.len() as i32;
    let dims = [nelems];
    let lbs = [1];
    let has_nulls = elem_nulls.iter().any(|&n| n);
    let nulls_opt = if has_nulls { Some(&elem_nulls[..]) } else { None };
    let arr = construct_md_array_values(
        mcx, &elems[..], nulls_opt, 1, &dims, &lbs, typ_oid, -1, false, b'd',
    )?;
    Ok(Datum::ByRef(arr))
}
