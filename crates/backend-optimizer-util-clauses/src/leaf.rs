//! Small expression-leaf helpers that sit alongside the clause manipulators.
//!
//! `clamp_row_est` (costsize.c) and `estimate_array_length` (selfuncs.c) are the
//! two tiny, self-contained routines `clauses.c`'s estimators reach for over the
//! `Expr` tree. They are ported in-line here (rather than hopped to the
//! costsize/selfuncs owners) because each is a few lines of self-contained
//! arithmetic over the modeled `Expr` shapes.
//!
//! The larger `cost_qual_eval` / `cost_qual_eval_node` / `order_qual_clauses` /
//! `extract_actual_clauses` routines that the src-idiomatic bundle co-located
//! here are NOT part of `clauses.c` — they live in `path/costsize.c`,
//! `plan/createplan.c`, and `util/restrictinfo.c`, and operate on `RestrictInfo`
//! / `PlannerInfo` / `QualCost` (with `RestrictInfo.clause` carried as a
//! `NodeId` handle in this repo's model, and `add_function_cost` /
//! `cpu_operator_cost` not yet seamed). They belong to those units and are
//! ported with them, not here.

use types_nodes::primnodes::{CoercionForm, Expr};

use backend_utils_adt_arrayfuncs_seams as arrayfuncs_seam;

/// `clamp_row_est(nrows)` (costsize.c) — force a row-count estimate to be at
/// least one row, rounding to an integer. A tiny self-contained arithmetic
/// helper, ported in-line to avoid a cross-module hop for one expression.
pub(crate) fn clamp_row_est(nrows: f64) -> f64 {
    if nrows.is_nan() || nrows <= 1.0 {
        1.0
    } else {
        nrows.round()
    }
}

/// `estimate_array_length(root, arrayexpr)` (selfuncs.c:2143), over the two
/// shapes a planned IN-list array operand takes (a folded array `Const` or an
/// `ArrayExpr`). The third C arm probes `pg_statistic` (DECHIST) via
/// `examine_variable`; with no analyzed stats on this engine the genuine result
/// is C's no-stats fall-through — the default guess of 10.
///
/// `Err` carries the `array_const_nitems` overflow / detoast surface.
pub fn estimate_array_length(arrayexpr: &Expr) -> types_error::PgResult<f64> {
    // look through any binary-compatible relabeling of arrayexpr
    let arrayexpr = strip_array_coercion(arrayexpr);

    if let Some(c) = arrayexpr.as_const() {
        if c.constisnull {
            return Ok(0.0);
        }
        // ArrayGetNItems(ARR_NDIM(arrayval), ARR_DIMS(arrayval)).
        let nitems = arrayfuncs_seam::array_const_nitems::call(types_datum::datum::Datum::from_usize(
            c.constvalue.as_usize(),
        ))?;
        return Ok(nitems as f64);
    }
    if let Some(ae) = arrayexpr.as_arrayexpr() {
        if !ae.multidims {
            return Ok(ae.elements.len() as f64);
        }
    }

    // Else use a default guess of 10 (the empty-pg_statistic result of the C
    // examine_variable/DECHIST probe too).
    Ok(10.0)
}

/// `strip_array_coercion(node)` (selfuncs.c, static) — strip binary-compatible
/// relabeling from an array node.
fn strip_array_coercion(node: &Expr) -> &Expr {
    let mut node = node;
    loop {
        if let Some(acoerce) = node.as_arraycoerceexpr() {
            // If it's an array-to-array conversion that just relabels the
            // element type, look through it.
            if acoerce
                .elemexpr
                .as_deref()
                .map(|e| matches!(e, Expr::RelabelType(_)))
                .unwrap_or(false)
            {
                if let Some(arg) = acoerce.arg.as_deref() {
                    node = arg;
                    continue;
                }
            }
            break;
        } else if let Some(relabel) = node.as_relabeltype() {
            if relabel.relabelformat != CoercionForm::COERCE_EXPLICIT_CAST {
                // (selfuncs.c looks through any RelabelType here.)
            }
            if let Some(arg) = relabel.arg.as_deref() {
                node = arg;
                continue;
            }
            break;
        } else {
            break;
        }
    }
    node
}
