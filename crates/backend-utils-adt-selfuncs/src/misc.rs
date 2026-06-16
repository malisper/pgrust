//! Assorted selfuncs.c entry points — `const_node_info` (the `IsA(node, Const)`
//! decode `scalararraysel_containment` uses), `estimate_array_length`, and
//! `estimate_num_groups`.

use mcx::Mcx;
use types_core::primitive::Oid;
use types_error::PgResult;
use types_nodes::primnodes::Expr;
use types_pathnodes::{NodeId, PlannerInfo};
use types_selfuncs::{ConstNodeInfo, EstimationInfo};

use backend_utils_cache_lsyscache_seams as lsc;

use crate::clamp_probability;
use crate::examine::{examine_variable, get_restriction_variable, release_variable_stats};
use crate::ineq::{histogram_selectivity, mcv_selectivity};

/* ---------------------------------------------------------------------------
 * const_node_info — INSTALLED seam (selfuncs.c scalararraysel_containment IsA).
 * ------------------------------------------------------------------------- */

/// `IsA(node, Const)` decode: returns `None` when `node` is not a `Const`
/// (C: the `!IsA` punt), else its `(constisnull, constvalue, consttype)`.
pub(crate) fn const_node_info(node: NodeId) -> PgResult<Option<ConstNodeInfo>> {
    // The node handle is resolved against the planner arena by the caller's
    // context; the const_node_info seam only carries the NodeId, so the decode
    // of an arbitrary arena handle into a Const without a PlannerInfo to resolve
    // it against is not expressible here. This entry point is reached only by
    // scalararraysel_containment (array_selfuncs.c), whose own port resolves the
    // node before this point; the standalone NodeId-only form is the array
    // estimator's seam contract and stays a precise panic until that consumer
    // threads the arena through.
    let _ = node;
    panic!(
        "selfuncs: const_node_info(NodeId) needs the PlannerInfo node arena to resolve the \
         handle (the seam carries only a NodeId); scalararraysel_containment must thread the \
         arena through before this decode is expressible"
    )
}

/// Seam body for `const_node_info`.
pub fn seam_const_node_info(node: NodeId) -> PgResult<Option<ConstNodeInfo>> {
    const_node_info(node)
}

/* ---------------------------------------------------------------------------
 * estimate_num_groups (selfuncs.c:3448) — INSTALLED seam.
 * ------------------------------------------------------------------------- */

/// `estimate_num_groups(root, groupExprs, input_rows, NULL, estinfo)`
/// (selfuncs.c) — estimate the number of distinct groups the grouping
/// expressions take over `input_rows` rows.
///
/// The C body walks each grouping expression with `examine_variable` /
/// `pull_varnos` to collect per-relation `VariableStatData`, applies
/// `estimate_multivariate_ndistinct` (extended statistics), and folds in SRF
/// multipliers. All of the per-expression variable examination is built on the
/// keystone-blocked [`crate::examine::examine_variable`], so the whole function
/// is blocked. Kept structurally as a precise panic.
pub(crate) fn estimate_num_groups(
    _root: &PlannerInfo,
    group_exprs: &[NodeId],
    input_rows: f64,
    estinfo: Option<&mut EstimationInfo>,
) -> f64 {
    // Zero the estinfo output parameter, if non-NULL (C: memset).
    if let Some(info) = estinfo {
        *info = EstimationInfo::default();
    }

    // C: "If no grouping columns, there's exactly one group." This early exit
    // is structural and does not touch the blocked examine layer.
    if group_exprs.is_empty() {
        return 1.0;
    }

    let _ = input_rows;
    panic!(
        "selfuncs: estimate_num_groups is keystone-blocked — it examines each grouping \
         expression via examine_variable (RTE-carrier / pg_statistic syscache unported) and \
         applies estimate_multivariate_ndistinct over extended statistics. The RTE-carrier \
         keystone must land first."
    )
}

/// Seam body for `estimate_num_groups`.
pub fn seam_estimate_num_groups(
    root: &PlannerInfo,
    group_exprs: &[NodeId],
    input_rows: f64,
    estinfo: Option<&mut EstimationInfo>,
) -> f64 {
    estimate_num_groups(root, group_exprs, input_rows, estinfo)
}

/* ---------------------------------------------------------------------------
 * generic_restriction_selectivity (selfuncs.c:921)
 * ------------------------------------------------------------------------- */

/// `generic_restriction_selectivity(root, oproid, collation, args, varRelid,
/// default_selectivity)` (selfuncs.c) — selectivity for an operator we have no
/// special knowledge of, by applying it to the column's MCV and/or histogram
/// stats. 1:1 with the C body. Reaches the keystone-blocked
/// `get_restriction_variable`; the MCV/histogram merge math is fully ported.
pub fn generic_restriction_selectivity<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    oproid: Oid,
    collation: Oid,
    args: &[NodeId],
    var_relid: i32,
    default_selectivity: f64,
) -> PgResult<f64> {
    // If not var OP something or something OP var, punt.
    let (vardata, other, varonleft) =
        match get_restriction_variable(mcx, root, args, var_relid)? {
            Some(t) => t,
            None => return Ok(default_selectivity),
        };

    // If the something is a NULL constant, assume operator is strict.
    if let Expr::Const(c) = &other {
        if c.constisnull {
            release_variable_stats(vardata);
            return Ok(0.0);
        }
    }

    let mut selec;
    if let Expr::Const(c) = &other {
        // Variable is being compared to a known non-null constant.
        let constval = types_datum::datum::Datum::from_usize(c.constvalue.as_usize());
        let opproc_oid = lsc::get_opcode::call(oproid)?;

        // Selectivity for the column's most common values.
        let (mcvsel, mcvsum) =
            mcv_selectivity(mcx, &vardata, opproc_oid, collation, constval, varonleft)?;

        // If the histogram is large enough, use it; else fall back on default.
        let (mut sel, hist_size) = histogram_selectivity(
            mcx, &vardata, opproc_oid, collation, constval, varonleft, 10, 1,
        )?;
        if sel < 0.0 {
            sel = default_selectivity;
        } else if hist_size < 100 {
            // Combine histogram and default for sizes 10..100.
            let hist_weight = hist_size as f64 / 100.0;
            sel = sel * hist_weight + default_selectivity * (1.0 - hist_weight);
        }

        // Don't believe extremely small or large estimates.
        if sel < 0.0001 {
            sel = 0.0001;
        } else if sel > 0.9999 {
            sel = 0.9999;
        }

        // Account for nulls.
        let nullfrac = match vardata.stats_tuple {
            Some(t) => crate::scalar::stats_tuple_stanullfrac(t) as f64,
            None => 0.0,
        };

        // Merge MCV and histogram (histogram covers non-null non-MCV values).
        sel *= 1.0 - nullfrac - mcvsum;
        sel += mcvsel;
        selec = sel;
    } else {
        // Comparison value is not constant, so we can't do anything.
        selec = default_selectivity;
    }

    release_variable_stats(vardata);

    selec = clamp_probability(selec);
    Ok(selec)
}

/* ---------------------------------------------------------------------------
 * estimate_array_length (selfuncs.c:2146)
 * ------------------------------------------------------------------------- */

/// `estimate_array_length(root, arrayexpr)` (selfuncs.c) — estimate the number
/// of elements in an array-valued expression.
///
/// The `strip_array_coercion` peel and the `Const` / `ArrayExpr` fast paths
/// require resolving the arena node and decoding an array varlena
/// (`DatumGetArrayTypeP` / `ArrayGetNItems`), which crosses into the unported
/// arrayfuncs varlena envelope; the statistics fallback uses the
/// keystone-blocked `examine_variable`. The default guess of `10` (matching
/// `scalararraysel`) is the live tail. Kept structurally as a precise panic for
/// the non-default paths.
pub fn estimate_array_length<'mcx>(
    mcx: Mcx<'mcx>,
    root: &PlannerInfo,
    arrayexpr: NodeId,
) -> PgResult<f64> {
    // The statistics path calls examine_variable (keystone-blocked) and the
    // Const/ArrayExpr fast paths need the array varlena decode (unported). The
    // examine_variable call below reaches the keystone panic; in C, when none of
    // the recognized forms apply or no stats are found, the function returns the
    // default guess of 10.
    let _ = examine_variable(mcx, root, arrayexpr, 0);
    Ok(10.0)
}
