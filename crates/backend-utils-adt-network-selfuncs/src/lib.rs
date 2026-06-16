//! Selectivity estimation of `inet`/`cidr` operators
//! (`utils/adt/network_selfuncs.c`, PostgreSQL 18.3).
//!
//! This module provides estimators for the subnet inclusion (`<<`, `<<=`, `>>`,
//! `>>=`) and overlap (`&&`) operators. Estimates are based on null fraction,
//! most common values, and histogram of `inet`/`cidr` columns.
//!
//! Every function defined in the C file is ported here, branch-for-branch:
//!
//!   * the SQL-callable [`networksel`] / [`networkjoinsel`] restriction & join
//!     estimators (their `FunctionCallInfo` arguments become explicit
//!     parameters: `root` is `&PlannerInfo`, `args` is the operator-argument
//!     `List *` as `&[NodeId]`, `sjinfo` is `&SpecialJoinInfo`, and the result
//!     is the plain `Selectivity` (`f64`) the C `PG_RETURN_FLOAT8` packs);
//!   * the inner / semi-anti join drivers [`networkjoinsel_inner`] /
//!     [`networkjoinsel_semi`];
//!   * the MCV/histogram value & join estimators `mcv_population`,
//!     `inet_hist_value_sel`, `inet_mcv_join_sel`, `inet_mcv_hist_sel`,
//!     `inet_hist_inclusion_join_sel`, `inet_semi_join_sel`; and
//!   * the inet inclusion/divider comparators `inet_opr_codenum`,
//!     `inet_inclusion_cmp`, `inet_masklen_inclusion_cmp`,
//!     `inet_hist_match_divider`.
//!
//! Control flow, loop bounds, the decimation factors (`(n-2)/MAX+1`,
//! `(n-3)/MAX+1`), the `1.0 / pow(2, max(divider))` partial-match weighting and
//! the operator-code negation conventions are preserved exactly.
//!
//! Cross-unit dependencies cross per-owner seams:
//!
//!   * the planner variable-resolution + MCV scan (`get_restriction_variable`,
//!     `get_join_variables`, `mcv_selectivity`, `ReleaseVariableStats`) live in
//!     `backend-utils-adt-selfuncs-seams`; the owning `selfuncs.c` unit is
//!     unported (the examine/estimate F1-F7 families), so those slots are
//!     uninstalled and panic loudly until it lands (mirror-PG-and-panic, exactly
//!     as the merged `backend-utils-adt-range-selfuncs` does);
//!   * `get_attstatsslot` (the `pg_statistic` slot detoast) and the
//!     `get_opcode` / `get_commutator` operator-metadata lookups are
//!     `lsyscache` seams — already installed by the ported lsyscache unit;
//!   * `stats_tuple_stanullfrac` reads `pg_statistic.stanullfrac` off the
//!     syscache-pinned tuple;
//!   * the fmgr operator call (`FunctionCall2`, the operator's underlying
//!     function looked up via `get_opcode`) crosses the fmgr `function_call2_coll`
//!     seam (the cached `FmgrInfo` is faithfully replaced by re-resolving the
//!     opcode OID per call — behavior identical);
//!   * `DatumGetInetPP` (the inet varlena detoast of a `pg_statistic` value /
//!     query `Const` word) crosses `backend-utils-adt-network-seams::inet`.
//!
//! The `bitncmp` / `bitncommon` bit helpers are the already-ported
//! [`backend_utils_adt_network`] crate (a direct dependency).

// The selectivity model mirrors `network_selfuncs.c`'s control flow and float
// arithmetic verbatim, so a handful of style lints fire on the faithful shape
// and are allowed here with the same rationale as the rest of the port:
//   * `CLAMP_PROBABILITY` keeps C's explicit `< 0 .. > 1` branch order (the NaN
//     semantics differ from `f64::clamp`), so `manual_clamp` must not rewrite it.
//   * several locals are declared up front (C89 style) and assigned later
//     (`needless_late_init`).
//   * `PgError` is a large value, so `result_large_err` fires on every fallible
//     fn; this is a project-wide property of the error type.
//   * the histogram/MCV loops walk parallel arrays by explicit index
//     (`needless_range_loop`).
#![allow(clippy::manual_clamp)]
#![allow(clippy::needless_late_init)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_range_loop)]
#![allow(non_upper_case_globals)]

extern crate alloc;

use mcx::Mcx;
use types_core::primitive::{InvalidOid, Selectivity};
use types_core::Oid;
use types_datum::datum::Datum;
use types_error::{PgError, PgResult, ERROR};
use types_network::inet_struct;
use types_nodes::primnodes::Expr;
use types_pathnodes::{
    NodeId, PlannerInfo, SpecialJoinInfo, JOIN_ANTI, JOIN_FULL, JOIN_INNER, JOIN_LEFT, JOIN_SEMI,
};
use types_selfuncs::{
    AttStatsSlot, VariableStatData, ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES,
};

use backend_utils_adt_network::{bitncmp, bitncommon};
use backend_utils_adt_network_seams::inet::datum_get_inet_pp;
use backend_utils_adt_selfuncs_seams::{
    get_join_variables, get_restriction_variable, mcv_selectivity, release_variable_stats,
    stats_tuple_stanullfrac,
};
use backend_utils_cache_lsyscache_seams::{get_attstatsslot, get_commutator, get_opcode};
use backend_utils_fmgr_fmgr_seams::function_call2_coll;

/// Install every seam this crate owns. This crate owns no inward seams (its
/// fmgr entry points are reached through fmgr dispatch, not a cross-cycle seam),
/// so there is nothing to install.
pub fn init_seams() {}

/* ---------------------------------------------------------------------------
 * Operator OIDs (catalog/pg_operator.dat oid values).
 * ------------------------------------------------------------------------- */

/// `OID_INET_SUB_OP` (oid 931) — `<<` (is subnet).
const OID_INET_SUB_OP: Oid = 931;
/// `OID_INET_SUBEQ_OP` (oid 932) — `<<=` (is subnet or equal).
const OID_INET_SUBEQ_OP: Oid = 932;
/// `OID_INET_SUP_OP` (oid 933) — `>>` (is supernet).
const OID_INET_SUP_OP: Oid = 933;
/// `OID_INET_SUPEQ_OP` (oid 934) — `>>=` (is supernet or equal).
const OID_INET_SUPEQ_OP: Oid = 934;
/// `OID_INET_OVERLAP_OP` (oid 3552) — `&&` (overlaps).
const OID_INET_OVERLAP_OP: Oid = 3552;

/* ---------------------------------------------------------------------------
 * Statistics-kind constants (catalog/pg_statistic.h).
 * ------------------------------------------------------------------------- */

/// `STATISTIC_KIND_MCV` — most-common-values slot.
const STATISTIC_KIND_MCV: i32 = 1;
/// `STATISTIC_KIND_HISTOGRAM` — histogram slot.
const STATISTIC_KIND_HISTOGRAM: i32 = 2;

/* ---------------------------------------------------------------------------
 * Module-private constants (network_selfuncs.c:33-44).
 * ------------------------------------------------------------------------- */

/// `DEFAULT_OVERLAP_SEL` — default selectivity for the inet overlap operator.
const DEFAULT_OVERLAP_SEL: f64 = 0.01;
/// `DEFAULT_INCLUSION_SEL` — default selectivity for the inclusion operators.
const DEFAULT_INCLUSION_SEL: f64 = 0.005;
/// `MAX_CONSIDERED_ELEMS` — max items considered in join selectivity calcs.
const MAX_CONSIDERED_ELEMS: i32 = 1024;

/// `DEFAULT_SEL(operator)` — default selectivity for the specified operator.
#[inline]
fn default_sel(operator: Oid) -> Selectivity {
    if operator == OID_INET_OVERLAP_OP {
        DEFAULT_OVERLAP_SEL
    } else {
        DEFAULT_INCLUSION_SEL
    }
}

/* ---------------------------------------------------------------------------
 * Local mirrors of the inline C macros / helpers.
 * ------------------------------------------------------------------------- */

/// `CLAMP_PROBABILITY(p)` (selfuncs.h) — clamp to `[0,1]`, keeping C's explicit
/// `< 0 .. > 1` branch order (the NaN semantics differ from `f64::clamp`).
#[inline]
fn clamp_probability(p: &mut Selectivity) {
    if *p < 0.0 {
        *p = 0.0;
    } else if *p > 1.0 {
        *p = 1.0;
    }
}

/// `Min(x, y)` for ints.
#[inline]
fn min_i32(x: i32, y: i32) -> i32 {
    if x < y {
        x
    } else {
        y
    }
}

/// `Max(x, y)` for ints.
#[inline]
fn max_i32(x: i32, y: i32) -> i32 {
    if x > y {
        x
    } else {
        y
    }
}

/// `Min(x, y)` for floats.
#[inline]
fn min_f64(x: f64, y: f64) -> f64 {
    if x < y {
        x
    } else {
        y
    }
}

/// `elog(ERROR, msg)` — raise an internal error as a recoverable value.
#[inline]
fn elog_error<T>(msg: impl Into<alloc::string::String>) -> PgResult<T> {
    Err(PgError::new(ERROR, msg))
}

/* ---------------------------------------------------------------------------
 * inet field accessors (utils/inet.h ip_* macros, applied to an `inet_struct`).
 * ------------------------------------------------------------------------- */

/// `ip_family(inetptr)`.
#[inline]
fn ip_family(p: &inet_struct) -> i32 {
    p.family as i32
}

/// `ip_bits(inetptr)`.
#[inline]
fn ip_bits(p: &inet_struct) -> i32 {
    p.bits as i32
}

/// `ip_addr(inetptr)` — the 16-byte address array.
#[inline]
fn ip_addr(p: &inet_struct) -> &[u8] {
    &p.ipaddr
}

/// `DatumGetInetPP(X)` — detoast the Datum word and return its `inet_struct`
/// payload (crosses the network varlena-detoast seam).
#[inline]
fn datum_get_inet_pp_word(value: Datum) -> PgResult<inet_struct> {
    datum_get_inet_pp::call(value)
}

/* ---------------------------------------------------------------------------
 * Variable-stats RAII guard (C: ReleaseVariableStats on every exit path).
 * ------------------------------------------------------------------------- */

/// Holds a `VariableStatData` acquired by `get_restriction_variable` /
/// `get_join_variables`, running `ReleaseVariableStats` (the
/// `release_variable_stats` seam) on drop — covering every early return / `?`
/// exit, the C cleanup that AGENTS.md requires be RAII.
struct VarStatsGuard {
    vardata: VariableStatData,
}

impl VarStatsGuard {
    fn new(vardata: VariableStatData) -> Self {
        Self { vardata }
    }

    fn data(&self) -> &VariableStatData {
        &self.vardata
    }
}

impl Drop for VarStatsGuard {
    fn drop(&mut self) {
        release_variable_stats::call(self.vardata);
    }
}

/* =========================================================================
 * mcv_population — fraction of a relation's population represented by the MCV
 * list. (network_selfuncs.c:553-565)
 * ====================================================================== */

fn mcv_population(mcv_numbers: &[f32], mcv_nvalues: i32) -> Selectivity {
    let mut sumcommon: Selectivity = 0.0;
    let mut i: i32 = 0;
    while i < mcv_nvalues {
        sumcommon += mcv_numbers[i as usize] as f64;
        i += 1;
    }
    sumcommon
}

/* =========================================================================
 * networksel — restriction selectivity. (network_selfuncs.c:78-183)
 * ====================================================================== */

/// `networksel` — selectivity estimation for the subnet inclusion/overlap
/// operators.
pub fn networksel(
    mcx: Mcx<'_>,
    root: &PlannerInfo,
    operator: Oid,
    args: &[NodeId],
    var_relid: i32,
) -> PgResult<Selectivity> {
    let mut selec: Selectivity;
    let mcv_selec: f64;
    let non_mcv_selec: f64;
    let constvalue: Datum;
    let sumcommon: f64;

    /*
     * Before all else, verify that the operator is one of the ones supported
     * by this function, which in turn proves that the input datatypes are what
     * we expect.
     */
    let opr_codenum = inet_opr_codenum(operator)?;

    /*
     * If expression is not (variable op something) or (something op variable),
     * then punt and return a default estimate.
     */
    let (vardata, other, varonleft) =
        match get_restriction_variable::call(mcx, root, args, var_relid)? {
            Some(t) => t,
            None => return Ok(default_sel(operator)),
        };
    /* RAII: ReleaseVariableStats on every exit path below. */
    let vardata = VarStatsGuard::new(vardata);

    /*
     * Can't do anything useful if the something is not a constant, either.
     */
    let other = match other {
        Expr::Const(c) => c,
        _ => return Ok(default_sel(operator)),
    };

    /* All of the operators handled here are strict. */
    if other.constisnull {
        return Ok(0.0);
    }
    /* The Const's value is the canonical Datum; pull out the bare word for the
     * inet varlena detoast / fmgr lane (mirrors range-selfuncs). */
    constvalue = Datum::from_usize(other.constvalue.as_usize());

    /* Otherwise, we need stats in order to produce a non-default estimate. */
    let stats_tuple = match vardata.data().stats_tuple {
        Some(t) => t,
        None => return Ok(default_sel(operator)),
    };

    let nullfrac = stats_tuple_stanullfrac::call(stats_tuple) as f64;

    /*
     * If we have most-common-values info, add up the fractions of the MCV
     * entries that satisfy MCV OP CONST.  These fractions contribute directly
     * to the result selectivity.  Also add up the total fraction represented by
     * MCV entries.
     */
    let opcode = get_opcode::call(operator)?;
    (mcv_selec, sumcommon) = mcv_selectivity::call(
        mcx,
        vardata.data(),
        opcode,
        InvalidOid,
        constvalue,
        varonleft,
    )?;

    /*
     * If we have a histogram, use it to estimate the proportion of the non-MCV
     * population that satisfies the clause.  If we don't, apply the default
     * selectivity to that population.
     */
    if let Some(hslot) = get_attstatsslot::call(
        mcx,
        stats_tuple,
        STATISTIC_KIND_HISTOGRAM,
        InvalidOid,
        ATTSTATSSLOT_VALUES,
    )? {
        /* Commute if needed, so we can consider histogram to be on the left */
        let h_codenum = if varonleft { opr_codenum } else { -opr_codenum };
        non_mcv_selec = inet_hist_value_sel(&hslot.values, constvalue, h_codenum)?;
        /* hslot frees on drop (C: free_attstatsslot) */
    } else {
        non_mcv_selec = default_sel(operator);
    }

    /* Combine selectivities for MCV and non-MCV populations */
    selec = mcv_selec + (1.0 - nullfrac - sumcommon) * non_mcv_selec;

    /* Result should be in range, but make sure... */
    clamp_probability(&mut selec);

    Ok(selec)
}

/* =========================================================================
 * networkjoinsel — join selectivity. (network_selfuncs.c:203-268)
 * ====================================================================== */

/// `networkjoinsel` — join selectivity estimation for the subnet
/// inclusion/overlap operators.  Same structure as `eqjoinsel`.
pub fn networkjoinsel(
    mcx: Mcx<'_>,
    root: &PlannerInfo,
    operator: Oid,
    args: &[NodeId],
    sjinfo: &SpecialJoinInfo,
) -> PgResult<Selectivity> {
    let mut selec: f64;

    /*
     * Before all else, verify that the operator is one of the ones supported by
     * this function.
     */
    let opr_codenum = inet_opr_codenum(operator)?;

    let (vardata1, vardata2, join_is_reversed) =
        get_join_variables::call(mcx, root, args, sjinfo)?;
    /* RAII: ReleaseVariableStats on every exit path below (both sides). */
    let vardata1 = VarStatsGuard::new(vardata1);
    let vardata2 = VarStatsGuard::new(vardata2);

    match sjinfo.jointype {
        JOIN_INNER | JOIN_LEFT | JOIN_FULL => {
            /*
             * Selectivity for left/full join is not exactly the same as inner
             * join, but we neglect the difference, as eqjoinsel does.
             */
            selec = networkjoinsel_inner(
                mcx,
                operator,
                opr_codenum,
                vardata1.data(),
                vardata2.data(),
            )?;
        }
        JOIN_SEMI | JOIN_ANTI => {
            /* Here, it's important that we pass the outer var on the left. */
            if !join_is_reversed {
                selec = networkjoinsel_semi(
                    mcx,
                    root,
                    operator,
                    opr_codenum,
                    vardata1.data(),
                    vardata2.data(),
                )?;
            } else {
                selec = networkjoinsel_semi(
                    mcx,
                    root,
                    get_commutator::call(operator)?,
                    -opr_codenum,
                    vardata2.data(),
                    vardata1.data(),
                )?;
            }
        }
        other => {
            /* other values not expected here */
            return elog_error(alloc::format!("unrecognized join type: {}", other as i32));
        }
    }

    clamp_probability(&mut selec);

    Ok(selec)
}

/* =========================================================================
 * networkjoinsel_inner. (network_selfuncs.c:282-398)
 * ====================================================================== */

/// `networkjoinsel_inner` — inner join selectivity for the subnet
/// inclusion/overlap operators.
fn networkjoinsel_inner(
    mcx: Mcx<'_>,
    operator: Oid,
    opr_codenum: i32,
    vardata1: &VariableStatData,
    vardata2: &VariableStatData,
) -> PgResult<Selectivity> {
    let mut nullfrac1: f64 = 0.0;
    let mut nullfrac2: f64 = 0.0;
    let mut selec: f64 = 0.0;
    let mut sumcommon1: f64 = 0.0;
    let mut sumcommon2: f64 = 0.0;
    let mut mcv1_length: i32 = 0;
    let mut mcv2_length: i32 = 0;

    /* `memset(&slot, 0, ...)` — an empty (absent) slot. */
    let mut mcv1_slot: Option<AttStatsSlot> = None;
    let mut mcv2_slot: Option<AttStatsSlot> = None;
    let mut hist1_slot: Option<AttStatsSlot> = None;
    let mut hist2_slot: Option<AttStatsSlot> = None;

    if let Some(stats_tuple) = vardata1.stats_tuple {
        nullfrac1 = stats_tuple_stanullfrac::call(stats_tuple) as f64;

        mcv1_slot = get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        )?;
        hist1_slot = get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        )?;
        /* Arbitrarily limit number of MCVs considered */
        if let Some(ref s) = mcv1_slot {
            mcv1_length = min_i32(s.values.len() as i32, MAX_CONSIDERED_ELEMS);
            sumcommon1 = mcv_population(&s.numbers, mcv1_length);
        }
    }

    if let Some(stats_tuple) = vardata2.stats_tuple {
        nullfrac2 = stats_tuple_stanullfrac::call(stats_tuple) as f64;

        mcv2_slot = get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        )?;
        hist2_slot = get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        )?;
        /* Arbitrarily limit number of MCVs considered */
        if let Some(ref s) = mcv2_slot {
            mcv2_length = min_i32(s.values.len() as i32, MAX_CONSIDERED_ELEMS);
            sumcommon2 = mcv_population(&s.numbers, mcv2_length);
        }
    }

    let mcv1_exists = mcv1_slot.is_some();
    let mcv2_exists = mcv2_slot.is_some();
    let hist1_exists = hist1_slot.is_some();
    let hist2_exists = hist2_slot.is_some();

    /*
     * Calculate selectivity for MCV vs MCV matches.
     */
    if let (Some(m1), Some(m2)) = (&mcv1_slot, &mcv2_slot) {
        selec += inet_mcv_join_sel(
            &m1.values,
            &m1.numbers,
            mcv1_length,
            &m2.values,
            &m2.numbers,
            mcv2_length,
            operator,
        )?;
    }

    /*
     * Add in selectivities for MCV vs histogram matches, scaling according to
     * the fractions of the populations represented by the histograms.  Note
     * that the second case needs to commute the operator.
     */
    if let (Some(m1), Some(h2)) = (&mcv1_slot, &hist2_slot) {
        selec += (1.0 - nullfrac2 - sumcommon2)
            * inet_mcv_hist_sel(&m1.values, &m1.numbers, mcv1_length, &h2.values, opr_codenum)?;
    }
    if let (Some(m2), Some(h1)) = (&mcv2_slot, &hist1_slot) {
        selec += (1.0 - nullfrac1 - sumcommon1)
            * inet_mcv_hist_sel(&m2.values, &m2.numbers, mcv2_length, &h1.values, -opr_codenum)?;
    }

    /*
     * Add in selectivity for histogram vs histogram matches, again scaling
     * appropriately.
     */
    if let (Some(h1), Some(h2)) = (&hist1_slot, &hist2_slot) {
        selec += (1.0 - nullfrac1 - sumcommon1)
            * (1.0 - nullfrac2 - sumcommon2)
            * inet_hist_inclusion_join_sel(&h1.values, &h2.values, opr_codenum)?;
    }

    /*
     * If useful statistics are not available then use the default estimate.  We
     * can apply null fractions if known, though.
     */
    if (!mcv1_exists && !hist1_exists) || (!mcv2_exists && !hist2_exists) {
        selec = (1.0 - nullfrac1) * (1.0 - nullfrac2) * default_sel(operator);
    }

    /* Release stats — the owned slots drop here (C: free_attstatsslot). */
    Ok(selec)
}

/* =========================================================================
 * networkjoinsel_semi. (network_selfuncs.c:406-547)
 * ====================================================================== */

/// `networkjoinsel_semi` — semi/anti join selectivity for the subnet
/// inclusion/overlap operators.
fn networkjoinsel_semi(
    mcx: Mcx<'_>,
    root: &PlannerInfo,
    operator: Oid,
    opr_codenum: i32,
    vardata1: &VariableStatData,
    vardata2: &VariableStatData,
) -> PgResult<Selectivity> {
    let mut selec: f64 = 0.0;
    let mut sumcommon1: f64 = 0.0;
    let mut sumcommon2: f64 = 0.0;
    let mut nullfrac1: f64 = 0.0;
    let mut nullfrac2: f64 = 0.0;
    let mut hist2_weight: f64 = 0.0;
    let mut mcv1_length: i32 = 0;
    let mut mcv2_length: i32 = 0;

    let mut mcv1_slot: Option<AttStatsSlot> = None;
    let mut mcv2_slot: Option<AttStatsSlot> = None;
    let mut hist1_slot: Option<AttStatsSlot> = None;
    let mut hist2_slot: Option<AttStatsSlot> = None;

    if let Some(stats_tuple) = vardata1.stats_tuple {
        nullfrac1 = stats_tuple_stanullfrac::call(stats_tuple) as f64;

        mcv1_slot = get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        )?;
        hist1_slot = get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        )?;
        if let Some(ref s) = mcv1_slot {
            mcv1_length = min_i32(s.values.len() as i32, MAX_CONSIDERED_ELEMS);
            sumcommon1 = mcv_population(&s.numbers, mcv1_length);
        }
    }

    if let Some(stats_tuple) = vardata2.stats_tuple {
        nullfrac2 = stats_tuple_stanullfrac::call(stats_tuple) as f64;

        mcv2_slot = get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        )?;
        hist2_slot = get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_HISTOGRAM,
            InvalidOid,
            ATTSTATSSLOT_VALUES,
        )?;
        if let Some(ref s) = mcv2_slot {
            mcv2_length = min_i32(s.values.len() as i32, MAX_CONSIDERED_ELEMS);
            sumcommon2 = mcv_population(&s.numbers, mcv2_length);
        }
    }

    let mcv1_exists = mcv1_slot.is_some();
    let mcv2_exists = mcv2_slot.is_some();
    let hist1_exists = hist1_slot.is_some();
    let hist2_exists = hist2_slot.is_some();

    let opcode = get_opcode::call(operator)?;

    /* Estimate number of input rows represented by RHS histogram. */
    if hist2_exists {
        if let Some(rel_id) = vardata2.rel {
            hist2_weight = (1.0 - nullfrac2 - sumcommon2) * root.rel(rel_id).rows;
        }
    }

    /*
     * Consider each element of the LHS MCV list, matching it to whatever RHS
     * stats we have.  Scale according to the known frequency of the MCV.
     */
    if mcv1_exists && (mcv2_exists || hist2_exists) {
        let m1 = mcv1_slot
            .as_ref()
            .expect("mcv1_exists implies mcv1_slot present");
        let mcv2_values = mcv2_slot.as_ref().map(|s| &s.values[..]);
        let hist2_values = hist2_slot.as_ref().map(|s| &s.values[..]);
        let mut i: i32 = 0;
        while i < mcv1_length {
            selec += (m1.numbers[i as usize] as f64)
                * inet_semi_join_sel(
                    m1.values[i as usize],
                    mcv2_exists,
                    mcv2_values,
                    mcv2_length,
                    hist2_exists,
                    hist2_values,
                    hist2_weight,
                    opcode,
                    opr_codenum,
                )?;
            i += 1;
        }
    }

    /*
     * Consider each element of the LHS histogram, except for the first and last
     * elements, which we exclude on the grounds that they're outliers and thus
     * not very representative.  Scale on the assumption that each such histogram
     * element represents an equal share of the LHS histogram population.
     *
     * If there are too many histogram elements, decimate to limit runtime.
     */
    let hist1_nvalues = hist1_slot.as_ref().map_or(0, |s| s.values.len() as i32);
    if hist1_exists && hist1_nvalues > 2 && (mcv2_exists || hist2_exists) {
        let h1 = hist1_slot
            .as_ref()
            .expect("hist1_exists implies hist1_slot present");
        let mcv2_values = mcv2_slot.as_ref().map(|s| &s.values[..]);
        let hist2_values = hist2_slot.as_ref().map(|s| &s.values[..]);

        let mut hist_selec_sum: f64 = 0.0;

        let k = (hist1_nvalues - 3) / MAX_CONSIDERED_ELEMS + 1;

        let mut n: i32 = 0;
        let mut i: i32 = 1;
        while i < hist1_nvalues - 1 {
            hist_selec_sum += inet_semi_join_sel(
                h1.values[i as usize],
                mcv2_exists,
                mcv2_values,
                mcv2_length,
                hist2_exists,
                hist2_values,
                hist2_weight,
                opcode,
                opr_codenum,
            )?;
            n += 1;
            i += k;
        }

        selec += (1.0 - nullfrac1 - sumcommon1) * hist_selec_sum / n as f64;
    }

    /*
     * If useful statistics are not available then use the default estimate.  We
     * can apply null fractions if known, though.
     */
    if (!mcv1_exists && !hist1_exists) || (!mcv2_exists && !hist2_exists) {
        selec = (1.0 - nullfrac1) * (1.0 - nullfrac2) * default_sel(operator);
    }

    /* Release stats — the owned slots drop here (C: free_attstatsslot). */
    Ok(selec)
}

/* =========================================================================
 * inet_hist_value_sel. (network_selfuncs.c:618-679)
 * ====================================================================== */

/// `inet_hist_value_sel` — fraction of the histogram population that satisfies
/// "value OPR CONST".
fn inet_hist_value_sel(
    values: &[Datum],
    constvalue: Datum,
    opr_codenum: i32,
) -> PgResult<f64> {
    let nvalues = values.len() as i32;
    let mut match_: f64 = 0.0;

    /* guard against zero-divide below */
    if nvalues <= 1 {
        return Ok(0.0);
    }

    /* if there are too many histogram elements, decimate to limit runtime */
    let k = (nvalues - 2) / MAX_CONSIDERED_ELEMS + 1;

    let query = datum_get_inet_pp_word(constvalue)?;

    /* "left" is the left boundary value of the current bucket ... */
    let mut left = datum_get_inet_pp_word(values[0])?;
    let mut left_order = inet_inclusion_cmp(&left, &query, opr_codenum);

    let mut n: i32 = 0;
    let mut i: i32 = k;
    while i < nvalues {
        /* ... and "right" is the right boundary value */
        let right = datum_get_inet_pp_word(values[i as usize])?;
        let right_order = inet_inclusion_cmp(&right, &query, opr_codenum);

        if left_order == 0 && right_order == 0 {
            /* The whole bucket matches, since both endpoints do. */
            match_ += 1.0;
        } else if (left_order <= 0 && right_order >= 0) || (left_order >= 0 && right_order <= 0) {
            /* Partial bucket match. */
            let left_divider: i32 = inet_hist_match_divider(&left, &query, opr_codenum);
            let right_divider: i32 = inet_hist_match_divider(&right, &query, opr_codenum);

            if left_divider >= 0 || right_divider >= 0 {
                match_ += 1.0 / 2.0f64.powf(max_i32(left_divider, right_divider) as f64);
            }
        }

        /* Shift the variables. */
        left = right;
        left_order = right_order;

        /* Count the number of buckets considered. */
        n += 1;

        i += k;
    }

    Ok(match_ / n as f64)
}

/* =========================================================================
 * inet_mcv_join_sel. (network_selfuncs.c:687-708)
 * ====================================================================== */

/// `inet_mcv_join_sel` — MCV vs MCV join selectivity.
fn inet_mcv_join_sel(
    mcv1_values: &[Datum],
    mcv1_numbers: &[f32],
    mcv1_nvalues: i32,
    mcv2_values: &[Datum],
    mcv2_numbers: &[f32],
    mcv2_nvalues: i32,
    operator: Oid,
) -> PgResult<f64> {
    let mut selec: f64 = 0.0;

    let opcode = get_opcode::call(operator)?;

    let mut i: i32 = 0;
    while i < mcv1_nvalues {
        let mut j: i32 = 0;
        while j < mcv2_nvalues {
            if function_call2_coll::call(
                opcode,
                InvalidOid,
                mcv1_values[i as usize],
                mcv2_values[j as usize],
            )?
            .as_bool()
            {
                selec += (mcv1_numbers[i as usize] as f64) * (mcv2_numbers[j as usize] as f64);
            }
            j += 1;
        }
        i += 1;
    }
    Ok(selec)
}

/* =========================================================================
 * inet_mcv_hist_sel. (network_selfuncs.c:719-740)
 * ====================================================================== */

/// `inet_mcv_hist_sel` — MCV vs histogram join selectivity.
fn inet_mcv_hist_sel(
    mcv_values: &[Datum],
    mcv_numbers: &[f32],
    mcv_nvalues: i32,
    hist_values: &[Datum],
    mut opr_codenum: i32,
) -> PgResult<f64> {
    let mut selec: f64 = 0.0;

    /*
     * We'll call inet_hist_value_sel with the histogram on the left, so we must
     * commute the operator.
     */
    opr_codenum = -opr_codenum;

    let mut i: i32 = 0;
    while i < mcv_nvalues {
        selec += (mcv_numbers[i as usize] as f64)
            * inet_hist_value_sel(hist_values, mcv_values[i as usize], opr_codenum)?;
        i += 1;
    }
    Ok(selec)
}

/* =========================================================================
 * inet_hist_inclusion_join_sel. (network_selfuncs.c:756-781)
 * ====================================================================== */

/// `inet_hist_inclusion_join_sel` — histogram vs histogram join selectivity.
fn inet_hist_inclusion_join_sel(
    hist1_values: &[Datum],
    hist2_values: &[Datum],
    opr_codenum: i32,
) -> PgResult<f64> {
    let hist2_nvalues = hist2_values.len() as i32;
    let mut match_: f64 = 0.0;

    if hist2_nvalues <= 2 {
        return Ok(0.0); /* no interior histogram elements */
    }

    /* if there are too many histogram elements, decimate to limit runtime */
    let k = (hist2_nvalues - 3) / MAX_CONSIDERED_ELEMS + 1;

    let mut n: i32 = 0;
    let mut i: i32 = 1;
    while i < hist2_nvalues - 1 {
        match_ += inet_hist_value_sel(hist1_values, hist2_values[i as usize], opr_codenum)?;
        n += 1;
        i += k;
    }

    Ok(match_ / n as f64)
}

/* =========================================================================
 * inet_semi_join_sel. (network_selfuncs.c:807-840)
 * ====================================================================== */

/// `inet_semi_join_sel` — semi join selectivity for one LHS value.
///
/// The C `FmgrInfo *proc` is carried as the operator's underlying function OID
/// (`opcode`); the fmgr seam re-resolves it per call (behavior identical to the
/// cached `FmgrInfo`).
fn inet_semi_join_sel(
    lhs_value: Datum,
    mcv_exists: bool,
    mcv_values: Option<&[Datum]>,
    mcv_nvalues: i32,
    hist_exists: bool,
    hist_values: Option<&[Datum]>,
    hist_weight: f64,
    opcode: Oid,
    opr_codenum: i32,
) -> PgResult<f64> {
    if mcv_exists {
        let mcv_values = mcv_values.expect("mcv_exists implies mcv_values present");
        let mut i: i32 = 0;
        while i < mcv_nvalues {
            if function_call2_coll::call(opcode, InvalidOid, lhs_value, mcv_values[i as usize])?
                .as_bool()
            {
                return Ok(1.0);
            }
            i += 1;
        }
    }

    if hist_exists && hist_weight > 0.0 {
        let hist_values = hist_values.expect("hist_exists implies hist_values present");
        /* Commute operator, since we're passing lhs_value on the right */
        let hist_selec = inet_hist_value_sel(hist_values, lhs_value, -opr_codenum)?;

        if hist_selec > 0.0 {
            return Ok(min_f64(1.0, hist_weight * hist_selec));
        }
    }

    Ok(0.0)
}

/* =========================================================================
 * inet_opr_codenum. (network_selfuncs.c:853-873)
 * ====================================================================== */

/// `inet_opr_codenum` — assign code numbers for the inclusion/overlap
/// operators.  Errors if `operator` is unsupported.
fn inet_opr_codenum(operator: Oid) -> PgResult<i32> {
    match operator {
        OID_INET_SUP_OP => Ok(-2),
        OID_INET_SUPEQ_OP => Ok(-1),
        OID_INET_OVERLAP_OP => Ok(0),
        OID_INET_SUBEQ_OP => Ok(1),
        OID_INET_SUB_OP => Ok(2),
        _ => elog_error(alloc::format!(
            "unrecognized operator {operator} for inet selectivity"
        )),
    }
}

/* =========================================================================
 * inet_inclusion_cmp. (network_selfuncs.c:896-912)
 * ====================================================================== */

/// `inet_inclusion_cmp` — inclusion/overlap comparison compatible with the inet
/// btree comparison.
fn inet_inclusion_cmp(left: &inet_struct, right: &inet_struct, opr_codenum: i32) -> i32 {
    if ip_family(left) == ip_family(right) {
        let order = bitncmp(
            ip_addr(left),
            ip_addr(right),
            min_i32(ip_bits(left), ip_bits(right)),
        );
        if order != 0 {
            return order;
        }

        return inet_masklen_inclusion_cmp(left, right, opr_codenum);
    }

    ip_family(left) - ip_family(right)
}

/* =========================================================================
 * inet_masklen_inclusion_cmp. (network_selfuncs.c:922-944)
 * ====================================================================== */

/// `inet_masklen_inclusion_cmp` — masklen comparison for the inclusion/overlap
/// operators.
fn inet_masklen_inclusion_cmp(left: &inet_struct, right: &inet_struct, opr_codenum: i32) -> i32 {
    let order = ip_bits(left) - ip_bits(right);

    /*
     * Return 0 if the operator would accept this combination of masklens.  Note
     * that opr_codenum zero (overlaps) will accept all cases.
     */
    if (order > 0 && opr_codenum >= 0)
        || (order == 0 && opr_codenum >= -1 && opr_codenum <= 1)
        || (order < 0 && opr_codenum <= 0)
    {
        return 0;
    }

    /*
     * Otherwise, return a negative value for sup/supeq, or a positive value for
     * sub/subeq.
     */
    opr_codenum
}

/* =========================================================================
 * inet_hist_match_divider. (network_selfuncs.c:956-990)
 * ====================================================================== */

/// `inet_hist_match_divider` — partial-match divider for a histogram boundary.
/// Returns -1 if it cannot be calculated.
fn inet_hist_match_divider(boundary: &inet_struct, query: &inet_struct, opr_codenum: i32) -> i32 {
    if ip_family(boundary) == ip_family(query)
        && inet_masklen_inclusion_cmp(boundary, query, opr_codenum) == 0
    {
        let min_bits = min_i32(ip_bits(boundary), ip_bits(query));

        /*
         * Set decisive_bits to the masklen of the one that should contain the
         * other according to the operator.
         */
        let decisive_bits: i32;
        if opr_codenum < 0 {
            decisive_bits = ip_bits(boundary);
        } else if opr_codenum > 0 {
            decisive_bits = ip_bits(query);
        } else {
            decisive_bits = min_bits;
        }

        /*
         * Now return the number of non-common decisive bits.  (This will be zero
         * if the boundary and query in fact match, else positive.)
         */
        if min_bits > 0 {
            return decisive_bits - bitncommon(ip_addr(boundary), ip_addr(query), min_bits);
        }
        return decisive_bits;
    }

    -1
}

#[cfg(test)]
mod tests;
