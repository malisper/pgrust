//! The join-selectivity math kernels of selfuncs.c — `eqjoinsel_inner` and
//! `eqjoinsel_semi`.
//!
//! These are the pure-arithmetic cores the `eqjoinsel` driver calls after it
//! has examined both join variables, looked up each side's MCV slot, and read
//! each `Form_pg_statistic`'s `stanullfrac`. The driver itself
//! (`eqjoinsel` / `neqjoinsel`) is built on [`crate::examine::get_join_variables`]
//! and the `GETSTRUCT` reads, so it is keystone-blocked; the kernels below are
//! ported 1:1 and take the already-extracted statistics as parameters, matching
//! the C signatures (the MCV value arrays, the `nd`/`isdefault` counts, the
//! `stanullfrac`s, and the `have_mcvs` flags).

use mcx::Mcx;
use types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_datum::datum::Datum;
use types_datum::NullableDatum;
use types_error::PgResult;
use types_pathnodes::planner_run::PlannerRun;
use types_pathnodes::{
    NodeId, PlannerInfo, RelId, Relids, SpecialJoinInfo, JOIN_ANTI, JOIN_FULL, JOIN_INNER,
    JOIN_LEFT, JOIN_SEMI,
};
use types_selfuncs::{AttStatsSlot, VariableStatData, ATTSTATSSLOT_NUMBERS, ATTSTATSSLOT_VALUES};

use crate::STATISTIC_KIND_MCV;

use backend_utils_cache_lsyscache_seams as lsc;
use backend_utils_fmgr_fmgr_seams as fmgr;

use crate::clamp_probability;
use crate::examine::{get_join_variables, release_variable_stats};
use crate::scalar::{get_variable_numdistinct, statistic_proc_security_check, stats_tuple_stanullfrac};

/// The MCV statistics one side of a join contributes to [`eqjoinsel_inner`] /
/// [`eqjoinsel_semi`]: the `(values, numbers)` of the side's `AttStatsSlot`
/// (empty when `have_mcvs` is false) and the side's `stanullfrac` (`None` when
/// there is no `Form_pg_statistic`, matching C's `stats ? stats->stanullfrac : 0`).
pub struct JoinSide<'a> {
    /// `sslot->values` — the side's MCV values (empty when no MCV slot).
    pub values: &'a [Datum],
    /// `sslot->numbers` — the side's MCV frequencies (empty when no MCV slot).
    pub numbers: &'a [f32],
    /// `stats->stanullfrac`, or `None` (`stats == NULL`).
    pub stanullfrac: Option<f32>,
    /// `have_mcvs` — whether the MCV slot was present.
    pub have_mcvs: bool,
}

impl JoinSide<'_> {
    fn nullfrac(&self) -> f64 {
        self.stanullfrac.map(|f| f as f64).unwrap_or(0.0)
    }
}

/// `eqjoinsel_inner(opfuncoid, collation, ..., nd1, nd2, ..., sslot1, sslot2,
/// stats1, stats2, have_mcvs1, have_mcvs2)` (selfuncs.c) — inner-join equality
/// selectivity. 1:1 with the C body.
pub fn eqjoinsel_inner(
    opfuncoid: Oid,
    collation: Oid,
    side1: &JoinSide<'_>,
    side2: &JoinSide<'_>,
    nd1: f64,
    nd2: f64,
) -> PgResult<f64> {
    let selec;

    if side1.have_mcvs && side2.have_mcvs {
        // We have MCV lists for both relations. Run through them to see which
        // MCVs actually join with the given operator.
        let nullfrac1 = side1.nullfrac();
        let nullfrac2 = side2.nullfrac();
        let nvalues1 = side1.values.len();
        let nvalues2 = side2.values.len();

        let mut hasmatch1 = alloc::vec![false; nvalues1];
        let mut hasmatch2 = alloc::vec![false; nvalues2];

        // Each MCV matches at most one member of the other MCV list.
        let mut matchprodfreq = 0.0f64;
        let mut nmatches = 0i32;
        for i in 0..nvalues1 {
            for j in 0..nvalues2 {
                if hasmatch2[j] {
                    continue;
                }
                let (fresult, isnull) = fmgr::function_call_invoke::call(
                    opfuncoid,
                    collation,
                    &[
                        NullableDatum::value(side1.values[i]),
                        NullableDatum::value(side2.values[j]),
                    ],
                )?;
                if !isnull && fresult.as_bool() {
                    hasmatch1[i] = true;
                    hasmatch2[j] = true;
                    matchprodfreq += side1.numbers[i] as f64 * side2.numbers[j] as f64;
                    nmatches += 1;
                    break;
                }
            }
        }
        matchprodfreq = clamp_probability(matchprodfreq);

        // Sum up frequencies of matched and unmatched MCVs.
        let mut matchfreq1 = 0.0f64;
        let mut unmatchfreq1 = 0.0f64;
        for i in 0..nvalues1 {
            if hasmatch1[i] {
                matchfreq1 += side1.numbers[i] as f64;
            } else {
                unmatchfreq1 += side1.numbers[i] as f64;
            }
        }
        matchfreq1 = clamp_probability(matchfreq1);
        unmatchfreq1 = clamp_probability(unmatchfreq1);
        let mut matchfreq2 = 0.0f64;
        let mut unmatchfreq2 = 0.0f64;
        for i in 0..nvalues2 {
            if hasmatch2[i] {
                matchfreq2 += side2.numbers[i] as f64;
            } else {
                unmatchfreq2 += side2.numbers[i] as f64;
            }
        }
        matchfreq2 = clamp_probability(matchfreq2);
        unmatchfreq2 = clamp_probability(unmatchfreq2);

        // Total frequency of non-null values not in the MCV lists.
        let mut otherfreq1 = 1.0 - nullfrac1 - matchfreq1 - unmatchfreq1;
        let mut otherfreq2 = 1.0 - nullfrac2 - matchfreq2 - unmatchfreq2;
        otherfreq1 = clamp_probability(otherfreq1);
        otherfreq2 = clamp_probability(otherfreq2);

        // Estimate total selectivity from the point of view of relation 1.
        let mut totalsel1 = matchprodfreq;
        if nd2 > nvalues2 as f64 {
            totalsel1 += unmatchfreq1 * otherfreq2 / (nd2 - nvalues2 as f64);
        }
        if nd2 > nmatches as f64 {
            totalsel1 += otherfreq1 * (otherfreq2 + unmatchfreq2) / (nd2 - nmatches as f64);
        }
        // Same estimate from the point of view of relation 2.
        let mut totalsel2 = matchprodfreq;
        if nd1 > nvalues1 as f64 {
            totalsel2 += unmatchfreq2 * otherfreq1 / (nd1 - nvalues1 as f64);
        }
        if nd1 > nmatches as f64 {
            totalsel2 += otherfreq2 * (otherfreq1 + unmatchfreq1) / (nd1 - nmatches as f64);
        }

        // Use the smaller of the two estimates.
        selec = if totalsel1 < totalsel2 {
            totalsel1
        } else {
            totalsel2
        };
    } else {
        // No MCV lists for both sides: MIN(1/nd1,1/nd2)*(1-nullfrac1)*(1-nullfrac2).
        let nullfrac1 = side1.nullfrac();
        let nullfrac2 = side2.nullfrac();

        let mut s = (1.0 - nullfrac1) * (1.0 - nullfrac2);
        if nd1 > nd2 {
            s /= nd1;
        } else {
            s /= nd2;
        }
        selec = s;
    }

    Ok(selec)
}

/// `eqjoinsel_semi(opfuncoid, collation, ..., nd1, nd2, isdefault1, isdefault2,
/// sslot1, sslot2, stats1, stats2, have_mcvs1, have_mcvs2, inner_rel)`
/// (selfuncs.c) — semi/anti-join equality selectivity. 1:1 with the C body.
/// `vardata2_rel_rows` is `vardata2->rel->rows` (`None` when `vardata2->rel`
/// is NULL); `inner_rel_rows` is `inner_rel->rows`. The clamping of `nd2` and
/// `isdefault2` is reproduced exactly.
pub fn eqjoinsel_semi(
    opfuncoid: Oid,
    collation: Oid,
    side1: &JoinSide<'_>,
    side2: &JoinSide<'_>,
    mut nd1: f64,
    mut nd2: f64,
    isdefault1: bool,
    mut isdefault2: bool,
    vardata2_rel_rows: Option<f64>,
    inner_rel_rows: f64,
) -> PgResult<f64> {
    let selec;

    // Clamp nd2 to the inner relation's estimated size.
    if let Some(rows) = vardata2_rel_rows {
        if nd2 >= rows {
            nd2 = rows;
            isdefault2 = false;
        }
    }
    if nd2 >= inner_rel_rows {
        nd2 = inner_rel_rows;
        isdefault2 = false;
    }

    if side1.have_mcvs && side2.have_mcvs && OidIsValid(opfuncoid) {
        let nullfrac1 = side1.nullfrac();
        let nvalues1 = side1.values.len();

        // nd2 could be < sslot2->nvalues after clamping; compare to only the
        // first nd2 members of the MCV list.
        let clamped_nvalues2 = core::cmp::min(side2.values.len(), nd2 as usize);

        let mut hasmatch1 = alloc::vec![false; nvalues1];
        let mut hasmatch2 = alloc::vec![false; clamped_nvalues2];

        let mut nmatches = 0i32;
        for i in 0..nvalues1 {
            for j in 0..clamped_nvalues2 {
                if hasmatch2[j] {
                    continue;
                }
                let (fresult, isnull) = fmgr::function_call_invoke::call(
                    opfuncoid,
                    collation,
                    &[
                        NullableDatum::value(side1.values[i]),
                        NullableDatum::value(side2.values[j]),
                    ],
                )?;
                if !isnull && fresult.as_bool() {
                    hasmatch1[i] = true;
                    hasmatch2[j] = true;
                    nmatches += 1;
                    break;
                }
            }
        }
        // Sum up frequencies of matched MCVs.
        let mut matchfreq1 = 0.0f64;
        for i in 0..nvalues1 {
            if hasmatch1[i] {
                matchfreq1 += side1.numbers[i] as f64;
            }
        }
        matchfreq1 = clamp_probability(matchfreq1);

        // Estimate the fraction of relation 1 with at least one join partner.
        let uncertainfrac;
        if !isdefault1 && !isdefault2 {
            nd1 -= nmatches as f64;
            nd2 -= nmatches as f64;
            if nd1 <= nd2 || nd2 < 0.0 {
                uncertainfrac = 1.0;
            } else {
                uncertainfrac = nd2 / nd1;
            }
        } else {
            uncertainfrac = 0.5;
        }
        let mut uncertain = 1.0 - matchfreq1 - nullfrac1;
        uncertain = clamp_probability(uncertain);
        selec = matchfreq1 + uncertainfrac * uncertain;
    } else {
        // Without MCV lists for both sides, use the heuristic about nd1 vs nd2.
        let nullfrac1 = side1.nullfrac();

        if !isdefault1 && !isdefault2 {
            if nd1 <= nd2 || nd2 < 0.0 {
                selec = 1.0 - nullfrac1;
            } else {
                selec = (nd2 / nd1) * (1.0 - nullfrac1);
            }
        } else {
            selec = 0.5 * (1.0 - nullfrac1);
        }
    }

    Ok(selec)
}

/* ---------------------------------------------------------------------------
 * eqjoinsel / neqjoinsel drivers (selfuncs.c:2279 / 2829)
 * ------------------------------------------------------------------------- */

/// Build the `(AttStatsSlot, JoinSide)` for one join side, fetching its MCV
/// slot when `get_mcv_stats` is set and the support function passes the security
/// check. Mirrors the per-side block in C `eqjoinsel`: the slot is fetched only
/// for the `ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS` request after the
/// both-sides existence probe; `stanullfrac` is read regardless of the security
/// check. The MCV slot is owned by the returned `AttStatsSlot` so the borrowed
/// `JoinSide` slices stay valid for the kernel call.
fn build_join_side<'mcx>(
    mcx: Mcx<'mcx>,
    vardata: &VariableStatData,
    opfuncoid: Oid,
    get_mcv_stats: bool,
) -> PgResult<(Option<AttStatsSlot<'mcx>>, Option<f32>, bool)> {
    let stats_tuple = match vardata.stats_tuple {
        None => return Ok((None, None, false)),
        Some(t) => t,
    };
    // note we allow use of nullfrac regardless of security check
    let stanullfrac = Some(stats_tuple_stanullfrac(stats_tuple));
    let mut have_mcvs = false;
    let mut slot = None;
    if get_mcv_stats && statistic_proc_security_check(vardata, opfuncoid)? {
        slot = lsc::get_attstatsslot::call(
            mcx,
            stats_tuple,
            STATISTIC_KIND_MCV,
            InvalidOid,
            ATTSTATSSLOT_VALUES | ATTSTATSSLOT_NUMBERS,
        )?;
        have_mcvs = slot.is_some();
    }
    Ok((slot, stanullfrac, have_mcvs))
}

/// `find_join_input_rel(root, relids)` (selfuncs.c, static) — the RelOptInfo for
/// `relids`: the base rel for a singleton, else the join rel.
fn find_join_input_rel(root: &PlannerInfo, relids: &Relids) -> RelId {
    use backend_optimizer_util_relnode_seams as rel_seams;
    if rel_seams::relids_is_empty::call(relids) {
        panic!("could not find RelOptInfo for given relids");
    }
    if let Some(relid) = rel_seams::relids_get_singleton_member::call(relids) {
        rel_seams::find_base_rel::call(root, relid)
    } else {
        rel_seams::find_join_rel::call(root, relids)
            .expect("could not find RelOptInfo for given relids")
    }
}

/// `eqjoinsel(PG_FUNCTION_ARGS)` (selfuncs.c) — join selectivity of `=`.
/// Orchestrates [`eqjoinsel_inner`] / [`eqjoinsel_semi`] over the two examined
/// join variables. 1:1 with the C body.
pub fn eqjoinsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operator: Oid,
    args: &[NodeId],
    collation: Oid,
    sjinfo: &SpecialJoinInfo,
) -> PgResult<f64> {
    let (vardata1, vardata2, join_is_reversed) = get_join_variables(mcx, run, root, args, sjinfo)?;

    let (nd1, isdefault1) = get_variable_numdistinct(root, &vardata1);
    let (nd2, isdefault2) = get_variable_numdistinct(root, &vardata2);

    let opfuncoid = lsc::get_opcode::call(operator)?;

    // There is no use fetching one side's MCVs if we lack MCVs for the other,
    // so verify both stats exist (the C "get_mcv_stats" both-sides probe).
    let get_mcv_stats = vardata1.stats_tuple.is_some()
        && vardata2.stats_tuple.is_some()
        && lsc::get_attstatsslot::call(
            mcx,
            vardata1.stats_tuple.unwrap(),
            STATISTIC_KIND_MCV,
            InvalidOid,
            0,
        )?
        .is_some()
        && lsc::get_attstatsslot::call(
            mcx,
            vardata2.stats_tuple.unwrap(),
            STATISTIC_KIND_MCV,
            InvalidOid,
            0,
        )?
        .is_some();

    let (slot1, stanullfrac1, have_mcvs1) =
        build_join_side(mcx, &vardata1, opfuncoid, get_mcv_stats)?;
    let (slot2, stanullfrac2, have_mcvs2) =
        build_join_side(mcx, &vardata2, opfuncoid, get_mcv_stats)?;

    let empty_v: &[Datum] = &[];
    let empty_n: &[f32] = &[];
    let side1 = JoinSide {
        values: slot1.as_ref().map(|s| s.values.as_slice()).unwrap_or(empty_v),
        numbers: slot1.as_ref().map(|s| s.numbers.as_slice()).unwrap_or(empty_n),
        stanullfrac: stanullfrac1,
        have_mcvs: have_mcvs1,
    };
    let side2 = JoinSide {
        values: slot2.as_ref().map(|s| s.values.as_slice()).unwrap_or(empty_v),
        numbers: slot2.as_ref().map(|s| s.numbers.as_slice()).unwrap_or(empty_n),
        stanullfrac: stanullfrac2,
        have_mcvs: have_mcvs2,
    };

    // We need to compute the inner-join selectivity in all cases.
    let selec_inner =
        eqjoinsel_inner(opfuncoid, collation, &side1, &side2, nd1, nd2)?;

    let mut selec = match sjinfo.jointype {
        JOIN_INNER | JOIN_LEFT | JOIN_FULL => selec_inner,
        JOIN_SEMI | JOIN_ANTI => {
            // Look up the join's inner relation (min_righthand is sufficient).
            let inner_rel = find_join_input_rel(root, &sjinfo.min_righthand);
            let inner_rel_rows = root.rel(inner_rel).rows;
            let vardata2_rel_rows = vardata2.rel.map(|r| root.rel(r).rows);
            let vardata1_rel_rows = vardata1.rel.map(|r| root.rel(r).rows);

            let s = if !join_is_reversed {
                eqjoinsel_semi(
                    opfuncoid, collation, &side1, &side2, nd1, nd2, isdefault1, isdefault2,
                    vardata2_rel_rows, inner_rel_rows,
                )?
            } else {
                let commop = lsc::get_commutator::call(operator)?;
                let commopfuncoid = if OidIsValid(commop) {
                    lsc::get_opcode::call(commop)?
                } else {
                    InvalidOid
                };
                eqjoinsel_semi(
                    commopfuncoid, collation, &side2, &side1, nd2, nd1, isdefault2, isdefault1,
                    vardata1_rel_rows, inner_rel_rows,
                )?
            };
            // Clamp Ssemi <= N2 * Sinner (a semijoin can't exceed the inner join).
            s.min(inner_rel_rows * selec_inner)
        }
        other => {
            return Err(types_error::PgError::error(alloc::format!(
                "unrecognized join type: {}",
                other as i32
            )))
        }
    };

    release_variable_stats(vardata1);
    release_variable_stats(vardata2);

    selec = clamp_probability(selec);
    Ok(selec)
}

/// `neqjoinsel(PG_FUNCTION_ARGS)` (selfuncs.c) — join selectivity of `<>`.
/// For SEMI/ANTI joins the estimate is `1 - nullfrac`; otherwise it is
/// `1 - eqjoinsel(negator)`. 1:1 with the C body.
pub fn neqjoinsel<'mcx>(
    mcx: Mcx<'mcx>,
    run: &PlannerRun<'mcx>,
    root: &mut PlannerInfo,
    operator: Oid,
    args: &[NodeId],
    jointype: types_pathnodes::JoinType,
    collation: Oid,
    sjinfo: &SpecialJoinInfo,
) -> PgResult<f64> {
    if jointype == JOIN_SEMI || jointype == JOIN_ANTI {
        // Either way, the selectivity estimate is 1 - nullfrac.
        let (leftvar, rightvar, reversed) = get_join_variables(mcx, run, root, args, sjinfo)?;
        let stats_tuple = if reversed {
            rightvar.stats_tuple
        } else {
            leftvar.stats_tuple
        };
        let nullfrac = match stats_tuple {
            Some(t) => stats_tuple_stanullfrac(t) as f64,
            None => 0.0,
        };
        release_variable_stats(leftvar);
        release_variable_stats(rightvar);
        Ok(1.0 - nullfrac)
    } else {
        // We want 1 - eqjoinsel() where the operator is this !='s negator.
        let eqop = lsc::get_negator::call(operator)?;
        let result = if OidIsValid(eqop) {
            eqjoinsel(mcx, run, root, eqop, args, collation, sjinfo)?
        } else {
            // Use default selectivity (should we raise an error instead?).
            crate::DEFAULT_EQ_SEL
        };
        Ok(1.0 - result)
    }
}
