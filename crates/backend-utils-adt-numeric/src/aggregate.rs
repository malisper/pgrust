//! Family: aggregates — the lazy [`NumericSumAccum`] sum accumulator
//! (`accum_sum_*`), the Youngs-Cramer [`NumericAggState`] for `sum`/`avg`/
//! `variance`/`stddev` over real `numeric[]` arrays (`do_numeric_accum`/
//! `discard` + the `numeric_*`/`numeric_poly_*` transition/inverse/combine/
//! serialize/deserialize/final functions), the 128-bit [`Int128AggState`] fast
//! path (`do_int128_accum`/`discard` + `int{2,4,8}_accum`), the int
//! sum/avg transitions, plus abbreviated-key sort-support (`numeric_sortsupport`/
//! `numeric_abbrev_*`) and the value hashers (`hash_numeric`/
//! `hash_numeric_extended`).
//!
//! Accumulators bear charged `PgVec`s (the `'mcx` lifetime); allocating
//! transitions take an explicit `Mcx<'mcx>` and return [`PgResult`] where the C
//! `ereport`s. The sort-support node registration and HyperLogLog cardinality
//! estimator are genuine externals reached via seams (NOT modeled here).

use mcx::{Mcx, PgVec};
use types_error::PgResult;
use types_numeric::var::{NumericAggState, NumericSumAccum, NumericVar};
use types_numeric::{Int128AggState, NumericSortSupport};

// ---------------------------------------------------------------------------
// NumericSumAccum (numeric.c accum_sum_*).
// ---------------------------------------------------------------------------

pub fn accum_sum_add(accum: &mut NumericSumAccum<'_>, val: &NumericVar<'_>) -> PgResult<()> {
    let _ = (accum, val);
    todo!("aggregate::accum_sum_add — numeric.c accum_sum_add")
}

pub fn accum_sum_rescale(accum: &mut NumericSumAccum<'_>, val: &NumericVar<'_>) -> PgResult<()> {
    let _ = (accum, val);
    todo!("aggregate::accum_sum_rescale — numeric.c accum_sum_rescale")
}

pub fn accum_sum_carry(accum: &mut NumericSumAccum<'_>) {
    let _ = accum;
    todo!("aggregate::accum_sum_carry — numeric.c accum_sum_carry")
}

pub fn accum_sum_reset(accum: &mut NumericSumAccum<'_>) {
    let _ = accum;
    todo!("aggregate::accum_sum_reset — numeric.c accum_sum_reset")
}

pub fn accum_sum_final<'mcx>(
    mcx: Mcx<'mcx>,
    accum: &NumericSumAccum<'_>,
) -> PgResult<NumericVar<'mcx>> {
    let _ = (mcx, accum);
    todo!("aggregate::accum_sum_final — numeric.c accum_sum_final")
}

pub fn accum_sum_copy<'mcx>(mcx: Mcx<'mcx>, src: &NumericSumAccum<'_>) -> NumericSumAccum<'mcx> {
    let _ = (mcx, src);
    todo!("aggregate::accum_sum_copy — numeric.c accum_sum_copy")
}

pub fn accum_sum_combine(accum: &mut NumericSumAccum<'_>, other: &NumericSumAccum<'_>) -> PgResult<()> {
    let _ = (accum, other);
    todo!("aggregate::accum_sum_combine — numeric.c accum_sum_combine")
}

// ---------------------------------------------------------------------------
// NumericAggState transitions (Youngs-Cramer) over on-disk numeric values.
// ---------------------------------------------------------------------------

pub fn do_numeric_accum(state: &mut NumericAggState<'_>, newval: &[u8]) -> PgResult<()> {
    let _ = (state, newval);
    todo!("aggregate::do_numeric_accum — numeric.c do_numeric_accum")
}

pub fn do_numeric_discard(state: &mut NumericAggState<'_>, newval: &[u8]) -> PgResult<bool> {
    let _ = (state, newval);
    todo!("aggregate::do_numeric_discard — numeric.c do_numeric_discard")
}

/// `numeric_avg(state)`: AVG(numeric) final.
pub fn numeric_avg<'mcx>(
    mcx: Mcx<'mcx>,
    state: &NumericAggState<'_>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let _ = (mcx, state);
    todo!("aggregate::numeric_avg — numeric.c numeric_avg")
}

/// `numeric_sum(state)`: SUM(numeric) final.
pub fn numeric_sum<'mcx>(
    mcx: Mcx<'mcx>,
    state: &NumericAggState<'_>,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let _ = (mcx, state);
    todo!("aggregate::numeric_sum — numeric.c numeric_sum")
}

/// `numeric_var_pop`/`var_samp`/`stddev_pop`/`stddev_samp` share the
/// `numeric_stddev_internal` core: variance/stddev final, sample vs population.
pub fn numeric_stddev_internal<'mcx>(
    mcx: Mcx<'mcx>,
    state: &NumericAggState<'_>,
    variance: bool,
    sample: bool,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let _ = (mcx, state, variance, sample);
    todo!("aggregate::numeric_stddev_internal — numeric.c numeric_stddev_internal")
}

/// `numeric_combine`: combine two `sumX`+`sumX2` transition states.
pub fn numeric_combine<'mcx>(
    mcx: Mcx<'mcx>,
    state1: Option<NumericAggState<'_>>,
    state2: &NumericAggState<'_>,
) -> PgResult<NumericAggState<'mcx>> {
    let _ = (mcx, state1, state2);
    todo!("aggregate::numeric_combine — numeric.c numeric_combine")
}

/// `numeric_serialize`: serialize a transition state for parallel transfer.
pub fn numeric_serialize<'mcx>(
    mcx: Mcx<'mcx>,
    state: &NumericAggState<'_>,
) -> PgResult<PgVec<'mcx, u8>> {
    let _ = (mcx, state);
    todo!("aggregate::numeric_serialize — numeric.c numeric_serialize")
}

/// `numeric_deserialize`: deserialize a transition state.
pub fn numeric_deserialize<'mcx>(mcx: Mcx<'mcx>, buf: &[u8]) -> PgResult<NumericAggState<'mcx>> {
    let _ = (mcx, buf);
    todo!("aggregate::numeric_deserialize — numeric.c numeric_deserialize")
}

// ---------------------------------------------------------------------------
// Int128AggState fast path (numeric.c do_int128_accum + int*_accum + the
// numeric_poly_* finals).
// ---------------------------------------------------------------------------

pub fn make_int128_agg_state(calc_sum_x2: bool) -> Int128AggState {
    let _ = calc_sum_x2;
    todo!("aggregate::make_int128_agg_state — numeric.c makeInt128AggState")
}

pub fn do_int128_accum(state: &mut Int128AggState, newval: i128) {
    let _ = (state, newval);
    todo!("aggregate::do_int128_accum — numeric.c do_int128_accum")
}

pub fn do_int128_discard(state: &mut Int128AggState, newval: i128) {
    let _ = (state, newval);
    todo!("aggregate::do_int128_discard — numeric.c do_int128_discard")
}

pub fn int2_accum(state: Option<Int128AggState>, newval: i16) -> PgResult<Int128AggState> {
    let _ = (state, newval);
    todo!("aggregate::int2_accum — numeric.c int2_accum")
}

pub fn int4_accum(state: Option<Int128AggState>, newval: i32) -> PgResult<Int128AggState> {
    let _ = (state, newval);
    todo!("aggregate::int4_accum — numeric.c int4_accum")
}

pub fn int8_accum<'mcx>(
    mcx: Mcx<'mcx>,
    state: Option<NumericAggState<'_>>,
    newval: i64,
) -> PgResult<NumericAggState<'mcx>> {
    let _ = (mcx, state, newval);
    todo!("aggregate::int8_accum — numeric.c int8_accum")
}

/// `numeric_poly_sum`/`avg`/`var_pop`/`var_samp`/`stddev_pop`/`stddev_samp`
/// share the `numeric_poly_stddev_internal`/`numeric_poly_sum` finals over the
/// 128-bit state.
pub fn numeric_poly_sum<'mcx>(
    mcx: Mcx<'mcx>,
    state: &Int128AggState,
) -> PgResult<Option<PgVec<'mcx, u8>>> {
    let _ = (mcx, state);
    todo!("aggregate::numeric_poly_sum — numeric.c numeric_poly_sum")
}

// ---------------------------------------------------------------------------
// Sort-support (numeric.c numeric_sortsupport/abbrev_*). Node registration +
// HyperLogLog estimator are genuine externals behind sort-support seams.
// ---------------------------------------------------------------------------

/// `numeric_abbrev_convert(original)`: produce the abbreviated key for an
/// on-disk numeric value, updating the [`NumericSortSupport`] estimator state.
pub fn numeric_abbrev_convert(original: &[u8], ssup: &mut NumericSortSupport) -> i64 {
    let _ = (original, ssup);
    todo!("aggregate::numeric_abbrev_convert — numeric.c numeric_abbrev_convert")
}

/// `numeric_fast_cmp`: the full comparator used by sort-support.
pub fn numeric_fast_cmp(a: &[u8], b: &[u8]) -> i32 {
    let _ = (a, b);
    todo!("aggregate::numeric_fast_cmp — numeric.c numeric_fast_cmp")
}

// ---------------------------------------------------------------------------
// Hashing (numeric.c hash_numeric/hash_numeric_extended).
// ---------------------------------------------------------------------------

pub fn hash_numeric(num: &[u8]) -> u32 {
    let _ = num;
    todo!("aggregate::hash_numeric — numeric.c hash_numeric")
}

pub fn hash_numeric_extended(num: &[u8], seed: u64) -> u64 {
    let _ = (num, seed);
    todo!("aggregate::hash_numeric_extended — numeric.c hash_numeric_extended")
}
