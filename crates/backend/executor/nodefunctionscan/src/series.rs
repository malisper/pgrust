//! `generate_series` recognized as a value GENERATOR rather than a
//! materialized tuplestore — the producer half of the lane's series fold
//! (`execmain::lanev2::series_fold`).
//!
//! WHY THIS EXISTS. `FunctionNext` is C-exact: the first pull runs
//! `ExecMakeTableFunctionResult`, which drains the whole ValuePerCall set
//! into a `Tuplestore` and every later pull reads one row back out. For a
//! bounded SRF that is the right shape (and it is what makes backward scans,
//! rescans and `WITH ORDINALITY` cheap), but for `generate_series` — a pure
//! arithmetic sequence whose whole state is `(current, finish, step)` — the
//! store is pure overhead proportional to the row count: on
//! `sum(i) FROM generate_series(1, 100000000)` it writes and reads back
//! 100M heap tuples and spills every byte past `work_mem` to disk
//! (issue #83).
//!
//! The generator below reproduces that row stream WITHOUT the store, one
//! staged batch at a time, for the ONE consumer that can prove it never
//! needs the store's other services (the plain-agg fold: forward-only,
//! single pass, no rescan-in-flight, no ordinality, no per-row slot). It is
//! deliberately a producer only — admission lives entirely at the consumer.
//!
//! SEMANTIC IDENTITY (why the fast path cannot change an answer):
//!   * the emitted VALUES are `GenerateSeriesInt4::next` /
//!     `GenerateSeriesInt8::next` — the very state machines
//!     `fc_generate_series_step_int4`/`_int8` step per SRF call, so the
//!     sequence (including the "next-value overflow makes this the last
//!     row" rule) is the same one the store would have been filled with;
//!   * ARGUMENTS evaluate exactly once, in the node's `argcontext`, through
//!     the same `ExecEvalFuncArgs` sequence `run_value_per_call` runs — so
//!     a volatile or erroring argument expression is evaluated the same
//!     number of times, in the same context, at the same point;
//!   * the strict-NULL contract (`no_function_result`: a NULL argument to a
//!     strict SRF is an EMPTY set, never a row) and the step-zero error
//!     (22023, raised from the same `new()` the SRF calls) are reproduced
//!     at the same point in the pull — the first pull of the scan;
//!   * `pgstat` function-usage accounting is NOT reproduced, so
//!     [`series_kind`] refuses whenever this call would be tracked.
//!
//! Everything else about the node is untouched: refusing here (or at the
//! consumer) leaves `FunctionScanState` exactly as it was, and the
//! unchanged `FunctionNext` store path runs.

use ::adt_int::series::GenerateSeriesInt4;
use ::adt_int8::GenerateSeriesInt8;
use ::datum::{Datum, NullableDatum};
use ::execexpr::{exec_eval_expr, EvalSlots};
use ::executils::EStateData;
use ::tuplestore::Tuplestore;
use ::types_error::PgResult;
use ::types_fmgr::TRACK_FUNC_ALL;
use ::types_slot::EXEC_FLAG_BACKWARD;
use ::types_tuple::TupleDescData;

use crate::FunctionScanState;

// pg_proc OIDs. 1066/1068 are the 3-argument (explicit step) forms; 1067/1069
// the 2-argument ones. Both members of each pair share one C body (PG_NARGS
// demuxes), exactly as `fc_generate_series_step_int4`/`_int8` do here.
const F_GENERATE_SERIES_STEP_INT4: u32 = 1066;
const F_GENERATE_SERIES_INT4: u32 = 1067;
const F_GENERATE_SERIES_STEP_INT8: u32 = 1068;
const F_GENERATE_SERIES_INT8: u32 = 1069;

/// Which `generate_series` overload a recognized scan runs — i.e. the datum
/// encoding of its one output column (`int4` / `int8`). The consumer maps
/// this to the lane width it must see on every folded transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SeriesKind {
    Int4,
    Int8,
}

/// A recognized scan's generator, opened by [`series_open`]. Copy: the node
/// retains the OPENED (rewound) state as its replay handle and hands out
/// copies to drive, so a rescan replays the same sequence — see
/// [`series_open`].
///
/// # Why a counted form and not the SRF's own state machine
///
/// `GenerateSeriesInt4::next` is a per-VALUE state machine: bound compare,
/// checked add, and a "next-value overflow ends the set" store into `step`.
/// That is the right shape for a per-call protocol and the wrong one for a
/// batch — the loop-carried branch and the conditional store defeat
/// vectorization, so it costs a branch per row where the fold that consumes
/// it costs a fraction of one. This form hoists the whole decision out of
/// the loop: the SRF's states are still what OPENS the feed (arguments,
/// step-zero error, initial value — see [`series_open`]), but the emission
/// count is derived once by [`series_len`], leaving the batch loop a bare
/// store + add over an induction variable. The equivalence to the per-value
/// walk is the whole correctness claim and is pinned exhaustively by
/// `matches_srf_state_machine`.
#[derive(Clone, Copy, Debug)]
pub struct SeriesFeed {
    kind: SeriesKind,
    /// Next value to emit, sign-extended (int4 series carry i32-range values).
    current: i64,
    step: i64,
    /// Values still to emit. `u128` because a full-range int8 series is
    /// 2^64 values — one more than `u64` can hold.
    remaining: u128,
}

// SAFETY (the `!needs_drop` const proof is the impl's own): a plain Copy
// value — no arena collection, no handle, nothing to reclaim.
::mcx::forget_safe_nodrop!(SeriesFeed);

/// How many values `GenerateSeries*::next` emits from `(current, finish,
/// step)` — the closed form of its emission count.
///
/// The two clauses of `next`'s guard become the direction test; its
/// overflow rule needs no clause of its own, and that is worth stating
/// because it looks like a gap: when `current + step` would overflow, the
/// state machine emits `current` and stops. But an overflowing next value
/// means `current` is within one step of the type bound, hence past
/// `finish` by less than a step, hence `(finish - current) / step == 0` and
/// the count is 1 — exactly the emission the machine makes. The arithmetic
/// runs in `i128` so `finish - current` cannot itself overflow.
fn series_len(current: i64, finish: i64, step: i64) -> u128 {
    debug_assert!(step != 0, "step zero is rejected by GenerateSeries*::new");
    let (c, f, s) = (current as i128, finish as i128, step as i128);
    if (s > 0 && c > f) || (s < 0 && c < f) {
        return 0;
    }
    ((f - c) / s + 1) as u128
}

impl SeriesFeed {
    /// A strict SRF skipped for a NULL argument (`no_function_result`): the
    /// empty set, never a row.
    fn empty(kind: SeriesKind) -> SeriesFeed {
        SeriesFeed { kind, current: 0, step: 1, remaining: 0 }
    }

    fn int4(s: GenerateSeriesInt4) -> SeriesFeed {
        SeriesFeed {
            kind: SeriesKind::Int4,
            current: s.current as i64,
            step: s.step as i64,
            remaining: series_len(s.current as i64, s.finish as i64, s.step as i64),
        }
    }

    fn int8(s: GenerateSeriesInt8) -> SeriesFeed {
        SeriesFeed {
            kind: SeriesKind::Int8,
            current: s.current,
            step: s.step,
            remaining: series_len(s.current, s.finish, s.step),
        }
    }

    /// Stage the next values into `out`, ascending in the SRF's own emission
    /// order; returns the count staged (`< out.len()` only on the final
    /// batch, `0` once the set is drained).
    ///
    /// The loop is a store plus an add over a register-resident induction
    /// variable — no bound test, no overflow check, no memory traffic but the
    /// stores. Both are `wrapping_add`: only the batch's LAST step can wrap
    /// (a wrapping step means the set has ended, so `remaining` is 0 and the
    /// wrapped value is never read), and using the wrapping form is what
    /// keeps the loop free of a per-value branch.
    pub fn next_batch(&mut self, out: &mut [Datum]) -> usize {
        let k = self.remaining.min(out.len() as u128) as usize;
        if k == 0 {
            return 0;
        }
        let (mut v, step) = (self.current, self.step);
        match self.kind {
            SeriesKind::Int4 => {
                for slot in &mut out[..k] {
                    *slot = Datum::from_i32(v as i32);
                    v = v.wrapping_add(step);
                }
            }
            SeriesKind::Int8 => {
                for slot in &mut out[..k] {
                    *slot = Datum::from_i64(v);
                    v = v.wrapping_add(step);
                }
            }
        }
        self.current = v;
        self.remaining -= k as u128;
        k
    }
}

/// STRUCTURAL recognition: does this scan generate a plain `generate_series`
/// sequence the store path exists only to buffer? `None` = no (the caller
/// keeps `FunctionNext` verbatim).
///
/// PURE — it evaluates nothing and mutates nothing, which is load-bearing:
/// the consumer must be able to test its own admission (the fold plan's lane
/// width) BEFORE anything is evaluated, because a refusal after argument
/// evaluation would evaluate a volatile argument expression twice.
///
/// The refuse-set is everything the generator does not reproduce:
///   * `ROWS FROM (...)` / `WITH ORDINALITY` — multi-leg column assembly and
///     the ordinal counter are `FunctionNext`'s, not the generator's;
///   * a qual or projection on the scan — the generator hands out values,
///     not slots, so nothing exists to run them over;
///   * an instrumented node — the fold reports no per-node row counts;
///   * `EXEC_FLAG_BACKWARD` — the store is the random-access buffer a
///     backward-capable scan was built with;
///   * an already-materialized scan — a rescan mid-flight keeps its store;
///   * a non-strict callee — the strict skip is the only NULL-argument
///     contract reproduced below;
///   * a tracked call (`pgstat_track_functions`) — the generator makes no
///     per-row `pgstat_init_function_usage` report, so an admitted scan
///     would silently drop `pg_stat_user_functions` rows.
pub fn series_kind(node: &FunctionScanState<'_>) -> Option<SeriesKind> {
    if !node.simple || node.ordinality {
        return None;
    }
    if node.eflags & EXEC_FLAG_BACKWARD != 0 {
        return None;
    }
    if node.ss.qual.is_some() || node.ss.ps_ProjInfo.is_some() || node.ss.instr_idx.is_some() {
        return None;
    }
    let fs = &node.funcstates[0];
    if fs.tstore.is_some() || fs.colcount != 1 || fs.tupdesc.natts != 1 {
        return None;
    }
    let setexpr = &fs.setexpr;
    if setexpr.returns_tuple || !setexpr.returns_set || setexpr.elided_func_state.is_some() {
        return None;
    }
    let flinfo = setexpr.flinfo.as_ref()?;
    if !flinfo.fn_strict {
        return None;
    }
    // The `pgstat_init_function_usage` gate `run_value_per_call` applies per
    // produced row: if it would fire, the generator's silence is observable.
    if flinfo.fn_stats < TRACK_FUNC_ALL
        && ::pgstat::function::pgstat_track_functions() > flinfo.fn_stats as i32
    {
        return None;
    }
    match (flinfo.fn_oid, setexpr.args.len()) {
        (F_GENERATE_SERIES_INT4, 2) | (F_GENERATE_SERIES_STEP_INT4, 3) => Some(SeriesKind::Int4),
        (F_GENERATE_SERIES_INT8, 2) | (F_GENERATE_SERIES_STEP_INT8, 3) => Some(SeriesKind::Int8),
        _ => None,
    }
}

/// Open the generator: `ExecEvalFuncArgs` in the node's argcontext, then the
/// SRF's own first-call setup. COMMITS the scan to the fast path — every
/// argument expression has been evaluated exactly once by the time this
/// returns, so the caller may not refuse afterwards.
///
/// # Rescan identity
///
/// `ExecReScanFunctionScan`'s chgParam-NULL arm REWINDS the tuplestore; it
/// does not re-run the SRF, so a second drive of the same node must replay
/// the SAME rows even when an argument expression is volatile. The opened
/// state is therefore retained on the funcstate and handed back rewound on
/// every later open — it is the generator's tuplestore. The chgParam arm
/// (`exec_rescan_function_scan_chg`) drops it in exactly the place it drops
/// the store, so a changed parameter re-evaluates exactly where C does.
///
/// `kind` must come from a [`series_kind`] call on this same node with no
/// intervening execution (it pins the argument count and datum encoding).
pub fn series_open<'mcx>(
    node: &mut FunctionScanState<'mcx>,
    estate: &mut EStateData<'mcx>,
    kind: SeriesKind,
) -> PgResult<SeriesFeed> {
    let FunctionScanState { funcstates, arg_mcx, .. } = node;
    let fs = &mut funcstates[0];
    if let Some(feed) = fs.series {
        return Ok(feed);
    }
    let setexpr = &mut fs.setexpr;
    debug_assert!(matches!(setexpr.args.len(), 2 | 3));

    // ExecEvalParamExec's pending-initplan arm, hoisted exactly as
    // `exec_make_table_function_result` hoists it.
    for st in setexpr.args.iter() {
        let deps = st.param_exec_deps();
        if !deps.is_empty() {
            ::executils::exec_eval_param_exec_params(estate, deps)?;
        }
    }

    // C evaluates SRF arguments in argContext (execSRF.c:119) so by-ref arg
    // datums survive the ValuePerCall loop's per-tuple resets. The generator
    // has no such loop, but the context discipline is kept verbatim: the
    // reset point and the result-mcx arming are what an argument expression
    // observes.
    //
    // SAFETY: the arena slot is armed by `make_arg_ctx` and dropped only by
    // the estate reset callback; the borrow is exclusive for this call (the
    // `funcstates` borrow above is a disjoint field).
    let arg_mcx = unsafe { arg_mcx.as_mut() };
    arg_mcx.reset();
    let nargs = setexpr.args.len();
    // Defaulted step (the 2-argument overload's implicit 1), matching
    // `fc_generate_series_step_int4`'s `nargs() == 3` demux.
    let mut vals = [0i64, 0, 1];
    let mut any_null = false;
    for i in 0..nargs {
        // SAFETY: `arg_mcx` outlives this scan (owned by the node state) and
        // is reset only at the next scan start.
        unsafe { setexpr.args[i].arm_result_mcx_raw(arg_mcx.mcx()) };
        let mut slots = EvalSlots { scan: None, inner: None, outer: None };
        let NullableDatum { value, isnull } = exec_eval_expr(&mut setexpr.args[i], &mut slots)?;
        if isnull {
            any_null = true;
        } else {
            vals[i] = match kind {
                SeriesKind::Int4 => value.as_i32() as i64,
                SeriesKind::Int8 => value.as_i64(),
            };
        }
    }

    // execSRF.c `no_function_result`: a strict function skipped for a NULL
    // argument acts like it returned NULL — for a set-returning function
    // that is the empty result. Checked AFTER every argument is evaluated
    // (C evaluates them all) and BEFORE the first call, so a NULL argument
    // beats a zero step exactly as it does through the SRF.
    let feed = if any_null {
        SeriesFeed::empty(kind)
    } else {
        // The SRF's OWN constructors: the initial state, and the step-zero
        // 22023, are theirs — only the emission loop is ours.
        match kind {
            SeriesKind::Int4 => SeriesFeed::int4(GenerateSeriesInt4::new(
                vals[0] as i32,
                vals[1] as i32,
                vals[2] as i32,
            )?),
            SeriesKind::Int8 => {
                SeriesFeed::int8(GenerateSeriesInt8::new(vals[0], vals[1], vals[2])?)
            }
        }
    };
    // The replay handle (rescan identity, above). A step-zero error escapes
    // before this line, exactly as it escapes the SRF's first call — so a
    // failed open leaves the node virgin and the store path would raise the
    // identical error at the identical point.
    fs.series = Some(feed);
    Ok(feed)
}

/// Materialize a retained generator into the tuplestore `FunctionNext`
/// expects, for the one path that can reach a fast-path node through the
/// store: a later pull the lane declines (its admission is per call). Replays
/// the RETAINED sequence rather than re-running `ExecEvalFuncArgs`, which is
/// what makes the decline invisible — see [`series_open`]'s rescan identity.
pub(crate) fn store_from_feed(
    feed: SeriesFeed,
    expected_desc: &TupleDescData<'_>,
    random_access: bool,
) -> PgResult<Tuplestore> {
    let mut feed = feed;
    let mut store =
        Tuplestore::begin_heap(random_access, false, ::init_small::globals::work_mem());
    let mut buf = [Datum::null(); 64];
    loop {
        let n = feed.next_batch(&mut buf);
        if n == 0 {
            break;
        }
        // The ValuePerCall fill's cancel/die cadence (execSRF.c parity): one
        // TLS read per group, the seam dispatch only on a pending interrupt.
        if ::init_small::globals::InterruptPending() {
            ::postgres_seams::check_for_interrupts::call()?;
        }
        for d in &buf[..n] {
            store.putvalues(expected_desc, &[*d], &[false])?;
        }
    }
    Ok(store)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Walk the SRF's per-value state machine — the sequence the tuplestore
    /// would have been filled with.
    fn srf_walk_i4(start: i32, finish: i32, step: i32) -> Vec<i32> {
        let mut s = GenerateSeriesInt4::new(start, finish, step).unwrap();
        let mut out = Vec::new();
        while let Some(v) = s.next() {
            out.push(v);
        }
        out
    }

    fn srf_walk_i8(start: i64, finish: i64, step: i64) -> Vec<i64> {
        let mut s = GenerateSeriesInt8::new(start, finish, step).unwrap();
        let mut out = Vec::new();
        while let Some(v) = s.next() {
            out.push(v);
        }
        out
    }

    /// Drain a feed through `next_batch` at the given grain.
    fn feed_walk(mut feed: SeriesFeed, grain: usize) -> Vec<Datum> {
        let mut buf = vec![Datum::null(); grain];
        let mut out = Vec::new();
        loop {
            let n = feed.next_batch(&mut buf);
            if n == 0 {
                break;
            }
            assert!(n <= grain);
            out.extend_from_slice(&buf[..n]);
        }
        out
    }

    /// THE correctness claim: the counted batch form emits exactly what the
    /// SRF's per-value state machine emits — same values, same order, same
    /// count — including the corners the closed form has to get right on its
    /// own (empty ranges, negative steps, inexact spans, and the
    /// next-value-overflow rule that ends a set early at both type bounds).
    /// Batch grains are varied so a set can end mid-batch, exactly on a
    /// boundary, or span many batches.
    #[test]
    fn matches_srf_state_machine() {
        const I4: [(i32, i32, i32); 22] = [
            (1, 10, 1),
            (1, 1, 1),
            (1, 0, 1),          // empty: start already past finish
            (10, 1, -1),
            (1, 10, -1),        // empty: wrong direction
            (1, 10, 3),         // inexact span (stops at 10, not past it)
            (1, 10, 100),       // one value, step past finish
            (10, 1, -3),
            (-5, 5, 2),
            (5, -5, -2),
            (0, 0, -1),
            (i32::MAX - 3, i32::MAX, 1),
            (i32::MAX - 3, i32::MAX, 2),
            (i32::MAX, i32::MAX, 1),
            (i32::MAX, i32::MAX, i32::MAX),      // next value overflows
            (i32::MAX - 1, i32::MAX, i32::MAX),  // ditto, span < step
            (i32::MIN, i32::MIN + 3, 1),
            (i32::MIN + 3, i32::MIN, -1),
            (i32::MIN, i32::MIN, -1),
            (i32::MIN, i32::MIN, i32::MIN),      // next value underflows
            (i32::MIN + 1, i32::MIN, i32::MIN),  // ditto, span < |step|
            (-3, 3, 1),
        ];
        for (start, finish, step) in I4 {
            let want = srf_walk_i4(start, finish, step);
            let feed = SeriesFeed::int4(GenerateSeriesInt4::new(start, finish, step).unwrap());
            for grain in [1usize, 2, 3, 7, 64, 1024] {
                let got: Vec<i32> =
                    feed_walk(feed, grain).iter().map(|d| d.as_i32()).collect();
                assert_eq!(got, want, "int4 ({start}, {finish}, {step}) grain {grain}");
            }
        }

        const I8: [(i64, i64, i64); 16] = [
            (1, 10, 1),
            (1, 0, 1),
            (10, 1, -1),
            (1, 10, 3),
            (-5, 5, 2),
            (5, -5, -2),
            (i64::MAX - 3, i64::MAX, 1),
            (i64::MAX - 3, i64::MAX, 2),
            (i64::MAX, i64::MAX, 1),
            (i64::MAX, i64::MAX, i64::MAX),
            (i64::MAX - 1, i64::MAX, i64::MAX),
            (i64::MIN, i64::MIN + 3, 1),
            (i64::MIN + 3, i64::MIN, -1),
            (i64::MIN, i64::MIN, i64::MIN),
            (i64::MIN + 1, i64::MIN, i64::MIN),
            (i64::MIN, i64::MIN + 5, 2),
        ];
        for (start, finish, step) in I8 {
            let want = srf_walk_i8(start, finish, step);
            let feed = SeriesFeed::int8(GenerateSeriesInt8::new(start, finish, step).unwrap());
            for grain in [1usize, 2, 3, 7, 64, 1024] {
                let got: Vec<i64> =
                    feed_walk(feed, grain).iter().map(|d| d.as_i64()).collect();
                assert_eq!(got, want, "int8 ({start}, {finish}, {step}) grain {grain}");
            }
        }
    }

    /// A full-range int8 series is 2^64 values — one more than `u64` holds,
    /// which is why `remaining` is `u128`. (Only the count is checked; the
    /// walk is not runnable.)
    #[test]
    fn full_int8_range_counts_past_u64() {
        assert_eq!(series_len(i64::MIN, i64::MAX, 1), 1u128 << 64);
        assert_eq!(series_len(i64::MAX, i64::MIN, -1), 1u128 << 64);
    }

    /// A NULL argument to a strict SRF is the empty set, never a row.
    #[test]
    fn empty_feed_stages_nothing() {
        for kind in [SeriesKind::Int4, SeriesKind::Int8] {
            assert!(feed_walk(SeriesFeed::empty(kind), 8).is_empty());
        }
    }
}
