//! M2 aggregation-sink core kernels (donor A re-homed onto the runtime's
//! ParallelSink contract — docs/design/m2-sinks.md §2, notes/m2-agg-sink.md).
//!
//! What lives here (pure computation, no executor, no morsel plumbing):
//!  * [`SinkRun`] — a self-contained, radix-partitioned flush of a bounded
//!    worker compact table (the Stage-4 exchange's wire shape rebuilt
//!    without the handoff registry: plain `Vec<u64>` buffers, byval-POD
//!    state blocks, `Send` by construction);
//!  * [`sink_partition_remainder`] — SEAL-time bucket index over a worker's
//!    remainder table (counting sort by the sink hash's top byte);
//!  * [`sink_combine_bucket`] — one combine partition: stream every Local's
//!    bucket slice (runs in flush order, then the remainder) into a fresh
//!    per-bucket [`LaneAggTable`], insert-or-combine with the resolved
//!    byval combine functions (single writer per bucket — the claimed
//!    partition IS the exclusivity domain);
//!  * [`sink_emit_bucket`] — the paremit identity finalize+project of one
//!    merged bucket into a self-contained [`SinkEmitBuf`] (byval datums
//!    only; Reduced shapes reconstruct their redundant keys exactly as the
//!    serial read-back does).
//!
//! Phase-1 scope (admission enforced by the execmain engagement, re-checked
//! here where cheap): single-word compact keys (Single / Reduced), byval
//! transition states whose catalog combine function is on the parallel
//! merge's `COMBINE_WHITELIST` (count/sum/min/max over int/bool/date/time).
//! PolyInt128 / NumericAgg states REFUSE — their transvalues are pointers
//! into worker arenas, which die with the helper executors before the
//! leader drains (phase 2 relocates them, the donor's
//! `relocate_states_into` discipline).
//!
//! Determinism: within a bucket, groups appear in first-seen order over
//! (Locals in worker-slot order → runs in flush order → run rows in
//! insertion order → remainder rows in insertion order) — deterministic
//! given the claim history, the sink contract's rule 1.
//!
//! Hashing: the sink's OWN partition hash ([`sink_hash`], splitmix64 over
//! the canonical key words) routes rows to buckets in runs, remainders and
//! the combine alike. It is deliberately independent of any
//! [`LaneAggTable`]-internal hash kind: two workers' tables may carry
//! different `HashKind`s, but every sink-side partition decision must
//! agree. The NULL group is out-of-band everywhere and merges in bucket
//! [`SINK_NULL_BUCKET`].

use ::datum::{Datum, NullableDatum};
use ::execexpr::AggPerGroup;
use ::lanetable::{EntryLayout, HashKind, KeyRepr, LaneAggTable};
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERROR};
use ::types_fmgr::{LocalFcinfo, PGFunction};

use crate::compact::{RedDerived, RedShape};
use crate::AggStateData;

/// Combine partition count — the donors' 256-bucket radix space (top 8 hash
/// bits). Fixed for the sink's lifetime.
pub const SINK_NBUCKETS: usize = 256;

/// The bucket the out-of-band NULL group merges in (deterministic; every
/// SinkRun carries at most one NULL block and the combine for this bucket
/// absorbs them all).
pub const SINK_NULL_BUCKET: usize = 0;

/// splitmix64 finalizer — the sink's partition mix.
#[inline]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// The sink partition hash over one row's canonical key words (`w1 = 0` for
/// single-word keys). Identical everywhere a bucket decision is made.
#[inline]
pub fn sink_hash(w0: u64, w1: u64) -> u64 {
    splitmix64(w0 ^ splitmix64(w1))
}

#[inline]
fn bucket_of(h: u64) -> usize {
    (h >> 56) as usize
}

// ---------------------------------------------------------------------------
// SinkRun — the flush wire format.
// ---------------------------------------------------------------------------

/// One self-contained, radix-partitioned flush of a worker's bounded compact
/// table. Buffers are plain Vecs; state blocks are byval-POD `AggPerGroup`
/// arrays copied verbatim (the phase-1 admission's guarantee), so the run is
/// `Send` and outlives its worker's executor by construction.
pub struct SinkRun {
    /// 1 (Int) or 2 (Int128) canonical key words per row.
    pub key_words: usize,
    /// State block size in u64 words (`state_bytes / 8`; LaneAggTable
    /// rounds state_bytes to 8).
    pub state_words: usize,
    /// 257 bucket offsets over the non-NULL rows.
    pub starts: Vec<u32>,
    /// `nrows × key_words` canonical key words, bucket-major.
    pub keys: Vec<u64>,
    /// `nrows × state_words` state words, bucket-major (parallel to keys).
    pub states: Vec<u64>,
    /// The out-of-band NULL group's state block.
    pub null_states: Option<Vec<u64>>,
}

impl SinkRun {
    /// Non-NULL rows.
    #[inline]
    pub fn nrows(&self) -> usize {
        if self.key_words == 0 {
            0
        } else {
            self.keys.len() / self.key_words
        }
    }

    /// Heap bytes this run holds against the Local's budget.
    pub fn bytes(&self) -> usize {
        self.starts.capacity() * 4
            + self.keys.capacity() * 8
            + self.states.capacity() * 8
            + self.null_states.as_ref().map_or(0, |b| b.capacity() * 8)
    }
}

#[inline]
fn table_key_words(t: &LaneAggTable) -> usize {
    match t.repr() {
        KeyRepr::Int => 1,
        KeyRepr::Int128 => 2,
        KeyRepr::Bytes => unreachable!("sink admission refuses byte-key tables (C2 car)"),
    }
}

/// Row `i`'s canonical key words; `None` = the NULL group.
#[inline]
fn row_key_words(t: &LaneAggTable, i: usize) -> Option<[u64; 2]> {
    match t.repr() {
        KeyRepr::Int => t.row_key_int(i).map(|k| [k as u64, 0]),
        KeyRepr::Int128 => t.row_key_i128(i),
        KeyRepr::Bytes => unreachable!("sink admission refuses byte-key tables (C2 car)"),
    }
}

/// Flush `t` into a self-contained radix-partitioned run and RESET the table
/// in place (allocations retained — the exchange's re-arm discipline). The
/// caller holds the phase-1 admission: every state block is byval-POD.
pub fn sink_flush_table(t: &mut LaneAggTable) -> SinkRun {
    let key_words = table_key_words(t);
    let state_words = t.state_bytes() / 8;
    let n = t.nrows();
    // Pass 1: bucket counts (NULL row excluded).
    let mut counts = [0u32; SINK_NBUCKETS];
    let mut null_row: Option<usize> = None;
    for i in 0..n {
        match row_key_words(t, i) {
            Some([w0, w1]) => counts[bucket_of(sink_hash(w0, w1))] += 1,
            None => null_row = Some(i),
        }
    }
    let mut starts: Vec<u32> = Vec::with_capacity(SINK_NBUCKETS + 1);
    let mut acc = 0u32;
    starts.push(0);
    for c in counts {
        acc += c;
        starts.push(acc);
    }
    let nonnull = acc as usize;
    let mut cursor: [u32; SINK_NBUCKETS] = core::array::from_fn(|b| starts[b]);
    let mut keys: Vec<u64> = vec![0; nonnull * key_words];
    let mut states: Vec<u64> = vec![0; nonnull * state_words];
    let mut null_states: Option<Vec<u64>> = None;
    for i in 0..n {
        match row_key_words(t, i) {
            Some([w0, w1]) => {
                let b = bucket_of(sink_hash(w0, w1));
                let slot = cursor[b] as usize;
                cursor[b] += 1;
                keys[slot * key_words] = w0;
                if key_words == 2 {
                    keys[slot * key_words + 1] = w1;
                }
                // SAFETY: the row's state block is state_words u64s
                // (8-aligned by the LaneAggTable state layout); dst was
                // sized above.
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        t.row_states(i).cast::<u64>().cast_const(),
                        states.as_mut_ptr().add(slot * state_words),
                        state_words,
                    );
                }
            }
            None => {
                let mut block = vec![0u64; state_words];
                // SAFETY: as above.
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        t.row_states(i).cast::<u64>().cast_const(),
                        block.as_mut_ptr(),
                        state_words,
                    );
                }
                null_states = Some(block);
            }
        }
    }
    debug_assert_eq!(null_row.is_some(), null_states.is_some());
    t.reset();
    SinkRun { key_words, state_words, starts, keys, states, null_states }
}

// ---------------------------------------------------------------------------
// SEAL-time remainder partitioning.
// ---------------------------------------------------------------------------

/// Bucket index over a remainder table's rows (counting sort by the sink
/// hash's top byte, non-NULL rows only; `has_null` marks the out-of-band
/// group). Built once at SEAL by the last accept worker; read-only during
/// combine.
pub struct SinkPart {
    pub starts: Vec<u32>,
    pub idx: Vec<u32>,
    pub has_null: bool,
}

impl SinkPart {
    /// Retained footprint (R3 accounting: the SEAL index lives until the
    /// combine set finishes and is charged like a run).
    pub fn bytes(&self) -> usize {
        (self.starts.capacity() + self.idx.capacity()) * core::mem::size_of::<u32>()
    }
}

pub fn sink_partition_remainder(t: &LaneAggTable) -> SinkPart {
    let n = t.nrows();
    let mut counts = [0u32; SINK_NBUCKETS];
    let mut has_null = false;
    for i in 0..n {
        match row_key_words(t, i) {
            Some([w0, w1]) => counts[bucket_of(sink_hash(w0, w1))] += 1,
            None => has_null = true,
        }
    }
    let mut starts: Vec<u32> = Vec::with_capacity(SINK_NBUCKETS + 1);
    let mut acc = 0u32;
    starts.push(0);
    for c in counts {
        acc += c;
        starts.push(acc);
    }
    let mut cursor: [u32; SINK_NBUCKETS] = core::array::from_fn(|b| starts[b]);
    let mut idx = vec![0u32; acc as usize];
    for i in 0..n {
        if let Some([w0, w1]) = row_key_words(t, i) {
            let b = bucket_of(sink_hash(w0, w1));
            idx[cursor[b] as usize] = i as u32;
            cursor[b] += 1;
        }
    }
    SinkPart { starts, idx, has_null }
}

// ---------------------------------------------------------------------------
// Combine-function resolution + application.
// ---------------------------------------------------------------------------

/// One transno's resolved combine: a bare whitelist fn pointer (the
/// thread-native `combine_one_par` byval discipline — the whitelist fns
/// read only their args; no flinfo, no fcinfo.context, byval result).
#[derive(Clone, Copy)]
pub struct SinkCombineFn {
    pub func: PGFunction,
    pub strict: bool,
    pub collation: Oid,
}

/// Resolve every transno's catalog combine function, fail-closed:
/// `Ok(None)` = a transition refuses the sink (non-byval state, missing or
/// non-whitelist combinefn, DISTINCT/ORDER BY qualifiers) — the caller falls
/// back to the serial arm. Never errors on shape; only on catalog access.
pub fn sink_resolve_combines(node: &AggStateData<'_>) -> PgResult<Option<Vec<SinkCombineFn>>> {
    let numtrans = node.numtrans;
    let mut out: Vec<Option<SinkCombineFn>> = vec![None; numtrans];
    for pa in node.peragg.iter() {
        let transno = pa.transno as usize;
        let aggref = pa.aggref;
        // Ordered-set / DISTINCT / ORDER BY transitions never combine.
        if !aggref.aggdistinct.is_nil() || !aggref.aggorder.is_nil() {
            return Ok(None);
        }
        if !node.trans_typ[transno].byval {
            return Ok(None);
        }
        let Some(shape) = ::syscache_seams::lookup_pg_aggregate_shape::call(aggref.aggfnoid)?
        else {
            return Ok(None);
        };
        if shape.aggcombinefn == 0
            || !crate::merge::COMBINE_WHITELIST.contains(&shape.aggcombinefn)
        {
            return Ok(None);
        }
        let flinfo = ::fmgr_core::fmgr_info(shape.aggcombinefn)?;
        let resolved = SinkCombineFn {
            func: flinfo.fn_addr,
            strict: flinfo.fn_strict,
            collation: aggref.inputcollid,
        };
        match &out[transno] {
            // Shared transno: both aggrefs resolved the same combine by the
            // catalog key; nothing to reconcile.
            Some(_) => {}
            None => out[transno] = Some(resolved),
        }
    }
    let mut combines = Vec::with_capacity(numtrans);
    for c in out {
        // A transno no peragg names would be a planner numbering gap.
        let Some(c) = c else { return Ok(None) };
        combines.push(c);
    }
    Ok(Some(combines))
}

/// C advance_combine over two byval state blocks (`combine_one_par`'s byval
/// leg): strict adopt-or-skip, else one bare fn-pointer call. `dst` is the
/// bucket table's block (single writer — the claimed partition); `src` feeds
/// exactly once.
///
/// # Safety
/// Both blocks hold `combines.len()` live `AggPerGroup`s; byval transvalues
/// only (phase-1 admission).
pub unsafe fn sink_combine_states(
    combines: &[SinkCombineFn],
    dst: *mut AggPerGroup,
    src: *const AggPerGroup,
) -> PgResult<()> {
    for (transno, c) in combines.iter().enumerate() {
        // SAFETY: caller contract.
        let (d, s) = unsafe { (&mut *dst.add(transno), &*src.add(transno)) };
        if c.strict {
            if s.trans_value_is_null {
                continue;
            }
            if d.trans_value_is_null {
                d.trans_value = s.trans_value;
                d.trans_value_is_null = false;
                d.no_trans_value = false;
                continue;
            }
        }
        let mut fcinfo = LocalFcinfo::<2>::fresh(c.collation);
        fcinfo.args[0] =
            NullableDatum { value: d.trans_value, isnull: d.trans_value_is_null };
        fcinfo.args[1] =
            NullableDatum { value: s.trans_value, isnull: s.trans_value_is_null };
        let value = (c.func)(None, &mut fcinfo)?;
        d.trans_value = value;
        d.trans_value_is_null = fcinfo.isnull;
        d.no_trans_value = false;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// The bucket combine.
// ---------------------------------------------------------------------------

/// One Local's combine-visible faces: its flushed runs (flush order) and its
/// remainder table + SEAL partition.
pub struct SinkLocalView<'a> {
    pub runs: &'a [SinkRun],
    pub remainder: Option<(&'a LaneAggTable, &'a SinkPart)>,
}

/// Merge bucket `b` across `locals` (slice order = worker-slot order) into a
/// fresh table: runs first (flush order, rows in insertion order), then the
/// remainder rows — the first-seen discipline. NULL blocks are absorbed only
/// in [`SINK_NULL_BUCKET`]. `state_bytes` and `key_words` are the sink's
/// (identical across all sources by construction — one worker plan).
pub fn sink_combine_bucket(
    b: usize,
    key_words: usize,
    state_bytes: usize,
    locals: &[SinkLocalView<'_>],
    combines: &[SinkCombineFn],
) -> PgResult<LaneAggTable> {
    debug_assert!(b < SINK_NBUCKETS);
    let mut total = 0usize;
    for l in locals {
        for r in l.runs {
            total += (r.starts[b + 1] - r.starts[b]) as usize;
        }
        if let Some((_, p)) = l.remainder {
            total += (p.starts[b + 1] - p.starts[b]) as usize;
        }
    }
    let repr = if key_words == 2 { KeyRepr::Int128 } else { KeyRepr::Int };
    // Salt8 for Int128 (lanetable layout constraint), Inline16 otherwise:
    // bucket tables are G/256-sized — well inside the Inline16 band.
    let layout = if key_words == 2 { EntryLayout::Salt8 } else { EntryLayout::Inline16 };
    let mut t = LaneAggTable::with_config(
        repr,
        state_bytes,
        total.max(4),
        HashKind::best(),
        layout,
    );
    let state_words = state_bytes / 8;

    let absorb = |t: &mut LaneAggTable,
                      kw: Option<[u64; 2]>,
                      src: *const u64|
     -> PgResult<()> {
        let pr = match kw {
            None => t.probe_null(),
            Some([w0, w1]) => {
                if key_words == 2 {
                    t.probe_i128([w0, w1], t.hash_key_i128([w0, w1]))
                } else {
                    t.probe_int(w0 as i64, t.hash_key_int(w0))
                }
            }
        };
        if pr.is_new {
            // SAFETY: fresh zeroed state block of state_words u64s; src is a
            // live block of the same layout (one worker plan).
            unsafe {
                core::ptr::copy_nonoverlapping(src, pr.states.cast::<u64>(), state_words);
            }
            return Ok(());
        }
        // SAFETY: both blocks hold numtrans pergroups (combines.len() ==
        // numtrans); dst is uniquely reachable through this claimed bucket.
        unsafe {
            sink_combine_states(
                combines,
                pr.states.cast::<AggPerGroup>(),
                src.cast::<AggPerGroup>(),
            )
        }
    };

    for l in locals {
        for r in l.runs {
            debug_assert_eq!(r.key_words, key_words);
            debug_assert_eq!(r.state_words, state_words);
            let lo = r.starts[b] as usize;
            let hi = r.starts[b + 1] as usize;
            for i in lo..hi {
                let w0 = r.keys[i * key_words];
                let w1 = if key_words == 2 { r.keys[i * key_words + 1] } else { 0 };
                absorb(&mut t, Some([w0, w1]), unsafe {
                    // SAFETY: states holds nrows state blocks (run layout).
                    r.states.as_ptr().add(i * state_words)
                })?;
            }
            if b == SINK_NULL_BUCKET {
                if let Some(block) = &r.null_states {
                    absorb(&mut t, None, block.as_ptr())?;
                }
            }
        }
        if let Some((rem, part)) = l.remainder {
            debug_assert_eq!(table_key_words(rem), key_words);
            debug_assert_eq!(rem.state_bytes(), t.state_bytes());
            let lo = part.starts[b] as usize;
            let hi = part.starts[b + 1] as usize;
            for &row in &part.idx[lo..hi] {
                let kw = row_key_words(rem, row as usize)
                    .expect("partition indexes only non-NULL rows");
                absorb(&mut t, Some(kw), rem.row_states(row as usize).cast_const().cast())?;
            }
            if b == SINK_NULL_BUCKET && part.has_null {
                // The remainder's NULL row: find it through the table's own
                // out-of-band accessor path (row scan — at most one row).
                for row in 0..rem.nrows() {
                    if row_key_words(rem, row).is_none() {
                        absorb(&mut t, None, rem.row_states(row).cast_const().cast())?;
                        break;
                    }
                }
            }
        }
    }
    Ok(t)
}

// ---------------------------------------------------------------------------
// Identity emit (paremit).
// ---------------------------------------------------------------------------

/// The sink's single-word key spec, snapshotted at admission (leader side —
/// the same decide the worker arms run).
#[derive(Clone, Debug)]
pub enum SinkKeySpec {
    Single { width: u8 },
    Reduced(RedShape),
}

impl SinkKeySpec {
    #[inline]
    pub fn width(&self) -> u8 {
        match self {
            SinkKeySpec::Single { width } => *width,
            SinkKeySpec::Reduced(s) => s.width,
        }
    }
}

/// One output column of the identity emit projection.
#[derive(Clone, Copy)]
pub enum SinkEmitCol {
    /// The representative key (NULL for the NULL group's row).
    Key,
    /// A reconstructed redundant key (Reduced shapes; NULL for NULL group).
    Derived(RedDerived),
    /// Aggregate result = the byval transvalue (no finalfn).
    Agg { transno: u32 },
}

pub struct SinkEmitPlan {
    pub width: u8,
    pub cols: Vec<SinkEmitCol>,
}

/// The identity-emit qualification (leader side, donor `build_emit_plan`
/// extended with Reduced derived keys). `None` = the shape needs the
/// finalize/project interpreter — phase 1 refuses the sink then.
pub fn sink_build_emit_plan(
    node: &AggStateData<'_>,
    key: &SinkKeySpec,
) -> Option<SinkEmitPlan> {
    if node.skip_final || node.qual.is_some() {
        return None;
    }
    for pa in node.peragg.iter() {
        if pa.finalfn.is_some()
            || !pa.direct_args.is_empty()
            || !node.trans_typ[pa.transno as usize].byval
        {
            return None;
        }
    }
    let ph = node.perhash.as_ref()?;
    let key_attnos = &ph.hash_grp_col_idx_input;
    let tlist = &node.plan.plan.targetlist;
    let mut cols = Vec::with_capacity(tlist.len());
    for n in tlist.iter() {
        let te = n.as_target_entry()?;
        if let Some(v) = te.expr.as_var() {
            if v.varno != ::types_nodes::primnodes::OUTER_VAR {
                return None;
            }
            // Which grouping key position does this Var name?
            let Some(j) = key_attnos.iter().position(|&a| a == v.varattno) else {
                return None;
            };
            match key {
                SinkKeySpec::Single { .. } => {
                    if j != 0 {
                        return None;
                    }
                    cols.push(SinkEmitCol::Key);
                }
                SinkKeySpec::Reduced(shape) => match shape.keys.get(j)? {
                    None => cols.push(SinkEmitCol::Key),
                    Some(d) => cols.push(SinkEmitCol::Derived(*d)),
                },
            }
            continue;
        }
        if let Some(a) = te.expr.as_aggref() {
            if a.aggno < 0 || a.aggno as usize >= node.peragg.len() {
                return None;
            }
            cols.push(SinkEmitCol::Agg { transno: node.peragg[a.aggno as usize].transno });
            continue;
        }
        return None;
    }
    Some(SinkEmitPlan { width: key.width(), cols })
}

/// One bucket's fully-projected output rows: row-major, stride `cols.len()`,
/// byval datums only — self-contained across threads and past the helpers'
/// teardown.
#[derive(Default)]
pub struct SinkEmitBuf {
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
    pub nrows: usize,
}

impl SinkEmitBuf {
    pub fn bytes(&self) -> usize {
        self.values.capacity() * core::mem::size_of::<Datum>() + self.nulls.capacity()
    }
}

#[inline]
fn key_datum(width: u8, k: i64) -> Datum {
    match width {
        2 => Datum::from_i16(k as i16),
        4 => Datum::from_i32(k as i32),
        _ => Datum::from_i64(k),
    }
}

/// Finalize+project one merged bucket (rows in insertion order — the merge's
/// first-seen order) into a [`SinkEmitBuf`].
pub fn sink_emit_bucket(plan: &SinkEmitPlan, t: &LaneAggTable) -> SinkEmitBuf {
    let natts = plan.cols.len();
    let n = t.nrows();
    let mut values: Vec<Datum> = Vec::with_capacity(n * natts);
    let mut nulls: Vec<bool> = Vec::with_capacity(n * natts);
    for row in 0..n {
        let key = t.row_key_int(row);
        let states = t.row_states(row).cast_const().cast::<AggPerGroup>();
        for c in &plan.cols {
            match *c {
                SinkEmitCol::Key => match key {
                    Some(k) => {
                        values.push(key_datum(plan.width, k));
                        nulls.push(false);
                    }
                    None => {
                        values.push(Datum::null());
                        nulls.push(true);
                    }
                },
                SinkEmitCol::Derived(d) => match key {
                    // Reconstruction is exact by the feed's admission-time
                    // range guard; a NULL representative derives NULL (the
                    // strict ± operators' per-row result).
                    Some(k) => {
                        values.push(key_datum(plan.width, d.eval(k)));
                        nulls.push(false);
                    }
                    None => {
                        values.push(Datum::null());
                        nulls.push(true);
                    }
                },
                // SAFETY: the row's state block holds numtrans pergroups
                // (bucket-table config = the sink's state_bytes); transno <
                // numtrans by plan construction. Byval transvalues only.
                SinkEmitCol::Agg { transno } => unsafe {
                    let pg = &*states.add(transno as usize);
                    values.push(pg.trans_value);
                    nulls.push(pg.trans_value_is_null);
                },
            }
        }
    }
    SinkEmitBuf { values, nulls, nrows: n }
}

/// Sanity error for engagement paths that must never see a non-single-word
/// table (fail-closed conversion helper).
pub fn sink_shape_error(what: &str) -> Box<PgError> {
    PgError::new(ERROR, format!("aggregation sink shape violation: {what}")).into()
}

/// A group count over an emit-buf set (observability).
pub fn sink_emit_rows(bufs: &[SinkEmitBuf]) -> usize {
    bufs.iter().map(|b| b.nrows).sum()
}

// ---------------------------------------------------------------------------
// Executor-coupled surface (the engagement's nodeagg seam).
// ---------------------------------------------------------------------------

/// The sink's plan-shape gate: a hashed, simple-split, non-grouping-sets
/// Agg with at least one grouping key (leader admission + worker re-check).
pub fn agg_sink_plan_shape_ok(node: &AggStateData<'_>) -> bool {
    node.plan.aggstrategy == ::types_pathnodes::AGG_HASHED
        && node.plan.aggsplit == ::types_pathnodes::AGGSPLIT_SIMPLE
        && node.plan.groupingSets.is_nil()
        && node.plan.numCols >= 1
        && node.gsets.is_none()
}

/// Arm SINK MODE on a worker build: the compact arms gate/size by `cap`
/// (bounded Local discipline) and the runtime backstop fails closed instead
/// of migrating. Must run BEFORE `agg_hash_compact_try_arm*`.
pub fn agg_sink_set_cap(node: &mut AggStateData<'_>, cap: u32) {
    if let Some(ph) = node.perhash.as_mut() {
        ph.sink_cap = Some(cap);
    }
}

/// The node's per-participant hash memory budget (C
/// `work_mem × hash_mem_multiplier` — `get_hash_memory_limit`), the R3
/// per-Local envelope.
pub fn agg_sink_hash_mem_limit(node: &AggStateData<'_>) -> Option<usize> {
    node.perhash.as_ref().map(|ph| ph.hash_mem_limit)
}

/// The grouped state block size (`additionalsize` — numtrans pergroups).
pub fn agg_sink_state_bytes(node: &AggStateData<'_>) -> Option<usize> {
    node.perhash.as_ref().map(|ph| ph.hashtable.additionalsize())
}

/// The single staged int grouping key's width (2/4/8), when the shape is the
/// K2 single-key class.
pub fn agg_sink_key_width(node: &AggStateData<'_>) -> Option<u8> {
    node.perhash.as_ref().and_then(|ph| ph.hashtable.staged_probe_int_width())
}

/// The ARMED compact table's sink key spec (worker-side shape re-check).
/// `None` = not armed or a shape the phase-1 sink refuses (Multi / intern).
pub fn agg_sink_key_spec(node: &AggStateData<'_>) -> Option<SinkKeySpec> {
    let ch = node.perhash.as_ref()?.compact.as_ref()?;
    if ch.intern.is_some() {
        return None;
    }
    match &ch.key {
        crate::compact::CompactKeySpec::Single { width } => {
            Some(SinkKeySpec::Single { width: *width })
        }
        crate::compact::CompactKeySpec::Reduced(shape) => {
            Some(SinkKeySpec::Reduced(shape.clone()))
        }
        crate::compact::CompactKeySpec::Multi(_) => None,
    }
}

/// Owned worker table handle: the ENTIRE armed compact state, moved between
/// the executor (`ph.compact`, during a morsel drain) and the sink Local
/// (between morsels / at SEAL). Opaque outside nodeagg.
pub struct SinkTableHandle(pub(crate) crate::compact::CompactHash);

// SAFETY: the handle's only non-Send payload is the CompactHash batch
// scratch (`states: Vec<*mut u8>` — per-batch probe outputs). The scratch is
// cleared at the start of every batch probe and read only within that batch
// on the probing thread; between morsels (the only time the handle crosses
// threads) it is stale garbage that nothing dereferences. The table itself
// is plain owned Vec storage, and under the sink's phase-1 admission every
// state block is byval-POD (no interior pointers).
unsafe impl Send for SinkTableHandle {}
// SAFETY: combine tasks read `&SinkTableHandle` (the table's rows) from many
// threads; the table is plain owned Vec storage with byval-POD state blocks
// under the sink admission, and the batch scratch is never dereferenced
// outside the owning worker's own morsel (see the Send justification).
unsafe impl Sync for SinkTableHandle {}

impl SinkTableHandle {
    #[inline]
    pub fn table(&self) -> &LaneAggTable {
        &self.0.table
    }

    #[inline]
    pub fn table_mut(&mut self) -> &mut LaneAggTable {
        &mut self.0.table
    }
}

/// Move the armed compact state OUT of the executor (end of a morsel drain:
/// the Local owns it until the next morsel / SEAL). `None` = not armed.
pub fn agg_sink_take_table(node: &mut AggStateData<'_>) -> Option<SinkTableHandle> {
    node.perhash.as_mut()?.compact.take().map(SinkTableHandle)
}

/// Move the compact state back INTO the executor (start of a morsel drain).
pub fn agg_sink_put_table(node: &mut AggStateData<'_>, h: SinkTableHandle) {
    if let Some(ph) = node.perhash.as_mut() {
        debug_assert!(ph.compact.is_none(), "sink put over a live compact table");
        ph.compact = Some(h.0);
    }
}

/// Flush the armed table into a run if it crossed `cap` (checked BEFORE a
/// batch — no caller-held group pointer is ever invalidated mid-batch).
pub fn agg_sink_flush_if_due(node: &mut AggStateData<'_>, cap: u32) -> Option<SinkRun> {
    let ch = node.perhash.as_mut()?.compact.as_mut()?;
    if ch.table.len() < cap as usize {
        return None;
    }
    Some(sink_flush_table(&mut ch.table))
}

/// The armed table's current footprint (budget accounting).
pub fn agg_sink_table_mem(node: &AggStateData<'_>) -> usize {
    node.perhash
        .as_ref()
        .and_then(|ph| ph.compact.as_ref())
        .map_or(0, |ch| ch.table.mem_used())
}

// ---------------------------------------------------------------------------
// Leader-side adopted emit (the published sink output as the Agg's source).
// ---------------------------------------------------------------------------

/// The leader's adopted parallel emit state: published per-bucket
/// identity-projected rows, drained bucket 0..255 in insertion order.
pub struct SinkEmitState {
    pub bufs: Vec<SinkEmitBuf>,
    pub natts: usize,
    bucket: usize,
    pos: usize,
}

/// Adopt the published emit set; subsequent [`agg_sink_emit_next`] calls
/// drain it. The Agg becomes a pure Source (its build never ran).
pub fn agg_sink_adopt_emit(node: &mut AggStateData<'_>, bufs: Vec<SinkEmitBuf>, natts: usize) {
    debug_assert_eq!(bufs.len(), SINK_NBUCKETS);
    node.sink_emit = Some(Box::new(SinkEmitState { bufs, natts, bucket: 0, pos: 0 }));
}

/// Mid-emit resume marker for the lane dispatch.
pub fn agg_sink_emitting(node: &AggStateData<'_>) -> bool {
    node.sink_emit.is_some()
}

/// One emitted row per call (the donor `agg_retrieve_emitted` shape: a datum
/// memcpy into the result slot — no finalize, no projection interpreter, no
/// per-row expr-context reset; byval datums only). `None` = drained
/// (agg_done set; the state drops).
pub fn agg_sink_emit_next<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut ::executils::EStateData<'mcx>,
) -> PgResult<Option<::executils::ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    let next = {
        let st = node.sink_emit.as_mut().expect("sink emit state adopted");
        loop {
            if st.bucket >= SINK_NBUCKETS {
                break None;
            }
            let b = &st.bufs[st.bucket];
            if st.pos >= b.nrows {
                st.bucket += 1;
                st.pos = 0;
                continue;
            }
            let row = st.pos;
            st.pos += 1;
            break Some((st.bucket, row));
        }
    };
    let Some((bucket, row)) = next else {
        node.sink_emit = None;
        node.agg_done = true;
        return Ok(None);
    };
    let st = node.sink_emit.as_ref().expect("sink emit state adopted");
    let natts = st.natts;
    let buf = &st.bufs[bucket];
    let base = row * natts;
    let slot = estate.slot_mut(node.ps_ResultTupleSlot);
    ::exectuples::exec_clear_tuple(slot, mcx);
    {
        let sb = slot.base_mut();
        sb.tts_values[..natts].copy_from_slice(&buf.values[base..base + natts]);
        sb.tts_isnull[..natts].copy_from_slice(&buf.nulls[base..base + natts]);
    }
    ::exectuples::exec_store_virtual_tuple(slot);
    Ok(Some(node.ps_ResultTupleSlot))
}

/// Drop any adopted emit state (rescan / teardown safety).
pub fn agg_sink_reset_emit(node: &mut AggStateData<'_>) {
    node.sink_emit = None;
}

// ---------------------------------------------------------------------------
// Unit tests: pure kernels, no executor.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const STATE_BYTES: usize = core::mem::size_of::<AggPerGroup>() * 2;

    fn mk_table(hint: usize) -> LaneAggTable {
        LaneAggTable::with_config(
            KeyRepr::Int,
            STATE_BYTES,
            hint,
            HashKind::best(),
            EntryLayout::Inline16,
        )
    }

    // Two toy transitions: [0] a count (int8, non-null from birth), [1] a
    // strict max (adopt-or-larger).
    fn bump(t: &mut LaneAggTable, key: Option<i64>, count: i64, max: i64) {
        let pr = match key {
            Some(k) => t.probe_int(k, t.hash_key_int(k as u64)),
            None => t.probe_null(),
        };
        let pg = pr.states.cast::<AggPerGroup>();
        unsafe {
            if pr.is_new {
                pg.write(AggPerGroup {
                    trans_value: Datum::from_i64(0),
                    trans_value_is_null: false,
                    no_trans_value: false,
                });
                pg.add(1).write(AggPerGroup {
                    trans_value: Datum::null(),
                    trans_value_is_null: true,
                    no_trans_value: true,
                });
            }
            let c = &mut *pg;
            c.trans_value = Datum::from_i64(c.trans_value.as_i64() + count);
            let m = &mut *pg.add(1);
            if m.trans_value_is_null || m.trans_value.as_i64() < max {
                m.trans_value = Datum::from_i64(max);
                m.trans_value_is_null = false;
                m.no_trans_value = false;
            }
        }
    }

    fn test_combines() -> Vec<SinkCombineFn> {
        fn add(
            _f: Option<&mut ::types_fmgr::FmgrInfo>,
            fcinfo: &mut ::types_fmgr::FunctionCallInfoBaseData,
        ) -> PgResult<Datum> {
            let a = fcinfo.args[0].value.as_i64();
            let b = fcinfo.args[1].value.as_i64();
            Ok(Datum::from_i64(a + b))
        }
        fn larger(
            _f: Option<&mut ::types_fmgr::FmgrInfo>,
            fcinfo: &mut ::types_fmgr::FunctionCallInfoBaseData,
        ) -> PgResult<Datum> {
            let a = fcinfo.args[0].value.as_i64();
            let b = fcinfo.args[1].value.as_i64();
            Ok(Datum::from_i64(a.max(b)))
        }
        vec![
            SinkCombineFn { func: add, strict: true, collation: Oid::from(0u8) },
            SinkCombineFn { func: larger, strict: true, collation: Oid::from(0u8) },
        ]
    }

    fn read_group(t: &LaneAggTable, key: Option<i64>) -> Option<(i64, i64)> {
        for row in 0..t.nrows() {
            if t.row_key_int(row) == key {
                let pg = t.row_states(row).cast_const().cast::<AggPerGroup>();
                unsafe {
                    return Some(((*pg).trans_value.as_i64(), (*pg.add(1)).trans_value.as_i64()));
                }
            }
        }
        None
    }

    #[test]
    fn flush_partition_combine_roundtrip() {
        // Worker 1: keys 0..1000 twice; worker 2: keys 500..1500 once; plus
        // NULL groups on both. Worker 1 flushes mid-way (run + remainder).
        let mut t1 = mk_table(64);
        for k in 0..1000 {
            bump(&mut t1, Some(k), 1, k);
        }
        bump(&mut t1, None, 1, 7);
        let run1 = sink_flush_table(&mut t1);
        assert_eq!(run1.nrows(), 1000);
        assert!(run1.null_states.is_some());
        assert_eq!(t1.nrows(), 0);
        for k in 0..1000 {
            bump(&mut t1, Some(k), 1, 2 * k);
        }
        bump(&mut t1, None, 2, 3);
        let part1 = sink_partition_remainder(&t1);
        assert!(part1.has_null);

        let mut t2 = mk_table(64);
        for k in 500..1500 {
            bump(&mut t2, Some(k), 1, 3 * k);
        }
        let part2 = sink_partition_remainder(&t2);
        assert!(!part2.has_null);

        let locals = [
            SinkLocalView { runs: core::slice::from_ref(&run1), remainder: Some((&t1, &part1)) },
            SinkLocalView { runs: &[], remainder: Some((&t2, &part2)) },
        ];
        let combines = test_combines();
        let mut merged: Vec<LaneAggTable> = Vec::with_capacity(SINK_NBUCKETS);
        for b in 0..SINK_NBUCKETS {
            merged.push(
                sink_combine_bucket(b, 1, STATE_BYTES, &locals, &combines).unwrap(),
            );
        }
        // Every key lands in exactly one bucket; totals add up.
        let mut seen = std::collections::HashMap::new();
        let mut null_seen = None;
        for (b, t) in merged.iter().enumerate() {
            for row in 0..t.nrows() {
                match t.row_key_int(row) {
                    Some(k) => {
                        let pg = t.row_states(row).cast_const().cast::<AggPerGroup>();
                        let (c, m) = unsafe {
                            ((*pg).trans_value.as_i64(), (*pg.add(1)).trans_value.as_i64())
                        };
                        assert!(seen.insert(k, (c, m)).is_none(), "key {k} in two buckets");
                        assert_eq!(b, bucket_of(sink_hash(k as u64, 0)));
                    }
                    None => {
                        assert_eq!(b, SINK_NULL_BUCKET);
                        let pg = t.row_states(row).cast_const().cast::<AggPerGroup>();
                        null_seen = Some(unsafe {
                            ((*pg).trans_value.as_i64(), (*pg.add(1)).trans_value.as_i64())
                        });
                    }
                }
            }
        }
        assert_eq!(seen.len(), 1500);
        for k in 0..1500i64 {
            let (c, m) = seen[&k];
            let want_c = i64::from(k < 1000) * 2 + i64::from(k >= 500);
            let want_m = if k < 500 {
                2 * k
            } else if k < 1000 {
                (2 * k).max(3 * k)
            } else {
                3 * k
            };
            assert_eq!(c, want_c, "count of key {k}");
            assert_eq!(m, want_m.max(if k < 1000 { k } else { want_m }), "max of key {k}");
        }
        assert_eq!(null_seen, Some((3, 7)));
    }

    #[test]
    fn combine_first_seen_order_is_source_major() {
        // Locals in slice order; runs before remainder. Keys chosen to share
        // one bucket: probe insertion order must be run1 keys, then
        // remainder keys, then local-2 keys.
        // Find 3 keys in the same bucket.
        let mut same: Vec<i64> = Vec::new();
        let want_bucket = bucket_of(sink_hash(1, 0));
        let mut k = 1i64;
        while same.len() < 3 {
            if bucket_of(sink_hash(k as u64, 0)) == want_bucket {
                same.push(k);
            }
            k += 1;
        }

        let mut t1 = mk_table(4);
        bump(&mut t1, Some(same[0]), 1, 0);
        let run1 = sink_flush_table(&mut t1);
        bump(&mut t1, Some(same[1]), 1, 0);
        let part1 = sink_partition_remainder(&t1);
        let mut t2 = mk_table(4);
        bump(&mut t2, Some(same[2]), 1, 0);
        bump(&mut t2, Some(same[0]), 1, 0);
        let part2 = sink_partition_remainder(&t2);
        let locals = [
            SinkLocalView { runs: core::slice::from_ref(&run1), remainder: Some((&t1, &part1)) },
            SinkLocalView { runs: &[], remainder: Some((&t2, &part2)) },
        ];
        let combines = test_combines();
        let t = sink_combine_bucket(want_bucket, 1, STATE_BYTES, &locals, &combines).unwrap();
        assert_eq!(t.nrows(), 3);
        assert_eq!(t.row_key_int(0), Some(same[0]));
        assert_eq!(t.row_key_int(1), Some(same[1]));
        assert_eq!(t.row_key_int(2), Some(same[2]));
        // same[0] merged across both locals.
        assert_eq!(read_group(&t, Some(same[0])), Some((2, 0)));
    }

    #[test]
    fn emit_bucket_identity_and_derived() {
        let mut t = mk_table(8);
        bump(&mut t, Some(41), 5, 9);
        bump(&mut t, None, 2, 1);
        let plan = SinkEmitPlan {
            width: 4,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::Derived(RedDerived {
                    op: crate::compact::RedOp::Sub,
                    konst: 1,
                    var_is_arg0: true,
                }),
                SinkEmitCol::Agg { transno: 0 },
                SinkEmitCol::Agg { transno: 1 },
            ],
        };
        let buf = sink_emit_bucket(&plan, &t);
        assert_eq!(buf.nrows, 2);
        // Row 0 = key 41: [41, 40, 5, 9].
        assert_eq!(buf.values[0].as_i32(), 41);
        assert!(!buf.nulls[0]);
        assert_eq!(buf.values[1].as_i32(), 40);
        assert_eq!(buf.values[2].as_i64(), 5);
        assert_eq!(buf.values[3].as_i64(), 9);
        // Row 1 = NULL group: [NULL, NULL, 2, 1].
        assert!(buf.nulls[4] && buf.nulls[5]);
        assert_eq!(buf.values[6].as_i64(), 2);
        assert_eq!(buf.values[7].as_i64(), 1);
    }

    #[test]
    fn int128_run_and_combine() {
        let mut t = LaneAggTable::with_config(
            KeyRepr::Int128,
            STATE_BYTES,
            8,
            HashKind::best(),
            EntryLayout::Salt8,
        );
        let keys: [[u64; 2]; 3] = [[1, 2], [3, 4], [1, 2]];
        for k in keys {
            let pr = t.probe_i128(k, t.hash_key_i128(k));
            let pg = pr.states.cast::<AggPerGroup>();
            unsafe {
                if pr.is_new {
                    pg.write(AggPerGroup {
                        trans_value: Datum::from_i64(0),
                        trans_value_is_null: false,
                        no_trans_value: false,
                    });
                    pg.add(1).write(AggPerGroup {
                        trans_value: Datum::from_i64(0),
                        trans_value_is_null: false,
                        no_trans_value: false,
                    });
                }
                let c = &mut *pg;
                c.trans_value = Datum::from_i64(c.trans_value.as_i64() + 1);
            }
        }
        let run = sink_flush_table(&mut t);
        assert_eq!(run.nrows(), 2);
        assert_eq!(run.key_words, 2);
        let locals = [SinkLocalView { runs: core::slice::from_ref(&run), remainder: None }];
        let combines = test_combines();
        let mut found = 0;
        for b in 0..SINK_NBUCKETS {
            let t = sink_combine_bucket(b, 2, STATE_BYTES, &locals, &combines).unwrap();
            for row in 0..t.nrows() {
                let k = t.row_key_i128(row).unwrap();
                let pg = t.row_states(row).cast_const().cast::<AggPerGroup>();
                let c = unsafe { (*pg).trans_value.as_i64() };
                if k == [1, 2] {
                    assert_eq!(c, 2);
                } else {
                    assert_eq!(k, [3, 4]);
                    assert_eq!(c, 1);
                }
                found += 1;
            }
        }
        assert_eq!(found, 2);
    }
}
