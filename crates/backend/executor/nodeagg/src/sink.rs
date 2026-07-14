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

use crate::compact::{MkCompKind, MkShape, RedDerived, RedShape};
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

/// The sink partition hash over CANONICAL KEY BYTES (text-bearing Multi
/// shapes): splitmix64 chained over 8-byte little-endian chunks, seeded by
/// the length. Value-derived only — identical across workers and
/// deliberately independent of any [`LaneAggTable`]-internal hash kind,
/// exactly the [`sink_hash`] discipline.
#[inline]
pub fn sink_hash_bytes(b: &[u8]) -> u64 {
    let mut h = splitmix64(b.len() as u64);
    let mut it = b.chunks_exact(8);
    for c in it.by_ref() {
        let w = u64::from_le_bytes(c.try_into().expect("exact 8-byte chunk"));
        h = splitmix64(h ^ w);
    }
    let rem = it.remainder();
    if !rem.is_empty() {
        let mut w = [0u8; 8];
        w[..rem.len()].copy_from_slice(rem);
        h = splitmix64(h ^ u64::from_le_bytes(w));
    }
    h
}

// ---------------------------------------------------------------------------
// SinkRun — the flush wire format.
// ---------------------------------------------------------------------------

/// One self-contained, radix-partitioned flush of a worker's bounded compact
/// table. Buffers are plain Vecs; state blocks are byval-POD `AggPerGroup`
/// arrays copied verbatim (the phase-1 admission's guarantee), so the run is
/// `Send` and outlives its worker's executor by construction.
pub struct SinkRun {
    /// 1 (Int) or 2 (Int128) canonical key words per row; 0 = BYTES MODE
    /// (canonical text-bearing shapes — keys live in `key_offs`/`key_bytes`).
    pub key_words: usize,
    /// State block size in u64 words (`state_bytes / 8`; LaneAggTable
    /// rounds state_bytes to 8).
    pub state_words: usize,
    /// 257 bucket offsets over the non-NULL rows.
    pub starts: Vec<u32>,
    /// `nrows × key_words` canonical key words, bucket-major (word modes).
    pub keys: Vec<u64>,
    /// `nrows × state_words` state words, bucket-major (parallel to keys).
    pub states: Vec<u64>,
    /// The out-of-band NULL group's state block (word modes; canonical
    /// bytes-mode shapes are non-nullable — never a NULL group).
    pub null_states: Option<Vec<u64>>,
    /// Bytes mode: `nrows + 1` offsets into `key_bytes`, bucket-major and
    /// contiguous (row i's canonical key = `key_bytes[key_offs[i]..
    /// key_offs[i+1]]`). Empty in word modes.
    pub key_offs: Vec<u32>,
    /// Bytes mode: canonical key bytes, COPIED at flush (the table reset
    /// frees its arena; the intern table is scan-lifetime but the run must
    /// stay self-contained — the groundwork's copy discipline).
    pub key_bytes: Vec<u8>,
}

impl SinkRun {
    /// Non-NULL rows.
    #[inline]
    pub fn nrows(&self) -> usize {
        if self.key_words == 0 {
            self.key_offs.len().saturating_sub(1)
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
            + self.key_offs.capacity() * 4
            + self.key_bytes.capacity()
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
    SinkRun {
        key_words,
        state_words,
        starts,
        keys,
        states,
        null_states,
        key_offs: Vec::new(),
        key_bytes: Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Canonical key bytes (text-bearing Multi shapes — the C2 car).
// ---------------------------------------------------------------------------

/// The armed compact state's canonical (text-bearing) Multi shape, when the
/// sink must merge on CANONICAL KEY BYTES: a Multi key spec carrying an
/// Intern component. `None` = word-keyed shapes (the existing paths).
fn compact_canon_shape(ch: &crate::compact::CompactHash) -> Option<&MkShape> {
    match &ch.key {
        crate::compact::CompactKeySpec::Multi(s) if s.intern_comp().is_some() => Some(s),
        _ => None,
    }
}

/// Row `row`'s packed key image as two little-endian words (one-word shapes
/// zero-fill the high word) — the sink-side twin of compact's `mk_row_words`
/// over a borrowed table.
#[inline]
fn mk_words_of(table: &LaneAggTable, shape: &MkShape, row: usize) -> [u64; 2] {
    if shape.two_words {
        table.row_key_i128(row).expect("multi-key tables have no NULL row")
    } else {
        let k = table.row_key_int(row).expect("multi-key tables have no NULL row");
        [k as u64, 0]
    }
}

/// Materialize row `row`'s CANONICAL KEY BYTES into `out`: the packed
/// image's `packed_bytes` little-endian bytes with the Intern component's
/// 4 id bytes ZEROED (intern ids are PER-WORKER — never canonical), followed
/// by the interned text bytes (the intern table's reverse map). Injective:
/// the prefix is fixed-width per shape, so the text tail decodes
/// unambiguously; equal component values produce identical bytes on every
/// worker — the cross-Local merge key.
fn canon_row_bytes(
    table: &LaneAggTable,
    shape: &MkShape,
    intern: &LaneAggTable,
    row: usize,
    out: &mut Vec<u8>,
) {
    debug_assert!(!shape.nullable, "canonical shapes are non-nullable (sink admission)");
    let words = mk_words_of(table, shape, row);
    let (_, icomp) =
        shape.intern_comp().expect("canonical shapes carry an Intern component");
    let id = crate::compact::mk_unpack(words, icomp) as u32;
    out.clear();
    let mut flat = [0u8; 16];
    flat[..8].copy_from_slice(&words[0].to_le_bytes());
    flat[8..].copy_from_slice(&words[1].to_le_bytes());
    out.extend_from_slice(&flat[..shape.packed_bytes as usize]);
    let ioff = icomp.off as usize;
    for b in &mut out[ioff..ioff + 4] {
        *b = 0;
    }
    let mut scratch = [0u8; 8];
    let bytes = intern
        .row_key_bytes(id as usize, &mut scratch)
        .expect("intern ids never map to a NULL row");
    out.extend_from_slice(bytes);
}

/// [`sink_flush_table`]'s canonical-bytes twin: flush the armed compact
/// table of a text-bearing Multi shape into a BYTES-MODE run (canonical key
/// bytes copied out — the reset frees the table's own storage; the intern
/// table is deliberately NOT reset: it is scan-lifetime and the remainder's
/// ids stay valid). Bucket-major two-pass counting sort by
/// [`sink_hash_bytes`] over the canonical bytes.
fn sink_flush_table_canon(ch: &mut crate::compact::CompactHash) -> SinkRun {
    let crate::compact::CompactHash { table, key, intern, .. } = ch;
    let crate::compact::CompactKeySpec::Multi(shape) = key else {
        unreachable!("canonical flush requires a Multi shape")
    };
    let intern = intern.as_ref().expect("canonical shapes carry the intern table");
    let state_words = table.state_bytes() / 8;
    let n = table.nrows();
    let mut canon: Vec<u8> = Vec::with_capacity(64);
    // Pass 1: per-bucket row + byte counts (hashes cached — the canonical
    // materialization reruns in pass 2, the hash need not).
    let mut counts = [0u32; SINK_NBUCKETS];
    let mut byte_counts = [0usize; SINK_NBUCKETS];
    let mut hashes: Vec<u64> = Vec::with_capacity(n);
    for i in 0..n {
        canon_row_bytes(table, shape, intern, i, &mut canon);
        let h = sink_hash_bytes(&canon);
        hashes.push(h);
        counts[bucket_of(h)] += 1;
        byte_counts[bucket_of(h)] += canon.len();
    }
    let mut starts: Vec<u32> = Vec::with_capacity(SINK_NBUCKETS + 1);
    let mut acc = 0u32;
    starts.push(0);
    for c in counts {
        acc += c;
        starts.push(acc);
    }
    let total_bytes: usize = byte_counts.iter().sum();
    let mut bstart = [0usize; SINK_NBUCKETS];
    {
        let mut b_acc = 0usize;
        for (b, &bc) in byte_counts.iter().enumerate() {
            bstart[b] = b_acc;
            b_acc += bc;
        }
    }
    let mut cursor: [u32; SINK_NBUCKETS] = core::array::from_fn(|b| starts[b]);
    let mut bcursor = bstart;
    let mut key_offs: Vec<u32> = vec![0; n + 1];
    let mut key_bytes: Vec<u8> = vec![0; total_bytes];
    let mut states: Vec<u64> = vec![0; n * state_words];
    for i in 0..n {
        canon_row_bytes(table, shape, intern, i, &mut canon);
        let b = bucket_of(hashes[i]);
        let slot = cursor[b] as usize;
        cursor[b] += 1;
        let off = bcursor[b];
        bcursor[b] += canon.len();
        key_offs[slot] = off as u32;
        key_bytes[off..off + canon.len()].copy_from_slice(&canon);
        // SAFETY: the row's state block is state_words u64s (8-aligned by
        // the LaneAggTable state layout); dst was sized above.
        unsafe {
            core::ptr::copy_nonoverlapping(
                table.row_states(i).cast::<u64>().cast_const(),
                states.as_mut_ptr().add(slot * state_words),
                state_words,
            );
        }
    }
    key_offs[n] = total_bytes as u32;
    // Offsets are consistent per slot: rows within a bucket fill both the
    // slot range and the byte range in the same order, and buckets are laid
    // out contiguously — slot s's key ends exactly where slot s+1 begins.
    debug_assert!(key_offs.windows(2).all(|w| w[0] <= w[1]));
    table.reset();
    SinkRun {
        key_words: 0,
        state_words,
        starts,
        keys: Vec::new(),
        states,
        null_states: None,
        key_offs,
        key_bytes,
    }
}

/// [`sink_partition_remainder`]'s canonical twin: bucket index by
/// [`sink_hash_bytes`] over each remainder row's canonical bytes. Canonical
/// shapes are non-nullable — `has_null` is structurally false.
fn sink_partition_remainder_canon(ch: &crate::compact::CompactHash) -> SinkPart {
    let crate::compact::CompactHash { table, key, intern, .. } = ch;
    let crate::compact::CompactKeySpec::Multi(shape) = key else {
        unreachable!("canonical partition requires a Multi shape")
    };
    let intern = intern.as_ref().expect("canonical shapes carry the intern table");
    let n = table.nrows();
    let mut canon: Vec<u8> = Vec::with_capacity(64);
    let mut counts = [0u32; SINK_NBUCKETS];
    let mut hashes: Vec<u64> = Vec::with_capacity(n);
    for i in 0..n {
        canon_row_bytes(table, shape, intern, i, &mut canon);
        let h = sink_hash_bytes(&canon);
        hashes.push(h);
        counts[bucket_of(h)] += 1;
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
    for (i, &h) in hashes.iter().enumerate() {
        let b = bucket_of(h);
        idx[cursor[b] as usize] = i as u32;
        cursor[b] += 1;
    }
    SinkPart { starts, idx, has_null: false }
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

/// How the sink owns and combines one transno's state.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SinkCombineKind {
    /// Byval whitelist combinefn — bare fn-pointer call, state rides in the
    /// pergroup word (self-contained everywhere).
    Byval,
    /// `Int128AggState` (INTERNAL transtype; `int8_avg_combine` 2785 — the
    /// avg/sum(int8) family): thread-native n/sum_x adds; a NULL dst adopts
    /// the src POINTER. Sources are consumed exactly once and every source
    /// (run state blocks, remainder tables, the worker aggcontexts their
    /// transvalues point into) outlives the combine task — `drive_pinned`
    /// holds every helper until the whole RG settles. Emit finalizes into
    /// self-contained bytes (the EmitBuf arena) before anything crosses to
    /// the leader.
    PolyInt128,
    /// int8[2] `{count,sum}` transarray (`_int8` 1016; `int4_avg_combine`
    /// 3324 — the avg(int2/int4) family): thread-native element adds through
    /// the live aggcontext image, same lifetime argument as PolyInt128.
    AvgInt8,
}

/// One transno's resolved combine: the kind + (byval only) a bare whitelist
/// fn pointer (the thread-native `combine_one_par` byval discipline — the
/// whitelist fns read only their args; no flinfo, no fcinfo.context, byval
/// result).
#[derive(Clone, Copy)]
pub struct SinkCombineFn {
    pub func: PGFunction,
    pub strict: bool,
    pub collation: Oid,
    pub kind: SinkCombineKind,
}

/// INTERNAL — the pointer-datum transition type of the poly agg family.
const INTERNALOID: Oid = 2281;
/// `_int8` (int8 array) — int8_avg's declared transition type.
const INT8ARRAYOID: Oid = 1016;
/// `int8_avg_combine` — the Int128AggState combine (avg/sum over int8).
const COMBINE_POLY: Oid = 2785;
/// `int4_avg_combine` — the int8[2] transarray combine (avg over int2/int4).
const COMBINE_INT4_AVG: Oid = 3324;
/// `int8_avg` — avg(int2/int4)'s finalfn over the int8[2] transarray.
const FINALFN_INT8_AVG: Oid = 1964;
/// `numeric_poly_avg` / `numeric_poly_sum` — the Int128AggState finalfns.
const FINALFN_POLY_AVG: Oid = 3389;
const FINALFN_POLY_SUM: Oid = 3388;

/// Resolve every transno's catalog combine function, fail-closed:
/// `Ok(None)` = a transition refuses the sink (unknown state class, missing
/// or non-whitelist combinefn, DISTINCT/ORDER BY qualifiers) — the caller
/// falls back to the serial arm. Never errors on shape; only on catalog
/// access. Admitted classes: byval whitelist, PolyInt128 (avg/sum int8),
/// AvgInt8 (avg int2/int4) — the two byref classes finalize at emit
/// ([`sink_emit_bucket`]), so nothing pointer-shaped ever reaches the
/// leader.
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
        let Some(shape) = ::syscache_seams::lookup_pg_aggregate_shape::call(aggref.aggfnoid)?
        else {
            return Ok(None);
        };
        if shape.aggcombinefn == 0 {
            return Ok(None);
        }
        let kind = if aggref.aggtranstype == INTERNALOID {
            if shape.aggcombinefn != COMBINE_POLY {
                return Ok(None);
            }
            SinkCombineKind::PolyInt128
        } else if aggref.aggtranstype == INT8ARRAYOID {
            if shape.aggcombinefn != COMBINE_INT4_AVG {
                return Ok(None);
            }
            SinkCombineKind::AvgInt8
        } else if node.trans_typ[transno].byval
            && crate::merge::COMBINE_WHITELIST.contains(&shape.aggcombinefn)
        {
            SinkCombineKind::Byval
        } else {
            return Ok(None);
        };
        let flinfo = ::fmgr_core::fmgr_info(shape.aggcombinefn)?;
        let resolved = SinkCombineFn {
            func: flinfo.fn_addr,
            strict: flinfo.fn_strict,
            collation: aggref.inputcollid,
            kind,
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

/// Whether any transno's state is byref (PolyInt128 / AvgInt8): the worker
/// drain adds the aggcontext subtree to its budget accounting exactly when
/// this holds (byref states live there, not in the table rows).
pub fn sink_combines_byref(combines: &[SinkCombineFn]) -> bool {
    combines.iter().any(|c| c.kind != SinkCombineKind::Byval)
}

/// C advance_combine over two state blocks (`combine_one_par`'s thread-
/// native discipline): strict adopt-or-skip, then — Byval — one bare
/// fn-pointer call, or — the byref classes — the combinefn's exact
/// arithmetic core run natively (the fmgr fns demand an agg context to
/// allocate their NULL-dst state; the sink adopts the src pointer instead,
/// identical field values, consumed exactly once). `dst` is the bucket
/// table's block (single writer — the claimed partition); `src` feeds
/// exactly once.
///
/// # Safety
/// Both blocks hold `combines.len()` live `AggPerGroup`s; non-null byref
/// transvalues are live states (worker aggcontexts, alive through the
/// combine — `drive_pinned` holds every helper to RG settlement), uniquely
/// reachable through their one feeding source.
pub unsafe fn sink_combine_states(
    combines: &[SinkCombineFn],
    dst: *mut AggPerGroup,
    src: *const AggPerGroup,
) -> PgResult<()> {
    for (transno, c) in combines.iter().enumerate() {
        // SAFETY: caller contract.
        let (d, s) = unsafe { (&mut *dst.add(transno), &*src.add(transno)) };
        if c.strict || c.kind != SinkCombineKind::Byval {
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
        match c.kind {
            SinkCombineKind::Byval => {
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
            // int8_avg_combine's HAVE_INT128 core (numeric.c), the merge's
            // combine_one_par arm verbatim. sum_x2 never accumulates: the
            // admitted combinefn (2785) pairs with avg/sum, whose transfns
            // never set calc_sum_x2.
            SinkCombineKind::PolyInt128 => unsafe {
                // SAFETY: non-null internal transvalues are live
                // Int128AggStates (caller contract).
                let dp = &mut *(d.trans_value.as_usize()
                    as *mut ::adt_numeric::aggregates::Int128AggState);
                let sp = &*(s.trans_value.as_usize()
                    as *const ::adt_numeric::aggregates::Int128AggState);
                if sp.n > 0 {
                    dp.n += sp.n;
                    dp.sum_x += sp.sum_x;
                    if dp.calc_sum_x2 {
                        dp.sum_x2 += sp.sum_x2;
                    }
                }
            },
            // int4_avg_combine's core (numeric.c:6832): element adds over
            // the int8[2] {count,sum} transarray.
            SinkCombineKind::AvgInt8 => unsafe {
                // SAFETY: non-null _int8 transvalues are live aggcontext
                // images (caller contract); layout validated per read.
                let (sc, ss) = crate::compact::int8_avg_trans_read(s.trans_value)?;
                let dd = int8_avg_trans_data_mut(d.trans_value)?;
                *dd += sc;
                *dd.add(1) += ss;
            },
        }
    }
    Ok(())
}

/// Mutable {count,sum} pointer into a live, MAXALIGNed int8[2] transarray
/// image (the aggcontext form — sink states never ride tuple-queue-packed
/// short headers). Validation mirrors `int8_avg_trans_read`'s 4B-U arm.
///
/// # Safety
/// `d` is a non-null int8[2] transvalue datum (live aggcontext image),
/// uniquely reachable by the caller.
unsafe fn int8_avg_trans_data_mut(d: Datum) -> PgResult<*mut i64> {
    use ::types_tuple::varatt;
    const ARR_OVERHEAD_NONULLS_1: usize = 24;
    const INT8_TRANSARRAY_SIZE: usize = ARR_OVERHEAD_NONULLS_1 + 16;
    let p = d.as_usize() as *mut u8;
    // SAFETY: caller contract — live varlena image.
    unsafe {
        if !varatt::varatt_is_4b_u(p) || varatt::varsize_4b(p) != INT8_TRANSARRAY_SIZE {
            return Err(sink_shape_error("malformed int8[2] transarray in a sink combine"));
        }
        if p.add(8).cast::<i32>().read() != 0 {
            return Err(sink_shape_error("null-bearing int8[2] transarray in a sink combine"));
        }
        Ok(p.add(ARR_OVERHEAD_NONULLS_1).cast::<i64>())
    }
}

// ---------------------------------------------------------------------------
// The bucket combine.
// ---------------------------------------------------------------------------

/// One Local's remainder face: the worker's compact table + SEAL partition,
/// plus — canonical (text-bearing) shapes only — the armed Multi shape and
/// the Local's intern table, through which remainder rows materialize their
/// canonical bytes at combine (flushed runs copied theirs at flush).
pub struct SinkRemainder<'a> {
    pub table: &'a LaneAggTable,
    pub part: &'a SinkPart,
    pub canon: Option<(&'a MkShape, &'a LaneAggTable)>,
}

/// One Local's combine-visible faces: its flushed runs (flush order) and its
/// remainder table + SEAL partition.
pub struct SinkLocalView<'a> {
    pub runs: &'a [SinkRun],
    pub remainder: Option<SinkRemainder<'a>>,
}

/// Merge bucket `b` across `locals` (slice order = worker-slot order) into a
/// fresh table: runs first (flush order, rows in insertion order), then the
/// remainder rows — the first-seen discipline. NULL blocks are absorbed only
/// in [`SINK_NULL_BUCKET`]. `state_bytes` and `key_words` are the sink's
/// (identical across all sources by construction — one worker plan);
/// `key_words == 0` = CANONICAL BYTES MODE (text-bearing shapes): the bucket
/// table keys on canonical byte strings ([`KeyRepr::Bytes`], length+content
/// compare — embedded NULs are safe).
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
        if let Some(rem) = &l.remainder {
            total += (rem.part.starts[b + 1] - rem.part.starts[b]) as usize;
        }
    }
    let (repr, layout) = match key_words {
        // Bytes keys are Salt8-only (3 key words never inline).
        0 => (KeyRepr::Bytes, EntryLayout::Salt8),
        2 => (KeyRepr::Int128, EntryLayout::Salt8),
        // Inline16: bucket tables are G/256-sized — well inside the band.
        _ => (KeyRepr::Int, EntryLayout::Inline16),
    };
    let mut t = LaneAggTable::with_config(
        repr,
        state_bytes,
        total.max(4),
        HashKind::best(),
        layout,
    );
    let state_words = state_bytes / 8;

    // Shared merge tail: seed a new group's block or combine into the
    // existing one.
    let merge_states = |pr: ::lanetable::Probe, src: *const u64| -> PgResult<()> {
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
        merge_states(pr, src)
    };
    let absorb_bytes = |t: &mut LaneAggTable, key: &[u8], src: *const u64| -> PgResult<()> {
        let pr = t.probe_bytes(key, t.hash_key_bytes(key));
        merge_states(pr, src)
    };

    // Canonical remainder scratch (bytes mode only).
    let mut canon: Vec<u8> = Vec::new();
    for l in locals {
        for r in l.runs {
            debug_assert_eq!(r.key_words, key_words);
            debug_assert_eq!(r.state_words, state_words);
            let lo = r.starts[b] as usize;
            let hi = r.starts[b + 1] as usize;
            for i in lo..hi {
                let src = unsafe {
                    // SAFETY: states holds nrows state blocks (run layout).
                    r.states.as_ptr().add(i * state_words)
                };
                if key_words == 0 {
                    let ks = r.key_offs[i] as usize;
                    let ke = r.key_offs[i + 1] as usize;
                    absorb_bytes(&mut t, &r.key_bytes[ks..ke], src)?;
                } else {
                    let w0 = r.keys[i * key_words];
                    let w1 = if key_words == 2 { r.keys[i * key_words + 1] } else { 0 };
                    absorb(&mut t, Some([w0, w1]), src)?;
                }
            }
            if b == SINK_NULL_BUCKET {
                if let Some(block) = &r.null_states {
                    debug_assert_ne!(key_words, 0, "bytes-mode runs never carry NULL blocks");
                    absorb(&mut t, None, block.as_ptr())?;
                }
            }
        }
        if let Some(rem) = &l.remainder {
            let (rt, part) = (rem.table, rem.part);
            debug_assert_eq!(rt.state_bytes(), t.state_bytes());
            let lo = part.starts[b] as usize;
            let hi = part.starts[b + 1] as usize;
            if key_words == 0 {
                let (shape, intern) = rem
                    .canon
                    .ok_or_else(|| sink_shape_error("bytes-mode remainder without a canon face"))?;
                for &row in &part.idx[lo..hi] {
                    canon_row_bytes(rt, shape, intern, row as usize, &mut canon);
                    absorb_bytes(&mut t, &canon, rt.row_states(row as usize).cast_const().cast())?;
                }
                debug_assert!(!part.has_null, "canonical shapes are non-nullable");
            } else {
                debug_assert_eq!(table_key_words(rt), key_words);
                for &row in &part.idx[lo..hi] {
                    let kw = row_key_words(rt, row as usize)
                        .expect("partition indexes only non-NULL rows");
                    absorb(&mut t, Some(kw), rt.row_states(row as usize).cast_const().cast())?;
                }
                if b == SINK_NULL_BUCKET && part.has_null {
                    // The remainder's NULL row: find it through the table's
                    // own out-of-band accessor path (row scan — one row max).
                    for row in 0..rt.nrows() {
                        if row_key_words(rt, row).is_none() {
                            absorb(&mut t, None, rt.row_states(row).cast_const().cast())?;
                            break;
                        }
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

/// The sink's compact key spec, snapshotted at admission (leader side —
/// the same decide the worker arms run).
#[derive(Clone, Debug)]
pub enum SinkKeySpec {
    Single { width: u8 },
    Reduced(RedShape),
    /// Packed multi-int composite (Mk car): the canonical key words ARE the
    /// packed image, merged across workers verbatim (value-derived — no
    /// per-worker state like intern ids can appear; admission enforces
    /// all-Int non-nullable components).
    Multi(MkShape),
}

impl SinkKeySpec {
    /// The single-word key width (Single/Reduced emit's `Key`/`Derived`
    /// datum width). Multi shapes never emit those columns — their per-
    /// component widths ride [`SinkEmitCol::MultiComp`].
    #[inline]
    pub fn width(&self) -> u8 {
        match self {
            SinkKeySpec::Single { width } => *width,
            SinkKeySpec::Reduced(s) => s.width,
            SinkKeySpec::Multi(_) => 8,
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
    /// One packed multi-key Int component: `width` bytes at byte `off` of
    /// the row's key image, sign-extended (`compact_key_datums_mk`'s Int
    /// arm, exactly). Multi tables have no NULL group row.
    MultiComp { off: u8, width: u8 },
    /// The Intern (text) component of a CANONICAL bytes-keyed table: the
    /// canonical key's tail (after the `plan.fixed` image prefix) is the
    /// raw text payload — materialized as a 4B-header text varlena into the
    /// buf arena (nothing worker-owned crosses to the leader).
    MultiText,
    /// A packed Numeric component (the q19 `extract(minute ...)` key class):
    /// `width` bytes at byte `off` decode through the canonical keypack form
    /// (`mk_numeric_key_decode` → `numeric_key_unpack`) into a NUMERIC image
    /// in the buf arena — byte-identical to the packed first-arrival datum
    /// by the keypack canonicality gates.
    MultiNumeric { off: u8, width: u8 },
    /// Aggregate result = the byval transvalue (no finalfn).
    Agg { transno: u32 },
    /// `avg(int2/int4)` (finalfn `int8_avg` 1964): {count,sum} int8[2]
    /// transarray → `ops::int64_avg_div` NUMERIC image into the buf arena
    /// (`BatchEmitCol::AvgInt8`'s exact core).
    AvgInt8 { transno: u32 },
    /// `avg(int8)` (finalfn `numeric_poly_avg` 3389): Int128AggState →
    /// `aggregates::numeric_poly_avg` image into the buf arena.
    AvgInt128 { transno: u32 },
    /// `sum(int8)` (finalfn `numeric_poly_sum` 3388): Int128AggState →
    /// `aggregates::numeric_poly_sum` image into the buf arena.
    SumInt128 { transno: u32 },
}

pub struct SinkEmitPlan {
    pub width: u8,
    pub cols: Vec<SinkEmitCol>,
    /// CANONICAL (bytes-keyed) shapes: the fixed image prefix length
    /// (`shape.packed_bytes`) — rows split into image prefix + text tail.
    /// `None` = word-keyed tables.
    pub fixed: Option<u8>,
}

/// The emit qualification (leader side, donor `build_emit_plan` extended
/// with Reduced derived keys, Multi components, and the finalize-at-emit
/// numeric-avg vocabulary — `batch_emit_resolve`'s exact finalfn gates).
/// `None` = the shape needs the general finalize/project interpreter — the
/// sink refuses then (the HAVING/non-identity car).
pub fn sink_build_emit_plan(
    node: &AggStateData<'_>,
    key: &SinkKeySpec,
) -> Option<SinkEmitPlan> {
    if node.skip_final || node.qual.is_some() {
        return None;
    }
    for pa in node.peragg.iter() {
        if !pa.direct_args.is_empty() {
            return None;
        }
        match pa.finalfn.as_ref() {
            // Raw-transvalue emission requires a byval word; INTERNAL is
            // byval-but-pointer — refuse (batch_emit_resolve's gate).
            None => {
                if !node.trans_typ[pa.transno as usize].byval
                    || pa.aggref.aggtranstype == INTERNALOID
                {
                    return None;
                }
            }
            // The batched finalize vocabulary: byte-identical native cores.
            Some(f) => match (f.fn_oid, pa.aggref.aggtranstype) {
                (FINALFN_INT8_AVG, t) if t == INT8ARRAYOID => {}
                (FINALFN_POLY_AVG | FINALFN_POLY_SUM, t) if t == INTERNALOID => {}
                _ => return None,
            },
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
                SinkKeySpec::Multi(shape) => {
                    // Int components decode from the key image; Intern (C2
                    // car) emits the canonical tail as text; Numeric (q19
                    // minute() class) decodes through keypack. Nullable
                    // images stay heap-source-only — refused fail-closed.
                    let comp = shape.comps.get(j)?;
                    if shape.nullable {
                        return None;
                    }
                    match comp.kind {
                        MkCompKind::Int { width } => {
                            cols.push(SinkEmitCol::MultiComp { off: comp.off, width });
                        }
                        MkCompKind::Intern => cols.push(SinkEmitCol::MultiText),
                        MkCompKind::Numeric { width } => {
                            cols.push(SinkEmitCol::MultiNumeric { off: comp.off, width });
                        }
                    }
                }
            }
            continue;
        }
        if let Some(a) = te.expr.as_aggref() {
            if a.aggno < 0 || a.aggno as usize >= node.peragg.len() {
                return None;
            }
            let pa = &node.peragg[a.aggno as usize];
            let col = match pa.finalfn.as_ref() {
                None => SinkEmitCol::Agg { transno: pa.transno },
                Some(f) => match f.fn_oid {
                    FINALFN_INT8_AVG => SinkEmitCol::AvgInt8 { transno: pa.transno },
                    FINALFN_POLY_AVG => SinkEmitCol::AvgInt128 { transno: pa.transno },
                    FINALFN_POLY_SUM => SinkEmitCol::SumInt128 { transno: pa.transno },
                    _ => return None,
                },
            };
            cols.push(col);
            continue;
        }
        return None;
    }
    let fixed = match key {
        SinkKeySpec::Multi(shape) if shape.intern_comp().is_some() => Some(shape.packed_bytes),
        _ => None,
    };
    Some(SinkEmitPlan { width: key.width(), cols, fixed })
}

/// One bucket's fully-projected output rows: row-major, stride `cols.len()`.
/// Datums are byval OR point into the buf's OWN `arena` (finalized NUMERIC
/// images, 8-aligned) — self-contained across threads and past the helpers'
/// teardown either way. Moving the struct never moves the arena's heap
/// buffer; the arena is never resized after the emit's fix-up pass.
#[derive(Default)]
pub struct SinkEmitBuf {
    pub values: Vec<Datum>,
    pub nulls: Vec<bool>,
    pub nrows: usize,
    /// Byref payload arena (finalized varlena images the values point into).
    pub arena: Vec<u8>,
}

impl SinkEmitBuf {
    pub fn bytes(&self) -> usize {
        self.values.capacity() * core::mem::size_of::<Datum>()
            + self.nulls.capacity()
            + self.arena.capacity()
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
/// first-seen order) into a [`SinkEmitBuf`]. Byref outputs (the numeric-avg
/// finalize vocabulary) materialize into the buf's own arena: images land in
/// `arena` during the row loop and the datums are fixed up to point into it
/// once the arena's length is final — nothing worker-owned survives in the
/// published buf.
pub fn sink_emit_bucket(plan: &SinkEmitPlan, t: &LaneAggTable) -> PgResult<SinkEmitBuf> {
    let natts = plan.cols.len();
    let n = t.nrows();
    let mut values: Vec<Datum> = Vec::with_capacity(n * natts);
    let mut nulls: Vec<bool> = Vec::with_capacity(n * natts);
    let mut arena: Vec<u8> = Vec::new();
    // (values index, arena offset) fix-ups, resolved after the arena stops
    // growing (Vec growth may move the heap buffer).
    let mut fixups: Vec<(usize, usize)> = Vec::new();
    let push_image = |values: &mut Vec<Datum>,
                          nulls: &mut Vec<bool>,
                          arena: &mut Vec<u8>,
                          fixups: &mut Vec<(usize, usize)>,
                          head: &[u8],
                          body: &[u8]| {
        // 8-align every image (varlena consumers may read 4-byte headers +
        // aligned payloads).
        let pad = (8 - arena.len() % 8) % 8;
        arena.resize(arena.len() + pad, 0);
        let off = arena.len();
        arena.extend_from_slice(head);
        arena.extend_from_slice(body);
        fixups.push((values.len(), off));
        values.push(Datum::null());
        nulls.push(false);
    };
    let mut scratch8 = [0u8; 8];
    for row in 0..n {
        // Single/Reduced tables: kw[0] IS the canonical i64 key (Int repr);
        // Multi tables: kw is the packed key image (1 or 2 words). None =
        // the out-of-band NULL group (single-word shapes only — Multi
        // tables never probe it). CANONICAL (bytes-keyed) tables split the
        // key into the image prefix (reconstructed words) + the text tail.
        let (kw, tail): (Option<[u64; 2]>, Option<&[u8]>) = if t.repr() == KeyRepr::Bytes {
            let fixed =
                plan.fixed.ok_or_else(|| {
                    sink_shape_error("bytes-keyed emit without a canonical prefix")
                })? as usize;
            let cb = t.row_key_bytes(row, &mut scratch8).ok_or_else(|| {
                sink_shape_error("NULL group row in a canonical bucket table")
            })?;
            if cb.len() < fixed || fixed > 16 {
                return Err(sink_shape_error("canonical key shorter than its image prefix"));
            }
            let mut flat = [0u8; 16];
            flat[..fixed].copy_from_slice(&cb[..fixed]);
            let w0 = u64::from_le_bytes(flat[..8].try_into().expect("8-byte prefix"));
            let w1 = u64::from_le_bytes(flat[8..].try_into().expect("8-byte suffix"));
            (Some([w0, w1]), Some(&cb[fixed..]))
        } else {
            (row_key_words(t, row), None)
        };
        let key = kw.map(|w| w[0] as i64);
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
                SinkEmitCol::MultiComp { off, width } => match kw {
                    // compact_key_datums_mk's Int arm: width bytes at off,
                    // sign-extended, datum at the component's width.
                    Some(w) => {
                        let image = (w[0] as u128) | ((w[1] as u128) << 64);
                        let bits = (image >> (off as u32 * 8)) as u64;
                        let sh = 64 - width as u32 * 8;
                        let v =
                            if sh == 0 { bits as i64 } else { ((bits << sh) as i64) >> sh };
                        values.push(key_datum(width, v));
                        nulls.push(false);
                    }
                    None => {
                        // Unreachable for Multi tables (no NULL group row);
                        // fail-soft as SQL NULL rather than asserting.
                        values.push(Datum::null());
                        nulls.push(true);
                    }
                },
                // The canonical text tail as a 4B-header text varlena in the
                // buf's own arena (equal payload bytes = the serial path's
                // text value; header form is representation, not identity).
                SinkEmitCol::MultiText => {
                    let tail = tail.ok_or_else(|| {
                        sink_shape_error("MultiText emit on a word-keyed table")
                    })?;
                    let head =
                        ::datum::varlena::set_varsize_4b(tail.len() + ::datum::varlena::VARHDRSZ);
                    push_image(&mut values, &mut nulls, &mut arena, &mut fixups, &head, tail);
                }
                // Packed numeric key bits → canonical keypack decode →
                // NUMERIC image (byte-identical to the packed first-arrival
                // datum by the keypack canonicality gates).
                SinkEmitCol::MultiNumeric { off, width } => {
                    let w = kw.ok_or_else(|| {
                        sink_shape_error("MultiNumeric emit on a NULL group row")
                    })?;
                    let image = (w[0] as u128) | ((w[1] as u128) << 64);
                    let bits = (image >> (off as u32 * 8)) as u64;
                    let wbits = width as u32 * 8;
                    let masked =
                        if wbits == 64 { bits } else { bits & ((1u64 << wbits) - 1) };
                    let img = ::adt_numeric::numeric_key_unpack(
                        crate::compact::mk_numeric_key_decode(masked, width),
                    )?;
                    push_image(
                        &mut values, &mut nulls, &mut arena, &mut fixups,
                        img.as_bytes(), &[],
                    );
                }
                // SAFETY: the row's state block holds numtrans pergroups
                // (bucket-table config = the sink's state_bytes); transno <
                // numtrans by plan construction. Byval transvalues only.
                SinkEmitCol::Agg { transno } => unsafe {
                    let pg = &*states.add(transno as usize);
                    values.push(pg.trans_value);
                    nulls.push(pg.trans_value_is_null);
                },
                // fc_int8_avg's exact core: strict (NULL trans → NULL),
                // count == 0 → NULL, else the int64_avg_div image.
                // SAFETY: non-null _int8 transvalue is a live merged image
                // (combine contract).
                SinkEmitCol::AvgInt8 { transno } => unsafe {
                    let pg = &*states.add(transno as usize);
                    if pg.trans_value_is_null {
                        values.push(Datum::null());
                        nulls.push(true);
                    } else {
                        let (count, sum) =
                            crate::compact::int8_avg_trans_read(pg.trans_value)?;
                        if count == 0 {
                            values.push(Datum::null());
                            nulls.push(true);
                        } else {
                            let img = ::adt_numeric::ops::int64_avg_div(sum, count)?;
                            push_image(
                                &mut values, &mut nulls, &mut arena, &mut fixups,
                                img.as_bytes(), &[],
                            );
                        }
                    }
                },
                // numeric_poly_avg / numeric_poly_sum's exact cores over the
                // merged Int128AggState (NULL trans → None → NULL).
                // SAFETY: as AvgInt8 — live merged state, sole reader.
                SinkEmitCol::AvgInt128 { transno } | SinkEmitCol::SumInt128 { transno } => unsafe {
                    let pg = &*states.add(transno as usize);
                    let state = (!pg.trans_value_is_null).then(|| {
                        &*(pg.trans_value.as_usize()
                            as *const ::adt_numeric::aggregates::Int128AggState)
                    });
                    let img = match *c {
                        SinkEmitCol::AvgInt128 { .. } => {
                            ::adt_numeric::aggregates::numeric_poly_avg(state)?
                        }
                        _ => ::adt_numeric::aggregates::numeric_poly_sum(state)?,
                    };
                    match img {
                        Some(img) => push_image(
                            &mut values, &mut nulls, &mut arena, &mut fixups,
                            img.as_bytes(), &[],
                        ),
                        None => {
                            values.push(Datum::null());
                            nulls.push(true);
                        }
                    }
                },
            }
        }
    }
    // Arena is final — resolve the byref datums.
    for (i, off) in fixups {
        values[i] = Datum::from_usize(arena[off..].as_ptr() as usize);
    }
    Ok(SinkEmitBuf { values, nulls, nrows: n, arena })
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

/// Disarm SINK MODE (leader-side cap-aware admission probes): the leader's
/// own executor may still run the SERIAL build (engagement refusal / budget
/// fallback / rescan), which must never see sink mode — under a live cap the
/// compact backstop fails closed instead of migrating.
pub fn agg_sink_clear_cap(node: &mut AggStateData<'_>) {
    if let Some(ph) = node.perhash.as_mut() {
        ph.sink_cap = None;
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
/// `None` = not armed or a shape the sink refuses: nullable Multi images
/// (heap sources), or an intern table on a single-word spec (structurally
/// impossible; belt). Intern (text) components ARE admitted — the C2 car
/// merges them on canonical raw bytes; Numeric components are demote-safe
/// (a mid-build pack failure maps to the budget-refusal rerun).
pub fn agg_sink_key_spec(node: &AggStateData<'_>) -> Option<SinkKeySpec> {
    let ch = node.perhash.as_ref()?.compact.as_ref()?;
    match &ch.key {
        crate::compact::CompactKeySpec::Single { width } => {
            if ch.intern.is_some() {
                return None;
            }
            Some(SinkKeySpec::Single { width: *width })
        }
        crate::compact::CompactKeySpec::Reduced(shape) => {
            if ch.intern.is_some() {
                return None;
            }
            Some(SinkKeySpec::Reduced(shape.clone()))
        }
        crate::compact::CompactKeySpec::Multi(shape) => {
            if shape.nullable {
                return None;
            }
            // Exactly one Intern component (the canonical tail decodes
            // unambiguously only then); intern table presence must match.
            let n_intern =
                shape.comps.iter().filter(|c| c.kind == MkCompKind::Intern).count();
            if n_intern > 1 || (n_intern == 1) != ch.intern.is_some() {
                return None;
            }
            Some(SinkKeySpec::Multi(shape.clone()))
        }
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

    /// SEAL-time bucket index over this handle's remainder — canonical
    /// (text-bearing) shapes partition by their canonical bytes, word shapes
    /// by the key words ([`sink_partition_remainder`]).
    pub fn partition_remainder(&self) -> SinkPart {
        if compact_canon_shape(&self.0).is_some() {
            sink_partition_remainder_canon(&self.0)
        } else {
            sink_partition_remainder(&self.0.table)
        }
    }

    /// This handle's retained footprint (compact + intern tables) — the
    /// SEAL-time budget accounting twin of [`agg_sink_table_mem`].
    pub fn mem_used(&self) -> usize {
        self.0.table.mem_used()
            + self.0.intern.as_ref().map_or(0, ::lanetable::LaneAggTable::mem_used)
    }

    /// The combine-visible remainder face over this handle (+ the canonical
    /// shape/intern refs when the shape is text-bearing).
    pub fn remainder_view<'a>(&'a self, part: &'a SinkPart) -> SinkRemainder<'a> {
        let canon = compact_canon_shape(&self.0).map(|shape| {
            let intern = self
                .0
                .intern
                .as_ref()
                .expect("canonical shapes carry the intern table");
            (shape, intern)
        });
        SinkRemainder { table: &self.0.table, part, canon }
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
/// Canonical (text-bearing) shapes flush through the canonical-bytes twin —
/// key bytes copied out, intern table kept (scan-lifetime).
pub fn agg_sink_flush_if_due(node: &mut AggStateData<'_>, cap: u32) -> Option<SinkRun> {
    let ch = node.perhash.as_mut()?.compact.as_mut()?;
    if ch.table.len() < cap as usize {
        return None;
    }
    if compact_canon_shape(ch).is_some() {
        Some(sink_flush_table_canon(ch))
    } else {
        Some(sink_flush_table(&mut ch.table))
    }
}

/// The armed table's current footprint (budget accounting) — the intern
/// table (text-bearing shapes) is retained per-Local state and counts too
/// (the backstop's own mem formula includes it).
pub fn agg_sink_table_mem(node: &AggStateData<'_>) -> usize {
    node.perhash
        .as_ref()
        .and_then(|ph| ph.compact.as_ref())
        .map_or(0, |ch| {
            ch.table.mem_used()
                + ch.intern.as_ref().map_or(0, ::lanetable::LaneAggTable::mem_used)
        })
}

/// The node's aggcontext footprint — the byref state classes (PolyInt128 /
/// AvgInt8) live THERE, not in the table rows, so byref-bearing sink drains
/// add this to their budget accounting (the backstop's own mem formula).
pub fn agg_sink_aggctx_mem(node: &AggStateData<'_>) -> usize {
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    unsafe { node.agg_node.as_ref() }.aggcontext().context().subtree_used()
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
        // KEEP the drained state (its bufs' arenas back byref datums already
        // handed out this scan — C's aggcontext lifetime analog); it drops
        // at rescan/teardown through agg_sink_reset_emit.
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
            SinkCombineFn { func: add, strict: true, collation: Oid::from(0u8), kind: SinkCombineKind::Byval },
            SinkCombineFn { func: larger, strict: true, collation: Oid::from(0u8), kind: SinkCombineKind::Byval },
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
            SinkLocalView { runs: core::slice::from_ref(&run1), remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None }) },
            SinkLocalView { runs: &[], remainder: Some(SinkRemainder { table: &t2, part: &part2, canon: None }) },
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
            SinkLocalView { runs: core::slice::from_ref(&run1), remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None }) },
            SinkLocalView { runs: &[], remainder: Some(SinkRemainder { table: &t2, part: &part2, canon: None }) },
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
            fixed: None,
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
        let buf = sink_emit_bucket(&plan, &t).unwrap();
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

    // A minimal MAXALIGNed int8[2] {count,sum} transarray image (4B-U,
    // 24-byte overhead, no null bitmap) — the aggcontext form.
    #[repr(C, align(8))]
    struct Int8TransArray {
        hdr: [u8; 24],
        data: [i64; 2],
    }

    fn mk_transarray(count: i64, sum: i64) -> Box<Int8TransArray> {
        let mut a = Box::new(Int8TransArray { hdr: [0; 24], data: [count, sum] });
        let size: u32 = 40u32 << 2; // varatt 4B-U header: len << 2
        a.hdr[0..4].copy_from_slice(&size.to_le_bytes());
        a.hdr[4..8].copy_from_slice(&1i32.to_le_bytes()); // ndim
        a.hdr[8..12].copy_from_slice(&0i32.to_le_bytes()); // dataoffset (no nulls)
        a
    }

    #[test]
    fn byref_combine_and_finalize_emit() {
        use ::adt_numeric::aggregates::Int128AggState;
        // Transno 0: PolyInt128 (avg(int8)); transno 1: AvgInt8 (avg(int4)).
        let combines = vec![
            SinkCombineFn {
                func: test_combines()[0].func,
                strict: false,
                collation: Oid::from(0u8),
                kind: SinkCombineKind::PolyInt128,
            },
            SinkCombineFn {
                func: test_combines()[0].func,
                strict: true,
                collation: Oid::from(0u8),
                kind: SinkCombineKind::AvgInt8,
            },
        ];
        assert!(sink_combines_byref(&combines));

        let mut d_poly = Int128AggState { calc_sum_x2: false, n: 3, sum_x: 30, sum_x2: 0 };
        let s_poly = Int128AggState { calc_sum_x2: false, n: 2, sum_x: 12, sum_x2: 0 };
        let d_arr = mk_transarray(4, 100);
        let s_arr = mk_transarray(6, 44);

        let mut dst = [
            AggPerGroup {
                trans_value: Datum::from_usize(&mut d_poly as *mut _ as usize),
                trans_value_is_null: false,
                no_trans_value: false,
            },
            AggPerGroup {
                trans_value: Datum::from_usize(&*d_arr as *const _ as usize),
                trans_value_is_null: false,
                no_trans_value: false,
            },
        ];
        let src = [
            AggPerGroup {
                trans_value: Datum::from_usize(&s_poly as *const _ as usize),
                trans_value_is_null: false,
                no_trans_value: false,
            },
            AggPerGroup {
                trans_value: Datum::from_usize(&*s_arr as *const _ as usize),
                trans_value_is_null: false,
                no_trans_value: false,
            },
        ];
        unsafe {
            sink_combine_states(&combines, dst.as_mut_ptr(), src.as_ptr()).unwrap();
        }
        assert_eq!(d_poly.n, 5);
        assert_eq!(d_poly.sum_x, 42);
        assert_eq!(d_arr.data, [10, 144]);

        // NULL dst adopts the src pointer (both byref kinds).
        let mut dst2 = [
            AggPerGroup {
                trans_value: Datum::null(),
                trans_value_is_null: true,
                no_trans_value: true,
            },
            AggPerGroup {
                trans_value: Datum::null(),
                trans_value_is_null: true,
                no_trans_value: true,
            },
        ];
        unsafe {
            sink_combine_states(&combines, dst2.as_mut_ptr(), src.as_ptr()).unwrap();
        }
        assert_eq!(dst2[0].trans_value.as_usize(), &s_poly as *const _ as usize);
        assert!(!dst2[0].trans_value_is_null);

        // Finalize-at-emit: one row whose 2 pergroups are the merged states;
        // outputs must be the finalfn cores' exact images, self-contained in
        // the buf arena.
        let mut t = mk_table(4);
        let pr = t.probe_int(7, t.hash_key_int(7));
        unsafe {
            core::ptr::copy_nonoverlapping(dst.as_ptr(), pr.states.cast::<AggPerGroup>(), 2);
        }
        let plan = SinkEmitPlan {
            width: 8,
            fixed: None,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::AvgInt128 { transno: 0 },
                SinkEmitCol::AvgInt8 { transno: 1 },
            ],
        };
        let buf = sink_emit_bucket(&plan, &t).unwrap();
        assert_eq!(buf.nrows, 1);
        assert_eq!(buf.values[0].as_i64(), 7);
        let expect_poly =
            ::adt_numeric::aggregates::numeric_poly_avg(Some(&d_poly)).unwrap().unwrap();
        let expect_arr = ::adt_numeric::ops::int64_avg_div(144, 10).unwrap();
        for (v, expect) in
            [(buf.values[1], expect_poly.as_bytes()), (buf.values[2], expect_arr.as_bytes())]
        {
            let p = v.as_usize();
            // The datum points into the buf's OWN arena.
            let lo = buf.arena.as_ptr() as usize;
            assert!(p >= lo && p + expect.len() <= lo + buf.arena.len());
            let got = unsafe { core::slice::from_raw_parts(p as *const u8, expect.len()) };
            assert_eq!(got, expect);
        }
        assert!(!buf.nulls[1] && !buf.nulls[2]);
    }

    #[test]
    fn emit_bucket_multi_components() {
        // One-word packed image: int4 at off 0, int2 at off 4 (q42 class).
        let mut t = mk_table(8);
        let img: u64 = ((-7i32 as u32) as u64) | ((300u16 as u64) << 32);
        bump(&mut t, Some(img as i64), 5, 9);
        let plan = SinkEmitPlan {
            width: 8,
            fixed: None,
            cols: vec![
                SinkEmitCol::MultiComp { off: 0, width: 4 },
                SinkEmitCol::MultiComp { off: 4, width: 2 },
                SinkEmitCol::Agg { transno: 0 },
            ],
        };
        let buf = sink_emit_bucket(&plan, &t).unwrap();
        assert_eq!(buf.nrows, 1);
        assert_eq!(buf.values[0].as_i32(), -7);
        assert_eq!(buf.values[1].as_i16(), 300);
        assert_eq!(buf.values[2].as_i64(), 5);
        assert!(!buf.nulls[0] && !buf.nulls[1] && !buf.nulls[2]);

        // Two-word packed image: int8 at off 0, int4 at off 8 (q41 class —
        // the component at off 8 lives entirely in the high key word).
        let mut t2 = LaneAggTable::with_config(
            KeyRepr::Int128,
            STATE_BYTES,
            8,
            HashKind::best(),
            EntryLayout::Salt8,
        );
        let w0 = (-123456789i64) as u64;
        let w1 = (54321u32 as u64) & 0xFFFF_FFFF;
        let pr = t2.probe_i128([w0, w1], t2.hash_key_i128([w0, w1]));
        let pg = pr.states.cast::<AggPerGroup>();
        unsafe {
            pg.write(AggPerGroup {
                trans_value: Datum::from_i64(2),
                trans_value_is_null: false,
                no_trans_value: false,
            });
            pg.add(1).write(AggPerGroup {
                trans_value: Datum::from_i64(0),
                trans_value_is_null: false,
                no_trans_value: false,
            });
        }
        let plan2 = SinkEmitPlan {
            width: 8,
            fixed: None,
            cols: vec![
                SinkEmitCol::MultiComp { off: 0, width: 8 },
                SinkEmitCol::MultiComp { off: 8, width: 4 },
                SinkEmitCol::Agg { transno: 0 },
            ],
        };
        let buf2 = sink_emit_bucket(&plan2, &t2).unwrap();
        assert_eq!(buf2.nrows, 1);
        assert_eq!(buf2.values[0].as_i64(), -123456789);
        assert_eq!(buf2.values[1].as_i32(), 54321);
        assert_eq!(buf2.values[2].as_i64(), 2);
    }

    // -- Canonical (text-bearing) shapes — the C2 car ------------------------

    /// int8 + text shape: Int{8} at off 0, Intern at off 8 (12-byte image,
    /// two words) — the q17/q18 `UserID, SearchPhrase` class.
    fn canon_shape_int8_text() -> MkShape {
        MkShape {
            comps: vec![
                crate::compact::MkComp { att: 0, off: 0, kind: MkCompKind::Int { width: 8 } },
                crate::compact::MkComp { att: 1, off: 8, kind: MkCompKind::Intern },
            ],
            packed_bytes: 12,
            nullable: false,
            two_words: true,
        }
    }

    /// A worker-shaped compact state for the canonical tests: the mk table
    /// (Int128 for the 12-byte image) + the intern table, wrapped the way
    /// `agg_hash_compact_try_arm_mk` builds them.
    fn canon_worker(shape: MkShape) -> crate::compact::CompactHash {
        let (repr, layout) = if shape.two_words {
            (KeyRepr::Int128, EntryLayout::Salt8)
        } else {
            (KeyRepr::Int, EntryLayout::Inline16)
        };
        let table = LaneAggTable::with_config(repr, STATE_BYTES, 16, HashKind::best(), layout);
        let intern = LaneAggTable::new(KeyRepr::Bytes, 8, 16);
        crate::compact::compact_hash_for_tests(
            table,
            crate::compact::CompactKeySpec::Multi(shape),
            Some(intern),
        )
    }

    /// The feed's intern + pack + probe sequence for one row —
    /// `scan_mk_batch`'s Intern arm in miniature. `k = None` = a 1-comp
    /// single-text shape (image = the id word alone).
    fn bump_canon(ch: &mut crate::compact::CompactHash, k: Option<i64>, text: &[u8], count: i64) {
        let t = ch.intern.as_mut().unwrap();
        let hash = t.hash_key_bytes(text);
        let pr = t.probe_bytes(text, hash);
        let id = if pr.is_new {
            let id = (t.nrows() - 1) as u32;
            // SAFETY: fresh zeroed 8-byte state block (intern contract).
            unsafe { pr.states.cast::<u32>().write(id) };
            id
        } else {
            // SAFETY: live state block written at insert.
            unsafe { pr.states.cast::<u32>().read() }
        };
        let pr = match k {
            Some(k) => {
                let image = ((k as u64) as u128) | ((id as u128) << 64);
                let kw = [image as u64, (image >> 64) as u64];
                ch.table.probe_i128(kw, ch.table.hash_key_i128(kw))
            }
            None => {
                let kw = id as i64;
                ch.table.probe_int(kw, ch.table.hash_key_int(kw as u64))
            }
        };
        let pg = pr.states.cast::<AggPerGroup>();
        // SAFETY: STATE_BYTES holds two AggPerGroup slots, zeroed at birth.
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
            c.trans_value = Datum::from_i64(c.trans_value.as_i64() + count);
        }
    }

    fn emit_text(buf: &SinkEmitBuf, v: Datum) -> Vec<u8> {
        let p = v.as_usize();
        let lo = buf.arena.as_ptr() as usize;
        assert!(p >= lo && p < lo + buf.arena.len(), "text datum points into the buf arena");
        // SAFETY: the emit wrote a 4B-header varlena at p.
        unsafe { ::datum::VarlenaRef::from_ptr(p as *const u8) }.data().to_vec()
    }

    #[test]
    fn canonical_flush_combine_emit_roundtrip() {
        // Worker 1 interns apple(0) banana(1); worker 2 interns zzz(0)
        // banana(1) apple(2) — DIFFERENT per-worker ids for the same text,
        // the exact hazard canonical bytes exist to erase.
        let mut w1 = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w1, Some(1), b"apple", 1);
        bump_canon(&mut w1, Some(1), b"banana", 2);
        bump_canon(&mut w1, Some(2), b"apple", 3);
        let run1 = sink_flush_table_canon(&mut w1);
        assert_eq!(run1.key_words, 0);
        assert_eq!(run1.nrows(), 3);
        assert!(run1.null_states.is_none());
        assert_eq!(w1.table.nrows(), 0, "flush resets the mk table");
        assert_eq!(w1.intern.as_ref().unwrap().nrows(), 2, "intern survives the flush");
        // Remainder after the flush: apple's intern id is REUSED (same id,
        // same canonical bytes) + a new text.
        bump_canon(&mut w1, Some(1), b"apple", 10);
        bump_canon(&mut w1, Some(3), b"cherry", 5);
        let h1 = SinkTableHandle(w1);
        let part1 = h1.partition_remainder();
        assert!(!part1.has_null);

        let mut w2 = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w2, Some(9), b"zzz", 7);
        bump_canon(&mut w2, Some(1), b"banana", 20);
        bump_canon(&mut w2, Some(1), b"apple", 30);
        let h2 = SinkTableHandle(w2);
        let part2 = h2.partition_remainder();

        let locals = [
            SinkLocalView {
                runs: core::slice::from_ref(&run1),
                remainder: Some(h1.remainder_view(&part1)),
            },
            SinkLocalView { runs: &[], remainder: Some(h2.remainder_view(&part2)) },
        ];
        let combines = test_combines();
        let plan = SinkEmitPlan {
            width: 8,
            fixed: Some(12),
            cols: vec![
                SinkEmitCol::MultiComp { off: 0, width: 8 },
                SinkEmitCol::MultiText,
                SinkEmitCol::Agg { transno: 0 },
            ],
        };
        let mut seen: std::collections::HashMap<(i64, Vec<u8>), i64> =
            std::collections::HashMap::new();
        for b in 0..SINK_NBUCKETS {
            let t = sink_combine_bucket(b, 0, STATE_BYTES, &locals, &combines).unwrap();
            assert_eq!(t.repr(), KeyRepr::Bytes);
            let buf = sink_emit_bucket(&plan, &t).unwrap();
            for row in 0..buf.nrows {
                let k = buf.values[row * 3].as_i64();
                let text = emit_text(&buf, buf.values[row * 3 + 1]);
                let c = buf.values[row * 3 + 2].as_i64();
                assert!(
                    seen.insert((k, text.clone()), c).is_none(),
                    "group ({k}, {text:?}) in two buckets"
                );
            }
        }
        assert_eq!(seen.len(), 5);
        assert_eq!(seen[&(1, b"apple".to_vec())], 41, "1 + 10 + 30 across run/remainders");
        assert_eq!(seen[&(1, b"banana".to_vec())], 22);
        assert_eq!(seen[&(2, b"apple".to_vec())], 3);
        assert_eq!(seen[&(3, b"cherry".to_vec())], 5);
        assert_eq!(seen[&(9, b"zzz".to_vec())], 7);
    }

    #[test]
    fn canonical_single_text_short_and_long_keys() {
        // 1-comp Intern shape (4-byte image, one word — the q13/q34 single
        // text class). Canonical keys span probe_bytes' packed8 arm
        // (len <= 8: empty + short texts) AND the arena arm (long text).
        let shape = MkShape {
            comps: vec![crate::compact::MkComp {
                att: 0,
                off: 0,
                kind: MkCompKind::Intern,
            }],
            packed_bytes: 4,
            nullable: false,
            two_words: false,
        };
        let mut w = canon_worker(shape);
        let texts: [&[u8]; 4] = [b"", b"a", b"abcd", b"abcdefghijklmnop"];
        for (i, t) in texts.iter().enumerate() {
            bump_canon(&mut w, None, t, (i + 1) as i64);
        }
        let run = sink_flush_table_canon(&mut w);
        assert_eq!(run.nrows(), 4);
        // Second epoch re-inserts two of them (ids reused from intern).
        bump_canon(&mut w, None, b"a", 100);
        bump_canon(&mut w, None, b"abcdefghijklmnop", 200);
        let h = SinkTableHandle(w);
        let part = h.partition_remainder();
        let locals = [SinkLocalView {
            runs: core::slice::from_ref(&run),
            remainder: Some(h.remainder_view(&part)),
        }];
        let combines = test_combines();
        let plan = SinkEmitPlan {
            width: 8,
            fixed: Some(4),
            cols: vec![SinkEmitCol::MultiText, SinkEmitCol::Agg { transno: 0 }],
        };
        let mut seen: std::collections::HashMap<Vec<u8>, i64> = std::collections::HashMap::new();
        for b in 0..SINK_NBUCKETS {
            let t = sink_combine_bucket(b, 0, STATE_BYTES, &locals, &combines).unwrap();
            let buf = sink_emit_bucket(&plan, &t).unwrap();
            for row in 0..buf.nrows {
                let text = emit_text(&buf, buf.values[row * 2]);
                let c = buf.values[row * 2 + 1].as_i64();
                // Bucket routing: the canonical bytes' own hash.
                let mut canon = vec![0u8; 4];
                canon.extend_from_slice(&text);
                assert_eq!(b, bucket_of(sink_hash_bytes(&canon)), "bucket law for {text:?}");
                assert!(seen.insert(text, c).is_none());
            }
        }
        assert_eq!(seen.len(), 4);
        assert_eq!(seen[&b"".to_vec()], 1);
        assert_eq!(seen[&b"a".to_vec()], 102);
        assert_eq!(seen[&b"abcd".to_vec()], 3);
        assert_eq!(seen[&b"abcdefghijklmnop".to_vec()], 204);
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
