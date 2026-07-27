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
pub use ::lanefold::StrStateArena;
use ::lanetable::{EntryLayout, HashKind, KeyRepr};
// Re-exported for the runtime combine-split's leaf emit (execmain names the
// fragment table type without a direct lanetable dependency).
pub use ::lanetable::LaneAggTable;
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

/// `PGRUST_RUNTIME_AGG_AVGPACK` kill switch (default ON): the avgpack lane —
/// AvgInt8 (avg(int2/int4)) states packed INLINE in the sink table's state
/// words (`[count: i64, sum: i64]` in the transno's 16-byte `AggPerGroup`
/// slot) instead of a per-group 40-byte transarray in the worker
/// aggcontext. Kills the unspillable byref-floor refusal class (the
/// proportionality-audit high-cardinality @dop2 172s verdict): flush/spill/combine copy
/// state words verbatim, so with nothing pointer-shaped left the spill law
/// drains ALL pressure. Off restores the aggcontext representation
/// everywhere, bit-exactly.
pub fn sink_avgpack_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_AVGPACK").as_deref(), Ok("0") | Ok("off"))
    })
}

/// The node's avgpack shape mask: bit per transno of the AvgInt8 class
/// (`_int8` transarray transtype — on an admitted sink engagement that is
/// exactly the `int4_avg_combine` family). 0 when the kill switch is off or
/// any such transno is >= 64 (mask capacity — packing is then refused
/// WHOLESALE so the leader's combine/emit resolution and the worker's table
/// arm can never disagree per-transno). Computed once at node build
/// (`AggStateData::avgpack_shape_mask`); worker sink builds adopt it as the
/// compact table's packed-representation mask at TABLE CREATION.
pub(crate) fn sink_avgpack_shape_mask(peragg: &[crate::PerAggData<'_>]) -> u64 {
    if !sink_avgpack_enabled() {
        return 0;
    }
    let mut mask = 0u64;
    for pa in peragg {
        if pa.aggref.aggtranstype == INT8ARRAYOID {
            if pa.transno >= 64 {
                return 0;
            }
            mask |= 1u64 << pa.transno;
        }
    }
    mask
}

/// Whether `transno` is packed under an avgpack mask.
#[inline(always)]
pub(crate) fn avgpack_of(mask: u64, transno: u32) -> bool {
    transno < 64 && (mask >> transno) & 1 == 1
}

/// avgpack: read a packed slot's `[count, sum]` words.
///
/// # Safety
/// `states` is a live state block of numtrans 16-byte slots and `transno`
/// is packed under the engagement's mask (so the slot holds the inline
/// image, not an `AggPerGroup`).
#[inline(always)]
unsafe fn avgpack_read_slot(states: *const AggPerGroup, transno: usize) -> (i64, i64) {
    // SAFETY: caller contract — 16-byte 8-aligned slot holding two i64s.
    unsafe {
        let w = states.add(transno).cast::<i64>();
        (*w, *w.add(1))
    }
}

/// `PGRUST_RUNTIME_AGG_SPILL_CANON` kill switch (default ON): the canonical
/// bytes spill record (canon-sink-increments car 3). Off, canonical
/// (text-bearing) engagements restore the train-13 composition gate exactly
/// — no spill arm, budget crossings refuse to the serial rerun. ONE source
/// of truth for both the leader's engagement mirror and the worker arms'
/// `mk_admit_n` estimate gate (the F1 leader/worker-verdict invariant).
pub fn sink_spill_canon_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_RUNTIME_AGG_SPILL_CANON").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `PGRUST_RUNTIME_AGG_GIDMERGE=1` opt-in (default OFF): the combine-side
/// GID merge (canon-sink-increments car 2 — per-worker packed-word group ids
/// short-circuit the canonical bytes probe for repeat arrivals).
/// MEASURED NO-SHIP at 100M (2026-07-14 A/B, four text-family shapes,
/// rta16, jobs -2b22 on / -064e off): ON is +10/+10/+19/+32% hot — the
/// near-unique text classes re-arrive too rarely for map hits to pay for
/// the per-claim map allocation + the flush-side word fill. The mechanism
/// stays as the evidence channel; the chartered follow-up is the
/// text-kernels catalog design (runs carry first-seen id CATALOGS and the
/// merged table itself goes word-mode — deletes the bytes table entirely
/// instead of caching around it). Byte-identical either way — the map only
/// redirects state combines to the same merged rows.
pub fn sink_gid_merge_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_RUNTIME_AGG_GIDMERGE").as_deref(), Ok("1") | Ok("on"))
    })
}

/// combine16 kill switch (default ON): build each combine claim's merged
/// bucket table FLAT — one single-level entry set presized from the claim's
/// arrival count, two-level conversion suppressed, long-key arena reserved
/// from the directory's byte counts. Root cause: the sink bucket and the
/// table's two-level bucket both key on `hash >> 56`, and bytes-mode combine
/// probes reuse the carried SINK hash — constant top byte within a claim —
/// so a `total > TWO_LEVEL_THRESHOLD` two-level table funnels every member
/// into ONE sub-EntrySet (re-grown through full rehashes) while the other
/// 255 presized sets are allocated + zeroed unused. Byte-invisible: entry
/// layout/growth never changes dedup results or row insertion order, and
/// every consumer reads rows in insertion order.
pub fn sink_combine16_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_RUNTIME_AGG_COMBINE16").as_deref(), Ok("0") | Ok("off"))
    })
}

/// `PGRUST_SINK_FLUSH_STEAL=1` opt-in (default OFF — arena-strings inc-1,
/// scratchpad/night/arena-string-tables-design.md §4.3/§4.6): the canonical
/// flush STEALS the accept-time store-once byte store (`canon_store`) into
/// the run instead of permute-copying every key image bucket-major — the
/// url-key-profile flush-copy bucket. Slots stay bucket-major (starts, states,
/// hashes, gid words are permuted exactly as before — u64 traffic only);
/// key bytes stay arrival-ordered with per-slot (off, end) into the stolen
/// store (`SinkRun::key_ends`, read via [`SinkRun::key_slice`]). Byte-
/// identical results by construction: same rows, same slots, same bytes —
/// only WHERE the bytes live differs, and the spill record (self-describing
/// per-row `key_len`) serializes identically from either law.
pub fn sink_flush_steal_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(std::env::var("PGRUST_SINK_FLUSH_STEAL").as_deref(), Ok("1") | Ok("on"))
    })
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
    /// (canonical text-bearing shapes — keys live in `key_offs`/`key_bytes`;
    /// `keys` then optionally carries per-row GID WORDS, see `gid_gen`).
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
    /// Bytes mode: `nrows + 1` offsets into `key_bytes`. CONTIGUOUS law
    /// (`key_ends` empty): bucket-major and monotone — row i's canonical
    /// key = `key_bytes[key_offs[i]..key_offs[i+1]]`. STOLEN law
    /// (`key_ends` non-empty, arena-strings inc-1): slot i's key =
    /// `key_bytes[key_offs[i]..key_ends[i]]` — offsets point into the
    /// ARRIVAL-ordered stolen store and are NOT monotone in slot order
    /// (the final entry stays `key_bytes.len()` so `nrows` holds). ALWAYS
    /// read through [`SinkRun::key_slice`]. Empty in word modes.
    pub key_offs: Vec<u32>,
    /// Bytes mode, STOLEN law only: slot i's key END offset (parallel to
    /// `key_offs[..n]`). Empty = the contiguous law. The spill record is
    /// UNAFFECTED either way (self-describing per-row `key_len`; the
    /// serializer reads through `key_slice`).
    pub key_ends: Vec<u32>,
    /// Bytes mode: canonical key bytes — copied bucket-major at flush
    /// (contiguous law), or the table's canonical store STOLEN whole
    /// (arena-strings inc-1: kills the per-flush byte permute-copy; the
    /// run must stay self-contained either way — the reset frees nothing
    /// the run still references).
    pub key_bytes: Vec<u8>,
    /// Bytes mode: row i's [`sink_hash_bytes`] over its canonical bytes,
    /// bucket-major (parallel to `key_offs` slots). Computed once at flush
    /// and REUSED as the combine table's probe hash (`probe_bytes` takes
    /// the hash as a parameter; its slot index reads the hash's low bits
    /// and its salt bits 32..48 — the sink's constant-per-bucket top byte
    /// is never consumed). Empty in word modes.
    pub hashes: Vec<u64>,
    /// Bytes mode, GID-merge car: the intern-table GENERATION this run's
    /// rows were packed under. When `keys` is non-empty (2 words per row,
    /// bucket-major — the worker table's PACKED key image, per-worker
    /// intern ids included), the combine may merge repeat arrivals of one
    /// (worker, generation, words) triple WORD-MODE instead of re-probing
    /// canonical bytes: within a generation the packed words biject onto
    /// the worker's groups (intern ids are insert-once). Spill replay drops
    /// the words (`keys` empty) — those rows always bytes-probe. 0 and
    /// unused in word modes.
    pub gid_gen: u64,
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
            + self.key_ends.capacity() * 4
            + self.key_bytes.capacity()
            + self.hashes.capacity() * 8
    }

    /// Bytes mode: slot `i`'s canonical key bytes — THE read path for
    /// `key_offs`/`key_bytes` (dispatches the contiguous vs stolen law;
    /// see the field docs).
    #[inline(always)]
    pub fn key_slice(&self, i: usize) -> &[u8] {
        let s = self.key_offs[i] as usize;
        let e = if self.key_ends.is_empty() {
            self.key_offs[i + 1] as usize
        } else {
            self.key_ends[i] as usize
        };
        &self.key_bytes[s..e]
    }

    /// Bytes mode: total canonical key bytes of bucket `b`'s rows (the
    /// combine's arena pre-reserve hint). O(1) under the contiguous law,
    /// O(rows-in-bucket) under the stolen law.
    #[inline]
    pub fn bucket_key_bytes(&self, b: usize) -> usize {
        let lo = self.starts[b] as usize;
        let hi = self.starts[b + 1] as usize;
        if self.key_ends.is_empty() {
            (self.key_offs[hi] - self.key_offs[lo]) as usize
        } else {
            (lo..hi).map(|i| (self.key_ends[i] - self.key_offs[i]) as usize).sum()
        }
    }
}

#[inline]
fn table_key_words(t: &LaneAggTable) -> usize {
    match t.repr() {
        KeyRepr::Int => 1,
        KeyRepr::Int128 => 2,
        // Canonical bytes-keyed tables (c3) never take the word-mode key
        // paths: flush/spill are disarmed for canonical shapes (the word-
        // mode fixed-width record cannot round-trip key bytes — the
        // train-13 m35 x c3 composition gate) and partition/emit/topn all
        // dispatch on repr.
        KeyRepr::Bytes => unreachable!("bytes-keyed table on a word-mode key path"),
    }
}

/// Row `i`'s canonical key words; `None` = the NULL group.
#[inline]
fn row_key_words(t: &LaneAggTable, i: usize) -> Option<[u64; 2]> {
    match t.repr() {
        KeyRepr::Int => t.row_key_int(i).map(|k| [k as u64, 0]),
        KeyRepr::Int128 => t.row_key_i128(i),
        // See table_key_words: bytes-keyed callers dispatch on repr first.
        KeyRepr::Bytes => unreachable!("bytes-keyed table on a word-mode key path"),
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
        key_ends: Vec::new(),
        state_words,
        starts,
        keys,
        states,
        null_states,
        key_offs: Vec::new(),
        key_bytes: Vec::new(),
        hashes: Vec::new(),
        gid_gen: 0,
    }
}

/// Freeze a COMBINE-MERGED bucket table into a self-contained single-bucket
/// run (numa-combine two-level pass A). Every non-NULL row of `t` belongs to
/// bucket `b` by construction — the caller merged only bucket-`b` arrivals —
/// and the run preserves the table's INSERTION ORDER verbatim: the first-seen
/// discipline composes across the second-level merge exactly because a
/// partial run replays its half's arrivals in the flat pass's own order.
/// Handles all three key reprs; the NULL group (word modes,
/// `b == SINK_NULL_BUCKET` only — the two-level arm routes the NULL bucket
/// flat, but the conversion stays total) rides out-of-band as every run's
/// NULL face does. State blocks are copied VERBATIM, so a byref transvalue
/// stays a pointer into whatever `t` was pointing at: the caller must keep
/// that memory alive for the run's whole life — for a min/max(text) shape
/// that is the source [`CombinedBucket`]'s own store, which therefore
/// travels with the partial.
/// Bytes-mode rows re-derive [`sink_hash_bytes`] from their canonical image
/// (bit-identical to the flush-side hash — same function, same bytes); GID
/// words are dropped (`keys` empty ⇒ the final merge always bytes-probes,
/// byte-invisible by the GID map's own contract).
pub fn sink_run_from_bucket_table(b: usize, t: &LaneAggTable) -> SinkRun {
    debug_assert!(b < SINK_NBUCKETS);
    let state_words = t.state_bytes() / 8;
    let n = t.nrows();
    let bytes_mode = t.repr() == KeyRepr::Bytes;
    let key_words = if bytes_mode { 0 } else { table_key_words(t) };
    let mut keys: Vec<u64> = Vec::new();
    let mut key_offs: Vec<u32> = Vec::new();
    let mut key_bytes: Vec<u8> = Vec::new();
    let mut hashes: Vec<u64> = Vec::new();
    let mut states: Vec<u64> = Vec::with_capacity(n * state_words);
    let mut null_states: Option<Vec<u64>> = None;
    if bytes_mode {
        key_offs.reserve(n + 1);
        key_offs.push(0);
        hashes.reserve(n);
    } else {
        keys.reserve(n * key_words);
    }
    // SAFETY (both closures below): row `i < nrows`; the row's state block is
    // `state_words` u64s, 8-aligned by the LaneAggTable state layout.
    let push_states = |states: &mut Vec<u64>, i: usize| unsafe {
        states.extend_from_slice(core::slice::from_raw_parts(
            t.row_states(i).cast::<u64>().cast_const(),
            state_words,
        ));
    };
    let mut scratch = [0u8; 8];
    for i in 0..n {
        if bytes_mode {
            let k = t
                .row_key_bytes(i, &mut scratch)
                .expect("canonical shapes are non-nullable");
            let h = sink_hash_bytes(k);
            debug_assert_eq!(bucket_of(h), b, "merged bucket table carries a foreign row");
            key_bytes.extend_from_slice(k);
            key_offs.push(key_bytes.len() as u32);
            hashes.push(h);
            push_states(&mut states, i);
        } else {
            match row_key_words(t, i) {
                Some([w0, w1]) => {
                    debug_assert_eq!(
                        bucket_of(sink_hash(w0, w1)),
                        b,
                        "merged bucket table carries a foreign row"
                    );
                    keys.push(w0);
                    if key_words == 2 {
                        keys.push(w1);
                    }
                    push_states(&mut states, i);
                }
                None => {
                    debug_assert_eq!(b, SINK_NULL_BUCKET);
                    let mut block = vec![0u64; state_words];
                    // SAFETY: as push_states.
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
    }
    let nonnull = if bytes_mode {
        key_offs.len() - 1
    } else {
        keys.len() / key_words
    } as u32;
    // Single-bucket geometry: all rows in `b`.
    let mut starts = vec![0u32; SINK_NBUCKETS + 1];
    for s in starts[b + 1..].iter_mut() {
        *s = nonnull;
    }
    SinkRun {
        key_words,
        key_ends: Vec::new(),
        state_words,
        starts,
        keys,
        states,
        null_states,
        key_offs,
        key_bytes,
        hashes,
        gid_gen: 0,
    }
}

// ---------------------------------------------------------------------------
// Canonical key bytes (text-bearing Multi shapes — the C2 car).
// ---------------------------------------------------------------------------

/// The armed compact state's canonical (text-bearing) Multi shape, when the
/// sink must merge on CANONICAL KEY BYTES: a Multi key spec carrying an
/// Intern component. `None` = word-keyed shapes (the existing paths) AND
/// the DIRECT single-text arm (arena-strings inc-3: the table already keys
/// on the canonical image — no intern table exists and none of the
/// intern-side canon machinery may run; every canon consumer dispatches
/// `text_direct` FIRST).
fn compact_canon_shape(ch: &crate::compact::CompactHash) -> Option<&MkShape> {
    match &ch.key {
        crate::compact::CompactKeySpec::Multi(s)
            if s.intern_comp().is_some() && !ch.text_direct =>
        {
            Some(s)
        }
        _ => None,
    }
}

/// Extend the compact state's stored canonical hashes to cover every table
/// row (rows are append-only within an epoch; a flush resets both). Called
/// at the BATCH TAIL by the packed probes (new groups hash while their
/// text bytes are cache-warm, on the accepting worker — parallel), and
/// defensively at flush/SEAL entry (no-op when the batch tails covered
/// everything; covers the per-row test/fallback insert paths). Word shapes
/// return immediately.
pub(crate) fn compact_extend_canon_hashes(ch: &mut crate::compact::CompactHash) {
    let crate::compact::CompactHash {
        table, key, intern, canon_hashes, canon_store, canon_offs, ..
    } = ch;
    let crate::compact::CompactKeySpec::Multi(shape) = key else { return };
    if shape.intern_comp().is_none() {
        return;
    }
    let Some(intern) = intern.as_ref() else { return };
    let n = table.nrows();
    if canon_hashes.len() >= n {
        debug_assert_eq!(canon_hashes.len(), n, "canon hashes never outrun the table");
        return;
    }
    let spk_t0 = crate::spankey::spankey_t0();
    let start = canon_hashes.len();
    let mut spk_bytes = 0u64;
    canon_hashes.reserve(n - start);
    if crate::spankey::spankey_store_enabled() {
        // STORE-ONCE (spankey step 2): the image this hash pass had to
        // build anyway is KEPT — flush pass-1 and the combine remainder
        // face read it verbatim instead of re-running word-unpack +
        // intern-reverse-chase + tail assembly per consumer. Alignment:
        // the switch is process-constant, so the store covers rows 0..n.
        if canon_offs.is_empty() {
            canon_offs.push(0);
        }
        debug_assert_eq!(canon_offs.len(), start + 1, "store aligned with hashes");
        canon_offs.reserve(n - start);
        for row in start..n {
            let base = canon_store.len();
            canon_row_bytes_append(table, shape, intern, row, canon_store);
            canon_offs.push(canon_store.len() as u32);
            if spk_t0.is_some() {
                spk_bytes += (canon_store.len() - base) as u64;
            }
            canon_hashes.push(sink_hash_bytes(&canon_store[base..]));
        }
    } else {
        let mut scratch: Vec<u8> = Vec::with_capacity(64);
        for row in start..n {
            scratch.clear();
            canon_row_bytes_append(table, shape, intern, row, &mut scratch);
            if spk_t0.is_some() {
                spk_bytes += scratch.len() as u64;
            }
            canon_hashes.push(sink_hash_bytes(&scratch));
        }
    }
    if spk_t0.is_some() {
        use crate::spankey::{spankey_add, spankey_lap, SPANKEY_CTRS as S};
        spankey_add(&S.canon_accept_rows, (n - start) as u64);
        spankey_add(&S.canon_accept_bytes, spk_bytes);
        spankey_lap(&S.canon_accept_ns, spk_t0);
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
/// image's `packed_bytes` little-endian bytes with EVERY Intern component's
/// 4 id bytes ZEROED (intern ids are PER-WORKER — never canonical), followed
/// by the interned text bytes (the intern table's reverse map). Tail
/// encoding is arity-dispatched:
///  * ONE Intern component (the C2 single-text classes): the raw text bytes
///    verbatim — the historical image, byte-for-byte (freeze snapshots,
///    topn tie order, and every landed gate keep their exact bytes).
///  * TWO+ Intern components (the CaseDict two-text class): each tail is
///    length-prefixed (`u32` LE len + content) in component order — the two
///    tails decode unambiguously (canon-sink-increments car 1).
/// Injective either way: the prefix is fixed-width per shape and the tail
/// grammar is self-describing; equal component values produce identical
/// bytes on every worker — the cross-Local merge key, hash input, and
/// rule-5 selection image alike.
fn canon_row_bytes(
    table: &LaneAggTable,
    shape: &MkShape,
    intern: &LaneAggTable,
    row: usize,
    out: &mut Vec<u8>,
) {
    out.clear();
    canon_row_bytes_append(table, shape, intern, row, out);
}

/// [`canon_row_bytes`] without the clear: appends row `row`'s canonical
/// image to `out` (the flush's flat single-materialization buffer — each
/// row's image is built exactly once and permuted into bucket order by a
/// plain byte copy).
fn canon_row_bytes_append(
    table: &LaneAggTable,
    shape: &MkShape,
    intern: &LaneAggTable,
    row: usize,
    out: &mut Vec<u8>,
) {
    debug_assert!(!shape.nullable, "canonical shapes are non-nullable (sink admission)");
    let words = mk_words_of(table, shape, row);
    debug_assert!(shape.intern_comp().is_some(), "canonical shapes carry an Intern component");
    let n_intern = shape.n_intern();
    let base = out.len();
    let mut flat = [0u8; 16];
    flat[..8].copy_from_slice(&words[0].to_le_bytes());
    flat[8..].copy_from_slice(&words[1].to_le_bytes());
    out.extend_from_slice(&flat[..shape.packed_bytes as usize]);
    // Zero every Intern component's id bytes in the prefix (per-worker ids
    // are never canonical), then append the tails in component order.
    for (_, icomp) in shape.intern_comps() {
        let ioff = base + icomp.off as usize;
        for b in &mut out[ioff..ioff + 4] {
            *b = 0;
        }
    }
    let mut scratch = [0u8; 8];
    for (_, icomp) in shape.intern_comps() {
        let id = crate::compact::mk_unpack(words, icomp) as u32;
        let bytes = intern
            .row_key_bytes(id as usize, &mut scratch)
            .expect("intern ids never map to a NULL row");
        if n_intern > 1 {
            out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        }
        out.extend_from_slice(bytes);
    }
}

/// [`sink_flush_table`]'s canonical-bytes twin: flush the armed compact
/// table of a text-bearing Multi shape into a BYTES-MODE run (canonical key
/// bytes copied out — the reset frees the table's own storage; the intern
/// table is deliberately NOT reset: it is scan-lifetime and the remainder's
/// ids stay valid). Bucket-major two-pass counting sort by
/// [`sink_hash_bytes`] over the canonical bytes.
fn sink_flush_table_canon(ch: &mut crate::compact::CompactHash) -> SinkRun {
    sink_flush_table_canon_impl(ch, sink_gid_merge_enabled(), sink_flush_steal_enabled())
}

/// [`sink_flush_table_canon`] with the GID-word fill and store-steal
/// decisions injected (the unit tests exercise both lanes regardless of the
/// process env).
fn sink_flush_table_canon_impl(
    ch: &mut crate::compact::CompactHash,
    gid: bool,
    steal: bool,
) -> SinkRun {
    let spk_t0 = crate::spankey::spankey_t0();
    // The batch tails already hashed every row's canonical image
    // (`compact_extend_canon_hashes` — accept-time, parallel); the extend
    // here is the defensive no-op sweep for the non-batched insert paths.
    // Pass 1: materialize each row's canonical image EXACTLY ONCE into a
    // flat arrival-order scratch (image offsets recorded) and take
    // per-bucket row + byte counts off the stored hashes. Pass 2 is then a
    // plain permuting byte copy — the old shape re-ran the whole canonical
    // materialization (word unpack + intern reverse-map chase + component
    // assembly) a second time per row AND hashed at flush, which the
    // interned-key/expr-key profiles put at ~14% of the engaged 16-thread query.
    compact_extend_canon_hashes(ch);
    let gid_gen = ch.intern_gen;
    let crate::compact::CompactHash {
        table, key, intern, canon_hashes, canon_store, canon_offs, ..
    } = ch;
    let crate::compact::CompactKeySpec::Multi(shape) = key else {
        unreachable!("canonical flush requires a Multi shape")
    };
    let intern = intern.as_ref().expect("canonical shapes carry the intern table");
    let state_words = table.state_bytes() / 8;
    let n = table.nrows();
    debug_assert_eq!(canon_hashes.len(), n, "flush entry extended the hashes");
    let hashes = &*canon_hashes;
    let mut counts = [0u32; SINK_NBUCKETS];
    let mut byte_counts = [0usize; SINK_NBUCKETS];
    // STORE-ONCE (spankey step 2): the accept-time extension already
    // materialized every row's image — pass 1 collapses to per-bucket
    // counting off the stored offsets (no rebuild). Kill switch off (or
    // non-sink test paths without a store): build the local scratch as
    // before, IDENTICAL bytes by the stored-hash law either way.
    let mut lscratch: Vec<u8> = Vec::new();
    let mut loffs: Vec<u32> = Vec::with_capacity(n + 1);
    // STEAL (arena-strings inc-1): the store-once law already left every
    // image materialized arrival-ordered in `canon_store` — take the store
    // whole and permute OFFSETS into bucket-major slots instead of the
    // bytes (u32/u64 traffic replaces the whole-key-volume memcpy). Only
    // the store-once shape qualifies (the non-batched fallback below
    // rebuilds into a scratch it owns anyway, where a copy is the build).
    let steal = steal && canon_offs.len() == n + 1;
    let (scratch, scratch_offs): (&[u8], &[u32]) = if canon_offs.len() == n + 1 {
        for i in 0..n {
            let img = &canon_store[canon_offs[i] as usize..canon_offs[i + 1] as usize];
            debug_assert_eq!(hashes[i], sink_hash_bytes(img), "stored canon hash law");
            let h = hashes[i];
            counts[bucket_of(h)] += 1;
            byte_counts[bucket_of(h)] += img.len();
        }
        (&canon_store[..], &canon_offs[..])
    } else {
        loffs.push(0);
        for i in 0..n {
            let base = lscratch.len();
            canon_row_bytes_append(table, shape, intern, i, &mut lscratch);
            let img = &lscratch[base..];
            debug_assert_eq!(hashes[i], sink_hash_bytes(img), "stored canon hash law");
            let h = hashes[i];
            counts[bucket_of(h)] += 1;
            byte_counts[bucket_of(h)] += img.len();
            loffs.push(lscratch.len() as u32);
        }
        (&lscratch[..], &loffs[..])
    };
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
    let mut key_ends: Vec<u32> = if steal { vec![0; n] } else { Vec::new() };
    let mut key_bytes: Vec<u8> = if steal { Vec::new() } else { vec![0; total_bytes] };
    let mut states: Vec<u64> = vec![0; n * state_words];
    let mut run_hashes: Vec<u64> = vec![0; n];
    // GID-merge car: carry each row's PACKED key words (per-worker intern
    // ids included) so the combine can merge repeat arrivals of one
    // (worker, generation, words) triple word-mode instead of re-probing
    // canonical bytes.
    let mut gid_words: Vec<u64> = if gid { vec![0; n * 2] } else { Vec::new() };
    for i in 0..n {
        let b = bucket_of(hashes[i]);
        let slot = cursor[b] as usize;
        cursor[b] += 1;
        if steal {
            // STOLEN law: the slot points at the row's arrival-order image
            // in the (about-to-be-stolen) store — no byte copy.
            key_offs[slot] = scratch_offs[i];
            key_ends[slot] = scratch_offs[i + 1];
        } else {
            let img = &scratch[scratch_offs[i] as usize..scratch_offs[i + 1] as usize];
            let off = bcursor[b];
            bcursor[b] += img.len();
            key_offs[slot] = off as u32;
            key_bytes[off..off + img.len()].copy_from_slice(img);
        }
        run_hashes[slot] = hashes[i];
        if gid {
            let w = mk_words_of(table, shape, i);
            gid_words[slot * 2] = w[0];
            gid_words[slot * 2 + 1] = w[1];
        }
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
    if steal {
        // Take the store whole; the run is self-contained exactly as under
        // the copy law (the store restarts empty for the next epoch, below).
        key_bytes = core::mem::take(canon_store);
        debug_assert_eq!(key_bytes.len(), total_bytes);
    } else {
        // Offsets are consistent per slot: rows within a bucket fill both
        // the slot range and the byte range in the same order, and buckets
        // are laid out contiguously — slot s's key ends exactly where slot
        // s+1 begins.
        debug_assert!(key_offs.windows(2).all(|w| w[0] <= w[1]));
    }
    table.reset();
    // The epoch's rows are gone — the stored hashes AND the store-once
    // canonical images restart with them.
    canon_hashes.clear();
    canon_store.clear();
    canon_offs.clear();
    if spk_t0.is_some() {
        use crate::spankey::{spankey_add, spankey_lap, SPANKEY_CTRS as S};
        spankey_add(&S.flush_canon_rows, n as u64);
        spankey_add(&S.flush_canon_bytes, total_bytes as u64);
        spankey_lap(&S.flush_canon_ns, spk_t0);
    }
    SinkRun {
        key_words: 0,
        key_ends,
        state_words,
        starts,
        keys: gid_words,
        states,
        null_states: None,
        key_offs,
        key_bytes,
        hashes: run_hashes,
        gid_gen,
    }
}

/// [`sink_partition_remainder`]'s canonical twin: bucket index by the
/// STORED canonical hashes (`compact_extend_canon_hashes` — accept-time,
/// parallel). This runs on the single-threaded last-worker-out SEAL, which
/// the expr-key @100M profile showed serializing a canon+hash sweep over every
/// Local's remainder while 15 workers waited — with the hashes carried it
/// is a plain counting sort. Canonical shapes are non-nullable —
/// `has_null` is structurally false.
fn sink_partition_remainder_canon(ch: &mut crate::compact::CompactHash) -> SinkPart {
    compact_extend_canon_hashes(ch);
    let crate::compact::CompactHash { table, canon_hashes, .. } = ch;
    let n = table.nrows();
    debug_assert_eq!(canon_hashes.len(), n, "partition entry extended the hashes");
    let hashes = &*canon_hashes;
    let mut counts = [0u32; SINK_NBUCKETS];
    for &h in hashes.iter() {
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
    let mut part_hashes = vec![0u64; acc as usize];
    for (i, &h) in hashes.iter().enumerate() {
        let b = bucket_of(h);
        idx[cursor[b] as usize] = i as u32;
        part_hashes[cursor[b] as usize] = h;
        cursor[b] += 1;
    }
    SinkPart { starts, idx, has_null: false, hashes: part_hashes }
}

/// [`sink_flush_table_canon`]'s DIRECT-arm twin (arena-strings inc-3): the
/// direct table's rows already ARE the canonical images and their saved
/// hash words already ARE the sink hashes (the probe-hash law —
/// `agg_hash_compact_probe_text_direct`), so the flush is a plain bucket-
/// major two-pass counting sort off the stored hashes: no canon rebuild, no
/// rehash. `sink_flush_table_canon_impl`'s exact copy law — contiguous
/// `key_offs` (`key_ends` empty), arrival order preserved within buckets,
/// states verbatim; no GID words (no packed image exists — the combine
/// always bytes-probes these runs, `gid_gen` 0). The table is RESET — for
/// direct tables the table IS the vocabulary, so the caller must propagate
/// the cache-invalidation signal on EVERY direct flush. (Arena-steal for the
/// direct flush is a later increment — this is the simple copy.)
fn sink_flush_table_direct(ch: &mut crate::compact::CompactHash) -> SinkRun {
    debug_assert!(ch.text_direct, "direct flush requires the direct arm");
    debug_assert!(ch.canon_hashes.is_empty() && ch.canon_store.is_empty());
    let table = &mut ch.table;
    debug_assert_eq!(table.repr(), KeyRepr::Bytes);
    let state_words = table.state_bytes() / 8;
    let n = table.nrows();
    // Pass 1: per-bucket row + byte counts off the stored hashes.
    let mut counts = [0u32; SINK_NBUCKETS];
    let mut byte_counts = [0usize; SINK_NBUCKETS];
    let mut scratch = [0u8; 8];
    for i in 0..n {
        let h = table.row_key_hash(i);
        let k = table
            .row_key_bytes(i, &mut scratch)
            .expect("direct tables are non-nullable");
        debug_assert_eq!(h, sink_hash_bytes(k), "direct probe-hash law");
        counts[bucket_of(h)] += 1;
        byte_counts[bucket_of(h)] += k.len();
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
    // Pass 2: permuting copy into bucket-major slots (arrival order within
    // each bucket — the counting sort is stable by construction).
    let mut cursor: [u32; SINK_NBUCKETS] = core::array::from_fn(|b| starts[b]);
    let mut bcursor = bstart;
    let mut key_offs: Vec<u32> = vec![0; n + 1];
    let mut key_bytes: Vec<u8> = vec![0; total_bytes];
    let mut states: Vec<u64> = vec![0; n * state_words];
    let mut run_hashes: Vec<u64> = vec![0; n];
    for i in 0..n {
        let h = table.row_key_hash(i);
        let b = bucket_of(h);
        let slot = cursor[b] as usize;
        cursor[b] += 1;
        let k = table
            .row_key_bytes(i, &mut scratch)
            .expect("direct tables are non-nullable");
        let off = bcursor[b];
        bcursor[b] += k.len();
        key_offs[slot] = off as u32;
        key_bytes[off..off + k.len()].copy_from_slice(k);
        run_hashes[slot] = h;
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
    // Offsets are consistent per slot (the canon flush's exact argument):
    // rows within a bucket fill both the slot range and the byte range in
    // the same order, and buckets are laid out contiguously.
    debug_assert!(key_offs.windows(2).all(|w| w[0] <= w[1]));
    table.reset();
    SinkRun {
        key_words: 0,
        key_ends: Vec::new(),
        state_words,
        starts,
        keys: Vec::new(),
        states,
        null_states: None,
        key_offs,
        key_bytes,
        hashes: run_hashes,
        gid_gen: 0,
    }
}

/// [`sink_partition_remainder_canon`]'s DIRECT-arm twin: bucket index by the
/// table's SAVED hash words (== the sink hashes by the probe-hash law) — a
/// plain counting sort, no canon extension, no rehash. Direct shapes are
/// non-nullable (`has_null` structurally false).
fn sink_partition_remainder_direct(ch: &crate::compact::CompactHash) -> SinkPart {
    debug_assert!(ch.text_direct, "direct partition requires the direct arm");
    let table = &ch.table;
    let n = table.nrows();
    let mut counts = [0u32; SINK_NBUCKETS];
    for i in 0..n {
        counts[bucket_of(table.row_key_hash(i))] += 1;
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
    let mut part_hashes = vec![0u64; acc as usize];
    for i in 0..n {
        let h = table.row_key_hash(i);
        let b = bucket_of(h);
        idx[cursor[b] as usize] = i as u32;
        part_hashes[cursor[b] as usize] = h;
        cursor[b] += 1;
    }
    SinkPart { starts, idx, has_null: false, hashes: part_hashes }
}

// ---------------------------------------------------------------------------
// LIMIT-k-no-ORDER group-admission FREEZE (band-kernels-2a, the grouped-LIMIT
// class): `GROUP BY ... LIMIT k` with NO ORDER BY needs only k groups with
// EXACT aggregates — the ratified PASS-TIE membership class (count-gated:
// rowcount equal, values exact for whichever groups emit). The law:
//  * OPEN: every worker admits groups normally (nothing is ever dropped).
//  * INSTALL: the first worker whose live compact table holds >= bound
//    groups wins a CAS election and publishes those groups' CANONICAL key
//    bytes as the frozen set. ANY bound groups are a valid set — every row
//    of every group present anywhere has been counted so far (no drops
//    before FROZEN), and set members keep counting after.
//  * FROZEN: workers drop rows whose key is NOT in the set BEFORE the table
//    probe (the per-row build cost collapses to a tiny membership check);
//    rows of set members flow exactly as before, so members' aggregates are
//    exact over ALL their input rows.
//  * COMBINE: pre-freeze straggler groups (admitted before their owner
//    observed FROZEN) are UNDERCOUNTED from the freeze point on — the
//    combine filters every merged bucket to set members only, so stragglers
//    never emit. Total emitted rows == bound (when >= bound groups exist;
//    otherwise the freeze never installs and the drain is the plain full
//    drain, byte-identical).
// Mutual exclusion with the composed top-N is structural: the topn spec is
// derived only from a bounded Sort consumer; the freeze bound only from a
// bare Limit-over-Agg (no Sort) — both never arm together.
// ---------------------------------------------------------------------------

/// Freeze bound ceiling: entry masks ride a u64 in the worker filter, and
/// the class only pays off for small k (the motivating shape is LIMIT 10). Larger bounds
/// decline at arming and keep the full drain.
pub const SINK_FREEZE_MAX_BOUND: u32 = 64;

const FREEZE_OPEN: u8 = 0;
const FREEZE_INSTALLING: u8 = 1;
const FREEZE_FROZEN: u8 = 2;
const FREEZE_DISABLED: u8 = 3;

/// The engagement-shared freeze control: bound + install election + the
/// published canonical key set. One per sink engagement (leader-armed),
/// shared by every worker through the sink.
pub struct SinkFreeze {
    bound: u32,
    /// OPEN -> INSTALLING (CAS, the election) -> FROZEN (Release publish).
    /// DISABLED = an install could not extract (fail-open: no drops ever
    /// happen, the drain stays full — correct, just unoptimized).
    state: core::sync::atomic::AtomicU8,
    /// Canonical key bytes per entry (the seal/flush encoding — see
    /// [`canon_row_bytes`]; word-keyed Multi shapes use the packed image's
    /// `packed_bytes` little-endian bytes). Written ONLY by the installer
    /// between the CAS and the FROZEN store; read only at/after FROZEN
    /// (Acquire pairs with the Release store).
    set: core::cell::UnsafeCell<Vec<Vec<u8>>>,
    /// Rows dropped by worker filters (observability).
    dropped: core::sync::atomic::AtomicU64,
    /// Straggler groups filtered at combine (observability).
    stragglers: core::sync::atomic::AtomicU64,
}

// SAFETY: `set` is written only by the single CAS-elected installer before
// the FROZEN Release store, and read only after an Acquire load observes
// FROZEN — a happens-before edge orders every read after the last write.
unsafe impl Sync for SinkFreeze {}

impl SinkFreeze {
    pub fn new(bound: u32) -> SinkFreeze {
        debug_assert!(bound >= 1 && bound <= SINK_FREEZE_MAX_BOUND);
        SinkFreeze {
            bound,
            state: core::sync::atomic::AtomicU8::new(FREEZE_OPEN),
            set: core::cell::UnsafeCell::new(Vec::new()),
            dropped: core::sync::atomic::AtomicU64::new(0),
            stragglers: core::sync::atomic::AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn bound(&self) -> u32 {
        self.bound
    }

    /// The frozen canonical set, or None while OPEN/INSTALLING/DISABLED.
    #[inline]
    pub fn entries(&self) -> Option<&[Vec<u8>]> {
        if self.state.load(core::sync::atomic::Ordering::Acquire) == FREEZE_FROZEN {
            // SAFETY: FROZEN observed with Acquire — the installer's writes
            // happened-before; nobody writes after FROZEN.
            Some(unsafe { &*self.set.get() })
        } else {
            None
        }
    }

    #[inline]
    pub fn frozen(&self) -> bool {
        self.state.load(core::sync::atomic::Ordering::Acquire) == FREEZE_FROZEN
    }

    /// Election: exactly one caller wins the right to install. The winner
    /// MUST follow with [`Self::publish`] or [`Self::disable`].
    pub fn try_begin_install(&self) -> bool {
        self.state
            .compare_exchange(
                FREEZE_OPEN,
                FREEZE_INSTALLING,
                core::sync::atomic::Ordering::AcqRel,
                core::sync::atomic::Ordering::Acquire,
            )
            .is_ok()
    }

    /// Installer-only: publish the canonical set and flip FROZEN.
    pub fn publish(&self, entries: Vec<Vec<u8>>) {
        debug_assert_eq!(entries.len(), self.bound as usize);
        // SAFETY: single writer by the CAS election; no reader until the
        // Release store below.
        unsafe { *self.set.get() = entries };
        self.state.store(FREEZE_FROZEN, core::sync::atomic::Ordering::Release);
    }

    /// Installer-only: the extraction failed — fail OPEN forever (no drops
    /// ever happen; the engagement drains fully, correct but unoptimized).
    pub fn disable(&self) {
        self.state.store(FREEZE_DISABLED, core::sync::atomic::Ordering::Release);
    }

    #[inline]
    pub fn note_dropped(&self, n: u64) {
        if n > 0 {
            self.dropped.fetch_add(n, core::sync::atomic::Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn note_stragglers(&self, n: u64) {
        if n > 0 {
            self.stragglers.fetch_add(n, core::sync::atomic::Ordering::Relaxed);
        }
    }

    pub fn dropped(&self) -> u64 {
        self.dropped.load(core::sync::atomic::Ordering::Relaxed)
    }

    pub fn stragglers(&self) -> u64 {
        self.stragglers.load(core::sync::atomic::Ordering::Relaxed)
    }
}

/// Extract the first `bound` insertion-order groups of the ARMED compact
/// Multi table as canonical key bytes (the install source). `None` when the
/// table is not an armed Multi shape or holds fewer than `bound` groups.
/// ANY `bound` groups form a valid frozen set (see the section doc) — the
/// first rows are simply the cheapest to name.
pub fn sink_freeze_extract(node: &AggStateData<'_>, bound: u32) -> Option<Vec<Vec<u8>>> {
    let ph = node.perhash.as_ref()?;
    sink_freeze_extract_ch(ph.compact.as_ref()?, bound)
}

/// [`sink_freeze_extract`] over the armed compact state itself (split for
/// the unit tests, which build [`crate::compact::CompactHash`] directly).
pub(crate) fn sink_freeze_extract_ch(
    ch: &crate::compact::CompactHash,
    bound: u32,
) -> Option<Vec<Vec<u8>>> {
    let crate::compact::CompactKeySpec::Multi(shape) = &ch.key else { return None };
    if shape.nullable || ch.table.nrows() < bound as usize {
        return None;
    }
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(bound as usize);
    // DIRECT single-text arm (arena-strings inc-3): the rows already ARE
    // the canonical images — read them back verbatim.
    if ch.text_direct {
        let mut scratch = [0u8; 8];
        for i in 0..bound as usize {
            out.push(
                ch.table
                    .row_key_bytes(i, &mut scratch)
                    .expect("direct tables are non-nullable")
                    .to_vec(),
            );
        }
        return Some(out);
    }
    match compact_canon_shape(ch) {
        Some(shape) => {
            let intern = ch.intern.as_ref()?;
            let mut canon: Vec<u8> = Vec::with_capacity(64);
            for i in 0..bound as usize {
                canon_row_bytes(&ch.table, shape, intern, i, &mut canon);
                out.push(canon.clone());
            }
        }
        None => {
            // Word-keyed Multi shape: the canonical bytes are the packed
            // image's little-endian `packed_bytes` prefix (value-derived —
            // identical on every worker).
            for i in 0..bound as usize {
                let words = mk_words_of(&ch.table, shape, i);
                let mut flat = [0u8; 16];
                flat[..8].copy_from_slice(&words[0].to_le_bytes());
                flat[8..].copy_from_slice(&words[1].to_le_bytes());
                out.push(flat[..shape.packed_bytes as usize].to_vec());
            }
        }
    }
    Some(out)
}

/// Combine-side membership filter: the merged bucket table's rows whose
/// canonical key bytes are in the frozen set, ascending row order (the
/// [`sink_emit_bucket_rows`] contract). `key_words == 0` = bytes-mode table
/// (rows key on canonical byte strings); word modes reconstruct the image
/// prefix per row.
pub fn sink_freeze_member_rows(
    t: &LaneAggTable,
    key_words: usize,
    shape: &MkShape,
    entries: &[Vec<u8>],
) -> Vec<u32> {
    let set: std::collections::HashSet<&[u8]> =
        entries.iter().map(|e| e.as_slice()).collect();
    let mut out: Vec<u32> = Vec::new();
    let mut scratch = [0u8; 8];
    for i in 0..t.nrows() {
        let member = if key_words == 0 {
            t.row_key_bytes(i, &mut scratch).is_some_and(|b| set.contains(b))
        } else {
            let words = mk_words_of(t, shape, i);
            let mut flat = [0u8; 16];
            flat[..8].copy_from_slice(&words[0].to_le_bytes());
            flat[8..].copy_from_slice(&words[1].to_le_bytes());
            set.contains(&flat[..shape.packed_bytes as usize])
        };
        if member {
            out.push(i as u32);
        }
    }
    out
}

// ---------------------------------------------------------------------------
// M3.5 spill record contract (docs/design/m3.5-spill.md §3): a spilled
// bucket segment is interleaved row-major u64 native-endian words —
// `key_words` canonical key words then `state_words` state words per row.
// NULL-group blocks NEVER touch the file (they ride the Local in memory,
// the distinctset seen_null discipline applied to agg states).
// ---------------------------------------------------------------------------

/// Byte width of one spilled row.
#[inline]
pub fn sink_spill_row_bytes(key_words: usize, state_words: usize) -> usize {
    (key_words + state_words) * 8
}

/// Append bucket `b`'s rows of `run` to `out` in the spill record contract.
/// Word modes write the fixed-width interleaved record; bytes mode
/// (canonical shapes, `key_words == 0`) writes the CANONICAL BYTES record
/// (see [`sink_canon_spill_append`] — the C2 record, canon-sink car 3).
pub fn sink_run_spill_bucket(run: &SinkRun, b: usize, out: &mut Vec<u8>) {
    let lo = run.starts[b] as usize;
    let hi = run.starts[b + 1] as usize;
    if run.key_words == 0 {
        for i in lo..hi {
            let states = &run.states[i * run.state_words..(i + 1) * run.state_words];
            // key_slice dispatches the contiguous/stolen law; the on-disk
            // record is self-describing (per-row key_len) and therefore
            // byte-identical from either representation.
            sink_canon_spill_append(run.key_slice(i), run.hashes[i], states, out);
        }
        return;
    }
    out.reserve((hi - lo) * sink_spill_row_bytes(run.key_words, run.state_words));
    for i in lo..hi {
        for w in 0..run.key_words {
            out.extend_from_slice(&run.keys[i * run.key_words + w].to_ne_bytes());
        }
        for w in 0..run.state_words {
            out.extend_from_slice(&run.states[i * run.state_words + w].to_ne_bytes());
        }
    }
}

// ---------------------------------------------------------------------------
// Canonical BYTES spill record (canon-sink-increments car 3 — the AGG-side
// sibling of the distinct sink's bytes record v2). Variable-width,
// self-describing, 8-aligned:
//   [rec_len u64][hash u64][key_len u64][key bytes, 8-padded]
//   [state words u64 × state_words]
// `rec_len` = the whole record's byte length (8-aligned — the streaming
// reader's alignment law); `hash` = the row's [`sink_hash_bytes`] over its
// canonical key (the replay's probe hash AND the combine-split's routing
// axis — value-derived, so sub-bucket routing by deeper bits of the SAME
// hash partitions groups exactly, the M3.5 law). Canonical shapes are
// non-nullable: no NULL block ever touches a bytes-mode file. Replay and
// routing FAIL CLOSED on any torn/malformed record.
// ---------------------------------------------------------------------------

/// Header bytes of the canonical record (rec_len + hash + key_len).
const CANON_REC_HDR: usize = 24;

#[inline]
fn pad8(n: usize) -> usize {
    n.div_ceil(8) * 8
}

/// The minimum canonical record width (empty text key) — the combine
/// pre-build check's conservative row-count divisor (over-counts rows, the
/// safe direction).
#[inline]
pub fn sink_canon_min_record_bytes(state_words: usize) -> usize {
    CANON_REC_HDR + state_words * 8
}

/// Append one canonical spill record.
fn sink_canon_spill_append(key: &[u8], hash: u64, states: &[u64], out: &mut Vec<u8>) {
    let rec_len = CANON_REC_HDR + pad8(key.len()) + states.len() * 8;
    out.reserve(rec_len);
    out.extend_from_slice(&(rec_len as u64).to_ne_bytes());
    out.extend_from_slice(&hash.to_ne_bytes());
    out.extend_from_slice(&(key.len() as u64).to_ne_bytes());
    out.extend_from_slice(key);
    out.resize(out.len() + (pad8(key.len()) - key.len()), 0);
    for w in states {
        out.extend_from_slice(&w.to_ne_bytes());
    }
}

/// Parse one canonical record header at `off`, fail-closed. Returns
/// `(rec_len, hash, key_range)` — state words occupy the record's last
/// `state_words × 8` bytes.
#[inline]
fn sink_canon_rec_parse(
    bytes: &[u8],
    off: usize,
    state_words: usize,
) -> PgResult<(usize, u64, core::ops::Range<usize>)> {
    let torn = || sink_shape_error("torn canonical spill record");
    if bytes.len() < off + CANON_REC_HDR {
        return Err(torn());
    }
    let rd = |o: usize| u64::from_ne_bytes(bytes[o..o + 8].try_into().expect("8 bytes"));
    let rec_len = rd(off) as usize;
    let hash = rd(off + 8);
    let key_len = rd(off + 16) as usize;
    if rec_len % 8 != 0
        || rec_len > bytes.len() - off
        || key_len > rec_len
        || rec_len != CANON_REC_HDR + pad8(key_len) + state_words * 8
    {
        return Err(torn());
    }
    Ok((rec_len, hash, off + CANON_REC_HDR..off + CANON_REC_HDR + key_len))
}

/// Rebuild a single-bucket BYTES-MODE [`SinkRun`] from canonical spill
/// records: every row lands in bucket `b`, insertion order = file order
/// (= flush order, the first-seen discipline). No GID words survive the
/// file (`keys` empty — replayed rows always bytes-probe at combine).
/// Fail-closed on any torn/malformed record.
pub fn sink_run_from_spill_bytes(
    b: usize,
    state_words: usize,
    bytes: &[u8],
) -> PgResult<SinkRun> {
    let mut key_offs: Vec<u32> = vec![0];
    let mut key_bytes: Vec<u8> = Vec::new();
    let mut hashes: Vec<u64> = Vec::new();
    let mut states: Vec<u64> = Vec::new();
    let mut off = 0usize;
    while off < bytes.len() {
        let (rec_len, hash, key) = sink_canon_rec_parse(bytes, off, state_words)?;
        key_bytes.extend_from_slice(&bytes[key.clone()]);
        key_offs.push(key_bytes.len() as u32);
        hashes.push(hash);
        let s0 = off + rec_len - state_words * 8;
        for w in 0..state_words {
            let o = s0 + w * 8;
            states.push(u64::from_ne_bytes(bytes[o..o + 8].try_into().expect("8 bytes")));
        }
        off += rec_len;
    }
    let n = hashes.len();
    let mut starts: Vec<u32> = Vec::with_capacity(SINK_NBUCKETS + 1);
    for i in 0..=SINK_NBUCKETS {
        starts.push(if i > b { n as u32 } else { 0 });
    }
    Ok(SinkRun {
        key_words: 0,
        key_ends: Vec::new(),
        state_words,
        starts,
        keys: Vec::new(),
        states,
        null_states: None,
        key_offs,
        key_bytes,
        hashes,
        gid_gen: 0,
    })
}

/// [`sink_route_records`]'s canonical twin: route canonical spill records
/// into 256 SUB-buckets by the STORED hash's byte `depth` levels below the
/// top-8 (value-derived — sub-partitioning by strictly deeper bits of the
/// SAME hash partitions groups exactly). Fail-closed on torn input.
pub fn sink_route_records_bytes(
    bytes: &[u8],
    state_words: usize,
    depth: u32,
    out: &mut [Vec<u8>],
) -> PgResult<()> {
    debug_assert_eq!(out.len(), SINK_NBUCKETS);
    debug_assert!((1..=6).contains(&depth), "sub-bucket depth out of range");
    let shift = 56 - 8 * depth;
    let mut off = 0usize;
    while off < bytes.len() {
        let (rec_len, hash, _key) = sink_canon_rec_parse(bytes, off, state_words)?;
        let s = ((hash >> shift) & 0xFF) as usize;
        out[s].extend_from_slice(&bytes[off..off + rec_len]);
        off += rec_len;
    }
    Ok(())
}

/// Serialize bucket-`b`'s CANONICAL remainder rows (via the SEAL partition
/// index + the Local's shape/intern faces) into canonical spill records —
/// the combine-split's remainder serialization for bytes-mode shapes.
pub fn sink_remainder_spill_bucket_canon(
    rem: &SinkRemainder<'_>,
    b: usize,
    out: &mut Vec<u8>,
) -> PgResult<()> {
    // DIRECT single-text arm (arena-strings inc-3): the rows ARE the
    // canonical images and the SEAL carried their sink hashes — serialize
    // verbatim (the record is identical to the intern arm's by the
    // canonical-bytes law).
    if rem.direct {
        let (t, part) = (rem.table, rem.part);
        let state_words = t.state_bytes() / 8;
        let lo = part.starts[b] as usize;
        let hi = part.starts[b + 1] as usize;
        let mut k8 = [0u8; 8];
        let mut states: Vec<u64> = vec![0; state_words];
        for (slot, &row) in part.idx[lo..hi].iter().enumerate() {
            let img = t
                .row_key_bytes(row as usize, &mut k8)
                .expect("direct tables are non-nullable");
            // SAFETY: the row's state block is state_words u64s (8-aligned
            // by the LaneAggTable state layout).
            unsafe {
                core::ptr::copy_nonoverlapping(
                    t.row_states(row as usize).cast::<u64>().cast_const(),
                    states.as_mut_ptr(),
                    state_words,
                );
            }
            sink_canon_spill_append(img, part.hashes[lo + slot], &states, out);
        }
        return Ok(());
    }
    let (shape, intern) = rem
        .canon
        .ok_or_else(|| sink_shape_error("canonical remainder spill without a canon face"))?;
    let t = rem.table;
    let part = rem.part;
    let state_words = t.state_bytes() / 8;
    let lo = part.starts[b] as usize;
    let hi = part.starts[b + 1] as usize;
    let mut canon: Vec<u8> = Vec::with_capacity(64);
    let mut states: Vec<u64> = vec![0; state_words];
    for (slot, &row) in part.idx[lo..hi].iter().enumerate() {
        canon_row_bytes(t, shape, intern, row as usize, &mut canon);
        // SAFETY: the row's state block is state_words u64s (8-aligned by
        // the LaneAggTable state layout).
        unsafe {
            core::ptr::copy_nonoverlapping(
                t.row_states(row as usize).cast::<u64>().cast_const(),
                states.as_mut_ptr(),
                state_words,
            );
        }
        sink_canon_spill_append(&canon, part.hashes[lo + slot], &states, out);
    }
    Ok(())
}

/// Bucket-`b` CONTENT bytes of a canonical remainder (canonical images,
/// materialization-exact) — the combine pre-build estimate's key-content
/// term for the face the spill directory cannot answer.
pub fn sink_remainder_canon_content(rem: &SinkRemainder<'_>, b: usize) -> usize {
    // DIRECT arm: image bytes = the rows' own key bytes.
    if rem.direct {
        let (t, part) = (rem.table, rem.part);
        let lo = rem.part.starts[b] as usize;
        let hi = rem.part.starts[b + 1] as usize;
        let mut k8 = [0u8; 8];
        return part.idx[lo..hi]
            .iter()
            .map(|&row| {
                t.row_key_bytes(row as usize, &mut k8)
                    .expect("direct tables are non-nullable")
                    .len()
            })
            .sum();
    }
    let Some((shape, intern)) = rem.canon else { return 0 };
    let (t, part) = (rem.table, rem.part);
    let lo = part.starts[b] as usize;
    let hi = part.starts[b + 1] as usize;
    let mut canon: Vec<u8> = Vec::with_capacity(64);
    let mut total = 0usize;
    for &row in &part.idx[lo..hi] {
        canon_row_bytes(t, shape, intern, row as usize, &mut canon);
        total += canon.len();
    }
    total
}

/// Rebuild a single-bucket [`SinkRun`] from spilled bytes: every row lands
/// in bucket `b`, insertion order = file order (= flush order, the
/// first-seen discipline). Fail-closed on a torn record.
pub fn sink_run_from_spill(
    b: usize,
    key_words: usize,
    state_words: usize,
    bytes: &[u8],
) -> PgResult<SinkRun> {
    let row = sink_spill_row_bytes(key_words, state_words);
    if bytes.len() % row != 0 {
        return Err(sink_shape_error("torn spill record (partial row)"));
    }
    let n = bytes.len() / row;
    let mut keys: Vec<u64> = Vec::with_capacity(n * key_words);
    let mut states: Vec<u64> = Vec::with_capacity(n * state_words);
    let mut off = 0usize;
    for _ in 0..n {
        for _ in 0..key_words {
            keys.push(u64::from_ne_bytes(bytes[off..off + 8].try_into().unwrap()));
            off += 8;
        }
        for _ in 0..state_words {
            states.push(u64::from_ne_bytes(bytes[off..off + 8].try_into().unwrap()));
            off += 8;
        }
    }
    let mut starts: Vec<u32> = Vec::with_capacity(SINK_NBUCKETS + 1);
    for i in 0..=SINK_NBUCKETS {
        starts.push(if i > b { n as u32 } else { 0 });
    }
    // Word modes only: the M3.5 spill record contract predates bytes-mode
    // (canonical text) shapes — the spill arm's admission is word-keyed.
    Ok(SinkRun {
        key_words,
        key_ends: Vec::new(),
        state_words,
        starts,
        keys,
        states,
        null_states: None,
        key_offs: Vec::new(),
        key_bytes: Vec::new(),
        hashes: Vec::new(),
        gid_gen: 0,
    })
}

/// Serialize bucket-`b`'s REMAINDER rows (via the SEAL partition index)
/// into the spill record contract.
pub fn sink_remainder_spill_bucket(
    t: &LaneAggTable,
    part: &SinkPart,
    b: usize,
    out: &mut Vec<u8>,
) {
    let key_words = table_key_words(t);
    let state_words = t.state_bytes() / 8;
    let lo = part.starts[b] as usize;
    let hi = part.starts[b + 1] as usize;
    out.reserve((hi - lo) * sink_spill_row_bytes(key_words, state_words));
    for &row in &part.idx[lo..hi] {
        let [w0, w1] =
            row_key_words(t, row as usize).expect("partition indexes only non-NULL rows");
        out.extend_from_slice(&w0.to_ne_bytes());
        if key_words == 2 {
            out.extend_from_slice(&w1.to_ne_bytes());
        }
        let states = t.row_states(row as usize).cast_const().cast::<u64>();
        for w in 0..state_words {
            // SAFETY: the row's state block is state_words u64s (8-aligned
            // LaneAggTable state layout).
            out.extend_from_slice(&unsafe { *states.add(w) }.to_ne_bytes());
        }
    }
}

/// The remainder table's NULL-group state block, if any (the combine's own
/// row-scan discipline, extracted for the split path).
pub fn sink_remainder_null_block(t: &LaneAggTable) -> Option<Vec<u64>> {
    let state_words = t.state_bytes() / 8;
    for row in 0..t.nrows() {
        if row_key_words(t, row).is_none() {
            let mut block = vec![0u64; state_words];
            // SAFETY: as above.
            unsafe {
                core::ptr::copy_nonoverlapping(
                    t.row_states(row).cast::<u64>().cast_const(),
                    block.as_mut_ptr(),
                    state_words,
                );
            }
            return Some(block);
        }
    }
    None
}

/// Route raw spill records into 256 SUB-buckets by the hash byte `depth`
/// levels below the top-8 (depth 1 = bits 48..56). The M3.5 recursive
/// combine-split law: sub-partitioning a bucket by strictly deeper bits of
/// the SAME hash partitions its groups exactly. Fail-closed on torn input.
pub fn sink_route_records(
    bytes: &[u8],
    key_words: usize,
    state_words: usize,
    depth: u32,
    out: &mut [Vec<u8>],
) -> PgResult<()> {
    debug_assert_eq!(out.len(), SINK_NBUCKETS);
    debug_assert!((1..=6).contains(&depth), "sub-bucket depth out of range");
    let row = sink_spill_row_bytes(key_words, state_words);
    if bytes.len() % row != 0 {
        return Err(sink_shape_error("torn spill record (partial row) in split"));
    }
    let shift = 56 - 8 * depth;
    let mut off = 0usize;
    while off < bytes.len() {
        let w0 = u64::from_ne_bytes(bytes[off..off + 8].try_into().unwrap());
        let w1 = if key_words == 2 {
            u64::from_ne_bytes(bytes[off + 8..off + 16].try_into().unwrap())
        } else {
            0
        };
        let s = ((sink_hash(w0, w1) >> shift) & 0xFF) as usize;
        out[s].extend_from_slice(&bytes[off..off + row]);
        off += row;
    }
    Ok(())
}

/// A rows-free run carrying only a spilled NULL-group block (absorbed by
/// the [`SINK_NULL_BUCKET`] combine like any run's null face).
pub fn sink_null_only_run(key_words: usize, state_words: usize, block: Vec<u64>) -> SinkRun {
    debug_assert_eq!(block.len(), state_words);
    SinkRun {
        key_words,
        key_ends: Vec::new(),
        state_words,
        starts: vec![0; SINK_NBUCKETS + 1],
        keys: Vec::new(),
        states: Vec::new(),
        null_states: Some(block),
        key_offs: Vec::new(),
        key_bytes: Vec::new(),
        hashes: Vec::new(),
        gid_gen: 0,
    }
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
    /// Canonical (bytes-mode) shapes: slot i's [`sink_hash_bytes`] over
    /// `idx[i]`'s canonical bytes (parallel to `idx`) — computed by the
    /// SEAL partition anyway and carried so the combine's remainder probe
    /// reuses it instead of re-hashing. Empty in word modes.
    pub hashes: Vec<u64>,
}

impl SinkPart {
    /// Retained footprint (R3 accounting: the SEAL index lives until the
    /// combine set finishes and is charged like a run).
    pub fn bytes(&self) -> usize {
        (self.starts.capacity() + self.idx.capacity()) * core::mem::size_of::<u32>()
            + self.hashes.capacity() * 8
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
    SinkPart { starts, idx, has_null, hashes: Vec::new() }
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
    /// [`AvgInt8`](Self::AvgInt8) under the avgpack lane: the state is the
    /// PACKED inline `[count, sum]` image in the transno's own 16-byte
    /// state slot — SELF-CONTAINED (no aggcontext pointer, no null flags;
    /// AvgInt8 states are never SQL-null and `count == 0` encodes the
    /// all-NULL-input group). Combine = unconditional element adds; the
    /// new-group verbatim block copy IS the correct seed.
    AvgInt8Packed,
    /// min/max(text) — `text_smaller` 459 / `text_larger` 458 over a TEXT
    /// transvalue (SE-T2AGG CAR B, `PGRUST_LANE_V2_AGG_STRMINMAX`, default ON
    /// since the GL-STRMM-2 flip): the survivor is a live plain varlena in the
    /// SOURCE TABLE'S OWN `StrStateArena` — not in a worker aggcontext, which
    /// is what GL-SINKCRASH-2 had to fix on every drain (byref accounting via
    /// [`sink_combines_byref`]). The lifetime argument is NOT PolyInt128's:
    /// these transvalues do not rely on a helper's context outliving the
    /// combine, they are copied into the destination bucket's own store at
    /// insert (`sink_own_new_varlena`) and at both adopt points.
    /// Combine is memcmp + length tiebreak pick-pointer
    /// (`varstrfastcmp_c` — the merge.rs `CombineKind::VarlenaMinMax` kernel
    /// verbatim), admitted under memcmp-tier collations only
    /// (`str_collation_safe`), so ties are byte-equal and the pick is
    /// unobservable. Emit deep-copies the survivor image into the EmitBuf
    /// arena ([`SinkEmitCol::VarlenaTrans`]) — nothing pointer-shaped ever
    /// crosses to the leader.
    VarlenaMinMax { larger: bool },
}

/// SE-T2AGG CAR B kill/arm switch (`PGRUST_LANE_V2_AGG_STRMINMAX`, DEFAULT
/// ON since the GL-STRMM-2 flip; OFF iff exactly `0`/`off` — the t35/t36
/// flipped-kill idiom, a typo\'d kill leaves the arm live).
/// SAME spelling as the m5_suppress probe half (`agg_strminmax_enabled`):
/// both read sites flip together (the AGG_POLY knob-coherence law — a keyed
/// shape whose vocabulary is disarmed here would suppress a Gather and land
/// on the serial arm).
pub fn sink_strminmax_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_AGG_STRMINMAX").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

// ---------------------------------------------------------------------------
// Post-aggregate emit filter (HAVING; stragg-coverage inc-1).
// ---------------------------------------------------------------------------

/// stragg-coverage HAVING car knob (`PGRUST_LANE_V2_AGG_HAVING`):
/// **DEFAULT ON** since the GL-STRAGG-2 flip (t43; both cars together per
/// the letter). Kill spellings exactly `0|off` (the t35 flipped-kill
/// idiom). SAME spelling as the m5 probe half (`agg_having_enabled` in
/// m5_suppress.rs): both read sites flip together (knob-coherence law — a
/// probe that suppressed a post-aggregate-filtered shape this gate
/// refuses would land it on the serial rerun).
pub fn sink_having_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_AGG_HAVING").as_deref(),
            Ok("0") | Ok("off")
        )
    })
}

/// `count(*)` — the ONE aggregate whose transvalue the emit filter reads
/// (finalfn-free byval int8 word; the transvalue IS the final value, and a
/// count trans initializes to non-null 0, so a group can never carry a
/// NULL count). pg_proc OID of record (vendored REL 18.3 pg_proc.dat).
const F_COUNT_STAR_SINK: Oid = 2803;

/// int8-family comparison semantics, widened to exact i64 operands. The
/// admitted operator FUNCTIONS are the int8/int84/int48 comparison rows of
/// the canonical fmgr table (fmgr_core canonical.rs: 467-472 int8×int8,
/// 474-479 int8×int4, 852-857 int4×int8) — all six compare exact signed
/// values, so one widened i64 comparison is each C core verbatim.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HavingCmp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl HavingCmp {
    /// `a <cmp> b == b <mirror> a` — the argument-swap identity for quals
    /// spelled `Const <op> count(*)`.
    fn mirror(self) -> Self {
        match self {
            HavingCmp::Lt => HavingCmp::Gt,
            HavingCmp::Gt => HavingCmp::Lt,
            HavingCmp::Le => HavingCmp::Ge,
            HavingCmp::Ge => HavingCmp::Le,
            HavingCmp::Eq => HavingCmp::Eq,
            HavingCmp::Ne => HavingCmp::Ne,
        }
    }

    fn eval(self, a: i64, b: i64) -> bool {
        match self {
            HavingCmp::Eq => a == b,
            HavingCmp::Ne => a != b,
            HavingCmp::Lt => a < b,
            HavingCmp::Le => a <= b,
            HavingCmp::Gt => a > b,
            HavingCmp::Ge => a >= b,
        }
    }
}

/// The comparison an operator FUNCTION oid names (the int8-family rows
/// above); `None` = outside the vocabulary, refuse.
fn having_cmp_of(funcid: Oid) -> Option<HavingCmp> {
    match funcid {
        467 | 474 | 852 => Some(HavingCmp::Eq),
        468 | 475 | 853 => Some(HavingCmp::Ne),
        469 | 476 | 854 => Some(HavingCmp::Lt),
        470 | 477 | 855 => Some(HavingCmp::Gt),
        471 | 478 | 856 => Some(HavingCmp::Le),
        472 | 479 | 857 => Some(HavingCmp::Ge),
        _ => None,
    }
}

/// The emit-time post-aggregate filter: exactly ONE `count(*) <cmp> Const`
/// qual term, evaluated natively on the group's int8 transvalue at emit —
/// groups failing it never leave the sink. C-parity: the serial path runs
/// the HAVING qual per group after finalize and before projection, and a
/// non-TRUE verdict drops the group; for this class the count transvalue
/// equals the finalized value, so the filtered emit is byte-identical.
#[derive(Clone, Copy)]
pub struct SinkHavingFilter {
    transno: u32,
    cmp: HavingCmp,
    rhs: i64,
}

/// Compile the node's qual into the ONE admitted emit filter.
/// `Ok(None)` = no qual at all; `Err(())` = a qual outside the vocabulary
/// (or the knob disarmed) — the caller refuses the sink exactly as the
/// historical `node.qual.is_some()` gate did. Fail-closed gates, in order:
/// the knob; a single implicitly-ANDed term; an OpExpr whose function is an
/// int8-family comparison; one side a bare undecorated `count(*)` Aggref
/// resolving to a finalfn-free byval INT8 peragg outside any avgpack slot;
/// the other side a non-null int-family Const.
fn having_emit_filter(node: &AggStateData<'_>) -> Result<Option<SinkHavingFilter>, ()> {
    if node.qual.is_none() && node.plan.plan.qual.is_nil() {
        return Ok(None);
    }
    if !sink_having_enabled() {
        return Err(());
    }
    if node.plan.plan.qual.len() != 1 {
        return Err(());
    }
    let Some(op) = node.plan.plan.qual.nth(0).as_op_expr() else { return Err(()) };
    if op.opretset || op.args.len() != 2 {
        return Err(());
    }
    let Some(base) = having_cmp_of(op.opfuncid) else { return Err(()) };
    let (a, b) = (op.args.nth(0), op.args.nth(1));
    let (aggside, constside, cmp) =
        if a.as_aggref().is_some() { (a, b, base) } else { (b, a, base.mirror()) };
    let Some(ar) = aggside.as_aggref() else { return Err(()) };
    if ar.aggfnoid != F_COUNT_STAR_SINK
        || !ar.args.is_nil()
        || ar.aggfilter.is_some()
        || !ar.aggorder.is_nil()
        || !ar.aggdistinct.is_nil()
        || !ar.aggdirectargs.is_nil()
        || ar.agglevelsup != 0
        || ar.aggno < 0
        || ar.aggno as usize >= node.peragg.len()
    {
        return Err(());
    }
    let pa = &node.peragg[ar.aggno as usize];
    if pa.finalfn.is_some()
        || !pa.direct_args.is_empty()
        || pa.aggref.aggtranstype != HAVING_INT8OID
        || !node.trans_typ[pa.transno as usize].byval
        // Belt: an avgpack'd slot is a {count,sum} image, not an
        // AggPerGroup (count states never pack — avg-only machinery).
        || avgpack_of(node.avgpack_shape_mask, pa.transno)
    {
        return Err(());
    }
    let Some(c) = constside.as_const() else { return Err(()) };
    if c.constisnull {
        return Err(());
    }
    let rhs = match c.consttype {
        HAVING_INT8OID => c.constvalue.as_i64(),
        HAVING_INT4OID => i64::from(c.constvalue.as_i32()),
        HAVING_INT2OID => i64::from(c.constvalue.as_i16()),
        _ => return Err(()),
    };
    Ok(Some(SinkHavingFilter { transno: pa.transno, cmp, rhs }))
}

const HAVING_INT8OID: Oid = 20;
const HAVING_INT2OID: Oid = 21;
const HAVING_INT4OID: Oid = 23;

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
/// `text_larger` 458 / `text_smaller` 459 — min/max(text)'s transition AND
/// combine fns (pg_aggregate: aggcombinefn == aggtransfn for both); TEXT
/// transtype (SE-T2AGG CAR B).
const F_TEXT_LARGER_FN: Oid = 458;
const F_TEXT_SMALLER_FN: Oid = 459;
const SINK_TEXTOID: Oid = 25;

/// Resolve every transno's catalog combine function, fail-closed:
/// `Ok(None)` = a transition refuses the sink (unknown state class, missing
/// or non-whitelist combinefn, DISTINCT/ORDER BY qualifiers) — the caller
/// falls back to the serial arm. Never errors on shape; only on catalog
/// access. Admitted classes: byval whitelist, PolyInt128 (avg/sum int8),
/// AvgInt8 (avg int2/int4), VarlenaMinMax (min/max(text)) — the three byref
/// classes finalize at emit ([`sink_emit_bucket`]), so nothing pointer-shaped
/// ever reaches the leader. PolyInt128/AvgInt8 transvalues stay owned by the
/// drive-pinned worker aggcontexts; VarlenaMinMax transvalues are re-homed
/// into the destination bucket's own store ([`CombinedBucket`]).
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
            // avgpack: the node-build mask decides packedness — the SAME
            // deterministic value a worker sink build adopts as its table
            // representation (F1 leader/worker-verdict law; both sides
            // compute it from the plan + the process-constant kill switch).
            if avgpack_of(node.avgpack_shape_mask, pa.transno) {
                SinkCombineKind::AvgInt8Packed
            } else {
                SinkCombineKind::AvgInt8
            }
        } else if node.trans_typ[transno].byval
            && crate::merge::COMBINE_WHITELIST.contains(&shape.aggcombinefn)
        {
            SinkCombineKind::Byval
        } else if aggref.aggtranstype == SINK_TEXTOID
            && matches!(shape.aggcombinefn, F_TEXT_SMALLER_FN | F_TEXT_LARGER_FN)
            && sink_strminmax_enabled()
            && ::lanefold::str_collation_safe(aggref.inputcollid)
        {
            // SE-T2AGG CAR B: min/max(text) under a memcmp-tier collation
            // only (fail-closed on collation weirdness — nondeterministic /
            // libc/ICU collations keep the historical refusal; bpchar never
            // matches, its transtype is BPCHAR).
            SinkCombineKind::VarlenaMinMax { larger: shape.aggcombinefn == F_TEXT_LARGER_FN }
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

/// Whether any transno's state is byref (PolyInt128 / AvgInt8 /
/// VarlenaMinMax): the worker drain adds the aggcontext subtree to its
/// budget accounting exactly when this holds (byref states live there, not
/// in the table rows).
pub fn sink_combines_byref(combines: &[SinkCombineFn]) -> bool {
    combines.iter().any(|c| {
        // avgpack: packed states live INSIDE the table rows (self-contained
        // words) — nothing of theirs is in the aggcontext, so they do not
        // put the drain on byref accounting. This is the byref-floor kill:
        // pure-packed shapes stop counting the aggcontext subtree against
        // the budget, and flush/spill drain all live pressure.
        !matches!(c.kind, SinkCombineKind::Byval | SinkCombineKind::AvgInt8Packed)
    })
}

/// C advance_combine over two state blocks (`combine_one_par`'s thread-
/// native discipline): strict adopt-or-skip, then — Byval — one bare
/// fn-pointer call, or — the aggcontext byref classes — the combinefn's
/// exact arithmetic core run natively (the fmgr fns demand an agg context to
/// allocate their NULL-dst state; the sink adopts the src pointer instead,
/// identical field values, consumed exactly once). VarlenaMinMax is the
/// exception: its sources are owned by a SOURCE Local, so C's copy
/// discipline is followed literally — `datumCopy` into the destination store
/// on the no-value branch (advance_combine_function's `noTransValue` arm),
/// copy-new-then-free-old when the source wins (`ExecAggCopyTransValue`).
/// `dst` is the bucket table's block (single writer — the claimed
/// partition); `src` feeds exactly once.
///
/// # Safety
/// Both blocks hold `combines.len()` live `AggPerGroup`s; non-null
/// PolyInt128/AvgInt8 transvalues are live states in worker aggcontexts
/// (alive through the combine — `drive_pinned` holds every helper to RG
/// settlement), uniquely reachable through their one feeding source. Every
/// non-null VarlenaMinMax transvalue in `dst` was allocated by `sa` (the
/// bucket-store invariant — [`sink_own_new_varlena`] establishes it at every
/// insertion); `sa` is `Some` whenever `combines` carries a VarlenaMinMax.
pub unsafe fn sink_combine_states(
    combines: &[SinkCombineFn],
    dst: *mut AggPerGroup,
    src: *const AggPerGroup,
    mut sa: Option<&mut StrStateArena>,
) -> PgResult<()> {
    for (transno, c) in combines.iter().enumerate() {
        // avgpack: packed slots carry the inline [count, sum] image, no
        // null flags — combine is int4_avg_combine's element adds,
        // unconditional (an all-NULL-input group holds {0,0}; adding zeros
        // is C's own arithmetic on its {0,0} transarray). Runs BEFORE the
        // flag-reading strict/adopt block below.
        if c.kind == SinkCombineKind::AvgInt8Packed {
            // SAFETY: caller contract — both blocks hold numtrans 16-byte
            // slots; this transno's slots are packed images (one plan).
            unsafe {
                let dw = dst.add(transno).cast::<i64>();
                let sw = src.add(transno).cast::<i64>();
                *dw = (*dw).wrapping_add(*sw);
                *dw.add(1) = (*dw.add(1)).wrapping_add(*sw.add(1));
            }
            continue;
        }
        // SAFETY: caller contract.
        let (d, s) = unsafe { (&mut *dst.add(transno), &*src.add(transno)) };
        if c.strict || c.kind != SinkCombineKind::Byval {
            if s.trans_value_is_null {
                continue;
            }
            if d.trans_value_is_null {
                // C's advance_combine_function noTransValue arm: datumCopy
                // into curaggcontext, never an adopt. Only VarlenaMinMax has
                // a source the destination does not already outlive.
                if matches!(c.kind, SinkCombineKind::VarlenaMinMax { .. }) {
                    let Some(sa) = sa.as_deref_mut() else {
                        return Err(sink_shape_error(
                            "text min/max sink combine without a destination transvalue store",
                        ));
                    };
                    // SAFETY: caller contract — `s.trans_value` is a live
                    // varlena image; the header class is validated before
                    // the copy reads VARSIZE_ANY.
                    unsafe {
                        text_trans_payload(s.trans_value)?;
                        d.trans_value = sa.copy(s.trans_value);
                    }
                } else {
                    d.trans_value = s.trans_value;
                }
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
            // Handled (with continue) before the flag-reading block above.
            SinkCombineKind::AvgInt8Packed => unreachable!(),
            // text_smaller/text_larger's exact pick under a memcmp-tier
            // collation (SE-T2AGG CAR B; the merge.rs VarlenaMinMax kernel
            // verbatim): memcmp + length tiebreak. C returns arg1 (dst) only
            // on a STRICT win, so ties take the src datum — ties are
            // byte-equal under the admitted collations, so either side gives
            // byte-identical output.
            SinkCombineKind::VarlenaMinMax { larger } => unsafe {
                // SAFETY: `d.trans_value` is a live plain varlena THIS `sa`
                // allocated (the bucket-store invariant); `s.trans_value` is
                // a live plain varlena owned by a SOURCE Local — readable for
                // the compare, never adopted. The payload reader validates
                // the header class (plain short / 4B-uncompressed) and errors
                // on anything else, so the store only ever copies images the
                // emit can re-read.
                let (dp, dl) = text_trans_payload(d.trans_value)?;
                let (sp, sl) = text_trans_payload(s.trans_value)?;
                let cmp = ::varlena::varstrfastcmp_c(
                    core::slice::from_raw_parts(dp, dl),
                    core::slice::from_raw_parts(sp, sl),
                );
                let keep_dst = if larger { cmp > 0 } else { cmp < 0 };
                if !keep_dst {
                    // C's post-combine ExecAggCopyTransValue: datumCopy the
                    // winner into the aggregate's own memory and pfree the
                    // superseded copy.
                    let Some(sa) = sa.as_deref_mut() else {
                        return Err(sink_shape_error(
                            "text min/max sink combine without a destination transvalue store",
                        ));
                    };
                    debug_assert!(
                        sa.owns(d.trans_value.as_usize()),
                        "bucket-store invariant: the superseded copy must be store-allocated"
                    );
                    d.trans_value = sa.replace(d.trans_value, s.trans_value);
                }
                d.no_trans_value = false;
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

/// Re-home a JUST-INSERTED state block's VarlenaMinMax transvalues into the
/// bucket table's own store. The block arrived as a verbatim image of a
/// source Local's block, so its text pointers belong to that Local; this is
/// C's `datumCopy` into `curaggcontext` at group creation.
///
/// THE BUCKET-STORE INVARIANT: every non-null VarlenaMinMax transvalue
/// reachable from a bucket table was allocated by that table's store. It
/// holds because this runs at every one of the table's insertion points, and
/// [`sink_combine_states`] only ever writes store copies afterwards. Emit and
/// the replace-free both depend on it.
///
/// All-byval shapes pay one `None` test (the store is armed only when
/// `combines` carries a VarlenaMinMax).
///
/// # Safety
/// `dst` holds `combines.len()` live `AggPerGroup`s; non-null text
/// transvalues point at live varlena images.
#[inline]
unsafe fn sink_own_new_varlena(
    combines: &[SinkCombineFn],
    sa: &mut Option<StrStateArena>,
    dst: *mut AggPerGroup,
) -> PgResult<()> {
    let Some(sa) = sa.as_mut() else { return Ok(()) };
    for (transno, c) in combines.iter().enumerate() {
        if !matches!(c.kind, SinkCombineKind::VarlenaMinMax { .. }) {
            continue;
        }
        // SAFETY: caller contract.
        let d = unsafe { &mut *dst.add(transno) };
        if d.trans_value_is_null {
            continue;
        }
        // SAFETY: a non-null text transvalue is a live varlena image; the
        // header class is validated before the copy reads VARSIZE_ANY.
        unsafe {
            text_trans_payload(d.trans_value)?;
            d.trans_value = sa.copy(d.trans_value);
        }
    }
    Ok(())
}

/// Checked text-transvalue payload (SE-T2AGG CAR B): pointer + length of the
/// content bytes of a plain short-header or 4B-uncompressed varlena image.
/// Compressed/external headers ERROR (fail-closed shape backstop — the
/// admitted cbstore feeds stage detoasted plain images, so the class is
/// unreachable; the error mirrors `int8_avg_trans_read`'s discipline).
///
/// # Safety
/// `d` is a non-null text transvalue datum pointing at a live, readable
/// varlena image.
unsafe fn text_trans_payload(d: Datum) -> PgResult<(*const u8, usize)> {
    use ::types_tuple::varatt;
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract — live varlena image, header readable.
    unsafe {
        if varatt::varatt_is_1b(p) {
            Ok((p.add(varatt::VARHDRSZ_SHORT), varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT))
        } else if varatt::varatt_is_4b_u(p) {
            Ok((p.add(varatt::VARHDRSZ), varatt::varsize_4b(p) - varatt::VARHDRSZ))
        } else {
            Err(sink_shape_error("non-plain text transvalue in a sink combine/emit"))
        }
    }
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
    /// STORE-ONCE canonical images (spankey step 2): row i's image at
    /// `store[offs[i]..offs[i+1]]`, built once at the accept-time hash
    /// extension. `Some` iff the Local's store covers every table row
    /// (kill switch off ⇒ `None` ⇒ the incumbent per-arrival
    /// `canon_row_bytes` rebuild — identical bytes by the stored-hash
    /// law). Self-contained copies, safe to read cross-thread with the
    /// table (the canonical IMAGE law).
    pub canon_store: Option<(&'a [u8], &'a [u32])>,
    /// GID-merge car (canonical shapes): the Local's CURRENT intern-table
    /// generation — remainder rows sit in the live table, so their packed
    /// words are generation-current by construction. 0 for word shapes.
    pub gid_gen: u64,
    /// DIRECT single-text arm (arena-strings inc-3): the remainder table is
    /// `KeyRepr::Bytes` keyed on the canonical images themselves — the
    /// combine/spill faces read key bytes per row via `row_key_bytes` and
    /// the SEAL-carried hashes (== the saved sink hashes), never a canon
    /// rebuild (`canon`/`canon_store` are None). The GID arm never applies
    /// (no packed image exists).
    pub direct: bool,
}

/// One Local's combine-visible faces: its spill-synthesized runs (epoch
/// order — spilled epochs happened BEFORE anything still in memory, so they
/// are visited first under the first-seen discipline), its in-memory
/// flushed runs (flush order), and its remainder table + SEAL partition.
pub struct SinkLocalView<'a> {
    /// Runs rebuilt from spilled epochs ([`sink_run_from_spill`] /
    /// [`sink_null_only_run`]); empty when the Local never spilled.
    pub spilled: &'a [SinkRun],
    pub runs: &'a [SinkRun],
    pub remainder: Option<SinkRemainder<'a>>,
}

impl SinkLocalView<'_> {
    /// All run faces in first-seen order.
    fn all_runs(&self) -> impl Iterator<Item = &SinkRun> {
        self.spilled.iter().chain(self.runs.iter())
    }
}

/// Merge bucket `b` across `locals` (slice order = worker-slot order) into a
/// fresh table: runs first (flush order, rows in insertion order), then the
/// remainder rows — the first-seen discipline. NULL blocks are absorbed only
/// in [`SINK_NULL_BUCKET`]. `state_bytes` and `key_words` are the sink's
/// (identical across all sources by construction — one worker plan);
/// `key_words == 0` = CANONICAL BYTES MODE (text-bearing shapes): the bucket
/// table keys on canonical byte strings ([`KeyRepr::Bytes`], length+content
/// compare — embedded NULs are safe).
///
/// Row count of bucket `b` across all faces (the combine's pre-build size
/// check reads this before allocating anything — M3.5 §3).
pub fn sink_bucket_row_count(b: usize, locals: &[SinkLocalView<'_>]) -> usize {
    let mut total = 0usize;
    for l in locals {
        for r in l.all_runs() {
            total += (r.starts[b + 1] - r.starts[b]) as usize;
        }
        if let Some(SinkRemainder { part: p, .. }) = &l.remainder {
            total += (p.starts[b + 1] - p.starts[b]) as usize;
        }
    }
    total
}

/// Per-(worker, generation) packed-words → merged-state map (GID-merge,
/// canon-sink car 2). Open addressing over the two packed key words; a
/// slot is LIVE iff its stamp equals the map's current stamp, so the per-
/// Local / per-generation clear is O(1) (stamp bump) — a table memset per
/// Local would scale with the bucket total × locals and dwarf the probes
/// this map removes. Sized once per combine claim off the bucket's total
/// row count.
struct GidMap {
    gen: Option<u64>,
    stamp: u32,
    mask: usize,
    /// (w0, w1, merged-row state block, stamp); live iff stamp matches.
    slots: Vec<(u64, u64, *mut u8, u32)>,
    len: usize,
}

impl GidMap {
    fn new(expected: usize) -> GidMap {
        let cap = if expected == 0 {
            0
        } else {
            (expected * 2).next_power_of_two().max(16)
        };
        GidMap {
            gen: None,
            stamp: 1,
            mask: cap.saturating_sub(1),
            slots: vec![(0, 0, core::ptr::null_mut(), 0); cap],
            len: 0,
        }
    }

    /// Forget everything (new Local) — O(1) stamp bump.
    fn clear(&mut self) {
        self.stamp = self.stamp.wrapping_add(1);
        if self.stamp == 0 {
            // Wrap: a stale slot could alias stamp 0 — one real sweep.
            self.slots.fill((0, 0, core::ptr::null_mut(), 0));
            self.stamp = 1;
        }
        self.len = 0;
        self.gen = None;
    }

    /// Enter generation `gen`: a boundary crossing clears the map (packed
    /// words are ambiguous across intern resets).
    fn roll(&mut self, gen: u64) {
        if self.gen != Some(gen) {
            self.clear();
            self.gen = Some(gen);
        }
    }

    #[inline]
    fn find(&self, w: [u64; 2]) -> Option<*mut u8> {
        if self.slots.is_empty() {
            return None;
        }
        let mut i = (sink_hash(w[0], w[1]) as usize) & self.mask;
        loop {
            let (w0, w1, p, s) = self.slots[i];
            if s != self.stamp {
                return None;
            }
            if w0 == w[0] && w1 == w[1] {
                debug_assert!(!p.is_null());
                return Some(p);
            }
            i = (i + 1) & self.mask;
        }
    }

    #[inline]
    fn insert(&mut self, w: [u64; 2], p: *mut u8) {
        debug_assert!(!p.is_null());
        if self.slots.is_empty() || self.len * 2 >= self.slots.len() {
            // Sized off the bucket total up front; a crossing (spilled
            // duplicates inflating arrivals past the estimate) simply stops
            // caching — correctness never depends on an insert landing.
            return;
        }
        let mut i = (sink_hash(w[0], w[1]) as usize) & self.mask;
        loop {
            let (w0, w1, q, s) = self.slots[i];
            if s != self.stamp {
                self.slots[i] = (w[0], w[1], p, self.stamp);
                self.len += 1;
                return;
            }
            if w0 == w[0] && w1 == w[1] {
                debug_assert_eq!(q, p, "one merged row per (gen, words)");
                return;
            }
            i = (i + 1) & self.mask;
        }
    }
}

/// One combined bucket: the merged table plus the store that owns its
/// by-ref min/max(text) transvalues. The store MUST outlive every read of
/// the table's state blocks — emit included — so the two are one value and
/// Drop releases the strings only once the table itself is gone.
///
/// `str_store` is armed only for shapes whose `combines` carry a
/// `VarlenaMinMax` transno; all-byval buckets keep the allocation-free
/// `None`.
pub struct CombinedBucket {
    pub table: LaneAggTable,
    str_store: Option<StrStateArena>,
}

impl CombinedBucket {
    /// Detach the store, for a caller that hands the table's state blocks
    /// onward as a run and must keep their backing alive independently (the
    /// two-level pass-A partial).
    pub fn into_str_store(self) -> Option<StrStateArena> {
        self.str_store
    }

    /// Retained store bytes — 0 when unarmed (the partial's byref budget
    /// accounting term).
    pub fn str_store_bytes(&self) -> usize {
        self.str_store.as_ref().map_or(0, StrStateArena::bytes)
    }
}

impl core::ops::Deref for CombinedBucket {
    type Target = LaneAggTable;
    #[inline]
    fn deref(&self) -> &LaneAggTable {
        &self.table
    }
}

pub fn sink_combine_bucket(
    b: usize,
    key_words: usize,
    state_bytes: usize,
    locals: &[SinkLocalView<'_>],
    combines: &[SinkCombineFn],
) -> PgResult<CombinedBucket> {
    sink_combine_bucket_impl(
        b,
        key_words,
        state_bytes,
        locals,
        combines,
        sink_gid_merge_enabled(),
        sink_combine16_enabled(),
    )
}

/// [`sink_combine_bucket`] with the GID-map and combine16 flat-table
/// decisions injected (unit tests exercise both lanes regardless of the
/// process env).
fn sink_combine_bucket_impl(
    b: usize,
    key_words: usize,
    state_bytes: usize,
    locals: &[SinkLocalView<'_>],
    combines: &[SinkCombineFn],
    gid_enabled: bool,
    flat: bool,
) -> PgResult<CombinedBucket> {
    debug_assert!(b < SINK_NBUCKETS);
    let mut total = 0usize;
    // Bytes mode (combine16): the runs' key-byte volume for this bucket, an
    // O(faces) directory read. Run ranges are exact image bytes (a slight
    // over-count vs the arena — packed ≤8 B keys never land there — the
    // safe direction). Remainder-face images are NOT counted (they
    // materialize from shape + intern at absorb time; no cheap directory
    // length exists) — a hint is not a cap, the arena extends past it
    // freely, and the flush-heavy shapes where arena volume is material
    // are run-dominated. Feeds `reserve_arena` on the flat path only.
    let mut key_bytes = 0usize;
    for l in locals {
        for r in l.all_runs() {
            total += (r.starts[b + 1] - r.starts[b]) as usize;
            if key_words == 0 {
                key_bytes += r.bucket_key_bytes(b);
            }
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
    let mut t = if flat {
        let mut t = LaneAggTable::with_flat_capacity(
            repr,
            state_bytes,
            total.max(4),
            HashKind::best(),
            layout,
        );
        if key_words == 0 {
            t.reserve_arena(key_bytes);
        }
        t
    } else {
        LaneAggTable::with_config(
            repr,
            state_bytes,
            total.max(4),
            HashKind::best(),
            layout,
        )
    };
    let state_words = state_bytes / 8;
    // The destination-owned text store, armed only for min/max(text) shapes
    // (all-byval buckets stay allocation-free). Every VarlenaMinMax
    // transvalue this table ends up holding is a copy IT allocated — the
    // bucket-store invariant, see [`sink_own_new_varlena`].
    let mut sa: Option<StrStateArena> = combines
        .iter()
        .any(|c| matches!(c.kind, SinkCombineKind::VarlenaMinMax { .. }))
        .then(StrStateArena::default);

    // Shared merge tail: seed a new group's block or combine into the
    // existing one.
    let merge_states = |pr: ::lanetable::Probe,
                        src: *const u64,
                        sa: &mut Option<StrStateArena>|
     -> PgResult<()> {
        if pr.is_new {
            // SAFETY: fresh zeroed state block of state_words u64s; src is a
            // live block of the same layout (one worker plan). The copy takes
            // the source's text POINTERS, so the block is immediately
            // re-homed into this table's store.
            unsafe {
                core::ptr::copy_nonoverlapping(src, pr.states.cast::<u64>(), state_words);
                return sink_own_new_varlena(combines, sa, pr.states.cast::<AggPerGroup>());
            }
        }
        // SAFETY: both blocks hold numtrans pergroups (combines.len() ==
        // numtrans); dst is uniquely reachable through this claimed bucket.
        unsafe {
            sink_combine_states(
                combines,
                pr.states.cast::<AggPerGroup>(),
                src.cast::<AggPerGroup>(),
                sa.as_mut(),
            )
        }
    };

    let absorb = |t: &mut LaneAggTable,
                  sa: &mut Option<StrStateArena>,
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
        merge_states(pr, src, sa)
    };
    // Bytes-mode probes reuse the flush/SEAL-computed sink hash (carried in
    // the run / part) instead of re-hashing every arrival's byte image —
    // probe_bytes consumes the hash's low bits (slot) and bits 32..48
    // (salt), so the sink hash's constant-per-bucket top byte never hurts,
    // and one hash per (row, table) stays consistent across all probes.
    // Returns the row's merged state block for the GID map below.
    let absorb_bytes = |t: &mut LaneAggTable,
                        sa: &mut Option<StrStateArena>,
                        key: &[u8],
                        h: u64,
                        src: *const u64|
     -> PgResult<*mut u8> {
        let pr = t.probe_bytes(key, h);
        let states = pr.states;
        merge_states(pr, src, sa)?;
        Ok(states)
    };

    // GID MERGE (canon-sink car 2): repeat arrivals of one (worker,
    // generation, packed-words) triple resolve through a per-Local word map
    // instead of re-probing canonical bytes — within a generation a
    // worker's packed key words biject onto its groups (intern ids are
    // insert-once), so a map hit combines straight into the merged row's
    // state block (identical arithmetic, identical rows: byte-invisible).
    // The map resets per Local and at every generation boundary; faces
    // without carried words (spill replay) always bytes-probe.
    let use_gid = key_words == 0 && gid_enabled;
    let mut gmap = GidMap::new(if use_gid { total } else { 0 });

    // Canonical remainder scratch (bytes mode only).
    let mut canon: Vec<u8> = Vec::new();
    let spk = crate::spankey::spankey_ctr_enabled();
    for l in locals {
        gmap.clear();
        let spk_t0 = spk.then(std::time::Instant::now);
        for r in l.all_runs() {
            debug_assert_eq!(r.key_words, key_words);
            debug_assert_eq!(r.state_words, state_words);
            let lo = r.starts[b] as usize;
            let hi = r.starts[b + 1] as usize;
            let gids = use_gid && !r.keys.is_empty();
            if gids {
                gmap.roll(r.gid_gen);
            }
            for i in lo..hi {
                let src = unsafe {
                    // SAFETY: states holds nrows state blocks (run layout).
                    r.states.as_ptr().add(i * state_words)
                };
                if key_words == 0 {
                    if gids {
                        let w = [r.keys[i * 2], r.keys[i * 2 + 1]];
                        if let Some(dst) = gmap.find(w) {
                            // SAFETY: dst is a live merged-row state block
                            // (LaneAggTable rows are allocation-stable across
                            // inserts); src feeds exactly once.
                            unsafe {
                                sink_combine_states(
                                    combines,
                                    dst.cast::<AggPerGroup>(),
                                    src.cast::<AggPerGroup>(),
                                    sa.as_mut(),
                                )?;
                            }
                            continue;
                        }
                        let dst =
                            absorb_bytes(&mut t, &mut sa, r.key_slice(i), r.hashes[i], src)?;
                        gmap.insert(w, dst);
                        continue;
                    }
                    absorb_bytes(&mut t, &mut sa, r.key_slice(i), r.hashes[i], src)?;
                } else {
                    let w0 = r.keys[i * key_words];
                    let w1 = if key_words == 2 { r.keys[i * key_words + 1] } else { 0 };
                    absorb(&mut t, &mut sa, Some([w0, w1]), src)?;
                }
            }
            if b == SINK_NULL_BUCKET {
                if let Some(block) = &r.null_states {
                    debug_assert_ne!(key_words, 0, "bytes-mode runs never carry NULL blocks");
                    absorb(&mut t, &mut sa, None, block.as_ptr())?;
                }
            }
        }
        crate::spankey::spankey_lap(&crate::spankey::SPANKEY_CTRS.combine_runs_ns, spk_t0);
        let spk_t0 = spk.then(std::time::Instant::now);
        let mut spk_rows = 0u64;
        let mut spk_bytes = 0u64;
        if let Some(rem) = &l.remainder {
            let (rt, part) = (rem.table, rem.part);
            debug_assert_eq!(rt.state_bytes(), t.state_bytes());
            let lo = part.starts[b] as usize;
            let hi = part.starts[b + 1] as usize;
            if key_words == 0 && rem.direct {
                // DIRECT single-text remainder (arena-strings inc-3): the
                // rows already ARE the canonical images and the SEAL carried
                // their saved sink hashes — read both back verbatim (no
                // canon face, no rebuild). The GID arm never applies: direct
                // rows carry no packed image (byte-invisible either way —
                // the map only short-circuits probes).
                let mut k8 = [0u8; 8];
                for (slot, &row) in part.idx[lo..hi].iter().enumerate() {
                    let src: *const u64 = rt.row_states(row as usize).cast_const().cast();
                    let img = rt
                        .row_key_bytes(row as usize, &mut k8)
                        .expect("direct tables are non-nullable");
                    if spk {
                        spk_rows += 1;
                        spk_bytes += img.len() as u64;
                    }
                    absorb_bytes(&mut t, &mut sa, img, part.hashes[lo + slot], src)?;
                }
                debug_assert!(!part.has_null, "direct shapes are non-nullable");
            } else if key_words == 0 {
                let (shape, intern) = rem
                    .canon
                    .ok_or_else(|| sink_shape_error("bytes-mode remainder without a canon face"))?;
                // STORE-ONCE (spankey step 2): read the accept-time image
                // verbatim instead of the per-arrival rebuild (word-unpack
                // + intern-reverse-chase + tail assembly). Identical bytes
                // by the stored-hash law; kill switch off ⇒ `None` ⇒ the
                // incumbent rebuild.
                let stored = rem.canon_store;
                if use_gid {
                    gmap.roll(rem.gid_gen);
                }
                for (slot, &row) in part.idx[lo..hi].iter().enumerate() {
                    let src: *const u64 = rt.row_states(row as usize).cast_const().cast();
                    if use_gid {
                        let w = mk_words_of(rt, shape, row as usize);
                        if let Some(dst) = gmap.find(w) {
                            // Map hit: the group's canonical image never
                            // materializes at all for this arrival.
                            // SAFETY: as the run-face GID arm.
                            unsafe {
                                sink_combine_states(
                                    combines,
                                    dst.cast::<AggPerGroup>(),
                                    src.cast::<AggPerGroup>(),
                                    sa.as_mut(),
                                )?;
                            }
                            continue;
                        }
                        let img: &[u8] = match stored {
                            Some((sb, so)) => {
                                &sb[so[row as usize] as usize..so[row as usize + 1] as usize]
                            }
                            None => {
                                canon_row_bytes(rt, shape, intern, row as usize, &mut canon);
                                &canon
                            }
                        };
                        if spk {
                            spk_rows += 1;
                            spk_bytes += img.len() as u64;
                        }
                        let dst =
                            absorb_bytes(&mut t, &mut sa, img, part.hashes[lo + slot], src)?;
                        gmap.insert(w, dst);
                        continue;
                    }
                    let img: &[u8] = match stored {
                        Some((sb, so)) => {
                            &sb[so[row as usize] as usize..so[row as usize + 1] as usize]
                        }
                        None => {
                            canon_row_bytes(rt, shape, intern, row as usize, &mut canon);
                            &canon
                        }
                    };
                    if spk {
                        spk_rows += 1;
                        spk_bytes += img.len() as u64;
                    }
                    absorb_bytes(&mut t, &mut sa, img, part.hashes[lo + slot], src)?;
                }
                debug_assert!(!part.has_null, "canonical shapes are non-nullable");
            } else {
                debug_assert_eq!(table_key_words(rt), key_words);
                for &row in &part.idx[lo..hi] {
                    let kw = row_key_words(rt, row as usize)
                        .expect("partition indexes only non-NULL rows");
                    absorb(
                        &mut t,
                        &mut sa,
                        Some(kw),
                        rt.row_states(row as usize).cast_const().cast(),
                    )?;
                }
                if b == SINK_NULL_BUCKET && part.has_null {
                    // The remainder's NULL row: find it through the table's
                    // own out-of-band accessor path (row scan — one row max).
                    for row in 0..rt.nrows() {
                        if row_key_words(rt, row).is_none() {
                            absorb(
                                &mut t,
                                &mut sa,
                                None,
                                rt.row_states(row).cast_const().cast(),
                            )?;
                            break;
                        }
                    }
                }
            }
        }
        if spk {
            use crate::spankey::{spankey_add, spankey_lap, SPANKEY_CTRS as S};
            spankey_add(&S.combine_rem_rows, spk_rows);
            spankey_add(&S.combine_rem_bytes, spk_bytes);
            spankey_lap(&S.combine_rem_ns, spk_t0);
        }
    }
    Ok(CombinedBucket { table: t, str_store: sa })
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
    /// An Intern (text) component of a CANONICAL bytes-keyed table: the
    /// canonical key's tail region (after the `plan.fixed` image prefix)
    /// carries the text payload(s); `nth` names which tail (ordinal among
    /// the shape's Intern components — single-tail shapes carry the raw
    /// bytes, two+ tails are length-prefixed). Materialized as a 4B-header
    /// text varlena into the buf arena (nothing worker-owned crosses to the
    /// leader).
    MultiText { nth: u8 },
    /// A packed Numeric component (the `extract(minute ...)` ts-key class):
    /// `width` bytes at byte `off` decode through the canonical keypack form
    /// (`mk_numeric_key_decode` → `numeric_key_unpack`) into a NUMERIC image
    /// in the buf arena — byte-identical to the packed first-arrival datum
    /// by the keypack canonicality gates.
    MultiNumeric { off: u8, width: u8 },
    /// A constant tlist entry (the const-tlist `SELECT 1, URL, ...` class): the
    /// plan's Const datum, emitted verbatim on every row. Byval-only by
    /// admission (a byref image would need per-row arena copies — refused
    /// fail-closed), so nothing worker- or query-arena-owned crosses to the
    /// leader; NULL consts ride the isnull flag.
    ConstByval { value: Datum, isnull: bool },
    /// Aggregate result = the byval transvalue (no finalfn).
    Agg { transno: u32 },
    /// min/max(text) result = the byref TEXT transvalue, deep-copied into
    /// the buf arena at emit (SE-T2AGG CAR B; finalfn-none like `Agg`, but
    /// the survivor is a live worker-aggcontext varlena — the arena copy is
    /// what makes the published buf self-contained). Never table-adopted
    /// (`sink_emit_plan_all_byval` counts it byref).
    VarlenaTrans { transno: u32 },
    /// `avg(int2/int4)` (finalfn `int8_avg` 1964): {count,sum} int8[2]
    /// transarray → `ops::int64_avg_div` NUMERIC image into the buf arena
    /// (`BatchEmitCol::AvgInt8`'s exact core). `packed` = the avgpack
    /// inline representation (the transno's state slot IS the {count,sum}
    /// image; never SQL-null, `count == 0` finalizes to NULL exactly as
    /// C's `int8_avg` does on its {0,0} state).
    AvgInt8 { transno: u32, packed: bool },
    /// `avg(int8)` (finalfn `numeric_poly_avg` 3389): Int128AggState →
    /// `aggregates::numeric_poly_avg` image into the buf arena.
    AvgInt128 { transno: u32 },
    /// `sum(int8)` (finalfn `numeric_poly_sum` 3388): Int128AggState →
    /// `aggregates::numeric_poly_sum` image into the buf arena.
    SumInt128 { transno: u32 },
}

#[derive(Clone)]
pub struct SinkEmitPlan {
    pub width: u8,
    pub cols: Vec<SinkEmitCol>,
    /// CANONICAL (bytes-keyed) shapes: the fixed image prefix length
    /// (`shape.packed_bytes`) — rows split into image prefix + text tail(s).
    /// `None` = word-keyed tables.
    pub fixed: Option<u8>,
    /// CANONICAL shapes: Intern tail count (1 = the raw single tail, the
    /// historical image; 2+ = length-prefixed tails, canon-sink car 1).
    /// 0 = word-keyed tables.
    pub ntails: u8,
    /// Post-aggregate emit filter (the HAVING car): rows failing it never
    /// emit — EVERY emit driver consults [`SinkEmitPlan::row_admits`]
    /// before projecting a row. `None` = the historical unfiltered emit.
    pub filter: Option<SinkHavingFilter>,
}

impl SinkEmitPlan {
    /// Whether the plan carries a post-aggregate filter (the engagement
    /// vacates topn/freeze/adopt compositions on filtered plans — they all
    /// reason over UNFILTERED group sets).
    pub fn has_filter(&self) -> bool {
        self.filter.is_some()
    }

    /// The post-aggregate filter verdict for one table row. A count
    /// transvalue is never SQL-NULL (non-null 0 init), but a NULL fails
    /// closed exactly as SQL HAVING drops non-TRUE verdicts.
    #[inline]
    pub fn row_admits(&self, t: &LaneAggTable, row: usize) -> bool {
        let Some(f) = self.filter else { return true };
        // SAFETY: as emit_row's Agg arm — the row's state block holds
        // numtrans pergroups (bucket-table config = the sink's
        // state_bytes) and f.transno < numtrans by filter compile.
        let pg = unsafe {
            &*t.row_states(row).cast_const().cast::<AggPerGroup>().add(f.transno as usize)
        };
        !pg.trans_value_is_null && f.cmp.eval(pg.trans_value.as_i64(), f.rhs)
    }
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
    if node.skip_final {
        return None;
    }
    // Post-aggregate filtered grouped shapes (stragg-coverage inc-1): the
    // ONE admitted HAVING class compiles to an emit-row filter; every other
    // qual refuses exactly as the historical `node.qual.is_some()` gate
    // (the general finalize/project interpreter case).
    let filter = match having_emit_filter(node) {
        Ok(f) => f,
        Err(()) => return None,
    };
    for pa in node.peragg.iter() {
        if !pa.direct_args.is_empty() {
            return None;
        }
        match pa.finalfn.as_ref() {
            // Raw-transvalue emission requires a byval word; INTERNAL is
            // byval-but-pointer — refuse (batch_emit_resolve's gate).
            // SE-T2AGG CAR B: byref TEXT transvalues (min/max(text)) are
            // admitted knob-ON under memcmp-tier collations — the emit
            // deep-copies the survivor image into the buf arena
            // (SinkEmitCol::VarlenaTrans below).
            None => {
                let byval_ok = node.trans_typ[pa.transno as usize].byval
                    && pa.aggref.aggtranstype != INTERNALOID;
                let text_ok = pa.aggref.aggtranstype == SINK_TEXTOID
                    && !node.trans_typ[pa.transno as usize].byval
                    && sink_strminmax_enabled()
                    && ::lanefold::str_collation_safe(pa.aggref.inputcollid);
                if !byval_ok && !text_ok {
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
                    // car) emits the canonical tail as text; Numeric (the
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
                        MkCompKind::Intern => {
                            // Which canonical tail: the component's ordinal
                            // among the shape's Intern components (tail
                            // order == component order by construction).
                            let nth = shape
                                .intern_comps()
                                .position(|(cj, _)| cj == j)
                                .expect("Intern component is in intern_comps") as u8;
                            cols.push(SinkEmitCol::MultiText { nth });
                        }
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
                // The finalfn-none gate above proved byval-word OR the
                // knob-gated text min/max class (SE-T2AGG CAR B).
                None if !node.trans_typ[pa.transno as usize].byval => {
                    SinkEmitCol::VarlenaTrans { transno: pa.transno }
                }
                None => SinkEmitCol::Agg { transno: pa.transno },
                Some(f) => match f.fn_oid {
                    FINALFN_INT8_AVG => SinkEmitCol::AvgInt8 {
                        transno: pa.transno,
                        // avgpack: the same node-build mask the combine
                        // resolution and the worker table arm read.
                        packed: avgpack_of(node.avgpack_shape_mask, pa.transno),
                    },
                    FINALFN_POLY_AVG => SinkEmitCol::AvgInt128 { transno: pa.transno },
                    FINALFN_POLY_SUM => SinkEmitCol::SumInt128 { transno: pa.transno },
                    _ => return None,
                },
            };
            cols.push(col);
            continue;
        }
        if let Some(c) = te.expr.as_const() {
            // Const tlist entry (the `SELECT 1, URL, ...` class): byval
            // images only — the emit-buf and table drains copy the datum
            // verbatim per row; a byref const would need arena
            // materialization (refuse fail-closed, as before this arm).
            if !c.constbyval && !c.constisnull {
                return None;
            }
            cols.push(SinkEmitCol::ConstByval {
                value: if c.constisnull { Datum::null() } else { c.constvalue },
                isnull: c.constisnull,
            });
            continue;
        }
        return None;
    }
    let (fixed, ntails) = match key {
        SinkKeySpec::Multi(shape) if shape.intern_comp().is_some() => {
            (Some(shape.packed_bytes), shape.n_intern() as u8)
        }
        _ => (None, 0),
    };
    Some(SinkEmitPlan { width: key.width(), cols, fixed, ntails, filter })
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

/// Cross-table emit ACCUMULATOR (the M3.5 combine-split path emits one
/// table per sub-partition and concatenates — group order across
/// sub-partitions is a non-surface under the order-free posture). Fix-ups
/// stay UNRESOLVED until [`SinkEmitAcc::finish`], so byref outputs (numeric
/// finalize images, text tails) from every absorbed table land in ONE arena
/// and the datums resolve against its final heap buffer. The former
/// `SinkEmitBuf::append` copied resolved datums while dropping the source
/// buf's arena — a use-after-free for any byref emit column on the split
/// path (winners-phase2 finding; word-keyed spill shapes CAN carry
/// AvgInt8/AvgInt128 numeric images).
#[derive(Default)]
pub struct SinkEmitAcc {
    values: Vec<Datum>,
    nulls: Vec<bool>,
    nrows: usize,
    arena: Vec<u8>,
    fixups: Vec<(usize, usize)>,
}

impl SinkEmitAcc {
    /// Rows accumulated so far (the winners-only split path remaps its
    /// fragment candidates against this base before each absorb).
    pub fn nrows(&self) -> usize {
        self.nrows
    }

    /// Finalize+project EVERY row of `t` (insertion order — the merge's
    /// first-seen order) that passes the plan's post-aggregate filter,
    /// appending to the accumulator.
    pub fn emit_table(&mut self, plan: &SinkEmitPlan, t: &LaneAggTable) -> PgResult<()> {
        let n = t.nrows();
        self.values.reserve(n * plan.cols.len());
        self.nulls.reserve(n * plan.cols.len());
        for row in 0..n {
            if !plan.row_admits(t, row) {
                continue;
            }
            emit_row(plan, t, row, &mut self.values, &mut self.nulls, &mut self.arena, &mut self.fixups)?;
            self.nrows += 1;
        }
        Ok(())
    }

    /// Finalize+project ONLY `rows` of `t` (ascending, unique — the
    /// winners-only compact discipline of [`sink_emit_bucket_rows`]),
    /// appending to the accumulator. Row `rows[i]` becomes accumulator row
    /// `base + i` where `base` was `self.nrows()` before the call.
    pub fn emit_rows(&mut self, plan: &SinkEmitPlan, t: &LaneAggTable, rows: &[u32]) -> PgResult<()> {
        debug_assert!(rows.windows(2).all(|w| w[0] < w[1]), "rows sorted+unique");
        // Winners-only emission never composes with a post-aggregate filter
        // (the engagement vacates topn on filtered plans); the row gate
        // below is belt-and-suspenders.
        debug_assert!(plan.filter.is_none(), "winners emit on a filtered plan");
        self.values.reserve(rows.len() * plan.cols.len());
        self.nulls.reserve(rows.len() * plan.cols.len());
        for &row in rows {
            if !plan.row_admits(t, row as usize) {
                continue;
            }
            emit_row(plan, t, row as usize, &mut self.values, &mut self.nulls, &mut self.arena, &mut self.fixups)?;
            self.nrows += 1;
        }
        Ok(())
    }

    /// The arena is final — resolve the byref datums and seal the buf.
    pub fn finish(self) -> SinkEmitBuf {
        let SinkEmitAcc { mut values, nulls, nrows, arena, fixups } = self;
        for (i, off) in fixups {
            values[i] = Datum::from_usize(arena[off..].as_ptr() as usize);
        }
        SinkEmitBuf { values, nulls, nrows, arena }
    }
}

/// Resolve the `nth` text tail of a canonical key's tail region (the bytes
/// after the fixed image prefix). Single-tail shapes carry the raw payload
/// (the historical image); two+ tails are length-prefixed (`u32` LE len +
/// content, component order). Fail-closed on a malformed grammar — a
/// canonical key always decodes or the claim errors (never silent-wrong).
fn canon_tail(region: &[u8], ntails: u8, nth: u8) -> PgResult<&[u8]> {
    if ntails <= 1 {
        if nth != 0 {
            return Err(sink_shape_error("tail ordinal out of range on a single-tail key"));
        }
        return Ok(region);
    }
    let mut off = 0usize;
    for i in 0..ntails {
        if region.len() < off + 4 {
            return Err(sink_shape_error("canonical key tail truncated (len prefix)"));
        }
        let len = u32::from_le_bytes(region[off..off + 4].try_into().expect("4 bytes")) as usize;
        off += 4;
        if region.len() < off + len {
            return Err(sink_shape_error("canonical key tail truncated (content)"));
        }
        if i == nth {
            return Ok(&region[off..off + len]);
        }
        off += len;
    }
    Err(sink_shape_error("tail ordinal out of range on a multi-tail key"))
}

#[inline]
fn key_datum(width: u8, k: i64) -> Datum {
    match width {
        2 => Datum::from_i16(k as i16),
        4 => Datum::from_i32(k as i32),
        _ => Datum::from_i64(k),
    }
}

/// Append one 8-aligned byref image to the arena and record a (values
/// index, arena offset) fix-up, resolved after the arena stops growing
/// (Vec growth may move the heap buffer). Varlena consumers may read
/// 4-byte headers + aligned payloads — hence the 8-alignment.
fn push_image(
    values: &mut Vec<Datum>,
    nulls: &mut Vec<bool>,
    arena: &mut Vec<u8>,
    fixups: &mut Vec<(usize, usize)>,
    img: &[u8],
) {
    push_image2(values, nulls, arena, fixups, img, &[]);
}

/// `push_image` with a split (head, body) image — the text emit's varlena
/// header + canonical tail land contiguously without a concat allocation.
fn push_image2(
    values: &mut Vec<Datum>,
    nulls: &mut Vec<bool>,
    arena: &mut Vec<u8>,
    fixups: &mut Vec<(usize, usize)>,
    head: &[u8],
    body: &[u8],
) {
    let pad = (8 - arena.len() % 8) % 8;
    arena.resize(arena.len() + pad, 0);
    let off = arena.len();
    arena.extend_from_slice(head);
    arena.extend_from_slice(body);
    fixups.push((values.len(), off));
    values.push(Datum::null());
    nulls.push(false);
}

/// Finalize+project one table row into the emit vectors (the per-row core
/// of [`sink_emit_bucket`] / [`sink_emit_bucket_passthrough`]). Byref
/// outputs (the numeric finalize vocabulary) land in `arena` with a fix-up
/// recorded; the caller resolves fix-ups once the arena's length is final.
#[inline]
fn emit_row(
    plan: &SinkEmitPlan,
    t: &LaneAggTable,
    row: usize,
    values: &mut Vec<Datum>,
    nulls: &mut Vec<bool>,
    arena: &mut Vec<u8>,
    fixups: &mut Vec<(usize, usize)>,
) -> PgResult<()> {
    // Single/Reduced tables: kw[0] IS the canonical i64 key (Int repr);
    // Multi tables: kw is the packed key image (1 or 2 words). None =
    // the out-of-band NULL group (single-word shapes only — Multi
    // tables never probe it). CANONICAL (bytes-keyed) tables split the
    // key into the image prefix (reconstructed words) + the text tail.
    let mut scratch8 = [0u8; 8];
    let (kw, tail): (Option<[u64; 2]>, Option<&[u8]>) = if t.repr() == KeyRepr::Bytes {
        let fixed = plan
            .fixed
            .ok_or_else(|| sink_shape_error("bytes-keyed emit without a canonical prefix"))?
            as usize;
        let cb = t
            .row_key_bytes(row, &mut scratch8)
            .ok_or_else(|| sink_shape_error("NULL group row in a canonical bucket table"))?;
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
            SinkEmitCol::MultiText { nth } => {
                let region = tail
                    .ok_or_else(|| sink_shape_error("MultiText emit on a word-keyed table"))?;
                let tail = canon_tail(region, plan.ntails, nth)?;
                let head =
                    ::datum::varlena::set_varsize_4b(tail.len() + ::datum::varlena::VARHDRSZ);
                push_image2(values, nulls, arena, fixups, &head, tail);
            }
            // Packed numeric key bits → canonical keypack decode →
            // NUMERIC image (byte-identical to the packed first-arrival
            // datum by the keypack canonicality gates).
            SinkEmitCol::MultiNumeric { off, width } => {
                let w = kw
                    .ok_or_else(|| sink_shape_error("MultiNumeric emit on a NULL group row"))?;
                let image = (w[0] as u128) | ((w[1] as u128) << 64);
                let bits = (image >> (off as u32 * 8)) as u64;
                let wbits = width as u32 * 8;
                let masked = if wbits == 64 { bits } else { bits & ((1u64 << wbits) - 1) };
                let img = ::adt_numeric::numeric_key_unpack(
                    crate::compact::mk_numeric_key_decode(masked, width),
                )?;
                push_image(values, nulls, arena, fixups, img.as_bytes());
            }
            // SAFETY: the row's state block holds numtrans pergroups
            // (bucket-table config = the sink's state_bytes); transno <
            // numtrans by plan construction. Byval transvalues only.
            SinkEmitCol::Agg { transno } => unsafe {
                let pg = &*states.add(transno as usize);
                values.push(pg.trans_value);
                nulls.push(pg.trans_value_is_null);
            },
            // min/max(text) survivor (SE-T2AGG CAR B): deep-copy the varlena
            // image verbatim into the buf arena (header form included —
            // representation, not identity).
            // SAFETY: as `Agg`; a non-null text transvalue is a live plain
            // varlena image whose owner outlives this emit, and whose header
            // class was checked when it was stored. Merge arm: the
            // bucket-store invariant ([`sink_own_new_varlena`]) — every
            // transvalue is a copy the bucket's own store allocated, entered
            // through `text_trans_payload`, and `CombinedBucket` keeps the
            // store alive. Pass-through arm ([`sink_emit_bucket_passthrough`]):
            // the rows are the live Local's own, whose transition path
            // (lanefold `str_advance`) copied detoasted plain images into the
            // Local's aggcontext or its handle's `StrStateArena`.
            SinkEmitCol::VarlenaTrans { transno } => unsafe {
                let pg = &*states.add(transno as usize);
                if pg.trans_value_is_null {
                    values.push(Datum::null());
                    nulls.push(true);
                } else {
                    let p = pg.trans_value.as_usize() as *const u8;
                    let len = ::types_tuple::varatt::varsize_any(p);
                    push_image(
                        values,
                        nulls,
                        arena,
                        fixups,
                        core::slice::from_raw_parts(p, len),
                    );
                }
            },
            // Plan-owned byval datum, copied verbatim (admission gate).
            SinkEmitCol::ConstByval { value, isnull } => {
                values.push(value);
                nulls.push(isnull);
            }
            // fc_int8_avg's exact core: strict (NULL trans → NULL),
            // count == 0 → NULL, else the int64_avg_div image.
            // SAFETY: non-null _int8 transvalue is a live merged image
            // (combine contract).
            SinkEmitCol::AvgInt8 { transno, packed } => unsafe {
                // avgpack: the slot IS the {count,sum} image — same
                // finalize core over the same integers, only the storage
                // moved (byte-identical NUMERIC image).
                if packed {
                    let (count, sum) = avgpack_read_slot(states, transno as usize);
                    if count == 0 {
                        values.push(Datum::null());
                        nulls.push(true);
                    } else {
                        let img = ::adt_numeric::ops::int64_avg_div(sum, count)?;
                        push_image(values, nulls, arena, fixups, img.as_bytes());
                    }
                } else {
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
                            push_image(values, nulls, arena, fixups, img.as_bytes());
                        }
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
                    Some(img) => push_image(values, nulls, arena, fixups, img.as_bytes()),
                    None => {
                        values.push(Datum::null());
                        nulls.push(true);
                    }
                }
            },
        }
    }
    Ok(())
}

/// Finalize+project one merged bucket (rows in insertion order — the merge's
/// first-seen order) into a [`SinkEmitBuf`]. Byref outputs (the numeric-avg
/// finalize vocabulary) materialize into the buf's own arena: images land in
/// `arena` during the row loop and the datums are fixed up to point into it
/// once the arena's length is final — nothing worker-owned survives in the
/// published buf.
pub fn sink_emit_bucket(plan: &SinkEmitPlan, t: &LaneAggTable) -> PgResult<SinkEmitBuf> {
    let mut acc = SinkEmitAcc::default();
    acc.emit_table(plan, t)?;
    Ok(acc.finish())
}

/// WINNERS-ONLY compact materializer (topn-winners-only inc-3): finalize+
/// project ONLY the given table rows (ascending row order — the caller
/// sorts its candidate rows so the emit stays a single ordered table walk)
/// into a compact self-contained [`SinkEmitBuf`]. Row `rows[i]` of the
/// table becomes row `i` of the buf — the caller remaps its candidates'
/// `(bucket, row)` payloads to compact indices with the same ordering.
/// Byte-compatible with [`sink_emit_bucket`] by construction: the identical
/// `emit_row` body runs over a row subset, so each emitted row's datums and
/// arena images equal the full emit's rows at the original indices.
pub fn sink_emit_bucket_rows(
    plan: &SinkEmitPlan,
    t: &LaneAggTable,
    rows: &[u32],
) -> PgResult<SinkEmitBuf> {
    let mut acc = SinkEmitAcc::default();
    acc.emit_rows(plan, t, rows)?;
    Ok(acc.finish())
}

/// SINGLE-LOCAL PASS-THROUGH admission (GL-SINKSHAPE-1): whether
/// [`sink_emit_bucket_passthrough`] may emit straight from this Local table
/// under `plan`. [`emit_row`] keys its canonical-tail columns
/// ([`SinkEmitCol::MultiText`]) off the table's OWN representation, so the
/// pass-through admits exactly the tables whose keying class matches the
/// plan's: a canonical (tail-carrying, `fixed`-prefixed) plan needs the
/// canonical image AS the table key (`KeyRepr::Bytes` — the DIRECT
/// single-text arm), a word plan needs a word-keyed table. An INTERN-ARMED
/// canonical table keys on per-worker intern-id WORDS — its canonical bytes
/// exist only through the intern chase, which is the merge arm's remainder
/// face (`SinkRemainder::canon`) — so it REFUSES here and the combine falls
/// back to the merge arm (the sink law: admit the emit class it will
/// receive, or refuse to the fallback). Reachability of the mismatch is an
/// ENGAGEMENT COLLAPSE, not a plan property: Locals fork on first morsel
/// touch, so a pool saturated by concurrent sessions (the QPS window) can
/// hand every morsel of a dop-N engagement to ONE worker — exactly one
/// sealed Local, zero flushed runs. The `emit_row` tripwire ("MultiText
/// emit on a word-keyed table") stays the fail-closed defense behind this
/// admission.
pub fn sink_passthrough_admits(plan: &SinkEmitPlan, t: &LaneAggTable) -> bool {
    plan.fixed.is_some() == (t.repr() == KeyRepr::Bytes)
}

/// SINGLE-LOCAL PASS-THROUGH emit (dop1-tax fix 3, class b): when the
/// combine sees exactly one sealed Local with zero flushed runs, bucket `b`'s
/// merged table would be a verbatim re-insert of the Local's own rows — so
/// emit STRAIGHT from the Local's table through its SEAL partition index
/// instead (no per-bucket table build, no double insert). Output is
/// byte-identical to the merge arm's by construction: the SEAL index lists
/// bucket rows in insertion order (counting sort over ascending row index),
/// which is exactly [`sink_combine_bucket`]'s first-seen order for a single
/// no-runs source, the NULL row last in [`SINK_NULL_BUCKET`] (the merge
/// arm's absorb order), and a new-key absorb copies state blocks verbatim.
/// The decision is LIVE STATE (Local count + run count at combine time) —
/// a widened engagement (≥2 Locals) or a flushed Local takes the merge arm,
/// and so does a Local whose table representation cannot serve the emit
/// plan ([`sink_passthrough_admits`] — the GL-SINKSHAPE-1 admission).
pub fn sink_emit_bucket_passthrough(
    plan: &SinkEmitPlan,
    t: &LaneAggTable,
    part: &SinkPart,
    b: usize,
) -> PgResult<SinkEmitBuf> {
    debug_assert!(b < SINK_NBUCKETS);
    let natts = plan.cols.len();
    let lo = part.starts[b] as usize;
    let hi = part.starts[b + 1] as usize;
    let with_null = b == SINK_NULL_BUCKET && part.has_null;
    let n = hi - lo + usize::from(with_null);
    let mut values: Vec<Datum> = Vec::with_capacity(n * natts);
    let mut nulls: Vec<bool> = Vec::with_capacity(n * natts);
    let mut arena: Vec<u8> = Vec::new();
    let mut fixups: Vec<(usize, usize)> = Vec::new();
    let mut emitted = 0usize;
    for &row in &part.idx[lo..hi] {
        if !plan.row_admits(t, row as usize) {
            continue;
        }
        emit_row(plan, t, row as usize, &mut values, &mut nulls, &mut arena, &mut fixups)?;
        emitted += 1;
    }
    if with_null {
        // The out-of-band NULL group emits LAST in its bucket (the merge
        // arm's order: runs/remainder rows first, then the NULL absorb).
        for row in 0..t.nrows() {
            if t.row_key_int(row).is_none() {
                if plan.row_admits(t, row) {
                    emit_row(plan, t, row, &mut values, &mut nulls, &mut arena, &mut fixups)?;
                    emitted += 1;
                }
                break;
            }
        }
    }
    // Arena is final — resolve the byref datums.
    for (i, off) in fixups {
        values[i] = Datum::from_usize(arena[off..].as_ptr() as usize);
    }
    Ok(SinkEmitBuf { values, nulls, nrows: emitted, arena })
}

/// Sanity error for engagement paths that must never see a non-single-word
/// table (fail-closed conversion helper).
pub fn sink_shape_error(what: &str) -> Box<PgError> {
    PgError::new(ERROR, format!("aggregation sink shape violation: {what}")).into()
}

/// The compact backstop's sink-cap breach message (compact.rs raises it when
/// a worker table crosses the hash limits under a live sink cap — a
/// shape-ESTIMATE failure, not a correctness error). The runtime drain
/// classifies it into a budget-style refusal (serial rerun) by exact
/// message: a private, same-crate-family contract.
pub const SINK_CAP_BREACH_MSG: &str =
    "worker compact table crossed the hash memory limits under the sink cap";

/// True when `e` is the compact backstop's sink-cap breach.
pub fn is_sink_cap_breach(e: &PgError) -> bool {
    e.message().contains(SINK_CAP_BREACH_MSG)
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
        // SE-GROUPONLY fail-closed: zero-transition (grouping-only) builds
        // have no pergroup space to export — the SERIAL lane owns them
        // (lanefold::empty_plan); the parallel export/combine machinery
        // (runtime_partial states, worker exports) assumes numtrans > 0.
        && !node.trans_init.is_empty()
}

/// Arm SINK MODE on a worker build: the compact arms gate/size by `cap`
/// (bounded Local discipline) and the runtime backstop fails closed instead
/// of migrating. Must run BEFORE `agg_hash_compact_try_arm*`.
pub fn agg_sink_set_cap(node: &mut AggStateData<'_>, cap: u32) {
    agg_sink_set_cap_spill(node, cap, false);
}

/// [`agg_sink_set_cap`] with the M3.5 spill-armed admission flag: when the
/// engagement carries a live spill arm, the compact admission gates skip
/// the ESTIMATE-based SpillRisk refusal for word-keyed shapes (a budget
/// crossing degrades to spill epochs, not an error) — the ~10M-group @100M hmm=2
/// cliff was a pure estimate refusal that the landed spill arm could have
/// absorbed. Canonical bytes-keyed (Intern-bearing) shapes keep the
/// phase-1 refusal regardless (their runs are not spillable); the mk
/// admission checks that per shape. Leader probes and worker arms MUST
/// pass the same flag (the F1 leader/worker-verdict invariant).
pub fn agg_sink_set_cap_spill(node: &mut AggStateData<'_>, cap: u32, spill_ok: bool) {
    if let Some(ph) = node.perhash.as_mut() {
        ph.sink_cap = Some(cap);
        ph.sink_spill_ok = spill_ok;
    }
}

/// Disarm SINK MODE (leader-side cap-aware admission probes): the leader's
/// own executor may still run the SERIAL build (engagement refusal / budget
/// fallback / rescan), which must never see sink mode — under a live cap the
/// compact backstop fails closed instead of migrating.
pub fn agg_sink_clear_cap(node: &mut AggStateData<'_>) {
    if let Some(ph) = node.perhash.as_mut() {
        ph.sink_cap = None;
        ph.sink_spill_ok = false;
    }
}

/// The node's per-participant hash memory budget (C
/// `work_mem × hash_mem_multiplier` — `get_hash_memory_limit`), the R3
/// per-Local envelope.
/// The node's hash-groups admission limit (C `hash_agg_check_limits`
/// vocabulary) — the second bound of the sink admission gate; the
/// budget-derived flush cap must respect BOTH bounds or it manufactures
/// refusals the fixed cap never hit (dop1-tax inc-3b fix-up).
pub fn agg_sink_ngroups_limit(node: &AggStateData<'_>) -> Option<u64> {
    node.perhash.as_ref().map(|ph| ph.hash_ngroups_limit)
}

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
            // Intern component(s) decode through the canonical image (one
            // tail raw — the historical image; two tails length-prefixed,
            // canon-sink car 1); the intern table's presence must match the
            // shape — EXCEPT the DIRECT single-text arm (arena-strings
            // inc-3): the table keys on the canonical image itself and
            // carries NO intern table BY DESIGN. Its flush emits the same
            // bytes-mode runs as the intern arm, so the sink key spec is
            // the same Multi shape (GL-ARENASTR-1 smoke: this mismatch
            // returned None on direct-armed PARALLEL worker builds and
            // hard-failed the engagement's worker-side shape re-check —
            // fail-closed held, no wrong answers).
            if ch.text_direct {
                if shape.n_intern() != 1 || ch.intern.is_some() {
                    return None; // belt: direct is exactly 1-Intern, no intern table
                }
            } else if (shape.n_intern() >= 1) != ch.intern.is_some() {
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
// is plain owned Vec storage. State blocks are byval-POD (the sink's
// phase-1 admission) EXCEPT the byref classes the drains explicitly admit:
// PolyInt128/AvgInt8 pointers into the worker aggcontext (drive-pinned
// through combine) and — GL-DICTDRAIN-3 — str min/max transvalue pointers
// into the handle's OWN `str_arena` (they travel together; the arena is
// Send with &mut-serialized mutation, its struct doc). A combine never
// stores a pointer INTO this handle: text transvalues are copied into the
// destination bucket's own store (the bucket-store invariant), so the
// handle's memory has no reader once its Local is dropped.
unsafe impl Send for SinkTableHandle {}
// SAFETY: combine tasks read `&SinkTableHandle` (the table's rows) from many
// threads; the table is plain owned Vec storage, byref state pointers
// target drive-pinned worker aggcontexts or the handle's own str arena, and
// the batch scratch is never dereferenced outside the owning worker's own
// morsel (see the Send justification). The combine only READS this handle:
// PolyInt128/AvgInt8 sources are read field-wise and their pointers adopted
// into the merged table (the aggcontext outlives it), while text sources are
// read as value BYTES and deep-copied — nothing here is mutated and no
// pointer to it survives the merge.
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
    pub fn partition_remainder(&mut self) -> SinkPart {
        if self.0.text_direct {
            // DIRECT arm: counting sort off the saved sink hashes.
            sink_partition_remainder_direct(&self.0)
        } else if compact_canon_shape(&self.0).is_some() {
            sink_partition_remainder_canon(&mut self.0)
        } else {
            sink_partition_remainder(&self.0.table)
        }
    }

    /// SEAL-time remainder flush (the runtime sink's seal-flush arm): the
    /// whole remainder leaves as ONE more radix-partitioned run — the same
    /// flush bodies the cap/pressure flushes run, so flush-cadence
    /// semantics-freedom covers it (runs merge first-seen; a final flush at
    /// SEAL is byte-invisible) — and the combine then streams this Local's
    /// remainder sequentially (bucket-contiguous keys/states) instead of
    /// random-accessing its table through a SEAL index. `None` = empty
    /// table (nothing to hand). Intern-reset semantics are moot at SEAL —
    /// the table is dropped right after and nothing re-fills it (the run
    /// copied/stole its canonical bytes, self-contained either way).
    pub fn flush_remainder(&mut self) -> Option<SinkRun> {
        if self.0.table.len() == 0 {
            return None;
        }
        if self.0.text_direct {
            return Some(sink_flush_table_direct(&mut self.0));
        }
        if compact_canon_shape(&self.0).is_some() {
            return Some(sink_flush_table_canon(&mut self.0));
        }
        Some(sink_flush_table(&mut self.0.table))
    }

    /// This handle's retained footprint (compact + intern tables + the
    /// stored canonical row hashes) — the SEAL-time budget accounting twin
    /// of [`agg_sink_table_mem`].
    pub fn mem_used(&self) -> usize {
        self.0.table.mem_used()
            + self.0.intern.as_ref().map_or(0, ::lanetable::LaneAggTable::mem_used)
            + self.0.canon_hashes.capacity() * 8
            // Live store bytes: at SEAL these are the remainder images the
            // combine face retains and reads — a real charge; retained
            // capacity from flushed epochs is not (the leg-12t law).
            + self.0.canon_store.len()
            + self.0.canon_offs.len() * 4
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
        // STORE-ONCE face (spankey step 2): published only when the store
        // covers every row (accept-time extension ran with the switch on).
        let canon_store = (canon.is_some()
            && self.0.canon_offs.len() == self.0.table.nrows() + 1)
            .then(|| (&self.0.canon_store[..], &self.0.canon_offs[..]));
        SinkRemainder {
            table: &self.0.table,
            part,
            canon,
            canon_store,
            gid_gen: self.0.intern_gen,
            // DIRECT arm: `compact_canon_shape` excluded it above (canon and
            // canon_store are None) — the combine/spill faces read the rows
            // verbatim instead.
            direct: self.0.text_direct,
        }
    }
}

/// Move the armed compact state OUT of the executor (end of a morsel drain:
/// the Local owns it until the next morsel / SEAL). `None` = not armed.
/// Mark the node's armed compact table as RUNTIME-SINK-owned (idempotent;
/// no-op when no compact table is armed). Gates the batch-tail canonical
/// hashing — the serial lane shares the compact table and must not pay for
/// hashes it never consumes.
/// The armed compact table's avgpack mask (0 = not armed / nothing packed).
/// The lane's fold feeds pass it to lanefold's grouped kernels so packed
/// AvgAccum transnos advance the inline `[count, sum]` representation; it is
/// nonzero ONLY on sink worker builds (set at table creation, compact.rs).
pub fn agg_sink_avgpack_mask(node: &AggStateData<'_>) -> u64 {
    node.perhash
        .as_ref()
        .and_then(|ph| ph.compact.as_ref())
        .map_or(0, |ch| ch.avgpack_mask)
}

pub fn agg_sink_mark_sink_mode(node: &mut AggStateData<'_>) {
    if let Some(ph) = node.perhash.as_mut() {
        if let Some(ch) = ph.compact.as_mut() {
            ch.sink_mode = true;
        }
    }
}

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
/// key bytes copied out. The intern table is normally KEPT (scan-lifetime
/// vocabulary, ids reused across windows), but once it has grown past a
/// quarter of the hash-mem budget it is RESET with the table: the flushed
/// run copied its canonical bytes and the remainder is empty at this
/// moment, so no live row references an intern id — the next window
/// re-interns its own vocabulary (bounded memory instead of the backstop's
/// half-limit error on wide-vocabulary scans — the URL-key @100M class).
/// `true` in the pair = the intern table WAS reset: the caller MUST
/// invalidate any code→intern-id cache it holds (`MkScratch`/
/// `MultiKeyChain` epoch caches) — a stale id would materialize the wrong
/// bytes.
pub fn agg_sink_flush_if_due(
    node: &mut AggStateData<'_>,
    cap: u32,
) -> Option<(SinkRun, bool)> {
    let ph = node.perhash.as_mut()?;
    let hash_mem_limit = ph.hash_mem_limit;
    let ch = ph.compact.as_mut()?;
    if ch.table.len() < cap as usize {
        return None;
    }
    if ch.text_direct {
        // DIRECT single-text arm: the flush RESETS the table, and the table
        // IS the vocabulary — `true` (the intern-reset channel) on EVERY
        // flush, so the drain drops its code→group caches (the 830320fed
        // law: any code→X cache is (build, epoch, table-generation)-scoped).
        return Some((sink_flush_table_direct(ch), true));
    }
    if compact_canon_shape(ch).is_some() {
        let run = sink_flush_table_canon(ch);
        let reset_intern = ch
            .intern
            .as_ref()
            .is_some_and(|t| t.mem_used() > hash_mem_limit / 4);
        if reset_intern {
            if let Some(t) = ch.intern.as_mut() {
                t.reset();
            }
            // GID-merge: the reset restarts intern ids — packed words from
            // later epochs are ambiguous against this run's (the combine's
            // per-worker word map resets at the generation boundary).
            ch.intern_gen += 1;
        }
        Some((run, reset_intern))
    } else {
        Some((sink_flush_table(&mut ch.table), false))
    }
}

/// LIVE bytes of a word-keyed sink table: entry line (16 B at ≤0.5 fill),
/// key words, and the state block per live row — the compact spill gate's
/// own per-entry arithmetic, applied to `nrows` instead of retained
/// capacity. Used by the spill-armed pressure/backstop accounting only.
pub(crate) fn sink_table_live_bytes(t: &LaneAggTable) -> usize {
    // Bytes-keyed tables (the DIRECT single-text arm): 3 key words per row
    // + the live long-key arena bytes (short keys pack inline). Word modes
    // keep the historical arithmetic exactly.
    let key_bytes = match t.repr() {
        KeyRepr::Bytes => 24,
        _ => 8 * table_key_words(t),
    };
    let arena = if t.repr() == KeyRepr::Bytes { t.arena_len() } else { 0 };
    t.nrows() * (16 + key_bytes + t.state_bytes()) + arena
}

/// Force-flush the armed table into a run NOW, regardless of the cap
/// (`None` = empty table, nothing to flush). The budget-pressure spill law
/// (mt16-cliffs, the ~10M-group @100M hmm=2 cliff): when half-limit pressure trips
/// on a spill-armed engagement, the drain flushes the bounded table through
/// this and spills the accumulated runs as one epoch instead of refusing —
/// the mem-leg pressure is table-driven there, and the flush drains it.
/// Same canonical-twin + intern-reset semantics as [`agg_sink_flush_if_due`]
/// (the caller MUST honor the reset flag identically).
pub fn agg_sink_flush_now(node: &mut AggStateData<'_>) -> Option<(SinkRun, bool)> {
    let ph = node.perhash.as_mut()?;
    let hash_mem_limit = ph.hash_mem_limit;
    let ch = ph.compact.as_mut()?;
    if ch.table.len() == 0 {
        return None;
    }
    if ch.text_direct {
        // DIRECT arm: as agg_sink_flush_if_due — table reset = vocabulary
        // reset, the cache-invalidation signal is unconditional.
        return Some((sink_flush_table_direct(ch), true));
    }
    if compact_canon_shape(ch).is_some() {
        let run = sink_flush_table_canon(ch);
        let reset_intern = ch
            .intern
            .as_ref()
            .is_some_and(|t| t.mem_used() > hash_mem_limit / 4);
        if reset_intern {
            if let Some(t) = ch.intern.as_mut() {
                t.reset();
            }
            // GID-merge: generation boundary (see agg_sink_flush_if_due).
            ch.intern_gen += 1;
        }
        Some((run, reset_intern))
    } else {
        Some((sink_flush_table(&mut ch.table), false))
    }
}

/// Half-limit budget PRESSURE (the compact backstop's own condition plus
/// headroom): the sink drain refuses on `true` (RG abort → serial rerun)
/// BEFORE the backstop's sink-mode belt would raise its hard error — the
/// demote = refusal discipline. The headroom covers one batch's worst-case
/// growth between per-batch checks.
pub fn agg_sink_budget_pressure(node: &AggStateData<'_>) -> bool {
    let Some(ph) = node.perhash.as_ref() else { return false };
    let Some(ch) = ph.compact.as_ref() else { return false };
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext().context().subtree_used();
    // Spill-armed sink builds count the table's LIVE rows, not its retained
    // capacity: `LaneAggTable::reset` (the flush) keeps capacity, so
    // capacity-based accounting re-trips permanently after the first
    // pressure flush and the spill law could never drain the pressure. The
    // retained capacity is the bounded flush-cycle working set (≤ the cap's
    // sizing, inside the R3 full-budget envelope), not growth.
    let table_mem = if ph.sink_cap.is_some() && ph.sink_spill_ok {
        sink_table_live_bytes(&ch.table)
    } else {
        ch.table.mem_used()
    };
    let mem = table_mem
        + ch.intern.as_ref().map_or(0, ::lanetable::LaneAggTable::mem_used)
        // Store-once canonical images (spankey): LIVE bytes — the flush
        // clears the store, so post-flush pressure drains exactly as the
        // incumbent's (retained capacity deliberately uncounted, the
        // sink_table_live_bytes law; capacity-counting made the leg-12t
        // spill-armed engagement refuse instead of spill). Zero under the
        // kill switch.
        + ch.canon_store.len()
        + ch.canon_offs.len() * 4
        + aggctx;
    // Proportional headroom (an eighth of the half-limit, capped at 32MB):
    // at small work_mem the margin shrinks with the limit instead of
    // refusing everything; at production work_mem 32MB dwarfs any single
    // batch's growth.
    let half = ph.hash_mem_limit / 2;
    let headroom = (half / 8).min(32 << 20);
    (ch.table.len() as u64).saturating_add(4096) >= ph.hash_ngroups_limit / 2
        || mem.saturating_add(headroom) >= half
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
                // Live store bytes (see agg_sink_budget_pressure).
                + ch.canon_store.len()
                + ch.canon_offs.len() * 4
        })
}

/// The node's aggcontext footprint — the byref state classes (PolyInt128 /
/// AvgInt8) live THERE, not in the table rows, so byref-bearing sink drains
/// add this to their budget accounting (the backstop's own mem formula).
/// Plus the table-owned str state store when armed (GL-DICTDRAIN-3 —
/// min/max(text) transvalues live THERE, not in the aggcontext).
pub fn agg_sink_aggctx_mem(node: &AggStateData<'_>) -> usize {
    let arena = agg_sink_str_arena(node).map_or(0, |a| a.borrow().bytes());
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    unsafe { node.agg_node.as_ref() }.aggcontext().context().subtree_used() + arena
}

/// Arm the TABLE-OWNED by-ref str transvalue store on a sink WORKER build
/// (idempotent; `CompactHash::str_arena` doc).
///
/// **One caller: `runtime_agg::arm_sink_build`**, at the single exit every
/// worker drain passes through, keyed on `lanefold::plan_has_str_trans` — NOT
/// per drain arm. GL-SINKCRASH-2: this used to be called from the DictCoded
/// expr-key arm alone, so the K2 and Mk drains — which the vguard admission
/// also lets carry `min/max(text)` — copied their transvalues into the bump
/// aggcontext of whichever pool thread served each morsel while the table
/// migrated, and that shipped as a release blocker. Arming per drain identity
/// is the bug; arming per class predicate is the fix. Do not add a second call
/// site.
///
/// GL-DICTDRAIN-3 (supersedes the t45-reverted per-thread FREEING context,
/// `PGRUST_RUNTIME_AGG_STRCTX` retired with it): the drain's Local-owned
/// table is LENT to whichever pool thread serves each morsel, so a
/// per-(thread, query) context home for these transvalues broke the
/// replace-free's allocator-exactness — thread A's copy replace-freed on
/// thread B entered B's freelist while A's context still owned the chunk,
/// double-allocating it and leaving a LIVE pergroup with a freed pointer
/// (the 'aggregation sink shape violation' class). The store travels WITH
/// the table, restoring C's one-allocator-per-worker-table invariant, and
/// keeps the GL-DICTDRAIN-2 churn bound (free-on-replace) with whole-store
/// release on Drop — no leak on any path. No kill switch: reverting to a
/// context home reintroduces the unsoundness.
pub fn agg_sink_arm_str_state(node: &mut AggStateData<'_>) {
    let Some(ph) = node.perhash.as_mut() else { return };
    let Some(ch) = ph.compact.as_mut() else { return };
    if ph.sink_cap.is_some() && ch.str_arena.is_none() {
        ch.str_arena =
            Some(Box::new(core::cell::RefCell::new(::lanefold::StrStateArena::default())));
    }
}

/// The armed table-owned str state store, when present (the mm fold
/// threads it into the str advances; `None` = the context discipline,
/// classic builds byte-identical).
pub fn agg_sink_str_arena<'a>(
    node: &'a AggStateData<'_>,
) -> Option<&'a core::cell::RefCell<::lanefold::StrStateArena>> {
    node.perhash
        .as_ref()
        .and_then(|ph| ph.compact.as_ref())
        .and_then(|ch| ch.str_arena.as_deref())
}

// ---------------------------------------------------------------------------
// Combine-phase top-N composition (m3-sort-b car 1: agg sink → ORDER BY/
// LIMIT). When the sink's consumer is a bounded single-column Sort whose
// order column is a raw int8 transvalue (the topkfin/topnemit vocabulary),
// each COMBINE task additionally selects its partition's top-`bound` groups
// on the merged raw states — a pure bounded-heap pass over rows it already
// walks for the emit — and FINALIZE truncate-merges the 256 per-partition
// winner lists into one global winner list. The leader then drains ONLY the
// winners through the (real) Sort node above, killing the serialized
// all-groups sort tail. The emit buffers stay FULL (selection changes what
// the leader drains, never what was computed): a mid-combine decline (a
// NULL order transvalue — its rank depends on NULLS placement) degrades to
// the plain full drain with zero data loss, no abort, no rerun.
//
// Selection total order (the rule-2 analog for agg groups): (badness,
// null-key tier, canonical key image). The key image is repr-comparable:
// word tables use the canonical key words, canonical-bytes tables (the
// m2-coverage-c3 text car) use the canonical key BYTES themselves. Group
// keys are globally unique (hash-partitioned, one bucket each; the NULL
// group is unique), so the order is total and the winner set is a PURE
// FUNCTION OF THE DATA — independent of worker claim order and of bucket
// geometry. Against C / the serial relaxed arm the boundary tie group is
// the ratified count-gated class (the high-cardinality top-n precedent).
//
// SELECTION-ORDER TOTALITY LAW (train-14 P0, topn x bytes — the mt16 v4
// stop finding): every key representation the sink ADMITS must carry a
// repr-comparable image in this selection order; a car that adds a key
// repr without extending the image vocabulary must DEGRADE the top-N at
// leader-side admission, before any worker arms it. (Train-13 composed
// c3's bytes tables with sort-b's word-only selection and covered
// spill x bytes and spill x topn but not topn x bytes — every text
// `GROUP BY .. ORDER BY count DESC LIMIT` panicked at combine.)
// ---------------------------------------------------------------------------

/// The armed combine-phase top-N: `transno`'s raw int8 transvalue is the
/// order key (`topn_emit_resolve` proved it), `desc` folds the direction,
/// `bound` is the downstream sort's tuple bound (includes any OFFSET).
#[derive(Clone, Copy)]
pub struct SinkTopnSpec {
    pub transno: u32,
    pub desc: bool,
    pub bound: u32,
}

/// Serial-cap agreement with the sort lanes (`TOPN_MAX_BOUND`).
pub const SINK_TOPN_MAX_BOUND: u32 = 1 << 16;

/// One winner candidate. FIELD ORDER IS THE SELECTION TOTAL ORDER (derived
/// lexicographic Ord): badness first (monotone-worse image of the order key
/// under the direction), then the null-group tier, then the canonical key
/// image — unique per group, so two candidates never compare equal before
/// the payload fields. Word tables carry the key words in `kw` (and an
/// empty, allocation-free `key_bytes`); canonical-bytes tables carry the
/// key bytes in `key_bytes` (and `kw = [0, 0]`) — one engagement's
/// candidates are always same-repr, so the two vocabularies never
/// interleave in a compare that matters.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct SinkTopnCand {
    badness: u64,
    null_key: bool,
    kw: [u64; 2],
    /// Canonical key BYTES (c3 bytes-keyed tables): the repr-comparable
    /// selection image where no fixed-width key words exist. Owned — the
    /// merged partition table dies with its combine claim, but candidates
    /// live to the finalize truncate-merge. Allocated only for rows that
    /// actually enter the bounded heap (<= bound + improvements).
    key_bytes: Box<[u8]>,
    /// Payload: the winner's home bucket + row index in that bucket's emit
    /// buffer (`sink_emit_bucket` iterates merged rows 0..n in table order,
    /// so the selection row index IS the buf row index).
    pub bucket: u16,
    pub row: u32,
}

/// Select one merged partition's top-`bound` groups on the raw states
/// (rows 0..nrows include the NULL group's allocated row). Sorted
/// best-first. `None` = decline (a NULL/pending order transvalue) — the
/// caller degrades to the full drain; nothing here has side effects.
pub fn sink_topn_candidates(
    t: &LaneAggTable,
    spec: &SinkTopnSpec,
    bucket: u16,
) -> Option<Vec<SinkTopnCand>> {
    let n = t.nrows();
    let k = (spec.bound as usize).min(n);
    let bytes_repr = t.repr() == KeyRepr::Bytes;
    // Max-heap: the WORST kept candidate on top; strict-better replacement.
    let mut heap: std::collections::BinaryHeap<SinkTopnCand> =
        std::collections::BinaryHeap::with_capacity(k.saturating_add(1));
    let mut scratch = [0u8; 8];
    for row in 0..n {
        // SAFETY: row < nrows; the row's state block holds the merged
        // AggPerGroup array (combine contract); transno bounds-checked by
        // `topn_emit_resolve` on the leader's node.
        let pg = unsafe {
            &*t.row_states(row).cast_const().cast::<AggPerGroup>().add(spec.transno as usize)
        };
        if pg.no_trans_value || pg.trans_value_is_null {
            return None;
        }
        let badness = crate::compact::topkfin_badness(pg.trans_value.as_i64(), spec.desc);
        // Borrowed key image — the owned candidate (bytes copy) is built
        // only when the row actually enters the heap.
        let (null_key, kw, kb): (bool, [u64; 2], &[u8]) = if bytes_repr {
            match t.row_key_bytes(row, &mut scratch) {
                Some(b) => (false, [0, 0], b),
                None => (true, [0, 0], &[]),
            }
        } else {
            match row_key_words(t, row) {
                Some(w) => (false, w, &[]),
                None => (true, [0, 0], &[]),
            }
        };
        let keep = if heap.len() < k {
            true
        } else {
            // Strict-better against the worst kept candidate, compared in
            // the selection total order (field order of SinkTopnCand); the
            // key image is unique per group so a full tie never happens.
            heap.peek().is_some_and(|worst| {
                (badness, null_key, &kw, kb)
                    < (worst.badness, worst.null_key, &worst.kw, &*worst.key_bytes)
            })
        };
        if keep {
            if heap.len() >= k {
                heap.pop();
            }
            heap.push(SinkTopnCand {
                badness,
                null_key,
                kw,
                key_bytes: kb.into(),
                bucket,
                row: row as u32,
            });
        }
    }
    let mut v = heap.into_vec();
    v.sort_unstable();
    Some(v)
}

/// Truncate-merge the per-partition winner lists (each sorted best-first)
/// into the global winner list: ≤ `bound` `(bucket, row)` pairs in the
/// selection total order. K-way heap merge — O((P + bound)·log P), inside
/// the finalize's O(partitions)-ish envelope.
pub fn sink_topn_merge(lists: &[Vec<SinkTopnCand>], bound: usize) -> Vec<(u16, u32)> {
    use std::cmp::Reverse;
    // Borrowed heads: candidates own their bytes key image, so the merge
    // compares by reference instead of copying list entries around.
    let mut heads: std::collections::BinaryHeap<Reverse<(&SinkTopnCand, usize)>> =
        std::collections::BinaryHeap::with_capacity(lists.len());
    for (li, l) in lists.iter().enumerate() {
        if let Some(c) = l.first() {
            heads.push(Reverse((c, li)));
        }
    }
    let mut winners = Vec::with_capacity(bound.min(lists.iter().map(Vec::len).sum()));
    let mut cursor = vec![0usize; lists.len()];
    while winners.len() < bound {
        let Some(Reverse((c, li))) = heads.pop() else { break };
        winners.push((c.bucket, c.row));
        cursor[li] += 1;
        if let Some(next) = lists[li].get(cursor[li]) {
            heads.push(Reverse((next, li)));
        }
    }
    winners
}

/// SPLIT×SELECTION (winners-phase2): merge the per-FRAGMENT candidate lists
/// of one split partition into that partition's local candidate list, in the
/// selection total order, truncated to `bound`. Correctness is the design's
/// partition-local superset lemma applied one level deeper: split fragments
/// partition the partition's groups DISJOINTLY (sub-bucket hash routing), so
/// a group in the partition's top-`bound` is beaten by fewer than `bound`
/// groups in its own fragment and therefore survives its fragment's
/// top-`bound` list — the union of fragment lists is a superset of the
/// partition's top-`bound`, and the truncate-merge recovers exactly it.
/// Full candidates (not `(bucket, row)` pairs) survive: the result feeds the
/// finalize truncate-merge like any in-memory partition's list. Fragment
/// lists are ≤bound each and fragments are few — concat+sort is inside any
/// envelope that matters.
pub fn sink_topn_merge_fragments(
    lists: Vec<Vec<SinkTopnCand>>,
    bound: usize,
) -> Vec<SinkTopnCand> {
    let mut all: Vec<SinkTopnCand> = lists.into_iter().flatten().collect();
    all.sort_unstable();
    all.truncate(bound);
    all
}

// ---------------------------------------------------------------------------
// Leader-side adopted emit (the published sink output as the Agg's source).
// Two backings behind one drain interface:
//   Bufs — combine-materialized per-bucket EmitBufs (the general arm);
//   Table — TRUE TABLE ADOPT (dop1-tax2 inc-1): the single sealed Local's
//   whole table + SEAL partition index, published by finalize WITHOUT any
//   emit materialization. Rows are formed on demand at drain time (byval
//   emit plans only — a byref transvalue points into a WORKER aggcontext,
//   which dies with the helpers; byref shapes keep the EmitBuf arms, whose
//   arena copy is exactly what makes them self-contained).
// ---------------------------------------------------------------------------

/// Every emit column projects a byval datum (no arena materialization):
/// the TABLE-ADOPT shape gate.
pub fn sink_emit_plan_all_byval(plan: &SinkEmitPlan) -> bool {
    plan.cols.iter().all(|c| {
        matches!(
            c,
            SinkEmitCol::Key
                | SinkEmitCol::Derived(_)
                | SinkEmitCol::MultiComp { .. }
                | SinkEmitCol::ConstByval { .. }
                | SinkEmitCol::Agg { .. }
        )
    })
}

/// One emit column of table row `row`, formed directly from the adopted
/// table (byval kinds only — `sink_emit_plan_all_byval` gates adoption).
/// The `Agg` arm is the ledger's "transvalue read via the resolved transno":
/// the datum IS the raw transvalue, no copy, no arena.
#[inline]
fn table_emit_datum(
    plan: &SinkEmitPlan,
    t: &LaneAggTable,
    row: usize,
    col: usize,
) -> (Datum, bool) {
    match plan.cols[col] {
        SinkEmitCol::Key => match t.row_key_int(row) {
            Some(k) => (key_datum(plan.width, k), false),
            None => (Datum::null(), true),
        },
        SinkEmitCol::Derived(d) => match t.row_key_int(row) {
            Some(k) => (key_datum(plan.width, d.eval(k)), false),
            None => (Datum::null(), true),
        },
        SinkEmitCol::MultiComp { off, width } => match row_key_words(t, row) {
            Some(w) => {
                let image = (w[0] as u128) | ((w[1] as u128) << 64);
                let bits = (image >> (off as u32 * 8)) as u64;
                let sh = 64 - width as u32 * 8;
                let v = if sh == 0 { bits as i64 } else { ((bits << sh) as i64) >> sh };
                (key_datum(width, v), false)
            }
            None => (Datum::null(), true),
        },
        // SAFETY: the row's state block holds numtrans pergroups (adopted
        // table config = the sink's state_bytes); transno < numtrans by
        // plan construction. Byval transvalues only (adoption gate).
        SinkEmitCol::Agg { transno } => unsafe {
            let pg =
                &*t.row_states(row).cast_const().cast::<AggPerGroup>().add(transno as usize);
            (pg.trans_value, pg.trans_value_is_null)
        },
        // Plan-owned byval datum, copied verbatim (admission gate).
        SinkEmitCol::ConstByval { value, isnull } => (value, isnull),
        // Byref emit kinds never reach the table drain: table adoption is
        // gated by sink_emit_plan_all_byval (MultiText/MultiNumeric/Avg*/
        // VarlenaTrans are byref) — fail-soft NULL rather than asserting.
        SinkEmitCol::MultiText { .. }
        | SinkEmitCol::MultiNumeric { .. }
        | SinkEmitCol::VarlenaTrans { .. } => (Datum::null(), true),
        // Byref finalize kinds never reach a table-backed drain:
        // sink_emit_plan_all_byval refuses adoption (and the debug_assert
        // in agg_sink_adopt_table re-checks).
        SinkEmitCol::AvgInt8 { .. }
        | SinkEmitCol::AvgInt128 { .. }
        | SinkEmitCol::SumInt128 { .. } => {
            unreachable!("byref emit column in a table-backed sink drain")
        }
    }
}

/// The drain source behind [`SinkEmitState`].
enum SinkEmitSrc {
    /// Combine-materialized per-bucket rows.
    Bufs(Vec<SinkEmitBuf>),
    /// The adopted single-Local table, drained LINEARLY: bucket 0 carries
    /// every row in table insertion order (for a DOP1 build — the only
    /// shape that adopts — sequential claims make that the SERIAL build's
    /// own emit order, including the NULL group row at its insertion
    /// position); buckets 1..255 are empty. No SEAL partition exists and
    /// none is ever built.
    Table { table: SinkTableHandle, plan: SinkEmitPlan },
}

/// The leader's adopted parallel emit state, drained bucket 0..255 in
/// insertion order.
pub struct SinkEmitState {
    src: SinkEmitSrc,
    natts: usize,
    bucket: usize,
    pos: usize,
    /// Composed top-N (m3-sort-b car 1): `Some` = drain ONLY these
    /// `(bucket, row)` winners, in list order. The bufs stay complete —
    /// winners index into them.
    winners: Option<Vec<(u16, u32)>>,
}

impl SinkEmitState {
    /// Retained content bytes of the adopted result (the sink-teardown
    /// release floor's input): the emit buffers' content, or the adopted
    /// table's live memory on the table-adopt arm.
    pub fn retained_bytes(&self) -> usize {
        match &self.src {
            SinkEmitSrc::Bufs(bufs) => bufs.iter().map(|b| b.bytes()).sum(),
            SinkEmitSrc::Table { table, .. } => table.table().mem_used(),
        }
    }

    /// Bucket `b`'s row count.
    #[inline]
    fn bucket_len(&self, b: usize) -> usize {
        match &self.src {
            SinkEmitSrc::Bufs(bufs) => bufs[b].nrows,
            SinkEmitSrc::Table { table, .. } => {
                if b == 0 {
                    table.table().nrows()
                } else {
                    0
                }
            }
        }
    }

    /// One column datum of drain position (b, row). Table backing is
    /// LINEAR: bucket 0, position == table row.
    #[inline]
    fn row_datum(&self, b: usize, row: usize, col: usize) -> (Datum, bool) {
        match &self.src {
            SinkEmitSrc::Bufs(bufs) => {
                let buf = &bufs[b];
                let i = row * self.natts + col;
                (buf.values[i], buf.nulls[i])
            }
            SinkEmitSrc::Table { table, plan } => {
                debug_assert_eq!(b, 0);
                table_emit_datum(plan, table.table(), row, col)
            }
        }
    }

    /// Fill one drained row's datums/nulls (the slot-store body).
    #[inline]
    fn fill_row(&self, b: usize, row: usize, values: &mut [Datum], nulls: &mut [bool]) {
        match &self.src {
            SinkEmitSrc::Bufs(bufs) => {
                let buf = &bufs[b];
                debug_assert!(row < buf.nrows);
                let base = row * self.natts;
                values[..self.natts].copy_from_slice(&buf.values[base..base + self.natts]);
                nulls[..self.natts].copy_from_slice(&buf.nulls[base..base + self.natts]);
            }
            SinkEmitSrc::Table { table, plan } => {
                debug_assert_eq!(b, 0);
                for c in 0..self.natts {
                    let (v, isnull) = table_emit_datum(plan, table.table(), row, c);
                    values[c] = v;
                    nulls[c] = isnull;
                }
            }
        }
    }
}

/// Adopt the published emit set; subsequent [`agg_sink_emit_next`] calls
/// drain it. The Agg becomes a pure Source (its build never ran).
/// `winners`: the composed top-N winner list (`None` = full drain).
pub fn agg_sink_adopt_emit(
    node: &mut AggStateData<'_>,
    bufs: Vec<SinkEmitBuf>,
    natts: usize,
    winners: Option<Vec<(u16, u32)>>,
) {
    debug_assert_eq!(bufs.len(), SINK_NBUCKETS);
    node.sink_emit = Some(Box::new(SinkEmitState {
        src: SinkEmitSrc::Bufs(bufs),
        natts,
        bucket: 0,
        pos: 0,
        winners,
    }));
}

/// TRUE TABLE ADOPT (dop1-tax2 inc-1): adopt the published single-Local
/// table wholesale — zero emit materialization, zero partitioning; the
/// drain forms rows on demand (survivors only, under the consumers'
/// boundary cut), LINEARLY in table insertion order (the DOP1 build's
/// serial-equivalent order). Byval emit plans only (the adoption gate —
/// re-checked here).
pub fn agg_sink_adopt_table(
    node: &mut AggStateData<'_>,
    table: SinkTableHandle,
    plan: SinkEmitPlan,
) {
    debug_assert!(sink_emit_plan_all_byval(&plan), "table adopt over a byref emit plan");
    let natts = plan.cols.len();
    node.sink_emit = Some(Box::new(SinkEmitState {
        src: SinkEmitSrc::Table { table, plan },
        natts,
        bucket: 0,
        pos: 0,
        // The composed top-N never rides a table adopt (combine no-ops
        // under the adopted flag) — the table drain is always full.
        winners: None,
    }));
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
        if let Some(winners) = &st.winners {
            // Composed top-N: the winner list IS the drain (bufs stay
            // complete; `pos` doubles as the winner cursor).
            let w = winners.get(st.pos).map(|&(b, r)| (b as usize, r as usize));
            if w.is_some() {
                st.pos += 1;
            }
            w
        } else {
            loop {
                if st.bucket >= SINK_NBUCKETS {
                    break None;
                }
                if st.pos >= st.bucket_len(st.bucket) {
                    st.bucket += 1;
                    st.pos = 0;
                    continue;
                }
                let row = st.pos;
                st.pos += 1;
                break Some((st.bucket, row));
            }
        }
    };
    let Some((bucket, row)) = next else {
        // KEEP the drained state (its bufs' arenas back byref datums already
        // handed out this scan — C's aggcontext lifetime analog; the adopted
        // table backs handed-out transvalue datums the same way); it drops
        // at rescan/teardown through agg_sink_reset_emit.
        node.agg_done = true;
        return Ok(None);
    };
    let st = node.sink_emit.as_ref().expect("sink emit state adopted");
    let natts = st.natts;
    let slot = estate.slot_mut(node.ps_ResultTupleSlot);
    ::exectuples::exec_clear_tuple(slot, mcx);
    {
        let sb = slot.base_mut();
        st.fill_row(bucket, row, &mut sb.tts_values[..natts], &mut sb.tts_isnull[..natts]);
    }
    ::exectuples::exec_store_virtual_tuple(slot);
    Ok(Some(node.ps_ResultTupleSlot))
}

/// Drop any adopted emit state (rescan / teardown safety).
pub fn agg_sink_reset_emit(node: &mut AggStateData<'_>) {
    node.sink_emit = None;
}

// ---------------------------------------------------------------------------
// Batched drain of the adopted emit (dop1-tax fix 4): a consuming breaker
// (the lane's agg→sort feed) drains the published rows in per-bucket BLOCKS
// instead of pulling one row per produce through the emit cursor — same
// rows, same order (bucket 0..255, insertion order within), same slot
// contents as agg_sink_emit_next; only the per-row pull ceremony is hoisted.
// ---------------------------------------------------------------------------

/// Bucket `b`'s row count in the adopted emit state (`None` = not adopted).
pub fn agg_sink_emit_bucket_len(node: &AggStateData<'_>, b: usize) -> Option<usize> {
    node.sink_emit.as_ref().map(|st| st.bucket_len(b))
}

/// True while the adopted emit cursor has not advanced — the batched drain
/// starts from row 0 and must never double-emit after a partial per-row
/// drain (defensive; the lane's consumers never mix the two).
pub fn agg_sink_emit_unstarted(node: &AggStateData<'_>) -> bool {
    node.sink_emit.as_ref().is_some_and(|st| st.bucket == 0 && st.pos == 0)
}

/// Take the composed top-N winner list off the adopted emit state (the
/// batched drain's winner-directed put — topn-winners-only amendment: the
/// winner list IS the drain in BOTH selection modes, so the batched sort
/// feed emits the identical row sequence as the cursor drain's composed
/// path instead of re-selecting tie members in the bounded heap). `None` =
/// no composition (or degraded) — the caller walks the buckets as before.
/// Taking (not borrowing) keeps the caller free to re-borrow the node per
/// row; the drain consumes the state wholesale afterwards.
pub fn agg_sink_emit_take_winners(node: &mut AggStateData<'_>) -> Option<Vec<(u16, u32)>> {
    node.sink_emit.as_mut().and_then(|st| st.winners.take())
}

/// True when the adopted emit carries a composed top-N winner list — the
/// winner list IS the drain in that mode, so block consumers that walk the
/// buckets must stand down (the cursor and winner-directed drains own it).
pub fn agg_sink_emit_has_winners(node: &AggStateData<'_>) -> bool {
    node.sink_emit.as_ref().is_some_and(|st| st.winners.is_some())
}

/// Spend the adopted emit exactly as the cursor drain's EOF does: cursor
/// parked past the last bucket, `agg_done` set, STATE KEPT (its bufs'
/// arenas back byref datums handed out during the drain — the aggcontext
/// lifetime analog `agg_sink_emit_next`'s EOF documents; it drops at
/// rescan/teardown through `agg_sink_reset_emit`). For batch drains whose
/// consumer read the rows without advancing the cursor: a stray later pull
/// must serve EOF, never re-emit row 0. Winner-composed drains never come
/// here (block consumers refuse them at admission).
pub fn agg_sink_emit_consume_all(node: &mut AggStateData<'_>) {
    if let Some(st) = node.sink_emit.as_mut() {
        debug_assert!(st.winners.is_none(), "block drain over a winner-composed emit");
        st.bucket = SINK_NBUCKETS;
        st.pos = 0;
    }
    node.agg_done = true;
}

/// Store row `row` of bucket `b` into the node's result slot (the
/// agg_sink_emit_next body, cursor-free). Caller drives bucket/row order.
pub fn agg_sink_emit_block_row<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut ::executils::EStateData<'mcx>,
    b: usize,
    row: usize,
) -> ::executils::ExecSlotId {
    let mcx = estate.es_query_cxt;
    let st = node.sink_emit.as_ref().expect("sink emit state adopted");
    let natts = st.natts;
    let slot = estate.slot_mut(node.ps_ResultTupleSlot);
    ::exectuples::exec_clear_tuple(slot, mcx);
    {
        let sb = slot.base_mut();
        st.fill_row(b, row, &mut sb.tts_values[..natts], &mut sb.tts_isnull[..natts]);
    }
    ::exectuples::exec_store_virtual_tuple(slot);
    node.ps_ResultTupleSlot
}

/// One emitted-column datum of row `row` in bucket `b` (the batched drain's
/// boundary-cut key read — no slot build for rows the cut will skip; on a
/// table-backed drain this reads the raw transvalue straight off the
/// adopted table).
#[inline]
pub fn agg_sink_emit_datum(node: &AggStateData<'_>, b: usize, row: usize, col: usize) -> (Datum, bool) {
    let st = node.sink_emit.as_ref().expect("sink emit state adopted");
    st.row_datum(b, row, col)
}

/// End of a batched drain: the adopted state is consumed exactly as the
/// cursor drain's EOF (state dropped, agg_done set — rescans rebuild).
pub fn agg_sink_emit_drained(node: &mut AggStateData<'_>) {
    node.sink_emit = None;
    node.agg_done = true;
}

// ---------------------------------------------------------------------------
// Per-socket SHARED aggregate table — the mid-NDV architecture EXPERIMENT
// (cachebudget lane D2; docs/design/shared-agg-table-experiment.md). Hard
// default-OFF (PGRUST_RUNTIME_AGG_SHARED_TABLE=1 in the executor arms it).
//
// Design (a) of the design note: a Folklore*-class single-word-CAS
// open-addressing table (Xue & Marcus PVLDB'25's winning variant — written
// from scratch, the reference impl was consulted as literature only),
// PER SOCKET, sized at engagement to fit a socket-SLC share. PROTOTYPE
// SCOPE (recorded in the design note §4): single-int-word keys, COUNT-only
// states (count(*) / count(col) — i64 add is the one atomic fold the
// architecture question needs; the footprint-vs-coherence verdict is
// state-class-independent to first order). Workers keep their bounded
// Locals as the heavy-hitter pre-pass (Polychroniou composition): the
// shared face absorbs at FLUSH cadence — a flushed run's rows CAS/add into
// the socket table and the run is dropped instead of retained, so combine
// volume collapses to the (≤2) socket tables plus whatever overflowed.
//
// SPILL FALLBACK (the literature has no spill story; we do): occupancy is
// reserved run-by-run BEFORE merging; a reservation that would cross the
// sized capacity closes the face — that run and every later flush ride the
// EXISTING partitioned runs/spill path, and the combine key-merges both
// sources correctly by construction (runs from different sources are
// exactly what it merges). Fail-closed, no mid-run split.
//
// Result-identity class: insertion race order makes slot layout
// nondeterministic; the seal drain SORTS by key so a given dataset drains
// deterministically, but the merged first-seen order still differs from
// the incumbent's — LIMIT-boundary tie shapes are therefore outside the
// byte-identity law (the experiment's e2e corpus carries total ORDER BY;
// adoption as a default requires the canonical re-sort law — design note
// §4). The experiment gate refuses topn/freeze shapes.
// ---------------------------------------------------------------------------

use core::sync::atomic::{AtomicBool as ShAtomicBool, AtomicU64 as ShAtomicU64, AtomicUsize as ShAtomicUsize, Ordering as ShOrdering};

pub struct SharedCountTable {
    /// Slot key words; 0 = EMPTY sentinel (the literal key 0 rides the
    /// dedicated side slot below — Datum 0 is a common int key).
    keys: Box<[ShAtomicU64]>,
    /// Per-slot i64 count accumulators (parallel to `keys`).
    counts: Box<[ShAtomicU64]>,
    mask: usize,
    /// Conservative member reservations (run rows reserved up front; hits
    /// on existing keys over-reserve, which only closes the face early —
    /// the safe direction).
    members: ShAtomicUsize,
    cap_members: usize,
    /// Closed = a reservation crossed capacity; every later merge refuses
    /// (callers route runs to the incumbent path).
    closed: ShAtomicBool,
    /// The literal key-word 0 group (presence + count).
    zero_present: ShAtomicBool,
    zero_count: ShAtomicU64,
    /// The out-of-band NULL group (SinkRun::null_states).
    null_present: ShAtomicBool,
    null_count: ShAtomicU64,
}

impl SharedCountTable {
    /// The two slot arrays' heap bytes (GL-CONCMEM-1 estate grain: the
    /// shared absorb face is an est-groups-sized plain-Rust estate).
    fn slot_estate_bytes(slots: usize) -> usize {
        slots * 2 * core::mem::size_of::<u64>()
    }

    /// `cap_members` live groups, slots ≥ 2× that (power of two) — probe
    /// termination is structural: reservations bound distinct keys to
    /// cap_members ≤ slots/2, so an empty slot always exists.
    pub fn new(cap_members: usize) -> SharedCountTable {
        let cap_members = cap_members.max(64);
        let slots = (cap_members * 2).next_power_of_two();
        // GL-CONCMEM-1: charge the process ledger at construction (one
        // fixed-size allocation event — block grain by nature); Drop
        // balances. The cross-worker run handoff otherwise rides entirely
        // outside every context ledger.
        ::mcx::global_footprint::charge_engine_estate(Self::slot_estate_bytes(slots));
        SharedCountTable {
            keys: (0..slots).map(|_| ShAtomicU64::new(0)).collect(),
            counts: (0..slots).map(|_| ShAtomicU64::new(0)).collect(),
            mask: slots - 1,
            members: ShAtomicUsize::new(0),
            cap_members,
            closed: ShAtomicBool::new(false),
            zero_present: ShAtomicBool::new(false),
            zero_count: ShAtomicU64::new(0),
            null_present: ShAtomicBool::new(false),
            null_count: ShAtomicU64::new(0),
        }
    }

    /// Merge a flushed single-word count run into the shared face.
    /// `true` = absorbed (caller DROPS the run); `false` = the face is/
    /// became closed — caller keeps the run on the incumbent path. Any
    /// number of worker threads may call concurrently.
    pub fn merge_run(&self, run: &SinkRun) -> bool {
        debug_assert_eq!(run.key_words, 1, "shared count face is single-word-keyed");
        // State block = one AggPerGroup (16B = 2 words): word 0 the count
        // Datum, word 1 the trans_value_is_null/no_trans_value flag bytes —
        // structurally 0 for count (seeded non-null, never nulled).
        debug_assert_eq!(run.state_words, 2, "shared count face is count-state only");
        if self.closed.load(ShOrdering::Acquire) {
            return false;
        }
        let n = run.nrows();
        // FAIL-CLOSED flags pre-pass BEFORE any reservation or slot write:
        // a set trans_value_is_null / no_trans_value byte is not the count
        // shape this face folds — refuse the whole run (it rides the
        // incumbent path; never a partial merge). ONLY the two bool bytes
        // are meaningful: AggPerGroup is repr(C) {Datum, bool, bool} and
        // the trailing 6 PADDING bytes are UNDEFINED in fold-written
        // states (the wave-5 finding: requiring a fully-zero word refused
        // every real run — merged_runs=0 with the face live).
        const FLAG_BYTES: u64 = 0xFFFF;
        for i in 0..n {
            if run.states[i * 2 + 1] & FLAG_BYTES != 0 {
                return false;
            }
        }
        if run.null_states.as_ref().is_some_and(|b| b[1] & FLAG_BYTES != 0) {
            return false;
        }
        // Reserve BEFORE touching slots: crossing capacity closes the face
        // with the reservation backed out — no partial run is ever merged.
        if self.members.fetch_add(n, ShOrdering::AcqRel) + n > self.cap_members {
            self.members.fetch_sub(n, ShOrdering::AcqRel);
            self.closed.store(true, ShOrdering::Release);
            return false;
        }
        // RESERVATION RELEASE LAW: rows resolving to an EXISTING key (or
        // the key-0 side slot) release their reservation below, so steady-
        // state `members` counts CLAIMED SLOTS, not cumulative flushed
        // rows. Duplicate keys across runs are the COMMON case (every key
        // recurs in ~W×flushes runs); without the release the face closes
        // after ~cap total flushed rows regardless of NDV (the concurrent-
        // oracle unit test caught exactly this). Claimed slots therefore
        // never exceed cap_members ≤ slots/2 — probe termination stays
        // structural. Transient in-flight reservations (Σ concurrent runs'
        // rows) can spuriously close a face genuinely NEAR capacity —
        // fail-safe direction (the run rides the incumbent path).
        let mut release = 0usize;
        for i in 0..n {
            let key = run.keys[i];
            let cnt = run.states[i * 2];
            if key == 0 {
                self.zero_present.store(true, ShOrdering::Release);
                self.zero_count.fetch_add(cnt, ShOrdering::Relaxed);
                release += 1;
                continue;
            }
            let mut pos = (sink_hash(key, 0) as usize) & self.mask;
            loop {
                let cur = self.keys[pos].load(ShOrdering::Acquire);
                if cur == key {
                    self.counts[pos].fetch_add(cnt, ShOrdering::Relaxed);
                    release += 1;
                    break;
                }
                if cur == 0 {
                    match self.keys[pos].compare_exchange(
                        0,
                        key,
                        ShOrdering::AcqRel,
                        ShOrdering::Acquire,
                    ) {
                        Ok(_) => {
                            // Fresh slot claim: consumes its reservation.
                            self.counts[pos].fetch_add(cnt, ShOrdering::Relaxed);
                            break;
                        }
                        Err(now) if now == key => {
                            self.counts[pos].fetch_add(cnt, ShOrdering::Relaxed);
                            release += 1;
                            break;
                        }
                        Err(_) => { /* raced claim by another key: keep probing */ }
                    }
                } else {
                    pos = (pos + 1) & self.mask;
                    continue;
                }
                // CAS lost to a DIFFERENT key landing in this slot: advance.
                pos = (pos + 1) & self.mask;
            }
        }
        if release > 0 {
            self.members.fetch_sub(release, ShOrdering::AcqRel);
        }
        if let Some(block) = run.null_states.as_ref() {
            self.null_present.store(true, ShOrdering::Release);
            self.null_count.fetch_add(block[0], ShOrdering::Relaxed);
        }
        true
    }

    /// Claimed slots + in-flight reservations (observability; equals the
    /// live distinct group count at quiescence).
    pub fn reserved(&self) -> usize {
        self.members.load(ShOrdering::Relaxed)
    }

    pub fn is_closed(&self) -> bool {
        self.closed.load(ShOrdering::Relaxed)
    }

    /// Seal-time drain into a bucket-major [`SinkRun`] (the exact
    /// sink_flush_table framing, so the combine consumes it like any
    /// flushed run). SINGLE-THREADED BY CONTRACT: called at SEAL, after
    /// every accept worker has settled (the runtime's seal ordering is the
    /// happens-before). Rows are sorted by key first — a given dataset
    /// drains deterministically regardless of insertion races. `None` =
    /// nothing was absorbed.
    pub fn drain_to_run(&self) -> Option<SinkRun> {
        let mut pairs: Vec<(u64, u64)> = Vec::with_capacity(self.reserved().min(self.mask + 1));
        for pos in 0..=self.mask {
            let key = self.keys[pos].load(ShOrdering::Acquire);
            if key != 0 {
                pairs.push((key, self.counts[pos].load(ShOrdering::Acquire)));
            }
        }
        if self.zero_present.load(ShOrdering::Acquire) {
            pairs.push((0, self.zero_count.load(ShOrdering::Acquire)));
        }
        let null = self
            .null_present
            .load(ShOrdering::Acquire)
            .then(|| vec![self.null_count.load(ShOrdering::Acquire), 0]);
        if pairs.is_empty() && null.is_none() {
            return None;
        }
        pairs.sort_unstable_by_key(|&(k, _)| k);
        // Bucket-major framing, verbatim from sink_flush_table.
        let mut counts = [0u32; SINK_NBUCKETS];
        for &(k, _) in &pairs {
            counts[bucket_of(sink_hash(k, 0))] += 1;
        }
        let mut starts: Vec<u32> = Vec::with_capacity(SINK_NBUCKETS + 1);
        let mut acc = 0u32;
        starts.push(0);
        for c in counts {
            acc += c;
            starts.push(acc);
        }
        let mut cursor: [u32; SINK_NBUCKETS] = core::array::from_fn(|b| starts[b]);
        let n = pairs.len();
        let mut keys: Vec<u64> = vec![0; n];
        // Emitted states are valid AggPerGroup blocks: [count, flags=0].
        let mut states: Vec<u64> = vec![0; n * 2];
        for &(k, c) in &pairs {
            let b = bucket_of(sink_hash(k, 0));
            let slot = cursor[b] as usize;
            cursor[b] += 1;
            keys[slot] = k;
            states[slot * 2] = c;
        }
        Some(SinkRun {
            key_words: 1,
            key_ends: Vec::new(),
            state_words: 2,
            starts,
            keys,
            states,
            null_states: null,
            key_offs: Vec::new(),
            key_bytes: Vec::new(),
            hashes: Vec::new(),
            gid_gen: 0,
        })
    }
}

impl Drop for SharedCountTable {
    fn drop(&mut self) {
        // Ledger balance for the construction charge (GL-CONCMEM-1); the
        // slot count is fixed for the table's lifetime.
        ::mcx::global_footprint::uncharge_engine_estate(Self::slot_estate_bytes(
            self.keys.len(),
        ));
    }
}

/// Experiment gate helper: every aggregate is a COUNT (count(*) 2803 /
/// count(col) 2147), unfiltered, un-DISTINCTed — the i64-add state class
/// the shared count face folds atomically.
pub fn agg_sink_all_count(node: &AggStateData<'_>) -> bool {
    const COUNT_STAR: Oid = 2803;
    const COUNT_ANY: Oid = 2147;
    !node.peragg.is_empty()
        && node.peragg.iter().all(|pa| {
            let ar = pa.aggref;
            (ar.aggfnoid == COUNT_STAR || ar.aggfnoid == COUNT_ANY)
                && ar.aggfilter.is_none()
                && ar.aggdistinct.is_nil()
                && ar.aggorder.is_nil()
        })
}

// ---------------------------------------------------------------------------
// SCATTER ACCEPT (fold-bypass) — the near-unique-key drain's per-row law.
// ---------------------------------------------------------------------------
//
// On admitted low-α engagements (est_rows ≈ est_groups) nearly every accept
// row is a probe MISS: the worker table's probe + insert + cap-flush cycle
// does per-row work that folds nothing. The scatter accept SKIPS the worker
// hash table for the whole drain: each qualifying row becomes ONE
// SINGLE-ROW STATE BLOCK (the fold of one row from the seeded init state —
// count → 1, sum → the value) appended straight into bucket-contiguous
// buffers keyed by [`sink_hash`]'s top byte, exactly the [`SinkRun`] radix
// space. A buffer flush IS an ordinary run: entries may repeat a key (the
// duplicates the table would have folded), and the combine consumes repeats
// through the same probe-or-merge arithmetic it already runs across faces —
// a scatter run is just a run whose windows collapsed to α = 1.
//
// Byte identity (unit-pinned by `scatter_runs_combine_matches_fold`,
// modeled on `seal_flush_run_matches_remainder_view`): the admitted kinds
// (count/int-sum/int-min/max) combine associatively bit-exactly, and rows
// scatter in arrival order into stable per-bucket buffers, so the merged
// table's first-seen row order and every (trans_value, isnull) pair equal
// the fold path's — flush cadence is semantics-free (the ratified
// flush-cadence law the seal-flush arm rides). The mid-transition
// `no_trans_value` scratch flag is OUTSIDE this claim: the combine itself
// clears it on any second arrival, so it is cadence-dependent under the
// incumbent cadences too (a group split across two cap windows vs one) and
// nothing post-combine reads it on a scatter-admitted shape (topn — the one
// reader — is excluded at admission).
//
// NULL-key rows fold into a per-buffer NULL accumulator block (the same
// one-row ops applied cumulatively — for the admitted kinds the accumulate
// IS the fold) and ride the next flushed run's out-of-band `null_states`,
// the incumbent's own channel.

/// The scatter accept's per-worker state: 256 bucket-contiguous key/state
/// buffers plus the seeded init template and the validated fold
/// descriptors. Lives in the sink Local between morsels (the table's own
/// discipline); flushed to [`SinkRun`]s at the row cap and at SEAL.
pub struct SinkScatter {
    trans: Vec<::lanefold::LaneTrans>,
    /// Seeded init state block (`initialize_hash_entry`'s field values, the
    /// `seed_new_groups` law): one 16-byte `AggPerGroup` image per transno,
    /// padding zeroed (padding is not part of any consumer's read).
    init: Vec<u64>,
    /// Key canonicalization width (2/4/8 — the `agg_hash_compact_batch`
    /// sign-extension law).
    key_width: u8,
    keys: Vec<Vec<u64>>,
    states: Vec<Vec<u64>>,
    nrows: usize,
    null_block: Option<Vec<u64>>,
}

/// The scatter fold whitelist: kinds whose one-row synthesis is trivial and
/// whose combine arithmetic is associative BIT-EXACTLY (integer wrapping
/// adds, integer min/max), so single-row blocks re-associate to the fold's
/// bytes. Order-sensitive kinds (float sums/accums), pointer states, str
/// kinds, and filtered transitions refuse.
fn scatter_kind_ok(t: &::lanefold::LaneTrans) -> bool {
    use ::lanefold::{LaneKind, LaneWidth};
    let int_w = |w: LaneWidth| matches!(w, LaneWidth::I16 | LaneWidth::I32 | LaneWidth::I64);
    if t.filter != ::lanefold::NO_FILTER {
        return false;
    }
    match t.kind {
        LaneKind::CountStar => true,
        LaneKind::CountAny => true,
        LaneKind::Sum => int_w(t.width),
        LaneKind::Min | LaneKind::Max => int_w(t.width) && int_w(t.res_width),
        _ => false,
    }
}

/// Leader/worker scatter shape verdict (the F1 both-sides-same-inputs law):
/// the fold plan must be total (no residual transitions), proof-free (no
/// guards — a demote path would need the per-row program the scatter
/// deleted), all-whitelist, and every transition state byval (single-row
/// blocks are copied verbatim into runs).
pub fn sink_scatter_admits(node: &AggStateData<'_>) -> bool {
    let Some(plan) = crate::agg_lanefold_plan(node) else { return false };
    if plan.guarded || !plan.resid.is_empty() || !plan.vguards.is_empty() {
        return false;
    }
    if !plan.trans.iter().all(scatter_kind_ok) {
        return false;
    }
    node.trans_typ.iter().all(|t| t.byval)
}

/// Build the worker's scatter state. `None` = the shape verdict diverged
/// from the leader's admission (the caller fail-closes).
pub fn sink_scatter_new(node: &AggStateData<'_>, key_width: u8) -> Option<SinkScatter> {
    if !sink_scatter_admits(node) {
        return None;
    }
    let plan = crate::agg_lanefold_plan(node)?;
    let trans: Vec<::lanefold::LaneTrans> = plan.trans.iter().copied().collect();
    // The init template — `seed_new_groups`'s exact field values over a
    // zeroed block (byval-only by the admission above, so no datumCopy).
    let mut init = vec![0u64; node.numtrans * 2];
    for (transno, iv) in node.trans_init.iter().enumerate() {
        scatter_seed_slot(&mut init, transno, iv.value, iv.isnull);
    }
    Some(SinkScatter::from_parts(trans, init, key_width))
}

/// Write one transno's `AggPerGroup` init image into a state block: word 0
/// the init datum, bytes 8/9 the `trans_value_is_null`/`no_trans_value`
/// flags (both = the initval's nullness, `initialize_hash_entry`'s law),
/// padding left zero.
fn scatter_seed_slot(block: &mut [u64], transno: usize, value: Datum, isnull: bool) {
    const {
        assert!(core::mem::size_of::<AggPerGroup>() == 16);
        assert!(core::mem::align_of::<AggPerGroup>() == 8);
    }
    block[transno * 2] = value.as_u64();
    block[transno * 2 + 1] = if isnull { 0x0101 } else { 0 };
}

// --- One-row / accumulate kernels: mirrors of the lanefold fold bodies
// (`xform`, `lane_value`, `count_apply`, `sum_apply`, `minmax_advance`,
// `store_res`/`load_res` in lanefold/src/lib.rs). Kept textually tiny so
// drift is reviewable; the admitted-kind subset only ever reaches these.

#[inline(always)]
fn sc_xform(t: &::lanefold::LaneTrans, v: i64) -> i64 {
    let v = if t.divk != 1 { v / t.divk as i64 } else { v };
    let v = if t.mulk != 1 { v.wrapping_mul(t.mulk as i64) } else { v };
    v.wrapping_add(t.addend as i64)
}

#[inline(always)]
fn sc_lane_value(values: &[Datum], w: ::lanefold::LaneWidth, i: usize) -> i64 {
    match w {
        ::lanefold::LaneWidth::I16 => values[i].as_i16() as i64,
        ::lanefold::LaneWidth::I32 => values[i].as_i32() as i64,
        ::lanefold::LaneWidth::I64 => values[i].as_i64(),
        // scatter_kind_ok admits integer widths only.
        _ => unreachable!("non-integer lane width on a scatter transition"),
    }
}

#[inline(always)]
fn sc_store_res(t: &::lanefold::LaneTrans, v: i64) -> Datum {
    match t.res_width {
        ::lanefold::LaneWidth::I16 => Datum::from_i16(v as i16),
        ::lanefold::LaneWidth::I32 => Datum::from_i32(v as i32),
        ::lanefold::LaneWidth::I64 => Datum::from_i64(v),
        _ => unreachable!("non-integer result width on a scatter transition"),
    }
}

#[inline(always)]
fn sc_load_res(t: &::lanefold::LaneTrans, pg: &AggPerGroup) -> i64 {
    match t.res_width {
        ::lanefold::LaneWidth::I16 => pg.trans_value.as_i16() as i64,
        ::lanefold::LaneWidth::I32 => pg.trans_value.as_i32() as i64,
        ::lanefold::LaneWidth::I64 => pg.trans_value.as_i64(),
        _ => unreachable!("non-integer result width on a scatter transition"),
    }
}

/// Apply one admitted transition for one staged row into `pg` — the exact
/// per-row bodies `fold_rows_grouped` runs, so the same function serves the
/// single-row synthesis (over a fresh init block) AND the NULL-group
/// accumulator (cumulative application).
#[inline(always)]
fn scatter_apply_row(
    t: &::lanefold::LaneTrans,
    pg: &mut AggPerGroup,
    values: &[Datum],
    isnull: &[bool],
    i: usize,
) {
    use ::lanefold::LaneKind;
    match t.kind {
        LaneKind::CountStar => {
            pg.trans_value = Datum::from_i64(pg.trans_value.as_i64().wrapping_add(1));
            pg.trans_value_is_null = false;
            pg.no_trans_value = false;
        }
        LaneKind::CountAny => {
            if !isnull[i] {
                pg.trans_value = Datum::from_i64(pg.trans_value.as_i64().wrapping_add(1));
                pg.trans_value_is_null = false;
                pg.no_trans_value = false;
            }
        }
        LaneKind::Sum => {
            if !isnull[i] {
                let delta = sc_xform(t, sc_lane_value(values, t.width, i));
                let old = if pg.trans_value_is_null { 0 } else { pg.trans_value.as_i64() };
                pg.trans_value = Datum::from_i64(old.wrapping_add(delta));
                pg.trans_value_is_null = false;
            }
        }
        LaneKind::Min | LaneKind::Max => {
            if !isnull[i] {
                let v = sc_xform(t, sc_lane_value(values, t.width, i));
                if pg.no_trans_value {
                    pg.trans_value = sc_store_res(t, v);
                    pg.trans_value_is_null = false;
                    pg.no_trans_value = false;
                } else if !pg.trans_value_is_null {
                    let old = sc_load_res(t, pg);
                    let next =
                        if t.kind == LaneKind::Max { old.max(v) } else { old.min(v) };
                    if next != old {
                        pg.trans_value = sc_store_res(t, next);
                    }
                }
            }
        }
        _ => unreachable!("non-whitelist kind on a scatter transition"),
    }
}

impl SinkScatter {
    /// Shared constructor (the unit tests build descriptor sets directly;
    /// `sink_scatter_new` is the executor's validated path).
    fn from_parts(
        trans: Vec<::lanefold::LaneTrans>,
        init: Vec<u64>,
        key_width: u8,
    ) -> SinkScatter {
        SinkScatter {
            trans,
            init,
            key_width,
            keys: (0..SINK_NBUCKETS).map(|_| Vec::new()).collect(),
            states: (0..SINK_NBUCKETS).map(|_| Vec::new()).collect(),
            nrows: 0,
            null_block: None,
        }
    }

    /// Buffered non-NULL rows (the flush-cadence clock).
    #[inline]
    pub fn nrows(&self) -> usize {
        self.nrows
    }

    /// Heap bytes held against the Local's budget (capacity, the
    /// [`SinkRun::bytes`] discipline).
    pub fn bytes(&self) -> usize {
        self.keys.iter().map(|v| v.capacity() * 8).sum::<usize>()
            + self.states.iter().map(|v| v.capacity() * 8).sum::<usize>()
            + self.null_block.as_ref().map_or(0, |b| b.capacity() * 8)
    }

    /// Scatter one staged batch's survivors: for each row, canonicalize the
    /// key (the `agg_hash_compact_batch` width law), radix-route on
    /// [`sink_hash`]'s top byte, and append the one-row state block.
    /// `keys`/`knull` are parallel to `rows`; `cols` answers the staged
    /// lanes the transitions read.
    pub fn absorb_batch(
        &mut self,
        cols: &impl ::lanefold::LaneCols,
        rows: &[u32],
        keys: &[Datum],
        knull: &[bool],
    ) {
        debug_assert_eq!(rows.len(), keys.len());
        debug_assert_eq!(rows.len(), knull.len());
        let SinkScatter { trans, init, key_width, keys: bkeys, states, nrows, null_block } =
            self;
        let hoisted: Vec<(&[Datum], &[bool])> = trans
            .iter()
            .map(|t| (cols.col_values(t.col as usize), cols.col_isnull(t.col as usize)))
            .collect();
        let state_words = init.len();
        for (j, &row) in rows.iter().enumerate() {
            let i = row as usize;
            if knull[j] {
                let blk = null_block.get_or_insert_with(|| init.clone());
                let pgs = blk.as_mut_ptr().cast::<AggPerGroup>();
                for (t, (values, isnull)) in trans.iter().zip(hoisted.iter()) {
                    // SAFETY: blk holds numtrans AggPerGroup slots (the
                    // init template's layout).
                    scatter_apply_row(t, unsafe { &mut *pgs.add(t.transno as usize) },
                        values, isnull, i);
                }
                continue;
            }
            let k = match key_width {
                2 => keys[j].as_i16() as i64,
                4 => keys[j].as_i32() as i64,
                _ => keys[j].as_i64(),
            };
            let w0 = k as u64;
            let b = bucket_of(sink_hash(w0, 0));
            bkeys[b].push(w0);
            let st = &mut states[b];
            let base = st.len();
            st.extend_from_slice(init);
            let pgs = st[base..base + state_words].as_mut_ptr().cast::<AggPerGroup>();
            for (t, (values, isnull)) in trans.iter().zip(hoisted.iter()) {
                // SAFETY: the freshly appended block holds numtrans
                // AggPerGroup slots (the init template's layout).
                scatter_apply_row(t, unsafe { &mut *pgs.add(t.transno as usize) },
                    values, isnull, i);
            }
            *nrows += 1;
        }
    }

    /// Assemble the buffered rows into one bucket-contiguous [`SinkRun`]
    /// (single-word keys, arrival order preserved per bucket) and reset the
    /// buffers (allocations retained — the flush re-arm discipline).
    /// `None` = nothing buffered.
    pub fn take_run(&mut self) -> Option<SinkRun> {
        if self.nrows == 0 && self.null_block.is_none() {
            return None;
        }
        let state_words = self.init.len();
        let mut starts: Vec<u32> = Vec::with_capacity(SINK_NBUCKETS + 1);
        let mut acc = 0u32;
        starts.push(0);
        for b in 0..SINK_NBUCKETS {
            acc += self.keys[b].len() as u32;
            starts.push(acc);
        }
        let mut keys: Vec<u64> = Vec::with_capacity(self.nrows);
        let mut states: Vec<u64> = Vec::with_capacity(self.nrows * state_words);
        for b in 0..SINK_NBUCKETS {
            keys.extend_from_slice(&self.keys[b]);
            states.extend_from_slice(&self.states[b]);
            self.keys[b].clear();
            self.states[b].clear();
        }
        self.nrows = 0;
        Some(SinkRun {
            key_words: 1,
            key_ends: Vec::new(),
            state_words,
            starts,
            keys,
            states,
            null_states: self.null_block.take(),
            key_offs: Vec::new(),
            key_bytes: Vec::new(),
            hashes: Vec::new(),
            gid_gen: 0,
        })
    }
}

// ---------------------------------------------------------------------------
// Unit tests: pure kernels, no executor.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- SharedCountTable (cachebudget D2 experiment) ----------------

    /// Build a single-word count run from (key, count) pairs (+ optional
    /// NULL-group count), framed exactly like sink_flush_table.
    fn count_run(pairs: &[(u64, u64)], null: Option<u64>) -> SinkRun {
        let mut counts = [0u32; SINK_NBUCKETS];
        for &(k, _) in pairs {
            counts[bucket_of(sink_hash(k, 0))] += 1;
        }
        let mut starts = vec![0u32];
        let mut acc = 0u32;
        for c in counts {
            acc += c;
            starts.push(acc);
        }
        let mut cursor: [u32; SINK_NBUCKETS] = core::array::from_fn(|b| starts[b]);
        let mut keys = vec![0u64; pairs.len()];
        let mut states = vec![0u64; pairs.len() * 2];
        for &(k, c) in pairs {
            let b = bucket_of(sink_hash(k, 0));
            let s = cursor[b] as usize;
            cursor[b] += 1;
            keys[s] = k;
            states[s * 2] = c;
        }
        SinkRun {
            key_words: 1,
            key_ends: Vec::new(),
            state_words: 2,
            starts,
            keys,
            states,
            null_states: null.map(|c| vec![c, 0]),
            key_offs: Vec::new(),
            key_bytes: Vec::new(),
            hashes: Vec::new(),
            gid_gen: 0,
        }
    }

    fn drained_map(t: &SharedCountTable) -> (std::collections::HashMap<u64, u64>, Option<u64>) {
        let run = t.drain_to_run().expect("non-empty drain");
        // Bucket framing must be internally consistent.
        assert_eq!(run.starts.len(), SINK_NBUCKETS + 1);
        assert_eq!(*run.starts.last().unwrap() as usize, run.nrows());
        for b in 0..SINK_NBUCKETS {
            for i in run.starts[b] as usize..run.starts[b + 1] as usize {
                assert_eq!(bucket_of(sink_hash(run.keys[i], 0)), b, "bucket-major framing");
            }
        }
        let map = run
            .keys
            .iter()
            .copied()
            .zip(run.states.chunks(2).map(|st| {
                assert_eq!(st[1] & 0xFFFF, 0, "drained states are valid non-null AggPerGroup blocks");
                st[0]
            }))
            .collect();
        (map, run.null_states.as_ref().map(|b| b[0]))
    }

    /// Concurrent merges against a HashMap oracle: 8 threads × interleaved
    /// runs over an adversarial key set (dense small ints incl. the literal
    /// 0 side-slot key, plus hash-colliding sparse keys), with a NULL
    /// group. The drain must equal the oracle exactly.
    #[test]
    fn shared_count_concurrent_oracle() {
        // Capacity must clear the worst-case TRANSIENT reservation window
        // (Σ concurrent runs' rows ≈ 8×900 in-flight + ≤900 claimed), not
        // just the distinct-key count — the release law keeps steady-state
        // at claimed slots, but pre-checks see in-flight reservations.
        let t = SharedCountTable::new(16384);
        let mut oracle: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        let mut per_thread: Vec<Vec<(Vec<(u64, u64)>, Option<u64>)>> = vec![Vec::new(); 8];
        let mut null_total = 0u64;
        for th in 0..8u64 {
            for r in 0..6u64 {
                let mut pairs = Vec::new();
                for i in 0..500u64 {
                    // Overlapping keys across threads/runs; key 0 included.
                    let k = (i * (th % 3 + 1) + r) % 900;
                    let c = th + r + i % 7 + 1;
                    pairs.push((k, c));
                    *oracle.entry(k).or_insert(0) += c;
                }
                let null = (r % 2 == 0).then_some(th + r + 1);
                if let Some(n) = null {
                    null_total += n;
                }
                // count_run wants unique keys per run (a real flush is a
                // dedup'd table) — fold duplicates first.
                let mut folded: std::collections::HashMap<u64, u64> =
                    std::collections::HashMap::new();
                for (k, c) in pairs {
                    *folded.entry(k).or_insert(0) += c;
                }
                let folded: Vec<(u64, u64)> = folded.into_iter().collect();
                // Rebuild the oracle-side dedup accounting: already added above.
                per_thread[th as usize].push((folded, null));
            }
        }
        let tr = &t;
        std::thread::scope(|s| {
            for runs in per_thread.iter() {
                s.spawn(move || {
                    for (pairs, null) in runs {
                        assert!(tr.merge_run(&count_run(pairs, *null)), "capacity was sized");
                    }
                });
            }
        });
        assert!(!t.is_closed());
        let (map, null) = drained_map(&t);
        assert_eq!(map, oracle);
        assert_eq!(null, Some(null_total));
    }

    /// The reservation RELEASE law: repeated runs over the SAME key set —
    /// the production steady state (every key recurs in ~W×flushes runs) —
    /// must never burn capacity. Under the pre-release design this closed
    /// the face after ~cap total flushed ROWS regardless of NDV (the wave-3
    /// fleet failure).
    #[test]
    fn shared_count_repeat_keys_do_not_burn_capacity() {
        let t = SharedCountTable::new(512);
        let pairs: Vec<(u64, u64)> = (1..=100u64).map(|k| (k, 1)).collect();
        for _ in 0..1000 {
            assert!(t.merge_run(&count_run(&pairs, None)), "repeat keys must release");
        }
        assert!(!t.is_closed());
        assert_eq!(t.reserved(), 100, "steady state counts claimed slots only");
        let (map, _) = drained_map(&t);
        assert_eq!(map.len(), 100);
        assert!(map.values().all(|&c| c == 1000));
    }

    /// The spill fallback: a reservation crossing capacity CLOSES the face
    /// with the run unmerged, and everything absorbed before the close
    /// drains intact (the refused run rides the incumbent path).
    #[test]
    fn shared_count_overflow_closes() {
        let t = SharedCountTable::new(64);
        let small: Vec<(u64, u64)> = (1..=40u64).map(|k| (k, 1)).collect();
        assert!(t.merge_run(&count_run(&small, None)));
        let big: Vec<(u64, u64)> = (100..=200u64).map(|k| (k, 2)).collect();
        assert!(!t.merge_run(&count_run(&big, None)), "over-capacity run refused");
        assert!(t.is_closed());
        // Closed face refuses everything, even a tiny run.
        assert!(!t.merge_run(&count_run(&[(7, 1)], None)));
        let (map, null) = drained_map(&t);
        assert_eq!(map.len(), 40);
        assert_eq!(map[&7], 1);
        assert_eq!(null, None);
    }

    /// Drain determinism: same content, different insertion order →
    /// byte-identical runs (the sort-by-key law).
    #[test]
    fn shared_count_drain_deterministic() {
        let a = SharedCountTable::new(256);
        let b = SharedCountTable::new(256);
        let pairs: Vec<(u64, u64)> = (0..100u64).map(|k| (k * 37 % 251, k + 1)).collect();
        let mut folded: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
        for &(k, c) in &pairs {
            *folded.entry(k).or_insert(0) += c;
        }
        let folded: Vec<(u64, u64)> = folded.into_iter().collect();
        let mut rev = folded.clone();
        rev.reverse();
        assert!(a.merge_run(&count_run(&folded, Some(3))));
        assert!(b.merge_run(&count_run(&rev, Some(3))));
        let ra = a.drain_to_run().unwrap();
        let rb = b.drain_to_run().unwrap();
        assert_eq!(ra.keys, rb.keys);
        assert_eq!(ra.states, rb.states);
        assert_eq!(ra.starts, rb.starts);
        assert_eq!(ra.null_states, rb.null_states);
    }

    /// Empty face drains to None (no ghost runs at seal).
    #[test]
    fn shared_count_empty_drain() {
        assert!(SharedCountTable::new(64).drain_to_run().is_none());
    }

    /// GL-CONCMEM-1: the shared absorb face's process-ledger charge
    /// balances across construction and Drop. Delta-based (other tests run
    /// concurrently on the process-global counter) with a noise allowance
    /// on the held bound; the FINAL bound retries — a leaked charge is
    /// permanent and never converges (the lanetable balance-test law).
    #[test]
    fn shared_count_ledger_balances() {
        const NOISE: usize = 16 << 20;
        let expect = SharedCountTable::slot_estate_bytes(1 << 21);
        let base = ::mcx::global_footprint::bytes();
        // Concurrent tests move the process-global counter in BOTH
        // directions; the held bound retries over fresh construction
        // windows (a missing charge fails every window), and the final
        // bound retries after Drop against the pre-loop base (a leaked
        // charge is permanent — it accumulates across the windows and
        // never converges back).
        let mut charged = false;
        for _ in 0..8 {
            let pre = ::mcx::global_footprint::bytes();
            // 1M members → slots = 2^21 → two 16MB word arrays charged.
            let t = SharedCountTable::new(1 << 20);
            let held = ::mcx::global_footprint::bytes();
            drop(t);
            if held + NOISE >= pre + expect {
                charged = true;
                break;
            }
        }
        assert!(charged, "construction did not charge the ledger (expect {expect})");
        // Upper bound only: a leak leaves the counter permanently HIGH;
        // concurrent tests whose holdings were inside `base` can leave it
        // legitimately lower (one-sided — the leak direction).
        let mut ok = false;
        for _ in 0..50 {
            let after = ::mcx::global_footprint::bytes();
            if after <= base + NOISE {
                ok = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
        let after = ::mcx::global_footprint::bytes();
        assert!(ok, "ledger did not balance after Drop: base {base}, after {after}");
    }

    /// FAIL-CLOSED flags pre-pass: a null-marked (or padding-dirty) state
    /// block refuses the WHOLE run before any reservation or slot write —
    /// refusal, not closure (later clean runs still merge).
    #[test]
    fn shared_count_nonzero_flags_refused() {
        let t = SharedCountTable::new(64);
        let mut bad = count_run(&[(5, 1)], None);
        bad.states[1] = 1; // trans_value_is_null byte
        assert!(!t.merge_run(&bad));
        let mut bad2 = count_run(&[(6, 1)], None);
        bad2.states[1] = 1 << 8; // no_trans_value byte
        assert!(!t.merge_run(&bad2));
        assert!(!t.is_closed());
        assert_eq!(t.reserved(), 0, "refused before reserving");
        assert!(t.drain_to_run().is_none());
        // UNDEFINED PADDING (bytes 2..8 of the flags word) must be
        // ACCEPTED — fold-written AggPerGroup padding is garbage in
        // practice (the wave-5 all-runs-refused failure).
        let mut padded = count_run(&[(5, 2)], None);
        padded.states[1] = 0xDEAD_BEEF_0000_0000 | (0xCAFE << 16);
        assert!(t.merge_run(&padded));
        let (map, _) = drained_map(&t);
        assert_eq!(map[&5], 2);
    }

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
        bump_probe(pr, count, max);
    }

    // The same two toy transitions over a canonical-bytes key (the c3 text
    // car's table repr — the topn x bytes composition corpus).
    fn bump_bytes(t: &mut LaneAggTable, key: Option<&[u8]>, count: i64, max: i64) {
        let pr = match key {
            Some(k) => t.probe_bytes(k, t.hash_key_bytes(k)),
            None => t.probe_null(),
        };
        bump_probe(pr, count, max);
    }

    fn bump_probe(pr: ::lanetable::Probe, count: i64, max: i64) {
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
            SinkLocalView { spilled: &[], runs: core::slice::from_ref(&run1), remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None, canon_store: None, gid_gen: 0, direct: false }) },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(SinkRemainder { table: &t2, part: &part2, canon: None, canon_store: None, gid_gen: 0, direct: false }) },
        ];
        let combines = test_combines();
        let mut merged: Vec<CombinedBucket> = Vec::with_capacity(SINK_NBUCKETS);
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
    fn seal_flush_run_matches_remainder_view() {
        // The seal-flush arm's byte-identity claim, unit form: combining a
        // Local's remainder through the SEAL index (incumbent) and through a
        // final flush_remainder run appended LAST (seal-flush) must produce
        // per-bucket tables identical in row order and state content.
        let build = || {
            let mut t1 = mk_table(64);
            for k in 0..1000 {
                bump(&mut t1, Some(k), 1, k);
            }
            bump(&mut t1, None, 1, 7);
            let run = sink_flush_table(&mut t1);
            for k in 300..1300 {
                bump(&mut t1, Some(k), 2, 2 * k);
            }
            bump(&mut t1, None, 2, 3);
            let ch = crate::compact::compact_hash_for_tests(
                t1,
                crate::compact::CompactKeySpec::Single { width: 8 },
                None,
            );
            (SinkTableHandle(ch), run)
        };
        let combines = test_combines();

        // Incumbent: run + remainder view over the SEAL index.
        let (mut h_a, run_a) = build();
        let part_a = h_a.partition_remainder();
        let rem_a = h_a.remainder_view(&part_a);
        let locals_a = [SinkLocalView {
            spilled: &[],
            runs: core::slice::from_ref(&run_a),
            remainder: Some(rem_a),
        }];

        // Seal-flush: the remainder leaves as one more run, appended last.
        let (mut h_b, run_b) = build();
        let flushed = h_b.flush_remainder().expect("non-empty remainder");
        assert_eq!(h_b.table().nrows(), 0, "flush drains the table");
        assert!(h_b.flush_remainder().is_none(), "empty table flushes nothing");
        let runs_b = [run_b, flushed];
        let locals_b =
            [SinkLocalView { spilled: &[], runs: &runs_b, remainder: None }];

        for b in 0..SINK_NBUCKETS {
            let ta = sink_combine_bucket(b, 1, STATE_BYTES, &locals_a, &combines).unwrap();
            let tb = sink_combine_bucket(b, 1, STATE_BYTES, &locals_b, &combines).unwrap();
            assert_eq!(ta.nrows(), tb.nrows(), "bucket {b} row count");
            for row in 0..ta.nrows() {
                assert_eq!(
                    ta.row_key_int(row),
                    tb.row_key_int(row),
                    "bucket {b} row {row} key (first-seen order)"
                );
                assert_eq!(
                    row_counts(&ta, row),
                    row_counts(&tb, row),
                    "bucket {b} row {row} states"
                );
            }
        }
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
            SinkLocalView { spilled: &[], runs: core::slice::from_ref(&run1), remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None, canon_store: None, gid_gen: 0, direct: false }) },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(SinkRemainder { table: &t2, part: &part2, canon: None, canon_store: None, gid_gen: 0, direct: false }) },
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

    /// Row (count, max) via the toy AggPerGroup pair.
    fn row_counts(t: &LaneAggTable, row: usize) -> (i64, i64) {
        let pg = t.row_states(row).cast_const().cast::<AggPerGroup>();
        unsafe { ((*pg).trans_value.as_i64(), (*pg.add(1)).trans_value.as_i64()) }
    }

    /// numa-combine item 1, the superset-lemma-style order law: merging
    /// CONTIGUOUS halves of the locals slice into partial single-bucket runs
    /// ([`sink_run_from_bucket_table`]) and re-merging the two partials is
    /// ROW-FOR-ROW identical (keys, insertion order, states) to the flat
    /// pass — first-seen order composes across contiguous halves. Word keys.
    #[test]
    fn two_level_partial_runs_match_flat_combine() {
        // Four locals with overlapping keys, mixed faces: l0 run+remainder,
        // l1 remainder, l2 run, l3 remainder. No NULL group — the two-level
        // arm routes SINK_NULL_BUCKET flat (SinkRun carries NULLs
        // out-of-band, which would move the NULL group's first-seen slot).
        let mut t0 = mk_table(32);
        for k in 0..600 {
            bump(&mut t0, Some(k), 1, k);
        }
        let run0 = sink_flush_table(&mut t0);
        for k in 300..900 {
            bump(&mut t0, Some(k), 2, 2 * k);
        }
        let part0 = sink_partition_remainder(&t0);
        let mut t1 = mk_table(32);
        for k in (0..1200).step_by(3) {
            bump(&mut t1, Some(k), 1, 5);
        }
        let part1 = sink_partition_remainder(&t1);
        let mut t2 = mk_table(32);
        for k in 450..1050 {
            bump(&mut t2, Some(k), 4, 3 * k);
        }
        let run2 = sink_flush_table(&mut t2);
        let part2 = sink_partition_remainder(&t2);
        let mut t3 = mk_table(32);
        for k in (1..1500).step_by(7) {
            bump(&mut t3, Some(k), 1, k + 1);
        }
        let part3 = sink_partition_remainder(&t3);

        let locals = [
            SinkLocalView {
                spilled: &[],
                runs: core::slice::from_ref(&run0),
                remainder: Some(SinkRemainder { table: &t0, part: &part0, canon: None, canon_store: None, gid_gen: 0, direct: false }),
            },
            SinkLocalView {
                spilled: &[],
                runs: &[],
                remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None, canon_store: None, gid_gen: 0, direct: false }),
            },
            SinkLocalView {
                spilled: &[],
                runs: core::slice::from_ref(&run2),
                remainder: Some(SinkRemainder { table: &t2, part: &part2, canon: None, canon_store: None, gid_gen: 0, direct: false }),
            },
            SinkLocalView {
                spilled: &[],
                runs: &[],
                remainder: Some(SinkRemainder { table: &t3, part: &part3, canon: None, canon_store: None, gid_gen: 0, direct: false }),
            },
        ];
        let combines = test_combines();
        for b in 0..SINK_NBUCKETS {
            let flat = sink_combine_bucket(b, 1, STATE_BYTES, &locals, &combines).unwrap();
            // Pass A: contiguous halves → partial single-bucket runs.
            let m0 = sink_combine_bucket(b, 1, STATE_BYTES, &locals[..2], &combines).unwrap();
            let m1 = sink_combine_bucket(b, 1, STATE_BYTES, &locals[2..], &combines).unwrap();
            let r0 = sink_run_from_bucket_table(b, &m0);
            let r1 = sink_run_from_bucket_table(b, &m1);
            assert_eq!(r0.nrows(), m0.nrows());
            assert_eq!(r1.nrows(), m1.nrows());
            // Final: 2-way merge of the partials, socket order.
            let partial_views = [
                SinkLocalView { spilled: &[], runs: core::slice::from_ref(&r0), remainder: None },
                SinkLocalView { spilled: &[], runs: core::slice::from_ref(&r1), remainder: None },
            ];
            let two =
                sink_combine_bucket(b, 1, STATE_BYTES, &partial_views, &combines).unwrap();
            assert_eq!(two.nrows(), flat.nrows(), "bucket {b} row count");
            for row in 0..flat.nrows() {
                assert_eq!(
                    two.row_key_int(row),
                    flat.row_key_int(row),
                    "bucket {b} row {row} key/order"
                );
                assert_eq!(
                    row_counts(&two, row),
                    row_counts(&flat, row),
                    "bucket {b} row {row} states"
                );
            }
        }
    }

    /// The two-level order law over CANONICAL BYTES keys (text-bearing
    /// shapes): partial runs re-derive the flush-side hash from the
    /// canonical image, and per-worker intern-id skew across the halves
    /// still merges on bytes — row-for-row identical to the flat pass.
    #[test]
    fn two_level_partial_runs_match_flat_combine_canon() {
        // Worker intern orders deliberately DIFFER (the canonical-bytes
        // hazard); texts span the packed8 (len <= 8) and arena arms.
        let mut w1 = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w1, Some(1), b"apple", 1);
        bump_canon(&mut w1, Some(1), b"banana", 2);
        bump_canon(&mut w1, Some(2), b"apple", 3);
        let run1 = sink_flush_table_canon(&mut w1);
        bump_canon(&mut w1, Some(1), b"apple", 10);
        bump_canon(&mut w1, Some(3), b"a-rather-long-canonical-key", 5);
        let mut h1 = SinkTableHandle(w1);
        let part1 = h1.partition_remainder();

        let mut w2 = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w2, Some(9), b"zzz", 7);
        bump_canon(&mut w2, Some(1), b"banana", 20);
        bump_canon(&mut w2, Some(1), b"apple", 30);
        let mut h2 = SinkTableHandle(w2);
        let part2 = h2.partition_remainder();

        let mut w3 = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w3, Some(3), b"a-rather-long-canonical-key", 100);
        bump_canon(&mut w3, Some(1), b"banana", 1);
        bump_canon(&mut w3, Some(4), b"", 6);
        let mut h3 = SinkTableHandle(w3);
        let part3 = h3.partition_remainder();

        let mut w4 = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w4, Some(2), b"apple", 2);
        bump_canon(&mut w4, Some(9), b"zzz", 3);
        let run4 = sink_flush_table_canon(&mut w4);
        bump_canon(&mut w4, Some(4), b"", 4);
        let mut h4 = SinkTableHandle(w4);
        let part4 = h4.partition_remainder();

        let locals = [
            SinkLocalView {
                spilled: &[],
                runs: core::slice::from_ref(&run1),
                remainder: Some(h1.remainder_view(&part1)),
            },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(h2.remainder_view(&part2)) },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(h3.remainder_view(&part3)) },
            SinkLocalView {
                spilled: &[],
                runs: core::slice::from_ref(&run4),
                remainder: Some(h4.remainder_view(&part4)),
            },
        ];
        let combines = test_combines();
        let mut groups = 0usize;
        for b in 0..SINK_NBUCKETS {
            let flat = sink_combine_bucket(b, 0, STATE_BYTES, &locals, &combines).unwrap();
            let m0 = sink_combine_bucket(b, 0, STATE_BYTES, &locals[..2], &combines).unwrap();
            let m1 = sink_combine_bucket(b, 0, STATE_BYTES, &locals[2..], &combines).unwrap();
            let r0 = sink_run_from_bucket_table(b, &m0);
            let r1 = sink_run_from_bucket_table(b, &m1);
            assert_eq!(r0.key_words, 0);
            assert_eq!(r0.hashes.len(), r0.nrows(), "carried-hash law on the partial run");
            let partial_views = [
                SinkLocalView { spilled: &[], runs: core::slice::from_ref(&r0), remainder: None },
                SinkLocalView { spilled: &[], runs: core::slice::from_ref(&r1), remainder: None },
            ];
            let two =
                sink_combine_bucket(b, 0, STATE_BYTES, &partial_views, &combines).unwrap();
            assert_eq!(two.nrows(), flat.nrows(), "bucket {b} row count");
            groups += flat.nrows();
            let (mut sa, mut sb) = ([0u8; 8], [0u8; 8]);
            for row in 0..flat.nrows() {
                assert_eq!(
                    two.row_key_bytes(row, &mut sa).map(<[u8]>::to_vec),
                    flat.row_key_bytes(row, &mut sb).map(<[u8]>::to_vec),
                    "bucket {b} row {row} canonical key/order"
                );
                assert_eq!(
                    row_counts(&two, row),
                    row_counts(&flat, row),
                    "bucket {b} row {row} states"
                );
            }
        }
        assert_eq!(groups, 6, "distinct (int8, text) groups across the corpus");
    }

    #[test]
    fn emit_bucket_identity_and_derived() {
        let mut t = mk_table(8);
        bump(&mut t, Some(41), 5, 9);
        bump(&mut t, None, 2, 1);
        let plan = SinkEmitPlan {
            width: 4,
            fixed: None,
            ntails: 0,
            filter: None,
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

    /// The HAVING emit filter (stragg-coverage inc-1): rows failing the
    /// `count <cmp> rhs` transvalue read never emit, on the whole-table,
    /// winners, and passthrough drivers alike; the NULL group is filtered
    /// by the same gate (its states are ordinary).
    #[test]
    fn emit_filter_drops_failing_groups() {
        let mut t = mk_table(8);
        bump(&mut t, Some(41), 5, 9); // count 5 -> passes  > 3
        bump(&mut t, Some(7), 2, 4); //  count 2 -> filtered
        bump(&mut t, None, 4, 1); //     NULL group, count 4 -> passes
        let mk_plan = |filter| SinkEmitPlan {
            width: 4,
            fixed: None,
            ntails: 0,
            filter,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::Agg { transno: 0 },
                SinkEmitCol::Agg { transno: 1 },
            ],
        };
        let unfiltered = sink_emit_bucket(&mk_plan(None), &t).unwrap();
        assert_eq!(unfiltered.nrows, 3);
        let plan = mk_plan(Some(SinkHavingFilter {
            transno: 0,
            cmp: HavingCmp::Gt,
            rhs: 3,
        }));
        let buf = sink_emit_bucket(&plan, &t).unwrap();
        assert_eq!(buf.nrows, 2);
        // Row 0 = key 41 (count 5); row 1 = NULL group (count 4); the
        // count-2 group never emitted.
        assert_eq!(buf.values[0].as_i32(), 41);
        assert_eq!(buf.values[1].as_i64(), 5);
        assert!(buf.nulls[3]); // NULL group key
        assert_eq!(buf.values[4].as_i64(), 4);
        // The mirrored spelling (`Const < count`) evaluates identically.
        assert_eq!(HavingCmp::Lt.mirror(), HavingCmp::Gt);
        assert!(HavingCmp::Gt.eval(5, 3) && !HavingCmp::Gt.eval(2, 3));
        // Operator-fn table: int8gt / int84gt / int48gt all name Gt.
        assert!(matches!(having_cmp_of(470), Some(HavingCmp::Gt)));
        assert!(matches!(having_cmp_of(477), Some(HavingCmp::Gt)));
        assert!(matches!(having_cmp_of(855), Some(HavingCmp::Gt)));
        assert!(having_cmp_of(0).is_none());
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
            sink_combine_states(&combines, dst.as_mut_ptr(), src.as_ptr(), None).unwrap();
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
            sink_combine_states(&combines, dst2.as_mut_ptr(), src.as_ptr(), None).unwrap();
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
            ntails: 0,
            filter: None,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::AvgInt128 { transno: 0 },
                SinkEmitCol::AvgInt8 { transno: 1, packed: false },
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

    // SE-T2AGG CAR B: a minimal plain 4B-U text varlena image (header +
    // payload) — the aggcontext form a min/max(text) transvalue holds.
    #[repr(C, align(8))]
    struct TextImage {
        buf: [u8; 32],
    }

    fn mk_text(payload: &[u8]) -> Box<TextImage> {
        assert!(payload.len() <= 28);
        let mut t = Box::new(TextImage { buf: [0; 32] });
        let size = (4 + payload.len()) as u32;
        t.buf[0..4].copy_from_slice(&(size << 2).to_le_bytes()); // varatt 4B-U
        t.buf[4..4 + payload.len()].copy_from_slice(payload);
        t
    }

    fn pg_of(img: &TextImage) -> AggPerGroup {
        AggPerGroup {
            trans_value: Datum::from_usize(img as *const _ as usize),
            trans_value_is_null: false,
            no_trans_value: false,
        }
    }

    /// Content bytes behind a text transvalue datum.
    fn text_payload(d: Datum) -> Vec<u8> {
        unsafe {
            let (p, l) = text_trans_payload(d).expect("plain varlena");
            core::slice::from_raw_parts(p, l).to_vec()
        }
    }

    /// SE-T2AGG CAR B: the VarlenaMinMax combine is the merge.rs kernel's
    /// pick — memcmp + length tiebreak, keep-dst only on a STRICT win (ties
    /// take src; byte-equal under the admitted collations, so unobservable) —
    /// but the winner is materialized C's way: the source is never adopted,
    /// it is datumCopied into the DESTINATION store and the superseded copy
    /// freed back to it. Emit then deep-copies the survivor image into the
    /// buf's own arena (byref discipline: never table-adopted).
    #[test]
    fn varlena_minmax_combine_and_emit() {
        let combines = vec![
            SinkCombineFn {
                func: test_combines()[0].func, // never called by this kind
                strict: true,
                collation: Oid::from(0u8),
                kind: SinkCombineKind::VarlenaMinMax { larger: false },
            },
            SinkCombineFn {
                func: test_combines()[0].func,
                strict: true,
                collation: Oid::from(0u8),
                kind: SinkCombineKind::VarlenaMinMax { larger: true },
            },
        ];
        // Byref accounting + the table-adopt refusal (self-containment law).
        assert!(sink_combines_byref(&combines));
        let plan = SinkEmitPlan {
            width: 8,
            fixed: None,
            ntails: 0,
            filter: None,
            cols: vec![SinkEmitCol::Key, SinkEmitCol::VarlenaTrans { transno: 0 }],
        };
        assert!(!sink_emit_plan_all_byval(&plan), "text survivors never table-adopt");

        let apple = mk_text(b"apple");
        let pear = mk_text(b"pear");
        let app = mk_text(b"app");
        let apple2 = mk_text(b"apple");

        // The destination store, and destination transvalues built through it
        // (the bucket-store invariant: `sink_combine_states` may only free a
        // superseded copy its own store allocated).
        let mut sa = StrStateArena::default();
        let owned = |sa: &mut StrStateArena, img: &TextImage| AggPerGroup {
            trans_value: unsafe { sa.copy(Datum::from_usize(img as *const _ as usize)) },
            trans_value_is_null: false,
            no_trans_value: false,
        };

        // min keeps "apple" vs "pear"; max takes a COPY of "pear".
        let mut dst = [owned(&mut sa, &apple), owned(&mut sa, &apple)];
        let keep = dst[0].trans_value.as_usize();
        let src = [pg_of(&pear), pg_of(&pear)];
        unsafe {
            sink_combine_states(&combines, dst.as_mut_ptr(), src.as_ptr(), Some(&mut sa)).unwrap()
        };
        assert_eq!(dst[0].trans_value.as_usize(), keep, "min keeps dst, untouched");
        assert_eq!(text_payload(dst[0].trans_value), b"apple");
        assert_eq!(text_payload(dst[1].trans_value), b"pear", "max takes src's value");
        assert_ne!(
            dst[1].trans_value.as_usize(),
            &*pear as *const _ as usize,
            "the source pointer is never adopted"
        );
        assert!(sa.owns(dst[1].trans_value.as_usize()), "the winner is store-allocated");

        // Length tiebreak on a shared prefix: "app" < "apple".
        let mut dst_len = [owned(&mut sa, &apple), owned(&mut sa, &apple)];
        let keep_len = dst_len[1].trans_value.as_usize();
        let src_len = [pg_of(&app), pg_of(&app)];
        unsafe {
            sink_combine_states(&combines, dst_len.as_mut_ptr(), src_len.as_ptr(), Some(&mut sa))
                .unwrap()
        };
        assert_eq!(text_payload(dst_len[0].trans_value), b"app", "min: shorter");
        assert!(sa.owns(dst_len[0].trans_value.as_usize()));
        assert_eq!(dst_len[1].trans_value.as_usize(), keep_len, "max: longer, untouched");

        // Byte-equal tie: C returns arg1 only on a STRICT win → the src value
        // is copied in (unobservable — the images are byte-identical).
        let mut dst_tie = [owned(&mut sa, &apple), owned(&mut sa, &apple)];
        let src_tie = [pg_of(&apple2), pg_of(&apple2)];
        unsafe {
            sink_combine_states(&combines, dst_tie.as_mut_ptr(), src_tie.as_ptr(), Some(&mut sa))
                .unwrap()
        };
        assert_eq!(text_payload(dst_tie[0].trans_value), b"apple");
        assert_ne!(dst_tie[0].trans_value.as_usize(), &*apple2 as *const _ as usize);

        // Strict NULL handling: a no-value dst COPIES the src (C's
        // noTransValue datumCopy); NULL src is a skip.
        let mut dst_null = [
            AggPerGroup {
                trans_value: Datum::null(),
                trans_value_is_null: true,
                no_trans_value: true,
            },
            owned(&mut sa, &apple),
        ];
        let keep_null = dst_null[1].trans_value.as_usize();
        let src_null = [
            pg_of(&pear),
            AggPerGroup {
                trans_value: Datum::null(),
                trans_value_is_null: true,
                no_trans_value: true,
            },
        ];
        unsafe {
            sink_combine_states(&combines, dst_null.as_mut_ptr(), src_null.as_ptr(), Some(&mut sa))
                .unwrap()
        };
        assert_eq!(text_payload(dst_null[0].trans_value), b"pear");
        assert_ne!(dst_null[0].trans_value.as_usize(), &*pear as *const _ as usize);
        assert!(sa.owns(dst_null[0].trans_value.as_usize()));
        assert!(!dst_null[0].trans_value_is_null);
        assert_eq!(dst_null[1].trans_value.as_usize(), keep_null, "null src is a skip");

        // Fail-closed: a text combine with no destination store never adopts.
        let mut dst_nostore = [owned(&mut sa, &apple), owned(&mut sa, &apple)];
        assert!(
            unsafe {
                sink_combine_states(&combines, dst_nostore.as_mut_ptr(), src.as_ptr(), None)
            }
            .is_err(),
            "no store ⇒ error, never a borrowed pointer"
        );

        // Emit: the survivor image lands in the buf's OWN arena, verbatim.
        let mut t = mk_table(4);
        let pr = t.probe_int(7, t.hash_key_int(7));
        unsafe {
            core::ptr::copy_nonoverlapping(
                [pg_of(&apple), pg_of(&pear)].as_ptr(),
                pr.states.cast::<AggPerGroup>(),
                2,
            );
        }
        let buf = sink_emit_bucket(&plan, &t).unwrap();
        assert_eq!(buf.nrows, 1);
        assert_eq!(buf.values[0].as_i64(), 7);
        let expect = &apple.buf[..4 + 5];
        let p = buf.values[1].as_usize();
        let lo = buf.arena.as_ptr() as usize;
        assert!(p >= lo && p + expect.len() <= lo + buf.arena.len(), "datum points into arena");
        let got = unsafe { core::slice::from_raw_parts(p as *const u8, expect.len()) };
        assert_eq!(got, expect, "survivor image copied verbatim");
        assert!(!buf.nulls[1]);

        // All-NULL group emits SQL NULL.
        let mut t2 = mk_table(4);
        let pr2 = t2.probe_int(9, t2.hash_key_int(9));
        unsafe {
            core::ptr::copy_nonoverlapping(
                [
                    AggPerGroup {
                        trans_value: Datum::null(),
                        trans_value_is_null: true,
                        no_trans_value: true,
                    },
                    pg_of(&pear),
                ]
                .as_ptr(),
                pr2.states.cast::<AggPerGroup>(),
                2,
            );
        }
        let buf2 = sink_emit_bucket(&plan, &t2).unwrap();
        assert!(buf2.nulls[1], "all-NULL-input group finalizes to NULL");
    }

    /// Overwrite a source image in place with a same-shaped plain varlena
    /// (as `mk_text`) — a destination still pointing at it reads THIS value.
    fn overwrite_text(img: &mut TextImage, payload: &[u8]) {
        assert!(payload.len() <= 28);
        img.buf = [0; 32];
        let size = (4 + payload.len()) as u32;
        img.buf[0..4].copy_from_slice(&(size << 2).to_le_bytes());
        img.buf[4..4 + payload.len()].copy_from_slice(payload);
    }

    /// Seed key `k`'s pergroup pair (min, max) with one text image.
    fn put_text(t: &mut LaneAggTable, imgs: &mut Vec<Box<TextImage>>, k: i64, payload: &[u8]) {
        let img = mk_text(payload);
        let d = Datum::from_usize(&*img as *const TextImage as usize);
        imgs.push(img);
        let pr = t.probe_int(k, t.hash_key_int(k as u64));
        let pg = AggPerGroup { trans_value: d, trans_value_is_null: false, no_trans_value: false };
        // SAFETY: a fresh row's block is 2 pergroups (STATE_BYTES).
        unsafe {
            let p = pr.states.cast::<AggPerGroup>();
            *p = pg;
            *p.add(1) = pg;
        }
    }

    /// GL-SINKCRASH-1 (release blocker): a combined bucket OWNS its
    /// min/max(text) transvalues. Both legs emit only after every source
    /// image has been overwritten, so a destination that kept a source
    /// pointer emits the overwritten bytes instead of the survivor — the
    /// combine that adopted pointers fails here. (The sources are overwritten
    /// rather than freed so the failure is a value mismatch, not whatever the
    /// allocator left behind.)
    #[test]
    fn varlena_minmax_transvalues_survive_source_teardown() {
        let vmm = |larger: bool| SinkCombineFn {
            func: test_combines()[0].func, // never called by this kind
            strict: true,
            collation: Oid::from(0u8),
            kind: SinkCombineKind::VarlenaMinMax { larger },
        };
        let combines = vec![vmm(false), vmm(true)];
        let plan = SinkEmitPlan {
            width: 8,
            fixed: None,
            ntails: 0,
            filter: None,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::VarlenaTrans { transno: 0 },
                SinkEmitCol::VarlenaTrans { transno: 1 },
            ],
        };

        // Face A holds "a<k>", face B "b<k>". Keys 0..30 are in both (min
        // from A, max from B — the pairwise combine), 30..40 in A only and
        // 40..50 in B only (SINGLE-FACE groups: they only ever pass through
        // the new-row block copy).
        let text = |face: u8, k: i64| {
            let mut v = vec![face];
            v.extend_from_slice(format!("{k:04}").as_bytes());
            v
        };
        let mut expect: std::collections::HashMap<i64, (Vec<u8>, Vec<u8>)> =
            std::collections::HashMap::new();
        for k in 0..30i64 {
            expect.insert(k, (text(b'a', k), text(b'b', k)));
        }
        for k in 30..40i64 {
            expect.insert(k, (text(b'a', k), text(b'a', k)));
        }
        for k in 40..50i64 {
            expect.insert(k, (text(b'b', k), text(b'b', k)));
        }

        let build = || {
            let mut imgs: Vec<Box<TextImage>> = Vec::new();
            let mut ta = mk_table(64);
            for k in 0..40i64 {
                put_text(&mut ta, &mut imgs, k, &text(b'a', k));
            }
            let mut tb = mk_table(64);
            for k in 0..30i64 {
                put_text(&mut tb, &mut imgs, k, &text(b'b', k));
            }
            for k in 40..50i64 {
                put_text(&mut tb, &mut imgs, k, &text(b'b', k));
            }
            (ta, tb, imgs)
        };
        let poison = |imgs: &mut Vec<Box<TextImage>>| {
            for img in imgs.iter_mut() {
                overwrite_text(img, b"XXXXX");
            }
        };
        let check = |buf: &SinkEmitBuf, seen: &mut std::collections::HashMap<i64, ()>| {
            for row in 0..buf.nrows {
                let k = buf.values[row * 3].as_i64();
                let (lo, hi) = &expect[&k];
                assert_eq!(&emit_text(buf, buf.values[row * 3 + 1]), lo, "min of key {k}");
                assert_eq!(&emit_text(buf, buf.values[row * 3 + 2]), hi, "max of key {k}");
                assert!(seen.insert(k, ()).is_none(), "key {k} emitted twice");
            }
        };

        // Leg 1 — the flat combine: two remainder faces, one bucket table.
        {
            let (ta, tb, mut imgs) = build();
            let (pa, pb) = (sink_partition_remainder(&ta), sink_partition_remainder(&tb));
            let locals = [
                SinkLocalView {
                    spilled: &[],
                    runs: &[],
                    remainder: Some(SinkRemainder {
                        table: &ta,
                        part: &pa,
                        canon: None,
                        canon_store: None,
                        gid_gen: 0,
                        direct: false,
                    }),
                },
                SinkLocalView {
                    spilled: &[],
                    runs: &[],
                    remainder: Some(SinkRemainder {
                        table: &tb,
                        part: &pb,
                        canon: None,
                        canon_store: None,
                        gid_gen: 0,
                        direct: false,
                    }),
                },
            ];
            let merged: Vec<CombinedBucket> = (0..SINK_NBUCKETS)
                .map(|b| sink_combine_bucket(b, 1, STATE_BYTES, &locals, &combines).unwrap())
                .collect();
            poison(&mut imgs);
            let mut seen = std::collections::HashMap::new();
            for m in &merged {
                check(&sink_emit_bucket(&plan, m).unwrap(), &mut seen);
            }
            assert_eq!(seen.len(), expect.len(), "every group emitted");
        }

        // Leg 2 — the two-level pass: each half becomes a partial RUN whose
        // state blocks are its combined table's blocks VERBATIM, so the
        // half's store has to travel with the run and the final stage has to
        // copy out of it before that store is released.
        {
            let (ta, tb, mut imgs) = build();
            let (pa, pb) = (sink_partition_remainder(&ta), sink_partition_remainder(&tb));
            let half = |t: &LaneAggTable, p: &SinkPart, b: usize| {
                let locals = [SinkLocalView {
                    spilled: &[],
                    runs: &[],
                    remainder: Some(SinkRemainder {
                        table: t,
                        part: p,
                        canon: None,
                        canon_store: None,
                        gid_gen: 0,
                        direct: false,
                    }),
                }];
                let m = sink_combine_bucket(b, 1, STATE_BYTES, &locals, &combines).unwrap();
                let run = sink_run_from_bucket_table(b, &m);
                (run, m.into_str_store())
            };
            let partials: Vec<((SinkRun, Option<StrStateArena>), (SinkRun, Option<StrStateArena>))> =
                (0..SINK_NBUCKETS).map(|b| (half(&ta, &pa, b), half(&tb, &pb, b))).collect();
            poison(&mut imgs);
            let mut seen = std::collections::HashMap::new();
            for (b, ((r0, s0), (r1, s1))) in partials.into_iter().enumerate() {
                let views = [
                    SinkLocalView {
                        spilled: &[],
                        runs: core::slice::from_ref(&r0),
                        remainder: None,
                    },
                    SinkLocalView {
                        spilled: &[],
                        runs: core::slice::from_ref(&r1),
                        remainder: None,
                    },
                ];
                let fin = sink_combine_bucket(b, 1, STATE_BYTES, &views, &combines).unwrap();
                drop((r0, r1, s0, s1));
                check(&sink_emit_bucket(&plan, &fin).unwrap(), &mut seen);
            }
            assert_eq!(seen.len(), expect.len(), "every group emitted");
        }
    }

    /// SE-T2AGG CAR B knob: default OFF in a kill-free process (the probe /
    /// resolve-combines vocabulary stays int-only at default — inert pin).
    #[test]
    fn sink_strminmax_knob_default_on() {
        assert!(
            sink_strminmax_enabled(),
            "test process has no knob set => ON (GL-STRMM-2 flipped-kill default)"
        );
    }

    // avgpack: seed a packed [count, sum] image into a pergroup slot.
    fn mk_packed(count: i64, sum: i64) -> AggPerGroup {
        let mut pg = AggPerGroup {
            trans_value: Datum::null(),
            trans_value_is_null: false,
            no_trans_value: false,
        };
        // SAFETY: the slot is 16 repr(C) bytes, 8-aligned.
        unsafe { (&mut pg as *mut AggPerGroup).cast::<[i64; 2]>().write([count, sum]) };
        pg
    }

    fn read_packed(pg: &AggPerGroup) -> [i64; 2] {
        // SAFETY: as mk_packed.
        unsafe { (pg as *const AggPerGroup).cast::<[i64; 2]>().read() }
    }

    #[test]
    fn avgpack_combine_is_self_contained_element_adds() {
        // Transno 0: byval count; transno 1: PACKED AvgInt8 (avgpack).
        let combines = vec![
            SinkCombineFn {
                func: test_combines()[0].func,
                strict: false,
                collation: Oid::from(0u8),
                kind: SinkCombineKind::Byval,
            },
            SinkCombineFn {
                func: test_combines()[0].func,
                strict: true,
                collation: Oid::from(0u8),
                kind: SinkCombineKind::AvgInt8Packed,
            },
        ];
        // The byref-floor kill: packed shapes take NO byref accounting.
        assert!(!sink_combines_byref(&combines));

        let mut dst = [
            AggPerGroup {
                trans_value: Datum::from_i64(4),
                trans_value_is_null: false,
                no_trans_value: false,
            },
            mk_packed(4, 100),
        ];
        let src = [
            AggPerGroup {
                trans_value: Datum::from_i64(6),
                trans_value_is_null: false,
                no_trans_value: false,
            },
            mk_packed(6, 44),
        ];
        unsafe { sink_combine_states(&combines, dst.as_mut_ptr(), src.as_ptr(), None).unwrap() };
        assert_eq!(dst[0].trans_value.as_i64(), 10);
        assert_eq!(read_packed(&dst[1]), [10, 144]);

        // The all-NULL-input group ({0,0}) combines as C's own zero adds.
        let mut dz = [
            AggPerGroup {
                trans_value: Datum::from_i64(0),
                trans_value_is_null: false,
                no_trans_value: false,
            },
            mk_packed(0, 0),
        ];
        let sz = [
            AggPerGroup {
                trans_value: Datum::from_i64(0),
                trans_value_is_null: false,
                no_trans_value: false,
            },
            mk_packed(0, 0),
        ];
        unsafe { sink_combine_states(&combines, dz.as_mut_ptr(), sz.as_ptr(), None).unwrap() };
        assert_eq!(read_packed(&dz[1]), [0, 0]);
    }

    #[test]
    fn avgpack_flush_spill_replay_combine_emit_matches_unpacked() {
        // Full packed pipeline: worker table -> flush -> spill record ->
        // replay -> combine -> finalize-at-emit; the emitted NUMERIC image
        // must byte-equal the UNPACKED (transarray) arm's over the same
        // integers, and a count == 0 group must finalize to NULL.
        let combines = vec![
            SinkCombineFn {
                func: test_combines()[0].func,
                strict: false,
                collation: Oid::from(0u8),
                kind: SinkCombineKind::Byval,
            },
            SinkCombineFn {
                func: test_combines()[0].func,
                strict: true,
                collation: Oid::from(0u8),
                kind: SinkCombineKind::AvgInt8Packed,
            },
        ];
        // Two worker tables, overlapping keys; key 9 sees only NULL inputs
        // everywhere (packed {0,0} on both sides).
        let seed = |rows: &[(i64, i64, i64)]| -> LaneAggTable {
            let mut t = mk_table(4);
            for &(k, c, s) in rows {
                let pr = t.probe_int(k, t.hash_key_int(k as u64));
                let states = [
                    AggPerGroup {
                        trans_value: Datum::from_i64(c),
                        trans_value_is_null: false,
                        no_trans_value: false,
                    },
                    mk_packed(c, s),
                ];
                // SAFETY: fresh row's state block holds 2 slots.
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        states.as_ptr(),
                        pr.states.cast::<AggPerGroup>(),
                        2,
                    );
                }
            }
            t
        };
        let mut ta = seed(&[(7, 4, 100), (9, 0, 0)]);
        let mut tb = seed(&[(7, 6, 44), (9, 0, 0)]);
        let state_words = STATE_BYTES / 8;
        // Worker A's epoch goes through the SPILL RECORD (verbatim words);
        // worker B stays an in-memory flushed run.
        let run_a = sink_flush_table(&mut ta);
        let run_b = sink_flush_table(&mut tb);
        let mut spilled: Vec<SinkRun> = Vec::new();
        for b in 0..SINK_NBUCKETS {
            let mut bytes = Vec::new();
            sink_run_spill_bucket(&run_a, b, &mut bytes);
            if !bytes.is_empty() {
                spilled.push(sink_run_from_spill(b, 1, state_words, &bytes).unwrap());
            }
        }
        let locals = [
            SinkLocalView { spilled: &spilled, runs: &[], remainder: None },
            SinkLocalView { spilled: &[], runs: core::slice::from_ref(&run_b), remainder: None },
        ];
        let plan = SinkEmitPlan {
            width: 8,
            fixed: None,
            ntails: 0,
            filter: None,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::Agg { transno: 0 },
                SinkEmitCol::AvgInt8 { transno: 1, packed: true },
            ],
        };
        let mut rows: Vec<(i64, i64, Option<Vec<u8>>)> = Vec::new();
        for b in 0..SINK_NBUCKETS {
            let t = sink_combine_bucket(b, 1, STATE_BYTES, &locals, &combines).unwrap();
            let buf = sink_emit_bucket(&plan, &t).unwrap();
            for r in 0..buf.nrows {
                let key = buf.values[r * 3].as_i64();
                let count = buf.values[r * 3 + 1].as_i64();
                let avg = if buf.nulls[r * 3 + 2] {
                    None
                } else {
                    let p = buf.values[r * 3 + 2].as_usize();
                    let lo = buf.arena.as_ptr() as usize;
                    // Compare through the expected image's length below;
                    // capture generously (the arena is self-contained).
                    let len = buf.arena.len() - (p - lo);
                    Some(
                        unsafe { core::slice::from_raw_parts(p as *const u8, len) }.to_vec(),
                    )
                };
                rows.push((key, count, avg));
            }
        }
        rows.sort_by_key(|r| r.0);
        assert_eq!(rows.len(), 2);
        // Key 7: count 10, avg = int64_avg_div(144, 10) — the UNPACKED
        // finalfn core's exact image over the same integers.
        assert_eq!((rows[0].0, rows[0].1), (7, 10));
        let expect = ::adt_numeric::ops::int64_avg_div(144, 10).unwrap();
        let got = rows[0].2.as_ref().expect("non-NULL avg");
        assert_eq!(&got[..expect.as_bytes().len()], expect.as_bytes());
        // Key 9 (all-NULL inputs, count 0): NULL — C int8_avg's exact gate.
        assert_eq!((rows[1].0, rows[1].1), (9, 0));
        assert!(rows[1].2.is_none());
    }

    #[test]
    fn emit_bucket_multi_components() {
        // One-word packed image: int4 at off 0, int2 at off 4 (multi-int class).
        let mut t = mk_table(8);
        let img: u64 = ((-7i32 as u32) as u64) | ((300u16 as u64) << 32);
        bump(&mut t, Some(img as i64), 5, 9);
        let plan = SinkEmitPlan {
            width: 8,
            fixed: None,
            ntails: 0,
            filter: None,
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

        // Two-word packed image: int8 at off 0, int4 at off 8 (multi-int class —
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
            ntails: 0,
            filter: None,
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

    /// dop1-tax fix 3 oracle: the single-Local no-runs pass-through emit is
    /// byte-identical (values, nulls, row order) to the merge arm's emit of
    /// the same bucket, for every bucket including the NULL group's.
    #[test]
    fn passthrough_emit_matches_merge_arm() {
        let mut t = mk_table(64);
        for k in 0..2000 {
            bump(&mut t, Some(k), 1, 3 * k + 1);
        }
        bump(&mut t, None, 4, 11);
        let part = sink_partition_remainder(&t);
        assert!(part.has_null);
        let plan = SinkEmitPlan {
            fixed: None,
            ntails: 0,
            filter: None,
            width: 8,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::Agg { transno: 0 },
                SinkEmitCol::Agg { transno: 1 },
            ],
        };
        let locals =
            [SinkLocalView { spilled: &[], runs: &[], remainder: Some(SinkRemainder { table: &t, part: &part, canon: None, canon_store: None, gid_gen: 0, direct: false }) }];
        let combines = test_combines();
        let mut total_rows = 0usize;
        for b in 0..SINK_NBUCKETS {
            let merged =
                sink_combine_bucket(b, 1, STATE_BYTES, &locals, &combines).unwrap();
            let want = sink_emit_bucket(&plan, &merged).unwrap();
            let got = sink_emit_bucket_passthrough(&plan, &t, &part, b).unwrap();
            assert_eq!(got.nrows, want.nrows, "bucket {b} row count");
            assert_eq!(got.nulls, want.nulls, "bucket {b} null bitmap");
            let eq = got
                .values
                .iter()
                .zip(want.values.iter())
                .zip(got.nulls.iter())
                .all(|((g, w), &null)| null || g.as_i64() == w.as_i64());
            assert!(eq, "bucket {b} datums diverge");
            total_rows += got.nrows;
        }
        assert_eq!(total_rows, 2001);
    }

    /// TRUE TABLE ADOPT oracle (dop1-tax2 inc-1b): the LINEAR table-backed
    /// drain (`table_emit_datum` over rows 0..n) reproduces
    /// `sink_emit_bucket`'s whole-table emit byte-for-byte — the exact
    /// forming the merge arm applies (values, nulls; order = insertion
    /// order with the NULL group row at its insertion position). Content
    /// parity with the merge/pass-through arms is closed by
    /// `passthrough_emit_matches_merge_arm` (same emit_row core).
    #[test]
    fn table_linear_drain_matches_whole_table_emit() {
        let mut t = mk_table(64);
        for k in 0..2000 {
            bump(&mut t, Some(k), 1, 3 * k + 1);
        }
        bump(&mut t, None, 4, 11);
        let plan = SinkEmitPlan {
            fixed: None,
            ntails: 0,
            filter: None,
            width: 8,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::Agg { transno: 0 },
                SinkEmitCol::Agg { transno: 1 },
            ],
        };
        assert!(sink_emit_plan_all_byval(&plan));
        let natts = plan.cols.len();
        let want = sink_emit_bucket(&plan, &t).unwrap();
        assert_eq!(want.nrows, 2001);
        for row in 0..want.nrows {
            for c in 0..natts {
                let (v, isnull) = table_emit_datum(&plan, &t, row, c);
                assert_eq!(isnull, want.nulls[row * natts + c], "row {row} col {c} null");
                if !isnull {
                    assert_eq!(
                        v.as_i64(),
                        want.values[row * natts + c].as_i64(),
                        "row {row} col {c} datum"
                    );
                }
            }
        }
    }

    // -- Canonical (text-bearing) shapes — the C2 car ------------------------

    /// int8 + text shape: Int{8} at off 0, Intern at off 8 (12-byte image,
    /// two words) — the two-key `UserID, SearchPhrase` class.
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

    /// The 1-comp single-text shape (bump_canon's `k = None` twin — the
    /// single-text-class image is the intern id word alone).
    fn canon_shape_text_only() -> MkShape {
        MkShape {
            comps: vec![crate::compact::MkComp {
                att: 0,
                off: 0,
                kind: MkCompKind::Intern,
            }],
            packed_bytes: 4,
            nullable: false,
            two_words: false,
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
        // NEW rows need no seed writes: the zeroed slot already reads as
        // {trans_value: 0, non-null} — and FIELD-free seeding keeps the
        // struct padding deterministically zero, so the direct-arm
        // equivalence tests may compare raw state words (a whole-struct
        // `write` copies stack padding garbage into the row).
        unsafe {
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

    /// arena-strings inc-1: the STOLEN key-store law must be observationally
    /// identical to the contiguous copy law — same slots, same per-slot key
    /// bytes/hash/states, and a byte-identical spill stream (the record is
    /// self-describing). Mixed short (≤8 B packed) and long (arena) texts,
    /// duplicates, both the 2-comp and the 1-comp single-text shapes; the
    /// store restarts correctly across consecutive steal flushes.
    #[test]
    fn canon_flush_steal_matches_copy_law() {
        let corpus: [(&[u8], i64); 7] = [
            (b"apple", 1),
            (b"a-rather-long-canonical-key-way-past-eight", 2),
            (b"apple", 3),
            (b"", 4),
            (b"banana", 5),
            (b"zz", 6),
            (b"a-rather-long-canonical-key-way-past-eight", 7),
        ];
        for single in [false, true] {
            let shape = if single { canon_shape_text_only() } else { canon_shape_int8_text() };
            let build = |steal: bool| -> (SinkRun, SinkRun) {
                let mut w = canon_worker(shape.clone());
                for (i, (text, c)) in corpus.iter().enumerate() {
                    let k = (!single).then_some((i % 3) as i64);
                    bump_canon(&mut w, k, text, *c);
                }
                let first = sink_flush_table_canon_impl(&mut w, false, steal);
                // Epoch 2 re-feeds a suffix — the store must have restarted.
                for (text, c) in corpus.iter().skip(3) {
                    let k = (!single).then_some(9i64);
                    bump_canon(&mut w, k, text, *c);
                }
                (first, sink_flush_table_canon_impl(&mut w, false, steal))
            };
            let (copy1, copy2) = build(false);
            let (steal1, steal2) = build(true);
            for (a, b) in [(&copy1, &steal1), (&copy2, &steal2)] {
                assert_eq!(a.key_words, 0);
                assert!(a.key_ends.is_empty(), "copy law carries no ends");
                assert!(!b.key_ends.is_empty(), "steal law carries per-slot ends");
                assert_eq!(a.starts, b.starts);
                assert_eq!(a.hashes, b.hashes);
                assert_eq!(a.states, b.states);
                assert_eq!(a.nrows(), b.nrows());
                for i in 0..a.nrows() {
                    assert_eq!(a.key_slice(i), b.key_slice(i), "slot {i} key bytes diverge");
                }
                // The spill stream is byte-identical from either law.
                for bkt in 0..SINK_NBUCKETS {
                    let (mut sa, mut sb) = (Vec::new(), Vec::new());
                    sink_run_spill_bucket(a, bkt, &mut sa);
                    sink_run_spill_bucket(b, bkt, &mut sb);
                    assert_eq!(sa, sb, "spill records diverge in bucket {bkt}");
                }
                // The combine's arena hint law holds on both representations.
                let total: usize = (0..SINK_NBUCKETS).map(|bkt| a.bucket_key_bytes(bkt)).sum();
                let total_b: usize = (0..SINK_NBUCKETS).map(|bkt| b.bucket_key_bytes(bkt)).sum();
                assert_eq!(total, total_b);
                assert_eq!(total, b.key_bytes.len(), "stolen store holds exactly the images");
            }
        }
    }

    // -- arena-strings inc-3: the DIRECT single-text arm ----------------------

    /// A DIRECT-armed worker state (arena-strings inc-3): `KeyRepr::Bytes`
    /// table keyed on the canonical image itself, no intern table — exactly
    /// what `try_arm_mk_n`'s direct branch installs.
    fn direct_worker() -> crate::compact::CompactHash {
        let table = LaneAggTable::with_config(
            KeyRepr::Bytes,
            STATE_BYTES,
            16,
            HashKind::best(),
            EntryLayout::Salt8,
        );
        let mut ch = crate::compact::compact_hash_for_tests(
            table,
            crate::compact::CompactKeySpec::Multi(canon_shape_text_only()),
            None,
        );
        ch.text_direct = true;
        ch
    }

    /// The mk1 1-Intern canonical image of `text`: `packed_bytes` (4) zeroed
    /// id bytes + the raw text verbatim (`canon_row_bytes`' law).
    fn direct_image(text: &[u8]) -> Vec<u8> {
        let mut img = Vec::with_capacity(4 + text.len());
        img.extend_from_slice(&[0u8; 4]);
        img.extend_from_slice(text);
        img
    }

    /// The feed's DIRECT probe for one row — `scan_mk1_text_direct_batch`'s
    /// probe in miniature: canonical image, [`sink_hash_bytes`] as the probe
    /// hash (the probe-hash law), same toy transitions as `bump_canon`.
    fn bump_direct(ch: &mut crate::compact::CompactHash, text: &[u8], count: i64) {
        assert!(ch.text_direct);
        let img = direct_image(text);
        let h = sink_hash_bytes(&img);
        let pr = ch.table.probe_bytes(&img, h);
        let pg = pr.states.cast::<AggPerGroup>();
        // SAFETY: STATE_BYTES holds two AggPerGroup slots, zeroed at birth
        // (which already reads as the {0, non-null} count seed — see
        // bump_canon's padding-determinism note).
        unsafe {
            let c = &mut *pg;
            c.trans_value = Datum::from_i64(c.trans_value.as_i64() + count);
        }
    }

    /// Dup/short/long/empty single-text corpus (feed order fixed — the
    /// equivalence assertions below are slot-for-slot).
    const DIRECT_CORPUS: [(&[u8], i64); 8] = [
        (b"apple", 1),
        (b"a-rather-long-canonical-key-way-past-eight-bytes", 2),
        (b"apple", 3),
        (b"", 4),
        (b"banana", 5),
        (b"zz", 6),
        (b"a-rather-long-canonical-key-way-past-eight-bytes", 7),
        (b"12345678", 8),
    ];

    /// (a) Direct-vs-intern equivalence at the FLUSH face: identical feeds
    /// through the DIRECT arm and the intern arm produce slot-for-slot
    /// identical bytes-mode runs (starts, key bytes, hashes, states) and a
    /// byte-identical spill stream — the canonical-bytes law that makes
    /// cross-worker merges of the two arms correct by construction.
    #[test]
    fn direct_flush_matches_intern_arm() {
        let mut w = canon_worker(canon_shape_text_only());
        let mut d = direct_worker();
        for (text, c) in DIRECT_CORPUS {
            bump_canon(&mut w, None, text, c);
            bump_direct(&mut d, text, c);
        }
        let run_i = sink_flush_table_canon_impl(&mut w, false, false);
        let run_d = sink_flush_table_direct(&mut d);
        assert_eq!(d.table.nrows(), 0, "direct flush resets the table");
        for (a, b) in [(&run_i, &run_d)] {
            assert_eq!(a.key_words, 0);
            assert_eq!(b.key_words, 0);
            assert!(b.key_ends.is_empty(), "direct flush is the contiguous copy law");
            assert!(b.keys.is_empty() && b.gid_gen == 0, "no GID words on direct runs");
            assert_eq!(a.starts, b.starts);
            assert_eq!(a.hashes, b.hashes);
            assert_eq!(a.states, b.states);
            assert_eq!(a.nrows(), b.nrows());
            for i in 0..a.nrows() {
                assert_eq!(a.key_slice(i), b.key_slice(i), "slot {i} key bytes diverge");
            }
            for bkt in 0..SINK_NBUCKETS {
                let (mut sa, mut sb) = (Vec::new(), Vec::new());
                sink_run_spill_bucket(a, bkt, &mut sa);
                sink_run_spill_bucket(b, bkt, &mut sb);
                assert_eq!(sa, sb, "spill records diverge in bucket {bkt}");
            }
        }
        // Epoch 2 (post-flush): the direct table restarted its vocabulary —
        // both arms must again produce identical runs.
        for (text, c) in DIRECT_CORPUS.iter().skip(3) {
            bump_canon(&mut w, None, text, *c);
            bump_direct(&mut d, text, *c);
        }
        let run_i2 = sink_flush_table_canon_impl(&mut w, false, false);
        let run_d2 = sink_flush_table_direct(&mut d);
        assert_eq!(run_i2.starts, run_d2.starts);
        assert_eq!(run_i2.hashes, run_d2.hashes);
        assert_eq!(run_i2.states, run_d2.states);
        for i in 0..run_i2.nrows() {
            assert_eq!(run_i2.key_slice(i), run_d2.key_slice(i));
        }
    }

    /// (d) The direct flush hash law: every run slot's carried hash is
    /// [`sink_hash_bytes`] over its key slice, and slots sit in their hash's
    /// bucket (the combine reuses the hash verbatim — no rehash anywhere).
    #[test]
    fn direct_flush_hash_law() {
        let mut d = direct_worker();
        for (text, c) in DIRECT_CORPUS {
            bump_direct(&mut d, text, c);
        }
        let run = sink_flush_table_direct(&mut d);
        assert_eq!(run.nrows(), 6);
        for b in 0..SINK_NBUCKETS {
            for i in run.starts[b] as usize..run.starts[b + 1] as usize {
                assert_eq!(run.hashes[i], sink_hash_bytes(run.key_slice(i)), "slot {i}");
                assert_eq!(bucket_of(run.hashes[i]), b, "slot {i} bucket-major framing");
            }
        }
    }

    /// (b) Remainder face: unflushed direct tables partition by the SAVED
    /// sink hashes and the combine reads key bytes straight off the rows —
    /// merged results equal the intern-armed twin's, row for row, across a
    /// run + remainder mix, both GID-map arms, and the remainder spill
    /// serialization.
    #[test]
    fn direct_remainder_combine_matches_intern_arm() {
        let build_intern = || {
            let mut w1 = canon_worker(canon_shape_text_only());
            for (text, c) in DIRECT_CORPUS {
                bump_canon(&mut w1, None, text, c);
            }
            let run1 = sink_flush_table_canon(&mut w1);
            // Remainder epoch: reused + new texts, NOT flushed.
            bump_canon(&mut w1, None, b"apple", 10);
            bump_canon(&mut w1, None, b"remainder-only-key-way-past-eight-bytes", 11);
            bump_canon(&mut w1, None, b"", 12);
            let mut w2 = canon_worker(canon_shape_text_only());
            bump_canon(&mut w2, None, b"banana", 20);
            bump_canon(&mut w2, None, b"w2-only", 21);
            (run1, SinkTableHandle(w1), SinkTableHandle(w2))
        };
        let build_direct = || {
            let mut w1 = direct_worker();
            for (text, c) in DIRECT_CORPUS {
                bump_direct(&mut w1, text, c);
            }
            let run1 = sink_flush_table_direct(&mut w1);
            bump_direct(&mut w1, b"apple", 10);
            bump_direct(&mut w1, b"remainder-only-key-way-past-eight-bytes", 11);
            bump_direct(&mut w1, b"", 12);
            let mut w2 = direct_worker();
            bump_direct(&mut w2, b"banana", 20);
            bump_direct(&mut w2, b"w2-only", 21);
            (run1, SinkTableHandle(w1), SinkTableHandle(w2))
        };
        let (irun, mut ih1, mut ih2) = build_intern();
        let (drun, mut dh1, mut dh2) = build_direct();
        let (ipart1, ipart2) = (ih1.partition_remainder(), ih2.partition_remainder());
        let (dpart1, dpart2) = (dh1.partition_remainder(), dh2.partition_remainder());
        assert_eq!(ipart1.starts, dpart1.starts, "SEAL partition geometry diverges");
        assert_eq!(ipart1.hashes, dpart1.hashes, "SEAL-carried hashes diverge");
        assert!(!dpart1.has_null && !dpart2.has_null);
        let combines = test_combines();
        for gid in [false, true] {
            let ilocals = [
                SinkLocalView {
                    spilled: &[],
                    runs: core::slice::from_ref(&irun),
                    remainder: Some(ih1.remainder_view(&ipart1)),
                },
                SinkLocalView {
                    spilled: &[],
                    runs: &[],
                    remainder: Some(ih2.remainder_view(&ipart2)),
                },
            ];
            let dlocals = [
                SinkLocalView {
                    spilled: &[],
                    runs: core::slice::from_ref(&drun),
                    remainder: Some(dh1.remainder_view(&dpart1)),
                },
                SinkLocalView {
                    spilled: &[],
                    runs: &[],
                    remainder: Some(dh2.remainder_view(&dpart2)),
                },
            ];
            assert!(dlocals[0].remainder.as_ref().unwrap().direct);
            assert!(dlocals[0].remainder.as_ref().unwrap().canon.is_none());
            for b in 0..SINK_NBUCKETS {
                let mi =
                    sink_combine_bucket_impl(b, 0, STATE_BYTES, &ilocals, &combines, gid, true)
                        .unwrap();
                let md =
                    sink_combine_bucket_impl(b, 0, STATE_BYTES, &dlocals, &combines, gid, true)
                        .unwrap();
                assert_merged_identical(&mi, &md, 0);
            }
        }
        // The remainder spill serialization is byte-identical across arms
        // (the C2 canonical record — self-describing per-row key_len).
        for b in 0..SINK_NBUCKETS {
            let (mut si, mut sd) = (Vec::new(), Vec::new());
            sink_remainder_spill_bucket_canon(&ih1.remainder_view(&ipart1), b, &mut si)
                .unwrap();
            sink_remainder_spill_bucket_canon(&dh1.remainder_view(&dpart1), b, &mut sd)
                .unwrap();
            assert_eq!(si, sd, "remainder spill records diverge in bucket {b}");
            assert_eq!(
                sink_remainder_canon_content(&ih1.remainder_view(&ipart1), b),
                sink_remainder_canon_content(&dh1.remainder_view(&dpart1), b),
                "remainder content estimate diverges in bucket {b}"
            );
        }
    }

    /// (c) Flush-reset law (the 830320fed cache-invalidation contract at
    /// the table's own level): the direct flush RESETS the table — its
    /// vocabulary restarts — so a post-flush re-feed of the same texts must
    /// re-probe into FRESH rows, and both epochs' runs carry independent,
    /// correct counts (a dangling code→state cache would have folded epoch
    /// 2's rows into freed epoch-1 rows instead).
    #[test]
    fn direct_flush_resets_vocabulary() {
        let mut d = direct_worker();
        bump_direct(&mut d, b"alpha", 1);
        bump_direct(&mut d, b"beta-longer-than-eight-bytes", 2);
        bump_direct(&mut d, b"alpha", 3);
        assert_eq!(d.table.nrows(), 2);
        let run1 = sink_flush_table_direct(&mut d);
        assert_eq!(d.table.nrows(), 0, "the flush resets the table (vocabulary)");
        // Re-feed the SAME texts: fresh probes must create fresh rows.
        bump_direct(&mut d, b"alpha", 10);
        bump_direct(&mut d, b"beta-longer-than-eight-bytes", 20);
        assert_eq!(d.table.nrows(), 2, "post-flush arrivals re-probe, never dangle");
        let run2 = sink_flush_table_direct(&mut d);
        let count_of = |run: &SinkRun, text: &[u8]| -> Option<i64> {
            let img = direct_image(text);
            (0..run.nrows()).find_map(|i| {
                (run.key_slice(i) == img.as_slice())
                    .then(|| run.states[i * run.state_words] as i64)
            })
        };
        assert_eq!(count_of(&run1, b"alpha"), Some(4));
        assert_eq!(count_of(&run1, b"beta-longer-than-eight-bytes"), Some(2));
        assert_eq!(count_of(&run2, b"alpha"), Some(10));
        assert_eq!(count_of(&run2, b"beta-longer-than-eight-bytes"), Some(20));
    }

    /// The direct arm's freeze extraction reads the canonical images
    /// verbatim off the rows — identical to the intern arm's extraction.
    #[test]
    fn direct_freeze_extract_matches_intern_arm() {
        let mut w = canon_worker(canon_shape_text_only());
        let mut d = direct_worker();
        for (text, c) in DIRECT_CORPUS {
            bump_canon(&mut w, None, text, c);
            bump_direct(&mut d, text, c);
        }
        let ei = sink_freeze_extract_ch(&w, 3).expect("intern extractable");
        let ed = sink_freeze_extract_ch(&d, 3).expect("direct extractable");
        assert_eq!(ei, ed);
        assert!(sink_freeze_extract_ch(&d, 64).is_none(), "bound past nrows declines");
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
        let mut h1 = SinkTableHandle(w1);
        let part1 = h1.partition_remainder();
        assert!(!part1.has_null);

        let mut w2 = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w2, Some(9), b"zzz", 7);
        bump_canon(&mut w2, Some(1), b"banana", 20);
        bump_canon(&mut w2, Some(1), b"apple", 30);
        let mut h2 = SinkTableHandle(w2);
        let part2 = h2.partition_remainder();

        let locals = [
            SinkLocalView {
                spilled: &[],
                runs: core::slice::from_ref(&run1),
                remainder: Some(h1.remainder_view(&part1)),
            },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(h2.remainder_view(&part2)) },
        ];
        let combines = test_combines();
        let plan = SinkEmitPlan {
            width: 8,
            fixed: Some(12),
            ntails: 1,
            filter: None,
            cols: vec![
                SinkEmitCol::MultiComp { off: 0, width: 8 },
                SinkEmitCol::MultiText { nth: 0 },
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

    // -- combine16: flat presized merged tables ------------------------------

    /// Row-for-row identity (key, order, states) between two merged tables —
    /// the combine16 byte gate: entry-set layout/growth must never move a
    /// row or a state byte.
    fn assert_merged_identical(a: &LaneAggTable, b: &LaneAggTable, key_words: usize) {
        assert_eq!(a.nrows(), b.nrows());
        let state_words = a.state_bytes() / 8;
        assert_eq!(b.state_bytes() / 8, state_words);
        for row in 0..a.nrows() {
            match key_words {
                0 => {
                    let (mut sa, mut sb) = ([0u8; 8], [0u8; 8]);
                    assert_eq!(
                        a.row_key_bytes(row, &mut sa),
                        b.row_key_bytes(row, &mut sb),
                        "row {row} key"
                    );
                }
                2 => assert_eq!(a.row_key_i128(row), b.row_key_i128(row), "row {row} key"),
                _ => assert_eq!(a.row_key_int(row), b.row_key_int(row), "row {row} key"),
            }
            let (pa, pb) = (a.row_states(row).cast_const(), b.row_states(row).cast_const());
            // SAFETY: live rows; state blocks are state_words u64s.
            let (va, vb) = unsafe {
                (
                    core::slice::from_raw_parts(pa.cast::<u64>(), state_words),
                    core::slice::from_raw_parts(pb.cast::<u64>(), state_words),
                )
            };
            // AggPerGroup datums for the toy byval corpus are value words —
            // bit-comparable (byref corpora would need field-wise reads).
            assert_eq!(va, vb, "row {row} states");
        }
    }

    #[test]
    fn flat_combine_matches_incumbent() {
        // The roundtrip corpus (runs + remainders + NULL) through both
        // construction arms, all 256 buckets.
        let mut t1 = mk_table(64);
        for k in 0..1000 {
            bump(&mut t1, Some(k), 1, k);
        }
        bump(&mut t1, None, 1, 7);
        let run1 = sink_flush_table(&mut t1);
        for k in 500..1200 {
            bump(&mut t1, Some(k), 1, 2 * k);
        }
        bump(&mut t1, None, 2, 3);
        let part1 = sink_partition_remainder(&t1);
        let mut t2 = mk_table(64);
        for k in 300..1500 {
            bump(&mut t2, Some(k), 1, 3 * k);
        }
        let part2 = sink_partition_remainder(&t2);
        let locals = [
            SinkLocalView { spilled: &[], runs: core::slice::from_ref(&run1), remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None, canon_store: None, gid_gen: 0, direct: false }) },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(SinkRemainder { table: &t2, part: &part2, canon: None, canon_store: None, gid_gen: 0, direct: false }) },
        ];
        let combines = test_combines();
        for b in 0..SINK_NBUCKETS {
            let incumbent =
                sink_combine_bucket_impl(b, 1, STATE_BYTES, &locals, &combines, false, false)
                    .unwrap();
            let flat =
                sink_combine_bucket_impl(b, 1, STATE_BYTES, &locals, &combines, false, true)
                    .unwrap();
            assert_eq!(flat.grow_count(), 0, "bucket {b}: presized flat table grew");
            assert_eq!(flat.convert_count(), 0, "bucket {b}: flat table converted");
            assert_merged_identical(&incumbent, &flat, 1);
        }
    }

    #[test]
    fn flat_combine_matches_incumbent_canon() {
        // The canonical corpus (skewed per-worker intern ids, run +
        // remainder faces) through both arms — bytes-mode probes carry the
        // SINK hash, the degeneracy class this lane exists for.
        let mut w1 = canon_worker(canon_shape_int8_text());
        for i in 0..40i64 {
            bump_canon(&mut w1, Some(i % 7), format!("text-{i}").as_bytes(), 1);
        }
        let run1 = sink_flush_table_canon(&mut w1);
        for i in 20..60i64 {
            bump_canon(&mut w1, Some(i % 5), format!("text-{i}").as_bytes(), 2);
        }
        let mut h1 = SinkTableHandle(w1);
        let part1 = h1.partition_remainder();
        let mut w2 = canon_worker(canon_shape_int8_text());
        for i in (0..50i64).rev() {
            bump_canon(&mut w2, Some(i % 7), format!("text-{i}").as_bytes(), 3);
        }
        let mut h2 = SinkTableHandle(w2);
        let part2 = h2.partition_remainder();
        let locals = [
            SinkLocalView {
                spilled: &[],
                runs: core::slice::from_ref(&run1),
                remainder: Some(h1.remainder_view(&part1)),
            },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(h2.remainder_view(&part2)) },
        ];
        let combines = test_combines();
        for gid in [false, true] {
            for b in 0..SINK_NBUCKETS {
                let incumbent =
                    sink_combine_bucket_impl(b, 0, STATE_BYTES, &locals, &combines, gid, false)
                        .unwrap();
                let flat =
                    sink_combine_bucket_impl(b, 0, STATE_BYTES, &locals, &combines, gid, true)
                        .unwrap();
                assert_eq!(flat.grow_count(), 0, "bucket {b}: presized flat table grew");
                assert_eq!(flat.convert_count(), 0, "bucket {b}: flat table converted");
                assert_merged_identical(&incumbent, &flat, 0);
            }
        }
    }

    #[test]
    fn flat_suppresses_constant_top_byte_degeneracy() {
        // The root-cause proof in miniature: keys whose carried hashes share
        // one top byte (a combine claim's invariant — sink bucket = hash
        // top byte). Past TWO_LEVEL_THRESHOLD the incumbent converts
        // two-level and funnels every member into ONE sub-EntrySet (which
        // then re-grows); the flat table does neither. Same inserts, same
        // insertion order, identical read-back.
        const N: usize = ::lanetable::TWO_LEVEL_THRESHOLD + 20_000;
        let mk_key = |i: usize| (i as u64).to_le_bytes();
        // Carried-hash discipline: constant top byte, varying low bits —
        // the shape probe_bytes sees from a combine claim's run hashes.
        let mk_hash =
            |i: usize| (0xABu64 << 56) | (sink_hash(i as u64, 17) & ((1u64 << 56) - 1));
        let mut incumbent = LaneAggTable::with_config(
            KeyRepr::Bytes,
            STATE_BYTES,
            N,
            HashKind::best(),
            EntryLayout::Salt8,
        );
        let mut flat = LaneAggTable::with_flat_capacity(
            KeyRepr::Bytes,
            STATE_BYTES,
            N,
            HashKind::best(),
            EntryLayout::Salt8,
        );
        for i in 0..N {
            let (k, h) = (mk_key(i), mk_hash(i));
            let pi = incumbent.probe_bytes(&k, h);
            let pf = flat.probe_bytes(&k, h);
            assert_eq!(pi.is_new, pf.is_new, "insert {i}");
            assert!(pi.is_new, "distinct keys");
        }
        // Re-probe: every key hits in both.
        for i in 0..N {
            let (k, h) = (mk_key(i), mk_hash(i));
            assert!(!incumbent.probe_bytes(&k, h).is_new, "re-probe {i} (incumbent)");
            assert!(!flat.probe_bytes(&k, h).is_new, "re-probe {i} (flat)");
        }
        assert_eq!(flat.grow_count(), 0, "flat presized table must never grow");
        assert_eq!(flat.convert_count(), 0);
        // The incumbent, presized IDENTICALLY, still degrades: the constant
        // top byte defeats its 256-way presize (two-level at birth for this
        // hint), so the one live sub-EntrySet re-grows.
        assert!(incumbent.is_two_level(), "hint above threshold builds two-level");
        assert!(
            incumbent.grow_count() > 0,
            "constant-top-byte inserts must grow the incumbent's single live sub-set"
        );
        assert_merged_identical(&incumbent, &flat, 0);
    }

    /// GL-SINKSHAPE-1 born-RED twin: a canonical (tail-carrying) emit plan
    /// over the INTERN-ARMED canonical worker's WORD-keyed table. The
    /// admission verdict refuses (the combine falls to the merge arm), the
    /// `emit_row` tripwire behind it stays the fail-closed defense, and the
    /// merge arm over the SAME single Local (the fallback the combine
    /// takes) emits the groups the pass-through could not. This is the
    /// QPS-window collapse shape: fork-on-first-touch under a saturated
    /// pool seals exactly ONE Local with zero flushed runs.
    #[test]
    fn passthrough_admission_refuses_intern_armed_canonical() {
        let mut w = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w, Some(1), b"apple", 1);
        bump_canon(&mut w, Some(2), b"a-rather-long-canonical-key-way-past-eight", 2);
        bump_canon(&mut w, Some(1), b"apple", 3);
        let mut h = SinkTableHandle(w);
        let part = h.partition_remainder();
        let plan = SinkEmitPlan {
            width: 8,
            fixed: Some(12),
            ntails: 1,
            filter: None,
            cols: vec![
                SinkEmitCol::MultiComp { off: 0, width: 8 },
                SinkEmitCol::MultiText { nth: 0 },
                SinkEmitCol::Agg { transno: 0 },
            ],
        };
        assert!(
            !sink_passthrough_admits(&plan, h.table()),
            "canonical plan over an intern-armed word table must refuse the pass-through"
        );
        // The tripwire is the defense BEHIND the admission: the raw
        // pass-through over the mismatched pair keeps failing closed.
        let mut tripped = false;
        for b in 0..SINK_NBUCKETS {
            if part.starts[b + 1] > part.starts[b] {
                let Err(err) = sink_emit_bucket_passthrough(&plan, h.table(), &part, b)
                else {
                    panic!("word-keyed table cannot serve a MultiText emit")
                };
                assert!(
                    err.message().contains("MultiText emit on a word-keyed table"),
                    "unexpected error: {}",
                    err.message()
                );
                tripped = true;
                break;
            }
        }
        assert!(tripped, "fixture populates at least one bucket");
        // The FALLBACK: the merge arm over the same single Local runs the
        // intern chase and emits every group.
        let locals = [SinkLocalView {
            spilled: &[],
            runs: &[],
            remainder: Some(h.remainder_view(&part)),
        }];
        let combines = test_combines();
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
                assert!(seen.insert((k, text), c).is_none(), "group in two buckets");
            }
        }
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[&(1, b"apple".to_vec())], 4);
        assert_eq!(
            seen[&(2, b"a-rather-long-canonical-key-way-past-eight".to_vec())],
            2
        );
    }

    /// GL-SINKSHAPE-1 admission positives + the fail-closed inverse: the
    /// DIRECT (Bytes-keyed) single-text arm keeps its pass-through under
    /// the canonical plan — verdict ADMIT and output equal to the merge
    /// arm over the same Local (values byte-for-byte; MultiText payloads
    /// compared through each buf's own arena) — while word plans keep word
    /// tables and every cross pairing refuses.
    #[test]
    fn passthrough_admits_matching_keying_classes() {
        let mut d = direct_worker();
        for (text, c) in DIRECT_CORPUS {
            bump_direct(&mut d, text, c);
        }
        let mut h = SinkTableHandle(d);
        let part = h.partition_remainder();
        let plan = SinkEmitPlan {
            width: 8,
            fixed: Some(4),
            ntails: 1,
            filter: None,
            cols: vec![
                SinkEmitCol::MultiText { nth: 0 },
                SinkEmitCol::Agg { transno: 0 },
            ],
        };
        assert!(
            sink_passthrough_admits(&plan, h.table()),
            "the DIRECT arm's Bytes table serves the canonical plan"
        );
        let locals = [SinkLocalView {
            spilled: &[],
            runs: &[],
            remainder: Some(h.remainder_view(&part)),
        }];
        let combines = test_combines();
        let mut rows = 0usize;
        for b in 0..SINK_NBUCKETS {
            let merged = sink_combine_bucket(b, 0, STATE_BYTES, &locals, &combines).unwrap();
            let want = sink_emit_bucket(&plan, &merged).unwrap();
            let got = sink_emit_bucket_passthrough(&plan, h.table(), &part, b).unwrap();
            assert_eq!(got.nrows, want.nrows, "bucket {b} row count");
            assert_eq!(got.nulls, want.nulls, "bucket {b} null bitmap");
            for row in 0..got.nrows {
                assert_eq!(
                    emit_text(&got, got.values[row * 2]),
                    emit_text(&want, want.values[row * 2]),
                    "bucket {b} row {row} text"
                );
                assert_eq!(
                    got.values[row * 2 + 1].as_i64(),
                    want.values[row * 2 + 1].as_i64(),
                    "bucket {b} row {row} agg"
                );
            }
            rows += got.nrows;
        }
        assert_eq!(rows, 6, "DIRECT_CORPUS distinct texts");
        // Word plan × word table = the incumbent pass-through shape.
        let word_plan = SinkEmitPlan {
            width: 8,
            fixed: None,
            ntails: 0,
            filter: None,
            cols: vec![SinkEmitCol::Key, SinkEmitCol::Agg { transno: 0 }],
        };
        let wt = mk_table(16);
        assert!(sink_passthrough_admits(&word_plan, &wt));
        // Cross pairings refuse fail-closed (the word-plan × Bytes-table
        // direction is unreachable by construction today — belt).
        assert!(!sink_passthrough_admits(&word_plan, h.table()));
    }

    #[test]
    fn canonical_single_text_short_and_long_keys() {
        // 1-comp Intern shape (4-byte image, one word — the interned single
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
        let mut h = SinkTableHandle(w);
        let part = h.partition_remainder();
        let locals = [SinkLocalView {
            spilled: &[],
            runs: core::slice::from_ref(&run),
            remainder: Some(h.remainder_view(&part)),
        }];
        let combines = test_combines();
        let plan = SinkEmitPlan {
            width: 8,
            fixed: Some(4),
            ntails: 1,
            filter: None,
            cols: vec![SinkEmitCol::MultiText { nth: 0 }, SinkEmitCol::Agg { transno: 0 }],
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

    /// The carried-hash invariant (text-kernels W2): a bytes-mode run's
    /// `hashes[i]` is exactly `sink_hash_bytes` of slot i's canonical
    /// bytes, and a canonical SEAL partition's `hashes` are slot-parallel
    /// to `idx` — the combine probes with these values, so a drift here is
    /// a wrong-merge, not a slowdown.
    #[test]
    fn canonical_run_and_part_carry_slot_hashes() {
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
        let texts: [&[u8]; 5] =
            [b"", b"a", b"abcd", b"abcdefghijklmnop", b"zzzzzzzzzzzzzzzzzzzzzzzz"];
        for (i, t) in texts.iter().enumerate() {
            bump_canon(&mut w, None, t, (i + 1) as i64);
        }
        let run = sink_flush_table_canon(&mut w);
        assert_eq!(run.hashes.len(), run.nrows());
        for i in 0..run.nrows() {
            assert_eq!(
                run.hashes[i],
                sink_hash_bytes(run.key_slice(i)),
                "run slot {i} carries its own canonical hash"
            );
        }
        // Remainder epoch: two re-arrivals + one new key.
        bump_canon(&mut w, None, b"a", 100);
        bump_canon(&mut w, None, b"new-remainder-key", 7);
        let part = sink_partition_remainder_canon(&mut w);
        let crate::compact::CompactKeySpec::Multi(shape_ref) = &w.key else {
            unreachable!("canon worker is Multi");
        };
        assert_eq!(part.hashes.len(), part.idx.len());
        let intern = w.intern.as_ref().unwrap();
        let mut canon = Vec::new();
        for (slot, &row) in part.idx.iter().enumerate() {
            canon_row_bytes(&w.table, shape_ref, intern, row as usize, &mut canon);
            assert_eq!(
                part.hashes[slot],
                sink_hash_bytes(&canon),
                "part slot {slot} carries row {row}'s canonical hash"
            );
        }
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
        let locals = [SinkLocalView { spilled: &[], runs: core::slice::from_ref(&run), remainder: None }];
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

    /// M3.5 spill contract: serializing every bucket of a run set and
    /// rebuilding synthesized runs combines to EXACTLY the same groups as
    /// the in-memory runs (all buckets, null face included via the
    /// in-memory null-block path).
    #[test]
    fn spill_roundtrip_combine_equivalence() {
        let mut t1 = mk_table(64);
        for k in 0..1000 {
            bump(&mut t1, Some(k), 1, k);
        }
        bump(&mut t1, None, 5, 11);
        let mut run_a = sink_flush_table(&mut t1);
        for k in 300..1300 {
            bump(&mut t1, Some(k), 2, 4 * k);
        }
        let mut run_b = sink_flush_table(&mut t1);
        let combines = test_combines();

        // Reference: in-memory combine over [run_a, run_b].
        let runs = [run_a, run_b];
        let locals_mem = [SinkLocalView { spilled: &runs, runs: &[], remainder: None }];
        let mut reference: Vec<CombinedBucket> = Vec::with_capacity(SINK_NBUCKETS);
        for b in 0..SINK_NBUCKETS {
            reference.push(sink_combine_bucket(b, 1, STATE_BYTES, &locals_mem, &combines).unwrap());
        }
        let [mut run_a, mut run_b] = runs;

        // Spill image: per-bucket serialize both runs (epoch order), null
        // blocks pulled aside exactly as the Local does.
        let state_words = STATE_BYTES / 8;
        let mut null_blocks: Vec<Vec<u64>> = Vec::new();
        for r in [&mut run_a, &mut run_b] {
            if let Some(nb) = r.null_states.take() {
                null_blocks.push(nb);
            }
        }
        let mut found_rows = 0usize;
        for b in 0..SINK_NBUCKETS {
            let mut bytes = Vec::new();
            sink_run_spill_bucket(&run_a, b, &mut bytes);
            sink_run_spill_bucket(&run_b, b, &mut bytes);
            let mut synth = vec![sink_run_from_spill(b, 1, state_words, &bytes).unwrap()];
            if b == SINK_NULL_BUCKET {
                for nb in &null_blocks {
                    synth.push(sink_null_only_run(1, state_words, nb.clone()));
                }
            }
            let locals = [SinkLocalView { spilled: &synth, runs: &[], remainder: None }];
            assert_eq!(
                sink_bucket_row_count(b, &locals),
                (run_a.starts[b + 1] - run_a.starts[b] + run_b.starts[b + 1] - run_b.starts[b])
                    as usize
            );
            let got = sink_combine_bucket(b, 1, STATE_BYTES, &locals, &combines).unwrap();
            assert_eq!(got.nrows(), reference[b].nrows(), "bucket {b} group count");
            for row in 0..got.nrows() {
                let key = got.row_key_int(row);
                assert_eq!(
                    read_group(&got, key),
                    read_group(&reference[b], key),
                    "bucket {b} key {key:?}"
                );
                found_rows += 1;
            }
        }
        // 1300 distinct keys + the NULL group.
        assert_eq!(found_rows, 1301);
    }

    /// Torn spill records fail closed.
    #[test]
    fn spill_torn_record_refuses() {
        let bytes = vec![0u8; sink_spill_row_bytes(1, 2) + 3];
        assert!(sink_run_from_spill(0, 1, 2, &bytes).is_err());
    }

    /// M3.5 split invariance: routing a bucket's records by deeper hash
    /// bits and combining per sub-bucket yields exactly the direct
    /// combine's groups, each group in exactly one sub-bucket. Remainder
    /// serialization rides the same law.
    #[test]
    fn split_route_combine_invariance() {
        let mut t1 = mk_table(64);
        for k in 0..2000 {
            bump(&mut t1, Some(k), 1, k);
        }
        let run1 = sink_flush_table(&mut t1);
        // Remainder face: overlapping keys, serialized through the SEAL
        // partition index.
        for k in 1500..2500 {
            bump(&mut t1, Some(k), 3, k + 7);
        }
        let part1 = sink_partition_remainder(&t1);
        let combines = test_combines();
        let state_words = STATE_BYTES / 8;

        for b in [0usize, 17, SINK_NULL_BUCKET] {
            let locals = [SinkLocalView {
                spilled: core::slice::from_ref(&run1),
                runs: &[],
                remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None, canon_store: None, gid_gen: 0, direct: false }),
            }];
            let direct = sink_combine_bucket(b, 1, STATE_BYTES, &locals, &combines).unwrap();

            // Serialize the bucket (run + remainder), route at depth 1.
            let mut bytes = Vec::new();
            sink_run_spill_bucket(&run1, b, &mut bytes);
            sink_remainder_spill_bucket(&t1, &part1, b, &mut bytes);
            let mut subs: Vec<Vec<u8>> = vec![Vec::new(); SINK_NBUCKETS];
            sink_route_records(&bytes, 1, state_words, 1, &mut subs).unwrap();

            let mut seen = std::collections::HashMap::new();
            let mut total = 0usize;
            for sub in &subs {
                if sub.is_empty() {
                    continue;
                }
                let synth = sink_run_from_spill(b, 1, state_words, sub).unwrap();
                let sl = [SinkLocalView {
                    spilled: core::slice::from_ref(&synth),
                    runs: &[],
                    remainder: None,
                }];
                let merged = sink_combine_bucket(b, 1, STATE_BYTES, &sl, &combines).unwrap();
                for row in 0..merged.nrows() {
                    let key = merged.row_key_int(row).expect("no NULL rows in records");
                    let prev = seen.insert(key, read_group(&merged, Some(key)).unwrap());
                    assert!(prev.is_none(), "group {key} in two sub-buckets");
                    total += 1;
                }
            }
            // Every direct non-NULL group appears exactly once with equal
            // states; the NULL group (only bucket 255, remainder face)
            // stays OUT of routed records by contract.
            let mut direct_nonnull = 0usize;
            for row in 0..direct.nrows() {
                match direct.row_key_int(row) {
                    Some(key) => {
                        direct_nonnull += 1;
                        assert_eq!(
                            seen.get(&key),
                            read_group(&direct, Some(key)).as_ref(),
                            "bucket {b} key {key}"
                        );
                    }
                    None => {
                        assert_eq!(b, SINK_NULL_BUCKET);
                        assert!(sink_remainder_null_block(&t1).is_some());
                    }
                }
            }
            assert_eq!(total, direct_nonnull, "bucket {b} group counts");
        }
    }

    // -- Combine-phase top-N composition (m3-sort-b car 1) -------------------

    /// Reference selection: full sort of every group under the selection
    /// total order (badness, null tier, key words), truncated to k.
    fn topn_reference(t: &LaneAggTable, spec: &SinkTopnSpec, k: usize) -> Vec<Option<i64>> {
        let mut all: Vec<(u64, bool, [u64; 2], Option<i64>)> = (0..t.nrows())
            .map(|row| {
                let pg = unsafe {
                    &*t.row_states(row).cast_const().cast::<AggPerGroup>().add(spec.transno as usize)
                };
                assert!(!pg.trans_value_is_null && !pg.no_trans_value);
                let b = crate::compact::topkfin_badness(pg.trans_value.as_i64(), spec.desc);
                match row_key_words(t, row) {
                    Some(w) => (b, false, w, t.row_key_int(row)),
                    None => (b, true, [0, 0], None),
                }
            })
            .collect();
        all.sort_unstable();
        all.truncate(k);
        all.into_iter().map(|(_, _, _, key)| key).collect()
    }

    fn cand_keys(t: &LaneAggTable, cands: &[SinkTopnCand]) -> Vec<Option<i64>> {
        cands.iter().map(|c| t.row_key_int(c.row as usize)).collect()
    }

    #[test]
    fn topn_candidates_match_reference() {
        // Dense count ties (the boundary class) + a NULL group + both
        // directions x several bounds, vs the full-sort reference.
        let mut t = mk_table(64);
        for k in 0..200i64 {
            // counts collide heavily: count = k % 7 + 1 after the loop.
            for _ in 0..(k % 7 + 1) {
                bump(&mut t, Some(k), 1, k);
            }
        }
        bump(&mut t, None, 3, 0);
        for desc in [false, true] {
            for bound in [1u32, 7, 10, 100, 500] {
                let spec = SinkTopnSpec { transno: 0, desc, bound };
                let got = sink_topn_candidates(&t, &spec, 0).expect("no NULL order keys");
                assert_eq!(got.len(), (bound as usize).min(t.nrows()));
                assert_eq!(
                    cand_keys(&t, &got),
                    topn_reference(&t, &spec, bound as usize),
                    "desc={desc} bound={bound}"
                );
                // Sorted best-first under the total order.
                assert!(got.windows(2).all(|w| w[0] < w[1]));
            }
        }
    }

    #[test]
    fn topn_candidates_decline_on_null_order_key() {
        // Transition [1] (max) stays NULL for a never-bumped-max group:
        // write one group's max state back to NULL and select on transno 1.
        let mut t = mk_table(16);
        for k in 0..10 {
            bump(&mut t, Some(k), 1, k);
        }
        unsafe {
            let pg = t.row_states(3).cast::<AggPerGroup>().add(1);
            (*pg).trans_value_is_null = true;
        }
        let spec = SinkTopnSpec { transno: 1, desc: true, bound: 5 };
        assert!(sink_topn_candidates(&t, &spec, 0).is_none());
        // The count transition (never NULL) still selects.
        let spec0 = SinkTopnSpec { transno: 0, desc: true, bound: 5 };
        assert!(sink_topn_candidates(&t, &spec0, 0).is_some());
    }

    #[test]
    fn topn_merge_matches_flat_reference() {
        // Per-bucket selection + truncate-merge == selection over the union,
        // for an arbitrary 4-way bucket split (partition independence).
        let keys: Vec<i64> = (0..300).collect();
        let spec = SinkTopnSpec { transno: 0, desc: true, bound: 17 };
        let mut union = mk_table(64);
        let mut parts: Vec<LaneAggTable> = (0..4).map(|_| mk_table(64)).collect();
        for &k in &keys {
            let c = k % 5 + 1; // dense ties
            for _ in 0..c {
                bump(&mut union, Some(k), 1, k);
                bump(&mut parts[(k as usize * 7919) % 4], Some(k), 1, k);
            }
        }
        let lists: Vec<Vec<SinkTopnCand>> = parts
            .iter()
            .enumerate()
            .map(|(b, t)| sink_topn_candidates(t, &spec, b as u16).unwrap())
            .collect();
        let winners = sink_topn_merge(&lists, spec.bound as usize);
        let got: Vec<Option<i64>> = winners
            .iter()
            .map(|&(b, row)| parts[b as usize].row_key_int(row as usize))
            .collect();
        assert_eq!(got, topn_reference(&union, &spec, spec.bound as usize));
    }

    #[test]
    fn topn_merge_edges() {
        // Empty lists, bound beyond total, bound zero.
        assert!(sink_topn_merge(&[], 10).is_empty());
        assert!(sink_topn_merge(&[Vec::new(), Vec::new()], 10).is_empty());
        let mut t = mk_table(16);
        for k in 0..3 {
            bump(&mut t, Some(k), k + 1, k);
        }
        let spec = SinkTopnSpec { transno: 0, desc: true, bound: 100 };
        let l = sink_topn_candidates(&t, &spec, 5).unwrap();
        assert_eq!(l.len(), 3);
        let w = sink_topn_merge(&[l.clone()], 100);
        assert_eq!(w.len(), 3);
        assert_eq!(w[0], (5, l[0].row));
        assert!(sink_topn_merge(&[l], 0).is_empty());
    }

    // -- Top-N x canonical-bytes keys (train-14 P0: the topn x c3 panic) -----

    fn mk_bytes_table(hint: usize) -> LaneAggTable {
        LaneAggTable::with_config(
            KeyRepr::Bytes,
            STATE_BYTES,
            hint,
            HashKind::best(),
            EntryLayout::Salt8,
        )
    }

    /// The c3-class corpus: shared >16-byte prefixes (the analytics-bank URL
    /// shape — every prefix-only image would collide), keys equal through
    /// byte 16 differing only in length, short (<= 8 B packed-word) keys,
    /// and the empty canonical key.
    fn bytes_corpus() -> Vec<Vec<u8>> {
        let mut keys: Vec<Vec<u8>> = (0..40)
            .map(|i| format!("http://example.com/shared-prefix/{i:03}").into_bytes())
            .collect();
        keys.push(b"pppppppppppppppp".to_vec()); // exactly 16
        keys.push(b"ppppppppppppppppX".to_vec()); // 16-byte prefix tie
        keys.push(b"ppppppppppppppppXY".to_vec());
        keys.push(b"a".to_vec());
        keys.push(b"ab".to_vec());
        keys.push(b"abcdefgh".to_vec()); // 8-byte packed-word edge
        keys.push(Vec::new()); // the empty canonical key
        keys
    }

    /// Reference selection for bytes tables: full sort under (badness,
    /// null tier, canonical key bytes), truncated to k. `None` = the NULL
    /// group.
    fn topn_reference_bytes(
        t: &LaneAggTable,
        spec: &SinkTopnSpec,
        k: usize,
    ) -> Vec<Option<Vec<u8>>> {
        let mut scratch = [0u8; 8];
        let mut all: Vec<(u64, bool, Vec<u8>)> = (0..t.nrows())
            .map(|row| {
                let pg = unsafe {
                    &*t.row_states(row)
                        .cast_const()
                        .cast::<AggPerGroup>()
                        .add(spec.transno as usize)
                };
                assert!(!pg.trans_value_is_null && !pg.no_trans_value);
                let b = crate::compact::topkfin_badness(pg.trans_value.as_i64(), spec.desc);
                match t.row_key_bytes(row, &mut scratch) {
                    Some(kb) => (b, false, kb.to_vec()),
                    None => (b, true, Vec::new()),
                }
            })
            .collect();
        all.sort_unstable();
        all.truncate(k);
        all.into_iter().map(|(_, nl, kb)| if nl { None } else { Some(kb) }).collect()
    }

    fn cand_keys_bytes(t: &LaneAggTable, cands: &[SinkTopnCand]) -> Vec<Option<Vec<u8>>> {
        let mut scratch = [0u8; 8];
        cands
            .iter()
            .map(|c| t.row_key_bytes(c.row as usize, &mut scratch).map(<[u8]>::to_vec))
            .collect()
    }

    #[test]
    fn topn_candidates_bytes_match_reference() {
        // The mt16 stop-finding shape at unit altitude: a canonical-bytes
        // (c3 text) table under an armed top-N spec. Pre-fix this hit
        // row_key_words' Bytes unreachable!; post-fix the selection runs on
        // the canonical key bytes. Dense count ties force the bytes
        // tie-break; a NULL group rides along.
        let mut t = mk_bytes_table(64);
        for (i, key) in bytes_corpus().iter().enumerate() {
            for _ in 0..(i % 5 + 1) {
                bump_bytes(&mut t, Some(key.as_slice()), 1, i as i64);
            }
        }
        bump_bytes(&mut t, None, 3, 0);
        for desc in [false, true] {
            for bound in [1u32, 5, 10, 100] {
                let spec = SinkTopnSpec { transno: 0, desc, bound };
                let got = sink_topn_candidates(&t, &spec, 0).expect("no NULL order keys");
                assert_eq!(got.len(), (bound as usize).min(t.nrows()));
                assert_eq!(
                    cand_keys_bytes(&t, &got),
                    topn_reference_bytes(&t, &spec, bound as usize),
                    "desc={desc} bound={bound}"
                );
                // Sorted best-first under the total order.
                assert!(got.windows(2).all(|w| w[0] < w[1]));
            }
        }
    }

    #[test]
    fn topn_bytes_winner_set_insertion_order_independent() {
        // The determinism half of the selection-order totality law: the
        // winner KEY SET is a pure function of the data — merged-table row
        // order (worker claim order) must not leak into it, including on
        // the >16-byte shared-prefix and prefix-tie classes.
        let keys = bytes_corpus();
        let count_of = |i: usize| (i % 3 + 1) as i64; // dense badness ties
        let mut fwd = mk_bytes_table(64);
        for (i, key) in keys.iter().enumerate() {
            bump_bytes(&mut fwd, Some(key.as_slice()), count_of(i), 0);
        }
        let mut rev = mk_bytes_table(64);
        for (i, key) in keys.iter().enumerate().rev() {
            bump_bytes(&mut rev, Some(key.as_slice()), count_of(i), 0);
        }
        for desc in [false, true] {
            for bound in [1u32, 4, 9, 33] {
                let spec = SinkTopnSpec { transno: 0, desc, bound };
                let a = cand_keys_bytes(
                    &fwd,
                    &sink_topn_candidates(&fwd, &spec, 0).expect("selects"),
                );
                let b = cand_keys_bytes(
                    &rev,
                    &sink_topn_candidates(&rev, &spec, 0).expect("selects"),
                );
                assert_eq!(a, b, "desc={desc} bound={bound}");
            }
        }
    }

    #[test]
    fn topn_merge_bytes_matches_flat_reference() {
        // Per-bucket selection + truncate-merge == selection over the
        // union, for an arbitrary 4-way split of the bytes corpus
        // (partition independence over the bytes image).
        let keys = bytes_corpus();
        let spec = SinkTopnSpec { transno: 0, desc: true, bound: 13 };
        let mut union = mk_bytes_table(64);
        let mut parts: Vec<LaneAggTable> = (0..4).map(|_| mk_bytes_table(64)).collect();
        for (i, key) in keys.iter().enumerate() {
            let c = (i % 5 + 1) as i64; // dense ties
            bump_bytes(&mut union, Some(key.as_slice()), c, 0);
            bump_bytes(&mut parts[(i * 7919) % 4], Some(key.as_slice()), c, 0);
        }
        let lists: Vec<Vec<SinkTopnCand>> = parts
            .iter()
            .enumerate()
            .map(|(b, t)| sink_topn_candidates(t, &spec, b as u16).unwrap())
            .collect();
        let winners = sink_topn_merge(&lists, spec.bound as usize);
        let got: Vec<Option<Vec<u8>>> = winners
            .iter()
            .map(|&(b, row)| {
                let mut scratch = [0u8; 8];
                parts[b as usize].row_key_bytes(row as usize, &mut scratch).map(<[u8]>::to_vec)
            })
            .collect();
        assert_eq!(got, topn_reference_bytes(&union, &spec, spec.bound as usize));
    }

    // -- winners-only compact materialization (topn-winners-only inc-3) ----

    /// Row `ci` of `compact` must equal row `fi` of `full` under `plan`:
    /// byval datums bit-compare; MultiText compares arena payload bytes
    /// (each buf owns its arena, so pointers never compare).
    fn assert_rows_equal(
        plan: &SinkEmitPlan,
        full: &SinkEmitBuf,
        fi: usize,
        compact: &SinkEmitBuf,
        ci: usize,
    ) {
        let natts = plan.cols.len();
        for (c, col) in plan.cols.iter().enumerate() {
            let (fv, fn_) = (full.values[fi * natts + c], full.nulls[fi * natts + c]);
            let (cv, cn) = (compact.values[ci * natts + c], compact.nulls[ci * natts + c]);
            assert_eq!(fn_, cn, "null flag col {c} (full row {fi} vs compact {ci})");
            match col {
                SinkEmitCol::MultiText { .. } => {
                    assert_eq!(
                        emit_text(full, fv),
                        emit_text(compact, cv),
                        "text col {c} (full row {fi} vs compact {ci})"
                    );
                }
                _ => {
                    if !cn {
                        assert_eq!(
                            fv.as_i64(),
                            cv.as_i64(),
                            "datum col {c} (full row {fi} vs compact {ci})"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn emit_bucket_rows_matches_full_subsets() {
        // Word repr incl. the NULL group: every subset row of the compact
        // emit equals the full emit's row at the original index.
        let mut t = mk_table(64);
        for k in 0..97i64 {
            bump(&mut t, Some(k), k % 7 + 1, k);
        }
        bump(&mut t, None, 3, 0);
        let plan = SinkEmitPlan {
            width: 8,
            fixed: None,
            ntails: 0,
            filter: None,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::Agg { transno: 0 },
                SinkEmitCol::Agg { transno: 1 },
            ],
        };
        let full = sink_emit_bucket(&plan, &t).unwrap();
        let n = t.nrows() as u32;
        let subsets: Vec<Vec<u32>> = vec![
            Vec::new(),
            vec![0],
            vec![n - 1],
            (0..n).filter(|r| r % 3 == 1).collect(),
            (0..n).collect(),
        ];
        for rows in subsets {
            let compact = sink_emit_bucket_rows(&plan, &t, &rows).unwrap();
            assert_eq!(compact.nrows, rows.len());
            for (ci, &fi) in rows.iter().enumerate() {
                assert_rows_equal(&plan, &full, fi as usize, &compact, ci);
            }
        }
    }

    #[test]
    fn emit_bucket_rows_matches_full_bytes_arena() {
        // Bytes repr (c3 text keys — arena-copied MultiText tails): compact
        // emit's arena images equal the full emit's, row for row.
        let mut t = mk_bytes_table(64);
        for (i, key) in bytes_corpus().iter().enumerate() {
            bump_bytes(&mut t, Some(key.as_slice()), (i % 5 + 1) as i64, 0);
        }
        let plan = SinkEmitPlan {
            width: 8,
            fixed: Some(0),
            ntails: 1,
            filter: None,
            cols: vec![SinkEmitCol::MultiText { nth: 0 }, SinkEmitCol::Agg { transno: 0 }],
        };
        let full = sink_emit_bucket(&plan, &t).unwrap();
        let n = t.nrows() as u32;
        let rows: Vec<u32> = (0..n).filter(|r| r % 2 == 0).collect();
        let compact = sink_emit_bucket_rows(&plan, &t, &rows).unwrap();
        assert_eq!(compact.nrows, rows.len());
        for (ci, &fi) in rows.iter().enumerate() {
            assert_rows_equal(&plan, &full, fi as usize, &compact, ci);
        }
    }

    /// The winners-only remap contract end-to-end at the sink unit level:
    /// select candidates, remap their `row` payloads to compact indices
    /// (sorted-row order), materialize only those rows — every candidate's
    /// compact row must be byte-equal to the full emit's row at the
    /// candidate's original table index. Dense-tie key spaces × directions
    /// × bounds × word/bytes reprs (the design's inc-3 unit).
    #[test]
    fn winners_only_remap_matches_full_reference() {
        // Word repr.
        let mut tw = mk_table(64);
        for k in 0..150i64 {
            bump(&mut tw, Some(k), k % 7 + 1, k);
        }
        bump(&mut tw, None, 3, 0);
        let plan_w = SinkEmitPlan {
            width: 8,
            fixed: None,
            ntails: 0,
            filter: None,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::Agg { transno: 0 },
                SinkEmitCol::Agg { transno: 1 },
            ],
        };
        // Bytes repr (dense ties over the c3 corpus).
        let mut tb = mk_bytes_table(64);
        for (i, key) in bytes_corpus().iter().enumerate() {
            bump_bytes(&mut tb, Some(key.as_slice()), (i % 5 + 1) as i64, 0);
        }
        let plan_b = SinkEmitPlan {
            width: 8,
            fixed: Some(0),
            ntails: 1,
            filter: None,
            cols: vec![SinkEmitCol::MultiText { nth: 0 }, SinkEmitCol::Agg { transno: 0 }],
        };
        for (t, plan) in [(&tw, &plan_w), (&tb, &plan_b)] {
            let full = sink_emit_bucket(plan, t).unwrap();
            for desc in [false, true] {
                for bound in [1u32, 7, 10, 100] {
                    let spec = SinkTopnSpec { transno: 0, desc, bound };
                    let mut cands =
                        sink_topn_candidates(t, &spec, 0).expect("no NULL order keys");
                    let mut rows: Vec<u32> = cands.iter().map(|c| c.row).collect();
                    rows.sort_unstable();
                    let orig: Vec<u32> = cands.iter().map(|c| c.row).collect();
                    for c in &mut cands {
                        c.row = rows.binary_search(&c.row).expect("candidate row") as u32;
                    }
                    let compact = sink_emit_bucket_rows(plan, t, &rows).unwrap();
                    assert_eq!(compact.nrows, rows.len());
                    for (c, &fi) in cands.iter().zip(&orig) {
                        assert_rows_equal(plan, &full, fi as usize, &compact, c.row as usize);
                    }
                }
            }
        }
    }

    // -- Split×selection (winners-phase2) --------------------------------

    #[test]
    fn emit_acc_concat_matches_per_table_and_owns_arena() {
        // The combine-split concatenation through SinkEmitAcc: rows equal
        // the per-table emits, and every byref datum points into the
        // FINISHED buf's own arena (the former SinkEmitBuf::append copied
        // resolved datums while dropping the source arena — use-after-free
        // for byref emit columns; this is its regression pin, on the
        // arena-copying MultiText shape).
        let plan = SinkEmitPlan {
            width: 8,
            fixed: Some(0),
            ntails: 1,
            filter: None,
            cols: vec![SinkEmitCol::MultiText { nth: 0 }, SinkEmitCol::Agg { transno: 0 }],
        };
        let corpus = bytes_corpus();
        let (a, b) = corpus.split_at(corpus.len() / 2);
        let mut bufs: Vec<SinkEmitBuf> = Vec::new();
        let mut acc = SinkEmitAcc::default();
        for frag in [a, b] {
            let mut t = mk_bytes_table(64);
            for (i, key) in frag.iter().enumerate() {
                bump_bytes(&mut t, Some(key.as_slice()), i as i64 + 1, 0);
            }
            bufs.push(sink_emit_bucket(&plan, &t).unwrap());
            acc.emit_table(&plan, &t).unwrap();
        }
        let got = acc.finish();
        assert_eq!(got.nrows, bufs.iter().map(|b| b.nrows).sum::<usize>());
        let natts = plan.cols.len();
        let arena = got.arena.as_ptr() as usize..got.arena.as_ptr() as usize + got.arena.len();
        let mut ci = 0usize;
        for buf in &bufs {
            for fi in 0..buf.nrows {
                assert_rows_equal(&plan, buf, fi, &got, ci);
                // Ownership pin: the text datum resolves into GOT's arena.
                let v = got.values[ci * natts];
                assert!(
                    arena.contains(&v.as_usize()),
                    "byref datum must point into the finished buf's own arena"
                );
                ci += 1;
            }
        }
    }

    /// Disjoint fragment tables of one key space (the combine-split's
    /// sub-partitions) + the whole table they partition. Fragment 0 carries
    /// the NULL group (the split's NULL mini-combine leaf).
    fn split_fragments(nfrags: usize) -> (Vec<LaneAggTable>, LaneAggTable) {
        let mut whole = mk_table(64);
        let mut frags: Vec<LaneAggTable> = (0..nfrags).map(|_| mk_table(64)).collect();
        for k in 0..150i64 {
            bump(&mut whole, Some(k), k % 7 + 1, k);
            bump(&mut frags[(k % nfrags as i64) as usize], Some(k), k % 7 + 1, k);
        }
        bump(&mut whole, None, 3, 0);
        bump(&mut frags[0], None, 3, 0);
        (frags, whole)
    }

    #[test]
    fn fragment_merge_matches_whole_partition_selection() {
        // The split×selection lemma at unit altitude: per-fragment
        // top-`bound` lists (disjoint sub-partitions), truncate-merged,
        // select EXACTLY the whole partition's top-`bound` in the selection
        // total order — candidates survive the split because a partition
        // winner is beaten by fewer than `bound` groups in its own
        // fragment (the design's superset lemma one level deeper).
        let (frags, whole) = split_fragments(3);
        for desc in [false, true] {
            for bound in [1u32, 7, 10, 100, 200] {
                let spec = SinkTopnSpec { transno: 0, desc, bound };
                let want: Vec<(u64, bool, [u64; 2])> =
                    sink_topn_candidates(&whole, &spec, 3)
                        .expect("no NULL order keys")
                        .iter()
                        .map(|c| (c.badness, c.null_key, c.kw))
                        .collect();
                let lists: Vec<Vec<SinkTopnCand>> = frags
                    .iter()
                    .map(|t| sink_topn_candidates(t, &spec, 3).expect("no NULL order keys"))
                    .collect();
                let got: Vec<(u64, bool, [u64; 2])> =
                    sink_topn_merge_fragments(lists, bound as usize)
                        .iter()
                        .map(|c| (c.badness, c.null_key, c.kw))
                        .collect();
                assert_eq!(got, want, "desc={desc} bound={bound}");
            }
        }
    }

    #[test]
    fn fragment_winners_only_emit_remap_end_to_end() {
        // The runtime split-leaf discipline end-to-end at unit level:
        // per fragment select → sort rows → remap against the accumulator
        // base → emit only those rows; after the fragment merge, every
        // surviving candidate's accumulator row must carry ITS group (key
        // column datum equals the candidate's key words) with the whole
        // table's values (compared against the whole-table full emit).
        let plan = SinkEmitPlan {
            width: 8,
            fixed: None,
            ntails: 0,
            filter: None,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::Agg { transno: 0 },
                SinkEmitCol::Agg { transno: 1 },
            ],
        };
        let (frags, whole) = split_fragments(3);
        let full = sink_emit_bucket(&plan, &whole).unwrap();
        // Whole-table row index by key (NULL group under i64::MIN).
        let mut by_key = std::collections::HashMap::new();
        for row in 0..whole.nrows() {
            by_key.insert(whole.row_key_int(row).unwrap_or(i64::MIN), row);
        }
        let natts = plan.cols.len();
        for desc in [false, true] {
            for bound in [1u32, 7, 10, 100] {
                let spec = SinkTopnSpec { transno: 0, desc, bound };
                let mut acc = SinkEmitAcc::default();
                let mut lists: Vec<Vec<SinkTopnCand>> = Vec::new();
                for t in &frags {
                    let mut cands =
                        sink_topn_candidates(t, &spec, 3).expect("no NULL order keys");
                    let mut rows: Vec<u32> = cands.iter().map(|c| c.row).collect();
                    rows.sort_unstable();
                    let base = acc.nrows() as u32;
                    for c in &mut cands {
                        c.row = base + rows.binary_search(&c.row).expect("own row") as u32;
                    }
                    acc.emit_rows(&plan, t, &rows).unwrap();
                    lists.push(cands);
                }
                let winners = sink_topn_merge_fragments(lists, bound as usize);
                let buf = acc.finish();
                assert_eq!(winners.len(), (bound as usize).min(whole.nrows()));
                for w in &winners {
                    let key = if w.null_key { i64::MIN } else { w.kw[0] as i64 };
                    let fi = by_key[&key];
                    assert_rows_equal(&plan, &full, fi, &buf, w.row as usize);
                    // The key column datum IS the candidate's group.
                    let ci = w.row as usize * natts;
                    if w.null_key {
                        assert!(buf.nulls[ci]);
                    } else {
                        assert_eq!(buf.values[ci].as_i64(), key);
                    }
                }
            }
        }
    }

    // -- LIMIT-k-no-ORDER group-admission freeze (band-2a) -----------------

    /// SinkFreeze state machine: election is exclusive, entries visible
    /// only after publish, disable fails open.
    #[test]
    fn freeze_state_machine() {
        let fz = SinkFreeze::new(3);
        assert!(!fz.frozen());
        assert!(fz.entries().is_none());
        assert!(fz.try_begin_install(), "first election wins");
        assert!(!fz.try_begin_install(), "second election loses");
        assert!(fz.entries().is_none(), "no entries mid-install");
        fz.publish(vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert!(fz.frozen());
        assert_eq!(fz.entries().unwrap().len(), 3);
        assert!(!fz.try_begin_install(), "frozen never re-elects");

        let dz = SinkFreeze::new(2);
        assert!(dz.try_begin_install());
        dz.disable();
        assert!(!dz.frozen());
        assert!(dz.entries().is_none());
        assert!(!dz.try_begin_install(), "disabled never re-elects");
    }

    /// Extraction + membership + subset emit, end to end at the sink unit
    /// level over the canonical (int8, text) shape: freeze on worker 1's
    /// first two groups, combine both workers' faces, filter every bucket —
    /// exactly the two member groups emit, with their FULL cross-worker
    /// combined counts; stragglers never emit.
    #[test]
    fn freeze_member_filter_end_to_end() {
        // Worker 1: (1,apple) (1,banana) (2,apple) — install source.
        let mut w1 = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w1, Some(1), b"apple", 1);
        bump_canon(&mut w1, Some(1), b"banana", 2);
        bump_canon(&mut w1, Some(2), b"apple", 3);
        let entries = sink_freeze_extract_ch(&w1, 2).expect("extractable");
        assert_eq!(entries.len(), 2);
        // Under-bound tables refuse extraction.
        assert!(sink_freeze_extract_ch(&w1, 4).is_none());
        // Worker 2 counts more rows of the members (different intern ids)
        // plus stragglers.
        let mut w2 = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w2, Some(9), b"zzz", 7);
        bump_canon(&mut w2, Some(1), b"banana", 20);
        bump_canon(&mut w2, Some(1), b"apple", 30);
        let mut h1 = SinkTableHandle(w1);
        let part1 = h1.partition_remainder();
        let mut h2 = SinkTableHandle(w2);
        let part2 = h2.partition_remainder();
        let locals = [
            SinkLocalView {
                spilled: &[],
                runs: &[],
                remainder: Some(h1.remainder_view(&part1)),
            },
            SinkLocalView {
                spilled: &[],
                runs: &[],
                remainder: Some(h2.remainder_view(&part2)),
            },
        ];
        let combines = test_combines();
        let plan = SinkEmitPlan {
            width: 8,
            fixed: Some(12),
            ntails: 1,
            filter: None,
            cols: vec![
                SinkEmitCol::MultiComp { off: 0, width: 8 },
                SinkEmitCol::MultiText { nth: 0 },
                SinkEmitCol::Agg { transno: 0 },
            ],
        };
        let shape = canon_shape_int8_text();
        let mut seen: std::collections::HashMap<(i64, Vec<u8>), i64> =
            std::collections::HashMap::new();
        let mut stragglers = 0usize;
        for b in 0..SINK_NBUCKETS {
            let t = sink_combine_bucket(b, 0, STATE_BYTES, &locals, &combines).unwrap();
            let rows = sink_freeze_member_rows(&t, 0, &shape, &entries);
            assert!(rows.windows(2).all(|w| w[0] < w[1]), "ascending rows");
            stragglers += t.nrows() - rows.len();
            let full = sink_emit_bucket(&plan, &t).unwrap();
            let buf = sink_emit_bucket_rows(&plan, &t, &rows).unwrap();
            assert_eq!(buf.nrows, rows.len());
            for (ci, &fi) in rows.iter().enumerate() {
                // Subset emit == full emit at the original indices.
                for c in 0..3usize {
                    let (fv, fn_) = (
                        full.values[fi as usize * 3 + c],
                        full.nulls[fi as usize * 3 + c],
                    );
                    let (cv, cn) = (buf.values[ci * 3 + c], buf.nulls[ci * 3 + c]);
                    assert_eq!(fn_, cn);
                    if c == 1 {
                        assert_eq!(emit_text(&full, fv), emit_text(&buf, cv));
                    } else {
                        assert_eq!(fv.as_i64(), cv.as_i64());
                    }
                }
                let k = buf.values[ci * 3].as_i64();
                let text = emit_text(&buf, buf.values[ci * 3 + 1]);
                let c = buf.values[ci * 3 + 2].as_i64();
                assert!(seen.insert((k, text), c).is_none(), "member in two buckets");
            }
        }
        // Exactly the two members, full cross-worker counts; the (2,apple)
        // and (9,zzz) stragglers were filtered.
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[&(1, b"apple".to_vec())], 31, "1 + 30 across workers");
        assert_eq!(seen[&(1, b"banana".to_vec())], 22, "2 + 20 across workers");
        assert_eq!(stragglers, 2);
    }

    /// Word-keyed Multi shapes: canonical entries are the packed image's LE
    /// prefix; the member filter reconstructs and matches them.
    #[test]
    fn freeze_member_filter_word_mode() {
        let shape = MkShape {
            comps: vec![
                crate::compact::MkComp { att: 0, off: 0, kind: MkCompKind::Int { width: 4 } },
                crate::compact::MkComp { att: 1, off: 4, kind: MkCompKind::Int { width: 2 } },
            ],
            packed_bytes: 6,
            nullable: false,
            two_words: false,
        };
        // Packed images as the mk feed would build them (LE component
        // packing of (int4, int2) pairs, negative values included).
        let pack = |a: i32, b: i16| -> i64 {
            ((a as u32 as u64) | (((b as u16 as u64) & 0xFFFF) << 32)) as i64
        };
        let mut t = mk_table(16);
        for (a, b, c) in [(7, -1i16, 5i64), (-3, 2, 6), (100, 0, 7)] {
            bump(&mut t, Some(pack(a, b)), c, 0);
        }
        let ch = crate::compact::compact_hash_for_tests(
            t,
            crate::compact::CompactKeySpec::Multi(shape.clone()),
            None,
        );
        let entries = sink_freeze_extract_ch(&ch, 2).expect("extractable");
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().all(|e| e.len() == 6), "6-byte LE image prefix");
        let rows = sink_freeze_member_rows(&ch.table, 1, &shape, &entries);
        assert_eq!(rows, vec![0, 1], "first two insertion rows are the members");
    }

    // -- canon-sink-increments: two-text tails, canonical spill, GID merge --

    /// int2 + TWO Intern components (the CaseDict two-text image class):
    /// Int{2} at 0, Intern at 2, Intern at 6 — 10-byte image, two words.
    fn canon_shape_two_text() -> MkShape {
        MkShape {
            comps: vec![
                crate::compact::MkComp { att: 0, off: 0, kind: MkCompKind::Int { width: 2 } },
                crate::compact::MkComp { att: 1, off: 2, kind: MkCompKind::Intern },
                crate::compact::MkComp { att: 2, off: 6, kind: MkCompKind::Intern },
            ],
            packed_bytes: 10,
            nullable: false,
            two_words: true,
        }
    }

    /// The feed's intern + pack + probe sequence for one two-text row —
    /// the CaseDict pack arm in miniature (shared intern pool, both ids).
    fn bump_canon2(
        ch: &mut crate::compact::CompactHash,
        k: i16,
        t1: &[u8],
        t2: &[u8],
        count: i64,
    ) {
        let intern_one = |t: &mut LaneAggTable, text: &[u8]| -> u32 {
            let hash = t.hash_key_bytes(text);
            let pr = t.probe_bytes(text, hash);
            if pr.is_new {
                let id = (t.nrows() - 1) as u32;
                // SAFETY: fresh zeroed 8-byte state block (intern contract).
                unsafe { pr.states.cast::<u32>().write(id) };
                id
            } else {
                // SAFETY: live state block written at insert.
                unsafe { pr.states.cast::<u32>().read() }
            }
        };
        let t = ch.intern.as_mut().unwrap();
        let id1 = intern_one(t, t1);
        let id2 = intern_one(t, t2);
        let image: u128 = ((k as u16 as u128) & 0xFFFF)
            | ((id1 as u128) << 16)
            | ((id2 as u128) << 48);
        let kw = [image as u64, (image >> 64) as u64];
        let pr = ch.table.probe_i128(kw, ch.table.hash_key_i128(kw));
        bump_probe(pr, count, 0);
    }

    /// Drain every bucket of a canonical combine into (emit datums) keyed
    /// rows — the equivalence oracle for the spill/GID tests.
    fn canon_combine_all(
        locals: &[SinkLocalView<'_>],
        plan: &SinkEmitPlan,
    ) -> std::collections::HashMap<(i64, Vec<u8>, Vec<u8>), i64> {
        let combines = test_combines();
        let mut seen = std::collections::HashMap::new();
        for b in 0..SINK_NBUCKETS {
            let t = sink_combine_bucket(b, 0, STATE_BYTES, locals, &combines).unwrap();
            let buf = sink_emit_bucket(plan, &t).unwrap();
            let natts = plan.cols.len();
            for row in 0..buf.nrows {
                let k = buf.values[row * natts].as_i64();
                let t1 = emit_text(&buf, buf.values[row * natts + 1]);
                let t2 = emit_text(&buf, buf.values[row * natts + 2]);
                let c = buf.values[row * natts + 3].as_i64();
                assert!(
                    seen.insert((k, t1, t2), c).is_none(),
                    "group in two buckets"
                );
            }
        }
        seen
    }

    fn two_text_plan() -> SinkEmitPlan {
        SinkEmitPlan {
            width: 8,
            fixed: Some(10),
            ntails: 2,
            filter: None,
            cols: vec![
                SinkEmitCol::MultiComp { off: 0, width: 2 },
                SinkEmitCol::MultiText { nth: 0 },
                SinkEmitCol::MultiText { nth: 1 },
                SinkEmitCol::Agg { transno: 0 },
            ],
        }
    }

    #[test]
    fn two_text_canonical_flush_combine_emit_roundtrip() {
        // Worker 1 and worker 2 intern the same texts under DIFFERENT ids;
        // the length-prefixed canonical tails must erase the id skew AND
        // keep the two tails apart. ("ab","c") vs ("a","bc") is the
        // injectivity hazard the length prefixes exist for.
        let mut w1 = canon_worker(canon_shape_two_text());
        bump_canon2(&mut w1, 1, b"ab", b"c", 1);
        bump_canon2(&mut w1, 1, b"a", b"bc", 2);
        bump_canon2(&mut w1, 2, b"apple", b"", 3);
        let run1 = sink_flush_table_canon(&mut w1);
        assert_eq!(run1.key_words, 0);
        assert_eq!(run1.nrows(), 3);
        // Remainder: same groups again (ids reused) + a new one.
        bump_canon2(&mut w1, 1, b"ab", b"c", 10);
        bump_canon2(&mut w1, 3, b"", b"zz", 4);
        let mut h1 = SinkTableHandle(w1);
        let part1 = h1.partition_remainder();

        let mut w2 = canon_worker(canon_shape_two_text());
        bump_canon2(&mut w2, 9, b"other", b"text", 7);
        bump_canon2(&mut w2, 1, b"a", b"bc", 20);
        let mut h2 = SinkTableHandle(w2);
        let part2 = h2.partition_remainder();

        let locals = [
            SinkLocalView {
                spilled: &[],
                runs: core::slice::from_ref(&run1),
                remainder: Some(h1.remainder_view(&part1)),
            },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(h2.remainder_view(&part2)) },
        ];
        let seen = canon_combine_all(&locals, &two_text_plan());
        assert_eq!(seen.len(), 5, "(ab,c) and (a,bc) stay distinct groups");
        assert_eq!(seen[&(1, b"ab".to_vec(), b"c".to_vec())], 11);
        assert_eq!(seen[&(1, b"a".to_vec(), b"bc".to_vec())], 22);
        assert_eq!(seen[&(2, b"apple".to_vec(), b"".to_vec())], 3);
        assert_eq!(seen[&(3, b"".to_vec(), b"zz".to_vec())], 4);
        assert_eq!(seen[&(9, b"other".to_vec(), b"text".to_vec())], 7);
    }

    #[test]
    fn canon_tail_grammar_single_multi_and_malformed() {
        // Single tail: the raw region, nth 0 only.
        assert_eq!(canon_tail(b"hello", 1, 0).unwrap(), b"hello");
        assert!(canon_tail(b"hello", 1, 1).is_err());
        // Two tails, length-prefixed.
        let mut region = Vec::new();
        region.extend_from_slice(&2u32.to_le_bytes());
        region.extend_from_slice(b"ab");
        region.extend_from_slice(&3u32.to_le_bytes());
        region.extend_from_slice(b"cde");
        assert_eq!(canon_tail(&region, 2, 0).unwrap(), b"ab");
        assert_eq!(canon_tail(&region, 2, 1).unwrap(), b"cde");
        assert!(canon_tail(&region, 2, 2).is_err());
        // Malformed: truncated content and truncated prefix.
        assert!(canon_tail(&region[..7], 2, 1).is_err());
        assert!(canon_tail(&region[..2], 2, 0).is_err());
    }

    #[test]
    fn canonical_spill_roundtrip_merge_equivalence() {
        // Two epochs of flushed runs + a live remainder; the spilled
        // replay (runs serialized to canonical records, remainder
        // serialized through the SEAL index) must merge EXACTLY like the
        // in-memory faces.
        let mut w = canon_worker(canon_shape_two_text());
        bump_canon2(&mut w, 1, b"alpha", b"x", 1);
        bump_canon2(&mut w, 2, b"beta", b"yy", 2);
        bump_canon2(&mut w, 3, b"", b"", 3);
        let run1 = sink_flush_table_canon(&mut w);
        bump_canon2(&mut w, 1, b"alpha", b"x", 10);
        bump_canon2(&mut w, 4, b"abcdefghijklmnop-long-key-payload", b"tail2", 5);
        let run2 = sink_flush_table_canon(&mut w);
        bump_canon2(&mut w, 2, b"beta", b"yy", 100);
        bump_canon2(&mut w, 5, b"last", b"one", 6);
        let mut h = SinkTableHandle(w);
        let part = h.partition_remainder();

        // Reference: all faces in memory.
        let runs = [run1, run2];
        let reference = {
            let locals = [SinkLocalView {
                spilled: &[],
                runs: &runs,
                remainder: Some(h.remainder_view(&part)),
            }];
            canon_combine_all(&locals, &two_text_plan())
        };

        // Spilled twin: serialize per bucket (runs in flush order — one
        // epoch buffer each, the spill_epoch layout) and the remainder as
        // canonical records; replay through sink_run_from_spill_bytes.
        let state_words = STATE_BYTES / 8;
        let mut synth_by_bucket: Vec<Vec<SinkRun>> = Vec::with_capacity(SINK_NBUCKETS);
        for b in 0..SINK_NBUCKETS {
            let mut v: Vec<SinkRun> = Vec::new();
            let mut bytes: Vec<u8> = Vec::new();
            for r in &runs {
                sink_run_spill_bucket(r, b, &mut bytes);
            }
            sink_remainder_spill_bucket_canon(&h.remainder_view(&part), b, &mut bytes)
                .unwrap();
            if !bytes.is_empty() {
                v.push(sink_run_from_spill_bytes(b, state_words, &bytes).unwrap());
            }
            synth_by_bucket.push(v);
        }
        let combines = test_combines();
        let plan = two_text_plan();
        let mut spilled_seen = std::collections::HashMap::new();
        for b in 0..SINK_NBUCKETS {
            let locals = [SinkLocalView {
                spilled: &synth_by_bucket[b],
                runs: &[],
                remainder: None,
            }];
            let t = sink_combine_bucket(b, 0, STATE_BYTES, &locals, &combines).unwrap();
            let buf = sink_emit_bucket(&plan, &t).unwrap();
            let natts = plan.cols.len();
            for row in 0..buf.nrows {
                let k = buf.values[row * natts].as_i64();
                let t1 = emit_text(&buf, buf.values[row * natts + 1]);
                let t2 = emit_text(&buf, buf.values[row * natts + 2]);
                let c = buf.values[row * natts + 3].as_i64();
                assert!(spilled_seen.insert((k, t1, t2), c).is_none());
            }
        }
        assert_eq!(reference, spilled_seen, "spill replay == in-memory merge");
    }

    #[test]
    fn canonical_spill_torn_records_fail_closed() {
        let state_words = STATE_BYTES / 8;
        let mut w = canon_worker(canon_shape_two_text());
        bump_canon2(&mut w, 1, b"alpha", b"x", 1);
        let run = sink_flush_table_canon(&mut w);
        let b = bucket_of(run.hashes[0]);
        let mut bytes: Vec<u8> = Vec::new();
        sink_run_spill_bucket(&run, b, &mut bytes);
        assert!(!bytes.is_empty());
        // Clean parse round-trips.
        assert_eq!(sink_run_from_spill_bytes(b, state_words, &bytes).unwrap().nrows(), 1);
        // Truncated tail.
        assert!(sink_run_from_spill_bytes(b, state_words, &bytes[..bytes.len() - 8]).is_err());
        // rec_len unaligned.
        let mut bad = bytes.clone();
        bad[0] = bad[0].wrapping_add(1);
        assert!(sink_run_from_spill_bytes(b, state_words, &bad).is_err());
        // key_len inconsistent with rec_len.
        let mut bad = bytes.clone();
        bad[16] = bad[16].wrapping_add(8);
        assert!(sink_run_from_spill_bytes(b, state_words, &bad).is_err());
        // Router fail-closed on the same classes.
        let mut out: Vec<Vec<u8>> = vec![Vec::new(); SINK_NBUCKETS];
        assert!(sink_route_records_bytes(&bytes[..bytes.len() - 8], state_words, 1, &mut out)
            .is_err());
    }

    #[test]
    fn canonical_route_records_bytes_partitions_by_stored_hash() {
        let state_words = STATE_BYTES / 8;
        let mut w = canon_worker(canon_shape_two_text());
        for i in 0..200i16 {
            bump_canon2(&mut w, i, format!("key-{i}").as_bytes(), b"t", 1);
        }
        let run = sink_flush_table_canon(&mut w);
        // Serialize EVERY bucket into one stream, route at depth 1, then
        // verify each record landed by its stored hash's depth-1 byte and
        // that every routed record still parses.
        let mut bytes: Vec<u8> = Vec::new();
        for b in 0..SINK_NBUCKETS {
            sink_run_spill_bucket(&run, b, &mut bytes);
        }
        let mut out: Vec<Vec<u8>> = vec![Vec::new(); SINK_NBUCKETS];
        sink_route_records_bytes(&bytes, state_words, 1, &mut out).unwrap();
        let mut total = 0usize;
        for (s, sub) in out.iter().enumerate() {
            if sub.is_empty() {
                continue;
            }
            let synth = sink_run_from_spill_bytes(0, state_words, sub).unwrap();
            total += synth.nrows();
            for i in 0..synth.nrows() {
                assert_eq!(((synth.hashes[i] >> 48) & 0xFF) as usize, s);
            }
        }
        assert_eq!(total, 200);
    }

    #[test]
    fn gid_merge_matches_bytes_probe_and_respects_generations() {
        // Build a run ladder with duplicates across epochs, an intern-table
        // GENERATION BOUNDARY in the middle (same packed words, DIFFERENT
        // canonical bytes across it — the ambiguity the generation stamp
        // exists to kill), and a remainder duplicating a post-boundary
        // group. The GID-carrying combine must equal the words-stripped
        // (pure bytes-probe) combine exactly.
        let mut w = canon_worker(canon_shape_int8_text());
        bump_canon(&mut w, Some(1), b"first-gen-a", 1);
        bump_canon(&mut w, Some(2), b"first-gen-b", 2);
        let run1 = sink_flush_table_canon_impl(&mut w, true, false);
        bump_canon(&mut w, Some(1), b"first-gen-a", 10);
        let run2 = sink_flush_table_canon_impl(&mut w, true, false);
        // Simulate the wide-vocabulary intern reset (agg_sink_flush_now's
        // reset arm): ids restart, the generation bumps.
        w.intern.as_mut().unwrap().reset();
        w.intern_gen += 1;
        // Post-reset: "second-gen-a" gets intern id 0 — the SAME packed
        // words as key (1, "first-gen-a") pre-reset.
        bump_canon(&mut w, Some(1), b"second-gen-a", 100);
        let run3 = sink_flush_table_canon_impl(&mut w, true, false);
        assert_eq!(run3.gid_gen, 1, "post-reset runs carry the new generation");
        bump_canon(&mut w, Some(1), b"second-gen-a", 1000);
        bump_canon(&mut w, Some(2), b"first-gen-b", 3);
        let mut h = SinkTableHandle(w);
        let part = h.partition_remainder();

        let runs = [run1, run2, run3];
        assert!(runs.iter().all(|r| !r.keys.is_empty()), "flush carries gid words");
        let plan = SinkEmitPlan {
            width: 8,
            fixed: Some(12),
            ntails: 1,
            filter: None,
            cols: vec![
                SinkEmitCol::MultiComp { off: 0, width: 8 },
                SinkEmitCol::MultiText { nth: 0 },
                SinkEmitCol::Agg { transno: 0 },
            ],
        };
        let combines = test_combines();
        let drain = |locals: &[SinkLocalView<'_>]| {
            let mut seen = std::collections::HashMap::new();
            for b in 0..SINK_NBUCKETS {
                // GID lane forced ON (the default is the measured-off
                // evidence channel; the law under test is byte-invisibility).
                let t =
                    sink_combine_bucket_impl(b, 0, STATE_BYTES, locals, &combines, true, true)
                        .unwrap();
                let buf = sink_emit_bucket(&plan, &t).unwrap();
                for row in 0..buf.nrows {
                    let k = buf.values[row * 3].as_i64();
                    let text = emit_text(&buf, buf.values[row * 3 + 1]);
                    let c = buf.values[row * 3 + 2].as_i64();
                    assert!(seen.insert((k, text), c).is_none());
                }
            }
            seen
        };
        let with_gids = {
            let locals = [SinkLocalView {
                spilled: &[],
                runs: &runs,
                remainder: Some(h.remainder_view(&part)),
            }];
            drain(&locals)
        };
        // Words-stripped twin: identical faces, gid words removed — every
        // arrival bytes-probes (the map never engages).
        let stripped: Vec<SinkRun> = runs
            .iter()
            .map(|r| SinkRun {
                key_words: 0,
                state_words: r.state_words,
                starts: r.starts.clone(),
                keys: Vec::new(),
                states: r.states.clone(),
                null_states: None,
                key_offs: r.key_offs.clone(),
                key_ends: r.key_ends.clone(),
                key_bytes: r.key_bytes.clone(),
                hashes: r.hashes.clone(),
                gid_gen: 0,
            })
            .collect();
        let without_gids = {
            let locals = [SinkLocalView {
                spilled: &[],
                runs: &stripped,
                remainder: Some(h.remainder_view(&part)),
            }];
            drain(&locals)
        };
        assert_eq!(with_gids, without_gids, "GID merge is byte-invisible");
        assert_eq!(with_gids.len(), 3);
        assert_eq!(with_gids[&(1, b"first-gen-a".to_vec())], 11);
        assert_eq!(with_gids[&(2, b"first-gen-b".to_vec())], 5);
        assert_eq!(with_gids[&(1, b"second-gen-a".to_vec())], 1100);
        // The cross-generation words collision stayed two distinct groups
        // with exact counts — the generation stamp did its job.
    }

    // ---- SCATTER ACCEPT (GL-RADIX-3 fold-bypass) ---------------------

    /// Staged-lane stand-in for the scatter tests.
    struct TestCols {
        vals: Vec<Vec<Datum>>,
        nulls: Vec<Vec<bool>>,
    }

    impl ::lanefold::LaneCols for TestCols {
        fn col_values(&self, c: usize) -> &[Datum] {
            &self.vals[c]
        }
        fn col_isnull(&self, c: usize) -> &[bool] {
            &self.nulls[c]
        }
    }

    /// The scatter fixture's transitions: count(*), count(col1),
    /// sum(col2 + 3) (int4 lane, addend transform), max(col3) (int4).
    fn scatter_trans() -> Vec<::lanefold::LaneTrans> {
        use ::lanefold::{FloatConv, LaneKind, LaneTrans, LaneWidth, NO_FILTER};
        let mk = |kind, col: u16, width, res_width, addend: i32, transno: u16| LaneTrans {
            kind,
            col,
            col2: col,
            width,
            res_width,
            fconv: FloatConv::None,
            fconv2: FloatConv::None,
            filter: NO_FILTER,
            addend,
            mulk: 1,
            divk: 1,
            transno,
        };
        vec![
            mk(LaneKind::CountStar, 0, LaneWidth::I64, LaneWidth::I64, 0, 0),
            mk(LaneKind::CountAny, 1, LaneWidth::I64, LaneWidth::I64, 0, 1),
            mk(LaneKind::Sum, 2, LaneWidth::I32, LaneWidth::I64, 3, 2),
            mk(LaneKind::Max, 3, LaneWidth::I32, LaneWidth::I32, 0, 3),
        ]
    }

    /// The fixture's seeded init template (`seed_new_groups` field values):
    /// counts non-null 0, sum/max NULL initvals.
    fn scatter_init() -> Vec<u64> {
        let mut init = vec![0u64; 4 * 2];
        scatter_seed_slot(&mut init, 0, Datum::from_i64(0), false);
        scatter_seed_slot(&mut init, 1, Datum::from_i64(0), false);
        scatter_seed_slot(&mut init, 2, Datum::from_i64(0), true);
        scatter_seed_slot(&mut init, 3, Datum::from_i64(0), true);
        init
    }

    fn scatter_combines() -> Vec<SinkCombineFn> {
        fn add(
            _f: Option<&mut ::types_fmgr::FmgrInfo>,
            fcinfo: &mut ::types_fmgr::FunctionCallInfoBaseData,
        ) -> PgResult<Datum> {
            let a = fcinfo.args[0].value.as_i64();
            let b = fcinfo.args[1].value.as_i64();
            Ok(Datum::from_i64(a.wrapping_add(b)))
        }
        fn larger32(
            _f: Option<&mut ::types_fmgr::FmgrInfo>,
            fcinfo: &mut ::types_fmgr::FunctionCallInfoBaseData,
        ) -> PgResult<Datum> {
            let a = fcinfo.args[0].value.as_i32();
            let b = fcinfo.args[1].value.as_i32();
            Ok(Datum::from_i32(a.max(b)))
        }
        let f = |func| SinkCombineFn {
            func,
            strict: true,
            collation: Oid::from(0u8),
            kind: SinkCombineKind::Byval,
        };
        vec![f(add), f(add), f(add), f(larger32)]
    }

    /// Row states as the OBSERVABLE pairs (isnull, value-as-i64) per
    /// transno. `no_trans_value` is deliberately outside the comparison:
    /// the combine clears it on any second arrival, so it is
    /// flush-cadence-dependent under the INCUMBENT cadences too (a group
    /// split across two cap windows vs one) — it is not part of the
    /// run/combine contract's observable state, and the one post-combine
    /// reader (topn) is excluded from scatter admission.
    fn row_obs(t: &LaneAggTable, row: usize) -> Vec<(bool, i64)> {
        let pg = t.row_states(row).cast_const().cast::<AggPerGroup>();
        (0..4)
            .map(|k| unsafe {
                let s = &*pg.add(k);
                (
                    s.trans_value_is_null,
                    if s.trans_value_is_null { 0 } else { s.trans_value.as_i64() },
                )
            })
            .collect()
    }

    /// The scatter arm's byte-identity claim, unit form (the
    /// seal_flush_run_matches_remainder_view model): scatter-built runs
    /// (single-row state blocks, duplicate keys, arbitrary flush cadence)
    /// must combine to per-bucket tables identical in row order and
    /// observable state content to the incumbent fold path (probe + seeded
    /// init + per-row transitions + cap-flush runs + SEAL remainder).
    #[test]
    fn scatter_runs_combine_matches_fold() {
        let trans = scatter_trans();
        let init = scatter_init();
        // 2000 rows over 500 int keys (α = 4 — every group merges across
        // scattered arrivals), plus NULL keys and NULL inputs.
        let n = 2000usize;
        let mut rows: Vec<u32> = Vec::new();
        let mut keys: Vec<Datum> = Vec::new();
        let mut knull: Vec<bool> = Vec::new();
        let mut cols = TestCols {
            vals: vec![vec![Datum::from_i64(0); n]; 4],
            nulls: vec![vec![false; n]; 4],
        };
        for i in 0..n {
            rows.push(i as u32);
            if i % 97 == 13 {
                keys.push(Datum::from_i64(0));
                knull.push(true);
            } else {
                keys.push(Datum::from_i64((i % 500) as i64));
                knull.push(false);
            }
            // col1: count(col) input, NULL every 7th row.
            cols.nulls[1][i] = i % 7 == 0;
            cols.vals[1][i] = Datum::from_i64(1);
            // col2: sum input (int4 lane), NULL every 11th row.
            cols.nulls[2][i] = i % 11 == 0;
            cols.vals[2][i] = Datum::from_i32((i % 61) as i32 - 30);
            // col3: max input (int4 lane), NULL every 5th row.
            cols.nulls[3][i] = i % 5 == 0;
            cols.vals[3][i] = Datum::from_i32(((i * 37) % 101) as i32 - 50);
        }

        // Incumbent: fold every row into a real table (probe + seed + the
        // same per-row transition bodies), cap-flushing at 300 entries —
        // groups straddle the flush boundary, exercising cross-run merges.
        let mut t = LaneAggTable::with_config(
            KeyRepr::Int,
            4 * core::mem::size_of::<AggPerGroup>(),
            64,
            HashKind::best(),
            EntryLayout::Inline16,
        );
        let mut fold_runs: Vec<SinkRun> = Vec::new();
        for j in 0..n {
            if t.nrows() >= 300 {
                fold_runs.push(sink_flush_table(&mut t));
            }
            let pr = if knull[j] {
                t.probe_null()
            } else {
                let k = keys[j].as_i64();
                t.probe_int(k, t.hash_key_int(k as u64))
            };
            let pgs = pr.states.cast::<AggPerGroup>();
            if pr.is_new {
                // seed_new_groups' field values (the init template).
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        init.as_ptr(),
                        pr.states.cast::<u64>(),
                        init.len(),
                    );
                }
            }
            for tr in &trans {
                // SAFETY: live 4-slot state block.
                let pg = unsafe { &mut *pgs.add(tr.transno as usize) };
                scatter_apply_row(tr, pg, &cols.vals[tr.col as usize],
                    &cols.nulls[tr.col as usize], j);
            }
        }
        let part = sink_partition_remainder(&t);
        let locals_fold = [SinkLocalView {
            spilled: &[],
            runs: &fold_runs,
            remainder: Some(SinkRemainder {
                table: &t,
                part: &part,
                canon: None,
                canon_store: None,
                gid_gen: 0,
                direct: false,
            }),
        }];

        // Scatter: same rows in batch slices, a DIFFERENT flush cadence
        // (take_run mid-stream + at the end — the cadence-freedom claim).
        let mut sc = SinkScatter::from_parts(trans.clone(), init.clone(), 8);
        let mut sc_runs: Vec<SinkRun> = Vec::new();
        for (bi, chunk) in rows.chunks(97).enumerate() {
            let lo = bi * 97;
            sc.absorb_batch(&cols, chunk, &keys[lo..lo + chunk.len()],
                &knull[lo..lo + chunk.len()]);
            if sc.nrows() >= 700 {
                sc_runs.extend(sc.take_run());
            }
        }
        sc_runs.extend(sc.take_run());
        assert!(sc_runs.len() >= 2, "cadence exercise wants multiple runs");
        assert_eq!(
            sc_runs.iter().map(SinkRun::nrows).sum::<usize>(),
            n - rows.iter().filter(|&&r| knull[r as usize]).count(),
            "every non-NULL-key row scatters exactly once"
        );
        let locals_sc =
            [SinkLocalView { spilled: &[], runs: &sc_runs, remainder: None }];

        let combines = scatter_combines();
        let state_bytes = 4 * core::mem::size_of::<AggPerGroup>();
        let mut total = 0usize;
        for b in 0..SINK_NBUCKETS {
            let ta = sink_combine_bucket(b, 1, state_bytes, &locals_fold, &combines).unwrap();
            let tb = sink_combine_bucket(b, 1, state_bytes, &locals_sc, &combines).unwrap();
            assert_eq!(ta.nrows(), tb.nrows(), "bucket {b} row count");
            total += ta.nrows();
            for row in 0..ta.nrows() {
                assert_eq!(
                    ta.row_key_int(row),
                    tb.row_key_int(row),
                    "bucket {b} row {row} key (first-seen order)"
                );
                assert_eq!(
                    row_obs(&ta, row),
                    row_obs(&tb, row),
                    "bucket {b} row {row} states"
                );
            }
        }
        assert_eq!(total, 501, "500 int groups + the NULL group");
    }

    /// The single-row block law itself: one scattered row's state block is
    /// the fold of that one row from the seeded init (count → 1, sum → the
    /// transformed value non-null, strict max → the value adopted; NULL
    /// inputs leave the seeded state).
    #[test]
    fn scatter_single_row_block_is_one_row_fold() {
        let mut sc = SinkScatter::from_parts(scatter_trans(), scatter_init(), 8);
        let cols = TestCols {
            vals: vec![
                vec![Datum::from_i64(0), Datum::from_i64(0)],
                vec![Datum::from_i64(1), Datum::from_i64(1)],
                vec![Datum::from_i32(40), Datum::from_i32(40)],
                vec![Datum::from_i32(-7), Datum::from_i32(-7)],
            ],
            // Row 1: every input NULL — the block must stay the seed
            // (counts 0, sum/max NULL).
            nulls: vec![
                vec![false, false],
                vec![false, true],
                vec![false, true],
                vec![false, true],
            ],
        };
        sc.absorb_batch(
            &cols,
            &[0, 1],
            &[Datum::from_i64(42), Datum::from_i64(43)],
            &[false, false],
        );
        let run = sc.take_run().expect("two rows buffered");
        assert_eq!(run.nrows(), 2);
        assert!(run.null_states.is_none());
        assert_eq!(run.state_words, 8);
        // Locate each key's slot (bucket-major layout).
        let slot_of = |key: u64| run.keys.iter().position(|&k| k == key).unwrap();
        let obs = |slot: usize, transno: usize| {
            let w0 = run.states[slot * 8 + transno * 2];
            let flags = run.states[slot * 8 + transno * 2 + 1];
            (flags & 0xFF != 0, w0)
        };
        let s0 = slot_of(42);
        assert_eq!(obs(s0, 0), (false, 1), "count(*) of one row");
        assert_eq!(obs(s0, 1), (false, 1), "count(col) of one non-null row");
        assert_eq!(obs(s0, 2), (false, 43), "sum: value 40 + addend 3");
        assert_eq!(
            obs(s0, 3),
            (false, Datum::from_i32(-7).as_u64()),
            "max: the value at res_width"
        );
        let s1 = slot_of(43);
        assert_eq!(obs(s1, 0), (false, 1), "count(*) counts the NULL-input row");
        assert_eq!(obs(s1, 1), (false, 0), "count(col) skips the NULL input");
        assert_eq!(obs(s1, 2), (true, 0), "sum stays the NULL seed");
        assert_eq!(obs(s1, 3), (true, 0), "max stays the NULL seed");
    }
}
