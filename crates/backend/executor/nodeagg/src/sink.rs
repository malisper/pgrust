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
/// MEASURED NO-SHIP at 100M (2026-07-14 A/B, text family q13/15/17/19,
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
    /// Bytes mode: `nrows + 1` offsets into `key_bytes`, bucket-major and
    /// contiguous (row i's canonical key = `key_bytes[key_offs[i]..
    /// key_offs[i+1]]`). Empty in word modes.
    pub key_offs: Vec<u32>,
    /// Bytes mode: canonical key bytes, COPIED at flush (the table reset
    /// frees its arena; the intern table is scan-lifetime but the run must
    /// stay self-contained — the groundwork's copy discipline).
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
            + self.key_bytes.capacity()
            + self.hashes.capacity() * 8
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

/// Extend the compact state's stored canonical hashes to cover every table
/// row (rows are append-only within an epoch; a flush resets both). Called
/// at the BATCH TAIL by the packed probes (new groups hash while their
/// text bytes are cache-warm, on the accepting worker — parallel), and
/// defensively at flush/SEAL entry (no-op when the batch tails covered
/// everything; covers the per-row test/fallback insert paths). Word shapes
/// return immediately.
pub(crate) fn compact_extend_canon_hashes(ch: &mut crate::compact::CompactHash) {
    let crate::compact::CompactHash { table, key, intern, canon_hashes, .. } = ch;
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
    let mut scratch: Vec<u8> = Vec::with_capacity(64);
    canon_hashes.reserve(n - canon_hashes.len());
    for row in canon_hashes.len()..n {
        scratch.clear();
        canon_row_bytes_append(table, shape, intern, row, &mut scratch);
        canon_hashes.push(sink_hash_bytes(&scratch));
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
///  * TWO+ Intern components (the CaseDict q40 class): each tail is
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
    sink_flush_table_canon_impl(ch, sink_gid_merge_enabled())
}

/// [`sink_flush_table_canon`] with the GID-word fill decision injected (the
/// unit tests exercise the GID lane regardless of the process env).
fn sink_flush_table_canon_impl(ch: &mut crate::compact::CompactHash, gid: bool) -> SinkRun {
    // The batch tails already hashed every row's canonical image
    // (`compact_extend_canon_hashes` — accept-time, parallel); the extend
    // here is the defensive no-op sweep for the non-batched insert paths.
    // Pass 1: materialize each row's canonical image EXACTLY ONCE into a
    // flat arrival-order scratch (image offsets recorded) and take
    // per-bucket row + byte counts off the stored hashes. Pass 2 is then a
    // plain permuting byte copy — the old shape re-ran the whole canonical
    // materialization (word unpack + intern reverse-map chase + component
    // assembly) a second time per row AND hashed at flush, which the
    // q13/q19 profiles put at ~14% of the engaged 16-thread query.
    compact_extend_canon_hashes(ch);
    let gid_gen = ch.intern_gen;
    let crate::compact::CompactHash { table, key, intern, canon_hashes, .. } = ch;
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
    let mut scratch: Vec<u8> = Vec::new();
    let mut scratch_offs: Vec<u32> = Vec::with_capacity(n + 1);
    scratch_offs.push(0);
    for i in 0..n {
        let base = scratch.len();
        canon_row_bytes_append(table, shape, intern, i, &mut scratch);
        let img = &scratch[base..];
        debug_assert_eq!(hashes[i], sink_hash_bytes(img), "stored canon hash law");
        let h = hashes[i];
        counts[bucket_of(h)] += 1;
        byte_counts[bucket_of(h)] += img.len();
        scratch_offs.push(scratch.len() as u32);
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
    let mut run_hashes: Vec<u64> = vec![0; n];
    // GID-merge car: carry each row's PACKED key words (per-worker intern
    // ids included) so the combine can merge repeat arrivals of one
    // (worker, generation, words) triple word-mode instead of re-probing
    // canonical bytes.
    let mut gid_words: Vec<u64> = if gid { vec![0; n * 2] } else { Vec::new() };
    for i in 0..n {
        let img =
            &scratch[scratch_offs[i] as usize..scratch_offs[i + 1] as usize];
        let b = bucket_of(hashes[i]);
        let slot = cursor[b] as usize;
        cursor[b] += 1;
        let off = bcursor[b];
        bcursor[b] += img.len();
        key_offs[slot] = off as u32;
        key_bytes[off..off + img.len()].copy_from_slice(img);
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
    // Offsets are consistent per slot: rows within a bucket fill both the
    // slot range and the byte range in the same order, and buckets are laid
    // out contiguously — slot s's key ends exactly where slot s+1 begins.
    debug_assert!(key_offs.windows(2).all(|w| w[0] <= w[1]));
    table.reset();
    // The epoch's rows are gone — the stored hashes restart with them.
    canon_hashes.clear();
    SinkRun {
        key_words: 0,
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
/// the q19@100M profile showed serializing a canon+hash sweep over every
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

// ---------------------------------------------------------------------------
// LIMIT-k-no-ORDER group-admission FREEZE (band-kernels-2a, ClickBench q18
// class): `GROUP BY ... LIMIT k` with NO ORDER BY needs only k groups with
// EXACT aggregates — the ratified q18 PASS-TIE membership class (count-gated:
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
/// the class only pays off for small k (q18 is LIMIT 10). Larger bounds
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
            let ks = run.key_offs[i] as usize;
            let ke = run.key_offs[i + 1] as usize;
            let states = &run.states[i * run.state_words..(i + 1) * run.state_words];
            sink_canon_spill_append(&run.key_bytes[ks..ke], run.hashes[i], states, out);
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
    /// GID-merge car (canonical shapes): the Local's CURRENT intern-table
    /// generation — remainder rows sit in the live table, so their packed
    /// words are generation-current by construction. 0 for word shapes.
    pub gid_gen: u64,
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

pub fn sink_combine_bucket(
    b: usize,
    key_words: usize,
    state_bytes: usize,
    locals: &[SinkLocalView<'_>],
    combines: &[SinkCombineFn],
) -> PgResult<LaneAggTable> {
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
) -> PgResult<LaneAggTable> {
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
                key_bytes += (r.key_offs[r.starts[b + 1] as usize]
                    - r.key_offs[r.starts[b] as usize]) as usize;
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
    // Bytes-mode probes reuse the flush/SEAL-computed sink hash (carried in
    // the run / part) instead of re-hashing every arrival's byte image —
    // probe_bytes consumes the hash's low bits (slot) and bits 32..48
    // (salt), so the sink hash's constant-per-bucket top byte never hurts,
    // and one hash per (row, table) stays consistent across all probes.
    // Returns the row's merged state block for the GID map below.
    let absorb_bytes =
        |t: &mut LaneAggTable, key: &[u8], h: u64, src: *const u64| -> PgResult<*mut u8> {
            let pr = t.probe_bytes(key, h);
            let states = pr.states;
            merge_states(pr, src)?;
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
    for l in locals {
        gmap.clear();
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
                                )?;
                            }
                            continue;
                        }
                        let ks = r.key_offs[i] as usize;
                        let ke = r.key_offs[i + 1] as usize;
                        let dst =
                            absorb_bytes(&mut t, &r.key_bytes[ks..ke], r.hashes[i], src)?;
                        gmap.insert(w, dst);
                        continue;
                    }
                    let ks = r.key_offs[i] as usize;
                    let ke = r.key_offs[i + 1] as usize;
                    absorb_bytes(&mut t, &r.key_bytes[ks..ke], r.hashes[i], src)?;
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
                                )?;
                            }
                            continue;
                        }
                        canon_row_bytes(rt, shape, intern, row as usize, &mut canon);
                        let dst =
                            absorb_bytes(&mut t, &canon, part.hashes[lo + slot], src)?;
                        gmap.insert(w, dst);
                        continue;
                    }
                    canon_row_bytes(rt, shape, intern, row as usize, &mut canon);
                    absorb_bytes(&mut t, &canon, part.hashes[lo + slot], src)?;
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
    /// An Intern (text) component of a CANONICAL bytes-keyed table: the
    /// canonical key's tail region (after the `plan.fixed` image prefix)
    /// carries the text payload(s); `nth` names which tail (ordinal among
    /// the shape's Intern components — single-tail shapes carry the raw
    /// bytes, two+ tails are length-prefixed). Materialized as a 4B-header
    /// text varlena into the buf arena (nothing worker-owned crosses to the
    /// leader).
    MultiText { nth: u8 },
    /// A packed Numeric component (the q19 `extract(minute ...)` key class):
    /// `width` bytes at byte `off` decode through the canonical keypack form
    /// (`mk_numeric_key_decode` → `numeric_key_unpack`) into a NUMERIC image
    /// in the buf arena — byte-identical to the packed first-arrival datum
    /// by the keypack canonicality gates.
    MultiNumeric { off: u8, width: u8 },
    /// A constant tlist entry (the q35 `SELECT 1, URL, ...` class): the
    /// plan's Const datum, emitted verbatim on every row. Byval-only by
    /// admission (a byref image would need per-row arena copies — refused
    /// fail-closed), so nothing worker- or query-arena-owned crosses to the
    /// leader; NULL consts ride the isnull flag.
    ConstByval { value: Datum, isnull: bool },
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
        if let Some(c) = te.expr.as_const() {
            // Const tlist entry (q35's `SELECT 1, URL, ...` class): byval
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
    Some(SinkEmitPlan { width: key.width(), cols, fixed, ntails })
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
    /// first-seen order), appending to the accumulator.
    pub fn emit_table(&mut self, plan: &SinkEmitPlan, t: &LaneAggTable) -> PgResult<()> {
        let n = t.nrows();
        self.values.reserve(n * plan.cols.len());
        self.nulls.reserve(n * plan.cols.len());
        for row in 0..n {
            emit_row(plan, t, row, &mut self.values, &mut self.nulls, &mut self.arena, &mut self.fixups)?;
        }
        self.nrows += n;
        Ok(())
    }

    /// Finalize+project ONLY `rows` of `t` (ascending, unique — the
    /// winners-only compact discipline of [`sink_emit_bucket_rows`]),
    /// appending to the accumulator. Row `rows[i]` becomes accumulator row
    /// `base + i` where `base` was `self.nrows()` before the call.
    pub fn emit_rows(&mut self, plan: &SinkEmitPlan, t: &LaneAggTable, rows: &[u32]) -> PgResult<()> {
        debug_assert!(rows.windows(2).all(|w| w[0] < w[1]), "rows sorted+unique");
        self.values.reserve(rows.len() * plan.cols.len());
        self.nulls.reserve(rows.len() * plan.cols.len());
        for &row in rows {
            emit_row(plan, t, row as usize, &mut self.values, &mut self.nulls, &mut self.arena, &mut self.fixups)?;
        }
        self.nrows += rows.len();
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
            // Plan-owned byval datum, copied verbatim (admission gate).
            SinkEmitCol::ConstByval { value, isnull } => {
                values.push(value);
                nulls.push(isnull);
            }
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
                        push_image(values, nulls, arena, fixups, img.as_bytes());
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
/// a widened engagement (≥2 Locals) or a flushed Local takes the merge arm.
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
    for &row in &part.idx[lo..hi] {
        emit_row(plan, t, row as usize, &mut values, &mut nulls, &mut arena, &mut fixups)?;
    }
    if with_null {
        // The out-of-band NULL group emits LAST in its bucket (the merge
        // arm's order: runs/remainder rows first, then the NULL absorb).
        for row in 0..t.nrows() {
            if t.row_key_int(row).is_none() {
                emit_row(plan, t, row, &mut values, &mut nulls, &mut arena, &mut fixups)?;
                break;
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
/// crossing degrades to spill epochs, not an error) — the q33@100M hmm=2
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
            // shape.
            if (shape.n_intern() >= 1) != ch.intern.is_some() {
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
    pub fn partition_remainder(&mut self) -> SinkPart {
        if compact_canon_shape(&self.0).is_some() {
            sink_partition_remainder_canon(&mut self.0)
        } else {
            sink_partition_remainder(&self.0.table)
        }
    }

    /// This handle's retained footprint (compact + intern tables + the
    /// stored canonical row hashes) — the SEAL-time budget accounting twin
    /// of [`agg_sink_table_mem`].
    pub fn mem_used(&self) -> usize {
        self.0.table.mem_used()
            + self.0.intern.as_ref().map_or(0, ::lanetable::LaneAggTable::mem_used)
            + self.0.canon_hashes.capacity() * 8
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
        SinkRemainder { table: &self.0.table, part, canon, gid_gen: self.0.intern_gen }
    }
}

/// Move the armed compact state OUT of the executor (end of a morsel drain:
/// the Local owns it until the next morsel / SEAL). `None` = not armed.
/// Mark the node's armed compact table as RUNTIME-SINK-owned (idempotent;
/// no-op when no compact table is armed). Gates the batch-tail canonical
/// hashing — the serial lane shares the compact table and must not pay for
/// hashes it never consumes.
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
/// half-limit error on wide-vocabulary scans — the q34@100M URL class).
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
    t.nrows() * (16 + 8 * table_key_words(t) + t.state_bytes())
}

/// Force-flush the armed table into a run NOW, regardless of the cap
/// (`None` = empty table, nothing to flush). The budget-pressure spill law
/// (mt16-cliffs, the q33@100M hmm=2 cliff): when half-limit pressure trips
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
// the ratified count-gated class (the q31/q32/q33 precedent).
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
        // gated by sink_emit_plan_all_byval (MultiText/MultiNumeric/Avg*
        // are byref) — fail-soft NULL rather than asserting.
        SinkEmitCol::MultiText { .. } | SinkEmitCol::MultiNumeric { .. } => (Datum::null(), true),
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
            SinkLocalView { spilled: &[], runs: core::slice::from_ref(&run1), remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None, gid_gen: 0 }) },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(SinkRemainder { table: &t2, part: &part2, canon: None, gid_gen: 0 }) },
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
            SinkLocalView { spilled: &[], runs: core::slice::from_ref(&run1), remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None, gid_gen: 0 }) },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(SinkRemainder { table: &t2, part: &part2, canon: None, gid_gen: 0 }) },
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
            ntails: 0,
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
            ntails: 0,
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
            ntails: 0,
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
            ntails: 0,
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
            width: 8,
            cols: vec![
                SinkEmitCol::Key,
                SinkEmitCol::Agg { transno: 0 },
                SinkEmitCol::Agg { transno: 1 },
            ],
        };
        let locals =
            [SinkLocalView { spilled: &[], runs: &[], remainder: Some(SinkRemainder { table: &t, part: &part, canon: None, gid_gen: 0 }) }];
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
            SinkLocalView { spilled: &[], runs: core::slice::from_ref(&run1), remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None, gid_gen: 0 }) },
            SinkLocalView { spilled: &[], runs: &[], remainder: Some(SinkRemainder { table: &t2, part: &part2, canon: None, gid_gen: 0 }) },
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
            let ks = run.key_offs[i] as usize;
            let ke = run.key_offs[i + 1] as usize;
            assert_eq!(
                run.hashes[i],
                sink_hash_bytes(&run.key_bytes[ks..ke]),
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
        let mut reference: Vec<LaneAggTable> = Vec::with_capacity(SINK_NBUCKETS);
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
                remainder: Some(SinkRemainder { table: &t1, part: &part1, canon: None, gid_gen: 0 }),
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

    /// The c3-class corpus: shared >16-byte prefixes (the ClickBench URL
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

    // -- LIMIT-k-no-ORDER group-admission freeze (band-2a q18) -------------

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

    /// int2 + TWO Intern components (the CaseDict q40 image class):
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
        let run1 = sink_flush_table_canon_impl(&mut w, true);
        bump_canon(&mut w, Some(1), b"first-gen-a", 10);
        let run2 = sink_flush_table_canon_impl(&mut w, true);
        // Simulate the wide-vocabulary intern reset (agg_sink_flush_now's
        // reset arm): ids restart, the generation bumps.
        w.intern.as_mut().unwrap().reset();
        w.intern_gen += 1;
        // Post-reset: "second-gen-a" gets intern id 0 — the SAME packed
        // words as key (1, "first-gen-a") pre-reset.
        bump_canon(&mut w, Some(1), b"second-gen-a", 100);
        let run3 = sink_flush_table_canon_impl(&mut w, true);
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

}
