//! Lane-v2 exact-DISTINCT set state — the uniqExact analog (pgrcolumnar-v2 plan
//! §2.3; both executor catalogs' set-state designs).
//!
//! One `DistinctSet` replaces the per-group TUPLESORT a non-presorted
//! DISTINCT aggregate otherwise runs (C nodeAgg's sortstates +
//! process_ordered_aggregate_single): the transition phase becomes set-insert
//! and the group finalize replays each distinct value once through the real
//! transfn. Value-identity with the C sort path holds because admission
//! (lib.rs `distinct_set_kind`) restricts to transitions that are
//! order-insensitive over a distinct-value multiset (count/sum/avg over ints,
//! count over deterministic-collation text) — the set changes only the
//! REPLAY ORDER, which those transfns cannot observe.
//!
//! Equality/hash pairing (charter: PG's own equality, equal-values-must-
//! hash-equal): admission proves the aggregate's DISTINCT equality operator
//! is *representational* equality —
//!   * int2/int4/int8: `int2eq`/`int4eq`/`int8eq` are value equality on the
//!     sign-extended word; the key stored here IS that sign-extended i64
//!     (`Datum::as_i16/as_i32/as_i64`), so set equality == PG equality and
//!     ANY deterministic hash of the key satisfies equal-hashes-equal.
//!   * text/varchar under a DETERMINISTIC collation: `texteq` is
//!     length+memcmp of the detoasted content bytes (varlena.rs `texteq`,
//!     the deterministic arm); the key here is exactly those content bytes.
//!     Nondeterministic collations (equal-but-byte-different) REFUSE at
//!     admission.
//! No numeric-style class types are admitted (numeric 1.0 == 1.00 would need
//! the type's own hash function); that is why the hash below can be a plain
//! mixer rather than the fmgr hash proc.
//!
//! The set is deliberately minimal open addressing (linear probe, pow2
//! table, entry-index slots): the C-ported tuplehash carries MinimalTuple +
//! per-entry context machinery this state does not need. A compact-set /
//! ported-tuplehash A/B is the Stage-2.2 companion measurement.
//!
//! Merge-shaped by design (Stage-4 payoff): the state is a plain value set —
//! set-union of two `DistinctSet`s over the same key kind is the natural
//! partial-aggregate merge. No parallel plumbing exists yet; nothing here
//! assumes single-threadedness except &mut.

use ::datum::Datum;
use ::mcx::Mcx;
use ::sort_storage::{LogicalTapeSet, TapeIdx};
use ::types_error::PgResult;
use ::types_tuple::varatt;

/// Admitted DISTINCT-argument representations (lib.rs `distinct_set_kind`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DistinctKeyKind {
    /// int2 argument; key = sign-extended i64 (int2eq semantics).
    Int16,
    /// int4 argument; key = sign-extended i64 (int4eq semantics).
    Int32,
    /// int8 argument; key = i64 (int8eq semantics).
    Int64,
    /// text/varchar under a deterministic collation; key = detoasted content
    /// bytes (texteq's deterministic length+memcmp arm).
    Bytes,
}

/// A stored text value: a canonical 4-byte-header varlena image in `blob`
/// (replay hands its pointer to the transfn), keyed on the content bytes.
struct BytesSpan {
    /// Offset of the varlena IMAGE (header included) in `blob`; 8-aligned.
    off: u32,
    /// Content length (bytes after the 4-byte header).
    len: u32,
    /// Saved content hash (rehash + probe prefilter).
    hash: u32,
}

/// Probe-table arm: the legacy slot->index table, or the stringhash
/// one-miss inline-key tables (crates/common/stringhash set variants). The
/// value arrays (`ints` / `blob`+`spans`) are IDENTICAL under both arms —
/// spill record formats, replay order, and the parallel export never see the
/// difference; the arm swap only replaces the probe INDEX. Kill switch
/// PGRUST_LANE_V2_STRINGHASH_SET=0/off keeps the legacy table.
///
/// SIZE GATE (train-10 near-unique-key follow-up): the stringhash tables' fixed cost —
/// a 2-3 KiB zeroed initial allocation (1<<INITIAL_DEGREE cells) per set,
/// paid again in dealloc — loses to the 256 B legacy table on the many
/// small per-group sets a grouped COUNT(DISTINCT) with high group count
/// builds (near-unique text-key class: +17% on train 10). Every set STARTS legacy and
/// promotes to the stringhash arm only when it holds
/// `stringhash_promote_len()` values (default 56 = the legacy table's first
/// grow boundary, so an ungated set never grows the legacy table): small
/// sets keep the cheap representation for their whole life, big sets
/// (the count(DISTINCT)-winner shapes) pay one trivial <=threshold-key rebuild.
/// PGRUST_LANE_V2_STRINGHASH_SET_MINLEN=0 restores the ungated train-10
/// behavior (promote on first insert) for A/B.
enum ProbeTab {
    /// Undecided (empty set) — also the replay-only state (`from_values`).
    Empty,
    Legacy(Vec<u32>),
    Int(::stringhash::IntSet),
    Bytes(::stringhash::BytesDedup),
}

fn stringhash_set_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(
            std::env::var("PGRUST_LANE_V2_STRINGHASH_SET").as_deref(),
            Ok("0") | Ok("off") | Ok("false")
        )
    })
}

/// Set size at which a legacy set promotes to the stringhash arm. Default
/// 56 = INIT_TABLE * 7/8, the legacy table's first grow point (a gated set
/// never grows legacy — it promotes instead). 0 = promote on first insert
/// (the ungated train-10 behavior).
fn stringhash_promote_len() -> usize {
    static LEN: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *LEN.get_or_init(|| {
        std::env::var("PGRUST_LANE_V2_STRINGHASH_SET_MINLEN")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(INIT_TABLE * 7 / 8)
    })
}

/// Exact-distinct hash set over one admitted key kind. Either `ints` or
/// (`blob`+`spans`) is populated, never both (the kind is fixed per
/// pertrans). `seen_null` stands in for the at-most-one NULL the C sort path
/// dedups to (two NULLs are "equal" for DISTINCT — nodeAgg.c
/// process_ordered_aggregate_single's `oldIsNull && *isNull` arm); the
/// replay passes it through the same transfn call C would.
pub(crate) struct DistinctSet<'mcx> {
    /// Probe table (see `ProbeTab`; legacy arm: slot -> entry index + 1,
    /// 0 = empty, pow2 len).
    table: ProbeTab,
    ints: Vec<i64>,
    blob: Vec<u8>,
    spans: Vec<BytesSpan>,
    pub(crate) seen_null: bool,
    /// v2 big-NDV spill (hash-partitioned flush runs onto logical tapes);
    /// `Some` once the first work_mem crossing chose the spill path. See the
    /// `SpillState` doc for the design + memory-bound argument.
    spill: Option<SpillState<'mcx>>,
}

// ===========================================================================
// v2 set spill — hash-partitioned flush runs (uniqExact big-NDV survival).
//
// Design (charter "SET SPILL" lever, radix-partitioned variant): the key
// space is partitioned by the TOP bits of the full-avalanche key hash
// (`spill_part`) into `nparts` DISJOINT partitions, one logical tape each
// (logtape.c's serial tape set: one temp file, per-tape block chains,
// blocks recycled through the freelist). Whenever the in-memory set crosses
// its work_mem budget, `spill_flush` appends every held value to its
// partition's tape and clears the set (capacities retained; a fill-level
// trigger captured at the first flush keeps the crossing check meaningful
// afterwards — capacity-based `mem_bytes` stays above budget once grown).
// The in-memory set keeps deduplicating WITHIN each flush epoch; the same
// value seen in two epochs is written twice and re-deduplicated at
// finalize.
//
// Finalize (`spill_load_partition` + lib.rs's replay): partitions are
// disjoint, so the group's distinct multiset is the disjoint union of the
// partitions' distinct sets — each partition is loaded alone into the
// (cleared) in-memory set, deduplicated there, replayed through the real
// transfn, and dropped before the next partition loads. No cross-partition
// merge exists by construction. Expected per-partition load is
// NDV/nparts; a skewed/huge partition that would itself cross the budget
// stops loading (`Ok(false)`) and the caller finishes THAT partition on a
// work_mem-bounded tuplesort (`spill_read_*` streams the tape's remaining
// raw values) — the C sort path's own spill machinery, per partition, so
// memory stays bounded for any NDV.
//
// Memory honesty: in-memory set ≤ budget + one insert; tape write buffers
// are BLCKSZ per partition, lazily allocated, and `spill_parts_for_budget`
// sizes `nparts` so they stay a small fraction of the budget (spilling is
// refused entirely below SPILL_MIN_BUDGET — the caller keeps the v1
// degrade-to-tuplesort path there, and for whatever else v2 refuses).
// Per-partition metadata is O(nparts), not O(runs): tapes are append
// streams, so flush count leaves no trace but the data itself.
//
// Value identity: exactly the v1 argument — the spill changes only the
// transfn REPLAY ORDER over the identical distinct-value multiset (dedup is
// exact: partition-local sets use the same representational-equality keys,
// and partitions are disjoint), and the admitted transitions are
// order-insensitive. NULLs never touch the tapes: `seen_null` survives
// flushes in memory and replays once, exactly as v1.
// ===========================================================================

/// Number of key-hash TOP bits consumed by partitioning at 32 partitions.
const SPILL_MAX_PARTS: usize = 32;
/// Budgets below this keep the v1 degrade path: nparts*BLCKSZ tape write
/// buffers must stay a small fraction of the budget for the spill to be
/// memory-honest.
pub(crate) const SPILL_MIN_BUDGET: usize = 128 * 1024;
const BLCKSZ: usize = 8192;

/// Partition count for `budget`: pow2, tape write buffers (nparts * BLCKSZ)
/// capped at ~1/4 of the budget, at most SPILL_MAX_PARTS.
fn spill_parts_for_budget(budget: usize) -> usize {
    let cap = (budget / (4 * BLCKSZ)).max(4);
    let mut p = 4usize;
    while p * 2 <= cap && p * 2 <= SPILL_MAX_PARTS {
        p *= 2;
    }
    p
}

struct SpillState<'mcx> {
    tapes_set: LogicalTapeSet<'mcx>,
    /// One append tape per partition; index = partition.
    tapes: Vec<TapeIdx>,
    /// In-memory fill levels captured at the first flush: once capacities
    /// have grown past the budget, `mem_bytes` can no longer signal the next
    /// crossing, so `over_budget` compares fill against these instead.
    flush_len: usize,
    flush_blob: usize,
    /// Finalize state: tapes rewound for reading (write side closed).
    reading: bool,
}

/// Partition of a full-avalanche 64-bit key hash: top log2(nparts) bits
/// (the probe uses the LOW bits via the pow2 table mask, so partition and
/// probe bits are independent).
#[inline]
fn spill_part(h: u64, nparts: usize) -> usize {
    ((h >> 32) as usize) & (nparts - 1)
}

/// splitmix64 finalizer — a full-avalanche mixer for the i64 keys. NOT PG's
/// hash function: legal because admitted equality is representational (see
/// module doc), so any deterministic hash of the canonical key satisfies
/// equal-values-hash-equal.
#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

const INIT_TABLE: usize = 64;

impl<'mcx> DistinctSet<'mcx> {
    pub(crate) fn new() -> Self {
        DistinctSet {
            table: ProbeTab::Empty,
            ints: Vec::new(),
            blob: Vec::new(),
            spans: Vec::new(),
            seen_null: false,
            spill: None,
        }
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        self.ints.len() + self.spans.len()
    }

    /// Distinct NON-NULL value count of a never-spilled set — the paremit
    /// finalization of count(DISTINCT x): n int8inc_any replays from the
    /// '0' initcond add exactly n (strict fn, by-val i64 state, no
    /// finalfn), and the at-most-one NULL (`seen_null`) strict-skips,
    /// contributing 0 — so the set IS the count (the
    /// `process_ordered_aggregates_set` distinctfin argument, applied
    /// worker-side). Spilled sets hold values on tape that `len` cannot
    /// see; the runtime distinct sink's merged sets are rebuilt in memory
    /// at combine and never spill (fail-loud here, not silently short).
    #[inline]
    pub(crate) fn value_count(&self) -> usize {
        debug_assert!(self.spill.is_none(), "value_count on a spilled set");
        self.len()
    }

    /// batch-insert lane: prefetch the probe line an `insert_i64(k)` will
    /// touch (look-ahead driver hint — no semantic effect). Promoted arm
    /// prefetches the IntSet cell; the legacy arm prefetches the slot word
    /// (its `ints` deref stays a miss — legacy tables promote at
    /// `should_promote` anyway, so the window is short-lived).
    #[inline(always)]
    pub(crate) fn prefetch_i64(&self, k: i64) {
        // Promoted arm only: pre-promotion tables (Empty/Legacy) are small
        // and cache-resident — a hint there is pure overhead.
        if let ProbeTab::Int(t) = &self.table {
            t.prefetch(k);
        }
    }

    /// Projection reserve (dedupsub I3): pre-size the int probe table for
    /// an expected final key count, replacing the doubling-rehash ladder
    /// with one jump. Arms: a PROMOTED IntSet reserves directly; a
    /// still-EMPTY set with a large-enough target installs a pre-sized
    /// IntSet arm up front (the promotion-at-56 size gate exists to spare
    /// SMALL sets the stringhash fixed cost — a target past the gate is
    /// exactly the promotion verdict, taken early with zero rebuild).
    /// Legacy-with-entries and bytes arms no-op; replay-only sets
    /// (`from_values`: Empty table, populated value arrays) are excluded by
    /// the emptiness guard. Pure geometry: set contents, insertion order,
    /// and every downstream byte are untouched by any target value.
    ///
    /// q9internals inc-1: the reserve covers the VALUE array too — `ints`
    /// is the other half of a big set's memory and was still growing
    /// through Vec's realloc-doubling ladder (~1x final bytes of memcpy
    /// per big set) after the table reserve landed. Same accounting story
    /// as the table jump (capacity is metered by the caller's window
    /// re-account); capacity is a non-surface.
    #[inline]
    pub(crate) fn reserve_projected(&mut self, target_len: usize) {
        match &mut self.table {
            ProbeTab::Int(t) => {
                t.reserve_for(target_len);
                if target_len > self.ints.capacity() {
                    self.ints.reserve(target_len - self.ints.len());
                }
            }
            ProbeTab::Empty
                if stringhash_set_enabled()
                    && self.ints.is_empty()
                    && self.spans.is_empty()
                    && target_len >= stringhash_promote_len().max(1) =>
            {
                let mut t = ::stringhash::IntSet::new();
                t.reserve_for(target_len);
                self.table = ProbeTab::Int(t);
                self.ints.reserve(target_len);
            }
            _ => {}
        }
    }

    /// Bytes the set holds (capacities — actual allocation, the conservative
    /// figure the work_mem budget check wants).
    pub(crate) fn mem_bytes(&self) -> usize {
        let tab = match &self.table {
            ProbeTab::Empty => 0,
            ProbeTab::Legacy(t) => t.capacity() * core::mem::size_of::<u32>(),
            ProbeTab::Int(t) => t.mem_bytes(),
            ProbeTab::Bytes(t) => t.mem_bytes(),
        };
        tab + self.ints.capacity() * core::mem::size_of::<i64>()
            + self.blob.capacity()
            + self.spans.capacity() * core::mem::size_of::<BytesSpan>()
    }

    /// Group-boundary reset: drop the values, keep the allocations (the next
    /// group refills a same-shaped set). Any spill state is released too —
    /// the finalize consumed it (or a rescan is abandoning it: the temp file
    /// closes; on close failure end-of-xact fd cleanup owns it, as every
    /// BufFile user).
    pub(crate) fn clear(&mut self) {
        self.reset_values();
        self.seen_null = false;
        if let Some(sp) = self.spill.take() {
            let _ = sp.tapes_set.close();
        }
    }

    /// Flush-time reset: values only — `seen_null` (never spilled) and the
    /// spill state survive; capacities are retained for the next epoch.
    /// Stringhash arms clear by CONTENTS (the value arrays), not capacity:
    /// the pooled per-group reuse (codedgroup emit) lets one big group
    /// inflate the retained table, and a capacity-bounded memset would then
    /// tax every later small group with it (the train-10 near-unique +17%).
    fn reset_values(&mut self) {
        let DistinctSet { table, ints, blob, spans, .. } = self;
        match table {
            ProbeTab::Empty => {}
            ProbeTab::Legacy(t) => t.iter_mut().for_each(|s| *s = 0),
            ProbeTab::Int(t) => t.clear_with_keys(ints),
            ProbeTab::Bytes(t) => t.clear_with_entries(
                spans.iter().map(|s| (s.hash, s.off + varatt::VARHDRSZ as u32)),
            ),
        }
        ints.clear();
        blob.clear();
        spans.clear();
    }

    /// Degrade-time reset: give the memory back (the tuplesort owns the
    /// group's values now).
    pub(crate) fn clear_shrink(&mut self) {
        self.clear();
        *self = DistinctSet::new();
    }

    /// Legacy arm: grow-if-needed, then return the probe mask. 7/8 load
    /// factor. Every set starts here (an empty table becomes legacy);
    /// promotion to the stringhash arms is the callers' size gate — at the
    /// default threshold the first grow below never fires (the set promotes
    /// at exactly the 7/8 boundary of INIT_TABLE), but a raised
    /// SET_MINLEN keeps growing legacy until the gate opens.
    #[inline]
    fn probe_ready_legacy(&mut self) -> usize {
        if matches!(self.table, ProbeTab::Empty) {
            self.table = ProbeTab::Legacy(vec![0u32; INIT_TABLE]);
        }
        let len = self.len();
        let ProbeTab::Legacy(t) = &self.table else { unreachable!("legacy arm") };
        if (len + 1) * 8 > t.len() * 7 {
            self.grow();
        }
        let ProbeTab::Legacy(t) = &self.table else { unreachable!("legacy arm") };
        t.len() - 1
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let ProbeTab::Legacy(cur) = &self.table else { unreachable!("legacy arm") };
        let new_len = cur.len() * 2;
        let mask = new_len - 1;
        let mut table = vec![0u32; new_len];
        let rehash = |table: &mut [u32], h: u64, e: u32| {
            let mut slot = (h as usize) & mask;
            while table[slot] != 0 {
                slot = (slot + 1) & mask;
            }
            table[slot] = e;
        };
        for (i, &k) in self.ints.iter().enumerate() {
            rehash(&mut table, mix64(k as u64), (i + 1) as u32);
        }
        for (i, sp) in self.spans.iter().enumerate() {
            rehash(&mut table, mix64(sp.hash as u64), (i + 1) as u32);
        }
        self.table = ProbeTab::Legacy(table);
    }

    /// Size gate (see `ProbeTab` doc): true when a legacy/empty set has
    /// reached the promotion threshold. Never true with the stringhash arms
    /// killed.
    #[inline]
    fn should_promote(&self) -> bool {
        stringhash_set_enabled() && self.len() >= stringhash_promote_len()
    }

    /// Rebuild the probe index for the held `ints` in a stringhash IntSet
    /// (one-time legacy->stringhash promotion). Pure index swap: the value
    /// array — the spill / replay / export authority — is untouched.
    #[cold]
    #[inline(never)]
    fn promote_ints(&mut self) {
        let mut t = ::stringhash::IntSet::new();
        for &k in &self.ints {
            let inserted = t.insert(k);
            debug_assert!(inserted, "value array holds distinct keys");
        }
        self.table = ProbeTab::Int(t);
    }

    /// `promote_ints`'s bytes counterpart: re-index the held spans (stored
    /// content hashes reused) in a stringhash BytesDedup. `blob` bytes and
    /// span order are untouched.
    #[cold]
    #[inline(never)]
    fn promote_bytes(&mut self) {
        let mut t = ::stringhash::BytesDedup::new();
        for sp in &self.spans {
            let at = sp.off as usize + varatt::VARHDRSZ;
            let content = &self.blob[at..at + sp.len as usize];
            let inserted = t.insert(sp.hash, content, &self.blob, at as u32);
            debug_assert!(inserted, "value array holds distinct values");
        }
        self.table = ProbeTab::Bytes(t);
    }

    /// Insert a sign-extended integer key (no-op if present).
    #[inline]
    pub(crate) fn insert_i64(&mut self, k: i64) {
        if let ProbeTab::Int(t) = &mut self.table {
            // Promoted arm hashes internally (hardware CRC); mix64 skipped.
            if t.insert(k) {
                self.ints.push(k);
            }
            return;
        }
        self.insert_i64_hashed(k, mix64(k as u64));
    }

    /// Staged batch insert (the lane drives' direct-key feed): pass 1 mixes
    /// every hash in one tight loop over the staged key lane, pass 2 probes
    /// in row order with the precomputed hash. Element-for-element identical
    /// to `insert_i64` in the same order.
    pub(crate) fn insert_i64_batch(&mut self, keys: &[i64], hashes: &mut Vec<u64>) {
        if matches!(self.table, ProbeTab::Int(_)) {
            // Promoted arm: no precomputed-hash pass (the kernel hashes
            // in-probe).
            for &k in keys {
                self.insert_i64(k);
            }
            return;
        }
        hashes.clear();
        hashes.extend(keys.iter().map(|&k| mix64(k as u64)));
        for (&k, &h) in keys.iter().zip(hashes.iter()) {
            // May promote mid-batch; the hashed path re-dispatches (the
            // precomputed hash is simply unused after promotion).
            self.insert_i64_hashed(k, h);
        }
    }

    #[inline]
    fn insert_i64_hashed(&mut self, k: i64, h: u64) {
        if !matches!(self.table, ProbeTab::Int(_)) && self.should_promote() {
            self.promote_ints();
        }
        if let ProbeTab::Int(t) = &mut self.table {
            // One-miss inline-key probe; the value array stays the spill /
            // replay / export authority, insertion order unchanged.
            if t.insert(k) {
                self.ints.push(k);
            }
            return;
        }
        let mask = self.probe_ready_legacy();
        let ProbeTab::Legacy(table) = &mut self.table else { unreachable!("legacy arm") };
        let mut slot = (h as usize) & mask;
        loop {
            match table[slot] {
                0 => {
                    self.ints.push(k);
                    table[slot] = self.ints.len() as u32;
                    return;
                }
                e => {
                    if self.ints[(e - 1) as usize] == k {
                        return;
                    }
                    slot = (slot + 1) & mask;
                }
            }
        }
    }

    /// Insert detoasted text CONTENT bytes (no-op if present). Stores a
    /// canonical 4B-header varlena image so replay can hand the transfn a
    /// live datum pointer.
    pub(crate) fn insert_bytes(&mut self, content: &[u8]) {
        let hash = ::hashfn::hash_bytes(content);
        if !matches!(self.table, ProbeTab::Bytes(_)) && self.should_promote() {
            self.promote_bytes();
        }
        if let ProbeTab::Bytes(t) = &mut self.table {
            // Prospective image layout (identical bytes to the legacy arm);
            // committed only on true.
            let pad = (8 - (self.blob.len() & 7)) & 7;
            let img_off = self.blob.len() + pad;
            let content_off = (img_off + varatt::VARHDRSZ) as u32;
            if t.insert(hash, content, &self.blob, content_off) {
                self.blob.resize(img_off, 0);
                let word =
                    varatt::set_varsize_4b_word((content.len() + varatt::VARHDRSZ) as u32);
                self.blob.extend_from_slice(&word.to_ne_bytes());
                self.blob.extend_from_slice(content);
                self.spans.push(BytesSpan {
                    off: img_off as u32,
                    len: content.len() as u32,
                    hash,
                });
            }
            return;
        }
        let mask = self.probe_ready_legacy();
        let h = mix64(hash as u64);
        let mut slot = (h as usize) & mask;
        let ProbeTab::Legacy(table) = &mut self.table else { unreachable!("legacy arm") };
        loop {
            match table[slot] {
                0 => {
                    // 8-align the image (palloc alignment; varlena header
                    // reads stay in-bounds and aligned).
                    let pad = (8 - (self.blob.len() & 7)) & 7;
                    self.blob.resize(self.blob.len() + pad, 0);
                    let off = self.blob.len();
                    let word = varatt::set_varsize_4b_word(
                        (content.len() + varatt::VARHDRSZ) as u32,
                    );
                    self.blob.extend_from_slice(&word.to_ne_bytes());
                    self.blob.extend_from_slice(content);
                    self.spans.push(BytesSpan {
                        off: off as u32,
                        len: content.len() as u32,
                        hash,
                    });
                    table[slot] = self.spans.len() as u32;
                    return;
                }
                e => {
                    let sp = &self.spans[(e - 1) as usize];
                    if sp.hash == hash
                        && sp.len as usize == content.len()
                        && &self.blob[sp.off as usize + varatt::VARHDRSZ
                            ..sp.off as usize + varatt::VARHDRSZ + sp.len as usize]
                            == content
                    {
                        return;
                    }
                    slot = (slot + 1) & mask;
                }
            }
        }
    }

    /// The distinct integer keys, insertion order (order is replay-invisible
    /// — module doc).
    #[inline]
    pub(crate) fn ints(&self) -> &[i64] {
        &self.ints
    }

    #[inline]
    pub(crate) fn n_bytes(&self) -> usize {
        self.spans.len()
    }

    /// Datum for stored text value `i`: a pointer to the canonical varlena
    /// image inside `blob`. Live until the next `insert_bytes`/`clear`.
    #[inline]
    pub(crate) fn bytes_datum(&self, i: usize) -> Datum {
        Datum::from_usize(self.blob[self.spans[i].off as usize..].as_ptr() as usize)
    }

    // ------------------------------------------------------------------
    // Parallel-partial export/import (pardistinct.rs). Plain-data views of
    // the held values, and a values-only constructor for merged results.
    // ------------------------------------------------------------------

    /// (image offset, content length, content hash) of stored value `i`.
    /// The CONTENT starts `VARHDRSZ` bytes past the image offset.
    #[inline]
    pub(crate) fn bytes_span(&self, i: usize) -> (u32, u32, u32) {
        let s = &self.spans[i];
        (s.off, s.len, s.hash)
    }

    /// Content bytes for a `bytes_span` result.
    #[inline]
    pub(crate) fn bytes_content(&self, off: u32, len: u32) -> &[u8] {
        &self.blob[off as usize + varatt::VARHDRSZ..off as usize + varatt::VARHDRSZ + len as usize]
    }

    /// Take the integer values out (the parallel union's per-partition
    /// export; the set is spent afterwards).
    #[inline]
    pub(crate) fn take_ints(&mut self) -> Vec<i64> {
        debug_assert!(self.spill.is_none());
        self.table = ProbeTab::Empty;
        core::mem::take(&mut self.ints)
    }

    /// Rebind an UNSPILLED set to another lifetime (the parallel merge
    /// builds sets on plain scoped threads, then hands them to the node's
    /// `'mcx` pertrans slots). Every field but `spill` is lifetime-free.
    pub(crate) fn unspilled_into<'b>(self) -> DistinctSet<'b> {
        debug_assert!(self.spill.is_none());
        DistinctSet {
            table: self.table,
            ints: self.ints,
            blob: self.blob,
            spans: self.spans,
            seen_null: self.seen_null,
            spill: None,
        }
    }

    /// Build a REPLAY-ONLY set from already-deduplicated values (the
    /// parallel merge's output). The probe table is left empty — inserting
    /// into such a set would break dedup; the finalize replay only reads
    /// `ints()` / `bytes_datum()` / `seen_null`. `spans` are
    /// (content offset in `content_blob`, content length, content hash).
    pub(crate) fn from_values(
        kind: DistinctKeyKind,
        ints: Vec<i64>,
        content_blob: Vec<u8>,
        spans: Vec<(u32, u32, u32)>,
        seen_null: bool,
    ) -> DistinctSet<'mcx> {
        let mut set: DistinctSet<'mcx> = DistinctSet::new();
        set.seen_null = seen_null;
        match kind {
            DistinctKeyKind::Bytes => {
                debug_assert!(ints.is_empty());
                // Rebuild canonical 4B-header images (replay hands live
                // varlena pointers to the transfn).
                let mut blob =
                    Vec::with_capacity(content_blob.len() + spans.len() * (varatt::VARHDRSZ + 8));
                let mut out_spans = Vec::with_capacity(spans.len());
                for &(off, len, hash) in &spans {
                    let pad = (8 - (blob.len() & 7)) & 7;
                    blob.resize(blob.len() + pad, 0);
                    let img_off = blob.len();
                    let word =
                        varatt::set_varsize_4b_word((len as usize + varatt::VARHDRSZ) as u32);
                    blob.extend_from_slice(&word.to_ne_bytes());
                    blob.extend_from_slice(
                        &content_blob[off as usize..off as usize + len as usize],
                    );
                    out_spans.push(BytesSpan { off: img_off as u32, len, hash });
                }
                set.blob = blob;
                set.spans = out_spans;
            }
            _ => {
                debug_assert!(spans.is_empty());
                set.ints = ints;
            }
        }
        set
    }

    // ------------------------------------------------------------------
    // v2 spill (section doc above `SpillState`).
    // ------------------------------------------------------------------

    #[inline]
    pub(crate) fn spilled(&self) -> bool {
        self.spill.is_some()
    }

    /// The budget-crossing check. Pre-spill it is the v1 capacity check;
    /// once spilled, capacities stay above the budget forever, so the
    /// fill levels captured at the first flush signal the next epoch's
    /// crossing instead.
    #[inline]
    pub(crate) fn over_budget(&self, budget: usize) -> bool {
        match &self.spill {
            None => self.mem_bytes() > budget,
            Some(sp) => {
                self.len() >= sp.flush_len
                    || (sp.flush_blob > 0 && self.blob.len() >= sp.flush_blob)
            }
        }
    }

    /// Append every held value to its partition's tape and clear the values
    /// (capacities and `seen_null` retained). First call creates the tape
    /// set; `budget` fixes the partition count then.
    pub(crate) fn spill_flush(
        &mut self,
        kind: DistinctKeyKind,
        budget: usize,
        mcx: Mcx<'mcx>,
    ) -> PgResult<()> {
        if self.spill.is_none() {
            let mut tapes_set = LogicalTapeSet::create(mcx, false)?;
            let nparts = spill_parts_for_budget(budget);
            let tapes = (0..nparts).map(|_| tapes_set.create_tape()).collect();
            self.spill = Some(SpillState {
                tapes_set,
                tapes,
                flush_len: self.len().max(1),
                flush_blob: self.blob.len(),
                reading: false,
            });
        }
        {
            let DistinctSet { spill, ints, spans, blob, .. } = self;
            let sp = spill.as_mut().expect("armed above");
            debug_assert!(!sp.reading);
            let nparts = sp.tapes.len();
            match kind {
                DistinctKeyKind::Int16 | DistinctKeyKind::Int32 | DistinctKeyKind::Int64 => {
                    for &k in ints.iter() {
                        let p = spill_part(mix64(k as u64), nparts);
                        sp.tapes_set.write(sp.tapes[p], &k.to_ne_bytes())?;
                    }
                }
                DistinctKeyKind::Bytes => {
                    for s in spans.iter() {
                        // Record = u32 content length + content bytes; the
                        // partition reuses the stored content hash (any
                        // deterministic function of the content works).
                        let p = spill_part(mix64(s.hash as u64), nparts);
                        sp.tapes_set.write(sp.tapes[p], &s.len.to_ne_bytes())?;
                        let at = s.off as usize + varatt::VARHDRSZ;
                        sp.tapes_set.write(sp.tapes[p], &blob[at..at + s.len as usize])?;
                    }
                }
            }
        }
        self.reset_values();
        Ok(())
    }

    #[inline]
    pub(crate) fn spill_nparts(&self) -> usize {
        self.spill.as_ref().map_or(0, |sp| sp.tapes.len())
    }

    /// Finalize step 1: flush the residual epoch (uniform per-partition
    /// handling) and rewind every tape for reading.
    pub(crate) fn spill_finish_writes(
        &mut self,
        kind: DistinctKeyKind,
        budget: usize,
        mcx: Mcx<'mcx>,
    ) -> PgResult<()> {
        debug_assert!(self.spilled());
        self.spill_flush(kind, budget, mcx)?;
        // Drop the build-phase capacities (they crossed the budget by
        // definition): the per-partition loads regrow to partition size, and
        // `mem_bytes` — capacity-based — must meter THAT, not the build peak.
        self.table = ProbeTab::Empty;
        self.ints = Vec::new();
        self.blob = Vec::new();
        self.spans = Vec::new();
        let sp = self.spill.as_mut().expect("spilled");
        for i in 0..sp.tapes.len() {
            sp.tapes_set.rewind_for_read(sp.tapes[i], BLCKSZ)?;
        }
        sp.reading = true;
        Ok(())
    }

    /// Finalize step 2, per partition: load partition `p`'s values into the
    /// cleared set (exact dedup — flush epochs may have written a value more
    /// than once), stopping if the load itself crosses `budget`.
    /// `Ok(true)` = complete: the set holds exactly partition `p`'s distinct
    /// values (tape closed). `Ok(false)` = the partition alone exceeds the
    /// budget: the set holds a deduplicated prefix and the caller must
    /// stream the remainder through `spill_read_ints`/`spill_read_bytes`
    /// into a work_mem-bounded tuplesort.
    pub(crate) fn spill_load_partition(
        &mut self,
        kind: DistinctKeyKind,
        p: usize,
        budget: usize,
    ) -> PgResult<bool> {
        debug_assert!(self.spill.as_ref().is_some_and(|sp| sp.reading));
        self.reset_values();
        match kind {
            DistinctKeyKind::Int16 | DistinctKeyKind::Int32 | DistinctKeyKind::Int64 => {
                let mut buf = [0u8; 4096];
                loop {
                    let n = {
                        let sp = self.spill.as_mut().expect("spilled");
                        sp.tapes_set.read(sp.tapes[p], &mut buf)?
                    };
                    if n == 0 {
                        break;
                    }
                    debug_assert_eq!(n % 8, 0, "int spill tape holds whole i64 records");
                    for c in buf[..n].chunks_exact(8) {
                        self.insert_i64(i64::from_ne_bytes(c.try_into().unwrap()));
                    }
                    if self.mem_bytes() > budget {
                        return Ok(false);
                    }
                }
            }
            DistinctKeyKind::Bytes => {
                let mut rec: Vec<u8> = Vec::new();
                loop {
                    let more = {
                        let sp = self.spill.as_mut().expect("spilled");
                        read_bytes_record(sp, p, &mut rec)?
                    };
                    if !more {
                        break;
                    }
                    self.insert_bytes(&rec);
                    if self.mem_bytes() > budget {
                        return Ok(false);
                    }
                }
            }
        }
        let sp = self.spill.as_mut().expect("spilled");
        sp.tapes_set.close_tape(sp.tapes[p]);
        Ok(true)
    }

    /// Stream raw i64 values remaining on partition `p`'s tape after a
    /// partial `spill_load_partition` (values may repeat across epochs; the
    /// consumer dedups). Appends up to one chunk to `out`; `Ok(false)` = tape
    /// exhausted and closed.
    pub(crate) fn spill_read_ints(&mut self, p: usize, out: &mut Vec<i64>) -> PgResult<bool> {
        let sp = self.spill.as_mut().expect("spilled");
        let mut buf = [0u8; 4096];
        let n = sp.tapes_set.read(sp.tapes[p], &mut buf)?;
        if n == 0 {
            sp.tapes_set.close_tape(sp.tapes[p]);
            return Ok(false);
        }
        debug_assert_eq!(n % 8, 0, "int spill tape holds whole i64 records");
        out.extend(buf[..n].chunks_exact(8).map(|c| i64::from_ne_bytes(c.try_into().unwrap())));
        Ok(true)
    }

    /// One raw bytes record remaining on partition `p`'s tape after a
    /// partial load; `Ok(false)` = tape exhausted and closed.
    pub(crate) fn spill_read_bytes(&mut self, p: usize, out: &mut Vec<u8>) -> PgResult<bool> {
        let sp = self.spill.as_mut().expect("spilled");
        let more = read_bytes_record(sp, p, out)?;
        if !more {
            sp.tapes_set.close_tape(sp.tapes[p]);
        }
        Ok(more)
    }

    /// Finalize complete: release the spill (temp file closes).
    pub(crate) fn spill_end(&mut self) -> PgResult<()> {
        if let Some(sp) = self.spill.take() {
            sp.tapes_set.close()?;
        }
        Ok(())
    }
}

/// Build a canonical 4B-header varlena image of `content` into `img` (u32
/// backing — text's 4-byte typalign) and return its by-ref datum (live until
/// `img` is next touched).
pub(crate) fn varlena_image(content: &[u8], img: &mut Vec<u32>) -> Datum {
    let total = varatt::VARHDRSZ + content.len();
    img.clear();
    img.resize(total.div_ceil(4), 0);
    img[0] = varatt::set_varsize_4b_word(total as u32);
    // SAFETY: img holds ceil(total/4) u32s ≥ total bytes past the header.
    unsafe {
        core::ptr::copy_nonoverlapping(
            content.as_ptr(),
            (img.as_mut_ptr() as *mut u8).add(varatt::VARHDRSZ),
            content.len(),
        );
    }
    Datum::from_usize(img.as_ptr() as usize)
}

/// Read one (u32 len, content) record off partition `p`'s tape into `out`
/// (cleared); false = EOF.
fn read_bytes_record(sp: &mut SpillState<'_>, p: usize, out: &mut Vec<u8>) -> PgResult<bool> {
    let mut lenbuf = [0u8; 4];
    let n = sp.tapes_set.read(sp.tapes[p], &mut lenbuf)?;
    if n == 0 {
        return Ok(false);
    }
    debug_assert_eq!(n, 4, "bytes spill tape holds whole records");
    let len = u32::from_ne_bytes(lenbuf) as usize;
    out.clear();
    out.resize(len, 0);
    if len > 0 {
        let got = sp.tapes_set.read(sp.tapes[p], out)?;
        debug_assert_eq!(got, len, "bytes spill tape holds whole records");
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_dedup_and_growth() {
        let mut s = DistinctSet::new();
        for round in 0..3 {
            for i in 0..10_000i64 {
                s.insert_i64(i * 7 - 5_000);
            }
            assert_eq!(s.len(), 10_000, "round {round}");
        }
        s.insert_i64(i64::MIN);
        s.insert_i64(i64::MAX);
        s.insert_i64(0);
        assert_eq!(s.len(), 10_003);
        s.clear();
        assert_eq!(s.len(), 0);
        assert!(!s.seen_null);
        s.insert_i64(42);
        assert_eq!(s.ints(), &[42]);
    }

    #[test]
    fn bytes_dedup_and_images() {
        let mut s = DistinctSet::new();
        for round in 0..2 {
            for i in 0..1_000u32 {
                s.insert_bytes(format!("value-{i}").as_bytes());
            }
            assert_eq!(s.len(), 1_000, "round {round}");
        }
        s.insert_bytes(b"");
        assert_eq!(s.len(), 1_001);
        // Every stored image is a valid 4B varlena whose content round-trips.
        for i in 0..s.n_bytes() {
            let d = s.bytes_datum(i);
            let p = d.as_usize() as *const u8;
            // SAFETY: bytes_datum points at a canonical in-blob image.
            unsafe {
                assert!(!varatt::varatt_is_1b(p));
                let n = varatt::varsize_4b(p) - varatt::VARHDRSZ;
                let content = core::slice::from_raw_parts(p.add(varatt::VARHDRSZ), n);
                if n == 0 {
                    assert_eq!(content, b"");
                } else {
                    assert!(content.starts_with(b"value-"));
                }
            }
        }
        assert!(s.mem_bytes() > 1_000 * 8);
    }

    #[test]
    fn promotion_boundary_dedups_across_arms() {
        // Walk the set size across the default promotion threshold (56):
        // values inserted while legacy must stay deduplicated after the
        // stringhash promotion, and vice versa, for both key kinds.
        let boundary = super::stringhash_promote_len().max(1);
        let n = boundary * 2 + 3;

        let mut s = DistinctSet::new();
        for i in 0..n as i64 {
            s.insert_i64(i);
            s.insert_i64(i); // immediate dup
        }
        for i in 0..n as i64 {
            s.insert_i64(i); // post-promotion re-insert of legacy-era keys
        }
        s.insert_i64(0); // has_zero arm
        assert_eq!(s.len(), n);
        // Value array order is insertion order regardless of arm.
        assert_eq!(s.ints()[0], 0);
        assert_eq!(s.ints()[n - 1], (n - 1) as i64);

        let mut b = DistinctSet::new();
        for i in 0..n {
            b.insert_bytes(format!("k-{i}").as_bytes());
            b.insert_bytes(format!("k-{i}").as_bytes());
        }
        for i in 0..n {
            b.insert_bytes(format!("k-{i}").as_bytes());
        }
        assert_eq!(b.len(), n);
        // Clear keeps the promoted arm usable and empty.
        b.clear();
        assert_eq!(b.len(), 0);
        b.insert_bytes(b"fresh");
        assert_eq!(b.len(), 1);
    }

    /// dedupsub I3: reserve_projected arms — fresh-empty installs a
    /// pre-sized Int arm past the promote gate, small targets stay on the
    /// legacy ladder, replay-only sets are untouched, dedup exact always.
    #[test]
    fn reserve_projected_arms_and_guards() {
        // Fresh set, big target: pre-sized Int arm, no legacy phase.
        let mut s = DistinctSet::new();
        s.reserve_projected(50_000);
        // q9internals inc-1: the value array is pre-sized too — no realloc
        // ladder while filling to the target (geometry pin: capacity holds
        // the target and the fill never moves the buffer).
        assert!(s.ints.capacity() >= 50_000);
        let base_ptr = s.ints.as_ptr();
        for i in 0..50_000i64 {
            s.insert_i64(i * 7);
            s.insert_i64(i * 7); // immediate dup
        }
        assert_eq!(s.len(), 50_000);
        assert_eq!(s.ints()[0], 0);
        assert_eq!(s.ints.as_ptr(), base_ptr, "value array reallocated despite reserve");
        // Small target below the promote gate: table decision deferred
        // (legacy on first insert), dedup unchanged.
        let mut t = DistinctSet::new();
        t.reserve_projected(4);
        for _ in 0..3 {
            t.insert_i64(1);
            t.insert_i64(2);
        }
        assert_eq!(t.len(), 2);
        // Promoted set: reserve is a plain table jump; values keep order.
        let mut p = DistinctSet::new();
        for i in 0..200i64 {
            p.insert_i64(i);
        }
        p.reserve_projected(30_000);
        assert!(p.ints.capacity() >= 30_000, "promoted-arm reserve covers the value array");
        for i in 0..200i64 {
            p.insert_i64(i); // still dups
        }
        assert_eq!(p.len(), 200);
        assert_eq!(p.ints()[199], 199);
        // Replay-only set (from_values): the emptiness guard leaves the
        // probe table Empty (inserting into such a set is forbidden anyway).
        let mut r = DistinctSet::from_values(
            DistinctKeyKind::Int64,
            vec![1, 2, 3],
            Vec::new(),
            Vec::new(),
            false,
        );
        r.reserve_projected(100_000);
        assert!(matches!(r.table, ProbeTab::Empty));
        assert_eq!(r.ints(), &[1, 2, 3]);
    }

    #[test]
    fn hash_collision_still_compares_bytes() {
        // Same length, different content: even if the 32-bit hashes ever
        // collided, the memcmp arm keeps them distinct.
        let mut s = DistinctSet::new();
        s.insert_bytes(b"abcd");
        s.insert_bytes(b"abce");
        s.insert_bytes(b"abcd");
        assert_eq!(s.len(), 2);
    }
}
