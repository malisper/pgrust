//! Lane-v2 parallel exact-DISTINCT partials (lane-v2-pardistinct).
//!
//! The planner can never emit a Partial Agg for a DISTINCT aggregate
//! (prepagg hasNonPartialAggs), so the parallel plan for the ClickBench
//! Q5/Q6/Q9/Q10 shapes is always `Agg ← GatherMerge ← Sort ←
//! ParallelSeqScan`: workers sort ALL rows (group prefix + distinct-arg
//! suffix) and the leader deduplicates serially. This module supplies the
//! winning algorithm instead: the LEADER (which owns the Agg node and its
//! admission proofs) registers a build spec keyed by the Sort plan node's
//! address; each WORKER whose fragment top is that Sort skips the sort
//! entirely and drains its share of the shared claim cursor into a compact
//! group table — integer group-key words, a fixed per-transition vocabulary
//! of exact-integer partial states, and one exact-DISTINCT set
//! (`distinctset::DistinctSet`, reused wholesale) per distinct transition —
//! then installs the frozen table through a merge.rs-style handoff and
//! emits ZERO rows. The leader builds its own partial over the local
//! fragment (leader participation), folds any stray rows arriving through
//! the tuple queues (degraded/refused workers), merges the tables
//! (per-partition set union — partitions are disjoint by construction, so
//! the union has no cross-partition work), and emits through the serial
//! arms' unchanged finalize tails.
//!
//! Byte identity: the arm changes (a) which thread deduplicates — sets are
//! exact and their equality is representational (`distinct_set_kind`), (b)
//! the transfn REPLAY ORDER over the identical distinct-value multiset —
//! the admitted transitions are order-insensitive-exact, and (c) the
//! association order of the non-distinct vocabulary states — pure counting
//! and exact integer accumulation, reassociation unobservable. Groups,
//! group order (the plan Sort's prefix), and every projected byte match the
//! serial hashgrouped arm's identity argument.
//!
//! Memory: each worker meters its table (like the hashgrouped arm) and on
//! crossing FREEZES it (within budget), installs it, and degrades the
//! REMAINDER of its share to the classic path — the plan's real Sort is fed
//! the remaining rows and the worker emits them as ordinary sorted rows
//! (pre-freeze rows ride the frozen table, post-freeze rows ride the queue;
//! disjoint, exact). The leader's builder is mcx-backed: crossing evicts
//! the largest sets to the DistinctSet hash-partitioned spill tapes, so
//! leader memory stays budget-bounded and spilled sets replay through the
//! existing spilled-set machinery at finalize.

use std::sync::{Arc, Mutex, Weak};

use ::datum::Datum;
use ::executils::{EStateData, EcxtId, ExecSlotId};
use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult};

use crate::distinctset::{DistinctKeyKind, DistinctSet};

/// splitmix64 finalizer (distinctset.rs's mixer — legal for the same
/// representational-equality reason).
#[inline]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Integer width of a group key / vocab argument column.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PdInt {
    I16,
    I32,
    I64,
}

impl PdInt {
    #[inline]
    pub(crate) fn read(self, d: Datum) -> i64 {
        match self {
            PdInt::I16 => d.as_i16() as i64,
            PdInt::I32 => d.as_i32() as i64,
            PdInt::I64 => d.as_i64(),
        }
    }
}

/// One NON-distinct transition in the worker vocabulary. Every vocab state
/// is two i64 words: (acc, count). The kinds mirror the
/// `order_insensitive_exact_transfn` whitelist minus the Int128 family.
#[derive(Clone, Copy, Debug)]
pub enum PdVocabKind {
    /// count(*) — int8inc: acc = row count.
    CountStar,
    /// count(x) — int8inc_any (strict): acc = non-null count.
    CountAny { att: u16 },
    /// sum(int2/int4) — int2_sum/int4_sum: acc = sum, count = non-null
    /// count (state NULL iff count == 0, the non-strict null-initval law).
    SumInt { att: u16, kind: PdInt },
    /// avg(int2/int4) — int2/4_avg_accum: (acc, count) = Int8TransTypeData
    /// {sum, count} with initcond {0,0} (never NULL).
    AvgInt { att: u16, kind: PdInt },
}

/// A vocab entry is keyed by its transno (the pergroup slot the leader
/// rebuilds at emit).
#[derive(Clone, Copy, Debug)]
pub struct PdVocab {
    pub transno: u32,
    pub kind: PdVocabKind,
}

/// One DISTINCT transition: indexed like `pertrans_sort` (the leader
/// installs merged sets back into those slots at emit).
#[derive(Clone, Copy, Debug)]
pub struct PdSetSpec {
    pub(crate) att: u16,
    pub(crate) kind: DistinctKeyKind,
}

/// Element partitions for the plain (nkeys == 0) shape's parallel union.
pub const PD_ELEM_PARTS: usize = 64;
/// Group partitions (top-8 hash bits) for the grouped parallel merge.
const PD_GROUP_PARTS: usize = 256;

/// The leader-derived build recipe workers run. Everything is plain data.
pub struct PdSpec {
    pub key_atts: Vec<u16>,
    pub key_kinds: Vec<PdInt>,
    pub vocab: Vec<PdVocab>,
    pub sets: Vec<PdSetSpec>,
    /// 1 + the largest referenced 0-based attno (slot_getsomeattrs bound).
    pub max_att: i32,
    /// Per-worker build budget (freeze-and-degrade crossing point).
    pub worker_budget: usize,
}

impl PdSpec {
    #[inline]
    pub fn nkeys(&self) -> usize {
        self.key_atts.len()
    }

    /// Any bytes-kind set (the worker feed then resets its detoast scratch
    /// context per row).
    #[inline]
    pub fn any_bytes_set(&self) -> bool {
        self.sets.iter().any(|s| matches!(s.kind, DistinctKeyKind::Bytes))
    }
}

// ===========================================================================
// Handoff registry — merge.rs's pattern, keyed by the SORT plan node's
// address (unique per live plan; worker pstmts share the leader's plan tree
// by reference).
// ===========================================================================

pub struct PdHandoff {
    pub spec: Arc<PdSpec>,
    slots: Mutex<Vec<PdHandedTable>>,
    /// The leader consumed this handoff (one drive per registration). A
    /// RESCAN relaunches workers against the ORIGINAL ParallelExecShared
    /// registry snapshot, whose Arc keeps this object alive — the flag
    /// makes those workers refuse (classic sorted rows, which the rescan's
    /// fresh leader drive folds; correct, merely unaccelerated).
    spent: core::sync::atomic::AtomicBool,
}

impl PdHandoff {
    pub fn new(spec: Arc<PdSpec>) -> PdHandoff {
        PdHandoff {
            spec,
            slots: Mutex::new(Vec::new()),
            spent: core::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn install(&self, t: PdHandedTable) {
        self.slots.lock().unwrap_or_else(|e| e.into_inner()).push(t);
    }

    pub fn take_all(&self) -> Vec<PdHandedTable> {
        self.spent.store(true, core::sync::atomic::Ordering::Release);
        core::mem::take(&mut *self.slots.lock().unwrap_or_else(|e| e.into_inner()))
    }

    pub fn is_spent(&self) -> bool {
        self.spent.load(core::sync::atomic::Ordering::Acquire)
    }
}

std::thread_local! {
    static PD_REGISTRY: core::cell::RefCell<Vec<(usize, Weak<PdHandoff>)>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

pub fn pd_registry_insert(key: usize, h: &Arc<PdHandoff>) {
    PD_REGISTRY.with(|r| {
        let mut v = r.borrow_mut();
        v.retain(|(_, w)| w.strong_count() > 0);
        v.push((key, Arc::downgrade(h)));
    });
}

pub fn pd_registry_remove(key: usize) {
    let _ = PD_REGISTRY.try_with(|r| r.borrow_mut().retain(|(k, _)| *k != key));
}

pub fn pd_registry_get(key: usize) -> Option<Arc<PdHandoff>> {
    PD_REGISTRY.with(|r| {
        r.borrow().iter().find_map(|(k, w)| (*k == key).then(|| w.upgrade()).flatten())
    })
}

/// True iff this thread's registry has ANY live entry (the worker hook's
/// cheap first gate — serial sessions never allocate past this).
pub fn pd_registry_nonempty() -> bool {
    PD_REGISTRY
        .try_with(|r| r.borrow().iter().any(|(_, w)| w.strong_count() > 0))
        .unwrap_or(false)
}

/// Leader-side snapshot for execParallel (execparallel.rs carries it in
/// ParallelExecShared next to the agg handoff export).
pub struct PdExport(Vec<(usize, Arc<PdHandoff>)>);

pub fn pd_export_registry() -> PdExport {
    PdExport(PD_REGISTRY.with(|r| {
        r.borrow().iter().filter_map(|(k, w)| w.upgrade().map(|a| (*k, a))).collect()
    }))
}

pub fn pd_adopt_registry(export: &PdExport) {
    PD_REGISTRY.with(|r| {
        let mut v = r.borrow_mut();
        for (k, a) in &export.0 {
            v.push((*k, Arc::downgrade(a)));
        }
    });
}

pub fn pd_clear_thread_registry() {
    let _ = PD_REGISTRY.try_with(|r| r.borrow_mut().clear());
}

// ===========================================================================
// The builder — one participant's partial table.
// ===========================================================================

const INIT_TABLE: usize = 64;
/// Fixed per-group overhead estimate (table slot, hash, vec headers).
const GROUP_FIXED_COST: usize = 48;

/// Feed verdict: `Crossed` = the shared budget crossed AFTER this row was
/// fully absorbed — a worker freezes + degrades; the leader evicts sets.
#[derive(PartialEq, Eq)]
pub enum PdFeed {
    Ok,
    Crossed,
}

pub struct PdBuilder<'mcx> {
    spec: Arc<PdSpec>,
    /// Open addressing: slot -> group index + 1; 0 = empty. Pow2 len.
    table: Vec<u32>,
    hashes: Vec<u64>,
    /// Group g's key words at [g*nkeys ..] (sign-extended).
    keys: Vec<i64>,
    keynulls: Vec<u32>,
    /// Group g's vocab state at [g*2*nvocab ..]: (acc, count) pairs.
    states: Vec<i64>,
    /// Group g's sets at [g*nsets ..].
    dsets: Vec<DistinctSet<'mcx>>,
    /// Per-group cached set memory (delta accounting like hashgrouped).
    set_mem: Vec<usize>,
    base_mem: usize,
    total_set_mem: usize,
    budget: usize,
    /// The leader's spill context: `Some` = crossing evicts the largest
    /// sets to tapes instead of freezing (workers pass `None`).
    mcx: Option<Mcx<'mcx>>,
    /// Any set spilled (parallel fast-path refusal; leader only).
    pub ever_spilled: bool,
    /// Post-eviction high-water: capacities are retained by the set spill
    /// flushes, so `mem()` cannot drop below what the first crossing left;
    /// re-evict only once memory GROWS past this (epoch cadence).
    evict_floor: usize,
    frozen: bool,
}

impl<'mcx> PdBuilder<'mcx> {
    pub fn new(spec: Arc<PdSpec>, budget: usize, mcx: Option<Mcx<'mcx>>) -> Self {
        PdBuilder {
            spec,
            table: vec![0u32; INIT_TABLE],
            hashes: Vec::new(),
            keys: Vec::new(),
            keynulls: Vec::new(),
            states: Vec::new(),
            dsets: Vec::new(),
            set_mem: Vec::new(),
            base_mem: INIT_TABLE * 4,
            total_set_mem: 0,
            budget,
            mcx,
            ever_spilled: false,
            evict_floor: 0,
            frozen: false,
        }
    }

    #[inline]
    pub fn ngroups(&self) -> usize {
        self.hashes.len()
    }

    #[inline]
    fn mem(&self) -> usize {
        self.base_mem + self.total_set_mem
    }

    pub fn mem_bytes(&self) -> usize {
        self.mem()
    }

    #[cold]
    #[inline(never)]
    fn grow(&mut self) {
        let new_len = self.table.len() * 2;
        self.base_mem += (new_len - self.table.len()) * 4;
        let mask = new_len - 1;
        let mut table = vec![0u32; new_len];
        for (g, &h) in self.hashes.iter().enumerate() {
            let mut slot = (h as usize) & mask;
            while table[slot] != 0 {
                slot = (slot + 1) & mask;
            }
            table[slot] = (g + 1) as u32;
        }
        self.table = table;
    }

    fn probe(&self, words: &[i64], nulls: u32, h: u64) -> (Option<u32>, usize) {
        let nkeys = self.spec.nkeys();
        let mask = self.table.len() - 1;
        let mut slot = (h as usize) & mask;
        loop {
            match self.table[slot] {
                0 => return (None, slot),
                e => {
                    let g = (e - 1) as usize;
                    if self.hashes[g] == h
                        && self.keynulls[g] == nulls
                        && &self.keys[g * nkeys..(g + 1) * nkeys] == words
                    {
                        return (Some(e - 1), slot);
                    }
                    slot = (slot + 1) & mask;
                }
            }
        }
    }

    fn create_group(&mut self, words: &[i64], nulls: u32, h: u64, slot_idx: usize) -> u32 {
        let g = self.ngroups() as u32;
        self.hashes.push(h);
        self.keynulls.push(nulls);
        self.keys.extend_from_slice(words);
        self.states.extend(core::iter::repeat(0i64).take(2 * self.spec.vocab.len()));
        for _ in 0..self.spec.sets.len() {
            self.dsets.push(DistinctSet::new());
        }
        self.set_mem.push(0);
        self.table[slot_idx] = g + 1;
        self.base_mem += self.spec.nkeys() * 8
            + 2 * self.spec.vocab.len() * 8
            + self.spec.sets.len() * core::mem::size_of::<DistinctSet<'_>>()
            + GROUP_FIXED_COST;
        if (self.ngroups() + 1) * 8 > self.table.len() * 7 {
            self.grow();
        }
        g
    }

    /// Feed one row from the (deformed) scan slot. `tmp` is a per-row-reset
    /// expr context whose per-tuple memory absorbs text detoast copies (the
    /// set retains its own canonical image — collect_distinct_set's law).
    pub fn accept(
        &mut self,
        estate: &mut EStateData<'mcx>,
        id: ExecSlotId,
        tmp: EcxtId,
    ) -> PgResult<PdFeed> {
        debug_assert!(!self.frozen);
        let mut words = [0i64; 32];
        let nkeys = self.spec.nkeys();
        // NO per-row Arc clone of the spec: every participant's builder holds
        // the SAME Arc<PdSpec> allocation, so a clone+drop here is two
        // contended refcount RMWs per row on one shared cache line across all
        // workers (the __aarch64_ldadd8_relax/_rel flat-profile signature).
        // Disjoint field borrows below make the clone unnecessary.
        let max_att = self.spec.max_att;
        let slot = estate.slot_mut(id);
        exectuples::slot_getsomeattrs(slot, max_att);
        let g = {
            let base = slot.base();
            let mut nulls = 0u32;
            for (i, (&att, &kind)) in
                self.spec.key_atts.iter().zip(self.spec.key_kinds.iter()).enumerate()
            {
                if base.tts_isnull[att as usize] {
                    nulls |= 1 << i;
                    words[i] = 0;
                } else {
                    words[i] = kind.read(base.tts_values[att as usize]);
                }
            }
            let h = key_hash(&words[..nkeys], nulls);
            let (found, slot_idx) = self.probe(&words[..nkeys], nulls, h);
            let g = match found {
                Some(g) => g,
                None => self.create_group(&words[..nkeys], nulls, h, slot_idx),
            };
            let gi = g as usize;
            // Vocab transitions (spec/states are disjoint fields).
            let spec = &self.spec;
            let st = &mut self.states[gi * 2 * spec.vocab.len()..];
            for (vi, v) in spec.vocab.iter().enumerate() {
                let (acc, cnt) = (2 * vi, 2 * vi + 1);
                match v.kind {
                    PdVocabKind::CountStar => st[acc] += 1,
                    PdVocabKind::CountAny { att } => {
                        if !base.tts_isnull[att as usize] {
                            st[acc] += 1;
                        }
                    }
                    PdVocabKind::SumInt { att, kind } | PdVocabKind::AvgInt { att, kind } => {
                        if !base.tts_isnull[att as usize] {
                            st[acc] += kind.read(base.tts_values[att as usize]);
                            st[cnt] += 1;
                        }
                    }
                }
            }
            gi
        };
        // Distinct-set collects (after the immutable-borrow block: bytes
        // inserts may need the estate for detoast).
        let nsets = self.spec.sets.len();
        if nsets != 0 {
            let mut sets_mem = 0usize;
            for j in 0..nsets {
                let PdSetSpec { att, kind } = self.spec.sets[j];
                // Re-borrow per set: the bytes arm needs estate for detoast.
                let (value, isnull) = {
                    let base = estate.slot_mut(id).base();
                    (base.tts_values[att as usize], base.tts_isnull[att as usize])
                };
                let dset = &mut self.dsets[g * nsets + j];
                if isnull {
                    dset.seen_null = true;
                } else {
                    match kind {
                        DistinctKeyKind::Int16 => dset.insert_i64(value.as_i16() as i64),
                        DistinctKeyKind::Int32 => dset.insert_i64(value.as_i32() as i64),
                        DistinctKeyKind::Int64 => dset.insert_i64(value.as_i64()),
                        DistinctKeyKind::Bytes => {
                            // SAFETY: non-null live text/varchar varlena (the
                            // leader's admission proved the argument type);
                            // detoast copies land in per-tuple memory.
                            let v = unsafe {
                                ::types_fmgr::datum_varlena_packed(
                                    value,
                                    estate.ecxt(tmp).per_tuple_mcx(),
                                )
                            }?;
                            dset.insert_bytes(v.data());
                        }
                    }
                }
                sets_mem += dset.mem_bytes();
            }
            self.total_set_mem = self.total_set_mem + sets_mem - self.set_mem[g];
            self.set_mem[g] = sets_mem;
        }
        if self.mem() <= self.budget.max(self.evict_floor) {
            return Ok(PdFeed::Ok);
        }
        // Leader (mcx-backed): evict the largest sets to the DistinctSet
        // spill tapes until back under budget; workers freeze instead.
        if self.mcx.is_some() {
            self.evict_sets()?;
            return Ok(PdFeed::Ok);
        }
        Ok(PdFeed::Crossed)
    }

    /// Leader crossing: spill the largest in-memory sets to their own
    /// hash-partitioned tapes until under budget (each spill_flush resets
    /// the set's values, capacities retained). Bounded: memory <= budget +
    /// one insert, exactly the serial plain arm's law.
    #[cold]
    #[inline(never)]
    fn evict_sets(&mut self) -> PgResult<()> {
        let mcx = self.mcx.expect("evict_sets is leader-only");
        let budget = self.budget;
        let nsets = self.spec.sets.len();
        while self.mem() > budget {
            // Largest set by held bytes.
            let mut best: Option<(usize, usize)> = None;
            for (i, d) in self.dsets.iter().enumerate() {
                let m = d.mem_bytes();
                if d.len() > 0 && best.is_none_or(|(_, bm)| m > bm) {
                    best = Some((i, m));
                }
            }
            let Some((i, _)) = best else {
                // Nothing evictable right now (flushed capacities +
                // metadata hold the floor — estimate-gated upstream);
                // ratchet the floor so the crossing check re-arms only on
                // real growth (epoch cadence, not per row).
                self.evict_floor = self.mem() + (self.budget / 16).max(4096);
                return Ok(());
            };
            let kind = self.spec.sets[i % nsets].kind;
            self.dsets[i].spill_flush(kind, budget, mcx)?;
            self.ever_spilled = true;
            let gi = i / nsets;
            let sets_mem: usize = self.dsets[gi * nsets..(gi + 1) * nsets]
                .iter()
                .map(|d| d.mem_bytes())
                .sum();
            self.total_set_mem = self.total_set_mem + sets_mem - self.set_mem[gi];
            self.set_mem[gi] = sets_mem;
        }
        self.evict_floor = self.mem() + (self.budget / 16).max(4096);
        Ok(())
    }

    /// Fold a HANDED table into this (leader) builder — the serial merge
    /// path (spill-capable through the same eviction lever).
    pub fn merge_handed(&mut self, t: &PdHandedTable) -> PgResult<()> {
        let spec = self.spec.clone();
        let nkeys = spec.nkeys();
        let nvocab = spec.vocab.len();
        let nsets = spec.sets.len();
        for g in 0..t.ngroups {
            let words = &t.keys[g * nkeys..(g + 1) * nkeys];
            let nulls = t.keynulls[g];
            let h = t.hashes[g];
            let (found, slot_idx) = self.probe(words, nulls, h);
            let dst = match found {
                Some(d) => d,
                None => self.create_group(words, nulls, h, slot_idx),
            } as usize;
            // Vocab: pairwise add (count/sum reassociation unobservable).
            for vi in 0..2 * nvocab {
                self.states[dst * 2 * nvocab + vi] += t.states[g * 2 * nvocab + vi];
            }
            let mut sets_mem = 0usize;
            for j in 0..nsets {
                let si = g * nsets + j;
                let dset = &mut self.dsets[dst * nsets + j];
                for &v in t.set_ints(si) {
                    dset.insert_i64(v);
                }
                for (content, _) in t.set_bytes(si) {
                    dset.insert_bytes(content);
                }
                if t.set_null[si] {
                    dset.seen_null = true;
                }
                sets_mem += dset.mem_bytes();
            }
            self.total_set_mem = self.total_set_mem + sets_mem - self.set_mem[dst];
            self.set_mem[dst] = sets_mem;
            if self.mem() > self.budget.max(self.evict_floor) && self.mcx.is_some() {
                self.evict_sets()?;
            }
        }
        Ok(())
    }

    /// Freeze into the handed wire format (plain data, Send). Grouped
    /// tables carry a group partition (top-8 hash bits); the plain shape
    /// (nkeys == 0) carries per-set ELEMENT partitions instead.
    pub fn freeze(mut self) -> PgResult<PdHandedTable> {
        debug_assert!(!self.frozen);
        debug_assert!(!self.ever_spilled, "frozen tables are in-memory only");
        self.frozen = true;
        let spec = self.spec.clone();
        let nsets = spec.sets.len();
        let n = self.ngroups();
        let total_sets = n * nsets;
        let mut set_ints: Vec<i64> = Vec::new();
        let mut set_int_offs: Vec<u32> = Vec::with_capacity(total_sets + 1);
        let mut set_blob: Vec<u8> = Vec::new();
        let mut set_spans: Vec<PdSpan> = Vec::new();
        let mut set_span_offs: Vec<u32> = Vec::with_capacity(total_sets + 1);
        let mut set_null: Vec<bool> = Vec::with_capacity(total_sets);
        let mut elem_parts: Vec<u32> = Vec::new();
        set_int_offs.push(0);
        set_span_offs.push(0);
        let plain = spec.nkeys() == 0;
        for (si, d) in self.dsets.iter().enumerate() {
            set_null.push(d.seen_null);
            match spec.sets[si % nsets.max(1)].kind {
                DistinctKeyKind::Bytes => {
                    if plain {
                        // Element-partitioned export: spans ordered by the
                        // partition of their content hash.
                        let mut idx: Vec<u32> = (0..d.n_bytes() as u32).collect();
                        let part_of = |i: u32| -> usize {
                            let (_, _, h) = d.bytes_span(i as usize);
                            ((mix64(h as u64) >> 32) as usize) & (PD_ELEM_PARTS - 1)
                        };
                        idx.sort_by_key(|&i| part_of(i));
                        let base = set_spans.len() as u32;
                        let mut starts = [0u32; PD_ELEM_PARTS + 1];
                        for &i in &idx {
                            starts[part_of(i) + 1] += 1;
                        }
                        for p in 0..PD_ELEM_PARTS {
                            starts[p + 1] += starts[p];
                        }
                        elem_parts.extend(starts.iter().map(|&s| base + s));
                        for &i in &idx {
                            let (content, h) = {
                                let (off, len, h) = d.bytes_span(i as usize);
                                (d.bytes_content(off, len).to_vec(), h)
                            };
                            let off = set_blob.len() as u32;
                            set_blob.extend_from_slice(&content);
                            set_spans.push(PdSpan { off, len: content.len() as u32, hash: h });
                        }
                    } else {
                        for i in 0..d.n_bytes() {
                            let (off, len, h) = d.bytes_span(i);
                            let content = d.bytes_content(off, len);
                            let noff = set_blob.len() as u32;
                            set_blob.extend_from_slice(content);
                            set_spans.push(PdSpan { off: noff, len, hash: h });
                        }
                    }
                }
                _ => {
                    if plain {
                        let base = set_ints.len() as u32;
                        let mut vals: Vec<i64> = d.ints().to_vec();
                        let part_of =
                            |v: i64| ((mix64(v as u64) >> 32) as usize) & (PD_ELEM_PARTS - 1);
                        vals.sort_by_key(|&v| part_of(v));
                        let mut starts = [0u32; PD_ELEM_PARTS + 1];
                        for &v in &vals {
                            starts[part_of(v) + 1] += 1;
                        }
                        for p in 0..PD_ELEM_PARTS {
                            starts[p + 1] += starts[p];
                        }
                        elem_parts.extend(starts.iter().map(|&s| base + s));
                        set_ints.extend_from_slice(&vals);
                    } else {
                        set_ints.extend_from_slice(d.ints());
                    }
                }
            }
            set_int_offs.push(set_ints.len() as u32);
            set_span_offs.push(set_spans.len() as u32);
        }
        // Group partition (grouped shapes): counting sort by top-8 bits.
        let parts = if !plain {
            let mut starts = vec![0u32; PD_GROUP_PARTS + 1];
            for &h in &self.hashes {
                starts[(h >> 56) as usize + 1] += 1;
            }
            for p in 0..PD_GROUP_PARTS {
                starts[p + 1] += starts[p];
            }
            let mut idx = vec![0u32; n];
            let mut cur = starts.clone();
            for (g, &h) in self.hashes.iter().enumerate() {
                let b = (h >> 56) as usize;
                idx[cur[b] as usize] = g as u32;
                cur[b] += 1;
            }
            Some(PdPartition { starts, idx })
        } else {
            None
        };
        Ok(PdHandedTable {
            ngroups: n,
            keys: core::mem::take(&mut self.keys),
            keynulls: core::mem::take(&mut self.keynulls),
            hashes: core::mem::take(&mut self.hashes),
            states: core::mem::take(&mut self.states),
            set_ints,
            set_int_offs,
            set_blob,
            set_spans,
            set_span_offs,
            set_null,
            elem_parts,
            parts,
        })
    }

    /// Tear the (leader) builder into merged-emit parts: keys + vocab
    /// states + the live DistinctSets (spilled ones included — the emit
    /// tail replays them through the existing spilled-set machinery).
    pub fn into_merged(self) -> PdMerged<'mcx> {
        PdMerged {
            ngroups: self.ngroups(),
            keys: self.keys,
            keynulls: self.keynulls,
            states: self.states,
            dsets: self.dsets.into_iter().map(Some).collect(),
        }
    }
}

#[inline]
pub(crate) fn key_hash(words: &[i64], nulls: u32) -> u64 {
    let mut h = (nulls as u64) ^ 0x9e37_79b9_7f4a_7c15;
    for &w in words {
        h = mix64(h ^ (w as u64));
    }
    h
}

// ===========================================================================
// The wire format.
// ===========================================================================

#[derive(Clone, Copy)]
pub struct PdSpan {
    off: u32,
    len: u32,
    hash: u32,
}

pub struct PdPartition {
    /// 257 prefix-sum starts into `idx`.
    starts: Vec<u32>,
    idx: Vec<u32>,
}

/// One participant's frozen partial table — plain data, self-contained.
pub struct PdHandedTable {
    pub ngroups: usize,
    keys: Vec<i64>,
    keynulls: Vec<u32>,
    hashes: Vec<u64>,
    states: Vec<i64>,
    set_ints: Vec<i64>,
    set_int_offs: Vec<u32>,
    set_blob: Vec<u8>,
    set_spans: Vec<PdSpan>,
    set_span_offs: Vec<u32>,
    set_null: Vec<bool>,
    /// Plain shape: per set, PD_ELEM_PARTS+1 absolute starts into
    /// set_ints/set_spans (laid consecutively per set).
    elem_parts: Vec<u32>,
    parts: Option<PdPartition>,
}

impl PdHandedTable {
    #[inline]
    fn set_ints(&self, si: usize) -> &[i64] {
        &self.set_ints[self.set_int_offs[si] as usize..self.set_int_offs[si + 1] as usize]
    }

    /// Iterate (content, hash) of set `si`'s byte elements.
    fn set_bytes(&self, si: usize) -> impl Iterator<Item = (&[u8], u32)> {
        self.set_spans[self.set_span_offs[si] as usize..self.set_span_offs[si + 1] as usize]
            .iter()
            .map(|sp| {
                (&self.set_blob[sp.off as usize..(sp.off + sp.len) as usize], sp.hash)
            })
    }

    pub fn mem_bytes(&self) -> usize {
        self.keys.len() * 8
            + self.keynulls.len() * 4
            + self.hashes.len() * 8
            + self.states.len() * 8
            + self.set_ints.len() * 8
            + self.set_blob.len()
            + self.set_spans.len() * core::mem::size_of::<PdSpan>()
            + self.set_null.len()
    }
}

// SAFETY: plain owned data, no interior pointers.
unsafe impl Send for PdHandedTable {}
unsafe impl Sync for PdHandedTable {}

// ===========================================================================
// Merged output (either merge path) — consumed by the emit adoptions.
// ===========================================================================

pub struct PdMerged<'mcx> {
    pub ngroups: usize,
    pub keys: Vec<i64>,
    pub keynulls: Vec<u32>,
    /// (acc, count) pairs, stride 2*nvocab.
    pub states: Vec<i64>,
    pub(crate) dsets: Vec<Option<DistinctSet<'mcx>>>,
}

impl PdMerged<'_> {
    /// Retained CONTENT bytes of one merged bucket (R3 accounting for the
    /// combine phase — the merged result is held until the leader adopts).
    /// Deliberately len-based, matching `PdHandedTable::mem_bytes`'s
    /// convention: the envelope check compares against the sum of the
    /// sealed tables' CONTENT, and capacity-based counting (Vec doubling +
    /// probe-table roundup on freshly rebuilt sets, ~2-4x slack) would
    /// spuriously cross it for legitimately near-budget merges (review
    /// finding R1). DistinctSet::mem_bytes is the builder's own metering,
    /// shared with the accept-phase budget.
    pub fn mem_bytes(&self) -> usize {
        self.keys.len() * 8
            + self.keynulls.len() * 4
            + self.states.len() * 8
            + self.dsets.len() * core::mem::size_of::<Option<DistinctSet<'_>>>()
            + self.dsets.iter().flatten().map(|d| d.mem_bytes()).sum::<usize>()
    }
}

impl PdMerged<'static> {
    /// Rebind a scoped-thread-built (never-spilled) merge result to the
    /// node's `'mcx` (see `DistinctSet::unspilled_into`).
    pub fn into_lt<'m>(self) -> PdMerged<'m> {
        PdMerged {
            ngroups: self.ngroups,
            keys: self.keys,
            keynulls: self.keynulls,
            states: self.states,
            dsets: self.dsets.into_iter().map(|d| d.map(DistinctSet::unspilled_into)).collect(),
        }
    }
}

// ===========================================================================
// Parallel merge — bucket-claim over group partitions (grouped) or element
// partitions (plain). Fast path only: every input in memory, no spills.
// ===========================================================================

struct PdParCtx<'a> {
    spec: &'a PdSpec,
    tables: &'a [PdHandedTable],
    next: core::sync::atomic::AtomicUsize,
    /// One exclusive output cell per bucket.
    out: Vec<core::cell::UnsafeCell<PdMerged<'static>>>,
}

// SAFETY: each bucket cell is written by exactly one claimer (fetch_add
// hands each bucket index out once); tables are read-only.
unsafe impl Sync for PdParCtx<'_> {}

fn merge_bucket(spec: &PdSpec, tables: &[PdHandedTable], b: usize) -> PdMerged<'static> {
    let refs: Vec<&PdHandedTable> = tables.iter().collect();
    merge_bucket_refs(spec, &refs, b)
}

/// The bucket merge over BORROWED tables (the M3.5 combine mixes sealed
/// in-memory tables with spill-synthesized ones it owns locally; everything
/// else about the merge is the donor verbatim). Implemented as the
/// incremental [`PdBucketMerger`] driven in one pass, so the donor
/// semantics stay single-sourced.
fn merge_bucket_refs(spec: &PdSpec, tables: &[&PdHandedTable], b: usize) -> PdMerged<'static> {
    let mut m = PdBucketMerger::new(spec);
    for &t in tables {
        m.absorb(t, b);
    }
    m.finish()
}

/// Incremental donor bucket merge (M3.5 inc-3b): [`merge_bucket_refs`]'s
/// loop body, restructured so the combine-split path can absorb tables IN
/// SEQUENCE — the sealed in-memory tables in one pass, then one value-hash
/// slice's synthesized table at a time, dropping each between absorbs so
/// transient memory stays bounded.
///
/// EXACTLY-ONCE LAW (the inc-3b hazard): value-hash slices partition each
/// group's VALUE SET disjointly, but everything that is NOT a per-value
/// fact must merge exactly once, not once per slice. The sealed IN-MEMORY
/// tables are the sole carriers of group-level state — vocab (acc,count)
/// words, `seen_null`, and group existence (a spilled record can never
/// reference a group its own Local's remainder lacks: groups are created
/// at accept and the epoch reset clears only set VALUES) — and they are
/// absorbed ONCE, before any slice. Each slice's synthesized table
/// ([`pd_table_from_spill`]) is built by replaying value records through a
/// fresh builder: its vocab states are all ZERO and its `set_null` faces
/// all FALSE (`create_group` zero-init; NULLs never touch the file), so
/// absorbing it adds 0 to every vocab word, ORs `false` into every
/// `seen_null`, and contributes ONLY set-value insertions — idempotent,
/// over slices that are disjoint by the routing law. Hence "in-memory once
/// + slices in any sequence" equals the direct one-pass donor merge
/// (property test `split_slice_merge_invariance`).
pub struct PdBucketMerger<'s> {
    spec: &'s PdSpec,
    out: PdMerged<'static>,
    /// Bucket-local open-addressed probe over the output groups.
    table: Vec<u32>,
    hashes: Vec<u64>,
}

impl<'s> PdBucketMerger<'s> {
    pub fn new(spec: &'s PdSpec) -> PdBucketMerger<'s> {
        PdBucketMerger {
            spec,
            out: PdMerged {
                ngroups: 0,
                keys: Vec::new(),
                keynulls: Vec::new(),
                states: Vec::new(),
                dsets: Vec::new(),
            },
            table: vec![0; 64],
            hashes: Vec::new(),
        }
    }

    /// Merge bucket `b` of `t` into the output — the donor loop body
    /// verbatim.
    pub fn absorb(&mut self, t: &PdHandedTable, b: usize) {
        let nkeys = self.spec.nkeys();
        let nvocab = self.spec.vocab.len();
        let nsets = self.spec.sets.len();
        let PdBucketMerger { out, table, hashes, .. } = self;
        let parts = t.parts.as_ref().expect("grouped tables are partitioned");
        let (s, e) = (parts.starts[b] as usize, parts.starts[b + 1] as usize);
        for &g in &parts.idx[s..e] {
            let g = g as usize;
            let words = &t.keys[g * nkeys..(g + 1) * nkeys];
            let nulls = t.keynulls[g];
            let h = t.hashes[g];
            // Probe.
            let mut mask = table.len() - 1;
            let mut slot = (h as usize) & mask;
            let dst = loop {
                match table[slot] {
                    0 => {
                        let d = out.ngroups;
                        out.ngroups += 1;
                        hashes.push(h);
                        out.keys.extend_from_slice(words);
                        out.keynulls.push(nulls);
                        out.states.extend(core::iter::repeat(0i64).take(2 * nvocab));
                        for _ in 0..nsets {
                            out.dsets.push(Some(DistinctSet::new()));
                        }
                        table[slot] = (d + 1) as u32;
                        if (out.ngroups + 1) * 8 > table.len() * 7 {
                            let new_len = table.len() * 2;
                            mask = new_len - 1;
                            let mut nt = vec![0u32; new_len];
                            for (gg, &hh) in hashes.iter().enumerate() {
                                let mut sl = (hh as usize) & mask;
                                while nt[sl] != 0 {
                                    sl = (sl + 1) & mask;
                                }
                                nt[sl] = (gg + 1) as u32;
                            }
                            *table = nt;
                        }
                        break d;
                    }
                    e2 => {
                        let d = (e2 - 1) as usize;
                        if hashes[d] == h
                            && out.keynulls[d] == nulls
                            && &out.keys[d * nkeys..(d + 1) * nkeys] == words
                        {
                            break d;
                        }
                        slot = (slot + 1) & mask;
                    }
                }
            };
            for vi in 0..2 * nvocab {
                out.states[dst * 2 * nvocab + vi] += t.states[g * 2 * nvocab + vi];
            }
            for j in 0..nsets {
                let si = g * nsets + j;
                let dset = out.dsets[dst * nsets + j].as_mut().unwrap();
                for &v in t.set_ints(si) {
                    dset.insert_i64(v);
                }
                for (content, _) in t.set_bytes(si) {
                    dset.insert_bytes(content);
                }
                if t.set_null[si] {
                    dset.seen_null = true;
                }
            }
        }
    }

    /// Capacity-based bytes of the merged-so-far bucket — the combine
    /// split's EXACT dedup-aware budget check (read after every slice
    /// absorb; no directory estimate can see through duplicates, this can).
    pub fn mem_bytes(&self) -> usize {
        self.out.keys.capacity() * 8
            + self.out.keynulls.capacity() * 4
            + self.out.states.capacity() * 8
            + self.hashes.capacity() * 8
            + self.table.capacity() * 4
            + self.out.dsets.capacity() * core::mem::size_of::<Option<DistinctSet<'static>>>()
            + self
                .out
                .dsets
                .iter()
                .map(|d| d.as_ref().map_or(0, |d| d.mem_bytes()))
                .sum::<usize>()
    }

    pub fn finish(self) -> PdMerged<'static> {
        self.out
    }
}

fn pd_claim_loop(ctx: &PdParCtx<'_>, nbuckets: usize) {
    loop {
        let b = ctx.next.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        if b >= nbuckets {
            return;
        }
        // SAFETY: bucket b handed to this claimer alone.
        unsafe { *ctx.out[b].get() = merge_bucket(ctx.spec, ctx.tables, b) };
    }
}

/// Grouped parallel merge: claimers own top-8-bit group buckets; the result
/// concatenates buckets in order (deterministic; emit re-orders by the plan
/// Sort prefix anyway).
pub fn pd_parallel_merge_grouped(
    spec: &Arc<PdSpec>,
    tables: Vec<PdHandedTable>,
    nthreads: usize,
) -> PdMerged<'static> {
    let ctx = PdParCtx {
        spec,
        tables: &tables,
        next: core::sync::atomic::AtomicUsize::new(0),
        out: (0..PD_GROUP_PARTS)
            .map(|_| {
                core::cell::UnsafeCell::new(PdMerged {
                    ngroups: 0,
                    keys: Vec::new(),
                    keynulls: Vec::new(),
                    states: Vec::new(),
                    dsets: Vec::new(),
                })
            })
            .collect(),
    };
    let extra = nthreads.saturating_sub(1);
    std::thread::scope(|s| {
        let handles: Vec<_> =
            (0..extra).map(|_| s.spawn(|| pd_claim_loop(&ctx, PD_GROUP_PARTS))).collect();
        pd_claim_loop(&ctx, PD_GROUP_PARTS);
        for h in handles {
            h.join().expect("pardistinct merge claimer panicked");
        }
    });
    // Concatenate buckets.
    let mut merged = PdMerged {
        ngroups: 0,
        keys: Vec::new(),
        keynulls: Vec::new(),
        states: Vec::new(),
        dsets: Vec::new(),
    };
    for cell in ctx.out {
        let m = cell.into_inner();
        merged.ngroups += m.ngroups;
        merged.keys.extend(m.keys);
        merged.keynulls.extend(m.keynulls);
        merged.states.extend(m.states);
        merged.dsets.extend(m.dsets);
    }
    merged
}

// --- plain (single-group) parallel union over element partitions ----------

struct PdElemCtx<'a> {
    spec: &'a PdSpec,
    tables: &'a [PdHandedTable],
    next: core::sync::atomic::AtomicUsize,
    /// out[set * PD_ELEM_PARTS + p]: the deduped elements of partition p.
    out: Vec<core::cell::UnsafeCell<(Vec<i64>, Vec<u8>, Vec<PdSpan>)>>,
}

// SAFETY: as PdParCtx — each (set, partition) cell has one writer.
unsafe impl Sync for PdElemCtx<'_> {}

fn pd_elem_claim_loop(ctx: &PdElemCtx<'_>) {
    let nsets = ctx.spec.sets.len();
    let total = nsets * PD_ELEM_PARTS;
    loop {
        let w = ctx.next.fetch_add(1, core::sync::atomic::Ordering::Relaxed);
        if w >= total {
            return;
        }
        let (j, p) = (w / PD_ELEM_PARTS, w % PD_ELEM_PARTS);
        let mut dset: DistinctSet<'static> = DistinctSet::new();
        for t in ctx.tables {
            let parts = &t.elem_parts[j * (PD_ELEM_PARTS + 1)..(j + 1) * (PD_ELEM_PARTS + 1)];
            match ctx.spec.sets[j].kind {
                DistinctKeyKind::Bytes => {
                    for sp in &t.set_spans[parts[p] as usize..parts[p + 1] as usize] {
                        dset.insert_bytes(
                            &t.set_blob[sp.off as usize..(sp.off + sp.len) as usize],
                        );
                    }
                }
                _ => {
                    for &v in &t.set_ints[parts[p] as usize..parts[p + 1] as usize] {
                        dset.insert_i64(v);
                    }
                }
            }
        }
        let mut blob = Vec::new();
        let mut spans = Vec::new();
        for i in 0..dset.n_bytes() {
            let (off, len, h) = dset.bytes_span(i);
            let noff = blob.len() as u32;
            blob.extend_from_slice(dset.bytes_content(off, len));
            spans.push(PdSpan { off: noff, len, hash: h });
        }
        // SAFETY: cell w has one writer.
        unsafe { *ctx.out[w].get() = (dset.take_ints(), blob, spans) };
    }
}

/// Plain-shape parallel union: claimers own (set, element-partition) cells;
/// partitions are disjoint by construction so the concatenation of the
/// per-partition deduped element lists IS the union.
pub fn pd_parallel_merge_plain<'m>(
    spec: &Arc<PdSpec>,
    tables: Vec<PdHandedTable>,
    nthreads: usize,
) -> PdMerged<'m> {
    let nsets = spec.sets.len();
    let ctx = PdElemCtx {
        spec,
        tables: &tables,
        next: core::sync::atomic::AtomicUsize::new(0),
        out: (0..nsets * PD_ELEM_PARTS)
            .map(|_| core::cell::UnsafeCell::new((Vec::new(), Vec::new(), Vec::new())))
            .collect(),
    };
    let extra = nthreads.saturating_sub(1);
    std::thread::scope(|s| {
        let handles: Vec<_> = (0..extra).map(|_| s.spawn(|| pd_elem_claim_loop(&ctx))).collect();
        pd_elem_claim_loop(&ctx);
        for h in handles {
            h.join().expect("pardistinct union claimer panicked");
        }
    });
    let mut dsets: Vec<Option<DistinctSet<'m>>> = Vec::with_capacity(nsets);
    let mut outs = ctx.out.into_iter().map(|c| c.into_inner());
    for j in 0..nsets {
        let mut ints: Vec<i64> = Vec::new();
        let mut blob: Vec<u8> = Vec::new();
        let mut spans: Vec<(u32, u32, u32)> = Vec::new();
        for _ in 0..PD_ELEM_PARTS {
            let (i, b, sp) = outs.next().expect("cell per (set, partition)");
            ints.extend(i);
            let base = blob.len() as u32;
            blob.extend(b);
            spans.extend(sp.iter().map(|s| (base + s.off, s.len, s.hash)));
        }
        let seen_null = tables.iter().any(|t| t.set_null[j]);
        dsets.push(Some(DistinctSet::from_values(spec.sets[j].kind, ints, blob, spans, seen_null)));
    }
    PdMerged { ngroups: 1, keys: Vec::new(), keynulls: Vec::new(), states: Vec::new(), dsets }
}

// ===========================================================================
// Spec derivation — the leader's vocabulary check over its initialized
// AggStateData. Everything here is per-plan static.
// ===========================================================================

/// Map an AGGREGATE (Aggref.aggfnoid — what the derivation actually holds)
/// to its vocab kind given the (single) argument's outer attno + width.
/// These aggregates' transfns are exactly the
/// `order_insensitive_exact_transfn` whitelist minus the Int128 family
/// (count(*)→int8inc, count(any)→int8inc_any, sum(int2/4)→int2/4_sum,
/// avg(int2/4)→int2/4_avg_accum; avg/sum(int8) accumulate Int128/numeric —
/// v1 refusal).
///
/// HISTORY NOTE (m2-distinct-sink): the original table listed the TRANSFN
/// proc oids while the caller passed `ar.aggfnoid` — no vocab shape could
/// ever derive. Unobservable under the Gather-era arm (its v1 economics
/// refused non-empty vocab before deriving); found by the sink's Q10-class
/// e2e engagement coverage.
pub(crate) fn vocab_kind(aggfnoid: Oid, att: Option<(u16, PdInt)>) -> Option<PdVocabKind> {
    /// pg_proc: count(*) / count(any) / sum(int2) / sum(int4) /
    /// avg(int2) / avg(int4).
    const AGG_COUNT_STAR: Oid = 2803;
    const AGG_COUNT_ANY: Oid = 2147;
    const AGG_SUM_INT2: Oid = 2109;
    const AGG_SUM_INT4: Oid = 2108;
    const AGG_AVG_INT2: Oid = 2102;
    const AGG_AVG_INT4: Oid = 2101;
    match aggfnoid {
        AGG_COUNT_STAR => Some(PdVocabKind::CountStar),
        AGG_COUNT_ANY => att.map(|(a, _)| PdVocabKind::CountAny { att: a }),
        AGG_SUM_INT2 => att.and_then(|(a, k)| {
            (k == PdInt::I16).then_some(PdVocabKind::SumInt { att: a, kind: k })
        }),
        AGG_SUM_INT4 => att.and_then(|(a, k)| {
            (k == PdInt::I32).then_some(PdVocabKind::SumInt { att: a, kind: k })
        }),
        AGG_AVG_INT2 => att.and_then(|(a, k)| {
            (k == PdInt::I16).then_some(PdVocabKind::AvgInt { att: a, kind: k })
        }),
        AGG_AVG_INT4 => att.and_then(|(a, k)| {
            (k == PdInt::I32).then_some(PdVocabKind::AvgInt { att: a, kind: k })
        }),
        _ => None,
    }
}

/// Uniform internal error for impossible wire states (defensive; never
/// expected to fire).
#[cold]
pub(crate) fn pd_internal(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("pardistinct internal: {msg}")))
}

// ===========================================================================
// M2 runtime-sink surface (m2-distinct-sink): the donor machinery above,
// re-shaped for the morsel runtime's SealedParallelSink contract. The
// builder becomes a lifetime-erased, Send-able worker Local; freeze is the
// seal; `pd_merge_bucket` is the per-partition combine; the concatenation
// helper assembles the published merged result. The Gather-era registry /
// handoff / leader-partial machinery above is NOT used by the sink (it
// remains the compat path until the runtime arm subsumes it).
// ===========================================================================

/// A worker-side [`PdBuilder`] with its `'mcx` lifetime erased to `'static`.
///
/// SOUNDNESS (the module-level worker discipline, made a type invariant):
/// the wrapped builder is constructed with `mcx: None`, so it can never
/// spill and never holds an arena handle; every byte it retains is owned
/// plain data (`DistinctSet` copies inserted content into its own blob; the
/// detoast scratch lives in the CALLER's per-tuple context and is reset per
/// row). Nothing borrowed from any `EStateData` survives an `accept` call,
/// which is what makes the lifetime erasure and the `Send` below sound —
/// the same self-contained-buffer argument as [`PdHandedTable`].
pub struct PdSinkLocal {
    builder: PdBuilder<'static>,
}

// SAFETY: `mcx` is `None` by construction (`new` is the only constructor)
// and `DistinctSet` without spill state is owned plain data; see the type
// doc.
unsafe impl Send for PdSinkLocal {}

impl PdSinkLocal {
    pub fn new(spec: Arc<PdSpec>, budget: usize) -> PdSinkLocal {
        PdSinkLocal { builder: PdBuilder::new(spec, budget, None) }
    }

    /// Feed one row (the worker accept). `PdFeed::Crossed` = the worker
    /// budget crossed — the sink arm's policy is abort-and-refuse (the
    /// runtime has no degrade target; the leader reruns the serial arm).
    #[inline]
    pub fn accept<'mcx>(
        &mut self,
        estate: &mut EStateData<'mcx>,
        id: ExecSlotId,
        tmp: EcxtId,
    ) -> PgResult<PdFeed> {
        // SAFETY: pure lifetime erasure — the `mcx: None` builder retains no
        // borrow from `estate` (type invariant above); shortening `'static`
        // to `'mcx` on the receiver is the safe direction for every field
        // the call can touch.
        let b: &mut PdBuilder<'mcx> =
            unsafe { core::mem::transmute::<&mut PdBuilder<'static>, &mut PdBuilder<'mcx>>(
                &mut self.builder,
            ) };
        b.accept(estate, id, tmp)
    }

    /// Seal: freeze into the partitioned wire form.
    pub fn freeze(self) -> PgResult<PdHandedTable> {
        self.builder.freeze()
    }

    pub fn ngroups(&self) -> usize {
        self.builder.ngroups()
    }

    pub fn mem_bytes(&self) -> usize {
        self.builder.mem_bytes()
    }
}

/// An empty, well-formed GROUPED handed table (the seal error path's
/// placeholder: the RG is already aborting, but the wire shape must stay
/// consumable by any combine that races the abort observation).
pub fn pd_empty_grouped_table(spec: &Arc<PdSpec>) -> PdHandedTable {
    PdBuilder::new(Arc::clone(spec), usize::MAX, None)
        .freeze()
        .expect("freezing an empty builder cannot fail")
}

/// Number of grouped combine partitions (the sink's partition space).
pub const PD_SINK_GROUP_PARTS: u64 = PD_GROUP_PARTS as u64;

/// Merge ONE group partition across the sealed tables (slice order = worker
/// slot order = the combine's deterministic input order). This is the
/// donors' `merge_bucket` verbatim, exposed for the runtime sink's
/// partition-claim combine.
pub fn pd_merge_bucket(
    spec: &PdSpec,
    tables: &[PdHandedTable],
    bucket: usize,
) -> PdMerged<'static> {
    merge_bucket(spec, tables, bucket)
}

/// [`pd_merge_bucket`] over borrowed tables: the M3.5 spill combine merges
/// the sealed in-memory tables together with spill-synthesized tables it
/// builds (and owns) on the combine thread.
pub fn pd_merge_bucket_refs(
    spec: &PdSpec,
    tables: &[&PdHandedTable],
    bucket: usize,
) -> PdMerged<'static> {
    merge_bucket_refs(spec, tables, bucket)
}

/// Concatenate per-bucket merge outputs (bucket order) into the one merged
/// result — the grouped parallel merge's tail, exposed for the sink's
/// finalize.
pub fn pd_concat_buckets(buckets: Vec<PdMerged<'static>>) -> PdMerged<'static> {
    let mut merged = PdMerged {
        ngroups: 0,
        keys: Vec::new(),
        keynulls: Vec::new(),
        states: Vec::new(),
        dsets: Vec::new(),
    };
    for m in buckets {
        merged.ngroups += m.ngroups;
        merged.keys.extend(m.keys);
        merged.keynulls.extend(m.keynulls);
        merged.states.extend(m.states);
        merged.dsets.extend(m.dsets);
    }
    merged
}

/// A merged result crossing threads (helper finalize → parked leader).
///
/// SAFETY invariant: only ever constructed from `pd_merge_bucket` outputs —
/// bucket merges build FRESH, never-spilled `DistinctSet<'static>`s (no
/// tape state, no arena handles), so the payload is owned plain data.
pub struct PdSinkMerged(PdMerged<'static>);

// SAFETY: see the type doc (never-spilled sets are owned plain data — the
// `PdHandedTable` argument).
unsafe impl Send for PdSinkMerged {}

impl PdSinkMerged {
    pub fn new(merged: PdMerged<'static>) -> PdSinkMerged {
        PdSinkMerged(merged)
    }

    /// Rebind to the consuming node's `'mcx` (the `into_lt` law: sound for
    /// never-spilled merge results, which is the constructor's invariant).
    pub fn into_merged<'m>(self) -> PdMerged<'m> {
        self.0.into_lt()
    }

    pub fn ngroups(&self) -> usize {
        self.0.ngroups
    }
}

// ===========================================================================
// M3.5 accept-side spill surface (docs/design/m3.5-spill.md §4, inc-3a).
// ADDITIVE ONLY: the builder's own Mcx-bound spill machinery (`evict_sets`
// / distinctset `SpillState`) is untouched — the sink Locals still carry
// `mcx: None` and freeze()'s `!ever_spilled` invariant keeps holding. What
// spills here are the DistinctSet VALUES alone, through an operator-owned
// byte contract the caller writes to a spillset file: group keys, vocab
// words, and `seen_null` stay in memory and ride the Local through SEAL.
//
// Record contract (fixed width per spec, native-endian — the DistinctSet
// int law, raw i64 words): one record per (group, set, value) =
//   [keynulls u64][key word i64 × nkeys][set index u64][value i64]
// NULLs NEVER touch the file: group-key null bits ride the keynulls word
// (part of the group identity, not a value), and set NULL presence rides
// the in-memory `seen_null` (the distinctset frozen rule).
//
// Partition law: top-8 bits of the group-key hash (`hash >> 56`) — EXACTLY
// the counting-sort partition freeze() builds and the bucket merge reads,
// so a spilled record replays into the same combine partition that claims
// its group.
// ===========================================================================

/// Byte width of one spilled (group, set, value) record for `spec`.
pub fn pd_spill_record_width(spec: &PdSpec) -> usize {
    (spec.nkeys() + 3) * 8
}

impl PdBuilder<'_> {
    /// Fail-closed shape gate: only grouped int-set builders spill exactly.
    /// Plain/element-partition shapes (nkeys == 0), bytes sets, and anything
    /// touching the leader's Mcx-bound machinery refuse (the caller falls
    /// through to the phase-1 Crossed abort → serial rerun).
    fn spill_eligible(&self) -> bool {
        self.spec.nkeys() > 0
            && !self.spec.sets.is_empty()
            && self
                .spec
                .sets
                .iter()
                .all(|s| !matches!(s.kind, DistinctKeyKind::Bytes))
            && self.mcx.is_none()
            && !self.ever_spilled
            && !self.frozen
    }

    /// Bytes of set VALUES currently held (what an epoch flush would move to
    /// disk). Observability figure; NOT the worthwhileness yardstick — see
    /// [`Self::spill_freeable_bytes`].
    fn spill_value_bytes(&self) -> usize {
        self.dsets.iter().map(|d| d.ints().len() * 8).sum()
    }

    /// Bytes an epoch flush would RELEASE: the sets' full capacity-based
    /// memory (`total_set_mem`) — `spill_reset_values` SHRINKS the sets, so
    /// the entire set side of `mem()` comes back. This is the caller's
    /// worthwhileness yardstick: a crossing is group-table-dominated exactly
    /// when the set side is a small fraction of the budget (`base_mem`
    /// drives the crossing), and THAT is what value spill cannot help.
    ///
    /// Calibration note (inc-3a followup, battery -82184): the original gate
    /// compared `spill_value_bytes` (payload alone) against budget/4, but
    /// `mem()` moves in capacity steps, so crossings land right after
    /// Vec/IntSet doublings, where the 8-byte payloads are only ~1/6..1/3 of
    /// set memory (IntSet's 50% max load = 16-32 table bytes/value, plus
    /// 8-16 ints-Vec bytes/value). A purely value-dominated uniform corpus
    /// (the q9 class: 97 sets filling in lockstep, all doubling together)
    /// deterministically sat below budget/4 at every crossing and the arm
    /// fail-closed to the serial fallback on every worker.
    fn spill_freeable_bytes(&self) -> usize {
        self.total_set_mem
    }

    /// Emit every held set value as spill records, partition-contiguous and
    /// partition-ascending (the spillset EpochWriter contract): groups are
    /// counting-sorted by the top-8 hash bits — freeze()'s own partition law
    /// — and each group's sets stream in set order, values in insertion
    /// order. Read-only: the caller resets values via
    /// [`Self::spill_reset_values`] only after its epoch write COMMITS.
    fn spill_emit(&self, emit: &mut dyn FnMut(u32, &[u8]) -> PgResult<()>) -> PgResult<()> {
        debug_assert!(self.spill_eligible());
        let nkeys = self.spec.nkeys();
        let nsets = self.spec.sets.len();
        let n = self.ngroups();
        // The freeze partition law, verbatim (counting sort, top-8 bits).
        let mut starts = vec![0u32; PD_GROUP_PARTS + 1];
        for &h in &self.hashes {
            starts[(h >> 56) as usize + 1] += 1;
        }
        for p in 0..PD_GROUP_PARTS {
            starts[p + 1] += starts[p];
        }
        let mut idx = vec![0u32; n];
        let mut cur = starts.clone();
        for (g, &h) in self.hashes.iter().enumerate() {
            let b = (h >> 56) as usize;
            idx[cur[b] as usize] = g as u32;
            cur[b] += 1;
        }
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        for p in 0..PD_GROUP_PARTS {
            buf.clear();
            for &g in &idx[starts[p] as usize..starts[p + 1] as usize] {
                let g = g as usize;
                let nulls = (self.keynulls[g] as u64).to_ne_bytes();
                let keys = &self.keys[g * nkeys..(g + 1) * nkeys];
                for j in 0..nsets {
                    let jw = (j as u64).to_ne_bytes();
                    for &v in self.dsets[g * nsets + j].ints() {
                        buf.extend_from_slice(&nulls);
                        for &w in keys {
                            buf.extend_from_slice(&w.to_ne_bytes());
                        }
                        buf.extend_from_slice(&jw);
                        buf.extend_from_slice(&v.to_ne_bytes());
                    }
                }
            }
            if !buf.is_empty() {
                emit(p as u32, &buf)?;
            }
        }
        Ok(())
    }

    /// Post-commit epoch reset: drop every set's VALUES, `seen_null`
    /// retained (the distinctset seen_null law — NULLs never spill).
    /// DEVIATION from the §4 sketch's "capacities retained": `mem()` is
    /// capacity-based, so retained capacities would re-arm the crossing
    /// only on capacity DOUBLING (a ~2× budget high-water); shrinking the
    /// sets keeps the R3 bound at ~budget + one insert with the plain
    /// budget check re-armed naturally. The small eviction-floor ratchet
    /// guards the group-table-dominated tail (base_mem alone near the
    /// budget), where the caller's worthwhileness gate then refuses.
    fn spill_reset_values(&mut self) {
        let nsets = self.spec.sets.len();
        for d in &mut self.dsets {
            let seen_null = d.seen_null;
            *d = DistinctSet::new();
            d.seen_null = seen_null;
        }
        if nsets > 0 {
            self.total_set_mem = 0;
            for g in 0..self.ngroups() {
                let m: usize =
                    self.dsets[g * nsets..(g + 1) * nsets].iter().map(|d| d.mem_bytes()).sum();
                self.set_mem[g] = m;
                self.total_set_mem += m;
            }
        }
        self.evict_floor = self.mem() + (self.budget / 16).max(4096);
    }
}

impl PdSinkLocal {
    /// See [`PdBuilder::spill_eligible`].
    pub fn pd_spill_eligible(&self) -> bool {
        self.builder.spill_eligible()
    }

    /// See [`PdBuilder::spill_value_bytes`].
    pub fn pd_spill_value_bytes(&self) -> usize {
        self.builder.spill_value_bytes()
    }

    /// See [`PdBuilder::spill_freeable_bytes`].
    pub fn pd_spill_freeable_bytes(&self) -> usize {
        self.builder.spill_freeable_bytes()
    }

    /// See [`PdBuilder::spill_emit`].
    pub fn pd_spill_emit(
        &self,
        emit: &mut dyn FnMut(u32, &[u8]) -> PgResult<()>,
    ) -> PgResult<()> {
        self.builder.spill_emit(emit)
    }

    /// See [`PdBuilder::spill_reset_values`].
    pub fn pd_spill_reset_values(&mut self) {
        self.builder.spill_reset_values()
    }
}

/// Rebuild ONE partition's spilled records into a merge-compatible
/// [`PdHandedTable`]: replay through a fresh (never-crossing, never-Mcx)
/// builder of the same spec — probe/create-group + set insert, the donor
/// kernel — then freeze. Cross-epoch duplicate values re-dedup here; vocab
/// states are zero (they never left the in-memory tables) so the bucket
/// merge adds nothing for them. Fail-closed on torn or corrupt records.
pub fn pd_table_from_spill(spec: &Arc<PdSpec>, bytes: &[u8]) -> PgResult<PdHandedTable> {
    let nkeys = spec.nkeys();
    let nsets = spec.sets.len();
    if nkeys == 0 || nsets == 0 {
        return Err(pd_internal("distinct spill replay on a non-grouped spec"));
    }
    let width = pd_spill_record_width(spec);
    if bytes.len() % width != 0 {
        return Err(pd_internal("torn distinct spill record (partial row)"));
    }
    let mut b = PdBuilder::new(Arc::clone(spec), usize::MAX, None);
    let mut words = vec![0i64; nkeys];
    let mut off = 0usize;
    let rd = |o: usize| u64::from_ne_bytes(bytes[o..o + 8].try_into().unwrap());
    while off < bytes.len() {
        let nulls = rd(off);
        off += 8;
        if nulls >= (1u64 << nkeys) {
            return Err(pd_internal("corrupt distinct spill record (keynulls)"));
        }
        for w in words.iter_mut() {
            *w = rd(off) as i64;
            off += 8;
        }
        let j = rd(off) as usize;
        off += 8;
        if j >= nsets {
            return Err(pd_internal("corrupt distinct spill record (set index)"));
        }
        let v = rd(off) as i64;
        off += 8;
        let nulls = nulls as u32;
        let h = key_hash(&words, nulls);
        let (found, slot) = b.probe(&words, nulls, h);
        let g = match found {
            Some(g) => g,
            None => b.create_group(&words, nulls, h, slot),
        } as usize;
        b.dsets[g * nsets + j].insert_i64(v);
    }
    b.freeze()
}

/// Route spilled distinct records into 256 value-hash SLICES by the byte of
/// `mix64(value)` `depth` levels from the top (depth 1 = bits 56..64,
/// depth 2 = bits 48..56, …, depth 6 = bits 16..24) — the M3.5 §4
/// COMBINE-SPLIT law (inc-3b). The mixer is distinctset.rs's own spill
/// mixer (splitmix64, the `spill_part` law); distinctset's serial spill
/// consumes bits UPWARD from bit 32 (`(mix64(v) >> 32) & (nparts-1)`),
/// while this routing consumes whole bytes TOP-DOWN — any deterministic
/// slicing of the same full-avalanche hash is legal (equal values hash
/// equal, so every distinct (group, set, value) lands in exactly one
/// slice), and top-down bytes make recursion levels strictly nested (a
/// depth-d slice is subdivided exactly by the next byte down). Fail-closed
/// on torn input and out-of-range depth.
pub fn pd_route_value_records(
    spec: &PdSpec,
    bytes: &[u8],
    depth: u32,
    out: &mut [Vec<u8>],
) -> PgResult<()> {
    debug_assert_eq!(out.len(), PD_GROUP_PARTS);
    if !(1..=6).contains(&depth) {
        return Err(pd_internal("distinct value-slice depth out of range"));
    }
    let width = pd_spill_record_width(spec);
    if bytes.len() % width != 0 {
        return Err(pd_internal("torn distinct spill record (partial row) in split"));
    }
    let voff = width - 8;
    let shift = 64 - 8 * depth;
    let mut off = 0usize;
    while off < bytes.len() {
        let v = u64::from_ne_bytes(bytes[off + voff..off + width].try_into().unwrap());
        let s = ((mix64(v) >> shift) & 0xFF) as usize;
        out[s].extend_from_slice(&bytes[off..off + width]);
        off += width;
    }
    Ok(())
}

/// Combine-side pre-count of one bucket's IN-MEMORY faces: (groups, set
/// values). Together with the spill directory's `part_len`, this is
/// everything the conservative over-budget refusal reads — nothing touches
/// disk before the decision. Groups are an upper bound on the merged
/// bucket's output groups: spilled records never introduce a group the
/// in-memory tables lack (group creation happens at accept; the epoch reset
/// clears only set values).
pub fn pd_bucket_precount(spec: &PdSpec, t: &PdHandedTable, bucket: usize) -> (usize, usize) {
    let Some(parts) = t.parts.as_ref() else { return (0, 0) };
    let nsets = spec.sets.len();
    let (s, e) = (parts.starts[bucket] as usize, parts.starts[bucket + 1] as usize);
    let mut vals = 0usize;
    for &g in &parts.idx[s..e] {
        let g = g as usize;
        for j in 0..nsets {
            let si = g * nsets + j;
            vals += (t.set_int_offs[si + 1] - t.set_int_offs[si]) as usize;
        }
    }
    (e - s, vals)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression (m2-distinct-sink): the vocab table must key on AGGREGATE
    /// oids — the derivation passes `Aggref.aggfnoid`. The original table
    /// listed transfn proc oids and no vocab shape could ever derive.
    #[test]
    fn vocab_kind_keys_on_aggregate_oids() {
        assert!(matches!(vocab_kind(2803, None), Some(PdVocabKind::CountStar)));
        assert!(matches!(
            vocab_kind(2147, Some((3, PdInt::I64))),
            Some(PdVocabKind::CountAny { att: 3 })
        ));
        assert!(matches!(
            vocab_kind(2109, Some((1, PdInt::I16))),
            Some(PdVocabKind::SumInt { att: 1, kind: PdInt::I16 })
        ));
        assert!(matches!(
            vocab_kind(2108, Some((2, PdInt::I32))),
            Some(PdVocabKind::SumInt { att: 2, kind: PdInt::I32 })
        ));
        assert!(matches!(
            vocab_kind(2102, Some((4, PdInt::I16))),
            Some(PdVocabKind::AvgInt { att: 4, kind: PdInt::I16 })
        ));
        assert!(matches!(
            vocab_kind(2101, Some((5, PdInt::I32))),
            Some(PdVocabKind::AvgInt { att: 5, kind: PdInt::I32 })
        ));
        // Width mismatches and the Int128/numeric families refuse.
        assert!(vocab_kind(2108, Some((2, PdInt::I16))).is_none());
        assert!(vocab_kind(2107, Some((2, PdInt::I64))).is_none()); // sum(int8)
        assert!(vocab_kind(2100, Some((2, PdInt::I64))).is_none()); // avg(int8)
        // The OLD (buggy) transfn oids must NOT match.
        assert!(vocab_kind(1219, None).is_none());
        assert!(vocab_kind(2804, Some((0, PdInt::I64))).is_none());
    }

    // --- M3.5 inc-3a spill surface (fleet-run: the known local nodeagg
    // test-binary link limitation) ---------------------------------------

    fn spill_test_spec() -> Arc<PdSpec> {
        Arc::new(PdSpec {
            key_atts: vec![0, 1],
            key_kinds: vec![PdInt::I64, PdInt::I32],
            vocab: vec![PdVocab { transno: 0, kind: PdVocabKind::CountStar }],
            sets: vec![
                PdSetSpec { att: 2, kind: DistinctKeyKind::Int64 },
                PdSetSpec { att: 3, kind: DistinctKeyKind::Int32 },
            ],
            max_att: 4,
            worker_budget: usize::MAX,
        })
    }

    /// Feed one (group, set, value) into a builder directly (the accept
    /// kernel minus the slot plumbing): probe/create + vocab bump + insert.
    fn feed(b: &mut PdBuilder<'static>, keys: &[i64], nulls: u32, j: usize, v: Option<i64>) {
        let nsets = b.spec.sets.len();
        let h = key_hash(keys, nulls);
        let (found, slot) = b.probe(keys, nulls, h);
        let g = match found {
            Some(g) => g,
            None => b.create_group(keys, nulls, h, slot),
        } as usize;
        b.states[g * 2 * b.spec.vocab.len()] += 1; // CountStar
        match v {
            Some(v) => b.dsets[g * nsets + j].insert_i64(v),
            None => b.dsets[g * nsets + j].seen_null = true,
        }
    }

    /// One deterministic worker's content (re-runnable: the reference and
    /// the spill arms must build identical inputs).
    fn build_worker(spec: &Arc<PdSpec>, salt: i64) -> PdBuilder<'static> {
        let mut b = PdBuilder::new(Arc::clone(spec), usize::MAX, None);
        for i in 0..4000i64 {
            let k = [(i * 13 + salt) % 37, ((i * 7 + salt) % 11) as i64];
            let nulls = if (i + salt) % 29 == 0 { 1 } else { 0 };
            feed(&mut b, &k, nulls, 0, Some((i * 104729 + salt) % 2500));
            feed(&mut b, &k, nulls, 1, Some(i % 97 - 40));
            if (i + salt) % 41 == 0 {
                feed(&mut b, &k, nulls, 1, None); // set NULL: seen_null face
            }
        }
        b
    }

    /// Canonical view of a merged bucket set: (keys, nulls, states,
    /// per-set sorted values + seen_null).
    fn canon(spec: &PdSpec, m: &PdMerged<'_>) -> Vec<(Vec<i64>, u32, Vec<i64>, Vec<(Vec<i64>, bool)>)> {
        let nkeys = spec.nkeys();
        let nsets = spec.sets.len();
        let nvocab = spec.vocab.len();
        let mut rows: Vec<_> = (0..m.ngroups)
            .map(|g| {
                let sets = (0..nsets)
                    .map(|j| {
                        let d = m.dsets[g * nsets + j].as_ref().unwrap();
                        let mut vals = d.ints().to_vec();
                        vals.sort_unstable();
                        (vals, d.seen_null)
                    })
                    .collect();
                (
                    m.keys[g * nkeys..(g + 1) * nkeys].to_vec(),
                    m.keynulls[g],
                    m.states[g * 2 * nvocab..(g + 1) * 2 * nvocab].to_vec(),
                    sets,
                )
            })
            .collect();
        rows.sort();
        rows
    }

    /// M3.5 §4 round-trip: drain values → records → rebuild → merge with the
    /// in-memory remainders EQUALS the direct (never-spilled) merge, on every
    /// bucket — groups, keynulls, vocab states, set values, and the
    /// seen_null face (which never touches the records).
    #[test]
    fn spill_roundtrip_merge_equivalence() {
        let spec = spill_test_spec();

        // Reference: direct merge of two never-spilled workers.
        let t1 = build_worker(&spec, 0).freeze().unwrap();
        let t2 = build_worker(&spec, 5).freeze().unwrap();
        let tables = [t1, t2];
        let direct = pd_concat_buckets(
            (0..PD_GROUP_PARTS).map(|b| pd_merge_bucket(&spec, &tables, b)).collect(),
        );

        // Spill arm: same two workers, drained mid-build (values → records),
        // then a second accept wave (cross-epoch duplicates included by
        // construction), remainder frozen, records replayed per bucket.
        let mut spilled: Vec<std::collections::HashMap<u32, Vec<u8>>> = Vec::new();
        let mut remainders: Vec<PdHandedTable> = Vec::new();
        for salt in [0i64, 5] {
            let mut b = build_worker(&spec, salt);
            assert!(b.spill_eligible());
            assert!(b.spill_value_bytes() > 0);
            let mut parts: std::collections::HashMap<u32, Vec<u8>> = Default::default();
            let mut last_p: i64 = -1;
            b.spill_emit(&mut |p, bytes| {
                assert!((p as i64) > last_p, "partitions ascend");
                last_p = p as i64;
                assert_eq!(bytes.len() % pd_spill_record_width(&spec), 0);
                parts.entry(p).or_default().extend_from_slice(bytes);
                Ok(())
            })
            .unwrap();
            b.spill_reset_values();
            // Second epoch: refeed a slice of the same content (duplicates
            // across epochs) plus fresh values; stays IN MEMORY (remainder).
            for i in 0..4000i64 {
                let k = [(i * 13 + salt) % 37, ((i * 7 + salt) % 11) as i64];
                let nulls = if (i + salt) % 29 == 0 { 1 } else { 0 };
                // Undo the double CountStar bump: subtract before refeeding.
                let h = key_hash(&k, nulls);
                let (found, _) = b.probe(&k, nulls, h);
                let g = found.expect("group exists from epoch 1") as usize;
                b.states[g * 2 * spec.vocab.len()] -= 1;
                feed(&mut b, &k, nulls, 0, Some((i * 104729 + salt) % 2500));
            }
            spilled.push(parts);
            remainders.push(b.freeze().unwrap());
        }
        let mut buckets = Vec::with_capacity(PD_GROUP_PARTS);
        for bkt in 0..PD_GROUP_PARTS {
            let mut synth: Vec<PdHandedTable> = Vec::new();
            for parts in &spilled {
                if let Some(bytes) = parts.get(&(bkt as u32)) {
                    synth.push(pd_table_from_spill(&spec, bytes).unwrap());
                }
            }
            let refs: Vec<&PdHandedTable> =
                remainders.iter().chain(synth.iter()).collect();
            buckets.push(pd_merge_bucket_refs(&spec, &refs, bkt));
        }
        let merged = pd_concat_buckets(buckets);

        assert_eq!(canon(&spec, &direct), canon(&spec, &merged));

        // Pre-count sanity: in-memory groups bound the merged groups; the
        // record width divides every partition's bytes (checked above).
        let mut groups = 0usize;
        for t in &tables {
            for bkt in 0..PD_GROUP_PARTS {
                groups += pd_bucket_precount(&spec, t, bkt).0;
            }
        }
        assert!(groups >= direct.ngroups);
    }

    /// Torn / corrupt records fail closed (never a silent wrong answer).
    #[test]
    fn spill_torn_record_fails_closed() {
        let spec = spill_test_spec();
        let width = pd_spill_record_width(&spec);
        assert_eq!(width, (2 + 3) * 8);
        // Torn: not a whole number of records.
        assert!(pd_table_from_spill(&spec, &vec![0u8; width + 1]).is_err());
        // Corrupt set index.
        let mut rec = vec![0u8; width];
        rec[(1 + spec.nkeys()) * 8..(2 + spec.nkeys()) * 8]
            .copy_from_slice(&(u64::MAX).to_ne_bytes());
        assert!(pd_table_from_spill(&spec, &rec).is_err());
        // Corrupt keynulls (bit beyond nkeys).
        let mut rec = vec![0u8; width];
        rec[..8].copy_from_slice(&(1u64 << 63).to_ne_bytes());
        assert!(pd_table_from_spill(&spec, &rec).is_err());
        // A well-formed empty image and a single record are fine.
        assert!(pd_table_from_spill(&spec, &[]).is_ok());
        assert!(pd_table_from_spill(&spec, &vec![0u8; width]).is_ok());
    }

    /// M3.5 inc-3b slice invariance: routing a bucket's spilled records by
    /// `mix64(value)` bytes and merging per-slice synth tables IN SEQUENCE
    /// (after the one-pass in-memory merge) equals the direct never-spilled
    /// merge on every bucket — groups, keynulls, vocab states, sorted set
    /// values, and the seen_null face — with every distinct (group, set,
    /// value) record in exactly one slice, at depth 1 and depth 2.
    #[test]
    fn split_slice_merge_invariance() {
        let spec = spill_test_spec();

        // Reference: direct merge of two never-spilled workers.
        let t1 = build_worker(&spec, 0).freeze().unwrap();
        let t2 = build_worker(&spec, 5).freeze().unwrap();
        let tables = [t1, t2];
        let direct = pd_concat_buckets(
            (0..PD_GROUP_PARTS).map(|b| pd_merge_bucket(&spec, &tables, b)).collect(),
        );

        // Spill arm: same two workers drained mid-build, then a second
        // accept wave (cross-epoch duplicates by construction) — the
        // inc-3a construction, reused.
        let mut spilled: Vec<std::collections::HashMap<u32, Vec<u8>>> = Vec::new();
        let mut remainders: Vec<PdHandedTable> = Vec::new();
        for salt in [0i64, 5] {
            let mut b = build_worker(&spec, salt);
            let mut parts: std::collections::HashMap<u32, Vec<u8>> = Default::default();
            b.spill_emit(&mut |p, bytes| {
                parts.entry(p).or_default().extend_from_slice(bytes);
                Ok(())
            })
            .unwrap();
            b.spill_reset_values();
            for i in 0..4000i64 {
                let k = [(i * 13 + salt) % 37, ((i * 7 + salt) % 11) as i64];
                let nulls = if (i + salt) % 29 == 0 { 1 } else { 0 };
                let h = key_hash(&k, nulls);
                let (found, _) = b.probe(&k, nulls, h);
                let g = found.expect("group exists from epoch 1") as usize;
                b.states[g * 2 * spec.vocab.len()] -= 1;
                feed(&mut b, &k, nulls, 0, Some((i * 104729 + salt) % 2500));
            }
            spilled.push(parts);
            remainders.push(b.freeze().unwrap());
        }

        let width = pd_spill_record_width(&spec);
        for depth in [1u32, 2] {
            let mut buckets = Vec::with_capacity(PD_GROUP_PARTS);
            for bkt in 0..PD_GROUP_PARTS {
                // Every Local's bucket records concatenated (the runtime
                // streams all Locals' partitions through one router).
                let mut bytes = Vec::new();
                for parts in &spilled {
                    if let Some(bb) = parts.get(&(bkt as u32)) {
                        bytes.extend_from_slice(bb);
                    }
                }
                let mut slices: Vec<Vec<u8>> = vec![Vec::new(); PD_GROUP_PARTS];
                pd_route_value_records(&spec, &bytes, depth, &mut slices).unwrap();
                // Routing loses nothing and duplicates nothing…
                assert_eq!(slices.iter().map(|s| s.len()).sum::<usize>(), bytes.len());
                // …and every distinct record (group identity + set + value)
                // has exactly one home slice.
                let mut home: std::collections::HashMap<Vec<u8>, usize> = Default::default();
                for (si, s) in slices.iter().enumerate() {
                    for r in s.chunks(width) {
                        if let Some(prev) = home.insert(r.to_vec(), si) {
                            assert_eq!(prev, si, "record in two slices");
                        }
                    }
                }
                // The split combine: in-memory tables ONCE, then each
                // slice's synth table in sequence, dropped between absorbs.
                let mut merger = PdBucketMerger::new(&spec);
                for t in &remainders {
                    merger.absorb(t, bkt);
                }
                for s in &slices {
                    if s.is_empty() {
                        continue;
                    }
                    let synth = pd_table_from_spill(&spec, s).unwrap();
                    merger.absorb(&synth, bkt);
                }
                buckets.push(merger.finish());
            }
            let merged = pd_concat_buckets(buckets);
            assert_eq!(canon(&spec, &direct), canon(&spec, &merged), "depth {depth}");
        }
    }

    /// Torn / out-of-range input to the value router fails closed; a single
    /// record routes to exactly the mix64-top-byte slice.
    #[test]
    fn value_route_torn_fails_closed() {
        let spec = spill_test_spec();
        let width = pd_spill_record_width(&spec);
        let mut out: Vec<Vec<u8>> = vec![Vec::new(); PD_GROUP_PARTS];
        // Torn: not a whole number of records.
        assert!(pd_route_value_records(&spec, &vec![0u8; width + 1], 1, &mut out).is_err());
        // Depth outside the routing vocabulary.
        assert!(pd_route_value_records(&spec, &vec![0u8; width], 0, &mut out).is_err());
        assert!(pd_route_value_records(&spec, &vec![0u8; width], 7, &mut out).is_err());
        // Empty image routes nothing.
        assert!(pd_route_value_records(&spec, &[], 1, &mut out).is_ok());
        assert!(out.iter().all(|s| s.is_empty()));
        // One record lands in exactly the depth-1 (top-byte) slice.
        let mut rec = vec![0u8; width];
        rec[width - 8..].copy_from_slice(&77i64.to_ne_bytes());
        pd_route_value_records(&spec, &rec, 1, &mut out).unwrap();
        let expect = (mix64(77i64 as u64) >> 56) as usize;
        assert_eq!(out[expect].len(), width);
        assert_eq!(out.iter().map(|s| s.len()).sum::<usize>(), width);
    }
}
