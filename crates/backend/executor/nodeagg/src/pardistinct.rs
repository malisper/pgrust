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
    fn read(self, d: Datum) -> i64 {
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
    let nkeys = spec.nkeys();
    let nvocab = spec.vocab.len();
    let nsets = spec.sets.len();
    let mut out = PdMerged {
        ngroups: 0,
        keys: Vec::new(),
        keynulls: Vec::new(),
        states: Vec::new(),
        dsets: Vec::new(),
    };
    // Bucket-local open-addressed probe over the output groups.
    let mut table: Vec<u32> = vec![0; 64];
    let mut hashes: Vec<u64> = Vec::new();
    for t in tables {
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
                            table = nt;
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
    out
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

/// Map a transfn to its vocab kind given the (single) argument's outer
/// attno + width. The oids are `order_insensitive_exact_transfn`'s, minus
/// the Int128 family (int8_avg_accum — v1 refusal).
pub(crate) fn vocab_kind(transfn_oid: Oid, att: Option<(u16, PdInt)>) -> Option<PdVocabKind> {
    const F_INT8INC: Oid = 1219;
    const F_INT8INC_ANY: Oid = 2804;
    const F_INT2_SUM: Oid = 1840;
    const F_INT4_SUM: Oid = 1841;
    const F_INT2_AVG_ACCUM: Oid = 1962;
    const F_INT4_AVG_ACCUM: Oid = 1963;
    match transfn_oid {
        F_INT8INC => Some(PdVocabKind::CountStar),
        F_INT8INC_ANY => att.map(|(a, _)| PdVocabKind::CountAny { att: a }),
        F_INT2_SUM => att.and_then(|(a, k)| {
            (k == PdInt::I16).then_some(PdVocabKind::SumInt { att: a, kind: k })
        }),
        F_INT4_SUM => att.and_then(|(a, k)| {
            (k == PdInt::I32).then_some(PdVocabKind::SumInt { att: a, kind: k })
        }),
        F_INT2_AVG_ACCUM => att.and_then(|(a, k)| {
            (k == PdInt::I16).then_some(PdVocabKind::AvgInt { att: a, kind: k })
        }),
        F_INT4_AVG_ACCUM => att.and_then(|(a, k)| {
            (k == PdInt::I32).then_some(PdVocabKind::AvgInt { att: a, kind: k })
        }),
        _ => None,
    }
}

/// Uniform internal error for impossible wire states (defensive; never
/// expected to fire).
#[allow(dead_code)]
#[cold]
pub(crate) fn pd_internal(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(format!("pardistinct internal: {msg}")))
}
