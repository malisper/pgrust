//! Parallel PLAIN exact-DISTINCT partial state (band-2b) — the zero-group-key
//! twin of `pardistinct.rs` for the ungrouped count(DISTINCT) shape:
//! `Aggregate(AGG_PLAIN, all-DISTINCT) → [Sort →] SeqScan`.
//!
//! The grouped runtime distinct sink partitions its combine space by GROUP
//! hash — with zero group columns everything lands in one partition and the
//! merge serializes, so this twin partitions by the DISTINCT VALUE hash
//! instead: every worker builds `PLAIN_PD_PARTS` value-partitioned
//! [`DistinctSet`]s; the combine task set claims one partition index and
//! unions the workers' slices of that partition (disjoint by construction —
//! partition is a pure function of the value); the leader concatenates the
//! merged partitions into ONE replay-only set ([`DistinctSet::from_values`])
//! and installs it into the plain agg's set-mode pertrans slot, where the
//! ordinary `agg_plain_finish` replay (count shortcut included) finishes the
//! node.
//!
//! Value identity: admission is exactly the serial set-mode admission
//! (`distinct_set_kind` established `set_kind` at init — representational
//! equality proven there, deterministic-collation text included), plus the
//! plain direct shape (`direct_att == Some(0)`, no FILTER). The parallel
//! split changes only the set INSERTION order, which the admitted
//! transitions cannot observe (the distinctset.rs module-doc argument); the
//! replay multiset is identical to the serial arm's. NULLs elide into
//! per-worker `seen_null` flags OR-reduced at install — the same one-NULL
//! collapse the serial set performs.
//!
//! Budget law (matched to the grouped sink): each worker Local carries
//! `worker_budget = distinct_set_budget() / 2`; a crossing flips the feed's
//! `crossed` flag and the engagement aborts to the serial fallback (the
//! phase-1 refusal law — no spill arm in v1; the serial rerun recomputes
//! from scratch, value-identically). The combine additionally checks the
//! ADMITTED envelope (forked Locals × worker_budget) exactly as the grouped
//! sink does.

use ::datum::Datum;
use ::types_error::PgResult;

use ::executils::{EStateData, EcxtId};

use crate::distinctset::{DistinctKeyKind, DistinctSet};
use crate::AggStateData;

/// Value-hash partition count. 256 keeps the combine task set ≥ 4x claims
/// at DOP > 64 (c8g.48xlarge readiness: 192 vCPU); per-worker fixed overhead
/// stays small (sets are lazily allocated per partition).
pub const PLAIN_PD_PARTS: usize = 256;

/// SplitMix64 finalizer — the partition router. Any deterministic mixer
/// works (partitioning must only agree across workers); this one is
/// independent of the sets' internal probe hashing by construction.
#[inline]
fn route64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E3779B97F4A7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

#[inline]
fn part_of_int(k: i64) -> usize {
    (route64(k as u64) >> 56) as usize // top 8 bits → 0..256
}

#[inline]
fn part_of_bytes(content: &[u8]) -> usize {
    // FNV-1a then route: cheap, deterministic, worker-independent.
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in content {
        h = (h ^ b as u64).wrapping_mul(0x100000001b3);
    }
    (route64(h) >> 56) as usize
}

/// The admitted shape, derived once on the leader (`plain_pd_derive_spec`).
pub struct PlainPdSpec {
    /// The single set transition's argument: 0-based OUTER attno. v1 pins
    /// this to column 0 (the staged direct-key feed's own requirement).
    pub att: u16,
    pub(crate) kind: DistinctKeyKind,
    /// Per-worker Local budget (bytes) — `distinct_set_budget() / 2`, the
    /// grouped sink's law.
    pub worker_budget: usize,
}

impl PlainPdSpec {
    #[inline]
    pub fn is_bytes(&self) -> bool {
        matches!(self.kind, DistinctKeyKind::Bytes)
    }

    #[inline]
    pub fn kind_is_i16(&self) -> bool {
        matches!(self.kind, DistinctKeyKind::Int16)
    }

    #[inline]
    pub fn kind_is_i32(&self) -> bool {
        matches!(self.kind, DistinctKeyKind::Int32)
    }
}

/// Derive the parallel plain-distinct spec. `None` = shape refused (the
/// caller falls to the serial drives, value-identically).
///
/// Gates: every transition is set-mode (`agg_plain_distinct_set_only` — the
/// presorted entries force-arm under the skip-sort law the caller already
/// applies), exactly ONE transition, its argument a bare OUTER column-0 Var
/// with no FILTER (the direct staged-key shape), int2/int4/int8 or
/// deterministic-collation text/varchar (proven at init by
/// `distinct_set_kind`).
pub fn plain_pd_derive_spec(node: &AggStateData<'_>) -> Option<std::sync::Arc<PlainPdSpec>> {
    if !crate::agg_plain_distinct_set_only(node) {
        return None;
    }
    if node.pertrans_sort.len() != 1 {
        return None;
    }
    let ps = &node.pertrans_sort[0];
    let kind = ps.set_kind?;
    if ps.num_inputs != 1 {
        return None;
    }
    // The direct staged-key contract: single bare OUTER var, no FILTER
    // (recorded at init). Column 0 is the projected over-Sort/over-scan
    // shape (`agg_plain_distinct_direct_shape`); a nonzero column is the
    // PRESORTED-bare physical-tlist face (GL-DISTALPHA-2), where scan
    // output == table row, so the recorded OUTER attno IS the scan column
    // the workers stage — the probe's `seq_scan_key_direct_att` proof
    // (which refuses any projected scan) is what makes that identity safe.
    let att = ps.direct_att?;
    Some(std::sync::Arc::new(PlainPdSpec {
        att,
        kind,
        worker_budget: crate::distinct_set_budget() / 2,
    }))
}

/// One worker's partial: value-partitioned exact sets + routing scratch.
///
/// Send soundness: a Local is touched by exactly one thread at a time (the
/// SealedParallelSink contract — accept by its worker, seal by its claimer,
/// merged reads by the combine claimer). The contained [`DistinctSet`]s are
/// `!Send` only through (i) the stringhash probe tables' `NonNull` cells —
/// global-allocator memory (`std::alloc`), sound to move/drop across
/// threads — and (ii) the `SpillState` variant, which this module NEVER
/// constructs (nothing here calls `spill_flush`; a budget crossing aborts
/// to the serial rerun instead). The sealed form carries the same argument.
pub struct PlainPdLocal {
    parts: Vec<DistinctSet<'static>>,
    /// Per-partition int routing buffers (reused across windows).
    route_ints: Vec<Vec<i64>>,
    hashes: Vec<u64>,
    seen_null: bool,
    budget: usize,
    crossed: bool,
}

// SAFETY: single-toucher discipline + global-allocator probe tables + the
// never-spilled invariant (struct doc above).
unsafe impl Send for PlainPdLocal {}

impl PlainPdLocal {
    pub fn new(budget: usize) -> PlainPdLocal {
        PlainPdLocal {
            parts: (0..PLAIN_PD_PARTS).map(|_| DistinctSet::new()).collect(),
            route_ints: vec![Vec::new(); PLAIN_PD_PARTS],
            hashes: Vec::new(),
            seen_null: false,
            budget,
            crossed: false,
        }
    }

    #[inline]
    pub fn crossed(&self) -> bool {
        self.crossed
    }

    fn check_budget(&mut self) {
        let total: usize = self.parts.iter().map(|s| s.mem_bytes()).sum();
        if total > self.budget {
            self.crossed = true;
        }
    }

    /// One staged key-lane window: `vals`/`isnull` in row order (the serial
    /// `agg_plain_distinct_insert_lane_batch` twin, partitioned). `kind`
    /// must be the spec's integer kind.
    pub fn accept_lane_ints(&mut self, kind_i16: bool, kind_i32: bool, vals: &[Datum], isnull: &[bool]) {
        if self.crossed {
            return;
        }
        debug_assert_eq!(vals.len(), isnull.len());
        for b in self.route_ints.iter_mut() {
            b.clear();
        }
        for (&d, &nl) in vals.iter().zip(isnull) {
            if nl {
                self.seen_null = true;
                continue;
            }
            let k = if kind_i16 {
                d.as_i16() as i64
            } else if kind_i32 {
                d.as_i32() as i64
            } else {
                d.as_i64()
            };
            self.route_ints[part_of_int(k)].push(k);
        }
        for (p, buf) in self.route_ints.iter().enumerate() {
            if !buf.is_empty() {
                self.parts[p].insert_i64_batch(buf, &mut self.hashes);
            }
        }
        self.check_budget();
    }

    /// One collected batch of NON-NULL key datums (the `emit_key` fallback
    /// staging), integer kinds.
    pub fn accept_datums_int(&mut self, kind_i16: bool, kind_i32: bool, keys: &[Datum], saw_null: bool) {
        if self.crossed {
            return;
        }
        if saw_null {
            self.seen_null = true;
        }
        for b in self.route_ints.iter_mut() {
            b.clear();
        }
        for &d in keys {
            let k = if kind_i16 {
                d.as_i16() as i64
            } else if kind_i32 {
                d.as_i32() as i64
            } else {
                d.as_i64()
            };
            self.route_ints[part_of_int(k)].push(k);
        }
        for (p, buf) in self.route_ints.iter().enumerate() {
            if !buf.is_empty() {
                self.parts[p].insert_i64_batch(buf, &mut self.hashes);
            }
        }
        self.check_budget();
    }

    /// One collected batch of NON-NULL text key datums: detoast into the
    /// caller's per-tuple context (`tmp`, reset by the caller per batch) and
    /// insert content bytes (the serial `agg_plain_distinct_insert_bytes_batch`
    /// twin, partitioned).
    pub fn accept_bytes_datums(
        &mut self,
        estate: &mut EStateData<'_>,
        tmp: EcxtId,
        keys: &[Datum],
        saw_null: bool,
    ) -> PgResult<()> {
        if self.crossed {
            return Ok(());
        }
        if saw_null {
            self.seen_null = true;
        }
        for &d in keys {
            // SAFETY: non-null live text/varchar varlena — admission proved
            // the argument type; detoast copies land in per-tuple memory.
            let v = unsafe { ::types_fmgr::datum_varlena_packed(d, estate.ecxt(tmp).per_tuple_mcx()) }?;
            let c = v.data();
            self.parts[part_of_bytes(c)].insert_bytes(c);
        }
        self.check_budget();
        Ok(())
    }

    /// One dict-coded text window (the pgrcolumnar zero-decode lane): the
    /// caller's identity-scoped `memo` filters repeat codes exactly as the
    /// serial `agg_plain_distinct_insert_dict_batch` does; novel codes
    /// detoast + route by content hash.
    pub fn accept_dict_window(
        &mut self,
        estate: &mut EStateData<'_>,
        tmp: EcxtId,
        codes: &[u32],
        dict: &[Datum],
        stitch: Option<&[u32]>,
        memo: &mut [u64],
    ) -> PgResult<()> {
        if self.crossed {
            return Ok(());
        }
        debug_assert!(stitch.is_none_or(|s| s.len() == dict.len()));
        let bit = |c: u32| -> usize {
            match stitch {
                Some(s) => s[c as usize] as usize,
                None => c as usize,
            }
        };
        for &c in codes {
            let i = bit(c);
            let (w, b) = (i / 64, i % 64);
            if memo[w] >> b & 1 == 0 {
                memo[w] |= 1 << b;
                // SAFETY: dict entries are live decoded text varlena images.
                let v = unsafe {
                    ::types_fmgr::datum_varlena_packed(dict[c as usize], estate.ecxt(tmp).per_tuple_mcx())
                }?;
                let content = v.data();
                self.parts[part_of_bytes(content)].insert_bytes(content);
            }
        }
        self.check_budget();
        Ok(())
    }

    /// Freeze into the combine-readable form (a plain move — the partition
    /// split already happened at insert).
    pub fn seal(self) -> PlainPdSealed {
        PlainPdSealed {
            parts: self.parts.into_iter().map(core::cell::UnsafeCell::new).collect(),
            seen_null: self.seen_null,
        }
    }
}

/// A frozen worker partial. Partition cells are `UnsafeCell` so the
/// low-width combine can TAKE a live set out of its sole claimer's
/// partition (see [`plain_pd_combine_steal`]); the generic combine reads
/// through the same cells.
pub struct PlainPdSealed {
    parts: Vec<core::cell::UnsafeCell<DistinctSet<'static>>>,
    seen_null: bool,
}

// SAFETY: the PlainPdLocal argument verbatim (sealed = the same sets,
// moved); combine claimers read disjoint partitions, one claimer per call.
unsafe impl Send for PlainPdSealed {}
// SAFETY: the sink contract visits each partition index EXACTLY ONCE, by a
// single claimer, across the whole sealed slice — so for every `p`, cell
// `parts[p]` of every sealed partial is touched (read OR taken) only by
// partition p's claimer, and never again: shared references never coexist
// with the mutation. `seen_null` is a plain bool read by finalize, which
// the runtime orders after every combine.
unsafe impl Sync for PlainPdSealed {}

impl PlainPdSealed {
    /// An empty sealed partial (poisoned/aborting workers hand this in; it
    /// unions as a no-op).
    pub fn empty() -> PlainPdSealed {
        PlainPdSealed { parts: Vec::new(), seen_null: false }
    }

    pub fn seen_null(&self) -> bool {
        self.seen_null
    }

    /// Partition `p`'s set, by shared reference. SAFETY (caller): only
    /// partition `p`'s combine claimer may call this, and not after a
    /// `take_part(p)` on the same sealed partial.
    #[inline]
    fn part(&self, p: usize) -> Option<&DistinctSet<'static>> {
        // SAFETY: single-claimer-per-partition contract (struct doc).
        self.parts.get(p).map(|c| unsafe { &*c.get() })
    }

    /// Move partition `p`'s set out (the low-width steal base). SAFETY
    /// (caller): only partition `p`'s combine claimer, at most once, with
    /// no live `part(p)` borrow.
    #[inline]
    fn take_part(&self, p: usize) -> Option<DistinctSet<'static>> {
        // SAFETY: single-claimer-per-partition contract (struct doc); the
        // replaced empty set keeps drops sound.
        self.parts.get(p).map(|c| unsafe { core::mem::replace(&mut *c.get(), DistinctSet::new()) })
    }

    /// Approximate memory of this partial (the combine envelope check).
    /// Leader-side only (before the combine set runs — never concurrent
    /// with claims).
    pub fn mem_bytes(&self) -> usize {
        // SAFETY: leader-side, pre-combine (doc above).
        self.parts.iter().map(|c| unsafe { (*c.get()).mem_bytes() }).sum()
    }
}

/// One merged value partition: already-deduplicated values in the
/// `from_values` wire shape.
pub struct PlainPdMerged {
    ints: Vec<i64>,
    blob: Vec<u8>,
    spans: Vec<(u32, u32, u32)>,
}

/// Union partition `part` across every worker's sealed partial. Partitions
/// are value-disjoint across indexes, so each claim merges independently.
pub fn plain_pd_combine(kind_bytes: bool, part: usize, sealed: &[PlainPdSealed]) -> PlainPdMerged {
    let mut set: DistinctSet<'static> = DistinctSet::new();
    let mut hashes: Vec<u64> = Vec::new();
    for s in sealed {
        let Some(p) = s.part(part) else { continue };
        if kind_bytes {
            for i in 0..p.n_bytes() {
                let (off, len, _h) = p.bytes_span(i);
                set.insert_bytes(p.bytes_content(off, len));
            }
        } else {
            set.insert_i64_batch(p.ints(), &mut hashes);
        }
    }
    export_merged(kind_bytes, set)
}

/// GL-LOWDIST-1 low-width combine: SIZE-ASYMMETRIC union — TAKE the largest
/// donor's live set (probe table intact — its values are never re-hashed,
/// re-probed, or copied; `take_ints` later moves its value vec out
/// wholesale) and insert only the OTHER donors' values into it. At width
/// 2-4 the largest donor is most of the partition, so most of the generic
/// combine's insert work vanishes.
///
/// Value identity: the merged VALUE SET is the same union (set insertion is
/// idempotent; representational equality unchanged); only set-internal
/// insertion ORDER differs, which the admitted order-insensitive replays
/// cannot observe (distinctset.rs module doc). The SELECT-DISTINCT
/// sub-arm's emitted row order within a partition follows the base donor
/// instead of worker 0 — DISTINCT row order is a non-surface (unordered
/// plan), and the sink's order was already engagement-shaped.
///
/// SAFETY: relies on the sink contract — this claimer is partition
/// `part`'s SOLE toucher across `sealed` (PlainPdSealed struct doc).
pub fn plain_pd_combine_steal(
    kind_bytes: bool,
    part: usize,
    sealed: &[PlainPdSealed],
) -> PlainPdMerged {
    // Choose the largest donor (ties: first).
    let mut base: Option<(usize, usize)> = None; // (index, len)
    let mut total = 0usize;
    for (i, s) in sealed.iter().enumerate() {
        let Some(p) = s.part(part) else { continue };
        let l = p.len();
        total += l;
        if base.is_none_or(|(_, bl)| l > bl) && l > 0 {
            base = Some((i, l));
        }
    }
    let Some((bi, blen)) = base else {
        return PlainPdMerged { ints: Vec::new(), blob: Vec::new(), spans: Vec::new() };
    };
    let mut set = sealed[bi].take_part(part).expect("chosen donor holds the partition");
    if total > blen {
        // Union ≤ total (exact pre-size; the projection gate keeps small
        // legacy-arm sets untouched).
        set.reserve_projected(total);
    }
    let mut hashes: Vec<u64> = Vec::new();
    for (i, s) in sealed.iter().enumerate() {
        if i == bi {
            continue;
        }
        let Some(p) = s.part(part) else { continue };
        if kind_bytes {
            for j in 0..p.n_bytes() {
                let (off, len, _h) = p.bytes_span(j);
                set.insert_bytes(p.bytes_content(off, len));
            }
        } else {
            set.insert_i64_batch(p.ints(), &mut hashes);
        }
    }
    export_merged(kind_bytes, set)
}

/// Export the merged values (the set is spent) — shared combine tail.
fn export_merged(kind_bytes: bool, mut set: DistinctSet<'static>) -> PlainPdMerged {
    if kind_bytes {
        let n = set.n_bytes();
        let mut blob = Vec::new();
        let mut spans = Vec::with_capacity(n);
        for i in 0..n {
            let (off, len, h) = set.bytes_span(i);
            let c = set.bytes_content(off, len);
            spans.push((blob.len() as u32, len, h));
            blob.extend_from_slice(c);
        }
        PlainPdMerged { ints: Vec::new(), blob, spans }
    } else {
        PlainPdMerged { ints: set.take_ints(), blob: Vec::new(), spans: Vec::new() }
    }
}

impl PlainPdMerged {
    pub fn len(&self) -> usize {
        self.ints.len().max(self.spans.len())
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Retained bytes of this merged partition (the combine envelope meter).
    pub fn mem_bytes(&self) -> usize {
        self.ints.len() * 8 + self.blob.len() + self.spans.len() * 12
    }
}

/// SE-T2AGG CAR A (distinct-plain-shape): derive the parallel plain
/// SELECT-DISTINCT spec — the `Agg(AGG_HASHED, zero aggregates) → SeqScan`
/// HashAggregate shape the serial hash-agg breaker owns today. `Ok(None)` =
/// shape refused (the caller falls to the serial arms, value-identically).
///
/// Gates (v1, fail-closed):
///   * AGG_HASHED, AGGSPLIT_SIMPLE, no HAVING, ZERO transitions (numtrans ==
///     0, no peragg, no pertrans_sort) — the pure-dedup node;
///   * exactly ONE grouping column = OUTER column 1 (the staged direct-key
///     feed's own col-0 requirement, the plain count(DISTINCT) discipline);
///   * the Agg's targetlist is exactly that one key Var (identity emit);
///   * the grouping equality is REPRESENTATIONAL on the stored key — the
///     `distinct_set_kind` equality matrix verbatim: int2/int4/int8eq over
///     the int family (sign-extended-word equality), or texteq over
///     text/varchar under a DETERMINISTIC collation (byte equality of
///     detoasted content — exactly `DistinctSet`'s byte key; nondeterministic
///     collations refuse).
///
/// `key_typ` is the scan-tlist column-0 type (the caller extracts it from
/// the proven SeqScan child — this module never walks plan trees).
pub fn plain_sd_derive_spec(
    node: &AggStateData<'_>,
    key_typ: ::types_core::Oid,
) -> PgResult<Option<std::sync::Arc<PlainPdSpec>>> {
    use ::types_pathnodes::{AGGSPLIT_SIMPLE, AGG_HASHED};
    const F_INT2EQ: ::types_core::Oid = 63;
    const F_INT4EQ: ::types_core::Oid = 65;
    const F_TEXTEQ: ::types_core::Oid = 67;
    const F_INT8EQ: ::types_core::Oid = 467;
    const INT2OID: ::types_core::Oid = 21;
    const INT4OID: ::types_core::Oid = 23;
    const INT8OID: ::types_core::Oid = 20;
    const TEXTOID: ::types_core::Oid = 25;
    const VARCHAROID: ::types_core::Oid = 1043;
    if node.plan.aggstrategy != AGG_HASHED || node.plan.aggsplit != AGGSPLIT_SIMPLE {
        return Ok(None);
    }
    if node.numtrans != 0 || !node.peragg.is_empty() || !node.pertrans_sort.is_empty() {
        return Ok(None);
    }
    if node.qual.is_some() {
        return Ok(None);
    }
    let [grp_col] = node.plan.grpColIdx else { return Ok(None) };
    if *grp_col < 1 || node.plan.grpOperators.len() != 1 {
        return Ok(None);
    }
    // Identity emit: the one tlist entry is the one key Var (the grouping
    // column — an arbitrary scan OUTPUT column: AGG_HASHED keeps the scan's
    // physical tlist, so the key need not be column 0; the staging arm
    // stages that exact column).
    let tlist = &node.plan.plan.targetlist;
    if tlist.len() != 1 {
        return Ok(None);
    }
    let Some(te) = tlist.nth(0).as_target_entry() else { return Ok(None) };
    let Some(v) = te.expr.as_var() else { return Ok(None) };
    if v.varno != ::execexpr::OUTER_VAR || v.varlevelsup != 0 || v.varattno != *grp_col {
        return Ok(None);
    }
    let eq_proc = ::lsyscache::get_opcode(node.plan.grpOperators[0])?;
    let collation = node.plan.grpCollations.first().copied().unwrap_or(0);
    let kind = match (eq_proc, key_typ) {
        (F_INT2EQ, INT2OID) => DistinctKeyKind::Int16,
        (F_INT4EQ, INT4OID) => DistinctKeyKind::Int32,
        (F_INT8EQ, INT8OID) => DistinctKeyKind::Int64,
        (F_TEXTEQ, TEXTOID | VARCHAROID)
            if collation != 0 && ::lsyscache::get_collation_isdeterministic(collation)? =>
        {
            DistinctKeyKind::Bytes
        }
        _ => return Ok(None),
    };
    Ok(Some(std::sync::Arc::new(PlainPdSpec {
        att: (*grp_col - 1) as u16,
        kind,
        worker_budget: crate::distinct_set_budget() / 2,
    })))
}

/// SE-T2AGG CAR A: materialize the merged distinct VALUES as the sink's
/// emit buffers — exactly [`crate::sink::SINK_NBUCKETS`] bufs (partition i
/// → bucket i), one output column (the key). Ints ride byval datums at the
/// spec's width (the sign-extended-word identity the equality admitted);
/// bytes wrap in 4B-header text varlenas in each buf's own arena (equal
/// payload bytes = the serial path's text value; header form is
/// representation, not identity). `seen_null` appends one SQL-NULL row
/// (DISTINCT groups all NULLs together) — unreachable on the cbstore feeds
/// (the AM refuses NULLs) but exact if a feed ever carries one.
///
/// Emit order is partition-then-set order — DIVERGENT from the serial hash
/// table's insertion order, legal under the 2026-07-13 order-relaxation
/// policy (same rows/values; group order free unless SQL mandates it — the
/// probe refuses ORDER BY shapes).
pub fn plain_sd_emit_bufs(
    spec: &PlainPdSpec,
    merged: &[PlainPdMerged],
    seen_null: bool,
) -> Vec<crate::sink::SinkEmitBuf> {
    let mut bufs = Vec::with_capacity(crate::sink::SINK_NBUCKETS);
    for b in 0..crate::sink::SINK_NBUCKETS {
        let Some(m) = merged.get(b).filter(|m| !m.is_empty()) else {
            bufs.push(crate::sink::SinkEmitBuf::default());
            continue;
        };
        let n = m.len();
        let mut values: Vec<Datum> = Vec::with_capacity(n);
        let mut arena: Vec<u8> = Vec::new();
        if spec.is_bytes() {
            arena.reserve(m.blob.len() + m.spans.len() * 12);
            let mut offs: Vec<usize> = Vec::with_capacity(n);
            for &(off, len, _h) in &m.spans {
                // 8-align each image (varlena consumers may read aligned
                // payloads — the sink emit arena's own law).
                let pad = (8 - arena.len() % 8) % 8;
                arena.resize(arena.len() + pad, 0);
                offs.push(arena.len());
                let hdr = ::datum::varlena::set_varsize_4b(
                    len as usize + ::datum::varlena::VARHDRSZ,
                );
                arena.extend_from_slice(&hdr);
                arena.extend_from_slice(&m.blob[off as usize..(off + len) as usize]);
            }
            // The arena is final — resolve the datums (Vec growth may have
            // moved the buffer during the loop).
            for o in offs {
                values.push(Datum::from_usize(arena[o..].as_ptr() as usize));
            }
        } else {
            for &k in &m.ints {
                values.push(match spec.kind {
                    DistinctKeyKind::Int16 => Datum::from_i16(k as i16),
                    DistinctKeyKind::Int32 => Datum::from_i32(k as i32),
                    _ => Datum::from_i64(k),
                });
            }
        }
        bufs.push(crate::sink::SinkEmitBuf {
            values,
            nulls: vec![false; n],
            nrows: n,
            arena,
        });
    }
    if seen_null {
        // The NULL group row rides the sink's own NULL bucket position.
        let b = crate::sink::SINK_NULL_BUCKET;
        bufs[b].values.push(Datum::null());
        bufs[b].nulls.push(true);
        bufs[b].nrows += 1;
    }
    bufs
}

/// Install the merged partitions as the plain agg's replay-only set and let
/// the ordinary set-mode finalize run. The caller must have run
/// `agg_plain_build_begin` (fresh pergroups) and, on the skip-sort shape,
/// `agg_force_distinct_set` — exactly the serial drives' sequence.
pub fn agg_plain_install_merged_set(
    node: &mut AggStateData<'_>,
    merged: Vec<PlainPdMerged>,
    seen_null: bool,
) {
    let ps = &mut node.pertrans_sort[0];
    let kind = ps.set_kind.expect("set-mode pertrans");
    let set = if matches!(kind, DistinctKeyKind::Bytes) {
        let mut blob = Vec::with_capacity(merged.iter().map(|m| m.blob.len()).sum());
        let mut spans = Vec::with_capacity(merged.iter().map(|m| m.spans.len()).sum());
        for m in &merged {
            let base = blob.len() as u32;
            blob.extend_from_slice(&m.blob);
            spans.extend(m.spans.iter().map(|&(off, len, h)| (base + off, len, h)));
        }
        DistinctSet::from_values(kind, Vec::new(), blob, spans, seen_null)
    } else {
        let mut ints = Vec::with_capacity(merged.iter().map(|m| m.ints.len()).sum());
        for m in &merged {
            ints.extend_from_slice(&m.ints);
        }
        DistinctSet::from_values(kind, ints, Vec::new(), Vec::new(), seen_null)
    };
    ps.dset = Some(set);
    ps.dset_degraded = false;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn local() -> PlainPdLocal {
        PlainPdLocal::new(usize::MAX / 2)
    }

    fn merged_int_values(merged: &[PlainPdMerged]) -> Vec<i64> {
        let mut v: Vec<i64> = merged.iter().flat_map(|m| m.ints.iter().copied()).collect();
        v.sort_unstable();
        v
    }

    fn merged_bytes_values(merged: &[PlainPdMerged]) -> Vec<Vec<u8>> {
        let mut v: Vec<Vec<u8>> = merged
            .iter()
            .flat_map(|m| {
                m.spans.iter().map(|&(off, len, _h)| {
                    m.blob[off as usize..off as usize + len as usize].to_vec()
                })
            })
            .collect();
        v.sort();
        v
    }

    /// Cross-worker duplicates union to one; partitioning is worker-independent.
    #[test]
    fn int_union_across_workers() {
        let mut a = local();
        let mut b = local();
        let mut c = local();
        a.accept_datums_int(false, false, &[Datum::from_i64(1), Datum::from_i64(-7)], false);
        a.accept_datums_int(false, false, &[Datum::from_i64(42)], false);
        b.accept_datums_int(false, false, &[Datum::from_i64(-7), Datum::from_i64(99)], false);
        c.accept_datums_int(false, false, &[Datum::from_i64(42), Datum::from_i64(1)], true);
        let sealed = vec![a.seal(), b.seal(), c.seal()];
        assert!(sealed[2].seen_null());
        assert!(!sealed[0].seen_null());
        let merged: Vec<PlainPdMerged> =
            (0..PLAIN_PD_PARTS).map(|p| plain_pd_combine(false, p, &sealed)).collect();
        assert_eq!(merged_int_values(&merged), vec![-7, 1, 42, 99]);
    }

    /// i32 sign extension matches the serial set (int4eq semantics).
    #[test]
    fn int32_sign_extension() {
        let mut a = local();
        a.accept_datums_int(false, true, &[Datum::from_i32(-1), Datum::from_i32(-1)], false);
        let sealed = vec![a.seal()];
        let merged: Vec<PlainPdMerged> =
            (0..PLAIN_PD_PARTS).map(|p| plain_pd_combine(false, p, &sealed)).collect();
        assert_eq!(merged_int_values(&merged), vec![-1i64]);
    }

    /// Bytes union round-trips content exactly, deduped across workers.
    #[test]
    fn bytes_union_across_workers() {
        // Drive insert_bytes directly through the partition router (the
        // datum path needs a live EState; content routing is the unit here).
        let mut a = local();
        let mut b = local();
        for s in [b"alpha".as_slice(), b"beta".as_slice(), b"".as_slice()] {
            a.parts[part_of_bytes(s)].insert_bytes(s);
        }
        for s in [b"beta".as_slice(), b"gamma".as_slice()] {
            b.parts[part_of_bytes(s)].insert_bytes(s);
        }
        let sealed = vec![a.seal(), b.seal()];
        let merged: Vec<PlainPdMerged> =
            (0..PLAIN_PD_PARTS).map(|p| plain_pd_combine(true, p, &sealed)).collect();
        let vals = merged_bytes_values(&merged);
        assert_eq!(vals, vec![b"".to_vec(), b"alpha".to_vec(), b"beta".to_vec(), b"gamma".to_vec()]);
        let n: usize = merged.iter().map(|m| m.len()).sum();
        assert_eq!(n, 4);
    }

    /// A tiny budget crosses and freezes the local (fail-closed).
    #[test]
    fn budget_crossing_flips() {
        let mut a = PlainPdLocal::new(1);
        let vals: Vec<Datum> = (0..1000i64).map(Datum::from_i64).collect();
        a.accept_datums_int(false, false, &vals, false);
        assert!(a.crossed());
    }

    /// GL-LOWDIST-1: the size-asymmetric steal combine produces the SAME
    /// value set as the generic combine (int face) — skewed widths, ties,
    /// empty donors, cross-worker duplicates.
    #[test]
    fn steal_combine_int_set_equivalence() {
        let mut a = local();
        let mut b = local();
        let mut c = local();
        // Skew: a is the big donor; b overlaps a; c is empty-ish.
        let big: Vec<Datum> = (0..5000i64).map(|k| Datum::from_i64(k * 37 % 4096)).collect();
        a.accept_datums_int(false, false, &big, false);
        let small: Vec<Datum> = (0..300i64).map(|k| Datum::from_i64(k * 37 % 4096 + 2048)).collect();
        b.accept_datums_int(false, false, &small, true);
        c.accept_datums_int(false, false, &[Datum::from_i64(7)], false);
        let sealed_g = vec![a.seal(), b.seal(), c.seal()];
        let generic: Vec<PlainPdMerged> =
            (0..PLAIN_PD_PARTS).map(|p| plain_pd_combine(false, p, &sealed_g)).collect();
        // Rebuild the same locals for the steal pass (take_part consumes).
        let mut a2 = local();
        let mut b2 = local();
        let mut c2 = local();
        a2.accept_datums_int(false, false, &big, false);
        b2.accept_datums_int(false, false, &small, true);
        c2.accept_datums_int(false, false, &[Datum::from_i64(7)], false);
        let sealed_s = vec![a2.seal(), b2.seal(), c2.seal()];
        let steal: Vec<PlainPdMerged> =
            (0..PLAIN_PD_PARTS).map(|p| plain_pd_combine_steal(false, p, &sealed_s)).collect();
        assert_eq!(merged_int_values(&generic), merged_int_values(&steal));
        // Per-partition lengths match too (partition routing untouched).
        let lens = |m: &[PlainPdMerged]| m.iter().map(|x| x.len()).collect::<Vec<_>>();
        assert_eq!(lens(&generic), lens(&steal));
    }

    /// GL-LOWDIST-1: steal ≡ generic on the bytes face.
    #[test]
    fn steal_combine_bytes_set_equivalence() {
        let strs: Vec<Vec<u8>> =
            (0..600).map(|i| format!("v{}", i * 13 % 400).into_bytes()).collect();
        let build = || {
            let mut a = local();
            let mut b = local();
            for s in &strs {
                a.parts[part_of_bytes(s)].insert_bytes(s);
            }
            for s in strs.iter().take(50) {
                b.parts[part_of_bytes(s)].insert_bytes(s);
            }
            for s in [b"only-b".as_slice(), b"".as_slice()] {
                b.parts[part_of_bytes(s)].insert_bytes(s);
            }
            vec![a.seal(), b.seal()]
        };
        let sealed_g = build();
        let generic: Vec<PlainPdMerged> =
            (0..PLAIN_PD_PARTS).map(|p| plain_pd_combine(true, p, &sealed_g)).collect();
        let sealed_s = build();
        let steal: Vec<PlainPdMerged> =
            (0..PLAIN_PD_PARTS).map(|p| plain_pd_combine_steal(true, p, &sealed_s)).collect();
        assert_eq!(merged_bytes_values(&generic), merged_bytes_values(&steal));
    }
}
