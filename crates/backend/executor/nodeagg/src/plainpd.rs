//! Parallel PLAIN exact-DISTINCT partial state (band-2b) — the zero-group-key
//! twin of `pardistinct.rs` for the CB q5/q6 shape:
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
    // The direct staged-key contract: single bare OUTER var, column 0, no
    // FILTER (recorded at init). Mirrors `agg_plain_distinct_direct_shape`.
    if ps.direct_att != Some(0) {
        return None;
    }
    Some(std::sync::Arc::new(PlainPdSpec {
        att: 0,
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

    /// One dict-coded text window (the cbstore zero-decode lane): the
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
        PlainPdSealed { parts: self.parts, seen_null: self.seen_null }
    }
}

/// A frozen worker partial.
pub struct PlainPdSealed {
    parts: Vec<DistinctSet<'static>>,
    seen_null: bool,
}

// SAFETY: the PlainPdLocal argument verbatim (sealed = the same sets,
// moved); combine claimers read disjoint partitions, one claimer per call.
unsafe impl Send for PlainPdSealed {}
// SAFETY: `&PlainPdSealed` exposes only reads of never-spilled sets
// (global-allocator memory); the sink contract serializes writers.
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

    /// Approximate memory of this partial (the combine envelope check).
    pub fn mem_bytes(&self) -> usize {
        self.parts.iter().map(|s| s.mem_bytes()).sum()
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
        let Some(p) = s.parts.get(part) else { continue };
        if kind_bytes {
            for i in 0..p.n_bytes() {
                let (off, len, _h) = p.bytes_span(i);
                set.insert_bytes(p.bytes_content(off, len));
            }
        } else {
            set.insert_i64_batch(p.ints(), &mut hashes);
        }
    }
    // Export the merged values (the set is spent).
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
}
