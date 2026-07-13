//! Lane-v2 compact-row aggregation table hosting (cbstore-v2 plan Stage 2.2).
//!
//! The `lanetable::LaneAggTable` replaces the C-ported tuplehash as the
//! GROUP BY table for a narrow, explicitly-admitted shape; everything else
//! keeps the C table exactly as today (refuse/fallback discipline). The
//! table owns LAYOUT AND PROBE ONLY — transition and finalize still run the
//! real C-ported aggregate code over `AggPerGroup` states stored in the
//! compact payload rows (zero-initialized at group birth, seeded by the same
//! `trans_init` datumCopy loop as `initialize_hash_entry`), so transvalues
//! are byte-identical to the C path's. Group OUTPUT ORDER diverges (row /
//! insertion order vs simplehash bucket order) — legal under the 2026-07-13
//! order-relaxation policy; ORDER BY-wrapped outputs are unaffected.
//!
//! Admission (v1, decided per build by the lane's scan-K2 feed — see
//! `execmain::lanev2`):
//!   * the scan-K2 shape already holds (AGG_HASHED, single grouping key with
//!     a kernel probe, fully lanefold-admitted transitions, no residuals,
//!     unguarded plan, key + needed columns staged in SoA lanes);
//!   * the key kernel is an INTEGER width (int2/int4/int8 — text keys keep
//!     the C table until the str8/arena hosting lands; the lanetable crate
//!     already implements it, microbench-proven);
//!   * `aggsplit == AGGSPLIT_SIMPLE`, or `AGGSPLIT_INITIAL_SERIAL` under an
//!     ARMED lane parallel pool (Stage 2.2 × Stage 4: worker partial builds
//!     use the compact table and export it into the merge handoff —
//!     `merge::maybe_install_handoff`'s compact arm; group estimates divide
//!     by the pool DOP, see `compact_split_divisor`);
//!   * NOT spill-eligible by estimate: planner `numGroups` must fit within
//!     HALF the hash-mem/ngroups limits (v1 policy: the compact table
//!     REFUSES spill-eligible plans; distinct-spill is v2, like the
//!     uniqExact lane). A RUNTIME BACKSTOP re-checks actual memory before
//!     every batch and MIGRATES to the C table when the half-limit is
//!     crossed (planner estimates lie — the rows=1 defect), after which the
//!     build continues on the C path, spill machinery intact. Peak memory
//!     during a migration is bounded by ~2× the half-limit = the limit.
//!
//! Memory accounting: `LaneAggTable::mem_used()` (entry arrays + row chunks
//! + arena, capacities not lengths) + the aggcontext's `subtree_used` stand
//! in for the C path's meta/entry/transvalue triple, compared against the
//! SAME `hash_mem_limit` at half margin — conservative by construction.

use core::ptr::NonNull;

use ::datum::Datum;
use ::execexpr::AggPerGroup;
use ::executils::EStateData;
use ::types_error::PgResult;

use crate::{AggStateData, PerHashData};

/// One packed multi-key component's kind (multikey spike §2.1a/§2.3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MkCompKind {
    /// Fixed-width byval int class: canonical i64's low `width` bytes at the
    /// component's offset (sign-extend on unpack).
    Int { width: u8 },
    /// Scan-lifetime intern id (u32) for a dict-coded / raw-bytes text
    /// component — resolved through [`agg_hash_compact_intern`].
    Intern,
}

/// One packed multi-key component: 0-based input attno + byte offset into
/// the ≤16-byte little-endian key image.
#[derive(Clone, Copy, Debug)]
pub struct MkComp {
    pub att: u16,
    pub off: u8,
    pub kind: MkCompKind,
}

impl MkComp {
    #[inline]
    pub fn width(&self) -> u8 {
        match self.kind {
            MkCompKind::Int { width } => width,
            MkCompKind::Intern => 4,
        }
    }
}

/// The packed multi-key layout (spike §2.4 admission): components at fixed
/// offsets in key order; on nullable (heap) sources one null-bitmap byte
/// (bit j = component j IS NULL, its value bits zeroed — CH
/// `nullable_keys128`) sits at offset `packed_bytes - 1`. `two_words` =
/// the image exceeds 8 bytes (KeyRepr::Int128); otherwise the image is one
/// u64 riding the existing KeyRepr::Int machinery.
#[derive(Clone, Debug)]
pub struct MkShape {
    pub comps: Vec<MkComp>,
    pub packed_bytes: u8,
    pub nullable: bool,
    pub two_words: bool,
}

impl MkShape {
    /// Null-bitmap byte offset (nullable shapes only).
    #[inline]
    pub fn null_off(&self) -> usize {
        debug_assert!(self.nullable);
        self.packed_bytes as usize - 1
    }
}

/// Key mode of an armed compact table.
pub(crate) enum CompactKeySpec {
    /// Single integer grouping key of `width` bytes (2/4/8) — compact v1.
    Single { width: u8 },
    /// Packed multi-key composite (multikey spike §2).
    Multi(MkShape),
}

/// Per-node compact-table state, hosted in [`PerHashData`].
pub(crate) struct CompactHash {
    pub(crate) table: ::lanetable::LaneAggTable,
    pub(crate) key: CompactKeySpec,
    /// Scan-lifetime intern table for `MkCompKind::Intern` components:
    /// text bytes → dense u32 id (id = insertion row index; the id is also
    /// stored in the row's 8 state bytes for hit-side read-back). The
    /// reverse map IS the table's key arena (`row_key_bytes`).
    intern: Option<::lanetable::LaneAggTable>,
    // Batch scratch (canonical keys + probe outputs), reused across batches.
    keys: Vec<i64>,
    states: Vec<*mut u8>,
    hashes: Vec<u64>,
    new_rows: Vec<u32>,
}

/// The compact-table arming verdict — lanev2 ticks its refuse-reason
/// accounting off the non-`Armed` variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactArm {
    Armed,
    /// Key kernel is not an admitted kind (text/expr — C table hosts it).
    KeyKind,
    /// Spill-eligible by planner estimate (v1 refuses; C table spills).
    SpillRisk,
    /// Kill switch (`PGRUST_LANE_V2_COMPACT=0`) or non-simple aggsplit.
    Off,
}

/// `PGRUST_LANE_V2_COMPACT` kill switch (default ON inside the lane; the
/// lane itself is behind `PGRUST_LANE_V2`). Resolved once per process.
fn compact_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_LANE_V2_COMPACT").map_or(true, |v| v != "0"))
}

/// Aggsplit admission + the per-worker group-estimate divisor (Stage 2.2 ×
/// Stage 4):
///   * `AGGSPLIT_SIMPLE` — the serial lane build; divisor 1.
///   * `AGGSPLIT_INITIAL_SERIAL` under an ARMED lane pool — a worker (or the
///     participating leader) partial build; the planner's `numGroups` is the
///     whole input's estimate while each of the DOP participants sees ~1/DOP
///     of the rows, so the spill/layout gates divide by the pool DOP. An
///     underestimate is bounded by the runtime migration backstop, which
///     works in-worker (thread-native) and falls back to the C table + row
///     emission. Pool-unarmed partial builds refuse: the parallel-finalize
///     handoff of ordinary (heap) parallel agg keeps its C-table behavior
///     byte-for-byte.
///   * everything else (`AGGSPLIT_FINAL_DESERIAL`) refuses — the finalize
///     combines states; it never runs transition builds the compact table
///     could host.
fn compact_split_divisor(aggsplit: ::types_pathnodes::AggSplit) -> Option<u64> {
    if aggsplit == ::types_pathnodes::AGGSPLIT_SIMPLE {
        return Some(1);
    }
    if aggsplit == ::types_pathnodes::AGGSPLIT_INITIAL_SERIAL {
        let dop = ::guc_tables::lane_pool::lane_parallel_pool_dop();
        if dop > 0 {
            return Some(dop as u64);
        }
    }
    None
}

/// Spill-eligibility estimate at the compact table's HALF MARGIN, exported
/// for feeds whose batching collapses aggcontext allocation sequences (the
/// code-histogram build's str tie-copies, lane-v2-codehist): such a feed is
/// output-byte-identical exactly while the hash build never spills, so it
/// must refuse spill-eligible estimates the same way the compact table does.
/// Conservative: false also for non-simple aggsplit shapes the divisor
/// refuses.
pub fn agg_hash_spill_unlikely(node: &mut AggStateData<'_>) -> bool {
    let Some(divisor) = compact_split_divisor(node.plan.aggsplit) else {
        return false;
    };
    let numgroups = (node.plan.numGroups.max(1) as u64 / divisor).max(1);
    let Some(ph) = node.perhash.as_mut() else {
        return false;
    };
    let additionalsize = ph.hashtable.additionalsize();
    // Entry (8 B at <=0.5 fill -> 16), key word, states, transvalue slack —
    // the compact arm's exact formula.
    let est_bytes = numgroups.saturating_mul(16 + 8 + additionalsize as u64 + 16);
    numgroups <= ph.hash_ngroups_limit / 2 && est_bytes <= ph.hash_mem_limit as u64 / 2
}

/// Decide + arm the compact table for this build. Caller (the lane's scan-K2
/// feed) has already admitted the K2 shape; this adds the compact-specific
/// gates (module doc). Idempotent per build: re-arming an armed node keeps
/// its table.
pub fn agg_hash_compact_try_arm(node: &mut AggStateData<'_>) -> CompactArm {
    if !compact_enabled() {
        return CompactArm::Off;
    }
    let Some(divisor) = compact_split_divisor(node.plan.aggsplit) else {
        return CompactArm::Off;
    };
    let numgroups = (node.plan.numGroups.max(1) as u64 / divisor).max(1);
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    if ph.compact.is_some() {
        return CompactArm::Armed;
    }
    let Some(width) = ph.hashtable.staged_probe_int_width() else {
        return CompactArm::KeyKind;
    };
    let additionalsize = ph.hashtable.additionalsize();
    debug_assert!(additionalsize > 0, "K2 shapes carry a fold plan (numtrans > 0)");
    // Spill-eligibility estimate at half margin: entry (8 B at ≤0.5 fill →
    // 16), key word, states, and a transvalue-slack allowance per group.
    let est_bytes = numgroups.saturating_mul(16 + 8 + additionalsize as u64 + 16);
    if numgroups > ph.hash_ngroups_limit / 2 || est_bytes > ph.hash_mem_limit as u64 / 2 {
        return CompactArm::SpillRisk;
    }
    // Entry layout by planner group estimate (pod A/B, tableresidual note):
    // Inline16 single-load entries win 14-25% on the two-phase production
    // shape at ≤1e6 groups but lose ~6-9% at the 8.4M-group band (2x entry
    // bytes turn DRAM-bound), so big estimates keep Salt8. Underestimates
    // are bounded by the runtime migration backstop (half hash_mem), which
    // caps how large an Inline16 table can actually grow.
    let layout = if numgroups <= (1 << 20) {
        ::lanetable::EntryLayout::Inline16
    } else {
        ::lanetable::EntryLayout::Salt8
    };
    ph.compact = Some(CompactHash {
        table: ::lanetable::LaneAggTable::with_config(
            ::lanetable::KeyRepr::Int,
            additionalsize,
            (numgroups as usize).min(1 << 20),
            ::lanetable::HashKind::best(),
            layout,
        ),
        key: CompactKeySpec::Single { width },
        intern: None,
        keys: Vec::new(),
        states: Vec::new(),
        hashes: Vec::new(),
        new_rows: Vec::new(),
    });
    CompactArm::Armed
}

/// Decide + arm the compact table for a MULTI-KEY (packed composite) build
/// (multikey spike §2/§5.4). The caller (the lane's multi-key scan feed) has
/// already admitted the shape's feed half (unguarded, no residuals, all key
/// columns staged); this adds the packing admission:
///   * 2..N grouping keys, each an `Int`-class kernel column, or (exactly
///     when `dict_att` names it) a raw-bytes text column hosted through the
///     scan-lifetime intern table;
///   * Σ canonical widths (+ 1 null-bitmap byte when `nullable`) ≤ 16 B;
///   * the compact v1 gates verbatim (kill switch, AGGSPLIT_SIMPLE,
///     spill-eligibility estimate at half margin).
/// Idempotent per build. Non-`Armed` verdicts tick the caller's refuse
/// accounting (`MultiKeyShape` class).
pub fn agg_hash_compact_try_arm_mk(
    node: &mut AggStateData<'_>,
    nullable: bool,
    dict_att: Option<u16>,
) -> CompactArm {
    if !compact_enabled() {
        return CompactArm::Off;
    }
    let Some(divisor) = compact_split_divisor(node.plan.aggsplit) else {
        return CompactArm::Off;
    };
    let numgroups = (node.plan.numGroups.max(1) as u64 / divisor).max(1);
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    if ph.compact.is_some() {
        return CompactArm::Armed;
    }
    let key_cols = ph.hashtable.key_cols();
    if key_cols.len() < 2 {
        return CompactArm::KeyKind;
    }
    let mut comps: Vec<MkComp> = Vec::with_capacity(key_cols.len());
    let mut off = 0usize;
    let mut has_intern = false;
    for (j, kc) in key_cols.iter().enumerate() {
        // MkComp.att is the 0-based INPUT column (the feed reads SoA lanes
        // by input colno); kc.att is the hashslot position, unused here.
        let input_att = (ph.hash_grp_col_idx_input[j] - 1) as u16;
        let kind = match kc.kind {
            ::execgrouping::GroupKeyKind::Int { width } => MkCompKind::Int { width },
            // Raw-bytes text packs ONLY through the dict/intern lane the
            // feed armed for exactly this column; NULL text cannot be
            // interned, so intern components require the no-NULLs proof.
            ::execgrouping::GroupKeyKind::TextRaw
                if dict_att == Some(input_att) && !nullable =>
            {
                has_intern = true;
                MkCompKind::Intern
            }
            _ => return CompactArm::KeyKind,
        };
        let comp = MkComp { att: input_att, off: off as u8, kind };
        off += comp.width() as usize;
        comps.push(comp);
    }
    let packed_bytes = off + nullable as usize;
    if packed_bytes > 16 || (nullable && comps.len() > 8) {
        return CompactArm::KeyKind;
    }
    let additionalsize = ph.hashtable.additionalsize();
    debug_assert!(additionalsize > 0, "fold-fed shapes carry transitions (numtrans > 0)");
    // Spill-eligibility estimate at half margin (compact v1 formula; the
    // 2-word key rides the same 8-B slack term — conservative either way).
    let est_bytes = numgroups.saturating_mul(16 + 16 + additionalsize as u64 + 16);
    if numgroups > ph.hash_ngroups_limit / 2 || est_bytes > ph.hash_mem_limit as u64 / 2 {
        return CompactArm::SpillRisk;
    }
    let two_words = packed_bytes > 8;
    let (repr, layout) = if two_words {
        // Int128 is Salt8-only (2 key words cannot inline into a 16-B slot).
        (::lanetable::KeyRepr::Int128, ::lanetable::EntryLayout::Salt8)
    } else if numgroups <= (1 << 20) {
        (::lanetable::KeyRepr::Int, ::lanetable::EntryLayout::Inline16)
    } else {
        (::lanetable::KeyRepr::Int, ::lanetable::EntryLayout::Salt8)
    };
    ph.compact = Some(CompactHash {
        table: ::lanetable::LaneAggTable::with_config(
            repr,
            additionalsize,
            (numgroups as usize).min(1 << 20),
            ::lanetable::HashKind::best(),
            layout,
        ),
        key: CompactKeySpec::Multi(MkShape {
            comps,
            packed_bytes: packed_bytes as u8,
            nullable,
            two_words,
        }),
        intern: has_intern.then(|| {
            ::lanetable::LaneAggTable::new(::lanetable::KeyRepr::Bytes, 8, 1 << 10)
        }),
        keys: Vec::new(),
        states: Vec::new(),
        hashes: Vec::new(),
        new_rows: Vec::new(),
    });
    CompactArm::Armed
}

/// The armed multi-key layout, cloned for the feed's packing loop. `None` =
/// not armed, or armed in single-key mode.
pub fn agg_hash_compact_mk_shape(node: &AggStateData<'_>) -> Option<MkShape> {
    let ph = node.perhash.as_ref()?;
    match &ph.compact.as_ref()?.key {
        CompactKeySpec::Multi(shape) => Some(shape.clone()),
        CompactKeySpec::Single { .. } => None,
    }
}

/// Resolve `bytes` (a text component's detoasted payload) to its scan-stable
/// intern id — insert-once; ids are dense insertion ordinals. The feed calls
/// this once per (epoch, code) resolve (or per row on Raw windows), off the
/// packed hot loop.
pub fn agg_hash_compact_intern(node: &mut AggStateData<'_>, bytes: &[u8]) -> u32 {
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.as_mut().expect("intern requires an armed table");
    let t = ch.intern.as_mut().expect("intern requires an intern-armed shape");
    let hash = t.hash_key_bytes(bytes);
    let pr = t.probe_bytes(bytes, hash);
    if pr.is_new {
        let id = (t.nrows() - 1) as u32;
        // SAFETY: fresh zeroed 8-byte state block; the id is its read-back.
        unsafe { pr.states.cast::<u32>().write(id) };
        id
    } else {
        // SAFETY: live state block written at insert.
        unsafe { pr.states.cast::<u32>().read() }
    }
}

/// Whether this build currently runs on the compact table.
pub fn agg_hash_compact_armed(node: &AggStateData<'_>) -> bool {
    node.perhash.as_ref().is_some_and(|ph| ph.compact.is_some())
}

/// One staged batch through the compact table: canonicalize the key lane to
/// i64 per the kernel width, batch-probe (hash inside the table — the PG
/// hash functions are bypassed entirely; internal tables carry no semantic
/// hash constraint), seed NEW groups with the same `trans_init` datumCopy
/// loop as `initialize_hash_entry`, and hand back one live `AggPerGroup`
/// pointer per input row for the caller's whole-batch fold.
///
/// Returns `false` when the runtime backstop fired BEFORE the batch: the
/// table migrated into the C tuplehash and disarmed — the caller re-probes
/// this batch (and all later ones) through the normal staged path.
pub fn agg_hash_compact_batch<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    keys: &[Datum],
    isnull: &[bool],
    groups: &mut Vec<NonNull<AggPerGroup>>,
) -> PgResult<bool> {
    debug_assert_eq!(keys.len(), isnull.len());
    // Runtime backstop (module doc): actual footprint against the half
    // limits BEFORE the batch, so migration never invalidates pointers the
    // caller's fold still holds.
    if !agg_hash_compact_backstop(node, estate)? {
        return Ok(false);
    }
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let AggStateData { perhash, trans_init, trans_typ, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.as_mut().expect("compact batch requires an armed table");
    let CompactHash { table, key, keys: ckeys, states, hashes, new_rows, .. } = ch;
    let CompactKeySpec::Single { width } = key else {
        unreachable!("datum-lane batches require a single-key table")
    };
    ckeys.clear();
    states.clear();
    new_rows.clear();
    groups.clear();
    // Canonicalize the key lane (the kernels compare exactly these widths).
    match *width {
        2 => ckeys.extend(keys.iter().map(|d| d.as_i16() as i64)),
        4 => ckeys.extend(keys.iter().map(|d| d.as_i32() as i64)),
        _ => ckeys.extend(keys.iter().map(|d| d.as_i64())),
    }
    if isnull.iter().any(|&n| n) {
        // NULL keys are rare in GROUP BY streams: per-row probe with the
        // out-of-band NULL group (the batched kernel stays null-free).
        for (i, &n) in isnull.iter().enumerate() {
            let pr = if n {
                table.probe_null()
            } else {
                let k = ckeys[i];
                table.probe_int(k, table.hash_key_int(k as u64))
            };
            states.push(pr.states);
            if pr.is_new {
                new_rows.push(i as u32);
            }
        }
    } else {
        // Prefetch idiom: CH-style ADAPTIVE, per the pod A/B verdict
        // (2026-07-14, ch-bench-pod, 8.4M-row staged hits keys): at u64
        // card 1e6/1e8 adaptive beat DuckDB pre-touch by 6–10% (191/170 vs
        // 204/189 Mns/pass) and no-prefetch by 17–24%; below the L2 gate all
        // three are equal by construction (both idioms disable there).
        table.probe_int_batch(
            ckeys,
            ::lanetable::PrefetchMode::Adaptive,
            hashes,
            states,
            new_rows,
        );
    }
    // Seed the new groups' states — initialize_hash_entry's datumCopy loop
    // verbatim, writing into the compact row's zeroed state bytes.
    seed_new_groups(aggctx, trans_init, trans_typ, states, new_rows)?;
    groups.extend(states.iter().map(|&s| {
        // SAFETY: probe never returns null state pointers.
        unsafe { NonNull::new_unchecked(s.cast::<AggPerGroup>()) }
    }));
    Ok(true)
}

/// The runtime backstop check, exposed for the multi-key feed (which packs
/// BEFORE probing and has no C staged-probe fallback — it re-checks armament
/// per batch and falls to the per-row arrival path after a migration):
/// actual footprint (compact table + intern table + aggcontext subtree)
/// against the half limits; over → migrate + disarm. `false` = migrated (or
/// not armed); the caller routes this batch through the C path.
pub fn agg_hash_compact_backstop<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<bool> {
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    {
        let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
        let Some(ch) = ph.compact.as_ref() else { return Ok(false) };
        let mem = ch.table.mem_used()
            + ch.intern.as_ref().map_or(0, ::lanetable::LaneAggTable::mem_used)
            + aggctx.context().subtree_used();
        if (ch.table.len() as u64) < ph.hash_ngroups_limit / 2 && mem < ph.hash_mem_limit / 2 {
            return Ok(true);
        }
    }
    compact_migrate(node, estate)?;
    Ok(false)
}

/// initialize_hash_entry's datumCopy loop over the batch's NEW groups,
/// writing into the compact rows' zeroed state bytes.
fn seed_new_groups(
    aggctx: ::mcx::Mcx<'_>,
    trans_init: &[::datum::NullableDatum],
    trans_typ: &[crate::TransTyp],
    states: &[*mut u8],
    new_rows: &[u32],
) -> PgResult<()> {
    for &i in new_rows.iter() {
        let pergroup = states[i as usize].cast::<AggPerGroup>();
        for (transno, init) in trans_init.iter().enumerate() {
            let typ = trans_typ[transno];
            let value = if !init.isnull && !typ.byval {
                // SAFETY: node-lifetime initval datum copied into the
                // aggcontext (C initialize_aggregate's datumCopy).
                unsafe { ::execexpr::agg_datum_copy(aggctx, init.value, typ.len)? }
            } else {
                init.value
            };
            // SAFETY: the row's state block holds numtrans AggPerGroup
            // slots, zeroed at creation (lanetable contract).
            unsafe {
                pergroup.add(transno).write(AggPerGroup {
                    trans_value: value,
                    trans_value_is_null: init.isnull,
                    no_trans_value: init.isnull,
                });
            }
        }
    }
    Ok(())
}

/// One PRE-PACKED multi-key batch through the compact table: probe the
/// packed key lane (one-word shapes — `packed_bytes ≤ 8`), seed NEW groups,
/// and hand back one live `AggPerGroup` pointer per input row. The caller
/// ran [`agg_hash_compact_backstop`] before packing (this path never
/// migrates mid-batch) and packed per the armed [`MkShape`] — NULLs are
/// already encoded in the key image, so there is no isnull lane and no
/// out-of-band NULL row.
pub fn agg_hash_compact_batch_mk1<'mcx>(
    node: &mut AggStateData<'mcx>,
    keys: &[i64],
    groups: &mut Vec<NonNull<AggPerGroup>>,
) -> PgResult<()> {
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let AggStateData { perhash, trans_init, trans_typ, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.as_mut().expect("compact batch requires an armed table");
    debug_assert!(matches!(&ch.key, CompactKeySpec::Multi(s) if !s.two_words));
    let CompactHash { table, states, hashes, new_rows, .. } = ch;
    states.clear();
    new_rows.clear();
    groups.clear();
    table.probe_int_batch(keys, ::lanetable::PrefetchMode::Adaptive, hashes, states, new_rows);
    seed_new_groups(aggctx, trans_init, trans_typ, states, new_rows)?;
    groups.extend(states.iter().map(|&s| {
        // SAFETY: probe never returns null state pointers.
        unsafe { NonNull::new_unchecked(s.cast::<AggPerGroup>()) }
    }));
    Ok(())
}

/// [`agg_hash_compact_batch_mk1`]'s two-word twin (`packed_bytes > 8` →
/// KeyRepr::Int128).
pub fn agg_hash_compact_batch_mk2<'mcx>(
    node: &mut AggStateData<'mcx>,
    keys: &[[u64; 2]],
    groups: &mut Vec<NonNull<AggPerGroup>>,
) -> PgResult<()> {
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let AggStateData { perhash, trans_init, trans_typ, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.as_mut().expect("compact batch requires an armed table");
    debug_assert!(matches!(&ch.key, CompactKeySpec::Multi(s) if s.two_words));
    let CompactHash { table, states, hashes, new_rows, .. } = ch;
    states.clear();
    new_rows.clear();
    groups.clear();
    table.probe_i128_batch(keys, ::lanetable::PrefetchMode::Adaptive, hashes, states, new_rows);
    seed_new_groups(aggctx, trans_init, trans_typ, states, new_rows)?;
    groups.extend(states.iter().map(|&s| {
        // SAFETY: probe never returns null state pointers.
        unsafe { NonNull::new_unchecked(s.cast::<AggPerGroup>()) }
    }));
    Ok(())
}

/// Force-disarm the compact table (migrating its groups into the C
/// tuplehash) — the lane calls this the moment a build batch must route
/// through the arrival probe (SoA fallback rows), so ALL groups always live
/// in exactly one table. No-op when not armed.
pub fn agg_hash_compact_disarm<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    if agg_hash_compact_armed(node) {
        compact_migrate(node, estate)?;
    }
    Ok(())
}

/// Reconstruct row `row`'s key datum per the kernel width (single-key mode).
/// `None` = the NULL group.
#[inline]
fn compact_key_datum(ch: &CompactHash, width: u8, row: usize) -> Option<Datum> {
    ch.table.row_key_int(row).map(|k| match width {
        2 => Datum::from_i16(k as i16),
        4 => Datum::from_i32(k as i32),
        _ => Datum::from_i64(k),
    })
}

/// Unpack component `comp`'s raw bits from a row's ≤16-byte key image.
#[inline]
fn mk_unpack(words: [u64; 2], comp: &MkComp) -> u64 {
    let image = (words[0] as u128) | ((words[1] as u128) << 64);
    let w = comp.width() as u32 * 8;
    let bits = (image >> (comp.off as u32 * 8)) as u64;
    if w == 64 {
        bits
    } else {
        bits & ((1u64 << w) - 1)
    }
}

/// Row `row`'s packed key image as two little-endian words (one-word shapes
/// zero-fill the high word).
#[inline]
fn mk_row_words(ch: &CompactHash, shape: &MkShape, row: usize) -> [u64; 2] {
    if shape.two_words {
        ch.table.row_key_i128(row).expect("multi-key tables have no NULL row")
    } else {
        let k = ch.table.row_key_int(row).expect("multi-key tables have no NULL row");
        [k as u64, 0]
    }
}

/// Materialize an interned text component as a text datum in `mcx` (the
/// reverse map is the intern table's key arena). The image is forgotten into
/// the context (bulk-freed at its reset — docs/no-drop.md).
fn mk_intern_datum(ch: &CompactHash, id: u32, mcx: ::mcx::Mcx<'_>) -> PgResult<Datum> {
    let t = ch.intern.as_ref().expect("intern component requires the intern table");
    let mut scratch = [0u8; 8];
    let bytes =
        t.row_key_bytes(id as usize, &mut scratch).expect("intern ids never map to a NULL row");
    let v = ::varlena::cstring_to_text(mcx, bytes)?;
    let d = Datum::from_usize(v.as_bytes().as_ptr() as usize);
    core::mem::forget(v.into_image());
    Ok(d)
}

/// Reconstruct row `row`'s component datums (key order) into `out` — the
/// read-back/migrate leg of the packed multi-key design (spike §2.1a):
/// shift/mask + sign-extend per Int component, intern-arena materialization
/// per Intern component, null-bitmap bit per component when nullable.
fn compact_key_datums_mk(
    ch: &CompactHash,
    shape: &MkShape,
    row: usize,
    mcx: ::mcx::Mcx<'_>,
    out: &mut Vec<(Datum, bool)>,
) -> PgResult<()> {
    out.clear();
    let words = mk_row_words(ch, shape, row);
    let nulls = if shape.nullable {
        let image = (words[0] as u128) | ((words[1] as u128) << 64);
        (image >> (shape.null_off() as u32 * 8)) as u8
    } else {
        0
    };
    for (j, comp) in shape.comps.iter().enumerate() {
        if nulls & (1 << j) != 0 {
            out.push((Datum::null(), true));
            continue;
        }
        let bits = mk_unpack(words, comp);
        let d = match comp.kind {
            MkCompKind::Int { width } => {
                let sh = 64 - width as u32 * 8;
                let v = if sh == 0 { bits as i64 } else { ((bits << sh) as i64) >> sh };
                match width {
                    2 => Datum::from_i16(v as i16),
                    4 => Datum::from_i32(v as i32),
                    _ => Datum::from_i64(v),
                }
            }
            MkCompKind::Intern => mk_intern_datum(ch, bits as u32, mcx)?,
        };
        out.push((d, false));
    }
    Ok(())
}

/// Present row `row`'s key datums in `hashslot` (hash_desc shape, key
/// order) — the shared read-back leg of the migration walk and the merge
/// handoff export. Interned text materializes into `table_mcx` (node
/// lifetime, exactly like the C entries / handed images that outlive it).
pub(crate) fn compact_row_into_hashslot<'mcx>(
    ch: &CompactHash,
    hashslot: &mut ::types_slot::SlotData<'mcx>,
    mk_scratch: &mut Vec<(Datum, bool)>,
    row: usize,
    table_mcx: ::mcx::Mcx<'_>,
    mcx: ::mcx::Mcx<'mcx>,
) -> PgResult<()> {
    ::exectuples::exec_clear_tuple(hashslot, mcx);
    match &ch.key {
        CompactKeySpec::Single { width } => {
            let (key, key_isnull) = match compact_key_datum(ch, *width, row) {
                Some(d) => (d, false),
                None => (Datum::null(), true),
            };
            let base = hashslot.base_mut();
            base.tts_values[0] = key;
            base.tts_isnull[0] = key_isnull;
        }
        CompactKeySpec::Multi(shape) => {
            compact_key_datums_mk(ch, shape, row, table_mcx, mk_scratch)?;
            let base = hashslot.base_mut();
            for (j, &(d, isnull)) in mk_scratch.iter().enumerate() {
                base.tts_values[j] = d;
                base.tts_isnull[j] = isnull;
            }
        }
    }
    ::exectuples::exec_store_virtual_tuple(hashslot);
    Ok(())
}

/// Row `row`'s byval kernel key cache for an exported handoff entry
/// (`TupleHashEntryData::from_parts`): the single-key datum + isnull.
/// Multi-key tables probe through the Expr kernel, whose entries never read
/// the cache — (null, false) matches what a fresh Expr insert stores.
pub(crate) fn compact_export_entry_key(ch: &CompactHash, row: usize) -> (Datum, bool) {
    match &ch.key {
        CompactKeySpec::Single { width } => match compact_key_datum(ch, *width, row) {
            Some(d) => (d, false),
            None => (Datum::null(), true),
        },
        CompactKeySpec::Multi(_) => (Datum::null(), false),
    }
}

/// Runtime backstop: move every compact group into the C tuplehash and
/// disarm. Entries land in first-arrival (row) order through the SAME
/// C-ported `lookup` insert leg the per-row path uses; the `AggPerGroup`
/// states are plain bytes whose by-ref transvalues live in the aggcontext —
/// pointer-stable across the copy. Group count and memory checks resume on
/// the C path right after (one `hash_agg_check_limits` here flips spill mode
/// if the merged footprint already crossed the real limit).
#[cold]
#[inline(never)]
fn compact_migrate<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<()> {
    let mcx = estate.es_query_cxt;
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.take().expect("migration requires an armed table");
    {
        // Same switch as lanev2's trace helpers (observability only).
        static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
        if *ON.get_or_init(|| {
            matches!(std::env::var("PGRUST_LANE_V2_TRACE").as_deref(), Ok("1") | Ok("on"))
        }) {
            eprintln!(
                "[lanev2] compact table migrating to C tuplehash ({} groups, {} bytes)",
                ch.table.len(),
                ch.table.mem_used()
            );
        }
    }
    let additionalsize = ph.hashtable.additionalsize();
    debug_assert!(!ph.spill.mode, "compact builds never enter spill mode");
    let mut mk_scratch: Vec<(Datum, bool)> = Vec::new();
    for row in 0..ch.table.nrows() {
        // Reconstruct every component; interned text materializes into the
        // table context (same lifetime as the C entries the lookup below
        // copies the slot into).
        compact_row_into_hashslot(
            &ch,
            &mut ph.hashslot,
            &mut mk_scratch,
            row,
            ph.table_ctx.mcx(),
            mcx,
        )?;
        let hash = ph.hashtable.hash_slot(&mut ph.hashslot)?;
        let table_mcx = ph.table_ctx.mcx();
        let (ix, isnew) = ph.hashtable.lookup(&mut ph.hashslot, hash, Some(table_mcx), mcx)?;
        let ix = ix.expect("non-spill-mode lookup always yields an entry");
        debug_assert!(isnew, "compact rows are distinct groups");
        ph.hash_ngroups_current += 1;
        let dst = ph
            .hashtable
            .entry_additional(ix)
            .expect("numtrans > 0 tables carry additional space");
        // SAFETY: both blocks are `additionalsize` bytes — the C entry's
        // zeroed additional area and the compact row's live states.
        unsafe {
            core::ptr::copy_nonoverlapping(
                ch.table.row_states(row),
                dst.as_ptr(),
                additionalsize,
            );
        }
    }
    // One post-hoc limits check (spill mode may engage for the C path's
    // subsequent inserts — exactly the safety property v1 promises).
    crate::hash_agg_check_limits(ph, aggctx, mcx)?;
    Ok(())
}

/// Read-back: the next compact group as (populated `first_slot`, pergroup).
/// Row (insertion) order; no spill refill (compact builds never spill).
/// `None` = drained. Cursor rides `ph.hashiter` (reset by the same sites the
/// C iterator's reset rides).
pub(crate) fn compact_retrieve_next<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<NonNull<AggPerGroup>>> {
    let mcx = estate.es_query_cxt;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let row = ph.hashiter;
    let nrows = ph.compact.as_ref().expect("compact retrieve requires the table").table.nrows();
    if row >= nrows {
        return Ok(None);
    }
    ph.hashiter += 1;
    ::exectuples::exec_store_all_null_tuple(&mut ph.first_slot, mcx);
    let ch = ph.compact.as_ref().expect("compact retrieve requires the table");
    match &ch.key {
        CompactKeySpec::Single { width } => {
            let (key, isnull) = match compact_key_datum(ch, *width, row) {
                Some(d) => (d, false),
                None => (Datum::null(), true),
            };
            let v = (ph.hash_grp_col_idx_input[0] - 1) as usize;
            let base = ph.first_slot.base_mut();
            base.tts_values[v] = key;
            base.tts_isnull[v] = isnull;
        }
        CompactKeySpec::Multi(shape) => {
            // Interned text materializes into the table context (node
            // lifetime — outlives every downstream read of the group row,
            // exactly like the C path's stored-tuple key bytes).
            let mut vals: Vec<(Datum, bool)> = Vec::with_capacity(shape.comps.len());
            compact_key_datums_mk(ch, shape, row, ph.table_ctx.mcx(), &mut vals)?;
            let base = ph.first_slot.base_mut();
            for (j, &(d, isnull)) in vals.iter().enumerate() {
                let v = (ph.hash_grp_col_idx_input[j] - 1) as usize;
                base.tts_values[v] = d;
                base.tts_isnull[v] = isnull;
            }
        }
    }
    // SAFETY: the row's state block is the group's live AggPerGroup array.
    Ok(Some(unsafe { NonNull::new_unchecked(ch.table.row_states(row).cast::<AggPerGroup>()) }))
}

/// Rescan/reset hook: drop the compact table (the next build re-decides).
pub(crate) fn compact_reset(ph: &mut PerHashData<'_>) {
    ph.compact = None;
}
