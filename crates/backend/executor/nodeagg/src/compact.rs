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
    /// numeric in the canonical (mantissa, exp10) key form
    /// (`adt_numeric::keypack` — the Q19 numeric key kind): low `width - 1`
    /// bytes = sign-extended mantissa, top byte = exp10 as i8 with -128
    /// reserved for specials (mantissa 1 = NaN, 2 = +Inf, 3 = -Inf).
    /// `width` is 4 or 8; values outside the width's mantissa range, or
    /// displaying at a non-minimal scale, are UNPACKABLE — the feed demotes
    /// (migrates) instead of packing lossily, so read-back stays
    /// byte-identical.
    Numeric { width: u8 },
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
            MkCompKind::Numeric { width } => width,
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

/// The arithmetic of one reconstructable (redundant) grouping key
/// (redundant-key lane, ClickBench Q36 class): `Var ± Const` int arithmetic
/// over the representative key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RedOp {
    Add,
    Sub,
}

/// One redundant grouping key: a deterministic `rep (op) konst` function of
/// the representative key's value. The feed's per-batch range guard proves
/// every grouped value overflow-free at the key's width, so emit-time
/// reconstruction never errors and is byte-identical to the per-row
/// int2/4/8 pl/mi result.
#[derive(Clone, Copy, Debug)]
pub struct RedDerived {
    pub op: RedOp,
    /// Canonical (sign-extended) constant at the key's width.
    pub konst: i64,
    /// The Var is the LEFT operand (`k - 1`); false = `1 - k`.
    pub var_is_arg0: bool,
}

impl RedDerived {
    /// The derived key's canonical value. Wrapping by design: the feed's
    /// admission-time range guard proved `rep` inside the overflow-free
    /// domain of every derived expression before any group was created.
    #[inline]
    pub fn eval(&self, rep: i64) -> i64 {
        let (a, b) = if self.var_is_arg0 { (rep, self.konst) } else { (self.konst, rep) };
        match self.op {
            RedOp::Add => a.wrapping_add(b),
            RedOp::Sub => a.wrapping_sub(b),
        }
    }
}

/// Reduced-key spec (redundant grouping-key elimination): the table probes
/// on the SINGLE representative key (canonical `width`-byte int), and every
/// other grouping key is reconstructed from it at read-back (retrieve /
/// migrate / handoff export). `keys` is in key (hash_desc) order; exactly
/// one entry is `None` — the representative itself.
#[derive(Clone, Debug)]
pub struct RedShape {
    pub width: u8,
    pub keys: Vec<Option<RedDerived>>,
}

/// Key mode of an armed compact table.
pub(crate) enum CompactKeySpec {
    /// Single integer grouping key of `width` bytes (2/4/8) — compact v1.
    Single { width: u8 },
    /// Packed multi-key composite (multikey spike §2).
    Multi(MkShape),
    /// Reduced multi-key: probe on the representative int key, reconstruct
    /// the redundant keys at read-back (redundant-key lane).
    Reduced(RedShape),
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
    let mut numgroups = (node.plan.numGroups.max(1) as u64 / divisor).max(1);
    // Stage-4 §4.4 exchange: a bounded table holds at most `cap` groups at a
    // time (over-cap flushes into the handoff), so the spill-eligibility
    // gate and the layout/capacity sizing work off the cap — high-NDV
    // partial builds keep the compact table instead of refusing SpillRisk.
    if let Some(cap) = crate::merge::exchange_cap_for_build(node) {
        numgroups = numgroups.min(cap as u64);
    }
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
    let mut numgroups = (node.plan.numGroups.max(1) as u64 / divisor).max(1);
    // Stage-4 §4.4 exchange: gate/size by the bound, as in the single-key arm.
    if let Some(cap) = crate::merge::exchange_cap_for_build(node) {
        numgroups = numgroups.min(cap as u64);
    }
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    if ph.compact.is_some() {
        return CompactArm::Armed;
    }
    let key_cols = ph.hashtable.key_cols();
    if key_cols.len() < 2 {
        return CompactArm::KeyKind;
    }
    // Component kinds first; offsets are laid out per numeric width below
    // (numeric components try the roomy 8-byte encoding, shrinking to 4
    // bytes when the image would exceed 16 — the Q19 shape's budget:
    // int8 + numeric4 + intern4 = 16).
    let mut kinds: Vec<(u16, MkCompKind)> = Vec::with_capacity(key_cols.len());
    let mut has_intern = false;
    let mut has_numeric = false;
    for (j, kc) in key_cols.iter().enumerate() {
        // MkComp.att is the 0-based INPUT column (the feed reads SoA lanes
        // by input colno); kc.att is the hashslot position, unused here.
        let input_att = (ph.hash_grp_col_idx_input[j] - 1) as u16;
        let kind = match kc.kind {
            ::execgrouping::GroupKeyKind::Int { width } => MkCompKind::Int { width },
            // Raw-bytes text packs ONLY through the dict/intern lane the
            // feed armed for exactly this column. NULL text is never
            // interned: non-nullable shapes carry the feed's no-NULLs proof
            // (cbstore) or its runtime NULL-demote pre-check (slot streams);
            // nullable shapes route NULL through the null-bitmap byte (bit
            // set, value bits zero) without touching the intern table.
            ::execgrouping::GroupKeyKind::TextRaw if dict_att == Some(input_att) => {
                has_intern = true;
                MkCompKind::Intern
            }
            // The canonical-form numeric key kind (keypack module doc);
            // per-value packability is the feed's runtime gate.
            ::execgrouping::GroupKeyKind::Numeric => {
                has_numeric = true;
                MkCompKind::Numeric { width: 8 }
            }
            _ => return CompactArm::KeyKind,
        };
        kinds.push((input_att, kind));
    }
    let layout = |kinds: &[(u16, MkCompKind)], numeric_width: u8| {
        let mut comps: Vec<MkComp> = Vec::with_capacity(kinds.len());
        let mut off = 0usize;
        for &(att, kind) in kinds {
            let kind = match kind {
                MkCompKind::Numeric { .. } => MkCompKind::Numeric { width: numeric_width },
                k => k,
            };
            let comp = MkComp { att, off: off as u8, kind };
            off += comp.width() as usize;
            comps.push(comp);
        }
        (comps, off + nullable as usize)
    };
    let (mut comps, mut packed_bytes) = layout(&kinds, 8);
    if packed_bytes > 16 && has_numeric {
        (comps, packed_bytes) = layout(&kinds, 4);
    }
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

/// Shared compact v1 gates for the single-word-key modes (Single/Reduced):
/// kill switch, aggsplit/divisor, and the spill-eligibility estimate at half
/// margin. `Ok(numgroups)` = admissible (the divided group estimate, for the
/// layout choice); `Err` = the refusing verdict.
fn compact_single_word_gates(node: &AggStateData<'_>) -> Result<u64, CompactArm> {
    if !compact_enabled() {
        return Err(CompactArm::Off);
    }
    let Some(divisor) = compact_split_divisor(node.plan.aggsplit) else {
        return Err(CompactArm::Off);
    };
    let numgroups = (node.plan.numGroups.max(1) as u64 / divisor).max(1);
    let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
    let additionalsize = ph.hashtable.additionalsize();
    debug_assert!(additionalsize > 0, "fold-fed shapes carry transitions (numtrans > 0)");
    let est_bytes = numgroups.saturating_mul(16 + 8 + additionalsize as u64 + 16);
    if numgroups > ph.hash_ngroups_limit / 2 || est_bytes > ph.hash_mem_limit as u64 / 2 {
        return Err(CompactArm::SpillRisk);
    }
    Ok(numgroups)
}

/// Read-only admission precheck for the REDUCED (redundant-key) mode: the
/// compact v1 gates without installing a table. The decide phase runs this
/// (it only holds `&AggStateData`); the feed arms for real per build with
/// [`agg_hash_compact_try_arm_reduced`] — same gates, same verdict.
pub fn agg_hash_compact_reduced_admissible(node: &AggStateData<'_>) -> CompactArm {
    match compact_single_word_gates(node) {
        Ok(_) => CompactArm::Armed,
        Err(v) => v,
    }
}

/// Decide + arm the compact table for a REDUCED-key build (redundant
/// grouping-key elimination, Q36 class). The caller (the lane's expr-key
/// feed) has already admitted the shape: 2..N int grouping keys where every
/// non-representative key is a deterministic `Var ± Const` function of the
/// representative, plus the feed half (unguarded-or-proven fold plan, no
/// residuals, representative key staged). The table probes on the single
/// representative word; read-back reconstructs the redundant keys.
/// Idempotent per build.
pub fn agg_hash_compact_try_arm_reduced(
    node: &mut AggStateData<'_>,
    shape: RedShape,
) -> CompactArm {
    let numgroups = match compact_single_word_gates(node) {
        Ok(n) => n,
        Err(v) => return v,
    };
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    if ph.compact.is_some() {
        return CompactArm::Armed;
    }
    debug_assert_eq!(shape.keys.len(), ph.hashtable.key_cols().len());
    debug_assert_eq!(shape.keys.iter().filter(|d| d.is_none()).count(), 1);
    debug_assert!(matches!(shape.width, 2 | 4 | 8));
    let additionalsize = ph.hashtable.additionalsize();
    // Same layout policy as compact v1 (single-word key).
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
        key: CompactKeySpec::Reduced(shape),
        intern: None,
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
        CompactKeySpec::Single { .. } | CompactKeySpec::Reduced(_) => None,
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
    // Single-word datum-lane probes: compact v1 and the reduced (redundant-
    // key) mode — the latter's key lane is the representative key.
    let width = match key {
        CompactKeySpec::Single { width } => width,
        CompactKeySpec::Reduced(shape) => &shape.width,
        CompactKeySpec::Multi(_) => {
            unreachable!("datum-lane batches require a single-word-key table")
        }
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
    // Stage-4 §4.4 exchange bound (merge.rs): an over-cap table flushes into
    // the finalize handoff radix-partitioned and continues emptied. BEFORE
    // the probes, same contract as the migration below — no caller-held
    // group pointer survives a batch boundary.
    crate::merge::exchange_maybe_flush(node, estate)?;
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

/// A canonical i64 as a width-typed int datum (byte-identical to the
/// per-row path's int2/int4/int8 datum image).
#[inline]
fn int_width_datum(width: u8, v: i64) -> Datum {
    match width {
        2 => Datum::from_i16(v as i16),
        4 => Datum::from_i32(v as i32),
        _ => Datum::from_i64(v),
    }
}

/// Reconstruct row `row`'s key datums (key order) for a REDUCED-key table:
/// the representative from the stored key word, every redundant key
/// re-evaluated from it (deterministic, overflow-free by the feed's range
/// guard). The NULL group reconstructs to all-NULL keys — the strict ±
/// operators map a NULL representative to NULL derived keys, exactly the
/// per-row result.
fn compact_key_datums_red(
    ch: &CompactHash,
    shape: &RedShape,
    row: usize,
    out: &mut Vec<(Datum, bool)>,
) {
    out.clear();
    match ch.table.row_key_int(row) {
        None => out.extend(core::iter::repeat_n((Datum::null(), true), shape.keys.len())),
        Some(rep) => out.extend(shape.keys.iter().map(|d| {
            let v = d.map_or(rep, |d| d.eval(rep));
            debug_assert!(
                match shape.width {
                    2 => i16::try_from(v).is_ok(),
                    4 => i32::try_from(v).is_ok(),
                    _ => true,
                },
                "reduced-key range guard admitted an overflowing group"
            );
            (int_width_datum(shape.width, v), false)
        })),
    }
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

// -- Numeric key components (the Q19 numeric key kind) -----------------------
//
// Bit codec for [`MkCompKind::Numeric`]: low `width - 1` bytes carry the
// canonical mantissa (sign-extended two's complement), the top byte carries
// exp10 as i8 with -128 reserved for specials (mantissa 1 = NaN, 2 = +Inf,
// 3 = -Inf; `numeric_eq` treats NaN = NaN, so one NaN key is correct).
// Injective over `numeric_eq` classes by the keypack canonical-form
// contract; per-VALUE packability (range, minimal display scale) is gated
// at pack time — unpackable values make the feed migrate to the C table,
// never pack lossily.

/// Largest admissible |mantissa| for a `width`-byte numeric component.
#[inline]
pub fn mk_numeric_mant_abs_max(width: u8) -> u64 {
    debug_assert!(width == 4 || width == 8);
    (1u64 << ((width as u32 - 1) * 8 - 1)) - 1
}

/// Encode a canonical key form into component bits.
#[inline]
pub fn mk_numeric_key_bits(key: ::adt_numeric::NumericKeyForm, width: u8) -> u64 {
    use ::adt_numeric::NumericKeyForm as K;
    let shift = (width as u32 - 1) * 8;
    let mant_mask = (1u64 << shift) - 1;
    match key {
        K::Finite { mantissa, exp10 } => {
            debug_assert!(mantissa.unsigned_abs() <= mk_numeric_mant_abs_max(width));
            debug_assert!((-127..=127).contains(&exp10));
            ((mantissa as u64) & mant_mask) | (((exp10 as i8 as u8) as u64) << shift)
        }
        K::NaN => (0x80u64 << shift) | 1,
        K::PInf => (0x80u64 << shift) | 2,
        K::NInf => (0x80u64 << shift) | 3,
    }
}

/// Pack an INTEGER value straight into its `width`-byte component bits —
/// the bits `mk_numeric_datum_bits` would produce for the materialized
/// `int64_to_numeric(v)` datum (dscale-0, canonical digit form: always
/// packable up to the mantissa range), without building the numeric. The
/// canonical key form of an integer strips trailing decimal zeros into
/// exp10. `None` = |mantissa| exceeds the width's range — the caller
/// demotes, exactly the datum path's verdict.
pub fn mk_numeric_i64_bits(v: i64, width: u8) -> Option<u64> {
    let mut m = v;
    let mut e: i32 = 0;
    while m != 0 && m % 10 == 0 {
        m /= 10;
        e += 1;
    }
    // i64's trailing-zero-stripped mantissa caps e at 18 << the exp bound.
    debug_assert!(e <= ::adt_numeric::NUMERIC_KEY_EXP_MAX);
    if m.unsigned_abs() > mk_numeric_mant_abs_max(width) {
        return None;
    }
    Some(mk_numeric_key_bits(
        ::adt_numeric::NumericKeyForm::Finite { mantissa: m, exp10: e },
        width,
    ))
}

/// Decode component bits back to the canonical key form.
#[inline]
fn mk_numeric_key_decode(bits: u64, width: u8) -> ::adt_numeric::NumericKeyForm {
    use ::adt_numeric::NumericKeyForm as K;
    let shift = (width as u32 - 1) * 8;
    let e = ((bits >> shift) as u8) as i8;
    let mant_bits = bits & ((1u64 << shift) - 1);
    if e == i8::MIN {
        return match mant_bits {
            1 => K::NaN,
            2 => K::PInf,
            _ => K::NInf,
        };
    }
    // Sign-extend the mantissa from its `shift`-bit field.
    let m = ((mant_bits << (64 - shift)) as i64) >> (64 - shift);
    K::Finite { mantissa: m, exp10: e as i32 }
}

/// Pack a live numeric varlena datum into its `width`-byte component bits.
/// `None` = unpackable — non-inline image, out-of-range value, or a
/// non-minimal display scale (keypack module doc) — the caller DEMOTES
/// (migrates to the C table); packing lossily would break read-back
/// byte-identity.
pub fn mk_numeric_datum_bits(d: Datum, width: u8) -> Option<u64> {
    let mut buf = [0u16; 64];
    let key = mk_numeric_datum_key(d, width, &mut buf)?;
    Some(mk_numeric_key_bits(key, width))
}

fn mk_numeric_datum_key(
    d: Datum,
    width: u8,
    buf: &mut [u16; 64],
) -> Option<::adt_numeric::NumericKeyForm> {
    let p = d.as_usize() as *const u8;
    if p.is_null() {
        return None;
    }
    // SAFETY: live numeric varlena datum (kernel selection proved the
    // column type; NULLs are handled by the caller's isnull lane).
    let b0 = unsafe { *p };
    let (src, must_copy): (&[u8], bool) = if b0 & 0x01 == 0x01 {
        if b0 == 0x01 {
            // External toast pointer: unpackable here (staged lanes carry
            // inline datums; belt for exotic sources).
            return None;
        }
        let total = ((b0 >> 1) & 0x7F) as usize;
        if total < 3 {
            return None;
        }
        // SAFETY: 1B-header varlena of `total` bytes including the header.
        (unsafe { core::slice::from_raw_parts(p.add(1), total - 1) }, true)
    } else {
        if b0 & 0x03 != 0 {
            // Compressed inline: unpackable (never staged today; belt).
            return None;
        }
        // SAFETY: live 4B-header varlena.
        let data = unsafe { ::datum::VarlenaRef::from_ptr(p) }.data();
        (data, false)
    };
    if src.len() < 2 {
        return None;
    }
    let payload: &[u8] = if must_copy || src.as_ptr() as usize % 2 != 0 {
        // Realign into the stack scratch: `Num::digits` requires 2-byte
        // alignment and short-header payloads are misaligned by
        // construction. Anything larger than the scratch has ndigits far
        // beyond the packable range — unpackable either way.
        if src.len() > 128 {
            return None;
        }
        // SAFETY: buf is 128 bytes, 2-aligned; src.len() <= 128.
        let dst = unsafe {
            core::slice::from_raw_parts_mut(buf.as_mut_ptr().cast::<u8>(), src.len())
        };
        dst.copy_from_slice(src);
        dst
    } else {
        src
    };
    ::adt_numeric::numeric_key_pack(
        ::adt_numeric::Num::from_payload(payload),
        mk_numeric_mant_abs_max(width),
    )
}

/// Materialize a numeric component's datum from its packed bits (read-back /
/// migrate leg) — byte-identical to the packed first-arrival datum by the
/// keypack canonicality gates.
fn mk_numeric_datum(bits: u64, width: u8, mcx: ::mcx::Mcx<'_>) -> PgResult<Datum> {
    let img = ::adt_numeric::numeric_key_unpack(mk_numeric_key_decode(bits, width))?;
    ::types_fmgr::byref_result(mcx, img.as_bytes())
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
            MkCompKind::Numeric { width } => mk_numeric_datum(bits, width, mcx)?,
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
        CompactKeySpec::Reduced(shape) => {
            compact_key_datums_red(ch, shape, row, mk_scratch);
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
        CompactKeySpec::Multi(_) | CompactKeySpec::Reduced(_) => (Datum::null(), false),
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

#[cfg(test)]
mod numeric_key_tests {
    use super::*;

    fn image(s: &str) -> ::adt_numeric::NumericImage {
        ::adt_numeric::numeric_in(s, -1, None).expect("parse").expect("non-soft parse")
    }

    fn datum_of(bytes: &[u8]) -> Datum {
        Datum::from_usize(bytes.as_ptr() as usize)
    }

    #[test]
    fn datum_bits_roundtrip_byte_identical() {
        let owner = ::mcx::MemoryContext::new_bump("numeric-key-test");
        let mcx = owner.mcx();
        for w in [4u8, 8] {
            for s in ["0", "1", "-1", "59", "1.5", "-0.07", "8388607", "-8388607", "NaN",
                      "Infinity", "-Infinity"] {
                let img = image(s);
                let bits = mk_numeric_datum_bits(datum_of(img.as_bytes()), w)
                    .unwrap_or_else(|| panic!("{s} must pack at width {w}"));
                let d = mk_numeric_datum(bits, w, mcx).expect("read-back");
                // SAFETY: byref_result produced a live 4B-header varlena.
                let back = unsafe { ::datum::VarlenaRef::from_ptr(d.as_usize() as *const u8) };
                assert_eq!(back.as_bytes(), img.as_bytes(), "{s} at width {w}");
            }
        }
    }

    #[test]
    fn width4_range_gate_is_exact() {
        let img_in = image("8388607");
        assert!(mk_numeric_datum_bits(datum_of(img_in.as_bytes()), 4).is_some());
        let img_out = image("8388608");
        assert_eq!(mk_numeric_datum_bits(datum_of(img_out.as_bytes()), 4), None);
        assert!(mk_numeric_datum_bits(datum_of(img_out.as_bytes()), 8).is_some());
    }

    #[test]
    fn non_minimal_display_scale_refuses() {
        for s in ["1.0", "1.50", "0.00"] {
            let img = image(s);
            assert_eq!(mk_numeric_datum_bits(datum_of(img.as_bytes()), 8), None, "{s}");
        }
    }

    #[test]
    fn short_header_datums_realign_and_pack() {
        // 1B-short varlena image of the same payload: the pack path must
        // copy it into the aligned scratch (heap tuple-packed numerics).
        let img = image("59");
        let payload = &img.as_bytes()[4..];
        let mut short = Vec::with_capacity(payload.len() + 1);
        short.push((((payload.len() + 1) as u8) << 1) | 1);
        short.extend_from_slice(payload);
        let a = mk_numeric_datum_bits(datum_of(&short), 4).expect("short image packs");
        let b = mk_numeric_datum_bits(datum_of(img.as_bytes()), 4).expect("long image packs");
        assert_eq!(a, b, "short and long images of one value pack identically");
    }

    #[test]
    fn i64_bits_match_materialized_datum_bits() {
        // The integer fast pack (Q19 extract-key class) must produce the
        // EXACT bits of the datum path over int64_to_numeric — same key,
        // same read-back datum — across the trailing-zero ladder, signs,
        // the width-4/8 range gates, and a deterministic sweep.
        let mut cases: Vec<i64> = (-70..=70).collect();
        cases.extend_from_slice(&[
            100, -100, 1000, 9999, 10000, 123450, 8388600, 8388607, 8388608, -8388607,
            -8388608, 83886070, 83886080, i64::MAX, i64::MIN, i64::MIN + 1,
        ]);
        let mut x: u64 = 0x243f6a8885a308d3;
        for _ in 0..2000 {
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            cases.push(x as i64);
            x = x.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            cases.push((x as i64) % 10_000);
        }
        for w in [4u8, 8] {
            for &v in &cases {
                let img = ::adt_numeric::int64_to_numeric(v);
                let datum_bits = mk_numeric_datum_bits(datum_of(img.as_bytes()), w);
                assert_eq!(mk_numeric_i64_bits(v, w), datum_bits, "v={v} width={w}");
            }
        }
    }

    #[test]
    fn distinct_values_pack_distinct_bits() {
        let mut seen = std::collections::HashSet::new();
        for s in ["0", "1", "-1", "10", "0.1", "59", "NaN", "Infinity", "-Infinity"] {
            let img = image(s);
            let bits = mk_numeric_datum_bits(datum_of(img.as_bytes()), 4).unwrap();
            assert!(seen.insert(bits), "distinct bits for {s}");
        }
    }
}

/// Read-back: the next compact group as (populated `first_slot`, pergroup).
/// Row (insertion) order; no spill refill (compact builds never spill).
/// `None` = drained. Cursor rides `ph.hashiter` (reset by the same sites the
/// C iterator's reset rides). `cut`: the lane's armed emit-side top-N
/// boundary (lane-v2 topnemit) — rows strictly worse than the downstream
/// bounded sort's k-th boundary are skipped HERE, before any key
/// reconstruction / intern materialization (admission proved the skipped
/// emit body observation-free).
pub(crate) fn compact_retrieve_next<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    cut: Option<&mut crate::TopnEmitCut<'_>>,
) -> PgResult<Option<NonNull<AggPerGroup>>> {
    let mcx = estate.es_query_cxt;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let mut row = ph.hashiter;
    let nrows = ph.compact.as_ref().expect("compact retrieve requires the table").table.nrows();
    if let Some(c) = cut {
        let table = &ph.compact.as_ref().expect("compact retrieve requires the table").table;
        while row < nrows {
            // SAFETY: the row's state block is the group's live AggPerGroup
            // array; transno < its length (resolve checked this node).
            let pg = unsafe {
                &*table.row_states(row).cast::<AggPerGroup>().add(c.spec.transno as usize)
            };
            if !c.skips(pg) {
                break;
            }
            *c.skipped += 1;
            row += 1;
            // The elided sort put's per-row cadence.
            ::postgres_seams::check_for_interrupts::call()?;
        }
    }
    if row >= nrows {
        ph.hashiter = row;
        return Ok(None);
    }
    ph.hashiter = row + 1;
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
        CompactKeySpec::Reduced(shape) => {
            // Redundant keys reconstructed from the representative word —
            // byval int datums, no materialization.
            let mut vals: Vec<(Datum, bool)> = Vec::with_capacity(shape.keys.len());
            compact_key_datums_red(ch, shape, row, &mut vals);
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

// ===========================================================================
// Lane-v2 batchemit block machinery (see the invariant block at
// `crate::batch_emit_resolve`): the block scan replaces the per-group
// `compact_retrieve_next` cursor walk, and the row build replaces
// finalize_aggregates + qual/projection for the admitted column vocabulary.
// ===========================================================================

/// Block granule of the batched compact emit: bounded per-tuple-context
/// residency (every finalized NUMERIC image lives only until the block's
/// sort puts copy it), and the boundary-cut hoist window (the topnemit
/// boundary is re-read once per block — a staler i.e. LOOSER boundary only
/// under-skips, and every under-skipped group is one the downstream bounded
/// sort discards with no state change; boundaries only tighten as puts land).
pub const BATCH_EMIT_BLOCK: usize = 1024;

/// Fill `plan.idx` with the next block of surviving compact rows (row /
/// insertion order — `compact_retrieve_next`'s exact walk), advancing
/// `ph.hashiter`. `cut`: the lane's emit-side top-N boundary, applied per
/// row exactly as the per-row retrieve applies it (same `skips` predicate,
/// same skipped-group accounting). Returns (survivors, drained); `drained`
/// also flips `agg_done`, the per-row retrieve's EOF contract. The
/// block-granular ExprContext reset happens HERE — the previous block's
/// finalized images were copied by its sort puts before this call.
pub fn batch_emit_scan_block<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan: &mut crate::BatchEmitPlan,
    mut cut: Option<crate::TopnEmitCut<'_>>,
) -> PgResult<(u32, bool)> {
    estate.reset_expr_context(node.ps_ExprContext);
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.as_ref().expect("batch emit requires the compact table");
    let nrows = ch.table.nrows();
    let mut row = ph.hashiter;
    plan.idx.clear();
    while row < nrows && plan.idx.len() < BATCH_EMIT_BLOCK {
        // The per-group retrieve cadence (skipped and emitted alike).
        ::postgres_seams::check_for_interrupts::call()?;
        if let Some(c) = cut.as_mut() {
            // SAFETY: the row's state block is the group's live AggPerGroup
            // array; transno < its length (resolve checked this node).
            let pg = unsafe {
                &*ch.table.row_states(row).cast::<AggPerGroup>().add(c.spec.transno as usize)
            };
            if c.skips(pg) {
                *c.skipped += 1;
                row += 1;
                continue;
            }
        }
        plan.idx.push(row as u32);
        row += 1;
    }
    ph.hashiter = row;
    let drained = row >= nrows;
    if drained {
        node.agg_done = true;
    }
    Ok((plan.idx.len() as u32, drained))
}

#[cold]
#[inline(never)]
fn bad_int8_transarray() -> Box<::types_error::PgError> {
    // int8_transarray's (numeric.c int8_avg family) exact error.
    Box::new(::types_error::PgError::error("expected 2-element int8 array"))
}

/// `int8_avg`'s transarray read without the fmgr frame: the SAME image
/// validation `adt_numeric::int8_transarray` performs (4B-U size == 24 + 16,
/// no null bitmap; a tuple-queue-packed 1B short image validates at the
/// packed size and reads unaligned), then the {count,sum} pair.
///
/// # Safety
/// `d` is a non-null int8[2] transvalue datum (aggcontext-lived image).
unsafe fn int8_avg_trans_read(d: Datum) -> PgResult<(i64, i64)> {
    use ::types_tuple::varatt;
    const ARR_OVERHEAD_NONULLS_1: usize = 24;
    const INT8_TRANSARRAY_SIZE: usize = ARR_OVERHEAD_NONULLS_1 + 16;
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract — live varlena image.
    unsafe {
        if varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p) {
            // Tuple-packed short image: 1-byte header, then the 4B-U payload
            // minus its 4-byte length word (ndim, dataoffset, elemtype, dim,
            // lbound, data), unaligned.
            let payload = varatt::varsize_1b(p) - 1;
            let hasnull = core::ptr::read_unaligned(p.add(1 + 4).cast::<i32>()) != 0;
            if hasnull || payload + 4 != INT8_TRANSARRAY_SIZE {
                return Err(bad_int8_transarray());
            }
            let data = p.add(1 + ARR_OVERHEAD_NONULLS_1 - 4);
            return Ok((
                core::ptr::read_unaligned(data.cast::<i64>()),
                core::ptr::read_unaligned(data.add(8).cast::<i64>()),
            ));
        }
        if !varatt::varatt_is_4b_u(p) {
            // int8_transarray's exact unreachable-arm behavior.
            panic!("int8 transarray: toasted array datum (detoast unported)");
        }
        let size = varatt::varsize_4b(p);
        let hasnull = p.add(8).cast::<i32>().read() != 0;
        if hasnull || size != INT8_TRANSARRAY_SIZE {
            return Err(bad_int8_transarray());
        }
        let data = p.add(ARR_OVERHEAD_NONULLS_1).cast::<i64>();
        Ok((data.read(), data.add(1).read()))
    }
}

#[cfg(test)]
mod batch_emit_tests {
    use super::*;

    /// An 8-aligned 4B-U int8[2] {count,sum} transarray image — the exact
    /// layout int4_avg_accum/int2_avg_accum build and int8_transarray reads.
    #[repr(align(8))]
    struct Aligned([u8; 40]);

    fn transarray(count: i64, sum: i64) -> Aligned {
        let mut buf = [0u8; 40];
        buf[0..4]
            .copy_from_slice(&::types_tuple::varatt::set_varsize_4b_word(40).to_ne_bytes());
        buf[4..8].copy_from_slice(&1i32.to_ne_bytes()); // ndim
        buf[8..12].copy_from_slice(&0i32.to_ne_bytes()); // dataoffset (no nulls)
        buf[12..16].copy_from_slice(&20i32.to_ne_bytes()); // elemtype int8
        buf[16..20].copy_from_slice(&2i32.to_ne_bytes()); // dim
        buf[20..24].copy_from_slice(&1i32.to_ne_bytes()); // lbound
        buf[24..32].copy_from_slice(&count.to_ne_bytes());
        buf[32..40].copy_from_slice(&sum.to_ne_bytes());
        Aligned(buf)
    }

    #[test]
    fn transarray_read_matches_layout() {
        for (c, s) in
            [(0, 0), (1, 5), (7, -123456789), (i64::MAX, i64::MIN), (1234567, 42)]
        {
            let img = transarray(c, s);
            let d = Datum::from_usize(img.0.as_ptr() as usize);
            // SAFETY: live, aligned int8[2] image.
            let got = unsafe { int8_avg_trans_read(d) }.expect("valid transarray");
            assert_eq!(got, (c, s));
        }
    }

    #[test]
    fn transarray_read_packed_short_image() {
        // Tuple-packed short form: 1-byte header + the 36-byte payload
        // (everything after the 4-byte length word), misaligned on purpose.
        let full = transarray(9, -42);
        let mut buf = [0u8; 64];
        let p = unsafe { buf.as_mut_ptr().add(3) };
        // SAFETY: 37 bytes fit in buf past offset 3.
        unsafe {
            ::types_tuple::varatt::set_varsize_short(p, 37);
            core::ptr::copy_nonoverlapping(full.0.as_ptr().add(4), p.add(1), 36);
        }
        // SAFETY: live short image.
        let got = unsafe { int8_avg_trans_read(Datum::from_usize(p as usize)) }
            .expect("valid packed transarray");
        assert_eq!(got, (9, -42));
    }

    #[test]
    fn transarray_read_rejects_bad_images() {
        // Null bitmap present (dataoffset != 0) — int8_transarray's refuse.
        let mut img = transarray(1, 2);
        img.0[8..12].copy_from_slice(&24i32.to_ne_bytes());
        // SAFETY: live image.
        assert!(unsafe { int8_avg_trans_read(Datum::from_usize(img.0.as_ptr() as usize)) }
            .is_err());
        // Wrong size (not exactly 2 int8 elements).
        #[repr(align(8))]
        struct Big([u8; 48]);
        let mut big = Big([0u8; 48]);
        big.0[0..4]
            .copy_from_slice(&::types_tuple::varatt::set_varsize_4b_word(48).to_ne_bytes());
        // SAFETY: live image.
        assert!(unsafe { int8_avg_trans_read(Datum::from_usize(big.0.as_ptr() as usize)) }
            .is_err());
    }

    /// The batched avg kernel composition (reader → int64_avg_div) feeds the
    /// SAME operands the fmgr finalfn parses, so the images are identical
    /// (int64_avg_div itself is pinned against div_var by adt_numeric's
    /// differential corpus).
    #[test]
    fn avg_int8_kernel_reader_operand_parity() {
        for (c, s) in
            [(1i64, 0i64), (3, 10), (7, -22), (9, i64::MAX / 2), (1_000_000, 999_999)]
        {
            let arr = transarray(c, s);
            // SAFETY: live, aligned image.
            let (rc, rs) =
                unsafe { int8_avg_trans_read(Datum::from_usize(arr.0.as_ptr() as usize)) }
                    .expect("valid transarray");
            assert_eq!((rc, rs), (c, s));
            let a = ::adt_numeric::ops::int64_avg_div(s, c).expect("avg image");
            let b = ::adt_numeric::ops::int64_avg_div(rs, rc).expect("avg image");
            assert_eq!(a.as_bytes(), b.as_bytes());
        }
    }
}

/// Build surviving block row `i` (a `plan.idx` position from the last
/// `batch_emit_scan_block`) directly into the node's result slot: grouping
/// keys through the SAME compact read-back legs the per-row retrieve uses
/// (`compact_key_datum` / `compact_key_datums_mk` / `compact_key_datums_red`
/// — interned text still materializes into the node-lifetime table context),
/// aggregates through the batched finalize kernels (invariant block at
/// `crate::batch_emit_resolve`). Returns the populated result slot id — the
/// same slot `exec_project` would have filled, with byte-identical datums.
pub fn batch_emit_row<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan: &mut crate::BatchEmitPlan,
    i: u32,
) -> PgResult<::executils::ExecSlotId> {
    use crate::BatchEmitCol;
    let row = plan.idx[i as usize] as usize;
    let per_tuple = estate.ecxt(node.ps_ExprContext).per_tuple_mcx();
    {
        let crate::BatchEmitPlan { cols, keyvals, vals, .. } = &mut *plan;
        let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
        let ch = ph.compact.as_ref().expect("batch emit requires the compact table");
        match &ch.key {
            CompactKeySpec::Single { width } => {
                keyvals.clear();
                keyvals.push(match compact_key_datum(ch, *width, row) {
                    Some(d) => (d, false),
                    None => (Datum::null(), true),
                });
            }
            CompactKeySpec::Multi(shape) => {
                compact_key_datums_mk(ch, shape, row, ph.table_ctx.mcx(), keyvals)?;
            }
            CompactKeySpec::Reduced(shape) => {
                compact_key_datums_red(ch, shape, row, keyvals);
            }
        }
        // SAFETY: the row's state block is the group's live AggPerGroup
        // array; every referenced transno < its length (resolve checked).
        let states = ch.table.row_states(row).cast::<AggPerGroup>();
        let pg_at = |t: u32| unsafe { &*states.add(t as usize) };
        vals.clear();
        for col in cols.iter() {
            let nd = match col {
                BatchEmitCol::Key(j) => keyvals[*j as usize],
                BatchEmitCol::Const { value, isnull } => (*value, *isnull),
                // The per-row finalize's no-finalfn arm over a byval
                // transtype: the raw transvalue word.
                BatchEmitCol::Trans(t) => {
                    let pg = pg_at(*t);
                    (pg.trans_value, pg.trans_value_is_null)
                }
                // fc_int8_avg: strict (NULL trans → NULL), count == 0 →
                // NULL, else the test-pinned int64_avg_div image.
                BatchEmitCol::AvgInt8(t) => {
                    let pg = pg_at(*t);
                    if pg.trans_value_is_null {
                        (Datum::null(), true)
                    } else {
                        // SAFETY: non-null int8[2] transvalue (admission).
                        let (count, sum) = unsafe { int8_avg_trans_read(pg.trans_value)? };
                        if count == 0 {
                            (Datum::null(), true)
                        } else {
                            let img = ::adt_numeric::ops::int64_avg_div(sum, count)?;
                            (::types_fmgr::byref_result(per_tuple, img.as_bytes())?, false)
                        }
                    }
                }
                // fc_numeric_poly_avg / fc_numeric_poly_sum: the fcs' exact
                // cores over the aggcontext-lived Int128AggState (NULL trans
                // → None → NULL, n == 0 → None → NULL).
                BatchEmitCol::AvgInt128(t) | BatchEmitCol::SumInt128(t) => {
                    let pg = pg_at(*t);
                    // SAFETY: a non-null INTERNAL transvalue is the
                    // aggcontext-lived Int128AggState (transfn contract);
                    // sole reference during the call.
                    let state = (!pg.trans_value_is_null).then(|| unsafe {
                        &*(pg.trans_value.as_usize()
                            as *const ::adt_numeric::aggregates::Int128AggState)
                    });
                    let img = match col {
                        BatchEmitCol::AvgInt128(_) => {
                            ::adt_numeric::aggregates::numeric_poly_avg(state)?
                        }
                        _ => ::adt_numeric::aggregates::numeric_poly_sum(state)?,
                    };
                    match img {
                        Some(img) => {
                            (::types_fmgr::byref_result(per_tuple, img.as_bytes())?, false)
                        }
                        None => (Datum::null(), true),
                    }
                }
            };
            vals.push(nd);
        }
    }
    // The projection's slot discipline (exec_project_prearmed): clear, fill,
    // store virtual.
    let mcx = estate.es_query_cxt;
    let slot = estate.slot_mut(node.ps_ResultTupleSlot);
    ::exectuples::exec_clear_tuple(slot, mcx);
    {
        let base = slot.base_mut();
        for (v, &(d, isnull)) in plan.vals.iter().enumerate() {
            base.tts_values[v] = d;
            base.tts_isnull[v] = isnull;
        }
    }
    ::exectuples::exec_store_virtual_tuple(slot);
    Ok(node.ps_ResultTupleSlot)
}
