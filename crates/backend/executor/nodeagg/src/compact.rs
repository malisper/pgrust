//! Lane-v2 compact-row aggregation table hosting (pgrcolumnar-v2 plan Stage 2.2).
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
    /// (`adt_numeric::keypack` — the ts-extract-key numeric key kind): low `width - 1`
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
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

    /// The shape's FIRST Intern (text) component, when one exists — the M2
    /// sink's canonical-bytes machinery keys "is this a canonical shape" off
    /// it. Shapes may carry up to two Intern components (the CaseDict
    /// class: computed CASE key + bare text Var); the canonical byte image
    /// length-prefixes each tail when more than one exists
    /// ([`crate::sink`]'s `canon_row_bytes` doc).
    #[inline]
    pub fn intern_comp(&self) -> Option<(usize, &MkComp)> {
        self.comps.iter().enumerate().find(|(_, c)| c.kind == MkCompKind::Intern)
    }

    /// All Intern (text) components, in component (key) order — the
    /// canonical image's tail order.
    #[inline]
    pub fn intern_comps(&self) -> impl Iterator<Item = (usize, &MkComp)> {
        self.comps.iter().enumerate().filter(|(_, c)| c.kind == MkCompKind::Intern)
    }

    /// Intern (text) component count.
    #[inline]
    pub fn n_intern(&self) -> usize {
        self.comps.iter().filter(|c| c.kind == MkCompKind::Intern).count()
    }
}

/// The arithmetic of one reconstructable (redundant) grouping key
/// (redundant-key lane, reduced-expr-key class): `Var ± Const` int arithmetic
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
    pub(crate) intern: Option<::lanetable::LaneAggTable>,
    /// Canonical (text-bearing Multi) shapes: row i's sink hash over its
    /// canonical byte image (`sink::sink_hash_bytes` of the row's
    /// `canon_row_bytes`), parallel to the table's rows. Extended at the
    /// BATCH TAIL for newly inserted rows (the accept path — parallel and
    /// cache-warm), so the flush and the single-threaded SEAL partition
    /// never hash: the SEAL collapses to a counting sort over these values
    /// (the expr-key class's @100M settle/finalize serial-tail profile). Cleared with
    /// every table reset (flush) — rows restart per epoch. Empty for word
    /// shapes.
    pub(crate) canon_hashes: Vec<u64>,
    /// STORE-ONCE canonical images (spankey step 2 — the lane owns the
    /// canonical image lifecycle across accept/flush/combine): row i's
    /// canonical byte image at `canon_store[canon_offs[i]..canon_offs[i+1]]`,
    /// built exactly once at the accept-time hash extension and consumed
    /// verbatim by the flush pass-1 and the combine remainder face (no
    /// rebuild: word-unpack + intern-reverse-chase + tail assembly happen
    /// once per group). Self-contained copies — valid across intern resets
    /// and thread crossings (the canonical IMAGE law). Engaged iff
    /// `spankey::spankey_store_enabled()` (kill switch reverts every
    /// consumer to the incumbent rebuild); when engaged
    /// `canon_offs.len() == canon_hashes.len() + 1` (leading 0); cleared
    /// beside `canon_hashes` at every table reset. The canonical SPILL
    /// serialization path deliberately does NOT read this store (condition
    /// of record: spill bytes identical, replay unaware).
    pub(crate) canon_store: Vec<u8>,
    pub(crate) canon_offs: Vec<u32>,
    /// True once the RUNTIME sink drain owns this table (set per worker by
    /// `agg_sink_mark_sink_mode`). Gates the batch-tail canonical hashing:
    /// the serial lane shares this compact table and never flushes or
    /// SEAL-partitions, so accept-time hashes would be pure overhead there.
    /// The flush/partition entries keep their unconditional defensive
    /// extend (first-morsel rows hashed before the flag, and the tests).
    pub(crate) sink_mode: bool,
    /// Intern-table GENERATION (canonical shapes; GID-merge car): bumped on
    /// every wide-vocabulary intern RESET (`agg_sink_flush_if_due` /
    /// `agg_sink_flush_now`). Within one generation a worker's packed key
    /// words are a BIJECTION onto its groups (intern ids are insert-once),
    /// so flushed runs stamped with the generation can merge word-mode at
    /// combine; across generations the words are ambiguous and the combine's
    /// per-worker map resets.
    pub(crate) intern_gen: u64,
    /// avgpack (SINK builds only): bit per transno whose state is the PACKED
    /// inline `[count, sum]` image in the row's 16-byte `AggPerGroup` slot
    /// instead of an aggcontext transarray pointer
    /// ([`crate::sink::sink_avgpack_mask`]). Decided at TABLE CREATION —
    /// `ph.sink_cap` is installed before the worker's try_arm — so one
    /// table never mixes representations. 0 everywhere else (the serial
    /// arm, the leader's own node, migration-eligible builds).
    pub(crate) avgpack_mask: u64,
    /// arena-strings inc-3: TRUE = the DIRECT single-text arm — `table` is
    /// `KeyRepr::Bytes` keyed on the canonical image itself (the mk1
    /// 1-Intern canonical bytes: `packed_bytes` zeroed id bytes + the raw
    /// text, `sink::canon_row_bytes`' exact encoding), probed with
    /// `sink::sink_hash_bytes` as the probe hash, `intern` is None, and the
    /// store-once canon fields stay empty (the table IS the canonical
    /// store). Every flush RESETS the table (it is the vocabulary), so the
    /// flush signals the cache-invalidation channel unconditionally.
    pub(crate) text_direct: bool,
    /// Direct-arm probe scratch: the canonical image under construction
    /// (prefix + text), reused across probes.
    pub(crate) direct_img: Vec<u8>,
    /// GL-DICTDRAIN-3: the table-owned by-ref str transvalue store for
    /// MIGRATING sink tables (armed by `sink::agg_sink_arm_str_state` on
    /// the dict-coded sink drain). It TRAVELS WITH the table through the
    /// morsel lend/reclaim, so every copy and replace-free hits the same
    /// allocator regardless of which pool thread runs the morsel — the
    /// allocator-exactness the per-thread freeing context could not give a
    /// thread-hopping table (the t45 sink-shape-violation revert).
    /// RefCell: the fold reads the node immutably; mutation is morsel-
    /// serialized (&mut Local per claim) and the combine/emit phase never
    /// borrows it (reads value BYTES through state pointers only).
    pub(crate) str_arena: Option<Box<core::cell::RefCell<::lanefold::StrStateArena>>>,
    // Batch scratch (canonical keys + probe outputs), reused across batches.
    keys: Vec<i64>,
    states: Vec<*mut u8>,
    hashes: Vec<u64>,
    new_rows: Vec<u32>,
}

/// Test-only constructor (the sink's canonical-bytes unit tests build bare
/// compact states around prebuilt tables; the batch scratch stays empty).
#[cfg(test)]
pub(crate) fn compact_hash_for_tests(
    table: ::lanetable::LaneAggTable,
    key: CompactKeySpec,
    intern: Option<::lanetable::LaneAggTable>,
) -> CompactHash {
    CompactHash {
        table,
        key,
        intern,
        canon_hashes: Vec::new(),
        canon_store: Vec::new(),
        canon_offs: Vec::new(),
        sink_mode: false,
        intern_gen: 0,
        avgpack_mask: 0,
        text_direct: false,
        direct_img: Vec::new(),
        str_arena: None,
        keys: Vec::new(),
        states: Vec::new(),
        hashes: Vec::new(),
        new_rows: Vec::new(),
    }
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

/// `PGRUST_LANETABLE_BATCH_INSTALL` (GL-ALPHA1-COUNTERS-1 Phase B, default
/// OFF; `1`/`on` arms): batched-install accept for the single-int-key
/// datum-lane batch — deferred-install probe (frozen pass + row-order
/// install pass with write-intent prefetch) + transno-outer group seeding.
/// Output-byte-identical by construction (same hash bytes, same create
/// order, same probe-walk family; only the WHEN of the installs moves).
/// OFF keeps every incumbent path branch-for-branch.
pub fn compact_batch_install_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        matches!(
            std::env::var("PGRUST_LANETABLE_BATCH_INSTALL").as_deref(),
            Ok("1") | Ok("on")
        )
    })
}

/// arena-strings inc-3 arm switch (`PGRUST_LANE_V2_TEXT_DIRECT`, **default
/// ON** since letter GL-ARENASTR-1; `=0`/`off` is the exact-spelling kill):
/// DIRECT single-text worker accept tables. The M2 sink's SINGLE-TEXT class
/// (mk1: one non-nullable Intern component — the wide-vocabulary `GROUP BY url`
/// shape) keys its LOCAL table directly on the CANONICAL IMAGE bytes
/// (`KeyRepr::Bytes`, probed with `sink::sink_hash_bytes` — the saved hash
/// word IS the sink hash) instead of the intern-id indirection
/// (text → intern id → packed image → Int probe → store-once canon rebuild).
/// SINK worker builds only (`ph.sink_cap` installed before the arm); the
/// serial lane and the coded-group (long-text regexp class) mk1 consumers keep the intern arm
/// verbatim regardless of the env. Kill = byte-identical incumbent paths.
///
/// FLIP PROVENANCE (letter GL-ARENASTR-1, scratchpad/night/, the mt16
/// forced-plan fleet channel, bench conf v2.1, dist builds, banks cbstore9-v8[x]-
/// sorted-v2): TDonly @ 34d377de5, c8gd — url-key 0.825 / const-tlist 0.808 / expr-key
/// 1.000 (FLAT) @10M (damped geomean 0.874); url-key 0.933 / const-tlist 0.928 / expr-key
/// 0.971 @100M (0.944). Parity: outputs byte-identical across arms, both
/// scales, both storage arms, every DOP mode (md5 matrix; explain-channel
/// sorted-sha single). Expr-key guard: zero direct-arm traces (expr-key class
/// out of scope), flush_bytes A0-class. DOP matrix @10M: wins every mode
/// ~15-20%. The sibling inc-1 STEAL knob stays OFF (its expr-key flush-bytes
/// inflation failed the guard — re-letter only after store compaction).
pub fn text_direct_enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_TEXT_DIRECT").as_deref(), Ok("0") | Ok("off"))
    })
}

/// mkaccept inc-1 kill switch (census U4): fused mk accept lanes — the
/// packed-key lane is VIEWED in place instead of repacked, and the probe
/// writes the state-pointer lane directly into the caller's groups vec
/// instead of through the `CompactHash::states` scratch + copy. `=0`
/// restores both copy paths. The fused lanes carry bit-identical values to
/// the copies they elide; this gate exists for A/B and revert.
fn mkaccept_fused() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| std::env::var("PGRUST_RUNTIME_AGG_MKACCEPT").map_or(true, |v| v != "0"))
}

/// The two-word packed key lane for the mk2 probe: on little-endian targets
/// (with the mkaccept switch on) an IN-PLACE view of the pack pre-pass's
/// `u128` accumulator — `[w as u64, (w >> 64) as u64]` IS the LE memory
/// image, so the view is bit-identical to the repack it elides. Otherwise
/// the historical repack into `scratch`.
pub fn mk_keys2_lane<'a>(packbuf: &'a [u128], scratch: &'a mut Vec<[u64; 2]>) -> &'a [[u64; 2]] {
    const _SIZE: () = assert!(core::mem::size_of::<u128>() == core::mem::size_of::<[u64; 2]>());
    const _ALIGN: () = assert!(core::mem::align_of::<u128>() >= core::mem::align_of::<[u64; 2]>());
    if cfg!(target_endian = "little") && mkaccept_fused() {
        // SAFETY: same element size, stronger alignment, and on LE the
        // u128's bytes are exactly [low u64, high u64] — the repack's
        // element values. Shared borrow for the probe's read-only pass.
        unsafe { core::slice::from_raw_parts(packbuf.as_ptr().cast::<[u64; 2]>(), packbuf.len()) }
    } else {
        scratch.clear();
        scratch.extend(packbuf.iter().map(|&w| [w as u64, (w >> 64) as u64]));
        scratch
    }
}

/// Take the caller's groups vec as the probe's raw `*mut u8` out-lane
/// (recycling its buffer — no allocation churn across batches).
#[inline]
fn groups_take_raw(groups: &mut Vec<NonNull<AggPerGroup>>) -> Vec<*mut u8> {
    let mut v = core::mem::ManuallyDrop::new(core::mem::take(groups));
    // SAFETY: `NonNull<AggPerGroup>` and `*mut u8` have identical size and
    // alignment (pointer-sized); length 0 reinterprets no element (NonNull
    // is Copy — forgetting the stale elements is sound); capacity and
    // allocator carry over unchanged (Vec::from_raw_parts contract).
    unsafe { Vec::from_raw_parts(v.as_mut_ptr().cast::<*mut u8>(), 0, v.capacity()) }
}

/// Hand the probed state-pointer lane back as the caller's groups vec —
/// BEFORE any fallible step, so an error path never leaks the buffer.
#[inline]
fn groups_restore(groups: &mut Vec<NonNull<AggPerGroup>>, raw: Vec<*mut u8>) {
    let mut raw = core::mem::ManuallyDrop::new(raw);
    let (len, cap) = (raw.len(), raw.capacity());
    // SAFETY: layout as in [`groups_take_raw`]; every element was written
    // by the batch probe, which never returns null state pointers (the
    // existing mk batch contract), so the NonNull invariant holds.
    *groups =
        unsafe { Vec::from_raw_parts(raw.as_mut_ptr().cast::<NonNull<AggPerGroup>>(), len, cap) };
}

/// The groups lane as [`seed_new_groups`]'s `*mut u8` slice.
#[inline]
fn groups_ptr_slice(groups: &[NonNull<AggPerGroup>]) -> &[*mut u8] {
    // SAFETY: identical element layout (pointer-sized), shared borrow.
    unsafe { core::slice::from_raw_parts(groups.as_ptr().cast::<*mut u8>(), groups.len()) }
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

/// THE single-word-key spill-eligibility gate at the compact table's HALF
/// MARGIN — the single source of the SpillRisk arithmetic (inc-2c): entry
/// (8 B at ≤0.5 fill → 16), key word, states, and a transvalue-slack
/// allowance per group. `true` = the arm would refuse `numgroups` as
/// spill-eligible. Every caller — `agg_hash_compact_try_arm`,
/// `compact_single_word_gates` (Reduced), `agg_hash_spill_unlikely`, and
/// the sink leader mirror `agg_hash_compact_sink_would_refuse` — asks this
/// so the two sides of the M2 sink engagement can never diverge.
fn single_word_spillrisk(ph: &PerHashData<'_>, numgroups: u64) -> bool {
    let additionalsize = ph.hashtable.additionalsize();
    let est_bytes = numgroups.saturating_mul(16 + 8 + additionalsize as u64 + 16);
    numgroups > ph.hash_ngroups_limit / 2 || est_bytes > ph.hash_mem_limit as u64 / 2
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
    !single_word_spillrisk(ph, numgroups)
}

/// Leader-side mirror of the SINK WORKER's compact-arm spill gate (inc-2c,
/// the leg-4d wedge class): would `agg_hash_compact_try_arm` /
/// `agg_hash_compact_try_arm_reduced` SpillRisk-refuse on a worker build of
/// this plan under sink cap `cap` (`agg_sink_set_cap` installed before the
/// worker's try_arm, so the worker's group estimate is min'd to the cap)?
/// READ-ONLY by contract: the leader's own node may already carry a
/// serial-armed compact table (`ph.compact`) — nothing here consults or
/// mutates it; only the plan estimate and the ph limits are read. Both sink
/// drain modes (K2 single-key and expr-key Single/Reduced) arm through the
/// same single-word gate, so one predicate covers them. Conservative:
/// `true` also for shapes whose aggsplit divisor refuses.
pub fn agg_hash_compact_sink_would_refuse(node: &AggStateData<'_>, cap: u32) -> bool {
    let Some(divisor) = compact_split_divisor(node.plan.aggsplit) else {
        return true;
    };
    let numgroups = (node.plan.numGroups.max(1) as u64 / divisor)
        .max(1)
        .min(cap as u64);
    let Some(ph) = node.perhash.as_ref() else {
        return true;
    };
    // Mirror the arms' extra-column refusal (fdgroup-wr): a worker build
    // with stored non-key columns refuses KeyKind.
    if ph.hash_grp_col_idx_input.len() > ph.num_cols {
        return true;
    }
    single_word_spillrisk(ph, numgroups)
}

/// Read-only LEADER-side admission precheck for the M2 runtime agg sink
/// (F1 chaos fix, defect layer 1 root cause): would a WORKER build's
/// `agg_hash_compact_try_arm*` arm under sink cap `cap`? Replicates the
/// sink-mode gate exactly — cap-bounded numgroups (the `sink_cap` leg of
/// `compact_single_word_gates`/`agg_hash_compact_try_arm`) against the
/// half-margin spill-eligibility formula — WITHOUT installing a table or
/// touching `sink_cap`. The workers run under the leader's restored GUCs,
/// so the leader's `hash_mem_limit`/`hash_ngroups_limit` are the workers'
/// numbers: false here means EVERY worker would refuse
/// ("worker compact arm refused under the sink cap"), erroring before it
/// ever joined the drive and stranding the pinned RG — the leader must
/// refuse engagement up front (fail-closed → serial arm) instead.
pub fn agg_hash_compact_sink_admissible(node: &AggStateData<'_>, cap: u32, spill_ok: bool) -> bool {
    if !compact_enabled() {
        return false;
    }
    let Some(divisor) = compact_split_divisor(node.plan.aggsplit) else {
        return false;
    };
    let numgroups = (node.plan.numGroups.max(1) as u64 / divisor)
        .max(1)
        .min(cap as u64);
    let Some(ph) = node.perhash.as_ref() else {
        return false;
    };
    // Mirror the arms' extra-column refusal (fdgroup-wr): every worker
    // would KeyKind-refuse a shape whose hash rows store non-key columns.
    if ph.hash_grp_col_idx_input.len() > ph.num_cols {
        return false;
    }
    // M3.5 spill-armed admission (the ~10M-group @100M hmm=2 cliff): with a live
    // spill arm on a word-keyed engagement the WORKER gates vacate the
    // estimate refusal (compact_single_word_gates / try_arm / mk_admit_n
    // under `sink_spill_ok`), so this leader mirror must vacate it too —
    // the F1 invariant is leader verdict == worker verdict, in both
    // directions. `spill_ok` here = the engagement's spill arm is live AND
    // the shape is word-keyed (the caller's `canon` predicate).
    if spill_ok {
        return true;
    }
    let additionalsize = ph.hashtable.additionalsize();
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
    // The compact table stores the packed key + agg states ONLY; its
    // read-back reconstructs just the grouping key. A stored extra column
    // (a functionally-dependent tlist Var riding the hash entry after the
    // planner reduced GROUP BY to the PK — fdgroup-wr, compat-matrix B4)
    // would emit NULL: refuse, keep the C tuplehash (full-image entries).
    if ph.hash_grp_col_idx_input.len() > ph.num_cols {
        return CompactArm::KeyKind;
    }
    let Some(width) = ph.hashtable.staged_probe_int_width() else {
        return CompactArm::KeyKind;
    };
    let additionalsize = ph.hashtable.additionalsize();
    // SE-GROUPONLY: zero-transition (grouping-only) builds carry 0-byte
    // state rows — legal since the vacuous fold plan (lanefold::empty_plan).
    debug_assert!(
        additionalsize > 0 || node.trans_init.is_empty(),
        "K2 shapes carry a fold plan (numtrans > 0) or none at all (group-only)"
    );
    // Spill-eligibility estimate at half margin ([`single_word_spillrisk`],
    // the single-sourced arithmetic — the M2 sink leader mirror reads it
    // too). M3.5 spill-armed sink builds vacate the estimate refusal
    // (single-word keys are always spillable; the cap bounds the table).
    if single_word_spillrisk(ph, numgroups) && !(ph.sink_cap.is_some() && ph.sink_spill_ok) {
        return CompactArm::SpillRisk;
    }
    // avgpack: packed inline AvgInt8 states, SINK builds only (decided at
    // table creation — before any group seeds).
    let avgpack_mask = if ph.sink_cap.is_some() { node.avgpack_shape_mask } else { 0 };
    // Entry layout by planner group estimate. Inline16 resolves hits from
    // the entry line alone (key inline; the probe never touches the payload
    // row) — ONE serialized miss per hit instead of Salt8's entry→row
    // two-load chain, with the row/states miss overlapped by the probe-time
    // states prefetch + the separate fold pass. The in-situ dict-int-key A/B
    // (2026-07-15) showed the 1.5-4M DRAM-bound band is chain-latency bound
    // (eliminating instruction overhead alone moved nothing), so Inline16's
    // band is 4M (was 1M); the old pod A/B's 8.4M-band loss (2x entry
    // bytes) keeps Salt8 above. Underestimates are bounded by the runtime
    // migration backstop (half hash_mem).
    let layout = if numgroups <= (1 << 22) {
        ::lanetable::EntryLayout::Inline16
    } else {
        ::lanetable::EntryLayout::Salt8
    };
    ph.compact = Some(CompactHash {
        table: ::lanetable::LaneAggTable::with_config(
            ::lanetable::KeyRepr::Int,
            additionalsize,
            // Capacity hint: the planner group estimate, honored well past
            // the two-level threshold (the table presizes its 256 buckets
            // from it — a dict-int-key 1.5M-group estimate previously clamped to 1M
            // and was then discarded at conversion). The 8M cap bounds the
            // birth prealloc at 256MB of entries; the spill/backstop gates
            // already bounded the estimate against hash_mem.
            (numgroups as usize).min(1 << 23),
            ::lanetable::HashKind::best(),
            layout,
        ),
        key: CompactKeySpec::Single { width },
        intern: None,
        canon_hashes: Vec::new(),
        canon_store: Vec::new(),
        canon_offs: Vec::new(),
        sink_mode: false,
        intern_gen: 0,
        avgpack_mask,
        text_direct: false,
        direct_img: Vec::new(),
        str_arena: None,
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
    let buf;
    let atts: &[u16] = match dict_att {
        Some(a) => {
            buf = [a];
            &buf
        }
        None => &[],
    };
    try_arm_mk_n(node, nullable, atts, 2)
}

/// [`agg_hash_compact_try_arm_mk`] over a SET of Intern (text) components
/// (band-2a CaseDict computed-text-key class): every att in `intern_atts` packs as a
/// 4-byte intern id through the SHARED intern pool (ids only distinguish
/// equal bytes, so one pool serves any number of components; read-back maps
/// each component id through the same reverse map). SINK admission caps
/// Intern components at TWO — the canonical multi-tail encoding
/// (canon-sink car 1, `PGRUST_RUNTIME_AGG_TEXT2`); wider shapes refuse the
/// sink upstream (`mk_shape_sink_ok`). Unprojected two-text scan feeds
/// reach here behind `PGRUST_LANE_V2_MULTIKEY_TEXT` (SE-MKTEXT).
pub fn agg_hash_compact_try_arm_mk_multi(
    node: &mut AggStateData<'_>,
    nullable: bool,
    intern_atts: &[u16],
) -> CompactArm {
    try_arm_mk_n(node, nullable, intern_atts, 2)
}

/// [`agg_hash_compact_try_arm_mk`] with the arity gate relaxed to ONE
/// grouping key — the M2 sink's SINGLE-TEXT worker arm (a 1-component
/// Intern shape riding the packed machinery: the intern id is the packed
/// image; canonical raw bytes are the cross-worker merge key). Serial
/// callers keep the >=2 gate verbatim (`scan_mk_plan_wanted` owns the
/// single-key kernels there).
pub fn agg_hash_compact_try_arm_mk1(
    node: &mut AggStateData<'_>,
    dict_att: Option<u16>,
) -> CompactArm {
    let buf;
    let atts: &[u16] = match dict_att {
        Some(a) => {
            buf = [a];
            &buf
        }
        None => &[],
    };
    try_arm_mk_n(node, false, atts, 1)
}

fn try_arm_mk_n(
    node: &mut AggStateData<'_>,
    nullable: bool,
    intern_atts: &[u16],
    min_keys: usize,
) -> CompactArm {
    if node.perhash.as_ref().is_some_and(|ph| ph.compact.is_some()) {
        return CompactArm::Armed;
    }
    let (shape, numgroups) = match mk_admit_n(node, nullable, intern_atts, min_keys) {
        Ok(admitted) => admitted,
        Err(verdict) => return verdict,
    };
    let has_intern = shape.comps.iter().any(|c| c.kind == MkCompKind::Intern);
    let two_words = shape.two_words;
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let additionalsize = ph.hashtable.additionalsize();
    // avgpack: packed inline AvgInt8 states, SINK builds only (decided at
    // table creation — before any group seeds).
    let avgpack_mask = if ph.sink_cap.is_some() { node.avgpack_shape_mask } else { 0 };
    // arena-strings inc-3: the DIRECT single-text arm — SINK worker builds
    // of the exact mk1 1-Intern non-nullable shape key the local table on
    // the canonical image bytes themselves (no intern table, no packed-id
    // probe, no store-once canon). The env is process-constant, so every
    // worker of one engagement arms the same way (the leader's admission
    // snapshot — key spec, emit plan, combine — is shape-only and
    // arm-agnostic: both arms flush identical bytes-mode runs).
    if text_direct_enabled()
        && ph.sink_cap.is_some()
        && !nullable
        && shape.comps.len() == 1
        && shape.comps[0].kind == MkCompKind::Intern
    {
        ph.compact = Some(CompactHash {
            table: ::lanetable::LaneAggTable::with_config(
                ::lanetable::KeyRepr::Bytes,
                additionalsize,
                // Capacity hint: as the intern arm below (cap-bounded).
                (numgroups as usize).min(1 << 23),
                ::lanetable::HashKind::best(),
                // Bytes keys are Salt8-only (3 key words never inline).
                ::lanetable::EntryLayout::Salt8,
            ),
            key: CompactKeySpec::Multi(shape),
            intern: None,
            canon_hashes: Vec::new(),
            canon_store: Vec::new(),
            canon_offs: Vec::new(),
            sink_mode: false,
            intern_gen: 0,
            avgpack_mask,
            text_direct: true,
            direct_img: Vec::new(),
            str_arena: None,
            keys: Vec::new(),
            states: Vec::new(),
            hashes: Vec::new(),
            new_rows: Vec::new(),
        });
        return CompactArm::Armed;
    }
    let (repr, layout) = if two_words {
        // Int128 is Salt8-only (2 key words cannot inline into a 16-B slot).
        (::lanetable::KeyRepr::Int128, ::lanetable::EntryLayout::Salt8)
    } else if numgroups <= (1 << 22) {
        (::lanetable::KeyRepr::Int, ::lanetable::EntryLayout::Inline16)
    } else {
        (::lanetable::KeyRepr::Int, ::lanetable::EntryLayout::Salt8)
    };
    ph.compact = Some(CompactHash {
        table: ::lanetable::LaneAggTable::with_config(
            repr,
            additionalsize,
            // Capacity hint: the planner group estimate, honored well past
            // the two-level threshold (the table presizes its 256 buckets
            // from it — a dict-int-key 1.5M-group estimate previously clamped to 1M
            // and was then discarded at conversion). The 8M cap bounds the
            // birth prealloc at 256MB of entries; the spill/backstop gates
            // already bounded the estimate against hash_mem.
            (numgroups as usize).min(1 << 23),
            ::lanetable::HashKind::best(),
            layout,
        ),
        key: CompactKeySpec::Multi(shape),
        intern: has_intern.then(|| {
            ::lanetable::LaneAggTable::new(::lanetable::KeyRepr::Bytes, 8, 1 << 10)
        }),
        canon_hashes: Vec::new(),
        canon_store: Vec::new(),
        canon_offs: Vec::new(),
        sink_mode: false,
        intern_gen: 0,
        avgpack_mask,
        text_direct: false,
        direct_img: Vec::new(),
        str_arena: None,
        keys: Vec::new(),
        states: Vec::new(),
        hashes: Vec::new(),
        new_rows: Vec::new(),
    });
    CompactArm::Armed
}

/// The multi-key admission + packed layout WITHOUT arming a table — the
/// probe half of [`agg_hash_compact_try_arm_mk`] (identical gates, identical
/// shape). The M2 sink leader runs this: it must know the exact shape it is
/// engaging without paying the worker table's prealloc on an executor that
/// will only ever adopt the published parallel emit. `Ok((shape, n))` = the
/// arm would install exactly `shape` with capacity hint `n`; `Err(verdict)`
/// = the non-`Armed` verdict the arm would return.
pub fn agg_hash_compact_mk_admit(
    node: &mut AggStateData<'_>,
    nullable: bool,
    dict_att: Option<u16>,
) -> Result<(MkShape, u64), CompactArm> {
    let buf;
    let atts: &[u16] = match dict_att {
        Some(a) => {
            buf = [a];
            &buf
        }
        None => &[],
    };
    mk_admit_n(node, nullable, atts, 2)
}

/// [`agg_hash_compact_mk_admit`] over a SET of Intern components — the
/// probe half of [`agg_hash_compact_try_arm_mk_multi`].
pub fn agg_hash_compact_mk_admit_multi(
    node: &mut AggStateData<'_>,
    nullable: bool,
    intern_atts: &[u16],
) -> Result<(MkShape, u64), CompactArm> {
    mk_admit_n(node, nullable, intern_atts, 2)
}

/// [`agg_hash_compact_mk_admit`]'s single-key relaxation — the probe half of
/// [`agg_hash_compact_try_arm_mk1`] (the M2 sink single-text leader probe).
pub fn agg_hash_compact_mk_admit1(
    node: &mut AggStateData<'_>,
    dict_att: Option<u16>,
) -> Result<(MkShape, u64), CompactArm> {
    let buf;
    let atts: &[u16] = match dict_att {
        Some(a) => {
            buf = [a];
            &buf
        }
        None => &[],
    };
    mk_admit_n(node, false, atts, 1)
}

fn mk_admit_n(
    node: &mut AggStateData<'_>,
    nullable: bool,
    intern_atts: &[u16],
    min_keys: usize,
) -> Result<(MkShape, u64), CompactArm> {
    if !compact_enabled() {
        return Err(CompactArm::Off);
    }
    let Some(divisor) = compact_split_divisor(node.plan.aggsplit) else {
        return Err(CompactArm::Off);
    };
    let mut numgroups = (node.plan.numGroups.max(1) as u64 / divisor).max(1);
    // Stage-4 §4.4 exchange: gate/size by the bound, as in the single-key arm.
    if let Some(cap) = crate::merge::exchange_cap_for_build(node) {
        numgroups = numgroups.min(cap as u64);
    }
    let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
    let key_cols = ph.hashtable.key_cols();
    if key_cols.len() < min_keys {
        return Err(CompactArm::KeyKind);
    }
    // As the single-key arm: packed tables reconstruct only the grouping
    // key — a stored extra column (functionally-dependent tlist Var,
    // fdgroup-wr) has no read-back and would emit NULL. Refuse.
    if ph.hash_grp_col_idx_input.len() > key_cols.len() {
        return Err(CompactArm::KeyKind);
    }
    // Component kinds first; offsets are laid out per numeric width below
    // (numeric components try the roomy 8-byte encoding, shrinking to 4
    // bytes when the image would exceed 16 — the ts-extract shape's budget:
    // int8 + numeric4 + intern4 = 16).
    let mut kinds: Vec<(u16, MkCompKind)> = Vec::with_capacity(key_cols.len());
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
            // (pgrcolumnar) or its runtime NULL-demote pre-check (slot streams);
            // nullable shapes route NULL through the null-bitmap byte (bit
            // set, value bits zero) without touching the intern table.
            ::execgrouping::GroupKeyKind::TextRaw if intern_atts.contains(&input_att) => {
                MkCompKind::Intern
            }
            // The canonical-form numeric key kind (keypack module doc);
            // per-value packability is the feed's runtime gate.
            ::execgrouping::GroupKeyKind::Numeric => {
                has_numeric = true;
                MkCompKind::Numeric { width: 8 }
            }
            _ => return Err(CompactArm::KeyKind),
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
        return Err(CompactArm::KeyKind);
    }
    let additionalsize = ph.hashtable.additionalsize();
    // SE-GROUPONLY: zero-transition builds carry 0-byte state rows.
    debug_assert!(
        additionalsize > 0 || node.trans_init.is_empty(),
        "fold-fed shapes carry transitions (numtrans > 0) or none at all (group-only)"
    );
    // Spill-eligibility estimate at half margin (compact v1 formula; the
    // 2-word key rides the same 8-B slack term — conservative either way).
    // M3.5 spill-armed sink admission (the ~10M-group @100M hmm=2 cliff): a live
    // spill arm absorbs budget crossings (runs spill as records), so the
    // ESTIMATE refusal is vacated then — the cap-bounded sizing (numgroups
    // min'd above) still holds. Word-keyed shapes spill fixed-width records;
    // Intern (canonical-bytes) shapes spill the C2 BYTES record
    // (canon-sink-increments car 3) and now vacate too, unless the canonical
    // spill kill switch restored the historical exclusion. The engagement's
    // spill_set creation gates on the SAME predicate (the F1 leader/worker-
    // verdict invariant): workers see `sink_spill_ok` from
    // `spill_set.is_some()`, the leader mirrors it at admission.
    let spill_admits = ph.sink_cap.is_some()
        && ph.sink_spill_ok
        && (!kinds.iter().any(|&(_, k)| matches!(k, MkCompKind::Intern))
            || crate::sink::sink_spill_canon_enabled());
    let est_bytes = numgroups.saturating_mul(16 + 16 + additionalsize as u64 + 16);
    if !spill_admits
        && (numgroups > ph.hash_ngroups_limit / 2 || est_bytes > ph.hash_mem_limit as u64 / 2)
    {
        return Err(CompactArm::SpillRisk);
    }
    let two_words = packed_bytes > 8;
    Ok((MkShape { comps, packed_bytes: packed_bytes as u8, nullable, two_words }, numgroups))
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
    let mut numgroups = (node.plan.numGroups.max(1) as u64 / divisor).max(1);
    let ph = node.perhash.as_ref().expect("hashed Agg has perhash");
    // Compact tables reconstruct only the grouping key at read-back; a
    // stored extra column (functionally-dependent tlist Var, fdgroup-wr)
    // would emit NULL. Refuse both single-word modes (Single/Reduced).
    if ph.hash_grp_col_idx_input.len() > ph.num_cols {
        return Err(CompactArm::KeyKind);
    }
    // M2 sink worker builds: the cap bounds the table (flush-at-cap), so the
    // spill gate and sizing work off the cap — the exchange-cap discipline.
    if let Some(cap) = ph.sink_cap {
        numgroups = numgroups.min(cap as u64);
    }
    // SE-GROUPONLY: zero-transition builds carry 0-byte state rows.
    debug_assert!(
        ph.hashtable.additionalsize() > 0 || node.trans_init.is_empty(),
        "fold-fed shapes carry transitions (numtrans > 0) or none at all (group-only)"
    );
    // M3.5 spill-armed sink admission: single-word keys are always
    // spillable — a live spill arm vacates the estimate refusal (the
    // cap-bounded sizing above still holds; see mk_admit_n's twin).
    if single_word_spillrisk(ph, numgroups) && !(ph.sink_cap.is_some() && ph.sink_spill_ok) {
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
/// grouping-key elimination, reduced-expr-key class). The caller (the lane's expr-key
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
    // avgpack: packed inline AvgInt8 states, SINK builds only (decided at
    // table creation — before any group seeds).
    let avgpack_mask = if ph.sink_cap.is_some() { node.avgpack_shape_mask } else { 0 };
    // Same layout policy as compact v1 (single-word key).
    let layout = if numgroups <= (1 << 22) {
        ::lanetable::EntryLayout::Inline16
    } else {
        ::lanetable::EntryLayout::Salt8
    };
    ph.compact = Some(CompactHash {
        table: ::lanetable::LaneAggTable::with_config(
            ::lanetable::KeyRepr::Int,
            additionalsize,
            // Capacity hint: the planner group estimate, honored well past
            // the two-level threshold (the table presizes its 256 buckets
            // from it — a dict-int-key 1.5M-group estimate previously clamped to 1M
            // and was then discarded at conversion). The 8M cap bounds the
            // birth prealloc at 256MB of entries; the spill/backstop gates
            // already bounded the estimate against hash_mem.
            (numgroups as usize).min(1 << 23),
            ::lanetable::HashKind::best(),
            layout,
        ),
        key: CompactKeySpec::Reduced(shape),
        intern: None,
        canon_hashes: Vec::new(),
        canon_store: Vec::new(),
        canon_offs: Vec::new(),
        sink_mode: false,
        intern_gen: 0,
        avgpack_mask,
        text_direct: false,
        direct_img: Vec::new(),
        str_arena: None,
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
    // spankey copy-tax counters (measurement only; cached-bool gated).
    if crate::spankey::spankey_ctr_enabled() {
        use crate::spankey::{spankey_add, SPANKEY_CTRS as S};
        spankey_add(&S.intern_calls, 1);
        if pr.is_new {
            spankey_add(&S.intern_new, 1);
            spankey_add(&S.intern_new_bytes, bytes.len() as u64);
        }
    }
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

/// Whether the armed compact table is the DIRECT single-text arm
/// (arena-strings inc-3) — the feed dispatches its accept branch on this.
pub fn agg_hash_compact_text_direct(node: &AggStateData<'_>) -> bool {
    node.perhash
        .as_ref()
        .and_then(|ph| ph.compact.as_ref())
        .is_some_and(|ch| ch.text_direct)
}

/// DIRECT single-text probe (arena-strings inc-3): resolve one row's text
/// payload to its live group state by probing the direct-armed table on the
/// CANONICAL IMAGE — the mk1 1-Intern canonical bytes (`packed_bytes`
/// zeroed id bytes + the raw text verbatim; `sink::canon_row_bytes`' exact
/// encoding, so flushed runs merge byte-for-byte with intern-armed
/// workers') — with [`crate::sink::sink_hash_bytes`] over that image as
/// the PROBE HASH. Probe-hash law: the table's saved hash word is therefore
/// THE sink hash (flush and the SEAL partition read it back, never
/// rehashing); growth/two-level conversion re-place entries off the saved
/// word (`lanetable` slot_hash's Bytes arm), so the external hash stays
/// consistent for the table's lifetime. NEW groups seed through the same
/// `trans_init` datumCopy loop as every other arrival. The returned pointer
/// is stable until the next flush (the caller's code→state cache rides the
/// flush-reset invalidation channel).
pub fn agg_hash_compact_probe_text_direct<'mcx>(
    node: &mut AggStateData<'mcx>,
    text: &[u8],
) -> PgResult<NonNull<AggPerGroup>> {
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let AggStateData { perhash, trans_init, trans_typ, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.as_mut().expect("direct probe requires an armed table");
    debug_assert!(ch.text_direct, "direct probe requires the direct arm");
    let avgpack_mask = ch.avgpack_mask;
    let CompactHash { table, key, direct_img: img, .. } = &mut *ch;
    let CompactKeySpec::Multi(shape) = key else {
        unreachable!("direct tables carry the mk1 shape")
    };
    debug_assert!(
        shape.comps.len() == 1
            && shape.comps[0].kind == MkCompKind::Intern
            && !shape.nullable,
        "direct tables are the 1-Intern non-nullable shape"
    );
    // The canonical image: the packed prefix with the Intern component's id
    // bytes zeroed (for the 1-Intern shape that is `packed_bytes` zero
    // bytes) + the raw text tail.
    img.clear();
    img.resize(shape.packed_bytes as usize, 0);
    img.extend_from_slice(text);
    let hash = crate::sink::sink_hash_bytes(img);
    let pr = table.probe_bytes(img, hash);
    if pr.is_new {
        seed_new_groups(aggctx, trans_init, trans_typ, &[pr.states], &[0], avgpack_mask)?;
    }
    // SAFETY: probe never returns null state pointers.
    Ok(unsafe { NonNull::new_unchecked(pr.states.cast::<AggPerGroup>()) })
}

/// Budget peek for the coded-group feed (q29coded lane): TRUE = the armed
/// compact build has crossed the backstop's half limits, so the caller must
/// tear down its pointer caches and only THEN call
/// [`agg_hash_compact_disarm`] — the coded feed's per-code caches point
/// INTO the compact rows, which die with the migration, so it cannot let
/// [`agg_hash_compact_backstop`] migrate as a side effect the way the
/// cache-free feeds do. Exactly the backstop's classic-arm accounting
/// (table + intern + store-once canon + aggcontext subtree, live rows);
/// `false` when not armed. Never called on sink builds (the coded arm is
/// not sink-admissible), where live-bytes accounting would differ.
pub fn agg_hash_compact_over_limits(node: &AggStateData<'_>) -> bool {
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let Some(ph) = node.perhash.as_ref() else { return false };
    let Some(ch) = ph.compact.as_ref() else { return false };
    debug_assert!(ph.sink_cap.is_none(), "coded-group builds are never sink builds");
    let mem = ch.table.mem_used()
        + ch.intern.as_ref().map_or(0, ::lanetable::LaneAggTable::mem_used)
        + ch.canon_store.len()
        + ch.canon_offs.len() * 4
        + aggctx.context().subtree_used();
    (ch.table.len() as u64) >= ph.hash_ngroups_limit / 2 || mem >= ph.hash_mem_limit / 2
}

/// Coded-group single resolve (q29coded lane + the GL-DICTDRAIN-1 sink
/// drain): intern `bytes` — the Dict expr-key memo's OUTPUT VALUE payload —
/// and probe the armed mk1 single-Intern table by the id (one group per
/// distinct output value; the packed one-word image is the zero-extended
/// id, exactly the mk pack convention), seeding a NEW group with the same
/// `trans_init` datumCopy loop as every other arrival. This path never
/// migrates, so the returned pointer is stable until the caller-driven
/// invalidation: classic builds check [`agg_hash_compact_over_limits`] per
/// batch and tear down; SINK builds ride the cap/pressure flush law — the
/// drive drops the code→pergroup cache on every flush, and flushed rows
/// export as canonical bytes (the intern reverse map materializes them at
/// flush entry — `compact_extend_canon_hashes`' defensive leg).
pub fn agg_hash_compact_probe_coded<'mcx>(
    node: &mut AggStateData<'mcx>,
    bytes: &[u8],
) -> PgResult<NonNull<AggPerGroup>> {
    let id = agg_hash_compact_intern(node, bytes);
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let AggStateData { perhash, trans_init, trans_typ, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.as_mut().expect("coded probe requires an armed table");
    debug_assert!(
        matches!(&ch.key, CompactKeySpec::Multi(s) if !s.two_words
            && s.comps.len() == 1
            && s.comps[0].kind == MkCompKind::Intern),
        "coded probe requires the mk1 single-Intern shape"
    );
    debug_assert!(!ch.text_direct, "DIRECT tables probe agg_hash_compact_probe_text_direct");
    let avgpack_mask = ch.avgpack_mask;
    let k = id as u64 as i64;
    let pr = ch.table.probe_int(k, ch.table.hash_key_int(k as u64));
    if pr.is_new {
        seed_new_groups(aggctx, trans_init, trans_typ, &[pr.states], &[0], avgpack_mask)?;
    }
    // SAFETY: probe never returns null state pointers.
    Ok(unsafe { NonNull::new_unchecked(pr.states.cast::<AggPerGroup>()) })
}

/// Live group count of the armed compact table (`None` = not armed). The
/// freeze install election reads this after each batch.
pub fn agg_hash_compact_ngroups(node: &AggStateData<'_>) -> Option<usize> {
    node.perhash.as_ref()?.compact.as_ref().map(|ch| ch.table.nrows())
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
    let avgpack_mask = ch.avgpack_mask;
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
        if compact_batch_install_enabled() {
            table.probe_int_batch_install(ckeys, hashes, states, new_rows);
        } else {
            table.probe_int_batch(
                ckeys,
                ::lanetable::PrefetchMode::Adaptive,
                hashes,
                states,
                new_rows,
            );
        }
    }
    // Seed the new groups' states — initialize_hash_entry's datumCopy loop
    // verbatim, writing into the compact row's zeroed state bytes.
    if compact_batch_install_enabled() {
        // Batched-install arm: same writes, transno-outer loop order (the
        // per-transno avgpack/byval decisions hoist out of the row loop;
        // state slots are disjoint, so write order across transnos is
        // unobservable).
        seed_new_groups_inverted(aggctx, trans_init, trans_typ, states, new_rows, avgpack_mask)?;
    } else {
        seed_new_groups(aggctx, trans_init, trans_typ, states, new_rows, avgpack_mask)?;
    }
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
        // Spill-armed sink builds count the table's LIVE rows, not retained
        // capacity — the flush keeps capacity, and the pressure→spill law
        // (runtime_agg drain) drains live pressure BEFORE this belt would
        // trip (32MB headroom); capacity-based accounting here would raise
        // the breach on the batch right after a pressure flush. Mirrors
        // agg_sink_budget_pressure's accounting exactly.
        let table_mem = if ph.sink_cap.is_some() && ph.sink_spill_ok {
            crate::sink::sink_table_live_bytes(&ch.table)
        } else {
            ch.table.mem_used()
        };
        let mem = table_mem
            + ch.intern.as_ref().map_or(0, ::lanetable::LaneAggTable::mem_used)
            // Store-once canonical images (spankey): honest new-memory
            // terms — zero under the kill switch (store stays empty), so
            // switch-off cadence is byte-for-byte the incumbent's. LIVE
            // bytes (len), not retained capacity: a flush clears the store
            // (its bytes became the run's), and counting the retained
            // allocation made post-flush pressure UNDRAINABLE — the
            // leg-12t refusal class (spill-armed engagements must see
            // pressure fall after flush_now; sink_table_live_bytes is the
            // same law for the table itself).
            + ch.canon_store.len()
            + ch.canon_offs.len() * 4
            + aggctx.context().subtree_used();
        if (ch.table.len() as u64) < ph.hash_ngroups_limit / 2 && mem < ph.hash_mem_limit / 2 {
            return Ok(true);
        }
        // M2 sink worker builds must NEVER migrate into the C tuplehash (the
        // sink cannot export it). The sink drain flushes at its cap well
        // below these limits; reaching them is a shape-estimate failure —
        // fail the parallel attempt (RG abort → serial rerun), never a
        // silent migration.
        if ph.sink_cap.is_some() {
            return Err(crate::sink::sink_shape_error(crate::sink::SINK_CAP_BREACH_MSG));
        }
    }
    compact_migrate(node, estate)?;
    Ok(false)
}

/// initialize_hash_entry's datumCopy loop over the batch's NEW groups,
/// writing into the compact rows' zeroed state bytes. avgpack: a masked
/// transno seeds the PACKED inline `[count = 0, sum = 0]` image (the `{0,0}`
/// initval's exact state) instead of datumCopying a 40-byte transarray into
/// the aggcontext — the byref floor kill (sink builds only; the mask is 0
/// everywhere else).
fn seed_new_groups(
    aggctx: ::mcx::Mcx<'_>,
    trans_init: &[::datum::NullableDatum],
    trans_typ: &[crate::TransTyp],
    states: &[*mut u8],
    new_rows: &[u32],
    avgpack_mask: u64,
) -> PgResult<()> {
    for &i in new_rows.iter() {
        let pergroup = states[i as usize].cast::<AggPerGroup>();
        for (transno, init) in trans_init.iter().enumerate() {
            if transno < 64 && (avgpack_mask >> transno) & 1 == 1 {
                // SAFETY: the row's state block holds numtrans 16-byte
                // slots, 8-aligned (lanetable contract).
                unsafe { pergroup.add(transno).cast::<[i64; 2]>().write([0, 0]) };
                continue;
            }
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

/// [`seed_new_groups`] with the loops inverted (batched-install arm): the
/// per-transno decisions — avgpack membership, byval/byref, init nullness —
/// are resolved ONCE per transno, then a tight row loop writes that
/// transno's slot across every new group. Writes and values are identical
/// to [`seed_new_groups`]'s (disjoint state slots; cross-transno write
/// order is unobservable). The byref datumCopy leg keeps its per-row copy
/// (each group owns its datum) — it is structurally absent on the sink's
/// byval-POD admission.
fn seed_new_groups_inverted(
    aggctx: ::mcx::Mcx<'_>,
    trans_init: &[::datum::NullableDatum],
    trans_typ: &[crate::TransTyp],
    states: &[*mut u8],
    new_rows: &[u32],
    avgpack_mask: u64,
) -> PgResult<()> {
    for (transno, init) in trans_init.iter().enumerate() {
        if transno < 64 && (avgpack_mask >> transno) & 1 == 1 {
            for &i in new_rows.iter() {
                let pergroup = states[i as usize].cast::<AggPerGroup>();
                // SAFETY: the row's state block holds numtrans 16-byte
                // slots, 8-aligned (lanetable contract).
                unsafe { pergroup.add(transno).cast::<[i64; 2]>().write([0, 0]) };
            }
            continue;
        }
        let typ = trans_typ[transno];
        if !init.isnull && !typ.byval {
            for &i in new_rows.iter() {
                let pergroup = states[i as usize].cast::<AggPerGroup>();
                // SAFETY: node-lifetime initval datum copied into the
                // aggcontext (C initialize_aggregate's datumCopy).
                let value = unsafe { ::execexpr::agg_datum_copy(aggctx, init.value, typ.len)? };
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
        } else {
            let image = AggPerGroup {
                trans_value: init.value,
                trans_value_is_null: init.isnull,
                no_trans_value: init.isnull,
            };
            for &i in new_rows.iter() {
                let pergroup = states[i as usize].cast::<AggPerGroup>();
                // SAFETY: as above — zeroed numtrans-slot state block.
                unsafe { pergroup.add(transno).write(image) };
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
    let spk_t0 = crate::spankey::spankey_t0();
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let AggStateData { perhash, trans_init, trans_typ, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.as_mut().expect("compact batch requires an armed table");
    debug_assert!(matches!(&ch.key, CompactKeySpec::Multi(s) if !s.two_words));
    let avgpack_mask = ch.avgpack_mask;
    if mkaccept_fused() {
        // Fused state lane (mkaccept inc-1): probe directly into the
        // caller's groups vec — same pointers, minus the states-scratch
        // pass. Restore precedes the fallible seed (no leak on error).
        let CompactHash { table, hashes, new_rows, .. } = &mut *ch;
        new_rows.clear();
        let mut raw = groups_take_raw(groups);
        table.probe_int_batch(keys, ::lanetable::PrefetchMode::Adaptive, hashes, &mut raw, new_rows);
        groups_restore(groups, raw);
        seed_new_groups(aggctx, trans_init, trans_typ, groups_ptr_slice(groups), new_rows, avgpack_mask)?;
    } else {
        let CompactHash { table, states, hashes, new_rows, .. } = &mut *ch;
        states.clear();
        new_rows.clear();
        groups.clear();
        table.probe_int_batch(keys, ::lanetable::PrefetchMode::Adaptive, hashes, states, new_rows);
        seed_new_groups(aggctx, trans_init, trans_typ, states, new_rows, avgpack_mask)?;
        groups.extend(states.iter().map(|&s| {
            // SAFETY: probe never returns null state pointers.
            unsafe { NonNull::new_unchecked(s.cast::<AggPerGroup>()) }
        }));
    }
    // Canonical shapes under the RUNTIME SINK ONLY: hash the batch's NEW
    // rows' canonical images while their text bytes are cache-warm, on this
    // (accepting) worker — the flush and the single-threaded SEAL partition
    // then never hash. Serial lane: flag unset, zero added work (no-op for
    // word shapes either way).
    if ch.sink_mode {
        crate::sink::compact_extend_canon_hashes(ch);
    }
    crate::spankey::spankey_lap(&crate::spankey::SPANKEY_CTRS.probe_ns, spk_t0);
    Ok(())
}

/// [`agg_hash_compact_batch_mk1`]'s two-word twin (`packed_bytes > 8` →
/// KeyRepr::Int128).
pub fn agg_hash_compact_batch_mk2<'mcx>(
    node: &mut AggStateData<'mcx>,
    keys: &[[u64; 2]],
    groups: &mut Vec<NonNull<AggPerGroup>>,
) -> PgResult<()> {
    let spk_t0 = crate::spankey::spankey_t0();
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let AggStateData { perhash, trans_init, trans_typ, .. } = node;
    let ph = perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.as_mut().expect("compact batch requires an armed table");
    debug_assert!(matches!(&ch.key, CompactKeySpec::Multi(s) if s.two_words));
    let avgpack_mask = ch.avgpack_mask;
    if mkaccept_fused() {
        // Fused state lane — see the mk1 twin.
        let CompactHash { table, hashes, new_rows, .. } = &mut *ch;
        new_rows.clear();
        let mut raw = groups_take_raw(groups);
        table.probe_i128_batch(keys, ::lanetable::PrefetchMode::Adaptive, hashes, &mut raw, new_rows);
        groups_restore(groups, raw);
        seed_new_groups(aggctx, trans_init, trans_typ, groups_ptr_slice(groups), new_rows, avgpack_mask)?;
    } else {
        let CompactHash { table, states, hashes, new_rows, .. } = &mut *ch;
        states.clear();
        new_rows.clear();
        groups.clear();
        table.probe_i128_batch(keys, ::lanetable::PrefetchMode::Adaptive, hashes, states, new_rows);
        seed_new_groups(aggctx, trans_init, trans_typ, states, new_rows, avgpack_mask)?;
        groups.extend(states.iter().map(|&s| {
            // SAFETY: probe never returns null state pointers.
            unsafe { NonNull::new_unchecked(s.cast::<AggPerGroup>()) }
        }));
    }
    // Canonical shapes under the runtime sink only — see the mk1 twin.
    if ch.sink_mode {
        crate::sink::compact_extend_canon_hashes(ch);
    }
    crate::spankey::spankey_lap(&crate::spankey::SPANKEY_CTRS.probe_ns, spk_t0);
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
pub(crate) fn mk_unpack(words: [u64; 2], comp: &MkComp) -> u64 {
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
pub(crate) fn mk_row_words(ch: &CompactHash, shape: &MkShape, row: usize) -> [u64; 2] {
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

// -- Numeric key components (the ts-extract-key numeric key kind) ------------
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
pub(crate) fn mk_numeric_key_decode(bits: u64, width: u8) -> ::adt_numeric::NumericKeyForm {
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
    // A SINK build must NEVER migrate. This used to be asserted in prose ("the
    // backstop errors on `sink_cap` before reaching here") and backed only by
    // the debug_assert below — and the prose was false: `scan_mk_batch`'s
    // numeric-pack demote calls `agg_hash_compact_disarm` directly, so an Mk
    // sink drain reached this function with the backstop never consulted.
    //
    // Migrating a sink table copies its state blocks WORD FOR WORD into the C
    // tuplehash and then drops the `CompactHash` — including the table-owned
    // `str_arena` that owns every `min/max(text)` transvalue those words point
    // at. The migrated entries would hold pointers into released slabs.
    // avgpack has the same shape of problem: a packed inline state slot the C
    // tuplehash cannot read.
    //
    // So refuse, in RELEASE, before anything is taken from the node. The error
    // propagates to the drain, the RG aborts and the serial arm reruns the
    // statement — which is what every sink caller of this path was going to end
    // up doing anyway (they discard the migration and return a demote), so this
    // costs nothing and removes a use-after-free shape. Per the debug-assert
    // masking law an invariant this load-bearing cannot be enforced by a check
    // that compiles out.
    if node.perhash.as_ref().is_some_and(|ph| ph.sink_cap.is_some()) {
        return Err(crate::sink::sink_shape_error(
            "compact table migration on a sink build (state blocks are not self-contained)",
        ));
    }
    // SAFETY: read of the once-allocated node; no &mut to it is live.
    let aggctx = unsafe { node.agg_node.as_ref() }.aggcontext();
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    let ch = ph.compact.take().expect("migration requires an armed table");
    // Now guaranteed by the release refusal above (kept as a tripwire in case
    // the two ever drift): packed inline states exist only on sink builds.
    debug_assert_eq!(ch.avgpack_mask, 0, "packed avgpack states in a compact migration");
    debug_assert!(ch.str_arena.is_none(), "table-owned str store in a compact migration");
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
        // SE-GROUPONLY: zero-transition tables have no state block to carry
        // over — the C entry is complete with its key image alone.
        if additionalsize > 0 {
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
        // The integer fast pack (ts-extract key class) must produce the
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
    let mut row = ph.hashiter as usize;
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
        ph.hashiter = row as u64;
        return Ok(None);
    }
    ph.hashiter = row as u64 + 1;
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
    let mut row = ph.hashiter as usize;
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
    ph.hashiter = row as u64;
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
pub(crate) unsafe fn int8_avg_trans_read(d: Datum) -> PgResult<(i64, i64)> {
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

// ===========================================================================
// Lane-v2 topkfin (hot-c1-topk-finalize): top-k GROUP SELECTION over the raw
// compact-table states, ahead of finalize + emit. On the admitted
// `GROUP BY … ORDER BY <int8 finalfn-none agg> LIMIT k` shape (the exact
// topnemit spec — count(*)/count(x)/sum-int leading key) the downstream
// bounded tuplesort keeps only k of the ~ngroups finalized rows; the
// streaming topnemit boundary cut only elides groups STRICTLY worse than the
// live k-th boundary, which on flat count distributions (~10M-group class: the 10th
// boundary count is ~1) cuts nothing. This pass instead selects the k
// surviving groups FIRST — a bounded (badness, row) heap over the raw int8
// transvalues, one sequential read per group — so finalize (numeric avg
// division), key reconstruction, tuple forming and sort puts run for k
// groups instead of all of them.
//
// Tie semantics (docs/conformance/tie-ordering.md, rule 2 applied to agg
// groups): the heap's total order is (key, insertion row ascending) — the
// strict-better replacement (`badness < worst.badness` only; arriving rows
// have monotonically increasing row index, so an equal key can never evict a
// kept one) keeps the FIRST-ARRIVED members of the k-th key's tie group, a
// deterministic function of group birth order. C's bounded heap keeps a
// heap-shape-arbitrary subset of the same tie group, so the selected set is
// C-LEGAL but not byte-equal to a given lane-off run at a boundary tie —
// exactly rule 2's ratified surface (gates count-gate the boundary tie
// group). The lane arms this only in relaxed adaptive-topk mode; `tracked`
// / `0` remain byte-exact channels, and PGRUST_LANE_V2_TOPKFIN=0 kills the
// pass on its own.
//
// Fail-closed: any group whose leading-key transvalue is NULL or pending
// bails the WHOLE pass (a NULL's rank depends on NULLS placement — the
// tuplesort comparator stays the authority), before any side effect: the
// selection scan is read-only and the caller falls through to the exact
// pre-existing feed. Multi-column ORDER BY never admits (the k-th boundary's
// tie-break needs the secondary keys; the lane checks numCols == 1).
// ===========================================================================

/// Monotone "badness" image of an int8 sort key: strictly increasing as the
/// key gets WORSE under the sort direction (asc: bigger = worse; desc:
/// smaller = worse). Total on all i64 values, no overflow cases.
#[inline]
pub(crate) fn topkfin_badness(key: i64, desc: bool) -> u64 {
    let asc = (key as u64) ^ (1u64 << 63);
    if desc {
        !asc
    } else {
        asc
    }
}

/// Phase 1 — select the top-k groups on raw states. Returns the surviving
/// compact row indices in ROW (insertion) order plus the total group count,
/// or `None` when the pass declines (no compact table, an emit already in
/// progress, or a NULL/pending leading-key transvalue — the caller runs the
/// pre-existing feed unchanged; nothing here mutates node state).
pub fn topk_finalize_select(
    node: &AggStateData<'_>,
    spec: crate::TopnEmitSpec,
    k: usize,
) -> PgResult<Option<(Vec<u32>, u64)>> {
    let Some(ph) = node.perhash.as_ref() else { return Ok(None) };
    if ph.hashiter != 0 {
        // A partially-drained emit cursor: the remaining groups are no longer
        // "all groups", so selection over 0..nrows would be wrong. (The lane
        // feeds drain in one call; this is a belt-and-braces guard.)
        return Ok(None);
    }
    let Some(ch) = ph.compact.as_ref() else { return Ok(None) };
    let nrows = ch.table.nrows();
    let k = k.min(nrows);
    // Max-heap of (badness, row): the root is the WORST kept group, ties on
    // badness keep the larger row on top so eviction (which is strict-better
    // only) can never touch an earlier-arrived tie member.
    let mut heap: std::collections::BinaryHeap<(u64, u32)> =
        std::collections::BinaryHeap::with_capacity(k.saturating_add(1));
    for row in 0..nrows {
        // The per-group retrieve cadence (batch_emit_scan_block's).
        ::postgres_seams::check_for_interrupts::call()?;
        // SAFETY: the row's state block is the group's live AggPerGroup
        // array; transno < its length (topn_emit_resolve checked this node).
        let pg = unsafe {
            &*ch.table.row_states(row).cast::<AggPerGroup>().add(spec.transno as usize)
        };
        if pg.no_trans_value || pg.trans_value_is_null {
            return Ok(None);
        }
        let b = topkfin_badness(pg.trans_value.as_i64(), spec.desc);
        if heap.len() < k {
            heap.push((b, row as u32));
        } else {
            match heap.peek() {
                Some(&(wb, _)) if b < wb => {
                    heap.pop();
                    heap.push((b, row as u32));
                }
                _ => {}
            }
        }
    }
    let mut rows: Vec<u32> = heap.into_iter().map(|(_, row)| row).collect();
    rows.sort_unstable();
    Ok(Some((rows, nrows as u64)))
}

/// Phase 2 block staging: park an explicit survivor block in `plan.idx` for
/// `batch_emit_row`, with `batch_emit_scan_block`'s block-granular
/// ExprContext reset (the previous block's finalized images were copied by
/// its sort puts before this call).
pub fn batch_emit_set_block<'mcx>(
    node: &mut AggStateData<'mcx>,
    estate: &mut EStateData<'mcx>,
    plan: &mut crate::BatchEmitPlan,
    rows: &[u32],
) {
    estate.reset_expr_context(node.ps_ExprContext);
    plan.idx.clear();
    plan.idx.extend_from_slice(rows);
}

/// Emit-drain contract after an owned topkfin feed: park the cursor at EOF
/// and flip `agg_done`, exactly where a full block walk would have left them.
pub fn agg_emit_mark_drained(node: &mut AggStateData<'_>) {
    let ph = node.perhash.as_mut().expect("hashed Agg has perhash");
    ph.hashiter =
        ph.compact.as_ref().expect("topkfin requires the compact table").table.nrows() as u64;
    node.agg_done = true;
}

#[cfg(test)]
mod topkfin_tests {
    use super::topkfin_badness;

    /// Badness is a strictly monotone image of "worse under the direction":
    /// asc worse = bigger key, desc worse = smaller key; total on extremes.
    #[test]
    fn badness_orders_keys() {
        let keys = [i64::MIN, -3, -1, 0, 1, 2, i64::MAX];
        for w in keys.windows(2) {
            let (lo, hi) = (w[0], w[1]);
            // asc: hi is worse.
            assert!(topkfin_badness(hi, false) > topkfin_badness(lo, false));
            // desc: lo is worse.
            assert!(topkfin_badness(lo, true) > topkfin_badness(hi, true));
        }
    }

    /// The bounded-heap selection invariant this file's phase-1 loop relies
    /// on: strict-better replacement over (badness, row) keeps the top-k
    /// multiset AND the first-arrived members of the boundary tie group.
    #[test]
    fn bounded_heap_keeps_first_arrived_ties() {
        // keys in arrival (row) order; desc top-3 selection.
        let keys: [i64; 8] = [1, 5, 2, 2, 7, 2, 2, 1];
        let k = 3;
        let mut heap: std::collections::BinaryHeap<(u64, u32)> =
            std::collections::BinaryHeap::new();
        for (row, &key) in keys.iter().enumerate() {
            let b = topkfin_badness(key, true);
            if heap.len() < k {
                heap.push((b, row as u32));
            } else if let Some(&(wb, _)) = heap.peek() {
                if b < wb {
                    heap.pop();
                    heap.push((b, row as u32));
                }
            }
        }
        let mut rows: Vec<u32> = heap.into_iter().map(|(_, r)| r).collect();
        rows.sort_unstable();
        // top-3 by key desc = {7, 5, and ONE of the four 2s} — the
        // first-arrived 2 (row 2) survives.
        assert_eq!(rows, vec![1, 2, 4]);
    }
}
