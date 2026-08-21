//! The parallel scan driver — claim-plane CONSUMER #2 (M3-L3),
//! parallel-native from birth (binding law 8).
//!
//! Shape (the v3 `lx_source/pgrc.rs` drive rebuilt on the frozen
//! contracts):
//!
//! - workers claim GRANULE SPANS through the shared [`ClaimCursor`]
//!   (PC-2.1/PC-2.3; part edges are hard boundaries, so one claim = one
//!   part);
//! - within a claim the drive is claim → stage → per-window batch →
//!   end_claim (PC-2.4): the claim's part pin is attached to the
//!   [`ClaimGuard`] and released BY CONSTRUCTION at end_claim — the IN-4
//!   law at read grain (the #802 lineage; v3 held every pin for the
//!   scan's life);
//! - per-part cached state (cursors, dict spaces, meta bodies) drops on
//!   PART ADVANCE (release-on-advance; kill switch
//!   `PGRUST_SCAN_PART_RELEASE=0` reproduces the v3 whole-scan-hold arm —
//!   the born-RED pin witness);
//! - batches are `pgrc2_batch` shapes: dict-code lanes publish under the
//!   OD-9 per-batch vectorized max-code guard (violation raises
//!   `GUARD_CODE_BOUND` and demotes to CHECKED gather — the reference
//!   drive owns error identity, AB-7.2/7.3), StrView gather rides the
//!   pinned dict payload (AB-2.4/AB-4.3), the RowId lane packs 32/19/13
//!   (AB-5.1);
//! - the verdict plane consults zone keys, PSMA windows, and blooms per
//!   granule with exact attribution (XC-5; `crate::meta`);
//! - output is keyed by GLOBAL GRANULE UNIT, so assembly is
//!   schedule-independent: dirsha(serial) == dirsha(DOP-N) by
//!   construction (PC-3.4), witnessed — never assumed — by the skew
//!   probes.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// THE single lock library (determinism-lint rawsync law; permit-scheduler.md
// #2): the native world re-exports std's Mutex verbatim (identical type,
// zero cost — pgsync G1), so this import is behavior-identical to the
// std::sync form by construction; call sites are unchanged.
use pgsync::Mutex;

use pgrc2_batch::{
    Batch, ClaimArena, ColRep, Column, DictCodes as LaneDictCodes, DictHandle as LaneDictHandle,
    DictSpace as _, GuardWord, GUARD_CODE_BOUND,
};
use pgrc2_claim::{ClaimCursor, ClaimOutcome, NoObserver};
use pgrc2_format::abi::{ByteArena, DecodeOut, ValidityVerdict};
use pgrc2_format::class::{ColSchema, StorageClass};
use pgrc2_format::enc::EncodingId;
use pgrc2_format::shredlane::ShredLaneKind;
use pgrc2_format::part::{StreamRole, STREAMF_DICT_EXEC};
use pgrc2_format::rowid::pack_rowid;
use pgrc2_format::FormatError;
use pgrc2_meta::census::MetaEngagement;
use pgrc2_meta::lower::{lower_const, ConstInput};
use pgrc2_meta::profile::MetaProfile;
use pgrc2_read::cursor::CodecBinding;
use pgrc2_read::registry::PartPin;
use pgrc2_read::{OpenPart, ReadError, ReadResult, SectionUnwrapper, StreamCursor};

use crate::dictspace::ScanDictSpace;
use crate::meta::{consult_granule, ColumnMeta, GranuleConsult, ScanConst, ScanPredicate};
use crate::spans::GranuleSpans;

/// One scanned column: the frozen-class schema fact set (the caller's
/// catalog knowledge — the v3 `PgrcColumnMeta` posture).
#[derive(Debug, Clone)]
pub struct ScanColumn {
    pub schema: ColSchema,
    /// Caller's catalog fact: dict code order embeds value order (C
    /// collation class) — AND'd with the format's byte-rank contract.
    pub dict_value_order: bool,
    /// Shredded-jsonb lane addressing (TY-3 read supply). `None` = the
    /// root stream (path_ord 0 — every pre-existing column). `Some(path)`
    /// = the typed shred lane of the parent jsonb column `schema.attno`
    /// at that dotted path: resolved PER PART through the PathTable
    /// (elections are per-part; the same path can hold different
    /// path_ords in different parts), kind-checked against the sealed
    /// StreamDir entry, and REFUSED TYPED (`ReadError::ShredLaneRefused`)
    /// where the part carries no such lane — absence is never a
    /// fabricated-NULL column. `schema` declares the EXPECTED lane shape
    /// (`ShredLaneKind::col_schema` is the mint); once resolved, the lane
    /// stages/emits through the one column path — batches, dict lanes,
    /// StrViews, canon bytes all unchanged (type-driven, never
    /// query-shaped).
    pub shred_path: Option<String>,
}

impl ScanColumn {
    /// An ordinary root-stream column (the universal pre-lane shape).
    pub fn root(schema: ColSchema, dict_value_order: bool) -> ScanColumn {
        ScanColumn {
            schema,
            dict_value_order,
            shred_path: None,
        }
    }

    /// A shredded-jsonb lane column: `parent_attno` is the jsonb column,
    /// `path` the dotted shred path, `kind` the expected lane kind (the
    /// schema is minted from it — one authority,
    /// `pgrc2_format::shredlane`). Lane text is C-collation byte-ranked by
    /// construction, so dict value order holds where a lane dict-elects.
    pub fn shred_lane(parent_attno: u32, path: &str, kind: ShredLaneKind) -> ScanColumn {
        ScanColumn {
            schema: kind.col_schema(parent_attno),
            dict_value_order: true,
            shred_path: Some(path.to_string()),
        }
    }
}

/// Scan options. Defaults are the production posture.
#[derive(Debug, Clone)]
pub struct ScanOptions {
    /// PC-3.3: a member of { n : n divides GRANULE_ROWS } — never a
    /// per-query or per-DOP tunable.
    pub batch_rows: u32,
    pub publish_dict_lanes: bool,
    /// AP-3: build StrView cells over published dict lanes.
    pub build_strviews: bool,
    /// IN-4 release-on-advance (kill switch PGRUST_SCAN_PART_RELEASE=0
    /// reproduces the v3 whole-scan-hold arm).
    pub release_on_advance: bool,
    /// AB-5.1: emit the RowId lane.
    pub rowid_lane: bool,
    /// Canon COMPARISON-CURRENCY fold (the L4 MANIFEST's above-1m law):
    /// when set, each granule's canonical bytes are folded to a fixed
    /// 24-byte digest (len + 2x FNV-1a-64, independent seeds) AT INSERT —
    /// identity comparisons become digest comparisons and the scan never
    /// holds a full-bank canon in memory (~2.2GB/arm per 1m row at full
    /// width; ~220GB/arm at 100m). Default OFF: byte currency, the 1m/
    /// corpus grain of record.
    pub canon_digest: bool,
}

impl Default for ScanOptions {
    fn default() -> ScanOptions {
        ScanOptions {
            batch_rows: 1024,
            publish_dict_lanes: true,
            build_strviews: true,
            release_on_advance: !matches!(
                std::env::var("PGRUST_SCAN_PART_RELEASE").as_deref(),
                Ok("0") | Ok("off")
            ),
            rowid_lane: true,
            canon_digest: false,
        }
    }
}

/// The canon digest fold: (len u64 LE) + FNV-1a-64 + FNV-1a-64 at an
/// independent seed — deterministic, schedule-free, 24 bytes/granule.
fn canon_digest_fold(bytes: &[u8]) -> Vec<u8> {
    let mut h1: u64 = 0xcbf29ce484222325;
    let mut h2: u64 = 0x84222325cbf29ce4 ^ 0x9e3779b97f4a7c15;
    for &b in bytes {
        h1 ^= b as u64;
        h1 = h1.wrapping_mul(0x100000001b3);
        h2 ^= b as u64;
        h2 = h2.wrapping_mul(0x100000001b3);
        h2 = h2.rotate_left(29);
    }
    let mut out = Vec::with_capacity(24);
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(&h1.to_le_bytes());
    out.extend_from_slice(&h2.to_le_bytes());
    out
}

/// The schedule-INDEPENDENT census (PC-6.1 fold: per-worker plain sums,
/// folded at drain; serial==parallel identity is a gate clause, PC-6.3).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ScanCensus {
    pub granules_scanned: u64,
    pub granules_zone_pruned: u64,
    pub psma_windows_skipped: u64,
    pub rows_emitted: u64,
    pub dict_lanes_published: u64,
    pub strview_lanes_built: u64,
    pub code_bound_demotions: u64,
    /// DM-2 (append-only): rows excluded from window selections by
    /// delete-vector application. Schedule-independent (a pure function of
    /// the Dv and the window grid), so it folds under the PC-6.1 identity
    /// law like every other census term.
    pub rows_deleted_skipped: u64,
    /// M5d.cells.q20-span (append-only): windows whose text filter
    /// column certified the contiguity witness (TextSpan published).
    /// Schedule-independent (a pure function of staged granule content
    /// and the window grid), so it folds under the PC-6.1 identity law.
    pub text_spans_certified: u64,
    pub meta: MetaEngagement,
}

impl ScanCensus {
    pub fn fold(&mut self, o: &ScanCensus) {
        self.granules_scanned += o.granules_scanned;
        self.granules_zone_pruned += o.granules_zone_pruned;
        self.psma_windows_skipped += o.psma_windows_skipped;
        self.rows_emitted += o.rows_emitted;
        self.dict_lanes_published += o.dict_lanes_published;
        self.strview_lanes_built += o.strview_lanes_built;
        self.code_bound_demotions += o.code_bound_demotions;
        self.rows_deleted_skipped += o.rows_deleted_skipped;
        self.text_spans_certified += o.text_spans_certified;
        self.meta.fold(&o.meta);
    }
}

/// The schedule-DEPENDENT pin/residency witness (reported, never
/// identity-compared): the IN-4 law's evidence surface.
#[derive(Debug, Default)]
pub struct PinWitness {
    pub pins_taken: u64,
    pub pins_released: u64,
    /// Peak simultaneously pinned parts across the scan (the
    /// release-on-advance arm bounds this by ~DOP; the kill-switch arm
    /// grows it toward the part count — the born-RED direction).
    pub parts_pinned_peak: u64,
    /// SB-7 residency: peak faulted dict payload bytes across live dict
    /// spaces (the pgrc2_pinned_resident-class census term).
    pub dict_resident_bytes_peak: u64,
}

pub struct ScanResult {
    pub census: ScanCensus,
    pub pins: PinWitness,
    /// Canonical per-granule output bytes, keyed by global granule unit —
    /// the byte-identity currency of the dirsha probes.
    pub canon: BTreeMap<u64, Vec<u8>>,
    pub claimed_units: u64,
}

pub struct TableScan {
    pub parts: Vec<Arc<OpenPart>>,
    pub binding: &'static CodecBinding<'static>,
    pub unwrappers: &'static [&'static dyn SectionUnwrapper],
    pub columns: Vec<ScanColumn>,
    /// Optional single-column equality probe (the L3 verdict-plane
    /// consumer shape) with the probe column's meta profile.
    pub predicate: Option<(ScanPredicate, MetaProfile)>,
    /// Column ordinals whose staged windows must carry a StrView cell
    /// lane even on the eager varlena path (M4-S5: the LIKE band's
    /// consumer declares its string filter columns; dict-published lanes
    /// build cells under `opts.build_strviews` regardless). Empty =
    /// today's posture, byte-untouched.
    pub strview_cols: Vec<u16>,
    /// M5c (the grouped band's residency declaration): column ordinals
    /// whose published dict-code lanes STAY the batch rep — the AP-3
    /// StrView flip is suppressed for them, so the grouped sink's accept
    /// leg sees `ColRep::DictCodes` (u32 code keys, AB-2.2). Non-dict
    /// parts and guard-demoted windows still present the hydrated rep —
    /// the sink's interpreted twin owns those batches (AB-7.3). Empty =
    /// today's posture, byte-untouched.
    pub code_key_cols: Vec<u16>,
    /// DM-2: per-part decoded delete vectors, ALIGNED WITH `parts`
    /// (`deletes[i]` applies to `parts[i]`; `None` = dv_gen == 0). An
    /// empty vec = no part carries deletions (the common shape; every
    /// pre-DM writer publishes dv_gen=0). The masks intersect into each
    /// window's SELECTION at emit — the one row currency (AB-3.3), so
    /// deleted rows are dead to kernels, canon bytes, and COUNT(*) by
    /// construction.
    pub deletes: Vec<Option<Arc<crate::dv::PartDeletes>>>,
    pub opts: ScanOptions,
}

// ---------------------------------------------------------------------------
// per-worker state
// ---------------------------------------------------------------------------

struct ColState {
    cursor: StreamCursor<'static>,
    /// Whole-granule staged scratch (v3's R4 tier).
    staged: Option<u32>,
    datums: Vec<u64>,
    arena: Vec<u64>,
    vwords: Vec<u64>,
    verdict: ValidityVerdict,
    /// Dict lane state.
    codes: Vec<u32>,
    codes_staged: Option<u32>,
    dict: Option<Box<ScanDictSpace>>,
}

struct PartState {
    part: Arc<OpenPart>,
    cols: Vec<ColState>,
    meta: Option<ColumnMeta>,
    /// DM-2: this part's decoded delete vector (None = dv_gen == 0).
    deletes: Option<Arc<crate::dv::PartDeletes>>,
    granule_count: u32,
    /// The v3 whole-scan-hold pin, taken ONLY on the kill-switch arm
    /// (PGRUST_SCAN_PART_RELEASE=0): reproduces the pre-#802 posture the
    /// pin witness goes born-RED on. The release arm holds NO pin here —
    /// claim pins (PC-2.4) are the only pins it ever takes.
    held_pin: Option<PartPin>,
}

/// Worker-local part-state cache: single-slot on the release-on-advance
/// arm (part advance drops the old state — dict buffers, cursors, and on
/// the kill arm its held pin), unbounded on the kill arm (the v3
/// whole-scan-hold reproduction).
/// Where one staged window goes (M4-S3): the M3 canon currency or a
/// consumer visitor (the executor-binding face). ONE staging/emit code
/// path serves both — the canon bytes a consumer-driven scan WOULD have
/// produced are byte-identical to the M3 rig's, so the standing dirsha
/// determinism evidence covers the consumer path's staging by
/// construction.
enum WindowSink<'a, 'b> {
    /// Canonicalize into the per-granule byte vector (the M3 rig form:
    /// pruned granules write the `P` marker, skipped windows `S`).
    Canon(&'a mut Vec<u8>),
    /// Hand the staged batch to the consumer (lx4 pipeline grain) with
    /// its window identity (global granule unit, window start row) — the
    /// deterministic-assembly key (PC-3.4). The borrow never escapes the
    /// call (PC-2.4: batches are claim-scoped).
    Visit(&'a mut (dyn FnMut(&Batch, GuardWord, u64, u32) -> ReadResult<()> + 'b)),
}

struct WorkerCache {
    states: std::collections::HashMap<usize, PartState>,
    /// R-1 at the pipe grain (M4-S6, the S5 triage re-home): ONE staging
    /// batch + StrView arena per WORKER, reused across granules and claims
    /// (was `Batch::new` + `ClaimArena::new` per granule). The per-window
    /// reset discipline in `emit_window` (cols.clear/sel.clear/arena.reset
    /// — the R4 stale-pointer law) is what makes the reuse sound; the
    /// hoist only widens the buffers' lifetime.
    batch: Batch,
    strview_arena: ClaimArena,
}

impl WorkerCache {
    fn new() -> WorkerCache {
        WorkerCache {
            states: std::collections::HashMap::new(),
            batch: Batch::new(),
            strview_arena: ClaimArena::new(),
        }
    }
}

/// The scan-wide shared pin/residency counters (public at M4-S3 so an
/// external claim consumer — the lx4 pipeline drive — folds into the SAME
/// pin-witness surface `run` reports; fields stay private, the witness is
/// the read face).
pub struct SharedCounters {
    pins_taken: AtomicU64,
    pins_released: AtomicU64,
    pins_active: AtomicU64,
    pins_peak: AtomicU64,
    dict_resident_peak: AtomicU64,
}

impl SharedCounters {
    pub fn new() -> SharedCounters {
        SharedCounters {
            pins_taken: AtomicU64::new(0),
            pins_released: AtomicU64::new(0),
            pins_active: AtomicU64::new(0),
            pins_peak: AtomicU64::new(0),
            dict_resident_peak: AtomicU64::new(0),
        }
    }

    fn pin_taken(&self) {
        self.pins_taken.fetch_add(1, Ordering::Relaxed);
        let now = self.pins_active.fetch_add(1, Ordering::Relaxed) + 1;
        self.pins_peak.fetch_max(now, Ordering::Relaxed);
    }

    fn pin_released(&self) {
        self.pins_released.fetch_add(1, Ordering::Relaxed);
        self.pins_active.fetch_sub(1, Ordering::Relaxed);
    }

    /// Drop a cached PartState, counting its held pin's release (kill arm).
    fn drop_state(&self, st: PartState) {
        if st.held_pin.is_some() {
            self.pin_released();
        }
        drop(st);
    }

    /// The pin-witness read face (`Default` shape matches `ScanResult`).
    pub fn witness(&self) -> PinWitness {
        PinWitness {
            pins_taken: self.pins_taken.load(Ordering::Relaxed),
            pins_released: self.pins_released.load(Ordering::Relaxed),
            parts_pinned_peak: self.pins_peak.load(Ordering::Relaxed),
            dict_resident_bytes_peak: self.dict_resident_peak.load(Ordering::Relaxed),
        }
    }
}

impl Default for SharedCounters {
    fn default() -> Self {
        SharedCounters::new()
    }
}

impl TableScan {
    /// M5c (the grouped sink's per-part dict face): open the dict space
    /// for `(part_idx, column ordinal)` under EXACTLY the lane
    /// eligibility gate the scan's own dict publish uses (dict-coded
    /// values stream ∧ STREAMF_DICT_EXEC ∧ zero-null proof). `None` =
    /// the part/column is not dict-lane eligible — the caller's
    /// interpreted (hydrated) twin owns those parts. The returned space
    /// pins the part (its own `PartPin`), so dict entry addresses stay
    /// generation-stable for the holder's lifetime — the sink holds it
    /// across its per-part flush walk and drops it at part advance.
    pub fn dict_space_of(
        &self,
        part_idx: usize,
        ordinal: u16,
    ) -> ReadResult<Option<ScanDictSpace>> {
        let Some(spec) = self.columns.get(ordinal as usize) else {
            return Ok(None);
        };
        let Some(part) = self.parts.get(part_idx) else {
            return Ok(None);
        };
        if !self.opts.publish_dict_lanes {
            return Ok(None);
        }
        // Shred-lane columns resolve their per-part path_ord exactly as
        // the scan's own part entry does (one resolution law); refusals
        // propagate typed.
        let path_ord = resolve_stream_ord(part, spec, ordinal)?;
        let dir = part.stream_directory()?;
        let attno = spec.schema.attno;
        let eligible = dir
            .lookup(attno, path_ord, StreamRole::Values)
            .map(|v| {
                v.entry.encoding == EncodingId::DictCodes.as_u16()
                    && v.entry.flags & STREAMF_DICT_EXEC != 0
            })
            .unwrap_or(false)
            && dir.lookup(attno, path_ord, StreamRole::Validity).is_none();
        if !eligible {
            return Ok(None);
        }
        Ok(Some(ScanDictSpace::open(
            part.clone(),
            self.unwrappers,
            attno,
            path_ord,
            spec.dict_value_order,
        )?))
    }

    /// Resolve EVERY declared column against EVERY part — the engagement
    /// probe (JSON-routing landing): a consumer with a no-fallback-within-
    /// engagement law calls this BEFORE committing to the scan, so a
    /// per-part lane refusal (`ReadError::ShredLaneRefused` — elections
    /// are per-part) surfaces as a typed pre-engagement decline, never a
    /// mid-scan error the incumbent would not have raised. Metadata-only
    /// (PathTable + StreamDir lookups; no payload decode, no pins held).
    /// Root columns resolve trivially; a table with no lane columns is a
    /// cheap no-op walk.
    pub fn verify_columns(&self) -> ReadResult<()> {
        if self.columns.iter().all(|c| c.shred_path.is_none()) {
            return Ok(());
        }
        for part in &self.parts {
            for (ci, spec) in self.columns.iter().enumerate() {
                resolve_stream_ord(part, spec, ci as u16)?;
            }
        }
        Ok(())
    }

    /// Run the scan at `dop` workers (dop = 1 is the serial determinism
    /// reference — same code path, PC-3.4). `skew_worker`: a worker index
    /// paced between claims (the worker-speed-SKEW probe: claim-order
    /// independence WITNESSED, never assumed).
    pub fn run(&self, dop: usize, skew_worker: Option<usize>) -> ReadResult<ScanResult> {
        assert!(dop >= 1);
        let granule_counts: Vec<u32> = self
            .parts
            .iter()
            .map(|p| p.footer().granule_count)
            .collect();
        let spans = GranuleSpans::new(&granule_counts);
        let cursor = ClaimCursor::new();
        let shared = SharedCounters::new();
        let canon: Mutex<BTreeMap<u64, Vec<u8>>> = Mutex::new(BTreeMap::new());
        let censuses: Mutex<Vec<ScanCensus>> = Mutex::new(Vec::new());
        let errors: Mutex<Vec<ReadError>> = Mutex::new(Vec::new());

        std::thread::scope(|s| {
            for w in 0..dop {
                let spans = &spans;
                let cursor = &cursor;
                let shared = &shared;
                let canon = &canon;
                let censuses = &censuses;
                let errors = &errors;
                let slow = skew_worker == Some(w);
                s.spawn(move || {
                    let mut census = ScanCensus::default();
                    let mut cache = WorkerCache::new();
                    let observer = NoObserver;
                    loop {
                        if slow {
                            std::thread::sleep(std::time::Duration::from_micros(200));
                        }
                        let mut guard = match cursor.begin_claim(spans, w, &observer) {
                            Ok(g) => g,
                            Err(ClaimOutcome::Drained) => break,
                            Err(_) => {
                                std::thread::yield_now();
                                continue;
                            }
                        };
                        if let Err(e) = self.process_claim(
                            &mut guard, spans, &mut cache, &mut census, shared, canon,
                        ) {
                            errors.lock().unwrap().push(e);
                            break;
                        }
                        // guard drops HERE: end_claim runs the attached
                        // pin releases (PC-2.4, by construction).
                    }
                    // Worker drain: drop the cache, counting held pins.
                    for (_, st) in cache.states.drain() {
                        shared.drop_state(st);
                    }
                    censuses.lock().unwrap().push(census);
                });
            }
        });

        if let Some(e) = errors.into_inner().unwrap().into_iter().next() {
            return Err(e);
        }
        let mut census = ScanCensus::default();
        for c in censuses.into_inner().unwrap() {
            census.fold(&c);
        }
        Ok(ScanResult {
            census,
            pins: shared.witness(),
            canon: canon.into_inner().unwrap(),
            claimed_units: cursor.claimed_units(),
        })
    }

    /// The shared span prologue (PC-2.4): part advance/release, part
    /// entry, and the structural claim pin. Returns the span's part index.
    fn begin_span<'g>(
        &self,
        guard: &mut pgrc2_claim::ClaimGuard<'g>,
        spans: &GranuleSpans,
        cache: &mut WorkerCache,
        shared: &'g SharedCounters,
    ) -> ReadResult<usize> {
        let span = guard.span();
        let (pidx, _) = spans.locate(span.start);
        self.begin_part(guard, pidx, cache, shared)?;
        Ok(pidx)
    }

    /// The part-directed prologue half (M4-S5: the survivor-projected
    /// claim space knows the part directly — the unit list, not the span
    /// start, names it). Same advance/enter/pin discipline as
    /// [`Self::begin_span`], one code path.
    fn begin_part<'g>(
        &self,
        guard: &mut pgrc2_claim::ClaimGuard<'g>,
        pidx: usize,
        cache: &mut WorkerCache,
        shared: &'g SharedCounters,
    ) -> ReadResult<()> {
        // Part advance (IN-4 release-on-advance, the #802 lineage): drop
        // every OTHER part's cached state — dict payload buffers, cursors,
        // and (kill arm) held pins. The kill-switch arm
        // (PGRUST_SCAN_PART_RELEASE=0) retains everything for the scan's
        // life: the v3 whole-scan-hold posture, kept ONLY as the born-RED
        // pin-witness arm. The CLAIM pin below is structural in both arms
        // (PC-2.4 is never switchable).
        if self.opts.release_on_advance {
            let stale: Vec<usize> = cache
                .states
                .keys()
                .copied()
                .filter(|k| *k != pidx)
                .collect();
            for k in stale {
                let st = cache.states.remove(&k).expect("keyed");
                shared.drop_state(st);
            }
        }
        if !cache.states.contains_key(&pidx) {
            cache.states.insert(pidx, self.enter_part(pidx, shared)?);
        }
        let st = cache.states.get_mut(&pidx).expect("entered");
        // The claim pin (PC-2.4): taken per claim, attached to the guard,
        // released at end_claim BY CONSTRUCTION — never held across claims.
        {
            let pin = PartPin::pin(&st.part);
            shared.pin_taken();
            guard.attach_release(move || {
                drop(pin);
                shared.pin_released();
            });
        }
        Ok(())
    }

    fn process_claim<'g>(
        &self,
        guard: &mut pgrc2_claim::ClaimGuard<'g>,
        spans: &GranuleSpans,
        cache: &mut WorkerCache,
        census: &mut ScanCensus,
        shared: &'g SharedCounters,
        canon: &Mutex<BTreeMap<u64, Vec<u8>>>,
    ) -> ReadResult<()> {
        let span = guard.span();
        let pidx = self.begin_span(guard, spans, cache, shared)?;
        let WorkerCache { states, batch, strview_arena } = cache;
        let st = states.get_mut(&pidx).expect("entered");
        for unit in span.start..span.end {
            let (p2, g) = spans.locate(unit);
            debug_assert_eq!(p2, pidx, "span crossed a part edge");
            let mut bytes = Vec::new();
            self.process_granule(st, g, unit, batch, strview_arena, census, shared, &mut WindowSink::Canon(&mut bytes))?;
            // Digest currency (opts.canon_digest): fold BEFORE insert so
            // the full per-granule bytes never accumulate — pruned-marker
            // granules fold uniformly with scanned ones.
            let bytes = if self.opts.canon_digest {
                canon_digest_fold(&bytes)
            } else {
                bytes
            };
            canon.lock().unwrap().insert(unit, bytes);
        }
        Ok(())
    }

    fn enter_part(&self, pidx: usize, shared: &SharedCounters) -> ReadResult<PartState> {
        let part = self.parts[pidx].clone();
        let dir = part.stream_directory()?;
        let mut cols = Vec::with_capacity(self.columns.len());
        for (ci, c) in self.columns.iter().enumerate() {
            let attno = c.schema.attno;
            // Stream ordinal: 0 for root columns; shred-lane columns
            // resolve through THIS part's PathTable under the
            // typed-refusal law (per-part elections — resolution is a
            // part-entry fact, never a table fact).
            let path_ord = resolve_stream_ord(&part, c, ci as u16)?;
            let cursor =
                StreamCursor::open(part.clone(), self.binding, attno, path_ord)?;
            // Dict-lane gate (v3 enter_part:1786-1826): dict-coded values
            // stream ∧ STREAMF_DICT_EXEC ∧ zero-null proof (no Validity
            // stream) ∧ opted in. Lane streams sit in the same directory
            // vocabulary at their path_ord — one gate, both shapes.
            let dict = if self.opts.publish_dict_lanes {
                let values = dir.lookup(attno, path_ord, StreamRole::Values);
                let eligible = values
                    .map(|v| {
                        v.entry.encoding == EncodingId::DictCodes.as_u16()
                            && v.entry.flags & STREAMF_DICT_EXEC != 0
                    })
                    .unwrap_or(false)
                    && dir.lookup(attno, path_ord, StreamRole::Validity).is_none();
                if eligible {
                    Some(Box::new(ScanDictSpace::open(
                        part.clone(),
                        self.unwrappers,
                        attno,
                        path_ord,
                        c.dict_value_order,
                    )?))
                } else {
                    None
                }
            } else {
                None
            };
            cols.push(ColState {
                cursor,
                staged: None,
                datums: Vec::new(),
                arena: Vec::new(),
                vwords: Vec::new(),
                verdict: ValidityVerdict::AllValid,
                codes: Vec::new(),
                codes_staged: None,
                dict,
            });
        }
        let meta = match &self.predicate {
            Some((pred, profile)) => Some(ColumnMeta::load(&part, *profile, pred.attno)?),
            None => None,
        };
        let granule_count = part.footer().granule_count;
        // Kill arm only: the v3 scan-life pin (the born-RED reproduction).
        let held_pin = if self.opts.release_on_advance {
            None
        } else {
            shared.pin_taken();
            Some(PartPin::pin(&part))
        };
        let deletes = self.deletes.get(pidx).cloned().flatten();
        Ok(PartState {
            part,
            cols,
            meta,
            deletes,
            granule_count,
            held_pin,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn process_granule(
        &self,
        st: &mut PartState,
        g: u32,
        unit: u64,
        batch: &mut Batch,
        strview_arena: &mut ClaimArena,
        census: &mut ScanCensus,
        shared: &SharedCounters,
        sink: &mut WindowSink<'_, '_>,
    ) -> ReadResult<()> {
        self.process_granule_ext(st, g, unit, None, batch, strview_arena, census, shared, sink)
    }

    /// [`Self::process_granule`] with an EXTERNALLY derived candidate
    /// window (M4-S5: the prune plane's leader-derived PSMA window,
    /// supplied at claim grain). Composed by intersection with any
    /// in-scan predicate consult — one staging/emit code path (the M3
    /// dirsha evidence covers both arms by construction).
    #[allow(clippy::too_many_arguments)]
    fn process_granule_ext(
        &self,
        st: &mut PartState,
        g: u32,
        unit: u64,
        ext_window: Option<(u32, u32)>,
        batch: &mut Batch,
        strview_arena: &mut ClaimArena,
        census: &mut ScanCensus,
        shared: &SharedCounters,
        sink: &mut WindowSink<'_, '_>,
    ) -> ReadResult<()> {
        let rows_g =
            pgrc2_format::geom::rows_in_granule_at(st.part.rows(), st.part.grain(), g);
        // Verdict plane (zone + bloom, exact attribution).
        let mut psma_window: Option<(u32, u32)> = ext_window;
        if let (Some(meta), Some((pred, _))) = (&st.meta, &self.predicate) {
            let lowered = match &pred.eq {
                ScanConst::Word(w) => lower_const(&meta.profile, ConstInput::Word(*w)),
                ScanConst::VarlenaImage(img) => {
                    lower_const(&meta.profile, ConstInput::VarlenaImage(img))
                }
            };
            if let Some(lc) = lowered.lowered() {
                match consult_granule(meta, &lc, g, rows_g, st.granule_count, &mut census.meta) {
                    GranuleConsult::AllFail => {
                        census.granules_zone_pruned += 1;
                        // Deterministic pruned-granule canonical marker
                        // (canon arm); the consumer arm simply never sees
                        // the granule — the census row is the witness.
                        if let WindowSink::Canon(out) = sink {
                            out.push(b'P');
                        }
                        return Ok(());
                    }
                    GranuleConsult::Scan { psma_window: w } => {
                        // Intersect with any externally derived window
                        // (both are sound over-approximations of the
                        // candidate rows; their intersection is too).
                        psma_window = match (psma_window, w) {
                            (None, w) => w,
                            (acc, None) => acc,
                            (Some((alo, ahi)), Some((lo, hi))) => {
                                Some((alo.max(lo), ahi.min(hi)))
                            }
                        };
                        // Normalize an empty intersection to the (0, 0)
                        // proven-absent sentinel (win_start >= hi skips
                        // every window; a non-zero empty range would leak
                        // window 0 through the subtractive check).
                        if let Some((lo, hi)) = psma_window {
                            if lo >= hi {
                                psma_window = Some((0, 0));
                            }
                        }
                    }
                }
            }
        }
        census.granules_scanned += 1;
        // Stage the granule (v3 stage_granule / ensure_col_granule).
        for (spec, cs) in self.columns.iter().zip(st.cols.iter_mut()) {
            stage_column(spec, cs, g, rows_g)?;
        }
        // Emit windows (v3 emit_window), batch-by-batch into the sink —
        // the staging scratch is the worker's (R-1: reused across
        // granules, reset per window).
        let bw = self.opts.batch_rows;
        let windows = rows_g.div_ceil(bw);
        for w in 0..windows {
            let win_start = w * bw;
            let win_rows = bw.min(rows_g - win_start);
            if let Some((lo, hi)) = psma_window {
                // Subtractive window skip (v3 emit_window PSMA compose):
                // a window wholly outside the candidate range never
                // stages.
                if win_start + win_rows <= lo || win_start >= hi {
                    census.psma_windows_skipped += 1;
                    if let WindowSink::Canon(out) = sink {
                        out.push(b'S');
                    }
                    continue;
                }
            }
            self.emit_window(st, g, unit, win_start, win_rows, batch, strview_arena, census, shared, sink)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn emit_window(
        &self,
        st: &mut PartState,
        g: u32,
        unit: u64,
        win_start: u32,
        win_rows: u32,
        batch: &mut Batch,
        strview_arena: &mut ClaimArena,
        census: &mut ScanCensus,
        shared: &SharedCounters,
        sink: &mut WindowSink<'_, '_>,
    ) -> ReadResult<()> {
        // Fresh window: drop every overlay and stale cell (the R4
        // stale-pointer discipline — Batch::begin semantics, with the
        // column set rebuilt per window) and re-arm the StrView arena.
        batch.cols.clear();
        batch.sel.clear();
        batch.nrows = win_rows;
        strview_arena.reset();
        let mut guard = GuardWord::clear();
        for (spec, cs) in self.columns.iter().zip(st.cols.iter_mut()) {
            let base = base_rep_of(&spec.schema.class);
            let mut col = Column::new(base);
            col.ensure_rows(win_rows as usize);
            if let (Some(dict), Some(_)) = (&cs.dict, cs.codes_staged) {
                let codes = &cs.codes[win_start as usize..(win_start + win_rows) as usize];
                if code_bound_guard(codes, dict.ncodes()) {
                    guard.raise(GUARD_CODE_BOUND);
                    census.code_bound_demotions += 1;
                    // Checked gather — the reference drive owns error
                    // identity (typed refusal on the corrupt code).
                    for (r, &code) in codes.iter().enumerate() {
                        let e = dict.handle().entry(code)?;
                        col.datums[r] = datum::Datum::from_u64(e.image.as_ptr() as u64);
                        col.validity.set_valid(r);
                    }
                    col.rep = ColRep::Varlena {
                        inline_proven: true,
                    };
                } else {
                    dict.prepare_frames(codes)?;
                    // Publish the dict-code lane (AB-2.2) + StrView gather
                    // (AB-2.4, AP-3) beside the datum lane.
                    col.validity.reset_all_valid(win_rows as usize);
                    let ptr = core::ptr::NonNull::new(codes.as_ptr() as *mut u32)
                        .expect("codes non-null");
                    let handle = LaneDictHandle::new(&**dict);
                    // SAFETY: publish contract — codes live for the claim
                    // scope (worker-private granule scratch), every code
                    // guarded < ncodes above, column all-valid (zero-null
                    // proof at the lane gate), frames prepared.
                    let lane = unsafe {
                        LaneDictCodes::publish(ptr, handle, dict.epoch(), dict.epoch_key())
                    };
                    col.rep = ColRep::DictCodes(lane);
                    census.dict_lanes_published += 1;
                    let keep_codes = self
                        .code_key_cols
                        .iter()
                        .any(|&k| k as usize == batch.cols.len());
                    if self.opts.build_strviews && !keep_codes {
                        // SAFETY: claim-scoped overlay build; dict payload
                        // region is generation-stable (SB-7 frame grain).
                        let built = unsafe {
                            col.build_strviews_from_dict(win_rows as usize, strview_arena)
                        };
                        if built {
                            census.strview_lanes_built += 1;
                        }
                    }
                    shared
                        .dict_resident_peak
                        .fetch_max(dict.resident_payload_bytes(), Ordering::Relaxed);
                }
            } else {
                // Eager copy from whole-granule scratch (v3 emit_window's
                // positional copy: byte-identical to staging).
                for r in 0..win_rows as usize {
                    let src = win_start as usize + r;
                    let valid = match cs.verdict {
                        ValidityVerdict::AllValid => true,
                        ValidityVerdict::Mixed { .. } => {
                            (cs.vwords[src / 64] >> (src % 64)) & 1 == 1
                        }
                    };
                    if valid {
                        col.datums[r] = datum::Datum::from_u64(cs.datums[src]);
                        col.validity.set_valid(r);
                    } else {
                        col.datums[r] = datum::Datum::null();
                        col.validity.set_null(r);
                    }
                }
            }
            batch.cols.push(col);
        }
        if self.opts.rowid_lane {
            let part_no = st.part.header().part_no;
            let mut col = Column::new(ColRep::RowId);
            col.ensure_rows(win_rows as usize);
            for r in 0..win_rows as usize {
                col.datums[r] = datum::Datum::from_u64(pack_rowid(
                    part_no,
                    g,
                    win_start + r as u32,
                ));
                col.validity.set_valid(r);
            }
            batch.cols.push(col);
        }
        // The AB-3.3 verdict layer's one selection lowering (DM-2): the
        // part's delete-vector mask intersects into the window's selection
        // HERE — the single kernel boundary — so deleted rows are dead to
        // every consumer (filters, aggregates, COUNT(*), canon bytes) by
        // construction. No deletions ⇒ the identity selection, unchanged.
        match st.deletes.as_ref().and_then(|d| d.granule_mask(g)) {
            None => batch.sel_all(),
            Some(mask) => {
                batch.sel.clear();
                for r in 0..win_rows {
                    if !crate::dv::PartDeletes::is_deleted(mask, win_start + r) {
                        batch.sel.push(r);
                    }
                }
                census.rows_deleted_skipped +=
                    win_rows as u64 - batch.sel.len() as u64;
            }
        }
        // M4-S5 (the LIKE band's staging half): consumer-declared string
        // filter columns get their StrView cell lane built over the eager
        // varlena copy too (the dict path built cells above under
        // opts.build_strviews). Fail-open per the builder's law — a
        // still-toasted datum leaves the rep untouched and the consumer
        // declines at its own grain (pgrc2 decode stages plain images, so
        // this is the never-expected arm). Runs AFTER the DM-2 selection
        // lowering: cells build over the surviving selection only.
        for &ci in &self.strview_cols {
            let ci = ci as usize;
            if ci < batch.cols.len() {
                let Batch { cols, sel, .. } = batch;
                let col = &mut cols[ci];
                if matches!(col.rep, ColRep::Varlena { .. }) {
                    // SAFETY: the staged datums are live plain varlena
                    // images from the granule scratch (claim scope);
                    // win_rows covers every selected position.
                    let built =
                        unsafe { col.build_strviews(sel, win_rows as usize, strview_arena) };
                    if built {
                        census.strview_lanes_built += 1;
                    }
                    // The CONTIGUITY WITNESS (M5d.cells.q20-span; the v2
                    // SoaTextSpan lineage): supplied by THIS reader only
                    // where true — the eager verbatim staging path, whose
                    // decode_full bump-allocated every image into ONE
                    // granule-arena pass in row order. Dict-gathered
                    // columns (entry datums in code order) and the
                    // checked-gather guard path never certify — the
                    // consumer's typed demotion to the per-value scan.
                    // Verify-don't-assume: `TextSpan::certify` re-proves
                    // monotonicity/bounds per window before publishing.
                    let cs = &st.cols[ci];
                    if built && cs.dict.is_none() {
                        let alo = cs.arena.as_ptr() as usize;
                        let ahi = alo + cs.arena.len() * 8;
                        // SAFETY: the granule scratch arena is one live
                        // allocation for the claim scope (the same
                        // lifetime the datum lane already aliases).
                        col.text_span = unsafe {
                            pgrc2_batch::TextSpan::certify(
                                &col.datums[..win_rows as usize],
                                &col.validity,
                                alo,
                                ahi,
                            )
                        };
                        if col.text_span.is_some() {
                            census.text_spans_certified += 1;
                        }
                    }
                }
            }
        }
        census.rows_emitted += batch.sel.len() as u64;
        debug_assert!(!guard.demotes() || census.code_bound_demotions > 0);
        match sink {
            WindowSink::Canon(out) => canonicalize_batch(&self.columns, batch, win_rows, out),
            WindowSink::Visit(f) => f(batch, guard, unit, win_start)?,
        }
        Ok(())
    }
}

/// The per-worker consumer drive (M4-S3, the executor-binding face —
/// the v3-17 "engine executor binding remains M4's" remainder): one
/// [`ScanWorker`] per participant thread, claims driven through the SAME
/// span/pin/staging path as [`TableScan::run`] (one code path — the M3
/// dirsha evidence covers this staging by construction), each staged
/// window handed to the visitor at batch grain (PC-2.4: the borrow is
/// claim-scoped and never escapes the call).
pub struct ScanWorker<'s> {
    scan: &'s TableScan,
    cache: WorkerCache,
    /// Per-worker census (PC-6.1: folded at drain by the consumer).
    pub census: ScanCensus,
}

impl<'s> ScanWorker<'s> {
    pub fn new(scan: &'s TableScan) -> ScanWorker<'s> {
        ScanWorker {
            scan,
            cache: WorkerCache::new(),
            census: ScanCensus::default(),
        }
    }

    /// Drive one claimed span: every surviving window reaches `visit` as
    /// a staged ABI batch with its per-batch guard word (AB-7.3 —
    /// `GUARD_CODE_BOUND` means the batch is already the hydrated checked
    /// form). Zone-pruned granules and PSMA-skipped windows never surface;
    /// their census rows are the witness (PC-6.2).
    pub fn drive_claim<'g>(
        &mut self,
        guard: &mut pgrc2_claim::ClaimGuard<'g>,
        spans: &GranuleSpans,
        shared: &'g SharedCounters,
        visit: &mut dyn FnMut(&Batch, GuardWord, u64, u32) -> ReadResult<()>,
    ) -> ReadResult<()> {
        let span = guard.span();
        let pidx = self.scan.begin_span(guard, spans, &mut self.cache, shared)?;
        let WorkerCache { states, batch, strview_arena } = &mut self.cache;
        let st = states.get_mut(&pidx).expect("entered");
        for unit in span.start..span.end {
            let (p2, g) = spans.locate(unit);
            debug_assert_eq!(p2, pidx, "span crossed a part edge");
            self.scan.process_granule(
                st,
                g,
                unit,
                batch,
                strview_arena,
                &mut self.census,
                shared,
                &mut WindowSink::Visit(visit),
            )?;
        }
        Ok(())
    }

    /// Drive one claimed SURVIVOR span (M4-S5, the prune-first drive): the
    /// claim came off a [`crate::SurvivorSpans`] index space, `units` is
    /// its global-unit list (one part, by the boundary law), and
    /// `ext_window` supplies the leader-derived PSMA candidate window per
    /// unit. `admit` is THE chokepoint consult (predicates installed after
    /// the derive); an inadmissible granule is skipped before any staging
    /// — its accounting lives on the plane's schedule-dependent witness,
    /// never in the identity-compared census.
    #[allow(clippy::too_many_arguments)]
    pub fn drive_claimed_units<'g>(
        &mut self,
        guard: &mut pgrc2_claim::ClaimGuard<'g>,
        spans: &GranuleSpans,
        units: &[u64],
        ext_window: &dyn Fn(u64) -> Option<(u32, u32)>,
        admit: &dyn Fn(usize, u32, u32) -> ReadResult<bool>,
        shared: &'g SharedCounters,
        visit: &mut dyn FnMut(&Batch, GuardWord, u64, u32) -> ReadResult<()>,
    ) -> ReadResult<()> {
        let Some(&first) = units.first() else { return Ok(()) };
        let (pidx, _) = spans.locate(first);
        self.scan.begin_part(guard, pidx, &mut self.cache, shared)?;
        let WorkerCache { states, batch, strview_arena } = &mut self.cache;
        let st = states.get_mut(&pidx).expect("entered");
        for &unit in units {
            let (p2, g) = spans.locate(unit);
            debug_assert_eq!(p2, pidx, "survivor span crossed a part edge");
            let rows_g =
                pgrc2_format::geom::rows_in_granule_at(st.part.rows(), st.part.grain(), g);
            if !admit(pidx, g, rows_g)? {
                continue;
            }
            self.scan.process_granule_ext(
                st,
                g,
                unit,
                ext_window(unit),
                batch,
                strview_arena,
                &mut self.census,
                shared,
                &mut WindowSink::Visit(visit),
            )?;
        }
        Ok(())
    }

    /// Worker drain: drop cached part state, counting held pins (the kill
    /// arm's witness), and yield the per-worker census for the fold.
    pub fn finish(mut self, shared: &SharedCounters) -> ScanCensus {
        for (_, st) in self.cache.states.drain() {
            shared.drop_state(st);
        }
        self.census
    }
}

/// OD-9 (AP-4): the per-batch VECTORIZED max-code guard — ONE max over the
/// batch's code lane vs the dict length. TRUE = the batch demotes to
/// checked gather (AB-7.3 `GUARD_CODE_BOUND`); the trusted gather never
/// runs over an unguarded lane. Public so the born-RED tooth exercises the
/// decision directly.
#[inline]
pub fn code_bound_guard(codes: &[u32], ncodes: u32) -> bool {
    // A reduction the autovectorizer turns into lane-wide NEON max — the
    // whole point of batch-grain (1024) guarding vs the v3 per-granule
    // scalar `any()` (pgrc.rs:1990).
    let maxc = codes.iter().copied().max().unwrap_or(0);
    maxc >= ncodes
}

/// Resolve one scan column's stream ordinal in `part`: 0 for root columns;
/// shred-lane columns (`shred_path`) resolve dotted path → path_ord through
/// the part's PathTable (spec §6.5 — positional, per-part), then verify the
/// sealed StreamDir entry's lane kind against the declared column class.
///
/// The typed-refusal law (TY-3 read supply): every way a part cannot serve
/// the declared lane is a `ReadError::ShredLaneRefused` with a
/// deterministic static cause — never a fabricated all-NULL column (lane
/// NULLs are a VALUE geometry: SQL-NULL/jsonb-null/absent/exception rows;
/// an unserved lane is not that). The NumericFs arm refuses even on a kind
/// match: the lane's chunk-shared decimal scale has no persistence slot
/// (the spec §6.5 gap `pgrc2_format::shredlane` documents), so no reader
/// can interpret its words soundly until the format grows the slot.
fn resolve_stream_ord(part: &Arc<OpenPart>, spec: &ScanColumn, ordinal: u16) -> ReadResult<u32> {
    let Some(path) = &spec.shred_path else {
        return Ok(0);
    };
    let attno = spec.schema.attno;
    let refuse = |why: &'static str| {
        Err(ReadError::ShredLaneRefused {
            attno,
            ordinal,
            why,
        })
    };
    let Some(declared) = ShredLaneKind::of_class(&spec.schema.class) else {
        return refuse("declared column class is not a shred lane class");
    };
    let Some(paths) = pgrc2_read::read_path_table(part)? else {
        return refuse("part carries no shred lanes (no PathTable section)");
    };
    let Some(pos) = paths.iter().position(|p| p == path) else {
        return refuse("path not elected in this part");
    };
    let path_ord = pos as u32 + 1;
    let dir = part.stream_directory()?;
    let Some(v) = dir.lookup(attno, path_ord, StreamRole::Values) else {
        // The PathTable names the lane but no Values stream exists at its
        // ordinal: structural corruption, not an election refusal.
        return Err(ReadError::StreamMissing {
            attno,
            path_ord,
            role: StreamRole::Values.as_u8(),
        });
    };
    match ShredLaneKind::of_entry(v.entry.class, v.entry.fixed_len) {
        Some(k) if k != declared => {
            refuse("sealed lane kind disagrees with the declared lane class")
        }
        Some(ShredLaneKind::NumericFs) => {
            // RULED 2026-08-14: the scale rides the Values entry's aux32
            // under STREAMF_LANE_SCALE. Scale-0 lanes serve as signed
            // int words (the µs-epoch class); non-zero scales await
            // their rendering family; flag-absent parts predate the
            // slot and stay refused.
            match pgrc2_format::shredlane::numeric_lane_scale(v.entry.flags, v.entry.aux32) {
                Err(why) => refuse(why),
                Ok(0) => Ok(path_ord),
                Ok(_) => refuse("non-zero-scale numeric lane (rendering family unrouted)"),
            }
        }
        Some(_) => Ok(path_ord),
        None => refuse("sealed lane stream carries a non-lane storage class"),
    }
}

/// The base rep of a storage class (the lane vocabulary mapping).
fn base_rep_of(class: &StorageClass) -> ColRep {
    match class {
        StorageClass::ByvalWord { .. } | StorageClass::Bool | StorageClass::F32 | StorageClass::F64 => {
            ColRep::ByvalWord
        }
        StorageClass::Fixed { len } => ColRep::FixedRef {
            width: (*len).min(u16::MAX as u32) as u16,
        },
        StorageClass::VarlenaVerbatim => ColRep::Varlena {
            inline_proven: false,
        },
    }
}

/// Stage one column's whole-granule scratch (decode_full + validity, or
/// decode_codes for a published dict lane) — v3 `ensure_col_granule`.
fn stage_column(spec: &ScanColumn, cs: &mut ColState, g: u32, rows_g: u32) -> ReadResult<()> {
    if cs.dict.is_some() {
        if cs.codes_staged == Some(g) {
            return Ok(());
        }
        cs.codes.resize(rows_g as usize, 0);
        let n = cs.cursor.decode_codes(g, &mut cs.codes[..rows_g as usize])?;
        if n != rows_g {
            return Err(ReadError::Format(FormatError::Corrupt {
                at: "codes count vs granule rows",
            }));
        }
        cs.codes_staged = Some(g);
        return Ok(());
    }
    if cs.staged == Some(g) {
        return Ok(());
    }
    let rows = rows_g as usize;
    if cs.arena.is_empty() {
        cs.arena.resize(96 * 1024, 0);
    }
    cs.datums.resize(rows.max(1), 0);
    loop {
        let arena_bytes = unsafe {
            core::slice::from_raw_parts_mut(cs.arena.as_mut_ptr() as *mut u8, cs.arena.len() * 8)
        };
        let mut dout = DecodeOut {
            datums: &mut cs.datums[..rows],
            arena: ByteArena::new(arena_bytes),
        };
        match cs.cursor.decode_full(g, &mut dout) {
            Ok(_) => break,
            Err(ReadError::Format(FormatError::ArenaExhausted { needed }))
                if cs.arena.len() < 64 * 1024 * 1024 =>
            {
                let grow = needed.div_ceil(8) + cs.arena.len() * 2;
                cs.arena = vec![0u64; grow];
                continue;
            }
            Err(e) => return Err(e),
        }
    }
    cs.vwords.resize(rows.div_ceil(64).max(1), 0);
    cs.verdict = cs.cursor.validity(g, &mut cs.vwords[..rows.div_ceil(64).max(1)])?;
    cs.staged = Some(g);
    let _ = spec;
    Ok(())
}

/// Canonical batch serialization — the byte-identity currency. Reads the
/// batch THROUGH the lane vocabulary (dict lanes via `DictSpace`
/// entry bytes, StrView/varlena via the authoritative datum lane), so the
/// bytes witness the ABI path, not a side channel.
fn canonicalize_batch(specs: &[ScanColumn], batch: &Batch, win_rows: u32, out: &mut Vec<u8>) {
    out.push(b'W');
    for (ci, _spec) in specs.iter().enumerate() {
        let col = &batch.cols[ci];
        out.push(match col.rep {
            ColRep::DictCodes(_) => b'd',
            ColRep::StrView(_) => b's',
            _ => b'b',
        });
        for &pos in batch.sel.as_slice() {
            let r = pos as usize;
            if r >= win_rows as usize {
                break;
            }
            if !col.validity.is_valid(r) {
                out.push(0xFF);
                continue;
            }
            out.push(1);
            match col.rep {
                ColRep::DictCodes(dc) => {
                    // Canonicalize the VALUE (not the code): entry bytes
                    // through the dict space — code identity is part-local
                    // (Law A), value identity is what byte-identity means.
                    // SAFETY: claim-scoped consume of the published lane.
                    let space = unsafe { dc.dict().space() };
                    let code = unsafe { dc.codes(win_rows as usize) }[r];
                    let d = space.entry_datum(code).as_u64();
                    push_varlena_canon(d, out);
                }
                ColRep::StrView(_) | ColRep::Varlena { .. } => {
                    push_varlena_canon(col.datums[r].as_u64(), out);
                }
                ColRep::FixedRef { width } => {
                    let p = col.datums[r].as_u64() as *const u8;
                    let bytes =
                        unsafe { core::slice::from_raw_parts(p, width as usize) };
                    out.extend_from_slice(bytes);
                }
                _ => {
                    out.extend_from_slice(&col.datums[r].as_u64().to_le_bytes());
                }
            }
        }
    }
    // RowId lane (last column when present) is covered by the loop above
    // only for spec'd columns; rowids are deterministic by construction
    // and canonicalized implicitly through granule keying.
}

pub(crate) fn push_varlena_canon(d: u64, out: &mut Vec<u8>) {
    // Plain 4B-U image (the canonical decoded shape; dict entries are §7b
    // varlena-shaped by law).
    let p = d as *const u8;
    let hdr = u32::from_le_bytes(unsafe { *(p as *const [u8; 4]) });
    let total = (hdr >> 2) as usize;
    let payload = unsafe { core::slice::from_raw_parts(p.add(4), total.saturating_sub(4)) };
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
}
