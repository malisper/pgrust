//! Sequential scan: RG visibility, zone-map pruning, granule decode, window
//! staging for the page-batch executor drive (docs/design/pgrcolumnar-impl.md §7.3).

use ::datum::Datum;
use ::types_error::{PgError, PgResult};
use ::types_slot::SlotData;

pub use ::tableam_vocab::{ZoneCmp, ZoneQual, ZoneVerdict};
use ::tableam_vocab::TableScanDescData;

use std::sync::atomic::Ordering;

use crate::format::*;
use crate::reader::Part;

// Reader-side intcodec kill switch: PGRUST_CBSTORE_INTCODEC_READ=off drops
// DeltaFor chunks from the int fast paths that key on granule zone maps
// (adaptive traversal, staged window min/max) — decode itself is unaffected
// (the data must stay readable regardless).
pub(crate) fn intcodec_read_fastpaths() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| match std::env::var("PGRUST_CBSTORE_INTCODEC_READ") {
        Ok(v) => !matches!(v.trim(), "off" | "0" | "false"),
        Err(_) => true,
    })
}

// An int-shape chunk whose granule zone maps may drive value fast paths.
pub(crate) fn int_zonemap_encoding(e: Encoding) -> bool {
    matches!(e, Encoding::Raw | Encoding::For | Encoding::Const)
        || (e == Encoding::DeltaFor && intcodec_read_fastpaths())
}

struct ColDecode {
    datums: Vec<Datum>,
    dict: Vec<Datum>,
    dict_rg: usize,
    // Lz4Text decompress target; u64-backed for varlena alignment.
    arena: Vec<u64>,
    // Dict-encoded granules decode codes only (no per-row dictionary
    // gather); every Datum consumer reads dict[codes[row]] on demand, and
    // the lane executor's dict-memo tier reads codes+dict zero-decode.
    codes: Vec<u32>,
    is_dict: bool,
    // Sub-granule dict frames (CHUNK_FLAG_DICT_FRAMED): the lazy ensure
    // state for the CURRENT dict table — `dict` points into its arena and
    // entry BYTES materialize per ensured frame. None = fully-materialized
    // dict (unframed chunk / lazy reads killed). Rebuilt with `dict`.
    lazy: Option<Box<crate::reader::DictLazy>>,
    // CHUNK_FLAG_DICT_SORTED: codes are byte-rank order (dict range preds).
    dict_sorted: bool,
    // Text-blob contiguity (likeband blob kernel): the decoded granule's
    // datums are ascending pointers into ONE readable span (RawText mmap
    // blob / Lz4Text decode arena) — the staged window may publish a
    // SoaTextSpan witness so blob-wide substring kernels run one search over
    // the whole span.
    contig_text: bool,
    // (rg, granule) this column's buffers hold; granule content per key is
    // immutable, so a matching key is valid across rescans. NONE_KEY = none.
    gkey: (u32, u32),
    // Per-dict-code length memo (length-lane fills over dict-encoded text
    // chunks): lens[code] = the dict entry's octet or UTF-8 character
    // length, computed ONCE per (row-group dictionary, kind) — every staged
    // row then reads its length as one table gather, never touching string
    // payload bytes per row. Keyed like `dict` (dict_rg) plus the kind.
    len_memo: Vec<i64>,
    len_memo_key: (usize, u8),
}

const NONE_KEY: (u32, u32) = (u32::MAX, u32::MAX);

/// Blob-span witness for a staged text window (likeband): the window's
/// images are complete 4B-U varlena values laid out back-to-back (4-byte
/// alignment padding) in one readable span, and the window's datums are
/// STRICTLY ascending pointers into it. Re-proved per window (the writer
/// never reorders/dedups the text blob, but the hit→row mapping depends on
/// it — verify, don't assume); an unprovable window returns None and the
/// consumer stays per-row.
fn staged_text_span(ds: &[Datum]) -> Option<::exectuples::SoaTextSpan> {
    let first = ds.first()?.as_usize();
    let mut prev = first;
    for d in &ds[1..] {
        let p = d.as_usize();
        if p <= prev {
            return None;
        }
        prev = p;
    }
    // SAFETY: pgrcolumnar text datums point at live complete varlena images
    // (decode contract); `prev` is the window's last (highest) image.
    let end = prev + unsafe { ::types_tuple::varatt::varsize_any(prev as *const u8) };
    Some(::exectuples::SoaTextSpan { base: first as *const u8, len: end - first })
}

// Exact granule fallback for metadata SUM (RGs without valid footer sums):
// Const granules fold aux * rows; other int encodings decode and fold the
// sign-extended datum words in i128 (int chunks are Raw/For/Const, so the
// dict/arena scratch stays untouched).
fn sum_granule(
    part: &Part,
    rg: usize,
    g: usize,
    sums: &mut [(u16, i128)],
    scratch: &mut (Vec<Datum>, Vec<Datum>, Vec<u64>),
) {
    let rg_rows = part.rgs[rg].nrows as usize;
    let n = (rg_rows - g * GRANULE_ROWS).min(GRANULE_ROWS);
    for e in sums.iter_mut() {
        let cv = part.chunk(rg, e.0 as usize);
        if cv.hdr.encoding == Encoding::Const {
            e.1 += cv.hdr.aux as i128 * n as i128;
            continue;
        }
        let (out, dict, arena) = (&mut scratch.0, &mut scratch.1, &mut scratch.2);
        cv.decode_granule(g, out, dict, arena);
        e.1 += out.iter().map(|d| d.as_i64() as i128).sum::<i128>();
    }
}

fn new_col_decode() -> ColDecode {
    ColDecode {
        datums: Vec::new(),
        dict: Vec::new(),
        dict_rg: usize::MAX,
        arena: Vec::new(),
        codes: Vec::new(),
        is_dict: false,
        lazy: None,
        dict_sorted: false,
        contig_text: false,
        gkey: NONE_KEY,
        len_memo: Vec::new(),
        len_memo_key: (usize::MAX, 0),
    }
}

// octet_length / length(text) of one decoded inline varlena image.
// `chars` = the UTF-8 character count with C text_length's exact semantics:
// the arming seam admits the chars kind only under a UTF-8 server encoding,
// where C calls pg_mbstrlen_with_len — reused through its seam verbatim
// (total over arbitrary bytes: NUL-stop and lead-byte jumps included), so
// the lane value is C's answer BY CONSTRUCTION for any payload, valid UTF-8
// or not (no countability proof needed).
//
// # Safety
// `d` is a live inline varlena image (pgrcolumnar decode contract: 1B short or
// plain 4B-U).
#[inline]
unsafe fn text_datum_len(d: Datum, chars: bool) -> i64 {
    let p = d.as_usize() as *const u8;
    // SAFETY: forwarded caller contract.
    let payload = unsafe {
        if ::types_tuple::varatt::varatt_is_1b(p) {
            core::slice::from_raw_parts(p.add(1), ::types_tuple::varatt::varsize_1b(p) - 1)
        } else {
            debug_assert!(::types_tuple::varatt::varatt_is_4b_u(p));
            core::slice::from_raw_parts(p.add(4), ::types_tuple::varatt::varsize_4b(p) - 4)
        }
    };
    if chars {
        ::mbutils_seams::pg_mbstrlen_with_len::call(payload)
            .expect("pg_mbstrlen_with_len is total over arbitrary bytes") as i64
    } else {
        payload.len() as i64
    }
}

// Returns true when the column's per-RG DICTIONARY was actually (re)built
// by this call — the drive-scaling observability channel's epoch-rebuild
// counter (per-worker per-RG dictionary rebuilds are a named suspect;
// serial scans count here too, so worker totals compare against the serial
// baseline directly).
fn decode_col(part: &Part, rg: usize, g: usize, c: usize, cd: &mut ColDecode) -> bool {
    if cd.gkey == (rg as u32, g as u32) {
        return false;
    }
    if cd.dict_rg != rg {
        cd.dict.clear();
        cd.dict_rg = rg;
    }
    let dict_was_empty = cd.dict.is_empty();
    let chunk = part.chunk(rg, c);
    cd.is_dict =
        chunk.decode_granule_codes(g, &mut cd.codes, &mut cd.dict, &mut cd.arena, &mut cd.lazy);
    cd.dict_sorted = cd.is_dict && chunk.hdr.flags & CHUNK_FLAG_DICT_SORTED != 0;
    cd.contig_text = false;
    if !cd.is_dict {
        chunk.decode_granule(g, &mut cd.datums, &mut cd.dict, &mut cd.arena);
        // Blob contiguity witness: both text encodings decode the granule's
        // rows into one span (RawText: the chunk's mmap blob; Lz4Text: the
        // per-granule decompress arena) with row-order offsets.
        cd.contig_text =
            matches!(chunk.hdr.encoding, Encoding::RawText | Encoding::Lz4Text);
    }
    cd.gkey = (rg as u32, g as u32);
    cd.is_dict && dict_was_empty
}

impl ColDecode {
    #[inline]
    fn datum(&self, row: usize) -> Datum {
        if self.is_dict {
            let code = self.codes[row];
            // Lazy sub-framed dict: the published Datum's bytes must exist
            // before any consumer dereferences them (store_slot / gather_row
            // per-row publishes).
            if let Some(l) = &self.lazy {
                l.ensure_code(code);
            }
            self.dict[code as usize]
        } else {
            self.datums[row]
        }
    }
}

// Ref-gather decode scratch (bounded-sort drain): its own ColDecode set so
// gathers never disturb the staged window's buffers. Keyed by
// (rg, granule, needed_epoch) — a needed-set change invalidates the decode.
struct GatherScratch {
    cols: Vec<ColDecode>,
    key: (usize, usize, u64),
}

/// Dict-coded view of one staged window column: u32 codes into the
/// per-row-group dictionary of decoded text Datums, plus the STABLE
/// DICTIONARY IDENTITY key (`epoch` = row-group index; dict content per RG
/// is immutable and the scan pins its `Arc<Part>`, so the key is stable
/// across rescans). Slices live until the granule's next decode of a
/// different (rg, granule) key — granule-long, covering every window staged
/// from it.
// ---- lazy sub-framed dict seam (CHUNK_FLAG_DICT_FRAMED) --------------------
// The SoaDictTable-facing ensure thunks: `p` is the publishing scan's live
// DictLazy for the staged window (same lifetime as the published `dict`
// pointer). Published only while frames remain unmaterialized — a fully-done
// table publishes a null seam (zero per-datum overhead).
unsafe fn dict_lazy_ensure_code(p: *const (), code: u32) {
    // SAFETY: seam contract above.
    unsafe { &*(p as *const crate::reader::DictLazy) }.ensure_code(code)
}

unsafe fn dict_lazy_ensure_all(p: *const ()) {
    // SAFETY: seam contract above.
    unsafe { &*(p as *const crate::reader::DictLazy) }.ensure_all()
}

type LazySeam = (*const (), Option<unsafe fn(*const (), u32)>, Option<unsafe fn(*const ())>);

fn lazy_seam(lazy: &Option<Box<crate::reader::DictLazy>>) -> LazySeam {
    match lazy {
        Some(l) if !l.all_done() => (
            (&**l as *const crate::reader::DictLazy).cast(),
            Some(dict_lazy_ensure_code as unsafe fn(*const (), u32)),
            Some(dict_lazy_ensure_all as unsafe fn(*const ())),
        ),
        _ => (std::ptr::null(), None, None),
    }
}

pub struct CbDictLane<'a> {
    pub codes: &'a [u32],
    pub dict: &'a [Datum],
    pub epoch: u64,
    /// Dict entries are byte-sorted (codes are rank order) — gates
    /// dict-code range predicates.
    pub sorted: bool,
}

/// Metadata MIN/MAX/COUNT/SUM answer: visible row count + per requested
/// column (col, min, max) over visible rows, i64-widened exactly as decode
/// datums, + per requested column exact i128 sums (footer sums where valid,
/// granule decode otherwise).
pub struct MetaAggScan {
    pub rows: u64,
    pub minmax: Vec<(u16, i64, i64)>,
    pub sums: Vec<(u16, i128)>,
}

/// v7 zero-count metadata qual: the scan qual is EXACTLY one
/// `col <> 0` / `col = 0` conjunct (stored-domain zero) over an int-family
/// column. `keep_nonzero` = true for `<>`. The meta arm then answers
/// COUNT as N - zeros (or zeros), and SUM/AVG over the SAME column from the
/// unchanged footer sum S (excluded zero rows contribute exactly zero; for
/// `= 0` the sum is identically 0) — the admission site enforces the
/// same-column restriction and refuses MIN/MAX.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MetaZeroQual {
    pub col: u16,
    pub keep_nonzero: bool,
}

// Zone-ordered adaptive traversal (docs/design/pgrcolumnar-zone-adaptive.md):
// granules visited best-first by the sort-key column's zone bound, with a
// consumer-fed stop bound (top-k heap floor / running MIN-MAX best). Armed
// only on serial scans over exact-zone int-family columns; the physical
// drive is untouched when unarmed.
struct AdaptiveOrder {
    entries: Vec<AdaptiveEntry>,
    cursor: usize,
    col: usize,
    desc: bool,
    // Skip granules whose bound EQUALS the stop bound (value objectives:
    // MIN/MAX). Top-k arms false: an equal-key row with a smaller row ref
    // beats the heap floor (tie-ordering rule 2,
    // docs/conformance/tie-ordering.md), so only strict domination skips.
    strict: bool,
    bound: Option<i64>,
    // Probe budget (measured failure mode: take-k sorted on the 10M bank — a sparse qual never
    // bounds the heap early, so the best-first walk degenerated into a full
    // scattered-order scan, 2.5x the physical drive). The walk is a PROBE:
    // if it isn't visibly paying by these thresholds, revert the REMAINING
    // entries to physical (rg, g) order — pure visitation-order change, so
    // it is always correct; per-granule bound skips stay live after revert
    // (strict domination is order-independent), only the sorted-order early
    // STOP is forfeited.
    //   nobound_budget: claims allowed before the consumer ever feeds a
    //     bound (heap never filled — the sparse-qual take-k class).
    //   projected_budget: with a bound in hand the sorted entry list makes
    //     the walk's end exact TODAY (binary search for the first dominated
    //     bound); if that projection says more than this many further
    //     claims, the zone/key correlation isn't there — revert now. The
    //     projection only shrinks as the bound tightens, so a walk that
    //     would have stopped within budget is never reverted (a 10M-bank sorted-limit projection walk
    //     stops after 34/1238 granules and must stay on the fast path).
    nobound_budget: usize,
    projected_budget: usize,
    reverted: bool,
}

impl AdaptiveOrder {
    // First index >= cursor whose bound the current stop-bound dominates;
    // entries.len() when none (entries are bound-sorted while !reverted, so
    // this is exactly where the sorted walk will stop).
    fn projected_stop(&self, b: i64) -> usize {
        let tail = &self.entries[self.cursor..];
        self.cursor
            + tail.partition_point(|e| match (self.desc, self.strict) {
                (true, false) => e.bound >= b,
                (true, true) => e.bound > b,
                (false, false) => e.bound <= b,
                (false, true) => e.bound < b,
            })
    }
}

#[derive(Clone, Copy)]
struct AdaptiveEntry {
    rg: u32,
    g: u32,
    bound: i64,
}

/// `granule_meta_peek` verdict (v7 length-stats granule metadata arm).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CbGranuleMetaStep {
    /// Scan exhausted (next_window would return 0).
    Exhausted,
    /// Not at a metadata-answerable fresh granule; stage windows normally.
    NotMeta,
    /// The upcoming granule, wholly visible, described by footer metadata.
    Meta { rows: u32 },
}

/// `agg_meta_peek` verdict (footer-stat consumption arm: whole-RG /
/// whole-granule aggregate answers under an all-rows-passing zone proof).
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum CbAggMetaStep {
    /// Scan exhausted (next_window would return 0).
    Exhausted,
    /// Not at a metadata-answerable position; stage windows normally.
    NotMeta,
    /// The upcoming WHOLE row group: wholly visible, every pushed zone qual
    /// AllPass over its footer extremes — described by footer metadata
    /// (row count + per-column exact (min, max) / i128 sums / Σ length).
    MetaRg { rows: u64 },
    /// The upcoming granule: wholly visible RG, every pushed zone qual
    /// AllPass over the granule's zone entry — row count + per-column exact
    /// (min, max) / Σ length (the format stores NO granule-altitude value
    /// sums, so `sum_cols` must be empty for this tier to serve).
    MetaGranule { rows: u32 },
}

// Condition-cache scan arm (pgrust.condition_cache): the staged-qual
// fingerprint plus the current row group's shared RgEntry, refreshed once
// per RG (the global cache lock is per-RG, window lookups/stores are
// lock-free through the Arc).
struct CondState {
    fp: u128,
    cur_rg: u32,
    entry: Option<std::sync::Arc<crate::condcache::RgEntry>>,
    // Per-scan hit/miss cells (folded into the process counters at drop or
    // via condcache_fold_stats) — the shared per-window fetch_add was the
    // 16-worker cache-line contention on condcache-hot scans (census U7).
    stats: crate::condcache::LocalStats,
}

pub struct CbScanDescData<'mcx> {
    pub rs_base: TableScanDescData<'mcx>,
    part: Option<std::sync::Arc<Part>>,
    coltypes: Vec<ColType>,
    needed: Vec<bool>,
    needed_idx: Vec<u16>,
    needed_epoch: u64,
    gather: Option<Box<GatherScratch>>,
    // One-time null-init of the scan's dedicated virtual slot: per-row store
    // then touches only needed columns.
    slot_inited: std::cell::Cell<bool>,
    zone_quals: Vec<ZoneQual>,
    cols: Vec<ColDecode>,
    // Next window to stage. `rg` is valid only while `rg_claimed`; claim
    // granularity is one row group — parallel workers draw from the shared
    // phs_nallocated cursor, serial scans from `serial_next`.
    rg: usize,
    rg_claimed: bool,
    serial_next: usize,
    granule: usize,
    win: usize,
    rg_checked: bool,
    decoded: bool,
    granule_rows: usize,
    // Granule-range drive (runtime morsels, M1 scan pipelines): exclusive
    // end granule WITHIN the positioned row group. While Some, the scan
    // serves exactly `set_granule_range`'s claim — it never claims another
    // RG (the range is the claim; morsel contract: a claim never crosses a
    // row-group/dict-epoch boundary) and returns 0 at the range end.
    range_end: Option<usize>,
    // Direct bounded top-N granule drive (`topn_direct_next_granule`): the
    // current granule was already handed out whole — the next call advances
    // first. Reset by every (re)position (`set_granule_range`).
    direct_handed: bool,
    // Per-1024-row block admission mask for the decoded granule (bit b =
    // block b may contain qual matches); windows in cleared blocks are
    // skipped without staging.
    block_mask: u32,
    // Forced-off knobs (byte-identical A/B gates): read once per scan.
    block_zm_enabled: bool,
    bloom_enabled: bool,
    // Staging window width (rows per staged batch): WINDOW_ROWS unless
    // overridden by PGRUST_CB_WINDOW_ROWS (see env_window_rows).
    window_rows: usize,
    // Post-qual materialization (pgrcolumnar_prewhere): granule decode is
    // per-column on demand — the SoA deform pulls only the columns it fills
    // and store_slot completes the needed set for surviving rows only.
    lazy: bool,
    // (rg, granule, needed_epoch) whose needed set is fully decoded; the
    // per-row store path's one-compare fast gate.
    all_ready: (u32, u32, u64),
    // SO_TEMP_SNAPSHOT (parallel worker scans): unregistered at endscan.
    pub rs_temp_snapshot: Option<std::rc::Rc<::types_snapshot::SnapshotData<'static>>>,
    // Staged window.
    staged_lo: usize,
    staged_rows: usize,
    // Per-row drive cursor within the staged window.
    row_cursor: usize,
    adaptive: Option<Box<AdaptiveOrder>>,
    // Condition cache (pgrust.condition_cache): armed by the PREWHERE driver
    // with the staged prefix's canonical fingerprint. None = unarmed.
    cond: Option<Box<CondState>>,
    // Claim-time readahead env gate (PGRUST_CBSTORE_READAHEAD; default on),
    // read once per scan — the global kill switch for every advise path
    // (legacy parallel arm, claim drive, serial drive, footer sections).
    readahead: bool,
    // Claim-drive readahead depth (cold-readahead lane): on a NEW row group
    // entered through `set_granule_range` (a runtime morsel claim), advise
    // the claimed RG's own needed-column extents plus this many RGs ahead.
    // 0 = hook off. Gated by `readahead` (the global kill switch) at every
    // use. Bounded by construction: at most (1 + depth) madvise spans per
    // RG switch, no queue, no allocation — the advised bytes are exactly
    // what the scan is about to read anyway.
    ra_claims: usize,
    // Serial physical-order drive readahead — DEFAULT ON (ratified
    // 2026-07-15, superseding the historical arm-A no-prefetch convention;
    // see env_readahead_serial). PGRUST_CBSTORE_READAHEAD_SERIAL=0 is the
    // opt-out; the global kill switch also silences it.
    ra_serial: bool,
    // pgstat-style counters for the verdict's bytes-read accounting.
    pub granules_pruned: u64,
    // Subset of granules_pruned attributed to a bloom rejection (the granule
    // survived every zone/block check but the Eq const hashed absent):
    // the bloom-utilization observability channel.
    pub granules_bloom_pruned: u64,
    pub granules_scanned: u64,
    pub blocks_pruned: u64,
    pub windows_staged: u64,
    pub granules_bound_skipped: u64,
    pub adaptive_probe_reverts: u64,
    // Row groups whose chunk extents were advised (claim-time readahead).
    // Test/observability channel: a serial scan must always read 0 here.
    pub rgs_readahead: u64,
    // Row groups advised by the CLAIM-DRIVE / serial hooks (cold-readahead
    // lane) — separate from `rgs_readahead` so the legacy serial guard
    // (readahead_scope.rs: serial physical drive reads 0) stays exact.
    pub rgs_claim_readahead: u64,
    // Granules answered wholesale from footer metadata (never decoded):
    // the sorted-fold v7 length-stats arm plus the plain-fold footer-stat
    // arm (agg_meta_peek — whole-RG consumes count the RG's granules).
    pub granules_meta: u64,
    // Drive-scaling observability (the runtime WFIN channel): granule-range
    // re-entries that landed in a NEW row group (per-claim epoch rolls) and
    // actual per-RG dictionary (re)builds across this scan's columns. Serial
    // scans count too (decode_col's dict_rg roll), so per-worker totals
    // compare against the serial baseline directly.
    pub rg_switches: u64,
    pub dict_builds: u64,
    // v7 stitch identity for this scan's dict lanes (SoaDictTable::gepoch):
    // process-unique per scan instance, stable across RGs and rescans (the
    // pinned part's stitch content never changes under a live scan). 0 when
    // stitch publication is disabled (PGRUST_LANE_V2_GLOBALDICT=0).
    scan_uid: u64,
}

// Why granule_admit refused a granule: Zone covers the zone-map and
// block-zone-map folds; Bloom is the per-granule filter's absent verdict
// (counted separately so bloom utilization is observable end-to-end).
#[derive(Clone, Copy, PartialEq, Eq)]
enum GranulePruneCause {
    Zone,
    Bloom,
}

fn env_off(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("on"))
}

// Claim-time readahead gate: default ON; PGRUST_CBSTORE_READAHEAD=0/off is
// the global kill switch across every advise path.
fn env_readahead_on() -> bool {
    !matches!(
        std::env::var("PGRUST_CBSTORE_READAHEAD").as_deref(),
        Ok("0") | Ok("off") | Ok("OFF"),
    )
}

// Claim-drive readahead depth: RGs advised AHEAD of a claimed row group
// (own RG always advised when the hook is on). Default 1. "0"/"off"
// disables the claim-drive hook; the global PGRUST_CBSTORE_READAHEAD=0
// kill switch disables every advise. Read at scan construction (same
// contract as env_readahead_on — tests set/unset around construction).
fn env_readahead_claims() -> usize {
    match std::env::var("PGRUST_CBSTORE_READAHEAD_CLAIMS").as_deref() {
        Ok("off") | Ok("OFF") => 0,
        Ok(s) => s.trim().parse::<usize>().map_or(1, |n| n.min(8)),
        Err(_) => 1,
    }
}

// Serial physical-order drive readahead — DEFAULT ON since 2026-07-15:
// Michael ratified flipping the historical arm-A no-prefetch convention
// after the cold-readahead lane's paired serial 100M A/B (cold geomean
// -23.2%, hot flat, outputs byte-identical; notes/cold-readahead-lane.md).
// PGRUST_CBSTORE_READAHEAD_SERIAL=0/off restores the historical
// prefetch-free serial arm for comparisons.
fn env_readahead_serial() -> bool {
    !matches!(
        std::env::var("PGRUST_CBSTORE_READAHEAD_SERIAL").as_deref(),
        Ok("0") | Ok("off") | Ok("OFF"),
    )
}

// v7 stitch identity source: nonzero, process-unique per scan instance.
// Returns 0 (publication disabled, per-epoch consumer behavior) under
// PGRUST_LANE_V2_GLOBALDICT=0/off — the one switch that A/Bs every global
// stitch consumer on the same on-disk bank.
fn next_scan_uid() -> u64 {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    if !*ENABLED.get_or_init(|| {
        !matches!(std::env::var("PGRUST_LANE_V2_GLOBALDICT").as_deref(), Ok("0") | Ok("off"))
    }) {
        return 0;
    }
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

// Staging window width (rows per staged batch), default WINDOW_ROWS.
// PGRUST_CB_WINDOW_ROWS overrides for the batch-granularity measurement
// sweep; accepted values are powers of two in [32, WINDOW_ROWS] that divide
// BLOCK_ROWS (the block-skip arithmetic stays exact). The ceiling is
// WINDOW_ROWS because every staged window deforms into an SoaBatch whose
// capacity is the compile-time SOA_MAX_ROWS (291) — wider windows need the
// deferred wide staging capacity (phase4 design §7 "count-only whole-granule
// batches / Batch{n} > SOA width"). Anything else falls back to the default.
fn env_window_rows() -> usize {
    match std::env::var("PGRUST_CB_WINDOW_ROWS") {
        Ok(v) => match v.parse::<usize>() {
            Ok(n)
                if n.is_power_of_two()
                    && (32..=WINDOW_ROWS).contains(&n)
                    && BLOCK_ROWS % n == 0 =>
            {
                n
            }
            _ => WINDOW_ROWS,
        },
        Err(_) => WINDOW_ROWS,
    }
}

impl<'mcx> CbScanDescData<'mcx> {
    pub fn new(rs_base: TableScanDescData<'mcx>) -> PgResult<CbScanDescData<'mcx>> {
        let rel = &rs_base.rs_rd;
        let coltypes = crate::writer::coltypes_of(rel)?;
        let part = crate::part_cache::cached_part(rel)?;
        Ok(Self::new_with_part(rs_base, part, coltypes))
    }

    /// TEST SUPPORT (dict-tier round-trip): the scan over an explicitly
    /// opened Part + coltypes, bypassing Relation-based part-cache/coltype
    /// resolution. The staging drive (next_window / batch_deform /
    /// staged_dict_lane / store_slot / getnextslot) is byte-identical to a
    /// TAM scan's; `new` is exactly this over `cached_part`/`coltypes_of`.
    #[doc(hidden)]
    pub fn new_with_part(
        rs_base: TableScanDescData<'mcx>,
        part: Option<std::sync::Arc<Part>>,
        coltypes: Vec<ColType>,
    ) -> CbScanDescData<'mcx> {
        let ncols = coltypes.len();
        CbScanDescData {
            rs_base,
            part,
            coltypes,
            needed: vec![true; ncols],
            needed_idx: (0..ncols as u16).collect(),
            slot_inited: std::cell::Cell::new(false),
            zone_quals: Vec::new(),
            cols: (0..ncols).map(|_| new_col_decode()).collect(),
            needed_epoch: 0,
            gather: None,
            rg: 0,
            rg_claimed: false,
            serial_next: 0,
            granule: 0,
            win: 0,
            rg_checked: false,
            decoded: false,
            granule_rows: 0,
            range_end: None,
            direct_handed: false,
            block_mask: !0,
            block_zm_enabled: !env_off("CBSTORE_DISABLE_BLOCK_ZM"),
            bloom_enabled: !env_off("CBSTORE_DISABLE_BLOOM"),
            window_rows: env_window_rows(),
            lazy: false,
            all_ready: (u32::MAX, u32::MAX, u64::MAX),
            rs_temp_snapshot: None,
            staged_lo: 0,
            staged_rows: 0,
            row_cursor: 0,
            adaptive: None,
            cond: None,
            readahead: env_readahead_on(),
            ra_claims: env_readahead_claims(),
            ra_serial: env_readahead_serial(),
            granules_pruned: 0,
            granules_bloom_pruned: 0,
            granules_scanned: 0,
            blocks_pruned: 0,
            windows_staged: 0,
            granules_bound_skipped: 0,
            adaptive_probe_reverts: 0,
            rgs_readahead: 0,
            rgs_claim_readahead: 0,
            granules_meta: 0,
            rg_switches: 0,
            dict_builds: 0,
            scan_uid: next_scan_uid(),
        }
    }

    pub fn set_needed_attrs(&mut self, needed: &[bool]) {
        debug_assert_eq!(needed.len(), self.needed.len());
        self.needed.copy_from_slice(needed);
        self.needed_idx =
            (0..needed.len() as u16).filter(|&c| needed[c as usize]).collect();
        // Mid-scan need-set changes: stale gather decodes and the slot's
        // once-per-scan null-init must both be redone under the new set.
        self.needed_epoch += 1;
        self.slot_inited.set(false);
    }

    pub fn push_zone_quals(&mut self, quals: &[ZoneQual]) {
        self.zone_quals.extend_from_slice(quals);
    }

    pub fn set_lazy_decode(&mut self, on: bool) {
        self.lazy = on;
    }

    // Parallel rescan additionally resets the shared cursor via
    // table_parallelscan_reinitialize (leader-only, before worker relaunch).
    pub fn reset_position(&mut self) {
        self.range_end = None;
        self.rg_claimed = false;
        self.serial_next = 0;
        self.granule = 0;
        self.win = 0;
        self.rg_checked = false;
        self.decoded = false;
        self.staged_rows = 0;
        self.row_cursor = 0;
        if let Some(ad) = self.adaptive.as_deref_mut() {
            ad.cursor = 0;
            ad.bound = None;
            // A probe revert re-sorted the tail physically; restore the full
            // bound order so the rescan probes from scratch.
            if ad.reverted {
                if ad.desc {
                    ad.entries.sort_unstable_by_key(|e| (std::cmp::Reverse(e.bound), e.rg, e.g));
                } else {
                    ad.entries.sort_unstable_by_key(|e| (e.bound, e.rg, e.g));
                }
                ad.reverted = false;
            }
        }
    }

    /// Part-global granule geometry for the runtime morsel source (M1 scan
    /// pipelines): (total granules, row-group start prefix sums — the hard
    /// morsel boundaries, since row group == dictionary epoch). None = empty
    /// table. The starts vector is returned BY VALUE (nrgs+1 u64s) so the
    /// Send+Sync morsel source never carries an Rc-era thread-bound Part (now Arc-shared; kept by value for interface stability).
    pub fn granule_geometry(&self) -> Option<(u64, Vec<u64>)> {
        let part = self.part.as_ref()?;
        Some((part.total_granules(), part.granule_starts().to_vec()))
    }

    /// Drive-scaling observability counters (the runtime WFIN channel):
    /// (rg_switches, dict_builds, granules_scanned, windows_staged).
    pub fn drive_counters(&self) -> (u64, u64, u64, u64) {
        (self.rg_switches, self.dict_builds, self.granules_scanned, self.windows_staged)
    }

    /// Position the scan on the absolute-granule range [g0, g1) — a runtime
    /// morsel claim. The range must lie inside ONE row group (the runtime's
    /// boundary-clamped claims guarantee it: a claim never splits a granule
    /// and never crosses a row-group/dictionary-epoch edge), so the per-RG
    /// visibility gate, dict memos, and zone pruning apply to the whole
    /// claim exactly as the physical-order drive applies them.
    ///
    /// After this call, `next_window`/`getnextslot` serve exactly the
    /// granules of the claim and report exhaustion at `g1` — the caller
    /// re-arms with the next claim. The per-RG check state is carried over
    /// when successive claims land in the same row group (same-verdict
    /// pure predicates; re-checking would only repeat work).
    pub fn set_granule_range(&mut self, g0: u64, g1: u64) -> PgResult<()> {
        debug_assert!(self.adaptive.is_none(), "granule-range drive vs adaptive drive");
        // GL-Q4142 — the tripwire's cbstore leg (heapam::heap_set_block_range
        // is the heap one, verbatim in shape). A scan carrying a SHARED
        // parallel descriptor divides its work through `phs_nallocated`
        // (claim_next_rg); a private granule range abandons that cursor, so
        // every participant would walk the whole part and every partial
        // aggregate would be the GLOBAL answer — a result silently inflated
        // by the participant count, with no error anywhere. Release-effective
        // by construction (an Err, never a debug_assert): the profile the
        // fleet runs is the profile that has to fail closed.
        if self.rs_base.rs_parallel.is_some() {
            return Err(Box::new(PgError::error(
                "cbstore: granule-range positioning on a parallel scan".to_string(),
            )));
        }
        let Some(part) = self.part.as_ref() else {
            return Err(Box::new(PgError::error(
                "cbstore: granule range on an empty part".to_string(),
            )));
        };
        if g0 >= g1 || g1 > part.total_granules() {
            return Err(Box::new(PgError::error(format!(
                "cbstore: invalid granule range [{g0}, {g1}) of {}",
                part.total_granules()
            ))));
        }
        let (rg, g_in_rg) = part.locate_granule(g0);
        let rg_granules =
            (part.rgs[rg].nrows as usize).div_ceil(GRANULE_ROWS);
        let len = (g1 - g0) as usize;
        if g_in_rg + len > rg_granules {
            return Err(Box::new(PgError::error(format!(
                "cbstore: granule range [{g0}, {g1}) crosses a row-group boundary"
            ))));
        }
        // Same-RG carry-over: keep the rg_checked verdict (pure per-RG
        // predicate — same snapshot, same footer, same answer) when
        // successive claims land in the same row group; everything at
        // granule grain resets per claim.
        if !(self.rg_claimed && self.rg == rg) {
            // Claim-drive readahead (cold-readahead lane): the runtime
            // morsel drive's analog of claim_next_rg's parallel-arm advise
            // (this entry point is the pool workers' claim funnel — the
            // runtime crate's own doc names the missing readahead as the
            // M1 gap). Advise own RG + ra_claims ahead on the RG switch
            // only (whole-boundary claims make that once per RG; coalesced
            // multi-epoch claims advance rg per segment and re-advise the
            // remainder ahead). PGRUST_CBSTORE_READAHEAD=0 kills it;
            // PGRUST_CBSTORE_READAHEAD_CLAIMS=off disables just this hook.
            if self.readahead && self.ra_claims > 0 {
                self.advise_claim_window(rg);
            }
            self.rg = rg;
            self.rg_checked = false;
            self.rg_switches += 1;
        }
        self.rg_claimed = true;
        self.granule = g_in_rg;
        self.decoded = false;
        self.win = 0;
        self.staged_rows = 0;
        self.row_cursor = 0;
        self.range_end = Some(g_in_rg + len);
        self.direct_handed = false;
        Ok(())
    }

    /// Direct bounded top-N granule drive (the runtime sort arm's
    /// PGRUST_RUNTIME_TOPN_HEAP feed): hand out the claim's next granule
    /// WHOLE — (nrows, rowref_base) — without arming any window/SoA staging.
    /// Requires a `set_granule_range` claim (the runtime morsel contract);
    /// columns decode lazily per `topn_direct_lane`. The per-RG visibility
    /// gate and prune accounting mirror `next_window`'s ranged loop head
    /// verbatim (an invisible RG ends the claim — its granules belong to no
    /// snapshot-visible row). `None` = claim exhausted. `rowref_base` is
    /// `(rg << 32) | first_row_in_rg`, the winner-gather rowref law.
    pub fn topn_direct_next_granule(&mut self) -> PgResult<Option<(u32, u64)>> {
        debug_assert!(self.adaptive.is_none(), "direct top-N drive vs adaptive drive");
        let (Some(end), true) = (self.range_end, self.rg_claimed) else {
            return Err(Box::new(PgError::error(
                "cbstore: direct top-N granule outside a granule-range claim".to_string(),
            )));
        };
        let Some(part) = self.part.clone() else { return Ok(None) };
        if self.rg >= part.rgs.len() {
            // Beyond the footer horizon: snapshot-invisible (see next_window).
            return Ok(None);
        }
        let rg_rows = part.rgs[self.rg].nrows as usize;
        let ngranules = rg_rows.div_ceil(GRANULE_ROWS);
        if !self.rg_checked {
            // Zone quals are empty for the admitted shape, but keep the
            // whole-RG gate exact (visibility AND zone — next_window
            // verbatim; ranged: charge only the claim's granules).
            if !self.rg_visible(self.rg)? || !self.rg_zone_ok(self.rg) {
                self.granules_pruned += (end - self.granule) as u64;
                self.granule = end;
                return Ok(None);
            }
            self.rg_checked = true;
        }
        if self.direct_handed {
            self.granule += 1;
            self.decoded = false;
            self.direct_handed = false;
        }
        if self.granule >= end.min(ngranules) {
            return Ok(None);
        }
        let grows = (rg_rows - self.granule * GRANULE_ROWS).min(GRANULE_ROWS);
        self.granule_rows = grows;
        self.granules_scanned += 1;
        self.direct_handed = true;
        let base = ((self.rg as u64) << 32) | (self.granule * GRANULE_ROWS) as u64;
        Ok(Some((grows as u32, base)))
    }

    /// The decoded datum lane of column `c` for the granule
    /// `topn_direct_next_granule` just handed out. Decode-on-demand at
    /// granule altitude (`ensure_col`, gkey-memoized — one decode per
    /// (granule, column) however often the lane is re-borrowed). Dict
    /// columns answer `None` (their datums gather through the dict table —
    /// the direct feed's admission excludes them; fail closed here).
    /// pgrcolumnar parts store no NULLs (null rows stay row-store
    /// resident), so the lane carries no null mask by construction.
    pub fn topn_direct_lane(&mut self, c: usize) -> Option<&[Datum]> {
        debug_assert!(self.direct_handed, "lane read without a handed granule");
        if c >= self.cols.len() {
            return None;
        }
        self.ensure_col(c);
        let cd = &self.cols[c];
        if cd.is_dict {
            return None;
        }
        cd.datums.get(..self.granule_rows)
    }

    /// Arm zone-ordered adaptive traversal on column `col` (0-based).
    /// `desc` visits by granule zone max descending (keep-largest
    /// objectives), else by zone min ascending. false = shape refused
    /// (parallel scan, text column, or a chunk without exact zone entries);
    /// the physical-order drive stays untouched.
    pub fn arm_adaptive_order(&mut self, col: usize, desc: bool, strict: bool) -> PgResult<bool> {
        self.adaptive = None;
        if self.rs_base.rs_parallel.is_some() {
            return Ok(false);
        }
        match self.coltypes.get(col) {
            Some(t) if !t.is_text() => {}
            _ => return Ok(false),
        }
        let mut entries = Vec::new();
        if let Some(part) = self.part.clone() {
            for rg in 0..part.rgs.len() {
                // Not counted into granules_pruned here: arming can still
                // refuse (encoding), and the physical drive would then
                // re-count the same prunes.
                if !self.rg_visible(rg)? || !self.rg_zone_ok(rg) {
                    continue;
                }
                let ngranules = (part.rgs[rg].nrows as usize).div_ceil(GRANULE_ROWS);
                let chunk = part.chunk(rg, col);
                if !int_zonemap_encoding(chunk.hdr.encoding) {
                    return Ok(false);
                }
                for g in 0..ngranules {
                    let ge = chunk.granule(g);
                    entries.push(AdaptiveEntry {
                        rg: rg as u32,
                        g: g as u32,
                        bound: if desc { ge.max } else { ge.min },
                    });
                }
            }
        }
        if desc {
            entries.sort_unstable_by_key(|e| (std::cmp::Reverse(e.bound), e.rg, e.g));
        } else {
            entries.sort_unstable_by_key(|e| (e.bound, e.rg, e.g));
        }
        // Probe budgets (rationale on the fields): floors of 64 cover the
        // bound-feed latency of the largest admitted top-N (the consumer
        // caps k at 65536 = 8 full granules) with slack; the fractions keep
        // the worst pre-revert scattered-order share small on big directories
        // while never reverting a walk that stops early (sorted-limit @10M: projection
        // ~34 << max(64, 1238/8)=154).
        let n = entries.len();
        self.adaptive = Some(Box::new(AdaptiveOrder {
            entries,
            cursor: 0,
            col,
            desc,
            strict,
            bound: None,
            nobound_budget: (n / 16).max(64),
            projected_budget: (n / 8).max(64),
            reverted: false,
        }));
        Ok(true)
    }

    /// Consumer bound feedback for an armed adaptive scan (top-k heap floor
    /// or running MIN/MAX best), widened from the key datum exactly as
    /// decode datums are.
    pub fn set_adaptive_bound(&mut self, key: Datum) {
        let Some(ad) = self.adaptive.as_deref_mut() else { return };
        let v = match self.coltypes[ad.col] {
            ColType::I16 => i64::from(key.as_i16()),
            ColType::I32 | ColType::Date => i64::from(key.as_i32()),
            ColType::I64 | ColType::Timestamp => key.as_i64(),
            ColType::Text => return,
        };
        ad.bound = Some(v);
    }

    /// Drop an armed adaptive traversal (the consumer demoted to the
    /// physical-order drive, e.g. a top-k boundary-tie demotion). The caller
    /// must rescan (`reset_position`) before pulling again: the physical
    /// rg/granule cursors were not maintained while the adaptive drive ran.
    pub fn disarm_adaptive_order(&mut self) {
        self.adaptive = None;
    }

    fn claim_next_rg(&mut self) -> usize {
        match self.rs_base.rs_parallel {
            Some(p) => {
                let r = unsafe { p.as_ref() }.phs_nallocated.fetch_add(1, Ordering::SeqCst)
                    as usize;
                // Claim-time readahead (parallelism-redesign §2.8): while
                // this worker computes row group `r`, hint the kernel at the
                // NEXT unclaimed row group's chunk extents (needed columns
                // only) so its pages stream in behind the compute. PARALLEL
                // ARM ONLY — the serial arm below never advises (arm A
                // serial-vs-CH-mt1 stays prefetch-free, structurally).
                if self.readahead {
                    self.advise_rg(r + 1);
                }
                r
            }
            None => {
                let r = self.serial_next;
                self.serial_next += 1;
                // Serial readahead — DEFAULT ON (ra_serial; ratified
                // 2026-07-15, superseding the historical arm-A structural
                // prohibition). Counts into rgs_claim_readahead, never
                // rgs_readahead — the legacy counter stays a parallel-
                // Gather-arm channel. PGRUST_CBSTORE_READAHEAD_SERIAL=0
                // restores the historical prefetch-free serial arm.
                if self.readahead && self.ra_serial {
                    self.advise_claim_window(r);
                }
                r
            }
        }
    }

    // madvise(WILLNEED) row group `rg`'s needed-column chunk extents.
    // Advisory only: no scan state beyond the counter changes, no mapped
    // byte is touched (extents come from footer metadata), and RGs the scan
    // would prune wholesale (zone-refused) are skipped rather than fetched.
    fn advise_rg(&mut self, rg: usize) {
        if self.advise_rg_extents(rg) {
            self.rgs_readahead += 1;
        }
    }

    // Claim-drive / serial-drive variant (cold-readahead lane): identical
    // advise, separate counter — the legacy serial guard (readahead_scope
    // tests) keeps its exact `rgs_readahead == 0` invariant.
    fn advise_rg_claim(&mut self, rg: usize) {
        if self.advise_rg_extents(rg) {
            self.rgs_claim_readahead += 1;
        }
    }

    fn advise_rg_extents(&mut self, rg: usize) -> bool {
        let Some(part) = self.part.as_ref() else { return false };
        if rg >= part.rgs.len() || self.needed_idx.is_empty() || !self.rg_zone_ok(rg) {
            return false;
        }
        part.advise_willneed(rg, &self.needed_idx)
    }

    // Claim-schedule-driven readahead (cold-readahead lane): entering row
    // group `rg` through a claim, advise its own needed-column extents
    // (the kernel streams the RG body in behind the granule-by-granule
    // decode — the cold first-touch overlap) plus `ra_claims` RGs ahead
    // (cross-RG overlap at the claim tail). Zone-refused RGs are skipped
    // by advise_rg_extents; out-of-range successors fall out of bounds.
    fn advise_claim_window(&mut self, rg: usize) {
        for r in rg..=rg.saturating_add(self.ra_claims) {
            self.advise_rg_claim(r);
        }
    }

    pub fn total_visible_rows(&self) -> u64 {
        self.part.as_ref().map_or(0, |p| p.total_rows())
    }

    /// ANALYZE row source: visible row groups with row counts, file order.
    pub fn analyze_visible_rgs(&self) -> PgResult<Vec<(u32, u32)>> {
        let Some(part) = self.part.as_ref() else { return Ok(Vec::new()) };
        let mut rgs = Vec::with_capacity(part.rgs.len());
        for rg in 0..part.rgs.len() {
            if self.rg_visible(rg)? {
                rgs.push((rg as u32, part.rgs[rg].nrows));
            }
        }
        Ok(rgs)
    }

    fn rg_visible(&self, rg: usize) -> PgResult<bool> {
        let part = self.part.as_ref().unwrap();
        let m = &part.rgs[rg];
        if m.flags & RG_FLAG_FROZEN != 0 {
            return Ok(true);
        }
        let xmin = m.xmin;
        if xact_seams::transaction_id_is_current_transaction_id::call(xmin) {
            return Ok(true);
        }
        if let Some(snap) = &self.rs_base.rs_snapshot {
            if snapmgr::XidInMVCCSnapshot(xmin, snap)? {
                return Ok(false);
            }
        }
        transam_seams::transaction_id_did_commit::call(xmin)
    }

    // Wholly-visible under the snapshot: every row of the RG is visible and
    // the footer nrows can stand in for a scan of it. Own-transaction xmins
    // demote to the scan gate (cid semantics stay rg_visible's), so this is
    // deliberately a subset of rg_visible-true.
    fn rg_wholly_visible(&self, rg: usize) -> PgResult<bool> {
        let m = &self.part.as_ref().unwrap().rgs[rg];
        if m.flags & RG_FLAG_FROZEN != 0 {
            return Ok(true);
        }
        let xmin = m.xmin;
        if xact_seams::transaction_id_is_current_transaction_id::call(xmin) {
            return Ok(false);
        }
        if let Some(snap) = &self.rs_base.rs_snapshot {
            if snapmgr::XidInMVCCSnapshot(xmin, snap)? {
                return Ok(false);
            }
        }
        transam_seams::transaction_id_did_commit::call(xmin)
    }

    /// COUNT(*) metadata drive: one claimed row group per call; 0 = horizon.
    /// A wholly-visible RG answers from its footer row count; any other RG
    /// demotes (fail-open) to the scan drive's per-granule gate and is
    /// counted exactly as next_window would stage it.
    pub fn next_meta_count(&mut self) -> PgResult<u32> {
        let Some(part) = self.part.as_ref() else { return Ok(0) };
        let nrgs = part.rgs.len();
        loop {
            let rg = self.claim_next_rg();
            if rg >= nrgs {
                return Ok(0);
            }
            let part = self.part.as_ref().unwrap();
            let rg_rows = part.rgs[rg].nrows;
            let ngranules = (rg_rows as usize).div_ceil(GRANULE_ROWS);
            if self.rg_wholly_visible(rg)? {
                return Ok(rg_rows);
            }
            if !self.rg_visible(rg)? || !self.rg_zone_ok(rg) {
                self.granules_pruned += ngranules as u64;
                continue;
            }
            let mut n = 0u32;
            for g in 0..ngranules {
                if !self.granule_zone_ok(rg, g) {
                    self.granules_pruned += 1;
                    continue;
                }
                self.granules_scanned += 1;
                self.windows_staged += 1;
                n += (rg_rows as usize - g * GRANULE_ROWS).min(GRANULE_ROWS) as u32;
            }
            if n > 0 {
                return Ok(n);
            }
        }
    }

    /// Metadata MIN/MAX/COUNT/SUM scan: exact per-column (min, max) and i128
    /// sums over every visible row plus the visible row count, from footer
    /// row counts, zone maps, and footer sums (exact for int-family columns;
    /// text zone entries carry byte lengths — refused). None = not
    /// answerable here; the scan drive owns the query. Wholly-visible RGs
    /// fold RG-level footer entries; any other RG takes the scan gate and
    /// folds per-granule entries (fail-open per RG) — sums for such RGs, and
    /// for RGs preserved from v<=3 footers (no RG_FLAG_SUMS), decode each
    /// granule and reconcile exactly. Serial one-shot: consumes no scan
    /// position.
    pub fn meta_agg_scan(
        &self,
        cols: &[u16],
        sum_cols: &[u16],
        zq: Option<MetaZeroQual>,
    ) -> PgResult<Option<MetaAggScan>> {
        if self.rs_base.rs_parallel.is_some() {
            return Ok(None);
        }
        for &c in cols.iter().chain(sum_cols) {
            match self.coltypes.get(c as usize) {
                Some(t) if !t.is_text() => {}
                _ => return Ok(None),
            }
        }
        let mut out = MetaAggScan {
            rows: 0,
            minmax: cols.iter().map(|&c| (c, i64::MAX, i64::MIN)).collect(),
            sums: sum_cols.iter().map(|&c| (c, 0i128)).collect(),
        };
        let Some(part) = self.part.as_ref() else { return Ok(Some(out)) };
        // With a zero-count qual the cbstore zone quals are exactly that
        // conjunct (advisory); the bare arm still requires none.
        debug_assert!(zq.is_some() || self.zone_quals.is_empty());
        // Per-granule visible-row count under the qual: grows - zeros for
        // `<> 0`, zeros for `= 0`. Sums fold unchanged for `<> 0` (excluded
        // zero rows contribute exactly 0 to S) and are identically 0 for
        // `= 0` (fold_sums gates below). minmax stays the all-visible-rows
        // fold — a superset of the qual rows' range, so the admission
        // site's overflow-guard re-proof stays conservative-safe.
        let gran_rows = |rg: usize, g: usize, grows: usize| -> u64 {
            match zq {
                None => grows as u64,
                Some(z) => {
                    let zc = part.granule_zerocnt(rg, g, z.col as usize) as u64;
                    if z.keep_nonzero { grows as u64 - zc } else { zc }
                }
            }
        };
        let fold_sums = zq.map_or(true, |z| z.keep_nonzero);
        let mut scratch = (Vec::new(), Vec::new(), Vec::new());
        for rg in 0..part.rgs.len() {
            let rg_rows = part.rgs[rg].nrows;
            let ngranules = (rg_rows as usize).div_ceil(GRANULE_ROWS);
            if self.rg_wholly_visible(rg)? {
                // Feature detection: a qual answer needs exact zero counts
                // on EVERY visible RG (v<=6-preserved RGs lack them) — the
                // scan drive owns the query otherwise, byte-identically.
                if zq.is_some() && !part.rg_has_zerocnt(rg) {
                    return Ok(None);
                }
                for g in 0..ngranules {
                    let grows = (rg_rows as usize - g * GRANULE_ROWS).min(GRANULE_ROWS);
                    out.rows += gran_rows(rg, g, grows);
                }
                for e in out.minmax.iter_mut() {
                    let (_, min, max) = part.rgs[rg].chunks[e.0 as usize];
                    e.1 = e.1.min(min);
                    e.2 = e.2.max(max);
                }
                if !fold_sums {
                } else if part.rgs[rg].flags & RG_FLAG_SUMS != 0 {
                    for e in out.sums.iter_mut() {
                        e.1 += part.rg_sum(rg, e.0 as usize);
                    }
                } else {
                    for g in 0..ngranules {
                        sum_granule(part, rg, g, &mut out.sums, &mut scratch);
                    }
                }
                continue;
            }
            if !self.rg_visible(rg)? || !self.rg_zone_ok(rg) {
                continue;
            }
            if zq.is_some() && !part.rg_has_zerocnt(rg) {
                return Ok(None);
            }
            for g in 0..ngranules {
                if !self.granule_zone_ok(rg, g) {
                    // Zone-pruned granules are provably all-false under the
                    // (single) qual the zone quals mirror: they contribute
                    // no rows and no sum; skipping them shrinks minmax
                    // toward the qual rows' range — still guard-safe.
                    continue;
                }
                let grows = (rg_rows as usize - g * GRANULE_ROWS).min(GRANULE_ROWS);
                out.rows += gran_rows(rg, g, grows);
                for e in out.minmax.iter_mut() {
                    let ge = part.chunk(rg, e.0 as usize).granule(g);
                    e.1 = e.1.min(ge.min);
                    e.2 = e.2.max(ge.max);
                }
                if fold_sums {
                    sum_granule(part, rg, g, &mut out.sums, &mut scratch);
                }
            }
        }
        Ok(Some(out))
    }

    // Zone-only per-granule gate for the metadata arms (they engage only
    // with no quals, so this never diverges from granule_admit's stronger
    // bloom/block pruning on the scan drive).
    fn granule_zone_ok(&self, rg: usize, g: usize) -> bool {
        let part = self.part.as_ref().unwrap();
        self.zone_quals.iter().all(|q| {
            let ge = part.chunk(rg, (q.attnum - 1) as usize).granule(g);
            zone_can_match(q, ge.min, ge.max)
        })
    }

    fn rg_zone_ok(&self, rg: usize) -> bool {
        let part = self.part.as_ref().unwrap();
        self.zone_quals.iter().all(|q| {
            let (_, min, max) = part.rgs[rg].chunks[(q.attnum - 1) as usize];
            zone_can_match(q, min, max)
        })
    }

    // Err(cause) = pruned (zone map, block zone maps, or bloom say no row
    // can match; cause attributes bloom rejections for the utilization
    // counter). Ok(mask) = admitted; bit b covers rows [b*BLOCK_ROWS,
    // (b+1)*BLOCK_ROWS) of the granule. Bloom and block pruning are
    // advisory-only: admitted rows always get the ordinary qual evaluation.
    fn granule_admit(
        &self,
        rg: usize,
        g: usize,
        granule_rows: usize,
    ) -> Result<u32, GranulePruneCause> {
        let part = self.part.as_ref().unwrap();
        let nblocks = granule_rows.div_ceil(BLOCK_ROWS);
        let mut mask: u32 = (1u32 << nblocks) - 1;
        for q in &self.zone_quals {
            let chunk = part.chunk(rg, (q.attnum - 1) as usize);
            let ge = chunk.granule(g);
            if !zone_can_match(q, ge.min, ge.max) {
                return Err(GranulePruneCause::Zone);
            }
            if matches!(q.op, ZoneCmp::Eq)
                && self.bloom_enabled
                && chunk.has_bloom()
                && !chunk.bloom_may_contain(g, q.val)
            {
                return Err(GranulePruneCause::Bloom);
            }
            if self.block_zm_enabled && chunk.has_block_zm() {
                for b in 0..nblocks {
                    if mask & (1 << b) != 0 {
                        let (bmin, bmax) = chunk.block_minmax(g, b);
                        if !zone_can_match(q, bmin, bmax) {
                            mask &= !(1 << b);
                        }
                    }
                }
                if mask == 0 {
                    return Err(GranulePruneCause::Zone);
                }
            }
        }
        Ok(mask)
    }

    /// Compressed-domain constant-fold of `q` against the currently staged
    /// granule's decoded [min,max] (int/date/timestamp only; the granule
    /// entries carry exact decoded extremes for FOR/CONST/RAW ints). The
    /// staged prewhere drive skips a clause's decode+eval on AllPass and
    /// short-circuits the window on AllFail. Non-erroring by construction:
    /// pure integer compares over footer metadata, no data touched.
    pub fn staged_granule_verdict(&self, q: &ZoneQual) -> ZoneVerdict {
        let Some(part) = self.part.as_ref() else { return ZoneVerdict::Mixed };
        let ge = part.chunk(self.rg, (q.attnum - 1) as usize).granule(self.granule);
        zone_verdict(q, ge.min, ge.max)
    }

    /// Arm the condition cache for this scan (pgrust.condition_cache): `fp`
    /// = the staged prefix's canonical fingerprint (laneexec), `capacity` =
    /// the byte budget GUC. false = no part (empty relation) — nothing to
    /// cache. Parallel workers arm independently and share entries through
    /// the global cache (identical plan => identical fingerprint).
    pub fn condcache_arm(&mut self, fp: u128, capacity: u64) -> bool {
        if self.part.is_none() {
            return false;
        }
        crate::condcache::set_capacity(capacity);
        self.cond = Some(Box::new(CondState {
            fp,
            cur_rg: u32::MAX,
            entry: None,
            stats: Default::default(),
        }));
        true
    }

    // The currently staged window's cache coordinates: (RgEntry, slot).
    // None = unarmed, nothing staged, or a non-canonical window (the
    // count-only whole-granule staging; lane quals never produce it, belt
    // anyway). Fetches the RG entry once per row group.
    fn cond_slot(&mut self) -> Option<(&crate::condcache::RgEntry, u32)> {
        if self.cond.is_none() {
            return None;
        }
        if !(self.rg_claimed && self.decoded && self.staged_rows > 0) {
            return None;
        }
        if self.staged_rows > self.window_rows || self.staged_lo % self.window_rows != 0 {
            return None;
        }
        let part = self.part.as_ref()?;
        let rg = self.rg as u32;
        let window_rows = self.window_rows as u32;
        let cond = self.cond.as_deref_mut()?;
        if cond.cur_rg != rg || cond.entry.is_none() {
            cond.entry = Some(crate::condcache::get_or_insert(
                part.identity,
                cond.fp,
                rg,
                window_rows,
                part.rgs[rg as usize].nrows,
            ));
            cond.cur_rg = rg;
        }
        let slot = ((self.granule * GRANULE_ROWS + self.staged_lo) / self.window_rows) as u32;
        Some((cond.entry.as_deref().expect("entry set above"), slot))
    }

    /// Condition-cache lookup for the CURRENT staged window: on a hit the
    /// staged prefix's survivor bits are written into `sel` (whole words;
    /// live width = staged_rows) and the caller skips the qual's decode+eval
    /// legs entirely. false = miss (or unarmed): evaluate as always, then
    /// `condcache_store` the bits.
    pub fn condcache_lookup(&mut self, sel: &mut [u64]) -> bool {
        let hit = match self.cond_slot() {
            Some((entry, slot)) => entry.lookup(slot, sel),
            None => return false,
        };
        // Count in the per-scan cell (cond is Some — cond_slot said so);
        // the shared statics see one fold at scan teardown, not one
        // fetch_add per window (the selective-qual-grouped/plain-agg contention line, census U7).
        if let Some(cond) = self.cond.as_deref_mut() {
            cond.stats.count(hit);
        }
        hit
    }

    /// Fold this scan's condition-cache stat cells into the process
    /// counters (idempotent; CondState's drop folds too — this exists so a
    /// stats read at scan shutdown sees the scan's own counts).
    pub fn condcache_fold_stats(&mut self) {
        if let Some(cond) = self.cond.as_deref_mut() {
            cond.stats.fold();
        }
    }

    /// Record the CURRENT staged window's freshly evaluated survivor bits.
    /// `sel`'s live width is staged_rows (bits past it are zero by the
    /// driver's bitmap-init contract).
    pub fn condcache_store(&mut self, sel: &[u64]) {
        let nwords = self.staged_rows.div_ceil(64);
        if let Some((entry, slot)) = self.cond_slot() {
            entry.store(slot, sel, nwords);
        }
    }

    fn decode_current_granule(&mut self) {
        let part = self.part.as_ref().unwrap();
        let rg = self.rg;
        let g = self.granule;
        let nrows = part.rgs[rg].nrows as usize;
        self.granule_rows = (nrows - g * GRANULE_ROWS).min(GRANULE_ROWS);
        if !self.lazy {
            let mut built = 0u64;
            for (c, cd) in self.cols.iter_mut().enumerate() {
                if !self.needed[c] {
                    continue;
                }
                built += decode_col(part, rg, g, c, cd) as u64;
            }
            self.dict_builds += built;
            self.all_ready = (rg as u32, g as u32, self.needed_epoch);
        }
        self.decoded = true;
    }

    /// Complete the needed set's decode for the current granule (post-qual
    /// materialization of a surviving row).
    #[inline]
    fn ensure_needed_cols(&mut self) {
        let key = (self.rg as u32, self.granule as u32, self.needed_epoch);
        if self.all_ready == key {
            return;
        }
        let part = self.part.as_ref().unwrap();
        let mut built = 0u64;
        for &c in &self.needed_idx {
            built +=
                decode_col(part, self.rg, self.granule, c as usize, &mut self.cols[c as usize])
                    as u64;
        }
        self.dict_builds += built;
        self.all_ready = key;
    }

    #[inline]
    fn ensure_col(&mut self, c: usize) {
        let part = self.part.as_ref().unwrap();
        let built = decode_col(part, self.rg, self.granule, c, &mut self.cols[c]);
        self.dict_builds += built as u64;
    }

    /// Stage the next surviving <=WINDOW_ROWS window; 0 = scan exhausted.
    pub fn next_window(&mut self) -> PgResult<u32> {
        if self.adaptive.is_some() {
            return self.next_window_adaptive();
        }
        let Some(part) = self.part.as_ref() else { return Ok(0) };
        let nrgs = part.rgs.len();
        loop {
            if !self.rg_claimed {
                // Granule-range drive: the range IS the claim — its end (or
                // a whole-claim prune below) is exhaustion, never a new RG.
                if self.range_end.is_some() {
                    return Ok(0);
                }
                self.rg = self.claim_next_rg();
                self.rg_claimed = true;
                self.granule = 0;
                self.win = 0;
                self.rg_checked = false;
                self.decoded = false;
                self.rg_switches += 1;
            }
            // A claimed index beyond this scan's footer horizon is safe to
            // drop: footer publish is ordered before COPY's commit, so every
            // snapshot-visible RG is inside every participant's footer — a
            // horizon mismatch can only cover snapshot-invisible RGs.
            if self.rg >= nrgs {
                return Ok(0);
            }
            let rg_rows = self.part.as_ref().unwrap().rgs[self.rg].nrows as usize;
            let ngranules = rg_rows.div_ceil(GRANULE_ROWS);
            if !self.rg_checked {
                if !self.rg_visible(self.rg)? || !self.rg_zone_ok(self.rg) {
                    // Ranged: charge only the claim's granules (the rest of
                    // the RG belongs to other claims, each pruned on its own
                    // set_granule_range re-entry — rg_checked resets there).
                    self.granules_pruned += match self.range_end {
                        Some(end) => (end - self.granule) as u64,
                        None => ngranules as u64,
                    };
                    self.rg_claimed = false;
                    continue;
                }
                self.rg_checked = true;
            }
            if self.granule >= self.range_end.unwrap_or(usize::MAX).min(ngranules) {
                // Ranged exhaustion keeps the RG claim so a contiguous next
                // claim in the same row group carries the rg_checked verdict.
                if self.range_end.is_some() {
                    return Ok(0);
                }
                self.rg_claimed = false;
                continue;
            }
            if !self.decoded {
                let grows = (rg_rows - self.granule * GRANULE_ROWS).min(GRANULE_ROWS);
                let mask = match self.granule_admit(self.rg, self.granule, grows) {
                    Ok(mask) => mask,
                    Err(cause) => {
                        self.granules_pruned += 1;
                        if cause == GranulePruneCause::Bloom {
                            self.granules_bloom_pruned += 1;
                        }
                        self.granule += 1;
                        continue;
                    }
                };
                self.block_mask = mask;
                self.decode_current_granule();
                self.granules_scanned += 1;
                self.win = 0;
            }
            let lo = self.win * self.window_rows;
            if lo >= self.granule_rows {
                self.granule += 1;
                self.decoded = false;
                continue;
            }
            if self.block_mask & (1 << (lo / BLOCK_ROWS)) == 0 {
                self.blocks_pruned += 1;
                self.win = (lo / BLOCK_ROWS + 1) * (BLOCK_ROWS / self.window_rows);
                continue;
            }
            // Count-only scans (no needed columns => no SoA batch can be
            // armed): stage the whole granule as one batch. Requires a full
            // block mask (needed_idx empty implies no quals, but stay exact).
            self.windows_staged += 1;
            if self.needed_idx.is_empty()
                && self.block_mask.count_ones() as usize >= self.granule_rows.div_ceil(BLOCK_ROWS)
            {
                self.staged_lo = 0;
                self.staged_rows = self.granule_rows;
                self.row_cursor = 0;
                self.win = GRANULE_ROWS / self.window_rows;
                return Ok(self.staged_rows as u32);
            }
            self.staged_lo = lo;
            self.staged_rows = (self.granule_rows - lo).min(self.window_rows);
            self.row_cursor = 0;
            self.win += 1;
            return Ok(self.staged_rows as u32);
        }
    }

    /// v7 granule length-stats metadata peek (the sorted-fold granule meta
    /// arm): when the NEXT `next_window` call would decode a fresh granule,
    /// describe that granule from footer metadata alone — row count, exact
    /// zone (min, max) per requested key column, and (sum(octet_length),
    /// non-null, empty) per requested text column. The caller either consumes
    /// it (`granule_meta_consume` — the granule is never decoded) or declines
    /// (state untouched beyond RG claiming, which `next_window`'s own loop
    /// head performs identically — the staged row stream is unchanged).
    ///
    /// NotMeta whenever any gate fails: parallel/adaptive scans, zone quals
    /// (their pruning must keep next_window's exact skip accounting),
    /// mid-granule position, an RG without RG_FLAG_LENSTATS or not wholly
    /// visible, a key chunk without exact zone entries, or a column without
    /// stats. Exhausted mirrors next_window's 0.
    pub fn granule_meta_peek(
        &mut self,
        key_cols: &[u16],
        len_cols: &[u16],
        key_mm: &mut [(i64, i64)],
        len_stats: &mut [(u64, u32, u32)],
    ) -> PgResult<CbGranuleMetaStep> {
        debug_assert_eq!(key_cols.len(), key_mm.len());
        debug_assert_eq!(len_cols.len(), len_stats.len());
        if self.adaptive.is_some()
            || self.rs_base.rs_parallel.is_some()
            || !self.zone_quals.is_empty()
        {
            return Ok(CbGranuleMetaStep::NotMeta);
        }
        let Some(part) = self.part.clone() else { return Ok(CbGranuleMetaStep::Exhausted) };
        if len_cols.iter().any(|&c| !part.has_len_stats(c as usize)) {
            return Ok(CbGranuleMetaStep::NotMeta);
        }
        let nrgs = part.rgs.len();
        loop {
            if !self.rg_claimed {
                // Granule-range drive (a runtime morsel claim): the range IS
                // the claim — never walk past it or claim another RG (the
                // serial_next cursor belongs to the whole-scan drive).
                // Mirrors next_window's ranged loop head exactly.
                if self.range_end.is_some() {
                    return Ok(CbGranuleMetaStep::Exhausted);
                }
                self.rg = self.claim_next_rg();
                self.rg_claimed = true;
                self.granule = 0;
                self.win = 0;
                self.rg_checked = false;
                self.decoded = false;
                self.rg_switches += 1;
            }
            if self.rg >= nrgs {
                return Ok(CbGranuleMetaStep::Exhausted);
            }
            let rg_rows = part.rgs[self.rg].nrows as usize;
            let ngranules = rg_rows.div_ceil(GRANULE_ROWS);
            if !self.rg_checked {
                // Zone quals are empty here; the visibility gate and prune
                // accounting are next_window's verbatim (ranged: charge only
                // the claim's granules — the rest of the RG belongs to other
                // claims).
                if !self.rg_visible(self.rg)? {
                    self.granules_pruned += match self.range_end {
                        Some(end) => (end - self.granule) as u64,
                        None => ngranules as u64,
                    };
                    self.rg_claimed = false;
                    continue;
                }
                self.rg_checked = true;
            }
            if self.granule >= self.range_end.unwrap_or(usize::MAX).min(ngranules) {
                // Ranged exhaustion keeps the RG claim (same-RG carry-over,
                // next_window's own contract).
                if self.range_end.is_some() {
                    return Ok(CbGranuleMetaStep::Exhausted);
                }
                self.rg_claimed = false;
                continue;
            }
            if self.decoded {
                if self.win * self.window_rows < self.granule_rows {
                    // Mid-granule: staged windows pending.
                    return Ok(CbGranuleMetaStep::NotMeta);
                }
                // Decoded granule fully consumed: advance exactly as
                // next_window's own loop head would.
                self.granule += 1;
                self.decoded = false;
                continue;
            }
            // Fresh granule. Metadata answer requires exact per-granule
            // stats and a wholly-visible RG (footer counts stand in for the
            // rows; own-transaction RGs demote — the next_meta_count
            // precedent).
            let m = &part.rgs[self.rg];
            if m.flags & RG_FLAG_LENSTATS == 0 || !self.rg_wholly_visible(self.rg)? {
                return Ok(CbGranuleMetaStep::NotMeta);
            }
            let grows = (rg_rows - self.granule * GRANULE_ROWS).min(GRANULE_ROWS);
            for (k, &c) in key_cols.iter().enumerate() {
                let chunk = part.chunk(self.rg, c as usize);
                // Exact decoded-value zone entries only (the
                // staged_window_value_minmax gate).
                match chunk.hdr.encoding {
                    Encoding::Raw | Encoding::For | Encoding::Const => {}
                    _ => return Ok(CbGranuleMetaStep::NotMeta),
                }
                let ge = chunk.granule(self.granule);
                key_mm[k] = (ge.min, ge.max);
            }
            for (k, &c) in len_cols.iter().enumerate() {
                let Some(st) = part.granule_len_stats(self.rg, self.granule, c as usize)
                else {
                    return Ok(CbGranuleMetaStep::NotMeta);
                };
                // pgrcolumnar stores no NULLs; a mismatch means foreign/corrupt
                // stats — refuse rather than answer.
                if st.1 != grows as u32 {
                    return Ok(CbGranuleMetaStep::NotMeta);
                }
                len_stats[k] = st;
            }
            return Ok(CbGranuleMetaStep::Meta { rows: grows as u32 });
        }
    }

    /// Consume the granule `granule_meta_peek` just answered: advance past it
    /// without decoding. Only legal immediately after a `Meta` verdict.
    pub fn granule_meta_consume(&mut self) {
        debug_assert!(self.rg_claimed && !self.decoded);
        self.granules_meta += 1;
        self.granule += 1;
        self.win = 0;
    }

    /// GCUT zone summary for the runtime parallel top-N (night/sort-merge-
    /// redesign inc-2). Returns, over the WHOLE part in absolute granule
    /// order (the morsel-range granule space):
    ///   * per-granule BEST direction-folded order word of key column `col`
    ///     (`key_order_word(asc ? min : max)` — the best any row of that
    ///     granule could contribute), and
    ///   * the zone-max SEED word: the smallest folded WORST word `W` such
    ///     that wholly-visible exact-zone granules with worst word <= `W`
    ///     together hold >= `bound` rows — so the global k-th order word is
    ///     provably <= `W` and any entry with a strictly greater word is
    ///     out of the top-k before a single row is read.
    ///
    /// Correctness posture:
    ///   * BEST words bound STORED values — a superset of every snapshot's
    ///     visible rows (deletes only shrink, appends are invisible-or-
    ///     covered) — so a granule whose best word exceeds a proven cutoff
    ///     cannot contribute regardless of visibility. Granules without
    ///     exact decoded-value zone entries (encodings other than
    ///     Raw/For/Const — the `granule_meta_peek` gate) get best word 0:
    ///     never skippable, never wrong.
    ///   * SEED rows count only WHOLLY-VISIBLE RGs with exact zones (the
    ///     `granule_meta_peek` visibility law — invisible rows must not
    ///     stand in for the k rows the bound needs); `None` when the
    ///     eligible rows never reach `bound`.
    ///   * pgrcolumnar stores no NULLs (the `gather_row` law), so zone
    ///     words describe every stored row; the caller folds the null
    ///     tier itself.
    /// `None` = no columnar part (nothing to summarize).
    pub fn zone_topk_words(
        &self,
        col: u16,
        desc: bool,
        bound: u64,
    ) -> PgResult<Option<(Vec<u64>, Option<u64>)>> {
        let Some(part) = self.part.clone() else { return Ok(None) };
        let fold = |v: i64| -> u64 {
            let asc = (v as u64) ^ (1 << 63);
            if desc {
                !asc
            } else {
                asc
            }
        };
        let mut best: Vec<u64> = Vec::new();
        let mut seedable: Vec<(u64, u32)> = Vec::new();
        for rg in 0..part.rgs.len() {
            let rg_rows = part.rgs[rg].nrows as usize;
            let ngranules = rg_rows.div_ceil(GRANULE_ROWS);
            let chunk = part.chunk(rg, col as usize);
            // Value-exact zone encodings for INT columns: Raw/For/Const (the
            // granule_meta_peek set) + DeltaFor, whose format doc pins "zone
            // maps ... computed from the plain values exactly as For/Raw —
            // value-domain metadata is untouched by the payload transform"
            // (format.rs Encoding::DeltaFor). Dict/text encodings carry
            // code/length-domain entries — never valid here (the caller
            // admits int-family keys only; this is the belt).
            let exact = matches!(
                chunk.hdr.encoding,
                Encoding::Raw | Encoding::For | Encoding::Const | Encoding::DeltaFor
            );
            let vis = exact && self.rg_wholly_visible(rg)?;
            for g in 0..ngranules {
                if !exact {
                    best.push(0);
                    continue;
                }
                let ge = chunk.granule(g);
                let (b, w) = if desc { (ge.max, ge.min) } else { (ge.min, ge.max) };
                best.push(fold(b));
                if vis {
                    let grows = (rg_rows - g * GRANULE_ROWS).min(GRANULE_ROWS) as u32;
                    seedable.push((fold(w), grows));
                }
            }
        }
        seedable.sort_unstable_by_key(|&(w, _)| w);
        let mut acc = 0u64;
        let mut seed = None;
        for (w, rows) in seedable {
            acc += rows as u64;
            if acc >= bound {
                seed = Some(w);
                break;
            }
        }
        Ok(Some((best, seed)))
    }

    /// RG-altitude meta-answerability census over the PUSHED zone quals
    /// (the GL-SERIALTERM-META qual-zone helper): (allpass_rgs, total_rgs)
    /// where an RG counts iff it is wholly visible, every pushed zone qual
    /// folds AllPass over its footer extremes, and (when `need_sums`) it
    /// carries the v4 footer sums — EXACTLY `agg_meta_peek`'s whole-RG-tier
    /// precondition, evaluated for the whole part in one footer walk
    /// (O(RGs x quals); ~24B footer peeks, no payload). ECONOMICS SIGNAL
    /// ONLY: callers use the fraction to predict the serial fold-meta
    /// arm's flat wall; the serial arm re-proves every unit itself, so a
    /// misprediction can only route, never corrupt. An empty pushed set
    /// counts every visible RG (the no-qual meta posture). `None` = no
    /// columnar part.
    pub fn zone_meta_rg_census(&self, need_sums: bool) -> PgResult<Option<(u64, u64)>> {
        let Some(part) = self.part.clone() else { return Ok(None) };
        let mut allpass = 0u64;
        let mut total = 0u64;
        for rg in 0..part.rgs.len() {
            total += 1;
            if need_sums && part.rgs[rg].flags & RG_FLAG_SUMS == 0 {
                continue;
            }
            if !self.rg_wholly_visible(rg)? {
                continue;
            }
            let ok = self.zone_quals.iter().all(|q| {
                let (_, min, max) = part.rgs[rg].chunks[(q.attnum - 1) as usize];
                zone_verdict(q, min, max) == ZoneVerdict::AllPass
            });
            if ok {
                allpass += 1;
            }
        }
        Ok(Some((allpass, total)))
    }

    /// Footer-stat aggregate metadata peek (the plain fold drive's meta
    /// arm): when the NEXT `next_window` call would decode a fresh scan
    /// unit, describe that unit from footer metadata alone — IF every
    /// pushed zone qual is AllPass over the unit's footer extremes (all
    /// rows provably pass; the CALLER must separately prove the pushed
    /// zone quals mirror the ENTIRE scan qual) and the unit is wholly
    /// visible. Two tiers:
    ///   * whole RG (fresh-RG boundary): rows + exact per-column (min,
    ///     max) from the RG footer chunks, exact i128 sums from the v4
    ///     footer sums (RG_FLAG_SUMS), Σ octet_length folded over the v7
    ///     granule length stats (RG_FLAG_LENSTATS);
    ///   * granule: rows + (min, max) from the granule zone entry and
    ///     length stats from its v7 entry — only when `sum_cols` is empty
    ///     (the format stores no granule-altitude value sums; the v9 spec
    ///     owns that).
    /// The caller either consumes the unit (`agg_meta_consume_rg` /
    /// `agg_meta_consume_granule` — never decoded) or declines (state
    /// untouched beyond RG claiming, which `next_window`'s own loop head
    /// performs identically, prune accounting included).
    ///
    /// NotMeta whenever any gate fails: parallel/adaptive/granule-ranged
    /// scans, mid-granule position, an RG not wholly visible or missing a
    /// required stats flag, a text column requested for (min, max)/sums
    /// (text zone entries carry byte lengths), a length column without v7
    /// stats, or a non-AllPass zone verdict. Exhausted mirrors
    /// next_window's 0.
    #[allow(clippy::too_many_arguments)]
    pub fn agg_meta_peek(
        &mut self,
        mm_cols: &[u16],
        sum_cols: &[u16],
        len_cols: &[u16],
        mm: &mut [(i64, i64)],
        sums: &mut [i128],
        lens: &mut [i64],
    ) -> PgResult<CbAggMetaStep> {
        debug_assert_eq!(mm_cols.len(), mm.len());
        debug_assert_eq!(sum_cols.len(), sums.len());
        debug_assert_eq!(len_cols.len(), lens.len());
        if self.adaptive.is_some()
            || self.rs_base.rs_parallel.is_some()
            || self.range_end.is_some()
        {
            return Ok(CbAggMetaStep::NotMeta);
        }
        let Some(part) = self.part.clone() else { return Ok(CbAggMetaStep::Exhausted) };
        // (min, max) and value sums are int-family only (text zone entries
        // carry byte lengths, text has no footer value sums); length sums
        // need the column flagged in the v7 prelude.
        for &c in mm_cols.iter().chain(sum_cols) {
            match self.coltypes.get(c as usize) {
                Some(t) if !t.is_text() => {}
                _ => return Ok(CbAggMetaStep::NotMeta),
            }
        }
        if len_cols.iter().any(|&c| !part.has_len_stats(c as usize)) {
            return Ok(CbAggMetaStep::NotMeta);
        }
        let nrgs = part.rgs.len();
        loop {
            if !self.rg_claimed {
                self.rg = self.claim_next_rg();
                self.rg_claimed = true;
                self.granule = 0;
                self.win = 0;
                self.rg_checked = false;
                self.decoded = false;
                self.rg_switches += 1;
            }
            if self.rg >= nrgs {
                return Ok(CbAggMetaStep::Exhausted);
            }
            let rg_rows = part.rgs[self.rg].nrows as usize;
            let ngranules = rg_rows.div_ceil(GRANULE_ROWS);
            if !self.rg_checked {
                // next_window's verbatim RG gate + prune accounting.
                if !self.rg_visible(self.rg)? || !self.rg_zone_ok(self.rg) {
                    self.granules_pruned += ngranules as u64;
                    self.rg_claimed = false;
                    continue;
                }
                self.rg_checked = true;
            }
            if self.granule >= ngranules {
                self.rg_claimed = false;
                continue;
            }
            if self.decoded {
                if self.win * self.window_rows < self.granule_rows {
                    // Mid-granule: staged windows pending.
                    return Ok(CbAggMetaStep::NotMeta);
                }
                // Decoded granule fully consumed: advance exactly as
                // next_window's own loop head would.
                self.granule += 1;
                self.decoded = false;
                continue;
            }
            // Fresh granule. Metadata answers require a wholly-visible RG
            // (footer counts stand in for the rows; own-transaction RGs
            // demote — the next_meta_count precedent).
            let m = &part.rgs[self.rg];
            if !self.rg_wholly_visible(self.rg)? {
                return Ok(CbAggMetaStep::NotMeta);
            }
            // Whole-RG tier: at the RG's first granule with every zone qual
            // AllPass over the RG footer extremes, the whole row group is
            // provably all-passing.
            if self.granule == 0
                && (sum_cols.is_empty() || m.flags & RG_FLAG_SUMS != 0)
                && (len_cols.is_empty() || m.flags & RG_FLAG_LENSTATS != 0)
                && self.zone_quals.iter().all(|q| {
                    let (_, min, max) = part.rgs[self.rg].chunks[(q.attnum - 1) as usize];
                    zone_verdict(q, min, max) == ZoneVerdict::AllPass
                })
            {
                for (k, &c) in mm_cols.iter().enumerate() {
                    let (_, min, max) = part.rgs[self.rg].chunks[c as usize];
                    mm[k] = (min, max);
                }
                for (k, &c) in sum_cols.iter().enumerate() {
                    sums[k] = part.rg_sum(self.rg, c as usize);
                }
                if !self.rg_len_stats(&part, ngranules, rg_rows, len_cols, lens) {
                    return Ok(CbAggMetaStep::NotMeta);
                }
                return Ok(CbAggMetaStep::MetaRg { rows: rg_rows as u64 });
            }
            // Granule tier: no granule-altitude value sums exist.
            if !sum_cols.is_empty() {
                return Ok(CbAggMetaStep::NotMeta);
            }
            if !len_cols.is_empty() && m.flags & RG_FLAG_LENSTATS == 0 {
                return Ok(CbAggMetaStep::NotMeta);
            }
            let g = self.granule;
            if !self
                .zone_quals
                .iter()
                .all(|q| {
                    let ge = part.chunk(self.rg, (q.attnum - 1) as usize).granule(g);
                    zone_verdict(q, ge.min, ge.max) == ZoneVerdict::AllPass
                })
            {
                return Ok(CbAggMetaStep::NotMeta);
            }
            let grows = (rg_rows - g * GRANULE_ROWS).min(GRANULE_ROWS);
            for (k, &c) in mm_cols.iter().enumerate() {
                let ge = part.chunk(self.rg, c as usize).granule(g);
                mm[k] = (ge.min, ge.max);
            }
            for (k, &c) in len_cols.iter().enumerate() {
                let Some(st) = part.granule_len_stats(self.rg, g, c as usize) else {
                    return Ok(CbAggMetaStep::NotMeta);
                };
                // pgrcolumnar stores no NULLs; a mismatch means foreign/corrupt
                // stats — refuse rather than answer.
                if st.1 != grows as u32 {
                    return Ok(CbAggMetaStep::NotMeta);
                }
                lens[k] = st.0 as i64;
            }
            return Ok(CbAggMetaStep::MetaGranule { rows: grows as u32 });
        }
    }

    // Fold the v7 per-granule length stats to RG altitude (exact: granule
    // sums partition the RG's rows). false = a missing/foreign entry —
    // refuse rather than answer (the granule_meta_peek precedent).
    fn rg_len_stats(
        &self,
        part: &Part,
        ngranules: usize,
        rg_rows: usize,
        len_cols: &[u16],
        lens: &mut [i64],
    ) -> bool {
        for (k, &c) in len_cols.iter().enumerate() {
            let mut sum = 0i64;
            for g in 0..ngranules {
                let grows = (rg_rows - g * GRANULE_ROWS).min(GRANULE_ROWS);
                let Some(st) = part.granule_len_stats(self.rg, g, c as usize) else {
                    return false;
                };
                if st.1 != grows as u32 {
                    return false;
                }
                // Bounded: a granule sum < 2^44 and <= 8 granules per RG.
                sum += st.0 as i64;
            }
            lens[k] = sum;
        }
        true
    }

    /// Consume the whole row group `agg_meta_peek` just answered (`MetaRg`):
    /// advance past it without decoding any granule. Only legal immediately
    /// after a `MetaRg` verdict.
    pub fn agg_meta_consume_rg(&mut self) {
        debug_assert!(self.rg_claimed && !self.decoded && self.granule == 0);
        let part = self.part.as_ref().unwrap();
        let ngranules = (part.rgs[self.rg].nrows as usize).div_ceil(GRANULE_ROWS);
        self.granules_meta += ngranules as u64;
        self.rg_claimed = false;
    }

    /// Consume the granule `agg_meta_peek` just answered (`MetaGranule`):
    /// advance past it without decoding. Only legal immediately after a
    /// `MetaGranule` verdict.
    pub fn agg_meta_consume_granule(&mut self) {
        debug_assert!(self.rg_claimed && !self.decoded);
        self.granules_meta += 1;
        self.granule += 1;
        self.win = 0;
    }

    // Adaptive drive: one bound-ordered granule per claim; window/block
    // staging inside a decoded granule matches the physical drive. Because
    // entries are bound-sorted, the first bound-dominated entry ends the
    // scan (every remaining bound is at least as dominated).
    fn next_window_adaptive(&mut self) -> PgResult<u32> {
        loop {
            if self.decoded {
                let lo = self.win * self.window_rows;
                if lo >= self.granule_rows {
                    self.decoded = false;
                    continue;
                }
                if self.block_mask & (1 << (lo / BLOCK_ROWS)) == 0 {
                    self.blocks_pruned += 1;
                    self.win = (lo / BLOCK_ROWS + 1) * (BLOCK_ROWS / self.window_rows);
                    continue;
                }
                self.windows_staged += 1;
                self.staged_lo = lo;
                self.staged_rows = (self.granule_rows - lo).min(self.window_rows);
                self.row_cursor = 0;
                self.win += 1;
                return Ok(self.staged_rows as u32);
            }
            // Probe budget (fields' comment): a best-first walk that isn't
            // visibly paying reverts its unvisited tail to physical order —
            // never a correctness event, purely visitation order.
            let reverted_now = {
                let ad = self.adaptive.as_deref_mut().unwrap();
                let over = !ad.reverted
                    && match ad.bound {
                        None => ad.cursor >= ad.nobound_budget,
                        Some(b) => ad.projected_stop(b) - ad.cursor > ad.projected_budget,
                    };
                if over {
                    let cursor = ad.cursor;
                    ad.entries[cursor..].sort_unstable_by_key(|e| (e.rg, e.g));
                    ad.reverted = true;
                }
                over
            };
            if reverted_now {
                self.adaptive_probe_reverts += 1;
            }
            let ad = self.adaptive.as_deref_mut().unwrap();
            let Some(&e) = ad.entries.get(ad.cursor) else { return Ok(0) };
            if let Some(b) = ad.bound {
                let dominated = match (ad.desc, ad.strict) {
                    (true, false) => e.bound < b,
                    (true, true) => e.bound <= b,
                    (false, false) => e.bound > b,
                    (false, true) => e.bound >= b,
                };
                if dominated {
                    if ad.reverted {
                        // Physical-order tail: domination no longer implies
                        // anything about later entries — skip just this one.
                        self.granules_bound_skipped += 1;
                        ad.cursor += 1;
                        continue;
                    }
                    self.granules_bound_skipped += (ad.entries.len() - ad.cursor) as u64;
                    ad.cursor = ad.entries.len();
                    return Ok(0);
                }
            }
            ad.cursor += 1;
            self.rg = e.rg as usize;
            self.granule = e.g as usize;
            self.rg_claimed = true;
            let rg_rows = self.part.as_ref().unwrap().rgs[self.rg].nrows as usize;
            let grows = (rg_rows - self.granule * GRANULE_ROWS).min(GRANULE_ROWS);
            let mask = match self.granule_admit(self.rg, self.granule, grows) {
                Ok(mask) => mask,
                Err(cause) => {
                    self.granules_pruned += 1;
                    if cause == GranulePruneCause::Bloom {
                        self.granules_bloom_pruned += 1;
                    }
                    continue;
                }
            };
            self.block_mask = mask;
            self.decode_current_granule();
            self.granules_scanned += 1;
            self.win = 0;
        }
    }

    pub fn nblocks(&self) -> u32 {
        self.part.as_ref().map_or(0, |p| (p.bytes().len() / 8192) as u32)
    }

    /// Total committed rows across the scan's Part (footer metadata only —
    /// no decode). The lane's tiny-input admission floor reads this before
    /// running any arm cascade.
    pub fn total_rows(&self) -> u64 {
        self.part.as_ref().map_or(0, |p| p.rgs.iter().map(|rg| rg.nrows as u64).sum())
    }

    /// Footer value min/max of the staged window's granule for column `c`;
    /// int-encoded chunks only (text granule entries carry byte lengths).
    /// The bounds cover the whole granule — a superset of any staged window
    /// inside it (pgrcolumnar stores no NULLs, so they bound every row).
    pub fn staged_window_value_minmax(&self, c: usize) -> Option<(i64, i64)> {
        if !self.decoded {
            return None;
        }
        let part = self.part.as_ref()?;
        let chunk = part.chunk(self.rg, c);
        if !int_zonemap_encoding(chunk.hdr.encoding) {
            return None;
        }
        let ge = chunk.granule(self.granule);
        Some((ge.min, ge.max))
    }

    /// Fill the SoA batch's prefix columns from the staged window (only
    /// needed columns carry decoded data; unneeded prefix cells stay stale
    /// and are never read — the virtual-slot publish is a no-op).
    pub fn batch_deform(
        &mut self,
        ncols: usize,
        soa: &mut ::exectuples::SoaBatch<'_>,
        qual_col_only: Option<u16>,
        sel: Option<&[u64]>,
    ) {
        let n = self.staged_rows;
        soa.begin(n as u32);
        let (first, last) = match qual_col_only {
            Some(c) => (c as usize, c as usize + 1),
            None => (0, ncols),
        };
        for c in first..last.min(self.needed.len()) {
            if !self.needed[c] {
                continue;
            }
            // Lane-read-only skip (lane_fill_skip): on lane-armed scans no
            // SoA consumer reads unmasked columns' Datum cells (consumers
            // read the slot store_slot populates; the SoA publish is a
            // no-op on virtual slots) — their fill is dead work.
            if !soa.lane_fill_wanted(c) {
                continue;
            }
            self.batch_deform_col_sel(c, soa, sel);
        }
    }

    /// Fill (or dict-answer) one staged column. Prewhere staged drives call
    /// this per clause so undeformed clauses' columns never decode; the
    /// caller owns soa.begin and the needed/fill-mask checks.
    pub fn batch_deform_col(&mut self, c: usize, soa: &mut ::exectuples::SoaBatch<'_>) {
        self.batch_deform_col_sel(c, soa, None)
    }

    /// `batch_deform_col` under an optional PREWHERE selection (the
    /// COMPLETING deform of survivor windows — `seq_scan_batch_lane_armed`'s
    /// stale-cell contract: consumers read SELECTED rows only). `sel` only
    /// narrows the lazy sub-framed dict ENSURE set to selected rows' codes;
    /// every cell write (pointers included) is identical to the plain path,
    /// and unframed dicts are entirely unaffected.
    pub fn batch_deform_col_sel(
        &mut self,
        c: usize,
        soa: &mut ::exectuples::SoaBatch<'_>,
        sel: Option<&[u64]>,
    ) {
        debug_assert!(self.needed[c]);
        let n = self.staged_rows;
        self.ensure_col(c);
        // Length-lane fill (fold length admissions): the column's ONLY SoA
        // consumer reads lengths, so the fill answers `Datum::from_i64(len)`
        // per row — per-dict-code table gather on dict chunks (string bytes
        // touched once per distinct value per RG), header-read/EXACT C walk
        // on Raw chunks — and never materializes the datum lane. Dict-wanted
        // columns (dict-tier qual coexist) keep the dict-lane answer for the
        // qual; the post-qual gather converts them (`gather_len_lane_bytes`
        // / `convert_lane_to_len_bytes` at the nodeseqscan fill).
        let lw = soa.len_want(c);
        if lw != 0 && !soa.dict_want(c) {
            self.fill_len_col(c, lw == ::exectuples::LEN_WANT_CHARS, soa);
            return;
        }
        let cd = &self.cols[c];
        if cd.is_dict {
            let codes = &cd.codes[self.staged_lo..self.staged_lo + n];
            if soa.dict_want(c) {
                // Zero-decode dict lane: codes + RG dictionary + epoch =
                // rg index (dict content per RG is immutable and the scan
                // pins its Arc<Part>, so the epoch key is stable across
                // rescans). Values/isnull cells stay stale per the
                // set_dict_lane contract.
                // v7 stitch: local -> part-global codes for this (rg, col),
                // published when present, length-consistent with the dict,
                // and the scan's stitch identity is armed (scan_uid != 0).
                // Consumers fail open to per-epoch keying on a null stitch.
                let stitch = if self.scan_uid != 0 {
                    self.part
                        .as_ref()
                        .and_then(|p| p.stitch(self.rg, c))
                        .filter(|s| s.len() == cd.dict.len())
                } else {
                    None
                };
                let gndv = self
                    .part
                    .as_ref()
                    .map(|p| p.stitch_gndv(c))
                    .filter(|&g| stitch.is_some() && g <= u32::MAX as u64)
                    .unwrap_or(0);
                let stitch = if gndv != 0 { stitch } else { None };
                let (lazy, lazy_ensure, lazy_ensure_all) = lazy_seam(&cd.lazy);
                soa.set_dict_lane(
                    c,
                    ::exectuples::SoaDictLane {
                        codes: codes.as_ptr(),
                        table: ::exectuples::SoaDictTable {
                            dict: cd.dict.as_ptr(),
                            ndict: cd.dict.len() as u32,
                            epoch: self.rg as u64,
                            sorted: cd.dict_sorted,
                            stitch: stitch.map_or(std::ptr::null(), |s| s.as_ptr()),
                            gndv: gndv as u32,
                            gepoch: if stitch.is_some() { self.scan_uid } else { 0 },
                            lazy,
                            lazy_ensure,
                            lazy_ensure_all,
                            // Witness law (F-R1-1): every dict build puts
                            // the images in ONE owned region — build_dict's
                            // arena / the mmap payload, or DictLazy's single
                            // backing buf (bytes exist post-ensure, which
                            // whole-dict consumers already run first per the
                            // lazy-seam contract).
                            contig: true,
                        },
                    },
                );
                return;
            }
            // No dict-lane consumer for this column: one-instruction
            // escape, gather dict[code] into the Datum cells. Published
            // pointer Datums may be dereferenced by ANY later consumer, so
            // a lazy sub-framed dict ensures each gathered code's bytes
            // here (all-done tables take the plain loop).
            match &cd.lazy {
                Some(l) if !l.all_done() => match sel {
                    // PREWHERE completing deform: ensure SELECTED rows'
                    // codes only (unselected cells hold valid pointers to
                    // possibly-unmaterialized bytes — never read under the
                    // armed batch's stale-cell contract). The ensure walk
                    // word-skips cleared selection words (same ensured
                    // codes — sparse survivor windows stop paying a per-row
                    // bit test), and the pointer gather runs unconditionally
                    // over the window exactly as before (a tight,
                    // branch-free loop). Rows past the selection words are
                    // unselected (the old `get`-based walk's contract).
                    Some(sel) => {
                        let lim = (sel.len() * 64).min(codes.len()) as u32;
                        let _ = ::exectuples::for_each_live::<core::convert::Infallible>(
                            Some(sel),
                            0,
                            lim,
                            |i| {
                                l.ensure_code(codes[i as usize]);
                                Ok(())
                            },
                        );
                        for (out, &code) in soa.col_values_mut(c).iter_mut().zip(codes) {
                            *out = cd.dict[code as usize];
                        }
                    }
                    None => {
                        for (out, &code) in soa.col_values_mut(c).iter_mut().zip(codes) {
                            l.ensure_code(code);
                            *out = cd.dict[code as usize];
                        }
                    }
                },
                _ => {
                    for (out, &code) in soa.col_values_mut(c).iter_mut().zip(codes) {
                        *out = cd.dict[code as usize];
                    }
                }
            }
        } else {
            soa.col_values_mut(c).copy_from_slice(self.staged_col(c));
            // Blob-span witness (likeband): a text window's images sit
            // ascending inside one readable span (RawText mmap blob /
            // Lz4Text arena) — publish it so the contains-LIKE kernel can
            // run ONE blob-wide search instead of a per-row loop. The
            // ascending proof is re-verified per window (<= SOA window
            // rows, trivial); an unprovable layout just stays per-row.
            if cd.contig_text {
                if let Some(span) = staged_text_span(self.staged_col(c)) {
                    soa.set_text_span(c, span);
                }
            }
        }
        soa.col_isnull_mut(c).fill(false);
    }

    /// Length-lane fill of one staged column window, callable at ANY point
    /// after the window stages (the post-qual gather path for dict-tier qual
    /// columns re-answers the lane as lengths through here): reads the
    /// scan-side decode state (codes/datums per staged window), never the
    /// SoA cells, so it is safe whether the column currently holds a dict
    /// lane answer, Raw datums, or stale cells.
    pub fn batch_fill_len_col(
        &mut self,
        c: usize,
        chars: bool,
        soa: &mut ::exectuples::SoaBatch<'_>,
    ) {
        self.ensure_col(c);
        self.fill_len_col(c, chars, soa);
    }

    /// Length-lane fill for one staged column window (see the
    /// `batch_deform_col` length branch). `chars` = UTF-8 character length
    /// (C `text_length` parity by seam reuse), else octet length.
    fn fill_len_col(&mut self, c: usize, chars: bool, soa: &mut ::exectuples::SoaBatch<'_>) {
        let n = self.staged_rows;
        let lo = self.staged_lo;
        let cd = &mut self.cols[c];
        let out = &mut soa.col_values_mut(c)[..n];
        if cd.is_dict {
            // Per-code memo: one length per distinct value per (RG dict,
            // kind); rebuilt only when the dictionary changes.
            let key = (cd.dict_rg, chars as u8 + 1);
            if cd.len_memo_key != key {
                // Whole-dict sweep: materialize a lazy sub-framed dict.
                if let Some(l) = &cd.lazy {
                    l.ensure_all();
                }
                cd.len_memo.clear();
                cd.len_memo.reserve(cd.dict.len());
                for &d in &cd.dict {
                    // SAFETY: dict entries are live inline varlena images
                    // (decode contract).
                    cd.len_memo.push(unsafe { text_datum_len(d, chars) });
                }
                cd.len_memo_key = key;
            }
            for (o, &code) in out.iter_mut().zip(&cd.codes[lo..lo + n]) {
                *o = Datum::from_i64(cd.len_memo[code as usize]);
            }
        } else {
            for (o, &d) in out.iter_mut().zip(&cd.datums[lo..lo + n]) {
                // SAFETY: decoded window datums are live inline varlena
                // images (decode contract).
                *o = Datum::from_i64(unsafe { text_datum_len(d, chars) });
            }
        }
        soa.col_isnull_mut(c).fill(false);
    }

    /// Fused-sort varlena key feed: staged text Datums into SoA column 0.
    /// When the consumer opted into dict codes (`set_dict_want(0)` — the
    /// distinct-set text key feed) a dict-encoded window answers with the
    /// zero-gather dict lane instead (same identity discipline as
    /// `batch_deform_col`: epoch = rg index, stable for the pinned scan);
    /// the datum/isnull cells stay stale per the `set_dict_lane` contract.
    pub fn batch_stage_varkey(&mut self, key: usize, soa: &mut ::exectuples::SoaBatch<'_>) {
        let n = self.staged_rows;
        soa.begin(n as u32);
        self.ensure_col(key);
        let cd = &self.cols[key];
        if cd.is_dict {
            let codes = &cd.codes[self.staged_lo..self.staged_lo + n];
            if soa.dict_want(0) {
                // v7 stitch: same publication discipline as
                // `batch_deform_col` — local -> part-global codes for
                // (rg, key) when present, length-consistent with the dict,
                // and the scan's stitch identity armed (scan_uid != 0).
                // Consumers (the distinct-set dict memo) fail open to
                // per-epoch keying on a null stitch.
                let stitch = if self.scan_uid != 0 {
                    self.part
                        .as_ref()
                        .and_then(|p| p.stitch(self.rg, key))
                        .filter(|s| s.len() == cd.dict.len())
                } else {
                    None
                };
                let gndv = self
                    .part
                    .as_ref()
                    .map(|p| p.stitch_gndv(key))
                    .filter(|&g| stitch.is_some() && g <= u32::MAX as u64)
                    .unwrap_or(0);
                let stitch = if gndv != 0 { stitch } else { None };
                let (lazy, lazy_ensure, lazy_ensure_all) = lazy_seam(&cd.lazy);
                soa.set_dict_lane(
                    0,
                    ::exectuples::SoaDictLane {
                        codes: codes.as_ptr(),
                        table: ::exectuples::SoaDictTable {
                            dict: cd.dict.as_ptr(),
                            ndict: cd.dict.len() as u32,
                            epoch: self.rg as u64,
                            sorted: cd.dict_sorted,
                            stitch: stitch.map_or(std::ptr::null(), |s| s.as_ptr()),
                            gndv: gndv as u32,
                            gepoch: if stitch.is_some() { self.scan_uid } else { 0 },
                            lazy,
                            lazy_ensure,
                            lazy_ensure_all,
                            // Witness law (F-R1-1): every dict build puts
                            // the images in ONE owned region — build_dict's
                            // arena / the mmap payload, or DictLazy's single
                            // backing buf (bytes exist post-ensure, which
                            // whole-dict consumers already run first per the
                            // lazy-seam contract).
                            contig: true,
                        },
                    },
                );
                return;
            }
            // Same published-pointer contract as `batch_deform_col`'s
            // gather: ensure each gathered code on lazy dicts.
            match &cd.lazy {
                Some(l) if !l.all_done() => {
                    for (out, &code) in soa.col_values_mut(0).iter_mut().zip(codes) {
                        l.ensure_code(code);
                        *out = cd.dict[code as usize];
                    }
                }
                _ => {
                    for (out, &code) in soa.col_values_mut(0).iter_mut().zip(codes) {
                        *out = cd.dict[code as usize];
                    }
                }
            }
        } else {
            soa.col_values_mut(0).copy_from_slice(self.staged_col(key));
        }
        soa.col_isnull_mut(0).fill(false);
    }

    #[inline]
    pub fn staged_col(&self, c: usize) -> &[Datum] {
        &self.cols[c].datums[self.staged_lo..self.staged_lo + self.staged_rows]
    }


    /// STABLE DICTIONARY IDENTITY of the staged window's column `c`, when
    /// the chunk is dict-encoded and already decoded (codes-only decode):
    /// per-row u32 codes into the per-row-group dictionary of decoded text
    /// Datums, plus the identity key. `epoch` = row-group index — dict
    /// content per RG is immutable and the scan pins its `Arc<Part>`, so the
    /// key is stable across rescans and per-code memos keyed on it stay
    /// valid for the life of the scan. `sorted` = codes are byte-rank order
    /// (CHUNK_FLAG_DICT_SORTED), gating dict-code range predicates.
    /// Downstream lanes carry dict codes through breakers on this identity;
    /// nothing in the scan may strip it.
    #[inline]
    pub fn staged_dict_lane(&self, c: usize) -> Option<CbDictLane<'_>> {
        let cd = &self.cols[c];
        if !cd.is_dict || cd.gkey != (self.rg as u32, self.granule as u32) {
            return None;
        }
        // The borrowed CbDictLane carries no lazy seam — materialize fully
        // (the seam-carrying channel is `staged_codes_lane`).
        if let Some(l) = &cd.lazy {
            l.ensure_all();
        }
        Some(CbDictLane {
            codes: &cd.codes[self.staged_lo..self.staged_lo + self.staged_rows],
            dict: &cd.dict,
            epoch: self.rg as u64,
            sorted: cd.dict_sorted,
        })
    }

    /// Physical rowref base of the CURRENT staged window (tie-ordering rule
    /// 2: rowref = `(row_group << 32) | rg-global-row`, monotone in physical
    /// position): staged row `i`'s rowref is `base + i` — windows never
    /// cross a granule, let alone a row group, so the low word never carries
    /// into the rg bits. `None` when nothing is staged or the part exceeds
    /// the consumer's 48-bit envelope (>= 2^16 row groups — never on the analytics
    /// banks; the consumer then keeps its demote backstop).
    #[inline]
    pub fn staged_rowref_base(&self) -> Option<u64> {
        if self.staged_rows == 0 || !self.rg_claimed || self.rg > u16::MAX as usize {
            return None;
        }
        let row = self.granule * GRANULE_ROWS + self.staged_lo;
        // u64 arithmetic: `u32::MAX as usize + 1` overflows on 32-bit
        // (wasm32) usize; identical bound on 64-bit targets.
        debug_assert!(row as u64 + self.staged_rows as u64 <= u32::MAX as u64 + 1);
        Some(((self.rg as u64) << 32) | row as u64)
    }

    /// `staged_dict_lane` repackaged as the batch-currency `SoaDictLane`
    /// (raw pointers; the same window-lifetime contract as the dict-lane
    /// answers `batch_deform_col` publishes) — the str MIN/MAX dict-code
    /// side channel (lane-v2-dictminmax). The caller (nodeseqscan seam) owns
    /// the "values cells were gathered from this dictionary" proof; this
    /// accessor only certifies codes/dict/epoch/sorted for the CURRENT
    /// staged window. `dict[code]` here is pointer-identical to the Raw
    /// gather's values fill (`batch_deform_col`'s `cd.dict[code]`).
    #[inline]
    pub fn staged_codes_lane(&self, c: usize) -> Option<::exectuples::SoaDictLane> {
        let cd = &self.cols[c];
        if !cd.is_dict || cd.gkey != (self.rg as u32, self.granule as u32) {
            return None;
        }
        let (lazy, lazy_ensure, lazy_ensure_all) = lazy_seam(&cd.lazy);
        Some(::exectuples::SoaDictLane {
            codes: cd.codes[self.staged_lo..self.staged_lo + self.staged_rows].as_ptr(),
            table: ::exectuples::SoaDictTable {
                dict: cd.dict.as_ptr(),
                ndict: cd.dict.len() as u32,
                epoch: self.rg as u64,
                sorted: cd.dict_sorted,
                // The dict-code side channel publishes no stitch (its
                // consumers key on the per-RG epoch).
                stitch: std::ptr::null(),
                gndv: 0,
                gepoch: 0,
                lazy,
                lazy_ensure,
                lazy_ensure_all,
                // Witness law (F-R1-1): see the dict-lane fill sites.
                contig: true,
            },
        })
    }

    /// `staged_codes_lane` with the v7 part-global STITCH PUBLISHED when
    /// the scan carries one — the DictCode sort-key side channel
    /// (docs/design/dict-code-flow.md inc-1). Same publication gating as
    /// the deform's dict-lane answer (`batch_deform_col_sel`): stitch
    /// identity armed (`scan_uid != 0`), per-(rg, col) stitch present and
    /// length-consistent with the dict, `gndv` in the u32 envelope. A
    /// SEPARATE accessor so the landed per-epoch consumers of
    /// `staged_codes_lane` (str MIN/MAX code folds) keep their keying
    /// unchanged. Consumers gate order use on `table.has_stitch()` and
    /// fail closed otherwise.
    #[inline]
    pub fn staged_codes_lane_global(&mut self, c: usize) -> Option<::exectuples::SoaDictLane> {
        // The key column may sit outside every other consumer's read set for
        // this window (no qual on it, past the fixed-width deform's
        // coverage, no per-row emit ran): complete its decode first —
        // needed-set columns only (a key column is always in the needed
        // set), idempotent per (rg, granule) via `decode_col`'s gkey check.
        if c >= self.cols.len() || !self.needed[c] {
            return None;
        }
        self.ensure_col(c);
        let mut lane = self.staged_codes_lane(c)?;
        if self.scan_uid != 0 {
            let ndict = self.cols[c].dict.len();
            let stitch = self
                .part
                .as_ref()
                .and_then(|p| p.stitch(self.rg, c))
                .filter(|s| s.len() == ndict);
            let gndv = self
                .part
                .as_ref()
                .map(|p| p.stitch_gndv(c))
                .filter(|&g| stitch.is_some() && g <= u32::MAX as u64)
                .unwrap_or(0);
            if let Some(s) = stitch {
                if gndv != 0 {
                    lane.table.stitch = s.as_ptr();
                    lane.table.gndv = gndv as u32;
                    lane.table.gepoch = self.scan_uid;
                }
            }
        }
        Some(lane)
    }

    /// Publish staged row `i` into the virtual slot (needed columns only;
    /// unneeded cells are nulled once per scan and never read).
    pub fn store_slot(&mut self, i: u32, slot: &mut SlotData<'_>) {
        debug_assert!((i as usize) < self.staged_rows);
        self.ensure_needed_cols();
        let row = self.staged_lo + i as usize;
        let base = slot.base_mut();
        if !self.slot_inited.get() {
            base.tts_values.fill(Datum::null());
            base.tts_isnull.fill(true);
            for &c in &self.needed_idx {
                base.tts_isnull[c as usize] = false;
            }
            self.slot_inited.set(true);
        }
        for &c in &self.needed_idx {
            base.tts_values[c as usize] = self.cols[c as usize].datum(row);
        }
        base.tts_nvalid = self.coltypes.len() as ::types_core::AttrNumber;
        base.mark_not_empty();
    }

    /// Staged-window base for ref-carrying consumers: (row group, rg-global
    /// row index of staged row 0); ref = base + i resolves via `gather_row`
    /// for the life of the scan (the Part mmap). None = nothing staged, or
    /// the part exceeds the consumers' 48-bit rowref envelope.
    ///
    /// The envelope refusal is the SAME bound `staged_rowref_base` applies,
    /// and for the same reason: every consumer of this pair packs it as
    /// `(rg << 32) | row` into a 48-bit carrier — the sort-heap entry's
    /// `TopnEntry` field (`nodesort::sink::TOPN_MAX_ROWREF`) and the
    /// tuplesort's six `mt_padding` bytes. At `>= 2^16` row groups the pack
    /// silently overruns both: the high rg bits land on the sort key's low
    /// bits and the address itself is masked back down, so the top-N would
    /// return the wrong rows in the wrong order with no error. Refusing here
    /// hands the consumer its existing demote backstop instead. Never
    /// reached on the analytics banks (2^16 row groups is > 2^32 rows).
    pub fn window_ref(&self) -> Option<(u32, u32)> {
        (self.rg_claimed && self.decoded && self.staged_rows > 0 && self.rg <= u16::MAX as usize)
            .then(|| (self.rg as u32, (self.granule * GRANULE_ROWS + self.staged_lo) as u32))
    }

    /// Materialize rg-global `row` of row group `rg` into the slot under the
    /// CURRENT needed set (store_slot cell semantics: unneeded cells null).
    /// Decodes into a gather-local scratch keyed by (rg, granule,
    /// needed_epoch) — the staged window's buffers are untouched. Row refs
    /// only come from windows this scan already claimed, visibility-checked
    /// and zone-passed, so no rg_visible re-check runs here. The slot's
    /// by-ref datums live until the next gather decode of a different key
    /// (the per-row store contract store_slot already has).
    pub fn gather_row(&mut self, rg: u32, row: u32, slot: &mut SlotData<'_>) -> bool {
        let Some(part) = self.part.as_ref() else { return false };
        let (rg, row) = (rg as usize, row as usize);
        if rg >= part.rgs.len() || row >= part.rgs[rg].nrows as usize {
            debug_assert!(false, "cbstore gather_row: ref out of range");
            return false;
        }
        let g = row / GRANULE_ROWS;
        let r = row % GRANULE_ROWS;
        let ncols = self.coltypes.len();
        let gs = self.gather.get_or_insert_with(|| {
            Box::new(GatherScratch {
                cols: (0..ncols).map(|_| new_col_decode()).collect(),
                key: (usize::MAX, usize::MAX, u64::MAX),
            })
        });
        if gs.key != (rg, g, self.needed_epoch) {
            for (c, cd) in gs.cols.iter_mut().enumerate() {
                if !self.needed[c] {
                    continue;
                }
                // Scratch reuse across needed-set changes: gkey may claim
                // (rg, g) while the buffers predate this needed set — force
                // the decode.
                cd.gkey = NONE_KEY;
                cd.dict.clear();
                cd.dict_rg = rg;
                decode_col(part, rg, g, c, cd);
            }
            gs.key = (rg, g, self.needed_epoch);
        }
        let base = slot.base_mut();
        base.tts_values.fill(Datum::null());
        base.tts_isnull.fill(true);
        for &c in &self.needed_idx {
            base.tts_isnull[c as usize] = false;
            base.tts_values[c as usize] = gs.cols[c as usize].datum(r);
        }
        base.tts_nvalid = ncols as ::types_core::AttrNumber;
        base.mark_not_empty();
        true
    }

    /// ANALYZE parallel sample fetch (loadfinal lane): materialize `refs`
    /// ((rg, rg-global row), ascending file order — the reservoir sample's
    /// physical positions) into `slot` one row at a time via `per_row`,
    /// decoding (rg, granule) tasks on `pool` worker threads.
    ///
    /// Motivation: a 30k-row uniform sample over 100M rows lands ~2-3 rows
    /// in nearly every granule, so acquisition decodes ~every granule of
    /// every column — a serial full-part decode on the leader (~50 s of the
    /// ~55 s VACUUM ANALYZE tail @100M). The sample positions are pure
    /// PRNG/row-count arithmetic known up front, so the decode parallelizes
    /// while the published rows, values, and order stay IDENTICAL to serial
    /// `gather_row` fetches by construction (same decode routines, values
    /// byte-copied out; the leader consumes task outputs in refs order).
    pub fn analyze_gather_rows(
        &mut self,
        refs: &[(u32, u32)],
        pool: usize,
        slot: &mut SlotData<'_>,
        per_row: &mut dyn FnMut(&mut SlotData<'_>) -> PgResult<()>,
    ) -> PgResult<u64> {
        let Some(part) = self.part.as_deref() else {
            debug_assert!(refs.is_empty());
            return Ok(0);
        };
        let ncols = self.coltypes.len();
        let (needed_idx, coltypes) = (&self.needed_idx, &self.coltypes);
        let mut ntasks = 0u64;
        let mut res: PgResult<()> = Ok(());
        {
            let res = &mut res;
            let slot = &mut *slot;
            let mut on_task = |task: &AnalyzeTask, out: &AnalyzeTaskOut| -> bool {
                ntasks += 1;
                let nneed = needed_idx.len();
                let bytes_base = out.bytes.as_ptr() as usize;
                for i in 0..(task.hi - task.lo) {
                    let base = slot.base_mut();
                    base.tts_values.fill(Datum::null());
                    base.tts_isnull.fill(true);
                    for (j, &c) in needed_idx.iter().enumerate() {
                        let w = out.words[i * nneed + j];
                        base.tts_isnull[c as usize] = false;
                        base.tts_values[c as usize] = if coltypes[c as usize].is_text() {
                            // w = 4-aligned offset of the copied varlena
                            // image; the image lives until on_task returns
                            // (per_row's copy contract matches store_slot's
                            // per-row publish).
                            Datum::from_usize(bytes_base + w as usize)
                        } else {
                            Datum::from_usize(w as usize)
                        };
                    }
                    base.tts_nvalid = ncols as ::types_core::AttrNumber;
                    base.mark_not_empty();
                    if let Err(e) = per_row(slot) {
                        *res = Err(e);
                        return false;
                    }
                }
                true
            };
            analyze_gather_pipeline(part, refs, needed_idx, coltypes, pool, &mut on_task);
        }
        res.map(|()| ntasks)
    }

    /// Per-row drive (`scan_getnextslot`): forward-only.
    pub fn getnextslot(&mut self, slot: &mut SlotData<'_>) -> PgResult<bool> {
        loop {
            if self.row_cursor < self.staged_rows {
                let i = self.row_cursor as u32;
                self.row_cursor += 1;
                self.store_slot(i, slot);
                return Ok(true);
            }
            if self.next_window()? == 0 {
                slot.base_mut().mark_empty();
                return Ok(false);
            }
        }
    }
}

// ---- ANALYZE parallel sample fetch internals (loadfinal lane) --------------

// One (rg, granule) decode task covering refs[lo..hi] (contiguous — refs
// ascend in file order, so a granule's refs are one slice).
struct AnalyzeTask {
    rg: u32,
    g: u32,
    lo: usize,
    hi: usize,
}

// Extracted sample values for one task, row-major in needed_idx order: for
// text columns the u64 is a 4-aligned byte offset into `bytes` (a complete
// inline varlena image, 1B-short or 4B-U, byte-copied out of the decode
// buffers); for every other pgrcolumnar column type (all by-val) it is the
// datum word verbatim.
struct AnalyzeTaskOut {
    words: Vec<u64>,
    bytes: Vec<u8>,
}

// `&Part` across scoped decode threads.
//
// SAFETY: Part is read-only over an immutable mmap — sealed row groups
// never mutate and the mapped extent is fixed at open (`data_end` and every
// lazy section offset come from the footer parsed then). Its only interior
// mutability is the `granule_starts` OnceLock, documented "builds it once
// under any thread". All decode scratch with real interior mutability
// (ColDecode incl. DictLazy's Cells) is constructed INSIDE each worker and
// never crosses threads.
struct PartSync<'a>(&'a Part);
unsafe impl Sync for PartSync<'_> {}
impl<'a> PartSync<'a> {
    // Accessor (not a field projection): worker closures then capture the
    // PartSync wrapper itself rather than disjoint-capturing a bare `&Part`
    // field (edition-2021 capture would sidestep the Sync wrapper).
    fn get(&self) -> &'a Part {
        self.0
    }
}

// Decode one task's needed columns with thread-local scratch and copy the
// sampled rows' values out. Same decode routines as gather_row; the copy
// direction (by-val word / full varlena image) is exactly what a serial
// per_row consumer would read through ColDecode::datum.
fn analyze_extract_task(
    part: &Part,
    task: &AnalyzeTask,
    refs: &[(u32, u32)],
    needed_idx: &[u16],
    coltypes: &[ColType],
    cds: &mut [ColDecode],
) -> AnalyzeTaskOut {
    let (rg, g) = (task.rg as usize, task.g as usize);
    for &c in needed_idx {
        decode_col(part, rg, g, c as usize, &mut cds[c as usize]);
    }
    let nneed = needed_idx.len();
    let nrows = task.hi - task.lo;
    let mut out =
        AnalyzeTaskOut { words: Vec::with_capacity(nrows * nneed), bytes: Vec::new() };
    for i in task.lo..task.hi {
        debug_assert_eq!(refs[i].0, task.rg);
        let r = refs[i].1 as usize - g * GRANULE_ROWS;
        for &c in needed_idx {
            let d = cds[c as usize].datum(r);
            if coltypes[c as usize].is_text() {
                while out.bytes.len() % 4 != 0 {
                    out.bytes.push(0);
                }
                let off = out.bytes.len() as u64;
                let p = d.as_usize() as *const u8;
                // SAFETY: pgrcolumnar decode contract — `d` is a live inline
                // varlena image in this thread's decode buffers.
                let len = unsafe { ::types_tuple::varatt::varsize_any(p) };
                // SAFETY: same contract; the image is `len` readable bytes.
                out.bytes.extend_from_slice(unsafe { core::slice::from_raw_parts(p, len) });
                out.words.push(off);
            } else {
                // Full Datum word; as_usize() truncates byval 8-byte values on wasm32.
                out.words.push(d.as_u64());
            }
        }
    }
    out
}

// The bounded producer/consumer pipeline: workers claim tasks in file order
// off an atomic cursor and park while more than `pool * 8` outputs are
// undelivered (consume-on-arrival keeps memory flat); the caller's
// `on_task` sees every task IN ORDER (return false = stop early, e.g. a
// consumer error). Worker panics (decode corruption) release the leader's
// wait and re-raise on scope exit — the backend's ordinary panic unwind.
fn analyze_gather_pipeline(
    part: &Part,
    refs: &[(u32, u32)],
    needed_idx: &[u16],
    coltypes: &[ColType],
    pool: usize,
    on_task: &mut dyn FnMut(&AnalyzeTask, &AnalyzeTaskOut) -> bool,
) {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    debug_assert!(pool >= 1);
    let mut tasks: Vec<AnalyzeTask> = Vec::new();
    for (i, &(rg, row)) in refs.iter().enumerate() {
        debug_assert!(i == 0 || refs[i - 1] < refs[i], "refs must ascend");
        let g = (row as usize / GRANULE_ROWS) as u32;
        match tasks.last_mut() {
            Some(t) if t.rg == rg && t.g == g => t.hi = i + 1,
            _ => tasks.push(AnalyzeTask { rg, g, lo: i, hi: i + 1 }),
        }
    }
    let ntasks = tasks.len();
    if ntasks == 0 {
        return;
    }
    let results: Vec<std::sync::Mutex<Option<AnalyzeTaskOut>>> =
        (0..ntasks).map(|_| std::sync::Mutex::new(None)).collect();
    let next = AtomicUsize::new(0);
    let consumed = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let wpanic = AtomicBool::new(false);
    let window = pool * 8;
    let pshare = PartSync(part);
    let ncols = coltypes.len();

    // Sets stop/wpanic when its worker unwinds, so the leader never waits
    // on a task that will not arrive.
    struct PanicGuard<'a>(&'a AtomicBool, &'a AtomicBool);
    impl Drop for PanicGuard<'_> {
        fn drop(&mut self) {
            if std::thread::panicking() {
                self.1.store(true, Ordering::Relaxed);
                self.0.store(true, Ordering::Relaxed);
            }
        }
    }

    std::thread::scope(|s| {
        for _ in 0..pool {
            s.spawn(|| {
                let _guard = PanicGuard(&stop, &wpanic);
                let part = pshare.get();
                let mut cds: Vec<ColDecode> = (0..ncols).map(|_| new_col_decode()).collect();
                loop {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    let t = next.fetch_add(1, Ordering::Relaxed);
                    if t >= ntasks {
                        return;
                    }
                    while t >= consumed.load(Ordering::Acquire) + window {
                        if stop.load(Ordering::Relaxed) {
                            return;
                        }
                        std::thread::sleep(std::time::Duration::from_micros(100));
                    }
                    let out = analyze_extract_task(
                        part, &tasks[t], refs, needed_idx, coltypes, &mut cds,
                    );
                    *results[t].lock().unwrap() = Some(out);
                }
            });
        }
        'consume: for t in 0..ntasks {
            let out = loop {
                if let Some(o) = results[t].lock().unwrap().take() {
                    break o;
                }
                if wpanic.load(Ordering::Relaxed) {
                    // The producing worker died; scope exit re-raises its
                    // panic below.
                    break 'consume;
                }
                std::thread::sleep(std::time::Duration::from_micros(50));
            };
            let go = on_task(&tasks[t], &out);
            consumed.store(t + 1, Ordering::Release);
            if !go {
                break 'consume;
            }
        }
        stop.store(true, Ordering::Relaxed);
    });
}

fn zone_can_match(q: &ZoneQual, min: i64, max: i64) -> bool {
    match q.op {
        ZoneCmp::Eq => q.val >= min && q.val <= max,
        ZoneCmp::Ne => !(min == max && min == q.val),
        ZoneCmp::Lt => min < q.val,
        ZoneCmp::Le => min <= q.val,
        ZoneCmp::Gt => max > q.val,
        ZoneCmp::Ge => max >= q.val,
    }
}

// Exact per-granule verdict for `col OP val` over decoded [min,max].
// AllPass = every row satisfies; AllFail is definitionally !zone_can_match.
fn zone_verdict(q: &ZoneQual, min: i64, max: i64) -> ZoneVerdict {
    let all_pass = match q.op {
        ZoneCmp::Eq => min == max && min == q.val,
        ZoneCmp::Ne => q.val < min || q.val > max,
        ZoneCmp::Lt => max < q.val,
        ZoneCmp::Le => max <= q.val,
        ZoneCmp::Gt => min > q.val,
        ZoneCmp::Ge => min >= q.val,
    };
    if all_pass {
        ZoneVerdict::AllPass
    } else if !zone_can_match(q, min, max) {
        ZoneVerdict::AllFail
    } else {
        ZoneVerdict::Mixed
    }
}

#[cfg(test)]
mod verdict_tests {
    use super::*;

    fn eval_row(op: ZoneCmp, x: i64, v: i64) -> bool {
        match op {
            ZoneCmp::Eq => x == v,
            ZoneCmp::Ne => x != v,
            ZoneCmp::Lt => x < v,
            ZoneCmp::Le => x <= v,
            ZoneCmp::Gt => x > v,
            ZoneCmp::Ge => x >= v,
        }
    }

    // Differential: the compressed-domain verdict must agree with
    // decode-then-evaluate over every value in [min,max] for every op and
    // every const spanning below/at/above the granule extremes (boundary
    // values, out-of-range consts, and the const/single-value granule).
    #[test]
    fn verdict_matches_decode_then_eval() {
        let ops = [
            ZoneCmp::Eq,
            ZoneCmp::Ne,
            ZoneCmp::Lt,
            ZoneCmp::Le,
            ZoneCmp::Gt,
            ZoneCmp::Ge,
        ];
        for min in -4i64..=4 {
            for max in min..=4 {
                for val in -6i64..=6 {
                    for op in ops {
                        let q = ZoneQual { attnum: 1, op, val };
                        let got = zone_verdict(&q, min, max);
                        let passes = (min..=max).filter(|&x| eval_row(op, x, val)).count();
                        let total = (max - min + 1) as usize;
                        let want = if passes == total {
                            ZoneVerdict::AllPass
                        } else if passes == 0 {
                            ZoneVerdict::AllFail
                        } else {
                            ZoneVerdict::Mixed
                        };
                        assert_eq!(got, want, "op={op:?} val={val} [{min},{max}]");
                    }
                }
            }
        }
    }

    #[test]
    fn verdict_agrees_with_zone_can_match() {
        let ops = [
            ZoneCmp::Eq,
            ZoneCmp::Ne,
            ZoneCmp::Lt,
            ZoneCmp::Le,
            ZoneCmp::Gt,
            ZoneCmp::Ge,
        ];
        for min in -4i64..=4 {
            for max in min..=4 {
                for val in -6i64..=6 {
                    for op in ops {
                        let q = ZoneQual { attnum: 1, op, val };
                        // AllFail iff the existing pruning says "cannot match".
                        assert_eq!(
                            zone_verdict(&q, min, max) == ZoneVerdict::AllFail,
                            !zone_can_match(&q, min, max),
                            "op={op:?} val={val} [{min},{max}]"
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod analyze_gather_tests {
    use super::*;
    use crate::format::RG_ROWS;

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-anlz-gather-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    // Low-cardinality (dict-able) and high-cardinality (blob/lz4) text
    // shapes, empties included.
    fn dict_text(i: usize) -> Vec<u8> {
        match i % 5 {
            0 => Vec::new(),
            k => format!("dict-val-{}", (i % 89) * k).into_bytes(),
        }
    }
    fn uniq_text(i: usize) -> Vec<u8> {
        format!("unique-payload-{i}-{}", "x".repeat(i % 37)).into_bytes()
    }

    // 2 full RGs + a partial granule; 4 columns exercising by-val + both
    // text shapes.
    fn build_part(path: &str) -> Part {
        let coltypes =
            vec![ColType::I64, ColType::Text, ColType::I32, ColType::Text];
        let mut w = crate::writer::open_writer_at(path, coltypes).unwrap();
        let n = 2 * RG_ROWS + GRANULE_ROWS + GRANULE_ROWS / 3;
        let mut keep = Vec::new();
        for i in 0..n {
            let (d, u) = (dict_text(i), uniq_text(i));
            let vals = [
                Datum::from_i64(i as i64 * 7 - 3),
                text_datum(&d, &mut keep),
                Datum::from_i64((i % 100_003) as i64),
                text_datum(&u, &mut keep),
            ];
            w.append_row(&vals, &[false; 4]).unwrap();
            keep.clear();
        }
        w.finish().unwrap();
        Part::open(path, 4).unwrap().unwrap()
    }

    // Serial reference: decode through the same ColDecode vocabulary,
    // one fresh scratch, refs in order — what gather_row publishes.
    fn reference_rows(
        part: &Part,
        refs: &[(u32, u32)],
        needed_idx: &[u16],
        coltypes: &[ColType],
    ) -> Vec<Vec<Vec<u8>>> {
        let mut cds: Vec<ColDecode> =
            (0..coltypes.len()).map(|_| new_col_decode()).collect();
        let mut out = Vec::new();
        for &(rg, row) in refs {
            let g = row as usize / GRANULE_ROWS;
            let r = row as usize % GRANULE_ROWS;
            let mut vals = Vec::new();
            for &c in needed_idx {
                decode_col(part, rg as usize, g, c as usize, &mut cds[c as usize]);
                let d = cds[c as usize].datum(r);
                if coltypes[c as usize].is_text() {
                    let p = d.as_usize() as *const u8;
                    let len = unsafe { ::types_tuple::varatt::varsize_any(p) };
                    vals.push(unsafe { core::slice::from_raw_parts(p, len) }.to_vec());
                } else {
                    // Full 8-byte Datum word (as_usize() is 4 bytes on wasm32).
                    vals.push(d.as_u64().to_le_bytes().to_vec());
                }
            }
            out.push(vals);
        }
        out
    }

    fn pipeline_rows(
        part: &Part,
        refs: &[(u32, u32)],
        needed_idx: &[u16],
        coltypes: &[ColType],
        pool: usize,
    ) -> Vec<Vec<Vec<u8>>> {
        let mut out = Vec::new();
        let mut on_task = |task: &AnalyzeTask, to: &AnalyzeTaskOut| -> bool {
            let nneed = needed_idx.len();
            for i in 0..(task.hi - task.lo) {
                let mut vals = Vec::new();
                for (j, &c) in needed_idx.iter().enumerate() {
                    let w = to.words[i * nneed + j];
                    if coltypes[c as usize].is_text() {
                        let p = unsafe { to.bytes.as_ptr().add(w as usize) };
                        assert_eq!(w % 4, 0, "text image offset must be 4-aligned");
                        let len = unsafe { ::types_tuple::varatt::varsize_any(p) };
                        vals.push(unsafe { core::slice::from_raw_parts(p, len) }.to_vec());
                    } else {
                        vals.push((w as usize).to_le_bytes().to_vec());
                    }
                }
                out.push(vals);
            }
            true
        };
        analyze_gather_pipeline(part, refs, needed_idx, coltypes, pool, &mut on_task);
        out
    }

    fn sample_refs(part: &Part, stride: usize) -> Vec<(u32, u32)> {
        let mut refs = Vec::new();
        for rg in 0..part.rgs.len() {
            let n = part.rgs[rg].nrows as usize;
            // Boundary rows + a stride walk (granule first/last rows, RG
            // first/last rows, granule-crossing spacing).
            let mut rows: Vec<usize> = vec![0, 1, GRANULE_ROWS - 1, GRANULE_ROWS, n - 1];
            let mut r = stride % 977;
            while r < n {
                rows.push(r);
                r += stride;
            }
            rows.sort_unstable();
            rows.dedup();
            refs.extend(rows.into_iter().filter(|&r| r < n).map(|r| (rg as u32, r as u32)));
        }
        refs
    }

    #[test]
    fn pipeline_matches_serial_reference_across_pools() {
        let path = tmp("pools");
        let part = build_part(&path);
        let coltypes =
            vec![ColType::I64, ColType::Text, ColType::I32, ColType::Text];
        let needed_idx: Vec<u16> = vec![0, 1, 2, 3];
        for stride in [3_333usize, 8_192, 12_345] {
            let refs = sample_refs(&part, stride);
            let want = reference_rows(&part, &refs, &needed_idx, &coltypes);
            for pool in [1usize, 3, 7] {
                let got = pipeline_rows(&part, &refs, &needed_idx, &coltypes, pool);
                assert_eq!(want.len(), got.len(), "stride {stride} pool {pool}");
                assert_eq!(want, got, "stride {stride} pool {pool}");
            }
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn pipeline_subset_needed_and_empty_refs() {
        let path = tmp("subset");
        let part = build_part(&path);
        let coltypes =
            vec![ColType::I64, ColType::Text, ColType::I32, ColType::Text];
        // Text-only needed subset (the copy-out path alone).
        let needed_idx: Vec<u16> = vec![1, 3];
        let refs = sample_refs(&part, 5_000);
        let want = reference_rows(&part, &refs, &needed_idx, &coltypes);
        let got = pipeline_rows(&part, &refs, &needed_idx, &coltypes, 2);
        assert_eq!(want, got);
        // Empty refs: no tasks, no hang.
        let got = pipeline_rows(&part, &[], &needed_idx, &coltypes, 2);
        assert!(got.is_empty());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn pipeline_early_stop_terminates() {
        let path = tmp("stop");
        let part = build_part(&path);
        let coltypes =
            vec![ColType::I64, ColType::Text, ColType::I32, ColType::Text];
        let needed_idx: Vec<u16> = vec![0, 1, 2, 3];
        let refs = sample_refs(&part, 2_000);
        let mut seen = 0usize;
        let mut on_task = |_t: &AnalyzeTask, _o: &AnalyzeTaskOut| -> bool {
            seen += 1;
            seen < 3
        };
        analyze_gather_pipeline(&part, &refs, &needed_idx, &coltypes, 4, &mut on_task);
        assert_eq!(seen, 3);
        std::fs::remove_file(&path).unwrap();
    }
}
