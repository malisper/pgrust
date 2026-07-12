//! Sequential scan: RG visibility, zone-map pruning, granule decode, window
//! staging for the page-batch executor drive (docs/design/cbstore-impl.md §7.3).

use ::datum::Datum;
use ::types_error::PgResult;
use ::types_slot::SlotData;

pub use ::tableam_vocab::{ZoneCmp, ZoneQual, ZoneVerdict};
use ::tableam_vocab::TableScanDescData;

use std::sync::atomic::Ordering;

use crate::format::*;
use crate::reader::Part;

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
    // CHUNK_FLAG_DICT_SORTED: codes are byte-rank order (dict range preds).
    dict_sorted: bool,
    // (rg, granule) this column's buffers hold; granule content per key is
    // immutable, so a matching key is valid across rescans. NONE_KEY = none.
    gkey: (u32, u32),
}

const NONE_KEY: (u32, u32) = (u32::MAX, u32::MAX);

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
        dict_sorted: false,
        gkey: NONE_KEY,
    }
}

fn decode_col(part: &Part, rg: usize, g: usize, c: usize, cd: &mut ColDecode) {
    if cd.gkey == (rg as u32, g as u32) {
        return;
    }
    if cd.dict_rg != rg {
        cd.dict.clear();
        cd.dict_rg = rg;
    }
    let chunk = part.chunk(rg, c);
    cd.is_dict = chunk.decode_granule_codes(g, &mut cd.codes, &mut cd.dict, &mut cd.arena);
    cd.dict_sorted = cd.is_dict && chunk.hdr.flags & CHUNK_FLAG_DICT_SORTED != 0;
    if !cd.is_dict {
        chunk.decode_granule(g, &mut cd.datums, &mut cd.dict, &mut cd.arena);
    }
    cd.gkey = (rg as u32, g as u32);
}

impl ColDecode {
    #[inline]
    fn datum(&self, row: usize) -> Datum {
        if self.is_dict {
            self.dict[self.codes[row] as usize]
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
/// is immutable and the scan pins its `Rc<Part>`, so the key is stable
/// across rescans). Slices live until the granule's next decode of a
/// different (rg, granule) key — granule-long, covering every window staged
/// from it.
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

// Zone-ordered adaptive traversal (docs/design/cbstore-zone-adaptive.md):
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
}

#[derive(Clone, Copy)]
struct AdaptiveEntry {
    rg: u32,
    g: u32,
    bound: i64,
}

pub struct CbScanDescData<'mcx> {
    pub rs_base: TableScanDescData<'mcx>,
    part: Option<std::rc::Rc<Part>>,
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
    // Post-qual materialization (cbstore_prewhere): granule decode is
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
    // pgstat-style counters for the verdict's bytes-read accounting.
    pub granules_pruned: u64,
    pub granules_scanned: u64,
    pub blocks_pruned: u64,
    pub windows_staged: u64,
    pub granules_bound_skipped: u64,
}

fn env_off(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("on"))
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
        part: Option<std::rc::Rc<Part>>,
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
            granules_pruned: 0,
            granules_scanned: 0,
            blocks_pruned: 0,
            windows_staged: 0,
            granules_bound_skipped: 0,
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
        }
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
                match chunk.hdr.encoding {
                    Encoding::Raw | Encoding::For | Encoding::Const => {}
                    _ => return Ok(false),
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
        self.adaptive =
            Some(Box::new(AdaptiveOrder { entries, cursor: 0, col, desc, strict, bound: None }));
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

    fn claim_next_rg(&mut self) -> usize {
        match self.rs_base.rs_parallel {
            Some(p) => {
                unsafe { p.as_ref() }.phs_nallocated.fetch_add(1, Ordering::SeqCst) as usize
            }
            None => {
                let r = self.serial_next;
                self.serial_next += 1;
                r
            }
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
        debug_assert!(self.zone_quals.is_empty());
        let mut scratch = (Vec::new(), Vec::new(), Vec::new());
        for rg in 0..part.rgs.len() {
            let rg_rows = part.rgs[rg].nrows;
            let ngranules = (rg_rows as usize).div_ceil(GRANULE_ROWS);
            if self.rg_wholly_visible(rg)? {
                out.rows += rg_rows as u64;
                for e in out.minmax.iter_mut() {
                    let (_, min, max) = part.rgs[rg].chunks[e.0 as usize];
                    e.1 = e.1.min(min);
                    e.2 = e.2.max(max);
                }
                if part.rgs[rg].flags & RG_FLAG_SUMS != 0 {
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
            for g in 0..ngranules {
                if !self.granule_zone_ok(rg, g) {
                    continue;
                }
                out.rows += (rg_rows as usize - g * GRANULE_ROWS).min(GRANULE_ROWS) as u64;
                for e in out.minmax.iter_mut() {
                    let ge = part.chunk(rg, e.0 as usize).granule(g);
                    e.1 = e.1.min(ge.min);
                    e.2 = e.2.max(ge.max);
                }
                sum_granule(part, rg, g, &mut out.sums, &mut scratch);
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

    // None = pruned (zone map, block zone maps, or bloom say no row can
    // match). Some(mask) = admitted; bit b covers rows [b*BLOCK_ROWS,
    // (b+1)*BLOCK_ROWS) of the granule. Bloom and block pruning are
    // advisory-only: admitted rows always get the ordinary qual evaluation.
    fn granule_admit(&self, rg: usize, g: usize, granule_rows: usize) -> Option<u32> {
        let part = self.part.as_ref().unwrap();
        let nblocks = granule_rows.div_ceil(BLOCK_ROWS);
        let mut mask: u32 = (1u32 << nblocks) - 1;
        for q in &self.zone_quals {
            let chunk = part.chunk(rg, (q.attnum - 1) as usize);
            let ge = chunk.granule(g);
            if !zone_can_match(q, ge.min, ge.max) {
                return None;
            }
            if matches!(q.op, ZoneCmp::Eq)
                && self.bloom_enabled
                && chunk.has_bloom()
                && !chunk.bloom_may_contain(g, q.val)
            {
                return None;
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
                    return None;
                }
            }
        }
        Some(mask)
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

    fn decode_current_granule(&mut self) {
        let part = self.part.as_ref().unwrap();
        let rg = self.rg;
        let g = self.granule;
        let nrows = part.rgs[rg].nrows as usize;
        self.granule_rows = (nrows - g * GRANULE_ROWS).min(GRANULE_ROWS);
        if !self.lazy {
            for (c, cd) in self.cols.iter_mut().enumerate() {
                if !self.needed[c] {
                    continue;
                }
                decode_col(part, rg, g, c, cd);
            }
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
        for &c in &self.needed_idx {
            decode_col(part, self.rg, self.granule, c as usize, &mut self.cols[c as usize]);
        }
        self.all_ready = key;
    }

    #[inline]
    fn ensure_col(&mut self, c: usize) {
        let part = self.part.as_ref().unwrap();
        decode_col(part, self.rg, self.granule, c, &mut self.cols[c]);
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
                self.rg = self.claim_next_rg();
                self.rg_claimed = true;
                self.granule = 0;
                self.win = 0;
                self.rg_checked = false;
                self.decoded = false;
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
            if !self.decoded {
                let grows = (rg_rows - self.granule * GRANULE_ROWS).min(GRANULE_ROWS);
                let Some(mask) = self.granule_admit(self.rg, self.granule, grows) else {
                    self.granules_pruned += 1;
                    self.granule += 1;
                    continue;
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
            let Some(mask) = self.granule_admit(self.rg, self.granule, grows) else {
                self.granules_pruned += 1;
                continue;
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

    /// Footer value min/max of the staged window's granule for column `c`;
    /// int-encoded chunks only (text granule entries carry byte lengths).
    /// The bounds cover the whole granule — a superset of any staged window
    /// inside it (cbstore stores no NULLs, so they bound every row).
    pub fn staged_window_value_minmax(&self, c: usize) -> Option<(i64, i64)> {
        if !self.decoded {
            return None;
        }
        let part = self.part.as_ref()?;
        let chunk = part.chunk(self.rg, c);
        match chunk.hdr.encoding {
            Encoding::Raw | Encoding::For | Encoding::Const => {}
            _ => return None,
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
            self.batch_deform_col(c, soa);
        }
    }

    /// Fill (or dict-answer) one staged column. Prewhere staged drives call
    /// this per clause so undeformed clauses' columns never decode; the
    /// caller owns soa.begin and the needed/fill-mask checks.
    pub fn batch_deform_col(&mut self, c: usize, soa: &mut ::exectuples::SoaBatch<'_>) {
        debug_assert!(self.needed[c]);
        let n = self.staged_rows;
        self.ensure_col(c);
        let cd = &self.cols[c];
        if cd.is_dict {
            let codes = &cd.codes[self.staged_lo..self.staged_lo + n];
            if soa.dict_want(c) {
                // Zero-decode dict lane: codes + RG dictionary + epoch =
                // rg index (dict content per RG is immutable and the scan
                // pins its Rc<Part>, so the epoch key is stable across
                // rescans). Values/isnull cells stay stale per the
                // set_dict_lane contract.
                soa.set_dict_lane(
                    c,
                    ::exectuples::SoaDictLane {
                        codes: codes.as_ptr(),
                        table: ::exectuples::SoaDictTable {
                            dict: cd.dict.as_ptr(),
                            ndict: cd.dict.len() as u32,
                            epoch: self.rg as u64,
                            sorted: cd.dict_sorted,
                        },
                    },
                );
                return;
            }
            // No dict-lane consumer for this column: one-instruction
            // escape, gather dict[code] into the Datum cells.
            for (out, &code) in soa.col_values_mut(c).iter_mut().zip(codes) {
                *out = cd.dict[code as usize];
            }
        } else {
            soa.col_values_mut(c).copy_from_slice(self.staged_col(c));
        }
        soa.col_isnull_mut(c).fill(false);
    }

    /// Fused-sort varlena key feed: staged text Datums into SoA column 0.
    pub fn batch_stage_varkey(&mut self, key: usize, soa: &mut ::exectuples::SoaBatch<'_>) {
        let n = self.staged_rows;
        soa.begin(n as u32);
        self.ensure_col(key);
        let cd = &self.cols[key];
        if cd.is_dict {
            let codes = &cd.codes[self.staged_lo..self.staged_lo + n];
            for (out, &code) in soa.col_values_mut(0).iter_mut().zip(codes) {
                *out = cd.dict[code as usize];
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
    /// content per RG is immutable and the scan pins its `Rc<Part>`, so the
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
        Some(CbDictLane {
            codes: &cd.codes[self.staged_lo..self.staged_lo + self.staged_rows],
            dict: &cd.dict,
            epoch: self.rg as u64,
            sorted: cd.dict_sorted,
        })
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
    /// for the life of the scan (the Part mmap). None = nothing staged.
    pub fn window_ref(&self) -> Option<(u32, u32)> {
        (self.rg_claimed && self.decoded && self.staged_rows > 0).then(|| {
            (self.rg as u32, (self.granule * GRANULE_ROWS + self.staged_lo) as u32)
        })
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
