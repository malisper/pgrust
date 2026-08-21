//! The SELECT face: a serial/parallel granule-at-a-time scan over the
//! frozen M3-F reader, feeding Virtual slots (the tableam
//! `TableScanDesc::Pgrcolumnar2` arm drives [`Pgrc2ScanDescData::
//! getnextslot`]).
//!
//! This is the DDL lane's CORRECTNESS scan (full decode of every column,
//! the FULL codec-registry binding — the M3-J election wiring writes real
//! elections, so this scan resolves the same vtables lx_source's
//! implementor does). Pruning, late materialization, dict lanes, and morsel-grain
//! parallel claims are M3-G's `lx_source` implementor; this scan exists so
//! CREATE→COPY→SELECT round-trips in a real backend and COPY TO works,
//! with parallel workers claiming at PART grain via the shared
//! `phs_nallocated` cursor (the old-AM parallelscan shape).
//!
//! Every part open is recorded in [`crate::inval`]'s relid ledger — the
//! ino-reuse hole closes at scan-begin recording + relcache-callback
//! invalidation.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use pgrc2_format::abi::{ByteArena, DecodeOut, ValidityVerdict};
use pgrc2_format::class::ColSchema;
use pgrc2_format::dirlayout::part_file_name;
use pgrc2_format::geom::GRANULE_ROWS;
use pgrc2_format::ident::schema_fingerprint;
use pgrc2_format::part::StreamSectionHdr;
use pgrc2_read::registry::PartKey;
use pgrc2_read::{
    resolve_effective, CodecBinding, OpenPart, PartExpect, PartPin, ReadError, SectionUnwrapper,
    StreamCursor, TableExpect, VfsPartIo, VfsTableDir,
};
use tableam_vocab::TableScanDescData;
use types_error::{PgError, PgResult};
use types_slot::SlotData;

use crate::inval::RecoveryProbe;
use crate::probe::{ClogTxnProbe, SnapshotCommitCheck};
use crate::read_error;
use pgrc2_read::{CommitCheck, EffectiveManifest};

// ---------------------------------------------------------------------------
// the product codec binding (the lx_source `pgrc_codec_binding` shape,
// mirrored: M3-C's full kernel registry + the LZ4 section unwrapper).
// The M3-J election wiring makes COPY-written parts carry real elections,
// so the DDL lane's correctness scan must resolve the SAME vtables a
// conformant reader does — the reference binding reads only the
// VERBATIM/CONST election set and refuses everything else.
// ---------------------------------------------------------------------------

struct Lz4Unwrapper;

impl SectionUnwrapper for Lz4Unwrapper {
    fn wrapper(&self) -> pgrc2_format::enc::Wrapper {
        pgrc2_format::enc::Wrapper::Lz4
    }
    fn unwrap_section(
        &self,
        _hdr: &StreamSectionHdr,
        section: &[u8],
    ) -> pgrc2_format::FormatResult<Vec<u8>> {
        let mut out = Vec::new();
        pgrc2_codec::wrapper::unwrap_section(section, &mut out)?;
        Ok(out)
    }
}

/// Zstd section unwrap (spec §6.4 `wrapper = 2`, the CMP-A slot-fill) —
/// same codec entry point; the wrapper byte in the section header selects
/// the arm.
struct ZstdUnwrapper;

impl SectionUnwrapper for ZstdUnwrapper {
    fn wrapper(&self) -> pgrc2_format::enc::Wrapper {
        pgrc2_format::enc::Wrapper::Zstd
    }
    fn unwrap_section(
        &self,
        _hdr: &StreamSectionHdr,
        section: &[u8],
    ) -> pgrc2_format::FormatResult<Vec<u8>> {
        let mut out = Vec::new();
        pgrc2_codec::wrapper::unwrap_section(section, &mut out)?;
        Ok(out)
    }
}

static LZ4_UNWRAPPER: Lz4Unwrapper = Lz4Unwrapper;
static ZSTD_UNWRAPPER: ZstdUnwrapper = ZstdUnwrapper;
static UNWRAPPERS: [&dyn SectionUnwrapper; 2] = [&LZ4_UNWRAPPER, &ZSTD_UNWRAPPER];

/// Init-once through `pgsync::OnceLock` (the single lock library; pure init
/// closure — the codec registry's own once-cell posture).
fn codec_binding() -> &'static CodecBinding<'static> {
    static BINDING: pgsync::OnceLock<CodecBinding<'static>> = pgsync::OnceLock::new();
    BINDING.get_or_init(|| CodecBinding {
        registry: pgrc2_codec::registry(),
        unwrappers: &UNWRAPPERS,
    })
}

/// DM-2: load one part's delete vector when its manifest record references
/// one (`dv_gen != 0`). The Dv is AUTHORITATIVE sidecar state (spec
/// §15/§16): the manifest triplet (`dv_gen`, `dv_len`, `dv_crc`) validates
/// the payload, and ANY miss — absent file, stale envelope, length or crc
/// mismatch, internal inconsistency — is a typed CORRUPTION refusal, never
/// a silent Dv-free scan (which would resurrect deleted rows).
fn load_part_deletes(
    dir: &str,
    rec: &pgrc2_format::manifest::PartRecord,
) -> PgResult<Option<Arc<pgrc2_scan::PartDeletes>>> {
    if rec.dv_gen == 0 {
        return Ok(None);
    }
    let corrupt = |detail: String| {
        Box::new(
            PgError::error(format!(
                "pgrcolumnar2: part {} delete vector (gen {}): {detail}",
                rec.part_no, rec.dv_gen
            ))
            .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED),
        )
    };
    let consult = pgrc2_read::sidecar::read_sidecar(
        &VfsTableDir::new(dir.to_string()),
        rec.part_no,
        pgrc2_format::sidecar::SidecarKind::Dv,
        rec.dv_gen,
        None,
        None,
    )
    .map_err(read_error)?;
    let payload = match consult {
        pgrc2_read::sidecar::SidecarConsult::Hit { payload, .. } => payload,
        pgrc2_read::sidecar::SidecarConsult::Absent => {
            return Err(corrupt("sidecar file absent (manifest references it)".to_string()))
        }
        pgrc2_read::sidecar::SidecarConsult::Stale(why) => {
            return Err(corrupt(format!("sidecar envelope stale: {why}")))
        }
    };
    if payload.len() as u64 != rec.dv_len {
        return Err(corrupt(format!(
            "payload length {} vs manifest dv_len {}",
            payload.len(),
            rec.dv_len
        )));
    }
    if pgrc2_format::wire::crc32c(&payload) != rec.dv_crc {
        return Err(corrupt("payload crc vs manifest dv_crc".to_string()));
    }
    let deletes = pgrc2_scan::PartDeletes::from_dv_payload(
        &payload,
        rec.part_no,
        rec.dv_gen,
        rec.granule_count,
    )
    .map_err(|e| corrupt(format!("payload refused: {e}")))?;
    Ok(Some(Arc::new(deletes)))
}

/// The reader-side directory open: recovery-before-readers (the #480 gap
/// wiring), THEN the strict scan-free walk. Every pure-read entry (scan
/// begin, and through it ANALYZE/COPY TO) resolves the effective manifest
/// through here, so a reader can never observe an un-recovered crashed
/// directory — post-recovery, `ManifestMissing` is a true corruption
/// tripwire (spec §13.3 walk law).
pub(crate) fn resolve_for_scan(
    dir: &str,
    relfilenumber: u64,
    probe: &dyn RecoveryProbe,
    check: &dyn CommitCheck,
    expect: &TableExpect,
) -> PgResult<Option<EffectiveManifest>> {
    crate::inval::ensure_dir_recovered(relfilenumber, dir, probe)?;
    resolve_effective(&VfsTableDir::new(dir.to_string()), check, expect).map_err(read_error)
}

/// The cheap manifest-head probe (the sqe engine-registry currency check,
/// server-tax C8): read `CURRENT` alone and return the candidate
/// generation it points at — `Ok(None)` = no committed publish (absent
/// `CURRENT`/directory). ONE small whole-file read, no chain walk, no part
/// opens. The caller pairs the head generation with the commit-visibility
/// of the generation it CACHED (a cached engine is current iff the head
/// still names its generation AND its publisher is visible to the
/// statement's snapshot); any structural surprise falls back to the full
/// `resolve_for_scan` path, which owns the typed refusals. Callers must
/// only probe directories that already passed `ensure_dir_recovered` this
/// lifetime (the standing registry entry is the witness).
pub fn probe_manifest_head(dir: &str) -> PgResult<Option<u64>> {
    use pgrc2_read::TableDirIo as _;
    let cur = VfsTableDir::new(dir.to_string())
        .read_file(pgrc2_format::dirlayout::CURRENT_FILE_NAME)
        .map_err(read_error)?;
    let Some(bytes) = cur else { return Ok(None) };
    let ptr = pgrc2_format::manifest::CommitPointer::decode(&bytes)
        .map_err(|e| read_error(ReadError::Format(e)))?;
    Ok(Some(ptr.gen))
}

const VALIDITY_WORDS: usize = (GRANULE_ROWS as usize).div_ceil(64);
/// Initial per-column arena (grows on Bounds refusal, capped).
const ARENA_INITIAL: usize = 256 << 10;
const ARENA_MAX: usize = 512 << 20;

struct ScanPart {
    /// Pin held ONLY while this part is the scan's current part (the 100M
    /// lesson: pins are eviction-exempt, so a whole-table eager pin set
    /// drives resident bytes toward on-disk table size and the kernel
    /// OOM-kills the server). Identity was validated at `begin`; `expect` +
    /// `path` re-pin through the shared registry on (re)claim — a cache hit
    /// when the entry survived, a fresh identity-checked open otherwise.
    pin: Option<PartPin>,
    path: std::ffi::CString,
    expect: PartExpect,
    rows: u64,
    granules: u32,
    /// DM-2: this part's decoded delete vector (`None` = dv_gen == 0).
    /// The correctness scan skips deleted rows at the slot boundary — the
    /// one read path serves SELECT/COPY TO/ANALYZE Dv-applied.
    deletes: Option<Arc<pgrc2_scan::PartDeletes>>,
}

struct ColBuf {
    datums: Vec<u64>,
    /// u64-backed so the byte view is always 8-aligned (the ByteArena law).
    arena: Vec<u64>,
    validity: Vec<u64>,
    all_valid: bool,
}

impl ColBuf {
    fn new() -> ColBuf {
        ColBuf {
            datums: vec![0u64; GRANULE_ROWS as usize],
            arena: vec![0u64; ARENA_INITIAL / 8],
            validity: vec![0u64; VALIDITY_WORDS],
            all_valid: true,
        }
    }

    fn is_valid(&self, row: usize) -> bool {
        self.all_valid || (self.validity[row / 64] >> (row % 64)) & 1 == 1
    }
}

pub struct Pgrc2ScanDescData<'mcx> {
    pub rs_base: TableScanDescData<'mcx>,
    /// SO_TEMP_SNAPSHOT carrier (parallel worker scans register a restored
    /// snapshot; endscan unregisters it — the old-AM shape).
    pub rs_temp_snapshot: Option<std::rc::Rc<types_snapshot::SnapshotData<'static>>>,
    schemas: Vec<ColSchema>,
    parts: Vec<ScanPart>,
    binding: &'static CodecBinding<'static>,
    /// Current position: `cur_part == parts.len()` = exhausted;
    /// `cur_part == usize::MAX` = before first advance.
    cur_part: usize,
    cur_granule: u32,
    rows_in_granule: u32,
    row_cursor: u32,
    /// DM-2: the CURRENT granule's deletion mask (copied out of the
    /// part's `PartDeletes` at decode; `None` = nothing deleted here).
    cur_dv_mask: Option<Box<[u64; pgrc2_scan::DV_GRANULE_WORDS]>>,
    cols: Vec<ColBuf>,
}

impl<'mcx> Pgrc2ScanDescData<'mcx> {
    /// Open the scan: resolve the snapshot-effective manifest, validate +
    /// record every listed part through the shared registry (identity
    /// checked at open; keys recorded for invalidation), then release the
    /// pins — the scan holds at most ONE part pinned while decoding it.
    pub fn begin(rs_base: TableScanDescData<'mcx>) -> PgResult<Box<Pgrc2ScanDescData<'mcx>>> {
        crate::inval::ensure_inval_registered()?;
        let rel = &rs_base.rs_rd;
        let locator = rel.rd_locator.get();
        let relfilenumber = locator.relNumber as u64;
        let schemas = crate::schema::col_schemas(rel)?;
        let fp = schema_fingerprint(&schemas);
        let dir = crate::dirpath::table_dir_path(locator, rel.rd_backend);

        let snapshot = rs_base.rs_snapshot.as_deref();
        let check = SnapshotCommitCheck::new(snapshot);
        let expect = TableExpect {
            relfilenumber: Some(relfilenumber),
            spc_db: Some((locator.spcOid, locator.dbOid)),
            schema_fingerprint: Some(fp),
        };
        let recovery_probe = ClogTxnProbe::new();
        let eff = resolve_for_scan(&dir, relfilenumber, &recovery_probe, &check, &expect)?;
        check.take_error()?;

        let mut parts = Vec::new();
        if let Some(eff) = eff {
            let reg = crate::inval::registry();
            for rec in &eff.manifest.parts {
                let pexpect = PartExpect::from_manifest(
                    rec,
                    fp,
                    relfilenumber,
                    locator.spcOid,
                    locator.dbOid,
                );
                let path = format!("{dir}/{}", part_file_name(rec.part_no));
                let cpath = std::ffi::CString::new(path.clone()).map_err(|_| {
                    Box::new(PgError::error(format!(
                        "pgrcolumnar2: part path contains NUL: {path}"
                    )))
                })?;
                let pin = reg
                    .open_pinned(&pexpect, || {
                        Ok(Box::new(VfsPartIo::open(&cpath)?) as Box<dyn pgrc2_read::PartIo>)
                    })
                    .map_err(read_error)?;
                let ident = pin.part().ident();
                let key: PartKey = (ident.dev, ident.ino, ident.len);
                crate::inval::record_part_key(rel.rd_id, key);
                // Identity validated + recorded; the pin drops here (open
                // faults only header/footer — cheap cache residents). The
                // scan re-pins each part while it is CURRENT.
                drop(pin);
                parts.push(ScanPart {
                    pin: None,
                    path: cpath,
                    expect: pexpect,
                    rows: rec.rows,
                    granules: rec.granule_count,
                    deletes: load_part_deletes(&dir, rec)?,
                });
            }
        }

        let ncols = schemas.len();
        Ok(Box::new(Pgrc2ScanDescData {
            rs_base,
            rs_temp_snapshot: None,
            schemas,
            parts,
            binding: codec_binding(),
            cur_part: usize::MAX,
            cur_granule: 0,
            rows_in_granule: 0,
            row_cursor: 0,
            cur_dv_mask: None,
            cols: (0..ncols).map(|_| ColBuf::new()).collect(),
        }))
    }

    /// Restart (rescan). Parallel claim state is reset by
    /// `parallelscan_reinitialize`, not here. Pins release here and
    /// re-acquire lazily on the next claim.
    pub fn reset_position(&mut self) {
        let mut released = false;
        for p in &mut self.parts {
            released |= p.pin.take().is_some();
        }
        if released {
            crate::inval::registry().maintain();
        }
        self.cur_part = usize::MAX;
        self.cur_granule = 0;
        self.rows_in_granule = 0;
        self.row_cursor = 0;
        self.cur_dv_mask = None;
    }

    /// Pin part `idx` (cache hit when the entry survived the budget; a
    /// fresh identity-checked open otherwise) and hand back its OpenPart.
    fn ensure_pinned(&mut self, idx: usize) -> PgResult<Arc<OpenPart>> {
        if self.parts[idx].pin.is_none() {
            let pin = {
                let p = &self.parts[idx];
                crate::inval::registry()
                    .open_pinned(&p.expect, || {
                        Ok(Box::new(VfsPartIo::open(&p.path)?) as Box<dyn pgrc2_read::PartIo>)
                    })
                    .map_err(read_error)?
            };
            self.parts[idx].pin = Some(pin);
        }
        Ok(Arc::clone(
            self.parts[idx].pin.as_ref().expect("just pinned").part(),
        ))
    }

    /// Release part `idx`'s pin and let the janitor reclaim finished
    /// residents down to the budget (the scan is forward-only per claim).
    fn release_part(&mut self, idx: usize) {
        if idx < self.parts.len() && self.parts[idx].pin.take().is_some() {
            crate::inval::registry().maintain();
        }
    }

    fn claim_next_part(&mut self) -> Option<usize> {
        if let Some(pscan) = self.rs_base.rs_parallel {
            // Part-grain parallel claims over the shared cursor.
            // SAFETY: the parallel descriptor outlives every worker scan
            // (the tableam parallel contract); we only touch the atomic.
            let idx = unsafe {
                pscan
                    .as_ref()
                    .phs_nallocated
                    .fetch_add(1, Ordering::SeqCst)
            } as usize;
            if idx < self.parts.len() {
                Some(idx)
            } else {
                None
            }
        } else {
            let next = if self.cur_part == usize::MAX {
                0
            } else {
                self.cur_part + 1
            };
            if next < self.parts.len() {
                Some(next)
            } else {
                None
            }
        }
    }

    /// Advance to the next granule (possibly claiming the next part);
    /// false = scan exhausted.
    fn next_granule(&mut self) -> PgResult<bool> {
        loop {
            if self.cur_part != usize::MAX
                && self.cur_part < self.parts.len()
                && self.cur_granule + 1 < self.parts[self.cur_part].granules
            {
                self.cur_granule += 1;
            } else {
                match self.claim_next_part() {
                    Some(idx) => {
                        let prev = self.cur_part;
                        if prev != usize::MAX && prev < self.parts.len() && prev != idx {
                            self.release_part(prev);
                        }
                        self.cur_part = idx;
                        self.cur_granule = 0;
                        if self.parts[idx].granules == 0 {
                            continue;
                        }
                    }
                    None => {
                        let prev = self.cur_part;
                        if prev != usize::MAX && prev < self.parts.len() {
                            self.release_part(prev);
                        }
                        self.cur_part = self.parts.len();
                        return Ok(false);
                    }
                }
            }
            self.decode_current_granule()?;
            if self.rows_in_granule > 0 {
                self.row_cursor = 0;
                return Ok(true);
            }
        }
    }

    fn decode_current_granule(&mut self) -> PgResult<()> {
        let part: Arc<OpenPart> = self.ensure_pinned(self.cur_part)?;
        let g = self.cur_granule;
        let mut rows: u32 = 0;
        for (c, schema) in self.schemas.iter().enumerate() {
            let mut cursor = StreamCursor::open(Arc::clone(&part), self.binding, schema.attno, 0)
                .map_err(read_error)?;
            let g_rows = cursor.rows_in_granule(g);
            if c == 0 {
                rows = g_rows;
            } else if g_rows != rows {
                return Err(Box::new(PgError::error(format!(
                    "pgrcolumnar2: column {} granule {} row skew ({} vs {})",
                    schema.attno, g, g_rows, rows
                ))
                .with_sqlstate(types_error::ERRCODE_DATA_CORRUPTED)));
            }
            let buf = &mut self.cols[c];
            // decode_full with arena growth on typed exhaustion.
            loop {
                let arena_words = buf.arena.len();
                let (datums, arena) = (&mut buf.datums, &mut buf.arena);
                let arena_bytes = unsafe {
                    // SAFETY: see ColBuf::arena_bytes (split borrows force
                    // the inline form here).
                    core::slice::from_raw_parts_mut(
                        arena.as_mut_ptr() as *mut u8,
                        arena.len() * 8,
                    )
                };
                let mut out = DecodeOut {
                    datums: &mut datums[..g_rows as usize],
                    arena: ByteArena::new(arena_bytes),
                };
                match cursor.decode_full(g, &mut out) {
                    Ok(_) => break,
                    Err(ReadError::Format(pgrc2_format::FormatError::ArenaExhausted { .. }))
                        if arena_words * 8 < ARENA_MAX =>
                    {
                        let new_words = (arena_words * 2).min(ARENA_MAX / 8);
                        buf.arena = vec![0u64; new_words];
                    }
                    Err(e) => return Err(read_error(e)),
                }
            }
            match cursor.validity(g, &mut buf.validity).map_err(read_error)? {
                ValidityVerdict::AllValid => buf.all_valid = true,
                ValidityVerdict::Mixed { .. } => buf.all_valid = false,
            }
        }
        self.rows_in_granule = rows;
        // DM-2: stage the granule's deletion mask beside the decoded
        // columns (a 1 KiB copy at granule grain — never per row).
        self.cur_dv_mask = self.parts[self.cur_part]
            .deletes
            .as_ref()
            .and_then(|d| d.granule_mask(g))
            .map(|m| Box::new(*m));
        Ok(())
    }

    /// Fill `slot` with the next row; false = exhausted (slot marked
    /// empty). DM-2: deleted rows never surface (the one read path — the
    /// slot boundary is this scan's verdict layer).
    pub fn getnextslot(&mut self, slot: &mut SlotData<'_>) -> PgResult<bool> {
        loop {
            if self.row_cursor < self.rows_in_granule {
                let row = self.row_cursor as usize;
                self.row_cursor += 1;
                if let Some(mask) = &self.cur_dv_mask {
                    if pgrc2_scan::PartDeletes::is_deleted(mask, row as u32) {
                        continue;
                    }
                }
                self.store_row(row, slot);
                return Ok(true);
            }
            if self.cur_part == self.parts.len() && self.cur_part != usize::MAX {
                slot.base_mut().mark_empty();
                return Ok(false);
            }
            if !self.next_granule()? {
                slot.base_mut().mark_empty();
                return Ok(false);
            }
        }
    }

    fn store_row(&self, row: usize, slot: &mut SlotData<'_>) {
        let base = slot.base_mut();
        for (c, buf) in self.cols.iter().enumerate() {
            if buf.is_valid(row) {
                base.tts_isnull[c] = false;
                base.tts_values[c] = datum::Datum::from_u64(buf.datums[row]);
            } else {
                base.tts_isnull[c] = true;
                base.tts_values[c] = datum::Datum::null();
            }
        }
        base.tts_nvalid = self.schemas.len() as types_core::AttrNumber;
        base.mark_not_empty();
    }

    /// Total LIVE rows across the scan's parts (COPY TO progress, ANALYZE
    /// totalrows) — manifest facts minus Dv deletions, no pin needed.
    pub fn total_rows(&self) -> u64 {
        self.parts
            .iter()
            .map(|p| {
                p.rows.saturating_sub(
                    p.deletes.as_ref().map(|d| d.deleted_rows).unwrap_or(0),
                )
            })
            .sum()
    }
}

// ---------------------------------------------------------------------------
// The sqe engine-binding bridge (M4-S3 landing 2): open this relation's
// snapshot-effective part set as a `pgrc2_scan::TableScan` — the SAME
// resolve/validate/record walk `Pgrc2ScanDescData::begin` runs (identity
// checked through the shared registry, part keys recorded for
// invalidation), holding UNPINNED `Arc<OpenPart>`s (alive via Arc,
// evictable from the registry cache — never the 100M eager-pin posture;
// the pipeline's claim drive takes its own claim-scoped pins, PC-2.4).
// ---------------------------------------------------------------------------

/// The bridged scan + the facts the engine seam consults at lowering.
pub struct EngineBridgedScan {
    pub scan: pgrc2_scan::TableScan,
    /// Manifest row total (exact at seal).
    pub total_rows: u64,
    /// DM-2 witness input: any part carries a delete vector (`dv_gen != 0`).
    pub deletion_bearing: bool,
    /// The staged columns' schemas in staged order (profile derivation).
    pub schemas: Vec<ColSchema>,
    /// The snapshot-effective manifest (P2-1: `sqe::bank::Bank::from_bridge`
    /// consumes it — generation identity + part geometry).
    pub manifest: pgrc2_format::manifest::Manifest,
    /// The table directory path the parts were opened under.
    pub dir: String,
}

/// One staged column for the lane-capable bridge face: the root stream of
/// a catalog column, or a shredded-jsonb typed lane of it (TY-3 read
/// supply). Lane columns declare (dotted path, expected lane kind); the
/// scan resolves them per part and refuses typed where a part cannot
/// serve them (`ReadError::ShredLaneRefused` — see `pgrc2_scan`).
#[derive(Debug, Clone)]
pub struct EngineScanCol {
    /// 1-based catalog attno (the parent jsonb column for lane specs).
    pub attno: u32,
    /// `None` = the root column; `Some((path, kind))` = its shred lane.
    pub shred: Option<(String, pgrc2_format::shredlane::ShredLaneKind)>,
}

/// Open the engine TableScan over `rel` for the 1-based catalog attno set
/// `attnos` (staged order preserved; the ColSchema.attno domain).
/// `Ok(None)` = no effective manifest (an empty, never-ingested table) —
/// the caller declines. Non-word storage classes are the CALLER's to
/// refuse; this face stages whatever schema the catalog carries.
pub fn open_engine_table_scan(
    rel: &types_rel::Relation<'_>,
    snapshot: Option<&types_snapshot::SnapshotData<'_>>,
    attnos: &[u32],
    open_width: usize,
) -> PgResult<Option<EngineBridgedScan>> {
    let cols: Vec<EngineScanCol> = attnos
        .iter()
        .map(|&attno| EngineScanCol { attno, shred: None })
        .collect();
    open_engine_table_scan_cols(rel, snapshot, &cols, open_width)
}

/// The lane-capable bridge face: root columns AND shredded-jsonb lane
/// columns in one staged order (a lane column's schema is minted from its
/// declared kind — `pgrc2_format::shredlane`, the one authority — since
/// lanes are derived streams the catalog never carries). The parent of a
/// lane spec must be a varlena catalog column (jsonb); everything past
/// declaration is the scan's per-part resolution + typed-refusal law.
pub fn open_engine_table_scan_cols(
    rel: &types_rel::Relation<'_>,
    snapshot: Option<&types_snapshot::SnapshotData<'_>>,
    cols: &[EngineScanCol],
    open_width: usize,
) -> PgResult<Option<EngineBridgedScan>> {
    crate::inval::ensure_inval_registered()?;
    let locator = rel.rd_locator.get();
    let relfilenumber = locator.relNumber as u64;
    let all_schemas = crate::schema::col_schemas(rel)?;
    let fp = schema_fingerprint(&all_schemas);
    let dir = crate::dirpath::table_dir_path(locator, rel.rd_backend);

    let check = SnapshotCommitCheck::new(snapshot);
    let expect = TableExpect {
        relfilenumber: Some(relfilenumber),
        spc_db: Some((locator.spcOid, locator.dbOid)),
        schema_fingerprint: Some(fp),
    };
    let recovery_probe = ClogTxnProbe::new();
    let eff = resolve_for_scan(&dir, relfilenumber, &recovery_probe, &check, &expect)?;
    check.take_error()?;
    let Some(eff) = eff else { return Ok(None) };

    let reg = crate::inval::registry();
    let nparts = eff.manifest.parts.len();
    let mut parts: Vec<Arc<OpenPart>> = Vec::with_capacity(nparts);
    let mut deletes: Vec<Option<Arc<pgrc2_scan::PartDeletes>>> = Vec::with_capacity(nparts);
    let mut total_rows = 0u64;
    let mut deletion_bearing = false;
    // [coldopen] Part opens are independent four-pread validations against
    // manifest facts — embarrassingly parallel. Open+validate through the
    // shared registry (Mutex-protected) on plain worker threads at
    // `open_width`; every PG-coupled consequence (thread_local part-key
    // recording, Dv sidecar loads, error raising) stays on the calling
    // thread, in manifest order — identical results/ordering to the
    // serial loop, including which error surfaces first.
    let opened: Vec<pgrc2_read::ReadResult<Arc<OpenPart>>> = {
        let width = open_width.clamp(1, nparts.max(1));
        let recs = &eff.manifest.parts;
        let dirref: &str = &dir;
        // Path NUL-validation stays a main-thread PgError (byte-stable
        // with the serial loop's refusal).
        let mut cpaths: Vec<std::ffi::CString> = Vec::with_capacity(nparts);
        for rec in recs {
            let path = format!("{dirref}/{}", part_file_name(rec.part_no));
            cpaths.push(std::ffi::CString::new(path.clone()).map_err(|_| {
                Box::new(PgError::error(format!(
                    "pgrcolumnar2: part path contains NUL: {path}"
                )))
            })?);
        }
        let cpaths = &cpaths;
        let open_one = |i: usize| {
            let rec = &recs[i];
            let pexpect =
                PartExpect::from_manifest(rec, fp, relfilenumber, locator.spcOid, locator.dbOid);
            let cpath = &cpaths[i];
            let pin = reg.open_pinned(&pexpect, || {
                Ok(Box::new(VfsPartIo::open(cpath)?) as Box<dyn pgrc2_read::PartIo>)
            })?;
            // Keep the Arc, drop the pin (unpinned residency: alive,
            // evictable).
            Ok(Arc::clone(pin.part()))
        };
        if width <= 1 || nparts <= 1 {
            (0..nparts).map(open_one).collect()
        } else {
            let next = std::sync::atomic::AtomicUsize::new(0);
            let slots: Vec<std::sync::Mutex<Option<pgrc2_read::ReadResult<Arc<OpenPart>>>>> =
                (0..nparts).map(|_| std::sync::Mutex::new(None)).collect();
            std::thread::scope(|s| {
                for _ in 0..width {
                    s.spawn(|| loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if i >= nparts {
                            break;
                        }
                        let r = open_one(i);
                        *slots[i].lock().unwrap() = Some(r);
                    });
                }
            });
            slots
                .into_iter()
                .map(|m| m.into_inner().unwrap().expect("part-open slot filled"))
                .collect()
        }
    };
    for (rec, opened) in eff.manifest.parts.iter().zip(opened) {
        let part = opened.map_err(read_error)?;
        let ident = part.ident();
        let key: PartKey = (ident.dev, ident.ino, ident.len);
        crate::inval::record_part_key(rel.rd_id, key);
        total_rows += rec.rows;
        deletion_bearing |= rec.dv_gen != 0;
        // DM-2: the generation-chain read hands the scan its Dv (validated
        // against the manifest triplet — a deletion-bearing part scans
        // WITH its deletions applied, or refuses typed, never Dv-blind).
        deletes.push(load_part_deletes(&dir, rec)?);
        parts.push(part);
    }
    if !deletion_bearing {
        // The common shape: the empty vec (no per-part slots to consult).
        deletes.clear();
    }

    let mut schemas: Vec<ColSchema> = Vec::with_capacity(cols.len());
    let mut columns: Vec<pgrc2_scan::ScanColumn> = Vec::with_capacity(cols.len());
    for c in cols {
        let a = c.attno;
        let Some(s) = all_schemas.iter().find(|s| s.attno == a) else {
            return Err(Box::new(PgError::error(format!(
                "pgrcolumnar2 engine bridge: staged attno {a} outside the schema"
            ))));
        };
        match &c.shred {
            None => {
                schemas.push(*s);
                columns.push(pgrc2_scan::ScanColumn::root(*s, true));
            }
            Some((path, kind)) => {
                // Lane parents are varlena catalog columns (jsonb images);
                // any other class cannot carry shred lanes — declaration
                // error at the bridge, before any part is consulted.
                if s.class != pgrc2_format::class::StorageClass::VarlenaVerbatim {
                    return Err(Box::new(PgError::error(format!(
                        "pgrcolumnar2 engine bridge: shred lane declared on non-varlena attno {a}"
                    ))));
                }
                let col = pgrc2_scan::ScanColumn::shred_lane(a, path, *kind);
                schemas.push(col.schema);
                columns.push(col);
            }
        }
    }

    let manifest = eff.manifest.clone();
    let binding = codec_binding();
    Ok(Some(EngineBridgedScan {
        scan: pgrc2_scan::TableScan {
            parts,
            binding,
            unwrappers: binding.unwrappers,
            columns,
            predicate: None,
            strview_cols: vec![],
            code_key_cols: vec![],
            deletes,
            opts: pgrc2_scan::ScanOptions::default(),
        },
        total_rows,
        deletion_bearing,
        schemas,
        manifest,
        dir,
    }))
}
