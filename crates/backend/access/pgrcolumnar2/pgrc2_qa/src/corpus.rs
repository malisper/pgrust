//! Deterministic table fixtures with full oracles — the corpus every
//! battery shares. Parts are REAL: written by D's `TableWriter` through the
//! frozen seal/publish path, encoded by C's registry (forced elections per
//! class + the reference CONST/VERBATIM arms), read back through F's open +
//! cursor dispatch. The oracle is the row-major truth; the verifier is the
//! old-or-new checker engine of the crash legs and the wrong-silent-result
//! detector of the fuzzer.

use pgrc2_format::abi::{ByteArena, DecodeOut, ValidityVerdict};
use pgrc2_format::class::{ColSchema, StorageClass};
use pgrc2_format::dirlayout::part_file_name;
use pgrc2_format::manifest::Manifest;
use pgrc2_read::cursor::{CodecBinding, StreamCursor};
use pgrc2_read::io::MemPartIo;
use pgrc2_read::openpart::{OpenPart, PartExpect};
use pgrc2_read::ReadError;
use pgrc2_write::elect::{CandidateSource, ReferenceCandidates};
use pgrc2_write::ingest::{NoExternalDetoast, RawDatum};
use pgrc2_write::publish::{PublishOutcome, TxnProbe};
use pgrc2_write::shred::NoShred;
use pgrc2_write::writer::{PartCutPolicy, SealEnv, SubxactEvidence, TableWriter, TxnStamp};
use pgrc2_write::wvfs::WriteVfs;
use pgrc2_write::WriteResult;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::adapters::{full_binding, ForcedElection, ForcedPlan, QaResolver};
use crate::{bool_col, f64_col, fixed16_col, img_4b_u, int8_col, text_col};

/// One logical value in oracle currency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OracleVal {
    /// Word classes: the exact datum word (int as u64 bits, f64 to_bits,
    /// bool 0/1).
    Word(u64),
    /// Varlena payload bytes / fixed-length image bytes.
    Bytes(Vec<u8>),
}

pub type OracleCol = Vec<Option<OracleVal>>;

/// Row-major truth for a table — fixtures store it, the harness computes it.
pub trait RowOracle {
    fn schema(&self) -> &[ColSchema];
    fn value(&self, col: usize, row: u64) -> Option<OracleVal>;
}

/// A deterministic fixture table.
pub struct Fixture {
    pub name: &'static str,
    pub dir: String,
    pub spc: u32,
    pub db: u32,
    pub relfilenumber: u64,
    pub schema: Vec<ColSchema>,
    pub oracle: Vec<OracleCol>,
    pub plans: Vec<ForcedPlan>,
    pub policy: PartCutPolicy,
}

impl Fixture {
    pub fn rows(&self) -> u64 {
        self.oracle.first().map(|c| c.len() as u64).unwrap_or(0)
    }
}

impl RowOracle for Fixture {
    fn schema(&self) -> &[ColSchema] {
        &self.schema
    }
    fn value(&self, col: usize, row: u64) -> Option<OracleVal> {
        self.oracle[col][row as usize].clone()
    }
}

// ---------------------------------------------------------------------------
// writing fixtures through the real writer
// ---------------------------------------------------------------------------

/// Append every oracle row and publish one generation. The caller scripts
/// the probe (and marks `fxid` committed AFTER this returns — the commit
/// record is the caller's act, exactly like the real transaction layer).
pub fn write_fixture(
    vfs: &mut dyn WriteVfs,
    fx: &Fixture,
    fxid: u64,
    probe: &dyn TxnProbe,
) -> WriteResult<PublishOutcome> {
    let mut w = open_writer(fx, fxid)?;
    append_rows(vfs, &mut w, fx, 0, fx.rows())?;
    finish(vfs, &mut w, fx)?;
    w.publish(vfs, probe)
}

pub fn open_writer(fx: &Fixture, fxid: u64) -> WriteResult<TableWriter> {
    TableWriter::open(
        fx.dir.clone(),
        fx.schema.clone(),
        fx.spc,
        fx.db,
        fx.relfilenumber,
        TxnStamp { fxid, cid: 0 },
        &SubxactEvidence::default(),
        fx.policy,
    )
}

/// Append oracle rows [from, to) (the harness appends round slices).
pub fn append_rows(
    vfs: &mut dyn WriteVfs,
    w: &mut TableWriter,
    oracle: &dyn RowOracle,
    from: u64,
    to: u64,
) -> WriteResult<()> {
    append_rows_planned(vfs, w, oracle, from, to, &[])
}

/// Like [`append_rows`] with forced election plans (fixtures carry theirs
/// in `Fixture::plans`; use [`append_fixture_rows`] for those).
pub fn append_rows_planned(
    vfs: &mut dyn WriteVfs,
    w: &mut TableWriter,
    oracle: &dyn RowOracle,
    from: u64,
    to: u64,
    plans: &[ForcedPlan],
) -> WriteResult<()> {
    let forced: Vec<ForcedElection> = plans.iter().map(|p| ForcedElection::new(*p)).collect();
    let reference = ReferenceCandidates;
    let mut sources: Vec<&dyn CandidateSource> = Vec::with_capacity(forced.len() + 1);
    for f in &forced {
        sources.push(f);
    }
    sources.push(&reference);
    append_rows_with_sources(vfs, w, oracle, from, to, &sources)
}

/// The fully general append loop: explicit candidate-source order (the
/// metamorphic leg permutes it).
pub fn append_rows_with_sources(
    vfs: &mut dyn WriteVfs,
    w: &mut TableWriter,
    oracle: &dyn RowOracle,
    from: u64,
    to: u64,
    sources: &[&dyn CandidateSource],
) -> WriteResult<()> {
    let mut ext = NoExternalDetoast;
    let mut shred = NoShred;
    let opts = pgrc2_format::relopt::ShredOptions::default();
    let resolver = QaResolver;
    let schema: Vec<ColSchema> = oracle.schema().to_vec();
    let ncols = schema.len();
    for row in from..to {
        // Owned images for this row (varlena needs a real 4B-U header).
        let vals: Vec<Option<OracleVal>> = (0..ncols).map(|c| oracle.value(c, row)).collect();
        let imgs: Vec<Option<Vec<u8>>> = vals
            .iter()
            .zip(schema.iter())
            .map(|(v, s)| match (v, s.class) {
                (Some(OracleVal::Bytes(b)), StorageClass::VarlenaVerbatim) => {
                    Some(img_4b_u(b))
                }
                _ => None,
            })
            .collect();
        let datums: Vec<RawDatum<'_>> = vals
            .iter()
            .zip(imgs.iter())
            .map(|(v, img)| match (v, img) {
                (None, _) => RawDatum::Null,
                (Some(OracleVal::Word(wd)), _) => RawDatum::Word(*wd),
                (Some(OracleVal::Bytes(_)), Some(img)) => RawDatum::Bytes(img),
                (Some(OracleVal::Bytes(b)), None) => RawDatum::Bytes(b),
            })
            .collect();
        let mut env = SealEnv {
            vfs,
            sources,
            resolver: &resolver,
            shred: &mut shred,
            shred_opts: &opts,
        };
        w.append_row(&datums, &mut ext, &mut env)?;
    }
    Ok(())
}

pub fn append_fixture_rows(
    vfs: &mut dyn WriteVfs,
    w: &mut TableWriter,
    fx: &Fixture,
    from: u64,
    to: u64,
) -> WriteResult<()> {
    append_rows_planned(vfs, w, fx, from, to, &fx.plans)
}

pub fn finish(vfs: &mut dyn WriteVfs, w: &mut TableWriter, fx: &Fixture) -> WriteResult<()> {
    finish_planned(vfs, w, &fx.plans)
}

pub fn finish_planned(
    vfs: &mut dyn WriteVfs,
    w: &mut TableWriter,
    plans: &[ForcedPlan],
) -> WriteResult<()> {
    let forced: Vec<ForcedElection> = plans.iter().map(|p| ForcedElection::new(*p)).collect();
    let reference = ReferenceCandidates;
    let mut sources: Vec<&dyn CandidateSource> = Vec::with_capacity(forced.len() + 1);
    for f in &forced {
        sources.push(f);
    }
    sources.push(&reference);
    finish_with_sources(vfs, w, &sources)
}

pub fn finish_with_sources(
    vfs: &mut dyn WriteVfs,
    w: &mut TableWriter,
    sources: &[&dyn CandidateSource],
) -> WriteResult<()> {
    let mut shred = NoShred;
    let opts = pgrc2_format::relopt::ShredOptions::default();
    let resolver = QaResolver;
    let mut env = SealEnv {
        vfs,
        sources,
        resolver: &resolver,
        shred: &mut shred,
        shred_opts: &opts,
    };
    w.finish(&mut env)
}

// ---------------------------------------------------------------------------
// reading back: decode vs oracle
// ---------------------------------------------------------------------------

/// A verification failure (typed; the crash legs and fuzzer classify on it).
#[derive(Debug)]
pub enum QaIssue {
    /// The reader refused (typed error — the EXPECTED outcome on hostile
    /// inputs; a FAILURE when the input is a pristine or acked part).
    Read(ReadError),
    /// Decode succeeded but disagreed with the oracle.
    Mismatch { col: u32, row: u64, what: String },
    /// A kernel emitted a pointer-class datum outside its sanctioned
    /// regions — the decode arena, plus (dict-varlena streams only) the
    /// part-resident dict payload region (the zero-copy gather law on
    /// `DecodeOut`). A product bug by itself (memory-safety adjacent),
    /// never acceptable.
    OutOfArena { col: u32, granule: u32 },
    /// Structural disagreement (missing part file, row-count skew, …).
    Structure(String),
}

impl From<ReadError> for QaIssue {
    fn from(e: ReadError) -> QaIssue {
        QaIssue::Read(e)
    }
}

/// Open a part image (bytes) with synthetic identity.
pub fn open_part_bytes(bytes: &[u8], ino: u64) -> Result<Arc<OpenPart>, QaIssue> {
    Ok(Arc::new(OpenPart::open(
        Box::new(MemPartIo::new(bytes.to_vec(), 7, ino)),
        &PartExpect::none(),
    )?))
}

fn align8(n: usize) -> usize {
    n.div_ceil(8) * 8
}

/// The sanctioned pointer-class output regions for one decode call: the
/// caller's arena, plus — dict-varlena streams — the part-resident dict
/// payload region (the zero-copy gather law on `DecodeOut`). Half-open
/// `(lo, hi)` address ranges.
#[derive(Clone, Copy)]
struct PtrRegions {
    arena: (u64, u64),
    /// `None` for non-dict streams — varlena datums are then arena-only.
    dict: Option<(u64, u64)>,
}

impl PtrRegions {
    /// TRUE iff `[lo, hi)` lies wholly inside ONE sanctioned region
    /// (straddling two regions is as much a defect as escaping both).
    fn contains(&self, lo: u64, hi: u64) -> bool {
        let (a0, a1) = self.arena;
        if lo >= a0 && hi <= a1 {
            return true;
        }
        if let Some((d0, d1)) = self.dict {
            if lo >= d0 && hi <= d1 {
                return true;
            }
        }
        false
    }
}

/// Convert one VALID pointer-class datum into oracle currency, enforcing
/// the containment invariant (the shared tooth of `decode_column` and
/// `decode_sel_granule`; unit fires-proof in `containment_teeth`).
///
/// Fixed-class datums are ARENA-ONLY (the kernel copies fixed dict entries
/// through the arena); varlena datums may alias the dict payload region.
fn pointer_val(
    class: StorageClass,
    d: u64,
    regions: &PtrRegions,
    col: u32,
    granule: u32,
) -> Result<OracleVal, QaIssue> {
    let out_of = || QaIssue::OutOfArena { col, granule };
    match class {
        StorageClass::ByvalWord { .. }
        | StorageClass::F32
        | StorageClass::F64
        | StorageClass::Bool => Ok(OracleVal::Word(d)),
        StorageClass::Fixed { len } => {
            let len = len as u64;
            let arena_only = PtrRegions {
                arena: regions.arena,
                dict: None,
            };
            if !arena_only.contains(d, d + len) {
                return Err(out_of());
            }
            let bytes = unsafe { std::slice::from_raw_parts(d as *const u8, len as usize) };
            Ok(OracleVal::Bytes(bytes.to_vec()))
        }
        StorageClass::VarlenaVerbatim => {
            if !regions.contains(d, d + 4) {
                return Err(out_of());
            }
            let hdr = u32::from_le_bytes(unsafe { *(d as *const [u8; 4]) });
            let total = (hdr >> 2) as u64;
            if total < 4 || !regions.contains(d, d + total) {
                return Err(out_of());
            }
            let payload = unsafe {
                std::slice::from_raw_parts((d + 4) as *const u8, (total - 4) as usize)
            };
            Ok(OracleVal::Bytes(payload.to_vec()))
        }
    }
}

/// Decode one column of one part fully (all granules, values + validity)
/// into oracle currency, enforcing the arena-containment invariant.
pub fn decode_column(
    part: &Arc<OpenPart>,
    binding: &'static CodecBinding<'static>,
    schema: &ColSchema,
) -> Result<OracleCol, QaIssue> {
    let mut cur = StreamCursor::open(Arc::clone(part), binding, schema.attno, 0)?;
    let mut out_col: OracleCol = Vec::new();
    let granules = cur.granule_count();
    // Dict streams: the part-resident payload region is a sanctioned
    // zero-copy aliasing target for varlena datums (`DecodeOut` law).
    let dict_region = cur
        .dict_payload_bounds()?
        .map(|(base, len)| (base as u64, (base + len) as u64));
    for g in 0..granules {
        let rows = cur.rows_in_granule(g) as usize;
        // Size the arena from the part's own claim: worst case every row is
        // a pointer-class value; overflow values materialize fully. We use
        // a generous fixed budget + per-granule growth on demand.
        let mut arena_words: Vec<u64> = vec![0u64; 96 * 1024];
        let mut datums = vec![0u64; rows.max(1)];
        let value_bytes;
        loop {
            let arena_bytes = unsafe {
                std::slice::from_raw_parts_mut(
                    arena_words.as_mut_ptr() as *mut u8,
                    arena_words.len() * 8,
                )
            };
            let mut out = DecodeOut {
                datums: &mut datums[..rows],
                arena: ByteArena::new(arena_bytes),
            };
            match cur.decode_full(g, &mut out) {
                Ok(_n) => {
                    value_bytes = out.arena.used();
                    let _ = value_bytes;
                    break;
                }
                Err(ReadError::Format(pgrc2_format::FormatError::ArenaExhausted { needed }))
                    if arena_words.len() < 64 * 1024 * 1024 =>
                {
                    let grow = align8(needed) / 8 + arena_words.len() * 2;
                    arena_words = vec![0u64; grow];
                    continue;
                }
                Err(e) => return Err(e.into()),
            }
        }
        // Validity (the canonical face).
        let mut vwords = vec![0u64; rows.div_ceil(64).max(1)];
        let verdict = cur.validity(g, &mut vwords)?;
        let arena_lo = arena_words.as_ptr() as u64;
        let regions = PtrRegions {
            arena: (arena_lo, arena_lo + (arena_words.len() * 8) as u64),
            dict: dict_region,
        };
        for r in 0..rows {
            let valid = match verdict {
                ValidityVerdict::AllValid => true,
                ValidityVerdict::Mixed { .. } => (vwords[r / 64] >> (r % 64)) & 1 == 1,
            };
            if !valid {
                out_col.push(None);
                continue;
            }
            let val = pointer_val(schema.class, datums[r], &regions, schema.attno, g)?;
            out_col.push(Some(val));
        }
    }
    Ok(out_col)
}

/// decode_sel over one granule (selection given as row ordinals), in oracle
/// currency — the `decode_sel ≡ decode_full ∘ select` cross-check arm used
/// by the fuzzer's Ok tier and the granule-edge differentials.
pub fn decode_sel_granule(
    part: &Arc<OpenPart>,
    binding: &'static CodecBinding<'static>,
    schema: &ColSchema,
    g: u32,
    sel_rows: &[u16],
) -> Result<Vec<Option<OracleVal>>, QaIssue> {
    use pgrc2_format::abi::Selection;
    let mut cur = StreamCursor::open(Arc::clone(part), binding, schema.attno, 0)?;
    let rows = cur.rows_in_granule(g) as usize;
    let dict_region = cur
        .dict_payload_bounds()?
        .map(|(base, len)| (base as u64, (base + len) as u64));
    let n = sel_rows.len();
    let mut arena_words: Vec<u64> = vec![0u64; 96 * 1024];
    let mut datums = vec![0u64; n.max(1)];
    loop {
        let arena_bytes = unsafe {
            std::slice::from_raw_parts_mut(
                arena_words.as_mut_ptr() as *mut u8,
                arena_words.len() * 8,
            )
        };
        let mut out = DecodeOut {
            datums: &mut datums[..n],
            arena: ByteArena::new(arena_bytes),
        };
        match cur.decode_sel(g, &Selection { rows: sel_rows }, &mut out) {
            Ok(_) => break,
            Err(ReadError::Format(pgrc2_format::FormatError::ArenaExhausted { needed }))
                if arena_words.len() < 64 * 1024 * 1024 =>
            {
                let grow = align8(needed) / 8 + arena_words.len() * 2;
                arena_words = vec![0u64; grow];
                continue;
            }
            Err(e) => return Err(e.into()),
        }
    }
    let mut vwords = vec![0u64; rows.div_ceil(64).max(1)];
    let verdict = cur.validity(g, &mut vwords)?;
    let arena_lo = arena_words.as_ptr() as u64;
    let regions = PtrRegions {
        arena: (arena_lo, arena_lo + (arena_words.len() * 8) as u64),
        dict: dict_region,
    };
    let mut out_vals = Vec::with_capacity(n);
    for (k, &r) in sel_rows.iter().enumerate() {
        let valid = match verdict {
            ValidityVerdict::AllValid => true,
            ValidityVerdict::Mixed { .. } => {
                (vwords[(r as usize) / 64] >> (r % 64)) & 1 == 1
            }
        };
        if !valid {
            out_vals.push(None);
            continue;
        }
        let val = pointer_val(schema.class, datums[k], &regions, schema.attno, g)?;
        out_vals.push(Some(val));
    }
    Ok(out_vals)
}

/// Decode EVERY column of a part image; the fuzzer's Ok-arm workhorse.
pub fn decode_part(
    bytes: &[u8],
    schema: &[ColSchema],
    ino: u64,
) -> Result<Vec<OracleCol>, QaIssue> {
    let part = open_part_bytes(bytes, ino)?;
    let binding = full_binding();
    schema
        .iter()
        .map(|s| decode_column(&part, binding, s))
        .collect()
}

/// Verify a manifest's parts against a row oracle: every part file present,
/// decodable, and byte/word-equal to the oracle over its row range.
/// Returns the total row count verified.
pub fn verify_manifest(
    files: &BTreeMap<String, Vec<u8>>,
    manifest: &Manifest,
    oracle: &dyn RowOracle,
) -> Result<u64, QaIssue> {
    let schema = oracle.schema();
    let mut base: u64 = 0;
    for (i, rec) in manifest.parts.iter().enumerate() {
        let name = part_file_name(rec.part_no);
        let bytes = files.get(&name).ok_or_else(|| {
            QaIssue::Structure(format!("live part {name} missing from directory"))
        })?;
        if bytes.len() as u64 != rec.file_len {
            return Err(QaIssue::Structure(format!(
                "part {name}: file len {} != manifest {}",
                bytes.len(),
                rec.file_len
            )));
        }
        let cols = decode_part(bytes, schema, (i as u64) + 100)?;
        for (c, col) in cols.iter().enumerate() {
            if col.len() as u64 != rec.rows {
                return Err(QaIssue::Structure(format!(
                    "part {name} col {c}: decoded {} rows, manifest says {}",
                    col.len(),
                    rec.rows
                )));
            }
            for (r, got) in col.iter().enumerate() {
                let row = base + r as u64;
                let want = oracle.value(c, row);
                if *got != want {
                    return Err(QaIssue::Mismatch {
                        col: c as u32,
                        row,
                        what: format!("decoded {got:?} != oracle {want:?}"),
                    });
                }
            }
        }
        base += rec.rows;
    }
    Ok(base)
}

// ---------------------------------------------------------------------------
// the standard corpus
// ---------------------------------------------------------------------------

fn mix(i: u64) -> u64 {
    i.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(31) ^ i
}

fn int8_oracle(rows: u64, f: impl Fn(u64) -> Option<i64>) -> OracleCol {
    (0..rows).map(|i| f(i).map(|v| OracleVal::Word(v as u64))).collect()
}

/// A custom int8 fixture (the granule-edge and metamorphic legs size their
/// own).
pub fn int8_fixture(
    name: &'static str,
    relf: u64,
    rows: u64,
    plans: Vec<ForcedPlan>,
    policy: PartCutPolicy,
    f: impl Fn(u64) -> Option<i64>,
) -> Fixture {
    Fixture {
        name,
        dir: format!("/qa/t{relf}"),
        spc: 1663,
        db: 5,
        relfilenumber: relf,
        schema: vec![int8_col(1)],
        oracle: vec![int8_oracle(rows, f)],
        plans,
        policy,
    }
}

fn text_payload(i: u64) -> Vec<u8> {
    if i % 50 == 0 {
        // ≥ OVERSIZE_THRESHOLD (32 KiB) — routes through the overflow
        // region.
        let mut v = vec![0u8; 33_000];
        for (k, b) in v.iter_mut().enumerate() {
            *b = (mix(i).wrapping_add(k as u64) & 0xFF) as u8;
        }
        v
    } else if i % 7 == 0 {
        Vec::new()
    } else {
        let len = 5 + (mix(i) % 120) as usize;
        (0..len).map(|k| (mix(i ^ (k as u64)) & 0xFF) as u8).collect()
    }
}

/// The standard corpus: one fixture per encoding/storage arm.
pub fn standard_corpus() -> Vec<Fixture> {
    let mut out = Vec::new();
    // 1. BYTE_FOR at NARROW delta width 2 (re-armed: issue #465 fixed by
    // the M3-A2 amendment — seal keys width + verify on the elected key).
    // Frame-local ranges stay under 65,536 by construction.
    out.push(int8_fixture(
        "int8_bytefor_smallrange",
        101,
        12_500,
        vec![ForcedPlan::ByteFor {
            delta_width: 2,
            signed: true,
        }],
        PartCutPolicy::default(),
        |i| {
            if i % 7 == 3 {
                None
            } else {
                Some(1_000_000 + (i % 50_000) as i64)
            }
        },
    ));
    // 2. BYTE_FOR delta-width 8: full-range values.
    out.push(int8_fixture(
        "int8_bytefor_w8",
        102,
        12_500,
        vec![ForcedPlan::ByteFor {
            delta_width: 8,
            signed: true,
        }],
        PartCutPolicy::default(),
        |i| {
            if i % 11 == 5 {
                None
            } else {
                Some(mix(i) as i64)
            }
        },
    ));
    // 3. f64 with NaN payloads + infinities (bit-exactness law) under a
    // FORCED ALP election (re-armed: issue #465 fixed by M3-A2; the
    // self-describing frames carry specials as bit-exact exceptions).
    out.push(Fixture {
        name: "f64_specials_alp",
        dir: "/qa/t103".to_string(),
        spc: 1663,
        db: 5,
        relfilenumber: 103,
        schema: vec![f64_col(1)],
        oracle: vec![(0..12_500u64)
            .map(|i| {
                if i % 13 == 4 {
                    None
                } else if i % 997 == 0 {
                    Some(OracleVal::Word(0x7FF8_0000_0000_0000 | (i & 0xFFFF)))
                } else if i % 499 == 1 {
                    Some(OracleVal::Word(f64::INFINITY.to_bits()))
                } else {
                    Some(OracleVal::Word(((i as f64) * 0.01).to_bits()))
                }
            })
            .collect()],
        plans: vec![ForcedPlan::Alp],
        policy: PartCutPolicy::default(),
    });
    // 4. f64 over bit-random doubles under forced ALP_RD (re-armed per the
    // #465 fix).
    out.push(Fixture {
        name: "f64_random_alprd",
        dir: "/qa/t104".to_string(),
        spc: 1663,
        db: 5,
        relfilenumber: 104,
        schema: vec![f64_col(1)],
        oracle: vec![(0..9_000u64)
            .map(|i| Some(OracleVal::Word(mix(i))))
            .collect()],
        plans: vec![ForcedPlan::AlpRd],
        policy: PartCutPolicy::default(),
    });
    // 5. BOOL_BITMAP with nulls.
    out.push(Fixture {
        name: "bool_bitmap",
        dir: "/qa/t105".to_string(),
        spc: 1663,
        db: 5,
        relfilenumber: 105,
        schema: vec![bool_col(1)],
        oracle: vec![(0..12_500u64)
            .map(|i| {
                if i % 3 == 1 {
                    None
                } else {
                    Some(OracleVal::Word(mix(i) & 1))
                }
            })
            .collect()],
        plans: vec![ForcedPlan::Bool],
        policy: PartCutPolicy::default(),
    });
    // 6. Verbatim text incl. empties + oversize (overflow region).
    out.push(Fixture {
        name: "text_verbatim_overflow",
        dir: "/qa/t106".to_string(),
        spc: 1663,
        db: 5,
        relfilenumber: 106,
        schema: vec![text_col(1)],
        oracle: vec![(0..3_000u64)
            .map(|i| {
                if i % 5 == 2 {
                    None
                } else {
                    Some(OracleVal::Bytes(text_payload(i)))
                }
            })
            .collect()],
        plans: Vec::new(),
        policy: PartCutPolicy::default(),
    });
    // 7. Fixed(16).
    out.push(Fixture {
        name: "fixed16",
        dir: "/qa/t107".to_string(),
        spc: 1663,
        db: 5,
        relfilenumber: 107,
        schema: vec![fixed16_col(1)],
        oracle: vec![(0..9_500u64)
            .map(|i| {
                if i % 9 == 7 {
                    None
                } else {
                    let mut b = [0u8; 16];
                    b[..8].copy_from_slice(&mix(i).to_le_bytes());
                    b[8..].copy_from_slice(&mix(i ^ 0xABCD).to_le_bytes());
                    Some(OracleVal::Bytes(b.to_vec()))
                }
            })
            .collect()],
        plans: Vec::new(),
        policy: PartCutPolicy::default(),
    });
    // 8. CONST via the reference election (a real election win, no forcing).
    out.push(int8_fixture(
        "int8_const",
        108,
        9_000,
        Vec::new(),
        PartCutPolicy::default(),
        |_| Some(42),
    ));
    // 9. Mixed multi-column verbatim.
    out.push(Fixture {
        name: "multi_mixed_verbatim",
        dir: "/qa/t109".to_string(),
        spc: 1663,
        db: 5,
        relfilenumber: 109,
        schema: vec![
            int8_col(1),
            bool_col(2),
            text_col(3),
            fixed16_col(4),
            f64_col(5),
        ],
        oracle: vec![
            int8_oracle(10_000, |i| if i % 4 == 0 { None } else { Some(mix(i) as i64) }),
            (0..10_000u64)
                .map(|i| if i % 6 == 5 { None } else { Some(OracleVal::Word(mix(i) & 1)) })
                .collect(),
            (0..10_000u64)
                .map(|i| {
                    if i % 8 == 3 {
                        None
                    } else {
                        let len = (mix(i) % 40) as usize;
                        Some(OracleVal::Bytes(
                            (0..len).map(|k| (mix(i ^ (k as u64 + 7)) & 0xFF) as u8).collect(),
                        ))
                    }
                })
                .collect(),
            (0..10_000u64)
                .map(|i| {
                    if i % 10 == 9 {
                        None
                    } else {
                        let mut b = [0u8; 16];
                        b[..8].copy_from_slice(&mix(i + 3).to_le_bytes());
                        Some(OracleVal::Bytes(b.to_vec()))
                    }
                })
                .collect(),
            (0..10_000u64)
                .map(|i| {
                    if i % 12 == 11 {
                        None
                    } else {
                        Some(OracleVal::Word(((i as f64) * 1.5).to_bits()))
                    }
                })
                .collect(),
        ],
        plans: Vec::new(),
        policy: PartCutPolicy::default(),
    });
    // 10. Multi-part (small cut policy → 3 parts).
    out.push(int8_fixture(
        "int8_multipart",
        110,
        30_000,
        vec![ForcedPlan::ByteFor {
            delta_width: 8,
            signed: true,
        }],
        PartCutPolicy {
            max_rows: 12_288,
            max_bytes: u64::MAX / 2,
            cut_granule_rows: 12_288,
        },
        |i| if i % 5 == 4 { None } else { Some(mix(i) as i64) },
    ));
    out
}

/// A fixture published on a fresh SimVfs, everything committed: the fuzz
/// corpus builder + pristine-verification input.
pub struct BuiltFixture {
    pub fx: Fixture,
    pub files: BTreeMap<String, Vec<u8>>,
    pub manifest: Manifest,
}

pub fn build_fixture(fx: Fixture) -> BuiltFixture {
    use crate::adapters::Probe;
    use pgrc2_write::publish::TxnVerdict;

    let mut vfs = crate::simvfs::SimVfs::new();
    vfs.mkdir_path(&fx.dir).expect("mkdir");
    let fxid = 900 + fx.relfilenumber;
    let mut probe = Probe::new(TxnVerdict::Aborted);
    probe.mark(fxid, TxnVerdict::InProgress);
    write_fixture(&mut vfs, &fx, fxid, &probe).expect("fixture publish");
    probe.mark(fxid, TxnVerdict::Committed);
    let files = vfs.snapshot_dir(&fx.dir);
    let manifest = {
        use pgrc2_write::publish::effective_manifest;
        effective_manifest(&mut vfs, &fx.dir, &probe, None)
            .expect("effective")
            .expect("committed generation")
    };
    BuiltFixture {
        fx,
        files,
        manifest,
    }
}

pub fn build_standard_corpus() -> Vec<BuiltFixture> {
    standard_corpus().into_iter().map(build_fixture).collect()
}

// ---------------------------------------------------------------------------
// containment teeth (born-RED: the widened gate proven able to fire)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod containment_teeth {
    use super::*;

    /// A varlena image in a local buffer standing in for each region.
    fn image(buf: &mut [u8], payload: &[u8]) -> u64 {
        buf[..4].copy_from_slice(
            &pgrc2_format::wire::varlena_header_4b_u(payload.len() as u32).to_le_bytes(),
        );
        buf[4..4 + payload.len()].copy_from_slice(payload);
        buf.as_ptr() as u64
    }

    #[test]
    fn varlena_containment_fires_and_admits() {
        let mut arena = vec![0u8; 64];
        let mut dict = vec![0u8; 64];
        let mut stray = vec![0u8; 64];
        let a = image(&mut arena, b"in-arena");
        let d = image(&mut dict, b"in-dict");
        let s = image(&mut stray, b"stray");
        let regions = PtrRegions {
            arena: (arena.as_ptr() as u64, arena.as_ptr() as u64 + 64),
            dict: Some((dict.as_ptr() as u64, dict.as_ptr() as u64 + 64)),
        };
        let cls = StorageClass::VarlenaVerbatim;
        // Admits: arena-homed and dict-homed images.
        assert!(matches!(
            pointer_val(cls, a, &regions, 1, 0),
            Ok(OracleVal::Bytes(ref b)) if b == b"in-arena"
        ));
        assert!(matches!(
            pointer_val(cls, d, &regions, 1, 0),
            Ok(OracleVal::Bytes(ref b)) if b == b"in-dict"
        ));
        // Tooth 1: a datum outside BOTH regions fires.
        assert!(matches!(
            pointer_val(cls, s, &regions, 1, 0),
            Err(QaIssue::OutOfArena { col: 1, granule: 0 })
        ));
        // Tooth 2: without a dict region, the dict-homed image fires too —
        // the widening is dict-stream-scoped, not a blanket loosening.
        let no_dict = PtrRegions {
            arena: regions.arena,
            dict: None,
        };
        assert!(matches!(
            pointer_val(cls, d, &no_dict, 2, 3),
            Err(QaIssue::OutOfArena { col: 2, granule: 3 })
        ));
        // Tooth 3: fixed-class datums stay ARENA-ONLY even with a dict
        // region present (the kernel copies fixed dict entries).
        assert!(matches!(
            pointer_val(StorageClass::Fixed { len: 8 }, d, &regions, 4, 5),
            Err(QaIssue::OutOfArena { col: 4, granule: 5 })
        ));
        // Tooth 4: an image whose declared extent ESCAPES its region fires
        // (header at the region tail claiming bytes past the end).
        let tail = arena.as_ptr() as u64 + 60;
        unsafe {
            std::ptr::write_unaligned(
                tail as *mut u32,
                pgrc2_format::wire::varlena_header_4b_u(32).to_le(),
            );
        }
        assert!(matches!(
            pointer_val(cls, tail, &regions, 6, 7),
            Err(QaIssue::OutOfArena { col: 6, granule: 7 })
        ));
    }
}
