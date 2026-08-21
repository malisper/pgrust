//! Bank opening: the standalone read spine. Resolve the effective manifest
//! through the CURRENT walk (every bank publisher committed — AllCommitted),
//! open every live part via VfsPartIo, and hold the codec binding (full
//! registry + LZ4/Zstd unwrappers — the pgrc2_qa `full_binding` shape).
//!
//! Port reshape (port-study/port-map.md §2): the schema is SUPPLIED by the
//! opener as `Vec<ColMeta>` carrying TypMeta — the PG catalog at P2-1, the
//! rig's cols.tsv stand-in until then. The engine never derives type facts
//! from column names. The bankstats plane consult is an open option, not
//! an env flag.

use crate::typmeta::TypMeta;
use pgrc2_format::class::StorageClass;
use pgrc2_format::dirlayout::part_file_name;
use pgrc2_format::enc::Wrapper;
use pgrc2_format::manifest::Manifest;
use pgrc2_format::part::StreamSectionHdr;
use pgrc2_format::FormatResult;
use pgrc2_read::cursor::{CodecBinding, SectionUnwrapper};
use pgrc2_read::io::{VfsPartIo, VfsTableDir};
use pgrc2_read::manifest_walk::{resolve_effective, AllCommitted, TableExpect};
use pgrc2_read::openpart::{OpenPart, PartExpect};
use std::ffi::CString;
use std::sync::Arc;

struct Lz4Unwrapper;
impl SectionUnwrapper for Lz4Unwrapper {
    fn wrapper(&self) -> Wrapper {
        Wrapper::Lz4
    }
    fn unwrap_section(&self, _hdr: &StreamSectionHdr, section: &[u8]) -> FormatResult<Vec<u8>> {
        let mut out = Vec::new();
        pgrc2_codec::wrapper::unwrap_section(section, &mut out)?;
        Ok(out)
    }
    // [stack] block-grain decode: the block-lazy dict payload plane.
    fn unwrap_block(&self, src: &[u8], dst: &mut [u8]) -> FormatResult<bool> {
        pgrc2_codec::wrapper::unwrap_block_into(Wrapper::Lz4, src, dst)?;
        Ok(true)
    }
}

struct ZstdUnwrapper;
impl SectionUnwrapper for ZstdUnwrapper {
    fn wrapper(&self) -> Wrapper {
        Wrapper::Zstd
    }
    fn unwrap_section(&self, _hdr: &StreamSectionHdr, section: &[u8]) -> FormatResult<Vec<u8>> {
        let mut out = Vec::new();
        pgrc2_codec::wrapper::unwrap_section(section, &mut out)?;
        Ok(out)
    }
    // [stack] block-grain decode: the block-lazy dict payload plane.
    fn unwrap_block(&self, src: &[u8], dst: &mut [u8]) -> FormatResult<bool> {
        pgrc2_codec::wrapper::unwrap_block_into(Wrapper::Zstd, src, dst)?;
        Ok(true)
    }
}

pub fn unwrappers() -> &'static [&'static dyn SectionUnwrapper] {
    static LZ4: Lz4Unwrapper = Lz4Unwrapper;
    static ZSTD: ZstdUnwrapper = ZstdUnwrapper;
    static SLOTS: [&dyn SectionUnwrapper; 2] = [&LZ4, &ZSTD];
    &SLOTS
}

pub fn binding() -> &'static CodecBinding<'static> {
    use std::sync::OnceLock;
    static B: OnceLock<CodecBinding<'static>> = OnceLock::new();
    B.get_or_init(|| CodecBinding {
        registry: pgrc2_codec::registry(),
        unwrappers: unwrappers(),
    })
}

/// One column of the opened relation (attno is 1-based). `typ` is the
/// typed-currency identity; `class` is the storage-class binding derived
/// from it (the writer's law: byval words carry signedness, varlena is
/// verbatim).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColMeta {
    pub attno: u32,
    /// Column name — RIG-ONLY currency (SQL front end name resolution).
    /// Engine paths key on attno + TypMeta; `.name ==` comparisons in the
    /// engine are a red-flag grep gate (risks.md §3).
    pub name: String,
    pub typ: TypMeta,
    pub class: StorageClass,
    /// [packednum] The PackedNumeric{scale} STORAGE witness: `Some(s)`
    /// iff EVERY part's Values stream for this column sealed the A6b
    /// PACKED_NUMERIC election at the SAME scale `s` (entry aux32 — the
    /// writer's sealed authority), or the bank has zero parts and the
    /// catalog typmod pins the scale. The word-lane face
    /// (`Face::PackedNumeric`) exists only under this witness; a
    /// varlena-demoted (mixed-dscale / special / over-budget) or
    /// mixed-scale-across-parts column stays `None` → Face::Varlena →
    /// typed refusals at the fold admissions.
    pub packed_scale: Option<i32>,
    /// [json-rung1] `Some` iff this is a jsonb SHRED-LANE VIRTUAL column
    /// (appended by `witness_jsonb_shred`, never in the catalog schema).
    /// Every stream/section consult for the column resolves through
    /// `Bank::stream_key` to (parent attno, this part's PathTable
    /// ordinal) — the lane rides the parent's stream family (spec §6.1).
    pub shred: Option<Arc<ShredRef>>,
}

/// [json-rung1] Storage address of one witnessed jsonb shred lane. The
/// witness is BANK-grain (exception-free seal law, shred_jsonb.rs): the
/// lane exists with the SAME kind in EVERY part — so its NULL geometry is
/// exactly SQL-NULL ∪ absent-path (the `->>`-class law) over the whole
/// relation, and typed reads never need a per-part fallback. A path any
/// part did not emit (absent, wrong-typed occurrence, jsonb null at the
/// path, over-budget election) witnesses nothing → typed refusal at the
/// seam, never a partial answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShredRef {
    /// The parent jsonb column (catalog attno) whose stream family
    /// carries the lane.
    pub base_attno: u32,
    /// The vendored `jsonb_shred` dotted path string (PathTable entry).
    pub path: String,
    /// Sealed lane kind, identical in every part. Rung-1 vocabulary:
    /// Text (verbatim jsonb string bytes == `->>` output) and NumericFs
    /// at sealed scale 0 (dscale-0 numerics: the i64 mantissa IS the
    /// value and `->>` renders plain digits — `fixed_scale_fit`'s
    /// dscale-exact law).
    pub kind: pgrc2_format::shredlane::ShredLaneKind,
    /// This lane's PathTable ordinal per part (Bank::parts order).
    pub per_part_ord: Vec<u32>,
}

impl ColMeta {
    /// Construct with a PLACEHOLDER storage class. The class recorded
    /// here is provisional: `Bank::open`/`Bank::from_bridge` overwrite it
    /// from the SEALED bank's own stream entries (the writer's ColSchema)
    /// — the SIGNED-flag law (type census 2026-08-17 CRITICAL): the
    /// engine never re-derives word signedness from TypMeta, because an
    /// unsigned-word column (oid/xid/"char") re-derived as signed would
    /// silently read high-bit values negative.
    ///
    /// The placeholder covers the FULL P1-1 face vocabulary (the census
    /// face-gap closure): bool / float4 / float8 / unsigned words /
    /// Fixed{len} (uuid) — so a zero-part bank (nothing to reconcile
    /// from) still carries the right face.
    pub fn new(attno: u32, name: &str, typ: TypMeta) -> ColMeta {
        use crate::typmeta::{is_unsigned_word, oids};
        let class = if typ.is_varlena() {
            StorageClass::VarlenaVerbatim
        } else if typ.is_fixed() {
            StorageClass::Fixed { len: typ.width as u32 }
        } else {
            match typ.oid {
                oids::BOOL => StorageClass::Bool,
                oids::FLOAT4 => StorageClass::F32,
                oids::FLOAT8 => StorageClass::F64,
                o if is_unsigned_word(o) => {
                    StorageClass::ByvalWord { width: typ.width as u8, signed: false }
                }
                _ => StorageClass::ByvalWord { width: typ.width as u8, signed: true },
            }
        };
        ColMeta { attno, name: name.to_string(), typ, class, packed_scale: None, shred: None }
    }
}

/// The DECODE face of a column, derived from the RECONCILED storage class
/// (the writer's sealed authority, never TypMeta) — what the kernels
/// consult to embed datums into the fold law's i64 word domain
/// (fold.rs face key laws).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Face {
    /// Sign-extend from `w` bytes (int2/4/8, date, timestamp...).
    SignedWord(u8),
    /// Zero-extend from `w` bytes (oid/xid/"char"; w <= 4 — a width-8
    /// unsigned word has no order-preserving i64 embed and refuses at
    /// lowering).
    UnsignedWord(u8),
    F32,
    F64,
    Bool,
    /// Fixed-length by-ref image of `len` bytes (uuid: 16, memcmp order).
    Fixed(u32),
    Varlena,
    /// [packednum] NUMERIC column whose EVERY part sealed the A6b
    /// PACKED_NUMERIC election at this shared scale (`ColMeta::
    /// packed_scale` witness). The word key is the exact scaled-integer
    /// mantissa `value·10^scale` (order == `cmp_numerics` at a shared
    /// scale — the vendored pin), so the ONE fold law serves
    /// sum/min/max/avg/count over real NUMERIC DDL; the render seam
    /// re-attaches the scale (`fixed_scale_unpack`/`_i128`,
    /// `numeric_avg_div`). NULL slots of a PackedNumeric decode carry NO
    /// image (raw mantissa word 0, not a pointer): `word_key` is
    /// valid-rows-only, same as every byte face.
    PackedNumeric { scale: i32 },
}

impl Face {
    pub fn of_class(class: StorageClass) -> Face {
        match class {
            StorageClass::ByvalWord { width, signed: true } => Face::SignedWord(width),
            StorageClass::ByvalWord { width, signed: false } => Face::UnsignedWord(width),
            StorageClass::F32 => Face::F32,
            StorageClass::F64 => Face::F64,
            StorageClass::Bool => Face::Bool,
            StorageClass::Fixed { len } => Face::Fixed(len),
            StorageClass::VarlenaVerbatim => Face::Varlena,
        }
    }
    /// Embed one decoded datum into the fold law's order-preserving i64
    /// word domain. Callers gate Fixed/Varlena (byte-fold faces) and
    /// width-8 unsigned (refused) before this.
    #[inline(always)]
    pub fn word_key(self, d: u64) -> i64 {
        match self {
            Face::SignedWord(w) => crate::stencils::sx(d, w),
            Face::UnsignedWord(w) => match w {
                1 => d as u8 as i64,
                2 => d as u16 as i64,
                4 => d as u32 as i64,
                _ => panic!("unsigned width-8 word has no i64 embed (refused at lowering)"),
            },
            Face::Bool => (d != 0) as i64,
            Face::F32 => crate::fold::f32_key(d),
            Face::F64 => crate::fold::f64_key(f64::from_bits(d)),
            // [packednum] Parse the decoded canonical image back to its
            // mantissa at the WITNESSED shared scale. Total under the
            // witness: every stored value fit `fixed_scale_fit` at its
            // part's elected scale == this scale (the seal's uniform-
            // dscale gate); a refusal here is a witness breach — loud.
            Face::PackedNumeric { scale } => {
                // SAFETY: valid-row decoded varlena datum (the same
                // contract every byte-face consumer relies on).
                let payload = unsafe { crate::scan::varlena_payload(d) };
                adt_numeric::fixed_scale_fit(adt_numeric::Num::from_payload(payload), scale)
                    .expect("packed-numeric witness breach: stored value refuses its sealed scale")
            }
            Face::Fixed(_) | Face::Varlena => panic!("byte face has no word key"),
        }
    }
    /// Is the fold-law word domain valid for this face?
    #[inline]
    pub fn word_foldable(self) -> bool {
        !matches!(self, Face::Fixed(_) | Face::Varlena | Face::UnsignedWord(8))
    }
}

/// The SIGNED-flag law applied: overwrite each schema column's storage
/// class from the opened parts' own stream entries (the writer's sealed
/// ColSchema — the one authority on word class). Zero-part banks keep the
/// opener's placeholder (no bytes exist to mis-read).
fn reconcile_classes(schema: &mut [ColMeta], parts: &[Arc<OpenPart>]) {
    use pgrc2_format::part::{StreamRole, STREAMF_SIGNED};
    let Some(p0) = parts.first() else { return };
    let Ok(dir) = p0.stream_directory() else { return };
    for c in schema.iter_mut() {
        if let Some(ps) = dir.lookup(c.attno, 0, StreamRole::Values) {
            let e = &ps.entry;
            // NB StreamEntry.width is the ENCODING width (spec §6.3 —
            // delta streams narrow it), so the LOGICAL byval width stays
            // the catalog's; the sealed entry supplies the class id and
            // the SIGNED flag (the bits TypMeta cannot know).
            let signed = e.flags & STREAMF_SIGNED != 0;
            let byval_width = match c.class {
                StorageClass::ByvalWord { width, .. } => width,
                _ => e.width,
            };
            if let Ok(class) =
                StorageClass::from_parts(e.class, byval_width, signed, e.fixed_len)
            {
                c.class = class;
            }
        }
    }
}

/// [packednum] The PackedNumeric witness pass: for each NUMERIC-typed
/// varlena column, `packed_scale = Some(s)` iff EVERY part's Values
/// stream entry sealed `EncodingId::PackedNumeric` with the SAME aux32
/// scale `s` (the writer's per-part election; scale domain-checked).
/// Zero-part banks witness from the catalog typmod (no bytes exist to
/// contradict it; every fold over them answers empty/NULL anyway, and
/// the render still needs the DDL scale). Any part that demoted
/// (VERBATIM: mixed dscale / specials / over-budget mantissa) or
/// elected a different scale withholds the witness — Face::Varlena —
/// and the fold admissions refuse typed.
fn witness_packed_numeric(schema: &mut [ColMeta], parts: &[Arc<OpenPart>]) {
    witness_packed_numeric_w(schema, parts, 1)
}

fn witness_packed_numeric_w(schema: &mut [ColMeta], parts: &[Arc<OpenPart>], threads: usize) {
    use crate::typmeta::{numeric_typmod_scale, oids};
    use pgrc2_format::enc::EncodingId;
    use pgrc2_format::part::StreamRole;
    // [coldopen] The witness consults every part's stream directory; on a
    // many-part bank that is a serial per-part section-fault chain. When a
    // NUMERIC candidate exists, prefetch the (per-part-cached) directories
    // part-parallel first — the serial pass below then answers from the
    // cache, byte-identically.
    if threads > 1
        && parts.len() > 1
        && schema
            .iter()
            .any(|c| c.typ.oid == oids::NUMERIC && c.class == StorageClass::VarlenaVerbatim)
    {
        let _ = crate::engine::par_parts_meta(threads, parts.len(), |pi| {
            let _ = parts[pi].stream_directory();
        });
    }
    for c in schema.iter_mut() {
        c.packed_scale = None;
        if c.typ.oid != oids::NUMERIC || c.class != StorageClass::VarlenaVerbatim {
            continue;
        }
        if parts.is_empty() {
            c.packed_scale = numeric_typmod_scale(c.typ.typmod);
            continue;
        }
        let mut scale: Option<i32> = None;
        let mut ok = true;
        for p in parts {
            let entry = p
                .stream_directory()
                .ok()
                .and_then(|d| d.lookup(c.attno, 0, StreamRole::Values).map(|ps| ps.entry));
            let Some(e) = entry else {
                ok = false;
                break;
            };
            if e.encoding != EncodingId::PackedNumeric.as_u16() {
                ok = false;
                break;
            }
            let s = e.aux32 as i32;
            // The vendored decode validates 0..=DSCALE_MAX; mirror it.
            if !(0..=0x3FFF).contains(&s) || scale.is_some_and(|w| w != s) {
                ok = false;
                break;
            }
            scale = Some(s);
        }
        if ok {
            c.packed_scale = scale;
        }
    }
}

/// [json-rung1] The jsonb shred-lane witness pass: append one VIRTUAL
/// ColMeta per (jsonb column, path) whose typed lane EVERY part sealed
/// with the SAME kind (the bank-grain witness). Rung-1 lane vocabulary:
/// Text, and NumericFs at sealed scale 0 with the SIGNED flag (the
/// dscale-exact `fixed_scale_fit` law: scale 0 ⟹ every occurrence is a
/// dscale-0 numeric ⟹ the i64 mantissa is the exact value and its `->>`
/// text is plain digits). Uuid16/Bool and non-zero scales stay outside —
/// no ColMeta, so the seam's lane lookup refuses typed.
///
/// Zero-part banks and parts without a PathTable witness NOTHING: the
/// lane law is existence-in-every-part, and existence needs at least one
/// sealed part to testify. Virtual attnos allocate above the catalog
/// max, deterministically (base attno asc, then path string asc).
fn witness_jsonb_shred(schema: &mut Vec<ColMeta>, parts: &[Arc<OpenPart>], threads: usize) {
    use crate::typmeta::oids;
    use pgrc2_format::part::{StreamRole, STREAMF_SIGNED};
    use pgrc2_format::shredlane::{numeric_lane_scale, ShredLaneKind};
    if parts.is_empty() {
        return;
    }
    let bases: Vec<u32> = schema
        .iter()
        .filter(|c| c.typ.oid == oids::JSONB && c.class == StorageClass::VarlenaVerbatim)
        .map(|c| c.attno)
        .collect();
    if bases.is_empty() {
        return;
    }
    // Per-part path tables. A part without one carries no lanes at all —
    // every witness is withheld (the all-parts law refuses bank-grain).
    // [coldopen] With jsonb candidates present, the per-part path-table
    // section reads (and the stream directories `lane_fact` consults) are
    // a serial cold chain; at width they fault part-parallel and the
    // witness passes below consume them in part order, byte-identically.
    let mut tables: Vec<Vec<String>> = Vec::with_capacity(parts.len());
    if threads > 1 && parts.len() > 1 {
        let read: Vec<Option<Vec<String>>> =
            crate::engine::par_parts_meta(threads, parts.len(), |pi| {
                let _ = parts[pi].stream_directory();
                pgrc2_read::streams::read_path_table(&parts[pi]).ok().flatten()
            });
        for t in read {
            match t {
                Some(t) => tables.push(t),
                None => return,
            }
        }
    } else {
        for p in parts {
            match pgrc2_read::streams::read_path_table(p) {
                Ok(Some(t)) => tables.push(t),
                _ => return,
            }
        }
    }
    // Sealed lane facts of (part, base, ord): kind + rung-1 admission.
    let lane_fact = |pi: usize, base: u32, ord: u32| -> Option<ShredLaneKind> {
        let dir = parts[pi].stream_directory().ok()?;
        let e = dir.lookup(base, ord, StreamRole::Values)?.entry;
        let kind = ShredLaneKind::of_entry(e.class, e.fixed_len)?;
        match kind {
            ShredLaneKind::Text => Some(kind),
            ShredLaneKind::NumericFs => {
                // Scale-0 + SIGNED: the mantissa word law. A pre-ruling
                // part (flag absent) or non-zero scale witnesses nothing.
                (e.flags & STREAMF_SIGNED != 0
                    && numeric_lane_scale(e.flags, e.aux32) == Ok(0))
                .then_some(kind)
            }
            ShredLaneKind::Uuid16 | ShredLaneKind::Bool => None,
        }
    };
    let mut next_attno = schema.iter().map(|c| c.attno).max().unwrap_or(0) + 1;
    let mut virt: Vec<ColMeta> = Vec::new();
    for &base in &bases {
        // Candidates from part 0, verified in every other part.
        let mut cands: Vec<(String, ShredLaneKind)> = Vec::new();
        for (i, path) in tables[0].iter().enumerate() {
            let Some(kind) = lane_fact(0, base, (i + 1) as u32) else { continue };
            cands.push((path.clone(), kind));
        }
        cands.sort_by(|a, b| a.0.cmp(&b.0));
        'cand: for (path, kind) in cands {
            let mut ords: Vec<u32> = Vec::with_capacity(parts.len());
            for (pi, table) in tables.iter().enumerate() {
                let Some(pos) = table.iter().position(|p| p == &path) else { continue 'cand };
                let ord = (pos + 1) as u32;
                if lane_fact(pi, base, ord) != Some(kind) {
                    continue 'cand;
                }
                ords.push(ord);
            }
            let (typ, class) = match kind {
                ShredLaneKind::Text => (
                    // `->>` output: text. The lane's byte order is plain
                    // memcmp (C collation currency, like every engine
                    // byte-text path).
                    TypMeta::varlena(oids::TEXT, crate::typmeta::COLLATION_C),
                    StorageClass::VarlenaVerbatim,
                ),
                ShredLaneKind::NumericFs => (
                    TypMeta::byval(oids::INT8, 8),
                    StorageClass::ByvalWord { width: 8, signed: true },
                ),
                _ => unreachable!("gated in lane_fact"),
            };
            virt.push(ColMeta {
                attno: next_attno,
                name: format!("$jsonb{base}.{path}"),
                typ,
                class,
                packed_scale: None,
                shred: Some(Arc::new(ShredRef {
                    base_attno: base,
                    path,
                    kind,
                    per_part_ord: ords,
                })),
            });
            next_attno += 1;
        }
    }
    schema.extend(virt);
}

/// Open options (SqeConfig side-channel free: no env reads in-crate).
#[derive(Debug, Clone, Default)]
pub struct OpenOpts {
    /// Consult a `bankstats-<gen>.pgrc2bs` bank-grain stats plane when
    /// present and valid (fmt-land default posture on the reference rig).
    pub bankstats: bool,
    /// [coldopen] Worker width for the part-open fan-out (0/1 = serial).
    /// Part opens are independent four-pread validations; the opener
    /// passes its pool width so a many-part bank opens at meta width.
    pub threads: usize,
}

pub struct Bank {
    pub dir: String,
    pub manifest: Manifest,
    pub parts: Vec<Arc<OpenPart>>,
    pub schema: Vec<ColMeta>,
    /// [fmt-layout] bank-grain stats plane (None = per-part sections).
    pub stats_plane: Option<Arc<crate::bankstats::Plane>>,
    /// Per-column null-freedom proofs, computed lazily once per open
    /// (sealed banks never change). `true` = no validity stream in any
    /// part, or every granule verdict AllValid — the lowering's admission
    /// fact for the null-blind stencil shapes and the dict lanes'
    /// zero-null proof. NOT NULL reference-workload columns answer from stream-
    /// directory metadata alone (no payload fault).
    nullfree: std::sync::Mutex<std::collections::HashMap<u32, bool>>,
    /// [coldopen] The width the bank opened at — the per-open fan-out
    /// election the demand-time proofs (null_free) reuse.
    open_width: usize,
    /// [json-rung4] Lane-grain stats plane: the bank-grain sidecar is
    /// root-attno keyed, so word-lane min/max consults re-faulted per-part
    /// Stats sections every statement (the faces memo clears per query).
    /// Word lanes aggregate ONCE here at open width; byte-identical (the
    /// same §8.1 section bodies, the same parse).
    lane_stats: Vec<(u32, Arc<crate::statsview::StatsView>)>,
}

impl Bank {
    pub fn open(dir: &str, schema: Vec<ColMeta>, opts: &OpenOpts) -> Bank {
        let t0 = std::time::Instant::now();
        let dirio = VfsTableDir::new(dir.to_string());
        let eff = resolve_effective(&dirio, &AllCommitted, &TableExpect::default())
            .expect("manifest walk refused (format skew? — see REFUSAL note in report)")
            .expect("no CURRENT/effective generation in bank dir");
        let m = eff.manifest;
        crate::coldledger::note(
            "open_manifest",
            format!("parts={}", m.parts.len()),
            t0,
            0,
            crate::coldledger::Reason::TouchedByQuery,
        );
        let t0 = std::time::Instant::now();
        // [coldopen] Part opens are embarrassingly parallel (four
        // validated preads each, no shared state): fan out at meta width.
        let parts: Vec<Arc<OpenPart>> =
            crate::engine::par_parts_meta(opts.threads.max(1), m.parts.len(), |pi| {
                let rec = &m.parts[pi];
                let path = format!("{}/{}", dir, part_file_name(rec.part_no));
                let c = CString::new(path.clone()).unwrap();
                let io = VfsPartIo::open(&c)
                    .unwrap_or_else(|e| panic!("open {path}: {e:?}"));
                let expect = PartExpect {
                    part_no: Some(rec.part_no),
                    rows: Some(rec.rows),
                    file_len: Some(rec.file_len),
                    footer_off: Some(rec.footer_off),
                    schema_fingerprint: Some(m.header.schema_fingerprint),
                    relfilenumber: Some(m.header.relfilenumber),
                    ..PartExpect::default()
                };
                Arc::new(
                    OpenPart::open(Box::new(io), &expect)
                        .unwrap_or_else(|e| panic!("part {path} REFUSED: {e:?}")),
                )
            });
        let open_bytes: u64 = parts
            .iter()
            .map(|p| p.faults().iter().map(|f| f.len).sum::<u64>())
            .sum();
        crate::coldledger::note(
            "open_parts",
            format!("parts={}", parts.len()),
            t0,
            open_bytes,
            crate::coldledger::Reason::TouchedByQuery,
        );
        // [fmt-layout] plane consult when armed by the opener; open
        // validates the identity witness against the JUST-resolved
        // manifest. Meta-only here; column payload faults happen lazily.
        let t0 = std::time::Instant::now();
        let stats_plane = if opts.bankstats {
            crate::bankstats::Plane::open(dir, &m)
        } else {
            None
        };
        crate::coldledger::note(
            "open_bankstats",
            format!("armed={}", opts.bankstats),
            t0,
            0,
            crate::coldledger::Reason::FlatStatsDerived,
        );
        let mut schema = schema;
        let t0 = std::time::Instant::now();
        reconcile_classes(&mut schema, &parts);
        witness_packed_numeric_w(&mut schema, &parts, opts.threads.max(1));
        witness_jsonb_shred(&mut schema, &parts, opts.threads.max(1));
        crate::coldledger::note(
            "open_schema",
            format!("cols={}", schema.len()),
            t0,
            0,
            crate::coldledger::Reason::TouchedByQuery,
        );
        let mut bank = Bank {
            dir: dir.to_string(),
            manifest: m,
            parts,
            schema,
            stats_plane,
            nullfree: Default::default(),
            open_width: opts.threads.max(1),
            lane_stats: Vec::new(),
        };
        bank.build_lane_stats();
        bank
    }

    /// Construct from an ALREADY-OPENED part set — the P2-1 AM bridge's
    /// entry (`pgrc2_am::scan::open_engine_table_scan_cols` resolved the
    /// snapshot-effective manifest, ran recovery-before-readers, and
    /// validated every part through the shared registry; this constructor
    /// re-walks nothing). Classes reconcile from the sealed entries (the
    /// SIGNED-flag law).
    pub fn from_bridge(
        dir: String,
        manifest: Manifest,
        parts: Vec<Arc<OpenPart>>,
        mut schema: Vec<ColMeta>,
        opts: &OpenOpts,
    ) -> Bank {
        let t0 = std::time::Instant::now();
        let stats_plane = if opts.bankstats {
            crate::bankstats::Plane::open(&dir, &manifest)
        } else {
            None
        };
        crate::coldledger::note(
            "open_bankstats",
            format!("armed={}", opts.bankstats),
            t0,
            0,
            crate::coldledger::Reason::FlatStatsDerived,
        );
        let t0 = std::time::Instant::now();
        reconcile_classes(&mut schema, &parts);
        witness_packed_numeric_w(&mut schema, &parts, opts.threads.max(1));
        witness_jsonb_shred(&mut schema, &parts, opts.threads.max(1));
        crate::coldledger::note(
            "open_schema",
            format!("cols={}", schema.len()),
            t0,
            0,
            crate::coldledger::Reason::TouchedByQuery,
        );
        let mut bank = Bank {
            dir,
            manifest,
            parts,
            schema,
            stats_plane,
            nullfree: Default::default(),
            open_width: opts.threads.max(1),
            lane_stats: Vec::new(),
        };
        bank.build_lane_stats();
        bank
    }

    /// A zero-part bank for a never-ingested relation (no effective
    /// manifest). Under R1 the engine ANSWERS empty relations — there is
    /// no incumbent to fall to (production-plan §P2-1; the lanev4
    /// `never-ingested` refusal is deleted, dispatch.md §3).
    pub fn empty(dir: String, schema: Vec<ColMeta>) -> Bank {
        let mut schema = schema;
        // [packednum] zero-part witness: the catalog typmod scale.
        witness_packed_numeric(&mut schema, &[]);
        Bank {
            dir,
            manifest: Manifest {
                header: pgrc2_format::manifest::ManifestHeader {
                    gen: 0,
                    prev_gen: 0,
                    publisher_fxid: 0,
                    relfilenumber: 0,
                    schema_fingerprint: 0,
                    magic: pgrc2_format::manifest::MANIFEST_MAGIC,
                    format_version: pgrc2_format::FORMAT_VERSION,
                    spc: 0,
                    db: 0,
                    part_count: 0,
                    next_part_no: 0,
                    flags: 0,
                    reserved: 0,
                },
                parts: Vec::new(),
            },
            parts: Vec::new(),
            schema,
            stats_plane: None,
            nullfree: Default::default(),
            open_width: 1,
            lane_stats: Vec::new(),
        }
    }

    pub fn rows_total(&self) -> u64 {
        self.manifest.parts.iter().map(|p| p.rows).sum()
    }

    /// Type identity of a column by attno (lowering-time consult).
    pub fn typ(&self, attno: u32) -> TypMeta {
        self.schema
            .iter()
            .find(|c| c.attno == attno)
            .unwrap_or_else(|| panic!("no attno {attno} in schema"))
            .typ
    }

    /// The reconciled decode face of a column (the writer's sealed class,
    /// never TypMeta-derived). [packednum] A witnessed PackedNumeric
    /// column faces its word lane; the storage class stays VARLENA (the
    /// decode ABI is unchanged) — only the fold-law embed changes.
    pub fn face(&self, attno: u32) -> Face {
        let c = self
            .schema
            .iter()
            .find(|c| c.attno == attno)
            .unwrap_or_else(|| panic!("no attno {attno} in schema"));
        if let Some(scale) = c.packed_scale {
            return Face::PackedNumeric { scale };
        }
        Face::of_class(c.class)
    }

    /// [json-rung1] Stream/section address of a column in one part:
    /// root columns are `(attno, path_ord 0)`; shred-lane virtual
    /// columns resolve to the PARENT jsonb attno + this part's PathTable
    /// ordinal (witnessed at open — every part has one). EVERY per-part
    /// stream or metadata-section consult routes through here so a lane
    /// column behaves as an ordinary column everywhere downstream.
    pub fn stream_key(&self, part_idx: usize, attno: u32) -> (u32, u32) {
        match self.shred_of(attno) {
            Some(s) => (s.base_attno, s.per_part_ord[part_idx]),
            None => (attno, 0),
        }
    }

    /// [json-rung4] Aggregate the word lanes' whole-bank stats views at
    /// open (StatsView::open fans out at open width). Root columns keep
    /// the sidecar plane; text lanes keep the lazy path (coarse keys
    /// answer no exact minmax anyway).
    fn build_lane_stats(&mut self) {
        let lanes: Vec<u32> = self
            .schema
            .iter()
            .filter(|c| {
                c.shred
                    .as_deref()
                    .is_some_and(|s| s.kind == pgrc2_format::shredlane::ShredLaneKind::NumericFs)
            })
            .map(|c| c.attno)
            .collect();
        if lanes.is_empty() {
            return;
        }
        let t0 = std::time::Instant::now();
        let w = self.open_width;
        let built: Vec<_> = lanes
            .iter()
            .map(|&a| (a, Arc::new(crate::statsview::StatsView::open(self, a, w))))
            .collect();
        self.lane_stats = built;
        crate::coldledger::note(
            "open_lanestats",
            format!("lanes={}", self.lane_stats.len()),
            t0,
            0,
            crate::coldledger::Reason::FlatStatsDerived,
        );
    }

    /// [json-rung4] The open-aggregated stats view of a word lane
    /// (None: not a lane column — the per-query faces memo serves).
    pub fn lane_stats(&self, attno: u32) -> Option<Arc<crate::statsview::StatsView>> {
        self.lane_stats.iter().find(|(a, _)| *a == attno).map(|(_, v)| Arc::clone(v))
    }

    /// [json-rung1] The shred ref of a virtual lane column (None for
    /// catalog columns).
    pub fn shred_of(&self, attno: u32) -> Option<&ShredRef> {
        self.schema
            .iter()
            .find(|c| c.attno == attno)
            .and_then(|c| c.shred.as_deref())
    }

    /// [json-rung1] Bank-grain lane lookup for the seam: the virtual
    /// column serving `(base jsonb column, path)` at `kind`, if the
    /// all-parts witness held at open. `None` = refuse typed at the
    /// seam (`type/jsonb/unshredded-path` — absent path, wrong-typed
    /// lane, non-zero scale, or a lane-free part).
    pub fn shred_lane(
        &self,
        base: u32,
        path: &str,
        kind: pgrc2_format::shredlane::ShredLaneKind,
    ) -> Option<&ColMeta> {
        self.schema.iter().find(|c| {
            c.shred
                .as_deref()
                .is_some_and(|s| s.base_attno == base && s.kind == kind && s.path == path)
        })
    }

    /// [coldopen] The open-time fan-out width (proof passes reuse it).
    pub fn open_width(&self) -> usize {
        self.open_width
    }

    /// Null-freedom proof for a column (lazy, cached per open). The
    /// lowering's admission fact: null-blind stencil shapes and dict
    /// lanes require it; the null-threaded shapes hoist it out of their
    /// row loops (the AllValid fast path — law 11).
    pub fn null_free(&self, attno: u32) -> bool {
        if let Some(&v) = self.nullfree.lock().unwrap().get(&attno) {
            return v;
        }
        let t0 = std::time::Instant::now();
        let v = crate::scan::assert_null_free(self, attno);
        crate::coldledger::note(
            "nullfree",
            format!("attno={attno}"),
            t0,
            0,
            crate::coldledger::Reason::TouchedByQuery,
        );
        self.nullfree.lock().unwrap().insert(attno, v);
        v
    }

    /// Name → ColMeta resolution — rig/lowering currency only.
    pub fn col(&self, name: &str) -> &ColMeta {
        self.schema
            .iter()
            .find(|c| c.name == name)
            .unwrap_or_else(|| panic!("no column {name}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::typmeta::oids;

    // -----------------------------------------------------------------
    // [packednum] witness + word-lane tests: seal a real bank through the
    // REAL writer (numeric posture on), open it through Bank::open, and
    // assert the PackedNumeric face law end to end.
    // -----------------------------------------------------------------

    fn numeric_img(s: &str) -> Vec<u8> {
        adt_numeric::io::numeric_in(s, -1, None)
            .expect("parse")
            .expect("non-soft")
            .as_bytes()
            .to_vec()
    }

    /// `mant` at `scale` as a decimal string with EXACTLY `scale`
    /// fraction digits (numeric_in then yields dscale == scale).
    fn dec_str(mant: i64, scale: i32) -> String {
        let neg = mant < 0;
        let mut a = mant.unsigned_abs().to_string();
        let s = scale as usize;
        if a.len() <= s {
            a = format!("{}{a}", "0".repeat(s + 1 - a.len()));
        }
        let dot = a.len() - s;
        let body = if s == 0 {
            a
        } else {
            format!("{}.{}", &a[..dot], &a[dot..])
        };
        if neg { format!("-{body}") } else { body }
    }

    /// Seal one numeric column (values from `f`; None = NULL) into a
    /// fresh bank dir under the target tmpdir; returns the dir.
    fn seal_numeric_bank(tag: &str, nrows: u64, f: impl Fn(u64) -> Option<String>) -> String {
        use pgrc2_format::class::{CollationClass, ColSchema, TypeSemantics};
        use pgrc2_write::elect::{CodecCandidates, ColumnPosture};
        use pgrc2_write::ingest::RawDatum;
        use pgrc2_write::publish::TxnVerdict;
        use pgrc2_write::seal::CodecResolver;
        use pgrc2_write::testkit::Probe;
        use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
        use pgrc2_write::wvfs::RealVfs;
        let dir = format!(
            "{}/sqe-packednum-{}-{}",
            std::env::temp_dir().display(),
            tag,
            std::process::id()
        );
        if std::path::Path::new(&dir).exists() {
            std::fs::remove_dir_all(&dir).expect("clear");
        }
        std::fs::create_dir_all(&dir).expect("mkdir");
        let schema = vec![ColSchema {
            attno: 1,
            class: StorageClass::VarlenaVerbatim,
            typlen: -1,
            typbyval: false,
            typalign: b'i',
            collation_class: CollationClass::C,
            semantics: TypeSemantics::NumericUnpacked,
        }];
        let cands = CodecCandidates::new(ColumnPosture::default()).with_column(
            1,
            0,
            ColumnPosture { numeric: true, ..ColumnPosture::default() },
        );
        let resolver = CodecResolver;
        let mut shred = pgrc2_write::shred::NoShred;
        let opts = pgrc2_format::relopt::ShredOptions::default();
        let mut ext = pgrc2_write::ingest::NoExternalDetoast;
        let mut vfs = RealVfs;
        let mut w = TableWriter::open(
            dir.clone(),
            schema,
            1663,
            5,
            777,
            TxnStamp { fxid: 100, cid: 1 },
            &Default::default(),
            PartCutPolicy { max_rows: 4096, max_bytes: u64::MAX, cut_granule_rows: 1024 },
        )
        .expect("open writer");
        for r in 0..nrows {
            let img = f(r).map(|s| numeric_img(&s));
            let d = match &img {
                Some(i) => RawDatum::Bytes(i),
                None => RawDatum::Null,
            };
            let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&cands];
            let mut env = SealEnv {
                vfs: &mut vfs,
                sources: &sources,
                resolver: &resolver,
                shred: &mut shred,
                shred_opts: &opts,
            };
            w.append_row(&[d], &mut ext, &mut env).expect("append");
        }
        {
            let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&cands];
            let mut env = SealEnv {
                vfs: &mut vfs,
                sources: &sources,
                resolver: &resolver,
                shred: &mut shred,
                shred_opts: &opts,
            };
            w.finish(&mut env).expect("finish");
        }
        let probe = Probe::new(TxnVerdict::Committed);
        w.publish(&mut vfs, &probe).expect("publish");
        dir
    }

    fn open_numeric_bank(dir: &str) -> Bank {
        Bank::open(
            dir,
            vec![ColMeta::new(1, "n", TypMeta::NUMERIC)],
            &OpenOpts::default(),
        )
    }

    #[test]
    fn packed_witness_word_lane_and_folds() {
        // Multi-part bank (part cut 4096 over 10k rows), NULLs threaded,
        // negatives + boundary mantissas at scale 2.
        let mant = |r: u64| -> Option<i64> {
            if r % 7 == 3 {
                return None; // NULL rows
            }
            Some(match r % 5 {
                0 => -(r as i64) * 37 - 1,
                1 => 0,
                2 => (r as i64) * 91 + 5,
                3 => -5,
                _ => 1_000_000 + r as i64,
            })
        };
        let dir =
            seal_numeric_bank("folds", 10_000, |r| mant(r).map(|m| dec_str(m, 2)));
        let bank = open_numeric_bank(&dir);
        assert!(bank.parts.len() > 1, "multi-part geometry expected");
        assert_eq!(bank.face(1), Face::PackedNumeric { scale: 2 });
        assert!(!bank.null_free(1));

        // Decode every granule; fold through THE fold law; compare with
        // the independent i128 recomputation from the generator.
        use crate::fold::{minmax_answer, scatter_cell_fold, AccumCell, AggFoldOp};
        let face = bank.face(1);
        let mut scr = crate::scan::Scratch::new();
        let (mut cs, mut cn, mut cx) =
            (AccumCell::default(), AccumCell::default(), AccumCell::default());
        let mut row_base = 0u64;
        for (pi, g, rows, _) in crate::scan::granule_walk(&bank, 1) {
            let mut cur = crate::scan::open_cursor(&bank, pi, 1);
            let gv = scr.validity(&mut cur, g, rows as usize);
            let d = scr.decode_full(&mut cur, g, rows as usize);
            let d: Vec<u64> = d.to_vec();
            for (r, &x) in d.iter().enumerate() {
                let valid = gv.all_valid() || scr.row_valid(r);
                if !valid {
                    continue;
                }
                let k = face.word_key(x);
                assert_eq!(Some(k), mant(row_base + r as u64), "mantissa at row");
                scatter_cell_fold(AggFoldOp::Sum, &mut cs, k, true);
                scatter_cell_fold(AggFoldOp::Min, &mut cn, k, true);
                scatter_cell_fold(AggFoldOp::Max, &mut cx, k, true);
            }
            row_base += rows as u64;
        }
        let (mut esum, mut emin, mut emax, mut en) = (0i128, i64::MAX, i64::MIN, 0i64);
        for r in 0..10_000u64 {
            if let Some(m) = mant(r) {
                esum += m as i128;
                emin = emin.min(m);
                emax = emax.max(m);
                en += 1;
            }
        }
        assert_eq!((cs.a, cs.b), (esum, en));
        assert_eq!(minmax_answer(&cn), Some(emin));
        assert_eq!(minmax_answer(&cx), Some(emax));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn mixed_dscale_demotes_and_withholds_witness() {
        // dscale 1 first value elects S=1; a later dscale-2 value refuses
        // → the stream demotes to VERBATIM → no witness, Face::Varlena.
        let dir = seal_numeric_bank("mixed", 2_000, |r| {
            Some(if r % 2 == 0 { dec_str(15 + r as i64, 1) } else { dec_str(125, 2) })
        });
        let bank = open_numeric_bank(&dir);
        assert_eq!(bank.face(1), Face::Varlena);
        assert_eq!(bank.schema[0].packed_scale, None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn zero_part_bank_witnesses_from_typmod() {
        // NUMERIC(15,2): typmod = ((15 << 16) | 2) + 4.
        let mut t = TypMeta::NUMERIC;
        t.typmod = ((15 << 16) | 2) + 4;
        let b = Bank::empty("/nonexistent".into(), vec![ColMeta::new(1, "n", t)]);
        assert_eq!(b.face(1), Face::PackedNumeric { scale: 2 });
        // Bare `numeric` (typmod -1): no DDL scale, no witness.
        let b2 = Bank::empty(
            "/nonexistent".into(),
            vec![ColMeta::new(1, "n", TypMeta::NUMERIC)],
        );
        assert_eq!(b2.face(1), Face::Varlena);
    }

    #[test]
    fn typmod_scale_law() {
        use crate::typmeta::numeric_typmod_scale;
        assert_eq!(numeric_typmod_scale(-1), None);
        assert_eq!(numeric_typmod_scale(((15 << 16) | 2) + 4), Some(2));
        assert_eq!(numeric_typmod_scale(((9 << 16) | 0) + 4), Some(0));
        // PG 15+ negative scale (scale -2 encodes as 0x7fe... low bits).
        let neg = ((5i32 << 16) | ((-2i32) & 0x7ff)) + 4;
        assert_eq!(numeric_typmod_scale(neg), None);
        assert_eq!(oids::NUMERIC, 1700);
    }
}
