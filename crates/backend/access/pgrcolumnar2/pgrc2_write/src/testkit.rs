//! Test-support kit (schemas, vfs universes, corpus builders, provider
//! bundles, the minimal part view). NOT production surface — the pgrc2_read
//! `testpart` precedent: shipped in the lib so integration tests of this
//! crate AND of the `pgrc2_ingest_par` rtpool-binding crate drive one kit.

#![allow(clippy::missing_panics_doc)]

use crate::elect::ReferenceCandidates;
use crate::ingest::{NoExternalDetoast, RawDatum};
use crate::publish::{TxnProbe, TxnVerdict};
use crate::seal::ReferenceResolver;
use crate::shred::NoShred;
use crate::writer::{PartCutPolicy, SealEnv, SubxactEvidence, TableWriter, TxnStamp};
use crate::wvfs::{MemVfs, WriteVfs};
use pgrc2_format::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};
use pgrc2_format::manifest::Manifest;
use pgrc2_format::part::{
    ExtentRecord, FooterFixed, PartTail, SectionEntry, SectionKind, StreamEntry, StreamRole,
};
use pgrc2_format::relopt::ShredOptions;
use pgrc2_format::wire::Cur;
use std::collections::BTreeMap;

pub const DIR: &str = "/tbl/pgrc2_777";
pub const RELFILENUMBER: u64 = 777;
pub const SPC: u32 = 1663;
pub const DB: u32 = 5;

pub fn mem_with_dir() -> MemVfs {
    let mut v = MemVfs::new();
    v.mkdir_path(DIR).expect("mkdir");
    v
}

pub fn int8_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        typlen: 8,
        typbyval: true,
        typalign: b'd',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::SignedInt,
    }
}

pub fn bool_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::Bool,
        typlen: 1,
        typbyval: true,
        typalign: b'c',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::Bool,
    }
}

pub fn text_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::VarlenaVerbatim,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::TextCollated,
    }
}

pub fn fixed16_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::Fixed { len: 16 },
        typlen: 16,
        typbyval: false,
        typalign: b'c',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::MemcmpOrdered,
    }
}

/// Scripted clog probe.
pub struct Probe {
    verdicts: BTreeMap<u64, TxnVerdict>,
    default: TxnVerdict,
}

impl Probe {
    pub fn new(default: TxnVerdict) -> Probe {
        Probe {
            verdicts: BTreeMap::new(),
            default,
        }
    }
    pub fn set(mut self, fxid: u64, v: TxnVerdict) -> Probe {
        self.verdicts.insert(fxid, v);
        self
    }
}

impl TxnProbe for Probe {
    fn verdict(&self, fxid: u64) -> TxnVerdict {
        self.verdicts.get(&fxid).copied().unwrap_or(self.default)
    }
}

pub fn stamp(fxid: u64, cid: u32) -> TxnStamp {
    TxnStamp { fxid, cid }
}

pub fn open_writer(schema: Vec<ColSchema>, st: TxnStamp) -> TableWriter {
    open_writer_policy(schema, st, PartCutPolicy::default())
}

pub fn open_writer_policy(
    schema: Vec<ColSchema>,
    st: TxnStamp,
    policy: PartCutPolicy,
) -> TableWriter {
    TableWriter::open(
        DIR.to_string(),
        schema,
        SPC,
        DB,
        RELFILENUMBER,
        st,
        &SubxactEvidence::default(),
        policy,
    )
    .expect("open writer")
}

/// The default env kit: reference candidates/resolver, no shred.
pub struct Kit {
    pub cands: ReferenceCandidates,
    pub resolver: ReferenceResolver,
    pub shred: NoShred,
    pub opts: ShredOptions,
    pub ext: NoExternalDetoast,
}

impl Kit {
    pub fn new() -> Kit {
        Kit {
            cands: ReferenceCandidates,
            resolver: ReferenceResolver,
            shred: NoShred,
            opts: ShredOptions::default(),
            ext: NoExternalDetoast,
        }
    }
}

/// Append `n` int8 rows (value = f(i)) to a single-int8-column writer.
pub fn append_int8_rows(
    w: &mut TableWriter,
    vfs: &mut MemVfs,
    kit: &mut Kit,
    n: u64,
    f: impl Fn(u64) -> Option<i64>,
) {
    for i in 0..n {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        let d = match f(i) {
            None => RawDatum::Null,
            Some(v) => RawDatum::Word(v as u64),
        };
        w.append_row(&[d], &mut kit.ext, &mut env).expect("append");
    }
}

/// Finish + publish a writer in one motion; returns the outcome.
pub fn finish_and_publish(
    w: &mut TableWriter,
    vfs: &mut MemVfs,
    kit: &mut Kit,
    probe: &dyn TxnProbe,
) -> crate::publish::PublishOutcome {
    let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
    let mut env = SealEnv {
        vfs,
        sources: &sources,
        resolver: &kit.resolver,
        shred: &mut kit.shred,
        shred_opts: &kit.opts,
    };
    w.finish(&mut env).expect("finish");
    w.publish(vfs, probe).expect("publish")
}

/// An 8-aligned decode-arena backing buffer (the `ByteArena` base-alignment
/// law, abi.rs §19.4 — debug-asserted; a `Vec<u8>` or stack `[u8; N]` base
/// carries no alignment guarantee). Mirrors the pgrc2_read test-battery
/// `ArenaBuf`. `new(0)` is legal: an empty `Vec<u64>`'s dangling pointer is
/// still 8-aligned, so zero-arena byval decodes stay assert-clean.
pub struct ArenaBuf {
    words: Vec<u64>,
}

impl ArenaBuf {
    pub fn new(bytes: usize) -> ArenaBuf {
        ArenaBuf {
            words: vec![0u64; bytes.div_ceil(8)],
        }
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: u64 → u8 reinterpret of an exclusively borrowed buffer.
        unsafe {
            core::slice::from_raw_parts_mut(
                self.words.as_mut_ptr() as *mut u8,
                self.words.len() * 8,
            )
        }
    }
}

/// 4B-U inline varlena image.
pub fn img_4b_u(payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + payload.len());
    v.extend_from_slice(&(((payload.len() as u32 + 4) << 2).to_le_bytes()));
    v.extend_from_slice(payload);
    v
}

/// 1B short-header varlena image (payload ≤ 126 B).
pub fn img_short(payload: &[u8]) -> Vec<u8> {
    let total = payload.len() + 1;
    assert!(total <= 0x7F);
    let mut v = Vec::with_capacity(total);
    v.push(((total as u8) << 1) | 0x01);
    v.extend_from_slice(payload);
    v
}

/// 4B compressed (pglz) varlena image.
pub fn img_pglz(payload: &[u8]) -> Vec<u8> {
    let mut dest = vec![std::mem::MaybeUninit::<u8>::uninit(); pglz::pglz_max_output(payload.len())];
    let clen = pglz::pglz_compress_into(payload, &mut dest, &pglz::PGLZ_STRATEGY_ALWAYS)
        .expect("compressible test payload");
    let total = 8 + clen;
    let mut v = Vec::with_capacity(total);
    v.extend_from_slice(&((((total as u32) << 2) | 0x02).to_le_bytes()));
    let tcinfo = payload.len() as u32; // method 0 = pglz in the top 2 bits
    v.extend_from_slice(&tcinfo.to_le_bytes());
    // SAFETY: pglz_compress_into initialized dest[..clen].
    v.extend_from_slice(unsafe {
        std::slice::from_raw_parts(dest.as_ptr() as *const u8, clen)
    });
    v
}

// ---------------------------------------------------------------------------
// A minimal part view for asserting on sealed bytes (tests only; the real
// reader is M3-F's).
// ---------------------------------------------------------------------------

pub struct PartView {
    pub bytes: Vec<u8>,
}

impl PartView {
    pub fn open(vfs: &mut MemVfs, name: &str) -> PartView {
        let bytes = vfs.read_full(&format!("{DIR}/{name}")).expect("read part");
        PartView { bytes }
    }

    pub fn footer(&self) -> FooterFixed {
        let tail = PartTail::decode_at_eof(&self.bytes).expect("tail");
        FooterFixed::decode(&self.bytes[tail.footer_off as usize..]).expect("footer")
    }

    pub fn sections(&self) -> Vec<SectionEntry> {
        let f = self.footer();
        let mut c = Cur::new(&self.bytes[f.section_table_off as usize..]);
        (0..f.section_count)
            .map(|_| SectionEntry::decode(&mut c).expect("section entry"))
            .collect()
    }

    pub fn section_bytes(&self, kind: SectionKind, attno: u32, path_ord: u32) -> Option<&[u8]> {
        self.sections()
            .into_iter()
            .find(|s| s.kind == kind.as_u16() && s.attno == attno && s.path_ord == path_ord)
            .map(|s| &self.bytes[s.off as usize..(s.off + s.len) as usize])
    }

    /// A meta-plane section BODY, CMP-F-unwrapped (SB-6): stored raw bodies
    /// pass through; `SECTIONF_META_ZSTD` envelopes decode through the one
    /// shared helper. This is the face every meta consumer models.
    pub fn meta_body(&self, kind: SectionKind, attno: u32, path_ord: u32) -> Option<Vec<u8>> {
        let s = self
            .sections()
            .into_iter()
            .find(|s| s.kind == kind.as_u16() && s.attno == attno && s.path_ord == path_ord)?;
        let stored = &self.bytes[s.off as usize..(s.off + s.len) as usize];
        if s.flags & pgrc2_format::part::SECTIONF_META_ZSTD != 0 {
            Some(pgrc2_codec::wrapper::meta_unwrap_body(stored).expect("meta envelope"))
        } else {
            Some(stored.to_vec())
        }
    }

    /// All StreamDir rows + their extent tables.
    pub fn streams(&self) -> Vec<(StreamEntry, Vec<ExtentRecord>)> {
        let body = self
            .section_bytes(SectionKind::StreamDir, 0, 0)
            .expect("StreamDir");
        let f = self.footer();
        let mut c = Cur::new(body);
        let entries: Vec<StreamEntry> = (0..f.stream_count)
            .map(|_| StreamEntry::decode(&mut c).expect("stream entry"))
            .collect();
        entries
            .into_iter()
            .map(|e| {
                let mut ec = Cur::new(&body[e.extent_table_off as usize..]);
                let exts = (0..e.extent_count)
                    .map(|_| ExtentRecord::decode(&mut ec).expect("extent"))
                    .collect();
                (e, exts)
            })
            .collect()
    }

    pub fn stream(&self, attno: u32, path_ord: u32, role: StreamRole) -> Option<(StreamEntry, Vec<ExtentRecord>)> {
        self.streams()
            .into_iter()
            .find(|(e, _)| e.attno == attno && e.path_ord == path_ord && e.role == role.as_u8())
    }
}

// ---------------------------------------------------------------------------
// Parallel-ingest kit (chunk M3-I)
// ---------------------------------------------------------------------------

use crate::elect::ReferenceCandidates as RefCands2;
use crate::par::{
    NoDetoastProvider, NoShredProvider, ParProviders, RowChunk, SharedMemVfs,
    SharedMemVfsProvider,
};
use std::sync::Arc;

pub fn shared_mem_with_dir() -> SharedMemVfs {
    SharedMemVfs::new(mem_with_dir())
}

/// The reference provider bundle over a shared MemVfs universe — the
/// parallel analog of [`Kit`].
pub fn par_providers(shared: &SharedMemVfs) -> ParProviders {
    ParProviders {
        vfs: Arc::new(SharedMemVfsProvider(shared.clone())),
        detoast: Arc::new(NoDetoastProvider),
        shred: Arc::new(NoShredProvider),
        sources: vec![Arc::new(RefCands2)],
        resolver: Arc::new(crate::seal::ReferenceResolver),
        shred_opts: ShredOptions::default(),
        structural: crate::structural::StructuralPolicy::default(),
    }
}

/// The standing mixed two-column corpus row (int8 with a null stripe + text
/// with varied lengths — the determinism.rs shape).
pub fn mixed_row(i: u64) -> (Option<i64>, Option<String>) {
    let d1 = if i % 13 == 0 { None } else { Some(i as i64 - 5000) };
    let d2 = if i % 17 == 0 {
        None
    } else {
        Some(format!("value-{}", i % 977))
    };
    (d1, d2)
}

/// Append one mixed row to any row-shaped consumer.
pub fn with_mixed_row<R>(i: u64, f: impl FnOnce(&[RawDatum<'_>]) -> R) -> R {
    let (d1, d2) = mixed_row(i);
    let img = d2.as_ref().map(|s| img_4b_u(s.as_bytes()));
    let r1 = match d1 {
        None => RawDatum::Null,
        Some(v) => RawDatum::Word(v as u64),
    };
    let r2 = match &img {
        None => RawDatum::Null,
        Some(b) => RawDatum::Bytes(b),
    };
    f(&[r1, r2])
}

/// Capture `n` mixed rows into RowChunks of `chunk_rows` (the scripted
/// engine drivers' feed).
pub fn capture_mixed_chunks(n: u64, chunk_rows: u32) -> Vec<RowChunk> {
    let mut out = Vec::new();
    let mut cur = RowChunk::new(2, chunk_rows);
    for i in 0..n {
        with_mixed_row(i, |row| cur.push_row(row).expect("capture"));
        if cur.rows() >= chunk_rows {
            out.push(std::mem::replace(&mut cur, RowChunk::new(2, chunk_rows)));
        }
    }
    if cur.rows() > 0 {
        out.push(cur);
    }
    out
}

/// Serial oracle: run the serial writer over `n` mixed rows under `policy`,
/// FINISH ONLY (no publish), and return each sealed temp file's bytes in
/// seq order (the frozen-face byte oracle the parallel runs diff against).
pub fn serial_mixed_tmp_bytes(n: u64, fxid: u64, policy: PartCutPolicy) -> Vec<Vec<u8>> {
    let mut vfs = mem_with_dir();
    let mut kit = Kit::new();
    let mut w = open_writer_policy(vec![int8_col(1), text_col(2)], stamp(fxid, 1), policy);
    for i in 0..n {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        with_mixed_row(i, |row| w.append_row(row, &mut kit.ext, &mut env).expect("append"));
    }
    {
        let sources: [&dyn crate::elect::CandidateSource; 1] = [&kit.cands];
        let mut env = SealEnv {
            vfs: &mut vfs,
            sources: &sources,
            resolver: &kit.resolver,
            shred: &mut kit.shred,
            shred_opts: &kit.opts,
        };
        w.finish(&mut env).expect("finish");
    }
    (0..w.sealed_parts().len() as u32)
        .map(|seq| {
            vfs.read_full(&format!(
                "{DIR}/{}",
                pgrc2_format::dirlayout::temp_file_name(fxid, seq)
            ))
            .expect("tmp bytes")
        })
        .collect()
}

/// O-10 logical totals per (attno, path_ord): wrapping-add the per-part
/// lanes (the multiset-hash merge law).
pub fn logical_totals(
    reports: &[crate::seal::SealReport],
) -> BTreeMap<(u32, u32), (u64, u64, u64)> {
    let mut tot: BTreeMap<(u32, u32), (u64, u64, u64)> = BTreeMap::new();
    for r in reports {
        for &(attno, path_ord, (lo, hi, rows)) in &r.col_hashes {
            let e = tot.entry((attno, path_ord)).or_insert((0, 0, 0));
            e.0 = e.0.wrapping_add(lo);
            e.1 = e.1.wrapping_add(hi);
            e.2 += rows;
        }
    }
    tot
}

pub fn read_manifest(vfs: &mut MemVfs, gen: u64) -> Manifest {
    let bytes = vfs
        .read_full(&format!(
            "{DIR}/{}",
            pgrc2_format::dirlayout::manifest_file_name(gen)
        ))
        .expect("manifest bytes");
    Manifest::decode(&bytes).expect("manifest decode")
}
