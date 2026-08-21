//! P4-5a Values leaf (production-plan "trivial sources"): a `VALUES`
//! list is a tiny materialized bank. This module seals the recognized
//! rows into a REAL single-generation pgrc2 bank ENTIRELY IN MEMORY —
//! the real `TableWriter` over `MemVfs`, the real manifest walk over
//! `MemTableDir`, the real `OpenPart` over `MemPartIo` — so the engine's
//! join build/probe machinery (faces, dict election, granule decode)
//! composes UNCHANGED over a Values side. No temp files, no registry
//! entry, no invalidation surface: the bank lives exactly as long as the
//! lowered statement that built it (values-build artifacts never
//! memoize — seam `memo_bar`).
//!
//! Vocabulary: the join type set (int2/int4/int8/date/timestamp/
//! timestamptz + text/varchar); every cell must be a plan-time `Const`
//! (the planner folds VALUES cells; anything else refuses typed at the
//! seam before this module runs).

use pgrc2_format::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};
use pgrc2_format::dirlayout::{manifest_file_name, part_file_name, CURRENT_FILE_NAME};
use pgrc2_format::relopt::ShredOptions;
use pgrc2_read::io::{MemPartIo, MemTableDir};
use pgrc2_read::manifest_walk::{resolve_effective, AllCommitted, TableExpect};
use pgrc2_read::openpart::{OpenPart, PartExpect};
use pgrc2_write::elect::ReferenceCandidates;
use pgrc2_write::ingest::{NoExternalDetoast, RawDatum};
use pgrc2_write::publish::{TxnProbe, TxnVerdict};
use pgrc2_write::seal::ReferenceResolver;
use pgrc2_write::shred::NoShred;
use pgrc2_write::writer::{PartCutPolicy, SealEnv, TableWriter, TxnStamp};
use pgrc2_write::wvfs::{MemVfs, WriteVfs};
use std::sync::Arc;

use sqe::bank::{Bank, ColMeta, OpenOpts};
use sqe::typmeta::{oids, TypMeta};

/// One Values column, derived from the first row's Consts (the planner
/// coerces every row to the column type, so row 0 is authoritative).
#[derive(Clone, Copy)]
pub(crate) struct VCol {
    pub oid: u32,
    pub collation: u32,
}

/// One recognized Values cell.
pub(crate) enum VCell<'a> {
    Null,
    /// By-value word, raw datum bits (the `RawDatum::Word` currency).
    Word(u64),
    /// Inline varlena image (header included), borrowed from the plan.
    Bytes(&'a [u8]),
}

/// (writer ColSchema, engine ColMeta) for one supported column type;
/// `None` = outside the vocabulary (the caller refuses typed).
fn col_faces(attno: u32, c: &VCol) -> Option<(ColSchema, ColMeta)> {
    let (width, align): (u8, u8) = match c.oid {
        oids::INT2 => (2, b's'),
        oids::INT4 | oids::DATE => (4, b'i'),
        oids::INT8 | oids::TIMESTAMP | oids::TIMESTAMPTZ => (8, b'd'),
        oids::TEXT | oids::VARCHAR => {
            // [sqe-collation] plan collations resolve through the byte
            // law (class C pins 950 — collation-currency.md §3); non-C
            // stays verbatim and refuses at the engine's ColMeta gate.
            let coll = if super::seam::byte_order_collation(c.collation) {
                sqe::typmeta::COLLATION_C
            } else {
                c.collation
            };
            let ws = ColSchema {
                attno,
                class: StorageClass::VarlenaVerbatim,
                typlen: -1,
                typbyval: false,
                typalign: b'i',
                collation_class: if coll == sqe::typmeta::COLLATION_C {
                    CollationClass::C
                } else {
                    CollationClass::OtherDeterministic
                },
                semantics: TypeSemantics::TextCollated,
            };
            let cm = ColMeta::new(
                attno,
                &format!("column{attno}"),
                TypMeta::varlena(c.oid, coll),
            );
            return Some((ws, cm));
        }
        _ => return None,
    };
    let ws = ColSchema {
        attno,
        class: StorageClass::ByvalWord { width, signed: true },
        typlen: width as i16,
        typbyval: true,
        typalign: align,
        collation_class: CollationClass::C,
        semantics: TypeSemantics::SignedInt,
    };
    let cm = ColMeta::new(attno, &format!("column{attno}"), TypMeta::byval(c.oid, width as i8));
    Some((ws, cm))
}

/// TRUE iff the type is in the Values vocabulary.
pub(crate) fn vcol_supported(oid: u32) -> bool {
    col_faces(1, &VCol { oid, collation: 0 }).is_some()
}

struct AlwaysCommitted;
impl TxnProbe for AlwaysCommitted {
    fn verdict(&self, _fxid: u64) -> TxnVerdict {
        TxnVerdict::Committed
    }
}

/// A streaming in-memory bank seal: the Values-bank law behind a
/// row-append face, shared by the Values leaf and the heap-join side
/// staging (heap-face.md JOINS RUNG — the sealed bank is the join
/// build/probe currency). Same format-identity discipline: real writer,
/// real manifest walk, real `OpenPart`; nothing hand-built.
pub(crate) struct BankBuilder {
    dir: String,
    vfs: MemVfs,
    w: TableWriter,
    schema: Vec<ColMeta>,
    cands: ReferenceCandidates,
    resolver: ReferenceResolver,
    shred: NoShred,
    opts: ShredOptions,
    ext: NoExternalDetoast,
    rows: u64,
}

impl BankBuilder {
    pub(crate) fn open(name: &str, cols: &[VCol]) -> Result<BankBuilder, String> {
        let mut wschema = Vec::with_capacity(cols.len());
        let mut schema = Vec::with_capacity(cols.len());
        for (i, c) in cols.iter().enumerate() {
            let (ws, cm) =
                col_faces(i as u32 + 1, c).ok_or_else(|| format!("unsupported oid {}", c.oid))?;
            wschema.push(ws);
            schema.push(cm);
        }
        let mut vfs = MemVfs::new();
        vfs.mkdir_path(name).map_err(|e| format!("bank mkdir: {e:?}"))?;
        let w = TableWriter::open(
            name.to_string(),
            wschema,
            1663,
            1,
            1,
            TxnStamp { fxid: 1, cid: 0 },
            &Default::default(),
            PartCutPolicy { max_rows: u64::MAX, max_bytes: u64::MAX, cut_granule_rows: 8192 },
        )
        .map_err(|e| format!("bank writer open: {e:?}"))?;
        Ok(BankBuilder {
            dir: name.to_string(),
            vfs,
            w,
            schema,
            cands: ReferenceCandidates,
            resolver: ReferenceResolver,
            shred: NoShred,
            opts: ShredOptions::default(),
            ext: NoExternalDetoast,
            rows: 0,
        })
    }

    pub(crate) fn append_row(&mut self, datums: &[RawDatum<'_>]) -> Result<(), String> {
        let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&self.cands];
        let mut env = SealEnv {
            vfs: &mut self.vfs,
            sources: &sources,
            resolver: &self.resolver,
            shred: &mut self.shred,
            shred_opts: &self.opts,
        };
        self.w
            .append_row(datums, &mut self.ext, &mut env)
            .map_err(|e| format!("bank append: {e:?}"))?;
        self.rows += 1;
        Ok(())
    }

    /// Seal + publish + reader-side manifest walk. Zero appended rows =
    /// a partless bank (the writer has no zero-row publish; the join's
    /// empty law over a partless bank is the never-ingested answer).
    pub(crate) fn finish(mut self) -> Result<Bank, String> {
        if self.rows == 0 {
            return Ok(Bank::empty(self.dir, self.schema));
        }
        {
            let sources: [&dyn pgrc2_write::elect::CandidateSource; 1] = [&self.cands];
            let mut env = SealEnv {
                vfs: &mut self.vfs,
                sources: &sources,
                resolver: &self.resolver,
                shred: &mut self.shred,
                shred_opts: &self.opts,
            };
            self.w.finish(&mut env).map_err(|e| format!("bank finish: {e:?}"))?;
        }
        let outcome = self
            .w
            .publish(&mut self.vfs, &AlwaysCommitted)
            .map_err(|e| format!("bank publish: {e:?}"))?;

        let dir = &self.dir;
        let mut dirio = MemTableDir::new();
        let current = self
            .vfs
            .read_full(&format!("{dir}/{CURRENT_FILE_NAME}"))
            .map_err(|e| format!("bank CURRENT read: {e:?}"))?;
        dirio.put(CURRENT_FILE_NAME, current);
        let mname = manifest_file_name(outcome.gen);
        let mbytes = self
            .vfs
            .read_full(&format!("{dir}/{mname}"))
            .map_err(|e| format!("bank manifest read: {e:?}"))?;
        dirio.put(&mname, mbytes);

        let eff = resolve_effective(&dirio, &AllCommitted, &TableExpect::default())
            .map_err(|e| format!("bank manifest walk: {e:?}"))?
            .ok_or("bank: no effective generation")?;
        let m = eff.manifest;

        let mut parts: Vec<Arc<OpenPart>> = Vec::with_capacity(m.parts.len());
        for rec in &m.parts {
            let pbytes = self
                .vfs
                .read_full(&format!("{}/{}", dir, part_file_name(rec.part_no)))
                .map_err(|e| format!("bank part read: {e:?}"))?;
            let expect = PartExpect {
                part_no: Some(rec.part_no),
                rows: Some(rec.rows),
                file_len: Some(rec.file_len),
                footer_off: Some(rec.footer_off),
                schema_fingerprint: Some(m.header.schema_fingerprint),
                relfilenumber: Some(m.header.relfilenumber),
                ..PartExpect::default()
            };
            let io = MemPartIo::new(pbytes, 7, rec.part_no as u64 + 1);
            parts.push(Arc::new(
                OpenPart::open(Box::new(io), &expect)
                    .map_err(|e| format!("bank part open: {e:?}"))?,
            ));
        }

        Ok(Bank::from_bridge(
            self.dir.clone(),
            m,
            parts,
            self.schema,
            &OpenOpts { bankstats: false, threads: 1 },
        ))
    }
}

/// Seal `rows` into an in-memory bank. Every row must be `cols`-wide
/// (the seam's recognizer guarantees it). Errors are internal seal/open
/// failures — REAL errors at the seam, never refusals.
pub(crate) fn seal_values_bank(
    cols: &[VCol],
    rows: &[Vec<VCell<'_>>],
) -> Result<Bank, String> {
    let mut b = BankBuilder::open("sqe-values-bank", cols)?;
    let mut datums: Vec<RawDatum<'_>> = Vec::with_capacity(cols.len());
    for row in rows {
        datums.clear();
        datums.extend(row.iter().map(|c| match c {
            VCell::Null => RawDatum::Null,
            VCell::Word(v) => RawDatum::Word(*v),
            VCell::Bytes(bs) => RawDatum::Bytes(bs),
        }));
        b.append_row(&datums)?;
    }
    b.finish()
}
