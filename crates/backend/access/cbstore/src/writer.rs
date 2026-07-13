//! COPY/INSERT append path: RG builders, analyze-then-pick encoders, footer
//! publish (docs/design/cbstore-impl.md §2, §5).

use std::collections::HashMap;

use ::datum::Datum;
use ::types_core::primitive::{Oid, TransactionId};
use ::types_error::{PgError, PgResult};
use ::tuplesort_seams::{CbIngestSort, CbSortKeyKind};

use crate::format::*;
use crate::hll::Hll;
use crate::reader::read_header_opt;
use crate::segfile::SegFile;
use ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED;
use crate::varlena_bytes;

// ---- per-table writer options (CREATE TABLE ... USING cbstore WITH (...)) --

/// Table-level codec policy (the v6 per-column codec menu, plan §3.2).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CodecChoice {
    /// Sample-based per-chunk pick (day-0 evidence, ch-microbench note §1):
    /// LZ4 unless ZSTD's sampled ratio gain clears the class threshold.
    Auto,
    Lz4,
    Zstd,
    /// No compressed frames (v5 behavior for int/text raw lanes).
    Plain,
}

pub const ZSTD_LEVEL_DEFAULT: i32 = 3;

/// Options resolved against the relation's columns at writer open.
#[derive(Clone, Debug)]
pub struct CbWriterOpts {
    // (column index, sort-key kind) in declared cluster-key order.
    pub cluster_key: Vec<(u16, CbSortKeyKind)>,
    // Per-column codec choice (explicit override or the table default).
    pub codec: Vec<CodecChoice>,
    pub zstd_level: i32,
}

impl CbWriterOpts {
    pub fn plain(ncols: usize) -> CbWriterOpts {
        CbWriterOpts {
            cluster_key: Vec::new(),
            // Train #8 ingest default: LZ4 (matches CbstoreOptions::default).
            codec: vec![CodecChoice::Lz4; ncols],
            zstd_level: ZSTD_LEVEL_DEFAULT,
        }
    }
}

fn sort_kind_of(t: ColType) -> CbSortKeyKind {
    match t {
        ColType::I16 => CbSortKeyKind::Int16,
        ColType::I32 | ColType::Date => CbSortKeyKind::Int32,
        ColType::I64 | ColType::Timestamp => CbSortKeyKind::Int64,
        ColType::Text => CbSortKeyKind::TextC,
    }
}

fn col_index_of(rel: &::types_rel::Relation<'_>, name: &str) -> PgResult<u16> {
    for (i, a) in rel.rd_att.attrs.iter().enumerate() {
        if !a.attisdropped && a.attname.name_str() == name.as_bytes() {
            return Ok(i as u16);
        }
    }
    Err(Box::new(
        PgError::error(format!(
            "cbstore: cluster/codec option references unknown column \"{name}\""
        ))
        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    ))
}

/// Resolve the relation's cbstore reloptions into writer terms.
pub fn writer_opts_of(
    rel: &::types_rel::Relation<'_>,
    coltypes: &[ColType],
) -> PgResult<CbWriterOpts> {
    let mut out = CbWriterOpts::plain(coltypes.len());
    let Some(o) = rel.rd_options.as_ref().and_then(|o| o.cbstore()) else {
        return Ok(out);
    };
    out.zstd_level = o.zstd_level;
    let table_choice = match o.codec {
        ::types_rel::CbstoreCodec::Auto => CodecChoice::Auto,
        ::types_rel::CbstoreCodec::Lz4 => CodecChoice::Lz4,
        ::types_rel::CbstoreCodec::Zstd => CodecChoice::Zstd,
        ::types_rel::CbstoreCodec::Plain => CodecChoice::Plain,
    };
    out.codec = vec![table_choice; coltypes.len()];
    for part in o.cluster_key().split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let idx = col_index_of(rel, part)?;
        out.cluster_key.push((idx, sort_kind_of(coltypes[idx as usize])));
    }
    if out.cluster_key.len() > CB_CLUSTER_KEY_MAX_COLS {
        return Err(Box::new(PgError::error(format!(
            "cbstore: cluster_key supports at most {CB_CLUSTER_KEY_MAX_COLS} columns"
        ))));
    }
    for pair in o.codec_cols().split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (name, codec) = pair.split_once('=').ok_or_else(|| {
            Box::new(PgError::error(format!("cbstore: bad codec_cols entry \"{pair}\"")))
        })?;
        let idx = col_index_of(rel, name.trim())? as usize;
        out.codec[idx] = match codec.trim() {
            "auto" => CodecChoice::Auto,
            "lz4" => CodecChoice::Lz4,
            "zstd" => CodecChoice::Zstd,
            "plain" => CodecChoice::Plain,
            other => {
                return Err(Box::new(PgError::error(format!(
                    "cbstore: unknown codec \"{other}\" in codec_cols"
                ))))
            }
        };
    }
    Ok(out)
}

// ---- codec engine ----------------------------------------------------------

/// Per-chunk compression driver: sample-pick between LZ4/ZSTD, then frame.
pub(crate) struct CodecCtx {
    pub choice: CodecChoice,
    pub zstd_level: i32,
}

impl CodecCtx {
    pub(crate) fn compress(&self, codec: Codec, data: &[u8]) -> Vec<u8> {
        match codec {
            Codec::Lz4 => lz4_flex::compress(data),
            Codec::Zstd => zstd::bulk::compress(data, self.zstd_level)
                .expect("cbstore: zstd compress failed"),
            Codec::None => unreachable!("cbstore: compress with Codec::None"),
        }
    }

    /// Pick a codec from one sampled frame. `narrow_hot` marks 1-2B int lanes
    /// (decode-hot; day-0 §1: keep LZ4 unless ZSTD's win is outsized).
    /// Returns None when no codec clears the >=10% win-vs-raw gate.
    pub(crate) fn pick(&self, sample: &[u8], narrow_hot: bool) -> Option<Codec> {
        if sample.is_empty() {
            return None;
        }
        let wins = |comp: usize| comp * 10 <= sample.len() * 9;
        match self.choice {
            CodecChoice::Plain => None,
            CodecChoice::Lz4 => {
                wins(lz4_flex::compress(sample).len()).then_some(Codec::Lz4)
            }
            CodecChoice::Zstd => {
                wins(self.compress(Codec::Zstd, sample).len()).then_some(Codec::Zstd)
            }
            CodecChoice::Auto => {
                let lz4 = lz4_flex::compress(sample).len();
                let zst = self.compress(Codec::Zstd, sample).len();
                if !wins(lz4) && !wins(zst) {
                    return None;
                }
                // ZSTD must beat LZ4 by >=20% (day-0: its ratio edge is
                // 1.2-20x where it matters), >=30% on decode-hot narrow ints.
                let num = if narrow_hot { 7 } else { 8 };
                if zst * 10 <= lz4 * num {
                    Some(Codec::Zstd)
                } else if wins(lz4) {
                    Some(Codec::Lz4)
                } else {
                    Some(Codec::Zstd)
                }
            }
        }
    }
}

// Compressed-frame image: u32 raw_len | u32 comp_len | bytes, align4-padded.
pub(crate) fn push_frame(body: &mut Vec<u8>, raw_len: usize, comp: &[u8]) {
    put_u32(body, raw_len as u32);
    put_u32(body, comp.len() as u32);
    body.extend_from_slice(comp);
    while body.len() % 4 != 0 {
        body.push(0);
    }
}

pub(crate) fn frame_len(comp_len: usize) -> usize {
    align4(8 + comp_len)
}

#[cfg(test)]
pub(crate) fn test_codec_ctx() -> CodecCtx {
    CodecCtx { choice: CodecChoice::Auto, zstd_level: ZSTD_LEVEL_DEFAULT }
}

struct IntBuilder {
    vals: Vec<i64>,
}

struct TextBuilder {
    // Per-row byte ranges into `blob` (header-less string bytes).
    offs: Vec<(u32, u32)>,
    blob: Vec<u8>,
}

enum ColBuilder {
    Int(IntBuilder),
    Text(TextBuilder),
}

pub struct CbWriter {
    file: SegFile,
    xid: TransactionId,
    // Command that opened this writer: buffered ingest is per-statement
    // (tuple_insert buffers until the statement-end flush), so a writer left
    // behind by an errored statement must not leak rows into the next one.
    cid: ::types_core::CommandId,
    frozen: bool,
    ncols: usize,
    coltypes: Vec<ColType>,
    builders: Vec<ColBuilder>,
    nbuf: usize,
    // Committed footer chain state.
    write_off: u64,
    rgs: Vec<FooterRg>,
    fingerprint: u64,
    // Ingest-time per-column NDV sketches; None when appending to a part
    // with committed RGs (the finalized footer count cannot be merged into
    // a fresh sketch, so NDV is recorded as unknown).
    ndv: Option<Vec<Hll>>,
    // v5 whole-part sorted-asc trackers; None when appending to committed
    // RGs (the seam to the committed tail is unproven — the NDV precedent).
    sorted: Option<Vec<bool>>,
    prev_int: Vec<i64>,
    prev_text: Vec<Vec<u8>>,
    has_prev: bool,
    // v6 writer options (cluster key + codec menu).
    opts: CbWriterOpts,
    // Sort-on-ingest (plan §3.1): with a declared cluster key, rows buffer
    // into a spill-capable tuplesort and only reach append_row on the sorted
    // drain at finish(); RGs sealed from the drain carry RG_FLAG_CLUSTERED.
    sorter: Option<Box<dyn CbIngestSort>>,
    draining_clustered: bool,
}

pub struct FooterRg {
    pub file_off: u64,
    pub nrows: u32,
    pub xmin: TransactionId,
    pub flags: u32,
    // Per column: (chunk_off relative to RG start, min, max).
    pub chunks: Vec<(u64, i64, i64)>,
    // Per column i128 sums; meaningful only when flags & RG_FLAG_SUMS
    // (empty on RGs parsed from v<=3 footers).
    pub sums: Vec<i128>,
    // v7 per-granule text length stats: (sum(octet_length), non-null count,
    // empty-string count) per GRANULES_PER_RG granule slot x flagged text
    // column ascending (index = slot * nlencols + rank). Meaningful only when
    // flags & RG_FLAG_LENSTATS; empty on RGs parsed from v<=6 footers.
    pub lenstats: Vec<(u64, u32, u32)>,
}

thread_local! {
    static WRITERS: std::cell::RefCell<HashMap<Oid, CbWriter>> =
        std::cell::RefCell::new(HashMap::new());
}

pub fn coltypes_of(rel: &::types_rel::Relation<'_>) -> PgResult<Vec<ColType>> {
    rel.rd_att
        .attrs
        .iter()
        .map(|a| {
            ColType::of_type_oid(a.atttypid).ok_or_else(|| {
                Box::new(
                    PgError::error(format!(
                        "cbstore does not support the type of column \"{}\" (type oid {})",
                        String::from_utf8_lossy(a.attname.name_str()),
                        a.atttypid
                    ))
                    .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                )
            })
        })
        .collect()
}

fn open_writer(rel: &::types_rel::Relation<'_>) -> PgResult<CbWriter> {
    let coltypes = coltypes_of(rel)?;
    let opts = writer_opts_of(rel, &coltypes)?;
    let path = crate::rel_main_path(rel);
    let file = SegFile::open_rw(&path)?;
    let xid = xact_seams::get_current_transaction_id::call()?;
    // RG_FLAG_FROZEN bypasses the per-RG xmin visibility gate entirely, so
    // it is only sound when an abort of the writing (sub)transaction also
    // unlinks the file (or its whole relfilenode) it froze into. C parity:
    // copyfrom.c's FREEZE precheck demands the rel be created OR truncated
    // in the CURRENT subtransaction. A pre-existing empty part must NOT
    // freeze — a `BEGIN; COPY; ROLLBACK` publish into it would otherwise
    // stay visible forever.
    let cur_subid = xact_seams::get_current_sub_transaction_id::call();
    let frozen_ok = cur_subid != ::types_core::InvalidSubTransactionId
        && (rel.rd_createSubid.get() == cur_subid
            || rel.rd_newRelfilelocatorSubid.get() == cur_subid);
    let fingerprint = schema_fingerprint(
        &rel.rd_att.attrs.iter().map(|a| (a.atttypid, a.attlen)).collect::<Vec<_>>(),
    );
    let cid = xact_seams::get_current_command_id::call(false)?;
    let mut w = open_writer_inner(file, xid, cid, frozen_ok, coltypes, fingerprint, opts)?;
    if !w.opts.cluster_key.is_empty() {
        let keys: Vec<(i16, CbSortKeyKind)> =
            w.opts.cluster_key.iter().map(|&(c, k)| (c as i16 + 1, k)).collect();
        // SAFETY: lifetime erasure on the relcache tupdesc; the COPY/INSERT
        // statement keeps the relation open for the writer's lifetime (the
        // begin_index_btree contract), and the stale-xid check in
        // multi_insert drops writers abandoned by error unwinds.
        let tup_desc: std::rc::Rc<::types_tuple::TupleDescData<'static>> =
            unsafe { std::mem::transmute(rel.rd_att.clone()) };
        w.sorter = Some(::tuplesort_seams::cbstore_ingest_sort::call(
            tup_desc,
            &keys,
            init_small::globals::maintenance_work_mem(),
        )?);
    }
    Ok(w)
}

/// TEST SUPPORT (dict-tier round-trip / bench rigs): a writer over an
/// explicit path + coltypes, bypassing Relation and xact-seam resolution
/// (xid 1, frozen-ok — the sealed row groups need no visibility seams to
/// scan). `append_row`/`finish` ride the exact production write path.
#[doc(hidden)]
pub fn open_writer_at(path: &str, coltypes: Vec<ColType>) -> PgResult<CbWriter> {
    let opts = CbWriterOpts::plain(coltypes.len());
    open_writer_inner(SegFile::open_rw(path)?, 1, 0, true, coltypes, 0x5aa5, opts)
}

fn open_writer_inner(
    file: SegFile,
    xid: TransactionId,
    cid: ::types_core::CommandId,
    frozen_ok: bool,
    coltypes: Vec<ColType>,
    fingerprint: u64,
    opts: CbWriterOpts,
) -> PgResult<CbWriter> {
    let len = file.total_len();
    let ncols = coltypes.len();
    let mut w = CbWriter {
        file,
        xid,
        cid,
        // Freeze-on-load: first write into a file created by our own
        // transaction (empty part) makes RGs all-visible-on-commit.
        frozen: false,
        ncols,
        coltypes,
        builders: Vec::new(),
        nbuf: 0,
        write_off: CB_HEADER_LEN,
        rgs: Vec::new(),
        fingerprint,
        ndv: Some((0..ncols).map(|_| Hll::default()).collect()),
        sorted: Some(vec![true; ncols]),
        prev_int: vec![0; ncols],
        prev_text: vec![Vec::new(); ncols],
        has_prev: false,
        opts,
        sorter: None,
        draining_clustered: false,
    };
    w.reset_builders();
    // Empty part; also a part whose header page is still a zero hole (a
    // pre-header-first writer aborted mid-COPY before ever publishing) —
    // both mean "no committed row group can exist", so both (re)initialize.
    let mut init_header = len < CB_HEADER_LEN;
    if len >= CB_HEADER_LEN {
        let mut hdr = [0u8; CB_HEADER_LEN as usize];
        w.file.read_exact_at(&mut hdr, 0)?;
        match read_header_opt(&hdr)? {
            None => init_header = true,
            Some((footer_off, fp, version)) => {
                if fp != w.fingerprint {
                    return Err(Box::new(PgError::error(
                        "cbstore: schema fingerprint mismatch".to_string(),
                    )));
                }
                if footer_off != 0 {
                    let (rgs, footer_end, _ndv, _sorted, _ckey, _lenflags) =
                        crate::reader::read_footer_rgs(
                        &mut w.file,
                        footer_off,
                        w.ncols,
                        version,
                        true,
                    )?;
                    if !rgs.is_empty() {
                        w.ndv = None;
                        w.sorted = None;
                    }
                    w.rgs = rgs;
                    w.write_off = align64(footer_end.max(footer_off));
                }
            }
        }
    }
    if init_header {
        w.frozen = frozen_ok;
        // Header-first ingest (abort safety): publish a valid empty-part
        // header (footer_off = 0) BEFORE any row-group bytes hit the file.
        // An abort mid-COPY then leaves a readable empty part plus dead
        // bytes past the header — not a zero hole that wedges every later
        // read_header. Durable up front so a crash-kill mid-COPY restarts
        // into the same readable state.
        w.write_header(0)?;
        w.file.pad_and_sync(CB_HEADER_LEN)?;
    }
    Ok(w)
}

impl CbWriter {
    fn write_header(&mut self, footer_off: u64) -> PgResult<()> {
        let mut hdr = Vec::with_capacity(CB_HEADER_LEN as usize);
        put_u64(&mut hdr, CB_MAGIC);
        put_u32(&mut hdr, CB_VERSION);
        put_u32(&mut hdr, self.ncols as u32);
        put_u64(&mut hdr, footer_off);
        put_u64(&mut hdr, self.fingerprint);
        hdr.resize(CB_HEADER_LEN as usize, 0);
        self.file.write_all_at(&hdr, 0)
    }

    fn reset_builders(&mut self) {
        self.builders = self
            .coltypes
            .iter()
            .map(|t| {
                if t.is_text() {
                    ColBuilder::Text(TextBuilder { offs: Vec::new(), blob: Vec::new() })
                } else {
                    ColBuilder::Int(IntBuilder { vals: Vec::new() })
                }
            })
            .collect();
        self.nbuf = 0;
    }

    // COPY/INSERT entry: rows detour through the cluster-key sorter when one
    // is declared, reaching append_row only on the sorted drain in finish().
    fn ingest_row(&mut self, values: &[Datum], isnull: &[bool]) -> PgResult<()> {
        if let Some(s) = &mut self.sorter {
            if let Some(c) = isnull[..self.ncols].iter().position(|&n| n) {
                let _ = c;
                return Err(Box::new(
                    PgError::error("cbstore does not support NULL values".to_string())
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            return s.put_row(&values[..self.ncols], &isnull[..self.ncols]);
        }
        self.append_row(values, isnull)
    }

    /// pub for the test-support writer (`open_writer_at`); production
    /// callers reach this through multi_insert/tuple_insert (via ingest_row).
    #[doc(hidden)]
    pub fn append_row(&mut self, values: &[Datum], isnull: &[bool]) -> PgResult<()> {
        for c in 0..self.ncols {
            if isnull[c] {
                return Err(Box::new(
                    PgError::error("cbstore does not support NULL values".to_string())
                        .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            match &mut self.builders[c] {
                ColBuilder::Int(b) => {
                    let v = match self.coltypes[c] {
                        ColType::I16 => values[c].as_i16() as i64,
                        ColType::I32 | ColType::Date => values[c].as_i32() as i64,
                        ColType::I64 | ColType::Timestamp => values[c].as_i64(),
                        ColType::Text => unreachable!(),
                    };
                    if let Some(ndv) = &mut self.ndv {
                        ndv[c].add_i64(v);
                    }
                    if let Some(sorted) = &mut self.sorted {
                        if sorted[c] && self.has_prev && v < self.prev_int[c] {
                            sorted[c] = false;
                        }
                        self.prev_int[c] = v;
                    }
                    b.vals.push(v);
                }
                ColBuilder::Text(b) => {
                    let bytes = varlena_bytes(values[c])?;
                    if let Some(ndv) = &mut self.ndv {
                        ndv[c].add_bytes(bytes);
                    }
                    if let Some(sorted) = &mut self.sorted {
                        if sorted[c] {
                            if self.has_prev && bytes < self.prev_text[c].as_slice() {
                                sorted[c] = false;
                            } else {
                                self.prev_text[c].clear();
                                self.prev_text[c].extend_from_slice(bytes);
                            }
                        }
                    }
                    let off = b.blob.len() as u32;
                    b.blob.extend_from_slice(bytes);
                    b.offs.push((off, bytes.len() as u32));
                }
            }
        }
        self.has_prev = true;
        self.nbuf += 1;
        if self.nbuf == RG_ROWS {
            self.seal_rg()?;
        }
        Ok(())
    }

    fn seal_rg(&mut self) -> PgResult<()> {
        if self.nbuf == 0 {
            return Ok(());
        }
        let nrows = self.nbuf;
        let ngranules = nrows.div_ceil(GRANULE_ROWS) as u32;
        let flags = (if self.frozen { RG_FLAG_FROZEN } else { 0 })
            | RG_FLAG_SUMS
            | RG_FLAG_LENSTATS
            | (if self.draining_clustered { RG_FLAG_CLUSTERED } else { 0 });
        let mut body: Vec<u8> = Vec::with_capacity(1 << 20);
        // rg_header placeholder; length patched at the end.
        put_u32(&mut body, CB_RG_MAGIC);
        put_u32(&mut body, nrows as u32);
        put_u32(&mut body, self.xid);
        put_u32(&mut body, flags);
        put_u64(&mut body, 0);
        put_u64(&mut body, 0);

        let mut chunk_meta: Vec<(u64, i64, i64)> = Vec::with_capacity(self.ncols);
        let mut sums: Vec<i128> = Vec::with_capacity(self.ncols);
        let builders = std::mem::take(&mut self.builders);
        for (c, b) in builders.iter().enumerate() {
            while body.len() % 64 != 0 {
                body.push(0);
            }
            let chunk_off = body.len() as u64;
            let cc = CodecCtx { choice: self.opts.codec[c], zstd_level: self.opts.zstd_level };
            let (min, max) = match b {
                ColBuilder::Int(ib) => encode_int_chunk(&mut body, &ib.vals, ngranules, &cc),
                ColBuilder::Text(tb) => encode_text_chunk(&mut body, tb, ngranules, &cc),
            };
            chunk_meta.push((chunk_off, min, max));
            sums.push(match b {
                ColBuilder::Int(ib) => ib.vals.iter().map(|&v| v as i128).sum(),
                ColBuilder::Text(_) => 0,
            });
            let _ = c;
        }
        // v7 per-granule length stats, straight off the buffered per-row
        // (off, len) ranges (exact octet_length: the stored payload byte
        // count). Granule slots past the last row stay zero. cbstore stores
        // no NULLs (append_row errors), so nonnull = granule rows.
        let nlencols = self.coltypes.iter().filter(|t| t.is_text()).count();
        let mut lenstats: Vec<(u64, u32, u32)> = Vec::new();
        if nlencols > 0 {
            lenstats = vec![(0u64, 0u32, 0u32); GRANULES_PER_RG * nlencols];
            let mut rank = 0usize;
            for b in builders.iter() {
                let ColBuilder::Text(tb) = b else { continue };
                for g in 0..nrows.div_ceil(GRANULE_ROWS) {
                    let lo = g * GRANULE_ROWS;
                    let hi = (lo + GRANULE_ROWS).min(nrows);
                    let mut sum = 0u64;
                    let mut empty = 0u32;
                    for &(_, len) in &tb.offs[lo..hi] {
                        sum += len as u64;
                        empty += (len == 0) as u32;
                    }
                    lenstats[g * nlencols + rank] = (sum, (hi - lo) as u32, empty);
                }
                rank += 1;
            }
        }
        // Patch total RG length.
        let total = align64(body.len() as u64);
        body[16..24].copy_from_slice(&total.to_le_bytes());
        body.resize(total as usize, 0);

        let file_off = align64(self.write_off);
        self.file.write_all_at(&body, file_off)?;
        self.write_off = file_off + total;
        self.rgs.push(FooterRg {
            file_off,
            nrows: nrows as u32,
            xmin: self.xid,
            flags,
            chunks: chunk_meta,
            sums,
            lenstats,
        });
        self.reset_builders();
        Ok(())
    }

    /// pub for the test-support writer (`open_writer_at`).
    #[doc(hidden)]
    pub fn finish(&mut self) -> PgResult<()> {
        // Cluster-key drain: sort the buffered ingest and feed it through the
        // ordinary append path (NDV/sorted trackers and RG seals see rows in
        // final order, so their metadata is exact for the sorted part).
        if let Some(mut sorter) = self.sorter.take() {
            sorter.sort()?;
            self.draining_clustered = true;
            let mut values = vec![Datum::null(); self.ncols];
            let mut isnull = vec![false; self.ncols];
            while sorter.next_row(&mut values, &mut isnull)? {
                self.append_row(&values, &isnull)?;
            }
        }
        self.seal_rg()?;
        // Footer.
        let mut f: Vec<u8> = Vec::with_capacity(64 + self.rgs.len() * (24 + self.ncols * 24));
        put_u32(&mut f, self.rgs.len() as u32);
        put_u32(&mut f, self.ncols as u32);
        // v7 prelude tail: per-column length-stats flags (1 = the column has
        // per-granule entries in the trailing length-stats section). In the
        // prelude — not appended — because read_footer_rgs must size the
        // body read from nrgs/ncols alone.
        for t in &self.coltypes {
            f.push(t.is_text() as u8);
        }
        for rg in &self.rgs {
            put_u64(&mut f, rg.file_off);
            put_u32(&mut f, rg.nrows);
            put_u32(&mut f, rg.xmin);
            put_u32(&mut f, rg.flags);
            put_u32(&mut f, 0);
        }
        for rg in &self.rgs {
            for &(off, min, max) in &rg.chunks {
                put_u64(&mut f, off);
                put_i64(&mut f, min);
                put_i64(&mut f, max);
            }
        }
        // v2 NDV section; a distinct count can never be 0 for a nonempty
        // part, so 0 encodes unknown (append-invalidated sketch).
        let total_rows: u64 = self.rgs.iter().map(|rg| rg.nrows as u64).sum();
        for c in 0..self.ncols {
            let est = match &self.ndv {
                Some(hlls) => hlls[c].estimate().clamp(1, total_rows.max(1)),
                None => 0,
            };
            put_u64(&mut f, est);
        }
        // v4 sums section; RGs preserved from a v<=3 footer write zeros and
        // lack RG_FLAG_SUMS.
        for rg in &self.rgs {
            for c in 0..self.ncols {
                put_i128(&mut f, rg.sums.get(c).copied().unwrap_or(0));
            }
        }
        // v5 sorted section; 0 = unknown (append-invalidated tracker).
        for c in 0..self.ncols {
            f.push(self.sorted.as_ref().is_some_and(|s| s[c]) as u8);
        }
        // v6 cluster-key section (fixed width): the declared key this writer
        // sorted under (0 keys = none); adaptive traversal gates per RG on
        // RG_FLAG_CLUSTERED.
        debug_assert!(self.opts.cluster_key.len() <= CB_CLUSTER_KEY_MAX_COLS);
        f.extend_from_slice(&(self.opts.cluster_key.len() as u16).to_le_bytes());
        for slot in 0..CB_CLUSTER_KEY_MAX_COLS {
            let c = self.opts.cluster_key.get(slot).map(|&(c, _)| c).unwrap_or(0);
            f.extend_from_slice(&c.to_le_bytes());
        }
        // v7 length-stats section (format.rs doc): rg-major, GRANULES_PER_RG
        // fixed granule slots, flagged columns ascending. RGs preserved from
        // v<=6 footers carry no entries (empty lenstats) and lack
        // RG_FLAG_LENSTATS — they write zeros.
        let nlencols = self.coltypes.iter().filter(|t| t.is_text()).count();
        for rg in &self.rgs {
            for slot in 0..GRANULES_PER_RG * nlencols {
                let (s, n, e) = rg.lenstats.get(slot).copied().unwrap_or((0, 0, 0));
                put_u64(&mut f, s);
                put_u32(&mut f, n);
                put_u32(&mut f, e);
            }
        }
        let crc = crc32c(&f);
        let flen = (f.len() + 16) as u64;
        put_u64(&mut f, flen);
        put_u32(&mut f, crc);
        put_u32(&mut f, CB_FOOTER_MAGIC);

        let footer_off = align64(self.write_off);
        self.file.write_all_at(&f, footer_off)?;
        self.write_off = footer_off + flen;

        // Data + footer durable before the publish (impl doc §5); segment
        // tails padded to BLCKSZ multiples for md's block accounting.
        self.file.pad_and_sync(self.write_off)?;
        self.write_header(footer_off)?;
        self.file.sync_data()?;
        Ok(())
    }
}

fn granule_minmax<T: Copy, F: Fn(T) -> i64>(vals: &[T], g: usize, f: F) -> (i64, i64) {
    let lo = g * GRANULE_ROWS;
    let hi = (lo + GRANULE_ROWS).min(vals.len());
    let mut min = i64::MAX;
    let mut max = i64::MIN;
    for &v in &vals[lo..hi] {
        let x = f(v);
        min = min.min(x);
        max = max.max(x);
    }
    (min, max)
}

// Arm a bloom only where granule zone maps are uninformative (the average
// granule covers most of the chunk's range — i.e. the column is not a
// leading sort key of the load order) and the RG is high-NDV.
const BLOOM_MIN_NDV: usize = 4_096;

fn bloom_armed(vals: &[i64], min: i64, max: i64, gmm: &[(i64, i64)]) -> bool {
    let range = max as i128 - min as i128;
    if range == 0 || gmm.is_empty() {
        return false;
    }
    let sum: i128 = gmm.iter().map(|&(lo, hi)| hi as i128 - lo as i128).sum();
    if (sum / gmm.len() as i128) * 2 < range {
        return false;
    }
    let mut seen = HashMap::with_capacity(vals.len());
    for &v in vals {
        seen.insert(v, ());
        if seen.len() >= BLOOM_MIN_NDV {
            return true;
        }
    }
    false
}

// Serialize granule g's plain FOR/Raw payload image into `out`.
fn enc_int_granule(out: &mut Vec<u8>, vals: &[i64], g: usize, encoding: Encoding, width: u8, base: i64) {
    let lo = g * GRANULE_ROWS;
    let hi = (lo + GRANULE_ROWS).min(vals.len());
    match encoding {
        Encoding::For => match width {
            1 => out.extend(vals[lo..hi].iter().map(|&v| (v - base) as u8)),
            2 => {
                for &v in &vals[lo..hi] {
                    out.extend_from_slice(&(((v - base) as u16).to_le_bytes()));
                }
            }
            4 => {
                for &v in &vals[lo..hi] {
                    out.extend_from_slice(&(((v - base) as u32).to_le_bytes()));
                }
            }
            _ => unreachable!(),
        },
        Encoding::Raw => {
            for &v in &vals[lo..hi] {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
        _ => unreachable!(),
    }
}

pub(crate) fn encode_int_chunk(
    body: &mut Vec<u8>,
    vals: &[i64],
    ngranules: u32,
    cc: &CodecCtx,
) -> (i64, i64) {
    let n = vals.len();
    let min = vals.iter().copied().min().unwrap();
    let max = vals.iter().copied().max().unwrap();
    let (encoding, width) = if min == max {
        (Encoding::Const, 0u8)
    } else {
        let range = (max as i128 - min as i128) as u128;
        let w = if range <= u8::MAX as u128 {
            1
        } else if range <= u16::MAX as u128 {
            2
        } else if range <= u32::MAX as u128 {
            4
        } else {
            8
        };
        if w == 8 { (Encoding::Raw, 8) } else { (Encoding::For, w) }
    };
    let ng = ngranules as usize;
    let mut gmm = Vec::with_capacity(ng);
    for g in 0..ng {
        gmm.push(granule_minmax(vals, g, |v| v));
    }
    let mut flags = 0u16;
    if encoding != Encoding::Const {
        flags |= CHUNK_FLAG_BLOCK_ZM;
        if bloom_armed(vals, min, max, &gmm) {
            flags |= CHUNK_FLAG_BLOOM;
        }
    }
    // v6 codec menu: sample granule 0 to pick a codec, then frame every
    // granule; keep the plain zero-decode lane unless the full chunk clears
    // the >=10% win gate too. Narrow FOR lanes (1-2B) are the decode-hot
    // class — pick() holds them to a stricter ZSTD threshold.
    let mut frames: Vec<(Vec<u8>, u32)> = Vec::new();
    let mut codec = Codec::None;
    if encoding != Encoding::Const {
        let mut plain = Vec::with_capacity(GRANULE_ROWS * width as usize);
        enc_int_granule(&mut plain, vals, 0, encoding, width, min);
        if let Some(c) = cc.pick(&plain, encoding == Encoding::For && width <= 2) {
            let mut frames_len = 0usize;
            let mut raw_len = 0usize;
            frames.reserve(ng);
            for g in 0..ng {
                if g > 0 {
                    plain.clear();
                    enc_int_granule(&mut plain, vals, g, encoding, width, min);
                }
                let comp = cc.compress(c, &plain);
                frames_len += frame_len(comp.len());
                raw_len += plain.len();
                frames.push((comp, plain.len() as u32));
            }
            if frames_len * 10 <= raw_len * 9 {
                codec = c;
            } else {
                frames.clear();
            }
        }
    }
    let payload_len = match encoding {
        Encoding::Const => 0u64,
        _ if codec != Codec::None => {
            frames.iter().map(|(f, _)| frame_len(f.len()) as u64).sum()
        }
        _ => (n * width as usize) as u64,
    };
    ChunkHeader {
        encoding,
        width,
        flags,
        ngranules,
        aux: min,
        payload_len,
        codec,
    }
    .encode(body);
    let mut frame_off = 0u64;
    for (g, &(gmin, gmax)) in gmm.iter().enumerate() {
        let off = if codec != Codec::None {
            let o = frame_off;
            frame_off += frame_len(frames[g].0.len()) as u64;
            o
        } else {
            ((g * GRANULE_ROWS) * width as usize) as u64
        };
        put_u64(body, off);
        put_i64(body, gmin);
        put_i64(body, gmax);
    }
    if flags & CHUNK_FLAG_BLOCK_ZM != 0 {
        for g in 0..ng {
            for b in 0..BLOCKS_PER_GRANULE {
                let lo = g * GRANULE_ROWS + b * BLOCK_ROWS;
                let hi = (lo + BLOCK_ROWS).min(n);
                if lo >= n {
                    put_i64(body, i64::MAX);
                    put_i64(body, i64::MIN);
                    continue;
                }
                let mut bmin = i64::MAX;
                let mut bmax = i64::MIN;
                for &v in &vals[lo..hi] {
                    bmin = bmin.min(v);
                    bmax = bmax.max(v);
                }
                put_i64(body, bmin);
                put_i64(body, bmax);
            }
        }
    }
    if flags & CHUNK_FLAG_BLOOM != 0 {
        for g in 0..ng {
            let lo = g * GRANULE_ROWS;
            let hi = (lo + GRANULE_ROWS).min(n);
            let start = body.len();
            body.resize(start + crate::bloom::BLOOM_BYTES, 0);
            for &v in &vals[lo..hi] {
                crate::bloom::bloom_insert(&mut body[start..], v);
            }
        }
    }
    while body.len() % 64 != 0 {
        body.push(0);
    }
    match encoding {
        Encoding::Const => {}
        Encoding::For | Encoding::Raw => {
            if codec != Codec::None {
                for (comp, raw_len) in &frames {
                    push_frame(body, *raw_len as usize, comp);
                }
            } else {
                for g in 0..ng {
                    enc_int_granule(body, vals, g, encoding, width, min);
                }
            }
        }
        _ => unreachable!(),
    }
    (min, max)
}

// Text payloads (impl doc §1.4): blob entries are complete 4B-U varlena
// images, 4-byte aligned, so decode publishes pointers with no copies.
//   Dict:    codes[n] (1/2/4 B) | dict_off[ndv] u32 | blob
//   RawText: off[n] u32 | blob
fn encode_text_chunk(
    body: &mut Vec<u8>,
    tb: &TextBuilder,
    ngranules: u32,
    cc: &CodecCtx,
) -> (i64, i64) {
    let n = tb.offs.len();

    // Dictionary pass over the row set.
    let mut dict: HashMap<&[u8], u32> = HashMap::with_capacity(1024);
    let mut order: Vec<(u32, u32)> = Vec::new();
    let mut codes: Vec<u32> = Vec::with_capacity(n);
    let mut dict_blob_len = 0usize;
    for &(off, len) in &tb.offs {
        let s = &tb.blob[off as usize..(off + len) as usize];
        let next = order.len() as u32;
        let code = *dict.entry(s).or_insert_with(|| {
            order.push((off, len));
            dict_blob_len += align4(VARLENA_IMG_HDR + len as usize);
            next
        });
        codes.push(code);
    }
    let ndv = order.len();
    let code_w: usize = if ndv <= 1 << 8 {
        1
    } else if ndv <= 1 << 16 {
        2
    } else {
        4
    };
    let raw_blob_len: usize =
        tb.offs.iter().map(|&(_, l)| align4(VARLENA_IMG_HDR + l as usize)).sum();
    let dict_size = n * code_w + ndv * 4 + dict_blob_len;
    let raw_size = n * 4 + raw_blob_len;
    let use_dict = ndv <= 65_536 && dict_size < raw_size;

    // Zone maps: byte length min/max per granule.
    let (mut min, mut max) = (i64::MAX, i64::MIN);
    let mut gmm = Vec::with_capacity(ngranules as usize);
    for g in 0..ngranules as usize {
        let (gmin, gmax) = granule_minmax(&tb.offs, g, |(_, l)| l as i64);
        gmm.push((gmin, gmax));
        min = min.min(gmin);
        max = max.max(gmax);
    }

    if use_dict {
        // Byte-order dict sort + code remap (CHUNK_FLAG_DICT_SORTED): codes
        // become rank order so a LIKE-prefix can evaluate as a code-range
        // check. dict[code] is unchanged under the consistent remap.
        let entry = |&(off, len): &(u32, u32)| &tb.blob[off as usize..(off + len) as usize];
        let mut perm: Vec<u32> = (0..ndv as u32).collect();
        perm.sort_unstable_by(|&a, &b| entry(&order[a as usize]).cmp(entry(&order[b as usize])));
        let mut remap = vec![0u32; ndv];
        for (new, &old) in perm.iter().enumerate() {
            remap[old as usize] = new as u32;
        }
        let order: Vec<(u32, u32)> = perm.iter().map(|&o| order[o as usize]).collect();
        for c in codes.iter_mut() {
            *c = remap[*c as usize];
        }
        // Compressed-dict candidate: one frame over the varlena-image dict
        // blob (codec from the v6 menu, sampled on the blob itself), taken on
        // a >=10% payload win; codes + dict_off stay plain.
        let mut dict_blob: Vec<u8> = Vec::with_capacity(dict_blob_len);
        for &(off, len) in &order {
            push_varlena_image(&mut dict_blob, &tb.blob[off as usize..(off + len) as usize]);
        }
        debug_assert_eq!(dict_blob.len(), dict_blob_len);
        let head_len = align4(n * code_w) + ndv * 4;
        let picked = cc.pick(&dict_blob, false);
        let comp = picked.map(|c| cc.compress(c, &dict_blob)).unwrap_or_default();
        let comp_blob_len = frame_len(comp.len());
        let use_comp = picked.is_some()
            && (head_len + comp_blob_len) * 10 <= (head_len + dict_blob_len) * 9;
        let (encoding, codec, stored_blob_len) = if use_comp {
            (Encoding::Lz4Dict, picked.unwrap(), comp_blob_len)
        } else {
            (Encoding::Dict, Codec::None, dict_blob_len)
        };
        let payload_len = (head_len + stored_blob_len) as u64;
        ChunkHeader {
            encoding,
            width: code_w as u8,
            flags: CHUNK_FLAG_DICT_SORTED,
            ngranules,
            aux: ndv as i64,
            payload_len,
            codec,
        }
        .encode(body);
        for (g, &(gmin, gmax)) in gmm.iter().enumerate() {
            put_u64(body, (g * GRANULE_ROWS * code_w) as u64);
            put_i64(body, gmin);
            put_i64(body, gmax);
        }
        while body.len() % 64 != 0 {
            body.push(0);
        }
        match code_w {
            1 => body.extend(codes.iter().map(|&c| c as u8)),
            2 => {
                for &c in &codes {
                    body.extend_from_slice(&(c as u16).to_le_bytes());
                }
            }
            _ => {
                for &c in &codes {
                    body.extend_from_slice(&c.to_le_bytes());
                }
            }
        }
        while body.len() % 4 != 0 {
            body.push(0);
        }
        // dict_off table (offsets into the DECOMPRESSED blob) then the blob.
        let mut blob_off = 0u32;
        for &(_, len) in &order {
            put_u32(body, blob_off);
            blob_off += align4(VARLENA_IMG_HDR + len as usize) as u32;
        }
        if use_comp {
            let start = body.len();
            put_u32(body, dict_blob_len as u32);
            put_u32(body, comp.len() as u32);
            body.extend_from_slice(&comp);
            while body.len() - start != comp_blob_len {
                body.push(0);
            }
        } else {
            body.extend_from_slice(&dict_blob);
        }
    } else {
        // Compressed-text candidate (S3 footprint step): per-granule frames
        // (codec from the v6 menu, sampled on granule 0's blob) over the
        // varlena-image blob, granule-relative offsets; decode is
        // decompress-then-pointer-gather. Taken only on a >=10% payload win
        // so incompressible chunks keep the zero-decode RAWTEXT lane.
        let mut frames: Vec<(Vec<u8>, u32)> = Vec::with_capacity(ngranules as usize);
        let mut offs_rel: Vec<u32> = Vec::with_capacity(n);
        let mut max_raw = 0usize;
        let mut comp_frames_len = 0usize;
        let mut gblob: Vec<u8> = Vec::new();
        let mut picked: Option<Codec> = None;
        for g in 0..ngranules as usize {
            let lo = g * GRANULE_ROWS;
            let hi = (lo + GRANULE_ROWS).min(n);
            gblob.clear();
            for &(off, len) in &tb.offs[lo..hi] {
                offs_rel.push(gblob.len() as u32);
                push_varlena_image(&mut gblob, &tb.blob[off as usize..(off + len) as usize]);
            }
            max_raw = max_raw.max(gblob.len());
            if g == 0 {
                picked = cc.pick(&gblob, false);
            }
            let Some(c) = picked else { break };
            let comp = cc.compress(c, &gblob);
            comp_frames_len += frame_len(comp.len());
            frames.push((comp, gblob.len() as u32));
        }
        let comp_size = n * 4 + comp_frames_len;
        if picked.is_some() && comp_size * 10 <= (n * 4 + raw_blob_len) * 9 {
            let payload_len = (n * 4) as u64 + comp_frames_len as u64;
            ChunkHeader {
                encoding: Encoding::Lz4Text,
                width: 4,
                flags: 0,
                ngranules,
                aux: max_raw as i64,
                payload_len,
                codec: picked.unwrap(),
            }
            .encode(body);
            let mut frame_off = (n * 4) as u64;
            for (g, &(gmin, gmax)) in gmm.iter().enumerate() {
                put_u64(body, frame_off);
                put_i64(body, gmin);
                put_i64(body, gmax);
                frame_off += align4(8 + frames[g].0.len()) as u64;
            }
            while body.len() % 64 != 0 {
                body.push(0);
            }
            for &o in &offs_rel {
                put_u32(body, o);
            }
            for (comp, raw_len) in &frames {
                let start = body.len();
                put_u32(body, *raw_len);
                put_u32(body, comp.len() as u32);
                body.extend_from_slice(comp);
                while body.len() - start != align4(8 + comp.len()) {
                    body.push(0);
                }
            }
        } else {
            let payload_len = (n * 4) as u64 + raw_blob_len as u64;
            ChunkHeader {
                encoding: Encoding::RawText,
                width: 4,
                flags: 0,
                ngranules,
                aux: 0,
                payload_len,
                codec: Codec::None,
            }
            .encode(body);
            for (g, &(gmin, gmax)) in gmm.iter().enumerate() {
                put_u64(body, (g * GRANULE_ROWS * 4) as u64);
                put_i64(body, gmin);
                put_i64(body, gmax);
            }
            while body.len() % 64 != 0 {
                body.push(0);
            }
            let mut blob_off = 0u32;
            for &(_, len) in &tb.offs {
                put_u32(body, blob_off);
                blob_off += align4(VARLENA_IMG_HDR + len as usize) as u32;
            }
            for &(off, len) in &tb.offs {
                push_varlena_image(body, &tb.blob[off as usize..(off + len) as usize]);
            }
        }
    }
    (min, max)
}

const VARLENA_IMG_HDR: usize = 4;

fn push_varlena_image(body: &mut Vec<u8>, s: &[u8]) {
    body.extend_from_slice(&::datum::set_varsize_4b(VARLENA_IMG_HDR + s.len()));
    body.extend_from_slice(s);
    while body.len() % 4 != 0 {
        body.push(0);
    }
}

// ---- AM entry points -------------------------------------------------------

pub fn multi_insert<'mcx>(
    rel: &::types_rel::Relation<'mcx>,
    slots: &mut [&mut ::types_slot::SlotData<'mcx>],
) -> PgResult<()> {
    let oid = rel.rd_id;
    let xid = xact_seams::get_current_transaction_id::call()?;
    let cid = xact_seams::get_current_command_id::call(false)?;
    WRITERS.with(|w| {
        let mut map = w.borrow_mut();
        // Evict writers from another transaction OR another command: buffered
        // ingest is per-statement (the statement-end flush publishes it), so
        // a writer abandoned by an errored statement must not leak its rows
        // into a later statement of the same transaction.
        let stale = map.get(&oid).is_some_and(|cw| cw.xid != xid || cw.cid != cid);
        if stale {
            map.remove(&oid);
        }
        if !map.contains_key(&oid) {
            map.insert(oid, open_writer(rel)?);
        }
        let cw = map.get_mut(&oid).unwrap();
        for slot in slots.iter_mut() {
            // Deform before reading: buffer/heap-backed slots (CTAS from a
            // heap table feeds the scan slot straight in) arrive with
            // tts_nvalid == 0, and reading tts_values/tts_isnull raw off
            // them fabricated NULLs on NULL-free data. No-op on the
            // already-deformed COPY/INSERT..SELECT virtual slots.
            exectuples::slot_getallattrs(slot);
            let base = slot.base();
            debug_assert!(base.tts_nvalid as usize >= cw.ncols);
            cw.ingest_row(&base.tts_values, &base.tts_isnull)?;
        }
        Ok(())
    })
}

pub fn tuple_insert<'mcx>(
    rel: &::types_rel::Relation<'mcx>,
    slot: &mut ::types_slot::SlotData<'mcx>,
) -> PgResult<()> {
    // Single-row inserts buffer like COPY: the row joins the per-(xid, cid)
    // ingest writer and the statement-end flush (ExecModifyTable's cbstore
    // finish, or COPY's finish_bulk_insert) publishes RG-sized seals. The
    // old finish-per-row form sealed ONE ROW GROUP PER ROW on INSERT..SELECT
    // (24 GB for 2M rows), each with a full footer rewrite.
    let mut slots = [slot];
    multi_insert(rel, &mut slots)
}

pub fn finish_bulk_insert(rel: &::types_rel::Relation<'_>) -> PgResult<()> {
    let oid = rel.rd_id;
    WRITERS.with(|w| {
        let Some(mut cw) = w.borrow_mut().remove(&oid) else {
            return Ok(());
        };
        // Never publish a writer abandoned by an errored statement (or a
        // rolled-back subtransaction): a later statement's flush must drop
        // it, not commit its buffered rows. Mirror of multi_insert's stale
        // eviction; probes without get_current_transaction_id so a row-less
        // statement doesn't force an xid assignment.
        if !xact_seams::transaction_id_is_current_transaction_id::call(cw.xid)
            || cw.cid != xact_seams::get_current_command_id::call(false)?
        {
            return Ok(());
        }
        cw.finish()
    })
}

#[cfg(test)]
mod abortsafe_tests {
    use super::*;
    use crate::reader::{part_footer_rows, Part};

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-abortsafe-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    fn put_ints(w: &mut CbWriter, n: usize) {
        for i in 0..n {
            w.append_row(&[Datum::from_i64(i as i64)], &[false]).unwrap();
        }
    }

    // Abort mid-ingest on a fresh part (writer dropped without finish, row
    // groups already sealed to disk): header-first init keeps the part
    // readable as EMPTY, and a later committed load reads back exactly its
    // own rows.
    #[test]
    fn abort_mid_ingest_fresh_part_stays_readable() {
        let path = tmp("fresh-abort");
        let mut w = open_writer_at(&path, vec![ColType::I64]).unwrap();
        put_ints(&mut w, RG_ROWS + 5); // one RG sealed to disk + 5 buffered
        drop(w); // statement error: no finish, no publish
        assert!(std::fs::metadata(&path).unwrap().len() > CB_HEADER_LEN);
        assert_eq!(part_footer_rows(&path, 1).unwrap(), None);
        assert!(Part::open(&path, 1).unwrap().is_none());

        let mut w2 = open_writer_at(&path, vec![ColType::I64]).unwrap();
        put_ints(&mut w2, 3);
        w2.finish().unwrap();
        assert_eq!(part_footer_rows(&path, 1).unwrap(), Some(3));
        assert_eq!(Part::open(&path, 1).unwrap().unwrap().total_rows(), 3);
        std::fs::remove_file(&path).unwrap();
    }

    // Legacy corruption shape (pre-header-first writers): row-group bytes on
    // disk with a zero hole where the header belongs. Reads treat it as an
    // empty part instead of erroring forever, and a writer reopen heals it.
    #[test]
    fn legacy_zero_header_hole_heals() {
        let path = tmp("zero-hole");
        std::fs::write(&path, vec![0u8; 200_000]).unwrap();
        assert_eq!(part_footer_rows(&path, 1).unwrap(), None);
        assert!(Part::open(&path, 1).unwrap().is_none());

        let mut w = open_writer_at(&path, vec![ColType::I64]).unwrap();
        put_ints(&mut w, 7);
        w.finish().unwrap();
        assert_eq!(part_footer_rows(&path, 1).unwrap(), Some(7));
        assert_eq!(Part::open(&path, 1).unwrap().unwrap().total_rows(), 7);
        std::fs::remove_file(&path).unwrap();
    }

    // A header whose footer_off points past EOF (torn state) must produce a
    // clean error on every read path — never a huge alloc or a slice panic.
    #[test]
    fn torn_footer_pointer_errors_cleanly() {
        let path = tmp("torn-footer");
        let mut hdr = Vec::new();
        put_u64(&mut hdr, CB_MAGIC);
        put_u32(&mut hdr, CB_VERSION);
        put_u32(&mut hdr, 1);
        put_u64(&mut hdr, 1 << 40); // footer_off far past EOF
        put_u64(&mut hdr, 0x5aa5);
        hdr.resize(CB_HEADER_LEN as usize, 0);
        std::fs::write(&path, &hdr).unwrap();
        assert!(part_footer_rows(&path, 1).is_err());
        assert!(Part::open(&path, 1).is_err());
        std::fs::remove_file(&path).unwrap();
    }

    // Header-first: opening a writer over an empty part publishes a valid
    // empty header immediately (readers concurrent with the first COPY see
    // an empty part, and an abort at ANY later point leaves it readable).
    #[test]
    fn writer_open_initializes_header() {
        let path = tmp("init-hdr");
        let w = open_writer_at(&path, vec![ColType::I64]).unwrap();
        drop(w);
        let bytes = std::fs::read(&path).unwrap();
        assert!(bytes.len() as u64 >= CB_HEADER_LEN);
        let (footer_off, fp, version) =
            crate::reader::read_header(&bytes[..CB_HEADER_LEN as usize]).unwrap();
        assert_eq!((footer_off, fp, version), (0, 0x5aa5, CB_VERSION));
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod lenstats_tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-lenstats-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    fn writer_at(path: &str, coltypes: Vec<ColType>) -> CbWriter {
        let opts = CbWriterOpts::plain(coltypes.len());
        open_writer_inner(SegFile::open_rw(path).unwrap(), 1, 0, true, coltypes, 0x5aa5, opts)
            .unwrap()
    }

    fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    // Deterministic length pattern with empties sprinkled in (multibyte
    // bytes included — the stats are octet counts, encoding-agnostic).
    fn row_text(i: usize) -> Vec<u8> {
        match i % 7 {
            0 => Vec::new(),
            1 => b"\xc3\xa9".to_vec(), // 2-byte UTF-8
            k => vec![b'x'; k * 3],
        }
    }

    #[test]
    fn lenstats_roundtrip_exact_per_granule() {
        let path = tmp("rt");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::Text]);
        // 2 RGs, the second partial mid-granule (RG_ROWS + 1.5 granules).
        let n = RG_ROWS + GRANULE_ROWS + GRANULE_ROWS / 2;
        let mut keep = Vec::new();
        for i in 0..n {
            let t = row_text(i);
            let vals = [Datum::from_i64(i as i64), text_datum(&t, &mut keep)];
            w.append_row(&vals, &[false, false]).unwrap();
            keep.clear();
        }
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert!(part.has_len_stats(1));
        assert!(!part.has_len_stats(0));
        for rg in 0..part.rgs.len() {
            assert_ne!(part.rgs[rg].flags & RG_FLAG_LENSTATS, 0);
            let rg_rows = part.rgs[rg].nrows as usize;
            for g in 0..rg_rows.div_ceil(GRANULE_ROWS) {
                let lo = rg * RG_ROWS + g * GRANULE_ROWS;
                let hi = lo + (rg_rows - g * GRANULE_ROWS).min(GRANULE_ROWS);
                let (mut sum, mut empty) = (0u64, 0u32);
                for i in lo..hi {
                    let t = row_text(i);
                    sum += t.len() as u64;
                    empty += t.is_empty() as u32;
                }
                assert_eq!(
                    part.granule_len_stats(rg, g, 1),
                    Some((sum, (hi - lo) as u32, empty)),
                    "rg {rg} g {g}"
                );
                assert_eq!(part.granule_len_stats(rg, g, 0), None);
            }
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn lenstats_survive_reopen_append() {
        let path = tmp("reopen");
        let mut w = writer_at(&path, vec![ColType::I32, ColType::Text]);
        let mut keep = Vec::new();
        for i in 0..1000usize {
            let t = row_text(i);
            let vals = [Datum::from_i64(i as i64), text_datum(&t, &mut keep)];
            w.append_row(&vals, &[false, false]).unwrap();
            keep.clear();
        }
        w.finish().unwrap();
        // Reopen-append: the preserved RG's stats must re-emit exactly; the
        // new RG gets its own.
        let mut w2 = writer_at(&path, vec![ColType::I32, ColType::Text]);
        let d = text_datum(b"tail-row", &mut keep);
        w2.append_row(&[Datum::from_i64(7), d], &[false, false]).unwrap();
        w2.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.rgs.len(), 2);
        let (mut sum, mut empty) = (0u64, 0u32);
        for i in 0..1000usize {
            let t = row_text(i);
            sum += t.len() as u64;
            empty += t.is_empty() as u32;
        }
        assert_eq!(part.granule_len_stats(0, 0, 1), Some((sum, 1000, empty)));
        assert_eq!(part.granule_len_stats(1, 0, 1), Some((8, 1, 0)));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn lenstats_absent_without_text_columns() {
        let path = tmp("notext");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::I32]);
        w.append_row(&[Datum::from_i64(1), Datum::from_i64(2)], &[false, false]).unwrap();
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert!(!part.has_len_stats(0) && !part.has_len_stats(1));
        assert_eq!(part.granule_len_stats(0, 0, 0), None);
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod sorted_flag_tests {
    use super::*;

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-sorted-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    fn writer_at(path: &str, coltypes: Vec<ColType>) -> CbWriter {
        let opts = CbWriterOpts::plain(coltypes.len());
        open_writer_inner(SegFile::open_rw(path).unwrap(), 1, 0, true, coltypes, 0x5aa5, opts).unwrap()
    }

    // 4B-U inline varlena image; returns the backing buffer + datum.
    fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    fn put_rows(w: &mut CbWriter, ints: &[i64], texts: &[&[u8]]) {
        let mut keep = Vec::new();
        for (i, &v) in ints.iter().enumerate() {
            let vals = [Datum::from_i64(v), text_datum(texts[i], &mut keep)];
            w.append_row(&vals, &[false, false]).unwrap();
        }
    }

    #[test]
    fn sorted_flags_roundtrip_and_seams() {
        let path = tmp("rt");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::Text]);
        // Int non-decreasing across an RG seal; text dips at the last row.
        let n = RG_ROWS + 10;
        let ints: Vec<i64> = (0..n as i64).map(|i| i / 3).collect();
        let mut texts: Vec<Vec<u8>> = (0..n).map(|i| format!("k{:08}", i / 5).into_bytes()).collect();
        texts[n - 1] = b"a-dip".to_vec();
        let trefs: Vec<&[u8]> = texts.iter().map(|t| t.as_slice()).collect();
        put_rows(&mut w, &ints, &trefs);
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.sorted, vec![1, 0]);

        // Reopen-append invalidates to unknown even for still-sorted data.
        let mut w2 = writer_at(&path, vec![ColType::I64, ColType::Text]);
        assert!(w2.sorted.is_none());
        put_rows(&mut w2, &[i64::MAX], &[b"zzz"]);
        w2.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.sorted, vec![0, 0]);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn sorted_flags_equal_runs_and_empty_part() {
        let path = tmp("eq");
        let mut w = writer_at(&path, vec![ColType::I32, ColType::Text]);
        // All-equal columns are (vacuously) non-decreasing.
        let ints = vec![7i64; 100];
        let texts: Vec<&[u8]> = vec![b"same"; 100];
        put_rows(&mut w, &ints, &texts);
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.sorted, vec![1, 1]);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn sorted_flag_int_dip_detected_across_seal() {
        let path = tmp("dip");
        let mut w = writer_at(&path, vec![ColType::I64, ColType::Text]);
        // First row of the second RG dips below the first RG's tail.
        let mut ints: Vec<i64> = (0..(RG_ROWS + 1) as i64).collect();
        ints[RG_ROWS] = -1;
        let texts: Vec<&[u8]> = vec![b"t"; RG_ROWS + 1];
        put_rows(&mut w, &ints, &texts);
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.sorted, vec![0, 1]);
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod dict_sort_tests {
    use super::*;

    fn tb_of(rows: &[&[u8]]) -> TextBuilder {
        let mut tb = TextBuilder { offs: Vec::new(), blob: Vec::new() };
        for r in rows {
            tb.offs.push((tb.blob.len() as u32, r.len() as u32));
            tb.blob.extend_from_slice(r);
        }
        tb
    }

    // Parse an encode_text_chunk image back into (codes, dict payloads).
    fn parse_dict_chunk(body: &[u8], n: usize) -> (ChunkHeader, Vec<u32>, Vec<Vec<u8>>) {
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        let gdir_end = CB_CHUNK_HEADER_LEN + hdr.ngranules as usize * CB_GRANULE_ENTRY_LEN;
        let p = &body[align64(gdir_end as u64) as usize..];
        let w = hdr.width as usize;
        let ndv = hdr.aux as usize;
        let codes: Vec<u32> = (0..n)
            .map(|i| match w {
                1 => p[i] as u32,
                2 => u16::from_le_bytes(p[i * 2..i * 2 + 2].try_into().unwrap()) as u32,
                _ => get_u32(p, i * 4),
            })
            .collect();
        let codes_len = align4(n * w);
        let off_tab = &p[codes_len..codes_len + ndv * 4];
        let blob = &p[codes_len + ndv * 4..];
        let raw = if hdr.encoding == Encoding::Lz4Dict {
            let raw_len = get_u32(blob, 0) as usize;
            let comp_len = get_u32(blob, 4) as usize;
            lz4_flex::decompress(&blob[8..8 + comp_len], raw_len).unwrap()
        } else {
            blob.to_vec()
        };
        let entries = off_tab
            .chunks_exact(4)
            .map(|c| {
                let o = u32::from_le_bytes(c.try_into().unwrap()) as usize;
                crate::varlena_bytes(Datum::from_usize(raw[o..].as_ptr() as usize))
                    .unwrap()
                    .to_vec()
            })
            .collect();
        (hdr, codes, entries)
    }

    #[test]
    fn dict_chunk_sorted_flag_and_roundtrip() {
        // Appearance order deliberately != byte order; duplicates force the
        // dict encoding; includes "", 0xFF-saturated, and prefix-nested keys.
        let rows: Vec<&[u8]> = vec![
            b"zebra", b"apple", b"zebra", b"", b"ab\xff\xff", b"ab", b"apple", b"aa", b"zebra",
            b"\xff", b"ab", b"abz", b"", b"apple", b"a", b"ab\xff\xff",
        ];
        let mut body = Vec::new();
        encode_text_chunk(&mut body, &tb_of(&rows), 1, &test_codec_ctx());
        let (hdr, codes, entries) = parse_dict_chunk(&body, rows.len());
        assert!(matches!(hdr.encoding, Encoding::Dict | Encoding::Lz4Dict));
        assert_ne!(hdr.flags & CHUNK_FLAG_DICT_SORTED, 0);
        assert!(entries.windows(2).all(|w| w[0] < w[1]), "dict must be strictly byte-sorted");
        for (i, r) in rows.iter().enumerate() {
            assert_eq!(&entries[codes[i] as usize][..], *r, "row {i} decode identity");
        }
    }

    #[test]
    fn rawtext_chunk_carries_no_sorted_flag() {
        // All-distinct incompressible rows: dict loses, RAWTEXT wins; the
        // sorted bit must not leak onto non-dict encodings.
        let rows: Vec<Vec<u8>> = (0..64u32)
            .map(|i| {
                let mut v = i.to_le_bytes().to_vec();
                v.extend((0..48).map(|j| (i.wrapping_mul(2654435761).wrapping_add(j)) as u8));
                v
            })
            .collect();
        let refs: Vec<&[u8]> = rows.iter().map(|v| &v[..]).collect();
        let mut body = Vec::new();
        encode_text_chunk(&mut body, &tb_of(&refs), 1, &test_codec_ctx());
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        assert!(matches!(hdr.encoding, Encoding::RawText | Encoding::Lz4Text));
        assert_eq!(hdr.flags & CHUNK_FLAG_DICT_SORTED, 0);
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    use crate::reader::ChunkView;

    fn decode_ints(body: &[u8], nrows: usize) -> Vec<i64> {
        let cv = ChunkView::at(body, 0, nrows as u32);
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        let mut got = Vec::new();
        for g in 0..cv.hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            got.extend(out.iter().map(|d| d.as_i64()));
        }
        got
    }

    // Repetitive (compressible) i64s across 2.5 granules; force each codec
    // and prove framed round-trips + the codec tag.
    #[test]
    fn int_frames_roundtrip_lz4_and_zstd() {
        let n = GRANULE_ROWS * 2 + GRANULE_ROWS / 2;
        let vals: Vec<i64> = (0..n as i64).map(|i| 1_000_000 + (i % 97) * 3).collect();
        for choice in [CodecChoice::Lz4, CodecChoice::Zstd] {
            let cc = CodecCtx { choice, zstd_level: ZSTD_LEVEL_DEFAULT };
            let mut body = Vec::new();
            let (min, max) =
                encode_int_chunk(&mut body, &vals, n.div_ceil(GRANULE_ROWS) as u32, &cc);
            assert_eq!((min, max), (1_000_000, 1_000_000 + 96 * 3));
            let cv = ChunkView::at(&body, 0, n as u32);
            let want = if choice == CodecChoice::Lz4 { Codec::Lz4 } else { Codec::Zstd };
            assert_eq!(cv.hdr.codec, want, "{choice:?}");
            assert_eq!(decode_ints(&body, n), vals, "{choice:?}");
        }
    }

    // Auto keeps the plain zero-decode lane on incompressible data.
    #[test]
    fn auto_keeps_plain_on_incompressible_ints() {
        let n = GRANULE_ROWS;
        let vals: Vec<i64> = (0..n as u64).map(|i| crate::hll::mix64(i) as i64).collect();
        let mut body = Vec::new();
        encode_int_chunk(&mut body, &vals, 1, &test_codec_ctx());
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_eq!(cv.hdr.codec, Codec::None);
        assert_eq!(cv.hdr.payload_len, (n * cv.hdr.width as usize) as u64);
        assert_eq!(decode_ints(&body, n), vals);
    }

    // Auto picks a codec on compressible data and the >=10%-win gate holds.
    #[test]
    fn auto_compresses_compressible_ints() {
        let n = GRANULE_ROWS * 2;
        let vals: Vec<i64> = (0..n as i64).map(|i| i / 64).collect();
        let mut body = Vec::new();
        encode_int_chunk(&mut body, &vals, 2, &test_codec_ctx());
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_ne!(cv.hdr.codec, Codec::None);
        assert!(cv.hdr.payload_len as usize * 10 <= n * cv.hdr.width as usize * 9);
        assert_eq!(decode_ints(&body, n), vals);
    }

    // Plain choice = the v5 byte behavior (no frames anywhere).
    #[test]
    fn plain_choice_writes_v5_shape() {
        let n = GRANULE_ROWS;
        let vals: Vec<i64> = (0..n as i64).map(|i| i / 64).collect();
        let cc = CodecCtx { choice: CodecChoice::Plain, zstd_level: ZSTD_LEVEL_DEFAULT };
        let mut body = Vec::new();
        encode_int_chunk(&mut body, &vals, 1, &cc);
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_eq!(cv.hdr.codec, Codec::None);
        assert_eq!(decode_ints(&body, n), vals);
    }

    // ZSTD text frames: low-NDV rows force the dict lane; the compressed
    // dict blob round-trips under the zstd tag.
    #[test]
    fn zstd_dict_text_roundtrip() {
        let rows: Vec<Vec<u8>> = (0..4096)
            .map(|i| format!("value-{:02}-{}", i % 7, "x".repeat(2000)).into_bytes())
            .collect();
        let refs: Vec<&[u8]> = rows.iter().map(|v| &v[..]).collect();
        let mut tb = TextBuilder { offs: Vec::new(), blob: Vec::new() };
        for r in &refs {
            tb.offs.push((tb.blob.len() as u32, r.len() as u32));
            tb.blob.extend_from_slice(r);
        }
        let cc = CodecCtx { choice: CodecChoice::Zstd, zstd_level: ZSTD_LEVEL_DEFAULT };
        let mut body = Vec::new();
        encode_text_chunk(&mut body, &tb, 1, &cc);
        let cv = ChunkView::at(&body, 0, rows.len() as u32);
        assert_eq!(cv.hdr.encoding, Encoding::Lz4Dict);
        assert_eq!(cv.hdr.codec, Codec::Zstd);
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        cv.decode_granule(0, &mut out, &mut dict, &mut arena);
        for (i, d) in out.iter().enumerate() {
            assert_eq!(crate::varlena_bytes(*d).unwrap(), &refs[i][..], "row {i}");
        }
    }

    // Old-bank read-compat at the chunk level: a legacy Lz4Text image with
    // codec byte 0 (the v<=5 writer's layout) must decode as LZ4.
    #[test]
    fn legacy_lz4text_codec0_decodes() {
        let n = 512usize;
        let rows: Vec<Vec<u8>> =
            (0..n).map(|i| format!("legacy-{}-{}", i % 5, "pad".repeat(16)).into_bytes()).collect();
        // Old-writer layout: header (codec byte 0) | granule dir | offsets |
        // one LZ4 frame per granule.
        let mut gblob = Vec::new();
        let mut offs = Vec::with_capacity(n);
        for r in &rows {
            offs.push(gblob.len() as u32);
            push_varlena_image(&mut gblob, r);
        }
        let comp = lz4_flex::compress(&gblob);
        let mut body = Vec::new();
        ChunkHeader {
            encoding: Encoding::Lz4Text,
            width: 4,
            flags: 0,
            ngranules: 1,
            aux: gblob.len() as i64,
            payload_len: (n * 4 + frame_len(comp.len())) as u64,
            codec: Codec::None, // legacy tag byte
        }
        .encode(&mut body);
        put_u64(&mut body, (n * 4) as u64);
        put_i64(&mut body, 0);
        put_i64(&mut body, 0);
        while body.len() % 64 != 0 {
            body.push(0);
        }
        for &o in &offs {
            put_u32(&mut body, o);
        }
        push_frame(&mut body, gblob.len(), &comp);
        let cv = ChunkView::at(&body, 0, n as u32);
        assert_eq!(cv.hdr.codec, Codec::None);
        assert_eq!(cv.hdr.frame_codec(), Codec::Lz4);
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        cv.decode_granule(0, &mut out, &mut dict, &mut arena);
        for (i, d) in out.iter().enumerate() {
            assert_eq!(crate::varlena_bytes(*d).unwrap(), &rows[i][..], "row {i}");
        }
    }
}

#[cfg(test)]
mod cluster_key_tests {
    use super::*;

    fn seams_once() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(::tuplesort::init_seams);
    }

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir()
            .join(format!("cbstore-ckey-{}-{}", std::process::id(), name));
        let _ = std::fs::remove_file(&p);
        std::fs::write(&p, []).unwrap();
        p.to_str().unwrap().to_string()
    }

    // int8 + text tupdesc matching ColType::[I64, Text].
    fn tup_desc() -> std::rc::Rc<::types_tuple::TupleDescData<'static>> {
        use ::types_tuple::*;
        let m: &'static ::mcx::MemoryContext =
            Box::leak(Box::new(::mcx::MemoryContext::new("cbstore-ckey-test")));
        let mcx = m.mcx();
        let mut attrs = ::mcx::PgVec::new_in(mcx);
        let mut compact = ::mcx::PgVec::new_in(mcx);
        for (i, (typid, len, byval, align)) in
            [(20u32, 8i16, true, TYPALIGN_DOUBLE), (25, -1, false, TYPALIGN_INT)]
                .iter()
                .enumerate()
        {
            let att = FormData_pg_attribute {
                attnum: (i + 1) as i16,
                atttypid: *typid,
                attlen: *len,
                attbyval: *byval,
                attalign: *align,
                attstorage: TYPSTORAGE_PLAIN,
                ..Default::default()
            };
            compact.push(CompactAttribute::populate_from(&att));
            attrs.push(att);
        }
        std::rc::Rc::new(TupleDescData {
            natts: 2,
            tdtypeid: 2249,
            tdtypmod: -1,
            tdrefcount: -1,
            constr: None,
            compact_attrs: compact,
            attrs,
        })
    }

    fn text_datum(s: &[u8], keep: &mut Vec<Vec<u8>>) -> Datum {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&(((s.len() + 4) as u32) << 2).to_le_bytes());
        v.extend_from_slice(s);
        keep.push(v);
        Datum::from_usize(keep.last().unwrap().as_ptr() as usize)
    }

    // The 3.1 ordering property: rows ingested in adversarial order come out
    // key-sorted (text C-order, then i64), RGs carry RG_FLAG_CLUSTERED, the
    // footer records the declared key, and the v5 sorted flags read exact.
    #[test]
    fn cluster_key_sorts_ingest_and_stamps_metadata() {
        seams_once();
        let path = tmp("sort");
        let coltypes = vec![ColType::I64, ColType::Text];
        let mut opts = CbWriterOpts::plain(2);
        // Key: (text col 1, int col 0) — cross-column and cross-class.
        opts.cluster_key = vec![(1, CbSortKeyKind::TextC), (0, CbSortKeyKind::Int64)];
        let mut w = open_writer_inner(
            SegFile::open_rw(&path).unwrap(), 1, 0, true, coltypes, 0x5aa5, opts,
        )
        .unwrap();
        let keys: Vec<(i16, CbSortKeyKind)> =
            w.opts.cluster_key.iter().map(|&(c, k)| (c as i16 + 1, k)).collect();
        w.sorter =
            Some(::tuplesort_seams::cbstore_ingest_sort::call(tup_desc(), &keys, 65536).unwrap());

        // > 1 RG of rows, adversarial order (descending + interleaved).
        let n = RG_ROWS + 1234;
        let mut rows: Vec<(i64, Vec<u8>)> = (0..n)
            .map(|i| {
                let r = crate::hll::mix64(i as u64);
                ((r % 1000) as i64, format!("k{:04}", r % 300).into_bytes())
            })
            .collect();
        let mut keep = Vec::new();
        for (v, t) in &rows {
            let vals = [Datum::from_i64(*v), text_datum(t, &mut keep)];
            w.ingest_row(&vals, &[false, false]).unwrap();
        }
        // Nothing sealed before the drain: rows live in the sorter.
        assert_eq!(w.rgs.len(), 0);
        assert_eq!(w.nbuf, 0);
        w.finish().unwrap();

        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.cluster_key, vec![1, 0]);
        assert_eq!(part.total_rows(), n as u64);
        assert!(part.rgs.len() >= 2);
        for rg in &part.rgs {
            assert_ne!(rg.flags & RG_FLAG_CLUSTERED, 0);
        }
        // Text key column is whole-part sorted; int is not (tiebreak only).
        assert_eq!(part.sorted, vec![0, 1]);

        // Read every row back and compare against the host-sorted oracle.
        rows.sort_by(|a, b| a.1.cmp(&b.1).then(a.0.cmp(&b.0)));
        let (mut out_i, mut out_t) = (Vec::new(), Vec::new());
        let (mut dict, mut arena) = (Vec::new(), Vec::new());
        let (mut dict2, mut arena2) = (Vec::new(), Vec::new());
        let mut got: Vec<(i64, Vec<u8>)> = Vec::new();
        for rg in 0..part.rgs.len() {
            let ints = part.chunk(rg, 0);
            let texts = part.chunk(rg, 1);
            // Dict tables are per-RG (build_dict contract: caller clears at
            // RG boundaries).
            dict.clear();
            dict2.clear();
            for g in 0..ints.hdr.ngranules as usize {
                ints.decode_granule(g, &mut out_i, &mut dict, &mut arena);
                texts.decode_granule(g, &mut out_t, &mut dict2, &mut arena2);
                assert_eq!(out_i.len(), out_t.len());
                for (i, t) in out_i.iter().zip(out_t.iter()) {
                    got.push((i.as_i64(), crate::varlena_bytes(*t).unwrap().to_vec()));
                }
            }
        }
        assert_eq!(got.len(), rows.len());
        // Multiset equality by full row; order equality by the key columns
        // (equal-key rows may permute in payload—here the whole row IS the
        // key, so exact equality holds).
        for (i, (g, r)) in got.iter().zip(rows.iter()).enumerate() {
            assert_eq!(g, r, "row {i} of {}", rows.len());
        }
        std::fs::remove_file(&path).unwrap();
    }

    // No cluster key: ingest_row appends directly (no sorter detour).
    #[test]
    fn no_cluster_key_appends_directly() {
        seams_once();
        let path = tmp("nokey");
        let mut w = open_writer_inner(
            SegFile::open_rw(&path).unwrap(), 1, 0, true,
            vec![ColType::I64, ColType::Text], 0x5aa5, CbWriterOpts::plain(2),
        )
        .unwrap();
        assert!(w.sorter.is_none());
        let mut keep = Vec::new();
        let vals = [Datum::from_i64(7), text_datum(b"x", &mut keep)];
        w.ingest_row(&vals, &[false, false]).unwrap();
        assert_eq!(w.nbuf, 1);
        w.finish().unwrap();
        let part = crate::reader::Part::open(&path, 2).unwrap().unwrap();
        assert_eq!(part.cluster_key, Vec::<u16>::new());
        assert_eq!(part.rgs[0].flags & RG_FLAG_CLUSTERED, 0);
        std::fs::remove_file(&path).unwrap();
    }
}

#[cfg(test)]
mod lz4_decode_seat_tests {
    // Differential coverage of the lz4dec decoder IN ITS REAL SEAT: chunks
    // that the writer admits to Lz4Text / Lz4Dict, decoded back through
    // ChunkView::decode_granule (reader.rs), values compared byte-for-byte.
    use super::*;
    use crate::reader::ChunkView;

    fn tb_of_owned(rows: &[Vec<u8>]) -> TextBuilder {
        let mut tb = TextBuilder { offs: Vec::new(), blob: Vec::new() };
        for r in rows {
            tb.offs.push((tb.blob.len() as u32, r.len() as u32));
            tb.blob.extend_from_slice(r);
        }
        tb
    }

    fn decode_all(body: &[u8], n: usize) -> Vec<Vec<u8>> {
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        let cv = ChunkView::at(body, 0, n as u32);
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        let mut got = Vec::with_capacity(n);
        for g in 0..hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            for d in &out {
                got.push(crate::varlena_bytes(*d).unwrap().to_vec());
            }
        }
        got
    }

    #[test]
    fn lz4text_granule_decode_roundtrip() {
        // All-distinct compressible rows across a partial final granule:
        // dict loses (ndv == n), LZ4 wins >= 10% -> Lz4Text, whose granule
        // frames decode through lz4dec.
        let n = GRANULE_ROWS + 37;
        let rows: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("http://example.com/some/long/path/{i}?pad=aaaaaaaaaaaaaaaaaaaaaaaaaaaa").into_bytes())
            .collect();
        let mut body = Vec::new();
        encode_text_chunk(&mut body, &tb_of_owned(&rows), n.div_ceil(GRANULE_ROWS) as u32, &CodecCtx { choice: CodecChoice::Lz4, zstd_level: ZSTD_LEVEL_DEFAULT });
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        assert_eq!(hdr.encoding, Encoding::Lz4Text, "test premise: Lz4Text admitted");
        assert_eq!(decode_all(&body, n), rows);
    }

    #[test]
    fn lz4dict_granule_decode_roundtrip() {
        // Repeated compressible entries: dict wins, dict blob compresses
        // >= 10% -> Lz4Dict; build_dict decompresses the blob through lz4dec.
        let n = GRANULE_ROWS + 11;
        let rows: Vec<Vec<u8>> = (0..n)
            .map(|i| format!("searchterm-{:04}-cccccccccccccccccccccccccccc", i % 300).into_bytes())
            .collect();
        let mut body = Vec::new();
        encode_text_chunk(&mut body, &tb_of_owned(&rows), n.div_ceil(GRANULE_ROWS) as u32, &CodecCtx { choice: CodecChoice::Lz4, zstd_level: ZSTD_LEVEL_DEFAULT });
        let hdr = ChunkHeader::decode(&body[..CB_CHUNK_HEADER_LEN]);
        assert_eq!(hdr.encoding, Encoding::Lz4Dict, "test premise: Lz4Dict admitted");
        assert_eq!(decode_all(&body, n), rows);
    }
}
