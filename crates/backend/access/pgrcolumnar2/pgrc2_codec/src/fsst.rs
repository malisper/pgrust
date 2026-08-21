//! FSST — Fast Static Symbol Table string compression (Boncz/Neumann,
//! VLDB 2020), ENC 12, first-class per SB-4 with the OD-5 symbol-table
//! ruling (lanev4 format ledger, RULED 2026-08-12).
//!
//! Scope (OD-5): the symbol table is **per-(column,part)** — the writer
//! builds ONE table per column per part and every extent section of the
//! stream embeds a copy in its section header bytes (payload offset 0,
//! before the first frame mark — the DICT_CODES block-header idiom), so
//! sections stay self-describing and CRC-over-wrapped covers the table.
//! FSST-under-zstd is encoding+wrapper — two layers, legal per SB-2's
//! O-CMP-6 reading; the wrapper offer rides the ordinary seal path.
//!
//! Election posture: a varlena/text stream encoding arm competing under
//! the exact-bytes ≥10% law (`election::elect_text_fsst`); its target
//! class is the dict-loser text families (NDV-cap breach, BelowWinGate,
//! url/log-shaped near-unique — the measured 7–11% breach class). The
//! election carries a CODE-GRAIN incompressible guard: table + code bytes
//! must beat the raw value bytes by ≥10% before the total-bytes gate even
//! runs — fsst frames store no per-value varlena headers, and that
//! representation asymmetry must never be the winning margin
//! (`elect_text_fsst` doc).
//!
//! On-disk layout (all integers LE, payload-relative):
//!
//! ```text
//! payload := table | granule frames...
//! table   := nsymbols u8 | pad u8 (0) | lens (nsymbols × u8, each 1..=8)
//!          | symbol bytes (Σ lens)
//! frame   := slots ((values+1) × u32, frame-relative; slots[0] =
//!            (values+1)*4) | compressed value streams back-to-back
//! ```
//!
//! **ONE frame per granule**, marked at the granule ordinal — the
//! ALP-family granule-framed idiom (`section::granule_frame_base`).
//! Addressing is a pure function of the granule ordinal, valid at EVERY
//! SB-10 ladder grain: root FSST sections never need a gcount table, and
//! the frozen 8192-keyed `frame_base` closed form is never consulted (a
//! per-1024-row framing addressed through that closed form misaddresses
//! every granule after 0 the moment a part elects a non-default grain —
//! the wave-5 `t_belowwin` seal defect). Value `i` of a granule occupies
//! `frame[slots[i]..slots[i+1]]`. Null slots (and empty strings) compress
//! to zero bytes — validity is the only truth on decode (spec §6.6).
//! Compressed streams are code bytes: `c < nsymbols` emits symbol `c`
//! (1..=8 bytes); `c == 255` escapes the next literal byte; any other
//! byte is a typed corruption (`tests/corrupt.rs` born-RED). Symbol codes
//! therefore top out at 254.
//!
//! Decode is exact byte round-trip into the arena (varlena-shaped,
//! ≥8-aligned — StrView §7b): pass 1 walks the codes computing the exact
//! decompressed length (every code validated; typed error, never UB even
//! after CRC passes), pass 2 fills the sized slot. Allocation-free: the
//! parsed table view lives on the stack.
//!
//! The table build is the Boncz/Neumann iterative greedy: a bounded number
//! of passes over a sample, each pass greedily parsing with the current
//! table while counting symbol and adjacent-pair frequencies, then keeping
//! the top-gain candidates (gain = count × length; deterministic ordering
//! throughout — same sample ⇒ same table ⇒ same bytes, the
//! byte-identical-parts law).

use crate::section::{frame_start, granule_frame_base, open_section, payload_region, varlena_payload};
use pgrc2_format::abi::{
    refuse_decode_codes, refuse_dict_handle, validity_from_ctx, ByteArena, CodecVtable, DecodeOut,
    EncodeInput, GranuleEncoder, KernelCtx, KernelKey, Selection, ValidityVerdict,
};
use pgrc2_format::class::CLASS_VARLENA;
use pgrc2_format::enc::EncodingId;
use pgrc2_format::geom::{GRANULE_ROWS, OVERSIZE_THRESHOLD};
use pgrc2_format::meta::{MetaAnswer, MetaProbe};
use pgrc2_format::part::{OverflowSink, StreamSectionWriter};
use pgrc2_format::wire::varlena_header_4b_u;
use pgrc2_format::{FormatError, FormatResult};
use std::collections::BTreeMap;

/// The escape code: emits the next literal byte on decode.
pub const FSST_ESCAPE: u8 = 255;
/// At most 255 symbols (codes 0..=254; 255 is the escape).
pub const FSST_MAX_SYMBOLS: usize = 255;
/// Symbols are 1..=8 bytes (the Boncz/Neumann register width).
pub const FSST_MAX_SYMBOL_LEN: usize = 8;
/// nsymbols(1) + pad(1).
const TABLE_HDR_LEN: usize = 2;
/// Greedy build passes over the sample (the reference's bounded loop).
const FSST_BUILD_ITERATIONS: usize = 5;

// ---------------------------------------------------------------------------
// symbol table (writer-side object; the wire form is `serialize_into`)
// ---------------------------------------------------------------------------

/// Fixed-width symbol key for the deterministic build maps: zero-padded
/// bytes + length (Ord derives elementwise — pure tie-breaking).
type SymKey = ([u8; FSST_MAX_SYMBOL_LEN], u8);

fn sym_key(bytes: &[u8]) -> SymKey {
    debug_assert!((1..=FSST_MAX_SYMBOL_LEN).contains(&bytes.len()));
    let mut b = [0u8; FSST_MAX_SYMBOL_LEN];
    b[..bytes.len()].copy_from_slice(bytes);
    (b, bytes.len() as u8)
}

/// A built FSST symbol table: up to 255 symbols of 1..=8 bytes plus the
/// encode-side longest-match index. Pure data — cloning is cheap enough
/// for the per-extent encoder factories.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FsstSymbolTable {
    /// Symbol images concatenated; `starts[c]..starts[c+1]` is code `c`.
    bytes: Vec<u8>,
    /// nsymbols + 1 prefix offsets into `bytes`.
    starts: Vec<u16>,
    /// Per-first-byte candidate codes, longest symbol first (ties: lower
    /// code) — the greedy longest-match lookup structure.
    buckets: Vec<Vec<u8>>,
}

impl FsstSymbolTable {
    /// Assemble from symbol images (deterministic input order = code
    /// order). Caller guarantees ≤ [`FSST_MAX_SYMBOLS`] distinct symbols
    /// of 1..=8 bytes.
    fn from_symbols(symbols: Vec<Vec<u8>>) -> FsstSymbolTable {
        debug_assert!(symbols.len() <= FSST_MAX_SYMBOLS);
        let mut bytes = Vec::new();
        let mut starts = Vec::with_capacity(symbols.len() + 1);
        starts.push(0u16);
        for s in &symbols {
            debug_assert!((1..=FSST_MAX_SYMBOL_LEN).contains(&s.len()));
            bytes.extend_from_slice(s);
            starts.push(bytes.len() as u16);
        }
        let mut buckets: Vec<Vec<u8>> = vec![Vec::new(); 256];
        for (code, s) in symbols.iter().enumerate() {
            buckets[s[0] as usize].push(code as u8);
        }
        let sym_len = |c: u8| (starts[c as usize + 1] - starts[c as usize]) as usize;
        for b in buckets.iter_mut() {
            b.sort_by(|&x, &y| sym_len(y).cmp(&sym_len(x)).then(x.cmp(&y)));
        }
        FsstSymbolTable {
            bytes,
            starts,
            buckets,
        }
    }

    pub fn nsymbols(&self) -> usize {
        self.starts.len() - 1
    }

    fn symbol(&self, code: usize) -> &[u8] {
        &self.bytes[self.starts[code] as usize..self.starts[code + 1] as usize]
    }

    /// Greedy longest match at the head of `s` (nonempty), or None
    /// (escape). Ties keep the lower code — deterministic.
    fn longest_match(&self, s: &[u8]) -> Option<(u8, usize)> {
        for &code in &self.buckets[s[0] as usize] {
            let sym = self.symbol(code as usize);
            if sym.len() <= s.len() && &s[..sym.len()] == sym {
                return Some((code, sym.len()));
            }
        }
        None
    }

    /// Build a table from a sample of value payloads (the Boncz/Neumann
    /// iterative greedy — module doc). Pure function of the sample.
    pub fn build(sample: &[&[u8]]) -> FsstSymbolTable {
        let mut table = FsstSymbolTable::from_symbols(Vec::new());
        for _ in 0..FSST_BUILD_ITERATIONS {
            // One greedy parse of the sample with the current table,
            // counting emitted tokens and adjacent-pair concatenations.
            let mut counts: BTreeMap<SymKey, u64> = BTreeMap::new();
            for &s in sample {
                let mut pos = 0usize;
                let mut prev: Option<(usize, usize)> = None; // (start, len)
                while pos < s.len() {
                    let len = match table.longest_match(&s[pos..]) {
                        Some((_, l)) => l,
                        None => 1,
                    };
                    *counts.entry(sym_key(&s[pos..pos + len])).or_insert(0) += 1;
                    if let Some((ps, pl)) = prev {
                        let joined = (pl + len).min(FSST_MAX_SYMBOL_LEN);
                        *counts.entry(sym_key(&s[ps..ps + joined])).or_insert(0) += 1;
                    }
                    prev = Some((pos, len));
                    pos += len;
                }
            }
            // Keep the top-gain candidates: gain = count × covered bytes.
            // Deterministic ranking (gain desc, longer first, bytes asc).
            let mut ranked: Vec<(SymKey, u64)> = counts
                .into_iter()
                .map(|(sym, count)| (sym, count * sym.1 as u64))
                .collect();
            ranked.sort_by(|a, b| {
                b.1.cmp(&a.1)
                    .then(b.0 .1.cmp(&a.0 .1))
                    .then(a.0 .0.cmp(&b.0 .0))
            });
            ranked.truncate(FSST_MAX_SYMBOLS);
            table = FsstSymbolTable::from_symbols(
                ranked
                    .into_iter()
                    .map(|((bytes, len), _)| bytes[..len as usize].to_vec())
                    .collect(),
            );
        }
        table
    }

    /// Serialized wire size (the per-extent section header copy).
    pub fn serialized_len(&self) -> usize {
        TABLE_HDR_LEN + self.nsymbols() + self.bytes.len()
    }

    /// Emit the wire form (module-doc layout).
    pub fn serialize_into(&self, out: &mut Vec<u8>) {
        out.push(self.nsymbols() as u8);
        out.push(0); // pad
        for c in 0..self.nsymbols() {
            out.push((self.starts[c + 1] - self.starts[c]) as u8);
        }
        out.extend_from_slice(&self.bytes);
    }

    /// Greedy-compress one value payload, appending code bytes to `out`.
    pub fn compress_into(&self, input: &[u8], out: &mut Vec<u8>) {
        let mut pos = 0usize;
        while pos < input.len() {
            match self.longest_match(&input[pos..]) {
                Some((code, len)) => {
                    out.push(code);
                    pos += len;
                }
                None => {
                    out.push(FSST_ESCAPE);
                    out.push(input[pos]);
                    pos += 1;
                }
            }
        }
    }

    /// Exact compressed size of one value payload (election pricing —
    /// the same walk as [`FsstSymbolTable::compress_into`], no emission).
    pub fn compressed_len(&self, input: &[u8]) -> usize {
        let mut pos = 0usize;
        let mut n = 0usize;
        while pos < input.len() {
            match self.longest_match(&input[pos..]) {
                Some((_, len)) => {
                    n += 1;
                    pos += len;
                }
                None => {
                    n += 2;
                    pos += 1;
                }
            }
        }
        n
    }

    /// Decompress a code stream (writer-side verify / test oracle; the
    /// decode kernels run their own allocation-free twin over the wire
    /// table). Typed refusal on any invalid code or dangling escape.
    pub fn decompress_into(&self, comp: &[u8], out: &mut Vec<u8>) -> FormatResult<()> {
        let mut i = 0usize;
        while i < comp.len() {
            let c = comp[i];
            if c == FSST_ESCAPE {
                let lit = *comp.get(i + 1).ok_or(FormatError::Corrupt { at: "fsst escape" })?;
                out.push(lit);
                i += 2;
            } else if (c as usize) < self.nsymbols() {
                out.extend_from_slice(self.symbol(c as usize));
                i += 1;
            } else {
                return Err(FormatError::Corrupt { at: "fsst code" });
            }
        }
        Ok(())
    }
}

/// Exact frame bytes for one GRANULE frame of `vif` values totalling
/// `compressed` code bytes: the slot table + the data (election
/// arithmetic; module-doc layout — one frame per granule).
pub fn frame_payload_bytes(vif: usize, compressed: usize) -> usize {
    (vif + 1) * 4 + compressed
}

// ---------------------------------------------------------------------------
// encode (spec §19.6 faces)
// ---------------------------------------------------------------------------

/// The election's carried compression (SEAL-FUSION: walk #7f vs #9 — the
/// election trial-compressed EVERY value to price exactly; instead of
/// discarding those bytes and recompressing at encode, it stores them
/// once, row-dense over the whole part, and encode is a slot-table +
/// memcpy emit).
///
/// `ends` has rows+1 entries (row r's code bytes = `bytes[ends[r]..
/// ends[r+1]]`; null rows and empty strings are zero-length — exactly the
/// encoder's own per-row ends law). Row currency makes the carry valid at
/// EVERY SB-10 grain (granule rows are global rows).
///
/// Memory price (documented — the FSST keep-vs-recompress call): `bytes`
/// ≈ the encoded stream's compressed payload (the elected
/// candidate's code bytes) + 4·(rows+1) for `ends`, held from election to
/// the end of the stream's seal. The seal already holds the whole part
/// image plus every band section in memory, so this is a bounded second
/// copy of one column's compressed bytes, freed at stream end — KEEP was
/// chosen over recompress because the greedy longest-match walk is the
/// single hottest per-byte cost in text seals and this halves it.
#[derive(Debug, Clone)]
pub struct FsstCarry {
    pub bytes: std::sync::Arc<Vec<u8>>,
    pub ends: std::sync::Arc<Vec<u32>>,
}

/// Cursor over [`FsstCarry`] for one encoder (per extent): `row` is the
/// next global row to emit.
#[derive(Debug, Clone)]
pub struct FsstCarryCursor {
    pub carry: FsstCarry,
    pub row: usize,
}

/// The FSST granule encoder. The table is the per-(column,part) build the
/// election produced; every extent's section embeds the same image (OD-5).
pub struct FsstEncoder {
    table: FsstSymbolTable,
    /// Serialized table (emitted once per section, at payload offset 0).
    table_image: Vec<u8>,
    /// Per-granule compressed-stream staging.
    scratch: Vec<u8>,
    /// Per-granule value-end offsets into `scratch` (rows + 1 entries).
    ends: Vec<u32>,
    /// SEAL-FUSION: the election's carried compression (None = recompress,
    /// the pre-fusion path).
    carry: Option<FsstCarryCursor>,
}

impl FsstEncoder {
    pub fn new(table: FsstSymbolTable) -> FsstEncoder {
        let mut table_image = Vec::with_capacity(table.serialized_len());
        table.serialize_into(&mut table_image);
        FsstEncoder {
            table,
            table_image,
            scratch: Vec::new(),
            ends: Vec::new(),
            carry: None,
        }
    }

    /// [`FsstEncoder::new`] with the election's carried compression,
    /// starting at global row `row`.
    pub fn new_carried(table: FsstSymbolTable, carry: FsstCarry, row: usize) -> FsstEncoder {
        let mut e = FsstEncoder::new(table);
        e.carry = Some(FsstCarryCursor { carry, row });
        e
    }
}

impl GranuleEncoder for FsstEncoder {
    fn key(&self) -> KernelKey {
        KernelKey {
            encoding: EncodingId::Fsst.as_u16(),
            class: CLASS_VARLENA,
            width: 0,
        }
    }

    fn encode_granule(
        &mut self,
        input: &EncodeInput<'_>,
        w: &mut StreamSectionWriter<'_>,
        _ovf: &mut OverflowSink<'_>,
    ) -> FormatResult<()> {
        let rows = input.rows as usize;
        if rows > GRANULE_ROWS as usize {
            return Err(FormatError::EncodeContract {
                detail: "granule overflow",
            });
        }
        if input.datums.len() < rows {
            return Err(FormatError::EncodeContract {
                detail: "datums shorter than rows",
            });
        }
        // OD-5: the symbol table opens every extent's payload, before the
        // first frame mark (the DICT_CODES block-header idiom).
        if w.payload_off() == 0 {
            w.payload().extend_from_slice(&self.table_image);
        }
        // SEAL-FUSION carried arm: the election already compressed every
        // value (row currency — grain-proof); emit = slot table + memcpy.
        // Byte-identical to recompression: same table, same values, same
        // greedy walk produced the carried bytes.
        if let Some(cur) = &mut self.carry {
            let r0 = cur.row;
            let r1 = r0 + rows;
            let ends = &cur.carry.ends;
            if r1 + 1 > ends.len() {
                return Err(FormatError::EncodeContract {
                    detail: "fsst carry exhausted",
                });
            }
            let b0 = ends[r0] as usize;
            let b1 = ends[r1] as usize;
            w.begin_frame();
            let slot_base = ((rows + 1) * 4) as u32;
            let buf = w.payload();
            for &e in &ends[r0..=r1] {
                buf.extend_from_slice(&(slot_base + (e - ends[r0])).to_le_bytes());
            }
            buf.extend_from_slice(&cur.carry.bytes[b0..b1]);
            w.end_granule(input.rows);
            cur.row = r1;
            return Ok(());
        }
        // ONE frame per granule (module doc: granule-framed family —
        // addressing by granule ordinal is grain-proof at every SB-10
        // ladder grain). Two passes: compress into scratch recording
        // per-value ends, then emit the slot table + the data.
        self.scratch.clear();
        self.ends.clear();
        self.ends.push(0);
        for r in 0..rows {
            if input.valid(r as u32) {
                // SAFETY: EncodeInput pointer-class contract (§19.6).
                let payload = unsafe { varlena_payload(input.datums[r])? };
                if payload.len() >= OVERSIZE_THRESHOLD as usize {
                    // Oversize values live in the overflow stream — the
                    // election demotes FSST for such columns.
                    return Err(FormatError::EncodeContract {
                        detail: "oversize value in fsst stream",
                    });
                }
                self.table.compress_into(payload, &mut self.scratch);
            }
            // Null slots encode zero code bytes (placeholder; validity is
            // the only truth — spec §6.6).
            self.ends.push(self.scratch.len() as u32);
        }
        w.begin_frame();
        let slot_base = ((rows + 1) * 4) as u32;
        let buf = w.payload();
        for &e in &self.ends {
            buf.extend_from_slice(&(slot_base + e).to_le_bytes());
        }
        buf.extend_from_slice(&self.scratch);
        w.end_granule(input.rows);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// decode (allocation-free; typed errors, never UB)
// ---------------------------------------------------------------------------

/// Parsed wire symbol table (stack offsets over the section payload).
struct TableView<'a> {
    nsymbols: usize,
    /// Prefix offsets; entry c..c+1 delimits code c inside `bytes`.
    starts: [u16; FSST_MAX_SYMBOLS + 1],
    bytes: &'a [u8],
}

fn parse_table(payload: &[u8]) -> FormatResult<TableView<'_>> {
    let hdr = payload
        .get(..TABLE_HDR_LEN)
        .ok_or(FormatError::Truncated {
            at: "fsst symbol table",
        })?;
    let n = hdr[0] as usize;
    let lens = payload
        .get(TABLE_HDR_LEN..TABLE_HDR_LEN + n)
        .ok_or(FormatError::Truncated {
            at: "fsst symbol table",
        })?;
    let mut starts = [0u16; FSST_MAX_SYMBOLS + 1];
    let mut total = 0u16;
    for (i, &l) in lens.iter().enumerate() {
        if l == 0 || l as usize > FSST_MAX_SYMBOL_LEN {
            return Err(FormatError::Corrupt {
                at: "fsst symbol len",
            });
        }
        total += l as u16;
        starts[i + 1] = total;
    }
    let base = TABLE_HDR_LEN + n;
    let bytes = payload
        .get(base..base + total as usize)
        .ok_or(FormatError::Truncated {
            at: "fsst symbol table",
        })?;
    Ok(TableView {
        nsymbols: n,
        starts,
        bytes,
    })
}

struct FsstView<'a> {
    payload: &'a [u8],
    table: TableView<'a>,
    frame_table: Option<&'a [u32]>,
    /// This granule's frame index — the granule ordinal in the extent
    /// (granule-framed family: grain-proof at every SB-10 ladder grain).
    frame: u32,
    values: u32,
}

fn open_view<'a>(ctx: &KernelCtx<'a>) -> FormatResult<FsstView<'a>> {
    let hdr = open_section(ctx.bytes, EncodingId::Fsst.as_u16())?;
    let payload = payload_region(&hdr, ctx.bytes)?;
    let table = parse_table(payload)?;
    Ok(FsstView {
        payload,
        table,
        frame_table: ctx.frame_table,
        frame: granule_frame_base(&hdr, ctx.bytes, ctx.granule_in_extent)?,
        values: ctx.values,
    })
}

/// This granule's ONE frame: start from the frame table at the granule
/// ordinal, end at the next granule's frame start (or the payload end).
fn granule_frame<'a>(v: &FsstView<'a>) -> FormatResult<&'a [u8]> {
    let fs = frame_start(v.frame_table, v.frame)?;
    let fe = match v.frame_table.and_then(|ft| ft.get(v.frame as usize + 1)) {
        Some(&next) => next as usize,
        None => v.payload.len(),
    };
    v.payload
        .get(fs..fe)
        .ok_or(FormatError::Bounds { at: "fsst frame" })
}

#[inline]
fn slot(frame: &[u8], i: usize) -> FormatResult<u32> {
    let b = frame.get(i * 4..i * 4 + 4).ok_or(FormatError::Bounds {
        at: "fsst slot table",
    })?;
    Ok(u32::from_le_bytes(b.try_into().expect("len 4")))
}

/// Value `i`'s compressed stream, slot-table-validated.
fn value_comp<'a>(frame: &'a [u8], vif: usize, i: usize) -> FormatResult<&'a [u8]> {
    let s = slot(frame, i)? as usize;
    let e = slot(frame, i + 1)? as usize;
    let base = (vif + 1) * 4;
    if s < base || e < s || e > frame.len() {
        return Err(FormatError::Corrupt {
            at: "fsst slot table",
        });
    }
    Ok(&frame[s..e])
}

/// Pass 1: exact decompressed length, every code validated (typed error
/// on out-of-table codes and dangling escapes — the born-RED surface).
fn decomp_len(t: &TableView<'_>, comp: &[u8]) -> FormatResult<usize> {
    let mut i = 0usize;
    let mut n = 0usize;
    while i < comp.len() {
        let c = comp[i] as usize;
        if c == FSST_ESCAPE as usize {
            if i + 1 >= comp.len() {
                return Err(FormatError::Corrupt { at: "fsst escape" });
            }
            n += 1;
            i += 2;
        } else if c < t.nsymbols {
            n += (t.starts[c + 1] - t.starts[c]) as usize;
            i += 1;
        } else {
            return Err(FormatError::Corrupt { at: "fsst code" });
        }
    }
    Ok(n)
}

/// Decode one value into the arena as a varlena-shaped entry (StrView §7b:
/// 4B-U header, ≥8-aligned) and return its datum word. Pass 2 mirrors the
/// validated pass-1 walk, so the fill is straight-line.
fn materialize(t: &TableView<'_>, comp: &[u8], arena: &mut ByteArena<'_>) -> FormatResult<u64> {
    let n = decomp_len(t, comp)?;
    let dst = arena.alloc(4 + n)?;
    dst[..4].copy_from_slice(&varlena_header_4b_u(n as u32).to_le_bytes());
    let mut o = 4usize;
    let mut i = 0usize;
    while i < comp.len() {
        let c = comp[i] as usize;
        if c == FSST_ESCAPE as usize {
            dst[o] = comp[i + 1];
            o += 1;
            i += 2;
        } else {
            let sym = &t.bytes[t.starts[c] as usize..t.starts[c + 1] as usize];
            dst[o..o + sym.len()].copy_from_slice(sym);
            o += sym.len();
            i += 1;
        }
    }
    debug_assert_eq!(o, 4 + n);
    Ok(dst.as_ptr() as u64)
}

fn fsst_dec_full(ctx: &KernelCtx<'_>, out: &mut DecodeOut<'_>) -> FormatResult<u32> {
    let v = open_view(ctx)?;
    let n = v.values as usize;
    if out.datums.len() < n {
        return Err(FormatError::Bounds {
            at: "decode_full out",
        });
    }
    let frame = granule_frame(&v)?;
    for i in 0..n {
        let comp = value_comp(frame, n, i)?;
        out.datums[i] = materialize(&v.table, comp, &mut out.arena)?;
    }
    Ok(v.values)
}

fn fsst_dec_sel(
    ctx: &KernelCtx<'_>,
    sel: &Selection<'_>,
    out: &mut DecodeOut<'_>,
) -> FormatResult<u32> {
    let v = open_view(ctx)?;
    if out.datums.len() < sel.rows.len() {
        return Err(FormatError::Bounds {
            at: "decode_sel out",
        });
    }
    // Survivor-only over the granule's ONE frame: the slot table indexes
    // directly by row ordinal.
    let n = v.values as usize;
    let frame = granule_frame(&v)?;
    for (o, &r16) in out.datums.iter_mut().zip(sel.rows.iter()) {
        let r = r16 as u32;
        if r >= v.values {
            return Err(FormatError::Bounds {
                at: "decode_sel row",
            });
        }
        let comp = value_comp(frame, n, r as usize)?;
        *o = materialize(&v.table, comp, &mut out.arena)?;
    }
    Ok(sel.rows.len() as u32)
}

fn fsst_meta(ctx: &KernelCtx<'_>, probe: &MetaProbe) -> FormatResult<MetaAnswer> {
    Ok(match probe {
        MetaProbe::RowCount => MetaAnswer::Count(ctx.rows as u64),
        MetaProbe::NonNullCount => {
            let mut scratch = [0u64; (GRANULE_ROWS as usize).div_ceil(64)];
            match validity_from_ctx(ctx, &mut scratch)? {
                ValidityVerdict::AllValid => MetaAnswer::Count(ctx.rows as u64),
                ValidityVerdict::Mixed { nonnull } => MetaAnswer::Count(nonnull as u64),
            }
        }
        _ => MetaAnswer::Absent,
    })
}

fn fsst_validity(ctx: &KernelCtx<'_>, out: &mut [u64]) -> FormatResult<ValidityVerdict> {
    validity_from_ctx(ctx, out)
}

pub static VT_FSST_VARLENA: CodecVtable = CodecVtable {
    key: KernelKey {
        encoding: EncodingId::Fsst as u16,
        class: CLASS_VARLENA,
        width: 0,
    },
    decode_full: fsst_dec_full,
    decode_sel: fsst_dec_sel,
    decode_codes: refuse_decode_codes,
    dict_handle: refuse_dict_handle,
    meta_probe: fsst_meta,
    validity: fsst_validity,
};

// ---------------------------------------------------------------------------
// match-on-compressed (M5d.fsst-moc / AP-2 — the LIKE band's
// compressed-domain successor). The predicate runs over the fsst CODE
// stream: symbol expansions are fed through a needle automaton straight
// from the (L1-resident) symbol table, so a granule is adjudicated without
// materializing one value — no arena writes, no varlena headers, early
// exit per value on first hit. EXACT by construction (the automaton over
// the expansion byte stream computes the same function as
// decompress-then-match), which subsumes the never-under-match law; the
// hydrate form [`moc_probe_reference`] is the standing parity oracle
// (AD-1: the interpreted twin owns the reference semantics).
// ---------------------------------------------------------------------------

/// The MoC operator class — the codec-side mirror of the executor's
/// `StrMatchOp` vocabulary (layering: lx4 depends on pgrc2, never the
/// reverse; the two enums are pinned equal by the M5d admission face).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FsstMatchOp {
    /// `%needle%` (byte substring).
    Contains,
    /// `needle%` (byte prefix).
    Prefix,
    /// `<> ''` (nonempty).
    NeEmpty,
}

/// Needle cap: patterns beyond this decline typed at compile (the caller's
/// hydrate arm owns them — the classifier's fail-closed pattern).
pub const FSST_MOC_MAX_NEEDLE: usize = 64;

/// A needle program compiled against ONE section's symbol table (the OD-5
/// grain: per-(column, part) tables in production; per section here — the
/// caller re-compiles when the table identity changes; `table_crc`
/// pins misuse in debug).
pub struct FsstMocProgram {
    op: FsstMatchOp,
    needle: Vec<u8>,
    /// Symbol expansions (owned copy of the section's table; ≤ 255×8 B).
    nsymbols: usize,
    starts: [u16; FSST_MAX_SYMBOLS + 1],
    sym_bytes: Vec<u8>,
    /// Table identity pin (FNV over the wire table image).
    table_fp: u64,
    /// Contains/Prefix: the byte-grain DFA, `nstates × 256`, row-major
    /// (state-major). Contains: states 0..=m, m absorbing (hit). Prefix:
    /// states 0..=m absorbing-hit at m, plus DEAD = m+1 absorbing.
    byte_next: Vec<u8>,
    /// Per-(code, state) composition of the DFA over code expansions:
    /// `code_next[code * nstates + state]` — ONE table lookup per code
    /// byte instead of one DFA step per expansion byte.
    code_next: Vec<u8>,
    nstates: usize,
    hit_state: u8,
}

fn table_fingerprint(t: &TableView<'_>) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    let mut step = |b: u8| {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    };
    step(t.nsymbols as u8);
    for &b in t.bytes {
        step(b);
    }
    h
}

/// Compile `needle`/`op` against the symbol table of `ctx`'s section.
/// Typed refusal on needles beyond [`FSST_MOC_MAX_NEEDLE`].
pub fn moc_compile(
    ctx: &KernelCtx<'_>,
    op: FsstMatchOp,
    needle: &[u8],
) -> FormatResult<FsstMocProgram> {
    if needle.len() > FSST_MOC_MAX_NEEDLE {
        return Err(FormatError::EncodeContract {
            detail: "fsst moc needle beyond cap",
        });
    }
    let hdr = open_section(ctx.bytes, EncodingId::Fsst.as_u16())?;
    let payload = payload_region(&hdr, ctx.bytes)?;
    let t = parse_table(payload)?;
    let table_fp = table_fingerprint(&t);
    let m = needle.len();
    // DFA construction (Contains: KMP-derived full DFA with absorbing hit
    // state; Prefix: anchored walk with DEAD). NeEmpty compiles no tables.
    let (nstates, hit_state, byte_next) = match op {
        FsstMatchOp::NeEmpty => (0usize, 0u8, Vec::new()),
        FsstMatchOp::Contains => {
            let n = m + 1;
            let mut fail = vec![0usize; m.max(1)];
            for i in 1..m {
                let mut k = fail[i - 1];
                while k > 0 && needle[i] != needle[k] {
                    k = fail[k - 1];
                }
                if needle[i] == needle[k] {
                    k += 1;
                }
                fail[i] = k;
            }
            let mut d = vec![0u8; n * 256];
            for s in 0..n {
                for b in 0..256usize {
                    let next = if s == m {
                        m // absorbing hit
                    } else {
                        let mut k = s;
                        loop {
                            if needle[k] as usize == b {
                                break k + 1;
                            }
                            if k == 0 {
                                break 0;
                            }
                            k = fail[k - 1];
                        }
                    };
                    d[s * 256 + b] = next as u8;
                }
            }
            (n, m as u8, d)
        }
        FsstMatchOp::Prefix => {
            let n = m + 2; // 0..=m match states + DEAD
            let dead = (m + 1) as u8;
            let mut d = vec![dead; n * 256];
            for s in 0..m {
                d[s * 256 + needle[s] as usize] = (s + 1) as u8;
            }
            for b in 0..256usize {
                d[m * 256 + b] = m as u8; // hit absorbs
                d[(m + 1) * 256 + b] = dead; // dead absorbs
            }
            (n, m as u8, d)
        }
    };
    // Per-code composition: δ*(state, expansion(code)).
    let code_next = if nstates == 0 {
        Vec::new()
    } else {
        let mut cn = vec![0u8; t.nsymbols * nstates];
        for c in 0..t.nsymbols {
            let sym = &t.bytes[t.starts[c] as usize..t.starts[c + 1] as usize];
            for s0 in 0..nstates {
                let mut s = s0 as u8;
                for &b in sym {
                    s = byte_next[s as usize * 256 + b as usize];
                }
                cn[c * nstates + s0] = s;
            }
        }
        cn
    };
    Ok(FsstMocProgram {
        op,
        needle: needle.to_vec(),
        nsymbols: t.nsymbols,
        starts: t.starts,
        sym_bytes: t.bytes.to_vec(),
        table_fp,
        byte_next,
        code_next,
        nstates,
        hit_state,
    })
}

impl FsstMocProgram {
    /// One value's verdict off its compressed stream (exact; typed refusal
    /// on out-of-table codes / dangling escapes — the decode surface's
    /// laws verbatim). Early exits: the absorbing HIT state, and Prefix's
    /// absorbing DEAD state (a failed anchored match is decided at the
    /// first divergence — the measured 16.7× Title/prefix-absent lesson;
    /// EXIT SEMANTICS NOTE: early-decided values skip validating their
    /// comp tail, exactly like the hydrate arm's early-exit matchers — a
    /// corrupt tail past the decision point surfaces at decode, not at
    /// probe, and the decode surface owns that refusal).
    #[inline]
    fn matches_comp(&self, comp: &[u8]) -> FormatResult<bool> {
        match self.op {
            FsstMatchOp::NeEmpty => Ok(!comp.is_empty()),
            FsstMatchOp::Contains | FsstMatchOp::Prefix => {
                if self.needle.is_empty() {
                    // LIKE '%%' / LIKE '%': every value matches.
                    return Ok(true);
                }
                let dead: u8 = if self.op == FsstMatchOp::Prefix {
                    (self.nstates - 1) as u8
                } else {
                    u8::MAX // unreachable state id — Contains never dies
                };
                let mut s: u8 = 0;
                let mut i = 0usize;
                while i < comp.len() {
                    let c = comp[i] as usize;
                    if c == FSST_ESCAPE as usize {
                        let lit = *comp.get(i + 1).ok_or(FormatError::Corrupt {
                            at: "fsst escape",
                        })?;
                        s = self.byte_next[s as usize * 256 + lit as usize];
                        i += 2;
                    } else if c < self.nsymbols {
                        s = self.code_next[c * self.nstates + s as usize];
                        i += 1;
                    } else {
                        return Err(FormatError::Corrupt { at: "fsst code" });
                    }
                    if s == self.hit_state {
                        return Ok(true);
                    }
                    if s == dead {
                        return Ok(false);
                    }
                }
                Ok(false)
            }
        }
    }
}

/// Probe one granule through the compiled program: survivor row ordinals
/// into `out_rows` (validity-gated — null rows never match), survivor
/// count returned. The caller compiled the program against THIS section's
/// table (debug-pinned by fingerprint).
pub fn moc_probe(
    prog: &FsstMocProgram,
    ctx: &KernelCtx<'_>,
    out_rows: &mut [u16],
) -> FormatResult<u32> {
    let v = open_view(ctx)?;
    debug_assert_eq!(
        table_fingerprint(&v.table),
        prog.table_fp,
        "moc program compiled against a different section table"
    );
    let n = v.values as usize;
    if out_rows.len() < n {
        return Err(FormatError::Bounds { at: "moc out" });
    }
    let frame = granule_frame(&v)?;
    let mut scratch = [0u64; (GRANULE_ROWS as usize).div_ceil(64)];
    let verdict = validity_from_ctx(ctx, &mut scratch)?;
    let mut hits = 0u32;
    for r in 0..n {
        let valid = match verdict {
            ValidityVerdict::AllValid => true,
            ValidityVerdict::Mixed { .. } => (scratch[r / 64] >> (r % 64)) & 1 == 1,
        };
        if !valid {
            continue;
        }
        let comp = value_comp(frame, n, r)?;
        if prog.matches_comp(comp)? {
            out_rows[hits as usize] = r as u16;
            hits += 1;
        }
    }
    Ok(hits)
}

/// The parity oracle: hydrate-then-match through the SAME decode surface
/// the production hydrate arm uses (materialize + byte search). Identical
/// output contract to [`moc_probe`].
pub fn moc_probe_reference(
    op: FsstMatchOp,
    needle: &[u8],
    ctx: &KernelCtx<'_>,
    arena_buf: &mut [u8],
    out_rows: &mut [u16],
) -> FormatResult<u32> {
    let v = open_view(ctx)?;
    let n = v.values as usize;
    if out_rows.len() < n {
        return Err(FormatError::Bounds { at: "moc out" });
    }
    let frame = granule_frame(&v)?;
    let mut scratch = [0u64; (GRANULE_ROWS as usize).div_ceil(64)];
    let verdict = validity_from_ctx(ctx, &mut scratch)?;
    let mut hits = 0u32;
    for r in 0..n {
        let valid = match verdict {
            ValidityVerdict::AllValid => true,
            ValidityVerdict::Mixed { .. } => (scratch[r / 64] >> (r % 64)) & 1 == 1,
        };
        if !valid {
            continue;
        }
        let comp = value_comp(frame, n, r)?;
        let mut arena = ByteArena::new(arena_buf);
        let datum = materialize(&v.table, comp, &mut arena)?;
        // SAFETY: `materialize` just wrote a valid 4B-U varlena at `datum`.
        let bytes = unsafe { varlena_payload(datum)? };
        let hit = match op {
            FsstMatchOp::NeEmpty => !bytes.is_empty(),
            FsstMatchOp::Prefix => bytes.starts_with(needle),
            FsstMatchOp::Contains => contains_scalar(bytes, needle),
        };
        if hit {
            out_rows[hits as usize] = r as u16;
            hits += 1;
        }
    }
    Ok(hits)
}

/// Scalar first-byte-scan containment (the compiled STR-FILTER twin's
/// structure in scalar form — the instrument's hydrate/control matcher, so
/// hydrate-vs-moc deltas price the FORM, not two different search idioms).
pub fn contains_scalar(hay: &[u8], needle: &[u8]) -> bool {
    if needle.is_empty() {
        return true;
    }
    if hay.len() < needle.len() {
        return false;
    }
    let first = needle[0];
    let last = hay.len() - needle.len();
    let mut i = 0usize;
    while i <= last {
        if hay[i] == first && hay[i..i + needle.len()] == *needle {
            return true;
        }
        i += 1;
    }
    false
}
