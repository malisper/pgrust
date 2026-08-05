//! Per-column-chunk cursor: walks pages, dispatches the decode kernel PER
//! PAGE (dictionary-to-PLAIN fallback happens mid-chunk on exactly the
//! wide columns of the target workload class), and fills row-dense batches.
//!
//! Dictionary pages decode ONCE per chunk into a typed dictionary; data
//! pages under dictionary encoding decode as index streams mapped through
//! it. String dictionaries are UTF-8 validated once, so index pages need no
//! per-value validation — the structural win of dict-heavy files.

use types_error::{PgError, PgResult};

use crate::codec::{decompress_page, PAD};
use crate::meta::{unsupported, CodecId, Phys};
use crate::page::{parse_page_header, Enc, PageKind};
use crate::plain::{
    invalid_utf8, plain_byte_array, plain_f32, plain_f64, plain_i32, plain_i64, utf8_ok,
};
use crate::rle::{corrupt_page, def_levels_max1, BoolBits, HybridState};
use crate::thrift::corrupt;

/// Row-dense decoded values: one entry per row, nulls carry a default in the
/// data lane and `true` in the null lane.
pub struct ColumnBatch {
    pub nulls: Vec<bool>,
    pub data: BatchData,
}

pub enum BatchData {
    Bool(Vec<bool>),
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    /// `ends[i]` is the arena end of row i; row start = previous end.
    Bytes { ends: Vec<u32>, arena: Vec<u8> },
}

impl ColumnBatch {
    pub fn new_for(phys: Phys) -> ColumnBatch {
        let data = match phys {
            Phys::Boolean => BatchData::Bool(Vec::new()),
            Phys::Int32 => BatchData::I32(Vec::new()),
            Phys::Int64 => BatchData::I64(Vec::new()),
            Phys::Float => BatchData::F32(Vec::new()),
            Phys::Double => BatchData::F64(Vec::new()),
            Phys::ByteArray => BatchData::Bytes { ends: Vec::new(), arena: Vec::new() },
            // INT96/FLBA are refused at binding; a placeholder keeps the
            // constructor total.
            Phys::Int96 | Phys::Flba(_) => {
                BatchData::Bytes { ends: Vec::new(), arena: Vec::new() }
            }
        };
        ColumnBatch { nulls: Vec::new(), data }
    }

    pub fn len(&self) -> usize {
        self.nulls.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nulls.is_empty()
    }

    #[inline]
    pub fn is_null(&self, i: usize) -> bool {
        self.nulls[i]
    }

    #[inline]
    pub fn bytes_at(&self, i: usize) -> &[u8] {
        let BatchData::Bytes { ends, arena } = &self.data else {
            unreachable!("bytes_at on a non-bytes batch");
        };
        let start = if i == 0 { 0 } else { ends[i - 1] as usize };
        &arena[start..ends[i] as usize]
    }

    fn clear(&mut self) {
        self.nulls.clear();
        match &mut self.data {
            BatchData::Bool(v) => v.clear(),
            BatchData::I32(v) => v.clear(),
            BatchData::I64(v) => v.clear(),
            BatchData::F32(v) => v.clear(),
            BatchData::F64(v) => v.clear(),
            BatchData::Bytes { ends, arena } => {
                ends.clear();
                arena.clear();
            }
        }
    }
}

enum Dict {
    I32(Vec<i32>),
    I64(Vec<i64>),
    F32(Vec<f32>),
    F64(Vec<f64>),
    Bytes { ends: Vec<u32>, arena: Vec<u8> },
}

impl Dict {
    fn len(&self) -> usize {
        match self {
            Dict::I32(v) => v.len(),
            Dict::I64(v) => v.len(),
            Dict::F32(v) => v.len(),
            Dict::F64(v) => v.len(),
            Dict::Bytes { ends, .. } => ends.len(),
        }
    }
}

enum ValState {
    /// Dictionary-index stream (bit-width byte already consumed).
    Dict(HybridState),
    PlainFixed { pos: usize, end: usize },
    PlainBytes { pos: usize, end: usize },
    PlainBool { bits: BoolBits, end: usize },
    /// RLE-encoded boolean data page (4-byte length prefix consumed).
    RleBool(HybridState),
}

struct PageState {
    /// Payload lives in the cursor scratch buffer (decompressed) or directly
    /// in the chunk buffer (UNCOMPRESSED codec).
    in_chunk: bool,
    values_left: usize,
    def: Option<HybridState>,
    val: ValState,
}

/// Chunk byte storage: owned (per-chunk pread) or a slice of one shared
/// whole-row-group read (the coalesced-read lane — sequential range read +
/// in-memory demux).
pub(crate) enum ChunkBytes {
    Owned(Vec<u8>),
    Shared { buf: std::sync::Arc<Vec<u8>>, start: usize, len: usize },
}

impl ChunkBytes {
    #[inline]
    fn bytes(&self) -> &[u8] {
        match self {
            ChunkBytes::Owned(v) => v,
            ChunkBytes::Shared { buf, start, len } => &buf[*start..*start + *len],
        }
    }
}

pub struct ColumnCursor {
    /// Whole chunk bytes (dictionary page + data pages), 64-byte padded.
    chunk: ChunkBytes,
    chunk_len: usize,
    codec: CodecId,
    phys: Phys,
    max_def: u16,
    validate_utf8: bool,
    name: String,
    /// Values (rows, nulls included) remaining across the chunk.
    values_left: u64,
    /// Next page-header offset inside `chunk`.
    walk: usize,
    dict: Option<Dict>,
    page: Option<PageState>,
    scratch: Vec<u8>,
}

fn oom() -> Box<PgError> {
    Box::new(PgError::error("out of memory decoding parquet column"))
}

impl ColumnCursor {
    pub(crate) fn new(
        chunk: ChunkBytes,
        codec: CodecId,
        phys: Phys,
        max_def: u16,
        validate_utf8: bool,
        name: String,
        num_values: i64,
    ) -> PgResult<ColumnCursor> {
        // Owned chunks get their 64-byte pad here; shared buffers were
        // padded once at the whole-row-group read.
        let chunk = match chunk {
            ChunkBytes::Owned(mut v) => {
                let n = v.len();
                v.try_reserve(PAD).map_err(|_| oom())?;
                v.resize(n + PAD, 0);
                v.truncate(n);
                ChunkBytes::Owned(v)
            }
            shared => shared,
        };
        let chunk_len = chunk.bytes().len();
        Ok(ColumnCursor {
            chunk,
            chunk_len,
            codec,
            phys,
            max_def,
            validate_utf8,
            name,
            values_left: num_values as u64,
            walk: 0,
            dict: None,
            page: None,
            scratch: Vec::new(),
        })
    }

    /// Fill `out` with exactly `n` rows. The row-group driver never asks for
    /// more rows than the group holds, so a short chunk is a corruption
    /// error, not EOF.
    pub fn read_batch(&mut self, out: &mut ColumnBatch, n: usize) -> PgResult<()> {
        out.clear();
        out.nulls.try_reserve(n).map_err(|_| oom())?;
        while out.nulls.len() < n {
            let need_page = match &self.page {
                Some(p) => p.values_left == 0,
                None => true,
            };
            if need_page && !self.advance_page()? {
                return Err(corrupt_page("column chunk ends before the declared row count"));
            }
            let want = n - out.nulls.len();
            self.fill_from_page(out, want)?;
        }
        Ok(())
    }

    /// Decode up to `want` rows from the current page.
    fn fill_from_page(&mut self, out: &mut ColumnBatch, want: usize) -> PgResult<()> {
        let ColumnCursor { chunk, page, scratch, dict, validate_utf8, name, values_left, .. } =
            self;
        let page_st = page.as_mut().expect("fill_from_page with a page");
        let buf: &[u8] = if page_st.in_chunk { chunk.bytes() } else { scratch };
        let k = want.min(page_st.values_left);
        debug_assert!(k > 0);

        // Definition levels (flat OPTIONAL): present/absent mask for the k
        // rows; REQUIRED columns take the all-present lane.
        let (present, nulls_start) = match &mut page_st.def {
            Some(def) => {
                let mut lv = vec![0u32; k];
                let present = def_levels_max1(def, buf, &mut lv)?;
                let base = out.nulls.len();
                out.nulls.extend(lv.iter().map(|&v| v == 0));
                (present, Some((base, lv)))
            }
            None => {
                out.nulls.extend(core::iter::repeat_n(false, k));
                (k, None)
            }
        };

        // Decode `present` values, then scatter across nulls if any.
        match &mut page_st.val {
            ValState::Dict(idx) => {
                let mut ids = vec![0u32; present];
                idx.fill(buf, &mut ids)?;
                let d = dict
                    .as_ref()
                    .ok_or_else(|| corrupt_page("dictionary-encoded page without a dictionary"))?;
                let dlen = d.len() as u32;
                // Branch-free bound accumulation over the whole batch.
                let mut bad = false;
                for &i in &ids {
                    bad |= i >= dlen;
                }
                if bad {
                    return Err(corrupt_page("dictionary index out of range"));
                }
                match (&mut out.data, d) {
                    (BatchData::I32(v), Dict::I32(dv)) => {
                        scatter_fixed(v, &ids, |i| dv[i as usize], &nulls_start, k, 0)?
                    }
                    (BatchData::I64(v), Dict::I64(dv)) => {
                        scatter_fixed(v, &ids, |i| dv[i as usize], &nulls_start, k, 0)?
                    }
                    (BatchData::F32(v), Dict::F32(dv)) => {
                        scatter_fixed(v, &ids, |i| dv[i as usize], &nulls_start, k, 0.0)?
                    }
                    (BatchData::F64(v), Dict::F64(dv)) => {
                        scatter_fixed(v, &ids, |i| dv[i as usize], &nulls_start, k, 0.0)?
                    }
                    (BatchData::Bytes { ends, arena }, Dict::Bytes { ends: de, arena: da }) => {
                        ends.try_reserve(k).map_err(|_| oom())?;
                        let mut id_it = ids.iter();
                        for row in 0..k {
                            let is_null = nulls_start
                                .as_ref()
                                .map(|(_, lv)| lv[row] == 0)
                                .unwrap_or(false);
                            if !is_null {
                                let i = *id_it.next().expect("present count") as usize;
                                let start = if i == 0 { 0 } else { de[i - 1] as usize };
                                let val = &da[start..de[i] as usize];
                                if arena.len() + val.len() > u32::MAX as usize {
                                    return Err(corrupt_page("string batch exceeds 4GB"));
                                }
                                arena.try_reserve(val.len()).map_err(|_| oom())?;
                                arena.extend_from_slice(val);
                            }
                            ends.push(arena.len() as u32);
                        }
                    }
                    _ => return Err(corrupt_page("dictionary type does not match column type")),
                }
            }
            ValState::PlainFixed { pos, end } => match &mut out.data {
                BatchData::I32(v) => {
                    scatter_plain(v, buf, *end, pos, present, &nulls_start, k, 0, plain_i32)?
                }
                BatchData::I64(v) => {
                    scatter_plain(v, buf, *end, pos, present, &nulls_start, k, 0, plain_i64)?
                }
                BatchData::F32(v) => {
                    scatter_plain(v, buf, *end, pos, present, &nulls_start, k, 0.0, plain_f32)?
                }
                BatchData::F64(v) => {
                    scatter_plain(v, buf, *end, pos, present, &nulls_start, k, 0.0, plain_f64)?
                }
                _ => return Err(corrupt_page("PLAIN fixed page on a non-fixed column")),
            },
            ValState::PlainBytes { pos, end } => {
                let BatchData::Bytes { ends, arena } = &mut out.data else {
                    return Err(corrupt_page("PLAIN byte-array page on a non-string column"));
                };
                match &nulls_start {
                    None => {
                        plain_byte_array(
                            buf,
                            *end,
                            pos,
                            present,
                            ends,
                            arena,
                            *validate_utf8,
                            name,
                        )?;
                    }
                    Some((_, lv)) => {
                        // Interleave: walk values for present rows only.
                        ends.try_reserve(k).map_err(|_| oom())?;
                        let mut tmp_ends: Vec<u32> = Vec::new();
                        for &l in lv.iter() {
                            if l != 0 {
                                tmp_ends.clear();
                                plain_byte_array(
                                    buf,
                                    *end,
                                    pos,
                                    1,
                                    &mut tmp_ends,
                                    arena,
                                    *validate_utf8,
                                    name,
                                )?;
                            }
                            ends.push(arena.len() as u32);
                        }
                    }
                }
            }
            ValState::PlainBool { bits, end } => {
                let BatchData::Bool(v) = &mut out.data else {
                    return Err(corrupt_page("boolean page on a non-boolean column"));
                };
                let mut tmp = vec![false; present];
                bits.fill(buf, *end, &mut tmp)?;
                scatter_bools(v, &tmp, &nulls_start, k)?;
            }
            ValState::RleBool(st) => {
                let BatchData::Bool(v) = &mut out.data else {
                    return Err(corrupt_page("boolean page on a non-boolean column"));
                };
                let mut tmp32 = vec![0u32; present];
                st.fill(buf, &mut tmp32)?;
                let tmp: Vec<bool> = tmp32.iter().map(|&x| x != 0).collect();
                scatter_bools(v, &tmp, &nulls_start, k)?;
            }
        }

        page_st.values_left -= k;
        if (*values_left as usize) < k {
            return Err(corrupt_page("pages hold more values than the chunk declares"));
        }
        *values_left -= k as u64;
        Ok(())
    }

    /// Walk to the next data page, decoding any dictionary page on the way.
    /// Returns false when the chunk's value count is exhausted.
    fn advance_page(&mut self) -> PgResult<bool> {
        loop {
            if self.values_left == 0 {
                return Ok(false);
            }
            if self.walk >= self.chunk_len {
                return Err(corrupt_page("column chunk ends before the declared value count"));
            }
            let head =
                parse_page_header(&self.chunk.bytes()[..self.chunk_len], self.walk, &self.name)?;
            let payload_start = head.payload_at;
            let payload_end = payload_start + head.compressed_size;
            self.walk = payload_end;
            match head.kind {
                PageKind::Skip => continue,
                PageKind::Dict { num_values, encoding } => {
                    if self.dict.is_some() {
                        return Err(corrupt("second dictionary page in one column chunk"));
                    }
                    if !matches!(encoding, Enc::Plain | Enc::PlainDictionary) {
                        return Err(unsupported(format!(
                            "dictionary page encoding {} (column \"{}\")",
                            encoding.name(),
                            self.name
                        )));
                    }
                    let (in_chunk, start, end) = self.page_payload(
                        payload_start,
                        head.compressed_size,
                        head.uncompressed_size,
                    )?;
                    let buf: &[u8] = if in_chunk { self.chunk.bytes() } else { &self.scratch };
                    self.dict = Some(decode_dict(
                        buf,
                        start,
                        end,
                        self.phys,
                        num_values,
                        self.validate_utf8,
                        &self.name,
                    )?);
                    continue;
                }
                PageKind::Data { num_values, encoding, def_encoding } => {
                    let (in_chunk, start, end) = self.page_payload(
                        payload_start,
                        head.compressed_size,
                        head.uncompressed_size,
                    )?;
                    let buf: &[u8] = if in_chunk { self.chunk.bytes() } else { &self.scratch };
                    // v1 page layout: [rep levels: absent at max_rep=0]
                    // [def levels: 4-byte len + RLE at max_def=1] [values].
                    let mut vpos = start;
                    let def = if self.max_def == 1 {
                        if def_encoding != Enc::Rle {
                            return Err(unsupported(format!(
                                "definition-level encoding {} (column \"{}\")",
                                def_encoding.name(),
                                self.name
                            )));
                        }
                        let Some(lb) = buf.get(vpos..vpos + 4).filter(|_| vpos + 4 <= end)
                        else {
                            return Err(corrupt_page("definition levels overrun page"));
                        };
                        let dlen =
                            u32::from_le_bytes(lb.try_into().expect("4-byte slice")) as usize;
                        let dstart = vpos + 4;
                        if dstart + dlen > end {
                            return Err(corrupt_page("definition levels overrun page"));
                        }
                        vpos = dstart + dlen;
                        Some(HybridState::new(dstart, dstart + dlen, 1)?)
                    } else {
                        None
                    };
                    let val = match encoding {
                        Enc::Plain => match self.phys {
                            Phys::Boolean => {
                                ValState::PlainBool { bits: BoolBits::new(vpos), end }
                            }
                            Phys::ByteArray => ValState::PlainBytes { pos: vpos, end },
                            Phys::Int32 | Phys::Int64 | Phys::Float | Phys::Double => {
                                ValState::PlainFixed { pos: vpos, end }
                            }
                            Phys::Int96 | Phys::Flba(_) => {
                                return Err(unsupported(format!(
                                    "physical type {} (column \"{}\")",
                                    self.phys.name(),
                                    self.name
                                )))
                            }
                        },
                        Enc::PlainDictionary | Enc::RleDictionary => {
                            let Some(&w) = buf.get(vpos).filter(|_| vpos < end) else {
                                return Err(corrupt_page("dictionary page bit width missing"));
                            };
                            if w > 32 {
                                return Err(corrupt_page("dictionary bit width above 32"));
                            }
                            ValState::Dict(HybridState::new(vpos + 1, end, w as usize)?)
                        }
                        Enc::Rle => {
                            if self.phys != Phys::Boolean {
                                return Err(unsupported(format!(
                                    "RLE-encoded {} values (column \"{}\")",
                                    self.phys.name(),
                                    self.name
                                )));
                            }
                            let Some(lb) = buf.get(vpos..vpos + 4).filter(|_| vpos + 4 <= end)
                            else {
                                return Err(corrupt_page("RLE boolean run overruns page"));
                            };
                            let rlen =
                                u32::from_le_bytes(lb.try_into().expect("4-byte slice"))
                                    as usize;
                            if vpos + 4 + rlen > end {
                                return Err(corrupt_page("RLE boolean run overruns page"));
                            }
                            ValState::RleBool(HybridState::new(vpos + 4, vpos + 4 + rlen, 1)?)
                        }
                        other => {
                            return Err(unsupported(format!(
                                "encoding {} (column \"{}\")",
                                other.name(),
                                self.name
                            )))
                        }
                    };
                    self.page = Some(PageState { in_chunk, values_left: num_values, def, val });
                    return Ok(true);
                }
            }
        }
    }

    /// Resolve a page payload: either borrowed straight from the chunk
    /// (UNCOMPRESSED) or decompressed into the cursor scratch buffer.
    /// Returns (in_chunk, start, end) into the respective buffer.
    fn page_payload(
        &mut self,
        start: usize,
        compressed: usize,
        uncompressed: usize,
    ) -> PgResult<(bool, usize, usize)> {
        if self.codec == CodecId::Uncompressed {
            if compressed != uncompressed {
                return Err(corrupt_page("uncompressed page sizes disagree"));
            }
            return Ok((true, start, start + compressed));
        }
        let src = self
            .chunk
            .bytes()
            .get(start..start + compressed)
            .ok_or_else(|| corrupt_page("page payload overruns chunk"))?;
        decompress_page(self.codec, src, &mut self.scratch, uncompressed)?;
        Ok((false, 0, uncompressed))
    }
}

/// PLAIN fixed-width decoder shape shared by the scatter helper.
type PlainDecode<T> = fn(&[u8], usize, &mut usize, usize, &mut Vec<T>) -> PgResult<()>;

/// Scatter `present` decoded fixed-width values across `k` rows.
#[allow(clippy::too_many_arguments)]
fn scatter_plain<T: Copy + Default>(
    out: &mut Vec<T>,
    buf: &[u8],
    end: usize,
    pos: &mut usize,
    present: usize,
    nulls: &Option<(usize, Vec<u32>)>,
    k: usize,
    default: T,
    decode: PlainDecode<T>,
) -> PgResult<()> {
    match nulls {
        None => decode(buf, end, pos, k, out),
        Some((_, lv)) => {
            let mut tmp: Vec<T> = Vec::new();
            decode(buf, end, pos, present, &mut tmp)?;
            out.try_reserve(k).map_err(|_| oom())?;
            let mut j = 0usize;
            for &l in lv.iter() {
                if l != 0 {
                    out.push(tmp[j]);
                    j += 1;
                } else {
                    out.push(default);
                }
            }
            Ok(())
        }
    }
}

fn scatter_fixed<T: Copy>(
    out: &mut Vec<T>,
    ids: &[u32],
    get: impl Fn(u32) -> T,
    nulls: &Option<(usize, Vec<u32>)>,
    k: usize,
    default: T,
) -> PgResult<()> {
    out.try_reserve(k).map_err(|_| oom())?;
    match nulls {
        None => {
            debug_assert_eq!(ids.len(), k);
            out.extend(ids.iter().map(|&i| get(i)));
        }
        Some((_, lv)) => {
            let mut j = 0usize;
            for &l in lv.iter() {
                if l != 0 {
                    out.push(get(ids[j]));
                    j += 1;
                } else {
                    out.push(default);
                }
            }
        }
    }
    Ok(())
}

fn scatter_bools(
    out: &mut Vec<bool>,
    vals: &[bool],
    nulls: &Option<(usize, Vec<u32>)>,
    k: usize,
) -> PgResult<()> {
    out.try_reserve(k).map_err(|_| oom())?;
    match nulls {
        None => out.extend_from_slice(vals),
        Some((_, lv)) => {
            let mut j = 0usize;
            for &l in lv.iter() {
                if l != 0 {
                    out.push(vals[j]);
                    j += 1;
                } else {
                    out.push(false);
                }
            }
        }
    }
    Ok(())
}

/// Decode a PLAIN dictionary page into a typed dictionary. String
/// dictionaries validate UTF-8 here, once per chunk.
fn decode_dict(
    buf: &[u8],
    start: usize,
    end: usize,
    phys: Phys,
    num_values: usize,
    validate_utf8: bool,
    name: &str,
) -> PgResult<Dict> {
    let mut pos = start;
    match phys {
        Phys::Int32 => {
            let mut v = Vec::new();
            plain_i32(buf, end, &mut pos, num_values, &mut v)?;
            Ok(Dict::I32(v))
        }
        Phys::Int64 => {
            let mut v = Vec::new();
            plain_i64(buf, end, &mut pos, num_values, &mut v)?;
            Ok(Dict::I64(v))
        }
        Phys::Float => {
            let mut v = Vec::new();
            plain_f32(buf, end, &mut pos, num_values, &mut v)?;
            Ok(Dict::F32(v))
        }
        Phys::Double => {
            let mut v = Vec::new();
            plain_f64(buf, end, &mut pos, num_values, &mut v)?;
            Ok(Dict::F64(v))
        }
        Phys::ByteArray => {
            let mut ends = Vec::new();
            let mut arena = Vec::new();
            plain_byte_array(buf, end, &mut pos, num_values, &mut ends, &mut arena, false, name)?;
            if validate_utf8 {
                // Batched validation, once per chunk: UTF-8 is closed under
                // concatenation, so an invalid arena means some value is
                // invalid. A valid arena still needs every value to start on
                // a character boundary (a boundary inside a multi-byte
                // sequence makes both neighbors invalid) — one branch-free
                // continuation-byte test per value.
                if !utf8_ok(&arena) {
                    return Err(invalid_utf8(name));
                }
                let mut prev = 0usize;
                let mut bad = false;
                for &e in &ends {
                    if prev < arena.len() {
                        bad |= (arena[prev] & 0xc0) == 0x80;
                    }
                    prev = e as usize;
                }
                if bad {
                    return Err(invalid_utf8(name));
                }
            }
            Ok(Dict::Bytes { ends, arena })
        }
        Phys::Boolean | Phys::Int96 | Phys::Flba(_) => Err(unsupported(format!(
            "dictionary-encoded {} column \"{}\"",
            phys.name(),
            name
        ))),
    }
}
