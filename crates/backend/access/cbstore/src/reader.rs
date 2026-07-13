//! mmap read path: part open, footer parse, granule decode kernels
//! (docs/design/cbstore-impl.md §6).

use ::datum::Datum;
use ::types_error::{PgError, PgResult};

use crate::format::*;
use crate::segfile::{SegFile, SegMap};
use crate::writer::FooterRg;

// LZ4 frame decode target inside the u64-backed arena: `raw_len` payload
// bytes + the decoder's OUT_PAD tail slack (wild-copy spill scratch, never
// published). The arena is grown monotonically and NEVER shrunk/cleared, so
// (a) reuse across granules skips the zero-fill (the old clear+resize path
// memset the whole buffer before every decompress — a full extra touch of
// the output), and (b) every byte handed to the decoder is initialized.
// Base stays 8-aligned for the varlena images.
fn arena_frame(arena: &mut Vec<u64>, raw_len: usize) -> &mut [u8] {
    let words = (raw_len + crate::lz4dec::OUT_PAD).div_ceil(8);
    if arena.len() < words {
        arena.resize(words, 0);
    }
    // SAFETY: u64 backing reinterpreted as raw_len + OUT_PAD initialized
    // bytes (len >= words holds by the resize above).
    unsafe {
        std::slice::from_raw_parts_mut(
            arena.as_mut_ptr().cast::<u8>(),
            raw_len + crate::lz4dec::OUT_PAD,
        )
    }
}

pub fn read_header(hdr: &[u8]) -> PgResult<(u64, u64, u32)> {
    let version = get_u32(hdr, 8);
    if get_u64(hdr, 0) != CB_MAGIC || !(CB_VERSION_V1..=CB_VERSION).contains(&version) {
        return Err(Box::new(PgError::error("cbstore: bad part header".to_string())));
    }
    Ok((get_u64(hdr, 16), get_u64(hdr, 24), version))
}

/// Abort/crash-tolerant header read: an all-zero header page reads as "no
/// committed footer" (None) instead of erroring. Zeros can only mean a part
/// that never published — writers historically wrote row-group bytes past a
/// still-unwritten header, so a COPY aborted mid-statement left len >=
/// CB_HEADER_LEN files whose first page is a zero hole; a published part's
/// header is written and fsynced at every publish and is never zeroed again.
pub fn read_header_opt(hdr: &[u8]) -> PgResult<Option<(u64, u64, u32)>> {
    if hdr[..CB_HEADER_LEN as usize].iter().all(|&b| b == 0) {
        return Ok(None);
    }
    read_header(hdr).map(Some)
}

// want_sums materializes the v4 per-RG sums into FooterRg::sums (the writer's
// reopen-append path re-emits them); readers leave them on disk and consume
// lazily via Part::rg_sum. The CRC always covers the whole footer body.
pub fn read_footer_rgs(
    file: &mut SegFile,
    footer_off: u64,
    ncols: usize,
    version: u32,
    want_sums: bool,
) -> PgResult<(Vec<FooterRg>, u64, Vec<u64>, Vec<u8>, Vec<u16>)> {
    let total_len = file.total_len();
    if footer_off < CB_HEADER_LEN || footer_off.checked_add(8).is_none_or(|e| e > total_len) {
        return Err(Box::new(PgError::error(
            "cbstore: corrupt part (footer offset out of bounds)".to_string(),
        )));
    }
    let mut fixed = [0u8; 8];
    file.read_exact_at(&mut fixed, footer_off)?;
    let nrgs = get_u32(&fixed, 0) as usize;
    let fncols = get_u32(&fixed, 4) as usize;
    if fncols != ncols {
        return Err(Box::new(PgError::error("cbstore: footer ncols mismatch".to_string())));
    }
    let ndv_len = if version >= CB_VERSION_V2 { ncols * 8 } else { 0 };
    let sums_len = if version >= CB_VERSION_V4 { nrgs * ncols * 16 } else { 0 };
    let sorted_len = if version >= CB_VERSION_V5 { ncols } else { 0 };
    let ckey_len = if version >= CB_VERSION_V6 { CB_CLUSTER_KEY_SECTION_LEN } else { 0 };
    let body_len = 8 + nrgs * 24 + nrgs * ncols * 24 + ndv_len + sums_len + sorted_len + ckey_len + 16;
    // Bounds-gate before the allocation and read: a torn/garbage footer word
    // must produce a clean error, not a huge alloc or a read past EOF.
    if body_len as u64 > total_len - footer_off {
        return Err(Box::new(PgError::error(
            "cbstore: corrupt part (footer body out of bounds)".to_string(),
        )));
    }
    let mut buf = vec![0u8; body_len];
    file.read_exact_at(&mut buf, footer_off)?;
    parse_footer(&buf, nrgs, ncols, version, want_sums)
        .map(|(rgs, ndv, sorted, ckey)| (rgs, footer_off + body_len as u64, ndv, sorted, ckey))
}

pub fn parse_footer(
    buf: &[u8],
    nrgs: usize,
    ncols: usize,
    version: u32,
    want_sums: bool,
) -> PgResult<(Vec<FooterRg>, Vec<u64>, Vec<u8>, Vec<u16>)> {
    let tail = buf.len() - 16;
    if get_u32(buf, tail + 12) != CB_FOOTER_MAGIC
        || get_u64(buf, tail) != buf.len() as u64
        || get_u32(buf, tail + 8) != crc32c(&buf[..tail])
    {
        return Err(Box::new(PgError::error("cbstore: corrupt footer".to_string())));
    }
    let mut rgs = Vec::with_capacity(nrgs);
    let mut off = 8;
    for _ in 0..nrgs {
        rgs.push(FooterRg {
            file_off: get_u64(buf, off),
            nrows: get_u32(buf, off + 8),
            xmin: get_u32(buf, off + 12),
            flags: get_u32(buf, off + 16),
            chunks: Vec::with_capacity(ncols),
            sums: Vec::new(),
        });
        off += 24;
    }
    for rg in rgs.iter_mut() {
        for _ in 0..ncols {
            rg.chunks.push((get_u64(buf, off), get_i64(buf, off + 8), get_i64(buf, off + 16)));
            off += 24;
        }
    }
    let mut ndv = Vec::new();
    if version >= CB_VERSION_V2 {
        ndv.reserve(ncols);
        for _ in 0..ncols {
            ndv.push(get_u64(buf, off));
            off += 8;
        }
    }
    // The sums section precedes the v5 sorted flags: a lazy (want_sums=false)
    // parse must still advance past it or every later section reads skewed
    // (the compose-v6 cross-member bug class).
    if version >= CB_VERSION_V4 {
        if want_sums {
            for rg in rgs.iter_mut() {
                rg.sums.reserve(ncols);
                for _ in 0..ncols {
                    rg.sums.push(get_i128(buf, off));
                    off += 16;
                }
            }
        } else {
            off += nrgs * ncols * 16;
        }
    }
    // v5 sorted flags; pre-v5 parts read as all-unknown.
    let mut sorted = vec![0u8; ncols];
    if version >= CB_VERSION_V5 {
        sorted.copy_from_slice(&buf[off..off + ncols]);
        off += ncols;
    }
    // v6 cluster-key section; pre-v6 parts read as no-cluster-key.
    let mut cluster_key = Vec::new();
    if version >= CB_VERSION_V6 {
        let nkeys = u16::from_le_bytes(buf[off..off + 2].try_into().unwrap()) as usize;
        if nkeys > CB_CLUSTER_KEY_MAX_COLS {
            return Err(Box::new(PgError::error("cbstore: corrupt footer".to_string())));
        }
        for k in 0..nkeys {
            let o = off + 2 + k * 2;
            cluster_key.push(u16::from_le_bytes(buf[o..o + 2].try_into().unwrap()));
        }
    }
    Ok((rgs, ndv, sorted, cluster_key))
}

// Planner sizing: header + footer reads only (no SegMap mmap of the data
// body). None: empty table (no committed footer yet).
pub fn part_footer_rows(path: &str, ncols: usize) -> PgResult<Option<u64>> {
    let mut file = SegFile::open_rw(path)?;
    if file.total_len() < CB_HEADER_LEN {
        return Ok(None);
    }
    let mut hdr = [0u8; CB_HEADER_LEN as usize];
    file.read_exact_at(&mut hdr, 0)?;
    let Some((footer_off, _, version)) = read_header_opt(&hdr)? else { return Ok(None) };
    if footer_off == 0 {
        return Ok(None);
    }
    let (rgs, _, _, _, _) = read_footer_rgs(&mut file, footer_off, ncols, version, false)?;
    Ok(Some(rgs.iter().map(|rg| rg.nrows as u64).sum()))
}

// ANALYZE NDV source: header + footer reads only. None: no committed footer
// or a v1 part; per-entry 0 = unknown (append-invalidated).
pub fn part_footer_ndv(path: &str, ncols: usize) -> PgResult<Option<Vec<u64>>> {
    let mut file = SegFile::open_rw(path)?;
    if file.total_len() < CB_HEADER_LEN {
        return Ok(None);
    }
    let mut hdr = [0u8; CB_HEADER_LEN as usize];
    file.read_exact_at(&mut hdr, 0)?;
    let Some((footer_off, _, version)) = read_header_opt(&hdr)? else { return Ok(None) };
    if footer_off == 0 || version < CB_VERSION_V2 {
        return Ok(None);
    }
    let (_, _, ndv, _, _) = read_footer_rgs(&mut file, footer_off, ncols, version, false)?;
    Ok(Some(ndv))
}

pub struct Part {
    map: SegMap,
    pub rgs: Vec<FooterRg>,
    pub ncols: usize,
    // Header footer_off this footer was parsed from (part-cache probe key).
    pub footer_off: u64,
    // v5 per-column sorted flags (1 = part rows non-decreasing); all-0 on
    // pre-v5 parts.
    pub sorted: Vec<u8>,
    // v6 declared cluster key (zero-based column indexes, key order); empty
    // on pre-v6 parts or when none was declared. Per-RG sortedness under it
    // is RG_FLAG_CLUSTERED.
    pub cluster_key: Vec<u16>,
    // File offset of the v4 per-RG sums section (0 = none): sums stay on
    // disk, CRC-validated with the footer at open, and are read through the
    // mmap only when a metadata-sum consumer asks (rg_sum).
    sums_off: u64,
}

impl Part {
    // None: empty table (no committed footer yet).
    pub fn open(path: &str, ncols: usize) -> PgResult<Option<Part>> {
        let mut file = SegFile::open_rw(path)?;
        if file.total_len() < CB_HEADER_LEN {
            return Ok(None);
        }
        let mut hdr = [0u8; CB_HEADER_LEN as usize];
        file.read_exact_at(&mut hdr, 0)?;
        let Some((footer_off, _fp, version)) = read_header_opt(&hdr)? else { return Ok(None) };
        if footer_off == 0 {
            return Ok(None);
        }
        let (rgs, _footer_end, _ndv, sorted, cluster_key) =
            read_footer_rgs(&mut file, footer_off, ncols, version, false)?;
        drop(file);
        let Some(map) = SegMap::open(path)? else { return Ok(None) };
        // Structural skippability guard: a footer whose RG directory points
        // past the live mapping is torn state — readers must error cleanly
        // here rather than slice-panic on garbage row-group offsets.
        for rg in &rgs {
            if rg.file_off.saturating_add(CB_RG_HEADER_LEN as u64) > map.bytes().len() as u64 {
                return Err(Box::new(PgError::error(
                    "cbstore: corrupt part (row group out of bounds)".to_string(),
                )));
            }
        }
        let sums_off = if version >= CB_VERSION_V4 {
            let ndv_len = if version >= CB_VERSION_V2 { ncols * 8 } else { 0 };
            footer_off + (8 + rgs.len() * 24 + rgs.len() * ncols * 24 + ndv_len) as u64
        } else {
            0
        };
        Ok(Some(Part { map, rgs, ncols, footer_off, sorted, cluster_key, sums_off }))
    }

    // Footer sum for (rg, col); caller gates on RG_FLAG_SUMS (which only a
    // v4+ writer sets, so sums_off != 0 whenever the flag is present).
    pub fn rg_sum(&self, rg: usize, col: usize) -> i128 {
        debug_assert!(self.sums_off != 0 && self.rgs[rg].flags & RG_FLAG_SUMS != 0);
        get_i128(self.bytes(), self.sums_off as usize + (rg * self.ncols + col) * 16)
    }

    pub fn bytes(&self) -> &[u8] {
        self.map.bytes()
    }

    pub fn total_rows(&self) -> u64 {
        self.rgs.iter().map(|rg| rg.nrows as u64).sum()
    }

    pub fn chunk(&self, rg: usize, col: usize) -> ChunkView<'_> {
        let m = &self.rgs[rg];
        let base = (m.file_off + m.chunks[col].0) as usize;
        ChunkView::at(self.bytes(), base, m.nrows)
    }
}

// Decompress one v6 frame (u32 raw_len | u32 comp_len | bytes) body into
// `dst` (an arena_frame slice: raw_len payload + OUT_PAD slack). LZ4 goes
// through the lane-v2-decode padded kernel (wild copies spill into the pad);
// ZSTD writes exactly raw_len (the pad is untouched slack).
pub(crate) fn decompress_frame_into(codec: Codec, src: &[u8], dst: &mut [u8], raw_len: usize) {
    match codec {
        Codec::Lz4 => {
            crate::lz4dec::decompress_padded(src, dst, raw_len)
                .unwrap_or_else(|e| panic!("cbstore: corrupt LZ4 frame: {e}"));
        }
        Codec::Zstd => {
            let got = zstd::bulk::Decompressor::new()
                .expect("cbstore: zstd decompressor init failed")
                .decompress_to_buffer(src, &mut dst[..raw_len])
                .expect("cbstore: corrupt ZSTD frame");
            assert_eq!(got, raw_len, "cbstore: ZSTD frame length mismatch");
        }
        Codec::None => unreachable!("cbstore: decompress with Codec::None"),
    }
}

pub struct ChunkView<'a> {
    part: &'a [u8],
    pub hdr: ChunkHeader,
    gdir_off: usize,
    blockzm_off: usize,
    bloom_off: usize,
    payload_off: usize,
    nrows: u32,
}

// bench/rig fixture path only: a ChunkView over a bare payload image.
#[cfg(feature = "bench-internals")]
pub fn chunk_view_for_bench(part: &[u8], hdr: ChunkHeader, nrows: u32) -> ChunkView<'_> {
    ChunkView { part, hdr, gdir_off: 0, blockzm_off: 0, bloom_off: 0, payload_off: 0, nrows }
}

impl<'a> ChunkView<'a> {
    pub fn at(part: &'a [u8], base: usize, nrows: u32) -> ChunkView<'a> {
        let hdr = ChunkHeader::decode(&part[base..base + CB_CHUNK_HEADER_LEN]);
        let ng = hdr.ngranules as usize;
        let gdir_off = base + CB_CHUNK_HEADER_LEN;
        let blockzm_off = gdir_off + ng * CB_GRANULE_ENTRY_LEN;
        let blockzm_len = if hdr.flags & CHUNK_FLAG_BLOCK_ZM != 0 {
            ng * BLOCKS_PER_GRANULE * CB_BLOCK_ZM_ENTRY_LEN
        } else {
            0
        };
        let bloom_off = blockzm_off + blockzm_len;
        let bloom_len =
            if hdr.flags & CHUNK_FLAG_BLOOM != 0 { ng * crate::bloom::BLOOM_BYTES } else { 0 };
        let payload_off = align64((bloom_off + bloom_len) as u64) as usize;
        ChunkView { part, hdr, gdir_off, blockzm_off, bloom_off, payload_off, nrows }
    }

    pub fn has_block_zm(&self) -> bool {
        self.hdr.flags & CHUNK_FLAG_BLOCK_ZM != 0
    }

    pub fn block_minmax(&self, g: usize, b: usize) -> (i64, i64) {
        debug_assert!(self.has_block_zm());
        let off = self.blockzm_off + (g * BLOCKS_PER_GRANULE + b) * CB_BLOCK_ZM_ENTRY_LEN;
        (get_i64(self.part, off), get_i64(self.part, off + 8))
    }

    pub fn has_bloom(&self) -> bool {
        self.hdr.flags & CHUNK_FLAG_BLOOM != 0
    }

    pub fn bloom_may_contain(&self, g: usize, v: i64) -> bool {
        debug_assert!(self.has_bloom());
        let off = self.bloom_off + g * crate::bloom::BLOOM_BYTES;
        crate::bloom::bloom_may_contain(&self.part[off..off + crate::bloom::BLOOM_BYTES], v)
    }

    pub fn granule(&self, g: usize) -> GranuleEntry {
        let off = self.gdir_off + g * CB_GRANULE_ENTRY_LEN;
        GranuleEntry {
            payload_off: get_u64(self.part, off),
            min: get_i64(self.part, off + 8),
            max: get_i64(self.part, off + 16),
        }
    }

    fn payload(&self) -> &'a [u8] {
        &self.part[self.payload_off..self.payload_off + self.hdr.payload_len as usize]
    }

    fn granule_rows(&self, g: usize) -> usize {
        let lo = g * GRANULE_ROWS;
        (self.nrows as usize - lo).min(GRANULE_ROWS)
    }

    // Granule g's plain fixed-width payload bytes (n rows x width): the
    // in-file slice on unframed chunks, else the v6 granule frame
    // decompressed into `arena` (u64-backed scratch, reused per granule).
    fn fixed_granule_bytes<'s>(
        &'s self,
        g: usize,
        n: usize,
        arena: &'s mut Vec<u64>,
    ) -> &'s [u8] {
        let w = self.hdr.width as usize;
        let p = self.payload();
        if self.hdr.codec == Codec::None {
            let lo = g * GRANULE_ROWS;
            return &p[lo * w..(lo + n) * w];
        }
        let fo = self.granule(g).payload_off as usize;
        let raw_len = get_u32(p, fo) as usize;
        let comp_len = get_u32(p, fo + 4) as usize;
        assert_eq!(raw_len, n * w, "cbstore: framed granule length mismatch");
        let dst = arena_frame(arena, raw_len);
        decompress_frame_into(self.hdr.frame_codec(), &p[fo + 8..fo + 8 + comp_len], dst, raw_len);
        &dst[..raw_len]
    }

    // Build the RG's dictionary Datum table once (keyed on dict.is_empty();
    // the caller clears it at RG boundaries). Lz4Dict decompresses the blob
    // into `arena` — the dict table (and every published Datum) points there
    // for as long as the dict_rg cache holds.
    fn build_dict(&self, dict: &mut Vec<Datum>, arena: &mut Vec<u64>) {
        if !dict.is_empty() {
            return;
        }
        let ndv = self.hdr.aux as usize;
        let w = self.hdr.width as usize;
        let p = self.payload();
        let codes_len = align4(self.nrows as usize * w);
        let off_tab = &p[codes_len..codes_len + ndv * 4];
        let blob = &p[codes_len + ndv * 4..];
        let blob_base = if self.hdr.encoding == Encoding::Lz4Dict {
            let raw_len = get_u32(blob, 0) as usize;
            let comp_len = get_u32(blob, 4) as usize;
            let dst = arena_frame(arena, raw_len);
            decompress_frame_into(self.hdr.frame_codec(), &blob[8..8 + comp_len], dst, raw_len);
            dst.as_ptr() as usize
        } else {
            blob.as_ptr() as usize
        };
        dict.reserve(ndv);
        for c in off_tab.chunks_exact(4) {
            let o = u32::from_le_bytes(c.try_into().unwrap()) as usize;
            dict.push(Datum::from_usize(blob_base + o));
        }
    }

    /// Dict-lane decode of granule g: the granule's codes widened to u32
    /// into `codes` (no per-row dictionary gather) + the RG dict table.
    /// false = the chunk is not dict-encoded (caller decodes Datums).
    pub fn decode_granule_codes(
        &self,
        g: usize,
        codes: &mut Vec<u32>,
        dict: &mut Vec<Datum>,
        arena: &mut Vec<u64>,
    ) -> bool {
        if !matches!(self.hdr.encoding, Encoding::Dict | Encoding::Lz4Dict) {
            return false;
        }
        self.build_dict(dict, arena);
        let n = self.granule_rows(g);
        let lo = g * GRANULE_ROWS;
        let p = self.payload();
        codes.clear();
        codes.reserve(n);
        // Straight-line widen into spare capacity: `push` per element carries
        // a capacity check the autovectorizer can't prove dead across the
        // loop (asm-diff: per-byte push defeats vectorization).
        let dst = &mut codes.spare_capacity_mut()[..n];
        match self.hdr.width as usize {
            1 => {
                for (d, &c) in dst.iter_mut().zip(&p[lo..lo + n]) {
                    d.write(c as u32);
                }
            }
            2 => {
                for (d, c) in dst.iter_mut().zip(p[lo * 2..(lo + n) * 2].chunks_exact(2)) {
                    d.write(u16::from_le_bytes(c.try_into().unwrap()) as u32);
                }
            }
            _ => {
                for (d, c) in dst.iter_mut().zip(p[lo * 4..(lo + n) * 4].chunks_exact(4)) {
                    d.write(u32::from_le_bytes(c.try_into().unwrap()));
                }
            }
        }
        // SAFETY: the zipped loops above wrote exactly the first `n` slots of
        // the spare capacity reserved above.
        unsafe { codes.set_len(n) };
        true
    }

    // Decode granule g into `out` (reused, resized to the granule's rows).
    // Int columns produce sign-extended Datum words; text columns produce
    // pointers to in-file 4B-U varlena images — except Lz4Text, whose
    // pointers land in `arena` (u64-backed for varlena alignment; reused per
    // granule, so published Datums live exactly as long as `out`'s).
    pub fn decode_granule(
        &self,
        g: usize,
        out: &mut Vec<Datum>,
        dict: &mut Vec<Datum>,
        arena: &mut Vec<u64>,
    ) {
        let n = self.granule_rows(g);
        out.clear();
        out.reserve(n);
        let lo = g * GRANULE_ROWS;
        let p = self.payload();
        match self.hdr.encoding {
            Encoding::Const => {
                out.resize(n, Datum::from_i64(self.hdr.aux));
            }
            Encoding::For => {
                let base = self.hdr.aux;
                // v6: framed chunks decompress the granule into `arena` and
                // widen from there; unframed slices the file directly.
                let src = self.fixed_granule_bytes(g, n, arena);
                // Straight-line widen+add into spare capacity (asm-diff:
                // per-element `push` carries a capacity check the
                // autovectorizer can't hoist out of the loop).
                let dst = &mut out.spare_capacity_mut()[..n];
                match self.hdr.width {
                    1 => {
                        for (d, &b) in dst.iter_mut().zip(src) {
                            d.write(Datum::from_i64(base + b as i64));
                        }
                    }
                    2 => {
                        for (d, c) in dst.iter_mut().zip(src.chunks_exact(2)) {
                            d.write(Datum::from_i64(
                                base + u16::from_le_bytes(c.try_into().unwrap()) as i64,
                            ));
                        }
                    }
                    4 => {
                        for (d, c) in dst.iter_mut().zip(src.chunks_exact(4)) {
                            d.write(Datum::from_i64(
                                base + u32::from_le_bytes(c.try_into().unwrap()) as i64,
                            ));
                        }
                    }
                    w => panic!("cbstore: FOR width {w}"),
                }
                // SAFETY: every slot of dst (the first `n` spare slots) was
                // written by exactly one arm above.
                unsafe { out.set_len(out.len() + n) };
            }
            Encoding::Raw => {
                debug_assert_eq!(self.hdr.width, 8);
                let src = self.fixed_granule_bytes(g, n, arena);
                let dst = &mut out.spare_capacity_mut()[..n];
                for (d, c) in dst.iter_mut().zip(src.chunks_exact(8)) {
                    d.write(Datum::from_i64(i64::from_le_bytes(c.try_into().unwrap())));
                }
                // SAFETY: dst (the first `n` spare slots) was fully written above.
                unsafe { out.set_len(out.len() + n) };
            }
            Encoding::DeltaFor => {
                // Always framed (format.rs §DeltaFor). comp_len == raw_len
                // marks an in-place uncompressed frame — the writer's
                // expansion guard never emits a compressed frame at exactly
                // raw_len, so the test is unambiguous.
                let fo = self.granule(g).payload_off as usize;
                let raw_len = get_u32(p, fo) as usize;
                let comp_len = get_u32(p, fo + 4) as usize;
                let img: &[u8] = if comp_len == raw_len {
                    &p[fo + 8..fo + 8 + raw_len]
                } else {
                    let dst = arena_frame(arena, raw_len);
                    decompress_frame_into(
                        self.hdr.frame_codec(),
                        &p[fo + 8..fo + 8 + comp_len],
                        dst,
                        raw_len,
                    );
                    &dst[..raw_len]
                };
                let first = i64::from_le_bytes(img[0..8].try_into().unwrap());
                let dmin = i64::from_le_bytes(img[8..16].try_into().unwrap());
                let w = img[16] as usize;
                debug_assert_eq!(raw_len, 24 + (n - 1) * w);
                let packed = &img[24..];
                // Prefix sum in the wrapping domain: v[i] = v[i-1] +w
                // (dmin +w packed[i-1]); packed entries zero-extend.
                let dst = &mut out.spare_capacity_mut()[..n];
                dst[0].write(Datum::from_i64(first));
                let mut acc = first;
                match w {
                    0 => {
                        for d in dst[1..].iter_mut() {
                            acc = acc.wrapping_add(dmin);
                            d.write(Datum::from_i64(acc));
                        }
                    }
                    1 => {
                        for (d, &b) in dst[1..].iter_mut().zip(&packed[..n - 1]) {
                            acc = acc.wrapping_add(dmin.wrapping_add(b as i64));
                            d.write(Datum::from_i64(acc));
                        }
                    }
                    2 => {
                        for (d, c) in
                            dst[1..].iter_mut().zip(packed[..(n - 1) * 2].chunks_exact(2))
                        {
                            let b = u16::from_le_bytes(c.try_into().unwrap()) as i64;
                            acc = acc.wrapping_add(dmin.wrapping_add(b));
                            d.write(Datum::from_i64(acc));
                        }
                    }
                    4 => {
                        for (d, c) in
                            dst[1..].iter_mut().zip(packed[..(n - 1) * 4].chunks_exact(4))
                        {
                            let b = u32::from_le_bytes(c.try_into().unwrap()) as i64;
                            acc = acc.wrapping_add(dmin.wrapping_add(b));
                            d.write(Datum::from_i64(acc));
                        }
                    }
                    _ => {
                        for (d, c) in
                            dst[1..].iter_mut().zip(packed[..(n - 1) * 8].chunks_exact(8))
                        {
                            let b = i64::from_le_bytes(c.try_into().unwrap());
                            acc = acc.wrapping_add(dmin.wrapping_add(b));
                            d.write(Datum::from_i64(acc));
                        }
                    }
                }
                // SAFETY: dst[0] plus the n-1 zipped slots above cover all n.
                unsafe { out.set_len(out.len() + n) };
            }
            Encoding::Dict | Encoding::Lz4Dict => {
                self.build_dict(dict, arena);
                let w = self.hdr.width as usize;
                // The dictionary gather (dict[code]) is a data-dependent
                // random access — no autovectorizer or NEON gather beats a
                // scalar loop here. Still drop the per-element `push` check
                // on the write side.
                let dst = &mut out.spare_capacity_mut()[..n];
                match w {
                    1 => {
                        for (d, &c) in dst.iter_mut().zip(&p[lo..lo + n]) {
                            d.write(dict[c as usize]);
                        }
                    }
                    2 => {
                        for (d, c) in dst.iter_mut().zip(p[lo * 2..(lo + n) * 2].chunks_exact(2)) {
                            d.write(dict[u16::from_le_bytes(c.try_into().unwrap()) as usize]);
                        }
                    }
                    _ => {
                        for (d, c) in dst.iter_mut().zip(p[lo * 4..(lo + n) * 4].chunks_exact(4)) {
                            d.write(dict[u32::from_le_bytes(c.try_into().unwrap()) as usize]);
                        }
                    }
                }
                // SAFETY: dst (the first `n` spare slots) was fully written above.
                unsafe { out.set_len(out.len() + n) };
            }
            Encoding::RawText => {
                let offs_len = self.nrows as usize * 4;
                // One bounds check per granule; per-row offsets add to the
                // blob base unchecked, same file-trust posture as Lz4Text.
                let base = self.part[self.payload_off + offs_len..].as_ptr() as usize;
                let dst = &mut out.spare_capacity_mut()[..n];
                for (d, c) in dst.iter_mut().zip(p[lo * 4..(lo + n) * 4].chunks_exact(4)) {
                    let o = u32::from_le_bytes(c.try_into().unwrap()) as usize;
                    d.write(Datum::from_usize(base + o));
                }
                // SAFETY: dst (the first `n` spare slots) was fully written above.
                unsafe { out.set_len(out.len() + n) };
            }
            Encoding::Lz4Text => {
                let fo = self.granule(g).payload_off as usize;
                let raw_len = get_u32(p, fo) as usize;
                let comp_len = get_u32(p, fo + 4) as usize;
                let dst = arena_frame(arena, raw_len);
                decompress_frame_into(self.hdr.frame_codec(), &p[fo + 8..fo + 8 + comp_len], dst, raw_len);
                let base = dst.as_ptr() as usize;
                let dst = &mut out.spare_capacity_mut()[..n];
                for (d, c) in dst.iter_mut().zip(p[lo * 4..(lo + n) * 4].chunks_exact(4)) {
                    let o = u32::from_le_bytes(c.try_into().unwrap()) as usize;
                    d.write(Datum::from_usize(base + o));
                }
                // SAFETY: dst (the first `n` spare slots) was fully written above.
                unsafe { out.set_len(out.len() + n) };
            }
        }
    }
}

#[cfg(test)]
pub(crate) fn test_part(name: &str, rg_rows: &[u32], ncols: usize) -> String {
    let path = std::env::temp_dir().join(name);
    let path = path.to_str().unwrap().to_string();
    tests::write_part_v(&path, CB_HEADER_LEN, rg_rows, ncols, CB_VERSION, &vec![1; ncols], &[]);
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn write_part_v(
        path: &str,
        footer_off: u64,
        rg_rows: &[u32],
        ncols: usize,
        version: u32,
        ndv: &[u64],
        sorted: &[u8],
    ) {
        let mut f = Vec::new();
        put_u32(&mut f, rg_rows.len() as u32);
        put_u32(&mut f, ncols as u32);
        for &n in rg_rows {
            put_u64(&mut f, 0);
            put_u32(&mut f, n);
            put_u32(&mut f, 1);
            put_u32(&mut f, 0);
            put_u32(&mut f, 0);
        }
        for _ in rg_rows {
            for _ in 0..ncols {
                put_u64(&mut f, 0);
                put_i64(&mut f, 0);
                put_i64(&mut f, 0);
            }
        }
        if version >= CB_VERSION_V2 {
            for c in 0..ncols {
                put_u64(&mut f, ndv.get(c).copied().unwrap_or(0));
            }
        }
        if version >= CB_VERSION_V4 {
            for _ in rg_rows {
                for _ in 0..ncols {
                    put_i128(&mut f, 0);
                }
            }
        }
        if version >= CB_VERSION_V5 {
            for c in 0..ncols {
                f.push(sorted.get(c).copied().unwrap_or(0));
            }
        }
        if version >= CB_VERSION_V6 {
            f.extend_from_slice(&[0u8; CB_CLUSTER_KEY_SECTION_LEN]);
        }
        let crc = crc32c(&f);
        let flen = (f.len() + 16) as u64;
        put_u64(&mut f, flen);
        put_u32(&mut f, crc);
        put_u32(&mut f, CB_FOOTER_MAGIC);

        let mut hdr = Vec::new();
        put_u64(&mut hdr, CB_MAGIC);
        put_u32(&mut hdr, version);
        put_u32(&mut hdr, ncols as u32);
        put_u64(&mut hdr, footer_off);
        put_u64(&mut hdr, 0);
        hdr.resize(CB_HEADER_LEN as usize, 0);

        let mut bytes = hdr;
        bytes.resize(footer_off.max(CB_HEADER_LEN) as usize, 0);
        if footer_off != 0 {
            bytes.extend_from_slice(&f);
        }
        std::fs::write(path, bytes).unwrap();
    }

    fn write_part(path: &str, footer_off: u64, rg_rows: &[u32], ncols: usize) {
        write_part_v(path, footer_off, rg_rows, ncols, CB_VERSION_V1, &[], &[]);
    }

    fn tmp(name: &str) -> String {
        let p = std::env::temp_dir().join(format!("cbstore-footer-{}-{}", std::process::id(), name));
        p.to_str().unwrap().to_string()
    }

    #[test]
    fn footer_rows_sums_row_groups() {
        let path = tmp("sum");
        write_part(&path, CB_HEADER_LEN, &[1_048_576, 951_421, 3], 4);
        assert_eq!(part_footer_rows(&path, 4).unwrap(), Some(2_000_000));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_rows_none_before_publish() {
        let path = tmp("nofooter");
        write_part(&path, 0, &[], 4);
        assert_eq!(part_footer_rows(&path, 4).unwrap(), None);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_rows_none_on_short_file() {
        let path = tmp("short");
        std::fs::write(&path, [0u8; 8]).unwrap();
        assert_eq!(part_footer_rows(&path, 4).unwrap(), None);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_ndv_v2_roundtrip() {
        let path = tmp("ndv2");
        write_part_v(&path, CB_HEADER_LEN, &[100, 200], 3, CB_VERSION_V2, &[7, 0, 12_345_678], &[]);
        assert_eq!(part_footer_rows(&path, 3).unwrap(), Some(300));
        assert_eq!(part_footer_ndv(&path, 3).unwrap(), Some(vec![7, 0, 12_345_678]));
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_current_reads_and_future_rejected() {
        let path = tmp("v3v4");
        write_part_v(&path, CB_HEADER_LEN, &[100], 2, CB_VERSION, &[5, 9], &[1, 0]);
        assert_eq!(part_footer_rows(&path, 2).unwrap(), Some(100));
        assert_eq!(part_footer_ndv(&path, 2).unwrap(), Some(vec![5, 9]));
        write_part_v(&path, CB_HEADER_LEN, &[100], 2, CB_VERSION + 1, &[5, 9], &[1, 0]);
        assert!(part_footer_rows(&path, 2).is_err());
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_sums_v4_roundtrip() {
        let path = tmp("sums4");
        let mut f = Vec::new();
        put_u32(&mut f, 2);
        put_u32(&mut f, 2);
        for (n, flags) in [(100u32, RG_FLAG_SUMS), (50, 0)] {
            put_u64(&mut f, 0);
            put_u32(&mut f, n);
            put_u32(&mut f, 1);
            put_u32(&mut f, flags);
            put_u32(&mut f, 0);
        }
        for _ in 0..2 * 2 {
            put_u64(&mut f, 0);
            put_i64(&mut f, 0);
            put_i64(&mut f, 0);
        }
        for _ in 0..2 {
            put_u64(&mut f, 1);
        }
        let sums = [[-(1i128 << 70), 42], [0, 0]];
        for rg in &sums {
            for &s in rg {
                put_i128(&mut f, s);
            }
        }
        let crc = crc32c(&f);
        let flen = (f.len() + 16) as u64;
        put_u64(&mut f, flen);
        put_u32(&mut f, crc);
        put_u32(&mut f, CB_FOOTER_MAGIC);
        let mut bytes = vec![0u8; CB_HEADER_LEN as usize];
        bytes.extend_from_slice(&f);
        std::fs::write(&path, &bytes).unwrap();

        let mut file = SegFile::open_rw(&path).unwrap();
        let (rgs, _, _, _, _) =
            read_footer_rgs(&mut file, CB_HEADER_LEN, 2, CB_VERSION_V4, true).unwrap();
        assert_eq!(rgs[0].flags & RG_FLAG_SUMS, RG_FLAG_SUMS);
        assert_eq!(rgs[0].sums, vec![-(1i128 << 70), 42]);
        assert_eq!(rgs[1].flags & RG_FLAG_SUMS, 0);
        assert_eq!(rgs[1].sums, vec![0, 0]);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn part_open_sums_stay_on_disk_and_rg_sum_reads_exact() {
        let path = tmp("lazysums");
        let mut f = Vec::new();
        put_u32(&mut f, 2);
        put_u32(&mut f, 2);
        for n in [100u32, 50] {
            put_u64(&mut f, 0);
            put_u32(&mut f, n);
            put_u32(&mut f, 1);
            put_u32(&mut f, RG_FLAG_FROZEN | RG_FLAG_SUMS);
            put_u32(&mut f, 0);
        }
        for _ in 0..2 * 2 {
            put_u64(&mut f, 0);
            put_i64(&mut f, 0);
            put_i64(&mut f, 0);
        }
        for _ in 0..2 {
            put_u64(&mut f, 1);
        }
        let sums = [[-(1i128 << 70), 42], [7, -9]];
        for rg in &sums {
            for &s in rg {
                put_i128(&mut f, s);
            }
        }
        // v5 sorted flags + v6 cluster-key sections (header stamps CB_VERSION).
        f.extend_from_slice(&[0u8, 0u8]);
        f.extend_from_slice(&[0u8; CB_CLUSTER_KEY_SECTION_LEN]);
        let crc = crc32c(&f);
        let flen = (f.len() + 16) as u64;
        put_u64(&mut f, flen);
        put_u32(&mut f, crc);
        put_u32(&mut f, CB_FOOTER_MAGIC);
        let mut hdr = Vec::new();
        put_u64(&mut hdr, CB_MAGIC);
        put_u32(&mut hdr, CB_VERSION);
        put_u32(&mut hdr, 2);
        put_u64(&mut hdr, CB_HEADER_LEN);
        put_u64(&mut hdr, 0);
        hdr.resize(CB_HEADER_LEN as usize, 0);
        let mut bytes = hdr;
        bytes.extend_from_slice(&f);
        std::fs::write(&path, &bytes).unwrap();

        let part = Part::open(&path, 2).unwrap().unwrap();
        for rg in 0..2 {
            assert!(part.rgs[rg].sums.is_empty());
            for col in 0..2 {
                assert_eq!(part.rg_sum(rg, col), sums[rg][col]);
            }
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn footer_v3_reads_sums_unknown() {
        let path = tmp("sums3");
        write_part_v(&path, CB_HEADER_LEN, &[100, 200], 3, CB_VERSION_V3, &[1, 1, 1], &[]);
        let mut file = SegFile::open_rw(&path).unwrap();
        let (rgs, _, _, sorted, _) =
            read_footer_rgs(&mut file, CB_HEADER_LEN, 3, CB_VERSION_V3, true).unwrap();
        assert_eq!(sorted, vec![0, 0, 0]);
        for rg in &rgs {
            assert_eq!(rg.flags & RG_FLAG_SUMS, 0);
            assert!(rg.sums.is_empty());
        }
        std::fs::remove_file(&path).unwrap();
    }

    // ---- v3 chunk sections: block zone maps + blooms ----

    fn int_chunk(vals: &[i64]) -> (Vec<u8>, u32) {
        let ngranules = vals.len().div_ceil(GRANULE_ROWS) as u32;
        let mut body = Vec::new();
        crate::writer::encode_int_chunk(&mut body, vals, ngranules, &crate::writer::test_codec_ctx());
        (body, vals.len() as u32)
    }

    #[test]
    fn block_zone_maps_roundtrip_and_boundaries() {
        // 1.5 granules of ascending values: every 1024-row block has an
        // exact tight range, last granule's unused block slots are empty.
        let n = GRANULE_ROWS + GRANULE_ROWS / 2;
        let vals: Vec<i64> = (0..n as i64).collect();
        let (body, nrows) = int_chunk(&vals);
        let cv = ChunkView::at(&body, 0, nrows);
        assert!(cv.has_block_zm());
        for g in 0..2 {
            for b in 0..BLOCKS_PER_GRANULE {
                let lo = (g * GRANULE_ROWS + b * BLOCK_ROWS) as i64;
                let (bmin, bmax) = cv.block_minmax(g, b);
                if lo >= n as i64 {
                    assert_eq!((bmin, bmax), (i64::MAX, i64::MIN));
                } else {
                    assert_eq!(bmin, lo);
                    assert_eq!(bmax, (lo + BLOCK_ROWS as i64 - 1).min(n as i64 - 1));
                }
            }
        }
        // Decode is unchanged by the new sections.
        let (mut out, mut dict, mut arena) = (Vec::new(), Vec::new(), Vec::new());
        cv.decode_granule(0, &mut out, &mut dict, &mut arena);
        assert_eq!(out.len(), GRANULE_ROWS);
        assert_eq!(out[0].as_i64(), 0);
        assert_eq!(out[GRANULE_ROWS - 1].as_i64(), GRANULE_ROWS as i64 - 1);
        cv.decode_granule(1, &mut out, &mut dict, &mut arena);
        assert_eq!(out.len(), GRANULE_ROWS / 2);
        assert_eq!(out[0].as_i64(), GRANULE_ROWS as i64);
    }

    #[test]
    fn const_chunk_has_no_sections() {
        let vals = vec![42i64; GRANULE_ROWS];
        let (body, nrows) = int_chunk(&vals);
        let cv = ChunkView::at(&body, 0, nrows);
        assert!(!cv.has_block_zm());
        assert!(!cv.has_bloom());
    }

    #[test]
    fn bloom_arming_policy() {
        // Sorted (clustered) high-NDV: granule zone maps are tight => no
        // bloom. Unclustered high-NDV: armed. Unclustered low-NDV: not.
        let sorted: Vec<i64> = (0..2 * GRANULE_ROWS as i64).collect();
        let cv_body = int_chunk(&sorted);
        assert!(!ChunkView::at(&cv_body.0, 0, cv_body.1).has_bloom());

        let unclustered: Vec<i64> = (0..2 * GRANULE_ROWS as u64)
            .map(|i| crate::hll::mix64(i) as i64)
            .collect();
        let (body, nrows) = int_chunk(&unclustered);
        let cv = ChunkView::at(&body, 0, nrows);
        assert!(cv.has_bloom());
        for (i, &v) in unclustered.iter().enumerate() {
            assert!(cv.bloom_may_contain(i / GRANULE_ROWS, v));
        }

        let lowndv: Vec<i64> =
            (0..2 * GRANULE_ROWS as u64).map(|i| (crate::hll::mix64(i) % 100) as i64).collect();
        let cv_body = int_chunk(&lowndv);
        assert!(!ChunkView::at(&cv_body.0, 0, cv_body.1).has_bloom());
    }

    #[test]
    fn bloom_prunes_absent_admits_present() {
        let vals: Vec<i64> = (0..GRANULE_ROWS as u64).map(|i| crate::hll::mix64(i) as i64).collect();
        let (body, nrows) = int_chunk(&vals);
        let cv = ChunkView::at(&body, 0, nrows);
        assert!(cv.has_bloom());
        // Values in-range for the zone map but absent: the bloom prunes the
        // overwhelming majority (fp ~5.7e-4).
        let mut pruned = 0;
        for i in 0..10_000u64 {
            if !cv.bloom_may_contain(0, crate::hll::mix64(u64::MAX - i) as i64) {
                pruned += 1;
            }
        }
        assert!(pruned >= 9_900, "pruned {pruned}/10000");
    }

    #[test]
    fn footer_ndv_none_on_v1() {
        let path = tmp("ndv1");
        write_part(&path, CB_HEADER_LEN, &[100], 2);
        assert_eq!(part_footer_rows(&path, 2).unwrap(), Some(100));
        assert_eq!(part_footer_ndv(&path, 2).unwrap(), None);
        std::fs::remove_file(&path).unwrap();
    }

    // ---- decode-kernel differential tests: chunk-edge, partial-final-granule
    // coverage for the FOR/Raw/dict-codes widen loops touched by simd-decode.
    // Expected values are computed independently of decode_granule's arms.

    fn view<'a>(part: &'a [u8], hdr: ChunkHeader, nrows: u32) -> ChunkView<'a> {
        ChunkView { part, hdr, gdir_off: 0, blockzm_off: 0, bloom_off: 0, payload_off: 0, nrows }
    }

    fn for_payload(nrows: usize, width: u8) -> (Vec<u8>, Vec<i64>) {
        let mut bytes = Vec::new();
        let mut expected = Vec::with_capacity(nrows);
        for i in 0..nrows {
            let v = ((i as u64).wrapping_mul(2654435761) & 0xFFFF_FFFF) as u64;
            match width {
                1 => {
                    let b = (v & 0xFF) as u8;
                    bytes.push(b);
                    expected.push(b as i64);
                }
                2 => {
                    let b = (v & 0xFFFF) as u16;
                    bytes.extend_from_slice(&b.to_le_bytes());
                    expected.push(b as i64);
                }
                4 => {
                    let b = (v & 0xFFFF_FFFF) as u32;
                    bytes.extend_from_slice(&b.to_le_bytes());
                    expected.push(b as i64);
                }
                w => panic!("bad width {w}"),
            }
        }
        (bytes, expected)
    }

    // Drives decode_granule across every granule of a chunk with `nrows`
    // rows (deliberately not a multiple of GRANULE_ROWS to cover the
    // partial-final-granule boundary), returning the concatenated i64s.
    fn decode_all_for(nrows: usize, base: i64, width: u8) -> (Vec<i64>, Vec<i64>) {
        let (payload, deltas) = for_payload(nrows, width);
        let hdr = ChunkHeader {
            encoding: Encoding::For,
            width,
            flags: 0,
            ngranules: (nrows.div_ceil(GRANULE_ROWS)) as u32,
            aux: base,
            payload_len: payload.len() as u64,
            codec: Codec::None,
        };
        let cv = view(&payload, hdr, nrows as u32);
        let mut out = Vec::new();
        let mut dict = Vec::new();
        let mut arena = Vec::new();
        let mut got = Vec::new();
        for g in 0..cv.hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            got.extend(out.iter().map(|d| d.as_i64()));
        }
        let expected: Vec<i64> = deltas.iter().map(|d| base + d).collect();
        (got, expected)
    }

    #[test]
    fn for_widen_exact_granule_boundary() {
        for &width in &[1u8, 2, 4] {
            let (got, expected) = decode_all_for(GRANULE_ROWS * 2, 1_000, width);
            assert_eq!(got, expected, "FOR width {width} exact boundary");
        }
    }

    #[test]
    fn for_widen_partial_final_granule() {
        for &width in &[1u8, 2, 4] {
            let (got, expected) = decode_all_for(GRANULE_ROWS * 2 + 37, -500, width);
            assert_eq!(got, expected, "FOR width {width} partial final granule");
        }
    }

    #[test]
    fn for_widen_single_partial_granule() {
        for &width in &[1u8, 2, 4] {
            let (got, expected) = decode_all_for(1, 0, width);
            assert_eq!(got, expected, "FOR width {width} single row");
        }
    }

    #[test]
    fn raw_widen_matches_le_i64() {
        let nrows = GRANULE_ROWS + 5;
        let mut payload = Vec::new();
        let mut expected = Vec::with_capacity(nrows);
        for i in 0..nrows {
            let v = (i as i64) * 3 - 7;
            payload.extend_from_slice(&v.to_le_bytes());
            expected.push(v);
        }
        let hdr = ChunkHeader {
            encoding: Encoding::Raw,
            width: 8,
            flags: 0,
            ngranules: nrows.div_ceil(GRANULE_ROWS) as u32,
            aux: 0,
            payload_len: payload.len() as u64,
            codec: Codec::None,
        };
        let cv = view(&payload, hdr, nrows as u32);
        let mut out = Vec::new();
        let mut dict = Vec::new();
        let mut arena = Vec::new();
        let mut got = Vec::new();
        for g in 0..cv.hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            got.extend(out.iter().map(|d| d.as_i64()));
        }
        assert_eq!(got, expected);
    }

    #[test]
    fn dict_codes_widen_matches_raw_codes() {
        for &width in &[1u8, 2, 4] {
            let nrows = GRANULE_ROWS * 2 + 11;
            let ndv = 5usize;
            let mut payload = Vec::new();
            let mut expected = Vec::with_capacity(nrows);
            for i in 0..nrows {
                let code = (i % ndv) as u32;
                match width {
                    1 => payload.push(code as u8),
                    2 => payload.extend_from_slice(&(code as u16).to_le_bytes()),
                    4 => payload.extend_from_slice(&code.to_le_bytes()),
                    w => panic!("bad width {w}"),
                }
                expected.push(code);
            }
            let codes_len = align4(nrows * width as usize);
            payload.resize(codes_len, 0);
            // Minimal valid off_tab + blob so build_dict doesn't fault; the
            // codes-only path never reads dict values.
            for _ in 0..ndv {
                put_u32(&mut payload, 0);
            }
            payload.extend_from_slice(&0u64.to_le_bytes());

            let hdr = ChunkHeader {
                encoding: Encoding::Dict,
                width,
                flags: 0,
                ngranules: nrows.div_ceil(GRANULE_ROWS) as u32,
                aux: ndv as i64,
                payload_len: payload.len() as u64,
                codec: Codec::None,
            };
            let cv = view(&payload, hdr, nrows as u32);
            let mut codes = Vec::new();
            let mut dict = Vec::new();
            let mut arena = Vec::new();
            let mut got = Vec::new();
            for g in 0..cv.hdr.ngranules as usize {
                assert!(cv.decode_granule_codes(g, &mut codes, &mut dict, &mut arena));
                got.extend_from_slice(&codes);
            }
            assert_eq!(got, expected, "dict codes width {width}");
        }
    }

    #[test]
    fn rawtext_offsets_walk_partial_final_granule() {
        let nrows = GRANULE_ROWS + 41;
        let mut payload = Vec::new();
        let mut offs = Vec::with_capacity(nrows);
        let mut o = 0u32;
        for i in 0..nrows {
            put_u32(&mut payload, o);
            offs.push(o);
            o += 8 + (i % 7) as u32;
        }
        payload.resize(payload.len() + o as usize, 0x42);
        let blob_base = payload[nrows * 4..].as_ptr() as usize;
        let hdr = ChunkHeader {
            encoding: Encoding::RawText,
            width: 4,
            flags: 0,
            ngranules: nrows.div_ceil(GRANULE_ROWS) as u32,
            aux: 0,
            payload_len: payload.len() as u64,
            codec: Codec::None,
        };
        let cv = ChunkView { part: &payload, hdr, gdir_off: 0, blockzm_off: 0, bloom_off: 0, payload_off: 0, nrows: nrows as u32 };
        let mut out = Vec::new();
        let mut dict = Vec::new();
        let mut arena = Vec::new();
        let mut got = Vec::new();
        for g in 0..cv.hdr.ngranules as usize {
            cv.decode_granule(g, &mut out, &mut dict, &mut arena);
            got.extend(out.iter().map(|d| d.as_usize()));
        }
        let expected: Vec<usize> = offs.iter().map(|&off| blob_base + off as usize).collect();
        assert_eq!(got, expected);
    }
}
