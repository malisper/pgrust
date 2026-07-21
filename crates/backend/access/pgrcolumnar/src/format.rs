//! On-disk framing for pgrcolumnar parts (docs/design/pgrcolumnar-impl.md §1-2).

use ::types_core::Oid;

pub const CB_MAGIC: u64 = 0x6362_7374_6f72_6531; // "cbstore1"
pub const CB_VERSION_V1: u32 = 1;
// v2 appends a per-column NDV section (ncols x u64, 0 = unknown) between the
// chunk directory and the footer tail; v1 parts read as NDV-unknown.
pub const CB_VERSION_V2: u32 = 2;
// v3 chunks may carry per-block zone maps and per-granule bloom filters
// between the granule directory and the payload, gated per chunk by
// CHUNK_FLAG_*; the footer layout is unchanged from v2. v1/v2 chunks always
// have flags 0, so mixed parts (reopen-append) read correctly per chunk.
pub const CB_VERSION_V3: u32 = 3;
// v4 appends a per-RG per-column i128 SUM section (nrgs x ncols x 16B LE,
// text columns 0) after the NDV section; validity is per-RG via
// RG_FLAG_SUMS (0 is a valid sum, so NDV's 0-encodes-unknown does not
// port). v<=3 parts and their preserved RGs read as sums-unknown.
pub const CB_VERSION_V4: u32 = 4;
// v5 appends a per-column sorted flag section (ncols x u8) after the sums
// section: 1 = every row of the part is non-decreasing in this column under
// the column class's order (ints/date/timestamp: signed value; text: payload
// memcmp), 0 = unknown. Append onto committed RGs invalidates to unknown
// (the NDV precedent). v<=4 parts read as all-unknown.
pub const CB_VERSION_V5: u32 = 5;
// v6 (the codec-menu + cluster-key format):
//  - ChunkHeader byte 24 (previously reserved-zero) is a per-chunk codec tag
//    (see Codec). v<=5 chunks read as Codec::None, under which Lz4Text /
//    Lz4Dict frames keep their historical LZ4 meaning — old banks stay
//    readable chunk-for-chunk.
//  - For/Raw chunks with codec != None store per-granule compressed frames
//    (u32 raw_len | u32 comp_len | bytes, align4) at the granule directory's
//    payload_off instead of the plain g*GRANULE_ROWS*width array.
//  - Lz4Text / Lz4Dict with codec == Zstd carry ZSTD frames in the identical
//    framing (the encoding names keep their shape meaning; the codec tag
//    picks the compressor).
//  - The footer appends a fixed-size cluster-key section after the v5
//    sorted flags: u16 nkeys | CB_CLUSTER_KEY_MAX_COLS x u16 zero-based
//    column indices (slots past nkeys are 0; nkeys = 0: none declared).
//    Fixed-width because the footer parse sizes its read before it sees the
//    body. v<=5 parts read as no-cluster-key.
//  - RG_FLAG_CLUSTERED marks an RG whose rows were sorted by the declared
//    cluster key at write time (per-RG sortedness; whole-part order is the
//    v5 sorted flags' business).
pub const CB_VERSION_V6: u32 = 6;
// v7 — the train-9 format-group union (one version bump carrying three
// additive extensions; per-lane v7 layouts never shipped):
//
// (a) Per-granule string-length statistics (the ORC StringStatistics.sum
//     precedent):
//  - The footer prelude grows from 8 to 8 + ncols bytes: after nrgs/ncols,
//    ncols u8 flags mark the columns carrying length-stats entries (the
//    writer flags every text column). Fixed-position because read_footer_rgs
//    sizes the body read before it parses anything, and ncols is known from
//    the caller — one read still covers the prelude.
//  - Validity is per-RG via RG_FLAG_LENSTATS (0 is a valid sum, the
//    RG_FLAG_SUMS precedent): RGs preserved from a v<=6 footer write zeros
//    and lack the flag. v<=6 parts read as no-length-stats entirely.
//
// (b) Global stitched dictionaries: a stitch blob is a per-RG per-column u32
//     array mapping the RG's LOCAL dict codes to PART-GLOBAL codes; global
//     codes are byte-rank over the union of every RG's dict entries, so
//     global-code order == byte order (the CHUNK_FLAG_DICT_SORTED property
//     lifted to part scope). Blobs live in the file body between the last RG
//     and the footer (64-aligned; dead on append-refinalize, the
//     footer-chain precedent). A column gets a stitch only when EVERY RG's
//     chunk is Dict/Lz4Dict with sorted codes; append onto committed RGs
//     invalidates to none (the NDV/sorted precedent). v<=6 parts read as
//     no-stitch.
//
// (c) Per-granule zero/empty counts: int/date/timestamp columns count
//     stored value == 0, text columns count empty strings. Validity is
//     per-RG via RG_FLAG_ZEROCNT; preserved v<=6 RGs write zeros flagless.
//     v<=6 parts read as no-zerocnt entirely.
//
// Footer sections AFTER the v6 cluster-key section, in this order (parse
// order == write order; every length closed-form in (nrgs, ncols, prelude
// lenstats flags) so read_footer_rgs can size the body read before parsing):
//  1. length-stats body: nrgs x GRANULES_PER_RG x nlencols entries of
//     CB_LENSTATS_ENTRY_LEN bytes (sum(octet_length) u64 | non-null count
//     u32 | empty-string count u32, LE), rg-major, then granule slot, then
//     flagged column in ascending column order. Granule slots past an RG's
//     last row are zero. nlencols = number of set prelude flag bytes.
//  2. stitch section: ncols x u64 global NDV (0 = no stitch for the
//     column), then nrgs x ncols x (u64 file_off | u32 count) stitch-blob
//     directory (all-zero where absent).
//  3. zero-count body: nrgs x GRANULES_PER_RG x ncols u32 entries, rg-major,
//     then granule slot, then column ascending; slots past an RG's last row
//     are zero.
pub const CB_VERSION_V7: u32 = 7;
// v8 — NDV register persistence (the append-invalidation fix): the v2
// scalar NDV section keeps only the whole-part HLL ESTIMATE, so a writer
// reopening a nonempty part could not continue the sketch and invalidated
// every column to 0 — any part built by more than one COPY permanently lost
// footer NDV. v8 persists the per-column HLL REGISTER images so reopen
// loads-and-continues (registers are per-bucket maxima: continue ==
// recompute exactly).
//  - Register blobs live in the file body ahead of the footer (64-aligned;
//    dead on append-refinalize — the stitch-blob precedent), one blob per
//    column, encoded per hll.rs to_blob (dense or sparse; low-NDV columns
//    are tens of bytes).
//  - The footer appends a per-column blob directory after the v7
//    zero-count body: ncols x (u64 file_off | u32 len), all-zero where
//    absent (pre-v8 append chains, or the lane kill switch
//    PGRUST_CBSTORE_NDV_REGS=0). Fixed-width so read_footer_rgs still
//    sizes the body read closed-form; the register bytes themselves are
//    outside the footer body (and its CRC — the stitch-blob trust model).
//  - The v2 scalar section is still written (derived from the registers,
//    or exact from the v7 stitch gndv where a global dict exists): every
//    scalar consumer (part cache, ANALYZE, planner fallback) reads v8
//    parts unchanged. Consumption ladder: v8 registers -> v2 scalar ->
//    sampled estimate; strictly monotone, v<=7 parts lose nothing.
pub const CB_VERSION_V8: u32 = 8;
pub const CB_VERSION: u32 = 8;
// v8 NDV-registers directory entry: u64 file_off | u32 len.
pub const CB_NDVREGS_DIR_ENTRY_LEN: usize = 12;
// Stitch directory entry: u64 file_off | u32 count.
pub const CB_STITCH_DIR_ENTRY_LEN: usize = 12;
pub const CB_CLUSTER_KEY_MAX_COLS: usize = 8;
pub const CB_CLUSTER_KEY_SECTION_LEN: usize = 2 + CB_CLUSTER_KEY_MAX_COLS * 2;
// v7 length-stats entry: sum u64 | nonnull u32 | empty u32. A granule sum is
// bounded by GRANULE_ROWS x 2^30 (the varlena payload cap) < 2^44, and a
// whole part's fold by 2^44 x nrgs — i128 accumulation can never overflow.
pub const CB_LENSTATS_ENTRY_LEN: usize = 16;
pub const CB_HEADER_LEN: u64 = 64;
pub const CB_RG_MAGIC: u32 = 0x4342_5247; // "CBRG"
pub const CB_RG_HEADER_LEN: usize = 32;
pub const CB_CHUNK_HEADER_LEN: usize = 32;
pub const CB_GRANULE_ENTRY_LEN: usize = 24;
pub const CB_FOOTER_MAGIC: u32 = 0x4342_4654; // "CBFT"

pub const RG_ROWS: usize = 65_536;
pub const GRANULE_ROWS: usize = 8_192;
// v7 length-stats / zero-count granule slots per RG (fixed — partial RGs
// zero-pad).
pub const GRANULES_PER_RG: usize = RG_ROWS / GRANULE_ROWS;
// v7 zero-count entry: one u32 per (rg, granule slot, column). GRANULE_ROWS
// = 2^13 bounds every count.
pub const CB_ZEROCNT_ENTRY_LEN: usize = 4;
// Executor window: divides GRANULE_ROWS exactly and fits SOA_MAX_ROWS (291).
pub const WINDOW_ROWS: usize = 256;
pub const WINDOWS_PER_GRANULE: usize = GRANULE_ROWS / WINDOW_ROWS;
// Sub-granule zone-map block: divides GRANULE_ROWS and is a WINDOW_ROWS
// multiple, so a pruned block skips whole windows.
pub const BLOCK_ROWS: usize = 1_024;
pub const BLOCKS_PER_GRANULE: usize = GRANULE_ROWS / BLOCK_ROWS;
pub const WINDOWS_PER_BLOCK: usize = BLOCK_ROWS / WINDOW_ROWS;
pub const CB_BLOCK_ZM_ENTRY_LEN: usize = 16;

pub const RG_FLAG_FROZEN: u32 = 1;
// Footer sums exact for this RG (sealed by a v4+ writer). |v| <= 2^63 and
// RG_ROWS = 2^16 bound a per-RG |sum| by 2^79; folding <= 2^32 such RGs
// stays under 2^127, so i128 sum accumulation can never overflow.
pub const RG_FLAG_SUMS: u32 = 2;
// v6: the RG's rows are non-decreasing under the part's declared cluster key
// (footer cluster-key section) using the writer's column-class order
// (ints/date/timestamp: signed value; text: payload memcmp).
pub const RG_FLAG_CLUSTERED: u32 = 4;
// v7: the RG's per-granule length-stats entries are exact (sealed by a v7+
// writer). Like RG_FLAG_SUMS, needed because every field of an entry can
// legitimately be 0.
pub const RG_FLAG_LENSTATS: u32 = 8;
// v7: the RG's per-granule zero/empty counts are exact (sealed by a v7+
// writer). Like RG_FLAG_SUMS, needed because 0 is a legitimate count.
pub const RG_FLAG_ZEROCNT: u32 = 16;

// Chunk sections between the granule directory and the payload, in this
// order. Block zone maps: ngranules x BLOCKS_PER_GRANULE x (min i64, max
// i64); slots past the chunk's last row hold (i64::MAX, i64::MIN). Blooms:
// ngranules x BLOOM_BYTES (crate::bloom).
pub const CHUNK_FLAG_BLOCK_ZM: u16 = 1;
pub const CHUNK_FLAG_BLOOM: u16 = 2;

// ChunkHeader.flags bit: Dict/Lz4Dict entries are stored byte-sorted (codes
// are rank order). Parts written before this bit keep first-appearance code
// order; readers must gate rank-order tricks (dict range predicates) on it.
// Bit 4, not 1: bits 1/2 are BLOCK_ZM/BLOOM (the compdomain branch allocated
// bit 1 on a base without chunk sections; colliding with BLOCK_ZM skewed
// ChunkView::at section offsets on every sorted-dict chunk).
pub const CHUNK_FLAG_DICT_SORTED: u16 = 4;

// ChunkHeader.flags bit: the Lz4Dict blob region is SUB-FRAMED — split at
// entry boundaries (byte-sorted order UNCHANGED; codes stay rank order)
// into independently decompressible frames, so a reader can materialize
// only the frames containing requested codes (the survivor gather/store
// path) instead of the whole arena. The blob region after the dict_off
// table replaces the single `u32 raw_len | u32 comp_len | bytes` frame
// with:
//   u32 nframes
//   nframes x (u32 raw_len | u32 comp_len)   -- frame directory
//   frame bytes back-to-back (comp_len == raw_len marks a stored-raw
//   frame), region 4-padded at the end
// `dict_off` offsets stay offsets into the CONCATENATED decompressed blob
// (frame f covers raw [sum raw_len[..f], +raw_len[f])); an entry never
// straddles a frame edge (the writer cuts at entry boundaries only). Like
// the DeltaFor precedent this is flag-versioning, not a CB_VERSION bump:
// frozen banks are never rewritten and new banks get new keys, so no
// pre-frames reader ever sees the bit. Write-side arm:
// PGRUST_CBSTORE_DICT_FRAMES=1 (default off).
pub const CHUNK_FLAG_DICT_FRAMED: u16 = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Encoding {
    Raw = 0,
    For = 1,
    Const = 2,
    Dict = 3,
    RawText = 4,
    // RAWTEXT shape with the varlena-image blob LZ4-compressed per granule;
    // offsets stay plain but are granule-relative (see writer §Lz4Text).
    Lz4Text = 5,
    // DICT shape with the dict blob LZ4-compressed as one frame; codes and
    // the dict_off table stay plain (the per-row gather is untouched — the
    // blob is decompressed once per RG when the dict table is built).
    Lz4Dict = 6,
    // Int shape, delta-transformed (intcodec lane). Per-granule frames
    // (u32 raw_len | u32 comp_len | image, align4) at the granule
    // directory's payload_off — ALWAYS framed, even under Codec::None
    // (comp_len == raw_len, image stored in place). The raw image is
    //   [0..8)   first row value (i64 LE)
    //   [8..16)  delta FOR base dmin (i64 LE, wrapping domain)
    //   [16]     delta byte width in {0,1,2,4,8}; 0 = every delta == dmin
    //   [17..24) zero pad
    //   [24..)   (d[i] -w dmin) at width, i in 1..n (n-1 entries), where
    //            d[i] = v[i] -w v[i-1] (wrapping i64 arithmetic throughout)
    // Zone maps / block ZMs / blooms / footer sums are computed from the
    // plain values exactly as For/Raw — value-domain metadata is untouched
    // by the payload transform. Like the RawText/Lz4Text reuse precedent
    // this is an encoding tag, NOT a version bump: frozen banks are never
    // rewritten and new banks get new keys, so no pre-intcodec reader ever
    // sees the tag. ChunkHeader.aux = chunk min (as For); width = 0.
    DeltaFor = 7,
}

impl Encoding {
    pub fn from_u8(v: u8) -> Option<Encoding> {
        Some(match v {
            0 => Encoding::Raw,
            1 => Encoding::For,
            2 => Encoding::Const,
            3 => Encoding::Dict,
            4 => Encoding::RawText,
            5 => Encoding::Lz4Text,
            6 => Encoding::Lz4Dict,
            7 => Encoding::DeltaFor,
            _ => return None,
        })
    }
}

// v6 per-chunk codec tag (ChunkHeader byte 24). None on v<=5 chunks, under
// which Lz4Text/Lz4Dict payloads are LZ4 (their historical meaning) and all
// other encodings are uncompressed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Codec {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl Codec {
    pub fn from_u8(v: u8) -> Option<Codec> {
        Some(match v {
            0 => Codec::None,
            1 => Codec::Lz4,
            2 => Codec::Zstd,
            _ => return None,
        })
    }
}

// Logical column class, gated at CREATE TABLE (impl doc §3).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColType {
    I16,
    I32,
    I64,
    Date,
    Timestamp,
    Text,
}

impl ColType {
    pub fn of_type_oid(atttypid: Oid) -> Option<ColType> {
        use ::types_core::catalog::*;
        Some(match atttypid {
            INT2OID => ColType::I16,
            INT4OID => ColType::I32,
            INT8OID => ColType::I64,
            DATEOID => ColType::Date,
            TIMESTAMPOID => ColType::Timestamp,
            TEXTOID | VARCHAROID | BPCHAROID => ColType::Text,
            _ => return None,
        })
    }

    pub fn is_text(self) -> bool {
        matches!(self, ColType::Text)
    }
}

pub fn align64(off: u64) -> u64 {
    (off + 63) & !63
}

pub fn align4(off: usize) -> usize {
    (off + 3) & !3
}

// ---- little helpers over byte images (all fields little-endian) ----

pub fn get_u32(b: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(b[off..off + 4].try_into().unwrap())
}

pub fn get_u64(b: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

pub fn get_i64(b: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(b[off..off + 8].try_into().unwrap())
}

pub fn put_u32(b: &mut Vec<u8>, v: u32) {
    b.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u64(b: &mut Vec<u8>, v: u64) {
    b.extend_from_slice(&v.to_le_bytes());
}

pub fn put_i64(b: &mut Vec<u8>, v: i64) {
    b.extend_from_slice(&v.to_le_bytes());
}

pub fn get_i128(b: &[u8], off: usize) -> i128 {
    i128::from_le_bytes(b[off..off + 16].try_into().unwrap())
}

pub fn put_i128(b: &mut Vec<u8>, v: i128) {
    b.extend_from_slice(&v.to_le_bytes());
}

pub struct ChunkHeader {
    pub encoding: Encoding,
    pub width: u8,
    pub flags: u16,
    pub ngranules: u32,
    // FOR base / CONST value / DICT ndv / RawText 0.
    pub aux: i64,
    pub payload_len: u64,
    // v6 codec tag (byte 24); v<=5 chunks wrote 0 there, so they decode as
    // Codec::None with the legacy Lz4Text/Lz4Dict-means-LZ4 reading.
    pub codec: Codec,
}

impl ChunkHeader {
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.push(self.encoding as u8);
        out.push(self.width);
        out.extend_from_slice(&self.flags.to_le_bytes());
        put_u32(out, self.ngranules);
        put_i64(out, self.aux);
        put_u64(out, self.payload_len);
        out.push(self.codec as u8);
        out.extend_from_slice(&[0u8; 7]);
    }

    pub fn decode(b: &[u8]) -> ChunkHeader {
        ChunkHeader {
            encoding: Encoding::from_u8(b[0]).expect("cbstore: bad chunk encoding"),
            width: b[1],
            flags: u16::from_le_bytes(b[2..4].try_into().unwrap()),
            ngranules: get_u32(b, 4),
            aux: get_i64(b, 8),
            payload_len: get_u64(b, 16),
            codec: Codec::from_u8(b[24]).expect("cbstore: bad chunk codec"),
        }
    }

    // The codec the payload's compressed frames actually use: v<=5 chunks
    // carry Codec::None but their Lz4Text/Lz4Dict frames are LZ4.
    pub fn frame_codec(&self) -> Codec {
        match (self.codec, self.encoding) {
            (Codec::None, Encoding::Lz4Text | Encoding::Lz4Dict) => Codec::Lz4,
            (c, _) => c,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GranuleEntry {
    pub payload_off: u64,
    pub min: i64,
    pub max: i64,
}

pub fn schema_fingerprint(cols: &[(Oid, i16)]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &(oid, len) in cols {
        h = h.wrapping_mul(0x100_0000_01b3) ^ (oid as u64);
        h = h.wrapping_mul(0x100_0000_01b3) ^ (len as u16 as u64);
    }
    h
}

// Ran on every Part open over the whole footer (~24B x rgs x cols) until
// the sectioned lazy read (reader::read_footer_rgs): reader opens now skip
// the whole-body CRC; this remains on the writer's reopen-append read and
// the PGRUST_CBSTORE_FOOTER_EAGER=1 path. History: bitwise-per-bit here put
// ~16ms on a 100M-row 105-col table's full-scan count.
pub fn crc32c(bytes: &[u8]) -> u32 {
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("crc") {
        // SAFETY: crc feature presence checked above.
        return unsafe { crc32c_hw(bytes) };
    }
    crc32c_sw(bytes)
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
unsafe fn crc32c_hw(bytes: &[u8]) -> u32 {
    use core::arch::aarch64::{__crc32cb, __crc32cd};
    let mut crc: u32 = !0;
    let mut chunks = bytes.chunks_exact(8);
    for c in chunks.by_ref() {
        crc = __crc32cd(crc, u64::from_le_bytes(c.try_into().unwrap()));
    }
    for &b in chunks.remainder() {
        crc = __crc32cb(crc, b);
    }
    !crc
}

const fn crc32c_table() -> [u32; 256] {
    let mut t = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut k = 0;
        while k < 8 {
            c = (c >> 1) ^ (0x82F6_3B78 & (0u32.wrapping_sub(c & 1)));
            k += 1;
        }
        t[i] = c;
        i += 1;
    }
    t
}

static CRC32C_TABLE: [u32; 256] = crc32c_table();

fn crc32c_sw(bytes: &[u8]) -> u32 {
    let mut crc: u32 = !0;
    for &b in bytes {
        crc = (crc >> 8) ^ CRC32C_TABLE[((crc ^ b as u32) & 0xff) as usize];
    }
    !crc
}

#[cfg(test)]
mod crc_tests {
    // The hw/table paths must reproduce the original bitwise CRC32C exactly
    // (on-disk footers were written with it).
    #[test]
    fn crc32c_matches_bitwise() {
        let bitwise = |bytes: &[u8]| -> u32 {
            let mut crc: u32 = !0;
            for &b in bytes {
                crc ^= b as u32;
                for _ in 0..8 {
                    crc = (crc >> 1) ^ (0x82F6_3B78 & (0u32.wrapping_sub(crc & 1)));
                }
            }
            !crc
        };
        let data: Vec<u8> =
            (0..8193u32).map(|i| (i.wrapping_mul(2654435761) >> 13) as u8).collect();
        for len in (0..64).chain([100, 1024, 4096, 8191, 8192, 8193]) {
            assert_eq!(super::crc32c(&data[..len]), bitwise(&data[..len]), "len {len}");
        }
    }
}
