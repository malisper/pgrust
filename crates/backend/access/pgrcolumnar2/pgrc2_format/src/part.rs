//! Part file structures (spec §5–§6): header, footer, section table, stream
//! directory, stream extents, and the section writer every codec emits
//! through. Wire order == declaration order (all structs padding-free
//! repr(C), layout-pinned in `tests/layout.rs`); all integers LE.

use crate::enc::Wrapper;
use crate::wire::{crc32c, put_bytes, put_u16, put_u32, put_u64, put_u8, Cur};
use crate::{FormatError, FormatResult, FORMAT_VERSION};

/// Part-file header magic: ASCII "PGRC2FMT".
pub const PART_MAGIC: u64 = u64::from_le_bytes(*b"PGRC2FMT");
/// FooterFixed magic: ASCII "PRCF".
pub const FOOTER_MAGIC: u32 = u32::from_le_bytes(*b"PRCF");
/// PartTail magic: ASCII "PRC2".
pub const TAIL_MAGIC: u32 = u32::from_le_bytes(*b"PRC2");
/// StreamSectionHdr magic: ASCII "PRCX".
pub const STREAM_SECTION_MAGIC: u32 = u32::from_le_bytes(*b"PRCX");

/// The varlena-slot overflow marker (spec §6.7): never a valid 4B-U varlena
/// header (its low 2 bits are set).
pub const OVERFLOW_MARK: u32 = 0xFFFF_FFFF;

// ---------------------------------------------------------------------------
// section kinds (spec §5.6)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum SectionKind {
    StreamDir = 1,
    Stream = 2,
    PathTable = 3,
    Stats = 4,
    Psma = 5,
    Bloom = 6,
    NdvRegisters = 7,
    SortKey = 8,
    SidecarDir = 9,
    /// pgrc2.1 §2.1 (spec §8.5): flat SoA stats arrays — the §8.1 facts
    /// transposed per (column, stat-kind), indexed by granule ordinal, +1
    /// part rollup entry. OPTIONAL + always raw (mmap-cast law). Layout in
    /// `meta::flatstats_encode`. Readers fall back to §8.1 when absent.
    FlatStats = 10,
    /// pgrc2.1 §2.2 (spec §8.6): the per-(column, part) exact digest —
    /// exact NDV (the dict entry count, free at seal when the part is
    /// dict-encoded) + exact zero_count. OPTIONAL + raw; 24 B fixed.
    /// Layout in `meta::PartDigest`. Readers fall back to the §8.1 part
    /// record's `ndv_est` when absent or when the NDV_EXACT flag is unset.
    PartDigest = 11,
}

impl SectionKind {
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
    pub fn from_u16(v: u16) -> FormatResult<SectionKind> {
        match v {
            1 => Ok(SectionKind::StreamDir),
            2 => Ok(SectionKind::Stream),
            3 => Ok(SectionKind::PathTable),
            4 => Ok(SectionKind::Stats),
            5 => Ok(SectionKind::Psma),
            6 => Ok(SectionKind::Bloom),
            7 => Ok(SectionKind::NdvRegisters),
            8 => Ok(SectionKind::SortKey),
            9 => Ok(SectionKind::SidecarDir),
            10 => Ok(SectionKind::FlatStats),
            11 => Ok(SectionKind::PartDigest),
            _ => Err(FormatError::UnknownSectionKind { kind: v }),
        }
    }
}

/// SectionEntry flag: readers may skip an unknown kind carrying this flag
/// (forward compatibility); an unknown kind WITHOUT it is a typed refusal.
pub const SECTION_OPTIONAL: u16 = 1 << 0;

/// SectionEntry flag (SB-6, CMP-F): the section BODY is zstd-wrapped in the
/// meta-plane envelope `[raw_len: u32 LE][zstd frame]`. Applies to the
/// meta-section class (Stats/Psma/Bloom/NdvRegisters — the census's
/// measured ~2.1GB-at-100m plane), written iff the wrap clears the SB-2
/// ≥20% gate; `SectionEntry.crc` stays over the STORED (wrapped) bytes.
/// Consumers unwrap through `pgrc2_codec::wrapper::meta_unwrap_body`; a
/// flagged section handed to a locator raw is a typed-refusal shape (the
/// envelope never parses as the section's own body).
pub const SECTIONF_META_ZSTD: u16 = 1 << 1;

// ---------------------------------------------------------------------------
// stream roles + flags (spec §6.1/§6.3)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum StreamRole {
    Values = 0,
    Validity = 1,
    Sizes = 2,
    ChildValues = 3,
    DictIndex = 4,
    DictPayload = 5,
    Overflow = 6,
}

/// RESERVED role ids (spec §6.1, amendment M3-A2): the O-4 shred-elision
/// vocabulary, frozen now so role ids in fingerprinted keys cannot move.
/// `StreamRole::from_u8` deliberately REFUSES them until their activation
/// amendment — no writer may emit them at M3 (a part carrying them refuses
/// at open), and adding them to the enum would silently admit them.
pub const STREAM_ROLE_SHRED_EXCEPTION_MASK_RESERVED: u8 = 7;
/// See [`STREAM_ROLE_SHRED_EXCEPTION_MASK_RESERVED`].
pub const STREAM_ROLE_SHRED_RESIDUAL_RESERVED: u8 = 8;

impl StreamRole {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
    pub fn from_u8(v: u8) -> FormatResult<StreamRole> {
        match v {
            0 => Ok(StreamRole::Values),
            1 => Ok(StreamRole::Validity),
            2 => Ok(StreamRole::Sizes),
            3 => Ok(StreamRole::ChildValues),
            4 => Ok(StreamRole::DictIndex),
            5 => Ok(StreamRole::DictPayload),
            6 => Ok(StreamRole::Overflow),
            _ => Err(FormatError::UnknownStreamRole { role: v }),
        }
    }
}

/// Stream flags (spec §6.3).
pub const STREAMF_SIGNED: u16 = 1 << 0;
/// code-eq == value-eq holds; execution lanes may consume codes (spec §7).
pub const STREAMF_DICT_EXEC: u16 = 1 << 1;
/// The column has an Overflow stream and value slots may carry OverflowRefs.
pub const STREAMF_HAS_OVERFLOW: u16 = 1 << 2;
/// DictIndex only (M5d.char-len-form, the S8 §2(a) delta form): the index
/// entries' third field stores `byte_len − char_len` instead of absolute
/// `char_len`. Zero for every pure-ASCII (and every BytesOnly) entry, so
/// the wrapped DictIndex stream flattens to near-nothing under zstd on
/// ASCII-dominant corpora — the S8-measured +62.2MB char_len entropy class
/// dies while `char_len` STAYS a seal-time verified fact (reconstructed
/// exactly at decode; `dict_entry`/`DictHandle::lengths` return absolute
/// char_len either way). Self-describing per stream: readers consult this
/// flag, never a writer posture.
pub const STREAMF_CHARLEN_DELTA: u16 = 1 << 3;
/// DictIndex only (M5d Option-C / S8 §2(b), the lengths-prepass probe):
/// the index entries' third field stores NOTHING (zeros — ~free under the
/// wrapper). `char_len` is recomputed from the entry bytes: at the decode
/// funnels (`dict_entry` counts UTF-8 lead bytes over the payload in
/// hand) and at the reader's length face (`DictHandle::lengths` builds a
/// load-time one-pass per-code table — the index-only/no-payload-fault
/// contract is DEVIATED under this form, documented there). Footer
/// char_len_min/max are untouched (value-fold facts, never index-derived).
/// Probe-only form (`PGRUST_FSST_OPTC_PROBE`): no blessed bank carries it.
pub const STREAMF_CHARLEN_ABSENT: u16 = 1 << 4;
/// The Values entry's `aux32` carries a shredded NUMERIC lane's
/// chunk-shared decimal scale (JSON routing, RULED 2026-08-14: the
/// ArrayDual-aux32 precedent). Set ONLY on `path_ord >= 1` byval lane
/// streams sealed since the ruling; flag-absent numeric lanes are
/// scale-unknowable (pre-ruling parts) and readers refuse them typed.
pub const STREAMF_LANE_SCALE: u16 = 1 << 5;

// ---------------------------------------------------------------------------
// PartHeader (64 B, spec §5.1)
// ---------------------------------------------------------------------------

pub const PART_HEADER_LEN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PartHeader {
    pub magic: u64,
    pub format_version: u32,
    pub header_len: u32,
    pub part_no: u32,
    pub flags: u32,
    pub schema_fingerprint: u64,
    pub spc: u32,
    pub db: u32,
    pub relfilenumber: u64,
    pub reserved: [u8; 12],
    pub header_crc: u32,
}

impl PartHeader {
    pub fn new(
        part_no: u32,
        schema_fingerprint: u64,
        spc: u32,
        db: u32,
        relfilenumber: u64,
    ) -> PartHeader {
        PartHeader {
            magic: PART_MAGIC,
            format_version: FORMAT_VERSION,
            header_len: PART_HEADER_LEN as u32,
            part_no,
            flags: 0,
            schema_fingerprint,
            spc,
            db,
            relfilenumber,
            reserved: [0; 12],
            header_crc: 0,
        }
    }

    /// Encode (computes and embeds `header_crc` over bytes 0..60).
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let start = out.len();
        put_u64(out, self.magic);
        put_u32(out, self.format_version);
        put_u32(out, self.header_len);
        put_u32(out, self.part_no);
        put_u32(out, self.flags);
        put_u64(out, self.schema_fingerprint);
        put_u32(out, self.spc);
        put_u32(out, self.db);
        put_u64(out, self.relfilenumber);
        put_bytes(out, &self.reserved);
        let crc = crc32c(&out[start..]);
        put_u32(out, crc);
        debug_assert_eq!(out.len() - start, PART_HEADER_LEN);
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<PartHeader> {
        if bytes.len() < PART_HEADER_LEN {
            return Err(FormatError::Truncated { at: "PartHeader" });
        }
        let body = &bytes[..PART_HEADER_LEN - 4];
        let mut c = Cur::new(bytes);
        let magic = c.u64("PartHeader")?;
        if magic != PART_MAGIC {
            return Err(FormatError::BadMagic { at: "PartHeader" });
        }
        let format_version = c.u32("PartHeader")?;
        if format_version != FORMAT_VERSION {
            return Err(FormatError::BadVersion {
                at: "PartHeader",
                got: format_version,
            });
        }
        let header_len = c.u32("PartHeader")?;
        if header_len != PART_HEADER_LEN as u32 {
            return Err(FormatError::Corrupt {
                at: "PartHeader header_len",
            });
        }
        let part_no = c.u32("PartHeader")?;
        let flags = c.u32("PartHeader")?;
        let schema_fingerprint = c.u64("PartHeader")?;
        let spc = c.u32("PartHeader")?;
        let db = c.u32("PartHeader")?;
        let relfilenumber = c.u64("PartHeader")?;
        let reserved: [u8; 12] = c.take(12, "PartHeader")?.try_into().expect("len 12");
        let header_crc = c.u32("PartHeader")?;
        if header_crc != crc32c(body) {
            return Err(FormatError::CrcMismatch { at: "PartHeader" });
        }
        Ok(PartHeader {
            magic,
            format_version,
            header_len,
            part_no,
            flags,
            schema_fingerprint,
            spc,
            db,
            relfilenumber,
            reserved,
            header_crc,
        })
    }
}

// ---------------------------------------------------------------------------
// SectionEntry (32 B, spec §5.2)
// ---------------------------------------------------------------------------

pub const SECTION_ENTRY_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SectionEntry {
    pub off: u64,
    pub len: u64,
    pub kind: u16,
    pub flags: u16,
    pub attno: u32,
    pub path_ord: u32,
    pub crc: u32,
}

impl SectionEntry {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        put_u64(out, self.off);
        put_u64(out, self.len);
        put_u16(out, self.kind);
        put_u16(out, self.flags);
        put_u32(out, self.attno);
        put_u32(out, self.path_ord);
        put_u32(out, self.crc);
    }

    pub fn decode(c: &mut Cur<'_>) -> FormatResult<SectionEntry> {
        Ok(SectionEntry {
            off: c.u64("SectionEntry")?,
            len: c.u64("SectionEntry")?,
            kind: c.u16("SectionEntry")?,
            flags: c.u16("SectionEntry")?,
            attno: c.u32("SectionEntry")?,
            path_ord: c.u32("SectionEntry")?,
            crc: c.u32("SectionEntry")?,
        })
    }

    /// Adjudicate the kind for this reader (spec §5.2): known kinds pass;
    /// unknown + `SECTION_OPTIONAL` returns Ok(None) (skip); unknown without
    /// the flag is a typed refusal.
    pub fn known_kind(&self) -> FormatResult<Option<SectionKind>> {
        match SectionKind::from_u16(self.kind) {
            Ok(k) => Ok(Some(k)),
            Err(_) if self.flags & SECTION_OPTIONAL != 0 => Ok(None),
            Err(e) => Err(e),
        }
    }
}

// ---------------------------------------------------------------------------
// FooterFixed (96 B, spec §5.3) + PartTail (16 B, spec §5.4)
// ---------------------------------------------------------------------------

pub const FOOTER_FIXED_LEN: usize = 96;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct FooterFixed {
    pub magic: u32,
    pub format_version: u32,
    pub rows: u64,
    pub granule_count: u32,
    pub band_count: u32,
    pub section_count: u32,
    pub flags: u32,
    pub section_table_off: u64,
    pub section_table_crc: u32,
    pub part_no: u32,
    pub schema_fingerprint: u64,
    pub stream_count: u32,
    /// The part's elected granule grain in rows (SB-10 byte-bounded
    /// geometry; a `geom::GRAIN_LADDER` value — v4 claims the v3 `pad`
    /// slot, so the footer stays 96 B). The FOOTER is the grain's one
    /// on-disk home: the open path reads tail → footer before anything
    /// else, and every granule/band closed form derives from
    /// (rows, granule_rows). Decode validates ladder membership and the
    /// geometry echoes against the grain-parameterized closed forms.
    pub granule_rows: u32,
    pub reserved: [u8; 28],
    pub footer_crc: u32,
}

impl FooterFixed {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        let start = out.len();
        put_u32(out, self.magic);
        put_u32(out, self.format_version);
        put_u64(out, self.rows);
        put_u32(out, self.granule_count);
        put_u32(out, self.band_count);
        put_u32(out, self.section_count);
        put_u32(out, self.flags);
        put_u64(out, self.section_table_off);
        put_u32(out, self.section_table_crc);
        put_u32(out, self.part_no);
        put_u64(out, self.schema_fingerprint);
        put_u32(out, self.stream_count);
        put_u32(out, self.granule_rows);
        put_bytes(out, &self.reserved);
        let crc = crc32c(&out[start..]);
        put_u32(out, crc);
        debug_assert_eq!(out.len() - start, FOOTER_FIXED_LEN);
    }

    /// The part's validated granule grain (SB-10).
    pub fn grain(&self) -> FormatResult<crate::geom::GranuleGrain> {
        crate::geom::GranuleGrain::from_rows(self.granule_rows)
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<FooterFixed> {
        if bytes.len() < FOOTER_FIXED_LEN {
            return Err(FormatError::Truncated { at: "FooterFixed" });
        }
        let body = &bytes[..FOOTER_FIXED_LEN - 4];
        let mut c = Cur::new(bytes);
        let magic = c.u32("FooterFixed")?;
        if magic != FOOTER_MAGIC {
            return Err(FormatError::BadMagic { at: "FooterFixed" });
        }
        let format_version = c.u32("FooterFixed")?;
        if format_version != FORMAT_VERSION {
            return Err(FormatError::BadVersion {
                at: "FooterFixed",
                got: format_version,
            });
        }
        let rows = c.u64("FooterFixed")?;
        let granule_count = c.u32("FooterFixed")?;
        let band_count = c.u32("FooterFixed")?;
        let section_count = c.u32("FooterFixed")?;
        let flags = c.u32("FooterFixed")?;
        let section_table_off = c.u64("FooterFixed")?;
        let section_table_crc = c.u32("FooterFixed")?;
        let part_no = c.u32("FooterFixed")?;
        let schema_fingerprint = c.u64("FooterFixed")?;
        let stream_count = c.u32("FooterFixed")?;
        let granule_rows = c.u32("FooterFixed")?;
        let reserved: [u8; 28] = c.take(28, "FooterFixed")?.try_into().expect("len 28");
        let footer_crc = c.u32("FooterFixed")?;
        if footer_crc != crc32c(body) {
            return Err(FormatError::CrcMismatch { at: "FooterFixed" });
        }
        // The grain must sit on the SB-10 ladder, and the geometry echoes
        // must agree with the grain-parameterized closed forms (spec §5.3).
        let grain = crate::geom::GranuleGrain::from_rows(granule_rows)?;
        if granule_count != crate::geom::granule_count_at(rows, grain)
            || band_count != crate::geom::band_count_at(rows, grain)
        {
            return Err(FormatError::Corrupt {
                at: "FooterFixed geometry echo",
            });
        }
        Ok(FooterFixed {
            magic,
            format_version,
            rows,
            granule_count,
            band_count,
            section_count,
            flags,
            section_table_off,
            section_table_crc,
            part_no,
            schema_fingerprint,
            stream_count,
            granule_rows,
            reserved,
            footer_crc,
        })
    }
}

pub const PART_TAIL_LEN: usize = 16;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PartTail {
    pub footer_off: u64,
    pub footer_len: u32,
    pub magic: u32,
}

impl PartTail {
    pub fn new(footer_off: u64) -> PartTail {
        PartTail {
            footer_off,
            footer_len: FOOTER_FIXED_LEN as u32,
            magic: TAIL_MAGIC,
        }
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        put_u64(out, self.footer_off);
        put_u32(out, self.footer_len);
        put_u32(out, self.magic);
    }

    /// Decode from the LAST 16 bytes of `file_bytes`.
    pub fn decode_at_eof(file_bytes: &[u8]) -> FormatResult<PartTail> {
        if file_bytes.len() < PART_TAIL_LEN {
            return Err(FormatError::Truncated { at: "PartTail" });
        }
        let mut c = Cur::new(&file_bytes[file_bytes.len() - PART_TAIL_LEN..]);
        let footer_off = c.u64("PartTail")?;
        let footer_len = c.u32("PartTail")?;
        let magic = c.u32("PartTail")?;
        if magic != TAIL_MAGIC {
            return Err(FormatError::BadMagic { at: "PartTail" });
        }
        if footer_len != FOOTER_FIXED_LEN as u32 {
            return Err(FormatError::Corrupt {
                at: "PartTail footer_len",
            });
        }
        if footer_off as usize + FOOTER_FIXED_LEN + PART_TAIL_LEN > file_bytes.len() {
            return Err(FormatError::Bounds {
                at: "PartTail footer_off",
            });
        }
        Ok(PartTail {
            footer_off,
            footer_len,
            magic,
        })
    }
}

// ---------------------------------------------------------------------------
// StreamEntry (48 B) + ExtentRecord (40 B) (spec §6.3/§6.4)
// ---------------------------------------------------------------------------

pub const STREAM_ENTRY_LEN: usize = 48;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct StreamEntry {
    pub extent_table_off: u64,
    pub values: u64,
    pub attno: u32,
    pub path_ord: u32,
    pub fixed_len: u32,
    pub aux32: u32,
    pub extent_count: u32,
    pub encoding: u16,
    pub flags: u16,
    pub role: u8,
    pub class: u8,
    pub width: u8,
    pub wrapper: u8,
    pub reserved: u32,
}

impl StreamEntry {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        put_u64(out, self.extent_table_off);
        put_u64(out, self.values);
        put_u32(out, self.attno);
        put_u32(out, self.path_ord);
        put_u32(out, self.fixed_len);
        put_u32(out, self.aux32);
        put_u32(out, self.extent_count);
        put_u16(out, self.encoding);
        put_u16(out, self.flags);
        put_u8(out, self.role);
        put_u8(out, self.class);
        put_u8(out, self.width);
        put_u8(out, self.wrapper);
        put_u32(out, self.reserved);
    }

    pub fn decode(c: &mut Cur<'_>) -> FormatResult<StreamEntry> {
        Ok(StreamEntry {
            extent_table_off: c.u64("StreamEntry")?,
            values: c.u64("StreamEntry")?,
            attno: c.u32("StreamEntry")?,
            path_ord: c.u32("StreamEntry")?,
            fixed_len: c.u32("StreamEntry")?,
            aux32: c.u32("StreamEntry")?,
            extent_count: c.u32("StreamEntry")?,
            encoding: c.u16("StreamEntry")?,
            flags: c.u16("StreamEntry")?,
            role: c.u8("StreamEntry")?,
            class: c.u8("StreamEntry")?,
            width: c.u8("StreamEntry")?,
            wrapper: c.u8("StreamEntry")?,
            reserved: c.u32("StreamEntry")?,
        })
    }
}

pub const EXTENT_RECORD_LEN: usize = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ExtentRecord {
    pub file_off: u64,
    pub len: u64,
    pub values: u64,
    pub granule_start: u32,
    pub granule_count: u32,
    pub crc: u32,
    pub flags: u32,
}

impl ExtentRecord {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        put_u64(out, self.file_off);
        put_u64(out, self.len);
        put_u64(out, self.values);
        put_u32(out, self.granule_start);
        put_u32(out, self.granule_count);
        put_u32(out, self.crc);
        put_u32(out, self.flags);
    }

    pub fn decode(c: &mut Cur<'_>) -> FormatResult<ExtentRecord> {
        let r = ExtentRecord {
            file_off: c.u64("ExtentRecord")?,
            len: c.u64("ExtentRecord")?,
            values: c.u64("ExtentRecord")?,
            granule_start: c.u32("ExtentRecord")?,
            granule_count: c.u32("ExtentRecord")?,
            crc: c.u32("ExtentRecord")?,
            flags: c.u32("ExtentRecord")?,
        };
        if r.len > crate::geom::EXTENT_MAX_LEN {
            return Err(FormatError::Corrupt {
                at: "ExtentRecord len > EXTENT_MAX_LEN",
            });
        }
        Ok(r)
    }
}

// ---------------------------------------------------------------------------
// StreamSectionHdr (32 B) + StreamSectionWriter (spec §6.4)
// ---------------------------------------------------------------------------

pub const STREAM_SECTION_HDR_LEN: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct StreamSectionHdr {
    pub magic: u32,
    pub encoding: u16,
    pub width: u8,
    pub wrapper: u8,
    pub frame_count: u32,
    /// Section-relative offset of the frame table; 0 = closed-form stride.
    pub frame_table_off: u32,
    /// Section-relative offset of the per-granule value-count table (child
    /// streams only); 0 = root stream (closed-form from part rows).
    pub gcount_table_off: u32,
    /// Uncompressed payload image size when `wrapper != 0`; else 0.
    pub uncompressed_len: u32,
    pub value_count: u32,
    pub reserved: u32,
}

impl StreamSectionHdr {
    pub fn decode(bytes: &[u8]) -> FormatResult<StreamSectionHdr> {
        let mut c = Cur::new(bytes);
        let h = StreamSectionHdr {
            magic: c.u32("StreamSectionHdr")?,
            encoding: c.u16("StreamSectionHdr")?,
            width: c.u8("StreamSectionHdr")?,
            wrapper: c.u8("StreamSectionHdr")?,
            frame_count: c.u32("StreamSectionHdr")?,
            frame_table_off: c.u32("StreamSectionHdr")?,
            gcount_table_off: c.u32("StreamSectionHdr")?,
            uncompressed_len: c.u32("StreamSectionHdr")?,
            value_count: c.u32("StreamSectionHdr")?,
            reserved: c.u32("StreamSectionHdr")?,
        };
        if h.magic != STREAM_SECTION_MAGIC {
            return Err(FormatError::BadMagic {
                at: "StreamSectionHdr",
            });
        }
        Ok(h)
    }

    /// The frame table slice (validated), or None for closed-form strides.
    pub fn frame_table<'a>(&self, section: &'a [u8]) -> FormatResult<Option<Vec<u32>>> {
        if self.frame_table_off == 0 {
            return Ok(None);
        }
        let off = self.frame_table_off as usize;
        let need = self.frame_count as usize * 4;
        if off < STREAM_SECTION_HDR_LEN || off + need > section.len() {
            return Err(FormatError::Bounds { at: "frame table" });
        }
        let mut v = Vec::with_capacity(self.frame_count as usize);
        for i in 0..self.frame_count as usize {
            let b = &section[off + i * 4..off + i * 4 + 4];
            v.push(u32::from_le_bytes(b.try_into().expect("len 4")));
        }
        Ok(Some(v))
    }
}

/// What `StreamSectionWriter::finish` reports back to the seal driver: the
/// facts an [`ExtentRecord`] needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamCloseout {
    pub len: u64,
    pub crc: u32,
    pub frame_count: u32,
    pub values: u64,
}

/// The one section-body assembler (spec §19.6): every codec emits identical
/// framing through it, so §6.4 mechanics live in exactly one place. Wrapper
/// block assembly is M3-C's; this writer emits unwrapped sections and
/// refuses `wrapper != None` typed.
pub struct StreamSectionWriter<'a> {
    buf: &'a mut Vec<u8>,
    start: usize,
    encoding: u16,
    width: u8,
    frame_offs: Vec<u32>,
    gcounts: Vec<u32>,
    values: u64,
}

impl<'a> StreamSectionWriter<'a> {
    pub fn begin(
        buf: &'a mut Vec<u8>,
        encoding: u16,
        width: u8,
        wrapper: Wrapper,
    ) -> FormatResult<StreamSectionWriter<'a>> {
        if wrapper != Wrapper::None {
            return Err(FormatError::WrapperUnsupported {
                wrapper: wrapper.as_u8(),
            });
        }
        let start = buf.len();
        buf.resize(start + STREAM_SECTION_HDR_LEN, 0);
        Ok(StreamSectionWriter {
            buf,
            start,
            encoding,
            width,
            frame_offs: Vec::new(),
            gcounts: Vec::new(),
            values: 0,
        })
    }

    /// Payload-relative offset of the next byte to be written.
    pub fn payload_off(&self) -> u32 {
        (self.buf.len() - self.start - STREAM_SECTION_HDR_LEN) as u32
    }

    /// Zero-pad so `payload_off() % align == 0` — section-start-relative
    /// alignment (sections themselves start 8-aligned in the file, writer
    /// law), so entry alignment becomes absolute (spec §1).
    pub fn align_payload(&mut self, align: u32) {
        let rem = self.payload_off() % align;
        if rem != 0 {
            let pad = (align - rem) as usize;
            let new_len = self.buf.len() + pad;
            self.buf.resize(new_len, 0);
        }
    }

    /// Mark the start of the next frame (records the frame-table entry).
    pub fn begin_frame(&mut self) {
        let off = self.payload_off();
        self.frame_offs.push(off);
    }

    /// The payload sink for the current frame.
    pub fn payload(&mut self) -> &mut Vec<u8> {
        self.buf
    }

    /// Close one granule of `values` values (feeds the gcount table; root
    /// streams still call it — the table is emitted only when asked for).
    pub fn end_granule(&mut self, values: u32) {
        self.gcounts.push(values);
        self.values += values as u64;
    }

    /// Finish the section: emit tables (frame table always when any frame was
    /// marked; gcount table iff `child`), patch the header, CRC the body.
    pub fn finish(self, child: bool) -> FormatResult<StreamCloseout> {
        let frame_count = self.frame_offs.len() as u32;
        let frame_table_off = if frame_count > 0 {
            let off = self.payload_off() + STREAM_SECTION_HDR_LEN as u32;
            for &f in &self.frame_offs {
                put_u32(self.buf, f);
            }
            off
        } else {
            0
        };
        let gcount_table_off = if child {
            let off = (self.buf.len() - self.start) as u32;
            for &g in &self.gcounts {
                put_u32(self.buf, g);
            }
            off
        } else {
            0
        };
        let len = (self.buf.len() - self.start) as u64;
        if len > crate::geom::EXTENT_MAX_LEN {
            return Err(FormatError::Corrupt {
                at: "stream section > EXTENT_MAX_LEN",
            });
        }
        // Patch the header in place.
        let hdr = &mut self.buf[self.start..self.start + STREAM_SECTION_HDR_LEN];
        hdr[0..4].copy_from_slice(&STREAM_SECTION_MAGIC.to_le_bytes());
        hdr[4..6].copy_from_slice(&self.encoding.to_le_bytes());
        hdr[6] = self.width;
        hdr[7] = Wrapper::None.as_u8();
        hdr[8..12].copy_from_slice(&frame_count.to_le_bytes());
        hdr[12..16].copy_from_slice(&frame_table_off.to_le_bytes());
        hdr[16..20].copy_from_slice(&gcount_table_off.to_le_bytes());
        hdr[20..24].copy_from_slice(&0u32.to_le_bytes());
        hdr[24..28].copy_from_slice(&(self.values.min(u32::MAX as u64) as u32).to_le_bytes());
        hdr[28..32].copy_from_slice(&0u32.to_le_bytes());
        let crc = crc32c(&self.buf[self.start..]);
        Ok(StreamCloseout {
            len,
            crc,
            frame_count,
            values: self.values,
        })
    }
}

/// Overflow-stream assembler (spec §6.8): varlena-shaped, 8-aligned entries;
/// returns the stream-payload-relative offset each `OverflowRef` records.
pub struct OverflowSink<'a> {
    buf: &'a mut Vec<u8>,
    base: usize,
    entries: u64,
}

impl<'a> OverflowSink<'a> {
    pub fn new(buf: &'a mut Vec<u8>) -> OverflowSink<'a> {
        let base = buf.len();
        OverflowSink {
            buf,
            base,
            entries: 0,
        }
    }

    /// Append one oversize value; returns its `ovf_off`.
    pub fn put_entry(&mut self, payload: &[u8]) -> u64 {
        // Entry alignment is relative to the overflow payload region start.
        let rel = self.buf.len() - self.base;
        let pad = (8 - (rel % 8)) % 8;
        self.buf.resize(self.buf.len() + pad, 0);
        let off = (self.buf.len() - self.base) as u64;
        put_u32(
            self.buf,
            crate::wire::varlena_header_4b_u(payload.len() as u32),
        );
        put_bytes(self.buf, payload);
        self.entries += 1;
        off
    }

    /// Entries appended so far (SEAL-FUSION #22: the entry count is known
    /// at append time — the seal no longer re-walks the overflow region's
    /// varlena headers to census it).
    pub fn entries(&self) -> u64 {
        self.entries
    }

    pub fn is_empty(&self) -> bool {
        self.buf.len() == self.base
    }
}
