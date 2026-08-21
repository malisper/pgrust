//! Tombstone + delete-vector vocabulary (spec §15; ruling O-2, the ruled DV
//! ladder). COMPLETE at M3-A so M5 adds machinery, never format:
//!
//! - **Tombstone** (heap delta-store row; M5 writes them): single payload
//!   column = the deleted columnar RowId (spec §10) as int8. Deletion
//!   visibility = ordinary heap MVCC on tombstones — C-exact by
//!   construction. At M3, under O-M3-1(a), trickle DML raises typed refusals
//!   at the AM surface (M3-H): no tombstone is ever written.
//! - **Delete vector** (M5 compaction-rung writer): at most ONE DV per part
//!   (replace-not-stack, Iceberg-v3), compiling only horizon-passed
//!   tombstones — frozen-only, so no DV visibility logic ever exists. The
//!   DV is AUTHORITATIVE sidecar state: referenced from the manifest
//!   (`dv_gen`/`dv_len`/`dv_crc`), never keyed on physical part identity
//!   (a copied bank must not forget deletions), published under the §13.3
//!   ordering BEFORE the deleting txn's commit record.

use crate::geom::{GRANULES_PER_BAND, GRANULE_ROWS};
use crate::wire::{crc32c, put_u16, put_u32, put_u64, put_u8, Cur};
use crate::{FormatError, FormatResult, FORMAT_VERSION};

/// The delta-store tombstone row schema (vocabulary only at M3): one int8
/// attribute carrying the packed RowId bits.
pub const TOMBSTONE_NATTS: usize = 1;

/// DV header magic: ASCII "PRCV".
pub const DV_MAGIC: u32 = u32::from_le_bytes(*b"PRCV");

pub const DV_HEADER_LEN: usize = 32;
pub const DV_GRANULE_BLOCK_HDR_LEN: usize = 8;
/// Bitmap-kind block payload: 8,192 bits.
pub const DV_BITMAP_BYTES: usize = (GRANULE_ROWS as usize) / 8;

/// DV payload header (32 B, spec §15). Wire order == declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct DvHeader {
    pub deleted_rows: u64,
    pub gen: u64,
    pub magic: u32,
    pub version: u32,
    pub part_no: u32,
    pub block_count: u32,
}

/// Per-granule block kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DvBlockKind {
    /// `count` × u16 row ordinals, strictly ascending.
    List = 0,
    /// 1 KiB bitmap (8,192 bits, LSB-first); `count` = popcount.
    Bitmap = 1,
}

impl DvBlockKind {
    pub fn from_u8(v: u8) -> FormatResult<DvBlockKind> {
        match v {
            0 => Ok(DvBlockKind::List),
            1 => Ok(DvBlockKind::Bitmap),
            _ => Err(FormatError::Corrupt { at: "DvBlockKind" }),
        }
    }
}

/// Per-granule block header (8 B).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct DvGranuleBlockHdr {
    pub granule: u32,
    pub kind: u8,
    pub pad: u8,
    pub count: u16,
}

/// One decoded DV block.
#[derive(Debug, Clone, Copy)]
pub struct DvBlock<'a> {
    pub granule: u32,
    pub kind: DvBlockKind,
    /// Deleted-row count in this granule (list len or bitmap popcount).
    pub count: u32,
    /// List: `2 × count` bytes of u16 ordinals; Bitmap: `DV_BITMAP_BYTES`.
    pub payload: &'a [u8],
}

impl DvHeader {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        put_u64(out, self.deleted_rows);
        put_u64(out, self.gen);
        put_u32(out, self.magic);
        put_u32(out, self.version);
        put_u32(out, self.part_no);
        put_u32(out, self.block_count);
    }
}

/// Encode a whole DV payload image (header + blocks + trailing crc32c).
/// Blocks must arrive in strictly-ascending granule order; list ordinals
/// strictly ascending. (The M5 writer's entry point; frozen now so the
/// format never moves.)
pub fn encode_dv(
    part_no: u32,
    gen: u64,
    blocks: &[(u32, DvBlockKind, &[u16])],
) -> FormatResult<Vec<u8>> {
    let mut out = Vec::new();
    let mut deleted: u64 = 0;
    for (_, _, rows) in blocks {
        deleted += rows.len() as u64;
    }
    DvHeader {
        deleted_rows: deleted,
        gen,
        magic: DV_MAGIC,
        version: FORMAT_VERSION,
        part_no,
        block_count: blocks.len() as u32,
    }
    .encode_into(&mut out);
    let mut prev_granule: Option<u32> = None;
    for (granule, kind, rows) in blocks {
        if let Some(p) = prev_granule {
            if *granule <= p {
                return Err(FormatError::EncodeContract {
                    detail: "DV granule order",
                });
            }
        }
        prev_granule = Some(*granule);
        put_u32(&mut out, *granule);
        put_u8(&mut out, *kind as u8);
        put_u8(&mut out, 0);
        match kind {
            DvBlockKind::List => {
                if rows.len() > u16::MAX as usize {
                    return Err(FormatError::EncodeContract {
                        detail: "DV list too long",
                    });
                }
                put_u16(&mut out, rows.len() as u16);
                let mut prev: Option<u16> = None;
                for &r in *rows {
                    if r as u32 >= GRANULE_ROWS {
                        return Err(FormatError::EncodeContract {
                            detail: "DV row ordinal",
                        });
                    }
                    if let Some(p) = prev {
                        if r <= p {
                            return Err(FormatError::EncodeContract {
                                detail: "DV list order",
                            });
                        }
                    }
                    prev = Some(r);
                    put_u16(&mut out, r);
                }
            }
            DvBlockKind::Bitmap => {
                put_u16(&mut out, rows.len().min(u16::MAX as usize) as u16);
                let mut bits = [0u8; DV_BITMAP_BYTES];
                for &r in *rows {
                    if r as u32 >= GRANULE_ROWS {
                        return Err(FormatError::EncodeContract {
                            detail: "DV row ordinal",
                        });
                    }
                    bits[(r / 8) as usize] |= 1 << (r % 8);
                }
                out.extend_from_slice(&bits);
            }
        }
    }
    let crc = crc32c(&out);
    put_u32(&mut out, crc);
    Ok(out)
}

/// Streaming DV reader (validates header, magic, version, crc, block order).
pub struct DvReader<'a> {
    cur: Cur<'a>,
    pub header: DvHeader,
    remaining_blocks: u32,
    prev_granule: Option<u32>,
}

impl<'a> DvReader<'a> {
    pub fn open(bytes: &'a [u8]) -> FormatResult<DvReader<'a>> {
        if bytes.len() < DV_HEADER_LEN + 4 {
            return Err(FormatError::Truncated { at: "DvHeader" });
        }
        let body = &bytes[..bytes.len() - 4];
        let stored = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().expect("len 4"));
        if stored != crc32c(body) {
            return Err(FormatError::CrcMismatch { at: "DeleteVector" });
        }
        let mut cur = Cur::new(body);
        let header = DvHeader {
            deleted_rows: cur.u64("DvHeader")?,
            gen: cur.u64("DvHeader")?,
            magic: cur.u32("DvHeader")?,
            version: cur.u32("DvHeader")?,
            part_no: cur.u32("DvHeader")?,
            block_count: cur.u32("DvHeader")?,
        };
        if header.magic != DV_MAGIC {
            return Err(FormatError::BadMagic { at: "DvHeader" });
        }
        if header.version != FORMAT_VERSION {
            return Err(FormatError::BadVersion {
                at: "DvHeader",
                got: header.version,
            });
        }
        let remaining_blocks = header.block_count;
        Ok(DvReader {
            cur,
            header,
            remaining_blocks,
            prev_granule: None,
        })
    }

    /// Next block, or None after the last (trailing bytes are a typed error).
    pub fn next_block(&mut self) -> FormatResult<Option<DvBlock<'a>>> {
        if self.remaining_blocks == 0 {
            if self.cur.remaining() != 0 {
                return Err(FormatError::Corrupt {
                    at: "DV trailing bytes",
                });
            }
            return Ok(None);
        }
        self.remaining_blocks -= 1;
        let granule = self.cur.u32("DvGranuleBlockHdr")?;
        if let Some(p) = self.prev_granule {
            if granule <= p {
                return Err(FormatError::Corrupt {
                    at: "DV granule order",
                });
            }
        }
        self.prev_granule = Some(granule);
        let kind = DvBlockKind::from_u8(self.cur.u8("DvGranuleBlockHdr")?)?;
        let _pad = self.cur.u8("DvGranuleBlockHdr")?;
        let count = self.cur.u16("DvGranuleBlockHdr")? as u32;
        let payload = match kind {
            DvBlockKind::List => self.cur.take(count as usize * 2, "DV list")?,
            DvBlockKind::Bitmap => self.cur.take(DV_BITMAP_BYTES, "DV bitmap")?,
        };
        Ok(Some(DvBlock {
            granule,
            kind,
            count,
            payload,
        }))
    }
}

// Whole-band claims never split DV state: blocks are granule-scoped and a
// band is a whole number of granules.
const _: () = assert!(GRANULES_PER_BAND == 8);
