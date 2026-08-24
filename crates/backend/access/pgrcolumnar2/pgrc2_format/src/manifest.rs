//! The part manifest + commit pointer (spec §13; rulings O-2/O-3): the
//! manifest generation REPLACES the single footer_off as the commit point.
//!
//! ## The publish ordering law (spec §13.3 — M3-D implements, M3-K's kill -9
//! ladder proves; the cbstore #253 acked-writes law is format law here)
//!
//! 1. write + fsync every new part file (and DV sidecar, once the M5 rung
//!    exists);
//! 2. write + fsync `manifest-<gen>` (`prev_gen` chained, `publisher_fxid` =
//!    the publishing txn's FullTransactionId);
//! 3. publish `CURRENT` via tmp + rename + file fsync;
//! 4. fsync the table directory (dirent durability — SimVfs dirent-loss is
//!    the test model);
//! 5. only then the commit record.
//!
//! ## The clog fence (#254)
//!
//! `publisher_fxid` is epoch-qualified (64-bit) — xid recycling cannot alias
//! it. A generation is EFFECTIVE iff its publisher committed; readers and
//! recovery walk `prev_gen` past non-committed generations, so an aborted or
//! crashed-before-commit publish is structurally invisible. `CURRENT` is a
//! candidate hint + O(1) entry point, never the effectiveness authority.

use crate::rowid::MAX_GRANULES_PER_PART;
use crate::wire::{crc32c, put_u32, put_u64, Cur};
use crate::{FormatError, FormatResult, FORMAT_VERSION};

/// Manifest magic: ASCII "PRCM".
pub const MANIFEST_MAGIC: u32 = u32::from_le_bytes(*b"PRCM");
/// Commit-pointer magic: ASCII "PRCC".
pub const CURRENT_MAGIC: u32 = u32::from_le_bytes(*b"PRCC");

pub const MANIFEST_HEADER_LEN: usize = 72;
pub const PART_RECORD_LEN: usize = 64;
pub const COMMIT_POINTER_LEN: usize = 32;

/// Manifest file header (72 B, spec §13.1). Wire order == declaration order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct ManifestHeader {
    pub gen: u64,
    /// 0 = first generation (no predecessor).
    pub prev_gen: u64,
    /// FullTransactionId of the publisher — the clog fence (#254).
    pub publisher_fxid: u64,
    pub relfilenumber: u64,
    pub schema_fingerprint: u64,
    pub magic: u32,
    pub format_version: u32,
    pub spc: u32,
    pub db: u32,
    pub part_count: u32,
    /// The next part_no this table will assign (monotone; rowid §10 order).
    pub next_part_no: u32,
    pub flags: u32,
    pub reserved: u32,
}

/// Per-part manifest record (64 B, spec §13.1). `dv_gen == 0` ⇒ no delete
/// vector; the manifest is the AUTHORITATIVE reference for DVs (spec §15/§16).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct PartRecord {
    pub rows: u64,
    pub file_len: u64,
    pub footer_off: u64,
    pub dv_gen: u64,
    pub dv_len: u64,
    pub part_no: u32,
    pub flags: u32,
    pub granule_count: u32,
    pub band_count: u32,
    pub dv_crc: u32,
    /// SB-10 grain echo (the A-lane manifest ripple, closed at M3-L2): the
    /// part's elected granule grain in rows. `0` = legacy default-grain
    /// record (v3-cut manifests): geometry echoes validate against the
    /// DEFAULT closed forms. Nonzero = a ladder grain: geometry echoes
    /// validate against the grain-parameterized closed forms and MATCH the
    /// part footer's `granule_rows` (the footer stays the grain truth;
    /// this field makes the manifest self-consistent instead of
    /// default-grain fiction for non-default parts).
    pub granule_rows: u32,
}

/// A decoded manifest generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Manifest {
    pub header: ManifestHeader,
    pub parts: Vec<PartRecord>,
}

impl Manifest {
    /// Encode the full manifest file image (trailing crc32c included).
    pub fn encode(&self) -> Vec<u8> {
        let mut out =
            Vec::with_capacity(MANIFEST_HEADER_LEN + self.parts.len() * PART_RECORD_LEN + 4);
        let h = &self.header;
        put_u64(&mut out, h.gen);
        put_u64(&mut out, h.prev_gen);
        put_u64(&mut out, h.publisher_fxid);
        put_u64(&mut out, h.relfilenumber);
        put_u64(&mut out, h.schema_fingerprint);
        put_u32(&mut out, h.magic);
        put_u32(&mut out, h.format_version);
        put_u32(&mut out, h.spc);
        put_u32(&mut out, h.db);
        put_u32(&mut out, h.part_count);
        put_u32(&mut out, h.next_part_no);
        put_u32(&mut out, h.flags);
        put_u32(&mut out, h.reserved);
        debug_assert_eq!(out.len(), MANIFEST_HEADER_LEN);
        for p in &self.parts {
            put_u64(&mut out, p.rows);
            put_u64(&mut out, p.file_len);
            put_u64(&mut out, p.footer_off);
            put_u64(&mut out, p.dv_gen);
            put_u64(&mut out, p.dv_len);
            put_u32(&mut out, p.part_no);
            put_u32(&mut out, p.flags);
            put_u32(&mut out, p.granule_count);
            put_u32(&mut out, p.band_count);
            put_u32(&mut out, p.dv_crc);
            put_u32(&mut out, p.granule_rows);
        }
        let crc = crc32c(&out);
        put_u32(&mut out, crc);
        out
    }

    /// Decode + validate a manifest file image: magic, version, crc,
    /// part_count consistency, strictly-increasing part_no, part_no <
    /// next_part_no, geometry echoes.
    pub fn decode(bytes: &[u8]) -> FormatResult<Manifest> {
        if bytes.len() < MANIFEST_HEADER_LEN + 4 {
            return Err(FormatError::Truncated { at: "Manifest" });
        }
        let body = &bytes[..bytes.len() - 4];
        let stored_crc = u32::from_le_bytes(bytes[bytes.len() - 4..].try_into().expect("len 4"));
        if stored_crc != crc32c(body) {
            return Err(FormatError::CrcMismatch { at: "Manifest" });
        }
        let mut c = Cur::new(body);
        let header = ManifestHeader {
            gen: c.u64("ManifestHeader")?,
            prev_gen: c.u64("ManifestHeader")?,
            publisher_fxid: c.u64("ManifestHeader")?,
            relfilenumber: c.u64("ManifestHeader")?,
            schema_fingerprint: c.u64("ManifestHeader")?,
            magic: c.u32("ManifestHeader")?,
            format_version: c.u32("ManifestHeader")?,
            spc: c.u32("ManifestHeader")?,
            db: c.u32("ManifestHeader")?,
            part_count: c.u32("ManifestHeader")?,
            next_part_no: c.u32("ManifestHeader")?,
            flags: c.u32("ManifestHeader")?,
            reserved: c.u32("ManifestHeader")?,
        };
        if header.magic != MANIFEST_MAGIC {
            return Err(FormatError::BadMagic {
                at: "ManifestHeader",
            });
        }
        if header.format_version != FORMAT_VERSION {
            return Err(FormatError::BadVersion {
                at: "ManifestHeader",
                got: header.format_version,
            });
        }
        if header.gen == 0 || (header.prev_gen != 0 && header.prev_gen >= header.gen) {
            return Err(FormatError::Corrupt {
                at: "ManifestHeader gen chain",
            });
        }
        if c.remaining() != header.part_count as usize * PART_RECORD_LEN {
            return Err(FormatError::Corrupt {
                at: "Manifest part_count vs length",
            });
        }
        let mut parts = Vec::with_capacity(header.part_count as usize);
        let mut prev_part_no: Option<u32> = None;
        for _ in 0..header.part_count {
            let p = PartRecord {
                rows: c.u64("PartRecord")?,
                file_len: c.u64("PartRecord")?,
                footer_off: c.u64("PartRecord")?,
                dv_gen: c.u64("PartRecord")?,
                dv_len: c.u64("PartRecord")?,
                part_no: c.u32("PartRecord")?,
                flags: c.u32("PartRecord")?,
                granule_count: c.u32("PartRecord")?,
                band_count: c.u32("PartRecord")?,
                dv_crc: c.u32("PartRecord")?,
                granule_rows: c.u32("PartRecord")?,
            };
            if let Some(prev) = prev_part_no {
                if p.part_no <= prev {
                    return Err(FormatError::Corrupt {
                        at: "PartRecord part_no order",
                    });
                }
            }
            if p.part_no >= header.next_part_no {
                return Err(FormatError::Corrupt {
                    at: "PartRecord part_no >= next_part_no",
                });
            }
            // SB-10 grain-aware geometry echo: legacy records (0) validate
            // at the default grain; grain-stamped records at their own.
            let grain = if p.granule_rows == 0 {
                crate::geom::GranuleGrain::DEFAULT
            } else {
                crate::geom::GranuleGrain::from_rows(p.granule_rows).map_err(|_| {
                    FormatError::Corrupt {
                        at: "PartRecord granule_rows off the ladder",
                    }
                })?
            };
            // The granule ordinal is a 19-bit rowid field (rowid §10): a part
            // may address at most MAX_GRANULES_PER_PART granules regardless of
            // grain. Enforcing this on the read side keeps an out-of-range
            // granule_count from surviving to pack time, where pack_rowid would
            // overflow the granule field into the neighboring part's rowid
            // space. Grain-independent: the field width never moves.
            if p.granule_count > MAX_GRANULES_PER_PART {
                return Err(FormatError::Corrupt {
                    at: "PartRecord granule_count > MAX_GRANULES_PER_PART",
                });
            }
            // Compare in u64: the closed forms are untruncated (geom §), so a
            // hostile manifest storing the mod-2^32 residue of an inconsistent
            // (rows, granule_count/band_count) can no longer alias the true
            // value (idx 253). Widening the stored u32 also rejects any true
            // count that does not fit u32 — band_count in particular is not
            // bounded by the MAX_GRANULES_PER_PART reject above.
            if (p.granule_count as u64) != crate::geom::granule_count_at(p.rows, grain)
                || (p.band_count as u64) != crate::geom::band_count_at(p.rows, grain)
            {
                return Err(FormatError::Corrupt {
                    at: "PartRecord geometry echo",
                });
            }
            if p.footer_off
                + crate::part::FOOTER_FIXED_LEN as u64
                + crate::part::PART_TAIL_LEN as u64
                > p.file_len
            {
                return Err(FormatError::Corrupt {
                    at: "PartRecord footer_off vs file_len",
                });
            }
            prev_part_no = Some(p.part_no);
            parts.push(p);
        }
        Ok(Manifest { header, parts })
    }
}

/// The `CURRENT` commit-pointer record (32 B, spec §13.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct CommitPointer {
    pub gen: u64,
    pub manifest_len: u64,
    pub magic: u32,
    pub version: u32,
    pub manifest_crc: u32,
    pub crc: u32,
}

impl CommitPointer {
    pub fn new(gen: u64, manifest_len: u64, manifest_crc: u32) -> CommitPointer {
        CommitPointer {
            gen,
            manifest_len,
            magic: CURRENT_MAGIC,
            version: FORMAT_VERSION,
            manifest_crc,
            crc: 0,
        }
    }

    pub fn encode(&self) -> [u8; COMMIT_POINTER_LEN] {
        let mut out = Vec::with_capacity(COMMIT_POINTER_LEN);
        put_u64(&mut out, self.gen);
        put_u64(&mut out, self.manifest_len);
        put_u32(&mut out, self.magic);
        put_u32(&mut out, self.version);
        put_u32(&mut out, self.manifest_crc);
        let crc = crc32c(&out);
        put_u32(&mut out, crc);
        out.try_into().expect("commit pointer is 32 bytes")
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<CommitPointer> {
        if bytes.len() < COMMIT_POINTER_LEN {
            return Err(FormatError::Truncated {
                at: "CommitPointer",
            });
        }
        let body = &bytes[..COMMIT_POINTER_LEN - 4];
        let mut c = Cur::new(bytes);
        let p = CommitPointer {
            gen: c.u64("CommitPointer")?,
            manifest_len: c.u64("CommitPointer")?,
            magic: c.u32("CommitPointer")?,
            version: c.u32("CommitPointer")?,
            manifest_crc: c.u32("CommitPointer")?,
            crc: c.u32("CommitPointer")?,
        };
        if p.magic != CURRENT_MAGIC {
            return Err(FormatError::BadMagic {
                at: "CommitPointer",
            });
        }
        if p.version != FORMAT_VERSION {
            return Err(FormatError::BadVersion {
                at: "CommitPointer",
                got: p.version,
            });
        }
        if p.crc != crc32c(body) {
            return Err(FormatError::CrcMismatch {
                at: "CommitPointer",
            });
        }
        Ok(p)
    }
}
