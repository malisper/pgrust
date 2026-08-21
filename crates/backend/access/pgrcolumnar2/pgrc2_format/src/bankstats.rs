//! Bank-grain stats plane (fmt-layout lane, 2026-08-16; Michael: "moving
//! the stats into one section sounds good" — APPROVED as a format change).
//!
//! THE PROBLEM (RESULTS-COLDSTART §8.5): under the per-query-run ruling a
//! reader re-faults each column's stats face as ~one small section pread
//! PER PART (511 preads/column at 100m, ~30–60 KB each) — 100–250 ms of
//! pure metadata IO per query on the official cold recipe.
//!
//! THE PLANE: one DERIVED sidecar file per bank generation
//! (`bankstats-<gen>.pgrc2bs`) laying out, for every column, ALL parts'
//! §8.1 Stats section bodies (unwrapped) plus the §8.6 PartDigest records
//! CONTIGUOUSLY — a reader takes a column's whole stats face in ONE pread.
//!
//! Composition with per-part sealing (spec §3.3 "no cross-part structures
//! in the format, ever"): parts stay self-contained and authoritative —
//! this file is a DERIVED artifact, a pure function of the sealed parts,
//! (re)written at publish/compaction time and REBUILDABLE at any time.
//! It is never the source of truth: a validity witness (manifest
//! generation + schema fingerprint + the full per-part identity vector)
//! binds it to one exact part-set; ANY mismatch is a typed refusal and
//! the reader falls back to the per-part sections (never wrong, only
//! slower). Crash story: atomic tmp+rename like every sidecar; a missing
//! or stale plane is a rebuild, never corruption.
//!
//! Wire layout (all LE; version 1):
//!
//! ```text
//! BankStatsHeader (48 B):
//!   magic u32 = "PRCB"    version u32 = 1
//!   gen u64               schema_fingerprint u64
//!   part_count u32        ncols u32
//!   meta_len u64          (= header + part idents + col dir + meta_crc)
//!   flags u32             pad u32 = 0
//! PartIdent (32 B) × part_count:
//!   part_no u32, granule_count u32, band_count u32, pad u32 = 0,
//!   rows u64, footer_off u64
//! ColDirEntry (40 B) × ncols:
//!   attno u32, flags u32, off u64, len u64, stats_len u64,
//!   crc u32 (crc32c of payload [off,off+len)), pad u32 = 0
//! meta_crc u32            (crc32c over bytes [0, meta_len-4))
//! -- 8-aligned column payloads, one per ColDirEntry --
//!   offtab  u64 × (part_count+1)   relative offsets into the stats blob
//!   stats blob                     concat of per-part UNWRAPPED §8.1
//!                                  Stats bodies (empty slice = the part
//!                                  carries no Stats section for the col)
//!   digests [u8;24] × part_count   §8.6 PartDigest record bytes;
//!                                  all-zero (version 0) = absent
//! ```
//!
//! Size at 100m (measured in RESULTS-FMTLAYOUT.md): §8.1 bodies are 80 B ×
//! (granules+bands+1) per (part,column) — ~1.7% of the bank. Append story:
//! a new part appends its slices and the meta region is rewritten (the
//! file is small; compaction rewrites it wholesale); either way the old
//! plane's identity vector no longer matches the new manifest, so a
//! half-updated plane is REFUSED, not misread.

use crate::wire::{crc32c, put_u32, put_u64, Cur};
use crate::{FormatError, FormatResult};

/// File magic: ASCII "PRCB".
pub const BANKSTATS_MAGIC: u32 = u32::from_le_bytes(*b"PRCB");
pub const BANKSTATS_VERSION: u32 = 1;
pub const BANKSTATS_HEADER_LEN: usize = 48;
pub const BANKSTATS_PARTIDENT_LEN: usize = 32;
pub const BANKSTATS_COLDIR_LEN: usize = 40;
/// §8.6 PartDigest record length as carried here (version-1 wire form).
pub const BANKSTATS_DIGEST_LEN: usize = 24;

/// The plane's file name inside the bank directory.
pub fn bankstats_file_name(gen: u64) -> String {
    format!("bankstats-{gen}.pgrc2bs")
}

/// Parse a plane file name back to its generation (`None` = not a plane
/// file). The recovery sweep's classifier (dead-generation planes and
/// `.pgrc2bs.tmp` residue are reclaimed there).
pub fn parse_bankstats_file_name(name: &str) -> Option<u64> {
    name.strip_prefix("bankstats-")?
        .strip_suffix(".pgrc2bs")?
        .parse()
        .ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BankStatsHeader {
    pub gen: u64,
    pub schema_fingerprint: u64,
    pub part_count: u32,
    pub ncols: u32,
    pub meta_len: u64,
    pub flags: u32,
}

/// The validity witness row: one sealed part's identity as the plane saw
/// it. A reader must match EVERY field against its resolved manifest's
/// PartRecord (plus the closed-form granule/band counts) or refuse.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartIdent {
    pub part_no: u32,
    pub granule_count: u32,
    pub band_count: u32,
    pub rows: u64,
    pub footer_off: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColDirEntry {
    pub attno: u32,
    pub flags: u32,
    /// Absolute file offset of the column payload (8-aligned).
    pub off: u64,
    /// Payload length: offtab + stats blob + digest array.
    pub len: u64,
    /// The stats blob's length (derivable; stored for validation).
    pub stats_len: u64,
    /// crc32c over the payload bytes [off, off+len).
    pub crc: u32,
}

// ---------------------------------------------------------------------------
// Builder (pure: bytes in, one file image out)
// ---------------------------------------------------------------------------

/// Per-column input: for each part IN MANIFEST ORDER, the part's unwrapped
/// §8.1 Stats body for this column (empty = absent) and its 24-B
/// PartDigest record bytes (None = absent → zeros).
pub struct ColInput<'a> {
    pub attno: u32,
    pub stats_bodies: Vec<&'a [u8]>,
    pub digests: Vec<Option<[u8; BANKSTATS_DIGEST_LEN]>>,
}

/// Assemble the whole plane image. `parts` in manifest order; every
/// column's vectors must have exactly `parts.len()` entries.
pub fn build_image(
    gen: u64,
    schema_fingerprint: u64,
    parts: &[PartIdent],
    cols: &[ColInput<'_>],
) -> FormatResult<Vec<u8>> {
    let pc = parts.len();
    let nc = cols.len();
    let meta_len =
        BANKSTATS_HEADER_LEN + pc * BANKSTATS_PARTIDENT_LEN + nc * BANKSTATS_COLDIR_LEN + 4;
    // Column payloads first (need offsets before the dir encodes).
    let mut payloads: Vec<Vec<u8>> = Vec::with_capacity(nc);
    for c in cols {
        if c.stats_bodies.len() != pc || c.digests.len() != pc {
            return Err(FormatError::Corrupt {
                at: "bankstats col input arity",
            });
        }
        let mut p = Vec::new();
        let mut rel = 0u64;
        for b in &c.stats_bodies {
            put_u64(&mut p, rel);
            rel += b.len() as u64;
        }
        put_u64(&mut p, rel);
        for b in &c.stats_bodies {
            p.extend_from_slice(b);
        }
        for d in &c.digests {
            match d {
                Some(bytes) => p.extend_from_slice(bytes),
                None => p.extend_from_slice(&[0u8; BANKSTATS_DIGEST_LEN]),
            }
        }
        payloads.push(p);
    }
    let mut out = Vec::new();
    // Header.
    put_u32(&mut out, BANKSTATS_MAGIC);
    put_u32(&mut out, BANKSTATS_VERSION);
    put_u64(&mut out, gen);
    put_u64(&mut out, schema_fingerprint);
    put_u32(&mut out, pc as u32);
    put_u32(&mut out, nc as u32);
    put_u64(&mut out, meta_len as u64);
    put_u32(&mut out, 0); // flags
    put_u32(&mut out, 0); // pad
    debug_assert_eq!(out.len(), BANKSTATS_HEADER_LEN);
    for p in parts {
        put_u32(&mut out, p.part_no);
        put_u32(&mut out, p.granule_count);
        put_u32(&mut out, p.band_count);
        put_u32(&mut out, 0);
        put_u64(&mut out, p.rows);
        put_u64(&mut out, p.footer_off);
    }
    // Column dir: compute payload offsets (8-aligned after meta).
    let mut off = (meta_len as u64 + 7) & !7;
    for (i, c) in cols.iter().enumerate() {
        let pl = &payloads[i];
        let stats_len = pl.len() as u64
            - 8 * (pc as u64 + 1)
            - (pc * BANKSTATS_DIGEST_LEN) as u64;
        put_u32(&mut out, c.attno);
        put_u32(&mut out, 0);
        put_u64(&mut out, off);
        put_u64(&mut out, pl.len() as u64);
        put_u64(&mut out, stats_len);
        put_u32(&mut out, crc32c(pl));
        put_u32(&mut out, 0);
        off = (off + pl.len() as u64 + 7) & !7;
    }
    let mc = crc32c(&out);
    out.extend_from_slice(&mc.to_le_bytes());
    debug_assert_eq!(out.len(), meta_len);
    // Payloads at their promised (8-aligned) offsets.
    for pl in &payloads {
        while out.len() % 8 != 0 {
            out.push(0);
        }
        out.extend_from_slice(pl);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Decode + validate the meta region (header, part idents, column dir).
/// `bytes` must hold at least `meta_len` bytes (callers read the 48-B
/// header first to learn `meta_len`).
pub fn decode_meta(
    bytes: &[u8],
) -> FormatResult<(BankStatsHeader, Vec<PartIdent>, Vec<ColDirEntry>)> {
    let mut c = Cur::new(bytes);
    let magic = c.u32("bankstats header")?;
    if magic != BANKSTATS_MAGIC {
        return Err(FormatError::BadMagic { at: "bankstats" });
    }
    let version = c.u32("bankstats header")?;
    if version != BANKSTATS_VERSION {
        return Err(FormatError::BadVersion {
            at: "bankstats",
            got: version,
        });
    }
    let h = BankStatsHeader {
        gen: c.u64("bankstats header")?,
        schema_fingerprint: c.u64("bankstats header")?,
        part_count: c.u32("bankstats header")?,
        ncols: c.u32("bankstats header")?,
        meta_len: c.u64("bankstats header")?,
        flags: c.u32("bankstats header")?,
    };
    let _pad = c.u32("bankstats header")?;
    let pc = h.part_count as usize;
    let nc = h.ncols as usize;
    let meta_len = BANKSTATS_HEADER_LEN
        + pc * BANKSTATS_PARTIDENT_LEN
        + nc * BANKSTATS_COLDIR_LEN
        + 4;
    if h.meta_len as usize != meta_len || bytes.len() < meta_len {
        return Err(FormatError::Corrupt {
            at: "bankstats meta_len",
        });
    }
    let mut parts = Vec::with_capacity(pc);
    for _ in 0..pc {
        let part_no = c.u32("bankstats partident")?;
        let granule_count = c.u32("bankstats partident")?;
        let band_count = c.u32("bankstats partident")?;
        let _pad = c.u32("bankstats partident")?;
        parts.push(PartIdent {
            part_no,
            granule_count,
            band_count,
            rows: c.u64("bankstats partident")?,
            footer_off: c.u64("bankstats partident")?,
        });
    }
    let mut cols = Vec::with_capacity(nc);
    for _ in 0..nc {
        let attno = c.u32("bankstats coldir")?;
        let flags = c.u32("bankstats coldir")?;
        let off = c.u64("bankstats coldir")?;
        let len = c.u64("bankstats coldir")?;
        let stats_len = c.u64("bankstats coldir")?;
        let crc = c.u32("bankstats coldir")?;
        let pad = c.u32("bankstats coldir")?;
        if pad != 0 {
            return Err(FormatError::Corrupt {
                at: "bankstats coldir pad",
            });
        }
        cols.push(ColDirEntry {
            attno,
            flags,
            off,
            len,
            stats_len,
            crc,
        });
    }
    let stored = u32::from_le_bytes(
        bytes[meta_len - 4..meta_len]
            .try_into()
            .expect("len 4"),
    );
    if stored != crc32c(&bytes[..meta_len - 4]) {
        return Err(FormatError::CrcMismatch { at: "bankstats meta" });
    }
    Ok((h, parts, cols))
}

/// Validated view over one column payload (the bytes at
/// `[entry.off, entry.off+entry.len)`, crc-checked).
pub struct ColPayloadRef<'a> {
    offtab: &'a [u8],
    stats: &'a [u8],
    digests: &'a [u8],
    part_count: usize,
}

impl<'a> ColPayloadRef<'a> {
    pub fn new(
        entry: &ColDirEntry,
        part_count: u32,
        payload: &'a [u8],
    ) -> FormatResult<ColPayloadRef<'a>> {
        if payload.len() as u64 != entry.len {
            return Err(FormatError::Truncated {
                at: "bankstats col payload",
            });
        }
        if crc32c(payload) != entry.crc {
            return Err(FormatError::CrcMismatch {
                at: "bankstats col payload",
            });
        }
        Self::new_validated(entry, part_count, payload)
    }

    /// Re-view a payload that [`ColPayloadRef::new`] ALREADY crc-validated
    /// (the reader validates once at fault and keeps the buffer resident;
    /// re-running the crc per consult would be a per-rep tax the size of
    /// the column payload). Shape checks only.
    pub fn new_validated(
        entry: &ColDirEntry,
        part_count: u32,
        payload: &'a [u8],
    ) -> FormatResult<ColPayloadRef<'a>> {
        if payload.len() as u64 != entry.len {
            return Err(FormatError::Truncated {
                at: "bankstats col payload",
            });
        }
        let pc = part_count as usize;
        let ot_len = 8 * (pc + 1);
        let dg_len = pc * BANKSTATS_DIGEST_LEN;
        let want = ot_len as u64 + entry.stats_len + dg_len as u64;
        if entry.len != want {
            return Err(FormatError::Corrupt {
                at: "bankstats col payload shape",
            });
        }
        let (offtab, rest) = payload.split_at(ot_len);
        let (stats, digests) = rest.split_at(entry.stats_len as usize);
        // offtab sanity: monotone, ends at stats_len.
        let rel = |i: usize| {
            u64::from_le_bytes(offtab[i * 8..i * 8 + 8].try_into().expect("len 8"))
        };
        let mut prev = 0u64;
        for i in 0..=pc {
            let v = rel(i);
            if v < prev || v > entry.stats_len {
                return Err(FormatError::Corrupt {
                    at: "bankstats offtab",
                });
            }
            prev = v;
        }
        if prev != entry.stats_len {
            return Err(FormatError::Corrupt {
                at: "bankstats offtab tail",
            });
        }
        Ok(ColPayloadRef {
            offtab,
            stats,
            digests,
            part_count: pc,
        })
    }

    fn rel(&self, i: usize) -> u64 {
        u64::from_le_bytes(self.offtab[i * 8..i * 8 + 8].try_into().expect("len 8"))
    }

    /// The part's unwrapped §8.1 Stats body for this column; `None` when
    /// the part carries no Stats section for it (empty slice on the wire).
    pub fn stats_body(&self, part_ix: usize) -> Option<&'a [u8]> {
        if part_ix >= self.part_count {
            return None;
        }
        let (a, b) = (self.rel(part_ix) as usize, self.rel(part_ix + 1) as usize);
        if a == b {
            None
        } else {
            Some(&self.stats[a..b])
        }
    }

    /// The part's raw 24-B PartDigest record; `None` when absent
    /// (all-zero on the wire — a version-0 record cannot exist).
    pub fn digest(&self, part_ix: usize) -> Option<&'a [u8]> {
        if part_ix >= self.part_count {
            return None;
        }
        let d = &self.digests
            [part_ix * BANKSTATS_DIGEST_LEN..(part_ix + 1) * BANKSTATS_DIGEST_LEN];
        if d.iter().all(|&b| b == 0) {
            None
        } else {
            Some(d)
        }
    }
}
