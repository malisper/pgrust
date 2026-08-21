//! Delete-vector application state (DM-2 — the AB-3.3 verdict-layer leg).
//!
//! The frozen §15 DV vocabulary (`pgrc2_format::dml`) is decoded ONCE per
//! part into granule-keyed 8,192-bit masks; the scan drive intersects the
//! mask into each staged window's SELECTION (the one row currency —
//! `pgrc2_batch::Selection`'s own doc law: "delete masks (ducklake) and
//! visibility compose into this one currency"). Kernels iterate the
//! selection, never `0..nrows`, so a deleted row is dead to every
//! downstream consumer — filters, aggregates, COUNT(*), canon bytes — by
//! construction, not by per-kernel cooperation.
//!
//! Authoritative-sidecar law (spec §15/§16): a part whose manifest record
//! carries `dv_gen != 0` REFERENCES its DV; the caller validates the
//! manifest triplet (`dv_gen`, `dv_len`, `dv_crc`) against the sidecar
//! payload BEFORE handing bytes here (a missing or mismatched DV on a
//! deletion-bearing part is CORRUPTION — silently scanning without it
//! would resurrect deleted rows). This module validates the PAYLOAD's own
//! internal consistency: header identity (part_no, gen), granule bounds,
//! ordinal bounds, and the deleted-rows total.

use std::collections::BTreeMap;

use pgrc2_format::dml::{DvBlockKind, DvReader};
use pgrc2_format::geom::GRANULE_ROWS;
use pgrc2_format::{FormatError, FormatResult};

/// 64-bit words per granule mask (8,192 rows).
pub const DV_GRANULE_WORDS: usize = (GRANULE_ROWS as usize) / 64;

/// One part's decoded delete vector: granule-keyed deletion masks.
/// Immutable after build; shared across workers via `Arc`.
#[derive(Debug, Clone)]
pub struct PartDeletes {
    /// granule -> deletion mask (bit r set = granule-row r is deleted).
    granules: BTreeMap<u32, Box<[u64; DV_GRANULE_WORDS]>>,
    /// Total deleted rows in this part (the header fact, re-verified
    /// against the block payloads at build).
    pub deleted_rows: u64,
}

impl PartDeletes {
    /// Decode + validate one DV payload image (the `encode_dv` output —
    /// the sidecar envelope and the manifest triplet are the CALLER's
    /// checks). `part_no`/`dv_gen` bind the header identity;
    /// `granule_count` bounds the block keys.
    pub fn from_dv_payload(
        payload: &[u8],
        part_no: u32,
        dv_gen: u64,
        granule_count: u32,
    ) -> FormatResult<PartDeletes> {
        let mut rd = DvReader::open(payload)?;
        if rd.header.part_no != part_no {
            return Err(FormatError::Corrupt {
                at: "DV header part_no vs manifest",
            });
        }
        if rd.header.gen != dv_gen {
            return Err(FormatError::Corrupt {
                at: "DV header gen vs manifest dv_gen",
            });
        }
        let mut granules = BTreeMap::new();
        let mut total: u64 = 0;
        while let Some(block) = rd.next_block()? {
            if block.granule >= granule_count {
                return Err(FormatError::Corrupt {
                    at: "DV block granule beyond part granule_count",
                });
            }
            let mut mask = Box::new([0u64; DV_GRANULE_WORDS]);
            let count = match block.kind {
                DvBlockKind::List => {
                    let mut prev: Option<u16> = None;
                    for pair in block.payload.chunks_exact(2) {
                        let r = u16::from_le_bytes([pair[0], pair[1]]);
                        if r as u32 >= GRANULE_ROWS {
                            return Err(FormatError::Corrupt {
                                at: "DV list ordinal beyond granule rows",
                            });
                        }
                        if let Some(p) = prev {
                            if r <= p {
                                return Err(FormatError::Corrupt { at: "DV list order" });
                            }
                        }
                        prev = Some(r);
                        mask[(r / 64) as usize] |= 1u64 << (r % 64);
                    }
                    (block.payload.len() / 2) as u32
                }
                DvBlockKind::Bitmap => {
                    // LSB-first bytes -> LE words: bit r of the granule is
                    // word[r/64] >> (r%64), byte-for-byte.
                    for (w, chunk) in block.payload.chunks_exact(8).enumerate() {
                        mask[w] = u64::from_le_bytes(chunk.try_into().expect("chunks_exact(8)"));
                    }
                    mask.iter().map(|w| w.count_ones() as u32).sum()
                }
            };
            if count != block.count {
                return Err(FormatError::Corrupt {
                    at: "DV block count vs payload",
                });
            }
            if count == 0 {
                // A zero-count block is legal wire but carries nothing —
                // never keyed (granule lookup stays None = no deletions).
                continue;
            }
            total += count as u64;
            granules.insert(block.granule, mask);
        }
        if total != rd.header.deleted_rows {
            return Err(FormatError::Corrupt {
                at: "DV deleted_rows vs block sum",
            });
        }
        Ok(PartDeletes {
            granules,
            deleted_rows: total,
        })
    }

    /// The granule's deletion mask; `None` = no deletions in this granule.
    #[inline]
    pub fn granule_mask(&self, g: u32) -> Option<&[u64; DV_GRANULE_WORDS]> {
        self.granules.get(&g).map(|b| &**b)
    }

    /// True when the mask marks granule-row `row` deleted.
    #[inline]
    pub fn is_deleted(mask: &[u64; DV_GRANULE_WORDS], row: u32) -> bool {
        (mask[(row / 64) as usize] >> (row % 64)) & 1 == 1
    }
}
