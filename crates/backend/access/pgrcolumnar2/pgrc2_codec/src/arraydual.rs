//! ARRAY_DUAL — the A9 sizes+elements dual-substream election (spec §4
//! id 10; child-substream mechanics per spec §6.5: sizes-of-sizes is the
//! single nesting currency).
//!
//! id 10 is a STRUCTURAL election: no §6.4 section carries it in its
//! header. An elected array column stores a row-aligned `Sizes` stream and
//! a dense `ChildValues` stream (per-granule value counts in the child's
//! gcount table), each under its own elected integer encoding (BYTE_FOR
//! primary — "element encodings recurse"); the encoding id 10 is recorded
//! at the manifest-election grain (M3-D's witness fields). The composition
//! back into array datums is [`assemble_array_datums`] — the decode leg of
//! the pair, invoked by the reader/scan after the two child decodes
//! (`KernelCtx` is single-section by ABI construction, so a one-section
//! kernel cannot compose two streams; spec §19.2).
//!
//! Election predicate (INPUT-DECIDABLE, refusal demotes to VERBATIM):
//! 1-D, lower bound 1, no null bitmap (`dataoffset == 0`), element type ==
//! the catalog-derived facts the writer supplies (never an OID table —
//! charter §3), fixed-width byval elements, and exact length arithmetic.
//! Empty arrays (`ndim == 0`, the PG canonical `'{}'` image) are legal and
//! reassemble byte-identically. Anything else — nulls-in-elements,
//! multidim, exotic lbounds, byref elements — refuses; the round-trip
//! verify would catch a wrong acceptance as a byte mismatch.

use crate::section::varlena_payload;
use pgrc2_format::abi::{ByteArena, EncodeInput};
use pgrc2_format::wire::varlena_header_4b_u;
use pgrc2_format::{FormatError, FormatResult};

/// Payload-relative offsets of the PG array header (past the 4-byte varlena
/// header): ndim@0, dataoffset@4, elemtype@8, dims[0]@12, lbound[0]@16,
/// data@20 (== MAXALIGN(24) − VARHDRSZ for ndim 1).
const HDR_1D: usize = 20;
/// ndim-0 (empty) images are exactly ndim+dataoffset+elemtype.
const HDR_EMPTY: usize = 12;

/// Catalog-derived element facts (writer-supplied; charter §3 — properties,
/// never an OID table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArrayElemFacts {
    /// pg_type oid of the element (validated against stored images; echoed
    /// into rebuilt ones).
    pub elemtype: u32,
    /// Element width in bytes: 1/2/4/8 (fixed-width byval only).
    pub elem_len: u8,
}

/// One granule's split output: `sizes` row-aligned (null rows 0), `elems`
/// dense in row order (zero-extended element words).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ArraySplit {
    pub sizes: Vec<u64>,
    pub elems: Vec<u64>,
}

/// Split one granule of array datums, or None (refusal — demote).
pub fn elect_array_split(
    input: &EncodeInput<'_>,
    facts: ArrayElemFacts,
) -> FormatResult<Option<ArraySplit>> {
    if !matches!(facts.elem_len, 1 | 2 | 4 | 8) {
        return Ok(None);
    }
    let w = facts.elem_len as usize;
    let mut out = ArraySplit::default();
    for r in 0..input.rows as usize {
        if !input.valid(r as u32) {
            out.sizes.push(0);
            continue;
        }
        // SAFETY: EncodeInput pointer-class contract (spec §19.6).
        let p = unsafe { varlena_payload(input.datums[r])? };
        if p.len() < HDR_EMPTY {
            return Ok(None);
        }
        let ndim = i32::from_le_bytes(p[0..4].try_into().expect("len 4"));
        let dataoffset = i32::from_le_bytes(p[4..8].try_into().expect("len 4"));
        let elemtype = u32::from_le_bytes(p[8..12].try_into().expect("len 4"));
        if elemtype != facts.elemtype || dataoffset != 0 {
            return Ok(None);
        }
        match ndim {
            0 => {
                if p.len() != HDR_EMPTY {
                    return Ok(None);
                }
                out.sizes.push(0);
            }
            1 => {
                if p.len() < HDR_1D {
                    return Ok(None);
                }
                let count = i32::from_le_bytes(p[12..16].try_into().expect("len 4"));
                let lbound = i32::from_le_bytes(p[16..20].try_into().expect("len 4"));
                if lbound != 1 || count <= 0 {
                    return Ok(None);
                }
                if p.len() != HDR_1D + count as usize * w {
                    return Ok(None);
                }
                out.sizes.push(count as u64);
                for i in 0..count as usize {
                    let off = HDR_1D + i * w;
                    let mut word = [0u8; 8];
                    word[..w].copy_from_slice(&p[off..off + w]);
                    out.elems.push(u64::from_le_bytes(word));
                }
            }
            _ => return Ok(None),
        }
    }
    Ok(Some(out))
}

/// Exact reassembled arena bytes the split will need at decode (sizing).
pub fn assembled_arena_bytes(facts: ArrayElemFacts, sizes: &[u64]) -> usize {
    sizes
        .iter()
        .map(|&c| {
            let payload = if c == 0 {
                HDR_EMPTY
            } else {
                HDR_1D + c as usize * facts.elem_len as usize
            };
            // 4-byte varlena header + payload, 8-aligned per entry.
            (4 + payload).div_ceil(8) * 8
        })
        .sum()
}

/// The decode leg: rebuild array datums from decoded sizes + element words.
///
/// `sizes[..rows]` row-aligned (invalid rows MUST carry 0 — the split's
/// placeholder), `elems` dense in row order, `valid(row)` the column
/// validity. Outputs one datum word per row into `out[..rows]` (null rows
/// unspecified — validity is the only truth). Byte-identical to the stored
/// images for everything [`elect_array_split`] accepted.
pub fn assemble_array_datums(
    facts: ArrayElemFacts,
    sizes: &[u64],
    elems: &[u64],
    valid: impl Fn(u32) -> bool,
    out: &mut [u64],
    arena: &mut ByteArena<'_>,
) -> FormatResult<()> {
    if out.len() < sizes.len() {
        return Err(FormatError::Bounds {
            at: "array assemble out",
        });
    }
    let w = facts.elem_len as usize;
    if !matches!(w, 1 | 2 | 4 | 8) {
        return Err(FormatError::Corrupt {
            at: "array element width",
        });
    }
    let mut e = 0usize;
    for (r, &count) in sizes.iter().enumerate() {
        if !valid(r as u32) {
            if count != 0 {
                return Err(FormatError::Corrupt {
                    at: "null array with elements",
                });
            }
            continue;
        }
        let count = count as usize;
        let payload_len = if count == 0 {
            HDR_EMPTY
        } else {
            HDR_1D + count * w
        };
        let slot = arena.alloc(4 + payload_len)?;
        slot[0..4].copy_from_slice(&varlena_header_4b_u(payload_len as u32).to_le_bytes());
        let p = &mut slot[4..];
        if count == 0 {
            p[0..4].copy_from_slice(&0i32.to_le_bytes());
            p[4..8].copy_from_slice(&0i32.to_le_bytes());
            p[8..12].copy_from_slice(&facts.elemtype.to_le_bytes());
        } else {
            p[0..4].copy_from_slice(&1i32.to_le_bytes());
            p[4..8].copy_from_slice(&0i32.to_le_bytes());
            p[8..12].copy_from_slice(&facts.elemtype.to_le_bytes());
            p[12..16].copy_from_slice(&(count as i32).to_le_bytes());
            p[16..20].copy_from_slice(&1i32.to_le_bytes());
            let need = elems.get(e..e + count).ok_or(FormatError::Corrupt {
                at: "array element underflow",
            })?;
            for (i, &word) in need.iter().enumerate() {
                let off = HDR_1D + i * w;
                p[off..off + w].copy_from_slice(&word.to_le_bytes()[..w]);
            }
            e += count;
        }
        out[r] = slot.as_ptr() as u64;
    }
    if e != elems.len() {
        return Err(FormatError::Corrupt {
            at: "array element overflow",
        });
    }
    Ok(())
}
