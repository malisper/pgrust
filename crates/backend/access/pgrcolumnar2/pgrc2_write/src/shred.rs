//! Shredded-jsonb dual-store emission (rulings O-4/O-9; spec §6.5/§17).
//!
//! **Dual-store** (O-4, the posture of record): the verbatim IMAGE lane
//! (path_ord 0 — the parent column itself, byte-exact per the detoast law)
//! is ALWAYS emitted; typed path lanes are additional row-aligned substreams
//! with `path_ord != 0`, mapped by the part-level `PathTable` section
//! (spec §6.5). Reconstruction-read is NOT an M3 deliverable — the image
//! lane is the read truth until the elision flip.
//!
//! **The lane seam.** Path election + document walking is the vendored
//! `jsonb_shred` machinery — an M3-B deliverable that has NOT been vendored
//! yet (no `jsonb_shred` crate exists in-tree at this lane's freeze base).
//! The dual-store STRUCTURE is therefore built against [`ShredLaneSource`]:
//! a source derives row-aligned typed lanes from the accumulated parent
//! column at seal time (whole-part derivation — exactly the grain the
//! shred election needs). When M3-B lands, the real `jsonb_shred`-backed
//! source plugs in here; nothing in the emission path changes. The default
//! [`NoShred`] emits the image lane only — a valid dual-store degenerate
//! (O-9 hint-primary: no hints, no auto-elections, no lanes).
//!
//! **O-9 enforcement**: `pgrc2_shred_max_paths` is the path budget — a
//! source proposing more lanes than the budget is a typed contract error
//! (the budget rides the reloption vocabulary, spec §17). Shred elections
//! are per-part permanent by construction here: lanes are derived at seal,
//! per part, and recorded in that part's PathTable.

use pgrc2_format::relopt::ShredOptions;
use pgrc2_format::wire::{put_u16, put_u32};

use crate::ingest::ColBuffer;
use crate::{WriteError, WriteResult};

/// One derived typed lane: row-aligned with its parent column.
pub struct ShredLane {
    pub parent_attno: u32,
    /// The shred path string (vendored `jsonb_shred` grammar once M3-B
    /// lands; the format pins only the PathTable framing, spec §6.5).
    pub path: String,
    pub col: ColBuffer,
    /// NumericFs lanes: the chunk-shared decimal scale (RULED 2026-08-14
    /// — persisted in the lane Values entry's aux32 under
    /// STREAMF_LANE_SCALE). `None` for every other lane kind.
    pub scale: Option<i32>,
}

/// The seam the vendored shred machinery plugs into (M3-B/M3-C follow-on).
pub trait ShredLaneSource {
    /// Derive typed lanes from the accumulated parent column. Contract:
    /// every lane is row-aligned (`lane.col.rows() == parent.rows()`) and
    /// lane order is deterministic (part bytes law).
    fn shred(&mut self, parent: &ColBuffer, opts: &ShredOptions) -> WriteResult<Vec<ShredLane>>;
}

/// The M3-D default: image lane only.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoShred;

impl ShredLaneSource for NoShred {
    fn shred(&mut self, _parent: &ColBuffer, _opts: &ShredOptions) -> WriteResult<Vec<ShredLane>> {
        Ok(Vec::new())
    }
}

/// Validate lanes against the parent + the O-9 budget.
pub fn validate_lanes(
    parent: &ColBuffer,
    lanes: &[ShredLane],
    opts: &ShredOptions,
) -> WriteResult<()> {
    if lanes.len() as i64 > opts.max_paths as i64 {
        return Err(WriteError::Contract {
            detail: "shred path budget exceeded (pgrc2_shred_max_paths)",
        });
    }
    for lane in lanes {
        if lane.col.rows() != parent.rows() {
            return Err(WriteError::Contract {
                detail: "shred lane not row-aligned with parent",
            });
        }
        if lane.parent_attno != parent.schema.attno {
            return Err(WriteError::Contract {
                detail: "shred lane parent attno mismatch",
            });
        }
    }
    Ok(())
}

/// Encode the part-level PathTable section body (spec §6.5):
/// `{ count: u32, entries: [{ len: u16, bytes… }] }`, entries 4-byte
/// aligned. `paths[i]` is path_ord `i + 1` (path_ord 0 = root, never
/// listed).
///
/// The per-entry length rides a u16 on-disk field, so a path whose byte
/// length exceeds `u16::MAX` cannot be framed honestly. Shred paths are
/// minted verbatim from attacker-supplied jsonb keys (which the vendored
/// machinery allows up to the 28-bit JEntry bound, far past 64 KiB), so a
/// silent `as u16` cast would truncate the declared length modulo 65536
/// while still emitting the full path bytes — an internally inconsistent
/// section that lets a reader decode an aliased (attacker-chosen) path.
/// Reject such a path with a typed contract error instead of truncating;
/// valid paths (≤ `u16::MAX` bytes) round-trip unchanged.
pub fn encode_path_table(paths: &[&str]) -> WriteResult<Vec<u8>> {
    let mut out = Vec::new();
    put_u32(&mut out, paths.len() as u32);
    for p in paths {
        // 4-byte-align each entry's start.
        let rem = out.len() % 4;
        if rem != 0 {
            out.resize(out.len() + (4 - rem), 0);
        }
        let len = u16::try_from(p.len()).map_err(|_| WriteError::Contract {
            detail: "shred path length exceeds PathTable u16 framing",
        })?;
        put_u16(&mut out, len);
        out.extend_from_slice(p.as_bytes());
    }
    Ok(out)
}

/// Decode a PathTable body (the round-trip witness for the emission tests;
/// readers get their own in M3-F).
pub fn decode_path_table(bytes: &[u8]) -> WriteResult<Vec<String>> {
    let corrupt = |_: &'static str| WriteError::Contract {
        detail: "corrupt path table",
    };
    if bytes.len() < 4 {
        return Err(corrupt("len"));
    }
    let count = u32::from_le_bytes(bytes[..4].try_into().expect("len 4")) as usize;
    let mut off = 4usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        off = off.div_ceil(4) * 4;
        let len_bytes = bytes.get(off..off + 2).ok_or_else(|| corrupt("entry len"))?;
        let len = u16::from_le_bytes(len_bytes.try_into().expect("len 2")) as usize;
        off += 2;
        let s = bytes.get(off..off + len).ok_or_else(|| corrupt("entry bytes"))?;
        out.push(
            String::from_utf8(s.to_vec()).map_err(|_| corrupt("utf8"))?,
        );
        off += len;
    }
    Ok(out)
}
