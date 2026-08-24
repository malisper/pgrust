//! Sidecar directory vocabulary (spec §16; charter §10): derived state as
//! format citizens. Slot kinds are assigned here only.
//!
//! Two sidecar classes:
//! - **Authoritative** (`Dv`): referenced from the MANIFEST (spec §13.1),
//!   validated by (dv_gen, dv_len, dv_crc) — never keyed on physical part
//!   identity (a copied bank must not forget deletions). Its file header's
//!   `part_uuid` is zero.
//! - **Derived** (`PredicateCache`, `Memo`, `Trgm` — all RESERVED at M3):
//!   the qceil memo envelope verbatim — part-identity keying (`part_uuid` at
//!   build time), whole-body checksum, atomic tmp+rename,
//!   refuse-and-rebuild on ANY mismatch, HIT/BUILT/STALE witnesses.

use crate::wire::{crc32c, put_u16, put_u32, put_u64, Cur};
use crate::{FormatError, FormatResult, FORMAT_VERSION};

/// Sidecar file magic: ASCII "PRCS".
pub const SIDECAR_MAGIC: u32 = u32::from_le_bytes(*b"PRCS");

/// Slot kinds (spec §16). 6..=31 reserved for A-lane assignment.
///
/// v4 commitment posture (format-ledger row FT-7, OD-1 RULED 2026-08-12):
/// slot IDs are allocated for all five (append-only vocabulary, costless);
/// **format-COMMITTED slots are exactly `Dv` (DM-2) and `Stats` (ST-1/OD-2)**
/// — their consumers are chartered and their writers land at M3/M5.
/// `PredicateCache`/`Memo`/`Trgm` stay RESERVED-uncommitted: no reader or
/// writer obligation; any future go-live rides the planned-16 landing gate
/// (owner/invalidation story or recorded DROP). PredicateCache additionally
/// carries the standing cross-query-caches deny posture (deny-list DL-2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum SidecarKind {
    /// Delete vector (spec §15) — AUTHORITATIVE; format frozen, writer M5.
    /// COMMITTED (OD-1/OD-3): the DM-2 deletion representation of record.
    Dv = 1,
    /// RESERVED-uncommitted (O-11 + OD-1): persisted predicate-bitmap cache;
    /// v1 is in-memory. Standing cross-query-caches deny posture (DL-2).
    PredicateCache = 2,
    /// RESERVED-uncommitted (OD-1; M4 builders; O-12 reporting tier — never
    /// a headline).
    Memo = 3,
    /// RESERVED-uncommitted (OD-1; Michael's 2026-08-09 Trgm refusal stands
    /// — nothing owed).
    Trgm = 4,
    /// Distribution-stats sketches (ST-1, OD-2 RULED 2026-08-12): per-part
    /// MERGEABLE sketches written AT SEAL — MCV top-k with exact per-part
    /// counts + histogram/quantile bounds (HLL NDV registers stay in-footer,
    /// spec §8.4). Exactness class EXACT-AT-SEAL, per part-generation;
    /// ANALYZE = fold-from-sketches into pg_statistic (ST-2). COMMITTED:
    /// the M3 stats build (planned-33 supply half) writes it; banks ship
    /// with stats built. Part-identity keyed (regenerated at DM-3
    /// compaction with the part's fresh generation).
    Stats = 5,
}

impl SidecarKind {
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    pub fn from_u16(v: u16) -> FormatResult<SidecarKind> {
        match v {
            1 => Ok(SidecarKind::Dv),
            2 => Ok(SidecarKind::PredicateCache),
            3 => Ok(SidecarKind::Memo),
            4 => Ok(SidecarKind::Trgm),
            5 => Ok(SidecarKind::Stats),
            _ => Err(FormatError::Corrupt { at: "SidecarKind" }),
        }
    }

    /// The file-name tag (spec §12).
    pub const fn tag(self) -> &'static str {
        match self {
            SidecarKind::Dv => "dv",
            SidecarKind::PredicateCache => "pcache",
            SidecarKind::Memo => "memo",
            SidecarKind::Trgm => "trgm",
            SidecarKind::Stats => "stats",
        }
    }

    /// Authoritative sidecars validate against the manifest; derived ones
    /// against physical part identity (refuse-and-rebuild). `Stats` is
    /// part-identity keyed (derived class) even though COMMITTED: its
    /// content is exact-at-seal per part-generation, and DM-3 compaction
    /// regenerates it with the fresh part rather than referencing it from
    /// the manifest.
    pub const fn authoritative(self) -> bool {
        matches!(self, SidecarKind::Dv)
    }
}

pub const SIDECAR_SLOT_RECORD_LEN: usize = 48;

/// One footer-listed slot record (48 B, spec §16). Wire order == declaration
/// order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SidecarSlotRecord {
    pub key_fingerprint: u64,
    pub gen: u64,
    pub len: u64,
    /// Build cost in nanoseconds (builder-measured via the routed clock;
    /// recorded fact, not a determinism surface — sidecars are derived
    /// state, not part bytes).
    pub build_cost_ns: u64,
    pub kind: u16,
    pub flags: u16,
    pub version: u32,
    pub crc: u32,
    pub pad: u32,
}

impl SidecarSlotRecord {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        put_u64(out, self.key_fingerprint);
        put_u64(out, self.gen);
        put_u64(out, self.len);
        put_u64(out, self.build_cost_ns);
        put_u16(out, self.kind);
        put_u16(out, self.flags);
        put_u32(out, self.version);
        put_u32(out, self.crc);
        put_u32(out, self.pad);
    }

    pub fn decode(c: &mut Cur<'_>) -> FormatResult<SidecarSlotRecord> {
        let r = SidecarSlotRecord {
            key_fingerprint: c.u64("SidecarSlotRecord")?,
            gen: c.u64("SidecarSlotRecord")?,
            len: c.u64("SidecarSlotRecord")?,
            build_cost_ns: c.u64("SidecarSlotRecord")?,
            kind: c.u16("SidecarSlotRecord")?,
            flags: c.u16("SidecarSlotRecord")?,
            version: c.u32("SidecarSlotRecord")?,
            crc: c.u32("SidecarSlotRecord")?,
            pad: c.u32("SidecarSlotRecord")?,
        };
        SidecarKind::from_u16(r.kind)?;
        Ok(r)
    }
}

pub const SIDECAR_FILE_HEADER_LEN: usize = 48;

/// Companion-file header (48 B, spec §16): `[header][payload][crc32c u32]`,
/// written via atomic tmp+rename.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct SidecarFileHeader {
    /// Build-time physical identity for DERIVED sidecars; zero for
    /// authoritative kinds (they validate against the manifest).
    pub part_uuid: [u8; 16],
    pub key_fingerprint: u64,
    pub payload_len: u64,
    pub magic: u32,
    pub kind: u16,
    pub flags: u16,
    pub version: u32,
    pub pad: u32,
}

impl SidecarFileHeader {
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.part_uuid);
        put_u64(out, self.key_fingerprint);
        put_u64(out, self.payload_len);
        put_u32(out, self.magic);
        put_u16(out, self.kind);
        put_u16(out, self.flags);
        put_u32(out, self.version);
        put_u32(out, self.pad);
    }

    pub fn decode(bytes: &[u8]) -> FormatResult<SidecarFileHeader> {
        let mut c = Cur::new(bytes);
        let part_uuid: [u8; 16] = c.take(16, "SidecarFileHeader")?.try_into().expect("len 16");
        let h = SidecarFileHeader {
            part_uuid,
            key_fingerprint: c.u64("SidecarFileHeader")?,
            payload_len: c.u64("SidecarFileHeader")?,
            magic: c.u32("SidecarFileHeader")?,
            kind: c.u16("SidecarFileHeader")?,
            flags: c.u16("SidecarFileHeader")?,
            version: c.u32("SidecarFileHeader")?,
            pad: c.u32("SidecarFileHeader")?,
        };
        if h.magic != SIDECAR_MAGIC {
            return Err(FormatError::BadMagic {
                at: "SidecarFileHeader",
            });
        }
        if h.version != FORMAT_VERSION {
            return Err(FormatError::BadVersion {
                at: "SidecarFileHeader",
                got: h.version,
            });
        }
        SidecarKind::from_u16(h.kind)?;
        Ok(h)
    }

    /// Validate a whole sidecar file image (header + payload + trailing crc).
    /// Returns the payload slice. Any mismatch is a typed refusal — the
    /// refuse-and-rebuild law for derived kinds.
    pub fn validate_file<'a>(bytes: &'a [u8]) -> FormatResult<(SidecarFileHeader, &'a [u8])> {
        if bytes.len() < SIDECAR_FILE_HEADER_LEN + 4 {
            return Err(FormatError::Truncated { at: "sidecar file" });
        }
        let h = SidecarFileHeader::decode(bytes)?;
        let end = SIDECAR_FILE_HEADER_LEN
            .checked_add(h.payload_len as usize)
            .ok_or(FormatError::Bounds {
                at: "sidecar payload_len",
            })?;
        if end + 4 != bytes.len() {
            return Err(FormatError::Corrupt {
                at: "sidecar file length",
            });
        }
        let body = &bytes[..end];
        let stored = u32::from_le_bytes(bytes[end..end + 4].try_into().expect("len 4"));
        if stored != crc32c(body) {
            return Err(FormatError::CrcMismatch { at: "sidecar file" });
        }
        Ok((h, &bytes[SIDECAR_FILE_HEADER_LEN..end]))
    }
}

/// Build witnesses (charter §10): every sidecar consult reports one of these
/// — measured-only counters ride them (M3-F/M4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SidecarWitness {
    Hit,
    Built,
    Stale,
}

// ---------------------------------------------------------------------------
// The Stats sidecar payload (ST-1, OD-2 — the COMMITTED slot's schema)
// ---------------------------------------------------------------------------

/// Stats sidecar payload version (independent of `FORMAT_VERSION`: the
/// envelope's `version` field pins the format; this pins the payload).
pub const STATS_PAYLOAD_VERSION: u32 = 1;
/// MCV top-k retained per column.
pub const STATS_MCV_K: usize = 32;
/// Histogram bound count (STATS_HIST_BOUNDS - 1 equi-depth buckets).
pub const STATS_HIST_BOUNDS: usize = 33;
/// Values longer than this participate in COUNTS but are ineligible for
/// the value lists (mcv/histogram) — recorded per column as `long_values`.
pub const STATS_MCV_VALUE_MAX: usize = 256;
/// Minimum on-wire size of one per-column record's fixed prefix (attno u32,
/// path_ord u32, nonnull u64, ndv_eligible u64, long_values u64, nmcv u32,
/// nhist u32) before any variable mcv/hist bytes. Used to bound the
/// wire-claimed column count against the bytes actually present, so a tiny
/// hostile payload cannot command a huge up-front reservation (CWE-789).
pub const STATS_COL_RECORD_MIN_LEN: usize = 40;

/// One column's part-grain distribution sketch (OD-2 exactness class:
/// EXACT-AT-SEAL, per part-generation, mergeable): MCV top-k with exact
/// per-part counts + equi-depth histogram bounds over the canonical value
/// bytes. NDV stays the footer's HLL (spec §8.4); `ndv_eligible` here is
/// the exact distinct count among list-ELIGIBLE values (len ≤
/// [`STATS_MCV_VALUE_MAX`]) — advisory next to the HLL, never its
/// replacement.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ColDistribution {
    pub nonnull: u64,
    pub ndv_eligible: u64,
    pub long_values: u64,
    /// (canonical value bytes, exact part count), count-descending with
    /// byte-order tiebreak — deterministic (byte-identical-parts law).
    pub mcv: Vec<(Vec<u8>, u64)>,
    /// Equi-depth bounds over eligible nonnull values, byte order,
    /// first == min, last == max (≤ [`STATS_HIST_BOUNDS`]).
    pub hist_bounds: Vec<Vec<u8>>,
}

/// Encode the whole Stats payload: header + per-(attno, path_ord) records.
pub fn encode_stats_payload(cols: &[(u32, u32, ColDistribution)]) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, STATS_PAYLOAD_VERSION);
    put_u32(&mut out, cols.len() as u32);
    for (attno, path_ord, d) in cols {
        put_u32(&mut out, *attno);
        put_u32(&mut out, *path_ord);
        put_u64(&mut out, d.nonnull);
        put_u64(&mut out, d.ndv_eligible);
        put_u64(&mut out, d.long_values);
        put_u32(&mut out, d.mcv.len() as u32);
        put_u32(&mut out, d.hist_bounds.len() as u32);
        for (bytes, count) in &d.mcv {
            put_u64(&mut out, *count);
            put_u32(&mut out, bytes.len() as u32);
            out.extend_from_slice(bytes);
        }
        for b in &d.hist_bounds {
            put_u32(&mut out, b.len() as u32);
            out.extend_from_slice(b);
        }
    }
    out
}

fn cur_bytes<'a>(c: &mut Cur<'a>, len: usize, at: &'static str) -> FormatResult<&'a [u8]> {
    c.take(len, at)
}

/// Decode a Stats payload (typed refusals; the envelope CRC already ran).
pub fn decode_stats_payload(
    payload: &[u8],
) -> FormatResult<Vec<(u32, u32, ColDistribution)>> {
    let mut c = Cur::new(payload);
    let version = c.u32("StatsPayload")?;
    if version != STATS_PAYLOAD_VERSION {
        return Err(FormatError::Corrupt {
            at: "StatsPayload version",
        });
    }
    let ncols = c.u32("StatsPayload")? as usize;
    // Bound the wire-claimed column count against the bytes actually
    // remaining before reserving: each column record occupies at least
    // STATS_COL_RECORD_MIN_LEN bytes, so a count claiming more records than
    // could possibly fit is corruption — refuse it (typed) rather than let
    // an untrusted length-prefix drive an infallible Vec::with_capacity into
    // an uncatchable handle_alloc_error abort (CWE-789).
    if ncols > c.remaining() / STATS_COL_RECORD_MIN_LEN {
        return Err(FormatError::Corrupt {
            at: "StatsPayload ncols vs length",
        });
    }
    let mut out = Vec::with_capacity(ncols);
    for _ in 0..ncols {
        let attno = c.u32("StatsPayload col")?;
        let path_ord = c.u32("StatsPayload col")?;
        let nonnull = c.u64("StatsPayload col")?;
        let ndv_eligible = c.u64("StatsPayload col")?;
        let long_values = c.u64("StatsPayload col")?;
        let nmcv = c.u32("StatsPayload col")? as usize;
        let nhist = c.u32("StatsPayload col")? as usize;
        if nmcv > STATS_MCV_K || nhist > STATS_HIST_BOUNDS {
            return Err(FormatError::Corrupt {
                at: "StatsPayload list bounds",
            });
        }
        let mut mcv = Vec::with_capacity(nmcv);
        for _ in 0..nmcv {
            let count = c.u64("StatsPayload mcv")?;
            let len = c.u32("StatsPayload mcv")? as usize;
            if len > STATS_MCV_VALUE_MAX {
                return Err(FormatError::Corrupt {
                    at: "StatsPayload mcv value len",
                });
            }
            mcv.push((cur_bytes(&mut c, len, "StatsPayload mcv bytes")?.to_vec(), count));
        }
        let mut hist_bounds = Vec::with_capacity(nhist);
        for _ in 0..nhist {
            let len = c.u32("StatsPayload hist")? as usize;
            if len > STATS_MCV_VALUE_MAX {
                return Err(FormatError::Corrupt {
                    at: "StatsPayload hist bound len",
                });
            }
            hist_bounds.push(cur_bytes(&mut c, len, "StatsPayload hist bytes")?.to_vec());
        }
        out.push((
            attno,
            path_ord,
            ColDistribution {
                nonnull,
                ndv_eligible,
                long_values,
                mcv,
                hist_bounds,
            },
        ));
    }
    if c.remaining() != 0 {
        return Err(FormatError::Corrupt {
            at: "StatsPayload trailing bytes",
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::put_u32;

    /// A tiny payload claiming `ncols = u32::MAX` must be refused (typed)
    /// before any allocation proportional to the claim — no infallible
    /// Vec::with_capacity of 2^32 records (CWE-789).
    #[test]
    fn stats_payload_rejects_huge_ncols_without_allocating() {
        let mut payload = Vec::new();
        put_u32(&mut payload, STATS_PAYLOAD_VERSION);
        put_u32(&mut payload, u32::MAX);
        // No column records follow: remaining bytes cannot back the claim.
        let err = decode_stats_payload(&payload).err().unwrap();
        assert!(matches!(err, FormatError::Corrupt { .. }));
    }

    /// A valid round-trip still decodes: the bound preserves good sidecars.
    #[test]
    fn stats_payload_roundtrip() {
        let cols = vec![
            (
                1u32,
                0u32,
                ColDistribution {
                    nonnull: 10,
                    ndv_eligible: 5,
                    long_values: 1,
                    mcv: vec![(b"abc".to_vec(), 4), (b"de".to_vec(), 2)],
                    hist_bounds: vec![b"a".to_vec(), b"z".to_vec()],
                },
            ),
            (2u32, 1u32, ColDistribution::default()),
        ];
        let payload = encode_stats_payload(&cols);
        let got = decode_stats_payload(&payload).expect("valid payload decodes");
        assert_eq!(got, cols);
    }
}
