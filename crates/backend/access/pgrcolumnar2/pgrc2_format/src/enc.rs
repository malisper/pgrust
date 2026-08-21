//! Encoding registry (spec §4; the M0-S4 frozen matrix). IDs are assigned
//! only here + spec §4, by A-lane PRs. Resolution distinguishes UNKNOWN
//! (never assigned, a typed refusal, never a fallback) from assigned IDs;
//! the RESERVED class (assigned-not-implemented) is EMPTY as of the SB-4 /
//! OD-5 FSST activation (lanev4 ledger, RULED 2026-08-12) — id 12 resolves
//! first-class.

use crate::{FormatError, FormatResult};

/// The assigned encoding IDs (spec §4 table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum EncodingId {
    /// Class-shaped byte-exact fallback (spec §6.7) — the universal demotion
    /// target: refusal demotes, never normalizes.
    Verbatim = 0,
    /// One value per extent (spec §6.9).
    Const = 1,
    /// Byte-aligned FOR, per-frame i64 refs, widths 1/2/4/8 (spec §6.10) —
    /// the int-family primary (M0-S4 frozen).
    ByteFor = 2,
    /// FastLanes-style transposed FFOR, interleave layout — the elected
    /// narrow-width tier for FUSED consumers only (S4 verdict (a)).
    FforInterleave = 3,
    /// Cold/size election only (S4 verdict (b) struck it from hot tiers).
    DeltaFor = 4,
    /// Float primary-by-election, RawF(=Verbatim) fallback (S4 verdict (c)).
    Alp = 5,
    /// ALP-RD for full-mantissa columns (weak trade — expect Verbatim to win
    /// the ≥10% election often).
    AlpRd = 6,
    /// 1-bit bitmap (bool primary; also the validity substream layout,
    /// spec §6.6).
    BoolBitmap = 7,
    /// Bit-packed GLOBAL codes, per-granule base+width (spec §6.11).
    DictCodes = 8,
    /// A6b fixed-scale packed i64 (elected scale in `aux32`).
    PackedNumeric = 9,
    /// A9 sizes+elements dual substreams (children per spec §6.5).
    ArrayDual = 10,
    /// Shredded jsonb path lanes (vendored `jsonb_shred` vocabulary).
    JsonbShred = 11,
    /// FSST (Boncz/Neumann) string compression, first-class per SB-4 with
    /// the OD-5 symbol-table ruling (per-(column,part), table in the
    /// stream's section header bytes, inside CRC-over-wrapped;
    /// FSST-under-zstd is encoding+wrapper — two layers, legal). The id was
    /// burned RESERVED at O-M3-3 and activated by the lanev4 codec chunk.
    Fsst = 12,
}

/// FSST's encoding id, 12. HISTORICAL name: assigned-but-reserved from
/// O-M3-3 until the SB-4 / OD-5 activation made it first-class
/// ([`EncodingId::Fsst`]); the constant survives for the provenance trail
/// and doc links — it equals `EncodingId::Fsst.as_u16()`.
pub const ENC_FSST_RESERVED: u16 = 12;

/// IDs 13..=127 are reserved for A-lane assignment; resolution refuses them
/// as UNKNOWN (they were never assigned).
pub const ENC_RESERVED_MAX: u16 = 127;

impl EncodingId {
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Adjudicate a wire encoding id (spec §4). Born-RED-tested with seeded
    /// unknown IDs in 13..=127 (`tests/refusal.rs`).
    pub fn resolve(id: u16) -> FormatResult<EncodingId> {
        match id {
            0 => Ok(EncodingId::Verbatim),
            1 => Ok(EncodingId::Const),
            2 => Ok(EncodingId::ByteFor),
            3 => Ok(EncodingId::FforInterleave),
            4 => Ok(EncodingId::DeltaFor),
            5 => Ok(EncodingId::Alp),
            6 => Ok(EncodingId::AlpRd),
            7 => Ok(EncodingId::BoolBitmap),
            8 => Ok(EncodingId::DictCodes),
            9 => Ok(EncodingId::PackedNumeric),
            10 => Ok(EncodingId::ArrayDual),
            11 => Ok(EncodingId::JsonbShred),
            12 => Ok(EncodingId::Fsst),
            _ => Err(FormatError::UnknownEncoding { id }),
        }
    }
}

/// Normalize stream-entry fields to the [`KernelKey`] a codec registered
/// (spec §6.3 + §19.5, A-lane amendment M3-A2): the entry's `width` byte is
/// **per-encoding vocabulary** (ByvalWord width, BYTE_FOR delta width,
/// DICT_CODES max code width — for DICT_CODES a stats fact, never a dispatch
/// axis), while `KernelKey.width` is the **normalized dispatch width**. This
/// is the SINGLE mapping every resolver consults — the read side
/// (`pgrc2_read` cursor open + every ctx it builds) and the write side's
/// round-trip verify both come through here; building a `KernelKey` from raw
/// entry fields is a contract defect (the M3-G gap-1 seam: a spec-conformant
/// dict stream whose width byte carries the max code width cannot resolve
/// through a raw-field key).
///
/// Lives in the format crate because both `pgrc2_codec` (which cannot be a
/// `pgrc2_read` dependency) and `pgrc2_read` (which cannot depend on the
/// codec crate) must agree on it; `pgrc2_codec::dispatch::stream_kernel_key`
/// delegates here.
pub fn stream_kernel_key(
    encoding: u16,
    class: u8,
    stream_width: u8,
) -> FormatResult<crate::abi::KernelKey> {
    use crate::class::{CLASS_BOOL, CLASS_BYVAL, CLASS_F32, CLASS_F64};
    let id = EncodingId::resolve(encoding)?;
    let width = match id {
        // Verbatim/Const key on the class width the reference registered.
        EncodingId::Verbatim | EncodingId::Const => match class {
            CLASS_BYVAL => match stream_width {
                1 | 2 | 4 | 8 => stream_width,
                _ => {
                    return Err(FormatError::Corrupt {
                        at: "stream width byte",
                    })
                }
            },
            CLASS_F32 => 4,
            CLASS_F64 => 8,
            CLASS_BOOL => 1,
            _ => 0,
        },
        // BYTE_FOR / PACKED_NUMERIC key on the elected delta/mantissa
        // width — the full byte-aligned ladder {1..=8} (SB-3 widened the
        // v3 pow2 set {1,2,4,8}).
        EncodingId::ByteFor | EncodingId::PackedNumeric => match stream_width {
            1..=8 => stream_width,
            _ => {
                return Err(FormatError::Corrupt {
                    at: "stream width byte",
                })
            }
        },
        EncodingId::BoolBitmap => 1,
        // Granule/section-self-describing families key on width 0.
        // DICT_CODES' entry width byte carries the stream's max code width
        // (spec §6.3 — a stats fact; 0 = unrecorded); it never keys
        // dispatch. FSST's symbol table lives in its section header bytes
        // (OD-5), so the entry width byte is likewise no dispatch axis.
        EncodingId::FforInterleave
        | EncodingId::DeltaFor
        | EncodingId::Alp
        | EncodingId::AlpRd
        | EncodingId::DictCodes
        | EncodingId::Fsst => 0,
        // Structural elections never appear on disk (spec §4 ruling): a
        // stream entry or section header carrying them is corrupt —
        // ARRAY_DUAL/JSONB_SHRED are election-grain markers whose
        // substreams carry real IDs.
        EncodingId::ArrayDual | EncodingId::JsonbShred => {
            return Err(FormatError::Corrupt {
                at: "structural encoding in stream entry",
            })
        }
    };
    Ok(crate::abi::KernelKey {
        encoding,
        class,
        width,
    })
}

/// Section-payload wrapper (spec §4/§6.4): heavy codecs survive only as
/// elected wrappers on cold/loser chunks — never on hot fixed-width columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Wrapper {
    None = 0,
    Lz4 = 1,
    Zstd = 2,
}

impl Wrapper {
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
    pub fn from_u8(v: u8) -> FormatResult<Wrapper> {
        match v {
            0 => Ok(Wrapper::None),
            1 => Ok(Wrapper::Lz4),
            2 => Ok(Wrapper::Zstd),
            _ => Err(FormatError::Corrupt { at: "Wrapper" }),
        }
    }
}
