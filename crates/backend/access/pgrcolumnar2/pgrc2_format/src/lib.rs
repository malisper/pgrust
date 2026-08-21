//! # pgrc2_format — the pgrcolumnar2 on-disk format + codec ABI (chunk M3-A)
//!
//! THE frozen M3 surface: every downstream pgrcolumnar2 chunk (M3-B..L,
//! `docs/design/lanev3-m3-chunks.md` §2/§4) builds against this crate's merged
//! tip. The byte-level contract is `docs/design/pgrc2-format.md` (cited below
//! as `spec §N`); the design authority is `docs/design/pgrcolumnar-v2.md`.
//! Amending anything here is an A-lane PR against BOTH files — consumers never
//! side-edit the freeze (`lanev3-m3-chunks.md` §4 wave-1 law).
//!
//! ## Ruling provenance (binding; never re-litigated here)
//!
//! - **O-M3-2** (naming): the `pgrcolumnar2` sibling family; the old
//!   `access/pgrcolumnar` crate is a design-only donor, untouched, deleted M7.
//! - **O-2 / O-M3-1(a)**: tombstones-first DV ladder — the complete
//!   tombstone/DV vocabulary is [`dml`] (spec §15); no deletion machinery at
//!   M3 (COPY + SELECT only; trickle DML gets typed refusals at M3-H).
//! - **O-3**: single-node crash-safe publish ([`manifest`], spec §13); the
//!   part-seal WAL record SHAPE is reserved on paper in [`wal`] (spec §14).
//! - **O-5 / O-M3-5**: eq-only enums in v1; the enumsortorder zone-key slot is
//!   reserved as [`meta::KeyKind::EnumRankReserved`] (spec §8.1).
//! - **O-7**: own-directory residency vocabulary in [`dirlayout`] (spec §12).
//! - **O-8 / O-M3-5**: packed columnar RowId in [`rowid`] (spec §10) with the
//!   10B-row bit-budget static assert; typed-TID-refusal posture is M3-H's.
//! - **O-9**: the ruled reloption vocabulary in [`relopt`] (spec §17).
//! - **O-10**: logical bank identity + required MANIFEST fields in [`bank`]
//!   (spec §18).
//! - **O-11 / O-12**: predicate-cache/memo/trgm sidecar slots RESERVED in
//!   [`sidecar`] (spec §16).
//! - **O-M3-3 (superseded by the v4 M2 close, ledger SB-4/OD-5)**: FSST is
//!   FIRST-CLASS — `EncodingId::Fsst` (12) resolves; the historical reserved
//!   posture survives only as the [`enc::ENC_FSST_RESERVED`] doc constant.
//!   The unassigned band 13..=127 still refuses typed (UnknownEncoding).
//! - **M0-S1/M0-S4** frozen geometry + encoding matrix: [`geom`], [`enc`]
//!   (spec §2/§4); the pow2-switch dispatch law and ctx-relative kernel law
//!   are ABI contracts in [`abi`] (spec §19).
//! - **StrView §7b** (`lanev3-strview.md`): every string payload region is
//!   varlena-shaped, 8-aligned (spec §1) — pinned in this crate's tests.
//!
//! ## Module map (spec section in parens)
//!
//! [`geom`] (§2) · [`class`] (§3) · [`enc`] (§4) · [`part`] (§5–§6) ·
//! [`dict`] (§7) · [`meta`] (§8) · [`sortkey`] (§9) · [`rowid`] (§10) ·
//! [`ident`] (§11 + §5.5) · [`dirlayout`] (§12) · [`manifest`] (§13) ·
//! [`wal`] (§14, reserved) · [`dml`] (§15) · [`sidecar`] (§16) ·
//! [`relopt`] (§17) · [`bank`] (§18) · [`abi`] (§19) · [`verbatim`] (§19.1,
//! the reference codec proving the ABI) · [`wire`] (LE + CRC + varlena
//! plumbing).
//!
//! ## Crate laws
//!
//! - Pure: no I/O, no clocks, no locks, no thread-locals, no env — encode is
//!   `&mut Vec<u8>`-out, decode is `&[u8]`-in. (`Vec<u8>`/`String` here are
//!   wire-encode staging buffers per the in-tree format-crate precedent —
//!   `pgrcolumnar/src/format.rs` — not engine collections; callers own
//!   allocation policy, and decode-side kernels are allocation-free.)
//! - Every failure is a typed [`FormatError`]; decode kernels are no-panic and
//!   bounds-validated even after CRC passes (spec §1).
//! - Every on-disk struct is layout-pinned (`src/tests/layout.rs`, the
//!   issue-#69 template) and every wire shape golden-pinned
//!   (`src/tests/golden.rs`); the reference codec round-trips the full
//!   six-face ABI (`src/tests/roundtrip.rs`); unknown/reserved-encoding
//!   refusals are born-RED-tested with seeded IDs (`src/tests/refusal.rs`).
//! - `unsafe` is confined to [`abi`]'s datum-image access kernel (reading
//!   pointer-class datums during encode/verify), each block `// SAFETY:`
//!   documented.

pub mod abi;
pub mod bank;
pub mod bankstats;
pub mod class;
pub mod dict;
pub mod dirlayout;
pub mod dml;
pub mod enc;
pub mod geom;
pub mod ident;
pub mod manifest;
pub mod meta;
pub mod part;
pub mod relopt;
pub mod rowid;
pub mod shredlane;
pub mod sidecar;
pub mod sortkey;
pub mod verbatim;
pub mod wal;
pub mod wire;

#[cfg(test)]
mod tests;

/// On-disk format version (spec §5.1). Bumped only by A-lane PRs.
pub const FORMAT_VERSION: u32 = 1;

/// The codec/meta-builder ABI version (spec §19). Downstream crates may pin
/// against it; it moves only with an A-lane ABI amendment.
pub const ABI_VERSION: u32 = 1;

/// The typed error vocabulary for the whole format surface (spec §1: every
/// failure is typed — no panic, no fallback, no silent skip).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatError {
    /// Input ended before the structure did. `at` names the structure.
    Truncated { at: &'static str },
    /// A magic word did not match.
    BadMagic { at: &'static str },
    /// A version field is outside what this crate reads.
    BadVersion { at: &'static str, got: u32 },
    /// A crc32c check failed (spec §5.2 per-section CRCs).
    CrcMismatch { at: &'static str },
    /// Structurally invalid contents (ordering, counts, ranges). Typed
    /// refusal, never UB — the #66/#340 incident-class law.
    Corrupt { at: &'static str },
    /// Encoding ID never assigned (spec §4). Born-RED-tested.
    UnknownEncoding { id: u16 },
    /// Encoding ID assigned but reserved — FSST until O-M3-3's follow-on.
    ReservedEncoding { id: u16 },
    /// Footer section table names a kind this reader does not know and the
    /// entry is not flagged `SECTION_OPTIONAL` (spec §5.2).
    UnknownSectionKind { kind: u16 },
    /// Stream directory names an unknown role (spec §6.1).
    UnknownStreamRole { role: u8 },
    /// Stream entry names an unknown storage class (spec §3).
    UnknownStorageClass { class: u8 },
    /// Catalog properties and the refinement hint disagree (spec §3).
    BadClassHint { detail: &'static str },
    /// The face is not served by this encoding (spec §19.3).
    FaceUnsupported { face: abi::Face, encoding: u16 },
    /// No kernel registered for the key (spec §19.5).
    KernelMissing { encoding: u16, class: u8, width: u8 },
    /// Decode arena exhausted (typed, never a resize — spec §19.4).
    ArenaExhausted { needed: usize },
    /// An offset/length reached outside its section (spec §1).
    Bounds { at: &'static str },
    /// Encode-side contract violation (e.g. CONST input not constant).
    EncodeContract { detail: &'static str },
    /// Wrapper block assembly is the codec crate's (M3-C); the format-crate
    /// section writer emits unwrapped sections only (spec §6.4).
    WrapperUnsupported { wrapper: u8 },
}

impl core::fmt::Display for FormatError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            FormatError::Truncated { at } => write!(f, "pgrc2 format: truncated at {at}"),
            FormatError::BadMagic { at } => write!(f, "pgrc2 format: bad magic at {at}"),
            FormatError::BadVersion { at, got } => {
                write!(f, "pgrc2 format: unreadable version {got} at {at}")
            }
            FormatError::CrcMismatch { at } => write!(f, "pgrc2 format: crc mismatch at {at}"),
            FormatError::Corrupt { at } => write!(f, "pgrc2 format: corrupt structure at {at}"),
            FormatError::UnknownEncoding { id } => {
                write!(f, "pgrc2 format: unknown encoding id {id}")
            }
            FormatError::ReservedEncoding { id } => {
                write!(
                    f,
                    "pgrc2 format: reserved encoding id {id} (not implemented)"
                )
            }
            FormatError::UnknownSectionKind { kind } => {
                write!(f, "pgrc2 format: unknown required section kind {kind}")
            }
            FormatError::UnknownStreamRole { role } => {
                write!(f, "pgrc2 format: unknown stream role {role}")
            }
            FormatError::UnknownStorageClass { class } => {
                write!(f, "pgrc2 format: unknown storage class {class}")
            }
            FormatError::BadClassHint { detail } => {
                write!(f, "pgrc2 format: class/hint mismatch: {detail}")
            }
            FormatError::FaceUnsupported { face, encoding } => {
                write!(
                    f,
                    "pgrc2 abi: face {face:?} unsupported by encoding {encoding}"
                )
            }
            FormatError::KernelMissing {
                encoding,
                class,
                width,
            } => write!(
                f,
                "pgrc2 abi: no kernel for (encoding {encoding}, class {class}, width {width})"
            ),
            FormatError::ArenaExhausted { needed } => {
                write!(
                    f,
                    "pgrc2 abi: decode arena exhausted ({needed} bytes needed)"
                )
            }
            FormatError::Bounds { at } => write!(f, "pgrc2 format: out-of-bounds at {at}"),
            FormatError::EncodeContract { detail } => {
                write!(f, "pgrc2 encode: contract violation: {detail}")
            }
            FormatError::WrapperUnsupported { wrapper } => {
                write!(
                    f,
                    "pgrc2 encode: wrapper {wrapper} assembly is the codec crate's"
                )
            }
        }
    }
}

impl std::error::Error for FormatError {}

/// Result alias for the whole crate.
pub type FormatResult<T> = Result<T, FormatError>;
