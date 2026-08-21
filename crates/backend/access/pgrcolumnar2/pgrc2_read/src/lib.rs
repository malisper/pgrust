//! # pgrc2_read — the pgrcolumnar2 reader (chunk M3-F)
//!
//! The read side of the frozen `pgrc2_format` surface
//! (`docs/design/pgrc2-format.md`, cited as `spec §N`;
//! `docs/design/lanev3-m3-chunks.md` §2/§5 M3-F row): part open +
//! substream-directory lazy faulting, CRC-validated section reads,
//! process-shared segment maps + the parsed-metadata registry, the budgeted
//! part cache (pins + inline LRU janitor), framed lazy dict handles, and the
//! decode-kernel invocation layer producing the lx_vec datum-word currency.
//!
//! ## The O(streams-touched) contract (spec §6.2; the 2.9 ms/MiB law)
//!
//! Cold-open cost is proportional to the streams actually touched, never the
//! part. The open path faults exactly: tail → footer → section table →
//! header. First stream access faults the StreamDir section. Each face call
//! faults only the sections its face needs (table below). Every fault is
//! recorded in the part's fault log — the born-RED gate's witness
//! (`src/tests/open_faults.rs`).
//!
//! Per-face fault policy (the sections a face may touch, beyond the fixed
//! open set + StreamDir):
//!
//! | face | values extent | validity extent | overflow extent | dict sections |
//! |---|---|---|---|---|
//! | `decode_full` / `decode_sel` | yes | iff stream exists | iff `HAS_OVERFLOW` | iff `DICT_CODES` |
//! | `decode_codes` | yes | iff stream exists | no | no |
//! | `validity` | no | iff stream exists | no | no |
//! | `meta_probe` | yes | iff stream exists | no | no |
//! | `dict_handle` (ABI face) | yes | iff stream exists | no | iff `DICT_CODES` |
//!
//! "iff stream exists" is ABI-forced: `KernelCtx.validity_bytes = None` MEANS
//! all-valid (spec §19.2), and a Validity stream exists iff the column has a
//! NULL in this part (spec §6.1) — handing a kernel `None` while the stream
//! exists would be a lie, so any ctx built for a nulled column faults its
//! validity extent. All-valid columns fault nothing extra.
//!
//! ## Design records (settled here, cited by the M3-F report)
//!
//! - **Section faulting is pread + CRC into owned buffers, not mmap.** The
//!   validated-decode law (spec §1) makes every first touch of a section read
//!   and checksum ALL of its bytes, so mmap's sub-section page laziness is
//!   unreachable; owned `Arc<[u8]>` buffers keep hostile-byte handling in
//!   safe Rust and give dict handles their generation-stable region for free.
//!   The realized lazy-fault grain is therefore the EXTENT/SECTION (CRC
//!   atomicity); `DictHandle::ensure_frame` keeps the spec §7 frame-grain
//!   contract shape, and a writer wanting finer dict laziness cuts extents at
//!   dict-frame boundaries.
//! - **Budget is capacity guidance, pins are law.** The janitor (inline at
//!   open/`maintain`, never a thread — no new thread populations) evicts
//!   least-recently-used UNPINNED parts while the resident total exceeds
//!   budget; pinned parts are never evicted, so the cache may run over budget
//!   under pin pressure (witnessed by a counter). Evicted-but-referenced
//!   parts stop counting against the budget: their memory is the holding
//!   scan's working set (the old part-cache "returned unregistered" posture).
//! - **LRU time is a logical clock** (a monotone counter), never wall time —
//!   determinism-ledger `time` category stays empty.
//! - **Dict epochs**: the structural identity is [`DictEpochKey`]
//!   `(part_uuid, attno, path_ord)` (spec §7 — Law A); the u64
//!   `lx_vec::DictEpoch` currency is minted once per (open-part instance,
//!   dict stream) from a process-global clock, so equal u64 epochs certify
//!   code-space identity structurally (same in-memory part instance), never
//!   probabilistically. Reopening an evicted part mints a fresh epoch —
//!   sound (codes are meaningless across epochs) and conservative.
//! - **Wrapped sections** (spec §6.4 `wrapper != 0`): decode-side unwrap is
//!   the codec crate's to implement (spec §20). The seam is
//!   [`cursor::SectionUnwrapper`] inside [`cursor::CodecBinding`]: M3-C
//!   registers unwrappers next to its kernel vtables and this crate never
//!   changes. The reference binding carries none — a wrapped section is a
//!   typed `WrapperUnsupported` refusal.
//! - **Single-slice ABI limits** (`KernelCtx.overflow` / `DictSections` are
//!   single slices): an UNWRAPPED multi-extent byte-run stream (SB-7: the
//!   v4 writer cuts DictPayload extents at dict-frame boundaries)
//!   ASSEMBLES back into the one region the frozen ctx shape needs
//!   (`cursor::load_region`; per-extent CRC validation IS the frame-grain
//!   fault-and-validate). A WRAPPED multi-extent byte run stays a typed
//!   refusal ([`ReadError::Unsupported`]) — O-CMP-5(a) unwrap is
//!   whole-section by design. Everything else handles multi-extent streams
//!   fully.
//! - **Identity staleness** (spec §11): sealed parts are immutable and every
//!   cache keys on `(dev, ino, len, footer_off)`, so equal identity implies
//!   identical bytes; the one hole is ino reuse after unlink+recreate at
//!   equal length, which the AM integration closes with relcache-style
//!   invalidation through [`registry::PartRegistry::invalidate`] (M3-G/H
//!   seam, named in the lane report).
//!
//! ## What this crate is NOT
//!
//! No encode paths (M3-D), no metadata verdicts (M3-E), no granule maps or
//! scan claims (M3-G), no DDL/catalog wiring (M3-H). It produces the decode
//! currency (datum words, code arrays, dict handles); the lx_source
//! implementor (M3-G) wraps that currency into `lx_vec` reprs — in
//! particular `lx_vec::DictSpace` is implemented over [`dicthandle::DictHandle`]
//! (its faces map one-to-one), which is the seam `lx_vec` chartered for M3.
//!
//! Standing laws honored throughout: typed errors on every hostile byte (no
//! panic paths — the M3-K read-fuzzer target), arenas with the ≥8-align law
//! (caller-owned, pass-through), whole-file one-pass authoring, pgsync-only
//! sync (loom models in `tests/loom.rs`), vfs-only I/O.

pub use pgrc2_format as format;

use pgrc2_format::FormatError;

pub mod cursor;
pub mod dicthandle;
pub mod io;
pub mod manifest_walk;
pub mod openpart;
pub mod registry;
pub mod sidecar;
pub mod streams;

#[cfg(any(test, loom))]
pub mod testpart;

#[cfg(test)]
mod tests;

pub use cursor::{reference_binding_leaked, CodecBinding, SectionUnwrapper, StreamCursor};
pub use dicthandle::{DictEntryCursor, DictEpochKey, DictFaultMode, DictHandle, DictTouch};
pub use io::{MemPartIo, MemTableDir, PartIo, TableDirIo, VfsPartIo, VfsTableDir};
pub use manifest_walk::{resolve_effective, AllCommitted, CommitCheck, EffectiveManifest, TableExpect};
pub use openpart::{FaultEntry, FaultTag, OpenPart, PartExpect, SegBuf};
pub use registry::{PartPin, PartRegistry};
pub use sidecar::{read_sidecar, SidecarConsult};
pub use streams::read_path_table;

/// The reader's typed error surface. Format-level refusals pass through as
/// [`FormatError`] (spec §1: typed, never UB, never a fallback); the reader
/// adds the I/O and composition refusals below. No variant carries an OS
/// message string — errno only (deterministic rendering).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadError {
    /// A frozen-format refusal (CRC, bounds, unknown/reserved IDs, …).
    Format(FormatError),
    /// A vfs call failed. `at` names the operation site; `errno` is the vfs
    /// errno at failure.
    Io { at: &'static str, errno: i32 },
    /// An open-time validation fact (manifest record / catalog expectation)
    /// disagreed with the file (spec §13.1: manifest facts are validation
    /// facts at open; spec §5.5: fingerprint disagreement refuses).
    OpenMismatch { field: &'static str },
    /// The stream directory has no stream for the requested
    /// (attno, path_ord, role).
    StreamMissing { attno: u32, path_ord: u32, role: u8 },
    /// A manifest generation named by the commit pointer / prev_gen chain is
    /// absent. Under the §13.3 publish ordering this cannot happen on an
    /// intact directory — absence is corruption, refused typed.
    ManifestMissing { gen: u64 },
    /// A structurally valid part uses a shape this reader version cannot
    /// present through the frozen ABI (documented in the crate doc; each
    /// site is a named A-lane report item, never a silent skip).
    Unsupported { what: &'static str },
    /// A DECLARED shredded-jsonb lane column could not be served by a part
    /// (the typed-refusal law: an absent/foreign lane is a refusal, never a
    /// fabricated-NULL column — shred elections are per-part, so the same
    /// declaration can serve one part and refuse the next). `attno` is the
    /// parent jsonb column; `ordinal` the declared scan-column position;
    /// `why` a deterministic static cause (absent election / path not
    /// elected / kind mismatch / the NumericFs scale gap).
    ShredLaneRefused {
        attno: u32,
        ordinal: u16,
        why: &'static str,
    },
}

impl From<FormatError> for ReadError {
    fn from(e: FormatError) -> ReadError {
        ReadError::Format(e)
    }
}

impl core::fmt::Display for ReadError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ReadError::Format(e) => write!(f, "pgrc2 read: {e}"),
            ReadError::Io { at, errno } => write!(f, "pgrc2 read: io error at {at} (errno {errno})"),
            ReadError::OpenMismatch { field } => {
                write!(f, "pgrc2 read: open validation mismatch on {field}")
            }
            ReadError::StreamMissing {
                attno,
                path_ord,
                role,
            } => write!(
                f,
                "pgrc2 read: no stream (attno {attno}, path {path_ord}, role {role})"
            ),
            ReadError::ManifestMissing { gen } => {
                write!(f, "pgrc2 read: manifest generation {gen} missing")
            }
            ReadError::Unsupported { what } => {
                write!(f, "pgrc2 read: unsupported part shape: {what}")
            }
            ReadError::ShredLaneRefused { attno, ordinal, why } => write!(
                f,
                "pgrc2 read: shred lane refused (attno {attno}, scan column {ordinal}): {why}"
            ),
        }
    }
}

impl std::error::Error for ReadError {}

/// Result alias for the whole crate.
pub type ReadResult<T> = Result<T, ReadError>;
