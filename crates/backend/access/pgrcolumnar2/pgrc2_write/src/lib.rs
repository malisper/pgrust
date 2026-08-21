//! # pgrc2_write — the pgrcolumnar2 writer/sealer (chunk M3-D)
//!
//! The ONE seal implementation (`lanev3-m3-chunks.md` §2 M3-D row): chunk
//! stats → election → encode → footer/meta assembly → part file; global dict
//! build; shredded-jsonb dual-store emission (O-4/O-9); detoast-on-ingest +
//! overflow region; manifest publish with the full spec §13.3 fsync ordering
//! + clog fence; header-first abort; per-(xid,cid) writer lifecycle + eoxact
//! purge; the serial ingest face. Built one-pass against the FROZEN M3-A
//! surface (`docs/design/pgrc2-format.md`, cited as `spec §N`); develops
//! against the reference Verbatim/Const codec (`pgrc2_format::verbatim`), so
//! nothing here waits on M3-C/E/F.
//!
//! ## Architecture (the settled design)
//!
//! - **Whole-part buffering.** Global dictionaries (spec §7) and per-stream
//!   elections (per-part permanence) both need exact whole-part knowledge,
//!   so [`ingest::ColBuffer`]s accumulate a full part before sealing;
//!   [`writer::PartCutPolicy`] bounds memory deterministically (cut points
//!   are a pure function of the accumulated rows — the byte-identical-parts
//!   law, charter §1). Extents are still emitted per band, so reader
//!   parallelism keeps its §6.4 decoupling.
//! - **Elections are per stream, per part** ([`elect`]): the ≥10%-win gate +
//!   incompressible guard are writer law; candidate analyzers are M3-C's,
//!   arriving through [`elect::CandidateSource`]. Refusal demotes to
//!   VERBATIM, never normalizes.
//! - **`verify_roundtrip` on every sealed value stream** ([`seal`]): the
//!   election quadruple's fixed leg (spec §19.6) runs per granule over the
//!   just-encoded section through the SAME vtables readers use — mandatory,
//!   witnessed in the [`seal::SealReport`].
//! - **Two-witness null law** (spec §6.6): the seal path cross-checks every
//!   granule's meta-builder `nonnull` against the EMITTED validity bitmap's
//!   popcount and refuses typed on skew.
//! - **Publish** ([`publish`]): the spec §13.3 five-step ordering through
//!   the sanctioned Vfs choke, generational manifests chained by `prev_gen`,
//!   `publisher_fxid` clog fence (#254), and the cbstore #253 commit fence
//!   (`xact_seams::force_sync_commit`, seam-guarded) as the final act.
//! - **Lifecycle** ([`writer`]): registry keyed by table with (fxid, cid)
//!   staleness fields — a mismatch EVICTS by abort (temp unlink, never a
//!   publish), mirroring the proven old-writer discipline; eoxact purge is
//!   unconditional on commit AND abort (the successful statement already
//!   took its writer at publish).
//! - **All file I/O behind [`wvfs::WriteVfs`]**: `RealVfs` calls the `vfs::`
//!   shims (zero determinism-ledger rows; under `--cfg pgrust_sim` the same
//!   binary composes with the tree's SimVfs crash model for M3-K), and the
//!   in-crate deterministic [`wvfs::MemVfs`] drives the kill-9-shaped crash
//!   matrix in default CI.
//!
//! ## Session context is a passed capability
//!
//! No thread-locals (the TLS census is exact-pinned), no clocks, no env, no
//! statics: fxid/cid/subxact evidence and the clog probe arrive as explicit
//! arguments ([`writer::TxnStamp`], [`publish::TxnProbe`]). Placement of the
//! registry in the session envelope is M3-H's declared wiring.
//!
//! ## Not in this crate (owned elsewhere, per spec §20)
//!
//! Hot codecs + sampled election analyzers (M3-C); real stats/PSMA/bloom/NDV
//! builders (M3-E — [`meta_standin`] is the ABI-conformant stand-in this
//! lane drives); part open/caches (M3-F); COPY/DDL wiring + reloption
//! registration + pendingDeletes (M3-H); parallel ingest (M3-I, taking the
//! ingest-file handoff from this crate's merged tip); DV/tombstone writers
//! (M5); seal-WAL emission (post-O-3 — `pgrc2_format::wal` stays paper).

pub use pgrc2_format as format;

pub mod bankplane;
pub mod dict;
pub mod dml;
pub mod elect;
pub mod ingest;
pub mod meta_standin;
pub mod meta_wire;
pub mod par;
pub mod publish;
pub mod seal;
pub mod shred;
pub mod shred_jsonb;
pub mod sidecar;
pub mod structural;
pub mod testkit;
pub mod writer;
pub mod wvfs;

#[cfg(test)]
mod tests;

use pgrc2_format::FormatError;

/// The typed error vocabulary of the writer (no panic, no fallback — the
/// same posture as `pgrc2_format::FormatError`, which it wraps).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// A frozen-format contract failure surfaced by `pgrc2_format`.
    Format(FormatError),
    /// A Vfs operation failed. `op` names the operation, `errno` is the
    /// C-style errno the vfs choke reported (0 when synthesized).
    Io {
        op: &'static str,
        path: String,
        errno: i32,
    },
    /// Caller/driver contract violation (mismatched row counts, unknown
    /// stream shapes, budget overruns).
    Contract { detail: &'static str },
    /// A typed refusal surface (external toast without a fetcher, lz4
    /// toast in a build without lz4 toast support, unsupported requests).
    Refused { what: &'static str },
    /// The two-witness null law fired (spec §6.6): the meta builder's
    /// granule `nonnull` disagreed with the emitted validity bitmap.
    TwoWitnessSkew {
        attno: u32,
        granule: u32,
        stats_nonnull: u32,
        bitmap_nonnull: u32,
    },
    /// `verify_roundtrip` refused the just-encoded granule (the seeded
    /// corruption gate; carries the underlying format error).
    RoundTrip {
        attno: u32,
        path_ord: u32,
        granule: u32,
        cause: FormatError,
    },
    /// The manifest chain is structurally broken (a committed generation's
    /// file is missing/corrupt — dirent durability was paid at its publish,
    /// so this is real corruption, never a crash residue).
    ManifestChain { at: &'static str },
}

impl From<FormatError> for WriteError {
    fn from(e: FormatError) -> WriteError {
        WriteError::Format(e)
    }
}

impl core::fmt::Display for WriteError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            WriteError::Format(e) => write!(f, "pgrc2 write: {e}"),
            WriteError::Io { op, path, errno } => {
                write!(f, "pgrc2 write: {op} failed on {path} (errno {errno})")
            }
            WriteError::Contract { detail } => {
                write!(f, "pgrc2 write: contract violation: {detail}")
            }
            WriteError::Refused { what } => write!(f, "pgrc2 write: refused: {what}"),
            WriteError::TwoWitnessSkew {
                attno,
                granule,
                stats_nonnull,
                bitmap_nonnull,
            } => write!(
                f,
                "pgrc2 write: two-witness null skew at column {attno} granule {granule}: \
                 stats {stats_nonnull} vs bitmap {bitmap_nonnull}"
            ),
            WriteError::RoundTrip {
                attno,
                path_ord,
                granule,
                cause,
            } => write!(
                f,
                "pgrc2 write: round-trip verify failed at column {attno} path {path_ord} \
                 granule {granule}: {cause}"
            ),
            WriteError::ManifestChain { at } => {
                write!(f, "pgrc2 write: broken manifest chain at {at}")
            }
        }
    }
}

impl std::error::Error for WriteError {}

/// Result alias for the whole crate.
pub type WriteResult<T> = Result<T, WriteError>;
