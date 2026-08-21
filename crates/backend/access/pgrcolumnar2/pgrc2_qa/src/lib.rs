//! # pgrc2_qa — the M3-K storage QA battery (chunk M3-K, tests-only)
//!
//! The adversarial lane over the MERGED pgrcolumnar2 storage crates
//! (`lanev3-m3-chunks.md` §2/§5 M3-K row; charter §1 QA surface). This crate
//! ships NO product code: the lib is rig scaffolding consumed by the tests
//! in `tests/` and by the `qa_crash_harness` binary. Product crates
//! (pgrc2_format/codec/write/read/meta) are strictly test SUBJECTS — a bug
//! found here is a filed issue with a minimized repro, never an in-lane fix.
//!
//! ## The batteries (one module each; tests are thin drivers)
//!
//! - [`simvfs`] — the fault-injecting VFS: MemVfs's kill-9 durability model
//!   (per-file volatile/durable content, per-dir volatile/durable dirents,
//!   crash-at-op-N arming) EXTENDED with the adversarial revive: per-sector
//!   (512 B) tearing of un-fsynced writes and per-op subset/reorder loss of
//!   un-fsynced namespace ops — the sector-tearing + dirent-loss composition
//!   M3-D's op-boundary crash matrix deferred to this lane.
//! - [`adapters`] — the QA-side bridges the wave-3 integration lanes have
//!   not merged yet: M3-C's real codec registry as the writer's
//!   [`pgrc2_write::seal::VerifyResolver`] and the reader's
//!   [`pgrc2_read::cursor::CodecBinding`], plus [`adapters::ForcedElection`]
//!   — a fixture `CandidateSource` that puts C's real encoders (BYTE_FOR /
//!   ALP / ALP_RD / DELTA_FOR / BOOL_BITMAP) into D's seal path so the
//!   corpus is real parts written by D's writer with C's codecs.
//! - [`corpus`] — deterministic per-class table fixtures with full oracles,
//!   written through `TableWriter` + `publish_parts`, verified back through
//!   `pgrc2_read` (open → cursor decode → oracle compare, with the
//!   arena-pointer containment invariant). This is also the old-or-new
//!   checker engine of every crash leg.
//! - [`mutate`] — the structure-aware mutator behind the regrown
//!   read-fuzzer (the #66/#340 incident class): a region map parsed from
//!   the PRISTINE image (tail / footer / section table / stream section
//!   headers / frame+gcount tables / stream dir / extent tables / payloads
//!   / overflow), raw mutations (must be refused or invisible) and
//!   CRC-fixed-up SEMANTIC mutations (hostile-but-checksummed frame tables
//!   and directory entries — the kernels' own bounds checks on trial).
//! - [`harness`] — the real-directory publish/commit protocol shared by the
//!   `qa_crash_harness` binary and the kill -9 ladder driver: rounds of
//!   (ingest → seal → publish → clog-append commit record), self-recovering
//!   on every start, with the full old-or-new + residue + oracle checker.
//!
//! ## Laws carried
//!
//! - Every gate is born-RED with two teeth (a seeded defect fires it; a
//!   not-run battery cannot pass — iteration witnesses).
//! - No clocks, no OS entropy, no thread spawns in the lib ([`XorShift`]
//!   is the only randomness; the ladder's kill timing lives in tests/,
//!   outside the determinism-lint production cone by path law).
//! - Scale is env-selected in tests/ only (`PGRC2_QA_SCALE=full` on CI cluster;
//!   the default is the laptop smoke tier).

pub mod adapters;
pub mod corpus;
pub mod harness;
pub mod mutate;
pub mod simvfs;

pub use pgrc2_format as format;

use pgrc2_format::class::{ColSchema, CollationClass, StorageClass, TypeSemantics};

/// Deterministic xorshift64* — the crate's ONLY randomness (no `rand`, no
/// clocks; the pgrc2_read test-battery precedent).
#[derive(Debug, Clone)]
pub struct XorShift(pub u64);

impl XorShift {
    pub fn new(seed: u64) -> XorShift {
        // Zero state is a fixed point; displace it deterministically.
        XorShift(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    pub fn coin(&mut self) -> bool {
        self.next() & 1 == 1
    }
}

// ---------------------------------------------------------------------------
// schema helpers (the D-suite shapes, restated — that module is cfg(test))
// ---------------------------------------------------------------------------

pub fn int8_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::ByvalWord {
            width: 8,
            signed: true,
        },
        typlen: 8,
        typbyval: true,
        typalign: b'd',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::SignedInt,
    }
}

pub fn f64_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::F64,
        typlen: 8,
        typbyval: true,
        typalign: b'd',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::Float,
    }
}

pub fn bool_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::Bool,
        typlen: 1,
        typbyval: true,
        typalign: b'c',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::Bool,
    }
}

pub fn text_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::VarlenaVerbatim,
        typlen: -1,
        typbyval: false,
        typalign: b'i',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::TextCollated,
    }
}

pub fn fixed16_col(attno: u32) -> ColSchema {
    ColSchema {
        attno,
        class: StorageClass::Fixed { len: 16 },
        typlen: 16,
        typbyval: false,
        typalign: b'c',
        collation_class: CollationClass::C,
        semantics: TypeSemantics::MemcmpOrdered,
    }
}

/// 4B-U inline varlena image (the ingest-face input shape).
pub fn img_4b_u(payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + payload.len());
    v.extend_from_slice(&(((payload.len() as u32 + 4) << 2).to_le_bytes()));
    v.extend_from_slice(payload);
    v
}
