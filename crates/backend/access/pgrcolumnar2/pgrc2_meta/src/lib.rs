//! # pgrc2_meta — the pgrcolumnar2 metadata plane (chunk M3-E)
//!
//! Wave-2 lane M3-E (`docs/design/lanev3-m3-chunks.md` §2/§5), built against
//! the merged, frozen M3-A surface only (`docs/design/pgrc2-format.md`
//! §8/§19.7, cited below as `spec §N`; design authority
//! `docs/design/pgrcolumnar-v2.md` §5, cited as `charter §5`). The FORMAT
//! owns the record bytes ([`format::meta`]); this crate computes them at seal
//! and adjudicates them at probe:
//!
//! - [`key`] — the universal signed-i64 order-embedded zone plane: the
//!   exact + coarse transforms per storage class, with the **coarse-key law
//!   carried in the types**: [`key::ExactKey`] and [`key::CoarseKey`] are
//!   distinct types, and the only Eq evaluation reachable from a
//!   [`key::CoarseKey`] returns [`verdict::CoarseEqVerdict`] — an enum with
//!   **no AllPass variant**, so a coarse Eq AllPass does not compile. A
//!   RELEASED assert at the single verdict chokepoint
//!   ([`verdict::finalize`]) backstops the type wall.
//! - [`profile`] — the per-column meta policy ([`profile::MetaProfile`]):
//!   which key derivation, which aggregates, whether equality metadata
//!   (blooms) is sound. The **collation gate** lives here: only
//!   collation-class C text gets order metadata (coarse prefix keys); other
//!   deterministic collations keep equality metadata only; nondeterministic
//!   collations get neither (charter §5, StrView §6 law).
//! - [`lower`] — constant lowering: exact-rescale-or-abstain for packed
//!   numeric, **fail-closed** on non-inline varlena const headers, the
//!   contains_nan gate on float constants (charter §5). Abstention is typed
//!   and always sound: an abstained constant produces no verdict.
//! - [`builder`] — [`builder::ColumnMeta`], the [`format::abi::
//!   ColumnMetaBuilder`] implementation M3-D drives at seal: typed footer
//!   aggregates (COUNT/MIN/MAX/SUM-i128/null/zero/NDV/sortedness; byte- AND
//!   char-length text stats — the #80 unit lesson) at granule/band/part
//!   grain, plus the aux section bodies.
//! - [`psma`] (spec §8.2) — positional SMA: 256-entry leading-byte(delta) →
//!   candidate row range per armed granule.
//! - [`bloom`] (spec §8.3) — per-granule equality blooms; arming policy
//!   carried from the charter (unclustered ∧ NDV floor); the hash family is
//!   [`hash`], golden-pinned.
//! - [`ndv`] (spec §8.4) — dense-HLL mergeable NDV registers
//!   (max-per-register merge: commutative, associative, idempotent).
//! - [`verdict`] — ternary verdict evaluation (AllPass/AllFail/Mixed) over
//!   `StatsRecord` + bloom evidence, **advisory-only** (a defect can lose
//!   speed, never rows) and **measured-only** (a stat is consulted only when
//!   the profile says the builder computed it), plus the
//!   verdict-vs-decode differential helper the QA suites reuse.
//! - [`pcache`] — the O-11 in-memory predicate-hash granule bitmap cache,
//!   keyed (part_uuid, predicate fingerprint), with HIT/BUILT/STALE
//!   witnesses and refuse-and-rebuild on mismatch.
//! - [`hash`] — the ONE hash family (bloom bits, NDV registers, predicate
//!   fingerprints): golden-pinned, deliberately independent of
//!   `format::ident`'s identity chain (that one is "NOT a general-purpose
//!   hash surface" by its own doc). Re-exported from the shared `order_key`
//!   support crate (see below).
//!
//! ## Build-against note (recorded deviation — RESOLVED 2026-08-08)
//!
//! The chunk row lists M3-B's vendored `adt_float::order_key` + bloom
//! modules as build-against inputs; M3-B was NOT on the lanev3 tip when this
//! lane branched (only M3-A, #436). The float/uuid/prefix key transforms and
//! the bloom/NDV hash family were therefore implemented HERE, golden-pinned,
//! with one definition serving both the build side (seal) and the probe side
//! (verdicts). Michael's 2026-08-08 ruling resolved the deviation in the
//! OTHER direction from the note's anticipation: THIS crate's
//! implementations were extracted VERBATIM into the shared
//! `crates/_support/order_key` crate (canonical home, fulfilling M3-B's
//! vendoring charter), and this crate now consumes them — [`key`] wraps the
//! raw transforms in the coarse-key-law types, [`bloom`]/[`hash`] re-export
//! the pure core. Golden continuity carried (every pin unchanged): NO
//! builder/derivation version bump.
//!
//! ## Crate laws
//!
//! - Pure: no I/O, no clocks, no locks, no thread-locals, no env, no
//!   statics. Seal-side accumulators own heap (writer-side staging, the
//!   format-crate precedent); probe-side evaluation is allocation-free.
//! - Advisory-only pruning: every verdict path is closed under the
//!   verdict-vs-decode differential ([`verdict::check_verdict_against_decode`]).
//! - Verdicts are measured-only: evaluation consults a `StatsRecord` field
//!   only when the [`profile::MetaProfile`] declares it computed, and key
//!   evidence additionally requires the record's own `key_kind` witness.
//! - `KeyKind::EnumRankReserved` (the O-5 slot) is NEVER written: v1 enums
//!   are eq-only ([`profile::TypeSemantics::EnumEqOnly`] lowers to
//!   `KeyDerivation::None`), and a record READ with the reserved kind is
//!   treated as `Absent` (conservative, no format break either way).
//! - `unsafe` is confined to the two documented datum-image adapters
//!   ([`builder`]'s value reader and [`lower`]'s datum adapter), both riding
//!   the same caller contract as `format::abi::datum_canonical_bytes`.

pub use pgrc2_format as format;

pub use order_key::hash;

pub(crate) mod batch;
pub mod bloom;
pub mod builder;
pub mod census;
pub mod key;
pub mod lower;
pub mod ndv;
pub mod pcache;
pub mod profile;
pub mod psma;
pub mod sketch;
pub mod fold;
pub mod verdict;

#[cfg(test)]
mod tests;
