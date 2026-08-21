//! Structural-election policy (lanev4 TY-1/TY-3 writer wiring): the
//! CATALOG-derived facts the writer needs to route a column through a
//! STRUCTURAL arm of the encoding lattice — facts the writer cannot derive
//! from `ColSchema` (charter §3: properties, never an OID table) and
//! therefore receives declared, exactly like `DictPolicy`/`ColumnPosture`.
//!
//! Today the policy carries ONE structural family:
//!
//! - **ArrayDual (ENC 10)**: an array column registered here routes through
//!   the codec's [`pgrc2_codec::arraydual::elect_array_split`] accept/refuse
//!   at seal. Acceptance across EVERY granule of the part seals the column
//!   as sizes+elements dual substreams (roles `Sizes`/`ChildValues`) under
//!   a parent `StreamEntry` whose `encoding` byte records `ArrayDual`; ANY
//!   refusal demotes the whole column to the ordinary election (VERBATIM for
//!   the corpus's refuse shapes) — per-part permanence, never a mixed part.
//!
//! JsonbShred (ENC 11) needs no entry here: its structural seam is
//! [`crate::shred::ShredLaneSource`] (dual-store lanes), and the seal marks
//! the parent's election witness from the DERIVED-LANES fact itself.
//!
//! **Cluster key (IN-1 / FT-6 / OD-8)**: the declared-DDL cluster key is a
//! catalog fact of exactly this policy's shape — declared once per writer,
//! never guessed, deterministic input to the byte-identical-parts law. Per
//! OD-8 (RULED 2026-08-12) there is NO implicit default: an undeclared
//! table is legal and simply unlicensed (mechanisms don't engage); the CB
//! bank recipe DECLARES the published key. With a declared key the v4
//! writer sorts each part NATIVELY at seal (`crate::seal`), verifies the
//! order it produced, and stamps the spec-§9 SortKey section — the
//! per-part `clustered` witness of FT-6 (key + sorted-within-part
//! attestation, both legs the writer's own).

pub use pgrc2_codec::arraydual::ArrayElemFacts;
pub use pgrc2_format::sortkey::{NullsOrder, SortDir};

/// One declared cluster-key column (the FT-6 fact currency; DDL-declared,
/// OD-8: never defaulted).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClusterKeyDecl {
    pub attno: u32,
    pub dir: SortDir,
    pub nulls: NullsOrder,
}

/// Per-table structural-election facts, keyed by attno. Deterministic input
/// to the byte-identical-parts law (declared once per writer, never guessed).
#[derive(Debug, Clone, Default)]
pub struct StructuralPolicy {
    arrays: Vec<(u32, ArrayElemFacts)>,
    cluster: Vec<ClusterKeyDecl>,
}

impl StructuralPolicy {
    pub fn new() -> StructuralPolicy {
        StructuralPolicy::default()
    }

    /// Declare `attno` as an array column with the given element facts.
    pub fn with_array(mut self, attno: u32, facts: ArrayElemFacts) -> StructuralPolicy {
        self.arrays.push((attno, facts));
        self
    }

    /// Declare the table's cluster key (IN-1/FT-6; OD-8 declared-DDL-only).
    /// Key order is significant (lexicographic sort precedence).
    pub fn with_cluster_key(mut self, keys: Vec<ClusterKeyDecl>) -> StructuralPolicy {
        self.cluster = keys;
        self
    }

    /// The array facts for `attno`, if declared.
    pub fn array_facts(&self, attno: u32) -> Option<ArrayElemFacts> {
        self.arrays
            .iter()
            .find(|(a, _)| *a == attno)
            .map(|(_, f)| *f)
    }

    /// The declared cluster key (empty = undeclared: legal, unlicensed).
    pub fn cluster_key(&self) -> &[ClusterKeyDecl] {
        &self.cluster
    }

    pub fn is_empty(&self) -> bool {
        self.arrays.is_empty() && self.cluster.is_empty()
    }
}
