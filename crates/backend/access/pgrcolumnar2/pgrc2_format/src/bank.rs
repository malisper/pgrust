//! Bank identity + MANIFEST vocabulary (spec §18; ruling O-10): bank
//! identity is LOGICAL — per-column content hashes + row counts,
//! decode-verified. Physical bytes are free to vary under parallel load
//! (the COPY-order ruling), so the hash is an order-insensitive MULTISET
//! accumulator. M3-J's recipe writer + verifier consume this vocabulary.

use crate::ident::fold64;

/// Required bank-MANIFEST field names (O-10; schema-validated by M3-J).
pub const BANK_FIELD_FORMAT_VERSION: &str = "format_version";
pub const BANK_FIELD_RECIPE_VERSION: &str = "bank_recipe_version";
pub const BANK_FIELD_BUILT_BY_TIP: &str = "built_by_tip";
/// Per-chunk encoding elections (the bank-names-lie law made contractual).
pub const BANK_FIELD_ELECTIONS: &str = "elections";
/// Geometry facts: part/granule counts, dict sizes.
pub const BANK_FIELD_GEOMETRY: &str = "geometry";
/// Per-column logical identity rows: (name, row_count, hash_hi, hash_lo).
pub const BANK_FIELD_COLUMNS: &str = "columns";

pub const BANK_REQUIRED_FIELDS: [&str; 6] = [
    BANK_FIELD_FORMAT_VERSION,
    BANK_FIELD_RECIPE_VERSION,
    BANK_FIELD_BUILT_BY_TIP,
    BANK_FIELD_ELECTIONS,
    BANK_FIELD_GEOMETRY,
    BANK_FIELD_COLUMNS,
];

/// Per-row lane seeds (ASCII "banklane" / "colident").
const LANE_LO_SEED: u64 = 0x6261_6e6b_6c61_6e65;
const LANE_HI_SEED: u64 = 0x636f_6c69_6465_6e74;
/// Null-row marker word.
const NULL_MARK: u64 = 0x6e75_6c6c_6d61_726b;

/// Order-insensitive per-column logical identity (spec §18): two independent
/// mix64 folds per row over the canonical value bytes (spec §18.1),
/// accumulated by wrapping add. Merge is commutative + associative, so any
/// row partition across parts/workers yields the same digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LogicalColHash {
    lo: u64,
    hi: u64,
    rows: u64,
}

fn row_digest(seed: u64, bytes: &[u8]) -> u64 {
    let mut acc = fold64(seed, bytes.len() as u64);
    let mut chunks = bytes.chunks_exact(8);
    for c in &mut chunks {
        acc = fold64(acc, u64::from_le_bytes(c.try_into().expect("len 8")));
    }
    let rem = chunks.remainder();
    if !rem.is_empty() {
        let mut w = [0u8; 8];
        w[..rem.len()].copy_from_slice(rem);
        acc = fold64(acc, u64::from_le_bytes(w));
    }
    acc
}

impl LogicalColHash {
    pub fn new() -> LogicalColHash {
        LogicalColHash::default()
    }

    /// Fold one non-null row's canonical value bytes (spec §18.1).
    pub fn observe(&mut self, canonical_bytes: &[u8]) {
        self.lo = self
            .lo
            .wrapping_add(row_digest(LANE_LO_SEED, canonical_bytes));
        self.hi = self
            .hi
            .wrapping_add(row_digest(LANE_HI_SEED, canonical_bytes));
        self.rows += 1;
    }

    /// Fold one null row.
    pub fn observe_null(&mut self) {
        self.lo = self.lo.wrapping_add(fold64(LANE_LO_SEED, NULL_MARK));
        self.hi = self.hi.wrapping_add(fold64(LANE_HI_SEED, NULL_MARK));
        self.rows += 1;
    }

    /// Merge another accumulator (any partition, any order).
    pub fn merge(&mut self, other: &LogicalColHash) {
        self.lo = self.lo.wrapping_add(other.lo);
        self.hi = self.hi.wrapping_add(other.hi);
        self.rows += other.rows;
    }

    /// (hash_lo, hash_hi, row_count) — the bank MANIFEST column row.
    pub fn digest(&self) -> (u64, u64, u64) {
        (self.lo, self.hi, self.rows)
    }
}
