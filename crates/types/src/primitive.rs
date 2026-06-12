//! PostgreSQL scalar type vocabulary — width-preserving aliases and constants.
//! Populated incrementally as ports need entries; source of truth is
//! ../pgrust/src-idiomatic/crates/types/src/primitive.rs.

pub type BlockNumber = u32;

pub type PgWChar = u32;

pub const BLCKSZ: usize = 8192;
