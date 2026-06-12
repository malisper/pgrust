//! Fmgr-adjacent catalog limits (`pg_config_manual.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/fmgr.rs
//! as ports need items; only the items currently consumed are present.

pub const INDEX_MAX_KEYS: i32 = 32;
pub const NAMEDATALEN: i32 = 64;

/// `F_INT4EQ` (`catalog/fmgroids.h`) — `int4eq`, pg_proc OID 65
/// (`pg_proc.dat`).
pub const F_INT4EQ: crate::primitive::RegProcedure = 65;
/// `F_OIDEQ` (`catalog/fmgroids.h`) — `oideq`, pg_proc OID 184
/// (`pg_proc.dat`).
pub const F_OIDEQ: crate::primitive::RegProcedure = 184;
