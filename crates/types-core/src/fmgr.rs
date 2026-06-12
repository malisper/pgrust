//! Fmgr-adjacent catalog limits (`pg_config_manual.h`).
//!
//! Populated incrementally from ../pgrust/src-idiomatic/crates/types/src/fmgr.rs
//! as ports need items; only the items currently consumed are present.

pub const INDEX_MAX_KEYS: i32 = 32;
pub const NAMEDATALEN: i32 = 64;

/// Opaque handle to a `FunctionCallInfo` (fmgr.h). The fmgr unit owns the
/// real shape; until it lands, fmgr-called functions thread this opaque
/// handle through to the funcapi seams.
pub type FunctionCallInfoHandle = usize;

/// Opaque handle to a materialized-SRF target: the `ReturnSetInfo` reachable
/// through `fcinfo->resultinfo` after `InitMaterializedSRF` (its
/// `setResult` tuplestore + `setDesc` descriptor). Owned and resolved by the
/// funcapi unit; SRF bodies only store rows into it.
pub type MaterializedSrfHandle = usize;
