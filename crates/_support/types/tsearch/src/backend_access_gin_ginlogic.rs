//! Runtime working models for `backend/access/gin/ginlogic.c`.
//!
//! [`GinScanKey`] is the full faithful model of `GinScanKeyData`
//! (`access/gin_private.h`) — now owned by the `types-gin` crate (the GIN index
//! AM's home), so the same struct that `ginscan.c` builds is the one the
//! consistent-fn routing reads and writes. It is re-exported here (and via the
//! `backend-access-gin-core-probe-seams` crate, which references it by value
//! across the fmgr consistent-call seam boundary, exactly as the C functions
//! pass the live `GinScanKey`) so the existing
//! `tsearch::backend_access_gin_ginlogic::*` paths keep resolving.
//!
//! `boolConsistentFn`/`triConsistentFn` are C function pointers; modeled as the
//! [`GinBoolConsistentKind`]/[`GinTriConsistentKind`] dispatch tags assigned by
//! `ginInitConsistentFunction` and dispatched by `callBoolConsistentFn` /
//! `callTriConsistentFn`.

pub use ::gin::{GinBoolConsistentKind, GinScanKey, GinTriConsistentKind};
