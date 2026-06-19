//! Namespace-subsystem vocabulary: the `catalog/namespace.h` API types
//! (`SearchPathMatcher`, `FuncCandidateList`, `TempNamespaceStatus`, the
//! `RVR_*` flags) and the seam-boundary catalog-row projections the
//! namespace unit's syscache seams thread.

#![no_std]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

extern crate alloc;

pub mod backend_catalog_namespace;
pub mod namespace;

pub use backend_catalog_namespace::{
    CatalogObjectName, CharArrayDatum, FastpathProcRow, FuncArgInfo, FuncProcAttrs, OidArrayDatum,
    OperRow, ProcCompileRow, ProcRow, TextArrayDatum,
};
pub use namespace::{
    FuncCandidate, FuncCandidateList, SearchPathMatcher, TempNamespaceStatus, RVR_MISSING_OK,
    RVR_NOWAIT, RVR_SKIP_LOCKED,
};
