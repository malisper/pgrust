//! `types-fmgr` — function-manager vocabulary owned by the calling-convention
//! core (`utils/fmgr/fmgr.c`).
//!
//! The ABI structs (`FmgrInfo`, `FunctionCallInfoBaseData`, `PGFunction`,
//! `FmgrBuiltin`) live in [`fmgr`]; the pass-by-reference boundary value family
//! in [`boundary`]; the resolution / catalog-fact vocabulary in [`resolution`].
//! `std` is required for the `Box<dyn Any>` internal lane and `Box<dyn
//! ExpandedObject>`.

#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

pub mod boundary;
pub mod fmgr;
pub mod mat_srf;
pub mod resolution;

pub use boundary::{ExpandedObject, FmgrArg, FmgrOut, RefPayload};
pub use mat_srf::{MatCell, MatRow, MatSrfGuard, MatSrfSink};
pub use fmgr::{
    ContextNode, ExternalFnExpr, FmgrBuiltin, FmgrInfo, FmgrInfoExtra, FnExpr,
    FunctionCallInfoBaseData, PGFunction, Pg_finfo_record, TRACK_FUNC_ALL, TRACK_FUNC_OFF,
    TRACK_FUNC_PL,
};
pub use resolution::{
    AclObjectType, BuiltinFunction, FmgrHookEventType, FmgrResolution, LangInfo, LoadedCFunc,
    LoadedExternalFunc, ProcInfo, ProcLanguage, ProcResultInfo, ResolvedFmgrInfo,
};
