//! Family: **params** — `nodes/params.c`, the `ParamListInfo` machinery.
//!
//! `makeParamList`, `copyParamList`, `EstimateParamListSpace`,
//! `SerializeParamList`, `RestoreParamList`, `BuildParamLogString`,
//! `ParamsErrorCallback`, `paramlist_parser_setup`, `paramlist_param_ref`.
//! The `ParamListInfo` open handle already exists in
//! `types_nodes::parsestmt::ParamListInfoHandle`.
//!
//! Owns the canonical `backend-nodes-params-seams` (`make_param_list`) —
//! installed in `init_seams()` when this family is filled.
//!
//! Deps: backend-utils-error (ParamsErrorCallback), the datum (de)serialization
//! owner for Serialize/RestoreParamList. Independent of the keystone. Skeleton:
//! the param machinery lands when filled.

#![allow(unused)]

/// Family marker — the params machinery lands here. See module docs.
pub fn params_family_unimplemented() -> ! {
    todo!("params: nodes/params.c not yet ported (decomp family)")
}
