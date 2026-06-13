#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

//! `backend/commands/functioncmds.c` — CREATE/ALTER/DROP FUNCTION & PROCEDURE,
//! CREATE CAST, CREATE TRANSFORM, DO, CALL support (PostgreSQL 18.3).
//!
//! Decomposed into per-family modules off the shared [`keystone`] foundation:
//!
//!   * [`keystone`] — shared carrier types / ABI / lifetime foundation, the
//!     `Node`/`DefElem`/`TypeName` extraction helpers, the shared error
//!     helpers, `check_language_permissions`, and the `ObjectType`/language-OID
//!     constants. Ported in full so every other family compiles.
//!   * [`ddl_core`] — `CreateFunction` / `AlterFunction` / `RemoveFunctionById`
//!     and their static helpers.
//!   * [`cast_transform_do`] — `CreateCast`, `CreateTransform`,
//!     `get_transform_oid`, `IsThereFunctionInNamespace`, `ExecuteDoStmt`.
//!   * [`call_stmt`] — `ExecuteCallStmt` + `CallStmtResultDesc` (Family 4, the
//!     genuine remaining decomp work).
//!
//! Carrier types live in the LAYERED `types-*` stack and the per-owner
//! [`backend_commands_functioncmds_seams`] crate (NOT the monolithic
//! src-idiomatic `seams`/`types` crates, which do not exist in this repo).
//! Every genuine external crosses a per-owner seam, panicking until its owner
//! lands; `QualifiedNameGetCreationNamespace` and the
//! `GetSysCacheOid2(TRFTYPELANG, …)` core are called directly.

mod call_stmt;
mod cast_transform_do;
mod ddl_core;
mod keystone;

pub use call_stmt::{CallStmtResultDesc, ExecuteCallStmt};
pub use cast_transform_do::{
    get_transform_oid, CreateCast, CreateTransform, ExecuteDoStmt, IsThereFunctionInNamespace,
};
pub use ddl_core::{
    interpret_function_parameter_list, AlterFunction, CreateFunction, InterpretedParameters,
    RemoveFunctionById,
};

// ===========================================================================
// Seam installation
// ===========================================================================

/// Install every seam this crate owns. functioncmds owns no inward seam (no
/// other crate calls back into it across a cycle yet), so this installs nothing.
pub fn init_seams() {}

#[allow(unused_imports)]
use backend_commands_functioncmds_seams as _functioncmds_seams_dep;
#[allow(unused_imports)]
use seam_core as _seam_core_dep;
#[allow(unused_imports)]
use types_nodes as _types_nodes_dep;
