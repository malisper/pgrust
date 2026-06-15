//! ABI vocabulary for `backend/commands/extension.c` (CREATE / ALTER / DROP
//! EXTENSION + version scripts).
//!
//! These `#[repr(C)]` structs / enums / constants cross the boundary between the
//! rewritten `backend-commands-extension` crate and the rest of the backend.
//! They mirror the C definitions in:
//!   * `nodes/parsenodes.h`        — `CreateExtensionStmt`, `AlterExtensionStmt`,
//!                                    `AlterExtensionContentsStmt`
//!   * `nodes/nodetags.h`          — `T_CreateExtensionStmt` etc.
//!   * `catalog/pg_extension_d.h`  — the `pg_extension` relation/index OIDs,
//!                                    `Anum_pg_extension_*`, `Natts_pg_extension`
//!   * `extension.c` (file-local)  — the `ExtensionControlFile`,
//!                                    `ExtensionVersionInfo`,
//!                                    `script_error_callback_arg`, and
//!                                    `ExtensionSiblingCache` working structs
//!                                    (these stay private to the crate; only the
//!                                    parse nodes + catalog constants are shared
//!                                    here).
//!
//! This module is referenced as `pgrust_pg_ffi::extension_abi::*` (mirroring the
//! `tcop.rs` / `dbcommands_abi.rs` convention) so the extension crate names the
//! whole ABI from one place without the ambiguous-glob trap (the
//! `T_*`/`Anum_*`/`Natts_*` names overlap other modules).  It reuses the broadly
//! shared base aliases (`Oid`, `NodeTag`, `List`, `Node`, `ObjectType`) rather
//! than redefining them.

use core::ffi::{c_char, c_int};

use crate::commands_parsenodes::ObjectType;
use crate::fmgr::Node;
use crate::{List, NodeTag, Oid};

// ---------------------------------------------------------------------------
// NodeTag discriminants (nodes/nodetags.h, PostgreSQL 18.3 — verified)
// ---------------------------------------------------------------------------

/// `T_CreateExtensionStmt` = 166.
pub const T_CreateExtensionStmt: NodeTag = 166;
/// `T_AlterExtensionStmt` = 167.
pub const T_AlterExtensionStmt: NodeTag = 167;
/// `T_AlterExtensionContentsStmt` = 168.
pub const T_AlterExtensionContentsStmt: NodeTag = 168;

// ---------------------------------------------------------------------------
// Statement parse nodes (nodes/parsenodes.h)
//
// Each struct mirrors the C layout field-for-field (`#[repr(C)]`, first field
// `type_: NodeTag`).  The `object` of `AlterExtensionContentsStmt` is carried as
// `*mut Node` (opaque): extension.c only forwards it to `get_object_address`,
// never inspecting its contents.
// ---------------------------------------------------------------------------

/// `typedef struct CreateExtensionStmt` — `CREATE EXTENSION`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateExtensionStmt {
    pub type_: NodeTag,
    /// name of the extension to create
    pub extname: *mut c_char,
    /// just do nothing if it already exists?
    pub if_not_exists: bool,
    /// List of `DefElem` nodes
    pub options: *mut List,
}

/// `typedef struct AlterExtensionStmt` — `ALTER EXTENSION ... UPDATE`.
///
/// (Only used for ALTER EXTENSION UPDATE; later might need an action field.)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterExtensionStmt {
    pub type_: NodeTag,
    pub extname: *mut c_char,
    /// List of `DefElem` nodes
    pub options: *mut List,
}

/// `typedef struct AlterExtensionContentsStmt` — `ALTER EXTENSION ... ADD/DROP`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterExtensionContentsStmt {
    pub type_: NodeTag,
    /// Extension's name
    pub extname: *mut c_char,
    /// +1 = add object, -1 = drop object
    pub action: c_int,
    /// Object's type
    pub objtype: ObjectType,
    /// Qualified name of the object
    pub object: *mut Node,
}

// ---------------------------------------------------------------------------
// pg_extension catalog macros (catalog/pg_extension_d.h, PostgreSQL 18.3)
// ---------------------------------------------------------------------------

/// `ExtensionRelationId` = 3079 (pg_extension OID).
pub const ExtensionRelationId: Oid = 3079;
/// `ExtensionOidIndexId` = 3080 (pg_extension_oid_index).
pub const ExtensionOidIndexId: Oid = 3080;
/// `ExtensionNameIndexId` = 3081 (pg_extension_name_index).
pub const ExtensionNameIndexId: Oid = 3081;

/// `Anum_pg_extension_oid` = 1.
pub const Anum_pg_extension_oid: c_int = 1;
/// `Anum_pg_extension_extname` = 2.
pub const Anum_pg_extension_extname: c_int = 2;
/// `Anum_pg_extension_extowner` = 3.
pub const Anum_pg_extension_extowner: c_int = 3;
/// `Anum_pg_extension_extnamespace` = 4.
pub const Anum_pg_extension_extnamespace: c_int = 4;
/// `Anum_pg_extension_extrelocatable` = 5.
pub const Anum_pg_extension_extrelocatable: c_int = 5;
/// `Anum_pg_extension_extversion` = 6.
pub const Anum_pg_extension_extversion: c_int = 6;
/// `Anum_pg_extension_extconfig` = 7.
pub const Anum_pg_extension_extconfig: c_int = 7;
/// `Anum_pg_extension_extcondition` = 8.
pub const Anum_pg_extension_extcondition: c_int = 8;

/// `Natts_pg_extension` = 8.
pub const Natts_pg_extension: usize = 8;
