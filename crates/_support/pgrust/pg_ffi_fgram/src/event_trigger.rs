//! ABI vocabulary for event-trigger machinery (`commands/event_trigger.c` and
//! `tcop/deparse_utility.c`).
//!
//! These `#[repr(C)]` structs / enums mirror, field-for-field, the C
//! definitions in:
//!   * `src/include/commands/event_trigger.h` — `EventTriggerData`
//!   * `src/include/tcop/deparse_utility.h`   — `CollectedCommandType`,
//!                                              `CollectedATSubcmd`,
//!                                              `CollectedCommand` (+ its `d`
//!                                              union)
//!   * `src/include/utils/aclchk_internal.h`  — `InternalGrant`
//!
//! `EventTriggerEvent` / `EVT_*` and `EventTriggerCacheItem` already live in
//! `cache_remainder.rs` (modeled from `utils/evtcache.h`); `ObjectType` lives in
//! `commands_parsenodes.rs` and `ObjectAddress` in `catalog_dependency.rs`.  We
//! re-use those here rather than re-defining them, to avoid the ambiguous-glob
//! trap.  `T_EventTriggerData` (the NodeTag) lives in `funccache.rs`.

use core::ffi::c_char;

use crate::acl::AclMode;
use crate::catalog_dependency::{DropBehavior, ObjectAddress};
use crate::commands_parsenodes::ObjectType;
use crate::list::List;
use crate::types::{NodeTag, Oid};
use crate::Node;

// ---------------------------------------------------------------------------
// EventTriggerData (commands/event_trigger.h)
//
// The node type passed as fmgr "context" info when a function is called by the
// event trigger manager.
// ---------------------------------------------------------------------------

/// `typedef struct EventTriggerData` (event_trigger.h).
#[repr(C)]
pub struct EventTriggerData {
    pub type_: NodeTag,
    /// event name.
    pub event: *const c_char,
    /// parse tree.
    pub parsetree: *mut Node,
    pub tag: crate::CommandTag,
}

// ---------------------------------------------------------------------------
// InternalGrant (utils/aclchk_internal.h)
//
// One GRANT/REVOKE statement in internal form (names resolved to Oids, the
// privilege list an AclMode bitmask).
// ---------------------------------------------------------------------------

/// `typedef struct { ... } InternalGrant` (aclchk_internal.h).
#[repr(C)]
pub struct InternalGrant {
    pub is_grant: bool,
    pub objtype: ObjectType,
    pub objects: *mut List,
    pub all_privs: bool,
    pub privileges: AclMode,
    /// list of untransformed `AccessPriv` nodes.
    pub col_privs: *mut List,
    pub grantees: *mut List,
    pub grant_option: bool,
    pub behavior: DropBehavior,
}

// ---------------------------------------------------------------------------
// Collected-command support (tcop/deparse_utility.h)
// ---------------------------------------------------------------------------

/// `typedef enum CollectedCommandType` (deparse_utility.h) — order matches the
/// C enum exactly so the discriminant equals the C value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CollectedCommandType {
    SCT_Simple,
    SCT_AlterTable,
    SCT_Grant,
    SCT_AlterOpFamily,
    SCT_AlterDefaultPrivileges,
    SCT_CreateOpClass,
    SCT_AlterTSConfig,
}

pub use CollectedCommandType::*;

/// `typedef struct CollectedATSubcmd` (deparse_utility.h) — one subcommand of a
/// collected ALTER TABLE.
#[repr(C)]
pub struct CollectedATSubcmd {
    /// affected column, constraint, index, ...
    pub address: ObjectAddress,
    pub parsetree: *mut Node,
}

/// The `simple` arm of `CollectedCommand.d` — most commands.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommandSimple {
    pub address: ObjectAddress,
    pub secondaryObject: ObjectAddress,
}

/// The `alterTable` arm — ALTER TABLE and internal uses thereof.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommandAlterTable {
    pub objectId: Oid,
    pub classId: Oid,
    pub subcmds: *mut List,
}

/// The `grant` arm — GRANT / REVOKE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommandGrant {
    pub istmt: *mut InternalGrant,
}

/// The `opfam` arm — ALTER OPERATOR FAMILY.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommandOpFam {
    pub address: ObjectAddress,
    pub operators: *mut List,
    pub procedures: *mut List,
}

/// The `createopc` arm — CREATE OPERATOR CLASS.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommandCreateOpClass {
    pub address: ObjectAddress,
    pub operators: *mut List,
    pub procedures: *mut List,
}

/// The `atscfg` arm — ALTER TEXT SEARCH CONFIGURATION ADD/ALTER/DROP MAPPING.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommandAtsCfg {
    pub address: ObjectAddress,
    pub dictIds: *mut Oid,
    pub ndicts: i32,
}

/// The `defprivs` arm — ALTER DEFAULT PRIVILEGES.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CollectedCommandDefPrivs {
    pub objtype: ObjectType,
}

/// The anonymous `union` member `d` of `CollectedCommand`.
#[repr(C)]
pub union CollectedCommandData {
    pub simple: CollectedCommandSimple,
    pub alterTable: CollectedCommandAlterTable,
    pub grant: CollectedCommandGrant,
    pub opfam: CollectedCommandOpFam,
    pub createopc: CollectedCommandCreateOpClass,
    pub atscfg: CollectedCommandAtsCfg,
    pub defprivs: CollectedCommandDefPrivs,
}

/// `typedef struct CollectedCommand` (deparse_utility.h) — a single DDL command
/// recorded for `pg_event_trigger_ddl_commands()`.
#[repr(C)]
pub struct CollectedCommand {
    pub type_: CollectedCommandType,
    pub in_extension: bool,
    pub parsetree: *mut Node,
    pub d: CollectedCommandData,
    /// when nested.
    pub parent: *mut CollectedCommand,
}
