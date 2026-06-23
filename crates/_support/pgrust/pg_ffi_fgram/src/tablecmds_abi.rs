//! Parse-node / utility command structs from `nodes/parsenodes.h` and
//! `tcop/utility.h` consumed by the `backend/commands/tablecmds.c` port
//! (CREATE TABLE / ALTER TABLE / DROP / TRUNCATE table-DDL engine).
//!
//! Each `struct` mirrors the C layout field-for-field (`#[repr(C)]`, same field
//! order, first field `type_: NodeTag`).  Pointers to node types not modeled in
//! detail are carried as `*mut Node` / `*mut List` / `*mut RangeVar`, matching
//! the C `Node *` / `List *` / `RangeVar *` shapes.
//!
//! `T_*` NodeTag discriminants and the `AlterTableType` ordinal values are
//! verified against `build-rust/src/include/nodes/nodetags.h` and
//! `nodes/parsenodes.h` (PostgreSQL 18.3).  Statement structs that already live
//! in `commands_parsenodes.rs` / `commands_ddl_parsenodes.rs`
//! (`DropStmt`, `RenameStmt`, `AlterObjectSchemaStmt`, `CreateStmt`,
//! `ColumnDef`, `Constraint`, `IndexStmt`, …) are NOT redefined here.

use crate::{AttrNumber, DropBehavior, List, Node, NodeTag, ObjectType, Oid, ParseLoc, RangeVar};

/// `INDEX_MAX_KEYS` (pg_config_manual.h) — fixed array bound shared by the
/// relcache FK-info arrays.
pub const INDEX_MAX_KEYS: usize = 32;

// ---------------------------------------------------------------------------
// NodeTag discriminants (nodes/nodetags.h, PostgreSQL 18.3)
// ---------------------------------------------------------------------------

/// `T_AlterTableStmt`.
pub const T_AlterTableStmt: NodeTag = 146;
/// `T_AlterTableCmd`.
pub const T_AlterTableCmd: NodeTag = 147;
/// `T_ATAlterConstraint`.
pub const T_ATAlterConstraint: NodeTag = 148;
/// `T_ReplicaIdentityStmt`.
pub const T_ReplicaIdentityStmt: NodeTag = 149;
/// `T_AlterTableMoveAllStmt`.
pub const T_AlterTableMoveAllStmt: NodeTag = 165;
/// `T_TruncateStmt`.
pub const T_TruncateStmt: NodeTag = 198;
/// `T_PartitionCmd`.
pub const T_PartitionCmd: NodeTag = 100;
/// `T_PartitionSpec`.
pub const T_PartitionSpec: NodeTag = 97;
/// `T_PartitionBoundSpec`.
pub const T_PartitionBoundSpec: NodeTag = 98;

// ---------------------------------------------------------------------------
// AlterTableType (nodes/parsenodes.h) — subtype of one ALTER TABLE subcommand.
// Ordinal values follow the C enum order exactly.
// ---------------------------------------------------------------------------

/// `typedef enum AlterTableType` — the kind of one ALTER TABLE subcommand.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub enum AlterTableType {
    /// add column
    AT_AddColumn = 0,
    /// implicitly via CREATE OR REPLACE VIEW
    AT_AddColumnToView,
    /// alter column default
    AT_ColumnDefault,
    /// add a pre-cooked column default
    AT_CookedColumnDefault,
    /// alter column drop not null
    AT_DropNotNull,
    /// alter column set not null
    AT_SetNotNull,
    /// alter column set expression
    AT_SetExpression,
    /// alter column drop expression
    AT_DropExpression,
    /// alter column set statistics
    AT_SetStatistics,
    /// alter column set ( options )
    AT_SetOptions,
    /// alter column reset ( options )
    AT_ResetOptions,
    /// alter column set storage
    AT_SetStorage,
    /// alter column set compression
    AT_SetCompression,
    /// drop column
    AT_DropColumn,
    /// add index
    AT_AddIndex,
    /// internal to commands/tablecmds.c
    AT_ReAddIndex,
    /// add constraint
    AT_AddConstraint,
    /// internal to commands/tablecmds.c
    AT_ReAddConstraint,
    /// internal to commands/tablecmds.c
    AT_ReAddDomainConstraint,
    /// alter constraint
    AT_AlterConstraint,
    /// validate constraint
    AT_ValidateConstraint,
    /// add constraint using existing index
    AT_AddIndexConstraint,
    /// drop constraint
    AT_DropConstraint,
    /// internal to commands/tablecmds.c
    AT_ReAddComment,
    /// alter column type
    AT_AlterColumnType,
    /// alter column OPTIONS (...)
    AT_AlterColumnGenericOptions,
    /// change owner
    AT_ChangeOwner,
    /// CLUSTER ON
    AT_ClusterOn,
    /// SET WITHOUT CLUSTER
    AT_DropCluster,
    /// SET LOGGED
    AT_SetLogged,
    /// SET UNLOGGED
    AT_SetUnLogged,
    /// SET WITHOUT OIDS
    AT_DropOids,
    /// SET ACCESS METHOD
    AT_SetAccessMethod,
    /// SET TABLESPACE
    AT_SetTableSpace,
    /// SET (...) -- AM specific parameters
    AT_SetRelOptions,
    /// RESET (...) -- AM specific parameters
    AT_ResetRelOptions,
    /// replace reloption list in its entirety
    AT_ReplaceRelOptions,
    /// ENABLE TRIGGER name
    AT_EnableTrig,
    /// ENABLE ALWAYS TRIGGER name
    AT_EnableAlwaysTrig,
    /// ENABLE REPLICA TRIGGER name
    AT_EnableReplicaTrig,
    /// DISABLE TRIGGER name
    AT_DisableTrig,
    /// ENABLE TRIGGER ALL
    AT_EnableTrigAll,
    /// DISABLE TRIGGER ALL
    AT_DisableTrigAll,
    /// ENABLE TRIGGER USER
    AT_EnableTrigUser,
    /// DISABLE TRIGGER USER
    AT_DisableTrigUser,
    /// ENABLE RULE name
    AT_EnableRule,
    /// ENABLE ALWAYS RULE name
    AT_EnableAlwaysRule,
    /// ENABLE REPLICA RULE name
    AT_EnableReplicaRule,
    /// DISABLE RULE name
    AT_DisableRule,
    /// INHERIT parent
    AT_AddInherit,
    /// NO INHERIT parent
    AT_DropInherit,
    /// OF <type_name>
    AT_AddOf,
    /// NOT OF
    AT_DropOf,
    /// REPLICA IDENTITY
    AT_ReplicaIdentity,
    /// ENABLE ROW SECURITY
    AT_EnableRowSecurity,
    /// DISABLE ROW SECURITY
    AT_DisableRowSecurity,
    /// FORCE ROW SECURITY
    AT_ForceRowSecurity,
    /// NO FORCE ROW SECURITY
    AT_NoForceRowSecurity,
    /// OPTIONS (...)
    AT_GenericOptions,
    /// ATTACH PARTITION
    AT_AttachPartition,
    /// DETACH PARTITION
    AT_DetachPartition,
    /// DETACH PARTITION FINALIZE
    AT_DetachPartitionFinalize,
    /// ADD IDENTITY
    AT_AddIdentity,
    /// SET identity column options
    AT_SetIdentity,
    /// DROP IDENTITY
    AT_DropIdentity,
    /// internal to commands/tablecmds.c
    AT_ReAddStatistics,
}

/// `typedef enum CoercionPathType` (parser/parse_coerce.h) — result of
/// `find_coercion_pathway`, returned by tablecmds.c's `findFkeyCast`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub enum CoercionPathType {
    /// failed to find any coercion pathway
    COERCION_PATH_NONE = 0,
    /// apply the specified coercion function
    COERCION_PATH_FUNC,
    /// binary-compatible cast, no function
    COERCION_PATH_RELABELTYPE,
    /// need an ArrayCoerceExpr node
    COERCION_PATH_ARRAYCOERCE,
    /// need a CoerceViaIO node
    COERCION_PATH_COERCEVIAIO,
}

// ---------------------------------------------------------------------------
// Statement / command structs
// ---------------------------------------------------------------------------

/// `typedef struct AlterTableStmt` — the parsed ALTER TABLE command.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterTableStmt {
    pub type_: NodeTag,
    /// table to work on
    pub relation: *mut RangeVar,
    /// list of subcommands
    pub cmds: *mut List,
    /// type of object
    pub objtype: ObjectType,
    /// skip error if table missing
    pub missing_ok: bool,
}

/// `typedef struct AlterTableCmd` — one subcommand of an ALTER TABLE.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterTableCmd {
    pub type_: NodeTag,
    /// Type of table alteration to apply
    pub subtype: AlterTableType,
    /// column, constraint, or trigger to act on, or tablespace, access method
    pub name: *mut core::ffi::c_char,
    /// attribute number for columns referenced by number
    pub num: i16,
    /// `RoleSpec *`
    pub newowner: *mut Node,
    /// definition of new column, index, constraint, or parent table
    pub def: *mut Node,
    /// RESTRICT or CASCADE for DROP cases
    pub behavior: DropBehavior,
    /// skip error if missing?
    pub missing_ok: bool,
    /// exec-time recursion
    pub recurse: bool,
}

/// `typedef struct ATAlterConstraint` — ad-hoc node for `AT_AlterConstraint`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ATAlterConstraint {
    pub type_: NodeTag,
    /// Constraint name
    pub conname: *mut core::ffi::c_char,
    /// changing enforceability properties?
    pub alterEnforceability: bool,
    /// ENFORCED?
    pub is_enforced: bool,
    /// changing deferrability properties?
    pub alterDeferrability: bool,
    /// DEFERRABLE?
    pub deferrable: bool,
    /// INITIALLY DEFERRED?
    pub initdeferred: bool,
    /// changing inheritability properties
    pub alterInheritability: bool,
    pub noinherit: bool,
}

/// `typedef struct ReplicaIdentityStmt` — ad-hoc node for `AT_ReplicaIdentity`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ReplicaIdentityStmt {
    pub type_: NodeTag,
    pub identity_type: core::ffi::c_char,
    pub name: *mut core::ffi::c_char,
}

/// `typedef struct TruncateStmt` — the parsed TRUNCATE command.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct TruncateStmt {
    pub type_: NodeTag,
    /// relations (RangeVars) to be truncated
    pub relations: *mut List,
    /// restart owned sequences?
    pub restart_seqs: bool,
    /// RESTRICT or CASCADE behavior
    pub behavior: DropBehavior,
}

/// `typedef struct PartitionCmd` — ATTACH/DETACH PARTITION subcommand payload.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PartitionCmd {
    pub type_: NodeTag,
    /// name of partition to attach/detach
    pub name: *mut RangeVar,
    /// FOR VALUES, if attaching (`PartitionBoundSpec *`)
    pub bound: *mut Node,
    pub concurrent: bool,
}

/// `typedef struct PartitionSpec` — PARTITION BY (...) specification.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PartitionSpec {
    pub type_: NodeTag,
    /// `PartitionStrategy`
    pub strategy: core::ffi::c_char,
    /// List of PartitionElems
    pub partParams: *mut List,
    /// token location, or -1 if unknown
    pub location: ParseLoc,
}

/// `typedef struct AlterTableMoveAllStmt` — ALTER ... ALL IN TABLESPACE.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterTableMoveAllStmt {
    pub type_: NodeTag,
    pub orig_tablespacename: *mut core::ffi::c_char,
    /// Object type to move
    pub objtype: ObjectType,
    /// List of roles to move objects of
    pub roles: *mut List,
    pub new_tablespacename: *mut core::ffi::c_char,
    pub nowait: bool,
}

/// `typedef struct ForeignKeyCacheInfo` (utils/rel.h) — the relcache's cached
/// foreign-key descriptor, threaded by `tryAttachPartitionForeignKey`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ForeignKeyCacheInfo {
    pub type_: NodeTag,
    /// oid of the constraint itself
    pub conoid: Oid,
    /// relation constrained by the foreign key
    pub conrelid: Oid,
    /// relation referenced by the foreign key
    pub confrelid: Oid,
    /// number of columns in the foreign key
    pub nkeys: core::ffi::c_int,
    /// Is enforced?
    pub conenforced: bool,
    /// cols in referencing table
    pub conkey: [AttrNumber; INDEX_MAX_KEYS],
    /// cols in referenced table
    pub confkey: [AttrNumber; INDEX_MAX_KEYS],
    /// PK = FK operator OIDs
    pub conpfeqop: [Oid; INDEX_MAX_KEYS],
}

/// `typedef struct AlterTableUtilityContext` (tcop/utility.h) — the execution
/// context threaded into `AlterTable` from ProcessUtility.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterTableUtilityContext {
    /// `PlannedStmt *` for outer ALTER TABLE command
    pub pstmt: *mut Node,
    /// its query string
    pub queryString: *const core::ffi::c_char,
    /// OID of ALTER's target table
    pub relid: Oid,
    /// any parameters available to ALTER TABLE (`ParamListInfo`)
    pub params: *mut core::ffi::c_void,
    /// execution environment for ALTER TABLE (`QueryEnvironment *`)
    pub queryEnv: *mut core::ffi::c_void,
}
