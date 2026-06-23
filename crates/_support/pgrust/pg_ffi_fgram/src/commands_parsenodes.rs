//! Parse-node command structs from `nodes/parsenodes.h` (plus the supporting
//! `primnodes.h` enums/nodes they reference) consumed by the
//! `backend/commands/*` DDL command crates.
//!
//! Each `struct` mirrors the C layout field-for-field (`#[repr(C)]`, same field
//! order, first field `type_: NodeTag`).  Pointers to other node types that are
//! not yet modeled in detail are carried as `*mut Node`/`*mut List`/`*mut
//! RangeVar`, matching the C `Node *`/`List *`/`RangeVar *` shapes.  `Query` is
//! opaque here (`*mut Node`), exactly as the commands layer treats a raw
//! parsetree.
//!
//! `ObjectType`, `DropBehavior` (re-exported from `catalog_dependency`),
//! `DiscardMode`, `ViewCheckOption`, `OnCommitAction`, `CoercionContext`,
//! `RoleSpecType` carry their C enum discriminants verbatim.

use core::ffi::{c_char, c_int};

use crate::{DropBehavior, List, Node, NodeTag, ParseLoc, RangeVar, TypeName};

// ---------------------------------------------------------------------------
// ObjectType (nodes/parsenodes.h) — order matches the C enum exactly so the
// discriminant equals the C value.
// ---------------------------------------------------------------------------

/// `typedef enum ObjectType` (parsenodes.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ObjectType {
    OBJECT_ACCESS_METHOD,
    OBJECT_AGGREGATE,
    OBJECT_AMOP,
    OBJECT_AMPROC,
    OBJECT_ATTRIBUTE,
    OBJECT_CAST,
    OBJECT_COLUMN,
    OBJECT_COLLATION,
    OBJECT_CONVERSION,
    OBJECT_DATABASE,
    OBJECT_DEFAULT,
    OBJECT_DEFACL,
    OBJECT_DOMAIN,
    OBJECT_DOMCONSTRAINT,
    OBJECT_EVENT_TRIGGER,
    OBJECT_EXTENSION,
    OBJECT_FDW,
    OBJECT_FOREIGN_SERVER,
    OBJECT_FOREIGN_TABLE,
    OBJECT_FUNCTION,
    OBJECT_INDEX,
    OBJECT_LANGUAGE,
    OBJECT_LARGEOBJECT,
    OBJECT_MATVIEW,
    OBJECT_OPCLASS,
    OBJECT_OPERATOR,
    OBJECT_OPFAMILY,
    OBJECT_PARAMETER_ACL,
    OBJECT_POLICY,
    OBJECT_PROCEDURE,
    OBJECT_PUBLICATION,
    OBJECT_PUBLICATION_NAMESPACE,
    OBJECT_PUBLICATION_REL,
    OBJECT_ROLE,
    OBJECT_ROUTINE,
    OBJECT_RULE,
    OBJECT_SCHEMA,
    OBJECT_SEQUENCE,
    OBJECT_SUBSCRIPTION,
    OBJECT_STATISTIC_EXT,
    OBJECT_TABCONSTRAINT,
    OBJECT_TABLE,
    OBJECT_TABLESPACE,
    OBJECT_TRANSFORM,
    OBJECT_TRIGGER,
    OBJECT_TSCONFIGURATION,
    OBJECT_TSDICTIONARY,
    OBJECT_TSPARSER,
    OBJECT_TSTEMPLATE,
    OBJECT_TYPE,
    OBJECT_USER_MAPPING,
    OBJECT_VIEW,
}

pub use ObjectType::*;

impl ObjectType {
    /// `(int) objtype` — the C enum value.
    pub fn as_int(self) -> i32 {
        self as i32
    }

    /// `(ObjectType) itype` — rebuild from the C enum value, or `None` for an
    /// out-of-range value.
    pub fn from_int(v: i32) -> Option<ObjectType> {
        if (0..=(OBJECT_VIEW as i32)).contains(&v) {
            // SAFETY: v is in range of the contiguous repr(i32) discriminants.
            Some(unsafe { core::mem::transmute::<i32, ObjectType>(v) })
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Supporting enums (primnodes.h / parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef enum CoercionContext` (primnodes.h).
///
/// The enum *ordering* is significant: `parse_coerce.c`'s
/// `find_coercion_pathway` relies on `ccontext >= castcontext` comparisons
/// (C "Rely on ordering of enum"), so `PartialOrd`/`Ord` are derived.
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Ord)]
#[repr(i32)]
pub enum CoercionContext {
    COERCION_IMPLICIT,
    COERCION_ASSIGNMENT,
    COERCION_PLPGSQL,
    COERCION_EXPLICIT,
}
pub use CoercionContext::*;

/// `typedef enum OnCommitAction` (primnodes.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum OnCommitAction {
    ONCOMMIT_NOOP,
    ONCOMMIT_PRESERVE_ROWS,
    ONCOMMIT_DELETE_ROWS,
    ONCOMMIT_DROP,
}
pub use OnCommitAction::*;

/// `typedef enum RoleSpecType` (parsenodes.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum RoleSpecType {
    ROLESPEC_CSTRING,
    ROLESPEC_CURRENT_ROLE,
    ROLESPEC_CURRENT_USER,
    ROLESPEC_SESSION_USER,
    ROLESPEC_PUBLIC,
}
pub use RoleSpecType::*;

/// `typedef enum DiscardMode` (parsenodes.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum DiscardMode {
    DISCARD_ALL,
    DISCARD_PLANS,
    DISCARD_SEQUENCES,
    DISCARD_TEMP,
}
pub use DiscardMode::*;

/// `typedef enum ViewCheckOption` (parsenodes.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ViewCheckOption {
    NO_CHECK_OPTION,
    LOCAL_CHECK_OPTION,
    CASCADED_CHECK_OPTION,
}
pub use ViewCheckOption::*;

// ---------------------------------------------------------------------------
// Supporting node structs (primnodes.h / parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef struct RoleSpec` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct RoleSpec {
    pub type_: NodeTag,
    pub roletype: RoleSpecType,
    pub rolename: *mut c_char,
    pub location: ParseLoc,
}

/// `typedef enum RoleStmtType` (parsenodes.h) — CREATE ROLE/USER/GROUP.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum RoleStmtType {
    ROLESTMT_ROLE,
    ROLESTMT_USER,
    ROLESTMT_GROUP,
}
pub use RoleStmtType::*;

// `AccessPriv` (the GRANT/REVOKE privilege element used by GRANT/REVOKE ROLE's
// `granted_roles` list) is defined in `aclchk.rs` and re-exported at the crate
// root, so it is not redefined here.

/// `typedef struct CreateRoleStmt` (parsenodes.h) — CREATE ROLE/USER/GROUP.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateRoleStmt {
    pub type_: NodeTag,
    /// ROLE/USER/GROUP
    pub stmt_type: RoleStmtType,
    /// role name
    pub role: *mut c_char,
    /// List of DefElem nodes
    pub options: *mut List,
}

/// `typedef struct AlterRoleStmt` (parsenodes.h) — ALTER ROLE.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterRoleStmt {
    pub type_: NodeTag,
    /// role
    pub role: *mut RoleSpec,
    /// List of DefElem nodes
    pub options: *mut List,
    /// +1 = add members, -1 = drop members
    pub action: c_int,
}

/// `typedef struct AlterRoleSetStmt` (parsenodes.h) — ALTER ROLE … SET.
/// `setstmt` is the `VariableSetStmt *` SET/RESET subcommand, carried opaquely
/// as `*mut Node` (the user.c logic forwards it untouched to `AlterSetting`).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterRoleSetStmt {
    pub type_: NodeTag,
    /// role, or NULL
    pub role: *mut RoleSpec,
    /// database name, or NULL
    pub database: *mut c_char,
    /// SET or RESET subcommand (`VariableSetStmt *`, opaque here)
    pub setstmt: *mut Node,
}

/// `typedef struct DropRoleStmt` (parsenodes.h) — DROP ROLE.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DropRoleStmt {
    pub type_: NodeTag,
    /// List of roles to remove
    pub roles: *mut List,
    /// skip error if a role is missing?
    pub missing_ok: bool,
}

/// `typedef struct GrantRoleStmt` (parsenodes.h) — GRANT/REVOKE ROLE.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct GrantRoleStmt {
    pub type_: NodeTag,
    /// list of roles to be granted/revoked (AccessPriv nodes)
    pub granted_roles: *mut List,
    /// list of member roles to add/delete (RoleSpec nodes)
    pub grantee_roles: *mut List,
    /// true = GRANT, false = REVOKE
    pub is_grant: bool,
    /// options e.g. WITH ADMIN OPTION (DefElem nodes)
    pub opt: *mut List,
    /// set grantor to other than current role
    pub grantor: *mut RoleSpec,
    /// drop behavior (for REVOKE)
    pub behavior: DropBehavior,
}

/// `typedef struct DropOwnedStmt` (parsenodes.h) — DROP OWNED BY.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DropOwnedStmt {
    pub type_: NodeTag,
    pub roles: *mut List,
    pub behavior: DropBehavior,
}

/// `typedef struct ReassignOwnedStmt` (parsenodes.h) — REASSIGN OWNED BY.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ReassignOwnedStmt {
    pub type_: NodeTag,
    pub roles: *mut List,
    pub newrole: *mut RoleSpec,
}

/// `typedef struct ObjectWithArgs` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ObjectWithArgs {
    pub type_: NodeTag,
    pub objname: *mut List,
    pub objargs: *mut List,
    pub objfuncargs: *mut List,
    pub args_unspecified: bool,
}

/// `typedef struct IntoClause` (primnodes.h).  `viewQuery` is the materialized
/// view's `Query *`, carried opaquely as `*mut Node`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct IntoClause {
    pub type_: NodeTag,
    pub rel: *mut RangeVar,
    pub colNames: *mut List,
    pub accessMethod: *mut c_char,
    pub options: *mut List,
    pub onCommit: OnCommitAction,
    pub tableSpaceName: *mut c_char,
    pub viewQuery: *mut Node,
    pub skipData: bool,
}

// ---------------------------------------------------------------------------
// Command parse-node statements (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef struct DefineStmt` — CREATE AGGREGATE / OPERATOR / TYPE / COLLATION
/// / CONVERSION / TEXT SEARCH … (commands/define.c et al).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DefineStmt {
    pub type_: NodeTag,
    pub kind: ObjectType,
    pub oldstyle: bool,
    pub defnames: *mut List,
    pub args: *mut List,
    pub definition: *mut List,
    pub if_not_exists: bool,
    pub replace: bool,
}

/// `typedef struct CompositeTypeStmt` (CREATE TYPE … AS).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CompositeTypeStmt {
    pub type_: NodeTag,
    pub typevar: *mut RangeVar,
    pub coldeflist: *mut List,
}

/// `typedef struct CreateEnumStmt` (CREATE TYPE … AS ENUM).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateEnumStmt {
    pub type_: NodeTag,
    pub typeName: *mut List,
    pub vals: *mut List,
}

/// `typedef struct CreateRangeStmt` (CREATE TYPE … AS RANGE).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateRangeStmt {
    pub type_: NodeTag,
    pub typeName: *mut List,
    pub params: *mut List,
}

/// `typedef struct AlterEnumStmt` (ALTER TYPE … ADD/RENAME VALUE).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterEnumStmt {
    pub type_: NodeTag,
    pub typeName: *mut List,
    pub oldVal: *mut c_char,
    pub newVal: *mut c_char,
    pub newValNeighbor: *mut c_char,
    pub newValIsAfter: bool,
    pub skipIfNewValExists: bool,
}

/// `typedef struct CreateConversionStmt` (CREATE CONVERSION).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateConversionStmt {
    pub type_: NodeTag,
    pub conversion_name: *mut List,
    pub for_encoding_name: *mut c_char,
    pub to_encoding_name: *mut c_char,
    pub func_name: *mut List,
    pub def: bool,
}

/// `typedef struct DropStmt` (DROP TABLE / TYPE / … ).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DropStmt {
    pub type_: NodeTag,
    pub objects: *mut List,
    pub removeType: ObjectType,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
    pub concurrent: bool,
}

/// `typedef struct CommentStmt` (COMMENT ON).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CommentStmt {
    pub type_: NodeTag,
    pub objtype: ObjectType,
    pub object: *mut Node,
    pub comment: *mut c_char,
}

/// `typedef struct SecLabelStmt` (SECURITY LABEL).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct SecLabelStmt {
    pub type_: NodeTag,
    pub objtype: ObjectType,
    pub object: *mut Node,
    pub provider: *mut c_char,
    pub label: *mut c_char,
}

/// `typedef struct RenameStmt` (ALTER … RENAME).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct RenameStmt {
    pub type_: NodeTag,
    pub renameType: ObjectType,
    pub relationType: ObjectType,
    pub relation: *mut RangeVar,
    pub object: *mut Node,
    pub subname: *mut c_char,
    pub newname: *mut c_char,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
}

/// `typedef struct AlterObjectDependsStmt` (ALTER … DEPENDS ON EXTENSION).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterObjectDependsStmt {
    pub type_: NodeTag,
    pub objectType: ObjectType,
    pub relation: *mut RangeVar,
    pub object: *mut Node,
    pub extname: *mut Node,
    pub remove: bool,
}

/// `typedef struct AlterObjectSchemaStmt` (ALTER … SET SCHEMA).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterObjectSchemaStmt {
    pub type_: NodeTag,
    pub objectType: ObjectType,
    pub relation: *mut RangeVar,
    pub object: *mut Node,
    pub newschema: *mut c_char,
    pub missing_ok: bool,
}

/// `typedef struct AlterOwnerStmt` (ALTER … OWNER TO).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterOwnerStmt {
    pub type_: NodeTag,
    pub objectType: ObjectType,
    pub relation: *mut RangeVar,
    pub object: *mut Node,
    pub newowner: *mut RoleSpec,
}

/// `typedef struct AlterOperatorStmt` (ALTER OPERATOR … SET).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterOperatorStmt {
    pub type_: NodeTag,
    pub opername: *mut ObjectWithArgs,
    pub options: *mut List,
}

/// `typedef struct ViewStmt` (CREATE VIEW).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ViewStmt {
    pub type_: NodeTag,
    pub view: *mut RangeVar,
    pub aliases: *mut List,
    pub query: *mut Node,
    pub replace: bool,
    pub options: *mut List,
    pub withCheckOption: ViewCheckOption,
}

/// `typedef struct CreateTableAsStmt` (CREATE TABLE AS / SELECT INTO / CREATE
/// MATERIALIZED VIEW).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateTableAsStmt {
    pub type_: NodeTag,
    pub query: *mut Node,
    pub into: *mut IntoClause,
    pub objtype: ObjectType,
    pub is_select_into: bool,
    pub if_not_exists: bool,
}

/// `typedef struct RefreshMatViewStmt` (REFRESH MATERIALIZED VIEW).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct RefreshMatViewStmt {
    pub type_: NodeTag,
    pub concurrent: bool,
    pub skipData: bool,
    pub relation: *mut RangeVar,
}

/// `typedef struct DiscardStmt` (DISCARD).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DiscardStmt {
    pub type_: NodeTag,
    pub target: DiscardMode,
}

/// `typedef struct LockStmt` (LOCK TABLE).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct LockStmt {
    pub type_: NodeTag,
    pub relations: *mut List,
    pub mode: c_int,
    pub nowait: bool,
}

/// `typedef struct PrepareStmt` (PREPARE).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct PrepareStmt {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub argtypes: *mut List,
    pub query: *mut Node,
}

/// `typedef struct ExecuteStmt` (EXECUTE).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ExecuteStmt {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub params: *mut List,
}

/// `typedef struct DeallocateStmt` (DEALLOCATE).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DeallocateStmt {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub isall: bool,
    pub location: ParseLoc,
}

/// `typedef struct DeclareCursorStmt` (DECLARE CURSOR) — used by portalcmds.c.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DeclareCursorStmt {
    pub type_: NodeTag,
    pub portalname: *mut c_char,
    pub options: c_int,
    pub query: *mut Node,
}

// Cursor option bitmask (parsenodes.h).
pub const CURSOR_OPT_BINARY: c_int = 0x0001;
pub const CURSOR_OPT_SCROLL: c_int = 0x0002;
pub const CURSOR_OPT_NO_SCROLL: c_int = 0x0004;
pub const CURSOR_OPT_INSENSITIVE: c_int = 0x0008;
pub const CURSOR_OPT_ASENSITIVE: c_int = 0x0010;
pub const CURSOR_OPT_HOLD: c_int = 0x0020;
pub const CURSOR_OPT_FAST_PLAN: c_int = 0x0100;
pub const CURSOR_OPT_GENERIC_PLAN: c_int = 0x0200;
pub const CURSOR_OPT_CUSTOM_PLAN: c_int = 0x0400;
pub const CURSOR_OPT_PARALLEL_OK: c_int = 0x0800;

// ---------------------------------------------------------------------------
// Create/Drop/Alter Table Space Statements (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef struct CreateTableSpaceStmt` (CREATE TABLESPACE).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateTableSpaceStmt {
    pub type_: NodeTag,
    pub tablespacename: *mut c_char,
    pub owner: *mut RoleSpec,
    pub location: *mut c_char,
    pub options: *mut List,
}

/// `typedef struct DropTableSpaceStmt` (DROP TABLESPACE).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DropTableSpaceStmt {
    pub type_: NodeTag,
    pub tablespacename: *mut c_char,
    /// skip error if missing?
    pub missing_ok: bool,
}

/// `typedef struct AlterTableSpaceOptionsStmt` (ALTER TABLESPACE … SET/RESET).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterTableSpaceOptionsStmt {
    pub type_: NodeTag,
    pub tablespacename: *mut c_char,
    pub options: *mut List,
    pub isReset: bool,
}

/// `typedef struct CreateCastStmt` (CREATE CAST) — referenced by alter.c.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateCastStmt {
    pub type_: NodeTag,
    pub sourcetype: *mut TypeName,
    pub targettype: *mut TypeName,
    pub func: *mut ObjectWithArgs,
    pub context: CoercionContext,
    pub inout: bool,
}

// ===========================================================================
// Data / maintenance / COPY command parse nodes (parsenodes.h) and the
// command-internal state structs (commands/vacuum.h, commands/explain_state.h,
// commands/copy.h) consumed by the
//   vacuum / analyze / cluster / explain / copy / copyfrom / copyto /
//   copyfromparse
// command crates.  `VacuumParams` / `VacOptValue` live in `access.rs` (single
// definition, no ambiguous glob).  `ExplainState` / `CopyFromState` /
// `CopyToState` are deep, mostly-private command-internal structs whose full
// layout is not needed at the crate boundary, so they are carried as opaque
// handles here (matching how the C exposes `CopyFromStateData *` /
// `CopyToStateData *` as forward-declared pointers).
// ===========================================================================

/// `typedef struct CopyStmt` (parsenodes.h) — `T_CopyStmt`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CopyStmt {
    pub type_: NodeTag,
    /// `RangeVar *relation` — the relation to copy.
    pub relation: *mut RangeVar,
    /// `Node *query` — the SELECT/DML-with-RETURNING raw parse tree, or NULL.
    pub query: *mut Node,
    /// `List *attlist` — column names (Strings), or NIL for all columns.
    pub attlist: *mut List,
    /// `bool is_from` — TO (false) or FROM (true).
    pub is_from: bool,
    /// `bool is_program` — is `filename` a program to popen?
    pub is_program: bool,
    /// `char *filename` — filename, or NULL for STDIN/STDOUT.
    pub filename: *mut c_char,
    /// `List *options` — list of DefElem nodes.
    pub options: *mut List,
    /// `Node *whereClause` — WHERE condition, or NULL.
    pub whereClause: *mut Node,
}

/// `typedef struct ClusterStmt` (parsenodes.h) — `T_ClusterStmt`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ClusterStmt {
    pub type_: NodeTag,
    /// `RangeVar *relation` — relation being indexed, or NULL if all.
    pub relation: *mut RangeVar,
    /// `char *indexname` — original index defined.
    pub indexname: *mut c_char,
    /// `List *params` — list of DefElem nodes.
    pub params: *mut List,
}

/// `CLUOPT_VERBOSE` (commands/cluster.h) — print progress info.
pub const CLUOPT_VERBOSE: crate::bits32 = 0x01;
/// `CLUOPT_RECHECK` (commands/cluster.h) — recheck relation state.
pub const CLUOPT_RECHECK: crate::bits32 = 0x02;
/// `CLUOPT_RECHECK_ISCLUSTERED` (commands/cluster.h) — recheck relation state
/// for `indisclustered`.
pub const CLUOPT_RECHECK_ISCLUSTERED: crate::bits32 = 0x04;

/// `typedef struct ClusterParams` (commands/cluster.h) — options for CLUSTER.
/// `#[repr(C)]`, field order matches the C struct exactly (PostgreSQL 18.3).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(C)]
pub struct ClusterParams {
    /// `bits32 options` — bitmask of `CLUOPT_*`.
    pub options: crate::bits32,
}

/// `typedef struct VacuumStmt` (parsenodes.h) — `T_VacuumStmt`.  Used for both
/// VACUUM and ANALYZE.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct VacuumStmt {
    pub type_: NodeTag,
    /// `List *options` — list of DefElem nodes.
    pub options: *mut List,
    /// `List *rels` — list of `VacuumRelation`, or NIL for all.
    pub rels: *mut List,
    /// `bool is_vacuumcmd` — true for VACUUM, false for ANALYZE.
    pub is_vacuumcmd: bool,
}

/// `typedef struct VacuumRelation` (parsenodes.h) — `T_VacuumRelation`.  A
/// single target table of VACUUM/ANALYZE.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct VacuumRelation {
    pub type_: NodeTag,
    /// `RangeVar *relation` — table name to process, or NULL.
    pub relation: *mut RangeVar,
    /// `Oid oid` — table's OID; `InvalidOid` if not looked up.
    pub oid: crate::Oid,
    /// `List *va_cols` — list of column names, or NIL for all.
    pub va_cols: *mut List,
}

/// `typedef struct ExplainStmt` (parsenodes.h) — `T_ExplainStmt`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ExplainStmt {
    pub type_: NodeTag,
    /// `Node *query` — raw parse tree initially, Query after analysis.
    pub query: *mut Node,
    /// `List *options` — list of DefElem nodes.
    pub options: *mut List,
}

/// `typedef enum ExplainSerializeOption` (commands/explain_state.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ExplainSerializeOption {
    EXPLAIN_SERIALIZE_NONE,
    EXPLAIN_SERIALIZE_TEXT,
    EXPLAIN_SERIALIZE_BINARY,
}

/// `typedef enum ExplainFormat` (commands/explain_state.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ExplainFormat {
    EXPLAIN_FORMAT_TEXT,
    EXPLAIN_FORMAT_XML,
    EXPLAIN_FORMAT_JSON,
    EXPLAIN_FORMAT_YAML,
}

/// `typedef struct ExplainState` (commands/explain_state.h) — opaque.
///
/// The full EXPLAIN output state is a deep, mostly file-private struct (output
/// buffer, grouping stacks, per-worker state, extension slots).  The command
/// crates pass `*mut ExplainState` across their boundaries; its layout is owned
/// by the explain crate, so it is carried as an opaque handle here.
pub type ExplainState = core::ffi::c_void;

/// `typedef enum CopyHeaderChoice` (commands/copy.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CopyHeaderChoice {
    COPY_HEADER_FALSE = 0,
    COPY_HEADER_TRUE,
    COPY_HEADER_MATCH,
}

/// `typedef enum CopyOnErrorChoice` (commands/copy.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CopyOnErrorChoice {
    COPY_ON_ERROR_STOP = 0,
    COPY_ON_ERROR_IGNORE,
}

/// `typedef enum CopyLogVerbosityChoice` (commands/copy.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CopyLogVerbosityChoice {
    COPY_LOG_VERBOSITY_SILENT = -1,
    COPY_LOG_VERBOSITY_DEFAULT = 0,
    COPY_LOG_VERBOSITY_VERBOSE,
}

/// `typedef struct CopyFormatOptions` (commands/copy.h) — parsed COPY options.
/// `#[repr(C)]`, field order matches the C struct exactly (PostgreSQL 18.3).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CopyFormatOptions {
    /// `int file_encoding` — file/remote-side encoding, -1 if unspecified.
    pub file_encoding: c_int,
    /// `bool binary` — binary format?
    pub binary: bool,
    /// `bool freeze` — freeze rows on loading?
    pub freeze: bool,
    /// `bool csv_mode` — Comma Separated Value format?
    pub csv_mode: bool,
    /// `CopyHeaderChoice header_line` — header line?
    pub header_line: CopyHeaderChoice,
    /// `char *null_print` — NULL marker string (server encoding).
    pub null_print: *mut c_char,
    /// `int null_print_len` — length of same.
    pub null_print_len: c_int,
    /// `char *null_print_client` — same, converted to file encoding.
    pub null_print_client: *mut c_char,
    /// `char *default_print` — DEFAULT marker string.
    pub default_print: *mut c_char,
    /// `int default_print_len` — length of same.
    pub default_print_len: c_int,
    /// `char *delim` — column delimiter (1 byte).
    pub delim: *mut c_char,
    /// `char *quote` — CSV quote char (1 byte).
    pub quote: *mut c_char,
    /// `char *escape` — CSV escape char (1 byte).
    pub escape: *mut c_char,
    /// `List *force_quote` — list of column names.
    pub force_quote: *mut List,
    /// `bool force_quote_all` — FORCE_QUOTE *?
    pub force_quote_all: bool,
    /// `bool *force_quote_flags` — per-column CSV FQ flags.
    pub force_quote_flags: *mut bool,
    /// `List *force_notnull` — list of column names.
    pub force_notnull: *mut List,
    /// `bool force_notnull_all` — FORCE_NOT_NULL *?
    pub force_notnull_all: bool,
    /// `bool *force_notnull_flags` — per-column CSV FNN flags.
    pub force_notnull_flags: *mut bool,
    /// `List *force_null` — list of column names.
    pub force_null: *mut List,
    /// `bool force_null_all` — FORCE_NULL *?
    pub force_null_all: bool,
    /// `bool *force_null_flags` — per-column CSV FN flags.
    pub force_null_flags: *mut bool,
    /// `bool convert_selectively` — do selective binary conversion?
    pub convert_selectively: bool,
    /// `CopyOnErrorChoice on_error` — what to do on error.
    pub on_error: CopyOnErrorChoice,
    /// `CopyLogVerbosityChoice log_verbosity` — verbosity of logged messages.
    pub log_verbosity: CopyLogVerbosityChoice,
    /// `int64 reject_limit` — maximum tolerable number of errors.
    pub reject_limit: i64,
    /// `List *convert_select` — list of column names (can be NIL).
    pub convert_select: *mut List,
}

/// `typedef enum CopySource` (commands/copyfrom_internal.h) — type of COPY FROM
/// source at the bottom level.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CopySource {
    /// from file (or a piped program)
    COPY_FILE,
    /// from frontend
    COPY_FRONTEND,
    /// from callback function
    COPY_CALLBACK,
}

/// `typedef enum EolType` (commands/copyfrom_internal.h) — end-of-line
/// terminator type of the input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum EolType {
    EOL_UNKNOWN,
    EOL_NL,
    EOL_CR,
    EOL_CRNL,
}

/// `typedef enum CopyInsertMethod` (commands/copyfrom_internal.h) — insert
/// method to be used during COPY FROM.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum CopyInsertMethod {
    /// use `table_tuple_insert` or `ExecForeignInsert`
    CIM_SINGLE,
    /// always use `table_multi_insert` or `ExecForeignBatchInsert`
    CIM_MULTI,
    /// use `table_multi_insert` / `ExecForeignBatchInsert` only if valid
    CIM_MULTI_CONDITIONAL,
}

/// `typedef int (*copy_data_source_cb) (void *outbuf, int minread, int maxread)`
/// (commands/copy.h) — callback signature for `COPY_CALLBACK` source.
pub type copy_data_source_cb = Option<
    unsafe extern "C" fn(outbuf: *mut core::ffi::c_void, minread: c_int, maxread: c_int) -> c_int,
>;

/// `INPUT_BUF_SIZE` (commands/copyfrom_internal.h) — `input_buf` palloc size is
/// `INPUT_BUF_SIZE + 1` bytes.
pub const INPUT_BUF_SIZE: c_int = 65536;
/// `RAW_BUF_SIZE` (commands/copyfrom_internal.h) — `raw_buf` palloc size is
/// `RAW_BUF_SIZE + 1` bytes.
pub const RAW_BUF_SIZE: c_int = 65536;

/// `typedef struct CopyFromStateData *CopyFromState` (commands/copyfrom_internal.h).
///
/// Full `#[repr(C)]` layout of the COPY FROM working state, field-for-field as
/// in PostgreSQL 18.3 `commands/copyfrom_internal.h`.  This struct is shared
/// between `copyfrom.c` (which allocates/initializes it) and `copyfromparse.c`
/// (which reads input and fills the parse buffers); both crates need the exact
/// field layout, so it lives here in shared ABI rather than as an opaque handle.
///
/// `routine` / `escontext` / `transition_capture` reference deep command- or
/// executor-internal structs that the parser only stores or dispatches through,
/// so they are kept as opaque pointers (`*mut c_void` / forward-declared).
#[repr(C)]
pub struct CopyFromStateData {
    /// `const struct CopyFromRoutine *routine` — format routine (opaque).
    pub routine: *const core::ffi::c_void,

    /* low-level state data */
    /// `CopySource copy_src` — type of copy source.
    pub copy_src: CopySource,
    /// `FILE *copy_file` — used if `copy_src == COPY_FILE` (opaque libc FILE).
    pub copy_file: *mut core::ffi::c_void,
    /// `StringInfo fe_msgbuf` — used if `copy_src == COPY_FRONTEND`.
    pub fe_msgbuf: crate::StringInfo,

    /// `EolType eol_type` — EOL type of input.
    pub eol_type: EolType,
    /// `int file_encoding` — file or remote side's character encoding.
    pub file_encoding: c_int,
    /// `bool need_transcoding` — file encoding differs from server?
    pub need_transcoding: bool,
    /// `Oid conversion_proc` — encoding conversion function.
    pub conversion_proc: crate::Oid,

    /* parameters from the COPY command */
    /// `Relation rel` — relation to copy from (opaque relcache handle).
    pub rel: crate::Relation,
    /// `List *attnumlist` — integer list of attnums to copy.
    pub attnumlist: *mut List,
    /// `char *filename` — filename, or NULL for STDIN.
    pub filename: *mut c_char,
    /// `bool is_program` — is `filename` a program to popen?
    pub is_program: bool,
    /// `copy_data_source_cb data_source_cb` — function for reading data.
    pub data_source_cb: copy_data_source_cb,

    /// `CopyFormatOptions opts`.
    pub opts: CopyFormatOptions,
    /// `bool *convert_select_flags` — per-column CSV/TEXT CS flags.
    pub convert_select_flags: *mut bool,
    /// `Node *whereClause` — WHERE condition (or NULL).
    pub whereClause: *mut Node,

    /* these are just for error messages, see CopyFromErrorCallback */
    /// `const char *cur_relname` — table name for error messages.
    pub cur_relname: *const c_char,
    /// `uint64 cur_lineno` — line number for error messages.
    pub cur_lineno: u64,
    /// `const char *cur_attname` — current att for error messages.
    pub cur_attname: *const c_char,
    /// `const char *cur_attval` — current att value for error messages.
    pub cur_attval: *const c_char,
    /// `bool relname_only` — don't output line number, att, etc.
    pub relname_only: bool,

    /*
     * Working state
     */
    /// `MemoryContext copycontext` — per-copy execution context.
    pub copycontext: crate::MemoryContext,

    /// `AttrNumber num_defaults` — count of att that are missing and have a
    /// default value.
    pub num_defaults: crate::AttrNumber,
    /// `FmgrInfo *in_functions` — array of input functions for each attr.
    pub in_functions: *mut crate::FmgrInfo,
    /// `Oid *typioparams` — array of element types for `in_functions`.
    pub typioparams: *mut crate::Oid,
    /// `ErrorSaveContext *escontext` — soft error trapped during in_functions
    /// execution (opaque; carried by pointer).
    pub escontext: *mut crate::ErrorSaveContext,
    /// `uint64 num_errors` — total number of rows which contained soft errors.
    pub num_errors: u64,
    /// `int *defmap` — array of default att numbers related to missing att.
    pub defmap: *mut c_int,
    /// `ExprState **defexprs` — array of default att expressions for all att.
    pub defexprs: *mut *mut crate::ExprState,
    /// `bool *defaults` — if DEFAULT marker was found for corresponding att.
    pub defaults: *mut bool,
    /// `bool volatile_defexprs` — is any of `defexprs` volatile?
    pub volatile_defexprs: bool,
    /// `List *range_table` — single element list of RangeTblEntry.
    pub range_table: *mut List,
    /// `List *rteperminfos` — single element list of RTEPermissionInfo.
    pub rteperminfos: *mut List,
    /// `ExprState *qualexpr`.
    pub qualexpr: *mut crate::ExprState,

    /// `TransitionCaptureState *transition_capture` (opaque).
    pub transition_capture: *mut core::ffi::c_void,

    /*
     * attribute_buf holds the separated, de-escaped text for each field of the
     * current line.
     */
    /// `StringInfoData attribute_buf`.
    pub attribute_buf: crate::StringInfoData,

    /* field raw data pointers found by COPY FROM */
    /// `int max_fields`.
    pub max_fields: c_int,
    /// `char **raw_fields`.
    pub raw_fields: *mut *mut c_char,

    /*
     * line_buf holds the whole input line being processed.
     */
    /// `StringInfoData line_buf`.
    pub line_buf: crate::StringInfoData,
    /// `bool line_buf_valid` — contains the row being processed?
    pub line_buf_valid: bool,

    /*
     * input_buf holds input data, already converted to database encoding.
     */
    /// `char *input_buf`.
    pub input_buf: *mut c_char,
    /// `int input_buf_index` — next byte to process.
    pub input_buf_index: c_int,
    /// `int input_buf_len` — total # of bytes stored.
    pub input_buf_len: c_int,
    /// `bool input_reached_eof` — true if we reached EOF.
    pub input_reached_eof: bool,
    /// `bool input_reached_error` — true if a conversion error happened.
    pub input_reached_error: bool,

    /*
     * raw_buf holds raw input data read from the data source.
     */
    /// `char *raw_buf`.
    pub raw_buf: *mut c_char,
    /// `int raw_buf_index` — next byte to process.
    pub raw_buf_index: c_int,
    /// `int raw_buf_len` — total # of bytes stored.
    pub raw_buf_len: c_int,
    /// `bool raw_reached_eof` — true if we reached EOF.
    pub raw_reached_eof: bool,

    /// `uint64 bytes_processed` — number of bytes processed so far.
    pub bytes_processed: u64,
}

impl CopyFromStateData {
    /// `INPUT_BUF_BYTES(cstate)` — `input_buf_len - input_buf_index`.
    #[inline]
    pub fn input_buf_bytes(&self) -> c_int {
        self.input_buf_len - self.input_buf_index
    }

    /// `RAW_BUF_BYTES(cstate)` — `raw_buf_len - raw_buf_index`.
    #[inline]
    pub fn raw_buf_bytes(&self) -> c_int {
        self.raw_buf_len - self.raw_buf_index
    }
}

/// `CopyFromState` — `*mut CopyFromStateData`.
pub type CopyFromState = *mut CopyFromStateData;

/// `typedef struct CopyToStateData *CopyToState` (commands/copy.h) — opaque.
///
/// The COPY TO state is a deep, file-private struct (output buffer, attribute
/// out-functions, dest receiver).  Carried as an opaque handle, matching the
/// C forward declaration.
pub type CopyToStateData = core::ffi::c_void;
/// `CopyToState` — `*mut CopyToStateData`.
pub type CopyToState = *mut CopyToStateData;

// ---------------------------------------------------------------------------
// VariableSetStmt (nodes/parsenodes.h) — SET / RESET.
// ---------------------------------------------------------------------------

/// `typedef enum VariableSetKind` (parsenodes.h).  `int`-sized C enum;
/// discriminants in declaration order.
pub type VariableSetKind = c_int;
/// `VAR_SET_VALUE` — `SET var = value`.
pub const VAR_SET_VALUE: VariableSetKind = 0;
/// `VAR_SET_DEFAULT` — `SET var TO DEFAULT`.
pub const VAR_SET_DEFAULT: VariableSetKind = 1;
/// `VAR_SET_CURRENT` — `SET var FROM CURRENT`.
pub const VAR_SET_CURRENT: VariableSetKind = 2;
/// `VAR_SET_MULTI` — special case for `SET TRANSACTION ...`.
pub const VAR_SET_MULTI: VariableSetKind = 3;
/// `VAR_RESET` — `RESET var`.
pub const VAR_RESET: VariableSetKind = 4;
/// `VAR_RESET_ALL` — `RESET ALL`.
pub const VAR_RESET_ALL: VariableSetKind = 5;

/// `typedef struct VariableSetStmt` (parsenodes.h) — SET / RESET command.
///
/// `kind` selects the `VAR_*` flavor; `name` is the variable to set and `args`
/// the `List *` of `A_Const` value nodes.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct VariableSetStmt {
    pub type_: NodeTag,
    pub kind: VariableSetKind,
    /// variable to be set
    pub name: *mut c_char,
    /// `List` of `A_Const` nodes
    pub args: *mut List,
    /// `true` if arguments should be accounted for in query jumbling
    pub jumble_args: bool,
    /// `SET LOCAL`?
    pub is_local: bool,
    /// token location, or -1 if unknown
    pub location: ParseLoc,
}
