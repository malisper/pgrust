//! DDL / utility-statement parse nodes (`nodes/parsenodes.h`): the
//! non-optimizable command family - `CreateStmt`, `AlterTableStmt`,
//! `IndexStmt`, `CreateSeqStmt`, `DropStmt`, `GrantStmt`, and the column/
//! constraint/option nodes they reference (`ColumnDef`, `Constraint`,
//! `DefElem`, `TypeName`, ...).
//!
//! Every struct here is `#[repr(C)]` with field order/types matching the C
//! backend exactly (cross-checked against the c2rust copyfuncs embedded defs),
//! because these node trees are `palloc`-allocated and deep-copied by
//! `copyObject`. `NodeTag`, `List`, `Bitmapset`, `Oid`, `AttrNumber` are reused
//! from `pgrust-pg-ffi`; the shared parse/plan enums (`CmdType`,
//! `CoercionForm`, `OnCommitAction`, `CoercionContext`) and the embedded node
//! structs (`RangeVar`, `FuncExpr`, `IntoClause`) are reused from
//! [`crate::primnodes`].
//!
//! `Node *` members of these statements (raw expressions, sub-statements such
//! as the SELECT under `CREATE TABLE AS`, etc.) stay as `*mut Node`, exactly
//! as in C: that is one pointer slot and is ABI-correct without committing to a
//! concrete pointee. The few raw-parse helper nodes that DDL structs point at
//! with a concrete C type (`CollateClause`, `FuncCall`, `WindowDef`, `SortBy`)
//! are modelled here so those pointers keep the correct pointee type.

use core::ffi::{c_char, c_int, c_long};

use pg_ffi_fgram::{List, Node, NodeTag, Oid};

use crate::primnodes::{
    CmdType, CoercionContext, FuncExpr, IntoClause, OnCommitAction, ParseLoc, RangeVar,
};
// Raw-parse helper nodes shared with the raw-DML family are modelled in
// `parsenodes_stmts`; reuse them here so DDL pointers keep the exact pointee
// type instead of duplicating the structs.
use crate::parsenodes_stmts::{CollateClause, FuncCall, TypeName};

// ---------------------------------------------------------------------------
// Supporting scalar typedefs (matching the c2rust/header widths).
// ---------------------------------------------------------------------------

/// 16-bit signed integer (`int16`).
pub type Int16 = i16;
/// `bits32` - a 32-bit bitmask (`uint32`).
pub type Bits32 = u32;
/// `RelFileNumber` - an `Oid`-width relation file number.
pub type RelFileNumber = Oid;
/// `SubTransactionId` - a 32-bit subtransaction id.
pub type SubTransactionId = u32;

// ---------------------------------------------------------------------------
// Supporting enums. C enums are `int`-sized, so these are `c_uint` (or `c_int`
// where a negative enumerator forces signedness), matching the c2rust output.
// ---------------------------------------------------------------------------

pub type ObjectType = core::ffi::c_uint;
pub const OBJECT_ACCESS_METHOD: ObjectType = 0;
pub const OBJECT_AGGREGATE: ObjectType = 1;
pub const OBJECT_AMOP: ObjectType = 2;
pub const OBJECT_AMPROC: ObjectType = 3;
pub const OBJECT_ATTRIBUTE: ObjectType = 4;
pub const OBJECT_CAST: ObjectType = 5;
pub const OBJECT_COLUMN: ObjectType = 6;
pub const OBJECT_COLLATION: ObjectType = 7;
pub const OBJECT_CONVERSION: ObjectType = 8;
pub const OBJECT_DATABASE: ObjectType = 9;
pub const OBJECT_DEFAULT: ObjectType = 10;
pub const OBJECT_DEFACL: ObjectType = 11;
pub const OBJECT_DOMAIN: ObjectType = 12;
pub const OBJECT_DOMCONSTRAINT: ObjectType = 13;
pub const OBJECT_EVENT_TRIGGER: ObjectType = 14;
pub const OBJECT_EXTENSION: ObjectType = 15;
pub const OBJECT_FDW: ObjectType = 16;
pub const OBJECT_FOREIGN_SERVER: ObjectType = 17;
pub const OBJECT_FOREIGN_TABLE: ObjectType = 18;
pub const OBJECT_FUNCTION: ObjectType = 19;
pub const OBJECT_INDEX: ObjectType = 20;
pub const OBJECT_LANGUAGE: ObjectType = 21;
pub const OBJECT_LARGEOBJECT: ObjectType = 22;
pub const OBJECT_MATVIEW: ObjectType = 23;
pub const OBJECT_OPCLASS: ObjectType = 24;
pub const OBJECT_OPERATOR: ObjectType = 25;
pub const OBJECT_OPFAMILY: ObjectType = 26;
pub const OBJECT_PARAMETER_ACL: ObjectType = 27;
pub const OBJECT_POLICY: ObjectType = 28;
pub const OBJECT_PROCEDURE: ObjectType = 29;
pub const OBJECT_PUBLICATION: ObjectType = 30;
pub const OBJECT_PUBLICATION_NAMESPACE: ObjectType = 31;
pub const OBJECT_PUBLICATION_REL: ObjectType = 32;
pub const OBJECT_ROLE: ObjectType = 33;
pub const OBJECT_ROUTINE: ObjectType = 34;
pub const OBJECT_RULE: ObjectType = 35;
pub const OBJECT_SCHEMA: ObjectType = 36;
pub const OBJECT_SEQUENCE: ObjectType = 37;
pub const OBJECT_SUBSCRIPTION: ObjectType = 38;
pub const OBJECT_STATISTIC_EXT: ObjectType = 39;
pub const OBJECT_TABCONSTRAINT: ObjectType = 40;
pub const OBJECT_TABLE: ObjectType = 41;
pub const OBJECT_TABLESPACE: ObjectType = 42;
pub const OBJECT_TRANSFORM: ObjectType = 43;
pub const OBJECT_TRIGGER: ObjectType = 44;
pub const OBJECT_TSCONFIGURATION: ObjectType = 45;
pub const OBJECT_TSDICTIONARY: ObjectType = 46;
pub const OBJECT_TSPARSER: ObjectType = 47;
pub const OBJECT_TSTEMPLATE: ObjectType = 48;
pub const OBJECT_TYPE: ObjectType = 49;
pub const OBJECT_USER_MAPPING: ObjectType = 50;
pub const OBJECT_VIEW: ObjectType = 51;

pub type DropBehavior = core::ffi::c_uint;
pub const DROP_RESTRICT: DropBehavior = 0;
pub const DROP_CASCADE: DropBehavior = 1;

pub type AlterTableType = core::ffi::c_uint;
pub const AT_AddColumn: AlterTableType = 0;
pub const AT_AddColumnToView: AlterTableType = 1;
pub const AT_ColumnDefault: AlterTableType = 2;
pub const AT_CookedColumnDefault: AlterTableType = 3;
pub const AT_DropNotNull: AlterTableType = 4;
pub const AT_SetNotNull: AlterTableType = 5;
pub const AT_SetExpression: AlterTableType = 6;
pub const AT_DropExpression: AlterTableType = 7;
pub const AT_SetStatistics: AlterTableType = 8;
pub const AT_SetOptions: AlterTableType = 9;
pub const AT_ResetOptions: AlterTableType = 10;
pub const AT_SetStorage: AlterTableType = 11;
pub const AT_SetCompression: AlterTableType = 12;
pub const AT_DropColumn: AlterTableType = 13;
pub const AT_AddIndex: AlterTableType = 14;
pub const AT_ReAddIndex: AlterTableType = 15;
pub const AT_AddConstraint: AlterTableType = 16;
pub const AT_ReAddConstraint: AlterTableType = 17;
pub const AT_ReAddDomainConstraint: AlterTableType = 18;
pub const AT_AlterConstraint: AlterTableType = 19;
pub const AT_ValidateConstraint: AlterTableType = 20;
pub const AT_AddIndexConstraint: AlterTableType = 21;
pub const AT_DropConstraint: AlterTableType = 22;
pub const AT_ReAddComment: AlterTableType = 23;
pub const AT_AlterColumnType: AlterTableType = 24;
pub const AT_AlterColumnGenericOptions: AlterTableType = 25;
pub const AT_ChangeOwner: AlterTableType = 26;
pub const AT_ClusterOn: AlterTableType = 27;
pub const AT_DropCluster: AlterTableType = 28;
pub const AT_SetLogged: AlterTableType = 29;
pub const AT_SetUnLogged: AlterTableType = 30;
pub const AT_DropOids: AlterTableType = 31;
pub const AT_SetAccessMethod: AlterTableType = 32;
pub const AT_SetTableSpace: AlterTableType = 33;
pub const AT_SetRelOptions: AlterTableType = 34;
pub const AT_ResetRelOptions: AlterTableType = 35;
pub const AT_ReplaceRelOptions: AlterTableType = 36;
pub const AT_EnableTrig: AlterTableType = 37;
pub const AT_EnableAlwaysTrig: AlterTableType = 38;
pub const AT_EnableReplicaTrig: AlterTableType = 39;
pub const AT_DisableTrig: AlterTableType = 40;
pub const AT_EnableTrigAll: AlterTableType = 41;
pub const AT_DisableTrigAll: AlterTableType = 42;
pub const AT_EnableTrigUser: AlterTableType = 43;
pub const AT_DisableTrigUser: AlterTableType = 44;
pub const AT_EnableRule: AlterTableType = 45;
pub const AT_EnableAlwaysRule: AlterTableType = 46;
pub const AT_EnableReplicaRule: AlterTableType = 47;
pub const AT_DisableRule: AlterTableType = 48;
pub const AT_AddInherit: AlterTableType = 49;
pub const AT_DropInherit: AlterTableType = 50;
pub const AT_AddOf: AlterTableType = 51;
pub const AT_DropOf: AlterTableType = 52;
pub const AT_ReplicaIdentity: AlterTableType = 53;
pub const AT_EnableRowSecurity: AlterTableType = 54;
pub const AT_DisableRowSecurity: AlterTableType = 55;
pub const AT_ForceRowSecurity: AlterTableType = 56;
pub const AT_NoForceRowSecurity: AlterTableType = 57;
pub const AT_GenericOptions: AlterTableType = 58;
pub const AT_AttachPartition: AlterTableType = 59;
pub const AT_DetachPartition: AlterTableType = 60;
pub const AT_DetachPartitionFinalize: AlterTableType = 61;
pub const AT_AddIdentity: AlterTableType = 62;
pub const AT_SetIdentity: AlterTableType = 63;
pub const AT_DropIdentity: AlterTableType = 64;
pub const AT_ReAddStatistics: AlterTableType = 65;

pub type ConstrType = core::ffi::c_uint;
pub const CONSTR_NULL: ConstrType = 0;
pub const CONSTR_NOTNULL: ConstrType = 1;
pub const CONSTR_DEFAULT: ConstrType = 2;
pub const CONSTR_IDENTITY: ConstrType = 3;
pub const CONSTR_GENERATED: ConstrType = 4;
pub const CONSTR_CHECK: ConstrType = 5;
pub const CONSTR_PRIMARY: ConstrType = 6;
pub const CONSTR_UNIQUE: ConstrType = 7;
pub const CONSTR_EXCLUSION: ConstrType = 8;
pub const CONSTR_FOREIGN: ConstrType = 9;
pub const CONSTR_ATTR_DEFERRABLE: ConstrType = 10;
pub const CONSTR_ATTR_NOT_DEFERRABLE: ConstrType = 11;
pub const CONSTR_ATTR_DEFERRED: ConstrType = 12;
pub const CONSTR_ATTR_IMMEDIATE: ConstrType = 13;
pub const CONSTR_ATTR_ENFORCED: ConstrType = 14;
pub const CONSTR_ATTR_NOT_ENFORCED: ConstrType = 15;

pub type GrantTargetType = core::ffi::c_uint;
pub const ACL_TARGET_OBJECT: GrantTargetType = 0;
pub const ACL_TARGET_ALL_IN_SCHEMA: GrantTargetType = 1;
pub const ACL_TARGET_DEFAULTS: GrantTargetType = 2;

pub type RoleSpecType = core::ffi::c_uint;
pub const ROLESPEC_CSTRING: RoleSpecType = 0;
pub const ROLESPEC_CURRENT_ROLE: RoleSpecType = 1;
pub const ROLESPEC_CURRENT_USER: RoleSpecType = 2;
pub const ROLESPEC_SESSION_USER: RoleSpecType = 3;
pub const ROLESPEC_PUBLIC: RoleSpecType = 4;

pub type RoleStmtType = core::ffi::c_uint;
pub const ROLESTMT_ROLE: RoleStmtType = 0;
pub const ROLESTMT_USER: RoleStmtType = 1;
pub const ROLESTMT_GROUP: RoleStmtType = 2;

pub type VariableSetKind = core::ffi::c_uint;
pub const VAR_SET_VALUE: VariableSetKind = 0;
pub const VAR_SET_DEFAULT: VariableSetKind = 1;
pub const VAR_SET_CURRENT: VariableSetKind = 2;
pub const VAR_SET_MULTI: VariableSetKind = 3;
pub const VAR_RESET: VariableSetKind = 4;
pub const VAR_RESET_ALL: VariableSetKind = 5;

pub type ViewCheckOption = core::ffi::c_uint;
pub const NO_CHECK_OPTION: ViewCheckOption = 0;
pub const LOCAL_CHECK_OPTION: ViewCheckOption = 1;
pub const CASCADED_CHECK_OPTION: ViewCheckOption = 2;

pub type DefElemAction = core::ffi::c_uint;
pub const DEFELEM_UNSPEC: DefElemAction = 0;
pub const DEFELEM_SET: DefElemAction = 1;
pub const DEFELEM_ADD: DefElemAction = 2;
pub const DEFELEM_DROP: DefElemAction = 3;

pub type SortByDir = core::ffi::c_uint;
pub const SORTBY_DEFAULT: SortByDir = 0;
pub const SORTBY_ASC: SortByDir = 1;
pub const SORTBY_DESC: SortByDir = 2;
pub const SORTBY_USING: SortByDir = 3;

pub type SortByNulls = core::ffi::c_uint;
pub const SORTBY_NULLS_DEFAULT: SortByNulls = 0;
pub const SORTBY_NULLS_FIRST: SortByNulls = 1;
pub const SORTBY_NULLS_LAST: SortByNulls = 2;

/// `PartitionStrategy` - C enum with char enumerators (`'l'`/`'r'`/`'h'`), but
/// `int`-sized so `c_uint` in the ABI (matches the c2rust output).
pub type PartitionStrategy = core::ffi::c_uint;
pub const PARTITION_STRATEGY_LIST: PartitionStrategy = b'l' as PartitionStrategy;
pub const PARTITION_STRATEGY_RANGE: PartitionStrategy = b'r' as PartitionStrategy;
pub const PARTITION_STRATEGY_HASH: PartitionStrategy = b'h' as PartitionStrategy;

/// `PartitionRangeDatumKind` - signed (has the `-1` MINVALUE enumerator), so
/// `c_int`.
pub type PartitionRangeDatumKind = core::ffi::c_int;
pub const PARTITION_RANGE_DATUM_MINVALUE: PartitionRangeDatumKind = -1;
pub const PARTITION_RANGE_DATUM_VALUE: PartitionRangeDatumKind = 0;
pub const PARTITION_RANGE_DATUM_MAXVALUE: PartitionRangeDatumKind = 1;

pub type FunctionParameterMode = core::ffi::c_uint;
pub const FUNC_PARAM_IN: FunctionParameterMode = b'i' as FunctionParameterMode;
pub const FUNC_PARAM_OUT: FunctionParameterMode = b'o' as FunctionParameterMode;
pub const FUNC_PARAM_INOUT: FunctionParameterMode = b'b' as FunctionParameterMode;
pub const FUNC_PARAM_VARIADIC: FunctionParameterMode = b'v' as FunctionParameterMode;
pub const FUNC_PARAM_TABLE: FunctionParameterMode = b't' as FunctionParameterMode;
pub const FUNC_PARAM_DEFAULT: FunctionParameterMode = b'd' as FunctionParameterMode;

pub type TransactionStmtKind = core::ffi::c_uint;
pub const TRANS_STMT_BEGIN: TransactionStmtKind = 0;
pub const TRANS_STMT_START: TransactionStmtKind = 1;
pub const TRANS_STMT_COMMIT: TransactionStmtKind = 2;
pub const TRANS_STMT_ROLLBACK: TransactionStmtKind = 3;
pub const TRANS_STMT_SAVEPOINT: TransactionStmtKind = 4;
pub const TRANS_STMT_RELEASE: TransactionStmtKind = 5;
pub const TRANS_STMT_ROLLBACK_TO: TransactionStmtKind = 6;
pub const TRANS_STMT_PREPARE: TransactionStmtKind = 7;
pub const TRANS_STMT_COMMIT_PREPARED: TransactionStmtKind = 8;
pub const TRANS_STMT_ROLLBACK_PREPARED: TransactionStmtKind = 9;

pub type FetchDirection = core::ffi::c_uint;
pub const FETCH_FORWARD: FetchDirection = 0;
pub const FETCH_BACKWARD: FetchDirection = 1;
pub const FETCH_ABSOLUTE: FetchDirection = 2;
pub const FETCH_RELATIVE: FetchDirection = 3;

pub type DiscardMode = core::ffi::c_uint;
pub const DISCARD_ALL: DiscardMode = 0;
pub const DISCARD_PLANS: DiscardMode = 1;
pub const DISCARD_SEQUENCES: DiscardMode = 2;
pub const DISCARD_TEMP: DiscardMode = 3;

pub type ReindexObjectType = core::ffi::c_uint;
pub const REINDEX_OBJECT_INDEX: ReindexObjectType = 0;
pub const REINDEX_OBJECT_TABLE: ReindexObjectType = 1;
pub const REINDEX_OBJECT_SCHEMA: ReindexObjectType = 2;
pub const REINDEX_OBJECT_SYSTEM: ReindexObjectType = 3;
pub const REINDEX_OBJECT_DATABASE: ReindexObjectType = 4;

pub type ImportForeignSchemaType = core::ffi::c_uint;
pub const FDW_IMPORT_SCHEMA_ALL: ImportForeignSchemaType = 0;
pub const FDW_IMPORT_SCHEMA_LIMIT_TO: ImportForeignSchemaType = 1;
pub const FDW_IMPORT_SCHEMA_EXCEPT: ImportForeignSchemaType = 2;

pub type PublicationObjSpecType = core::ffi::c_uint;
pub const PUBLICATIONOBJ_TABLE: PublicationObjSpecType = 0;
pub const PUBLICATIONOBJ_TABLES_IN_SCHEMA: PublicationObjSpecType = 1;
pub const PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA: PublicationObjSpecType = 2;
pub const PUBLICATIONOBJ_CONTINUATION: PublicationObjSpecType = 3;

pub type AlterPublicationAction = core::ffi::c_uint;
pub const AP_AddObjects: AlterPublicationAction = 0;
pub const AP_DropObjects: AlterPublicationAction = 1;
pub const AP_SetObjects: AlterPublicationAction = 2;

pub type AlterSubscriptionType = core::ffi::c_uint;
pub const ALTER_SUBSCRIPTION_OPTIONS: AlterSubscriptionType = 0;
pub const ALTER_SUBSCRIPTION_CONNECTION: AlterSubscriptionType = 1;
pub const ALTER_SUBSCRIPTION_SET_PUBLICATION: AlterSubscriptionType = 2;
pub const ALTER_SUBSCRIPTION_ADD_PUBLICATION: AlterSubscriptionType = 3;
pub const ALTER_SUBSCRIPTION_DROP_PUBLICATION: AlterSubscriptionType = 4;
pub const ALTER_SUBSCRIPTION_REFRESH: AlterSubscriptionType = 5;
pub const ALTER_SUBSCRIPTION_ENABLED: AlterSubscriptionType = 6;
pub const ALTER_SUBSCRIPTION_SKIP: AlterSubscriptionType = 7;

pub type AlterTSConfigType = core::ffi::c_uint;
pub const ALTER_TSCONFIG_ADD_MAPPING: AlterTSConfigType = 0;
pub const ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN: AlterTSConfigType = 1;
pub const ALTER_TSCONFIG_REPLACE_DICT: AlterTSConfigType = 2;
pub const ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN: AlterTSConfigType = 3;
pub const ALTER_TSCONFIG_DROP_MAPPING: AlterTSConfigType = 4;

// ---------------------------------------------------------------------------
// DDL-owned helper nodes (not modelled by the raw-DML family).
// ---------------------------------------------------------------------------

/// `RoleSpec` - a role name or one of a few special values.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RoleSpec {
    pub type_: NodeTag,
    pub roletype: RoleSpecType,
    pub rolename: *mut c_char,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// Column / constraint / option helper nodes.
// ---------------------------------------------------------------------------

/// `ColumnDef` - a column definition (used in CREATE TABLE and friends).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ColumnDef {
    pub type_: NodeTag,
    pub colname: *mut c_char,
    pub typeName: *mut TypeName,
    pub compression: *mut c_char,
    pub inhcount: Int16,
    pub is_local: bool,
    pub is_not_null: bool,
    pub is_from_type: bool,
    pub storage: c_char,
    pub storage_name: *mut c_char,
    pub raw_default: *mut Node,
    pub cooked_default: *mut Node,
    pub identity: c_char,
    pub identitySequence: *mut RangeVar,
    pub generated: c_char,
    pub collClause: *mut CollateClause,
    pub collOid: Oid,
    pub constraints: *mut List,
    pub fdwoptions: *mut List,
    pub location: ParseLoc,
}

/// `TableLikeClause` - the `( ... LIKE ... )` clause of CREATE TABLE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TableLikeClause {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub options: Bits32,
    pub relationOid: Oid,
}

/// `IndexElem` - index parameters (CREATE INDEX, ON CONFLICT).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexElem {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub expr: *mut Node,
    pub indexcolname: *mut c_char,
    pub collation: *mut List,
    pub opclass: *mut List,
    pub opclassopts: *mut List,
    pub ordering: SortByDir,
    pub nulls_ordering: SortByNulls,
}

/// `DefElem` - a generic `name = value` option definition.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DefElem {
    pub type_: NodeTag,
    pub defnamespace: *mut c_char,
    pub defname: *mut c_char,
    pub arg: *mut Node,
    pub defaction: DefElemAction,
    pub location: ParseLoc,
}

/// `Constraint` - a table or column constraint definition.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Constraint {
    pub type_: NodeTag,
    pub contype: ConstrType,
    pub conname: *mut c_char,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub is_enforced: bool,
    pub skip_validation: bool,
    pub initially_valid: bool,
    pub is_no_inherit: bool,
    pub raw_expr: *mut Node,
    pub cooked_expr: *mut c_char,
    pub generated_when: c_char,
    pub generated_kind: c_char,
    pub nulls_not_distinct: bool,
    pub keys: *mut List,
    pub without_overlaps: bool,
    pub including: *mut List,
    pub exclusions: *mut List,
    pub options: *mut List,
    pub indexname: *mut c_char,
    pub indexspace: *mut c_char,
    pub reset_default_tblspc: bool,
    pub access_method: *mut c_char,
    pub where_clause: *mut Node,
    pub pktable: *mut RangeVar,
    pub fk_attrs: *mut List,
    pub pk_attrs: *mut List,
    pub fk_with_period: bool,
    pub pk_with_period: bool,
    pub fk_matchtype: c_char,
    pub fk_upd_action: c_char,
    pub fk_del_action: c_char,
    pub fk_del_set_cols: *mut List,
    pub old_conpfeqop: *mut List,
    pub old_pktable_oid: Oid,
    pub location: ParseLoc,
}

/// `PartitionElem` - one partition-key column.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionElem {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub expr: *mut Node,
    pub collation: *mut List,
    pub opclass: *mut List,
    pub location: ParseLoc,
}

/// `PartitionSpec` - a partition key specification.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionSpec {
    pub type_: NodeTag,
    pub strategy: PartitionStrategy,
    pub partParams: *mut List,
    pub location: ParseLoc,
}

/// `PartitionBoundSpec` - a partition bound specification.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionBoundSpec {
    pub type_: NodeTag,
    pub strategy: c_char,
    pub is_default: bool,
    pub modulus: c_int,
    pub remainder: c_int,
    pub listdatums: *mut List,
    pub lowerdatums: *mut List,
    pub upperdatums: *mut List,
    pub location: ParseLoc,
}

/// `PartitionRangeDatum` - one value in a range partition bound.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionRangeDatum {
    pub type_: NodeTag,
    pub kind: PartitionRangeDatumKind,
    pub value: *mut Node,
    pub location: ParseLoc,
}

/// `PartitionCmd` - ATTACH/DETACH PARTITION subcommand info.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PartitionCmd {
    pub type_: NodeTag,
    pub name: *mut RangeVar,
    pub bound: *mut PartitionBoundSpec,
    pub concurrent: bool,
}

/// `ObjectWithArgs` - a function/operator name plus parameter identification.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ObjectWithArgs {
    pub type_: NodeTag,
    pub objname: *mut List,
    pub objargs: *mut List,
    pub objfuncargs: *mut List,
    pub args_unspecified: bool,
}

/// `AccessPriv` - an access privilege, with optional column list.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AccessPriv {
    pub type_: NodeTag,
    pub priv_name: *mut c_char,
    pub cols: *mut List,
}

/// `FunctionParameter` - one parameter of a CREATE FUNCTION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FunctionParameter {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub argType: *mut TypeName,
    pub mode: FunctionParameterMode,
    pub defexpr: *mut Node,
    pub location: ParseLoc,
}

/// `CreateOpClassItem` - one item of CREATE OPERATOR CLASS.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateOpClassItem {
    pub type_: NodeTag,
    pub itemtype: c_int,
    pub name: *mut ObjectWithArgs,
    pub number: c_int,
    pub order_family: *mut List,
    pub class_args: *mut List,
    pub storedtype: *mut TypeName,
}

/// `StatsElem` - one element of CREATE STATISTICS.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct StatsElem {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub expr: *mut Node,
}

/// `ReplicaIdentityStmt` - REPLICA IDENTITY clause for ALTER TABLE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReplicaIdentityStmt {
    pub type_: NodeTag,
    pub identity_type: c_char,
    pub name: *mut c_char,
}

/// `ATAlterConstraint` - ad-hoc node for `AT_AlterConstraint`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ATAlterConstraint {
    pub type_: NodeTag,
    pub conname: *mut c_char,
    pub alterEnforceability: bool,
    pub is_enforced: bool,
    pub alterDeferrability: bool,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub alterInheritability: bool,
    pub noinherit: bool,
}

/// `VacuumRelation` - one relation operand of VACUUM/ANALYZE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VacuumRelation {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub oid: Oid,
    pub va_cols: *mut List,
}

/// `PublicationTable` - one table in a CREATE/ALTER PUBLICATION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PublicationTable {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub whereClause: *mut Node,
    pub columns: *mut List,
}

/// `PublicationObjSpec` - one publication object specification.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PublicationObjSpec {
    pub type_: NodeTag,
    pub pubobjtype: PublicationObjSpecType,
    pub name: *mut c_char,
    pub pubtable: *mut PublicationTable,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// Schema / table / type creation and alteration.
// ---------------------------------------------------------------------------

/// `CreateSchemaStmt` - CREATE SCHEMA.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateSchemaStmt {
    pub type_: NodeTag,
    pub schemaname: *mut c_char,
    pub authrole: *mut RoleSpec,
    pub schemaElts: *mut List,
    pub if_not_exists: bool,
}

/// `AlterTableStmt` - ALTER TABLE (and similar) command.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterTableStmt {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub cmds: *mut List,
    pub objtype: ObjectType,
    pub missing_ok: bool,
}

/// `AlterTableCmd` - one subcommand of an ALTER TABLE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterTableCmd {
    pub type_: NodeTag,
    pub subtype: AlterTableType,
    pub name: *mut c_char,
    pub num: Int16,
    pub newowner: *mut RoleSpec,
    pub def: *mut Node,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
    pub recurse: bool,
}

/// `AlterCollationStmt` - ALTER COLLATION ... REFRESH VERSION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterCollationStmt {
    pub type_: NodeTag,
    pub collname: *mut List,
}

/// `AlterDomainStmt` - ALTER DOMAIN.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterDomainStmt {
    pub type_: NodeTag,
    pub subtype: c_char,
    pub typeName: *mut List,
    pub name: *mut c_char,
    pub def: *mut Node,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
}

/// `CreateStmt` - CREATE TABLE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateStmt {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub tableElts: *mut List,
    pub inhRelations: *mut List,
    pub partbound: *mut PartitionBoundSpec,
    pub partspec: *mut PartitionSpec,
    pub ofTypename: *mut TypeName,
    pub constraints: *mut List,
    pub nnconstraints: *mut List,
    pub options: *mut List,
    pub oncommit: OnCommitAction,
    pub tablespacename: *mut c_char,
    pub accessMethod: *mut c_char,
    pub if_not_exists: bool,
}

/// `CompositeTypeStmt` - CREATE TYPE ... AS (...).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CompositeTypeStmt {
    pub type_: NodeTag,
    pub typevar: *mut RangeVar,
    pub coldeflist: *mut List,
}

/// `CreateEnumStmt` - CREATE TYPE ... AS ENUM.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateEnumStmt {
    pub type_: NodeTag,
    pub typeName: *mut List,
    pub vals: *mut List,
}

/// `CreateRangeStmt` - CREATE TYPE ... AS RANGE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateRangeStmt {
    pub type_: NodeTag,
    pub typeName: *mut List,
    pub params: *mut List,
}

/// `AlterEnumStmt` - ALTER TYPE ... (enum value).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterEnumStmt {
    pub type_: NodeTag,
    pub typeName: *mut List,
    pub oldVal: *mut c_char,
    pub newVal: *mut c_char,
    pub newValNeighbor: *mut c_char,
    pub newValIsAfter: bool,
    pub skipIfNewValExists: bool,
}

/// `ViewStmt` - CREATE VIEW.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ViewStmt {
    pub type_: NodeTag,
    pub view: *mut RangeVar,
    pub aliases: *mut List,
    pub query: *mut Node,
    pub replace: bool,
    pub options: *mut List,
    pub withCheckOption: ViewCheckOption,
}

/// `CreateDomainStmt` - CREATE DOMAIN.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateDomainStmt {
    pub type_: NodeTag,
    pub domainname: *mut List,
    pub typeName: *mut TypeName,
    pub collClause: *mut CollateClause,
    pub constraints: *mut List,
}

// ---------------------------------------------------------------------------
// Index / statistics / sequence.
// ---------------------------------------------------------------------------

/// `IndexStmt` - CREATE INDEX.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct IndexStmt {
    pub type_: NodeTag,
    pub idxname: *mut c_char,
    pub relation: *mut RangeVar,
    pub accessMethod: *mut c_char,
    pub tableSpace: *mut c_char,
    pub indexParams: *mut List,
    pub indexIncludingParams: *mut List,
    pub options: *mut List,
    pub whereClause: *mut Node,
    pub excludeOpNames: *mut List,
    pub idxcomment: *mut c_char,
    pub indexOid: Oid,
    pub oldNumber: RelFileNumber,
    pub oldCreateSubid: SubTransactionId,
    pub oldFirstRelfilelocatorSubid: SubTransactionId,
    pub unique: bool,
    pub nulls_not_distinct: bool,
    pub primary: bool,
    pub isconstraint: bool,
    pub iswithoutoverlaps: bool,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub transformed: bool,
    pub concurrent: bool,
    pub if_not_exists: bool,
    pub reset_default_tblspc: bool,
}

/// `CreateStatsStmt` - CREATE STATISTICS.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateStatsStmt {
    pub type_: NodeTag,
    pub defnames: *mut List,
    pub stat_types: *mut List,
    pub exprs: *mut List,
    pub relations: *mut List,
    pub stxcomment: *mut c_char,
    pub transformed: bool,
    pub if_not_exists: bool,
}

/// `AlterStatsStmt` - ALTER STATISTICS.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterStatsStmt {
    pub type_: NodeTag,
    pub defnames: *mut List,
    pub stxstattarget: *mut Node,
    pub missing_ok: bool,
}

/// `CreateSeqStmt` - CREATE SEQUENCE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateSeqStmt {
    pub type_: NodeTag,
    pub sequence: *mut RangeVar,
    pub options: *mut List,
    pub ownerId: Oid,
    pub for_identity: bool,
    pub if_not_exists: bool,
}

/// `AlterSeqStmt` - ALTER SEQUENCE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterSeqStmt {
    pub type_: NodeTag,
    pub sequence: *mut RangeVar,
    pub options: *mut List,
    pub for_identity: bool,
    pub missing_ok: bool,
}

// ---------------------------------------------------------------------------
// Functions / operators / classes.
// ---------------------------------------------------------------------------

/// `DefineStmt` - CREATE AGGREGATE/OPERATOR/TYPE/... via DEFINE.
#[repr(C)]
#[derive(Clone, Copy)]
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

/// `CreateOpClassStmt` - CREATE OPERATOR CLASS.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateOpClassStmt {
    pub type_: NodeTag,
    pub opclassname: *mut List,
    pub opfamilyname: *mut List,
    pub amname: *mut c_char,
    pub datatype: *mut TypeName,
    pub items: *mut List,
    pub isDefault: bool,
}

/// `CreateOpFamilyStmt` - CREATE OPERATOR FAMILY.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateOpFamilyStmt {
    pub type_: NodeTag,
    pub opfamilyname: *mut List,
    pub amname: *mut c_char,
}

/// `AlterOpFamilyStmt` - ALTER OPERATOR FAMILY.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterOpFamilyStmt {
    pub type_: NodeTag,
    pub opfamilyname: *mut List,
    pub amname: *mut c_char,
    pub isDrop: bool,
    pub items: *mut List,
}

/// `CreateFunctionStmt` - CREATE FUNCTION / PROCEDURE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateFunctionStmt {
    pub type_: NodeTag,
    pub is_procedure: bool,
    pub replace: bool,
    pub funcname: *mut List,
    pub parameters: *mut List,
    pub returnType: *mut TypeName,
    pub options: *mut List,
    pub sql_body: *mut Node,
}

/// `AlterFunctionStmt` - ALTER FUNCTION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterFunctionStmt {
    pub type_: NodeTag,
    pub objtype: ObjectType,
    pub func: *mut ObjectWithArgs,
    pub actions: *mut List,
}

/// `DoStmt` - DO (anonymous code block).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DoStmt {
    pub type_: NodeTag,
    pub args: *mut List,
}

/// `CallStmt` - CALL (procedure invocation).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CallStmt {
    pub type_: NodeTag,
    pub funccall: *mut FuncCall,
    pub funcexpr: *mut FuncExpr,
    pub outargs: *mut List,
}

// ---------------------------------------------------------------------------
// Drop / rename / ownership / object alterations.
// ---------------------------------------------------------------------------

/// `DropStmt` - DROP <object>.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DropStmt {
    pub type_: NodeTag,
    pub objects: *mut List,
    pub removeType: ObjectType,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
    pub concurrent: bool,
}

/// `TruncateStmt` - TRUNCATE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TruncateStmt {
    pub type_: NodeTag,
    pub relations: *mut List,
    pub restart_seqs: bool,
    pub behavior: DropBehavior,
}

/// `CommentStmt` - COMMENT ON.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CommentStmt {
    pub type_: NodeTag,
    pub objtype: ObjectType,
    pub object: *mut Node,
    pub comment: *mut c_char,
}

/// `SecLabelStmt` - SECURITY LABEL.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SecLabelStmt {
    pub type_: NodeTag,
    pub objtype: ObjectType,
    pub object: *mut Node,
    pub provider: *mut c_char,
    pub label: *mut c_char,
}

/// `RenameStmt` - ALTER ... RENAME.
#[repr(C)]
#[derive(Clone, Copy)]
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

/// `AlterObjectDependsStmt` - ALTER ... DEPENDS ON EXTENSION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterObjectDependsStmt {
    pub type_: NodeTag,
    pub objectType: ObjectType,
    pub relation: *mut RangeVar,
    pub object: *mut Node,
    pub extname: *mut crate::String_,
    pub remove: bool,
}

/// `AlterObjectSchemaStmt` - ALTER ... SET SCHEMA.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterObjectSchemaStmt {
    pub type_: NodeTag,
    pub objectType: ObjectType,
    pub relation: *mut RangeVar,
    pub object: *mut Node,
    pub newschema: *mut c_char,
    pub missing_ok: bool,
}

/// `AlterOwnerStmt` - ALTER ... OWNER TO.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterOwnerStmt {
    pub type_: NodeTag,
    pub objectType: ObjectType,
    pub relation: *mut RangeVar,
    pub object: *mut Node,
    pub newowner: *mut RoleSpec,
}

/// `AlterOperatorStmt` - ALTER OPERATOR.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterOperatorStmt {
    pub type_: NodeTag,
    pub opername: *mut ObjectWithArgs,
    pub options: *mut List,
}

/// `AlterTypeStmt` - ALTER TYPE (base type properties).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterTypeStmt {
    pub type_: NodeTag,
    pub typeName: *mut List,
    pub options: *mut List,
}

/// `RuleStmt` - CREATE RULE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RuleStmt {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub rulename: *mut c_char,
    pub whereClause: *mut Node,
    pub event: CmdType,
    pub instead: bool,
    pub actions: *mut List,
    pub replace: bool,
}

// ---------------------------------------------------------------------------
// Notify / listen.
// ---------------------------------------------------------------------------

/// `NotifyStmt` - NOTIFY.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct NotifyStmt {
    pub type_: NodeTag,
    pub conditionname: *mut c_char,
    pub payload: *mut c_char,
}

/// `ListenStmt` - LISTEN.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ListenStmt {
    pub type_: NodeTag,
    pub conditionname: *mut c_char,
}

/// `UnlistenStmt` - UNLISTEN.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct UnlistenStmt {
    pub type_: NodeTag,
    pub conditionname: *mut c_char,
}

// ---------------------------------------------------------------------------
// Transaction control.
// ---------------------------------------------------------------------------

/// `TransactionStmt` - BEGIN/COMMIT/ROLLBACK/SAVEPOINT/PREPARE TRANSACTION/...
#[repr(C)]
#[derive(Clone, Copy)]
pub struct TransactionStmt {
    pub type_: NodeTag,
    pub kind: TransactionStmtKind,
    pub options: *mut List,
    pub savepoint_name: *mut c_char,
    pub gid: *mut c_char,
    pub chain: bool,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// SET / SHOW / utility.
// ---------------------------------------------------------------------------

/// `VariableSetStmt` - SET / RESET.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VariableSetStmt {
    pub type_: NodeTag,
    pub kind: VariableSetKind,
    pub name: *mut c_char,
    pub args: *mut List,
    pub jumble_args: bool,
    pub is_local: bool,
    pub location: ParseLoc,
}

/// `VariableShowStmt` - SHOW.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VariableShowStmt {
    pub type_: NodeTag,
    pub name: *mut c_char,
}

/// `LoadStmt` - LOAD.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LoadStmt {
    pub type_: NodeTag,
    pub filename: *mut c_char,
}

/// `CopyStmt` - COPY.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CopyStmt {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub query: *mut Node,
    pub attlist: *mut List,
    pub is_from: bool,
    pub is_program: bool,
    pub filename: *mut c_char,
    pub options: *mut List,
    pub whereClause: *mut Node,
}

// ---------------------------------------------------------------------------
// Cursor / prepared statement utilities.
// ---------------------------------------------------------------------------

/// `DeclareCursorStmt` - DECLARE CURSOR.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DeclareCursorStmt {
    pub type_: NodeTag,
    pub portalname: *mut c_char,
    pub options: c_int,
    pub query: *mut Node,
}

/// `ClosePortalStmt` - CLOSE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ClosePortalStmt {
    pub type_: NodeTag,
    pub portalname: *mut c_char,
}

/// `FetchStmt` - FETCH / MOVE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FetchStmt {
    pub type_: NodeTag,
    pub direction: FetchDirection,
    pub howMany: c_long,
    pub portalname: *mut c_char,
    pub ismove: bool,
}

/// `PrepareStmt` - PREPARE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PrepareStmt {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub argtypes: *mut List,
    pub query: *mut Node,
}

/// `ExecuteStmt` - EXECUTE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExecuteStmt {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub params: *mut List,
}

/// `DeallocateStmt` - DEALLOCATE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DeallocateStmt {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub isall: bool,
    pub location: ParseLoc,
}

// ---------------------------------------------------------------------------
// Explain / CTAS / matview / misc maintenance.
// ---------------------------------------------------------------------------

/// `ExplainStmt` - EXPLAIN.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ExplainStmt {
    pub type_: NodeTag,
    pub query: *mut Node,
    pub options: *mut List,
}

/// `CreateTableAsStmt` - CREATE TABLE AS / CREATE MATERIALIZED VIEW / SELECT INTO.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateTableAsStmt {
    pub type_: NodeTag,
    pub query: *mut Node,
    pub into: *mut IntoClause,
    pub objtype: ObjectType,
    pub is_select_into: bool,
    pub if_not_exists: bool,
}

/// `RefreshMatViewStmt` - REFRESH MATERIALIZED VIEW.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RefreshMatViewStmt {
    pub type_: NodeTag,
    pub concurrent: bool,
    pub skipData: bool,
    pub relation: *mut RangeVar,
}

/// `CheckPointStmt` - CHECKPOINT.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CheckPointStmt {
    pub type_: NodeTag,
}

/// `DiscardStmt` - DISCARD.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DiscardStmt {
    pub type_: NodeTag,
    pub target: DiscardMode,
}

/// `LockStmt` - LOCK TABLE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct LockStmt {
    pub type_: NodeTag,
    pub relations: *mut List,
    pub mode: c_int,
    pub nowait: bool,
}

/// `ConstraintsSetStmt` - SET CONSTRAINTS.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ConstraintsSetStmt {
    pub type_: NodeTag,
    pub constraints: *mut List,
    pub deferred: bool,
}

/// `ReindexStmt` - REINDEX.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReindexStmt {
    pub type_: NodeTag,
    pub kind: ReindexObjectType,
    pub relation: *mut RangeVar,
    pub name: *const c_char,
    pub params: *mut List,
}

/// `ClusterStmt` - CLUSTER.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ClusterStmt {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub indexname: *mut c_char,
    pub params: *mut List,
}

/// `VacuumStmt` - VACUUM / ANALYZE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct VacuumStmt {
    pub type_: NodeTag,
    pub options: *mut List,
    pub rels: *mut List,
    pub is_vacuumcmd: bool,
}

// ---------------------------------------------------------------------------
// Roles / grants.
// ---------------------------------------------------------------------------

/// `GrantStmt` - GRANT / REVOKE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GrantStmt {
    pub type_: NodeTag,
    pub is_grant: bool,
    pub targtype: GrantTargetType,
    pub objtype: ObjectType,
    pub objects: *mut List,
    pub privileges: *mut List,
    pub grantees: *mut List,
    pub grant_option: bool,
    pub grantor: *mut RoleSpec,
    pub behavior: DropBehavior,
}

/// `GrantRoleStmt` - GRANT / REVOKE role membership.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct GrantRoleStmt {
    pub type_: NodeTag,
    pub granted_roles: *mut List,
    pub grantee_roles: *mut List,
    pub is_grant: bool,
    pub opt: *mut List,
    pub grantor: *mut RoleSpec,
    pub behavior: DropBehavior,
}

/// `AlterDefaultPrivilegesStmt` - ALTER DEFAULT PRIVILEGES.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterDefaultPrivilegesStmt {
    pub type_: NodeTag,
    pub options: *mut List,
    pub action: *mut GrantStmt,
}

/// `CreateRoleStmt` - CREATE ROLE / USER / GROUP.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateRoleStmt {
    pub type_: NodeTag,
    pub stmt_type: RoleStmtType,
    pub role: *mut c_char,
    pub options: *mut List,
}

/// `AlterRoleStmt` - ALTER ROLE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterRoleStmt {
    pub type_: NodeTag,
    pub role: *mut RoleSpec,
    pub options: *mut List,
    pub action: c_int,
}

/// `AlterRoleSetStmt` - ALTER ROLE ... SET.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterRoleSetStmt {
    pub type_: NodeTag,
    pub role: *mut RoleSpec,
    pub database: *mut c_char,
    pub setstmt: *mut VariableSetStmt,
}

/// `DropRoleStmt` - DROP ROLE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DropRoleStmt {
    pub type_: NodeTag,
    pub roles: *mut List,
    pub missing_ok: bool,
}

/// `DropOwnedStmt` - DROP OWNED.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DropOwnedStmt {
    pub type_: NodeTag,
    pub roles: *mut List,
    pub behavior: DropBehavior,
}

/// `ReassignOwnedStmt` - REASSIGN OWNED.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ReassignOwnedStmt {
    pub type_: NodeTag,
    pub roles: *mut List,
    pub newrole: *mut RoleSpec,
}

// ---------------------------------------------------------------------------
// Tablespaces.
// ---------------------------------------------------------------------------

/// `CreateTableSpaceStmt` - CREATE TABLESPACE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateTableSpaceStmt {
    pub type_: NodeTag,
    pub tablespacename: *mut c_char,
    pub owner: *mut RoleSpec,
    pub location: *mut c_char,
    pub options: *mut List,
}

/// `DropTableSpaceStmt` - DROP TABLESPACE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DropTableSpaceStmt {
    pub type_: NodeTag,
    pub tablespacename: *mut c_char,
    pub missing_ok: bool,
}

/// `AlterTableSpaceOptionsStmt` - ALTER TABLESPACE ... SET/RESET.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterTableSpaceOptionsStmt {
    pub type_: NodeTag,
    pub tablespacename: *mut c_char,
    pub options: *mut List,
    pub isReset: bool,
}

/// `AlterTableMoveAllStmt` - ALTER TABLESPACE ... MOVE ALL.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterTableMoveAllStmt {
    pub type_: NodeTag,
    pub orig_tablespacename: *mut c_char,
    pub objtype: ObjectType,
    pub roles: *mut List,
    pub new_tablespacename: *mut c_char,
    pub nowait: bool,
}

// ---------------------------------------------------------------------------
// Extensions.
// ---------------------------------------------------------------------------

/// `CreateExtensionStmt` - CREATE EXTENSION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateExtensionStmt {
    pub type_: NodeTag,
    pub extname: *mut c_char,
    pub if_not_exists: bool,
    pub options: *mut List,
}

/// `AlterExtensionStmt` - ALTER EXTENSION ... UPDATE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterExtensionStmt {
    pub type_: NodeTag,
    pub extname: *mut c_char,
    pub options: *mut List,
}

/// `AlterExtensionContentsStmt` - ALTER EXTENSION ... ADD/DROP.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterExtensionContentsStmt {
    pub type_: NodeTag,
    pub extname: *mut c_char,
    pub action: c_int,
    pub objtype: ObjectType,
    pub object: *mut Node,
}

// ---------------------------------------------------------------------------
// Foreign data wrappers / servers / tables / user mappings.
// ---------------------------------------------------------------------------

/// `CreateFdwStmt` - CREATE FOREIGN DATA WRAPPER.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateFdwStmt {
    pub type_: NodeTag,
    pub fdwname: *mut c_char,
    pub func_options: *mut List,
    pub options: *mut List,
}

/// `AlterFdwStmt` - ALTER FOREIGN DATA WRAPPER.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterFdwStmt {
    pub type_: NodeTag,
    pub fdwname: *mut c_char,
    pub func_options: *mut List,
    pub options: *mut List,
}

/// `CreateForeignServerStmt` - CREATE SERVER.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateForeignServerStmt {
    pub type_: NodeTag,
    pub servername: *mut c_char,
    pub servertype: *mut c_char,
    pub version: *mut c_char,
    pub fdwname: *mut c_char,
    pub if_not_exists: bool,
    pub options: *mut List,
}

/// `AlterForeignServerStmt` - ALTER SERVER.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterForeignServerStmt {
    pub type_: NodeTag,
    pub servername: *mut c_char,
    pub version: *mut c_char,
    pub options: *mut List,
    pub has_version: bool,
}

/// `CreateForeignTableStmt` - CREATE FOREIGN TABLE; embeds a `CreateStmt`
/// base (the C struct inlines `CreateStmt base`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateForeignTableStmt {
    pub base: CreateStmt,
    pub servername: *mut c_char,
    pub options: *mut List,
}

/// `CreateUserMappingStmt` - CREATE USER MAPPING.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateUserMappingStmt {
    pub type_: NodeTag,
    pub user: *mut RoleSpec,
    pub servername: *mut c_char,
    pub if_not_exists: bool,
    pub options: *mut List,
}

/// `AlterUserMappingStmt` - ALTER USER MAPPING.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterUserMappingStmt {
    pub type_: NodeTag,
    pub user: *mut RoleSpec,
    pub servername: *mut c_char,
    pub options: *mut List,
}

/// `DropUserMappingStmt` - DROP USER MAPPING.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DropUserMappingStmt {
    pub type_: NodeTag,
    pub user: *mut RoleSpec,
    pub servername: *mut c_char,
    pub missing_ok: bool,
}

/// `ImportForeignSchemaStmt` - IMPORT FOREIGN SCHEMA.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ImportForeignSchemaStmt {
    pub type_: NodeTag,
    pub server_name: *mut c_char,
    pub remote_schema: *mut c_char,
    pub local_schema: *mut c_char,
    pub list_type: ImportForeignSchemaType,
    pub table_list: *mut List,
    pub options: *mut List,
}

// ---------------------------------------------------------------------------
// Policies / access methods / triggers / event triggers / languages.
// ---------------------------------------------------------------------------

/// `CreatePolicyStmt` - CREATE POLICY.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreatePolicyStmt {
    pub type_: NodeTag,
    pub policy_name: *mut c_char,
    pub table: *mut RangeVar,
    pub cmd_name: *mut c_char,
    pub permissive: bool,
    pub roles: *mut List,
    pub qual: *mut Node,
    pub with_check: *mut Node,
}

/// `AlterPolicyStmt` - ALTER POLICY.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterPolicyStmt {
    pub type_: NodeTag,
    pub policy_name: *mut c_char,
    pub table: *mut RangeVar,
    pub roles: *mut List,
    pub qual: *mut Node,
    pub with_check: *mut Node,
}

/// `CreateAmStmt` - CREATE ACCESS METHOD.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateAmStmt {
    pub type_: NodeTag,
    pub amname: *mut c_char,
    pub handler_name: *mut List,
    pub amtype: c_char,
}

/// `CreateTrigStmt` - CREATE TRIGGER.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateTrigStmt {
    pub type_: NodeTag,
    pub replace: bool,
    pub isconstraint: bool,
    pub trigname: *mut c_char,
    pub relation: *mut RangeVar,
    pub funcname: *mut List,
    pub args: *mut List,
    pub row: bool,
    pub timing: Int16,
    pub events: Int16,
    pub columns: *mut List,
    pub whenClause: *mut Node,
    pub transitionRels: *mut List,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub constrrel: *mut RangeVar,
}

/// `CreateEventTrigStmt` - CREATE EVENT TRIGGER.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateEventTrigStmt {
    pub type_: NodeTag,
    pub trigname: *mut c_char,
    pub eventname: *mut c_char,
    pub whenclause: *mut List,
    pub funcname: *mut List,
}

/// `AlterEventTrigStmt` - ALTER EVENT TRIGGER.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterEventTrigStmt {
    pub type_: NodeTag,
    pub trigname: *mut c_char,
    pub tgenabled: c_char,
}

/// `CreatePLangStmt` - CREATE LANGUAGE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreatePLangStmt {
    pub type_: NodeTag,
    pub replace: bool,
    pub plname: *mut c_char,
    pub plhandler: *mut List,
    pub plinline: *mut List,
    pub plvalidator: *mut List,
    pub pltrusted: bool,
}

// ---------------------------------------------------------------------------
// Databases.
// ---------------------------------------------------------------------------

/// `CreatedbStmt` - CREATE DATABASE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreatedbStmt {
    pub type_: NodeTag,
    pub dbname: *mut c_char,
    pub options: *mut List,
}

/// `AlterDatabaseStmt` - ALTER DATABASE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterDatabaseStmt {
    pub type_: NodeTag,
    pub dbname: *mut c_char,
    pub options: *mut List,
}

/// `AlterDatabaseRefreshCollStmt` - ALTER DATABASE ... REFRESH COLLATION VERSION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterDatabaseRefreshCollStmt {
    pub type_: NodeTag,
    pub dbname: *mut c_char,
}

/// `AlterDatabaseSetStmt` - ALTER DATABASE ... SET.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterDatabaseSetStmt {
    pub type_: NodeTag,
    pub dbname: *mut c_char,
    pub setstmt: *mut VariableSetStmt,
}

/// `DropdbStmt` - DROP DATABASE.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DropdbStmt {
    pub type_: NodeTag,
    pub dbname: *mut c_char,
    pub missing_ok: bool,
    pub options: *mut List,
}

/// `AlterSystemStmt` - ALTER SYSTEM.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterSystemStmt {
    pub type_: NodeTag,
    pub setstmt: *mut VariableSetStmt,
}

// ---------------------------------------------------------------------------
// Conversions / casts / transforms.
// ---------------------------------------------------------------------------

/// `CreateConversionStmt` - CREATE CONVERSION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateConversionStmt {
    pub type_: NodeTag,
    pub conversion_name: *mut List,
    pub for_encoding_name: *mut c_char,
    pub to_encoding_name: *mut c_char,
    pub func_name: *mut List,
    pub def: bool,
}

/// `CreateCastStmt` - CREATE CAST.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateCastStmt {
    pub type_: NodeTag,
    pub sourcetype: *mut TypeName,
    pub targettype: *mut TypeName,
    pub func: *mut ObjectWithArgs,
    pub context: CoercionContext,
    pub inout: bool,
}

/// `CreateTransformStmt` - CREATE TRANSFORM.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateTransformStmt {
    pub type_: NodeTag,
    pub replace: bool,
    pub type_name: *mut TypeName,
    pub lang: *mut c_char,
    pub fromsql: *mut ObjectWithArgs,
    pub tosql: *mut ObjectWithArgs,
}

// ---------------------------------------------------------------------------
// Text search.
// ---------------------------------------------------------------------------

/// `AlterTSDictionaryStmt` - ALTER TEXT SEARCH DICTIONARY.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterTSDictionaryStmt {
    pub type_: NodeTag,
    pub dictname: *mut List,
    pub options: *mut List,
}

/// `AlterTSConfigurationStmt` - ALTER TEXT SEARCH CONFIGURATION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterTSConfigurationStmt {
    pub type_: NodeTag,
    pub kind: AlterTSConfigType,
    pub cfgname: *mut List,
    pub tokentype: *mut List,
    pub dicts: *mut List,
    pub override_: bool,
    pub replace: bool,
    pub missing_ok: bool,
}

// ---------------------------------------------------------------------------
// Publications / subscriptions (logical replication).
// ---------------------------------------------------------------------------

/// `CreatePublicationStmt` - CREATE PUBLICATION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreatePublicationStmt {
    pub type_: NodeTag,
    pub pubname: *mut c_char,
    pub options: *mut List,
    pub pubobjects: *mut List,
    pub for_all_tables: bool,
}

/// `AlterPublicationStmt` - ALTER PUBLICATION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterPublicationStmt {
    pub type_: NodeTag,
    pub pubname: *mut c_char,
    pub options: *mut List,
    pub pubobjects: *mut List,
    pub for_all_tables: bool,
    pub action: AlterPublicationAction,
}

/// `CreateSubscriptionStmt` - CREATE SUBSCRIPTION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct CreateSubscriptionStmt {
    pub type_: NodeTag,
    pub subname: *mut c_char,
    pub conninfo: *mut c_char,
    pub publication: *mut List,
    pub options: *mut List,
}

/// `AlterSubscriptionStmt` - ALTER SUBSCRIPTION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct AlterSubscriptionStmt {
    pub type_: NodeTag,
    pub kind: AlterSubscriptionType,
    pub subname: *mut c_char,
    pub conninfo: *mut c_char,
    pub publication: *mut List,
    pub options: *mut List,
}

/// `DropSubscriptionStmt` - DROP SUBSCRIPTION.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DropSubscriptionStmt {
    pub type_: NodeTag,
    pub subname: *mut c_char,
    pub missing_ok: bool,
    pub behavior: DropBehavior,
}

// ---------------------------------------------------------------------------
// Compile-time layout assertions for representative structs.
//
// Each node struct begins with a `NodeTag` at offset 0 (so `((Node *) p)->type`
// resolves the tag), and a couple of representatives are size-checked against
// the C ABI on a 64-bit LP64 target.
// ---------------------------------------------------------------------------

const _: () = {
    use core::mem::{offset_of, size_of};

    // NodeTag is the first field of every node struct.
    assert!(offset_of!(CreateStmt, type_) == 0);
    assert!(offset_of!(ColumnDef, type_) == 0);
    assert!(offset_of!(Constraint, type_) == 0);
    assert!(offset_of!(AlterTableStmt, type_) == 0);
    assert!(offset_of!(IndexStmt, type_) == 0);
    assert!(offset_of!(GrantStmt, type_) == 0);
    assert!(offset_of!(TransactionStmt, type_) == 0);
    // CreateForeignTableStmt embeds CreateStmt as its base at offset 0, so its
    // NodeTag (the base's `type_`) is still at offset 0.
    assert!(offset_of!(CreateForeignTableStmt, base) == 0);
    assert!(offset_of!(CreateStmt, type_) == 0);

    // Representative size checks (LP64): pointers 8B, NodeTag 4B, bool 1B.
    // DefElem: tag(4) pad(4) + 2 ptrs(16) + ptr(8) + enum(4) + ParseLoc(4) = 40.
    assert!(size_of::<DefElem>() == 40);
    // RoleSpec: tag(4) enum(4) + ptr(8) + ParseLoc(4) pad(4) = 24.
    assert!(size_of::<RoleSpec>() == 24);
    // PartitionCmd: tag(4) pad(4) + 2 ptrs(16) + bool(1) pad(7) = 32.
    assert!(size_of::<PartitionCmd>() == 32);
    // AccessPriv: tag(4) pad(4) + 2 ptrs(16) = 24.
    assert!(size_of::<AccessPriv>() == 24);
};

// ---------------------------------------------------------------------------
// Coverage registration.
// ---------------------------------------------------------------------------

use crate::{NodeTypeCoverage, NodeTypeStatus};

/// Node types modelled by the DDL / utility-statement family.
///
/// `lib.rs` concatenates this slice with the other families' coverage into the
/// crate-wide coverage view; new structs ported here register themselves by
/// adding an entry to this list.
pub fn node_types_covered() -> &'static [NodeTypeStatus] {
    use crate::node_tags::*;
    const M: NodeTypeCoverage = NodeTypeCoverage::Modelled;
    &[
        // DDL-owned helper nodes (TypeName/CollateClause/SortBy/WindowDef/
        // FuncCall are registered by the raw-DML family and reused here).
        NodeTypeStatus {
            name: "RoleSpec",
            tag: T_RoleSpec,
            coverage: M,
        },
        // Column / constraint / option helper nodes.
        NodeTypeStatus {
            name: "ColumnDef",
            tag: T_ColumnDef,
            coverage: M,
        },
        NodeTypeStatus {
            name: "TableLikeClause",
            tag: T_TableLikeClause,
            coverage: M,
        },
        NodeTypeStatus {
            name: "IndexElem",
            tag: T_IndexElem,
            coverage: M,
        },
        NodeTypeStatus {
            name: "DefElem",
            tag: T_DefElem,
            coverage: M,
        },
        NodeTypeStatus {
            name: "Constraint",
            tag: T_Constraint,
            coverage: M,
        },
        NodeTypeStatus {
            name: "PartitionElem",
            tag: T_PartitionElem,
            coverage: M,
        },
        NodeTypeStatus {
            name: "PartitionSpec",
            tag: T_PartitionSpec,
            coverage: M,
        },
        NodeTypeStatus {
            name: "PartitionBoundSpec",
            tag: T_PartitionBoundSpec,
            coverage: M,
        },
        NodeTypeStatus {
            name: "PartitionRangeDatum",
            tag: T_PartitionRangeDatum,
            coverage: M,
        },
        NodeTypeStatus {
            name: "PartitionCmd",
            tag: T_PartitionCmd,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ObjectWithArgs",
            tag: T_ObjectWithArgs,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AccessPriv",
            tag: T_AccessPriv,
            coverage: M,
        },
        NodeTypeStatus {
            name: "FunctionParameter",
            tag: T_FunctionParameter,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateOpClassItem",
            tag: T_CreateOpClassItem,
            coverage: M,
        },
        NodeTypeStatus {
            name: "StatsElem",
            tag: T_StatsElem,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ReplicaIdentityStmt",
            tag: T_ReplicaIdentityStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ATAlterConstraint",
            tag: T_ATAlterConstraint,
            coverage: M,
        },
        NodeTypeStatus {
            name: "VacuumRelation",
            tag: T_VacuumRelation,
            coverage: M,
        },
        NodeTypeStatus {
            name: "PublicationTable",
            tag: T_PublicationTable,
            coverage: M,
        },
        NodeTypeStatus {
            name: "PublicationObjSpec",
            tag: T_PublicationObjSpec,
            coverage: M,
        },
        // Schema / table / type.
        NodeTypeStatus {
            name: "CreateSchemaStmt",
            tag: T_CreateSchemaStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterTableStmt",
            tag: T_AlterTableStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterTableCmd",
            tag: T_AlterTableCmd,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterCollationStmt",
            tag: T_AlterCollationStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterDomainStmt",
            tag: T_AlterDomainStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateStmt",
            tag: T_CreateStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CompositeTypeStmt",
            tag: T_CompositeTypeStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateEnumStmt",
            tag: T_CreateEnumStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateRangeStmt",
            tag: T_CreateRangeStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterEnumStmt",
            tag: T_AlterEnumStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ViewStmt",
            tag: T_ViewStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateDomainStmt",
            tag: T_CreateDomainStmt,
            coverage: M,
        },
        // Index / statistics / sequence.
        NodeTypeStatus {
            name: "IndexStmt",
            tag: T_IndexStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateStatsStmt",
            tag: T_CreateStatsStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterStatsStmt",
            tag: T_AlterStatsStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateSeqStmt",
            tag: T_CreateSeqStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterSeqStmt",
            tag: T_AlterSeqStmt,
            coverage: M,
        },
        // Functions / operators / classes.
        NodeTypeStatus {
            name: "DefineStmt",
            tag: T_DefineStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateOpClassStmt",
            tag: T_CreateOpClassStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateOpFamilyStmt",
            tag: T_CreateOpFamilyStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterOpFamilyStmt",
            tag: T_AlterOpFamilyStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateFunctionStmt",
            tag: T_CreateFunctionStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterFunctionStmt",
            tag: T_AlterFunctionStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "DoStmt",
            tag: T_DoStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CallStmt",
            tag: T_CallStmt,
            coverage: M,
        },
        // Drop / rename / ownership / object alterations.
        NodeTypeStatus {
            name: "DropStmt",
            tag: T_DropStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "TruncateStmt",
            tag: T_TruncateStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CommentStmt",
            tag: T_CommentStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "SecLabelStmt",
            tag: T_SecLabelStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "RenameStmt",
            tag: T_RenameStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterObjectDependsStmt",
            tag: T_AlterObjectDependsStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterObjectSchemaStmt",
            tag: T_AlterObjectSchemaStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterOwnerStmt",
            tag: T_AlterOwnerStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterOperatorStmt",
            tag: T_AlterOperatorStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterTypeStmt",
            tag: T_AlterTypeStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "RuleStmt",
            tag: T_RuleStmt,
            coverage: M,
        },
        // Notify / listen.
        NodeTypeStatus {
            name: "NotifyStmt",
            tag: T_NotifyStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ListenStmt",
            tag: T_ListenStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "UnlistenStmt",
            tag: T_UnlistenStmt,
            coverage: M,
        },
        // Transaction control.
        NodeTypeStatus {
            name: "TransactionStmt",
            tag: T_TransactionStmt,
            coverage: M,
        },
        // SET / SHOW / utility.
        NodeTypeStatus {
            name: "VariableSetStmt",
            tag: T_VariableSetStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "VariableShowStmt",
            tag: T_VariableShowStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "LoadStmt",
            tag: T_LoadStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CopyStmt",
            tag: T_CopyStmt,
            coverage: M,
        },
        // Cursor / prepared statement utilities.
        NodeTypeStatus {
            name: "DeclareCursorStmt",
            tag: T_DeclareCursorStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ClosePortalStmt",
            tag: T_ClosePortalStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "FetchStmt",
            tag: T_FetchStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "PrepareStmt",
            tag: T_PrepareStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ExecuteStmt",
            tag: T_ExecuteStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "DeallocateStmt",
            tag: T_DeallocateStmt,
            coverage: M,
        },
        // Explain / CTAS / matview / maintenance.
        NodeTypeStatus {
            name: "ExplainStmt",
            tag: T_ExplainStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateTableAsStmt",
            tag: T_CreateTableAsStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "RefreshMatViewStmt",
            tag: T_RefreshMatViewStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CheckPointStmt",
            tag: T_CheckPointStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "DiscardStmt",
            tag: T_DiscardStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "LockStmt",
            tag: T_LockStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ConstraintsSetStmt",
            tag: T_ConstraintsSetStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ReindexStmt",
            tag: T_ReindexStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ClusterStmt",
            tag: T_ClusterStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "VacuumStmt",
            tag: T_VacuumStmt,
            coverage: M,
        },
        // Roles / grants.
        NodeTypeStatus {
            name: "GrantStmt",
            tag: T_GrantStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "GrantRoleStmt",
            tag: T_GrantRoleStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterDefaultPrivilegesStmt",
            tag: T_AlterDefaultPrivilegesStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateRoleStmt",
            tag: T_CreateRoleStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterRoleStmt",
            tag: T_AlterRoleStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterRoleSetStmt",
            tag: T_AlterRoleSetStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "DropRoleStmt",
            tag: T_DropRoleStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "DropOwnedStmt",
            tag: T_DropOwnedStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ReassignOwnedStmt",
            tag: T_ReassignOwnedStmt,
            coverage: M,
        },
        // Tablespaces.
        NodeTypeStatus {
            name: "CreateTableSpaceStmt",
            tag: T_CreateTableSpaceStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "DropTableSpaceStmt",
            tag: T_DropTableSpaceStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterTableSpaceOptionsStmt",
            tag: T_AlterTableSpaceOptionsStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterTableMoveAllStmt",
            tag: T_AlterTableMoveAllStmt,
            coverage: M,
        },
        // Extensions.
        NodeTypeStatus {
            name: "CreateExtensionStmt",
            tag: T_CreateExtensionStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterExtensionStmt",
            tag: T_AlterExtensionStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterExtensionContentsStmt",
            tag: T_AlterExtensionContentsStmt,
            coverage: M,
        },
        // Foreign data wrappers / servers / tables / user mappings.
        NodeTypeStatus {
            name: "CreateFdwStmt",
            tag: T_CreateFdwStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterFdwStmt",
            tag: T_AlterFdwStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateForeignServerStmt",
            tag: T_CreateForeignServerStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterForeignServerStmt",
            tag: T_AlterForeignServerStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateForeignTableStmt",
            tag: T_CreateForeignTableStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateUserMappingStmt",
            tag: T_CreateUserMappingStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterUserMappingStmt",
            tag: T_AlterUserMappingStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "DropUserMappingStmt",
            tag: T_DropUserMappingStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "ImportForeignSchemaStmt",
            tag: T_ImportForeignSchemaStmt,
            coverage: M,
        },
        // Policies / access methods / triggers / event triggers / languages.
        NodeTypeStatus {
            name: "CreatePolicyStmt",
            tag: T_CreatePolicyStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterPolicyStmt",
            tag: T_AlterPolicyStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateAmStmt",
            tag: T_CreateAmStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateTrigStmt",
            tag: T_CreateTrigStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateEventTrigStmt",
            tag: T_CreateEventTrigStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterEventTrigStmt",
            tag: T_AlterEventTrigStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreatePLangStmt",
            tag: T_CreatePLangStmt,
            coverage: M,
        },
        // Databases.
        NodeTypeStatus {
            name: "CreatedbStmt",
            tag: T_CreatedbStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterDatabaseStmt",
            tag: T_AlterDatabaseStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterDatabaseRefreshCollStmt",
            tag: T_AlterDatabaseRefreshCollStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterDatabaseSetStmt",
            tag: T_AlterDatabaseSetStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "DropdbStmt",
            tag: T_DropdbStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterSystemStmt",
            tag: T_AlterSystemStmt,
            coverage: M,
        },
        // Conversions / casts / transforms.
        NodeTypeStatus {
            name: "CreateConversionStmt",
            tag: T_CreateConversionStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateCastStmt",
            tag: T_CreateCastStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateTransformStmt",
            tag: T_CreateTransformStmt,
            coverage: M,
        },
        // Text search.
        NodeTypeStatus {
            name: "AlterTSDictionaryStmt",
            tag: T_AlterTSDictionaryStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterTSConfigurationStmt",
            tag: T_AlterTSConfigurationStmt,
            coverage: M,
        },
        // Publications / subscriptions.
        NodeTypeStatus {
            name: "CreatePublicationStmt",
            tag: T_CreatePublicationStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterPublicationStmt",
            tag: T_AlterPublicationStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "CreateSubscriptionStmt",
            tag: T_CreateSubscriptionStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "AlterSubscriptionStmt",
            tag: T_AlterSubscriptionStmt,
            coverage: M,
        },
        NodeTypeStatus {
            name: "DropSubscriptionStmt",
            tag: T_DropSubscriptionStmt,
            coverage: M,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::{offset_of, size_of};

    /// Every DDL/utility node struct begins with its `NodeTag` at offset 0, so
    /// `((Node *) p)->type` dispatches correctly. `CreateForeignTableStmt`
    /// inlines a `CreateStmt` base at offset 0, so its tag is still at offset 0.
    #[test]
    fn node_tag_header_at_offset_zero() {
        assert_eq!(offset_of!(CreateStmt, type_), 0);
        assert_eq!(offset_of!(AlterTableStmt, type_), 0);
        assert_eq!(offset_of!(AlterTableCmd, type_), 0);
        assert_eq!(offset_of!(IndexStmt, type_), 0);
        assert_eq!(offset_of!(GrantStmt, type_), 0);
        assert_eq!(offset_of!(DropStmt, type_), 0);
        assert_eq!(offset_of!(RenameStmt, type_), 0);
        assert_eq!(offset_of!(TransactionStmt, type_), 0);
        assert_eq!(offset_of!(VariableSetStmt, type_), 0);
        assert_eq!(offset_of!(ColumnDef, type_), 0);
        assert_eq!(offset_of!(Constraint, type_), 0);
        assert_eq!(offset_of!(DefElem, type_), 0);
        assert_eq!(offset_of!(RoleSpec, type_), 0);
        // The FDW-table statement embeds CreateStmt as its base; tag stays at 0.
        assert_eq!(offset_of!(CreateForeignTableStmt, base), 0);
        assert_eq!(
            offset_of!(CreateForeignTableStmt, base) + offset_of!(CreateStmt, type_),
            0
        );
    }

    /// Representative field offsets / sizes against the LP64 C ABI (NodeTag 4B,
    /// pointers 8B, bool/char 1B, enums 4B, int16 2B).
    #[test]
    fn representative_layouts_match_c_abi() {
        // RoleSpec: tag(4) roletype(4) | rolename(8) | location(4) pad(4) = 24.
        assert_eq!(offset_of!(RoleSpec, roletype), 4);
        assert_eq!(offset_of!(RoleSpec, rolename), 8);
        assert_eq!(offset_of!(RoleSpec, location), 16);
        assert_eq!(size_of::<RoleSpec>(), 24);

        // DefElem: tag(4) pad(4) | defnamespace(8) defname(8) arg(8) |
        //          defaction(4) location(4) = 40.
        assert_eq!(offset_of!(DefElem, defnamespace), 8);
        assert_eq!(offset_of!(DefElem, arg), 24);
        assert_eq!(offset_of!(DefElem, defaction), 32);
        assert_eq!(offset_of!(DefElem, location), 36);
        assert_eq!(size_of::<DefElem>(), 40);

        // ColumnDef: int16 inhcount sits right after the typeName/compression
        // pointers; colname@8, typeName@16, compression@24, inhcount@32.
        assert_eq!(offset_of!(ColumnDef, colname), 8);
        assert_eq!(offset_of!(ColumnDef, typeName), 16);
        assert_eq!(offset_of!(ColumnDef, compression), 24);
        assert_eq!(offset_of!(ColumnDef, inhcount), 32);

        // CreateForeignTableStmt embeds the whole CreateStmt, then two more
        // members; its size is CreateStmt rounded up plus servername+options.
        assert!(size_of::<CreateForeignTableStmt>() >= size_of::<CreateStmt>());
        assert_eq!(
            offset_of!(CreateForeignTableStmt, servername),
            size_of::<CreateStmt>()
        );

        // PartitionCmd: tag(4) pad(4) | name(8) bound(8) | concurrent(1) pad(7).
        assert_eq!(offset_of!(PartitionCmd, name), 8);
        assert_eq!(offset_of!(PartitionCmd, bound), 16);
        assert_eq!(offset_of!(PartitionCmd, concurrent), 24);
        assert_eq!(size_of::<PartitionCmd>(), 32);
    }

    /// The family registers exactly as many coverage entries as it has, and the
    /// tags are all real (never `T_Invalid`).
    #[test]
    fn coverage_is_populated_and_valid() {
        use crate::node_tags::T_Invalid;
        let covered = node_types_covered();
        assert!(covered.len() >= 120);
        for entry in covered {
            assert_ne!(entry.tag, T_Invalid, "{} mapped to T_Invalid", entry.name);
            assert_eq!(entry.coverage, NodeTypeCoverage::Modelled);
        }
    }
}
