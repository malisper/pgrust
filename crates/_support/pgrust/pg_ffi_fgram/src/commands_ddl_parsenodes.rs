//! Parse-node command structs from `nodes/parsenodes.h` consumed by the
//! object-creation DDL command crates `backend/commands/{typecmds,functioncmds,
//! indexcmds,sequence,opclasscmds,trigger}.c`, plus the supporting node structs
//! (`ColumnDef`, `Constraint`, `IndexElem`, `CollateClause`, …) and the
//! `FormData_pg_sequence` / `TriggerDesc` catalog/relcache structs they touch.
//!
//! Each `struct` mirrors the C layout field-for-field (`#[repr(C)]`, same field
//! order, first field `type_: NodeTag`).  Pointers to node types not modeled in
//! detail are carried as `*mut Node`/`*mut List`/`*mut RangeVar`, matching the C
//! `Node *`/`List *`/`RangeVar *` shapes.
//!
//! `T_*` NodeTag discriminants are verified against
//! `build-rust/src/include/nodes/nodetags.h` (PostgreSQL 18.3).  The statement
//! structs that already live in `commands_parsenodes.rs` (`DefineStmt`,
//! `CompositeTypeStmt`, `CreateEnumStmt`, `CreateRangeStmt`, `AlterEnumStmt`,
//! `DropStmt`, …) are NOT redefined here.

use core::ffi::{c_char, c_int};

use crate::{
    int64, uint32, DropBehavior, List, Node, NodeTag, ObjectWithArgs, Oid, ParseLoc, RangeVar,
    RelFileNumber, SubTransactionId, Trigger, TypeName,
};

// ---------------------------------------------------------------------------
// NodeTag discriminants (nodes/nodetags.h, PostgreSQL 18.3)
// ---------------------------------------------------------------------------

pub const T_RangeVar: NodeTag = 3;
pub const T_ColumnDef: NodeTag = 90;
pub const T_TableLikeClause: NodeTag = 91;
pub const T_IndexElem: NodeTag = 92;
pub const T_AlterDomainStmt: NodeTag = 151;
pub const T_CreateStmt: NodeTag = 160;
pub const T_Constraint: NodeTag = 161;
pub const T_CreateDomainStmt: NodeTag = 192;
pub const T_AlterTypeStmt: NodeTag = 220;
pub const T_CreateTrigStmt: NodeTag = 181;
pub const T_CreateSeqStmt: NodeTag = 189;
pub const T_AlterSeqStmt: NodeTag = 190;
pub const T_CreateOpClassStmt: NodeTag = 193;
pub const T_CreateOpClassItem: NodeTag = 194;
pub const T_CreateOpFamilyStmt: NodeTag = 195;
pub const T_AlterOpFamilyStmt: NodeTag = 196;
pub const T_IndexStmt: NodeTag = 204;
pub const T_CreateStatsStmt: NodeTag = 205;
pub const T_StatsElem: NodeTag = 206;
pub const T_ReindexStmt: NodeTag = 248;
pub const T_CreateFunctionStmt: NodeTag = 208;
pub const T_FunctionParameter: NodeTag = 209;
pub const T_AlterFunctionStmt: NodeTag = 210;
pub const T_DoStmt: NodeTag = 211;
pub const T_InlineCodeBlock: NodeTag = 212;
pub const T_CallStmt: NodeTag = 213;
pub const T_CreateTransformStmt: NodeTag = 251;
pub const T_CollateClause: NodeTag = 74;
/// `T_CollateExpr` (primnodes.h) — the parse-analyzed COLLATE node (distinct
/// from the raw `CollateClause` parsenode); value from nodetags.h.
pub const T_CollateExpr: NodeTag = 31;
pub const T_CreateSubscriptionStmt: NodeTag = 263;
pub const T_AlterSubscriptionStmt: NodeTag = 264;
pub const T_DropSubscriptionStmt: NodeTag = 265;

// ---------------------------------------------------------------------------
// Supporting enums (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef enum SortByDir` (parsenodes.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum SortByDir {
    SORTBY_DEFAULT,
    SORTBY_ASC,
    SORTBY_DESC,
    /// not allowed in CREATE INDEX ...
    SORTBY_USING,
}
pub use SortByDir::*;

/// `typedef enum SortByNulls` (parsenodes.h).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum SortByNulls {
    SORTBY_NULLS_DEFAULT,
    SORTBY_NULLS_FIRST,
    SORTBY_NULLS_LAST,
}
pub use SortByNulls::*;

/// `typedef enum ConstrType` (parsenodes.h) — types of constraints.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ConstrType {
    CONSTR_NULL,
    CONSTR_NOTNULL,
    CONSTR_DEFAULT,
    CONSTR_IDENTITY,
    CONSTR_GENERATED,
    CONSTR_CHECK,
    CONSTR_PRIMARY,
    CONSTR_UNIQUE,
    CONSTR_EXCLUSION,
    CONSTR_FOREIGN,
    CONSTR_ATTR_DEFERRABLE,
    CONSTR_ATTR_NOT_DEFERRABLE,
    CONSTR_ATTR_DEFERRED,
    CONSTR_ATTR_IMMEDIATE,
    CONSTR_ATTR_ENFORCED,
    CONSTR_ATTR_NOT_ENFORCED,
}
pub use ConstrType::*;

/// `typedef enum FunctionParameterMode` (parsenodes.h).  The assigned values
/// appear in `pg_proc`, so the discriminants are the C `char` codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i8)]
pub enum FunctionParameterMode {
    /// input only
    FUNC_PARAM_IN = b'i' as i8,
    /// output only
    FUNC_PARAM_OUT = b'o' as i8,
    /// both
    FUNC_PARAM_INOUT = b'b' as i8,
    /// variadic (always input)
    FUNC_PARAM_VARIADIC = b'v' as i8,
    /// table function output column
    FUNC_PARAM_TABLE = b't' as i8,
    /// default; effectively same as IN
    FUNC_PARAM_DEFAULT = b'd' as i8,
}
pub use FunctionParameterMode::*;

// Foreign key action codes (parsenodes.h).
pub const FKCONSTR_ACTION_NOACTION: c_char = b'a' as c_char;
pub const FKCONSTR_ACTION_RESTRICT: c_char = b'r' as c_char;
pub const FKCONSTR_ACTION_CASCADE: c_char = b'c' as c_char;
pub const FKCONSTR_ACTION_SETNULL: c_char = b'n' as c_char;
pub const FKCONSTR_ACTION_SETDEFAULT: c_char = b'd' as c_char;

// Foreign key matchtype codes (parsenodes.h).
pub const FKCONSTR_MATCH_FULL: c_char = b'f' as c_char;
pub const FKCONSTR_MATCH_PARTIAL: c_char = b'p' as c_char;
pub const FKCONSTR_MATCH_SIMPLE: c_char = b's' as c_char;

// OpClass item type codes (parsenodes.h).
pub const OPCLASS_ITEM_OPERATOR: c_int = 1;
pub const OPCLASS_ITEM_FUNCTION: c_int = 2;
pub const OPCLASS_ITEM_STORAGETYPE: c_int = 3;

// ---------------------------------------------------------------------------
// Supporting node structs (nodes/parsenodes.h)
// ---------------------------------------------------------------------------

/// `typedef struct CollateClause` (parsenodes.h) — untransformed COLLATE spec.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CollateClause {
    pub type_: NodeTag,
    pub arg: *mut Node,
    pub collname: *mut List,
    pub location: ParseLoc,
}

/// `typedef struct CollateExpr` (primnodes.h) — the parse-analyzed COLLATE
/// node (`{ Expr xpr; Expr *arg; Oid collOid; ParseLoc location; }`); `xpr`
/// is just a `NodeTag`, so `arg` sits at the same offset as in `CollateClause`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CollateExpr {
    pub type_: NodeTag,
    pub arg: *mut Node,
    pub collOid: Oid,
    pub location: ParseLoc,
}

/// `typedef struct ColumnDef` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ColumnDef {
    pub type_: NodeTag,
    pub colname: *mut c_char,
    pub typeName: *mut TypeName,
    pub compression: *mut c_char,
    pub inhcount: i16,
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

/// `typedef struct TableLikeClause` (parsenodes.h) — CREATE TABLE ( ... LIKE ).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct TableLikeClause {
    pub type_: NodeTag,
    pub relation: *mut RangeVar,
    pub options: uint32,
    pub relationOid: Oid,
}

/// `typedef struct IndexElem` (parsenodes.h) — index parameters.
#[derive(Clone, Copy)]
#[repr(C)]
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

/// `typedef struct Constraint` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
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

/// `typedef struct StatsElem` (parsenodes.h) — CREATE STATISTICS parameter.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct StatsElem {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub expr: *mut Node,
}

// ---------------------------------------------------------------------------
// CREATE TYPE / domain command parse nodes (typecmds.c)
// ---------------------------------------------------------------------------

/// `typedef struct AlterDomainStmt` (parsenodes.h).
///
/// `subtype` is a `char`: `T` alter default, `N` drop not null, `O` set not
/// null, `C` add constraint, `X` drop constraint.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterDomainStmt {
    pub type_: NodeTag,
    pub subtype: c_char,
    pub typeName: *mut List,
    pub name: *mut c_char,
    pub def: *mut Node,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
}

/// `typedef struct CreateDomainStmt` (parsenodes.h) — CREATE DOMAIN.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateDomainStmt {
    pub type_: NodeTag,
    /// qualified name (list of String)
    pub domainname: *mut List,
    /// the base type
    pub typeName: *mut TypeName,
    /// untransformed COLLATE spec, if any
    pub collClause: *mut CollateClause,
    /// constraints (list of Constraint nodes)
    pub constraints: *mut List,
}

/// `typedef struct CreateStmt` (parsenodes.h) — used by `DefineCompositeType`
/// to drive `DefineRelation` for a composite-type rowtype.  Only the fields
/// touched by typecmds.c are exercised, but the full layout is modeled for ABI
/// faithfulness.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateStmt {
    pub type_: NodeTag,
    /// relation to create
    pub relation: *mut RangeVar,
    /// column definitions (list of ColumnDef)
    pub tableElts: *mut List,
    /// relations to inherit from (list of RangeVar)
    pub inhRelations: *mut List,
    /// FOR VALUES clause (PartitionBoundSpec *)
    pub partbound: *mut Node,
    /// PARTITION BY clause (PartitionSpec *)
    pub partspec: *mut Node,
    /// OF typename
    pub ofTypename: *mut TypeName,
    /// constraints (list of Constraint nodes)
    pub constraints: *mut List,
    /// NOT NULL constraints (ditto) — PG 18.3 (was missing from this struct).
    pub nnconstraints: *mut List,
    /// options from WITH clause
    pub options: *mut List,
    /// what do we do at COMMIT?
    pub oncommit: c_int,
    /// table space to use, or NULL
    pub tablespacename: *mut c_char,
    /// table access method
    pub accessMethod: *mut c_char,
    /// just do nothing if it already exists?
    pub if_not_exists: bool,
}

/// `typedef struct AlterTypeStmt` (parsenodes.h) — ALTER TYPE ... SET ( ... ).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterTypeStmt {
    pub type_: NodeTag,
    /// type name (list of String)
    pub typeName: *mut List,
    /// list of DefElem
    pub options: *mut List,
}

// ---------------------------------------------------------------------------
// CREATE FUNCTION / PROCEDURE command parse nodes (functioncmds.c)
// ---------------------------------------------------------------------------

/// `typedef struct CreateFunctionStmt` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
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

/// `typedef struct FunctionParameter` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct FunctionParameter {
    pub type_: NodeTag,
    pub name: *mut c_char,
    pub argType: *mut TypeName,
    pub mode: FunctionParameterMode,
    pub defexpr: *mut Node,
    pub location: ParseLoc,
}

/// `typedef struct AlterFunctionStmt` (parsenodes.h).  `objtype` is `ObjectType`
/// (carried as `c_int` to avoid depending on the `commands_parsenodes` enum;
/// same width and discriminants).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterFunctionStmt {
    pub type_: NodeTag,
    pub objtype: c_int,
    /// `ObjectWithArgs *func` — name and args of function, opaque here.
    pub func: *mut Node,
    pub actions: *mut List,
}

/// `typedef struct DoStmt` (parsenodes.h) — DO statement, raw parser output.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DoStmt {
    pub type_: NodeTag,
    pub args: *mut List,
}

/// `typedef struct InlineCodeBlock` (parsenodes.h) — execution-time API for DO.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct InlineCodeBlock {
    pub type_: NodeTag,
    pub source_text: *mut c_char,
    pub langOid: Oid,
    pub langIsTrusted: bool,
    pub atomic: bool,
}

/// `typedef struct CallStmt` (parsenodes.h).  `funccall` (`FuncCall *`) and
/// `funcexpr` (`FuncExpr *`) are carried opaquely as `*mut Node`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CallStmt {
    pub type_: NodeTag,
    pub funccall: *mut Node,
    pub funcexpr: *mut Node,
    pub outargs: *mut List,
}

/// `typedef struct CreateTransformStmt` (parsenodes.h) — CREATE TRANSFORM.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateTransformStmt {
    pub type_: NodeTag,
    pub replace: bool,
    pub type_name: *mut TypeName,
    pub lang: *mut c_char,
    pub fromsql: *mut ObjectWithArgs,
    pub tosql: *mut ObjectWithArgs,
}

// ---------------------------------------------------------------------------
// CREATE INDEX command parse nodes (indexcmds.c)
// ---------------------------------------------------------------------------

/// `typedef struct IndexStmt` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
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

/// `typedef struct CreateStatsStmt` (parsenodes.h) — CREATE STATISTICS.
#[derive(Clone, Copy)]
#[repr(C)]
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

/// `typedef enum ReindexObjectType` (parsenodes.h) — REINDEX target kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub enum ReindexObjectType {
    /// index
    REINDEX_OBJECT_INDEX = 0,
    /// table or materialized view
    REINDEX_OBJECT_TABLE,
    /// schema
    REINDEX_OBJECT_SCHEMA,
    /// system catalogs
    REINDEX_OBJECT_SYSTEM,
    /// database
    REINDEX_OBJECT_DATABASE,
}
pub use ReindexObjectType::*;

/// `typedef struct ReindexStmt` (parsenodes.h) — REINDEX command.  `name` is a
/// `const char *` (the database/schema name to reindex); carried as
/// `*const c_char`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ReindexStmt {
    pub type_: NodeTag,
    pub kind: ReindexObjectType,
    pub relation: *mut RangeVar,
    pub name: *const c_char,
    pub params: *mut List,
}

/// `typedef struct ReindexParams` (catalog/index.h) — options for REINDEX.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub struct ReindexParams {
    /// bitmask of `REINDEXOPT_*`
    pub options: u32,
    /// New tablespace to move indexes to.  `InvalidOid` to do nothing.
    pub tablespaceOid: Oid,
}

// --- REINDEXOPT_* flag bits (catalog/index.h) ------------------------------
/// print progress info
pub const REINDEXOPT_VERBOSE: u32 = 0x01;
/// report pgstat progress
pub const REINDEXOPT_REPORT_PROGRESS: u32 = 0x02;
/// skip missing relations
pub const REINDEXOPT_MISSING_OK: u32 = 0x04;
/// concurrent mode
pub const REINDEXOPT_CONCURRENTLY: u32 = 0x08;

// --- REINDEX_REL_* flag bits for reindex_relation (catalog/index.h) --------
pub const REINDEX_REL_PROCESS_TOAST: i32 = 0x01;
pub const REINDEX_REL_SUPPRESS_INDEX_USE: i32 = 0x02;
pub const REINDEX_REL_CHECK_CONSTRAINTS: i32 = 0x04;
pub const REINDEX_REL_FORCE_INDEXES_UNLOGGED: i32 = 0x08;
pub const REINDEX_REL_FORCE_INDEXES_PERMANENT: i32 = 0x10;

// --- index_create() flag bits (catalog/index.h) ----------------------------
pub const INDEX_CREATE_IS_PRIMARY: u16 = 1 << 0;
pub const INDEX_CREATE_ADD_CONSTRAINT: u16 = 1 << 1;
pub const INDEX_CREATE_SKIP_BUILD: u16 = 1 << 2;
pub const INDEX_CREATE_CONCURRENT: u16 = 1 << 3;
pub const INDEX_CREATE_IF_NOT_EXISTS: u16 = 1 << 4;
pub const INDEX_CREATE_PARTITIONED: u16 = 1 << 5;
pub const INDEX_CREATE_INVALID: u16 = 1 << 6;

// --- index constraint creation flag bits (catalog/index.h) ------------------
pub const INDEX_CONSTR_CREATE_MARK_AS_PRIMARY: u16 = 1 << 0;
pub const INDEX_CONSTR_CREATE_DEFERRABLE: u16 = 1 << 1;
pub const INDEX_CONSTR_CREATE_INIT_DEFERRED: u16 = 1 << 2;
pub const INDEX_CONSTR_CREATE_UPDATE_INDEX: u16 = 1 << 3;
pub const INDEX_CONSTR_CREATE_REMOVE_OLD_DEPS: u16 = 1 << 4;
pub const INDEX_CONSTR_CREATE_WITHOUT_OVERLAPS: u16 = 1 << 5;

/// `IndexStateFlagsAction` (catalog/index.h) — action for index_set_state_flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(C)]
pub enum IndexStateFlagsAction {
    INDEX_CREATE_SET_READY = 0,
    INDEX_CREATE_SET_VALID,
    INDEX_DROP_CLEAR_VALID,
    INDEX_DROP_SET_DEAD,
}
pub use IndexStateFlagsAction::*;

// ---------------------------------------------------------------------------
// CREATE SEQUENCE command parse nodes (sequence.c)
// ---------------------------------------------------------------------------

/// `typedef struct CreateSeqStmt` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateSeqStmt {
    pub type_: NodeTag,
    pub sequence: *mut RangeVar,
    pub options: *mut List,
    pub ownerId: Oid,
    pub for_identity: bool,
    pub if_not_exists: bool,
}

/// `typedef struct AlterSeqStmt` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterSeqStmt {
    pub type_: NodeTag,
    pub sequence: *mut RangeVar,
    pub options: *mut List,
    pub for_identity: bool,
    pub missing_ok: bool,
}

// ---------------------------------------------------------------------------
// CREATE OPERATOR CLASS / FAMILY command parse nodes (opclasscmds.c)
// ---------------------------------------------------------------------------

/// `typedef struct CreateOpClassStmt` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateOpClassStmt {
    pub type_: NodeTag,
    pub opclassname: *mut List,
    pub opfamilyname: *mut List,
    pub amname: *mut c_char,
    pub datatype: *mut TypeName,
    pub items: *mut List,
    pub isDefault: bool,
}

/// `typedef struct CreateOpClassItem` (parsenodes.h).  `name` is an
/// `ObjectWithArgs *`, carried opaquely as `*mut Node`.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateOpClassItem {
    pub type_: NodeTag,
    pub itemtype: c_int,
    pub name: *mut Node,
    pub number: c_int,
    pub order_family: *mut List,
    pub class_args: *mut List,
    pub storedtype: *mut TypeName,
}

/// `typedef struct CreateOpFamilyStmt` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateOpFamilyStmt {
    pub type_: NodeTag,
    pub opfamilyname: *mut List,
    pub amname: *mut c_char,
}

/// `typedef struct AlterOpFamilyStmt` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterOpFamilyStmt {
    pub type_: NodeTag,
    pub opfamilyname: *mut List,
    pub amname: *mut c_char,
    pub isDrop: bool,
    pub items: *mut List,
}

/// `typedef struct OpFamilyMember` (access/amapi.h).  The transient in-memory
/// record describing one operator or support function while CREATE/ALTER
/// OPERATOR CLASS/FAMILY builds up its `pg_amop`/`pg_amproc` entries.  Carried
/// `#[repr(C)]` because it is passed by-pointer to an index AM's
/// `amadjustmembers` callback.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct OpFamilyMember {
    /// is this an operator, or support func?
    pub is_func: bool,
    /// operator or support func's OID
    pub object: Oid,
    /// strategy or support func number
    pub number: c_int,
    /// lefttype
    pub lefttype: Oid,
    /// righttype
    pub righttype: Oid,
    /// ordering operator's sort opfamily, or 0
    pub sortfamily: Oid,
    /// hard or soft dependency?
    pub ref_is_hard: bool,
    /// is dependency on opclass or opfamily?
    pub ref_is_family: bool,
    /// OID of opclass or opfamily
    pub refobjid: Oid,
}

/// `#define AMOP_SEARCH 's'` (catalog/pg_amop.h) — operator is for search.
pub const AMOP_SEARCH: c_char = b's' as c_char;
/// `#define AMOP_ORDER 'o'` (catalog/pg_amop.h) — operator is for ordering.
pub const AMOP_ORDER: c_char = b'o' as c_char;

// ---------------------------------------------------------------------------
// CREATE TRIGGER command parse node (trigger.c)
// ---------------------------------------------------------------------------

/// `typedef struct CreateTrigStmt` (parsenodes.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateTrigStmt {
    pub type_: NodeTag,
    pub replace: bool,
    pub isconstraint: bool,
    pub trigname: *mut c_char,
    pub relation: *mut RangeVar,
    pub funcname: *mut List,
    pub args: *mut List,
    pub row: bool,
    pub timing: i16,
    pub events: i16,
    pub columns: *mut List,
    pub whenClause: *mut Node,
    pub transitionRels: *mut List,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub constrrel: *mut RangeVar,
}

// ---------------------------------------------------------------------------
// Catalog / relcache structs touched by the command crates
// ---------------------------------------------------------------------------

/// `FormData_pg_sequence` (catalog/pg_sequence.h) — the on-disk pg_sequence row.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct FormData_pg_sequence {
    pub seqrelid: Oid,
    pub seqtypid: Oid,
    pub seqstart: int64,
    pub seqincrement: int64,
    pub seqmax: int64,
    pub seqmin: int64,
    pub seqcache: int64,
    pub seqcycle: bool,
}

pub type Form_pg_sequence = *mut FormData_pg_sequence;

/// `FormData_pg_sequence_data` (catalog/pg_sequence.h) — the sequence relation's
/// single data tuple (current value / log count / called flag).
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct FormData_pg_sequence_data {
    pub last_value: int64,
    pub log_cnt: int64,
    pub is_called: bool,
}

pub type Form_pg_sequence_data = *mut FormData_pg_sequence_data;

/// `typedef struct TriggerDesc` (utils/reltrigger.h) — per-relation trigger
/// descriptor cached in the relcache.  `Trigger` already lives in `funccache`.
#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct TriggerDesc {
    pub triggers: *mut Trigger,
    pub numtriggers: c_int,
    pub trig_insert_before_row: bool,
    pub trig_insert_after_row: bool,
    pub trig_insert_instead_row: bool,
    pub trig_insert_before_statement: bool,
    pub trig_insert_after_statement: bool,
    pub trig_update_before_row: bool,
    pub trig_update_after_row: bool,
    pub trig_update_instead_row: bool,
    pub trig_update_before_statement: bool,
    pub trig_update_after_statement: bool,
    pub trig_delete_before_row: bool,
    pub trig_delete_after_row: bool,
    pub trig_delete_instead_row: bool,
    pub trig_delete_before_statement: bool,
    pub trig_delete_after_statement: bool,
    pub trig_truncate_before_statement: bool,
    pub trig_truncate_after_statement: bool,
    pub trig_insert_new_table: bool,
    pub trig_update_old_table: bool,
    pub trig_update_new_table: bool,
    pub trig_delete_old_table: bool,
}

// ---------------------------------------------------------------------------
// Subscription DDL parse nodes (nodes/parsenodes.h) — CREATE / ALTER / DROP
// SUBSCRIPTION, consumed by `backend/commands/subscriptioncmds.c`.
// ---------------------------------------------------------------------------

/// `typedef struct CreateSubscriptionStmt` (parsenodes.h) — CREATE SUBSCRIPTION.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct CreateSubscriptionStmt {
    pub type_: NodeTag,
    /// Name of the subscription
    pub subname: *mut c_char,
    /// Connection string to publisher
    pub conninfo: *mut c_char,
    /// One or more publication to subscribe to (list of String)
    pub publication: *mut List,
    /// List of DefElem nodes
    pub options: *mut List,
}

/// `typedef enum AlterSubscriptionType` (parsenodes.h) — the ALTER SUBSCRIPTION
/// subform discriminant carried in [`AlterSubscriptionStmt::kind`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(C)]
pub enum AlterSubscriptionType {
    ALTER_SUBSCRIPTION_OPTIONS,
    ALTER_SUBSCRIPTION_CONNECTION,
    ALTER_SUBSCRIPTION_SET_PUBLICATION,
    ALTER_SUBSCRIPTION_ADD_PUBLICATION,
    ALTER_SUBSCRIPTION_DROP_PUBLICATION,
    ALTER_SUBSCRIPTION_REFRESH,
    ALTER_SUBSCRIPTION_ENABLED,
    ALTER_SUBSCRIPTION_SKIP,
}
pub use AlterSubscriptionType::*;

/// `typedef struct AlterSubscriptionStmt` (parsenodes.h) — ALTER SUBSCRIPTION.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct AlterSubscriptionStmt {
    pub type_: NodeTag,
    /// ALTER_SUBSCRIPTION_OPTIONS, etc
    pub kind: AlterSubscriptionType,
    /// Name of the subscription
    pub subname: *mut c_char,
    /// Connection string to publisher
    pub conninfo: *mut c_char,
    /// One or more publication to subscribe to (list of String)
    pub publication: *mut List,
    /// List of DefElem nodes
    pub options: *mut List,
}

/// `typedef struct DropSubscriptionStmt` (parsenodes.h) — DROP SUBSCRIPTION.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct DropSubscriptionStmt {
    pub type_: NodeTag,
    /// Name of the subscription
    pub subname: *mut c_char,
    /// Skip error if missing?
    pub missing_ok: bool,
    /// RESTRICT or CASCADE behavior
    pub behavior: DropBehavior,
}

// ---------------------------------------------------------------------------
// pg_subscription catalog attribute numbers (catalog/pg_subscription_d.h).
// The relation/index OIDs already live in `catalog.rs`
// (SUBSCRIPTION_RELATION_ID / SUBSCRIPTION_OBJECT_INDEX_ID /
// SUBSCRIPTION_NAME_INDEX_ID).
// ---------------------------------------------------------------------------

pub const Anum_pg_subscription_oid: i32 = 1;
pub const Anum_pg_subscription_subdbid: i32 = 2;
pub const Anum_pg_subscription_subskiplsn: i32 = 3;
pub const Anum_pg_subscription_subname: i32 = 4;
pub const Anum_pg_subscription_subowner: i32 = 5;
pub const Anum_pg_subscription_subenabled: i32 = 6;
pub const Anum_pg_subscription_subbinary: i32 = 7;
pub const Anum_pg_subscription_substream: i32 = 8;
pub const Anum_pg_subscription_subtwophasestate: i32 = 9;
pub const Anum_pg_subscription_subdisableonerr: i32 = 10;
pub const Anum_pg_subscription_subpasswordrequired: i32 = 11;
pub const Anum_pg_subscription_subrunasowner: i32 = 12;
pub const Anum_pg_subscription_subfailover: i32 = 13;
pub const Anum_pg_subscription_subconninfo: i32 = 14;
pub const Anum_pg_subscription_subslotname: i32 = 15;
pub const Anum_pg_subscription_subsynccommit: i32 = 16;
pub const Anum_pg_subscription_subpublications: i32 = 17;
pub const Anum_pg_subscription_suborigin: i32 = 18;
pub const Natts_pg_subscription: i32 = 18;

#[cfg(test)]
mod subscription_tests {
    use super::*;
    use core::mem::offset_of;

    #[test]
    fn create_subscription_stmt_layout() {
        assert_eq!(offset_of!(CreateSubscriptionStmt, type_), 0);
        assert_eq!(offset_of!(CreateSubscriptionStmt, subname), 8);
        assert_eq!(offset_of!(CreateSubscriptionStmt, conninfo), 16);
        assert_eq!(offset_of!(CreateSubscriptionStmt, publication), 24);
        assert_eq!(offset_of!(CreateSubscriptionStmt, options), 32);
    }

    #[test]
    fn alter_subscription_stmt_layout() {
        assert_eq!(offset_of!(AlterSubscriptionStmt, type_), 0);
        assert_eq!(offset_of!(AlterSubscriptionStmt, kind), 4);
        assert_eq!(offset_of!(AlterSubscriptionStmt, subname), 8);
        assert_eq!(offset_of!(AlterSubscriptionStmt, conninfo), 16);
        assert_eq!(offset_of!(AlterSubscriptionStmt, publication), 24);
        assert_eq!(offset_of!(AlterSubscriptionStmt, options), 32);
    }

    #[test]
    fn drop_subscription_stmt_layout() {
        assert_eq!(offset_of!(DropSubscriptionStmt, type_), 0);
        assert_eq!(offset_of!(DropSubscriptionStmt, subname), 8);
        assert_eq!(offset_of!(DropSubscriptionStmt, missing_ok), 16);
    }

    #[test]
    fn alter_subscription_type_discriminants() {
        assert_eq!(ALTER_SUBSCRIPTION_OPTIONS as i32, 0);
        assert_eq!(ALTER_SUBSCRIPTION_CONNECTION as i32, 1);
        assert_eq!(ALTER_SUBSCRIPTION_SET_PUBLICATION as i32, 2);
        assert_eq!(ALTER_SUBSCRIPTION_ADD_PUBLICATION as i32, 3);
        assert_eq!(ALTER_SUBSCRIPTION_DROP_PUBLICATION as i32, 4);
        assert_eq!(ALTER_SUBSCRIPTION_REFRESH as i32, 5);
        assert_eq!(ALTER_SUBSCRIPTION_ENABLED as i32, 6);
        assert_eq!(ALTER_SUBSCRIPTION_SKIP as i32, 7);
    }
}
