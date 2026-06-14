//! Parse-tree vocabulary (nodes/parsenodes.h), trimmed.

use alloc::string::String;
use alloc::vec::Vec;

use mcx::PgBox;
use types_core::primitive::{Index, Oid};
use types_storage::lock::LOCKMODE;

use crate::bitmapset::Bitmapset;

/// `RTEKind` (nodes/parsenodes.h) — values verified against PostgreSQL 18.3.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum RTEKind {
    /// ordinary relation reference
    #[default]
    RTE_RELATION = 0,
    /// subquery in FROM
    RTE_SUBQUERY = 1,
    /// join
    RTE_JOIN = 2,
    /// function in FROM
    RTE_FUNCTION = 3,
    /// TableFunc(.., column list)
    RTE_TABLEFUNC = 4,
    /// VALUES (<exprlist>), (<exprlist>), ...
    RTE_VALUES = 5,
    /// common table expr (WITH list element)
    RTE_CTE = 6,
    /// tuplestore, e.g. for AFTER triggers
    RTE_NAMEDTUPLESTORE = 7,
    /// RTE represents an empty FROM clause (added by the planner)
    RTE_RESULT = 8,
    /// the grouping step
    RTE_GROUP = 9,
}

pub use RTEKind::{
    RTE_CTE, RTE_FUNCTION, RTE_GROUP, RTE_JOIN, RTE_NAMEDTUPLESTORE, RTE_RELATION, RTE_RESULT,
    RTE_SUBQUERY, RTE_TABLEFUNC, RTE_VALUES,
};

/// `RangeTblEntry` (nodes/parsenodes.h), trimmed to the fields ports consume.
#[derive(Clone, Copy, Debug, Default)]
pub struct RangeTblEntry {
    /// `RTEKind rtekind`.
    pub rtekind: RTEKind,
    /// `Oid relid` — OID of the relation (RTE_RELATION).
    pub relid: Oid,
    /// `char relkind` — relation kind.
    pub relkind: i8,
    /// `int rellockmode` — lock level that the query requires.
    pub rellockmode: LOCKMODE,
    /// `Index perminfoindex` — 1-based index of this RTE's
    /// `RTEPermissionInfo` in the query's `rteperminfos` list, or 0.
    pub perminfoindex: Index,
}

/// `RTEPermissionInfo` (nodes/parsenodes.h), trimmed.
#[derive(Debug, Default)]
pub struct RTEPermissionInfo<'mcx> {
    /// `Oid relid` — relation the permissions apply to.
    pub relid: Oid,
    /// `Oid checkAsUser` — user to check access as, or 0 for current user.
    pub checkAsUser: Oid,
    /// `Bitmapset *insertedCols` — columns needing INSERT permission.
    pub insertedCols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `Bitmapset *updatedCols` — columns needing UPDATE permission.
    pub updatedCols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

/// `ObjectType` (`nodes/parsenodes.h`). Discriminants mirror the C enum
/// order (implicit values 0..).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum ObjectType {
    AccessMethod = 0,
    Aggregate = 1,
    Amop = 2,
    Amproc = 3,
    /// type's attribute, when distinct from column
    Attribute = 4,
    Cast = 5,
    Column = 6,
    Collation = 7,
    Conversion = 8,
    Database = 9,
    Default = 10,
    Defacl = 11,
    Domain = 12,
    Domconstraint = 13,
    EventTrigger = 14,
    Extension = 15,
    Fdw = 16,
    ForeignServer = 17,
    ForeignTable = 18,
    Function = 19,
    Index = 20,
    Language = 21,
    Largeobject = 22,
    Matview = 23,
    Opclass = 24,
    Operator = 25,
    Opfamily = 26,
    ParameterAcl = 27,
    Policy = 28,
    Procedure = 29,
    Publication = 30,
    PublicationNamespace = 31,
    PublicationRel = 32,
    Role = 33,
    Routine = 34,
    Rule = 35,
    Schema = 36,
    Sequence = 37,
    Subscription = 38,
    StatisticExt = 39,
    Tabconstraint = 40,
    Table = 41,
    Tablespace = 42,
    Transform = 43,
    Trigger = 44,
    TsConfiguration = 45,
    TsDictionary = 46,
    TsParser = 47,
    TsTemplate = 48,
    Type = 49,
    UserMapping = 50,
    View = 51,
}

pub use ObjectType::{
    AccessMethod as OBJECT_ACCESS_METHOD, Aggregate as OBJECT_AGGREGATE, Amop as OBJECT_AMOP,
    Amproc as OBJECT_AMPROC, Attribute as OBJECT_ATTRIBUTE, Cast as OBJECT_CAST,
    Collation as OBJECT_COLLATION, Column as OBJECT_COLUMN, Conversion as OBJECT_CONVERSION,
    Database as OBJECT_DATABASE, Default as OBJECT_DEFAULT, Defacl as OBJECT_DEFACL,
    Domain as OBJECT_DOMAIN, Domconstraint as OBJECT_DOMCONSTRAINT,
    EventTrigger as OBJECT_EVENT_TRIGGER, Extension as OBJECT_EXTENSION, Fdw as OBJECT_FDW,
    ForeignServer as OBJECT_FOREIGN_SERVER, ForeignTable as OBJECT_FOREIGN_TABLE,
    Function as OBJECT_FUNCTION, Index as OBJECT_INDEX, Language as OBJECT_LANGUAGE,
    Largeobject as OBJECT_LARGEOBJECT, Matview as OBJECT_MATVIEW, Opclass as OBJECT_OPCLASS,
    Operator as OBJECT_OPERATOR, Opfamily as OBJECT_OPFAMILY, ParameterAcl as OBJECT_PARAMETER_ACL,
    Policy as OBJECT_POLICY, Procedure as OBJECT_PROCEDURE, Publication as OBJECT_PUBLICATION,
    PublicationNamespace as OBJECT_PUBLICATION_NAMESPACE, PublicationRel as OBJECT_PUBLICATION_REL,
    Role as OBJECT_ROLE, Routine as OBJECT_ROUTINE, Rule as OBJECT_RULE, Schema as OBJECT_SCHEMA,
    Sequence as OBJECT_SEQUENCE, StatisticExt as OBJECT_STATISTIC_EXT,
    Subscription as OBJECT_SUBSCRIPTION, Tabconstraint as OBJECT_TABCONSTRAINT,
    Table as OBJECT_TABLE, Tablespace as OBJECT_TABLESPACE, Transform as OBJECT_TRANSFORM,
    Trigger as OBJECT_TRIGGER, TsConfiguration as OBJECT_TSCONFIGURATION,
    TsDictionary as OBJECT_TSDICTIONARY, TsParser as OBJECT_TSPARSER,
    TsTemplate as OBJECT_TSTEMPLATE, Type as OBJECT_TYPE, UserMapping as OBJECT_USER_MAPPING,
    View as OBJECT_VIEW,
};

/// `DropBehavior` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum DropBehavior {
    /// `DROP_RESTRICT` — drop fails if any dependent objects.
    Restrict = 0,
    /// `DROP_CASCADE` — remove dependent objects too.
    Cascade = 1,
}

pub use DropBehavior::{Cascade as DROP_CASCADE, Restrict as DROP_RESTRICT};

/// `RoleSpecType` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum RoleSpecType {
    /// `ROLESPEC_CSTRING` — role name stored as a C string.
    Cstring = 0,
    /// `ROLESPEC_CURRENT_ROLE`.
    CurrentRole = 1,
    /// `ROLESPEC_CURRENT_USER`.
    CurrentUser = 2,
    /// `ROLESPEC_SESSION_USER`.
    SessionUser = 3,
    /// `ROLESPEC_PUBLIC` — role name is "public".
    Public = 4,
}

pub use RoleSpecType::{
    Cstring as ROLESPEC_CSTRING, CurrentRole as ROLESPEC_CURRENT_ROLE,
    CurrentUser as ROLESPEC_CURRENT_USER, Public as ROLESPEC_PUBLIC,
    SessionUser as ROLESPEC_SESSION_USER,
};

/// `RoleSpec` (`nodes/parsenodes.h`) — a role name or one of the
/// CURRENT_ROLE/CURRENT_USER/SESSION_USER/PUBLIC specials, trimmed to the
/// fields consumers read.
#[derive(Debug)]
pub struct RoleSpec<'mcx> {
    /// `roletype`.
    pub roletype: RoleSpecType,
    /// `rolename` — filled only for `ROLESPEC_CSTRING`.
    pub rolename: Option<mcx::PgString<'mcx>>,
}

/// `T_CreateAmStmt = 180` (`nodes/nodetags.h`) — verified against PostgreSQL
/// 18.3.
pub const T_CreateAmStmt: u32 = 180;

/// `T_CreateConversionStmt = 249` (`nodes/nodetags.h`) — verified against
/// PostgreSQL 18.3.
pub const T_CreateConversionStmt: u32 = 249;

/// `CreateConversionStmt` (`nodes/parsenodes.h`) — the `CREATE CONVERSION`
/// statement. `conversion_name` / `func_name` are `List *` of `String` value
/// nodes (qualified name components); the encoding names are `char *`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CreateConversionStmt {
    /// `conversion_name` — name of the conversion (list of String components).
    pub conversion_name: Vec<String>,
    /// `for_encoding_name` — source encoding name.
    pub for_encoding_name: Option<String>,
    /// `to_encoding_name` — destination encoding name.
    pub to_encoding_name: Option<String>,
    /// `func_name` — qualified conversion function name (list of String
    /// components).
    pub func_name: Vec<String>,
    /// `def` — is this a default conversion?
    pub def: bool,
}

/// `CreateAmStmt` (`nodes/parsenodes.h`) — the `CREATE ACCESS METHOD`
/// statement. `handler_name` is a `List *` of `String` value nodes (the
/// qualified handler-function name components); `amtype` is a single-character
/// `AMTYPE_*` discriminant.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CreateAmStmt {
    /// `amname` — access method name.
    pub amname: Option<String>,
    /// `handler_name` — handler function name (list of String components).
    pub handler_name: Vec<String>,
    /// `amtype` — type of access method (`AMTYPE_INDEX` / `AMTYPE_TABLE`).
    pub amtype: u8,
}
