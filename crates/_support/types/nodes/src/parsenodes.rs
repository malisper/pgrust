//! Parse-tree vocabulary (nodes/parsenodes.h), trimmed.

use alloc::string::String;
use alloc::vec::Vec;

use mcx::{Mcx, PgBox, PgVec};
use ::types_acl::AclMode;
use ::types_core::primitive::{Index, Oid};
use ::types_error::PgResult;
use ::types_storage::lock::LOCKMODE;

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

/// `RangeTblEntry` (nodes/parsenodes.h) — the full producer range-table entry.
///
/// K1-parsetree expansion: this was a 5-field consumer-trimmed scalar carrier
/// (`rtekind`/`relid`/`relkind`/`rellockmode`/`perminfoindex`, all kept). It now
/// carries the complete C `RangeTblEntry`, so it gains `<'mcx>` for the `Node`/
/// `List`/`char *` subtrees (`subquery`/`joinaliasvars`/`functions`/…). The
/// trimmed scalars keep their names/types; consumers reading only those are
/// unaffected beyond naming the lifetime. Use [`RangeTblEntry::new_in`] in place
/// of the former `Default` (a `PgVec` field can't derive `Default`).
#[derive(Debug)]
pub struct RangeTblEntry<'mcx> {
    /// `Alias *alias` — user-written alias clause, if any.
    pub alias: Option<PgBox<'mcx, crate::rawnodes::Alias<'mcx>>>,
    /// `Alias *eref` — expanded reference names.
    pub eref: Option<PgBox<'mcx, crate::rawnodes::Alias<'mcx>>>,
    /// `RTEKind rtekind`.
    pub rtekind: RTEKind,
    // --- Fields valid for a plain relation RTE (else zero): ---
    /// `Oid relid` — OID of the relation (RTE_RELATION).
    pub relid: Oid,
    /// `bool inh` — inheritance requested?
    pub inh: bool,
    /// `char relkind` — relation kind.
    pub relkind: i8,
    /// `int rellockmode` — lock level that the query requires.
    pub rellockmode: LOCKMODE,
    /// `Index perminfoindex` — 1-based index of this RTE's
    /// `RTEPermissionInfo` in the query's `rteperminfos` list, or 0.
    pub perminfoindex: Index,
    /// `TableSampleClause *tablesample` — sampling info, or `None`.
    pub tablesample: Option<crate::nodes::NodePtr<'mcx>>,
    // --- Fields valid for a subquery RTE (else NULL): ---
    /// `Query *subquery` — the sub-query.
    pub subquery: Option<PgBox<'mcx, crate::copy_query::Query<'mcx>>>,
    /// `bool security_barrier` — is from a security_barrier view?
    pub security_barrier: bool,
    // --- Fields valid for a join RTE (else NULL/zero): ---
    /// `JoinType jointype`.
    pub jointype: crate::jointype::JoinType,
    /// `int joinmergedcols` — number of merged (JOIN USING) columns.
    pub joinmergedcols: i32,
    /// `List *joinaliasvars` — list of alias-var expansions.
    pub joinaliasvars: PgVec<'mcx, crate::nodes::NodePtr<'mcx>>,
    /// `List *joinleftcols` — left-side input column numbers.
    pub joinleftcols: PgVec<'mcx, i32>,
    /// `List *joinrightcols` — right-side input column numbers.
    pub joinrightcols: PgVec<'mcx, i32>,
    /// `Alias *join_using_alias` — alias attached directly to JOIN/USING.
    pub join_using_alias: Option<PgBox<'mcx, crate::rawnodes::Alias<'mcx>>>,
    // --- Fields valid for a function RTE (else NIL/zero): ---
    /// `List *functions` — list of `RangeTblFunction` nodes.
    pub functions: PgVec<'mcx, crate::nodes::NodePtr<'mcx>>,
    /// `bool funcordinality` — is this called WITH ORDINALITY?
    pub funcordinality: bool,
    // --- Fields valid for a TableFunc RTE (else NULL): ---
    /// `TableFunc *tablefunc`.
    pub tablefunc: Option<crate::nodes::NodePtr<'mcx>>,
    // --- Fields valid for a values RTE (else NIL): ---
    /// `List *values_lists` — list of expression lists.
    pub values_lists: PgVec<'mcx, crate::nodes::NodePtr<'mcx>>,
    // --- Fields valid for a CTE RTE (else NULL/zero): ---
    /// `char *ctename` — name of the WITH list item.
    pub ctename: Option<::mcx::PgString<'mcx>>,
    /// `Index ctelevelsup` — number of query levels up.
    pub ctelevelsup: Index,
    /// `bool self_reference` — is this a recursive self-reference?
    pub self_reference: bool,
    // --- Fields valid for CTE, VALUES, ENR, and TableFunc RTEs (else NIL): ---
    /// `List *coltypes` — OID list of column type OIDs.
    pub coltypes: PgVec<'mcx, Oid>,
    /// `List *coltypmods` — integer list of column typmods.
    pub coltypmods: PgVec<'mcx, i32>,
    /// `List *colcollations` — OID list of column collation OIDs.
    pub colcollations: PgVec<'mcx, Oid>,
    // --- Fields valid for ENR RTEs (else NULL/zero): ---
    /// `char *enrname` — name of ephemeral named relation.
    pub enrname: Option<::mcx::PgString<'mcx>>,
    /// `Cardinality enrtuples` — estimated or actual from caller.
    pub enrtuples: f64,
    // --- Fields valid for a GROUP RTE (else NIL): ---
    /// `List *groupexprs` — list of grouping expressions.
    pub groupexprs: PgVec<'mcx, crate::nodes::NodePtr<'mcx>>,
    // --- Fields valid in all RTEs: ---
    /// `bool lateral` — was LATERAL specified?
    pub lateral: bool,
    /// `bool inFromCl` — present in FROM clause?
    pub inFromCl: bool,
    /// `List *securityQuals` — security barrier quals to apply, if any.
    pub securityQuals: PgVec<'mcx, crate::nodes::NodePtr<'mcx>>,
}

impl<'mcx> RangeTblEntry<'mcx> {
    /// A zero-initialized `makeNode(RangeTblEntry)` in `mcx` (replaces the
    /// former `Default`, which a `PgVec` field cannot derive). All list fields
    /// start empty, all `Node *`/`char *` `None`, scalars at their enum/zero
    /// default.
    pub fn new_in(mcx: Mcx<'mcx>) -> RangeTblEntry<'mcx> {
        RangeTblEntry {
            alias: None,
            eref: None,
            rtekind: RTEKind::default(),
            relid: Oid::default(),
            inh: false,
            relkind: 0,
            rellockmode: LOCKMODE::default(),
            perminfoindex: Index::default(),
            tablesample: None,
            subquery: None,
            security_barrier: false,
            jointype: crate::jointype::JoinType::default(),
            joinmergedcols: 0,
            joinaliasvars: PgVec::new_in(mcx),
            joinleftcols: PgVec::new_in(mcx),
            joinrightcols: PgVec::new_in(mcx),
            join_using_alias: None,
            functions: PgVec::new_in(mcx),
            funcordinality: false,
            tablefunc: None,
            values_lists: PgVec::new_in(mcx),
            ctename: None,
            ctelevelsup: Index::default(),
            self_reference: false,
            coltypes: PgVec::new_in(mcx),
            coltypmods: PgVec::new_in(mcx),
            colcollations: PgVec::new_in(mcx),
            enrname: None,
            enrtuples: 0.0,
            groupexprs: PgVec::new_in(mcx),
            lateral: false,
            inFromCl: false,
            securityQuals: PgVec::new_in(mcx),
        }
    }

    /// Deep copy into `mcx` (C: `copyObject` over `RangeTblEntry`). Every
    /// `Node`/`List`/`char *` subtree is re-homed onto the target context.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RangeTblEntry<'b>> {
        Ok(RangeTblEntry {
            alias: clone_opt_alias(&self.alias, mcx)?,
            eref: clone_opt_alias(&self.eref, mcx)?,
            rtekind: self.rtekind,
            relid: self.relid,
            inh: self.inh,
            relkind: self.relkind,
            rellockmode: self.rellockmode,
            perminfoindex: self.perminfoindex,
            tablesample: clone_opt_node(&self.tablesample, mcx)?,
            subquery: match &self.subquery {
                Some(q) => Some(::mcx::alloc_in(mcx, q.clone_in(mcx)?)?),
                None => None,
            },
            security_barrier: self.security_barrier,
            jointype: self.jointype,
            joinmergedcols: self.joinmergedcols,
            joinaliasvars: clone_node_vec(&self.joinaliasvars, mcx)?,
            joinleftcols: clone_scalar_vec(&self.joinleftcols, mcx)?,
            joinrightcols: clone_scalar_vec(&self.joinrightcols, mcx)?,
            join_using_alias: clone_opt_alias(&self.join_using_alias, mcx)?,
            functions: clone_node_vec(&self.functions, mcx)?,
            funcordinality: self.funcordinality,
            tablefunc: clone_opt_node(&self.tablefunc, mcx)?,
            values_lists: clone_node_vec(&self.values_lists, mcx)?,
            ctename: match &self.ctename {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            ctelevelsup: self.ctelevelsup,
            self_reference: self.self_reference,
            coltypes: clone_scalar_vec(&self.coltypes, mcx)?,
            coltypmods: clone_scalar_vec(&self.coltypmods, mcx)?,
            colcollations: clone_scalar_vec(&self.colcollations, mcx)?,
            enrname: match &self.enrname {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
            enrtuples: self.enrtuples,
            groupexprs: clone_node_vec(&self.groupexprs, mcx)?,
            lateral: self.lateral,
            inFromCl: self.inFromCl,
            securityQuals: clone_node_vec(&self.securityQuals, mcx)?,
        })
    }
}

fn clone_opt_alias<'b>(
    a: &Option<PgBox<'_, crate::rawnodes::Alias<'_>>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<PgBox<'b, crate::rawnodes::Alias<'b>>>> {
    match a {
        Some(a) => Ok(Some(::mcx::alloc_in(mcx, a.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

fn clone_opt_node<'b>(
    n: &Option<crate::nodes::NodePtr<'_>>,
    mcx: Mcx<'b>,
) -> PgResult<Option<crate::nodes::NodePtr<'b>>> {
    match n {
        Some(n) => Ok(Some(::mcx::alloc_in(mcx, n.clone_in(mcx)?)?)),
        None => Ok(None),
    }
}

fn clone_node_vec<'b>(
    v: &PgVec<'_, crate::nodes::NodePtr<'_>>,
    mcx: Mcx<'b>,
) -> PgResult<PgVec<'b, crate::nodes::NodePtr<'b>>> {
    let mut out = ::mcx::vec_with_capacity_in(mcx, v.len())?;
    for n in v.iter() {
        out.push(::mcx::alloc_in(mcx, n.clone_in(mcx)?)?);
    }
    Ok(out)
}

fn clone_scalar_vec<'b, T: Copy>(
    v: &PgVec<'_, T>,
    mcx: Mcx<'b>,
) -> PgResult<PgVec<'b, T>> {
    let mut out = ::mcx::vec_with_capacity_in(mcx, v.len())?;
    for x in v.iter() {
        out.push(*x);
    }
    Ok(out)
}

/// `RTEPermissionInfo` (nodes/parsenodes.h).
#[derive(Debug, Default)]
pub struct RTEPermissionInfo<'mcx> {
    /// `Oid relid` — relation the permissions apply to.
    pub relid: Oid,
    /// `bool inh` — separately check inheritance children?
    pub inh: bool,
    /// `AclMode requiredPerms` — bitmask of required access permissions.
    pub requiredPerms: AclMode,
    /// `Oid checkAsUser` — if valid, check access as this role.
    pub checkAsUser: Oid,
    /// `Bitmapset *selectedCols` — columns needing SELECT permission.
    pub selectedCols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `Bitmapset *insertedCols` — columns needing INSERT permission.
    pub insertedCols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
    /// `Bitmapset *updatedCols` — columns needing UPDATE permission.
    pub updatedCols: Option<PgBox<'mcx, Bitmapset<'mcx>>>,
}

impl RTEPermissionInfo<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `RTEPermissionInfo`). The
    /// `Bitmapset *` columns are copied through `Bitmapset::clone_in`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RTEPermissionInfo<'b>> {
        Ok(RTEPermissionInfo {
            relid: self.relid,
            inh: self.inh,
            requiredPerms: self.requiredPerms,
            checkAsUser: self.checkAsUser,
            selectedCols: match &self.selectedCols {
                Some(b) => Some(::mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
            insertedCols: match &self.insertedCols {
                Some(b) => Some(::mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
            updatedCols: match &self.updatedCols {
                Some(b) => Some(::mcx::alloc_in(mcx, b.clone_in(mcx)?)?),
                None => None,
            },
        })
    }
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

impl ObjectType {
    /// `(ObjectType) itype` — the C cast `pg_get_object_address` (objectaddress.c)
    /// applies to a non-negative `read_objtype_from_string` result. The
    /// `ObjectType` discriminants are contiguous `0 ..= 51`; an out-of-range
    /// value (the C `-1` "unmapped" sentinel) yields `None`.
    pub fn from_i32(value: i32) -> Option<ObjectType> {
        const LAST: i32 = ObjectType::View as i32;
        if (0..=LAST).contains(&value) {
            // SAFETY: `ObjectType` is `#[repr(u32)]` with contiguous
            // discriminants `0 ..= LAST`; `value` is in that range.
            Some(unsafe { core::mem::transmute::<u32, ObjectType>(value as u32) })
        } else {
            None
        }
    }
}

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
    pub rolename: Option<::mcx::PgString<'mcx>>,
}

impl RoleSpec<'_> {
    /// Deep copy into `mcx` (C: `copyObject` over `RoleSpec`). `rolename` is a
    /// `char *` copied via `PgString::clone_in`.
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RoleSpec<'b>> {
        Ok(RoleSpec {
            roletype: self.roletype,
            rolename: match &self.rolename {
                Some(s) => Some(s.clone_in(mcx)?),
                None => None,
            },
        })
    }
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

impl CreateConversionStmt {
    /// Deep copy (C: `copyObject` over `CreateConversionStmt`). The struct owns
    /// `String`/`Vec<String>` (backend-lifetime parser output, not `'mcx`), so
    /// the copy is a plain `clone`; the method gives it a uniform fallible
    /// `clone_in` like its peers.
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<CreateConversionStmt> {
        Ok(self.clone())
    }
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

impl CreateAmStmt {
    /// Deep copy (C: `copyObject` over `CreateAmStmt`). The struct owns
    /// `String`/`Vec<String>` (backend-lifetime parser output, not `'mcx`), so
    /// the copy is a plain `clone`; the method gives it a uniform fallible
    /// `clone_in` like its peers.
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<CreateAmStmt> {
        Ok(self.clone())
    }
}
