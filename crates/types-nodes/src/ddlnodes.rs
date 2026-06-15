//! Raw-grammar DDL "CREATE" family parse nodes (`nodes/parsenodes.h`): the
//! node vocabulary the bison grammar's CREATE-family rule actions build
//! (`CreateStmt`, `IndexStmt`, `CreateSeqStmt`, `CreateFunctionStmt`, the
//! type/role/schema/extension creators, and the column/constraint/option/
//! partition helper nodes they reference).
//!
//! These are authored field-for-field against `parsenodes.h` (cross-checked
//! against the c2rust `backend-nodes-types` defs the boundary converter reads
//! from). Following the same modelling rules as [`crate::rawnodes`]: `Node *`
//! subtrees are `Option<NodePtr>` (or required `NodePtr`); `List *` of nodes
//! are `PgVec<NodePtr>`; `char *` are `Option<PgString>`; `char` code fields
//! are `i8`; small C enums map to the repo's `#[repr]` enums by their shared C
//! discriminant.
//!
//! The few consumer crates that already carry a trimmed view of one of these
//! nodes (`parsenodes::RoleSpec`, `parsestmt::IntoClause`) keep those views;
//! this module supplies the full *producer* shape the parser emits, exactly as
//! the producer/consumer split in [`crate::rawnodes`].

#![allow(non_snake_case)]

use mcx::{Mcx, PgString, PgVec};
use types_core::primitive::{Oid, ParseLoc, RelFileNumber};
use types_error::PgResult;

use crate::nodes::NodePtr;
use crate::parsenodes::{ObjectType, RoleSpecType};
use crate::primnodes::OnCommitAction;
use crate::partition::{PartitionRangeDatumKind, PartitionStrategy};
use crate::rawnodes::{copy_node_vec, copy_opt_node, copy_opt_str, SortByDir, SortByNulls};

/// `SubTransactionId` (`c.h`) — a 32-bit subtransaction id.
pub type SubTransactionId = u32;

// ===========================================================================
// Small grammar enums (nodes/parsenodes.h)
// ===========================================================================

/// `DefElemAction` (`nodes/parsenodes.h`) — SET / ADD / DROP for a `DefElem`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum DefElemAction {
    /// `DEFELEM_UNSPEC` — no action given.
    #[default]
    DEFELEM_UNSPEC = 0,
    DEFELEM_SET = 1,
    DEFELEM_ADD = 2,
    DEFELEM_DROP = 3,
}
pub use DefElemAction::{DEFELEM_ADD, DEFELEM_DROP, DEFELEM_SET, DEFELEM_UNSPEC};

/// `ConstrType` (`nodes/parsenodes.h`) — kind of a table/column constraint.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ConstrType {
    #[default]
    CONSTR_NULL = 0,
    CONSTR_NOTNULL = 1,
    CONSTR_DEFAULT = 2,
    CONSTR_IDENTITY = 3,
    CONSTR_GENERATED = 4,
    CONSTR_CHECK = 5,
    CONSTR_PRIMARY = 6,
    CONSTR_UNIQUE = 7,
    CONSTR_EXCLUSION = 8,
    CONSTR_FOREIGN = 9,
    CONSTR_ATTR_DEFERRABLE = 10,
    CONSTR_ATTR_NOT_DEFERRABLE = 11,
    CONSTR_ATTR_DEFERRED = 12,
    CONSTR_ATTR_IMMEDIATE = 13,
    CONSTR_ATTR_ENFORCED = 14,
    CONSTR_ATTR_NOT_ENFORCED = 15,
}
pub use ConstrType::{
    CONSTR_ATTR_DEFERRABLE, CONSTR_ATTR_DEFERRED, CONSTR_ATTR_ENFORCED, CONSTR_ATTR_IMMEDIATE,
    CONSTR_ATTR_NOT_DEFERRABLE, CONSTR_ATTR_NOT_ENFORCED, CONSTR_CHECK, CONSTR_DEFAULT,
    CONSTR_EXCLUSION, CONSTR_FOREIGN, CONSTR_GENERATED, CONSTR_IDENTITY, CONSTR_NOTNULL,
    CONSTR_NULL, CONSTR_PRIMARY, CONSTR_UNIQUE,
};

/// `FunctionParameterMode` (`nodes/parsenodes.h`) — the discriminants are the C
/// `char` codes stored in `pg_proc`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(i8)]
pub enum FunctionParameterMode {
    /// input only
    #[default]
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
pub use FunctionParameterMode::{
    FUNC_PARAM_DEFAULT, FUNC_PARAM_IN, FUNC_PARAM_INOUT, FUNC_PARAM_OUT, FUNC_PARAM_TABLE,
    FUNC_PARAM_VARIADIC,
};

/// `RoleStmtType` (`nodes/parsenodes.h`) — CREATE ROLE / USER / GROUP.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum RoleStmtType {
    #[default]
    ROLESTMT_ROLE = 0,
    ROLESTMT_USER = 1,
    ROLESTMT_GROUP = 2,
}
pub use RoleStmtType::{ROLESTMT_GROUP, ROLESTMT_ROLE, ROLESTMT_USER};

/// `CoercionContext` (`nodes/primnodes.h`) — CREATE CAST coercion strength.
/// Ordering matters (`find_coercion_pathway` compares `>=`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u32)]
pub enum CoercionContext {
    #[default]
    COERCION_IMPLICIT = 0,
    COERCION_ASSIGNMENT = 1,
    COERCION_PLPGSQL = 2,
    COERCION_EXPLICIT = 3,
}
pub use CoercionContext::{
    COERCION_ASSIGNMENT, COERCION_EXPLICIT, COERCION_IMPLICIT, COERCION_PLPGSQL,
};

/// `ViewCheckOption` (`nodes/parsenodes.h`) — CREATE VIEW WITH CHECK OPTION.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ViewCheckOption {
    #[default]
    NO_CHECK_OPTION = 0,
    LOCAL_CHECK_OPTION = 1,
    CASCADED_CHECK_OPTION = 2,
}
pub use ViewCheckOption::{CASCADED_CHECK_OPTION, LOCAL_CHECK_OPTION, NO_CHECK_OPTION};

// ===========================================================================
// Supporting / helper nodes
// ===========================================================================

/// `RoleSpec` (`nodes/parsenodes.h`) — a role name or one of the
/// CURRENT_ROLE/CURRENT_USER/SESSION_USER/PUBLIC specials (full raw form).
#[derive(Debug)]
pub struct RoleSpec<'mcx> {
    /// `RoleSpecType roletype`.
    pub roletype: RoleSpecType,
    /// `char *rolename` — filled only for `ROLESPEC_CSTRING`.
    pub rolename: Option<PgString<'mcx>>,
    /// `ParseLoc location`.
    pub location: ParseLoc,
}

impl RoleSpec<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RoleSpec<'b>> {
        Ok(RoleSpec {
            roletype: self.roletype,
            rolename: copy_opt_str(&self.rolename, mcx)?,
            location: self.location,
        })
    }
}

/// `DefElem` (`nodes/parsenodes.h`) — a generic `name = value` option.
#[derive(Debug)]
pub struct DefElem<'mcx> {
    /// `char *defnamespace` — NULL if unqualified.
    pub defnamespace: Option<PgString<'mcx>>,
    /// `char *defname`.
    pub defname: Option<PgString<'mcx>>,
    /// `Node *arg` — typically Integer, Float, String or TypeName.
    pub arg: Option<NodePtr<'mcx>>,
    /// `DefElemAction defaction` — unspecified action, or SET/ADD/DROP.
    pub defaction: DefElemAction,
    /// `ParseLoc location`.
    pub location: ParseLoc,
}

impl DefElem<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DefElem<'b>> {
        Ok(DefElem {
            defnamespace: copy_opt_str(&self.defnamespace, mcx)?,
            defname: copy_opt_str(&self.defname, mcx)?,
            arg: copy_opt_node(&self.arg, mcx)?,
            defaction: self.defaction,
            location: self.location,
        })
    }
}

/// `Constraint` (`nodes/parsenodes.h`) — a table or column constraint.
#[derive(Debug)]
pub struct Constraint<'mcx> {
    pub contype: ConstrType,
    pub conname: Option<PgString<'mcx>>,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub is_enforced: bool,
    pub skip_validation: bool,
    pub initially_valid: bool,
    pub is_no_inherit: bool,
    pub raw_expr: Option<NodePtr<'mcx>>,
    pub cooked_expr: Option<PgString<'mcx>>,
    pub generated_when: i8,
    pub generated_kind: i8,
    pub nulls_not_distinct: bool,
    pub keys: PgVec<'mcx, NodePtr<'mcx>>,
    pub without_overlaps: bool,
    pub including: PgVec<'mcx, NodePtr<'mcx>>,
    pub exclusions: PgVec<'mcx, NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub indexname: Option<PgString<'mcx>>,
    pub indexspace: Option<PgString<'mcx>>,
    pub reset_default_tblspc: bool,
    pub access_method: Option<PgString<'mcx>>,
    pub where_clause: Option<NodePtr<'mcx>>,
    pub pktable: Option<NodePtr<'mcx>>,
    pub fk_attrs: PgVec<'mcx, NodePtr<'mcx>>,
    pub pk_attrs: PgVec<'mcx, NodePtr<'mcx>>,
    pub fk_with_period: bool,
    pub pk_with_period: bool,
    pub fk_matchtype: i8,
    pub fk_upd_action: i8,
    pub fk_del_action: i8,
    pub fk_del_set_cols: PgVec<'mcx, NodePtr<'mcx>>,
    pub old_conpfeqop: PgVec<'mcx, NodePtr<'mcx>>,
    pub old_pktable_oid: Oid,
    pub location: ParseLoc,
}

impl Constraint<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<Constraint<'b>> {
        Ok(Constraint {
            contype: self.contype,
            conname: copy_opt_str(&self.conname, mcx)?,
            deferrable: self.deferrable,
            initdeferred: self.initdeferred,
            is_enforced: self.is_enforced,
            skip_validation: self.skip_validation,
            initially_valid: self.initially_valid,
            is_no_inherit: self.is_no_inherit,
            raw_expr: copy_opt_node(&self.raw_expr, mcx)?,
            cooked_expr: copy_opt_str(&self.cooked_expr, mcx)?,
            generated_when: self.generated_when,
            generated_kind: self.generated_kind,
            nulls_not_distinct: self.nulls_not_distinct,
            keys: copy_node_vec(&self.keys, mcx)?,
            without_overlaps: self.without_overlaps,
            including: copy_node_vec(&self.including, mcx)?,
            exclusions: copy_node_vec(&self.exclusions, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            indexname: copy_opt_str(&self.indexname, mcx)?,
            indexspace: copy_opt_str(&self.indexspace, mcx)?,
            reset_default_tblspc: self.reset_default_tblspc,
            access_method: copy_opt_str(&self.access_method, mcx)?,
            where_clause: copy_opt_node(&self.where_clause, mcx)?,
            pktable: copy_opt_node(&self.pktable, mcx)?,
            fk_attrs: copy_node_vec(&self.fk_attrs, mcx)?,
            pk_attrs: copy_node_vec(&self.pk_attrs, mcx)?,
            fk_with_period: self.fk_with_period,
            pk_with_period: self.pk_with_period,
            fk_matchtype: self.fk_matchtype,
            fk_upd_action: self.fk_upd_action,
            fk_del_action: self.fk_del_action,
            fk_del_set_cols: copy_node_vec(&self.fk_del_set_cols, mcx)?,
            old_conpfeqop: copy_node_vec(&self.old_conpfeqop, mcx)?,
            old_pktable_oid: self.old_pktable_oid,
            location: self.location,
        })
    }
}

/// `TableLikeClause` (`nodes/parsenodes.h`) — the `( ... LIKE ... )` clause.
#[derive(Debug)]
pub struct TableLikeClause<'mcx> {
    /// `RangeVar *relation`.
    pub relation: Option<NodePtr<'mcx>>,
    /// `bits32 options` — OR of TableLikeOption flags.
    pub options: u32,
    /// `Oid relationOid`.
    pub relationOid: Oid,
}

impl TableLikeClause<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TableLikeClause<'b>> {
        Ok(TableLikeClause {
            relation: copy_opt_node(&self.relation, mcx)?,
            options: self.options,
            relationOid: self.relationOid,
        })
    }
}

/// `IndexElem` (`nodes/parsenodes.h`) — one index column / expression.
#[derive(Debug)]
pub struct IndexElem<'mcx> {
    pub name: Option<PgString<'mcx>>,
    pub expr: Option<NodePtr<'mcx>>,
    pub indexcolname: Option<PgString<'mcx>>,
    pub collation: PgVec<'mcx, NodePtr<'mcx>>,
    pub opclass: PgVec<'mcx, NodePtr<'mcx>>,
    pub opclassopts: PgVec<'mcx, NodePtr<'mcx>>,
    pub ordering: SortByDir,
    pub nulls_ordering: SortByNulls,
}

impl IndexElem<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<IndexElem<'b>> {
        Ok(IndexElem {
            name: copy_opt_str(&self.name, mcx)?,
            expr: copy_opt_node(&self.expr, mcx)?,
            indexcolname: copy_opt_str(&self.indexcolname, mcx)?,
            collation: copy_node_vec(&self.collation, mcx)?,
            opclass: copy_node_vec(&self.opclass, mcx)?,
            opclassopts: copy_node_vec(&self.opclassopts, mcx)?,
            ordering: self.ordering,
            nulls_ordering: self.nulls_ordering,
        })
    }
}

/// `FunctionParameter` (`nodes/parsenodes.h`) — one CREATE FUNCTION parameter.
#[derive(Debug)]
pub struct FunctionParameter<'mcx> {
    pub name: Option<PgString<'mcx>>,
    /// `TypeName *argType`.
    pub argType: Option<NodePtr<'mcx>>,
    pub mode: FunctionParameterMode,
    pub defexpr: Option<NodePtr<'mcx>>,
    pub location: ParseLoc,
}

impl FunctionParameter<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<FunctionParameter<'b>> {
        Ok(FunctionParameter {
            name: copy_opt_str(&self.name, mcx)?,
            argType: copy_opt_node(&self.argType, mcx)?,
            mode: self.mode,
            defexpr: copy_opt_node(&self.defexpr, mcx)?,
            location: self.location,
        })
    }
}

/// `ObjectWithArgs` (`nodes/parsenodes.h`) — a function/operator name + args.
#[derive(Debug)]
pub struct ObjectWithArgs<'mcx> {
    pub objname: PgVec<'mcx, NodePtr<'mcx>>,
    pub objargs: PgVec<'mcx, NodePtr<'mcx>>,
    pub objfuncargs: PgVec<'mcx, NodePtr<'mcx>>,
    pub args_unspecified: bool,
}

impl ObjectWithArgs<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ObjectWithArgs<'b>> {
        Ok(ObjectWithArgs {
            objname: copy_node_vec(&self.objname, mcx)?,
            objargs: copy_node_vec(&self.objargs, mcx)?,
            objfuncargs: copy_node_vec(&self.objfuncargs, mcx)?,
            args_unspecified: self.args_unspecified,
        })
    }
}

/// `AccessPriv` (`nodes/parsenodes.h`) — an access privilege + optional cols.
#[derive(Debug)]
pub struct AccessPriv<'mcx> {
    pub priv_name: Option<PgString<'mcx>>,
    pub cols: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AccessPriv<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AccessPriv<'b>> {
        Ok(AccessPriv {
            priv_name: copy_opt_str(&self.priv_name, mcx)?,
            cols: copy_node_vec(&self.cols, mcx)?,
        })
    }
}

/// `CreateOpClassItem` (`nodes/parsenodes.h`) — one item of CREATE OPERATOR CLASS.
#[derive(Debug)]
pub struct CreateOpClassItem<'mcx> {
    pub itemtype: i32,
    /// `ObjectWithArgs *name`.
    pub name: Option<NodePtr<'mcx>>,
    pub number: i32,
    pub order_family: PgVec<'mcx, NodePtr<'mcx>>,
    pub class_args: PgVec<'mcx, NodePtr<'mcx>>,
    /// `TypeName *storedtype`.
    pub storedtype: Option<NodePtr<'mcx>>,
}

impl CreateOpClassItem<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateOpClassItem<'b>> {
        Ok(CreateOpClassItem {
            itemtype: self.itemtype,
            name: copy_opt_node(&self.name, mcx)?,
            number: self.number,
            order_family: copy_node_vec(&self.order_family, mcx)?,
            class_args: copy_node_vec(&self.class_args, mcx)?,
            storedtype: copy_opt_node(&self.storedtype, mcx)?,
        })
    }
}

/// `StatsElem` (`nodes/parsenodes.h`) — one element of CREATE STATISTICS.
#[derive(Debug)]
pub struct StatsElem<'mcx> {
    pub name: Option<PgString<'mcx>>,
    pub expr: Option<NodePtr<'mcx>>,
}

impl StatsElem<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<StatsElem<'b>> {
        Ok(StatsElem {
            name: copy_opt_str(&self.name, mcx)?,
            expr: copy_opt_node(&self.expr, mcx)?,
        })
    }
}

/// `PartitionElem` (`nodes/parsenodes.h`) — one partition-key column.
#[derive(Debug)]
pub struct PartitionElem<'mcx> {
    pub name: Option<PgString<'mcx>>,
    pub expr: Option<NodePtr<'mcx>>,
    pub collation: PgVec<'mcx, NodePtr<'mcx>>,
    pub opclass: PgVec<'mcx, NodePtr<'mcx>>,
    pub location: ParseLoc,
}

impl PartitionElem<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PartitionElem<'b>> {
        Ok(PartitionElem {
            name: copy_opt_str(&self.name, mcx)?,
            expr: copy_opt_node(&self.expr, mcx)?,
            collation: copy_node_vec(&self.collation, mcx)?,
            opclass: copy_node_vec(&self.opclass, mcx)?,
            location: self.location,
        })
    }
}

/// `PartitionSpec` (`nodes/parsenodes.h`) — a partition key specification.
#[derive(Debug)]
pub struct PartitionSpec<'mcx> {
    pub strategy: PartitionStrategy,
    pub partParams: PgVec<'mcx, NodePtr<'mcx>>,
    pub location: ParseLoc,
}

impl PartitionSpec<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PartitionSpec<'b>> {
        Ok(PartitionSpec {
            strategy: self.strategy,
            partParams: copy_node_vec(&self.partParams, mcx)?,
            location: self.location,
        })
    }
}

/// `PartitionBoundSpec` (`nodes/parsenodes.h`) — a partition bound spec.
#[derive(Debug)]
pub struct PartitionBoundSpec<'mcx> {
    pub strategy: i8,
    pub is_default: bool,
    pub modulus: i32,
    pub remainder: i32,
    pub listdatums: PgVec<'mcx, NodePtr<'mcx>>,
    pub lowerdatums: PgVec<'mcx, NodePtr<'mcx>>,
    pub upperdatums: PgVec<'mcx, NodePtr<'mcx>>,
    pub location: ParseLoc,
}

impl PartitionBoundSpec<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PartitionBoundSpec<'b>> {
        Ok(PartitionBoundSpec {
            strategy: self.strategy,
            is_default: self.is_default,
            modulus: self.modulus,
            remainder: self.remainder,
            listdatums: copy_node_vec(&self.listdatums, mcx)?,
            lowerdatums: copy_node_vec(&self.lowerdatums, mcx)?,
            upperdatums: copy_node_vec(&self.upperdatums, mcx)?,
            location: self.location,
        })
    }
}

/// `PartitionRangeDatum` (`nodes/parsenodes.h`) — one value in a range bound.
#[derive(Debug)]
pub struct PartitionRangeDatum<'mcx> {
    pub kind: PartitionRangeDatumKind,
    pub value: Option<NodePtr<'mcx>>,
    pub location: ParseLoc,
}

impl PartitionRangeDatum<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PartitionRangeDatum<'b>> {
        Ok(PartitionRangeDatum {
            kind: self.kind,
            value: copy_opt_node(&self.value, mcx)?,
            location: self.location,
        })
    }
}

/// `IntoClause` (`nodes/primnodes.h`) — full raw SELECT INTO / CREATE TABLE AS
/// target spec (the producer shape; consumers use the trimmed
/// [`crate::parsestmt::IntoClause`]).
#[derive(Debug)]
pub struct IntoClause<'mcx> {
    /// `RangeVar *rel`.
    pub rel: Option<NodePtr<'mcx>>,
    /// `List *colNames` — column names to assign, or NIL.
    pub colNames: PgVec<'mcx, NodePtr<'mcx>>,
    /// `char *accessMethod` — table access method.
    pub accessMethod: Option<PgString<'mcx>>,
    /// `List *options` — options from WITH clause.
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    /// `OnCommitAction onCommit` — what to do at COMMIT.
    pub onCommit: OnCommitAction,
    /// `char *tableSpaceName`.
    pub tableSpaceName: Option<PgString<'mcx>>,
    /// `Node *viewQuery` — materialized view's SELECT (only for materialized).
    pub viewQuery: Option<NodePtr<'mcx>>,
    /// `bool skipData` — true for WITH NO DATA.
    pub skipData: bool,
}

impl IntoClause<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<IntoClause<'b>> {
        Ok(IntoClause {
            rel: copy_opt_node(&self.rel, mcx)?,
            colNames: copy_node_vec(&self.colNames, mcx)?,
            accessMethod: copy_opt_str(&self.accessMethod, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            onCommit: self.onCommit,
            tableSpaceName: copy_opt_str(&self.tableSpaceName, mcx)?,
            viewQuery: copy_opt_node(&self.viewQuery, mcx)?,
            skipData: self.skipData,
        })
    }
}

// ===========================================================================
// CREATE-family statements
// ===========================================================================

/// `CreateStmt` (`nodes/parsenodes.h`) — CREATE TABLE.
#[derive(Debug)]
pub struct CreateStmt<'mcx> {
    /// `RangeVar *relation`.
    pub relation: Option<NodePtr<'mcx>>,
    pub tableElts: PgVec<'mcx, NodePtr<'mcx>>,
    pub inhRelations: PgVec<'mcx, NodePtr<'mcx>>,
    /// `PartitionBoundSpec *partbound`.
    pub partbound: Option<NodePtr<'mcx>>,
    /// `PartitionSpec *partspec`.
    pub partspec: Option<NodePtr<'mcx>>,
    /// `TypeName *ofTypename`.
    pub ofTypename: Option<NodePtr<'mcx>>,
    pub constraints: PgVec<'mcx, NodePtr<'mcx>>,
    pub nnconstraints: PgVec<'mcx, NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub oncommit: OnCommitAction,
    pub tablespacename: Option<PgString<'mcx>>,
    pub accessMethod: Option<PgString<'mcx>>,
    pub if_not_exists: bool,
}

impl CreateStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateStmt<'b>> {
        Ok(CreateStmt {
            relation: copy_opt_node(&self.relation, mcx)?,
            tableElts: copy_node_vec(&self.tableElts, mcx)?,
            inhRelations: copy_node_vec(&self.inhRelations, mcx)?,
            partbound: copy_opt_node(&self.partbound, mcx)?,
            partspec: copy_opt_node(&self.partspec, mcx)?,
            ofTypename: copy_opt_node(&self.ofTypename, mcx)?,
            constraints: copy_node_vec(&self.constraints, mcx)?,
            nnconstraints: copy_node_vec(&self.nnconstraints, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            oncommit: self.oncommit,
            tablespacename: copy_opt_str(&self.tablespacename, mcx)?,
            accessMethod: copy_opt_str(&self.accessMethod, mcx)?,
            if_not_exists: self.if_not_exists,
        })
    }
}

/// `IndexStmt` (`nodes/parsenodes.h`) — CREATE INDEX.
#[derive(Debug)]
pub struct IndexStmt<'mcx> {
    pub idxname: Option<PgString<'mcx>>,
    /// `RangeVar *relation`.
    pub relation: Option<NodePtr<'mcx>>,
    pub accessMethod: Option<PgString<'mcx>>,
    pub tableSpace: Option<PgString<'mcx>>,
    pub indexParams: PgVec<'mcx, NodePtr<'mcx>>,
    pub indexIncludingParams: PgVec<'mcx, NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub whereClause: Option<NodePtr<'mcx>>,
    pub excludeOpNames: PgVec<'mcx, NodePtr<'mcx>>,
    pub idxcomment: Option<PgString<'mcx>>,
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

impl IndexStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<IndexStmt<'b>> {
        Ok(IndexStmt {
            idxname: copy_opt_str(&self.idxname, mcx)?,
            relation: copy_opt_node(&self.relation, mcx)?,
            accessMethod: copy_opt_str(&self.accessMethod, mcx)?,
            tableSpace: copy_opt_str(&self.tableSpace, mcx)?,
            indexParams: copy_node_vec(&self.indexParams, mcx)?,
            indexIncludingParams: copy_node_vec(&self.indexIncludingParams, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            whereClause: copy_opt_node(&self.whereClause, mcx)?,
            excludeOpNames: copy_node_vec(&self.excludeOpNames, mcx)?,
            idxcomment: copy_opt_str(&self.idxcomment, mcx)?,
            indexOid: self.indexOid,
            oldNumber: self.oldNumber,
            oldCreateSubid: self.oldCreateSubid,
            oldFirstRelfilelocatorSubid: self.oldFirstRelfilelocatorSubid,
            unique: self.unique,
            nulls_not_distinct: self.nulls_not_distinct,
            primary: self.primary,
            isconstraint: self.isconstraint,
            iswithoutoverlaps: self.iswithoutoverlaps,
            deferrable: self.deferrable,
            initdeferred: self.initdeferred,
            transformed: self.transformed,
            concurrent: self.concurrent,
            if_not_exists: self.if_not_exists,
            reset_default_tblspc: self.reset_default_tblspc,
        })
    }
}

/// `CreateSeqStmt` (`nodes/parsenodes.h`) — CREATE SEQUENCE.
#[derive(Debug)]
pub struct CreateSeqStmt<'mcx> {
    /// `RangeVar *sequence`.
    pub sequence: Option<NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub ownerId: Oid,
    pub for_identity: bool,
    pub if_not_exists: bool,
}

impl CreateSeqStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateSeqStmt<'b>> {
        Ok(CreateSeqStmt {
            sequence: copy_opt_node(&self.sequence, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            ownerId: self.ownerId,
            for_identity: self.for_identity,
            if_not_exists: self.if_not_exists,
        })
    }
}

/// `CreateStatsStmt` (`nodes/parsenodes.h`) — CREATE STATISTICS.
#[derive(Debug)]
pub struct CreateStatsStmt<'mcx> {
    pub defnames: PgVec<'mcx, NodePtr<'mcx>>,
    pub stat_types: PgVec<'mcx, NodePtr<'mcx>>,
    pub exprs: PgVec<'mcx, NodePtr<'mcx>>,
    pub relations: PgVec<'mcx, NodePtr<'mcx>>,
    pub stxcomment: Option<PgString<'mcx>>,
    pub transformed: bool,
    pub if_not_exists: bool,
}

impl CreateStatsStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateStatsStmt<'b>> {
        Ok(CreateStatsStmt {
            defnames: copy_node_vec(&self.defnames, mcx)?,
            stat_types: copy_node_vec(&self.stat_types, mcx)?,
            exprs: copy_node_vec(&self.exprs, mcx)?,
            relations: copy_node_vec(&self.relations, mcx)?,
            stxcomment: copy_opt_str(&self.stxcomment, mcx)?,
            transformed: self.transformed,
            if_not_exists: self.if_not_exists,
        })
    }
}

/// `CreateFunctionStmt` (`nodes/parsenodes.h`) — CREATE FUNCTION / PROCEDURE.
#[derive(Debug)]
pub struct CreateFunctionStmt<'mcx> {
    pub is_procedure: bool,
    pub replace: bool,
    pub funcname: PgVec<'mcx, NodePtr<'mcx>>,
    pub parameters: PgVec<'mcx, NodePtr<'mcx>>,
    /// `TypeName *returnType`.
    pub returnType: Option<NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub sql_body: Option<NodePtr<'mcx>>,
}

impl CreateFunctionStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateFunctionStmt<'b>> {
        Ok(CreateFunctionStmt {
            is_procedure: self.is_procedure,
            replace: self.replace,
            funcname: copy_node_vec(&self.funcname, mcx)?,
            parameters: copy_node_vec(&self.parameters, mcx)?,
            returnType: copy_opt_node(&self.returnType, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            sql_body: copy_opt_node(&self.sql_body, mcx)?,
        })
    }
}

/// `DefineStmt` (`nodes/parsenodes.h`) — CREATE AGGREGATE/OPERATOR/TYPE/...
#[derive(Debug)]
pub struct DefineStmt<'mcx> {
    pub kind: ObjectType,
    pub oldstyle: bool,
    pub defnames: PgVec<'mcx, NodePtr<'mcx>>,
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    pub definition: PgVec<'mcx, NodePtr<'mcx>>,
    pub if_not_exists: bool,
    pub replace: bool,
}

impl DefineStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DefineStmt<'b>> {
        Ok(DefineStmt {
            kind: self.kind,
            oldstyle: self.oldstyle,
            defnames: copy_node_vec(&self.defnames, mcx)?,
            args: copy_node_vec(&self.args, mcx)?,
            definition: copy_node_vec(&self.definition, mcx)?,
            if_not_exists: self.if_not_exists,
            replace: self.replace,
        })
    }
}

/// `CreateDomainStmt` (`nodes/parsenodes.h`) — CREATE DOMAIN.
#[derive(Debug)]
pub struct CreateDomainStmt<'mcx> {
    pub domainname: PgVec<'mcx, NodePtr<'mcx>>,
    /// `TypeName *typeName`.
    pub typeName: Option<NodePtr<'mcx>>,
    /// `CollateClause *collClause`.
    pub collClause: Option<NodePtr<'mcx>>,
    pub constraints: PgVec<'mcx, NodePtr<'mcx>>,
}

impl CreateDomainStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateDomainStmt<'b>> {
        Ok(CreateDomainStmt {
            domainname: copy_node_vec(&self.domainname, mcx)?,
            typeName: copy_opt_node(&self.typeName, mcx)?,
            collClause: copy_opt_node(&self.collClause, mcx)?,
            constraints: copy_node_vec(&self.constraints, mcx)?,
        })
    }
}

/// `CompositeTypeStmt` (`nodes/parsenodes.h`) — CREATE TYPE ... AS (...).
#[derive(Debug)]
pub struct CompositeTypeStmt<'mcx> {
    /// `RangeVar *typevar`.
    pub typevar: Option<NodePtr<'mcx>>,
    pub coldeflist: PgVec<'mcx, NodePtr<'mcx>>,
}

impl CompositeTypeStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CompositeTypeStmt<'b>> {
        Ok(CompositeTypeStmt {
            typevar: copy_opt_node(&self.typevar, mcx)?,
            coldeflist: copy_node_vec(&self.coldeflist, mcx)?,
        })
    }
}

/// `CreateEnumStmt` (`nodes/parsenodes.h`) — CREATE TYPE ... AS ENUM.
#[derive(Debug)]
pub struct CreateEnumStmt<'mcx> {
    pub typeName: PgVec<'mcx, NodePtr<'mcx>>,
    pub vals: PgVec<'mcx, NodePtr<'mcx>>,
}

impl CreateEnumStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateEnumStmt<'b>> {
        Ok(CreateEnumStmt {
            typeName: copy_node_vec(&self.typeName, mcx)?,
            vals: copy_node_vec(&self.vals, mcx)?,
        })
    }
}

/// `CreateRangeStmt` (`nodes/parsenodes.h`) — CREATE TYPE ... AS RANGE.
#[derive(Debug)]
pub struct CreateRangeStmt<'mcx> {
    pub typeName: PgVec<'mcx, NodePtr<'mcx>>,
    pub params: PgVec<'mcx, NodePtr<'mcx>>,
}

impl CreateRangeStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateRangeStmt<'b>> {
        Ok(CreateRangeStmt {
            typeName: copy_node_vec(&self.typeName, mcx)?,
            params: copy_node_vec(&self.params, mcx)?,
        })
    }
}

/// `ViewStmt` (`nodes/parsenodes.h`) — CREATE VIEW.
#[derive(Debug)]
pub struct ViewStmt<'mcx> {
    /// `RangeVar *view`.
    pub view: Option<NodePtr<'mcx>>,
    pub aliases: PgVec<'mcx, NodePtr<'mcx>>,
    pub query: Option<NodePtr<'mcx>>,
    pub replace: bool,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub withCheckOption: ViewCheckOption,
}

impl ViewStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ViewStmt<'b>> {
        Ok(ViewStmt {
            view: copy_opt_node(&self.view, mcx)?,
            aliases: copy_node_vec(&self.aliases, mcx)?,
            query: copy_opt_node(&self.query, mcx)?,
            replace: self.replace,
            options: copy_node_vec(&self.options, mcx)?,
            withCheckOption: self.withCheckOption,
        })
    }
}

/// `CreateTableAsStmt` (`nodes/parsenodes.h`) — CREATE TABLE AS / SELECT INTO /
/// CREATE MATERIALIZED VIEW.
#[derive(Debug)]
pub struct CreateTableAsStmt<'mcx> {
    pub query: Option<NodePtr<'mcx>>,
    /// `IntoClause *into`.
    pub into: Option<NodePtr<'mcx>>,
    pub objtype: ObjectType,
    pub is_select_into: bool,
    pub if_not_exists: bool,
}

impl CreateTableAsStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateTableAsStmt<'b>> {
        Ok(CreateTableAsStmt {
            query: copy_opt_node(&self.query, mcx)?,
            into: copy_opt_node(&self.into, mcx)?,
            objtype: self.objtype,
            is_select_into: self.is_select_into,
            if_not_exists: self.if_not_exists,
        })
    }
}

/// `CreateSchemaStmt` (`nodes/parsenodes.h`) — CREATE SCHEMA.
#[derive(Debug)]
pub struct CreateSchemaStmt<'mcx> {
    pub schemaname: Option<PgString<'mcx>>,
    /// `RoleSpec *authrole`.
    pub authrole: Option<NodePtr<'mcx>>,
    pub schemaElts: PgVec<'mcx, NodePtr<'mcx>>,
    pub if_not_exists: bool,
}

impl CreateSchemaStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateSchemaStmt<'b>> {
        Ok(CreateSchemaStmt {
            schemaname: copy_opt_str(&self.schemaname, mcx)?,
            authrole: copy_opt_node(&self.authrole, mcx)?,
            schemaElts: copy_node_vec(&self.schemaElts, mcx)?,
            if_not_exists: self.if_not_exists,
        })
    }
}

/// `CreateExtensionStmt` (`nodes/parsenodes.h`) — CREATE EXTENSION.
#[derive(Debug)]
pub struct CreateExtensionStmt<'mcx> {
    pub extname: Option<PgString<'mcx>>,
    pub if_not_exists: bool,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl CreateExtensionStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateExtensionStmt<'b>> {
        Ok(CreateExtensionStmt {
            extname: copy_opt_str(&self.extname, mcx)?,
            if_not_exists: self.if_not_exists,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `CreateTrigStmt` (`nodes/parsenodes.h`) — CREATE TRIGGER.
#[derive(Debug)]
pub struct CreateTrigStmt<'mcx> {
    pub replace: bool,
    pub isconstraint: bool,
    pub trigname: Option<PgString<'mcx>>,
    /// `RangeVar *relation`.
    pub relation: Option<NodePtr<'mcx>>,
    pub funcname: PgVec<'mcx, NodePtr<'mcx>>,
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    pub row: bool,
    pub timing: i16,
    pub events: i16,
    pub columns: PgVec<'mcx, NodePtr<'mcx>>,
    pub whenClause: Option<NodePtr<'mcx>>,
    pub transitionRels: PgVec<'mcx, NodePtr<'mcx>>,
    pub deferrable: bool,
    pub initdeferred: bool,
    /// `RangeVar *constrrel`.
    pub constrrel: Option<NodePtr<'mcx>>,
}

impl CreateTrigStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateTrigStmt<'b>> {
        Ok(CreateTrigStmt {
            replace: self.replace,
            isconstraint: self.isconstraint,
            trigname: copy_opt_str(&self.trigname, mcx)?,
            relation: copy_opt_node(&self.relation, mcx)?,
            funcname: copy_node_vec(&self.funcname, mcx)?,
            args: copy_node_vec(&self.args, mcx)?,
            row: self.row,
            timing: self.timing,
            events: self.events,
            columns: copy_node_vec(&self.columns, mcx)?,
            whenClause: copy_opt_node(&self.whenClause, mcx)?,
            transitionRels: copy_node_vec(&self.transitionRels, mcx)?,
            deferrable: self.deferrable,
            initdeferred: self.initdeferred,
            constrrel: copy_opt_node(&self.constrrel, mcx)?,
        })
    }
}

/// `CreateRoleStmt` (`nodes/parsenodes.h`) — CREATE ROLE / USER / GROUP.
#[derive(Debug)]
pub struct CreateRoleStmt<'mcx> {
    pub stmt_type: RoleStmtType,
    pub role: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl CreateRoleStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateRoleStmt<'b>> {
        Ok(CreateRoleStmt {
            stmt_type: self.stmt_type,
            role: copy_opt_str(&self.role, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `CreatedbStmt` (`nodes/parsenodes.h`) — CREATE DATABASE.
#[derive(Debug)]
pub struct CreatedbStmt<'mcx> {
    pub dbname: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl CreatedbStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreatedbStmt<'b>> {
        Ok(CreatedbStmt {
            dbname: copy_opt_str(&self.dbname, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `CreateCastStmt` (`nodes/parsenodes.h`) — CREATE CAST.
#[derive(Debug)]
pub struct CreateCastStmt<'mcx> {
    /// `TypeName *sourcetype`.
    pub sourcetype: Option<NodePtr<'mcx>>,
    /// `TypeName *targettype`.
    pub targettype: Option<NodePtr<'mcx>>,
    /// `ObjectWithArgs *func`.
    pub func: Option<NodePtr<'mcx>>,
    pub context: CoercionContext,
    pub inout: bool,
}

impl CreateCastStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateCastStmt<'b>> {
        Ok(CreateCastStmt {
            sourcetype: copy_opt_node(&self.sourcetype, mcx)?,
            targettype: copy_opt_node(&self.targettype, mcx)?,
            func: copy_opt_node(&self.func, mcx)?,
            context: self.context,
            inout: self.inout,
        })
    }
}

/// `CreateOpClassStmt` (`nodes/parsenodes.h`) — CREATE OPERATOR CLASS.
#[derive(Debug)]
pub struct CreateOpClassStmt<'mcx> {
    pub opclassname: PgVec<'mcx, NodePtr<'mcx>>,
    pub opfamilyname: PgVec<'mcx, NodePtr<'mcx>>,
    pub amname: Option<PgString<'mcx>>,
    /// `TypeName *datatype`.
    pub datatype: Option<NodePtr<'mcx>>,
    pub items: PgVec<'mcx, NodePtr<'mcx>>,
    pub isDefault: bool,
}

impl CreateOpClassStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateOpClassStmt<'b>> {
        Ok(CreateOpClassStmt {
            opclassname: copy_node_vec(&self.opclassname, mcx)?,
            opfamilyname: copy_node_vec(&self.opfamilyname, mcx)?,
            amname: copy_opt_str(&self.amname, mcx)?,
            datatype: copy_opt_node(&self.datatype, mcx)?,
            items: copy_node_vec(&self.items, mcx)?,
            isDefault: self.isDefault,
        })
    }
}

/// `CreateOpFamilyStmt` (`nodes/parsenodes.h`) — CREATE OPERATOR FAMILY.
#[derive(Debug)]
pub struct CreateOpFamilyStmt<'mcx> {
    pub opfamilyname: PgVec<'mcx, NodePtr<'mcx>>,
    pub amname: Option<PgString<'mcx>>,
}

impl CreateOpFamilyStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateOpFamilyStmt<'b>> {
        Ok(CreateOpFamilyStmt {
            opfamilyname: copy_node_vec(&self.opfamilyname, mcx)?,
            amname: copy_opt_str(&self.amname, mcx)?,
        })
    }
}

/// `CreatePLangStmt` (`nodes/parsenodes.h`) — CREATE LANGUAGE.
#[derive(Debug)]
pub struct CreatePLangStmt<'mcx> {
    pub replace: bool,
    pub plname: Option<PgString<'mcx>>,
    pub plhandler: PgVec<'mcx, NodePtr<'mcx>>,
    pub plinline: PgVec<'mcx, NodePtr<'mcx>>,
    pub plvalidator: PgVec<'mcx, NodePtr<'mcx>>,
    pub pltrusted: bool,
}

impl CreatePLangStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreatePLangStmt<'b>> {
        Ok(CreatePLangStmt {
            replace: self.replace,
            plname: copy_opt_str(&self.plname, mcx)?,
            plhandler: copy_node_vec(&self.plhandler, mcx)?,
            plinline: copy_node_vec(&self.plinline, mcx)?,
            plvalidator: copy_node_vec(&self.plvalidator, mcx)?,
            pltrusted: self.pltrusted,
        })
    }
}

/// `CreateTableSpaceStmt` (`nodes/parsenodes.h`) — CREATE TABLESPACE.
#[derive(Debug)]
pub struct CreateTableSpaceStmt<'mcx> {
    pub tablespacename: Option<PgString<'mcx>>,
    /// `RoleSpec *owner`.
    pub owner: Option<NodePtr<'mcx>>,
    pub location: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl CreateTableSpaceStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateTableSpaceStmt<'b>> {
        Ok(CreateTableSpaceStmt {
            tablespacename: copy_opt_str(&self.tablespacename, mcx)?,
            owner: copy_opt_node(&self.owner, mcx)?,
            location: copy_opt_str(&self.location, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `CreateConversionStmt` (`nodes/parsenodes.h`) — CREATE CONVERSION.
#[derive(Debug)]
pub struct CreateConversionStmt<'mcx> {
    pub conversion_name: PgVec<'mcx, NodePtr<'mcx>>,
    pub for_encoding_name: Option<PgString<'mcx>>,
    pub to_encoding_name: Option<PgString<'mcx>>,
    pub func_name: PgVec<'mcx, NodePtr<'mcx>>,
    pub def: bool,
}

impl CreateConversionStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateConversionStmt<'b>> {
        Ok(CreateConversionStmt {
            conversion_name: copy_node_vec(&self.conversion_name, mcx)?,
            for_encoding_name: copy_opt_str(&self.for_encoding_name, mcx)?,
            to_encoding_name: copy_opt_str(&self.to_encoding_name, mcx)?,
            func_name: copy_node_vec(&self.func_name, mcx)?,
            def: self.def,
        })
    }
}

/// `CreateAmStmt` (`nodes/parsenodes.h`) — CREATE ACCESS METHOD.
#[derive(Debug)]
pub struct CreateAmStmt<'mcx> {
    pub amname: Option<PgString<'mcx>>,
    pub handler_name: PgVec<'mcx, NodePtr<'mcx>>,
    pub amtype: i8,
}

impl CreateAmStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateAmStmt<'b>> {
        Ok(CreateAmStmt {
            amname: copy_opt_str(&self.amname, mcx)?,
            handler_name: copy_node_vec(&self.handler_name, mcx)?,
            amtype: self.amtype,
        })
    }
}
