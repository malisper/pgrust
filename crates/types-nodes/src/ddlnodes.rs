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
use crate::parsenodes::{DropBehavior, ObjectType, RoleSpecType};
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

// ===========================================================================
// ALTER / DROP family enums (nodes/parsenodes.h)
// ===========================================================================

/// `AlterTableType` (`nodes/parsenodes.h`) — the subtype of an
/// [`AlterTableCmd`]. Values verified against PostgreSQL 18.3.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum AlterTableType {
    /// add column
    #[default]
    AT_AddColumn = 0,
    /// implicitly via CREATE OR REPLACE VIEW
    AT_AddColumnToView = 1,
    /// alter column default
    AT_ColumnDefault = 2,
    /// add a pre-cooked column default
    AT_CookedColumnDefault = 3,
    /// alter column drop not null
    AT_DropNotNull = 4,
    /// alter column set not null
    AT_SetNotNull = 5,
    /// alter column set expression
    AT_SetExpression = 6,
    /// alter column drop expression
    AT_DropExpression = 7,
    /// alter column set statistics
    AT_SetStatistics = 8,
    /// alter column set ( options )
    AT_SetOptions = 9,
    /// alter column reset ( options )
    AT_ResetOptions = 10,
    /// alter column set storage
    AT_SetStorage = 11,
    /// alter column set compression
    AT_SetCompression = 12,
    /// drop column
    AT_DropColumn = 13,
    /// add index
    AT_AddIndex = 14,
    /// internal to commands/tablecmds.c
    AT_ReAddIndex = 15,
    /// add constraint
    AT_AddConstraint = 16,
    /// internal to commands/tablecmds.c
    AT_ReAddConstraint = 17,
    /// internal to commands/tablecmds.c
    AT_ReAddDomainConstraint = 18,
    /// alter constraint
    AT_AlterConstraint = 19,
    /// validate constraint
    AT_ValidateConstraint = 20,
    /// add constraint using existing index
    AT_AddIndexConstraint = 21,
    /// drop constraint
    AT_DropConstraint = 22,
    /// internal to commands/tablecmds.c
    AT_ReAddComment = 23,
    /// alter column type
    AT_AlterColumnType = 24,
    /// alter column OPTIONS (...)
    AT_AlterColumnGenericOptions = 25,
    /// change owner
    AT_ChangeOwner = 26,
    /// CLUSTER ON
    AT_ClusterOn = 27,
    /// SET WITHOUT CLUSTER
    AT_DropCluster = 28,
    /// SET LOGGED
    AT_SetLogged = 29,
    /// SET UNLOGGED
    AT_SetUnLogged = 30,
    /// SET WITHOUT OIDS
    AT_DropOids = 31,
    /// SET ACCESS METHOD
    AT_SetAccessMethod = 32,
    /// SET TABLESPACE
    AT_SetTableSpace = 33,
    /// SET (...) -- AM specific parameters
    AT_SetRelOptions = 34,
    /// RESET (...) -- AM specific parameters
    AT_ResetRelOptions = 35,
    /// replace reloption list in its entirety
    AT_ReplaceRelOptions = 36,
    /// ENABLE TRIGGER name
    AT_EnableTrig = 37,
    /// ENABLE ALWAYS TRIGGER name
    AT_EnableAlwaysTrig = 38,
    /// ENABLE REPLICA TRIGGER name
    AT_EnableReplicaTrig = 39,
    /// DISABLE TRIGGER name
    AT_DisableTrig = 40,
    /// ENABLE TRIGGER ALL
    AT_EnableTrigAll = 41,
    /// DISABLE TRIGGER ALL
    AT_DisableTrigAll = 42,
    /// ENABLE TRIGGER USER
    AT_EnableTrigUser = 43,
    /// DISABLE TRIGGER USER
    AT_DisableTrigUser = 44,
    /// ENABLE RULE name
    AT_EnableRule = 45,
    /// ENABLE ALWAYS RULE name
    AT_EnableAlwaysRule = 46,
    /// ENABLE REPLICA RULE name
    AT_EnableReplicaRule = 47,
    /// DISABLE RULE name
    AT_DisableRule = 48,
    /// INHERIT parent
    AT_AddInherit = 49,
    /// NO INHERIT parent
    AT_DropInherit = 50,
    /// OF <type_name>
    AT_AddOf = 51,
    /// NOT OF
    AT_DropOf = 52,
    /// REPLICA IDENTITY
    AT_ReplicaIdentity = 53,
    /// ENABLE ROW SECURITY
    AT_EnableRowSecurity = 54,
    /// DISABLE ROW SECURITY
    AT_DisableRowSecurity = 55,
    /// FORCE ROW SECURITY
    AT_ForceRowSecurity = 56,
    /// NO FORCE ROW SECURITY
    AT_NoForceRowSecurity = 57,
    /// OPTIONS (...)
    AT_GenericOptions = 58,
    /// ATTACH PARTITION
    AT_AttachPartition = 59,
    /// DETACH PARTITION
    AT_DetachPartition = 60,
    /// DETACH PARTITION FINALIZE
    AT_DetachPartitionFinalize = 61,
    /// ADD IDENTITY
    AT_AddIdentity = 62,
    /// SET identity column options
    AT_SetIdentity = 63,
    /// DROP IDENTITY
    AT_DropIdentity = 64,
    /// internal to commands/tablecmds.c
    AT_ReAddStatistics = 65,
}
pub use AlterTableType::{
    AT_AddColumn, AT_AddColumnToView, AT_AddConstraint, AT_AddIdentity, AT_AddIndex,
    AT_AddIndexConstraint, AT_AddInherit, AT_AddOf, AT_AlterColumnGenericOptions,
    AT_AlterColumnType, AT_AlterConstraint, AT_ChangeOwner, AT_ClusterOn, AT_ColumnDefault,
    AT_CookedColumnDefault, AT_DetachPartition, AT_DetachPartitionFinalize, AT_DisableRowSecurity,
    AT_DisableRule, AT_DisableTrig, AT_DisableTrigAll, AT_DisableTrigUser, AT_DropCluster,
    AT_DropColumn, AT_DropConstraint, AT_DropExpression, AT_DropIdentity, AT_DropInherit,
    AT_DropNotNull, AT_DropOf, AT_DropOids, AT_EnableAlwaysRule, AT_EnableAlwaysTrig,
    AT_EnableReplicaRule, AT_EnableReplicaTrig, AT_EnableRowSecurity, AT_EnableRule, AT_EnableTrig,
    AT_EnableTrigAll, AT_EnableTrigUser, AT_ForceRowSecurity, AT_GenericOptions,
    AT_NoForceRowSecurity, AT_ReAddComment, AT_ReAddConstraint, AT_ReAddDomainConstraint,
    AT_ReAddIndex, AT_ReAddStatistics, AT_ReplaceRelOptions, AT_ReplicaIdentity, AT_ResetOptions,
    AT_ResetRelOptions, AT_SetAccessMethod, AT_SetCompression, AT_SetExpression, AT_SetIdentity,
    AT_SetLogged, AT_SetNotNull, AT_SetOptions, AT_SetRelOptions, AT_SetStatistics, AT_SetStorage,
    AT_SetTableSpace, AT_SetUnLogged, AT_AttachPartition, AT_ValidateConstraint,
};

/// `AlterTSConfigType` (`nodes/parsenodes.h`) — kind of an
/// [`AlterTSConfigurationStmt`]. Values verified against PostgreSQL 18.3.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum AlterTSConfigType {
    #[default]
    ALTER_TSCONFIG_ADD_MAPPING = 0,
    ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN = 1,
    ALTER_TSCONFIG_REPLACE_DICT = 2,
    ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN = 3,
    ALTER_TSCONFIG_DROP_MAPPING = 4,
}
pub use AlterTSConfigType::{
    ALTER_TSCONFIG_ADD_MAPPING, ALTER_TSCONFIG_ALTER_MAPPING_FOR_TOKEN,
    ALTER_TSCONFIG_DROP_MAPPING, ALTER_TSCONFIG_REPLACE_DICT, ALTER_TSCONFIG_REPLACE_DICT_FOR_TOKEN,
};

/// `AlterPublicationAction` (`nodes/parsenodes.h`) — what
/// [`AlterPublicationStmt`] does. Values verified against PostgreSQL 18.3.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum AlterPublicationAction {
    /// add objects to publication
    #[default]
    AP_AddObjects = 0,
    /// remove objects from publication
    AP_DropObjects = 1,
    /// set list of objects
    AP_SetObjects = 2,
}
pub use AlterPublicationAction::{AP_AddObjects, AP_DropObjects, AP_SetObjects};

/// `AlterSubscriptionType` (`nodes/parsenodes.h`) — kind of an
/// [`AlterSubscriptionStmt`]. Values verified against PostgreSQL 18.3.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum AlterSubscriptionType {
    #[default]
    ALTER_SUBSCRIPTION_OPTIONS = 0,
    ALTER_SUBSCRIPTION_CONNECTION = 1,
    ALTER_SUBSCRIPTION_SET_PUBLICATION = 2,
    ALTER_SUBSCRIPTION_ADD_PUBLICATION = 3,
    ALTER_SUBSCRIPTION_DROP_PUBLICATION = 4,
    ALTER_SUBSCRIPTION_REFRESH = 5,
    ALTER_SUBSCRIPTION_ENABLED = 6,
    ALTER_SUBSCRIPTION_SKIP = 7,
}
pub use AlterSubscriptionType::{
    ALTER_SUBSCRIPTION_ADD_PUBLICATION, ALTER_SUBSCRIPTION_CONNECTION, ALTER_SUBSCRIPTION_DROP_PUBLICATION,
    ALTER_SUBSCRIPTION_ENABLED, ALTER_SUBSCRIPTION_OPTIONS, ALTER_SUBSCRIPTION_REFRESH,
    ALTER_SUBSCRIPTION_SET_PUBLICATION, ALTER_SUBSCRIPTION_SKIP,
};

// ===========================================================================
// ALTER / DROP family — supporting / helper nodes
// ===========================================================================

/// `PartitionCmd` (`nodes/parsenodes.h`) — ATTACH/DETACH PARTITION subcommand.
#[derive(Debug)]
pub struct PartitionCmd<'mcx> {
    /// `RangeVar *name`.
    pub name: Option<NodePtr<'mcx>>,
    /// `PartitionBoundSpec *bound`.
    pub bound: Option<NodePtr<'mcx>>,
    pub concurrent: bool,
}

impl PartitionCmd<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PartitionCmd<'b>> {
        Ok(PartitionCmd {
            name: copy_opt_node(&self.name, mcx)?,
            bound: copy_opt_node(&self.bound, mcx)?,
            concurrent: self.concurrent,
        })
    }
}

/// `ReplicaIdentityStmt` (`nodes/parsenodes.h`) — REPLICA IDENTITY clause.
#[derive(Debug)]
pub struct ReplicaIdentityStmt<'mcx> {
    pub identity_type: i8,
    pub name: Option<PgString<'mcx>>,
}

impl ReplicaIdentityStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ReplicaIdentityStmt<'b>> {
        Ok(ReplicaIdentityStmt {
            identity_type: self.identity_type,
            name: copy_opt_str(&self.name, mcx)?,
        })
    }
}

/// `ATAlterConstraint` (`nodes/parsenodes.h`) — ad-hoc node for
/// `AT_AlterConstraint`.
#[derive(Debug)]
pub struct ATAlterConstraint<'mcx> {
    pub conname: Option<PgString<'mcx>>,
    pub alterEnforceability: bool,
    pub is_enforced: bool,
    pub alterDeferrability: bool,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub alterInheritability: bool,
    pub noinherit: bool,
}

impl ATAlterConstraint<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ATAlterConstraint<'b>> {
        Ok(ATAlterConstraint {
            conname: copy_opt_str(&self.conname, mcx)?,
            alterEnforceability: self.alterEnforceability,
            is_enforced: self.is_enforced,
            alterDeferrability: self.alterDeferrability,
            deferrable: self.deferrable,
            initdeferred: self.initdeferred,
            alterInheritability: self.alterInheritability,
            noinherit: self.noinherit,
        })
    }
}

// ===========================================================================
// ALTER / DROP family — statements
// ===========================================================================

/// `AlterTableStmt` (`nodes/parsenodes.h`) — ALTER TABLE (and similar).
#[derive(Debug)]
pub struct AlterTableStmt<'mcx> {
    /// `RangeVar *relation`.
    pub relation: Option<NodePtr<'mcx>>,
    /// `List *cmds` — list of [`AlterTableCmd`].
    pub cmds: PgVec<'mcx, NodePtr<'mcx>>,
    pub objtype: ObjectType,
    pub missing_ok: bool,
}

impl AlterTableStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterTableStmt<'b>> {
        Ok(AlterTableStmt {
            relation: copy_opt_node(&self.relation, mcx)?,
            cmds: copy_node_vec(&self.cmds, mcx)?,
            objtype: self.objtype,
            missing_ok: self.missing_ok,
        })
    }
}

/// `AlterTableCmd` (`nodes/parsenodes.h`) — one subcommand of an ALTER TABLE.
#[derive(Debug)]
pub struct AlterTableCmd<'mcx> {
    pub subtype: AlterTableType,
    pub name: Option<PgString<'mcx>>,
    pub num: i16,
    /// `RoleSpec *newowner`.
    pub newowner: Option<NodePtr<'mcx>>,
    pub def: Option<NodePtr<'mcx>>,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
    pub recurse: bool,
}

impl AlterTableCmd<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterTableCmd<'b>> {
        Ok(AlterTableCmd {
            subtype: self.subtype,
            name: copy_opt_str(&self.name, mcx)?,
            num: self.num,
            newowner: copy_opt_node(&self.newowner, mcx)?,
            def: copy_opt_node(&self.def, mcx)?,
            behavior: self.behavior,
            missing_ok: self.missing_ok,
            recurse: self.recurse,
        })
    }
}

/// `AlterCollationStmt` (`nodes/parsenodes.h`) — ALTER COLLATION ... REFRESH.
#[derive(Debug)]
pub struct AlterCollationStmt<'mcx> {
    pub collname: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterCollationStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterCollationStmt<'b>> {
        Ok(AlterCollationStmt {
            collname: copy_node_vec(&self.collname, mcx)?,
        })
    }
}

/// `AlterDomainStmt` (`nodes/parsenodes.h`) — ALTER DOMAIN.
#[derive(Debug)]
pub struct AlterDomainStmt<'mcx> {
    pub subtype: i8,
    pub typeName: PgVec<'mcx, NodePtr<'mcx>>,
    pub name: Option<PgString<'mcx>>,
    pub def: Option<NodePtr<'mcx>>,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
}

impl AlterDomainStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterDomainStmt<'b>> {
        Ok(AlterDomainStmt {
            subtype: self.subtype,
            typeName: copy_node_vec(&self.typeName, mcx)?,
            name: copy_opt_str(&self.name, mcx)?,
            def: copy_opt_node(&self.def, mcx)?,
            behavior: self.behavior,
            missing_ok: self.missing_ok,
        })
    }
}

/// `AlterEnumStmt` (`nodes/parsenodes.h`) — ALTER TYPE ... (enum value).
#[derive(Debug)]
pub struct AlterEnumStmt<'mcx> {
    pub typeName: PgVec<'mcx, NodePtr<'mcx>>,
    pub oldVal: Option<PgString<'mcx>>,
    pub newVal: Option<PgString<'mcx>>,
    pub newValNeighbor: Option<PgString<'mcx>>,
    pub newValIsAfter: bool,
    pub skipIfNewValExists: bool,
}

impl AlterEnumStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterEnumStmt<'b>> {
        Ok(AlterEnumStmt {
            typeName: copy_node_vec(&self.typeName, mcx)?,
            oldVal: copy_opt_str(&self.oldVal, mcx)?,
            newVal: copy_opt_str(&self.newVal, mcx)?,
            newValNeighbor: copy_opt_str(&self.newValNeighbor, mcx)?,
            newValIsAfter: self.newValIsAfter,
            skipIfNewValExists: self.skipIfNewValExists,
        })
    }
}

/// `AlterStatsStmt` (`nodes/parsenodes.h`) — ALTER STATISTICS.
#[derive(Debug)]
pub struct AlterStatsStmt<'mcx> {
    pub defnames: PgVec<'mcx, NodePtr<'mcx>>,
    pub stxstattarget: Option<NodePtr<'mcx>>,
    pub missing_ok: bool,
}

impl AlterStatsStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterStatsStmt<'b>> {
        Ok(AlterStatsStmt {
            defnames: copy_node_vec(&self.defnames, mcx)?,
            stxstattarget: copy_opt_node(&self.stxstattarget, mcx)?,
            missing_ok: self.missing_ok,
        })
    }
}

/// `AlterSeqStmt` (`nodes/parsenodes.h`) — ALTER SEQUENCE.
#[derive(Debug)]
pub struct AlterSeqStmt<'mcx> {
    /// `RangeVar *sequence`.
    pub sequence: Option<NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub for_identity: bool,
    pub missing_ok: bool,
}

impl AlterSeqStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterSeqStmt<'b>> {
        Ok(AlterSeqStmt {
            sequence: copy_opt_node(&self.sequence, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            for_identity: self.for_identity,
            missing_ok: self.missing_ok,
        })
    }
}

/// `AlterOpFamilyStmt` (`nodes/parsenodes.h`) — ALTER OPERATOR FAMILY.
#[derive(Debug)]
pub struct AlterOpFamilyStmt<'mcx> {
    pub opfamilyname: PgVec<'mcx, NodePtr<'mcx>>,
    pub amname: Option<PgString<'mcx>>,
    pub isDrop: bool,
    pub items: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterOpFamilyStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterOpFamilyStmt<'b>> {
        Ok(AlterOpFamilyStmt {
            opfamilyname: copy_node_vec(&self.opfamilyname, mcx)?,
            amname: copy_opt_str(&self.amname, mcx)?,
            isDrop: self.isDrop,
            items: copy_node_vec(&self.items, mcx)?,
        })
    }
}

/// `AlterFunctionStmt` (`nodes/parsenodes.h`) — ALTER FUNCTION.
#[derive(Debug)]
pub struct AlterFunctionStmt<'mcx> {
    pub objtype: ObjectType,
    /// `ObjectWithArgs *func`.
    pub func: Option<NodePtr<'mcx>>,
    pub actions: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterFunctionStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterFunctionStmt<'b>> {
        Ok(AlterFunctionStmt {
            objtype: self.objtype,
            func: copy_opt_node(&self.func, mcx)?,
            actions: copy_node_vec(&self.actions, mcx)?,
        })
    }
}

/// `DropStmt` (`nodes/parsenodes.h`) — DROP <object>.
#[derive(Debug)]
pub struct DropStmt<'mcx> {
    pub objects: PgVec<'mcx, NodePtr<'mcx>>,
    pub removeType: ObjectType,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
    pub concurrent: bool,
}

impl DropStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DropStmt<'b>> {
        Ok(DropStmt {
            objects: copy_node_vec(&self.objects, mcx)?,
            removeType: self.removeType,
            behavior: self.behavior,
            missing_ok: self.missing_ok,
            concurrent: self.concurrent,
        })
    }
}

/// `RenameStmt` (`nodes/parsenodes.h`) — ALTER ... RENAME.
#[derive(Debug)]
pub struct RenameStmt<'mcx> {
    pub renameType: ObjectType,
    pub relationType: ObjectType,
    /// `RangeVar *relation`.
    pub relation: Option<NodePtr<'mcx>>,
    pub object: Option<NodePtr<'mcx>>,
    pub subname: Option<PgString<'mcx>>,
    pub newname: Option<PgString<'mcx>>,
    pub behavior: DropBehavior,
    pub missing_ok: bool,
}

impl RenameStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RenameStmt<'b>> {
        Ok(RenameStmt {
            renameType: self.renameType,
            relationType: self.relationType,
            relation: copy_opt_node(&self.relation, mcx)?,
            object: copy_opt_node(&self.object, mcx)?,
            subname: copy_opt_str(&self.subname, mcx)?,
            newname: copy_opt_str(&self.newname, mcx)?,
            behavior: self.behavior,
            missing_ok: self.missing_ok,
        })
    }
}

/// `AlterObjectDependsStmt` (`nodes/parsenodes.h`) — ALTER ... DEPENDS ON
/// EXTENSION.
#[derive(Debug)]
pub struct AlterObjectDependsStmt<'mcx> {
    pub objectType: ObjectType,
    /// `RangeVar *relation`.
    pub relation: Option<NodePtr<'mcx>>,
    pub object: Option<NodePtr<'mcx>>,
    /// `String *extname` — carried as a `Node` (String value node).
    pub extname: Option<NodePtr<'mcx>>,
    pub remove: bool,
}

impl AlterObjectDependsStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterObjectDependsStmt<'b>> {
        Ok(AlterObjectDependsStmt {
            objectType: self.objectType,
            relation: copy_opt_node(&self.relation, mcx)?,
            object: copy_opt_node(&self.object, mcx)?,
            extname: copy_opt_node(&self.extname, mcx)?,
            remove: self.remove,
        })
    }
}

/// `AlterObjectSchemaStmt` (`nodes/parsenodes.h`) — ALTER ... SET SCHEMA.
#[derive(Debug)]
pub struct AlterObjectSchemaStmt<'mcx> {
    pub objectType: ObjectType,
    /// `RangeVar *relation`.
    pub relation: Option<NodePtr<'mcx>>,
    pub object: Option<NodePtr<'mcx>>,
    pub newschema: Option<PgString<'mcx>>,
    pub missing_ok: bool,
}

impl AlterObjectSchemaStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterObjectSchemaStmt<'b>> {
        Ok(AlterObjectSchemaStmt {
            objectType: self.objectType,
            relation: copy_opt_node(&self.relation, mcx)?,
            object: copy_opt_node(&self.object, mcx)?,
            newschema: copy_opt_str(&self.newschema, mcx)?,
            missing_ok: self.missing_ok,
        })
    }
}

/// `AlterOwnerStmt` (`nodes/parsenodes.h`) — ALTER ... OWNER TO.
#[derive(Debug)]
pub struct AlterOwnerStmt<'mcx> {
    pub objectType: ObjectType,
    /// `RangeVar *relation`.
    pub relation: Option<NodePtr<'mcx>>,
    pub object: Option<NodePtr<'mcx>>,
    /// `RoleSpec *newowner`.
    pub newowner: Option<NodePtr<'mcx>>,
}

impl AlterOwnerStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterOwnerStmt<'b>> {
        Ok(AlterOwnerStmt {
            objectType: self.objectType,
            relation: copy_opt_node(&self.relation, mcx)?,
            object: copy_opt_node(&self.object, mcx)?,
            newowner: copy_opt_node(&self.newowner, mcx)?,
        })
    }
}

/// `AlterOperatorStmt` (`nodes/parsenodes.h`) — ALTER OPERATOR.
#[derive(Debug)]
pub struct AlterOperatorStmt<'mcx> {
    /// `ObjectWithArgs *opername`.
    pub opername: Option<NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterOperatorStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterOperatorStmt<'b>> {
        Ok(AlterOperatorStmt {
            opername: copy_opt_node(&self.opername, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `AlterTypeStmt` (`nodes/parsenodes.h`) — ALTER TYPE (base type properties).
#[derive(Debug)]
pub struct AlterTypeStmt<'mcx> {
    pub typeName: PgVec<'mcx, NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterTypeStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterTypeStmt<'b>> {
        Ok(AlterTypeStmt {
            typeName: copy_node_vec(&self.typeName, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `AlterDefaultPrivilegesStmt` (`nodes/parsenodes.h`) — ALTER DEFAULT
/// PRIVILEGES. The `action` is a `GrantStmt` (GRANT/REVOKE family, F4) carried
/// as a `Node`.
#[derive(Debug)]
pub struct AlterDefaultPrivilegesStmt<'mcx> {
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    /// `GrantStmt *action`.
    pub action: Option<NodePtr<'mcx>>,
}

impl AlterDefaultPrivilegesStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterDefaultPrivilegesStmt<'b>> {
        Ok(AlterDefaultPrivilegesStmt {
            options: copy_node_vec(&self.options, mcx)?,
            action: copy_opt_node(&self.action, mcx)?,
        })
    }
}

/// `AlterRoleStmt` (`nodes/parsenodes.h`) — ALTER ROLE.
#[derive(Debug)]
pub struct AlterRoleStmt<'mcx> {
    /// `RoleSpec *role`.
    pub role: Option<NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    /// `+1 = add members, -1 = drop members`.
    pub action: i32,
}

impl AlterRoleStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterRoleStmt<'b>> {
        Ok(AlterRoleStmt {
            role: copy_opt_node(&self.role, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            action: self.action,
        })
    }
}

/// `AlterRoleSetStmt` (`nodes/parsenodes.h`) — ALTER ROLE ... SET. The
/// `setstmt` is a `VariableSetStmt` (SET family, F4) carried as a `Node`.
#[derive(Debug)]
pub struct AlterRoleSetStmt<'mcx> {
    /// `RoleSpec *role`.
    pub role: Option<NodePtr<'mcx>>,
    pub database: Option<PgString<'mcx>>,
    /// `VariableSetStmt *setstmt`.
    pub setstmt: Option<NodePtr<'mcx>>,
}

impl AlterRoleSetStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterRoleSetStmt<'b>> {
        Ok(AlterRoleSetStmt {
            role: copy_opt_node(&self.role, mcx)?,
            database: copy_opt_str(&self.database, mcx)?,
            setstmt: copy_opt_node(&self.setstmt, mcx)?,
        })
    }
}

/// `DropOwnedStmt` (`nodes/parsenodes.h`) — DROP OWNED.
#[derive(Debug)]
pub struct DropOwnedStmt<'mcx> {
    pub roles: PgVec<'mcx, NodePtr<'mcx>>,
    pub behavior: DropBehavior,
}

impl DropOwnedStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DropOwnedStmt<'b>> {
        Ok(DropOwnedStmt {
            roles: copy_node_vec(&self.roles, mcx)?,
            behavior: self.behavior,
        })
    }
}

/// `ReassignOwnedStmt` (`nodes/parsenodes.h`) — REASSIGN OWNED.
#[derive(Debug)]
pub struct ReassignOwnedStmt<'mcx> {
    pub roles: PgVec<'mcx, NodePtr<'mcx>>,
    /// `RoleSpec *newrole`.
    pub newrole: Option<NodePtr<'mcx>>,
}

impl ReassignOwnedStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ReassignOwnedStmt<'b>> {
        Ok(ReassignOwnedStmt {
            roles: copy_node_vec(&self.roles, mcx)?,
            newrole: copy_opt_node(&self.newrole, mcx)?,
        })
    }
}

/// `AlterTableSpaceOptionsStmt` (`nodes/parsenodes.h`) — ALTER TABLESPACE ...
/// SET/RESET.
#[derive(Debug)]
pub struct AlterTableSpaceOptionsStmt<'mcx> {
    pub tablespacename: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub isReset: bool,
}

impl AlterTableSpaceOptionsStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterTableSpaceOptionsStmt<'b>> {
        Ok(AlterTableSpaceOptionsStmt {
            tablespacename: copy_opt_str(&self.tablespacename, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            isReset: self.isReset,
        })
    }
}

/// `AlterTableMoveAllStmt` (`nodes/parsenodes.h`) — ALTER TABLESPACE ... MOVE
/// ALL.
#[derive(Debug)]
pub struct AlterTableMoveAllStmt<'mcx> {
    pub orig_tablespacename: Option<PgString<'mcx>>,
    pub objtype: ObjectType,
    pub roles: PgVec<'mcx, NodePtr<'mcx>>,
    pub new_tablespacename: Option<PgString<'mcx>>,
    pub nowait: bool,
}

impl AlterTableMoveAllStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterTableMoveAllStmt<'b>> {
        Ok(AlterTableMoveAllStmt {
            orig_tablespacename: copy_opt_str(&self.orig_tablespacename, mcx)?,
            objtype: self.objtype,
            roles: copy_node_vec(&self.roles, mcx)?,
            new_tablespacename: copy_opt_str(&self.new_tablespacename, mcx)?,
            nowait: self.nowait,
        })
    }
}

/// `AlterExtensionStmt` (`nodes/parsenodes.h`) — ALTER EXTENSION ... UPDATE.
#[derive(Debug)]
pub struct AlterExtensionStmt<'mcx> {
    pub extname: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterExtensionStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterExtensionStmt<'b>> {
        Ok(AlterExtensionStmt {
            extname: copy_opt_str(&self.extname, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `AlterExtensionContentsStmt` (`nodes/parsenodes.h`) — ALTER EXTENSION ...
/// ADD/DROP.
#[derive(Debug)]
pub struct AlterExtensionContentsStmt<'mcx> {
    pub extname: Option<PgString<'mcx>>,
    /// `+1 = add object, -1 = drop object`.
    pub action: i32,
    pub objtype: ObjectType,
    pub object: Option<NodePtr<'mcx>>,
}

impl AlterExtensionContentsStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterExtensionContentsStmt<'b>> {
        Ok(AlterExtensionContentsStmt {
            extname: copy_opt_str(&self.extname, mcx)?,
            action: self.action,
            objtype: self.objtype,
            object: copy_opt_node(&self.object, mcx)?,
        })
    }
}

/// `AlterFdwStmt` (`nodes/parsenodes.h`) — ALTER FOREIGN DATA WRAPPER.
#[derive(Debug)]
pub struct AlterFdwStmt<'mcx> {
    pub fdwname: Option<PgString<'mcx>>,
    pub func_options: PgVec<'mcx, NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterFdwStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterFdwStmt<'b>> {
        Ok(AlterFdwStmt {
            fdwname: copy_opt_str(&self.fdwname, mcx)?,
            func_options: copy_node_vec(&self.func_options, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `AlterForeignServerStmt` (`nodes/parsenodes.h`) — ALTER SERVER.
#[derive(Debug)]
pub struct AlterForeignServerStmt<'mcx> {
    pub servername: Option<PgString<'mcx>>,
    pub version: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub has_version: bool,
}

impl AlterForeignServerStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterForeignServerStmt<'b>> {
        Ok(AlterForeignServerStmt {
            servername: copy_opt_str(&self.servername, mcx)?,
            version: copy_opt_str(&self.version, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            has_version: self.has_version,
        })
    }
}

/// `AlterUserMappingStmt` (`nodes/parsenodes.h`) — ALTER USER MAPPING.
#[derive(Debug)]
pub struct AlterUserMappingStmt<'mcx> {
    /// `RoleSpec *user`.
    pub user: Option<NodePtr<'mcx>>,
    pub servername: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterUserMappingStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterUserMappingStmt<'b>> {
        Ok(AlterUserMappingStmt {
            user: copy_opt_node(&self.user, mcx)?,
            servername: copy_opt_str(&self.servername, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `AlterPolicyStmt` (`nodes/parsenodes.h`) — ALTER POLICY.
#[derive(Debug)]
pub struct AlterPolicyStmt<'mcx> {
    pub policy_name: Option<PgString<'mcx>>,
    /// `RangeVar *table`.
    pub table: Option<NodePtr<'mcx>>,
    pub roles: PgVec<'mcx, NodePtr<'mcx>>,
    pub qual: Option<NodePtr<'mcx>>,
    pub with_check: Option<NodePtr<'mcx>>,
}

impl AlterPolicyStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterPolicyStmt<'b>> {
        Ok(AlterPolicyStmt {
            policy_name: copy_opt_str(&self.policy_name, mcx)?,
            table: copy_opt_node(&self.table, mcx)?,
            roles: copy_node_vec(&self.roles, mcx)?,
            qual: copy_opt_node(&self.qual, mcx)?,
            with_check: copy_opt_node(&self.with_check, mcx)?,
        })
    }
}

/// `AlterDatabaseStmt` (`nodes/parsenodes.h`) — ALTER DATABASE.
#[derive(Debug)]
pub struct AlterDatabaseStmt<'mcx> {
    pub dbname: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterDatabaseStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterDatabaseStmt<'b>> {
        Ok(AlterDatabaseStmt {
            dbname: copy_opt_str(&self.dbname, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `AlterDatabaseRefreshCollStmt` (`nodes/parsenodes.h`) — ALTER DATABASE ...
/// REFRESH COLLATION VERSION.
#[derive(Debug)]
pub struct AlterDatabaseRefreshCollStmt<'mcx> {
    pub dbname: Option<PgString<'mcx>>,
}

impl AlterDatabaseRefreshCollStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterDatabaseRefreshCollStmt<'b>> {
        Ok(AlterDatabaseRefreshCollStmt {
            dbname: copy_opt_str(&self.dbname, mcx)?,
        })
    }
}

/// `AlterDatabaseSetStmt` (`nodes/parsenodes.h`) — ALTER DATABASE ... SET. The
/// `setstmt` is a `VariableSetStmt` (SET family, F4) carried as a `Node`.
#[derive(Debug)]
pub struct AlterDatabaseSetStmt<'mcx> {
    pub dbname: Option<PgString<'mcx>>,
    /// `VariableSetStmt *setstmt`.
    pub setstmt: Option<NodePtr<'mcx>>,
}

impl AlterDatabaseSetStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterDatabaseSetStmt<'b>> {
        Ok(AlterDatabaseSetStmt {
            dbname: copy_opt_str(&self.dbname, mcx)?,
            setstmt: copy_opt_node(&self.setstmt, mcx)?,
        })
    }
}

/// `AlterTSDictionaryStmt` (`nodes/parsenodes.h`) — ALTER TEXT SEARCH
/// DICTIONARY.
#[derive(Debug)]
pub struct AlterTSDictionaryStmt<'mcx> {
    pub dictname: PgVec<'mcx, NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterTSDictionaryStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterTSDictionaryStmt<'b>> {
        Ok(AlterTSDictionaryStmt {
            dictname: copy_node_vec(&self.dictname, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `AlterTSConfigurationStmt` (`nodes/parsenodes.h`) — ALTER TEXT SEARCH
/// CONFIGURATION.
#[derive(Debug)]
pub struct AlterTSConfigurationStmt<'mcx> {
    pub kind: AlterTSConfigType,
    pub cfgname: PgVec<'mcx, NodePtr<'mcx>>,
    pub tokentype: PgVec<'mcx, NodePtr<'mcx>>,
    pub dicts: PgVec<'mcx, NodePtr<'mcx>>,
    pub override_: bool,
    pub replace: bool,
    pub missing_ok: bool,
}

impl AlterTSConfigurationStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterTSConfigurationStmt<'b>> {
        Ok(AlterTSConfigurationStmt {
            kind: self.kind,
            cfgname: copy_node_vec(&self.cfgname, mcx)?,
            tokentype: copy_node_vec(&self.tokentype, mcx)?,
            dicts: copy_node_vec(&self.dicts, mcx)?,
            override_: self.override_,
            replace: self.replace,
            missing_ok: self.missing_ok,
        })
    }
}

/// `AlterPublicationStmt` (`nodes/parsenodes.h`) — ALTER PUBLICATION.
#[derive(Debug)]
pub struct AlterPublicationStmt<'mcx> {
    pub pubname: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub pubobjects: PgVec<'mcx, NodePtr<'mcx>>,
    pub for_all_tables: bool,
    pub action: AlterPublicationAction,
}

impl AlterPublicationStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterPublicationStmt<'b>> {
        Ok(AlterPublicationStmt {
            pubname: copy_opt_str(&self.pubname, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            pubobjects: copy_node_vec(&self.pubobjects, mcx)?,
            for_all_tables: self.for_all_tables,
            action: self.action,
        })
    }
}

/// `AlterSubscriptionStmt` (`nodes/parsenodes.h`) — ALTER SUBSCRIPTION.
#[derive(Debug)]
pub struct AlterSubscriptionStmt<'mcx> {
    pub kind: AlterSubscriptionType,
    pub subname: Option<PgString<'mcx>>,
    pub conninfo: Option<PgString<'mcx>>,
    pub publication: PgVec<'mcx, NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}

impl AlterSubscriptionStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterSubscriptionStmt<'b>> {
        Ok(AlterSubscriptionStmt {
            kind: self.kind,
            subname: copy_opt_str(&self.subname, mcx)?,
            conninfo: copy_opt_str(&self.conninfo, mcx)?,
            publication: copy_node_vec(&self.publication, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

// ===========================================================================
// Utility / GRANT / transaction family (parser grammar F4)
//
// The node vocabulary the bison grammar's utility rule actions build: GRANT/
// REVOKE, SET/SHOW/RESET, transaction control, COPY, EXPLAIN, the prepared-
// statement and cursor commands, maintenance (VACUUM/ANALYZE/CLUSTER/REINDEX/
// CHECKPOINT/...), object commands (COMMENT/SECURITY LABEL/RULE/...), the
// LISTEN/NOTIFY family, and the remaining CREATE/ALTER/DROP utility statements
// (FDW/foreign server/table, user mapping, policy, publication, subscription,
// event trigger, transform, role/db/tablespace drops).
//
// Authored field-for-field against `nodes/parsenodes.h`, same modelling rules
// as the rest of this module.
// ===========================================================================

use crate::nodes::CmdType;

/// `GrantTargetType` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum GrantTargetType {
    #[default]
    ACL_TARGET_OBJECT = 0,
    ACL_TARGET_ALL_IN_SCHEMA = 1,
    ACL_TARGET_DEFAULTS = 2,
}
pub use GrantTargetType::{ACL_TARGET_ALL_IN_SCHEMA, ACL_TARGET_DEFAULTS, ACL_TARGET_OBJECT};

/// `VariableSetKind` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum VariableSetKind {
    #[default]
    VAR_SET_VALUE = 0,
    VAR_SET_DEFAULT = 1,
    VAR_SET_CURRENT = 2,
    VAR_SET_MULTI = 3,
    VAR_RESET = 4,
    VAR_RESET_ALL = 5,
}
pub use VariableSetKind::{
    VAR_RESET, VAR_RESET_ALL, VAR_SET_CURRENT, VAR_SET_DEFAULT, VAR_SET_MULTI, VAR_SET_VALUE,
};

/// `TransactionStmtKind` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum TransactionStmtKind {
    #[default]
    TRANS_STMT_BEGIN = 0,
    TRANS_STMT_START = 1,
    TRANS_STMT_COMMIT = 2,
    TRANS_STMT_ROLLBACK = 3,
    TRANS_STMT_SAVEPOINT = 4,
    TRANS_STMT_RELEASE = 5,
    TRANS_STMT_ROLLBACK_TO = 6,
    TRANS_STMT_PREPARE = 7,
    TRANS_STMT_COMMIT_PREPARED = 8,
    TRANS_STMT_ROLLBACK_PREPARED = 9,
}
pub use TransactionStmtKind::{
    TRANS_STMT_BEGIN, TRANS_STMT_COMMIT, TRANS_STMT_COMMIT_PREPARED, TRANS_STMT_PREPARE,
    TRANS_STMT_RELEASE, TRANS_STMT_ROLLBACK, TRANS_STMT_ROLLBACK_PREPARED, TRANS_STMT_ROLLBACK_TO,
    TRANS_STMT_SAVEPOINT, TRANS_STMT_START,
};

/// `DiscardMode` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum DiscardMode {
    #[default]
    DISCARD_ALL = 0,
    DISCARD_PLANS = 1,
    DISCARD_SEQUENCES = 2,
    DISCARD_TEMP = 3,
}
pub use DiscardMode::{DISCARD_ALL, DISCARD_PLANS, DISCARD_SEQUENCES, DISCARD_TEMP};

/// `ReindexObjectType` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ReindexObjectType {
    #[default]
    REINDEX_OBJECT_INDEX = 0,
    REINDEX_OBJECT_TABLE = 1,
    REINDEX_OBJECT_SCHEMA = 2,
    REINDEX_OBJECT_SYSTEM = 3,
    REINDEX_OBJECT_DATABASE = 4,
}
pub use ReindexObjectType::{
    REINDEX_OBJECT_DATABASE, REINDEX_OBJECT_INDEX, REINDEX_OBJECT_SCHEMA, REINDEX_OBJECT_SYSTEM,
    REINDEX_OBJECT_TABLE,
};

/// `ImportForeignSchemaType` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ImportForeignSchemaType {
    #[default]
    FDW_IMPORT_SCHEMA_ALL = 0,
    FDW_IMPORT_SCHEMA_LIMIT_TO = 1,
    FDW_IMPORT_SCHEMA_EXCEPT = 2,
}
pub use ImportForeignSchemaType::{
    FDW_IMPORT_SCHEMA_ALL, FDW_IMPORT_SCHEMA_EXCEPT, FDW_IMPORT_SCHEMA_LIMIT_TO,
};

/// `PublicationObjSpecType` (`nodes/parsenodes.h`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum PublicationObjSpecType {
    #[default]
    PUBLICATIONOBJ_TABLE = 0,
    PUBLICATIONOBJ_TABLES_IN_SCHEMA = 1,
    PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA = 2,
    PUBLICATIONOBJ_CONTINUATION = 3,
}
pub use PublicationObjSpecType::{
    PUBLICATIONOBJ_CONTINUATION, PUBLICATIONOBJ_TABLE, PUBLICATIONOBJ_TABLES_IN_CUR_SCHEMA,
    PUBLICATIONOBJ_TABLES_IN_SCHEMA,
};

/// `FetchDirection` (`nodes/parsenodes.h`) — raw-grammar producer view.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum FetchDirection {
    #[default]
    FETCH_FORWARD = 0,
    FETCH_BACKWARD = 1,
    FETCH_ABSOLUTE = 2,
    FETCH_RELATIVE = 3,
}
pub use FetchDirection::{FETCH_ABSOLUTE, FETCH_BACKWARD, FETCH_FORWARD, FETCH_RELATIVE};

/// `GrantStmt` (`nodes/parsenodes.h`) — GRANT / REVOKE.
#[derive(Debug)]
pub struct GrantStmt<'mcx> {
    pub is_grant: bool,
    pub targtype: GrantTargetType,
    pub objtype: ObjectType,
    pub objects: PgVec<'mcx, NodePtr<'mcx>>,
    pub privileges: PgVec<'mcx, NodePtr<'mcx>>,
    pub grantees: PgVec<'mcx, NodePtr<'mcx>>,
    pub grant_option: bool,
    pub grantor: Option<NodePtr<'mcx>>,
    pub behavior: DropBehavior,
}
impl GrantStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<GrantStmt<'b>> {
        Ok(GrantStmt {
            is_grant: self.is_grant,
            targtype: self.targtype,
            objtype: self.objtype,
            objects: copy_node_vec(&self.objects, mcx)?,
            privileges: copy_node_vec(&self.privileges, mcx)?,
            grantees: copy_node_vec(&self.grantees, mcx)?,
            grant_option: self.grant_option,
            grantor: copy_opt_node(&self.grantor, mcx)?,
            behavior: self.behavior,
        })
    }
}

/// `GrantRoleStmt` (`nodes/parsenodes.h`) — GRANT / REVOKE role membership.
#[derive(Debug)]
pub struct GrantRoleStmt<'mcx> {
    pub granted_roles: PgVec<'mcx, NodePtr<'mcx>>,
    pub grantee_roles: PgVec<'mcx, NodePtr<'mcx>>,
    pub is_grant: bool,
    pub opt: PgVec<'mcx, NodePtr<'mcx>>,
    pub grantor: Option<NodePtr<'mcx>>,
    pub behavior: DropBehavior,
}
impl GrantRoleStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<GrantRoleStmt<'b>> {
        Ok(GrantRoleStmt {
            granted_roles: copy_node_vec(&self.granted_roles, mcx)?,
            grantee_roles: copy_node_vec(&self.grantee_roles, mcx)?,
            is_grant: self.is_grant,
            opt: copy_node_vec(&self.opt, mcx)?,
            grantor: copy_opt_node(&self.grantor, mcx)?,
            behavior: self.behavior,
        })
    }
}

/// `VariableSetStmt` (`nodes/parsenodes.h`) — SET / RESET.
#[derive(Debug)]
pub struct VariableSetStmt<'mcx> {
    pub kind: VariableSetKind,
    pub name: Option<PgString<'mcx>>,
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
    pub jumble_args: bool,
    pub is_local: bool,
    pub location: ParseLoc,
}
impl VariableSetStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<VariableSetStmt<'b>> {
        Ok(VariableSetStmt {
            kind: self.kind,
            name: copy_opt_str(&self.name, mcx)?,
            args: copy_node_vec(&self.args, mcx)?,
            jumble_args: self.jumble_args,
            is_local: self.is_local,
            location: self.location,
        })
    }
}

/// `VariableShowStmt` (`nodes/parsenodes.h`) — SHOW.
#[derive(Debug)]
pub struct VariableShowStmt<'mcx> {
    pub name: Option<PgString<'mcx>>,
}
impl VariableShowStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<VariableShowStmt<'b>> {
        Ok(VariableShowStmt { name: copy_opt_str(&self.name, mcx)? })
    }
}

/// `TransactionStmt` (`nodes/parsenodes.h`).
#[derive(Debug)]
pub struct TransactionStmt<'mcx> {
    pub kind: TransactionStmtKind,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub savepoint_name: Option<PgString<'mcx>>,
    pub gid: Option<PgString<'mcx>>,
    pub chain: bool,
    pub location: ParseLoc,
}
impl TransactionStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TransactionStmt<'b>> {
        Ok(TransactionStmt {
            kind: self.kind,
            options: copy_node_vec(&self.options, mcx)?,
            savepoint_name: copy_opt_str(&self.savepoint_name, mcx)?,
            gid: copy_opt_str(&self.gid, mcx)?,
            chain: self.chain,
            location: self.location,
        })
    }
}

/// `CopyStmt` (`nodes/parsenodes.h`) — COPY.
#[derive(Debug)]
pub struct CopyStmt<'mcx> {
    pub relation: Option<NodePtr<'mcx>>,
    pub query: Option<NodePtr<'mcx>>,
    pub attlist: PgVec<'mcx, NodePtr<'mcx>>,
    pub is_from: bool,
    pub is_program: bool,
    pub filename: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub where_clause: Option<NodePtr<'mcx>>,
}
impl CopyStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CopyStmt<'b>> {
        Ok(CopyStmt {
            relation: copy_opt_node(&self.relation, mcx)?,
            query: copy_opt_node(&self.query, mcx)?,
            attlist: copy_node_vec(&self.attlist, mcx)?,
            is_from: self.is_from,
            is_program: self.is_program,
            filename: copy_opt_str(&self.filename, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            where_clause: copy_opt_node(&self.where_clause, mcx)?,
        })
    }
}

/// `ExplainStmt` (`nodes/parsenodes.h`) — EXPLAIN.
#[derive(Debug)]
pub struct ExplainStmt<'mcx> {
    pub query: Option<NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}
impl ExplainStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ExplainStmt<'b>> {
        Ok(ExplainStmt {
            query: copy_opt_node(&self.query, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `PrepareStmt` (`nodes/parsenodes.h`) — PREPARE (raw-grammar producer).
#[derive(Debug)]
pub struct PrepareStmt<'mcx> {
    pub name: Option<PgString<'mcx>>,
    pub argtypes: PgVec<'mcx, NodePtr<'mcx>>,
    pub query: Option<NodePtr<'mcx>>,
}
impl PrepareStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PrepareStmt<'b>> {
        Ok(PrepareStmt {
            name: copy_opt_str(&self.name, mcx)?,
            argtypes: copy_node_vec(&self.argtypes, mcx)?,
            query: copy_opt_node(&self.query, mcx)?,
        })
    }
}

/// `ExecuteStmt` (`nodes/parsenodes.h`) — EXECUTE (raw-grammar producer).
#[derive(Debug)]
pub struct ExecuteStmt<'mcx> {
    pub name: Option<PgString<'mcx>>,
    pub params: PgVec<'mcx, NodePtr<'mcx>>,
}
impl ExecuteStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ExecuteStmt<'b>> {
        Ok(ExecuteStmt {
            name: copy_opt_str(&self.name, mcx)?,
            params: copy_node_vec(&self.params, mcx)?,
        })
    }
}

/// `DeallocateStmt` (`nodes/parsenodes.h`) — DEALLOCATE (raw-grammar producer).
#[derive(Debug)]
pub struct DeallocateStmt<'mcx> {
    pub name: Option<PgString<'mcx>>,
    pub isall: bool,
    pub location: ParseLoc,
}
impl DeallocateStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DeallocateStmt<'b>> {
        Ok(DeallocateStmt {
            name: copy_opt_str(&self.name, mcx)?,
            isall: self.isall,
            location: self.location,
        })
    }
}

/// `DeclareCursorStmt` (`nodes/parsenodes.h`) — DECLARE CURSOR (raw producer).
#[derive(Debug)]
pub struct DeclareCursorStmt<'mcx> {
    pub portalname: Option<PgString<'mcx>>,
    pub options: i32,
    pub query: Option<NodePtr<'mcx>>,
}
impl DeclareCursorStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DeclareCursorStmt<'b>> {
        Ok(DeclareCursorStmt {
            portalname: copy_opt_str(&self.portalname, mcx)?,
            options: self.options,
            query: copy_opt_node(&self.query, mcx)?,
        })
    }
}

/// `ClosePortalStmt` (`nodes/parsenodes.h`) — CLOSE.
#[derive(Debug)]
pub struct ClosePortalStmt<'mcx> {
    pub portalname: Option<PgString<'mcx>>,
}
impl ClosePortalStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ClosePortalStmt<'b>> {
        Ok(ClosePortalStmt { portalname: copy_opt_str(&self.portalname, mcx)? })
    }
}

/// `FetchStmt` (`nodes/parsenodes.h`) — FETCH / MOVE (raw-grammar producer).
#[derive(Debug)]
pub struct FetchStmt<'mcx> {
    pub direction: FetchDirection,
    pub how_many: i64,
    pub portalname: Option<PgString<'mcx>>,
    pub ismove: bool,
}
impl FetchStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<FetchStmt<'b>> {
        Ok(FetchStmt {
            direction: self.direction,
            how_many: self.how_many,
            portalname: copy_opt_str(&self.portalname, mcx)?,
            ismove: self.ismove,
        })
    }
}

/// `VacuumStmt` (`nodes/parsenodes.h`) — VACUUM / ANALYZE.
#[derive(Debug)]
pub struct VacuumStmt<'mcx> {
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub rels: PgVec<'mcx, NodePtr<'mcx>>,
    pub is_vacuumcmd: bool,
}
impl VacuumStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<VacuumStmt<'b>> {
        Ok(VacuumStmt {
            options: copy_node_vec(&self.options, mcx)?,
            rels: copy_node_vec(&self.rels, mcx)?,
            is_vacuumcmd: self.is_vacuumcmd,
        })
    }
}

/// `VacuumRelation` (`nodes/parsenodes.h`).
#[derive(Debug)]
pub struct VacuumRelation<'mcx> {
    pub relation: Option<NodePtr<'mcx>>,
    pub oid: Oid,
    pub va_cols: PgVec<'mcx, NodePtr<'mcx>>,
}
impl VacuumRelation<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<VacuumRelation<'b>> {
        Ok(VacuumRelation {
            relation: copy_opt_node(&self.relation, mcx)?,
            oid: self.oid,
            va_cols: copy_node_vec(&self.va_cols, mcx)?,
        })
    }
}

/// `ClusterStmt` (`nodes/parsenodes.h`) — CLUSTER.
#[derive(Debug)]
pub struct ClusterStmt<'mcx> {
    pub relation: Option<NodePtr<'mcx>>,
    pub indexname: Option<PgString<'mcx>>,
    pub params: PgVec<'mcx, NodePtr<'mcx>>,
}
impl ClusterStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ClusterStmt<'b>> {
        Ok(ClusterStmt {
            relation: copy_opt_node(&self.relation, mcx)?,
            indexname: copy_opt_str(&self.indexname, mcx)?,
            params: copy_node_vec(&self.params, mcx)?,
        })
    }
}

/// `ReindexStmt` (`nodes/parsenodes.h`) — REINDEX.
#[derive(Debug)]
pub struct ReindexStmt<'mcx> {
    pub kind: ReindexObjectType,
    pub relation: Option<NodePtr<'mcx>>,
    pub name: Option<PgString<'mcx>>,
    pub params: PgVec<'mcx, NodePtr<'mcx>>,
}
impl ReindexStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ReindexStmt<'b>> {
        Ok(ReindexStmt {
            kind: self.kind,
            relation: copy_opt_node(&self.relation, mcx)?,
            name: copy_opt_str(&self.name, mcx)?,
            params: copy_node_vec(&self.params, mcx)?,
        })
    }
}

/// `CheckPointStmt` (`nodes/parsenodes.h`) — CHECKPOINT.
#[derive(Clone, Copy, Debug, Default)]
pub struct CheckPointStmt;
impl CheckPointStmt {
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<CheckPointStmt> { Ok(CheckPointStmt) }
}

/// `DiscardStmt` (`nodes/parsenodes.h`) — DISCARD.
#[derive(Clone, Copy, Debug)]
pub struct DiscardStmt {
    pub target: DiscardMode,
}
impl DiscardStmt {
    pub fn clone_in<'b>(&self, _mcx: Mcx<'b>) -> PgResult<DiscardStmt> {
        Ok(DiscardStmt { target: self.target })
    }
}

/// `LockStmt` (`nodes/parsenodes.h`) — LOCK TABLE.
#[derive(Debug)]
pub struct LockStmt<'mcx> {
    pub relations: PgVec<'mcx, NodePtr<'mcx>>,
    pub mode: i32,
    pub nowait: bool,
}
impl LockStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<LockStmt<'b>> {
        Ok(LockStmt {
            relations: copy_node_vec(&self.relations, mcx)?,
            mode: self.mode,
            nowait: self.nowait,
        })
    }
}

/// `ConstraintsSetStmt` (`nodes/parsenodes.h`) — SET CONSTRAINTS.
#[derive(Debug)]
pub struct ConstraintsSetStmt<'mcx> {
    pub constraints: PgVec<'mcx, NodePtr<'mcx>>,
    pub deferred: bool,
}
impl ConstraintsSetStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ConstraintsSetStmt<'b>> {
        Ok(ConstraintsSetStmt {
            constraints: copy_node_vec(&self.constraints, mcx)?,
            deferred: self.deferred,
        })
    }
}

/// `LoadStmt` (`nodes/parsenodes.h`) — LOAD.
#[derive(Debug)]
pub struct LoadStmt<'mcx> {
    pub filename: Option<PgString<'mcx>>,
}
impl LoadStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<LoadStmt<'b>> {
        Ok(LoadStmt { filename: copy_opt_str(&self.filename, mcx)? })
    }
}

/// `TruncateStmt` (`nodes/parsenodes.h`) — TRUNCATE.
#[derive(Debug)]
pub struct TruncateStmt<'mcx> {
    pub relations: PgVec<'mcx, NodePtr<'mcx>>,
    pub restart_seqs: bool,
    pub behavior: DropBehavior,
}
impl TruncateStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<TruncateStmt<'b>> {
        Ok(TruncateStmt {
            relations: copy_node_vec(&self.relations, mcx)?,
            restart_seqs: self.restart_seqs,
            behavior: self.behavior,
        })
    }
}

/// `CommentStmt` (`nodes/parsenodes.h`) — COMMENT ON.
#[derive(Debug)]
pub struct CommentStmt<'mcx> {
    pub objtype: ObjectType,
    pub object: Option<NodePtr<'mcx>>,
    pub comment: Option<PgString<'mcx>>,
}
impl CommentStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CommentStmt<'b>> {
        Ok(CommentStmt {
            objtype: self.objtype,
            object: copy_opt_node(&self.object, mcx)?,
            comment: copy_opt_str(&self.comment, mcx)?,
        })
    }
}

/// `SecLabelStmt` (`nodes/parsenodes.h`) — SECURITY LABEL.
#[derive(Debug)]
pub struct SecLabelStmt<'mcx> {
    pub objtype: ObjectType,
    pub object: Option<NodePtr<'mcx>>,
    pub provider: Option<PgString<'mcx>>,
    pub label: Option<PgString<'mcx>>,
}
impl SecLabelStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<SecLabelStmt<'b>> {
        Ok(SecLabelStmt {
            objtype: self.objtype,
            object: copy_opt_node(&self.object, mcx)?,
            provider: copy_opt_str(&self.provider, mcx)?,
            label: copy_opt_str(&self.label, mcx)?,
        })
    }
}

/// `RuleStmt` (`nodes/parsenodes.h`) — CREATE RULE.
#[derive(Debug)]
pub struct RuleStmt<'mcx> {
    pub relation: Option<NodePtr<'mcx>>,
    pub rulename: Option<PgString<'mcx>>,
    pub where_clause: Option<NodePtr<'mcx>>,
    pub event: CmdType,
    pub instead: bool,
    pub actions: PgVec<'mcx, NodePtr<'mcx>>,
    pub replace: bool,
}
impl RuleStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RuleStmt<'b>> {
        Ok(RuleStmt {
            relation: copy_opt_node(&self.relation, mcx)?,
            rulename: copy_opt_str(&self.rulename, mcx)?,
            where_clause: copy_opt_node(&self.where_clause, mcx)?,
            event: self.event,
            instead: self.instead,
            actions: copy_node_vec(&self.actions, mcx)?,
            replace: self.replace,
        })
    }
}

/// `NotifyStmt` (`nodes/parsenodes.h`) — NOTIFY.
#[derive(Debug)]
pub struct NotifyStmt<'mcx> {
    pub conditionname: Option<PgString<'mcx>>,
    pub payload: Option<PgString<'mcx>>,
}
impl NotifyStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<NotifyStmt<'b>> {
        Ok(NotifyStmt {
            conditionname: copy_opt_str(&self.conditionname, mcx)?,
            payload: copy_opt_str(&self.payload, mcx)?,
        })
    }
}

/// `ListenStmt` (`nodes/parsenodes.h`) — LISTEN.
#[derive(Debug)]
pub struct ListenStmt<'mcx> {
    pub conditionname: Option<PgString<'mcx>>,
}
impl ListenStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ListenStmt<'b>> {
        Ok(ListenStmt { conditionname: copy_opt_str(&self.conditionname, mcx)? })
    }
}

/// `UnlistenStmt` (`nodes/parsenodes.h`) — UNLISTEN.
#[derive(Debug)]
pub struct UnlistenStmt<'mcx> {
    pub conditionname: Option<PgString<'mcx>>,
}
impl UnlistenStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<UnlistenStmt<'b>> {
        Ok(UnlistenStmt { conditionname: copy_opt_str(&self.conditionname, mcx)? })
    }
}

/// `DoStmt` (`nodes/parsenodes.h`) — DO.
#[derive(Debug)]
pub struct DoStmt<'mcx> {
    pub args: PgVec<'mcx, NodePtr<'mcx>>,
}
impl DoStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DoStmt<'b>> {
        Ok(DoStmt { args: copy_node_vec(&self.args, mcx)? })
    }
}

/// `CallStmt` (`nodes/parsenodes.h`) — CALL.
#[derive(Debug)]
pub struct CallStmt<'mcx> {
    pub funccall: Option<NodePtr<'mcx>>,
    pub funcexpr: Option<NodePtr<'mcx>>,
    pub outargs: PgVec<'mcx, NodePtr<'mcx>>,
}
impl CallStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CallStmt<'b>> {
        Ok(CallStmt {
            funccall: copy_opt_node(&self.funccall, mcx)?,
            funcexpr: copy_opt_node(&self.funcexpr, mcx)?,
            outargs: copy_node_vec(&self.outargs, mcx)?,
        })
    }
}

/// `RefreshMatViewStmt` (`nodes/parsenodes.h`) — REFRESH MATERIALIZED VIEW.
#[derive(Debug)]
pub struct RefreshMatViewStmt<'mcx> {
    pub concurrent: bool,
    pub skip_data: bool,
    pub relation: Option<NodePtr<'mcx>>,
}
impl RefreshMatViewStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<RefreshMatViewStmt<'b>> {
        Ok(RefreshMatViewStmt {
            concurrent: self.concurrent,
            skip_data: self.skip_data,
            relation: copy_opt_node(&self.relation, mcx)?,
        })
    }
}

/// `AlterSystemStmt` (`nodes/parsenodes.h`) — ALTER SYSTEM.
#[derive(Debug)]
pub struct AlterSystemStmt<'mcx> {
    pub setstmt: Option<NodePtr<'mcx>>,
}
impl AlterSystemStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterSystemStmt<'b>> {
        Ok(AlterSystemStmt { setstmt: copy_opt_node(&self.setstmt, mcx)? })
    }
}

/// `DropdbStmt` (`nodes/parsenodes.h`) — DROP DATABASE.
#[derive(Debug)]
pub struct DropdbStmt<'mcx> {
    pub dbname: Option<PgString<'mcx>>,
    pub missing_ok: bool,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}
impl DropdbStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DropdbStmt<'b>> {
        Ok(DropdbStmt {
            dbname: copy_opt_str(&self.dbname, mcx)?,
            missing_ok: self.missing_ok,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `DropRoleStmt` (`nodes/parsenodes.h`) — DROP ROLE/USER/GROUP.
#[derive(Debug)]
pub struct DropRoleStmt<'mcx> {
    pub roles: PgVec<'mcx, NodePtr<'mcx>>,
    pub missing_ok: bool,
}
impl DropRoleStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DropRoleStmt<'b>> {
        Ok(DropRoleStmt {
            roles: copy_node_vec(&self.roles, mcx)?,
            missing_ok: self.missing_ok,
        })
    }
}

/// `DropTableSpaceStmt` (`nodes/parsenodes.h`) — DROP TABLESPACE.
#[derive(Debug)]
pub struct DropTableSpaceStmt<'mcx> {
    pub tablespacename: Option<PgString<'mcx>>,
    pub missing_ok: bool,
}
impl DropTableSpaceStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DropTableSpaceStmt<'b>> {
        Ok(DropTableSpaceStmt {
            tablespacename: copy_opt_str(&self.tablespacename, mcx)?,
            missing_ok: self.missing_ok,
        })
    }
}

/// `CreateFdwStmt` (`nodes/parsenodes.h`) — CREATE FOREIGN DATA WRAPPER.
#[derive(Debug)]
pub struct CreateFdwStmt<'mcx> {
    pub fdwname: Option<PgString<'mcx>>,
    pub func_options: PgVec<'mcx, NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}
impl CreateFdwStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateFdwStmt<'b>> {
        Ok(CreateFdwStmt {
            fdwname: copy_opt_str(&self.fdwname, mcx)?,
            func_options: copy_node_vec(&self.func_options, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `CreateForeignServerStmt` (`nodes/parsenodes.h`) — CREATE SERVER.
#[derive(Debug)]
pub struct CreateForeignServerStmt<'mcx> {
    pub servername: Option<PgString<'mcx>>,
    pub servertype: Option<PgString<'mcx>>,
    pub version: Option<PgString<'mcx>>,
    pub fdwname: Option<PgString<'mcx>>,
    pub if_not_exists: bool,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}
impl CreateForeignServerStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateForeignServerStmt<'b>> {
        Ok(CreateForeignServerStmt {
            servername: copy_opt_str(&self.servername, mcx)?,
            servertype: copy_opt_str(&self.servertype, mcx)?,
            version: copy_opt_str(&self.version, mcx)?,
            fdwname: copy_opt_str(&self.fdwname, mcx)?,
            if_not_exists: self.if_not_exists,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `CreateForeignTableStmt` (`nodes/parsenodes.h`) — CREATE FOREIGN TABLE.
#[derive(Debug)]
pub struct CreateForeignTableStmt<'mcx> {
    pub base: mcx::PgBox<'mcx, CreateStmt<'mcx>>,
    pub servername: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}
impl CreateForeignTableStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateForeignTableStmt<'b>> {
        Ok(CreateForeignTableStmt {
            base: mcx::alloc_in(mcx, self.base.clone_in(mcx)?)?,
            servername: copy_opt_str(&self.servername, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `CreateUserMappingStmt` (`nodes/parsenodes.h`) — CREATE USER MAPPING.
#[derive(Debug)]
pub struct CreateUserMappingStmt<'mcx> {
    pub user: Option<NodePtr<'mcx>>,
    pub servername: Option<PgString<'mcx>>,
    pub if_not_exists: bool,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}
impl CreateUserMappingStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateUserMappingStmt<'b>> {
        Ok(CreateUserMappingStmt {
            user: copy_opt_node(&self.user, mcx)?,
            servername: copy_opt_str(&self.servername, mcx)?,
            if_not_exists: self.if_not_exists,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `DropUserMappingStmt` (`nodes/parsenodes.h`) — DROP USER MAPPING.
#[derive(Debug)]
pub struct DropUserMappingStmt<'mcx> {
    pub user: Option<NodePtr<'mcx>>,
    pub servername: Option<PgString<'mcx>>,
    pub missing_ok: bool,
}
impl DropUserMappingStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DropUserMappingStmt<'b>> {
        Ok(DropUserMappingStmt {
            user: copy_opt_node(&self.user, mcx)?,
            servername: copy_opt_str(&self.servername, mcx)?,
            missing_ok: self.missing_ok,
        })
    }
}

/// `ImportForeignSchemaStmt` (`nodes/parsenodes.h`) — IMPORT FOREIGN SCHEMA.
#[derive(Debug)]
pub struct ImportForeignSchemaStmt<'mcx> {
    pub server_name: Option<PgString<'mcx>>,
    pub remote_schema: Option<PgString<'mcx>>,
    pub local_schema: Option<PgString<'mcx>>,
    pub list_type: ImportForeignSchemaType,
    pub table_list: PgVec<'mcx, NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}
impl ImportForeignSchemaStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ImportForeignSchemaStmt<'b>> {
        Ok(ImportForeignSchemaStmt {
            server_name: copy_opt_str(&self.server_name, mcx)?,
            remote_schema: copy_opt_str(&self.remote_schema, mcx)?,
            local_schema: copy_opt_str(&self.local_schema, mcx)?,
            list_type: self.list_type,
            table_list: copy_node_vec(&self.table_list, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `CreatePolicyStmt` (`nodes/parsenodes.h`) — CREATE POLICY.
#[derive(Debug)]
pub struct CreatePolicyStmt<'mcx> {
    pub policy_name: Option<PgString<'mcx>>,
    pub table: Option<NodePtr<'mcx>>,
    pub cmd_name: Option<PgString<'mcx>>,
    pub permissive: bool,
    pub roles: PgVec<'mcx, NodePtr<'mcx>>,
    pub qual: Option<NodePtr<'mcx>>,
    pub with_check: Option<NodePtr<'mcx>>,
}
impl CreatePolicyStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreatePolicyStmt<'b>> {
        Ok(CreatePolicyStmt {
            policy_name: copy_opt_str(&self.policy_name, mcx)?,
            table: copy_opt_node(&self.table, mcx)?,
            cmd_name: copy_opt_str(&self.cmd_name, mcx)?,
            permissive: self.permissive,
            roles: copy_node_vec(&self.roles, mcx)?,
            qual: copy_opt_node(&self.qual, mcx)?,
            with_check: copy_opt_node(&self.with_check, mcx)?,
        })
    }
}

/// `PublicationTable` (`nodes/parsenodes.h`).
#[derive(Debug)]
pub struct PublicationTable<'mcx> {
    pub relation: Option<NodePtr<'mcx>>,
    pub where_clause: Option<NodePtr<'mcx>>,
    pub columns: PgVec<'mcx, NodePtr<'mcx>>,
}
impl PublicationTable<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PublicationTable<'b>> {
        Ok(PublicationTable {
            relation: copy_opt_node(&self.relation, mcx)?,
            where_clause: copy_opt_node(&self.where_clause, mcx)?,
            columns: copy_node_vec(&self.columns, mcx)?,
        })
    }
}

/// `PublicationObjSpec` (`nodes/parsenodes.h`).
#[derive(Debug)]
pub struct PublicationObjSpec<'mcx> {
    pub pubobjtype: PublicationObjSpecType,
    pub name: Option<PgString<'mcx>>,
    pub pubtable: Option<mcx::PgBox<'mcx, PublicationTable<'mcx>>>,
    pub location: ParseLoc,
}
impl PublicationObjSpec<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PublicationObjSpec<'b>> {
        let pubtable = match &self.pubtable {
            Some(t) => Some(mcx::alloc_in(mcx, t.clone_in(mcx)?)?),
            None => None,
        };
        Ok(PublicationObjSpec {
            pubobjtype: self.pubobjtype,
            name: copy_opt_str(&self.name, mcx)?,
            pubtable,
            location: self.location,
        })
    }
}

/// `CreatePublicationStmt` (`nodes/parsenodes.h`) — CREATE PUBLICATION.
#[derive(Debug)]
pub struct CreatePublicationStmt<'mcx> {
    pub pubname: Option<PgString<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
    pub pubobjects: PgVec<'mcx, NodePtr<'mcx>>,
    pub for_all_tables: bool,
}
impl CreatePublicationStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreatePublicationStmt<'b>> {
        Ok(CreatePublicationStmt {
            pubname: copy_opt_str(&self.pubname, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
            pubobjects: copy_node_vec(&self.pubobjects, mcx)?,
            for_all_tables: self.for_all_tables,
        })
    }
}

/// `CreateSubscriptionStmt` (`nodes/parsenodes.h`) — CREATE SUBSCRIPTION.
#[derive(Debug)]
pub struct CreateSubscriptionStmt<'mcx> {
    pub subname: Option<PgString<'mcx>>,
    pub conninfo: Option<PgString<'mcx>>,
    pub publication: PgVec<'mcx, NodePtr<'mcx>>,
    pub options: PgVec<'mcx, NodePtr<'mcx>>,
}
impl CreateSubscriptionStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateSubscriptionStmt<'b>> {
        Ok(CreateSubscriptionStmt {
            subname: copy_opt_str(&self.subname, mcx)?,
            conninfo: copy_opt_str(&self.conninfo, mcx)?,
            publication: copy_node_vec(&self.publication, mcx)?,
            options: copy_node_vec(&self.options, mcx)?,
        })
    }
}

/// `DropSubscriptionStmt` (`nodes/parsenodes.h`) — DROP SUBSCRIPTION.
#[derive(Debug)]
pub struct DropSubscriptionStmt<'mcx> {
    pub subname: Option<PgString<'mcx>>,
    pub missing_ok: bool,
    pub behavior: DropBehavior,
}
impl DropSubscriptionStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<DropSubscriptionStmt<'b>> {
        Ok(DropSubscriptionStmt {
            subname: copy_opt_str(&self.subname, mcx)?,
            missing_ok: self.missing_ok,
            behavior: self.behavior,
        })
    }
}

/// `CreateEventTrigStmt` (`nodes/parsenodes.h`) — CREATE EVENT TRIGGER.
#[derive(Debug)]
pub struct CreateEventTrigStmt<'mcx> {
    pub trigname: Option<PgString<'mcx>>,
    pub eventname: Option<PgString<'mcx>>,
    pub whenclause: PgVec<'mcx, NodePtr<'mcx>>,
    pub funcname: PgVec<'mcx, NodePtr<'mcx>>,
}
impl CreateEventTrigStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateEventTrigStmt<'b>> {
        Ok(CreateEventTrigStmt {
            trigname: copy_opt_str(&self.trigname, mcx)?,
            eventname: copy_opt_str(&self.eventname, mcx)?,
            whenclause: copy_node_vec(&self.whenclause, mcx)?,
            funcname: copy_node_vec(&self.funcname, mcx)?,
        })
    }
}

/// `AlterEventTrigStmt` (`nodes/parsenodes.h`) — ALTER EVENT TRIGGER.
#[derive(Debug)]
pub struct AlterEventTrigStmt<'mcx> {
    pub trigname: Option<PgString<'mcx>>,
    pub tgenabled: i8,
}
impl AlterEventTrigStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<AlterEventTrigStmt<'b>> {
        Ok(AlterEventTrigStmt {
            trigname: copy_opt_str(&self.trigname, mcx)?,
            tgenabled: self.tgenabled,
        })
    }
}

/// `CreateTransformStmt` (`nodes/parsenodes.h`) — CREATE TRANSFORM.
#[derive(Debug)]
pub struct CreateTransformStmt<'mcx> {
    pub replace: bool,
    pub type_name: Option<NodePtr<'mcx>>,
    pub lang: Option<PgString<'mcx>>,
    pub fromsql: Option<NodePtr<'mcx>>,
    pub tosql: Option<NodePtr<'mcx>>,
}
impl CreateTransformStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<CreateTransformStmt<'b>> {
        Ok(CreateTransformStmt {
            replace: self.replace,
            type_name: copy_opt_node(&self.type_name, mcx)?,
            lang: copy_opt_str(&self.lang, mcx)?,
            fromsql: copy_opt_node(&self.fromsql, mcx)?,
            tosql: copy_opt_node(&self.tosql, mcx)?,
        })
    }
}

/// `ReturnStmt` (`nodes/parsenodes.h`) — RETURN inside a SQL function body.
#[derive(Debug)]
pub struct ReturnStmt<'mcx> {
    pub returnval: Option<NodePtr<'mcx>>,
}
impl ReturnStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<ReturnStmt<'b>> {
        Ok(ReturnStmt { returnval: copy_opt_node(&self.returnval, mcx)? })
    }
}

/// `PLAssignStmt` (`nodes/parsenodes.h`) — PL/pgSQL assignment (produced only
/// in the `RAW_PARSE_PLPGSQL_ASSIGN*` raw-parse modes).
#[derive(Debug)]
pub struct PLAssignStmt<'mcx> {
    pub name: Option<PgString<'mcx>>,
    pub indirection: PgVec<'mcx, NodePtr<'mcx>>,
    pub nnames: i32,
    /// `SelectStmt *val`.
    pub val: Option<NodePtr<'mcx>>,
    pub location: ParseLoc,
}
impl PLAssignStmt<'_> {
    pub fn clone_in<'b>(&self, mcx: Mcx<'b>) -> PgResult<PLAssignStmt<'b>> {
        Ok(PLAssignStmt {
            name: copy_opt_str(&self.name, mcx)?,
            indirection: copy_node_vec(&self.indirection, mcx)?,
            nnames: self.nnames,
            val: copy_opt_node(&self.val, mcx)?,
            location: self.location,
        })
    }
}
