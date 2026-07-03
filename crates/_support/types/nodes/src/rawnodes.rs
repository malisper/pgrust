// Raw-grammar input nodes; field names/order mirror vendor/parsenodes.h
// (tests: *_field_order_matches_c, enum_values_match_c_headers).
#![allow(non_camel_case_types, non_snake_case)]

use mcx::Mcx;
use types_core::{Oid, ParseLoc};
use types_error::PgResult;

use crate::list::NodeList;
use crate::node_tree::{BitString, Boolean, Float, Integer, Node, NodeVariant, String};
use crate::nodes_enums::LimitOption;
use crate::parsenodes::SetOperation;
use crate::tags::NodeTag;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum A_Expr_Kind {
    #[default]
    AEXPR_OP = 0,
    AEXPR_OP_ANY = 1,
    AEXPR_OP_ALL = 2,
    AEXPR_DISTINCT = 3,
    AEXPR_NOT_DISTINCT = 4,
    AEXPR_NULLIF = 5,
    AEXPR_IN = 6,
    AEXPR_LIKE = 7,
    AEXPR_ILIKE = 8,
    AEXPR_SIMILAR = 9,
    AEXPR_BETWEEN = 10,
    AEXPR_NOT_BETWEEN = 11,
    AEXPR_BETWEEN_SYM = 12,
    AEXPR_NOT_BETWEEN_SYM = 13,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SortByDir {
    #[default]
    SORTBY_DEFAULT = 0,
    SORTBY_ASC = 1,
    SORTBY_DESC = 2,
    SORTBY_USING = 3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SortByNulls {
    #[default]
    SORTBY_NULLS_DEFAULT = 0,
    SORTBY_NULLS_FIRST = 1,
    SORTBY_NULLS_LAST = 2,
}

// C divergence: C's SelectStmt.distinctClause encodes plain DISTINCT as
// list_make1(NIL) — a one-NULL-cell list — and DISTINCT ON as the expression
// list. NodeList cells are non-null, so the three states are explicit here.
#[derive(Default)]
pub enum DistinctClause<'mcx> {
    #[default]
    None,
    All,
    On(NodeList<'mcx>),
}

impl DistinctClause<'_> {
    #[inline]
    pub fn is_none(&self) -> bool {
        matches!(self, DistinctClause::None)
    }
}

#[derive(Clone, Copy, Default)]
pub struct RawStmt<'mcx> {
    pub stmt: Option<Node<'mcx>>,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

#[derive(Default)]
pub struct SelectStmt<'mcx> {
    pub distinctClause: DistinctClause<'mcx>,
    pub intoClause: Option<Node<'mcx>>,
    pub targetList: NodeList<'mcx>,
    pub fromClause: NodeList<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub groupClause: NodeList<'mcx>,
    pub groupDistinct: bool,
    pub havingClause: Option<Node<'mcx>>,
    pub windowClause: NodeList<'mcx>,
    pub valuesLists: NodeList<'mcx>,
    pub sortClause: NodeList<'mcx>,
    pub limitOffset: Option<Node<'mcx>>,
    pub limitCount: Option<Node<'mcx>>,
    pub limitOption: LimitOption,
    pub lockingClause: NodeList<'mcx>,
    pub withClause: Option<Node<'mcx>>,
    pub op: SetOperation,
    pub all: bool,
    pub larg: Option<&'mcx SelectStmt<'mcx>>,
    pub rarg: Option<&'mcx SelectStmt<'mcx>>,
}

/// `relation` is a RangeVar node handle (the grammar scribbles its alias).
#[derive(Default)]
pub struct InsertStmt<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub cols: NodeList<'mcx>,
    pub selectStmt: Option<Node<'mcx>>,
    pub onConflictClause: Option<Node<'mcx>>,
    pub returningClause: Option<Node<'mcx>>,
    pub withClause: Option<Node<'mcx>>,
    pub r#override: crate::primnodes::OverridingKind,
}

#[derive(Default)]
pub struct DeleteStmt<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub usingClause: NodeList<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub returningClause: Option<Node<'mcx>>,
    pub withClause: Option<Node<'mcx>>,
}

#[derive(Default)]
pub struct UpdateStmt<'mcx> {
    pub relation: Option<Node<'mcx>>,
    pub targetList: NodeList<'mcx>,
    pub whereClause: Option<Node<'mcx>>,
    pub fromClause: NodeList<'mcx>,
    pub returningClause: Option<Node<'mcx>>,
    pub withClause: Option<Node<'mcx>>,
}

#[derive(Default)]
pub struct ResTarget<'mcx> {
    pub name: Option<&'mcx str>,
    pub indirection: NodeList<'mcx>,
    pub val: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct A_Expr<'mcx> {
    pub kind: A_Expr_Kind,
    pub name: NodeList<'mcx>,
    pub lexpr: Option<Node<'mcx>>,
    pub rexpr: Option<Node<'mcx>>,
    pub rexpr_list_start: ParseLoc,
    pub rexpr_list_end: ParseLoc,
    pub location: ParseLoc,
}

/// C `union ValUnion` — the embedded value-node union of `A_Const`.
#[derive(Clone, Copy)]
pub enum ValUnion<'mcx> {
    Integer(Integer),
    Float(Float<'mcx>),
    Boolean(Boolean),
    String(String<'mcx>),
    BitString(BitString<'mcx>),
}

// C divergence: C pairs an undefined-when-isnull union with a separate
// `bool isnull`; `val: None` IS the null case here (no undefined reads).
#[derive(Clone, Copy, Default)]
pub struct A_Const<'mcx> {
    pub val: Option<ValUnion<'mcx>>,
    pub location: ParseLoc,
}

impl A_Const<'_> {
    #[inline]
    pub fn isnull(&self) -> bool {
        self.val.is_none()
    }
}

#[derive(Default)]
pub struct ColumnRef<'mcx> {
    pub fields: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ParamRef {
    pub number: i32,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct A_Star;

#[derive(Default)]
pub struct SortBy<'mcx> {
    pub node: Option<Node<'mcx>>,
    pub sortby_dir: SortByDir,
    pub sortby_nulls: SortByNulls,
    pub useOp: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct FuncCall<'mcx> {
    pub funcname: NodeList<'mcx>,
    pub args: NodeList<'mcx>,
    pub agg_order: NodeList<'mcx>,
    pub agg_filter: Option<Node<'mcx>>,
    pub over: Option<Node<'mcx>>,
    pub agg_within_group: bool,
    pub agg_star: bool,
    pub agg_distinct: bool,
    pub func_variadic: bool,
    pub funcformat: crate::primnodes::CoercionForm,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct RangeFunction<'mcx> {
    pub lateral: bool,
    pub ordinality: bool,
    pub is_rowsfrom: bool,
    pub functions: NodeList<'mcx>,
    pub alias: Option<&'mcx crate::primnodes::Alias<'mcx>>,
    pub coldeflist: NodeList<'mcx>,
}

#[derive(Default)]
pub struct TypeName<'mcx> {
    pub names: NodeList<'mcx>,
    pub typeOid: Oid,
    pub setof: bool,
    pub pct_type: bool,
    pub typmods: NodeList<'mcx>,
    pub typemod: i32,
    pub arrayBounds: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct TypeCast<'mcx> {
    pub arg: Option<Node<'mcx>>,
    pub typeName: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

// C OnCommitAction (primnodes.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum OnCommitAction {
    #[default]
    ONCOMMIT_NOOP = 0,
    ONCOMMIT_PRESERVE_ROWS,
    ONCOMMIT_DELETE_ROWS,
    ONCOMMIT_DROP,
}

#[derive(Default)]
pub struct CreateStmt<'mcx> {
    pub relation: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub tableElts: NodeList<'mcx>,
    pub inhRelations: NodeList<'mcx>,
    pub partbound: Option<Node<'mcx>>,
    pub partspec: Option<Node<'mcx>>,
    pub ofTypename: Option<Node<'mcx>>,
    pub constraints: NodeList<'mcx>,
    pub nnconstraints: NodeList<'mcx>,
    pub options: NodeList<'mcx>,
    pub oncommit: OnCommitAction,
    pub tablespacename: Option<&'mcx str>,
    pub accessMethod: Option<&'mcx str>,
    pub if_not_exists: bool,
}

#[derive(Default)]
pub struct ColumnDef<'mcx> {
    pub colname: Option<&'mcx str>,
    pub typeName: Option<Node<'mcx>>,
    pub compression: Option<&'mcx str>,
    pub inhcount: i16,
    pub is_local: bool,
    pub is_not_null: bool,
    pub is_from_type: bool,
    pub storage: u8,
    pub storage_name: Option<&'mcx str>,
    pub raw_default: Option<Node<'mcx>>,
    pub cooked_default: Option<Node<'mcx>>,
    pub identity: u8,
    pub identitySequence: Option<&'mcx crate::primnodes::RangeVar<'mcx>>,
    pub generated: u8,
    pub collClause: Option<Node<'mcx>>,
    pub collOid: Oid,
    pub constraints: NodeList<'mcx>,
    pub fdwoptions: NodeList<'mcx>,
    pub location: ParseLoc,
}

// C ConstrType (parsenodes.h).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ConstrType {
    #[default]
    CONSTR_NULL = 0,
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

// DEFAULT/CHECK slice of C's Constraint; index/FK fields arrive with their DDL.
#[derive(Default)]
pub struct Constraint<'mcx> {
    pub contype: ConstrType,
    pub conname: Option<&'mcx str>,
    pub deferrable: bool,
    pub initdeferred: bool,
    pub is_enforced: bool,
    pub skip_validation: bool,
    pub initially_valid: bool,
    pub is_no_inherit: bool,
    pub raw_expr: Option<Node<'mcx>>,
    pub cooked_expr: Option<&'mcx str>,
    pub keys: NodeList<'mcx>,
    pub location: ParseLoc,
}

// SAFETY (each): tag/type pairing mirrors parsenodes.h.
unsafe impl<'mcx> NodeVariant<'mcx> for RawStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_RawStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SelectStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_SelectStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for InsertStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_InsertStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DeleteStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_DeleteStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for UpdateStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_UpdateStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ResTarget<'mcx> {
    const TAG: NodeTag = NodeTag::T_ResTarget;
}
unsafe impl<'mcx> NodeVariant<'mcx> for A_Expr<'mcx> {
    const TAG: NodeTag = NodeTag::T_A_Expr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for A_Const<'mcx> {
    const TAG: NodeTag = NodeTag::T_A_Const;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ColumnRef<'mcx> {
    const TAG: NodeTag = NodeTag::T_ColumnRef;
}
unsafe impl NodeVariant<'_> for ParamRef {
    const TAG: NodeTag = NodeTag::T_ParamRef;
}
unsafe impl NodeVariant<'_> for A_Star {
    const TAG: NodeTag = NodeTag::T_A_Star;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SortBy<'mcx> {
    const TAG: NodeTag = NodeTag::T_SortBy;
}
unsafe impl<'mcx> NodeVariant<'mcx> for FuncCall<'mcx> {
    const TAG: NodeTag = NodeTag::T_FuncCall;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RangeFunction<'mcx> {
    const TAG: NodeTag = NodeTag::T_RangeFunction;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TypeName<'mcx> {
    const TAG: NodeTag = NodeTag::T_TypeName;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TypeCast<'mcx> {
    const TAG: NodeTag = NodeTag::T_TypeCast;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CreateStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_CreateStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ColumnDef<'mcx> {
    const TAG: NodeTag = NodeTag::T_ColumnDef;
}

impl<'mcx> Node<'mcx> {
    pub fn mk_raw_stmt(
        mcx: Mcx<'mcx>,
        stmt: Option<Node<'mcx>>,
        stmt_location: ParseLoc,
        stmt_len: ParseLoc,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, RawStmt { stmt, stmt_location, stmt_len })
    }

    pub fn mk_res_target(
        mcx: Mcx<'mcx>,
        name: Option<&'mcx str>,
        indirection: NodeList<'mcx>,
        val: Option<Node<'mcx>>,
        location: ParseLoc,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, ResTarget { name, indirection, val, location })
    }

    /// C `makeA_Expr` (rexpr list bounds start unset).
    pub fn mk_a_expr(
        mcx: Mcx<'mcx>,
        kind: A_Expr_Kind,
        name: NodeList<'mcx>,
        lexpr: Option<Node<'mcx>>,
        rexpr: Option<Node<'mcx>>,
        location: ParseLoc,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(
            mcx,
            A_Expr { kind, name, lexpr, rexpr, rexpr_list_start: -1, rexpr_list_end: -1, location },
        )
    }

    pub fn mk_a_const(
        mcx: Mcx<'mcx>,
        val: Option<ValUnion<'mcx>>,
        location: ParseLoc,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, A_Const { val, location })
    }

    pub fn mk_column_ref(
        mcx: Mcx<'mcx>,
        fields: NodeList<'mcx>,
        location: ParseLoc,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, ColumnRef { fields, location })
    }

    pub fn mk_param_ref(mcx: Mcx<'mcx>, number: i32, location: ParseLoc) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, ParamRef { number, location })
    }

    pub fn mk_a_star(mcx: Mcx<'mcx>) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, A_Star)
    }

    #[inline]
    pub fn as_raw_stmt(self) -> Option<&'mcx RawStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_select_stmt(self) -> Option<&'mcx SelectStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_insert_stmt(self) -> Option<&'mcx InsertStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_delete_stmt(self) -> Option<&'mcx DeleteStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_update_stmt(self) -> Option<&'mcx UpdateStmt<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_res_target(self) -> Option<&'mcx ResTarget<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_a_expr(self) -> Option<&'mcx A_Expr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_a_const(self) -> Option<&'mcx A_Const<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_column_ref(self) -> Option<&'mcx ColumnRef<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_param_ref(self) -> Option<&'mcx ParamRef> {
        self.as_variant()
    }

    #[inline]
    pub fn as_a_star(self) -> Option<&'mcx A_Star> {
        self.as_variant()
    }

    #[inline]
    pub fn as_sort_by(self) -> Option<&'mcx SortBy<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_function(self) -> Option<&'mcx RangeFunction<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_func_call(self) -> Option<&'mcx FuncCall<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_type_name(self) -> Option<&'mcx TypeName<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_type_cast(self) -> Option<&'mcx TypeCast<'mcx>> {
        self.as_variant()
    }
}
unsafe impl<'mcx> NodeVariant<'mcx> for Constraint<'mcx> {
    const TAG: NodeTag = NodeTag::T_Constraint;
}
