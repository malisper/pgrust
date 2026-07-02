// Raw-grammar input nodes; field names/order mirror vendor/parsenodes.h
// (tests: *_field_order_matches_c, enum_values_match_c_headers).
#![allow(non_camel_case_types, non_snake_case)]

use mcx::Mcx;
use types_core::ParseLoc;
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

#[derive(Clone, Copy, Default)]
pub struct RawStmt<'mcx> {
    pub stmt: Option<Node<'mcx>>,
    pub stmt_location: ParseLoc,
    pub stmt_len: ParseLoc,
}

#[derive(Default)]
pub struct SelectStmt<'mcx> {
    pub distinctClause: NodeList<'mcx>,
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

// SAFETY (each): tag/type pairing mirrors parsenodes.h.
unsafe impl<'mcx> NodeVariant<'mcx> for RawStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_RawStmt;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SelectStmt<'mcx> {
    const TAG: NodeTag = NodeTag::T_SelectStmt;
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
}
