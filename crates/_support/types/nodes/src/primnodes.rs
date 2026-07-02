// Field names, order, and enum values mirror vendor/primnodes.h
// (tests: *_field_order_matches_c, enum_values_match_c_headers).
#![allow(non_camel_case_types, non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, Index, Oid, ParseLoc};
use types_error::PgResult;

use crate::bitmapset::Bitmapset;
use crate::list::NodeList;
use crate::node_tree::{Node, NodeVariant};
use crate::tags::NodeTag;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum OverridingKind {
    #[default]
    OVERRIDING_NOT_SET = 0,
    OVERRIDING_USER_VALUE = 1,
    OVERRIDING_SYSTEM_VALUE = 2,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum CoercionForm {
    #[default]
    COERCE_EXPLICIT_CALL = 0,
    COERCE_EXPLICIT_CAST = 1,
    COERCE_IMPLICIT_CAST = 2,
    COERCE_SQL_SYNTAX = 3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum ParamKind {
    #[default]
    PARAM_EXTERN = 0,
    PARAM_EXEC = 1,
    PARAM_SUBLINK = 2,
    PARAM_MULTIEXPR = 3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum VarReturningType {
    #[default]
    VAR_RETURNING_DEFAULT = 0,
    VAR_RETURNING_OLD = 1,
    VAR_RETURNING_NEW = 2,
}

#[derive(Default)]
pub struct Alias<'mcx> {
    pub aliasname: Option<&'mcx str>,
    pub colnames: NodeList<'mcx>,
}

#[derive(Default)]
pub struct RangeVar<'mcx> {
    pub catalogname: Option<&'mcx str>,
    pub schemaname: Option<&'mcx str>,
    pub relname: Option<&'mcx str>,
    pub inh: bool,
    pub relpersistence: u8,
    pub alias: Option<&'mcx Alias<'mcx>>,
    pub location: ParseLoc,
}

pub struct Var<'mcx> {
    pub varno: i32,
    pub varattno: AttrNumber,
    pub vartype: Oid,
    pub vartypmod: i32,
    pub varcollid: Oid,
    pub varnullingrels: Bitmapset<'mcx>,
    pub varlevelsup: Index,
    pub varreturningtype: VarReturningType,
    pub varnosyn: Index,
    pub varattnosyn: AttrNumber,
    pub location: ParseLoc,
}

impl Default for Var<'_> {
    fn default() -> Self {
        Var {
            varno: 0,
            varattno: 0,
            vartype: 0,
            vartypmod: 0,
            varcollid: 0,
            varnullingrels: Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
            varnosyn: 0,
            varattnosyn: 0,
            location: -1,
        }
    }
}

#[derive(Clone, Copy)]
pub struct Const {
    pub consttype: Oid,
    pub consttypmod: i32,
    pub constcollid: Oid,
    pub constlen: i32,
    pub constvalue: Datum,
    pub constisnull: bool,
    pub constbyval: bool,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Default)]
pub struct Param {
    pub paramkind: ParamKind,
    pub paramid: i32,
    pub paramtype: Oid,
    pub paramtypmod: i32,
    pub paramcollid: Oid,
    pub location: ParseLoc,
}

// C `Expr *expr` is never NULL in a live TargetEntry (makeTargetEntry
// requires it); modeled non-optional, so no Default.
pub struct TargetEntry<'mcx> {
    pub expr: Node<'mcx>,
    pub resno: AttrNumber,
    pub resname: Option<&'mcx str>,
    pub ressortgroupref: Index,
    pub resorigtbl: Oid,
    pub resorigcol: AttrNumber,
    pub resjunk: bool,
}

#[derive(Default)]
pub struct FromExpr<'mcx> {
    pub fromlist: NodeList<'mcx>,
    pub quals: Option<Node<'mcx>>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RangeTblRef {
    pub rtindex: i32,
}

#[derive(Default)]
pub struct OpExpr<'mcx> {
    pub opno: Oid,
    pub opfuncid: Oid,
    pub opresulttype: Oid,
    pub opretset: bool,
    pub opcollid: Oid,
    pub inputcollid: Oid,
    pub args: NodeList<'mcx>,
    pub location: ParseLoc,
}

// C `Expr *arg` is never NULL in a live RelabelType; modeled non-optional.
pub struct RelabelType<'mcx> {
    pub arg: Node<'mcx>,
    pub resulttype: Oid,
    pub resulttypmod: i32,
    pub resultcollid: Oid,
    pub relabelformat: CoercionForm,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct FuncExpr<'mcx> {
    pub funcid: Oid,
    pub funcresulttype: Oid,
    pub funcretset: bool,
    pub funcvariadic: bool,
    pub funcformat: CoercionForm,
    pub funccollid: Oid,
    pub inputcollid: Oid,
    pub args: NodeList<'mcx>,
    pub location: ParseLoc,
}

// SAFETY (each): tag/type pairing mirrors primnodes.h.
unsafe impl<'mcx> NodeVariant<'mcx> for Alias<'mcx> {
    const TAG: NodeTag = NodeTag::T_Alias;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RangeVar<'mcx> {
    const TAG: NodeTag = NodeTag::T_RangeVar;
}
unsafe impl<'mcx> NodeVariant<'mcx> for Var<'mcx> {
    const TAG: NodeTag = NodeTag::T_Var;
}
unsafe impl NodeVariant<'_> for Const {
    const TAG: NodeTag = NodeTag::T_Const;
}
unsafe impl NodeVariant<'_> for Param {
    const TAG: NodeTag = NodeTag::T_Param;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TargetEntry<'mcx> {
    const TAG: NodeTag = NodeTag::T_TargetEntry;
}
unsafe impl<'mcx> NodeVariant<'mcx> for FromExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_FromExpr;
}
unsafe impl NodeVariant<'_> for RangeTblRef {
    const TAG: NodeTag = NodeTag::T_RangeTblRef;
}
unsafe impl<'mcx> NodeVariant<'mcx> for OpExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_OpExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for FuncExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_FuncExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RelabelType<'mcx> {
    const TAG: NodeTag = NodeTag::T_RelabelType;
}

impl<'mcx> Node<'mcx> {
    /// C `makeConst` (constvalue passed in, location -1).
    #[allow(clippy::too_many_arguments)]
    pub fn mk_const(
        mcx: Mcx<'mcx>,
        consttype: Oid,
        consttypmod: i32,
        constcollid: Oid,
        constlen: i32,
        constvalue: Datum,
        constisnull: bool,
        constbyval: bool,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(
            mcx,
            Const {
                consttype,
                consttypmod,
                constcollid,
                constlen,
                constvalue,
                constisnull,
                constbyval,
                location: -1,
            },
        )
    }

    /// C `makeVar` (syn fields copied from varno/varattno, location -1).
    pub fn mk_var(
        mcx: Mcx<'mcx>,
        varno: i32,
        varattno: AttrNumber,
        vartype: Oid,
        vartypmod: i32,
        varcollid: Oid,
        varlevelsup: Index,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(
            mcx,
            Var {
                varno,
                varattno,
                vartype,
                vartypmod,
                varcollid,
                varnullingrels: Bitmapset::empty(),
                varlevelsup,
                varreturningtype: VarReturningType::VAR_RETURNING_DEFAULT,
                varnosyn: varno as Index,
                varattnosyn: varattno,
                location: -1,
            },
        )
    }

    /// C `makeTargetEntry`.
    pub fn mk_target_entry(
        mcx: Mcx<'mcx>,
        expr: Node<'mcx>,
        resno: AttrNumber,
        resname: Option<&'mcx str>,
        resjunk: bool,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(
            mcx,
            TargetEntry {
                expr,
                resno,
                resname,
                ressortgroupref: 0,
                resorigtbl: 0,
                resorigcol: 0,
                resjunk,
            },
        )
    }

    /// C `makeFromExpr`.
    pub fn mk_from_expr(
        mcx: Mcx<'mcx>,
        fromlist: NodeList<'mcx>,
        quals: Option<Node<'mcx>>,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, FromExpr { fromlist, quals })
    }

    pub fn mk_range_tbl_ref(mcx: Mcx<'mcx>, rtindex: i32) -> PgResult<Node<'mcx>> {
        Self::mk(mcx, RangeTblRef { rtindex })
    }

    #[inline]
    pub fn as_alias(self) -> Option<&'mcx Alias<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_var(self) -> Option<&'mcx RangeVar<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_var(self) -> Option<&'mcx Var<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_const(self) -> Option<&'mcx Const> {
        self.as_variant()
    }

    #[inline]
    pub fn as_param(self) -> Option<&'mcx Param> {
        self.as_variant()
    }

    #[inline]
    pub fn as_target_entry(self) -> Option<&'mcx TargetEntry<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_from_expr(self) -> Option<&'mcx FromExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_range_tbl_ref(self) -> Option<&'mcx RangeTblRef> {
        self.as_variant()
    }

    #[inline]
    pub fn as_op_expr(self) -> Option<&'mcx OpExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_relabel_type(self) -> Option<&'mcx RelabelType<'mcx>> {
        self.as_variant()
    }

    /// C `makeRelabelType`.
    pub fn mk_relabel_type(
        mcx: Mcx<'mcx>,
        arg: Node<'mcx>,
        rtype: Oid,
        typmod: i32,
        rcollid: Oid,
        rformat: CoercionForm,
    ) -> PgResult<Node<'mcx>> {
        Self::mk(
            mcx,
            RelabelType {
                arg,
                resulttype: rtype,
                resulttypmod: typmod,
                resultcollid: rcollid,
                relabelformat: rformat,
                location: -1,
            },
        )
    }

    #[inline]
    pub fn as_func_expr(self) -> Option<&'mcx FuncExpr<'mcx>> {
        self.as_variant()
    }
}
