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
pub enum SubLinkType {
    #[default]
    EXISTS_SUBLINK = 0,
    ALL_SUBLINK = 1,
    ANY_SUBLINK = 2,
    ROWCOMPARE_SUBLINK = 3,
    EXPR_SUBLINK = 4,
    MULTIEXPR_SUBLINK = 5,
    ARRAY_SUBLINK = 6,
    CTE_SUBLINK = 7,
}

// C `Node *subselect` is never NULL in a live SubLink; modeled non-optional.
pub struct SubLink<'mcx> {
    pub subLinkType: SubLinkType,
    pub subLinkId: i32,
    pub testexpr: Option<Node<'mcx>>,
    pub operName: NodeList<'mcx>,
    pub subselect: Node<'mcx>,
    pub location: ParseLoc,
}

/// primnodes.h AlternativeSubPlan: equivalent SubPlan implementations;
/// setrefs picks one (fix_alternative_subplan), the executor never sees it.
pub struct AlternativeSubPlan<'mcx> {
    pub subplans: NodeList<'mcx>,
}

pub struct SubPlan<'mcx> {
    pub subLinkType: SubLinkType,
    pub testexpr: Option<Node<'mcx>>,
    pub paramIds: crate::list::IntList<'mcx>,
    pub plan_id: i32,
    pub plan_name: Option<&'mcx str>,
    pub firstColType: Oid,
    pub firstColTypmod: i32,
    pub firstColCollation: Oid,
    pub useHashTable: bool,
    pub unknownEqFalse: bool,
    pub parallel_safe: bool,
    pub setParam: crate::list::IntList<'mcx>,
    pub parParam: crate::list::IntList<'mcx>,
    pub args: NodeList<'mcx>,
    pub startup_cost: f64,
    pub per_call_cost: f64,
}

impl Default for SubPlan<'_> {
    fn default() -> Self {
        SubPlan {
            subLinkType: SubLinkType::EXISTS_SUBLINK,
            testexpr: None,
            paramIds: crate::list::IntList::nil(),
            plan_id: 0,
            plan_name: None,
            firstColType: 0,
            firstColTypmod: -1,
            firstColCollation: 0,
            useHashTable: false,
            unknownEqFalse: false,
            parallel_safe: false,
            setParam: crate::list::IntList::nil(),
            parParam: crate::list::IntList::nil(),
            args: NodeList::nil(),
            startup_cost: 0.0,
            per_call_cost: 0.0,
        }
    }
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

// C: primnodes.h special varno values.
pub const INNER_VAR: i32 = -1;
pub const OUTER_VAR: i32 = -2;
pub const INDEX_VAR: i32 = -3;

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

// C: nodes.h AggSplit; AGGSPLITOP_* bit values.
pub type AggSplit = u32;
pub const AGGSPLIT_SIMPLE: AggSplit = 0;
pub const AGGSPLIT_INITIAL_SERIAL: AggSplit = 0x02 | 0x04;
pub const AGGSPLIT_FINAL_DESERIAL: AggSplit = 0x01 | 0x08;

pub const AGGKIND_NORMAL: i8 = b'n' as i8;
pub const AGGKIND_ORDERED_SET: i8 = b'o' as i8;
pub const AGGKIND_HYPOTHETICAL: i8 = b'h' as i8;

pub struct Aggref<'mcx> {
    pub aggfnoid: Oid,
    pub aggtype: Oid,
    pub aggcollid: Oid,
    pub inputcollid: Oid,
    pub aggtranstype: Oid,
    pub aggargtypes: crate::list::OidList<'mcx>,
    pub aggdirectargs: NodeList<'mcx>,
    pub args: NodeList<'mcx>,
    pub aggorder: NodeList<'mcx>,
    pub aggdistinct: NodeList<'mcx>,
    pub aggfilter: Option<Node<'mcx>>,
    pub aggstar: bool,
    pub aggvariadic: bool,
    pub aggkind: i8,
    pub aggpresorted: bool,
    pub agglevelsup: Index,
    pub aggsplit: AggSplit,
    pub aggno: i32,
    pub aggtransno: i32,
    pub location: ParseLoc,
}

impl Default for Aggref<'_> {
    fn default() -> Self {
        Aggref {
            aggfnoid: 0,
            aggtype: 0,
            aggcollid: 0,
            inputcollid: 0,
            aggtranstype: 0,
            aggargtypes: crate::list::OidList::nil(),
            aggdirectargs: NodeList::nil(),
            args: NodeList::nil(),
            aggorder: NodeList::nil(),
            aggdistinct: NodeList::nil(),
            aggfilter: None,
            aggstar: false,
            aggvariadic: false,
            aggkind: AGGKIND_NORMAL,
            aggpresorted: false,
            agglevelsup: 0,
            aggsplit: AGGSPLIT_SIMPLE,
            aggno: -1,
            aggtransno: -1,
            location: -1,
        }
    }
}

pub struct GroupingFunc<'mcx> {
    pub args: NodeList<'mcx>,
    pub refs: crate::list::IntList<'mcx>,
    pub cols: crate::list::IntList<'mcx>,
    pub agglevelsup: Index,
    pub location: ParseLoc,
}

impl Default for GroupingFunc<'_> {
    fn default() -> Self {
        GroupingFunc {
            args: NodeList::nil(),
            refs: crate::list::IntList::nil(),
            cols: crate::list::IntList::nil(),
            agglevelsup: 0,
            location: -1,
        }
    }
}

#[derive(Default)]
pub struct WindowFunc<'mcx> {
    pub winfnoid: Oid,
    pub wintype: Oid,
    pub wincollid: Oid,
    pub inputcollid: Oid,
    pub args: NodeList<'mcx>,
    pub aggfilter: Option<Node<'mcx>>,
    pub runCondition: NodeList<'mcx>,
    pub winref: Index,
    pub winstar: bool,
    pub winagg: bool,
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

// C `Node *larg/rarg` are never NULL in a live JoinExpr (the grammar always
// sets both); modeled non-optional, so no Default.
pub struct JoinExpr<'mcx> {
    pub jointype: crate::jointype::JoinType,
    pub isNatural: bool,
    pub larg: Node<'mcx>,
    pub rarg: Node<'mcx>,
    pub usingClause: NodeList<'mcx>,
    pub join_using_alias: Option<&'mcx Alias<'mcx>>,
    pub quals: Option<Node<'mcx>>,
    pub alias: Option<&'mcx Alias<'mcx>>,
    pub rtindex: i32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RangeTblRef {
    pub rtindex: i32,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SetToDefault {
    pub typeId: Oid,
    pub typeMod: i32,
    pub collation: Oid,
    pub location: ParseLoc,
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

// C `Expr *arg` is never NULL in a live CoerceViaIO; modeled non-optional.
pub struct CoerceViaIO<'mcx> {
    pub arg: Node<'mcx>,
    pub resulttype: Oid,
    pub resultcollid: Oid,
    pub coerceformat: CoercionForm,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum BoolExprType {
    #[default]
    AND_EXPR = 0,
    OR_EXPR = 1,
    NOT_EXPR = 2,
}

#[derive(Default)]
pub struct BoolExpr<'mcx> {
    pub boolop: BoolExprType,
    pub args: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum NullTestType {
    #[default]
    IS_NULL = 0,
    IS_NOT_NULL = 1,
}

#[derive(Default)]
pub struct NullTest<'mcx> {
    pub arg: Option<Node<'mcx>>,
    pub nulltesttype: NullTestType,
    pub argisrow: bool,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct CaseExpr<'mcx> {
    pub casetype: Oid,
    pub casecollid: Oid,
    pub arg: Option<Node<'mcx>>,
    pub args: NodeList<'mcx>,
    pub defresult: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Default)]
pub struct CaseTestExpr {
    pub typeId: Oid,
    pub typeMod: i32,
    pub collation: Oid,
}

#[derive(Default)]
pub struct CaseWhen<'mcx> {
    pub expr: Option<Node<'mcx>>,
    pub result: Option<Node<'mcx>>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct CoalesceExpr<'mcx> {
    pub coalescetype: Oid,
    pub coalescecollid: Oid,
    pub args: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum MinMaxOp {
    #[default]
    IS_GREATEST = 0,
    IS_LEAST = 1,
}

#[derive(Default)]
pub struct MinMaxExpr<'mcx> {
    pub minmaxtype: Oid,
    pub minmaxcollid: Oid,
    pub inputcollid: Oid,
    pub op: MinMaxOp,
    pub args: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum SQLValueFunctionOp {
    #[default]
    SVFOP_CURRENT_DATE = 0,
    SVFOP_CURRENT_TIME = 1,
    SVFOP_CURRENT_TIME_N = 2,
    SVFOP_CURRENT_TIMESTAMP = 3,
    SVFOP_CURRENT_TIMESTAMP_N = 4,
    SVFOP_LOCALTIME = 5,
    SVFOP_LOCALTIME_N = 6,
    SVFOP_LOCALTIMESTAMP = 7,
    SVFOP_LOCALTIMESTAMP_N = 8,
    SVFOP_CURRENT_ROLE = 9,
    SVFOP_CURRENT_USER = 10,
    SVFOP_USER = 11,
    SVFOP_SESSION_USER = 12,
    SVFOP_CURRENT_CATALOG = 13,
    SVFOP_CURRENT_SCHEMA = 14,
}

#[derive(Default)]
pub struct SQLValueFunction {
    pub op: SQLValueFunctionOp,
    pub r#type: Oid,
    pub typmod: i32,
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
unsafe impl<'mcx> NodeVariant<'mcx> for Aggref<'mcx> {
    const TAG: NodeTag = NodeTag::T_Aggref;
}
unsafe impl<'mcx> NodeVariant<'mcx> for GroupingFunc<'mcx> {
    const TAG: NodeTag = NodeTag::T_GroupingFunc;
}
unsafe impl<'mcx> NodeVariant<'mcx> for WindowFunc<'mcx> {
    const TAG: NodeTag = NodeTag::T_WindowFunc;
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
unsafe impl NodeVariant<'_> for SetToDefault {
    const TAG: NodeTag = NodeTag::T_SetToDefault;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JoinExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_JoinExpr;
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
unsafe impl<'mcx> NodeVariant<'mcx> for CoerceViaIO<'mcx> {
    const TAG: NodeTag = NodeTag::T_CoerceViaIO;
}
unsafe impl<'mcx> NodeVariant<'mcx> for BoolExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_BoolExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for NullTest<'mcx> {
    const TAG: NodeTag = NodeTag::T_NullTest;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CaseExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_CaseExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CaseWhen<'mcx> {
    const TAG: NodeTag = NodeTag::T_CaseWhen;
}
unsafe impl NodeVariant<'_> for CaseTestExpr {
    const TAG: NodeTag = NodeTag::T_CaseTestExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CoalesceExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_CoalesceExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for MinMaxExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_MinMaxExpr;
}
unsafe impl NodeVariant<'_> for SQLValueFunction {
    const TAG: NodeTag = NodeTag::T_SQLValueFunction;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SubLink<'mcx> {
    const TAG: NodeTag = NodeTag::T_SubLink;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SubPlan<'mcx> {
    const TAG: NodeTag = NodeTag::T_SubPlan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlternativeSubPlan<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlternativeSubPlan;
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
    pub fn as_aggref(self) -> Option<&'mcx Aggref<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_grouping_func(self) -> Option<&'mcx GroupingFunc<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_window_func(self) -> Option<&'mcx WindowFunc<'mcx>> {
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
    pub fn as_set_to_default(self) -> Option<&'mcx SetToDefault> {
        self.as_variant()
    }

    #[inline]
    pub fn as_join_expr(self) -> Option<&'mcx JoinExpr<'mcx>> {
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

    #[inline]
    pub fn as_bool_expr(self) -> Option<&'mcx BoolExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_coerce_via_io(self) -> Option<&'mcx CoerceViaIO<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_null_test(self) -> Option<&'mcx NullTest<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_case_expr(self) -> Option<&'mcx CaseExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_case_when(self) -> Option<&'mcx CaseWhen<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_case_test_expr(self) -> Option<&'mcx CaseTestExpr> {
        self.as_variant()
    }

    #[inline]
    pub fn as_coalesce_expr(self) -> Option<&'mcx CoalesceExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_min_max_expr(self) -> Option<&'mcx MinMaxExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_sql_value_function(self) -> Option<&'mcx SQLValueFunction> {
        self.as_variant()
    }

    #[inline]
    pub fn as_sub_link(self) -> Option<&'mcx SubLink<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_alternative_sub_plan(self) -> Option<&'mcx AlternativeSubPlan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_sub_plan(self) -> Option<&'mcx SubPlan<'mcx>> {
        self.as_variant()
    }
}

// SupportRequestOptimizeWindowClause (supportnodes.h), tag + frameOptions
// slice: the window_clause/window_func pointers are unread by every in-core
// window prosupport's OptimizeWindowClause arm (C divergence recorded).
#[repr(C)]
pub struct SupportRequestOptimizeWindowClause {
    pub tag: crate::NodeTag,
    pub frame_options: i32,
}
