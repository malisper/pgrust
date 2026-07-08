// Field names, order, and enum values mirror vendor/primnodes.h
// (tests: *_field_order_matches_c, enum_values_match_c_headers).
#![allow(non_camel_case_types, non_snake_case)]

use datum::Datum;
use mcx::Mcx;
use types_core::{AttrNumber, Index, Oid, ParseLoc};
use types_error::PgResult;

use crate::bitmapset::Bitmapset;
use crate::list::{IntList, NodeList, OidList, OptNodeList};
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
pub enum CoercionContext {
    #[default]
    COERCION_IMPLICIT = 0,
    COERCION_ASSIGNMENT = 1,
    COERCION_PLPGSQL = 2,
    COERCION_EXPLICIT = 3,
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

// pathnodes.h PlaceHolderVar (planner-created; never in stored rules).
pub struct PlaceHolderVar<'mcx> {
    pub phexpr: Node<'mcx>,
    pub phrels: Bitmapset<'mcx>,
    pub phnullingrels: Bitmapset<'mcx>,
    pub phid: Index,
    pub phlevelsup: Index,
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
pub const ROWID_VAR: i32 = -4;

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
pub const AGGSPLITOP_COMBINE: AggSplit = 0x01;
pub const AGGSPLITOP_SKIPFINAL: AggSplit = 0x02;
pub const AGGSPLITOP_SERIALIZE: AggSplit = 0x04;
pub const AGGSPLITOP_DESERIALIZE: AggSplit = 0x08;
pub const AGGSPLIT_SIMPLE: AggSplit = 0;
pub const AGGSPLIT_INITIAL_SERIAL: AggSplit = AGGSPLITOP_SKIPFINAL | AGGSPLITOP_SERIALIZE;
pub const AGGSPLIT_FINAL_DESERIAL: AggSplit = AGGSPLITOP_COMBINE | AGGSPLITOP_DESERIALIZE;

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

pub struct RowExpr<'mcx> {
    pub args: NodeList<'mcx>,
    pub row_typeid: Oid,
    pub row_format: CoercionForm,
    pub colnames: NodeList<'mcx>,
    pub location: ParseLoc,
}

impl Default for RowExpr<'_> {
    fn default() -> Self {
        RowExpr {
            args: NodeList::nil(),
            row_typeid: 0,
            row_format: CoercionForm::COERCE_EXPLICIT_CALL,
            colnames: NodeList::nil(),
            location: -1,
        }
    }
}

pub struct FieldStore<'mcx> {
    pub arg: Node<'mcx>,
    pub newvals: NodeList<'mcx>,
    pub fieldnums: crate::list::IntList<'mcx>,
    pub resulttype: Oid,
}

// cmptype is CompareType (cmptype.h); EQ/NE never appear here.
pub struct RowCompareExpr<'mcx> {
    pub cmptype: i32,
    pub opnos: crate::list::OidList<'mcx>,
    pub opfamilies: crate::list::OidList<'mcx>,
    pub inputcollids: crate::list::OidList<'mcx>,
    pub largs: NodeList<'mcx>,
    pub rargs: NodeList<'mcx>,
}

impl Default for RowCompareExpr<'_> {
    fn default() -> Self {
        RowCompareExpr {
            cmptype: 0,
            opnos: crate::list::OidList::nil(),
            opfamilies: crate::list::OidList::nil(),
            inputcollids: crate::list::OidList::nil(),
            largs: NodeList::nil(),
            rargs: NodeList::nil(),
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

pub struct WindowFuncRunCondition<'mcx> {
    pub opno: Oid,
    pub inputcollid: Oid,
    pub wfunc_left: bool,
    pub arg: Node<'mcx>,
}

/// MERGE_ACTION(); only legal in the RETURNING list of a MERGE command.
#[derive(Default)]
pub struct MergeSupportFunc {
    pub msftype: Oid,
    pub msfcollid: Oid,
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

// cvarno is a live RT index after parse analysis; varlevelsup is implicitly 0.
#[derive(Clone, Copy, Default)]
pub struct CurrentOfExpr<'mcx> {
    pub cvarno: Index,
    pub cursor_name: Option<&'mcx str>,
    pub cursor_param: i32,
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

#[derive(Default)]
pub struct ScalarArrayOpExpr<'mcx> {
    pub opno: Oid,
    pub opfuncid: Oid,
    pub hashfuncid: Oid,
    pub negfuncid: Oid,
    pub useOr: bool,
    pub inputcollid: Oid,
    pub args: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct ArrayExpr<'mcx> {
    pub array_typeid: Oid,
    pub array_collid: Oid,
    pub element_typeid: Oid,
    pub elements: NodeList<'mcx>,
    pub multidims: bool,
    pub list_start: ParseLoc,
    pub list_end: ParseLoc,
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
pub struct NextValueExpr {
    pub seqid: Oid,
    pub typeId: Oid,
}

// C `Expr *retexpr` is never NULL in a live ReturningExpr; modeled non-optional.
pub struct ReturningExpr<'mcx> {
    pub retlevelsup: i32,
    pub retold: bool,
    pub retexpr: Node<'mcx>,
}

// C `Expr *arg` is never NULL in a live FieldSelect; modeled non-optional.
pub struct FieldSelect<'mcx> {
    pub arg: Node<'mcx>,
    pub fieldnum: AttrNumber,
    pub resulttype: Oid,
    pub resulttypmod: i32,
    pub resultcollid: Oid,
}

// C `Expr *arg` is never NULL in a live CoerceViaIO; modeled non-optional.
pub struct CoerceViaIO<'mcx> {
    pub arg: Node<'mcx>,
    pub resulttype: Oid,
    pub resultcollid: Oid,
    pub coerceformat: CoercionForm,
    pub location: ParseLoc,
}

// C `Expr *arg` is never NULL in a live ArrayCoerceExpr; elemexpr can be
// NULL when the element coercion is binary-compatible.
pub struct ArrayCoerceExpr<'mcx> {
    pub arg: Node<'mcx>,
    pub elemexpr: Option<Node<'mcx>>,
    pub resulttype: Oid,
    pub resulttypmod: i32,
    pub resultcollid: Oid,
    pub coerceformat: CoercionForm,
    pub location: ParseLoc,
}

// C `Expr *arg` is never NULL in a live ConvertRowtypeExpr; modeled
// non-optional. No typmod/collation fields, like RowExpr.
pub struct ConvertRowtypeExpr<'mcx> {
    pub arg: Node<'mcx>,
    pub resulttype: Oid,
    pub convertformat: CoercionForm,
    pub location: ParseLoc,
}

// C `Expr *arg` is never NULL in a live CoerceToDomain; modeled non-optional.
pub struct CoerceToDomain<'mcx> {
    pub arg: Node<'mcx>,
    pub resulttype: Oid,
    pub resulttypmod: i32,
    pub resultcollid: Oid,
    pub coercionformat: CoercionForm,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CoerceToDomainValue {
    pub typeId: Oid,
    pub typeMod: i32,
    pub collation: Oid,
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

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum BoolTestType {
    #[default]
    IS_TRUE = 0,
    IS_NOT_TRUE = 1,
    IS_FALSE = 2,
    IS_NOT_FALSE = 3,
    IS_UNKNOWN = 4,
    IS_NOT_UNKNOWN = 5,
}

#[derive(Default)]
pub struct BooleanTest<'mcx> {
    pub arg: Option<Node<'mcx>>,
    pub booltesttype: BoolTestType,
    pub location: ParseLoc,
}

// C `typedef OpExpr DistinctExpr` (primnodes.h); the tag is the only difference.
#[derive(Default)]
pub struct DistinctExpr<'mcx> {
    pub opno: Oid,
    pub opfuncid: Oid,
    pub opresulttype: Oid,
    pub opretset: bool,
    pub opcollid: Oid,
    pub inputcollid: Oid,
    pub args: NodeList<'mcx>,
    pub location: ParseLoc,
}

// C `typedef OpExpr NullIfExpr` (primnodes.h); the tag is the only difference.
#[derive(Default)]
pub struct NullIfExpr<'mcx> {
    pub opno: Oid,
    pub opfuncid: Oid,
    pub opresulttype: Oid,
    pub opretset: bool,
    pub opcollid: Oid,
    pub inputcollid: Oid,
    pub args: NodeList<'mcx>,
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

// C `Expr *arg` is never NULL in a live CollateExpr; modeled non-optional.
pub struct CollateExpr<'mcx> {
    pub arg: Node<'mcx>,
    pub collOid: Oid,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum XmlExprOp {
    #[default]
    IS_XMLCONCAT = 0,
    IS_XMLELEMENT = 1,
    IS_XMLFOREST = 2,
    IS_XMLPARSE = 3,
    IS_XMLPI = 4,
    IS_XMLROOT = 5,
    IS_XMLSERIALIZE = 6,
    IS_DOCUMENT = 7,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum XmlOptionType {
    #[default]
    XMLOPTION_DOCUMENT = 0,
    XMLOPTION_CONTENT = 1,
}

#[derive(Default)]
pub struct XmlExpr<'mcx> {
    pub op: XmlExprOp,
    pub name: Option<&'mcx str>,
    pub named_args: NodeList<'mcx>,
    pub arg_names: NodeList<'mcx>,
    pub args: NodeList<'mcx>,
    pub xmloption: XmlOptionType,
    pub indent: bool,
    pub r#type: Oid,
    pub typmod: i32,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum TableFuncType {
    #[default]
    TFT_XMLTABLE = 0,
    TFT_JSON_TABLE = 1,
}

// ns_names holds String-or-NULL cells (NULL = DEFAULT namespace); colexprs /
// coldefexprs carry NULL cells for columns without a PATH / DEFAULT.
#[derive(Default)]
pub struct TableFunc<'mcx> {
    pub functype: TableFuncType,
    pub ns_uris: NodeList<'mcx>,
    pub ns_names: OptNodeList<'mcx>,
    pub docexpr: Option<Node<'mcx>>,
    pub rowexpr: Option<Node<'mcx>>,
    pub colnames: NodeList<'mcx>,
    pub coltypes: OidList<'mcx>,
    pub coltypmods: IntList<'mcx>,
    pub colcollations: OidList<'mcx>,
    pub colexprs: OptNodeList<'mcx>,
    pub coldefexprs: OptNodeList<'mcx>,
    pub colvalexprs: OptNodeList<'mcx>,
    pub passingvalexprs: NodeList<'mcx>,
    pub notnulls: Bitmapset<'mcx>,
    pub plan: Option<Node<'mcx>>,
    pub ordinalitycol: i32,
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

#[derive(Clone, Copy, Default)]
pub struct SQLValueFunction {
    pub op: SQLValueFunctionOp,
    pub r#type: Oid,
    pub typmod: i32,
    pub location: ParseLoc,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JsonEncoding {
    #[default]
    JS_ENC_DEFAULT = 0,
    JS_ENC_UTF8 = 1,
    JS_ENC_UTF16 = 2,
    JS_ENC_UTF32 = 3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JsonFormatType {
    #[default]
    JS_FORMAT_DEFAULT = 0,
    JS_FORMAT_JSON = 1,
    JS_FORMAT_JSONB = 2,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct JsonFormat {
    pub format_type: JsonFormatType,
    pub encoding: JsonEncoding,
    pub location: ParseLoc,
}

impl Default for JsonFormat {
    fn default() -> Self {
        JsonFormat {
            format_type: JsonFormatType::JS_FORMAT_DEFAULT,
            encoding: JsonEncoding::JS_ENC_DEFAULT,
            location: -1,
        }
    }
}

#[derive(Clone, Copy)]
pub struct JsonReturning<'mcx> {
    pub format: Option<&'mcx JsonFormat>,
    pub typid: Oid,
    pub typmod: i32,
}

impl Default for JsonReturning<'_> {
    fn default() -> Self {
        JsonReturning { format: None, typid: 0, typmod: -1 }
    }
}

#[derive(Default)]
pub struct JsonValueExpr<'mcx> {
    pub raw_expr: Option<Node<'mcx>>,
    pub formatted_expr: Option<Node<'mcx>>,
    pub format: Option<&'mcx JsonFormat>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JsonConstructorType {
    #[default]
    JSCTOR_JSON_OBJECT = 1,
    JSCTOR_JSON_ARRAY = 2,
    JSCTOR_JSON_OBJECTAGG = 3,
    JSCTOR_JSON_ARRAYAGG = 4,
    JSCTOR_JSON_PARSE = 5,
    JSCTOR_JSON_SCALAR = 6,
    JSCTOR_JSON_SERIALIZE = 7,
}

pub struct JsonConstructorExpr<'mcx> {
    pub r#type: JsonConstructorType,
    pub args: NodeList<'mcx>,
    pub func: Option<Node<'mcx>>,
    pub coercion: Option<Node<'mcx>>,
    pub returning: Option<&'mcx JsonReturning<'mcx>>,
    pub absent_on_null: bool,
    pub unique: bool,
    pub location: ParseLoc,
}

impl Default for JsonConstructorExpr<'_> {
    fn default() -> Self {
        JsonConstructorExpr {
            r#type: JsonConstructorType::JSCTOR_JSON_OBJECT,
            args: NodeList::nil(),
            func: None,
            coercion: None,
            returning: None,
            absent_on_null: false,
            unique: false,
            location: -1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JsonValueType {
    #[default]
    JS_TYPE_ANY = 0,
    JS_TYPE_OBJECT = 1,
    JS_TYPE_ARRAY = 2,
    JS_TYPE_SCALAR = 3,
}

pub struct JsonIsPredicate<'mcx> {
    pub expr: Option<Node<'mcx>>,
    pub format: Option<&'mcx JsonFormat>,
    pub item_type: JsonValueType,
    pub unique_keys: bool,
    pub location: ParseLoc,
}

impl Default for JsonIsPredicate<'_> {
    fn default() -> Self {
        JsonIsPredicate {
            expr: None,
            format: None,
            item_type: JsonValueType::JS_TYPE_ANY,
            unique_keys: false,
            location: -1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JsonWrapper {
    #[default]
    JSW_UNSPEC = 0,
    JSW_NONE = 1,
    JSW_CONDITIONAL = 2,
    JSW_UNCONDITIONAL = 3,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JsonBehaviorType {
    #[default]
    JSON_BEHAVIOR_NULL = 0,
    JSON_BEHAVIOR_ERROR = 1,
    JSON_BEHAVIOR_EMPTY = 2,
    JSON_BEHAVIOR_TRUE = 3,
    JSON_BEHAVIOR_FALSE = 4,
    JSON_BEHAVIOR_UNKNOWN = 5,
    JSON_BEHAVIOR_EMPTY_ARRAY = 6,
    JSON_BEHAVIOR_EMPTY_OBJECT = 7,
    JSON_BEHAVIOR_DEFAULT = 8,
}

pub struct JsonBehavior<'mcx> {
    pub btype: JsonBehaviorType,
    pub expr: Option<Node<'mcx>>,
    pub coerce: bool,
    pub location: ParseLoc,
}

impl Default for JsonBehavior<'_> {
    fn default() -> Self {
        JsonBehavior {
            btype: JsonBehaviorType::JSON_BEHAVIOR_NULL,
            expr: None,
            coerce: false,
            location: -1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum JsonExprOp {
    #[default]
    JSON_EXISTS_OP = 0,
    JSON_QUERY_OP = 1,
    JSON_VALUE_OP = 2,
    JSON_TABLE_OP = 3,
}

pub struct JsonExpr<'mcx> {
    pub op: JsonExprOp,
    pub column_name: Option<&'mcx str>,
    pub formatted_expr: Option<Node<'mcx>>,
    pub format: Option<&'mcx JsonFormat>,
    pub path_spec: Option<Node<'mcx>>,
    pub returning: Option<&'mcx JsonReturning<'mcx>>,
    pub passing_names: NodeList<'mcx>,
    pub passing_values: NodeList<'mcx>,
    pub on_empty: Option<Node<'mcx>>,
    pub on_error: Option<Node<'mcx>>,
    pub use_io_coercion: bool,
    pub use_json_coercion: bool,
    pub wrapper: JsonWrapper,
    pub omit_quotes: bool,
    pub collation: Oid,
    pub location: ParseLoc,
}

impl Default for JsonExpr<'_> {
    fn default() -> Self {
        JsonExpr {
            op: JsonExprOp::JSON_EXISTS_OP,
            column_name: None,
            formatted_expr: None,
            format: None,
            path_spec: None,
            returning: None,
            passing_names: NodeList::nil(),
            passing_values: NodeList::nil(),
            on_empty: None,
            on_error: None,
            use_io_coercion: false,
            use_json_coercion: false,
            wrapper: JsonWrapper::JSW_UNSPEC,
            omit_quotes: false,
            collation: 0,
            location: -1,
        }
    }
}

#[derive(Default)]
pub struct JsonTablePath<'mcx> {
    pub value: Option<Node<'mcx>>,
    pub name: Option<&'mcx str>,
}

pub struct JsonTablePathScan<'mcx> {
    pub path: Option<Node<'mcx>>,
    // Significant only on the top-level path's plan.
    pub errorOnError: bool,
    pub child: Option<Node<'mcx>>,
    pub colMin: i32,
    pub colMax: i32,
}

impl Default for JsonTablePathScan<'_> {
    fn default() -> Self {
        JsonTablePathScan { path: None, errorOnError: false, child: None, colMin: -1, colMax: -1 }
    }
}

#[derive(Default)]
pub struct JsonTableSiblingJoin<'mcx> {
    pub lplan: Option<Node<'mcx>>,
    pub rplan: Option<Node<'mcx>>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum OnConflictAction {
    #[default]
    ONCONFLICT_NONE = 0,
    ONCONFLICT_NOTHING = 1,
    ONCONFLICT_UPDATE = 2,
}

#[derive(Default)]
pub struct InferenceElem<'mcx> {
    pub expr: Option<Node<'mcx>>,
    pub infercollid: Oid,
    pub inferopclass: Oid,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum MergeMatchKind {
    #[default]
    MERGE_WHEN_MATCHED = 0,
    MERGE_WHEN_NOT_MATCHED_BY_SOURCE = 1,
    MERGE_WHEN_NOT_MATCHED_BY_TARGET = 2,
}

pub const NUM_MERGE_MATCH_KINDS: usize = 3;

/// `targetList` cells are TargetEntry; `updateColnos` set for UPDATE actions.
#[derive(Default)]
pub struct MergeAction<'mcx> {
    pub matchKind: MergeMatchKind,
    pub commandType: crate::nodes_enums::CmdType,
    pub r#override: OverridingKind,
    pub qual: Option<Node<'mcx>>,
    pub targetList: NodeList<'mcx>,
    pub updateColnos: crate::list::IntList<'mcx>,
}

/// `arbiterElems` cells are InferenceElem; `onConflictSet`/`exclRelTlist`
/// cells are TargetEntry.
#[derive(Default)]
pub struct OnConflictExpr<'mcx> {
    pub action: OnConflictAction,
    pub arbiterElems: NodeList<'mcx>,
    pub arbiterWhere: Option<Node<'mcx>>,
    pub constraint: Oid,
    pub onConflictSet: NodeList<'mcx>,
    pub onConflictWhere: Option<Node<'mcx>>,
    pub exclRelIndex: i32,
    pub exclRelTlist: NodeList<'mcx>,
}

#[derive(Default)]
pub struct CollateClause<'mcx> {
    pub arg: Option<Node<'mcx>>,
    pub collname: NodeList<'mcx>,
    pub location: ParseLoc,
}

#[derive(Default)]
pub struct SubscriptingRef<'mcx> {
    pub refcontainertype: Oid,
    pub refelemtype: Oid,
    pub refrestype: Oid,
    pub reftypmod: i32,
    pub refcollid: Oid,
    pub refupperindexpr: OptNodeList<'mcx>,
    pub reflowerindexpr: OptNodeList<'mcx>,
    pub refexpr: Option<Node<'mcx>>,
    pub refassgnexpr: Option<Node<'mcx>>,
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

#[derive(Default)]
pub struct NamedArgExpr<'mcx> {
    pub arg: Option<Node<'mcx>>,
    pub name: Option<&'mcx str>,
    pub argnumber: i32,
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
unsafe impl<'mcx> NodeVariant<'mcx> for CollateClause<'mcx> {
    const TAG: NodeTag = NodeTag::T_CollateClause;
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
unsafe impl<'mcx> NodeVariant<'mcx> for FieldStore<'mcx> {
    const TAG: NodeTag = NodeTag::T_FieldStore;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RowCompareExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_RowCompareExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RowExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_RowExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for WindowFunc<'mcx> {
    const TAG: NodeTag = NodeTag::T_WindowFunc;
}
unsafe impl<'mcx> NodeVariant<'mcx> for WindowFuncRunCondition<'mcx> {
    const TAG: NodeTag = NodeTag::T_WindowFuncRunCondition;
}
unsafe impl NodeVariant<'_> for MergeSupportFunc {
    const TAG: NodeTag = NodeTag::T_MergeSupportFunc;
}
unsafe impl NodeVariant<'_> for JsonFormat {
    const TAG: NodeTag = NodeTag::T_JsonFormat;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonReturning<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonReturning;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonValueExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonValueExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonConstructorExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonConstructorExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonIsPredicate<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonIsPredicate;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonBehavior<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonBehavior;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonExpr;
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
unsafe impl<'mcx> NodeVariant<'mcx> for CurrentOfExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_CurrentOfExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JoinExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_JoinExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for OpExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_OpExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ScalarArrayOpExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_ScalarArrayOpExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ArrayExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_ArrayExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for FuncExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_FuncExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for NamedArgExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_NamedArgExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SubscriptingRef<'mcx> {
    const TAG: NodeTag = NodeTag::T_SubscriptingRef;
}
unsafe impl<'mcx> NodeVariant<'mcx> for RelabelType<'mcx> {
    const TAG: NodeTag = NodeTag::T_RelabelType;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ReturningExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_ReturningExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for FieldSelect<'mcx> {
    const TAG: NodeTag = NodeTag::T_FieldSelect;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CoerceViaIO<'mcx> {
    const TAG: NodeTag = NodeTag::T_CoerceViaIO;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ArrayCoerceExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_ArrayCoerceExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for ConvertRowtypeExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_ConvertRowtypeExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CoerceToDomain<'mcx> {
    const TAG: NodeTag = NodeTag::T_CoerceToDomain;
}
unsafe impl NodeVariant<'_> for CoerceToDomainValue {
    const TAG: NodeTag = NodeTag::T_CoerceToDomainValue;
}
unsafe impl<'mcx> NodeVariant<'mcx> for NextValueExpr {
    const TAG: NodeTag = NodeTag::T_NextValueExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for BoolExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_BoolExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for NullTest<'mcx> {
    const TAG: NodeTag = NodeTag::T_NullTest;
}
unsafe impl<'mcx> NodeVariant<'mcx> for BooleanTest<'mcx> {
    const TAG: NodeTag = NodeTag::T_BooleanTest;
}
unsafe impl<'mcx> NodeVariant<'mcx> for DistinctExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_DistinctExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for NullIfExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_NullIfExpr;
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
unsafe impl<'mcx> NodeVariant<'mcx> for XmlExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_XmlExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonTablePath<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonTablePath;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonTablePathScan<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonTablePathScan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for JsonTableSiblingJoin<'mcx> {
    const TAG: NodeTag = NodeTag::T_JsonTableSiblingJoin;
}
unsafe impl<'mcx> NodeVariant<'mcx> for TableFunc<'mcx> {
    const TAG: NodeTag = NodeTag::T_TableFunc;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SubLink<'mcx> {
    const TAG: NodeTag = NodeTag::T_SubLink;
}
unsafe impl<'mcx> NodeVariant<'mcx> for CollateExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_CollateExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for SubPlan<'mcx> {
    const TAG: NodeTag = NodeTag::T_SubPlan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for PlaceHolderVar<'mcx> {
    const TAG: NodeTag = NodeTag::T_PlaceHolderVar;
}
unsafe impl<'mcx> NodeVariant<'mcx> for AlternativeSubPlan<'mcx> {
    const TAG: NodeTag = NodeTag::T_AlternativeSubPlan;
}
unsafe impl<'mcx> NodeVariant<'mcx> for InferenceElem<'mcx> {
    const TAG: NodeTag = NodeTag::T_InferenceElem;
}
unsafe impl<'mcx> NodeVariant<'mcx> for OnConflictExpr<'mcx> {
    const TAG: NodeTag = NodeTag::T_OnConflictExpr;
}
unsafe impl<'mcx> NodeVariant<'mcx> for MergeAction<'mcx> {
    const TAG: NodeTag = NodeTag::T_MergeAction;
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
    pub fn as_row_expr(self) -> Option<&'mcx RowExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_field_store(self) -> Option<&'mcx FieldStore<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_row_compare_expr(self) -> Option<&'mcx RowCompareExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_window_func(self) -> Option<&'mcx WindowFunc<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_window_func_run_condition(self) -> Option<&'mcx WindowFuncRunCondition<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_merge_support_func(self) -> Option<&'mcx MergeSupportFunc> {
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
    pub fn as_inference_elem(self) -> Option<&'mcx InferenceElem<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_on_conflict_expr(self) -> Option<&'mcx OnConflictExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_merge_action(self) -> Option<&'mcx MergeAction<'mcx>> {
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
    pub fn as_current_of_expr(self) -> Option<&'mcx CurrentOfExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_op_expr(self) -> Option<&'mcx OpExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_scalar_array_op_expr(self) -> Option<&'mcx ScalarArrayOpExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_array_expr(self) -> Option<&'mcx ArrayExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_relabel_type(self) -> Option<&'mcx RelabelType<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_field_select(self) -> Option<&'mcx FieldSelect<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_returning_expr(self) -> Option<&'mcx ReturningExpr<'mcx>> {
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
    pub fn as_named_arg_expr(self) -> Option<&'mcx NamedArgExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_subscripting_ref(self) -> Option<&'mcx SubscriptingRef<'mcx>> {
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
    pub fn as_array_coerce_expr(self) -> Option<&'mcx ArrayCoerceExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_convert_rowtype_expr(self) -> Option<&'mcx ConvertRowtypeExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_coerce_to_domain(self) -> Option<&'mcx CoerceToDomain<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_coerce_to_domain_value(self) -> Option<&'mcx CoerceToDomainValue> {
        self.as_variant()
    }

    #[inline]
    pub fn as_null_test(self) -> Option<&'mcx NullTest<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_boolean_test(self) -> Option<&'mcx BooleanTest<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_distinct_expr(self) -> Option<&'mcx DistinctExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_null_if_expr(self) -> Option<&'mcx NullIfExpr<'mcx>> {
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

    #[inline]
    pub fn as_place_holder_var(self) -> Option<&'mcx PlaceHolderVar<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_collate_expr(self) -> Option<&'mcx CollateExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_format(self) -> Option<&'mcx JsonFormat> {
        self.as_variant()
    }

    #[inline]
    pub fn as_xml_expr(self) -> Option<&'mcx XmlExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_returning(self) -> Option<&'mcx JsonReturning<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_value_expr(self) -> Option<&'mcx JsonValueExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_constructor_expr(self) -> Option<&'mcx JsonConstructorExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_is_predicate(self) -> Option<&'mcx JsonIsPredicate<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_behavior(self) -> Option<&'mcx JsonBehavior<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_expr(self) -> Option<&'mcx JsonExpr<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_table_func(self) -> Option<&'mcx TableFunc<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_table_path(self) -> Option<&'mcx JsonTablePath<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_table_path_scan(self) -> Option<&'mcx JsonTablePathScan<'mcx>> {
        self.as_variant()
    }

    #[inline]
    pub fn as_json_table_sibling_join(self) -> Option<&'mcx JsonTableSiblingJoin<'mcx>> {
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

pub const MONOTONICFUNC_NONE: i32 = 0;
pub const MONOTONICFUNC_INCREASING: i32 = 1 << 0;
pub const MONOTONICFUNC_DECREASING: i32 = 1 << 1;
pub const MONOTONICFUNC_BOTH: i32 = MONOTONICFUNC_INCREASING | MONOTONICFUNC_DECREASING;

// SupportRequestWFuncMonotonic (supportnodes.h); the window_func/
// window_clause pointers are narrowed to the fields in-core prosupports
// read (C divergence, same treatment as OptimizeWindowClause above).
#[repr(C)]
pub struct SupportRequestWFuncMonotonic {
    pub tag: crate::NodeTag,
    pub order_clause_empty: bool,
    pub frame_options: i32,
    pub winfnoid: Oid,
    pub agg_has_filter: bool,
    pub monotonic: i32,
}
