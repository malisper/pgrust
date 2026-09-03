//! P-NODEWALKER rail (bug catalog PR1604-1): every expression NodeTag that C's
//! nodeFuncs.c accessor switches handle must be handled by the nodes_core
//! accessors too — no panic, no "unrecognized node type" fallthrough.
//!
//! The class: a walker arm missing for one expression tag. On main such a
//! gap is masked by an execscan-local wrapper (execscan/src/lib.rs
//! expr_collation answers T_NextValueExpr before delegating), so no SQL path
//! reaches it and no differential harness sees it — the moment a Result-tlist
//! walk (exec_type_from_tl) is re-pointed at nodes_core, every INSERT into an
//! identity-column table panics. This rail exercises nodes_core directly with
//! one well-formed node per tag.
//!
//! Tag universe: the `case T_*` arms of exprType (PostgreSQL 18.6
//! src/backend/nodes/nodeFuncs.c:51-284) — exprCollation switches over the
//! identical set (nodeFuncs.c:830-1058). exprTypmod (:308-537, `default:
//! break` → -1 at :539-540) and exprLocation (:1392-1799, `default: break` →
//! -1 at :1802-1805) handle subsets; the tags each leaves to its default arm
//! are listed explicitly below and asserted to answer C's -1.
use std::panic::{catch_unwind, AssertUnwindSafe};

use datum::Datum;
use mcx::{Mcx, MemoryContext};
use types_core::Oid;
use types_nodes::parsenodes::Query;
use types_nodes::primnodes::{
    Aggref, AlternativeSubPlan, ArrayCoerceExpr, ArrayExpr, BoolExpr, BoolExprType, BooleanTest,
    CaseExpr, CaseTestExpr, CaseWhen, CoalesceExpr, CoerceToDomain, CoerceToDomainValue,
    CoerceViaIO, CoercionForm, CollateExpr, Const, ConvertRowtypeExpr, CurrentOfExpr,
    DistinctExpr, FieldSelect, FieldStore, FuncExpr, GroupingFunc, InferenceElem, JsonBehavior,
    JsonBehaviorType, JsonConstructorExpr, JsonConstructorType, JsonExpr, JsonExprOp,
    JsonIsPredicate, JsonReturning, JsonValueExpr, MergeSupportFunc, MinMaxExpr, NamedArgExpr,
    NextValueExpr, NullIfExpr, NullTest, OpExpr, Param, ParamKind, PlaceHolderVar, RelabelType,
    ReturningExpr, RowCompareExpr, RowExpr, SQLValueFunction, SQLValueFunctionOp,
    ScalarArrayOpExpr, SetToDefault, SubLink, SubLinkType, SubPlan, SubscriptingRef, Var,
    WindowFunc, XmlExpr, XmlExprOp,
};
use types_nodes::{Bitmapset, IntList, Node, NodeList, NodeTag, OptNodeList};

use crate::node_funcs::{expr_collation, expr_location, expr_type, expr_typmod};

const INT4OID: Oid = 23;
const BOOLOID: Oid = 16;
const INT8OID: Oid = 20;
const TEXTOID: Oid = 25;
const XMLOID: Oid = 142;
const JSONOID: Oid = 114;
const DATEOID: Oid = 1082;
const RECORDOID: Oid = 2249;
const INT4ARRAYOID: Oid = 1007;
const DEFAULT_COLLATION_OID: Oid = 100;

/// The 49 expression tags C's exprType switch handles (nodeFuncs.c:51-284),
/// in C's arm order. exprCollation's switch (nodeFuncs.c:830-1058) covers the
/// same 49.
const EXPR_TAGS: &[NodeTag] = &[
    NodeTag::T_Var,                 // :51
    NodeTag::T_Const,               // :54
    NodeTag::T_Param,               // :57
    NodeTag::T_Aggref,              // :60
    NodeTag::T_GroupingFunc,        // :63
    NodeTag::T_WindowFunc,          // :66
    NodeTag::T_MergeSupportFunc,    // :69
    NodeTag::T_SubscriptingRef,     // :72
    NodeTag::T_FuncExpr,            // :75
    NodeTag::T_NamedArgExpr,        // :78
    NodeTag::T_OpExpr,              // :81
    NodeTag::T_DistinctExpr,        // :84
    NodeTag::T_NullIfExpr,          // :87
    NodeTag::T_ScalarArrayOpExpr,   // :90
    NodeTag::T_BoolExpr,            // :93
    NodeTag::T_SubLink,             // :96
    NodeTag::T_SubPlan,             // :134
    NodeTag::T_AlternativeSubPlan,  // :165
    NodeTag::T_FieldSelect,         // :173
    NodeTag::T_FieldStore,          // :176
    NodeTag::T_RelabelType,         // :179
    NodeTag::T_CoerceViaIO,         // :182
    NodeTag::T_ArrayCoerceExpr,     // :185
    NodeTag::T_ConvertRowtypeExpr,  // :188
    NodeTag::T_CollateExpr,         // :191
    NodeTag::T_CaseExpr,            // :194
    NodeTag::T_CaseTestExpr,        // :197
    NodeTag::T_ArrayExpr,           // :200
    NodeTag::T_RowExpr,             // :203
    NodeTag::T_RowCompareExpr,      // :206
    NodeTag::T_CoalesceExpr,        // :209
    NodeTag::T_MinMaxExpr,          // :212
    NodeTag::T_SQLValueFunction,    // :215
    NodeTag::T_XmlExpr,             // :218
    NodeTag::T_JsonValueExpr,       // :226
    NodeTag::T_JsonConstructorExpr, // :233
    NodeTag::T_JsonIsPredicate,     // :236
    NodeTag::T_JsonExpr,            // :239
    NodeTag::T_JsonBehavior,        // :246
    NodeTag::T_NullTest,            // :253
    NodeTag::T_BooleanTest,         // :256
    NodeTag::T_CoerceToDomain,      // :259
    NodeTag::T_CoerceToDomainValue, // :262
    NodeTag::T_SetToDefault,        // :265
    NodeTag::T_CurrentOfExpr,       // :268
    NodeTag::T_NextValueExpr,       // :271
    NodeTag::T_InferenceElem,       // :274
    NodeTag::T_ReturningExpr,       // :281
    NodeTag::T_PlaceHolderVar,      // :284
];

/// Expression tags exprTypmod has no arm for: they reach `default: break;`
/// and return -1 (nodeFuncs.c:539-540, 543). Everything else in EXPR_TAGS
/// has a `case` at nodeFuncs.c:308-537.
const TYPMOD_C_DEFAULT: &[NodeTag] = &[
    NodeTag::T_Aggref,
    NodeTag::T_GroupingFunc,
    NodeTag::T_WindowFunc,
    NodeTag::T_MergeSupportFunc,
    NodeTag::T_OpExpr,
    NodeTag::T_DistinctExpr,
    NodeTag::T_ScalarArrayOpExpr,
    NodeTag::T_BoolExpr,
    NodeTag::T_FieldStore,
    NodeTag::T_CoerceViaIO,
    NodeTag::T_ConvertRowtypeExpr,
    NodeTag::T_RowExpr,
    NodeTag::T_RowCompareExpr,
    NodeTag::T_XmlExpr,
    NodeTag::T_JsonIsPredicate,
    NodeTag::T_NullTest,
    NodeTag::T_BooleanTest,
    NodeTag::T_CurrentOfExpr,
    NodeTag::T_NextValueExpr,
    NodeTag::T_InferenceElem,
];

/// Expression tags exprLocation has no arm for: `default: break;` → -1
/// (nodeFuncs.c:1802-1805). Everything else in EXPR_TAGS has a `case` at
/// nodeFuncs.c:1392-1799.
const LOCATION_C_DEFAULT: &[NodeTag] = &[
    NodeTag::T_SubPlan,
    NodeTag::T_AlternativeSubPlan,
    NodeTag::T_CaseTestExpr,
    NodeTag::T_CurrentOfExpr,
    NodeTag::T_NextValueExpr,
];

/// Arms C has that nodes_core is KNOWN to lack on this branch: recorded, not
/// fixed here (the fix is PR #1604: `NodeTag::T_NextValueExpr =>
/// types_core::InvalidOid` in expr_collation; C nodeFuncs.c:1048-1051
/// "NextValueExpr's result is an integer type ... so it has no collation").
/// When an entry's arm lands, this test fails with "expected-missing arm now
/// handled" and the entry must be deleted; a NEW gap anywhere fails with the
/// fallthrough message. Both directions are exact-set comparisons so the
/// rail never goes silently stale.
const KNOWN_MISSING_ARMS: &[(&str, NodeTag)] = &[("expr_collation", NodeTag::T_NextValueExpr)];

fn int4_const(mcx: Mcx<'_>, v: i32) -> Node<'_> {
    Node::mk(
        mcx,
        Const {
            consttype: INT4OID,
            consttypmod: -1,
            constcollid: 0,
            constlen: 4,
            constvalue: Datum::from_i32(v),
            constisnull: false,
            constbyval: true,
            location: 7,
        },
    )
    .unwrap()
}

fn bool_const(mcx: Mcx<'_>) -> Node<'_> {
    Node::mk(
        mcx,
        Const {
            consttype: BOOLOID,
            consttypmod: -1,
            constcollid: 0,
            constlen: 1,
            constvalue: Datum::from_i32(1),
            constisnull: false,
            constbyval: true,
            location: 3,
        },
    )
    .unwrap()
}

fn json_returning(mcx: Mcx<'_>) -> &JsonReturning<'_> {
    Node::mk_mut(mcx, JsonReturning { format: None, typid: JSONOID, typmod: -1 })
        .unwrap()
        .seal_ref()
}

fn expr_subplan<'mcx>(mcx: Mcx<'mcx>) -> Node<'mcx> {
    Node::mk(
        mcx,
        SubPlan {
            subLinkType: SubLinkType::EXPR_SUBLINK,
            plan_id: 1,
            plan_name: Some("InitPlan 1"),
            firstColType: INT4OID,
            firstColTypmod: -1,
            firstColCollation: 0,
            ..SubPlan::default()
        },
    )
    .unwrap()
}

/// The smallest well-formed node of each expression tag: every field an
/// accessor dereferences on its way to an answer is populated, and no
/// shape needs a catalog lookup (EXPR sublinks/subplans, XMLCONCAT, a
/// NamedArgExpr with its arg, JSON nodes with their `returning`).
fn build<'mcx>(mcx: Mcx<'mcx>, tag: NodeTag) -> Node<'mcx> {
    let c = int4_const(mcx, 1);
    let pair = NodeList::from_slice(mcx, &[c, c]).unwrap();
    let single = NodeList::from_slice(mcx, &[c]).unwrap();
    match tag {
        NodeTag::T_Var => Node::mk(
            mcx,
            Var {
                varno: 1,
                varattno: 1,
                vartype: INT4OID,
                vartypmod: -1,
                varnosyn: 1,
                varattnosyn: 1,
                location: 5,
                ..Var::default()
            },
        )
        .unwrap(),
        NodeTag::T_Const => c,
        NodeTag::T_Param => Node::mk(
            mcx,
            Param {
                paramkind: ParamKind::PARAM_EXTERN,
                paramid: 1,
                paramtype: INT4OID,
                paramtypmod: -1,
                paramcollid: 0,
                location: 9,
            },
        )
        .unwrap(),
        NodeTag::T_Aggref => {
            let te = Node::mk_target_entry(mcx, c, 1, None, false).unwrap();
            Node::mk(
                mcx,
                Aggref {
                    aggfnoid: 2108,
                    aggtype: INT8OID,
                    aggtranstype: INT8OID,
                    args: NodeList::from_slice(mcx, &[te]).unwrap(),
                    location: 11,
                    ..Aggref::default()
                },
            )
            .unwrap()
        }
        NodeTag::T_GroupingFunc => Node::mk(
            mcx,
            GroupingFunc { args: single, location: 13, ..GroupingFunc::default() },
        )
        .unwrap(),
        NodeTag::T_WindowFunc => Node::mk(
            mcx,
            WindowFunc {
                winfnoid: 3100,
                wintype: INT8OID,
                winref: 1,
                location: 15,
                ..WindowFunc::default()
            },
        )
        .unwrap(),
        NodeTag::T_MergeSupportFunc => Node::mk(
            mcx,
            MergeSupportFunc { msftype: TEXTOID, msfcollid: DEFAULT_COLLATION_OID, location: 17 },
        )
        .unwrap(),
        NodeTag::T_SubscriptingRef => Node::mk(
            mcx,
            SubscriptingRef {
                refcontainertype: INT4ARRAYOID,
                refelemtype: INT4OID,
                refrestype: INT4OID,
                reftypmod: -1,
                refcollid: 0,
                refupperindexpr: OptNodeList::from_slice(mcx, &[Some(c)]).unwrap(),
                refexpr: Some(c),
                ..SubscriptingRef::default()
            },
        )
        .unwrap(),
        NodeTag::T_FuncExpr => Node::mk(
            mcx,
            FuncExpr {
                funcid: 1,
                funcresulttype: INT4OID,
                funcformat: CoercionForm::COERCE_EXPLICIT_CALL,
                args: single,
                location: 19,
                ..FuncExpr::default()
            },
        )
        .unwrap(),
        NodeTag::T_NamedArgExpr => Node::mk(
            mcx,
            NamedArgExpr { arg: Some(c), name: Some("x"), argnumber: 0, location: 21 },
        )
        .unwrap(),
        NodeTag::T_OpExpr => Node::mk(
            mcx,
            OpExpr {
                opno: 96,
                opfuncid: 65,
                opresulttype: BOOLOID,
                args: pair,
                location: 23,
                ..OpExpr::default()
            },
        )
        .unwrap(),
        NodeTag::T_DistinctExpr => Node::mk(
            mcx,
            DistinctExpr {
                opno: 96,
                opfuncid: 65,
                opresulttype: BOOLOID,
                args: pair,
                location: 25,
                ..DistinctExpr::default()
            },
        )
        .unwrap(),
        NodeTag::T_NullIfExpr => Node::mk(
            mcx,
            NullIfExpr {
                opno: 96,
                opfuncid: 65,
                opresulttype: INT4OID,
                args: pair,
                location: 27,
                ..NullIfExpr::default()
            },
        )
        .unwrap(),
        NodeTag::T_ScalarArrayOpExpr => Node::mk(
            mcx,
            ScalarArrayOpExpr {
                opno: 96,
                opfuncid: 65,
                useOr: true,
                args: pair,
                location: 29,
                ..ScalarArrayOpExpr::default()
            },
        )
        .unwrap(),
        NodeTag::T_BoolExpr => Node::mk(
            mcx,
            BoolExpr {
                boolop: BoolExprType::NOT_EXPR,
                args: NodeList::from_slice(mcx, &[bool_const(mcx)]).unwrap(),
                location: 31,
            },
        )
        .unwrap(),
        NodeTag::T_SubLink => {
            let te = Node::mk_target_entry(mcx, c, 1, None, false).unwrap();
            let subselect = Node::mk(
                mcx,
                Query {
                    targetList: NodeList::from_slice(mcx, &[te]).unwrap(),
                    ..Query::default()
                },
            )
            .unwrap();
            Node::mk(
                mcx,
                SubLink {
                    subLinkType: SubLinkType::EXPR_SUBLINK,
                    subLinkId: 0,
                    testexpr: None,
                    operName: NodeList::nil(),
                    subselect,
                    location: 33,
                },
            )
            .unwrap()
        }
        NodeTag::T_SubPlan => expr_subplan(mcx),
        NodeTag::T_AlternativeSubPlan => Node::mk(
            mcx,
            AlternativeSubPlan {
                subplans: NodeList::from_slice(mcx, &[expr_subplan(mcx), expr_subplan(mcx)])
                    .unwrap(),
            },
        )
        .unwrap(),
        NodeTag::T_FieldSelect => Node::mk(
            mcx,
            FieldSelect { arg: c, fieldnum: 1, resulttype: INT4OID, resulttypmod: -1, resultcollid: 0 },
        )
        .unwrap(),
        NodeTag::T_FieldStore => Node::mk(
            mcx,
            FieldStore {
                arg: c,
                newvals: single,
                fieldnums: IntList::from_slice(mcx, &[1]).unwrap(),
                resulttype: RECORDOID,
            },
        )
        .unwrap(),
        NodeTag::T_RelabelType => Node::mk(
            mcx,
            RelabelType {
                arg: c,
                resulttype: INT4OID,
                resulttypmod: -1,
                resultcollid: 0,
                relabelformat: CoercionForm::COERCE_IMPLICIT_CAST,
                location: 35,
            },
        )
        .unwrap(),
        NodeTag::T_CoerceViaIO => Node::mk(
            mcx,
            CoerceViaIO {
                arg: c,
                resulttype: TEXTOID,
                resultcollid: DEFAULT_COLLATION_OID,
                coerceformat: CoercionForm::COERCE_EXPLICIT_CAST,
                location: 37,
            },
        )
        .unwrap(),
        NodeTag::T_ArrayCoerceExpr => Node::mk(
            mcx,
            ArrayCoerceExpr {
                arg: c,
                elemexpr: None,
                resulttype: INT4ARRAYOID,
                resulttypmod: -1,
                resultcollid: 0,
                coerceformat: CoercionForm::COERCE_EXPLICIT_CAST,
                location: 39,
            },
        )
        .unwrap(),
        NodeTag::T_ConvertRowtypeExpr => Node::mk(
            mcx,
            ConvertRowtypeExpr {
                arg: c,
                resulttype: RECORDOID,
                convertformat: CoercionForm::COERCE_EXPLICIT_CAST,
                location: 41,
            },
        )
        .unwrap(),
        NodeTag::T_CollateExpr => {
            Node::mk(mcx, CollateExpr { arg: c, collOid: DEFAULT_COLLATION_OID, location: 43 })
                .unwrap()
        }
        NodeTag::T_CaseExpr => {
            let when = Node::mk(
                mcx,
                CaseWhen { expr: Some(bool_const(mcx)), result: Some(c), location: 45 },
            )
            .unwrap();
            Node::mk(
                mcx,
                CaseExpr {
                    casetype: INT4OID,
                    casecollid: 0,
                    arg: None,
                    args: NodeList::from_slice(mcx, &[when]).unwrap(),
                    defresult: Some(c),
                    location: 47,
                },
            )
            .unwrap()
        }
        NodeTag::T_CaseTestExpr => {
            Node::mk(mcx, CaseTestExpr { typeId: INT4OID, typeMod: -1, collation: 0 }).unwrap()
        }
        NodeTag::T_ArrayExpr => Node::mk(
            mcx,
            ArrayExpr {
                array_typeid: INT4ARRAYOID,
                element_typeid: INT4OID,
                elements: single,
                location: 49,
                ..ArrayExpr::default()
            },
        )
        .unwrap(),
        NodeTag::T_RowExpr => Node::mk(
            mcx,
            RowExpr { args: single, row_typeid: RECORDOID, location: 51, ..RowExpr::default() },
        )
        .unwrap(),
        NodeTag::T_RowCompareExpr => Node::mk(
            mcx,
            RowCompareExpr {
                largs: single,
                rargs: NodeList::from_slice(mcx, &[c]).unwrap(),
                ..RowCompareExpr::default()
            },
        )
        .unwrap(),
        NodeTag::T_CoalesceExpr => Node::mk(
            mcx,
            CoalesceExpr { coalescetype: INT4OID, coalescecollid: 0, args: single, location: 53 },
        )
        .unwrap(),
        NodeTag::T_MinMaxExpr => Node::mk(
            mcx,
            MinMaxExpr { minmaxtype: INT4OID, args: single, location: 55, ..MinMaxExpr::default() },
        )
        .unwrap(),
        NodeTag::T_SQLValueFunction => Node::mk(
            mcx,
            SQLValueFunction {
                op: SQLValueFunctionOp::SVFOP_CURRENT_DATE,
                r#type: DATEOID,
                typmod: -1,
                location: 57,
            },
        )
        .unwrap(),
        NodeTag::T_XmlExpr => Node::mk(
            mcx,
            XmlExpr {
                op: XmlExprOp::IS_XMLCONCAT,
                args: single,
                r#type: XMLOID,
                typmod: -1,
                location: 59,
                ..XmlExpr::default()
            },
        )
        .unwrap(),
        NodeTag::T_JsonValueExpr => Node::mk(
            mcx,
            JsonValueExpr { raw_expr: Some(c), formatted_expr: Some(c), format: None },
        )
        .unwrap(),
        NodeTag::T_JsonConstructorExpr => Node::mk(
            mcx,
            JsonConstructorExpr {
                r#type: JsonConstructorType::JSCTOR_JSON_ARRAY,
                args: single,
                returning: Some(json_returning(mcx)),
                location: 61,
                ..JsonConstructorExpr::default()
            },
        )
        .unwrap(),
        NodeTag::T_JsonIsPredicate => Node::mk(
            mcx,
            JsonIsPredicate { expr: Some(c), location: 63, ..JsonIsPredicate::default() },
        )
        .unwrap(),
        NodeTag::T_JsonExpr => Node::mk(
            mcx,
            JsonExpr {
                op: JsonExprOp::JSON_QUERY_OP,
                formatted_expr: Some(c),
                path_spec: Some(c),
                returning: Some(json_returning(mcx)),
                location: 65,
                ..JsonExpr::default()
            },
        )
        .unwrap(),
        NodeTag::T_JsonBehavior => Node::mk(
            mcx,
            JsonBehavior {
                btype: JsonBehaviorType::JSON_BEHAVIOR_DEFAULT,
                expr: Some(c),
                coerce: false,
                location: 67,
            },
        )
        .unwrap(),
        NodeTag::T_NullTest => {
            Node::mk(mcx, NullTest { arg: Some(c), location: 69, ..NullTest::default() }).unwrap()
        }
        NodeTag::T_BooleanTest => Node::mk(
            mcx,
            BooleanTest { arg: Some(bool_const(mcx)), location: 71, ..BooleanTest::default() },
        )
        .unwrap(),
        NodeTag::T_CoerceToDomain => Node::mk(
            mcx,
            CoerceToDomain {
                arg: c,
                resulttype: INT4OID,
                resulttypmod: -1,
                resultcollid: 0,
                coercionformat: CoercionForm::COERCE_IMPLICIT_CAST,
                location: 73,
            },
        )
        .unwrap(),
        NodeTag::T_CoerceToDomainValue => Node::mk(
            mcx,
            CoerceToDomainValue { typeId: INT4OID, typeMod: -1, collation: 0, location: 75 },
        )
        .unwrap(),
        NodeTag::T_SetToDefault => Node::mk(
            mcx,
            SetToDefault { typeId: INT4OID, typeMod: -1, collation: 0, location: 77 },
        )
        .unwrap(),
        NodeTag::T_CurrentOfExpr => Node::mk(
            mcx,
            CurrentOfExpr { cvarno: 1, cursor_name: Some("cur"), cursor_param: 0 },
        )
        .unwrap(),
        // The PR1604-1 shape: an identity column's default expansion.
        NodeTag::T_NextValueExpr => {
            Node::mk(mcx, NextValueExpr { seqid: 16385, typeId: INT4OID }).unwrap()
        }
        NodeTag::T_InferenceElem => Node::mk(
            mcx,
            InferenceElem { expr: Some(c), infercollid: 0, inferopclass: 0 },
        )
        .unwrap(),
        NodeTag::T_ReturningExpr => {
            Node::mk(mcx, ReturningExpr { retlevelsup: 0, retold: false, retexpr: c }).unwrap()
        }
        NodeTag::T_PlaceHolderVar => Node::mk(
            mcx,
            PlaceHolderVar {
                phexpr: c,
                phrels: Bitmapset::empty(),
                phnullingrels: Bitmapset::empty(),
                phid: 1,
                phlevelsup: 0,
            },
        )
        .unwrap(),
        other => panic!("build(): {other:?} is not in EXPR_TAGS"),
    }
}

/// One accessor call, with the panic (if any) captured as its message.
fn probe<T>(f: impl FnOnce() -> T) -> Result<T, std::string::String> {
    catch_unwind(AssertUnwindSafe(f)).map_err(|payload| {
        if let Some(s) = payload.downcast_ref::<&str>() {
            (*s).to_owned()
        } else if let Some(s) = payload.downcast_ref::<std::string::String>() {
            s.clone()
        } else {
            "<non-string panic payload>".to_owned()
        }
    })
}

/// Runs the panic-capturing probes with the default hook silenced, so the
/// expected-missing arm does not spray a backtrace into the test log.
fn quietly<R>(f: impl FnOnce() -> R) -> R {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let r = f();
    std::panic::set_hook(prev);
    r
}

fn tags_equal_as_sets(a: &[NodeTag], b: &[NodeTag]) -> bool {
    a.len() == b.len() && a.iter().all(|t| b.contains(t))
}

#[test]
fn expr_tag_universe_matches_c_and_default_sets_are_subsets() {
    // 49 arms in C 18.6's exprType switch, no duplicates.
    assert_eq!(EXPR_TAGS.len(), 49);
    for (i, t) in EXPR_TAGS.iter().enumerate() {
        assert!(!EXPR_TAGS[..i].contains(t), "duplicate tag {t:?}");
    }
    for t in TYPMOD_C_DEFAULT.iter().chain(LOCATION_C_DEFAULT) {
        assert!(EXPR_TAGS.contains(t), "{t:?} listed as C-default but not an expression tag");
    }
    for (_, t) in KNOWN_MISSING_ARMS {
        assert!(EXPR_TAGS.contains(t), "{t:?} listed as known-missing but not an expression tag");
    }
    assert!(!tags_equal_as_sets(TYPMOD_C_DEFAULT, LOCATION_C_DEFAULT));
}

/// The rail proper: every expression tag through all four accessors.
/// Fallthrough panics are collected and compared EXACTLY against
/// KNOWN_MISSING_ARMS; any other panic is a malformed fixture or a new
/// crash and fails outright.
#[test]
fn every_expression_tag_is_handled_by_the_four_accessors() {
    let ctx = MemoryContext::new_bump("nodes_core-expr-tags");
    let mcx = ctx.mcx();

    let mut fallthroughs: Vec<(&'static str, NodeTag)> = Vec::new();
    let mut other_panics: Vec<std::string::String> = Vec::new();

    quietly(|| {
        for &tag in EXPR_TAGS {
            let node = build(mcx, tag);
            let results: [(&'static str, Result<i64, std::string::String>); 4] = [
                ("expr_type", probe(|| expr_type(node) as i64)),
                ("expr_typmod", probe(|| expr_typmod(node) as i64)),
                ("expr_collation", probe(|| expr_collation(node) as i64)),
                ("expr_location", probe(|| expr_location(node) as i64)),
            ];
            for (what, r) in results {
                match r {
                    Ok(v) => {
                        // C's default arms answer -1; a real arm on a
                        // located fixture never does (every fixture with a
                        // C exprLocation arm carries a non-negative
                        // location, directly or through its argument).
                        if what == "expr_typmod" && TYPMOD_C_DEFAULT.contains(&tag) {
                            assert_eq!(v, -1, "{what}({tag:?}): C default arm answers -1");
                        }
                        if what == "expr_location" {
                            if LOCATION_C_DEFAULT.contains(&tag) {
                                assert_eq!(v, -1, "{what}({tag:?}): C default arm answers -1");
                            } else {
                                assert!(
                                    v >= 0,
                                    "{what}({tag:?}) = {v}: C has an arm (nodeFuncs.c:1392-1799) \
                                     that reports a location for this fixture"
                                );
                            }
                        }
                    }
                    Err(msg) if msg.contains("unrecognized node type") => {
                        fallthroughs.push((what, tag));
                    }
                    Err(msg) => other_panics.push(format!("{what}({tag:?}): {msg}")),
                }
            }
        }
    });

    assert!(
        other_panics.is_empty(),
        "accessor panicked outside the unrecognized-node fallthrough:\n{}",
        other_panics.join("\n")
    );

    let unexpected: Vec<_> = fallthroughs
        .iter()
        .filter(|f| !KNOWN_MISSING_ARMS.contains(f))
        .collect();
    assert!(
        unexpected.is_empty(),
        "NEW missing walker arm(s) — C nodeFuncs.c handles these tags, nodes_core fell \
         through to the unrecognized-node panic: {unexpected:?}"
    );

    let now_handled: Vec<_> = KNOWN_MISSING_ARMS
        .iter()
        .filter(|k| !fallthroughs.contains(k))
        .collect();
    assert!(
        now_handled.is_empty(),
        "expected-missing arm now handled — delete from KNOWN_MISSING_ARMS: {now_handled:?}"
    );
}

/// The PR1604-1 answer itself, pinned so the day the arm lands the value is
/// C's: NextValueExpr is an integer, so no collation (nodeFuncs.c:1048-1051).
/// Recorded as a tolerated fallthrough until then (see KNOWN_MISSING_ARMS).
#[test]
fn next_value_expr_collation_is_invalid_oid_once_the_arm_exists() {
    let ctx = MemoryContext::new_bump("nodes_core-nextvalue");
    let mcx = ctx.mcx();
    let node = build(mcx, NodeTag::T_NextValueExpr);
    assert_eq!(expr_type(node), INT4OID);
    assert_eq!(expr_typmod(node), -1);
    assert_eq!(expr_location(node), -1);
    match quietly(|| probe(|| expr_collation(node))) {
        Ok(collid) => assert_eq!(collid, types_core::InvalidOid, "C: NextValueExpr has no collation"),
        Err(msg) => {
            assert!(
                KNOWN_MISSING_ARMS.contains(&("expr_collation", NodeTag::T_NextValueExpr)),
                "expr_collation(NextValueExpr) panicked and is not a recorded gap: {msg}"
            );
            assert!(msg.contains("unrecognized node type"), "unexpected panic: {msg}");
        }
    }
}
