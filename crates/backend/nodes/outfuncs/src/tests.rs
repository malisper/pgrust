use datum::Datum;
use mcx::MemoryContext;
use types_nodes::bitmapset::Bitmapset;
use types_nodes::list::NodeList;
use types_nodes::primnodes::{CoerceToDomainValue, Const, OpExpr, Var};
use types_nodes::Node;

use crate::nodeToString;

// Captured from live PostgreSQL 18.3:
// CREATE TABLE dctest (a int DEFAULT 42, b int CHECK (b > 0)).
const ADBIN_DEFAULT_42: &str = "{CONST :consttype 23 :consttypmod -1 :constcollid 0 \
    :constlen 4 :constbyval true :constisnull false :location -1 :constvalue 4 \
    [ 42 0 0 0 0 0 0 0 ]}";
const CONBIN_B_GT_0: &str = "{OPEXPR :opno 521 :opfuncid 147 :opresulttype 16 \
    :opretset false :opcollid 0 :inputcollid 0 :args ({VAR :varno 1 :varattno 2 \
    :vartype 23 :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 0 \
    :varreturningtype 0 :varnosyn 1 :varattnosyn 2 :location -1} {CONST \
    :consttype 23 :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true \
    :constisnull false :location -1 :constvalue 4 [ 0 0 0 0 0 0 0 0 ]}) \
    :location -1}";

fn int4_const(v: i32) -> Const {
    Const {
        consttype: 23,
        consttypmod: -1,
        constcollid: 0,
        constlen: 4,
        constvalue: Datum::from_i32(v),
        constisnull: false,
        constbyval: true,
        location: 7,
    }
}

#[test]
fn const_matches_live_adbin() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = Node::mk(mcx, int4_const(42)).unwrap();
    assert_eq!(nodeToString(mcx, node).unwrap().as_str(), ADBIN_DEFAULT_42);
}

#[test]
fn opexpr_matches_live_conbin() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let var = Node::mk(
        mcx,
        Var {
            varno: 1,
            varattno: 2,
            vartype: 23,
            vartypmod: -1,
            varcollid: 0,
            varnullingrels: Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
            varnosyn: 1,
            varattnosyn: 2,
            location: 33,
        },
    )
    .unwrap();
    let zero = Node::mk(mcx, int4_const(0)).unwrap();
    let mut args = NodeList::nil();
    args.lappend(mcx, var).unwrap();
    args.lappend(mcx, zero).unwrap();
    let op = Node::mk(
        mcx,
        OpExpr {
            opno: 521,
            opfuncid: 147,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args,
            location: 35,
        },
    )
    .unwrap();
    assert_eq!(nodeToString(mcx, op).unwrap().as_str(), CONBIN_B_GT_0);
}

#[test]
fn round_trips_through_readfuncs_scanner_shape() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = Node::mk(mcx, int4_const(-7)).unwrap();
    let s = nodeToString(mcx, node).unwrap();
    assert!(s.as_str().contains(":constvalue 4 [ 249 255 255 255 255 255 255 255 ]"));
}

// Captured from live PostgreSQL 18.3: CREATE TABLE (e bigint DEFAULT 42).
const ADBIN_BIGINT_DEFAULT_42: &str = "{FUNCEXPR :funcid 481 :funcresulttype 20 \
    :funcretset false :funcvariadic false :funcformat 2 :funccollid 0 \
    :inputcollid 0 :args ({CONST :consttype 23 :consttypmod -1 :constcollid 0 \
    :constlen 4 :constbyval true :constisnull false :location -1 :constvalue 4 \
    [ 42 0 0 0 0 0 0 0 ]}) :location -1}";

#[test]
fn funcexpr_matches_live_adbin_and_round_trips() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut args = NodeList::nil();
    args.lappend(mcx, Node::mk(mcx, int4_const(42)).unwrap()).unwrap();
    let f = Node::mk(
        mcx,
        types_nodes::primnodes::FuncExpr {
            funcid: 481,
            funcresulttype: 20,
            funcretset: false,
            funcvariadic: false,
            funcformat: types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
            funccollid: 0,
            inputcollid: 0,
            args,
            location: 30,
        },
    )
    .unwrap();
    let s = nodeToString(mcx, f).unwrap();
    assert_eq!(s.as_str(), ADBIN_BIGINT_DEFAULT_42);
    let back = readfuncs::stringToNode(mcx, s.as_str()).unwrap();
    let fx = back.as_variant::<types_nodes::primnodes::FuncExpr>().unwrap();
    assert_eq!(fx.funcid, 481);
    assert_eq!(fx.args.len(), 1);
}

// Captured from live PostgreSQL 18.3:
// CREATE DOMAIN posint AS int CHECK (VALUE > 0) NOT NULL.
const CONBIN_POSINT_CHECK: &str = "{OPEXPR :opno 521 :opfuncid 147 :opresulttype 16 \
    :opretset false :opcollid 0 :inputcollid 0 :args ({COERCETODOMAINVALUE :typeId 23 \
    :typeMod -1 :collation 0 :location -1} {CONST :consttype 23 :consttypmod -1 \
    :constcollid 0 :constlen 4 :constbyval true :constisnull false :location -1 \
    :constvalue 4 [ 0 0 0 0 0 0 0 0 ]}) :location -1}";

#[test]
fn domain_check_matches_live_conbin() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let domval = Node::mk(
        mcx,
        CoerceToDomainValue { typeId: 23, typeMod: -1, collation: 0, location: 35 },
    )
    .unwrap();
    let zero = Node::mk(mcx, int4_const(0)).unwrap();
    let op = Node::mk(
        mcx,
        OpExpr {
            opno: 521,
            opfuncid: 147,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, domval, zero).unwrap(),
            location: 41,
        },
    )
    .unwrap();
    assert_eq!(nodeToString(mcx, op).unwrap().as_str(), CONBIN_POSINT_CHECK);
}

#[test]
fn nulltest_saop_write_and_round_trip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let var = Node::mk(
        mcx,
        Var {
            varno: 1,
            varattno: 2,
            vartype: 23,
            vartypmod: -1,
            varcollid: 0,
            varnullingrels: Bitmapset::empty(),
            varlevelsup: 0,
            varreturningtype: types_nodes::primnodes::VarReturningType::VAR_RETURNING_DEFAULT,
            varnosyn: 1,
            varattnosyn: 2,
            location: 25,
        },
    )
    .unwrap();
    let ntest = Node::mk(
        mcx,
        types_nodes::primnodes::NullTest {
            arg: Some(var),
            nulltesttype: types_nodes::primnodes::NullTestType::IS_NOT_NULL,
            argisrow: false,
            location: 25,
        },
    )
    .unwrap();
    let s = nodeToString(mcx, ntest).unwrap();
    assert!(s.as_str().starts_with("{NULLTEST :arg {VAR "));
    assert!(s.as_str().ends_with(":nulltesttype 1 :argisrow false :location -1}"));
    let back = readfuncs::stringToNode(mcx, s.as_str()).unwrap();
    let nt = back.as_variant::<types_nodes::primnodes::NullTest>().unwrap();
    assert!(matches!(nt.nulltesttype, types_nodes::primnodes::NullTestType::IS_NOT_NULL));
    assert!(!nt.argisrow);

    let mut args = NodeList::nil();
    args.lappend(mcx, var).unwrap();
    args.lappend(mcx, Node::mk(mcx, int4_const(3)).unwrap()).unwrap();
    let saop = Node::mk(
        mcx,
        types_nodes::primnodes::ScalarArrayOpExpr {
            opno: 96,
            opfuncid: 65,
            hashfuncid: 0,
            negfuncid: 0,
            useOr: true,
            inputcollid: 0,
            args,
            location: 25,
        },
    )
    .unwrap();
    let s = nodeToString(mcx, saop).unwrap();
    assert!(s.as_str().starts_with(
        "{SCALARARRAYOPEXPR :opno 96 :opfuncid 65 :hashfuncid 0 :negfuncid 0 :useOr true :inputcollid 0 :args ("
    ));
    let back = readfuncs::stringToNode(mcx, s.as_str()).unwrap();
    let sx = back.as_variant::<types_nodes::primnodes::ScalarArrayOpExpr>().unwrap();
    assert_eq!(sx.opno, 96);
    assert!(sx.useOr);
    assert_eq!(sx.args.len(), 2);
}

#[test]
fn copy_boundary_arms_round_trip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let mut when_args = NodeList::nil();
    let when = Node::mk(
        mcx,
        types_nodes::primnodes::CaseWhen {
            expr: Some(Node::mk(mcx, int4_const(1)).unwrap()),
            result: Some(Node::mk(mcx, int4_const(2)).unwrap()),
            location: -1,
        },
    )
    .unwrap();
    when_args.lappend(mcx, when).unwrap();
    let case = Node::mk(
        mcx,
        types_nodes::primnodes::CaseExpr {
            casetype: 23,
            casecollid: 0,
            arg: None,
            args: when_args,
            defresult: Some(Node::mk(mcx, int4_const(0)).unwrap()),
            location: -1,
        },
    )
    .unwrap();

    let mut row_args = NodeList::nil();
    row_args.lappend(mcx, case).unwrap();
    let row = Node::mk(
        mcx,
        types_nodes::primnodes::RowExpr {
            args: row_args,
            row_typeid: 2249,
            row_format: types_nodes::primnodes::CoercionForm::COERCE_IMPLICIT_CAST,
            colnames: NodeList::nil(),
            location: -1,
        },
    )
    .unwrap();

    let collate = Node::mk(
        mcx,
        types_nodes::primnodes::CollateExpr { arg: row, collOid: 100, location: -1 },
    )
    .unwrap();

    let mut mm_args = NodeList::nil();
    mm_args.lappend(mcx, collate).unwrap();
    mm_args.lappend(mcx, Node::mk(mcx, int4_const(9)).unwrap()).unwrap();
    let minmax = Node::mk(
        mcx,
        types_nodes::primnodes::MinMaxExpr {
            minmaxtype: 23,
            minmaxcollid: 0,
            inputcollid: 0,
            op: types_nodes::primnodes::MinMaxOp::IS_LEAST,
            args: mm_args,
            location: -1,
        },
    )
    .unwrap();

    let sref = Node::mk(
        mcx,
        types_nodes::primnodes::SubscriptingRef {
            refcontainertype: 1007,
            refelemtype: 23,
            refrestype: 23,
            reftypmod: -1,
            refcollid: 0,
            refupperindexpr: {
                let mut l = types_nodes::list::OptNodeList::nil();
                l.lappend(mcx, Some(minmax)).unwrap();
                l
            },
            reflowerindexpr: types_nodes::list::OptNodeList::nil(),
            refexpr: Some(Node::mk(mcx, int4_const(5)).unwrap()),
            refassgnexpr: None,
        },
    )
    .unwrap();

    let rmc = Node::mk(
        mcx,
        types_nodes::parsenodes::RowMarkClause {
            rti: 1,
            strength: types_nodes::nodes_enums::LockClauseStrength::LCS_FORUPDATE,
            waitPolicy: types_nodes::nodes_enums::LockWaitPolicy::LockWaitSkip,
            pushedDown: false,
        },
    )
    .unwrap();

    for n in [sref, rmc] {
        let s1 = nodeToString(mcx, n).unwrap();
        let back = readfuncs::stringToNode(mcx, s1.as_str()).unwrap();
        let s2 = nodeToString(mcx, back).unwrap();
        assert_eq!(s1.as_str(), s2.as_str());
    }
}

// Live pg_rewrite ev_action for a recursive CTE view with SEARCH BREADTH
// FIRST + CYCLE. Round-trips readfuncs -> outfuncs byte-for-byte.
#[test]
fn search_cycle_ev_action_roundtrips() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let s = include_str!("../../readfuncs/fixtures/search_cycle_ev_action.txt").trim();
    let n = readfuncs::stringToNode(mcx, s).unwrap();
    let out = nodeToString(mcx, n).unwrap();
    assert_eq!(out.as_str(), s);
}

#[test]
fn notify_stmt_matches_c_format() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let n = Node::mk(
        mcx,
        types_nodes::parsenodes::NotifyStmt { conditionname: Some("chan"), payload: Some("hi") },
    )
    .unwrap();
    assert_eq!(
        nodeToString(mcx, n).unwrap().as_str(),
        "{NOTIFYSTMT :conditionname chan :payload hi}"
    );
}

#[test]
fn notify_stmt_no_payload_matches_c_format() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let n = Node::mk(
        mcx,
        types_nodes::parsenodes::NotifyStmt { conditionname: Some("chan"), payload: None },
    )
    .unwrap();
    assert_eq!(
        nodeToString(mcx, n).unwrap().as_str(),
        "{NOTIFYSTMT :conditionname chan :payload <>}"
    );
}

#[test]
fn next_value_expr_matches_c_format() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let n = Node::mk(mcx, types_nodes::primnodes::NextValueExpr { seqid: 16400, typeId: 20 })
        .unwrap();
    assert_eq!(
        nodeToString(mcx, n).unwrap().as_str(),
        "{NEXTVALUEEXPR :seqid 16400 :typeId 20}"
    );
}

// ROW(a, b) < ROW(c, d): two-column row comparison, as produced for
// `WHERE (a, b) < (c, d)`.
#[test]
fn row_compare_expr_matches_c_format() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let mut opnos = types_nodes::list::OidList::nil();
    opnos.lappend(mcx, 97).unwrap();
    opnos.lappend(mcx, 97).unwrap();
    let mut opfamilies = types_nodes::list::OidList::nil();
    opfamilies.lappend(mcx, 1976).unwrap();
    opfamilies.lappend(mcx, 1976).unwrap();
    let mut inputcollids = types_nodes::list::OidList::nil();
    inputcollids.lappend(mcx, 0).unwrap();
    inputcollids.lappend(mcx, 0).unwrap();

    let mut largs = NodeList::nil();
    largs.lappend(mcx, Node::mk(mcx, int4_const(1)).unwrap()).unwrap();
    largs.lappend(mcx, Node::mk(mcx, int4_const(2)).unwrap()).unwrap();
    let mut rargs = NodeList::nil();
    rargs.lappend(mcx, Node::mk(mcx, int4_const(3)).unwrap()).unwrap();
    rargs.lappend(mcx, Node::mk(mcx, int4_const(4)).unwrap()).unwrap();

    let n = Node::mk(
        mcx,
        types_nodes::primnodes::RowCompareExpr {
            cmptype: 1, // COMPARE_LT
            opnos,
            opfamilies,
            inputcollids,
            largs,
            rargs,
        },
    )
    .unwrap();

    let s1 = nodeToString(mcx, n).unwrap();
    assert_eq!(
        s1.as_str(),
        "{ROWCOMPAREEXPR :cmptype 1 :opnos (o 97 97) :opfamilies (o 1976 1976) \
         :inputcollids (o 0 0) :largs ({CONST :consttype 23 :consttypmod -1 \
         :constcollid 0 :constlen 4 :constbyval true :constisnull false \
         :location -1 :constvalue 4 [ 1 0 0 0 0 0 0 0 ]} {CONST :consttype 23 \
         :consttypmod -1 :constcollid 0 :constlen 4 :constbyval true \
         :constisnull false :location -1 :constvalue 4 [ 2 0 0 0 0 0 0 0 ]}) \
         :rargs ({CONST :consttype 23 :consttypmod -1 :constcollid 0 \
         :constlen 4 :constbyval true :constisnull false :location -1 \
         :constvalue 4 [ 3 0 0 0 0 0 0 0 ]} {CONST :consttype 23 :consttypmod \
         -1 :constcollid 0 :constlen 4 :constbyval true :constisnull false \
         :location -1 :constvalue 4 [ 4 0 0 0 0 0 0 0 ]})}"
    );

    let back = readfuncs::stringToNode(mcx, s1.as_str()).unwrap();
    let s2 = nodeToString(mcx, back).unwrap();
    assert_eq!(s1.as_str(), s2.as_str());
}

// The ILP32 datum-width class (wasm32): _outDatum must emit the FULL 8-byte
// Datum word for byval values — readDatum unconditionally consumes
// sizeof(Datum) == 8 byte tokens, so a pointer-width (4-byte) emission makes
// the reader die on the "]" token. High-word bits prove no truncation.
#[test]
fn byval_datum_emits_all_eight_word_bytes() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = Node::mk(
        mcx,
        Const {
            consttype: 20, // int8
            consttypmod: -1,
            constcollid: 0,
            constlen: 8,
            constvalue: Datum::from_i64(0x0102_0304_0506_0708),
            constisnull: false,
            constbyval: true,
            location: 3,
        },
    )
    .unwrap();
    let s = nodeToString(mcx, node).unwrap();
    assert!(
        s.as_str().contains(":constvalue 8 [ 8 7 6 5 4 3 2 1 ]"),
        "byval constvalue lost datum-word bytes: {}",
        s.as_str()
    );
    let back = readfuncs::stringToNode(mcx, s.as_str()).unwrap();
    assert_eq!(nodeToString(mcx, back).unwrap().as_str(), s.as_str());
}

// Captured from live pgrust (native --single, PG18.3 catalog):
// CREATE TABLE measurement (... ) PARTITION BY RANGE (logdate);
// CREATE TABLE measurement_2024 PARTITION OF measurement
//   FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');
// SELECT relpartbound FROM pg_class WHERE relname = 'measurement_2024';
// The wasm web-demo repro: partition 2's CREATE is the first reader of this
// string; a 4-byte constvalue emission breaks it with `bad integer token "]"`.
const RELPARTBOUND_MEASUREMENT_2024: &str = "{PARTITIONBOUNDSPEC :strategy r \
    :is_default false :modulus 0 :remainder 0 :listdatums <> :lowerdatums \
    ({PARTITIONRANGEDATUM :kind 0 :value {CONST :consttype 1082 :consttypmod -1 \
    :constcollid 0 :constlen 4 :constbyval true :constisnull false :location -1 \
    :constvalue 4 [ 62 34 0 0 0 0 0 0 ]} :location -1}) :upperdatums \
    ({PARTITIONRANGEDATUM :kind 0 :value {CONST :consttype 1082 :consttypmod -1 \
    :constcollid 0 :constlen 4 :constbyval true :constisnull false :location -1 \
    :constvalue 4 [ 172 35 0 0 0 0 0 0 ]} :location -1}) :location -1}";

#[test]
fn relpartbound_range_capture_roundtrips() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = readfuncs::stringToNode(mcx, RELPARTBOUND_MEASUREMENT_2024).unwrap();
    let written = nodeToString(mcx, node).unwrap();
    assert_eq!(written.as_str(), RELPARTBOUND_MEASUREMENT_2024);
}
