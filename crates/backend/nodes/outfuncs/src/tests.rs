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

// RTE_RESULT writes no kind-specific fields (C _outRangeTblEntry's
// RTE_RESULT arm) and round-trips through _readRangeTblEntry.
#[test]
fn rte_result_roundtrips() {
    use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut rte = Node::build::<RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_RESULT;
    rte.inFromCl = true;
    let node = rte.seal();
    let s = nodeToString(mcx, node).unwrap();
    assert_eq!(
        s.as_str(),
        "{RANGETBLENTRY :alias <> :eref <> :rtekind 8 :lateral false \
         :inFromCl true :securityQuals <>}"
    );
    let back = readfuncs::stringToNode(mcx, s.as_str()).unwrap();
    let rte = back.as_range_tbl_entry().unwrap();
    assert_eq!(rte.rtekind, RTEKind::RTE_RESULT);
    assert!(rte.inFromCl);
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

// ---- lane p1-nodes fix witness: float fields are Ryu shortest-decimal ----
// C's WRITE_FLOAT_FIELD goes through double_to_shortest_decimal_buf; Rust's
// `{}` Display uses a different NOTATION for large exponents (115 expanded
// digits vs "4.4444444444444444e+113"), so catalog text written by pgrust
// differed from C. Found by the nodesfam_diff CI cluster CONFIRM at 2.09M execs.
#[test]
fn float_fields_are_shortest_decimal() {
    let cx = mcx::MemoryContext::new("t");
    let m = cx.mcx();
    let text = "{SUBPLAN :subLinkType 0 :testexpr <> :paramIds <> :plan_id 0 \
        :plan_name a :firstColType 0 :firstColTypmod 0 :firstColCollation 0 \
        :useHashTable false :unknownEqFalse false :parallel_safe false \
        :setParam <> :parParam <> :args <> \
        :startup_cost 4.4444444444444444e+113 :per_call_cost 0 }";
    let node = readfuncs::stringToNode(m, text).unwrap();
    let out = crate::nodeToString(m, node).unwrap();
    assert!(
        out.as_str().contains(":startup_cost 4.4444444444444444e+113 "),
        "float field must print in Ryu shortest-decimal notation, got: {}",
        out.as_str()
    );
}

// ---- ALTER ... [NO] DEPENDS ON EXTENSION (AlterObjectDependsStmt) ----
// Expected strings captured from PostgreSQL 18.4 `debug_print_parse=on,
// debug_pretty_print=off` (nodeToStringWithLocations), with every location
// rewritten to -1 for nodeToString's write_location_fields=false.

fn depends_stmt<'m>(
    mcx: mcx::Mcx<'m>,
    objectType: types_nodes::parsenodes::ObjectType,
    relation: Option<&'m types_nodes::primnodes::RangeVar<'m>>,
    object: Option<Node<'m>>,
    remove: bool,
) -> Node<'m> {
    Node::mk(
        mcx,
        types_nodes::parsenodes::AlterObjectDependsStmt {
            objectType,
            relation,
            object,
            extname: Some(Node::mk_string(mcx, "plpgsql").unwrap()),
            remove,
        },
    )
    .unwrap()
}

fn range_var<'m>(
    mcx: mcx::Mcx<'m>,
    schemaname: Option<&'m str>,
    relname: &'m str,
) -> &'m types_nodes::primnodes::RangeVar<'m> {
    Node::mk(
        mcx,
        types_nodes::primnodes::RangeVar {
            catalogname: None,
            schemaname,
            relname: Some(relname),
            inh: true,
            relpersistence: b'p',
            alias: None,
            location: 26,
        },
    )
    .unwrap()
    .as_variant::<types_nodes::primnodes::RangeVar>()
    .unwrap()
}

// ALTER TRIGGER q3dep_tg ON public.q3dep_t NO DEPENDS ON EXTENSION plpgsql;
#[test]
fn alter_object_depends_trigger_matches_c_format() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let names = NodeList::make1(mcx, Node::mk_string(mcx, "q3dep_tg").unwrap()).unwrap();
    let n = depends_stmt(
        mcx,
        types_nodes::parsenodes::ObjectType::OBJECT_TRIGGER,
        Some(range_var(mcx, Some("public"), "q3dep_t")),
        Some(Node::mk_list(mcx, names).unwrap()),
        true,
    );
    assert_eq!(
        nodeToString(mcx, n).unwrap().as_str(),
        "{ALTEROBJECTDEPENDSSTMT :objectType 44 :relation {RANGEVAR :catalogname <> \
         :schemaname public :relname q3dep_t :inh true :relpersistence p :alias <> \
         :location -1} :object (\"q3dep_tg\") :extname \"plpgsql\" :remove true}"
    );
}

// ALTER INDEX q3dep_i DEPENDS ON EXTENSION plpgsql;
#[test]
fn alter_object_depends_index_matches_c_format() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let n = depends_stmt(
        mcx,
        types_nodes::parsenodes::ObjectType::OBJECT_INDEX,
        Some(range_var(mcx, None, "q3dep_i")),
        None,
        false,
    );
    assert_eq!(
        nodeToString(mcx, n).unwrap().as_str(),
        "{ALTEROBJECTDEPENDSSTMT :objectType 20 :relation {RANGEVAR :catalogname <> \
         :schemaname <> :relname q3dep_i :inh true :relpersistence p :alias <> \
         :location -1} :object <> :extname \"plpgsql\" :remove false}"
    );
}

// ALTER ROUTINE q3dep_nosuch DEPENDS ON EXTENSION plpgsql; (bare ColId form)
#[test]
fn alter_object_depends_args_unspecified_matches_c_format() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let objname = NodeList::make1(mcx, Node::mk_string(mcx, "q3dep_nosuch").unwrap()).unwrap();
    let owa = Node::mk(
        mcx,
        types_nodes::parsenodes::ObjectWithArgs {
            objname,
            objargs: types_nodes::list::OptNodeList::nil(),
            objfuncargs: NodeList::nil(),
            args_unspecified: true,
        },
    )
    .unwrap();
    let n = depends_stmt(
        mcx,
        types_nodes::parsenodes::ObjectType::OBJECT_ROUTINE,
        None,
        Some(owa),
        false,
    );
    assert_eq!(
        nodeToString(mcx, n).unwrap().as_str(),
        "{ALTEROBJECTDEPENDSSTMT :objectType 34 :relation <> :object {OBJECTWITHARGS \
         :objname (\"q3dep_nosuch\") :objargs <> :objfuncargs <> :args_unspecified true} \
         :extname \"plpgsql\" :remove false}"
    );
}

fn func_param<'m>(mcx: mcx::Mcx<'m>, argType: Node<'m>) -> Node<'m> {
    Node::mk(
        mcx,
        types_nodes::parsenodes::FunctionParameter {
            name: None,
            argType: Some(argType),
            mode: types_nodes::parsenodes::FunctionParameterMode::FUNC_PARAM_DEFAULT,
            defexpr: None,
            location: 23,
        },
    )
    .unwrap()
}

// ALTER FUNCTION q3dep_f(numeric(10,2), varchar[]) DEPENDS ON EXTENSION plpgsql;
// Exercises OBJECTWITHARGS/TYPENAME/FUNCTIONPARAMETER/A_CONST together:
// numeric carries typmods (two A_CONST Integers), varchar[] an arrayBounds
// IntList-shaped List of one Integer -1.
#[test]
fn alter_object_depends_function_matches_c_format() {
    use types_nodes::rawnodes::{A_Const, TypeName, ValUnion};
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let qual_name = |n: &'static str| {
        let mut l = NodeList::nil();
        l.lappend(mcx, Node::mk_string(mcx, "pg_catalog").unwrap()).unwrap();
        l.lappend(mcx, Node::mk_string(mcx, n).unwrap()).unwrap();
        l
    };
    let a_const_int = |v: i32| {
        Node::mk(
            mcx,
            A_Const {
                val: Some(ValUnion::Integer(types_nodes::Integer { ival: v })),
                location: 31,
            },
        )
        .unwrap()
    };
    let numeric = || {
        let mut typmods = NodeList::nil();
        typmods.lappend(mcx, a_const_int(10)).unwrap();
        typmods.lappend(mcx, a_const_int(2)).unwrap();
        Node::mk(
            mcx,
            TypeName {
                names: qual_name("numeric"),
                typeOid: 0,
                setof: false,
                pct_type: false,
                typmods,
                typemod: -1,
                arrayBounds: NodeList::nil(),
                location: 23,
            },
        )
        .unwrap()
    };
    let varchar_arr = || {
        let bounds =
            NodeList::make1(mcx, Node::mk(mcx, types_nodes::Integer { ival: -1 }).unwrap())
                .unwrap();
        Node::mk(
            mcx,
            TypeName {
                names: qual_name("varchar"),
                typeOid: 0,
                setof: false,
                pct_type: false,
                typmods: NodeList::nil(),
                typemod: -1,
                arrayBounds: bounds,
                location: 38,
            },
        )
        .unwrap()
    };
    let mut objargs = types_nodes::list::OptNodeList::nil();
    objargs.lappend(mcx, Some(numeric())).unwrap();
    objargs.lappend(mcx, Some(varchar_arr())).unwrap();
    let mut objfuncargs = NodeList::nil();
    objfuncargs.lappend(mcx, func_param(mcx, numeric())).unwrap();
    objfuncargs.lappend(mcx, func_param(mcx, varchar_arr())).unwrap();

    let owa = Node::mk(
        mcx,
        types_nodes::parsenodes::ObjectWithArgs {
            objname: NodeList::make1(mcx, Node::mk_string(mcx, "q3dep_f").unwrap()).unwrap(),
            objargs,
            objfuncargs,
            args_unspecified: false,
        },
    )
    .unwrap();
    let n = depends_stmt(
        mcx,
        types_nodes::parsenodes::ObjectType::OBJECT_FUNCTION,
        None,
        Some(owa),
        false,
    );

    const NUMERIC: &str = "{TYPENAME :names (\"pg_catalog\" \"numeric\") :typeOid 0 \
        :setof false :pct_type false :typmods ({A_CONST :val 10 :location -1} \
        {A_CONST :val 2 :location -1}) :typemod -1 :arrayBounds <> :location -1}";
    const VARCHAR: &str = "{TYPENAME :names (\"pg_catalog\" \"varchar\") :typeOid 0 \
        :setof false :pct_type false :typmods <> :typemod -1 :arrayBounds (-1) \
        :location -1}";
    let expected = format!(
        "{{ALTEROBJECTDEPENDSSTMT :objectType 19 :relation <> :object {{OBJECTWITHARGS \
         :objname (\"q3dep_f\") :objargs ({NUMERIC} {VARCHAR}) :objfuncargs \
         ({{FUNCTIONPARAMETER :name <> :argType {NUMERIC} :mode 100 :defexpr <> \
         :location -1}} {{FUNCTIONPARAMETER :name <> :argType {VARCHAR} :mode 100 \
         :defexpr <> :location -1}}) :args_unspecified false}} :extname \"plpgsql\" \
         :remove false}}"
    );
    assert_eq!(nodeToString(mcx, n).unwrap().as_str(), expected);
}

// _outA_Const's isnull arm writes " NULL" with no :val, and a BitString val
// goes through outToken unquoted (C `{A_CONST :val b101 :location N}`).
#[test]
fn a_const_null_and_bitstring_match_c_format() {
    use types_nodes::rawnodes::{A_Const, ValUnion};
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let null_const = Node::mk(mcx, A_Const { val: None, location: 38 }).unwrap();
    assert_eq!(nodeToString(mcx, null_const).unwrap().as_str(), "{A_CONST NULL :location -1}");

    let bits = Node::mk(
        mcx,
        A_Const {
            val: Some(ValUnion::BitString(types_nodes::BitString { bsval: "b101" })),
            location: 61,
        },
    )
    .unwrap();
    assert_eq!(nodeToString(mcx, bits).unwrap().as_str(), "{A_CONST :val b101 :location -1}");

    let spaced = Node::mk(
        mcx,
        A_Const {
            val: Some(ValUnion::String(types_nodes::String { sval: "x y" })),
            location: 5,
        },
    )
    .unwrap();
    assert_eq!(
        nodeToString(mcx, spaced).unwrap().as_str(),
        "{A_CONST :val \"x\\ y\" :location -1}"
    );
}

// ---------------------------------------------------------------------------
// audit-18.6 b053 (backend/nodes/outfuncs) regression witnesses.
// Each asserts the C outfuncs.c outcome; on the unfixed tree they fail with
// the panic or the wrong bytes named in the batch's adjudication rows.

fn int_a_const<'mcx>(mcx: mcx::Mcx<'mcx>, ival: i32, location: i32) -> Node<'mcx> {
    use types_nodes::rawnodes::{A_Const, ValUnion};
    Node::mk(
        mcx,
        A_Const { val: Some(ValUnion::Integer(types_nodes::Integer { ival })), location },
    )
    .unwrap()
}

fn name_list<'mcx>(mcx: mcx::Mcx<'mcx>, op: &'static str) -> NodeList<'mcx> {
    let mut name = NodeList::nil();
    name.lappend(mcx, Node::mk(mcx, types_nodes::String { sval: op }).unwrap()).unwrap();
    name
}

// C outfuncs.c _outA_Expr: the kind keyword follows the node type (nothing
// for AEXPR_OP), then :name :lexpr :rexpr and three location fields, all
// -1 under nodeToString.
#[test]
fn a_expr_matches_c_outfuncs_format() {
    use types_nodes::rawnodes::{A_Expr, A_Expr_Kind};
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let op = Node::mk(
        mcx,
        A_Expr {
            kind: A_Expr_Kind::AEXPR_OP,
            name: name_list(mcx, "="),
            lexpr: Some(int_a_const(mcx, 1, 16)),
            rexpr: Some(int_a_const(mcx, 1, 20)),
            rexpr_list_start: -1,
            rexpr_list_end: -1,
            location: 18,
        },
    )
    .unwrap();
    assert_eq!(
        nodeToString(mcx, op).unwrap().as_str(),
        "{A_EXPR :name (\"=\") :lexpr {A_CONST :val 1 :location -1} :rexpr {A_CONST :val 1 \
         :location -1} :rexpr_list_start -1 :rexpr_list_end -1 :location -1}"
    );

    let mut in_list = NodeList::nil();
    in_list.lappend(mcx, int_a_const(mcx, 1, 14)).unwrap();
    in_list.lappend(mcx, int_a_const(mcx, 2, 17)).unwrap();
    let in_expr = Node::mk(
        mcx,
        A_Expr {
            kind: A_Expr_Kind::AEXPR_IN,
            name: name_list(mcx, "="),
            lexpr: Some(int_a_const(mcx, 1, 7)),
            rexpr: Some(Node::mk_list(mcx, in_list).unwrap()),
            rexpr_list_start: 13,
            rexpr_list_end: 18,
            location: 9,
        },
    )
    .unwrap();
    assert_eq!(
        nodeToString(mcx, in_expr).unwrap().as_str(),
        "{A_EXPR IN :name (\"=\") :lexpr {A_CONST :val 1 :location -1} :rexpr ({A_CONST :val 1 \
         :location -1} {A_CONST :val 2 :location -1}) :rexpr_list_start -1 :rexpr_list_end -1 \
         :location -1}"
    );

    let not_between = Node::mk(
        mcx,
        A_Expr {
            kind: A_Expr_Kind::AEXPR_NOT_BETWEEN_SYM,
            name: name_list(mcx, "NOT BETWEEN SYMMETRIC"),
            lexpr: None,
            rexpr: None,
            rexpr_list_start: -1,
            rexpr_list_end: -1,
            location: -1,
        },
    )
    .unwrap();
    assert_eq!(
        nodeToString(mcx, not_between).unwrap().as_str(),
        "{A_EXPR NOT_BETWEEN_SYM :name (\"NOT\\ BETWEEN\\ SYMMETRIC\") :lexpr <> :rexpr <> \
         :rexpr_list_start -1 :rexpr_list_end -1 :location -1}"
    );
}

// C outfuncs.c outNode: `else if (IsA(obj, Bitmapset)) outBitmapset(...)`.
#[test]
fn bitmapset_node_writes_the_b_list() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut bms = Bitmapset::make_singleton(mcx, 1).unwrap();
    bms.add_member(mcx, 5).unwrap();
    bms.add_member(mcx, 130).unwrap();
    let n = Node::mk_bitmapset(mcx, bms).unwrap();
    assert_eq!(nodeToString(mcx, n).unwrap().as_str(), "(b 1 5 130)");
    let empty = Node::mk_bitmapset(mcx, Bitmapset::empty()).unwrap();
    assert_eq!(nodeToString(mcx, empty).unwrap().as_str(), "(b)");
}

// C outfuncs.c _outList: an XidList is "(x %u ...)".
#[test]
fn xid_list_node_writes_the_x_list() {
    use types_nodes::list::XidList;
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let xl = Node::mk_xid_list(mcx, XidList::make2(mcx, 100, 4_000_000_000).unwrap()).unwrap();
    assert_eq!(nodeToString(mcx, xl).unwrap().as_str(), "(x 100 4000000000)");
}

// A node tag without a ported writer must be a catchable, typed refusal
// carrying C outNode's message ("could not dump unrecognized node type: %d",
// outfuncs.c default arm), never a process panic. CompositeTypeStmt has no
// writer (its raw tree is not in the debug_print_* ported set yet).
#[test]
fn unported_node_tag_is_a_typed_refusal_not_a_panic() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let stmt = Node::mk(mcx, types_nodes::rawnodes::CompositeTypeStmt::default()).unwrap();
    let err = nodeToString(mcx, stmt).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(
        err.message(),
        format!(
            "could not dump unrecognized node type: {}",
            types_nodes::NodeTag::T_CompositeTypeStmt as u16
        )
    );
}

fn byref_const(constlen: i32, value: Datum) -> Const {
    Const {
        consttype: 25,
        consttypmod: -1,
        constcollid: 100,
        constlen,
        constvalue: value,
        constisnull: false,
        constbyval: false,
        location: -1,
    }
}

// C outfuncs.c outDatum -> datumGetSize -> VARSIZE_ANY: an external TOAST
// pointer (VARATT_IS_1B_E, header 0x01) is VARSIZE_EXTERNAL = 2 + 16 bytes
// for VARTAG_ONDISK, and all 18 bytes are written.
#[test]
fn external_toast_pointer_const_writes_varsize_external_bytes() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut img = [0u8; 18];
    img[0] = 0x01;
    img[1] = 18; // VARTAG_ONDISK
    for (i, b) in img.iter_mut().enumerate().skip(2) {
        *b = i as u8;
    }
    let c = byref_const(-1, Datum::from_usize(img.as_ptr() as usize));
    let s = nodeToString(mcx, Node::mk(mcx, c).unwrap()).unwrap();
    assert!(
        s.as_str().ends_with(
            ":constvalue 18 [ 1 18 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 ]}"
        ),
        "{}",
        s.as_str()
    );
}

// C datum.c datumGetSize (called by outDatum before anything is written):
// typlen outside {>0, -1, -2} (or a by-value length that is not 1/2/4/8) is
// elog(ERROR, "invalid typLen: %d"); a NULL by-reference pointer for a
// varlena/cstring is elog(ERROR, "invalid Datum pointer"). Both XX000.
#[test]
fn invalid_typlen_and_null_varlena_pointer_are_c_elog_errors_not_panics() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let img = [0x0bu8, b'h', b'e', b'l', b'l', b'o'];
    let p = Datum::from_usize(img.as_ptr() as usize);
    for (typlen, byval, value) in [
        (-3, false, p),
        (0, false, p),
        (3, true, Datum::from_i32(5)),
        (16, true, Datum::from_i32(5)),
    ] {
        let mut c = byref_const(typlen, value);
        c.constbyval = byval;
        let err = nodeToString(mcx, Node::mk(mcx, c).unwrap()).unwrap_err();
        assert_eq!(err.message(), format!("invalid typLen: {typlen}"));
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    }
    for typlen in [-1, -2] {
        let c = byref_const(typlen, Datum::null());
        let err = nodeToString(mcx, Node::mk(mcx, c).unwrap()).unwrap_err();
        assert_eq!(err.message(), "invalid Datum pointer");
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INTERNAL_ERROR);
    }
    // Fixed-length by-reference with a NULL pointer is the one "0 [ ]" case.
    let c = byref_const(16, Datum::null());
    let s = nodeToString(mcx, Node::mk(mcx, c).unwrap()).unwrap();
    assert!(s.as_str().ends_with(":constvalue 0 [ ]}"), "{}", s.as_str());
}

// C outfuncs.c outChar emits the raw byte; pgrust's node string is UTF-8 by
// invariant, so a non-ASCII "char" field (only reachable through a hand-
// edited catalog "char" column) must be a typed refusal, never the
// `expect("outChar ascii")` panic.
#[test]
fn non_ascii_char_field_is_a_typed_refusal_not_a_panic() {
    use types_nodes::parsenodes::{RTEKind, RangeTblEntry};
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut rte = Node::build::<RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = 16384;
    rte.relkind = 0xC3;
    rte.rellockmode = 1;
    rte.inFromCl = true;
    let node = rte.seal();
    let err = nodeToString(mcx, node).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert_eq!(err.message(), "could not dump non-ASCII char field value: 195");
    // ASCII chars still go through outToken unchanged.
    let mut rte = Node::build::<RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = 16384;
    rte.relkind = b'r';
    rte.rellockmode = 1;
    rte.inFromCl = true;
    let s = nodeToString(mcx, rte.seal()).unwrap();
    assert!(s.as_str().contains(" :relkind r :rellockmode 1 "), "{}", s.as_str());
}

// C outfuncs.c nodeToStringWithLocations (:811-814): every ParseLoc field
// renders as its real value; nodeToString keeps -1; the flag is restored
// after each call (nodeToStringInternal's save/restore).
#[test]
fn node_to_string_with_locations_writes_real_locations_and_restores_the_flag() {
    use crate::nodeToStringWithLocations;
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let node = Node::mk(mcx, int4_const(42)).unwrap();
    assert_eq!(
        nodeToStringWithLocations(mcx, node).unwrap().as_str(),
        ADBIN_DEFAULT_42.replace(":location -1", ":location 7")
    );
    assert_eq!(nodeToString(mcx, node).unwrap().as_str(), ADBIN_DEFAULT_42);

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
    let mut args = NodeList::nil();
    args.lappend(mcx, var).unwrap();
    args.lappend(mcx, Node::mk(mcx, int4_const(0)).unwrap()).unwrap();
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
    assert_eq!(
        nodeToStringWithLocations(mcx, op).unwrap().as_str(),
        "{OPEXPR :opno 521 :opfuncid 147 :opresulttype 16 :opretset false :opcollid 0 \
         :inputcollid 0 :args ({VAR :varno 1 :varattno 2 :vartype 23 :vartypmod -1 \
         :varcollid 0 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 1 \
         :varattnosyn 2 :location 33} {CONST :consttype 23 :consttypmod -1 :constcollid 0 \
         :constlen 4 :constbyval true :constisnull false :location 7 :constvalue 4 \
         [ 0 0 0 0 0 0 0 0 ]}) :location 35}"
    );
    assert_eq!(nodeToString(mcx, op).unwrap().as_str(), CONBIN_B_GT_0);

    // The raw A_Expr list-bound locations are ParseLoc fields too.
    use types_nodes::rawnodes::{A_Expr, A_Expr_Kind};
    let mut in_list = NodeList::nil();
    in_list.lappend(mcx, int_a_const(mcx, 1, 14)).unwrap();
    let in_expr = Node::mk(
        mcx,
        A_Expr {
            kind: A_Expr_Kind::AEXPR_IN,
            name: name_list(mcx, "="),
            lexpr: Some(int_a_const(mcx, 1, 7)),
            rexpr: Some(Node::mk_list(mcx, in_list).unwrap()),
            rexpr_list_start: 13,
            rexpr_list_end: 18,
            location: 9,
        },
    )
    .unwrap();
    assert_eq!(
        nodeToStringWithLocations(mcx, in_expr).unwrap().as_str(),
        "{A_EXPR IN :name (\"=\") :lexpr {A_CONST :val 1 :location 7} :rexpr ({A_CONST :val 1 \
         :location 14}) :rexpr_list_start 13 :rexpr_list_end 18 :location 9}"
    );
}

// C outfuncs.c bmsToString (:822-830).
#[test]
fn bms_to_string_matches_c() {
    use crate::bmsToString;
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut bms = Bitmapset::make_singleton(mcx, 1).unwrap();
    bms.add_member(mcx, 5).unwrap();
    assert_eq!(bmsToString(&bms), "(b 1 5)");
    assert_eq!(bmsToString(&Bitmapset::empty()), "(b)");
}

// _outForeignKeyOptInfo / _outEquivalenceClass (outfuncs.c:435-489) and the
// generated EquivalenceMember / JoinDomain / RestrictInfo writers they reach
// (field order = pathnodes.h; parent_ec/left_ec/right_ec/scansel_cache and
// em_parent are read_write_ignore). The records are PlannerInfo arena
// entries here, so the entry points take the owning root. Locations render
// as -1 (nodeToString's write_location_fields=false), the merged-away EC
// is chased to its canonical one, NULL pointers are "<>".
#[test]
fn planner_records_write_c_outfuncs_format() {
    use types_pathnodes::relids::{relids_add_member, relids_empty, relids_singleton};
    use types_pathnodes::{
        EquivalenceClass, EquivalenceMember, ForeignKeyOptInfo, JoinDomain, PlannerInfo,
        QualCost, RestrictInfo, VOLATILITY_NOVOLATILE,
    };
    use crate::{equivalenceClassToString, foreignKeyOptInfoToString, restrictInfoToString};
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut root = PlannerInfo::new(mcx);
    let var = |varno: i32| {
        Node::mk(
            mcx,
            Var {
                varno,
                varattno: 1,
                vartype: 23,
                vartypmod: -1,
                varnosyn: varno as u32,
                varattnosyn: 1,
                location: 12,
                ..Default::default()
            },
        )
        .unwrap()
    };
    let both = relids_add_member(mcx, &relids_singleton(mcx, 1), 2);
    root.join_domains.push(JoinDomain { jd_relids: both.clone() });
    let e1 = root.alloc_expr_node(var(1));
    let e2 = root.alloc_expr_node(var(2));
    let em1 = root.alloc_em(EquivalenceMember {
        em_expr: e1,
        em_relids: relids_singleton(mcx, 1),
        em_datatype: 23,
        em_jdomain: 0,
        ..Default::default()
    });
    let em2 = root.alloc_em(EquivalenceMember {
        em_expr: e2,
        em_relids: relids_singleton(mcx, 2),
        em_datatype: 23,
        em_jdomain: 0,
        ..Default::default()
    });
    let clause = Node::mk(
        mcx,
        OpExpr {
            opno: 96,
            opfuncid: 65,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::from_slice(mcx, &[var(1), var(2)]).unwrap(),
            location: 20,
        },
    )
    .unwrap();
    let clause = root.alloc_expr_node(clause);
    let mut mergeopfamilies = mcx::PgVec::new_in(mcx);
    mergeopfamilies.push(1976);
    let rinfo = root.alloc_rinfo(RestrictInfo {
        clause,
        is_pushed_down: true,
        can_join: true,
        pseudoconstant: false,
        has_clone: false,
        is_clone: false,
        leakproof: true,
        has_volatile: VOLATILITY_NOVOLATILE,
        security_level: 0,
        num_base_rels: 2,
        clause_relids: both.clone(),
        required_relids: both.clone(),
        incompatible_relids: relids_empty(),
        outer_relids: relids_empty(),
        left_relids: relids_singleton(mcx, 1),
        right_relids: relids_singleton(mcx, 2),
        orclause: None,
        rinfo_serial: 3,
        parent_ec: None,
        eval_cost: QualCost { startup: 0.0, per_tuple: 0.0025 },
        norm_selec: 0.005,
        outer_selec: -1.0,
        mergeopfamilies,
        left_ec: None,
        right_ec: None,
        left_em: Some(em1),
        right_em: Some(em2),
        scansel_cache: mcx::PgVec::new_in(mcx),
        outer_is_left: true,
        hashjoinoperator: 96,
        left_bucketsize: -1.0,
        right_bucketsize: -1.0,
        left_mcvfreq: -1.0,
        right_mcvfreq: -1.0,
        left_hasheqoperator: 96,
        right_hasheqoperator: 96,
    });
    let mut canonical = EquivalenceClass::new(mcx);
    canonical.ec_opfamilies.push(1976);
    canonical.ec_members.push(em1);
    canonical.ec_members.push(em2);
    canonical.ec_sources.push(rinfo);
    canonical.ec_relids = both.clone();
    let canonical = root.alloc_ec(canonical);
    let mut merged = EquivalenceClass::new(mcx);
    merged.ec_merged = Some(canonical);
    let merged = root.alloc_ec(merged);
    root.rinfo_mut(rinfo).parent_ec = Some(canonical);

    let em = |varno: i32| {
        format!(
            "{{EQUIVALENCEMEMBER :em_expr {{VAR :varno {varno} :varattno 1 :vartype 23 \
             :vartypmod -1 :varcollid 0 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 \
             :varnosyn {varno} :varattnosyn 1 :location -1}} :em_relids (b {varno}) \
             :em_is_const false :em_is_child false :em_datatype 23 :em_jdomain {{JOINDOMAIN \
             :jd_relids (b 1 2)}}}}"
        )
    };
    let ri = format!(
        "{{RESTRICTINFO :clause {{OPEXPR :opno 96 :opfuncid 65 :opresulttype 16 :opretset false \
         :opcollid 0 :inputcollid 0 :args ({{VAR :varno 1 :varattno 1 :vartype 23 :vartypmod -1 \
         :varcollid 0 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 1 \
         :varattnosyn 1 :location -1}} {{VAR :varno 2 :varattno 1 :vartype 23 :vartypmod -1 \
         :varcollid 0 :varnullingrels (b) :varlevelsup 0 :varreturningtype 0 :varnosyn 2 \
         :varattnosyn 1 :location -1}}) :location -1}} :is_pushed_down true :can_join true \
         :pseudoconstant false :has_clone false :is_clone false :leakproof true :has_volatile 2 \
         :security_level 0 :num_base_rels 2 :clause_relids (b 1 2) :required_relids (b 1 2) \
         :incompatible_relids (b) :outer_relids (b) :left_relids (b 1) :right_relids (b 2) \
         :orclause <> :rinfo_serial 3 :eval_cost.startup 0 :eval_cost.per_tuple 0.0025 \
         :norm_selec 0.005 :outer_selec -1 :mergeopfamilies (o 1976) :left_em {} :right_em {} \
         :outer_is_left true :hashjoinoperator 96 :left_bucketsize -1 :right_bucketsize -1 \
         :left_mcvfreq -1 :right_mcvfreq -1 :left_hasheqoperator 96 :right_hasheqoperator 96}}",
        em(1),
        em(2)
    );
    assert_eq!(restrictInfoToString(mcx, &root, rinfo).unwrap().as_str(), ri);
    let ec = format!(
        "{{EQUIVALENCECLASS :ec_opfamilies (o 1976) :ec_collation 0 :ec_childmembers_size 0 \
         :ec_members ({} {}) :ec_childmembers <> :ec_sources ({ri}) :ec_derives_list <> \
         :ec_relids (b 1 2) :ec_has_const false :ec_has_volatile false :ec_broken false \
         :ec_sortref 0 :ec_min_security 0 :ec_max_security 0}}",
        em(1),
        em(2)
    );
    assert_eq!(equivalenceClassToString(mcx, &root, canonical).unwrap().as_str(), ec);
    // the merged-away EC prints its canonical survivor
    assert_eq!(equivalenceClassToString(mcx, &root, merged).unwrap().as_str(), ec);

    let mut fk = ForeignKeyOptInfo::new(mcx);
    fk.con_relid = 1;
    fk.ref_relid = 2;
    fk.nkeys = 2;
    fk.conkey.extend([1, 2]);
    fk.confkey.extend([1, 3]);
    fk.conpfeqop.extend([96, 96]);
    fk.nmatched_ec = 1;
    fk.nmatched_rcols = 1;
    fk.nmatched_ri = 1;
    fk.eclass.extend([Some(canonical), None]);
    fk.fk_eclass_member.extend([Some(em1), None]);
    let mut rinfos0 = mcx::PgVec::new_in(mcx);
    rinfos0.push(rinfo);
    fk.rinfos.push(rinfos0);
    fk.rinfos.push(mcx::PgVec::new_in(mcx));
    assert_eq!(
        foreignKeyOptInfoToString(mcx, &fk).unwrap().as_str(),
        "{FOREIGNKEYOPTINFO :con_relid 1 :ref_relid 2 :nkeys 2 :conkey ( 1 2) :confkey ( 1 3) \
         :conpfeqop ( 96 96) :nmatched_ec 1 :nconst_ec 0 :nmatched_rcols 1 :nmatched_ri 1 \
         :eclass 1 0 :rinfos 1 0}"
    );
}

// audit-18.6 w2-018-nodes-1: the raw-node pair this crate writes (_outA_Expr
// outfuncs.c:588-659, _outA_Const :710-722) reads back through readfuncs'
// hand-written arms (_readA_Expr readfuncs.c:448, _readA_Const :310) and
// re-serialises byte-identically — every A_Expr kind spelling and every
// A_Const value kind, NULL included.
#[test]
fn a_expr_and_a_const_round_trip_through_readfuncs() {
    let kinds = [
        "", " ANY", " ALL", " DISTINCT", " NOT_DISTINCT", " NULLIF", " IN", " LIKE", " ILIKE",
        " SIMILAR", " BETWEEN", " NOT_BETWEEN", " BETWEEN_SYM", " NOT_BETWEEN_SYM",
    ];
    for keyword in kinds {
        let text = format!(
            "{{A_EXPR{keyword} :name (\"=\") :lexpr {{A_CONST :val \"x\" :location -1}} :rexpr \
             ({{A_CONST :val 1 :location -1}} {{A_CONST :val 1.5 :location -1}} {{A_CONST :val \
             true :location -1}} {{A_CONST :val false :location -1}} {{A_CONST :val b101 \
             :location -1}} {{A_CONST :val \"\" :location -1}} {{A_CONST NULL :location -1}}) \
             :rexpr_list_start -1 :rexpr_list_end -1 :location -1}}"
        );
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let n = readfuncs::stringToNode(mcx, &text)
            .unwrap_or_else(|e| panic!("{text:?}: {}", e.message()));
        assert_eq!(nodeToString(mcx, n).unwrap().as_str(), text);
    }
}
