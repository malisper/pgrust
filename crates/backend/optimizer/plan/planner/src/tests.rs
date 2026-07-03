use std::sync::Once;

use datum::Datum;
use mcx::{alloc_leak_in, Mcx, MemoryContext};
use types_nodes::list::NodeList;
use types_nodes::nodes_enums::CmdType;
use types_nodes::parsenodes::{Query, RTEKind};
use types_nodes::primnodes::FromExpr;
use types_nodes::{Node, NodeTag};
use types_portal::{ParamListHandle, CURSOR_OPT_PARALLEL_OK};
use types_tuple::PgTypeShape;

use crate::planner;

fn install_fixtures() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        crate::init_seams();
        backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok(match typid {
                23 => Some(PgTypeShape {
                    typlen: 4,
                    typbyval: true,
                    typalign: b'i' as i8,
                    typstorage: b'p' as i8,
                    typcollation: 0,
                }),
                _ => None,
            })
        });
        install_scan_fixtures();
    });
}

const TBL: u32 = 16384;
const IDX: u32 = 16385;
const INT4EQ_OP: u32 = 96;
const INT4EQ_PROC: u32 = 65;
const INT4_BTREE_FAM: u32 = 1976;

fn install_scan_fixtures() {
    syscache_seams::lookup_pg_proc_shape::set(|funcid| {
        Ok(match funcid {
            177 | 65 => Some(syscache_seams::PgProcShape {
                pronamespace: 11,
                prorettype: if funcid == 65 { 16 } else { 23 },
                provariadic: 0,
                prosupport: 0,
                pronargs: 2,
                prokind: b'f' as i8,
                provolatile: b'i' as i8,
                proparallel: b's' as i8,
                proretset: false,
                proisstrict: true,
                proleakproof: false,
            }),
            _ => None,
        })
    });
    clauses_seams::evaluate_expr::set(|_, _, _, _, _| panic!("evaluate_expr not exercised"));
    syscache_seams::lookup_pg_operator_shape::set(|opno| {
        Ok(match opno {
            INT4EQ_OP => Some(syscache_seams::PgOperatorShape {
                oprleft: 23,
                oprright: 23,
                oprresult: 16,
                oprcom: INT4EQ_OP,
                oprnegate: 518,
                oprcode: INT4EQ_PROC,
                oprrest: 101,
                oprjoin: 105,
                oprcanmerge: true,
                oprcanhash: true,
            }),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_amop_by_operator::set(|opno, purpose, opfamily| {
        Ok(
            if opno == INT4EQ_OP && purpose == b's' && opfamily == INT4_BTREE_FAM {
                Some(syscache_seams::PgAmopShape {
                    amopstrategy: 3,
                    amopsortfamily: 0,
                    amoplefttype: 23,
                    amoprighttype: 23,
                })
            } else {
                None
            },
        )
    });
    syscache_seams::lookup_pg_amop_members_by_operator::set(|mcx, opno| {
        let mut v = mcx::PgVec::new_in(mcx);
        if opno == INT4EQ_OP {
            v.push(syscache_seams::PgAmopMemberShape {
                amopfamily: INT4_BTREE_FAM,
                amoplefttype: 23,
                amoprighttype: 23,
                amopstrategy: 3,
                amopmethod: 403,
            });
        }
        Ok(v)
    });
    syscache_seams::pg_proc_cost_shape::set(|funcid| {
        Ok(match funcid {
            INT4EQ_PROC => Some(syscache_seams::PgProcCostShape { procost: 1.0, prosupport: 0 }),
            _ => None,
        })
    });
    syscache_seams::lookup_pg_statistic_shape::set(|_, _, _| Ok(None));
    syscache_seams::pg_statistic_stawidth::set(|_, _, _| Ok(None));
    relation_seams::relation_open::set(|mcx, relid, _lockmode| {
        Ok(match relid {
            TBL => make_heap_rel(mcx),
            IDX => make_index_rel(mcx),
            other => panic!("fixture relation_open: unknown oid {other}"),
        })
    });
    relcache_seams::relation_get_index_list::set(|mcx, relid| {
        let mut v = mcx::PgVec::new_in(mcx);
        if relid == TBL {
            v.push(IDX);
        }
        Ok(v)
    });
    bufmgr_seams::relation_get_number_of_blocks_in_fork::set(|rel, _fork| {
        Ok(match rel.rd_id {
            TBL => 100,
            IDX => 30,
            other => panic!("fixture nblocks: unknown oid {other}"),
        })
    });
}

fn make_pg_class(
    oid: u32,
    name: &str,
    relkind: u8,
    relam: u32,
    relhasindex: bool,
) -> types_rel::FormData_pg_class {
    let mut relname = types_tuple::NameData::default();
    relname.namestrcpy(name);
    types_rel::FormData_pg_class {
        relname,
        relnamespace: 2200,
        reltype: 0,
        relowner: 10,
        relam,
        relfilenode: oid,
        reltablespace: 0,
        relpages: 100,
        reltuples: 10000.0,
        relallvisible: 0,
        reltoastrelid: 0,
        relhasindex,
        relisshared: false,
        relpersistence: b'p',
        relkind,
        relhassubclass: false,
        relrowsecurity: false,
        relispopulated: true,
        relreplident: types_rel::REPLICA_IDENTITY_DEFAULT,
        relispartition: false,
        relfrozenxid: 3,
        relminmxid: 1,
    }
}

fn make_rel_data<'mcx>(
    mcx: Mcx<'mcx>,
    oid: u32,
    rd_rel: types_rel::FormData_pg_class,
    rd_att: std::rc::Rc<types_tuple::TupleDescData<'mcx>>,
) -> types_rel::RelationData<'mcx> {
    use std::cell::Cell;
    types_rel::RelationData {
        rd_id: oid,
        rd_backend: types_core::INVALID_PROC_NUMBER,
        rd_islocaltemp: false,
        rd_isvalid: Cell::new(true),
        rd_createSubid: Cell::new(0),
        rd_newRelfilelocatorSubid: Cell::new(0),
        rd_firstRelfilelocatorSubid: Cell::new(0),
        rd_droppedSubid: Cell::new(0),
        rd_lockInfo: types_rel::LockInfoData {
            lockRelId: types_rel::LockRelId { relId: oid, dbId: 5 },
        },
        rd_rel,
        rd_att,
        rd_index: None,
        rd_opcintype: mcx::PgVec::new_in(mcx),
        rd_opfamily: mcx::PgVec::new_in(mcx),
        rd_indoption: mcx::PgVec::new_in(mcx),
        rd_indcollation: mcx::PgVec::new_in(mcx),
        rd_options: None,
        pgstat_enabled: Cell::new(false),
        rd_amcache: Default::default(),
        rd_supportinfo: Default::default(),
    }
}

fn int4_attr(attnum: i16, name: &str, notnull: bool) -> types_tuple::FormData_pg_attribute {
    let mut attname = types_tuple::NameData::default();
    attname.namestrcpy(name);
    types_tuple::FormData_pg_attribute {
        attrelid: TBL,
        attname,
        atttypid: 23,
        attlen: 4,
        attnum,
        atttypmod: -1,
        attndims: 0,
        attbyval: true,
        attalign: b'i' as i8,
        attstorage: b'p' as i8,
        attcompression: 0,
        attnotnull: notnull,
        atthasdef: false,
        atthasmissing: false,
        attidentity: 0,
        attgenerated: 0,
        attisdropped: false,
        attislocal: true,
        attinhcount: 0,
        attcollation: 0,
    }
}

fn make_heap_rel<'mcx>(mcx: Mcx<'mcx>) -> types_rel::Relation<'mcx> {
    use types_tuple::tupdesc::{ATTNULLABLE_UNRESTRICTED, ATTNULLABLE_VALID};
    let mut attrs = mcx::PgVec::new_in(mcx);
    attrs.push(int4_attr(1, "pk", true));
    attrs.push(int4_attr(2, "val", false));
    let mut compact_attrs = mcx::PgVec::new_in(mcx);
    for a in attrs.iter() {
        let mut c = types_tuple::CompactAttribute::populate_from(a);
        c.attnullability =
            if a.attnotnull { ATTNULLABLE_VALID } else { ATTNULLABLE_UNRESTRICTED };
        compact_attrs.push(c);
    }
    let rd_att = std::rc::Rc::new(types_tuple::TupleDescData {
        natts: 2,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs,
        attrs,
    });
    types_rel::Relation::open(
        make_rel_data(mcx, TBL, make_pg_class(TBL, "t", b'r', 2, true), rd_att),
        None,
    )
}

fn make_index_rel<'mcx>(mcx: Mcx<'mcx>) -> types_rel::Relation<'mcx> {
    let rd_att = std::rc::Rc::new(types_tuple::TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 1,
        constr: None,
        compact_attrs: mcx::PgVec::new_in(mcx),
        attrs: mcx::PgVec::new_in(mcx),
    });
    let mut data = make_rel_data(mcx, IDX, make_pg_class(IDX, "t_pkey", b'i', 403, false), rd_att);
    let mut indkey = mcx::PgVec::new_in(mcx);
    indkey.push(1i16);
    data.rd_index = Some(types_rel::FormData_pg_index {
        indexrelid: IDX,
        indrelid: TBL,
        indnatts: 1,
        indnkeyatts: 1,
        indisunique: true,
        indnullsnotdistinct: false,
        indisprimary: true,
        indisexclusion: false,
        indimmediate: true,
        indisvalid: true,
        indisready: true,
        indkey,
        has_indpred: false,
    });
    data.rd_opfamily.push(INT4_BTREE_FAM);
    data.rd_opcintype.push(23);
    data.rd_indoption.push(0);
    data.rd_indcollation.push(0);
    data.rd_amcache.set(Some(types_nbtree::page::BTMetaPageData {
        btm_magic: types_nbtree::page::BTREE_MAGIC,
        btm_version: types_nbtree::page::BTREE_VERSION,
        btm_root: 3,
        btm_level: 1,
        btm_fastroot: 3,
        btm_fastlevel: 1,
        btm_last_cleanup_num_delpages: 0,
        btm_last_cleanup_num_heap_tuples: -1.0,
        btm_allequalimage: true,
    }));
    types_rel::Relation::open(data, None)
}

// The analyzer's output for `SELECT * FROM t [WHERE <quals>]`.
fn table_query<'mcx>(mcx: Mcx<'mcx>, quals: Option<Node<'mcx>>) -> Query<'mcx> {
    let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = TBL;
    rte.relkind = b'r';
    rte.rellockmode = 1;
    rte.inh = false;
    let rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
    let jointree = alloc_leak_in(
        mcx,
        FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals },
    )
    .unwrap();
    let pk = Node::mk_var(mcx, 1, 1, 23, -1, 0, 0).unwrap();
    let val = Node::mk_var(mcx, 1, 2, 23, -1, 0, 0).unwrap();
    let tle1 = Node::mk_target_entry(mcx, pk, 1, Some("pk"), false).unwrap();
    let tle2 = Node::mk_target_entry(mcx, val, 2, Some("val"), false).unwrap();
    let mut target_list = NodeList::make1(mcx, tle1).unwrap();
    target_list.lappend(mcx, tle2).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        rtable,
        targetList: target_list,
        stmt_location: 0,
        stmt_len: 30,
        ..Query::default()
    }
}

fn eq_qual<'mcx>(mcx: Mcx<'mcx>, attno: i16, value: i32) -> Node<'mcx> {
    let var = Node::mk_var(mcx, 1, attno, 23, -1, 0, 0).unwrap();
    let konst = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(value), false, true).unwrap();
    Node::mk(
        mcx,
        types_nodes::primnodes::OpExpr {
            opno: INT4EQ_OP,
            opfuncid: INT4EQ_PROC,
            opresulttype: 16,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, var, konst).unwrap(),
            location: -1,
        },
    )
    .unwrap()
}

#[test]
fn point_select_plans_to_index_scan() {
    let cx = cx();
    let mcx = cx.mcx();
    let parse = table_query(mcx, Some(eq_qual(mcx, 1, 42)));
    let stmt = planner(
        mcx,
        parse,
        "SELECT * FROM t WHERE pk = 42",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    assert_eq!(stmt.rtable.len(), 1);
    assert_eq!(stmt.relationOids.len(), 1);

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_IndexScan);
    let iscan = plan.as_index_scan().unwrap();
    assert_eq!(iscan.scan.scanrelid, 1);
    assert_eq!(iscan.indexid, IDX);
    assert_eq!(iscan.indexorderdir, 1);
    assert_eq!(iscan.scan.plan.plan_rows, 1.0);
    assert_eq!(iscan.scan.plan.plan_width, 8);
    assert!(iscan.scan.plan.qual.is_nil());
    assert_eq!(iscan.scan.plan.plan_node_id, 0);

    // EXPLAIN: Index Scan using t_pkey on t (cost=0.29..8.30 rows=1 width=8)
    // over 100 heap pages / 10000 tuples / 30 index pages / tree height 1.
    assert!((iscan.scan.plan.startup_cost - 0.285).abs() < 1e-9);
    assert!((iscan.scan.plan.total_cost - 8.3025).abs() < 1e-9);

    // indexqual carries the INDEX_VAR-rewritten copy; indexqualorig the
    // original table Var.
    assert_eq!(iscan.indexqual.len(), 1);
    let fixed = iscan.indexqual.nth(0).as_op_expr().unwrap();
    let fixed_var = fixed.args.nth(0).as_var().unwrap();
    assert_eq!(fixed_var.varno, -3);
    assert_eq!(fixed_var.varattno, 1);
    assert_eq!(iscan.indexqualorig.len(), 1);
    let orig = iscan.indexqualorig.nth(0).as_op_expr().unwrap();
    assert_eq!(orig.args.nth(0).as_var().unwrap().varno, 1);
    assert_eq!(orig.args.nth(1).as_const().unwrap().constvalue.as_i32(), 42);

    assert_eq!(iscan.scan.plan.targetlist.len(), 2);
    let tle = iscan.scan.plan.targetlist.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resname, Some("pk"));
    assert_eq!(tle.expr.as_var().unwrap().varattno, 1);
}

#[test]
fn select_star_plans_to_seqscan() {
    let cx = cx();
    let mcx = cx.mcx();
    let parse = table_query(mcx, None);
    let stmt = planner(mcx, parse, "SELECT * FROM t", CURSOR_OPT_PARALLEL_OK, ParamListHandle::NULL)
        .unwrap();

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_SeqScan);
    let sscan = plan.as_seq_scan().unwrap();
    assert_eq!(sscan.scan.scanrelid, 1);
    assert!(sscan.scan.plan.qual.is_nil());
    // EXPLAIN: Seq Scan on t (cost=0.00..200.00 rows=10000 width=8).
    assert_eq!(sscan.scan.plan.startup_cost, 0.0);
    assert!((sscan.scan.plan.total_cost - 200.0).abs() < 1e-9);
    assert_eq!(sscan.scan.plan.plan_rows, 10000.0);
    assert_eq!(sscan.scan.plan.plan_width, 8);
    assert_eq!(sscan.scan.plan.targetlist.len(), 2);
}

#[test]
fn competing_paths_pick_cheapest_total_and_startup() {
    let cx = cx();
    let mcx = cx.mcx();
    let parse = table_query(mcx, Some(eq_qual(mcx, 1, 42)));
    // tuple_fraction > 0 sets consider_startup: the seqscan (startup 0) and
    // the index scan (cheaper total) both survive add_path's fuzzy compare.
    let mut run = crate::run::PlannerRun::new(mcx);
    crate::subquery::subquery_planner(&mut run, parse, 0.1).unwrap();
    let final_rel = crate::planmain::fetch_final_rel(&mut run);
    let rel = run.root.rel(final_rel);
    assert_eq!(rel.pathlist.len(), 2);
    let total = rel.cheapest_total_path.unwrap();
    let startup = rel.cheapest_startup_path.unwrap();
    assert!(matches!(run.root.path(total), types_pathnodes::PathNode::IndexPath(_)));
    assert_eq!(
        run.root.path(startup).base().pathtype,
        crate::pathnode::tag16(NodeTag::T_SeqScan)
    );
    assert!(run.root.path(startup).base().startup_cost == 0.0);
    assert!(
        run.root.path(total).base().total_cost < run.root.path(startup).base().total_cost
    );
}

#[test]
fn non_index_qual_plans_to_seqscan_with_qual() {
    let cx = cx();
    let mcx = cx.mcx();
    let parse = table_query(mcx, Some(eq_qual(mcx, 2, 7)));
    let stmt = planner(
        mcx,
        parse,
        "SELECT * FROM t WHERE val = 7",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_SeqScan);
    let sscan = plan.as_seq_scan().unwrap();
    assert_eq!(sscan.scan.plan.qual.len(), 1);
    // No stats: selectivity 1/DEFAULT_NUM_DISTINCT -> rows 50; the eq
    // operator adds cpu_operator_cost per tuple.
    assert_eq!(sscan.scan.plan.plan_rows, 50.0);
    assert!((sscan.scan.plan.total_cost - 225.0).abs() < 1e-9);
}

fn cx() -> MemoryContext {
    install_fixtures();
    MemoryContext::new_bump("planner-test")
}

// The analyzer's output for `SELECT 1`.
fn select_1_query(mcx: Mcx<'_>) -> Query<'_> {
    let konst = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
    let tle = Node::mk_target_entry(mcx, konst, 1, Some("?column?"), false).unwrap();
    let jointree =
        alloc_leak_in(mcx, FromExpr { fromlist: NodeList::nil(), quals: None }).unwrap();
    Query {
        commandType: CmdType::CMD_SELECT,
        canSetTag: true,
        jointree: Some(jointree),
        targetList: NodeList::make1(mcx, tle).unwrap(),
        stmt_location: 0,
        stmt_len: 8,
        ..Query::default()
    }
}

#[test]
fn select_1_plans_to_a_result_node() {
    let cx = cx();
    let mcx = cx.mcx();
    let stmt = planner(
        mcx,
        select_1_query(mcx),
        "SELECT 1",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();

    assert_eq!(stmt.commandType, CmdType::CMD_SELECT);
    assert!(stmt.canSetTag);
    assert!(!stmt.hasReturning);
    assert_eq!(stmt.jitFlags, 0);
    assert!(stmt.subplans.is_nil());
    assert!(stmt.relationOids.is_nil());
    assert!(stmt.unprunableRelids.is_empty());
    assert_eq!(stmt.stmt_len, 8);

    // replace_empty_jointree's dummy RTE survives into the flat rtable.
    assert_eq!(stmt.rtable.len(), 1);
    let rte = stmt.rtable.nth(0).as_range_tbl_entry().unwrap();
    assert_eq!(rte.rtekind, RTEKind::RTE_RESULT);
    assert_eq!(rte.eref.unwrap().aliasname, Some("*RESULT*"));

    let plan = stmt.planTree.unwrap();
    assert_eq!(plan.node_tag(), NodeTag::T_Result);
    let result = plan.as_result().unwrap();
    assert!(result.plan.lefttree.is_none());
    assert!(result.resconstantqual.is_none());
    assert_eq!(result.plan.plan_node_id, 0);
    // EXPLAIN SELECT 1: cost=0.00..0.01 rows=1 width=4.
    assert_eq!(result.plan.startup_cost, 0.0);
    assert_eq!(result.plan.total_cost, 0.01);
    assert_eq!(result.plan.plan_rows, 1.0);
    assert_eq!(result.plan.plan_width, 4);

    assert_eq!(result.plan.targetlist.len(), 1);
    let tle = result.plan.targetlist.nth(0).as_target_entry().unwrap();
    assert_eq!(tle.resno, 1);
    assert_eq!(tle.resname, Some("?column?"));
    assert!(!tle.resjunk);
    let c = tle.expr.as_const().unwrap();
    assert_eq!(c.consttype, 23);
    assert_eq!(c.constvalue.as_i32(), 1);
}

#[test]
fn seam_routes_to_standard_planner() {
    let cx = cx();
    let mcx = cx.mcx();
    let stmt = planner_seams::planner::call(
        mcx,
        select_1_query(mcx),
        "SELECT 1",
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )
    .unwrap();
    assert_eq!(stmt.planTree.unwrap().node_tag(), NodeTag::T_Result);
}

#[test]
fn select_arithmetic_folds_before_planning() {
    let cx = cx();
    let mcx = cx.mcx();
    let mut parse = select_1_query(mcx);
    let one = Node::mk_const(mcx, 23, -1, 0, 4, Datum::from_i32(1), false, true).unwrap();
    let null = Node::mk_const(mcx, 23, -1, 0, 4, Datum::null(), true, true).unwrap();
    let op = Node::mk(
        mcx,
        types_nodes::primnodes::OpExpr {
            opno: 551,
            opfuncid: 177,
            opresulttype: 23,
            opretset: false,
            opcollid: 0,
            inputcollid: 0,
            args: NodeList::make2(mcx, one, null).unwrap(),
            location: -1,
        },
    )
    .unwrap();
    let tle = Node::mk_target_entry(mcx, op, 1, Some("?column?"), false).unwrap();
    parse.targetList = NodeList::make1(mcx, tle).unwrap();

    // int4pl is strict with a NULL arg: folds to a NULL Const, no executor.
    let stmt = planner(mcx, parse, "SELECT 1 + NULL", CURSOR_OPT_PARALLEL_OK, ParamListHandle::NULL)
        .unwrap();
    let plan = stmt.planTree.unwrap();
    let tle = plan.as_result().unwrap().plan.targetlist.nth(0).as_target_entry().unwrap();
    assert!(tle.expr.as_const().unwrap().constisnull);
}

#[test]
fn guc_boot_values_match_the_settings_tables() {
    use guc_tables::{GucDefaultValue, GucSetting};
    let expect: &[(&str, GucDefaultValue)] = &[
        ("cpu_tuple_cost", GucDefaultValue::Real(crate::gucs::cpu_tuple_cost())),
        ("cursor_tuple_fraction", GucDefaultValue::Real(crate::gucs::cursor_tuple_fraction())),
        ("jit_above_cost", GucDefaultValue::Real(crate::gucs::jit_above_cost())),
        (
            "jit_optimize_above_cost",
            GucDefaultValue::Real(crate::gucs::jit_optimize_above_cost()),
        ),
        ("jit_inline_above_cost", GucDefaultValue::Real(crate::gucs::jit_inline_above_cost())),
        ("jit", GucDefaultValue::Bool(crate::gucs::jit_enabled())),
        ("jit_expressions", GucDefaultValue::Bool(crate::gucs::jit_expressions())),
        ("jit_tuple_deforming", GucDefaultValue::Bool(crate::gucs::jit_tuple_deforming())),
        (
            "max_parallel_workers_per_gather",
            GucDefaultValue::Int(crate::gucs::max_parallel_workers_per_gather()),
        ),
        (
            "debug_parallel_query",
            GucDefaultValue::Enum(crate::gucs::debug_parallel_query()),
        ),
    ];
    for (name, have) in expect {
        let boot = guc_tables::all_settings()
            .find_map(|s| match s {
                GucSetting::Bool(b) if b.name == *name => Some(b.boot_val),
                GucSetting::Int(i) if i.name == *name => Some(i.boot_val),
                GucSetting::Real(r) if r.name == *name => Some(r.boot_val),
                GucSetting::Enum(e) if e.name == *name => Some(e.boot_val),
                _ => None,
            })
            .unwrap_or_else(|| panic!("{name} not in guc tables"));
        assert_eq!(boot, *have, "{name}");
    }
}

#[test]
#[should_panic(expected = "M2")]
fn from_relation_panics_loudly() {
    let cx = cx();
    let mcx = cx.mcx();
    let mut parse = select_1_query(mcx);
    let mut rte = Node::build::<types_nodes::parsenodes::RangeTblEntry>(mcx).unwrap();
    rte.rtekind = RTEKind::RTE_RELATION;
    rte.relid = 16384;
    rte.inh = true;
    parse.rtable = NodeList::make1(mcx, rte.seal()).unwrap();
    let rtr = Node::mk_range_tbl_ref(mcx, 1).unwrap();
    let jointree = alloc_leak_in(
        mcx,
        FromExpr { fromlist: NodeList::make1(mcx, rtr).unwrap(), quals: None },
    )
    .unwrap();
    parse.jointree = Some(jointree);

    let _ = planner(mcx, parse, "SELECT 1 FROM t", CURSOR_OPT_PARALLEL_OK, ParamListHandle::NULL);
}
