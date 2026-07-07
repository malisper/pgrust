// SELECT from an ephemeral named relation end to end: a tuplestore-backed ENR
// registered in a QueryEnvironment makes "SELECT a, b FROM olds" parse
// (addRangeTableEntryForENR), plan (NamedTuplestoreScan), and execute to the
// registered rows. Catalog access is limited to the type-shape seam; there is
// no heap relation anywhere in the query.
use std::rc::Rc;
use std::sync::Once;

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgString, PgVec};
use tcop_dest::DestReceiver;
use types_core::InvalidOid;
use types_nodes::parsenodes::RTEKind;
use types_nodes::plannodes::PlannedStmt;
use types_nodes::NodeTag;
use types_portal::ParamListHandle;
use types_slot::TupleSlotKind;
use types_tuple::{
    CompactAttribute, FormData_pg_attribute, TupleDescData, TYPALIGN_INT, TYPSTORAGE_PLAIN,
};

const INT4OID: u32 = 23;

static SEAMS: Once = Once::new();

fn install_seams() {
    SEAMS.call_once(|| {
        parse_expr::init_seams();
        parser_analyze::init_seams();
        rewrite_handler::init_seams();
        planner::init_seams();
        execmain::init_seams();
        xact::init_seams();
        miscinit_seams::get_user_id::set(|| 10);
        aclchk_seams::pg_class_aclmask::set(|_, _, mask, _| Ok(mask));
        backend_status_seams::pgstat_report_query_id::set(|_, _| {});
        backend_status_seams::pgstat_report_plan_id::set(|_, _| {});
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok((typid == INT4OID).then_some(types_tuple::PgTypeShape {
                typlen: 4,
                typbyval: true,
                typalign: TYPALIGN_INT,
                typstorage: TYPSTORAGE_PLAIN,
                typcollation: 0,
            }))
        });
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
    });
}

fn int4_tupdesc(mcx: Mcx<'static>, names: &[&str]) -> Rc<TupleDescData<'static>> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for (i, name) in names.iter().enumerate() {
        let mut att = FormData_pg_attribute {
            attnum: i as i16 + 1,
            atttypid: INT4OID,
            atttypmod: -1,
            attlen: 4,
            attbyval: true,
            attalign: TYPALIGN_INT,
            attstorage: TYPSTORAGE_PLAIN,
            ..Default::default()
        };
        att.attname.namestrcpy(name);
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: names.len() as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

#[test]
fn select_from_enr_scans_registered_tuplestore() {
    install_seams();
    let ctx: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("enr-test")));
    let mcx = ctx.mcx();

    let desc = int4_tupdesc(mcx, &["a", "b"]);
    let mut ts = tuplestore::Tuplestore::begin_heap(true, false, 1024);
    for (a, b) in [(1, 10), (7, 70), (4, 40)] {
        ts.putvalues(&desc, &[Datum::from_i32(a), Datum::from_i32(b)], &[false, false])
            .unwrap();
    }
    let reldata = tuplestore::hold::register(ts);

    let mut env = queryenvironment::create_queryEnv(mcx);
    queryenvironment::register_ENR(
        &mut env,
        queryenvironment::EphemeralNamedRelationData {
            md: queryenvironment::EphemeralNamedRelationMetadataData {
                name: PgString::from_str_in("olds", mcx).unwrap(),
                reliddesc: InvalidOid,
                tupdesc: Some(Rc::clone(&desc)),
                enrtype: queryenvironment::ENR_NAMED_TUPLESTORE,
                enrtuples: 3.0,
            },
            reldata,
        },
    )
    .unwrap();
    let qeh = queryenvironment::hold::register(env);

    let sql = "SELECT a, b FROM olds";
    let list =
        gram_core::raw_parser(mcx, sql, parser_seams::RawParseMode::RAW_PARSE_DEFAULT).unwrap();
    assert_eq!(list.len(), 1);
    let raw = list.nth(0).as_raw_stmt().unwrap();
    let query = parser_analyze::parse_analyze_fixedparams(mcx, raw, sql, &[], qeh).unwrap();
    let mut rewritten = rewrite_handler::QueryRewrite(mcx, query).unwrap();
    assert_eq!(rewritten.len(), 1);
    let query = rewritten.pop().unwrap();
    let pstmt = planner::planner(mcx, mcx::leak_in(mcx::alloc_in(mcx, query).unwrap()), sql, 0, ParamListHandle::NULL).unwrap();
    let pstmt: &'static PlannedStmt<'static> = mcx::leak_in(mcx::alloc_in(mcx, pstmt).unwrap());

    let scan = pstmt.planTree.expect("planned SELECT has a plan tree");
    assert_eq!(scan.node_tag(), NodeTag::T_NamedTuplestoreScan);
    let nts = scan.as_named_tuplestore_scan().unwrap();
    assert_eq!(nts.enrname, Some("olds"));
    assert_eq!(nts.scan.plan.targetlist.len(), 2);
    let rte = pstmt
        .rtable
        .nth(nts.scan.scanrelid as usize - 1)
        .as_range_tbl_entry()
        .unwrap();
    assert_eq!(rte.rtekind, RTEKind::RTE_NAMEDTUPLESTORE);
    assert_eq!(rte.enrname, Some("olds"));
    assert_eq!(rte.enrtuples, 3.0);
    // add_rte_to_flat_rtable zaps coltypes/coltypmods/colcollations.
    assert!(rte.coltypes.is_nil());

    let out = tuplestore::hold::register(tuplestore::Tuplestore::begin_heap(true, false, 1024));
    let mut dr = tstore_receiver::tstore_create_DR();
    tstore_receiver::set_params(&mut dr, out, false);
    let mut dest = DestReceiver::Tuplestore(dr);

    let qd = execmain_seams::create_query_desc::call(
        pstmt,
        sql,
        None,
        None,
        types_dest::CommandDest::None,
        ParamListHandle::NULL,
        qeh,
        0,
    )
    .unwrap();
    execmain_seams::executor_start::call(qd, 0).unwrap();
    execmain_seams::executor_run::call(qd, types_scan::sdir::ForwardScanDirection, 0, &mut dest)
        .unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd), 3);

    // ExecutorRewind exercises ExecReScanNamedTuplestoreScan on the private
    // read pointer: the same rows come back.
    execmain_seams::executor_rewind::call(qd).unwrap();
    execmain_seams::executor_run::call(qd, types_scan::sdir::ForwardScanDirection, 0, &mut dest)
        .unwrap();
    assert_eq!(execmain_seams::query_desc_es_processed::call(qd), 3);

    execmain_seams::executor_finish::call(qd).unwrap();
    execmain_seams::executor_end::call(qd).unwrap();
    execmain_seams::free_query_desc::call(qd);

    let mut slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::MinimalTuple, Some(desc));
    let mut rows = Vec::new();
    loop {
        let got = tuplestore::hold::with_store(out, |s| {
            s.gettupleslot(true, false, &mut slot, mcx)
        })
        .unwrap();
        if !got {
            break;
        }
        exectuples::slot_getallattrs(&mut slot);
        let base = slot.base();
        rows.push((base.tts_values[0].as_i32(), base.tts_values[1].as_i32()));
    }
    let expected = [(1, 10), (7, 70), (4, 40)];
    assert_eq!(rows.len(), 6, "two full scans of the ENR");
    assert_eq!(&rows[..3], &expected);
    assert_eq!(&rows[3..], &expected);

    tuplestore::hold::end(out);
    assert!(queryenvironment::hold::unregister(qeh).is_some());
    tuplestore::hold::end(reldata);
}
