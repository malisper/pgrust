use super::*;

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Once;

use ::datum::Datum;
use ::mcx::{MemoryContext, PgVec};
use ::types_core::catalog::TEXTOID;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::{Plan, Scan};
use ::types_portal::params::ParamExecData;
use ::types_tuple::{CompactAttribute, FormData_pg_attribute, PgTypeShape, TYPALIGN_INT};

const ROWS: usize = 3000;
const WIDTH: usize = 1000;

static SEAMS: Once = Once::new();
static WAL_SYNC_METHOD: AtomicI32 = AtomicI32::new(0);

// The tuplestore spill rig (tuplestore/src/tests.rs `spill::setup`): temp
// files under a private data directory, a resource owner for the VFDs.
fn install_seams() {
    SEAMS.call_once(|| {
        postgres_seams::check_for_interrupts::set(|| Ok(()));
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok((typid == TEXTOID).then_some(PgTypeShape {
                typlen: -1,
                typbyval: false,
                typalign: TYPALIGN_INT,
                typstorage: b'x' as i8,
                typcollation: 100,
            }))
        });
        guc_tables::init_seams();
        elog::init_seams();
        fd::init_seams();
        xact_seams::get_current_sub_transaction_id::set(|| 1);
        aio_seams::pgaio_closing_fd::set(|_| {});
        aio_seams::pgaio_io_start_readv::set(|_, _, _| Ok(()));
        waitevent_seams::pgstat_report_wait_start::set(|_| {});
        waitevent_seams::pgstat_report_wait_end::set(|| {});
        pgstat_seams::pgstat_report_tempfile::set(|_| {});
        ipc_seams::before_shmem_exit::set(|_, _| Ok(()));
        ipc_seams::on_shmem_exit::set(|_cb, _arg| {});
        resowner::init_seams();
        guc_tables::vars::wal_sync_method.install(guc_tables::GucVarAccessors {
            get: || WAL_SYNC_METHOD.load(Ordering::Relaxed),
            set: |v| WAL_SYNC_METHOD.store(v, Ordering::Relaxed),
        });
        if !guc_tables::vars::work_mem.installed() {
            init_small::init_seams();
        }
        let dir = format!(
            "{}/pgrust-ctescan-{}",
            std::env::temp_dir().display(),
            std::process::id()
        );
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(format!("{dir}/base/pgsql_tmp")).unwrap();
        std::env::set_current_dir(&dir).unwrap();
        ::fd::temp::SetTempTablespaces(&[]);
        fd::InitFileAccess();
        let _ = fd::InitTemporaryFileAccess();
        if resowner_seams::current_resource_owner::call().is_null() {
            let owner =
                resowner::ResourceOwnerCreate(types_resowner::ResourceOwner::NULL, "ctescan-test")
                    .unwrap();
            resowner_seams::set_current_resource_owner::call(owner);
        }
    });
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new_bump("nodectescan-test")));
    m.mcx()
}

fn text_desc(mcx: Mcx<'static>) -> Rc<TupleDescData<'static>> {
    let mut att = FormData_pg_attribute {
        attnum: 1,
        atttypid: TEXTOID,
        atttypmod: -1,
        attlen: -1,
        attbyval: false,
        attalign: TYPALIGN_INT,
        attstorage: b'x' as i8,
        attcollation: 100,
        ..Default::default()
    };
    att.attname.namestrcpy("payload");
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    compact.push(CompactAttribute::populate_from(&att));
    attrs.push(att);
    Rc::new(TupleDescData {
        natts: 1,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn text_image(fill: u8) -> Vec<u8> {
    let mut img = ::datum::set_varsize_4b(4 + WIDTH).to_vec();
    img.resize(4 + WIDTH, fill);
    img
}

// A materialized CTE that spilled (work_mem far below ROWS * WIDTH), fully
// filled by its leader: the scan under test is a second reader, so every
// row comes back through the copying tuplestore read.
fn filled_shared(mcx: Mcx<'static>, desc: &Rc<TupleDescData<'static>>) -> (CteShared, Vec<Vec<u8>>) {
    let mut ts = Tuplestore::begin_heap(true, false, 64);
    ts.set_eflags(EXEC_FLAG_REWIND).unwrap();
    let images: Vec<Vec<u8>> = (0..ROWS).map(|i| text_image(b'a' + (i % 26) as u8)).collect();
    let mut slot = exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(desc.clone()));
    for img in &images {
        exectuples::exec_clear_tuple(&mut slot, mcx);
        let base = slot.base_mut();
        base.tts_values[0] = Datum::from_usize(img.as_ptr() as usize);
        base.tts_isnull[0] = false;
        exectuples::exec_store_virtual_tuple(&mut slot);
        ts.puttupleslot(&mut slot, mcx).unwrap();
    }
    assert!(!ts.in_memory(), "rig precondition: the store spilled");
    (CteShared { tuplestore: ts, eof_cte: true, fills: ROWS as u32 }, images)
}

fn cte_plan(mcx: Mcx<'static>) -> CteScan<'static> {
    let var = Node::mk_var(mcx, 1, 1, TEXTOID, -1, 100, 0).unwrap();
    let tle = Node::mk_target_entry(mcx, var, 1, None, false).unwrap();
    let targetlist = NodeList::make1(mcx, tle).unwrap();
    CteScan {
        scan: Scan { plan: Plan { targetlist, ..Default::default() }, scanrelid: 1 },
        ctePlanId: 1,
        cteParam: 0,
    }
}

#[test]
fn scan_copies_do_not_accumulate_in_the_query_context() {
    install_seams();
    let mcx = leaked_mcx();
    let desc = text_desc(mcx);
    let mut estate = EStateData::new_in(mcx);
    estate.es_param_exec_vals.push(ParamExecData::EMPTY);
    let (shared, images) = filled_shared(mcx, &desc);
    *estate.cte_shared_slot(0) = Some(shared);
    let plan = cte_plan(mcx);
    let mut node =
        exec_init_cte_scan(mcx, &plan, &mut estate, 0, desc, &plan.scan.plan.targetlist).unwrap();
    assert!(!node.is_leader);

    let before = mcx.context().used();
    let mut n = 0usize;
    while let Some(id) = exec_cte_scan(&mut node, &mut estate).unwrap() {
        let mut isnull = false;
        let d = exectuples::slot_getattr(estate.slot_mut(id), 1, &mut isnull);
        assert!(!isnull);
        // SAFETY: a copied 4B varlena of WIDTH payload bytes, live in the slot.
        let got = unsafe { core::slice::from_raw_parts(d.as_usize() as *const u8, 4 + WIDTH) };
        assert_eq!(got, images[n].as_slice(), "row {n}");
        n += 1;
    }
    assert_eq!(n, ROWS);
    let growth = mcx.context().used() - before;
    assert!(
        growth < ROWS * WIDTH / 10,
        "query context grew by {growth} bytes over a {ROWS}-row CTE scan"
    );
    exec_end_cte_scan(&mut node, &mut estate);
}
