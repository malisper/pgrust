use ::datum::Datum;
use ::executils::EStateData;
use ::mcx::{Mcx, MemoryContext};
use ::types_nodes::list::NodeList;
use ::types_nodes::node_tree::Node;
use ::types_nodes::plannodes::ValuesScan;
use ::types_nodes::primnodes::{TargetEntry, Var};
use ::types_scan::ScanDirection;

use crate::*;

const INT4OID: u32 = 23;

fn install_seams() {
    static SEAMS: std::sync::Once = std::sync::Once::new();
    SEAMS.call_once(|| {
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            assert_eq!(typid, INT4OID);
            Ok(Some(::types_tuple::PgTypeShape {
                typlen: 4,
                typbyval: true,
                typalign: ::types_tuple::TYPALIGN_INT,
                typstorage: ::types_tuple::TYPSTORAGE_PLAIN,
                typcollation: 0,
            }))
        });
    });
}

fn leaked_mcx() -> Mcx<'static> {
    let m: &'static MemoryContext = Box::leak(Box::new(MemoryContext::new("nodevaluesscan-test")));
    m.mcx()
}

fn mk_i32_const(mcx: Mcx<'static>, v: i32, isnull: bool) -> Node<'static> {
    Node::mk_const(mcx, INT4OID, -1, 0, 4, Datum::from_i32(v), isnull, true).unwrap()
}

fn mk_values_plan(mcx: Mcx<'static>, rows: &[&[Option<i32>]]) -> &'static ValuesScan<'static> {
    let mut values_lists = NodeList::nil();
    for row in rows {
        let mut cells = NodeList::nil();
        for c in *row {
            let n = match c {
                Some(v) => mk_i32_const(mcx, *v, false),
                None => mk_i32_const(mcx, 0, true),
            };
            cells.lappend(mcx, n).unwrap();
        }
        values_lists.lappend(mcx, Node::mk_list(mcx, cells).unwrap()).unwrap();
    }
    let ncols = rows[0].len();
    let mut plan = Node::build::<ValuesScan>(mcx).unwrap();
    let mut tlist = NodeList::nil();
    for j in 0..ncols {
        let var = Node::mk(
            mcx,
            Var {
                varno: 1,
                varattno: (j + 1) as i16,
                vartype: INT4OID,
                vartypmod: -1,
                ..Var::default()
            },
        )
        .unwrap();
        let te = Node::mk(
            mcx,
            TargetEntry { expr: var, resno: (j + 1) as i16, resname: None, ressortgroupref: 0, resorigtbl: 0, resorigcol: 0, resjunk: false },
        )
        .unwrap();
        tlist.lappend(mcx, te).unwrap();
    }
    plan.scan.plan.targetlist = tlist;
    plan.scan.scanrelid = 1;
    plan.values_lists = values_lists;
    plan.seal().as_values_scan().unwrap()
}

fn pull_row(
    state: &mut ValuesScanState<'static>,
    estate: &mut EStateData<'static>,
) -> Option<Vec<(i32, bool)>> {
    let slot_id = exec_values_scan(state, estate).unwrap()?;
    let slot = estate.slot_mut(slot_id);
    exectuples::slot_getallattrs(slot);
    let base = slot.base_mut();
    let n = base.tts_nvalid as usize;
    Some(
        (0..n)
            .map(|i| (base.tts_values[i].as_i32(), base.tts_isnull[i]))
            .collect(),
    )
}

#[test]
fn values_rows_come_back_in_order() {
    install_seams();
    let mcx = leaked_mcx();
    let plan = mk_values_plan(mcx, &[&[Some(3)], &[Some(1)], &[Some(2)]]);
    let mut estate = EStateData::new_in(mcx);
    let mut state = exec_init_values_scan(mcx, plan, &mut estate).unwrap();

    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(3, false)]);
    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(1, false)]);
    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(2, false)]);
    assert!(pull_row(&mut state, &mut estate).is_none());
    // C: once past the end, stay at the end for forward pulls.
    assert!(pull_row(&mut state, &mut estate).is_none());
}

#[test]
fn multi_column_and_null() {
    install_seams();
    let mcx = leaked_mcx();
    let plan = mk_values_plan(mcx, &[&[Some(2), None], &[Some(1), Some(7)]]);
    let mut estate = EStateData::new_in(mcx);
    let mut state = exec_init_values_scan(mcx, plan, &mut estate).unwrap();

    let r1 = pull_row(&mut state, &mut estate).unwrap();
    assert_eq!(r1[0], (2, false));
    assert!(r1[1].1);
    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(1, false), (7, false)]);
    assert!(pull_row(&mut state, &mut estate).is_none());
}

#[test]
fn rescan_and_backward() {
    install_seams();
    let mcx = leaked_mcx();
    let plan = mk_values_plan(mcx, &[&[Some(10)], &[Some(20)]]);
    let mut estate = EStateData::new_in(mcx);
    let mut state = exec_init_values_scan(mcx, plan, &mut estate).unwrap();

    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(10, false)]);
    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(20, false)]);

    estate.es_direction = ScanDirection::BackwardScanDirection;
    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(10, false)]);
    assert!(pull_row(&mut state, &mut estate).is_none());

    estate.es_direction = ScanDirection::ForwardScanDirection;
    exec_rescan_values_scan(&mut state, &mut estate).unwrap();
    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(10, false)]);
}

/// nodeValuesscan.c:101 — ValuesNext runs `ReScanExprContext(econtext)`,
/// "not just ResetExprContext because we want any registered shutdown
/// callbacks to be called", before evaluating the next row.  A callback
/// registered while row 1 was evaluated must therefore have fired by the
/// time row 2 comes back; a rewind-only reset never fires it.
static ROWCXT_SHUTDOWN_FIRED: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn rowcxt_shutdown_cb(_m: Mcx<'_>, arg: Datum) {
    ROWCXT_SHUTDOWN_FIRED.fetch_add(arg.as_usize() as u64, std::sync::atomic::Ordering::SeqCst);
}

#[test]
fn next_row_runs_prior_rows_shutdown_callbacks() {
    use std::sync::atomic::Ordering::SeqCst;
    install_seams();
    let mcx = leaked_mcx();
    let plan = mk_values_plan(mcx, &[&[Some(1)], &[Some(2)], &[Some(3)]]);
    let mut estate = EStateData::new_in(mcx);
    let mut state = exec_init_values_scan(mcx, plan, &mut estate).unwrap();
    let rowcontext = state.rowcontext;

    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(1, false)]);
    // Something evaluated inside row 1 registered a shutdown callback on
    // the row econtext (C: RegisterExprContextCallback on rsi->econtext).
    let before = ROWCXT_SHUTDOWN_FIRED.load(SeqCst);
    estate.ecxt_mut(rowcontext).register_shutdown_callback(rowcxt_shutdown_cb, Datum::from_usize(1));
    assert_eq!(ROWCXT_SHUTDOWN_FIRED.load(SeqCst), before, "registering must not fire");

    // C ValuesNext: ReScanExprContext(econtext) fires it before row 2.
    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(2, false)]);
    assert_eq!(ROWCXT_SHUTDOWN_FIRED.load(SeqCst), before + 1, "shutdown callback not run between VALUES rows");

    // ShutdownExprContext empties the list: row 3 does not fire it again.
    assert_eq!(pull_row(&mut state, &mut estate).unwrap(), vec![(3, false)]);
    assert_eq!(ROWCXT_SHUTDOWN_FIRED.load(SeqCst), before + 1);
    assert!(pull_row(&mut state, &mut estate).is_none());
}
