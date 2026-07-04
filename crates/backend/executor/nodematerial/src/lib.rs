// nodeMaterial.c over the in-memory tuplestore, mark/restore (merge-join
// inner) included: read pointer 1 is the mark.
#![allow(non_snake_case)]

use std::rc::Rc;

use ::executils::{EStateData, ExecSlotId};
use ::tuplestore::Tuplestore;
use ::types_error::PgResult;
use ::types_nodes::plannodes::Material;
use ::types_slot::{TupleSlotKind, EXEC_FLAG_BACKWARD, EXEC_FLAG_MARK, EXEC_FLAG_REWIND};
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

pub trait MaterialChild<'mcx> {
    fn exec_proc(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<Option<ExecSlotId>>;
    fn rescan(&mut self, estate: &mut EStateData<'mcx>) -> PgResult<()>;
}

pub struct MaterialState<'mcx> {
    pub plan: &'mcx Material<'mcx>,
    pub ps_ResultTupleDesc: Option<Rc<TupleDescData<'static>>>,
    pub ps_ResultTupleSlot: ExecSlotId,
    eflags: i32,
    tuplestorestate: Option<Tuplestore>,
    eof_underlying: bool,
}

pub fn exec_init_material<'mcx>(
    node: &'mcx Material<'mcx>,
    estate: &mut EStateData<'mcx>,
    mut eflags: i32,
    result_desc: Rc<TupleDescData<'static>>,
) -> PgResult<MaterialState<'mcx>> {
    // BACKWARD without REWIND would let tuplestore_trim discard too much.
    if eflags & EXEC_FLAG_BACKWARD != 0 {
        eflags |= EXEC_FLAG_REWIND;
    }
    let ps_ResultTupleSlot =
        estate.exec_init_extra_tuple_slot(Some(result_desc.clone()), TupleSlotKind::MinimalTuple);
    Ok(MaterialState {
        plan: node,
        ps_ResultTupleDesc: Some(result_desc),
        ps_ResultTupleSlot,
        eflags: eflags & (EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK),
        tuplestorestate: None,
        eof_underlying: false,
    })
}

pub fn child_eflags(eflags: i32) -> i32 {
    eflags & !(EXEC_FLAG_REWIND | EXEC_FLAG_BACKWARD | EXEC_FLAG_MARK)
}

/// show_material_info's tuplestore read; None before the store exists.
pub fn storage_stats(
    node: &mut MaterialState<'_>,
) -> Option<types_core::instrument::TuplestoreInstrumentation> {
    node.tuplestorestate.as_mut().map(Tuplestore::get_stats)
}

pub fn exec_material<'mcx, C: MaterialChild<'mcx>>(
    node: &mut MaterialState<'mcx>,
    child: &mut C,
    estate: &mut EStateData<'mcx>,
) -> PgResult<Option<ExecSlotId>> {
    let mcx = estate.es_query_cxt;
    let forward =
        matches!(estate.es_direction, ::types_scan::ScanDirection::ForwardScanDirection);
    if node.tuplestorestate.is_none() && node.eflags != 0 {
        let mut ts = Tuplestore::begin_heap(true, false, init_small::globals::work_mem());
        ts.set_eflags(node.eflags);
        if node.eflags & EXEC_FLAG_MARK != 0 {
            let ptrno = ts.alloc_read_pointer(node.eflags);
            debug_assert_eq!(ptrno, 1);
        }
        node.tuplestorestate = Some(ts);
    }

    let mut eof_tuplestore = node.tuplestorestate.as_ref().is_none_or(Tuplestore::ateof);

    if !forward && eof_tuplestore {
        if !node.eof_underlying {
            // C: skip one back so gettupleslot yields the pre-EOF tuple's predecessor.
            let ts = node.tuplestorestate.as_mut().expect("backward read requires a store");
            if !ts.advance(forward) {
                return Ok(None);
            }
        }
        eof_tuplestore = false;
    }

    if !eof_tuplestore {
        let ts = node.tuplestorestate.as_mut().expect("checked above");
        let slot = node.ps_ResultTupleSlot;
        if ts.gettupleslot(forward, false, &mut estate.es_tupleTable[slot.0 as usize], mcx)? {
            return Ok(Some(slot));
        }
        if !forward {
            return Ok(None);
        }
    }
    if node.eof_underlying {
        return Ok(None);
    }

    let Some(outer_slot) = child.exec_proc(estate)? else {
        node.eof_underlying = true;
        return Ok(None);
    };
    let result = node.ps_ResultTupleSlot;
    if node.tuplestorestate.is_some() {
        let ts = node.tuplestorestate.as_mut().unwrap();
        let slot = &mut estate.es_tupleTable[outer_slot.0 as usize];
        ts.puttupleslot(slot, mcx)?;
    }
    let table = &mut estate.es_tupleTable[..];
    let [dst, src] = table
        .get_disjoint_mut([result.0 as usize, outer_slot.0 as usize])
        .expect("distinct in-range material slot ids");
    exectuples::exec_copy_slot(dst, src, mcx, mcx)?;
    Ok(Some(result))
}

pub fn exec_material_mark_pos(node: &mut MaterialState<'_>) {
    debug_assert!(node.eflags & EXEC_FLAG_MARK != 0);
    if let Some(ts) = node.tuplestorestate.as_mut() {
        ts.copy_read_pointer(0, 1);
        ts.trim();
    }
}

pub fn exec_material_restr_pos(node: &mut MaterialState<'_>) {
    debug_assert!(node.eflags & EXEC_FLAG_MARK != 0);
    if let Some(ts) = node.tuplestorestate.as_mut() {
        ts.copy_read_pointer(1, 0);
    }
}

pub fn exec_end_material(node: &mut MaterialState<'_>) {
    node.tuplestorestate = None;
    node.ps_ResultTupleDesc = None;
}

/// ExecReScanMaterial (nodeMaterial.c), chgParam-nonnull arm: stored results
/// are stale — drop and re-read.
pub fn exec_rescan_material_chg<'mcx>(
    node: &mut MaterialState<'mcx>,
    estate: &mut EStateData<'mcx>,
) {
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);
    node.tuplestorestate = None;
    node.eof_underlying = false;
}

/// Returns true when the caller must rescan the outer child.
pub fn exec_rescan_material<'mcx>(
    node: &mut MaterialState<'mcx>,
    estate: &mut EStateData<'mcx>,
) -> bool {
    let mcx = estate.es_query_cxt;
    exectuples::exec_clear_tuple(estate.slot_mut(node.ps_ResultTupleSlot), mcx);
    if node.eflags != 0 {
        if node.tuplestorestate.is_none() {
            return false;
        }
        // Without REWIND the store can't rewind (MARK-only mergejoin inner):
        // forget it and re-read the subplan.
        if node.eflags & EXEC_FLAG_REWIND == 0 {
            node.tuplestorestate = None;
            node.eof_underlying = false;
            true
        } else {
            node.tuplestorestate.as_mut().expect("checked above").rescan();
            false
        }
    } else {
        node.tuplestorestate = None;
        node.eof_underlying = false;
        true
    }
}

// Exempt: released in exec_end_material.
mcx::forget_safe_struct!(
    MaterialState<'_> { plan, ps_ResultTupleSlot, eflags, eof_underlying;
        ps_ResultTupleDesc, tuplestorestate },
);
