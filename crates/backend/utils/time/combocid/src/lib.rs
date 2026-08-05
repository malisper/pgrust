//! combocid.c: combo command ID support. cmin/cmax overlay in one header
//! field; a combo CID maps to the real pair via backend-private state.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use types_core::CommandId;
use types_error::PgResult;
use types_tuple::{HeapTupleHeaderData, HEAP_COMBOCID, HEAP_MOVED};

#[cfg(test)]
mod tests;

#[derive(Default)]
struct ComboCidState {
    comboCids: Vec<(CommandId, CommandId)>,
    comboHash: HashMap<(CommandId, CommandId), CommandId>,
}

thread_local! {
    static STATE: RefCell<ComboCidState> = RefCell::new(ComboCidState::default());
}

// GetCmin/GetCmax are only valid from the originating transaction; other
// readers must use raw_command_id() directly.
pub fn HeapTupleHeaderGetCmin(tup: &HeapTupleHeaderData) -> CommandId {
    let cid = tup.raw_command_id();

    debug_assert!((tup.t_infomask & HEAP_MOVED) == 0);
    debug_assert!(xact_seams::transaction_id_is_current_transaction_id::call(tup.xmin()));

    if (tup.t_infomask & HEAP_COMBOCID) != 0 {
        GetRealCmin(cid)
    } else {
        cid
    }
}

pub fn HeapTupleHeaderGetCmax(tup: &HeapTupleHeaderData) -> CommandId {
    let cid = tup.raw_command_id();

    debug_assert!((tup.t_infomask & HEAP_MOVED) == 0);
    // Skipped inside critical sections: GetUpdateXid may allocate (multixact).
    debug_assert!(
        init_small::globals::CritSectionCount() > 0
            || xact_seams::transaction_id_is_current_transaction_id::call(
                heapam::HeapTupleHeaderGetUpdateXid(tup).expect("HeapTupleHeaderGetUpdateXid")
            )
    );

    if (tup.t_infomask & HEAP_COMBOCID) != 0 {
        GetRealCmax(cid)
    } else {
        cid
    }
}

// The cmax to store into a tuple being deleted: (cmax, iscombo). A combo CID
// is needed iff (a subtransaction of) our transaction inserted the tuple.
pub fn HeapTupleHeaderAdjustCmax(
    tup: &HeapTupleHeaderData,
    cmax: CommandId,
) -> PgResult<(CommandId, bool)> {
    if !tup.xmin_committed()
        && xact_seams::transaction_id_is_current_transaction_id::call(tup.xmin_raw())
    {
        let cmin = HeapTupleHeaderGetCmin(tup);
        Ok((GetComboCommandId(cmin, cmax), true))
    } else {
        Ok((cmax, false))
    }
}

pub fn AtEOXact_ComboCid() {
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        s.comboCids.clear();
        s.comboHash.clear();
    });
}

fn GetComboCommandId(cmin: CommandId, cmax: CommandId) -> CommandId {
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        let ComboCidState { comboCids, comboHash } = &mut *s;
        let next = comboCids.len() as CommandId;
        match comboHash.entry((cmin, cmax)) {
            std::collections::hash_map::Entry::Occupied(e) => *e.get(),
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(next);
                comboCids.push((cmin, cmax));
                next
            }
        }
    })
}

fn GetRealCmin(combocid: CommandId) -> CommandId {
    STATE.with(|s| {
        let s = s.borrow();
        debug_assert!((combocid as usize) < s.comboCids.len());
        s.comboCids[combocid as usize].0
    })
}

fn GetRealCmax(combocid: CommandId) -> CommandId {
    STATE.with(|s| {
        let s = s.borrow();
        debug_assert!((combocid as usize) < s.comboCids.len());
        s.comboCids[combocid as usize].1
    })
}

// Arc'd snapshot instead of C's byte image: immutable while parallel mode's
// fences hold (no writer can create combo CIDs during a parallel operation).
pub fn SerializeComboCIDState() -> Arc<[(CommandId, CommandId)]> {
    STATE.with(|s| Arc::from(s.borrow().comboCids.as_slice()))
}

// Only valid in a worker with no combo CIDs yet.
pub fn RestoreComboCIDState(state: &Arc<[(CommandId, CommandId)]>) {
    STATE.with(|s| {
        let mut s = s.borrow_mut();
        debug_assert!(s.comboCids.is_empty() && s.comboHash.is_empty());
        s.comboCids.extend_from_slice(state);
        s.comboHash
            .extend(state.iter().enumerate().map(|(i, &key)| (key, i as CommandId)));
    });
}

pub fn init_seams() {
    combocid_seams::at_eoxact_combocid::set(AtEOXact_ComboCid);
    combocid_seams::heap_tuple_header_get_cmin::set(HeapTupleHeaderGetCmin);
    combocid_seams::heap_tuple_header_get_cmax::set(HeapTupleHeaderGetCmax);
    combocid_seams::heap_tuple_header_adjust_cmax::set(HeapTupleHeaderAdjustCmax);
}
