//! execJunk.c. The JunkFilter struct itself lives in `executils` (it is the
//! `es_junkFilter` field's type); divergence: the clean tupdesc arrives
//! precomputed because result descriptors are Rc-owned by execmain's
//! backend-lifetime desc context.
#![no_std]

extern crate alloc;

use alloc::rc::Rc;

use ::datum::Datum;
use ::exectuples::{
    exec_clear_tuple, exec_set_slot_descriptor, exec_store_virtual_tuple, slot_getallattrs,
};
use ::executils::{EStateData, ExecSlotId, JunkFilter};
use ::mcx::slice_borrow_in;
use ::types_error::PgResult;
use ::types_nodes::list::NodeList;
use ::types_tuple::TupleDescData;

pub fn init_seams() {}

/// `ExecInitJunkFilter` (execJunk.c); `slot` plays C's non-NULL slot arm.
pub fn exec_init_junk_filter<'mcx>(
    estate: &mut EStateData<'mcx>,
    target_list: &NodeList<'_>,
    clean_tup_type: Rc<TupleDescData<'mcx>>,
    slot: ExecSlotId,
) -> PgResult<JunkFilter<'mcx>> {
    let mcx = estate.es_query_cxt;
    exec_set_slot_descriptor(estate.slot_mut(slot), mcx, clean_tup_type.clone());

    let clean_length = clean_tup_type.natts as usize;
    let mut clean_map: ::mcx::PgVec<'mcx, i16> = ::mcx::PgVec::new_in(mcx);
    clean_map
        .try_reserve_exact(clean_length)
        .map_err(|_| mcx.oom(clean_length * 2))?;
    for tle_node in target_list.iter() {
        let tle = tle_node.as_target_entry().expect("targetlist holds TargetEntries");
        if !tle.resjunk {
            clean_map.push(tle.resno);
        }
    }
    debug_assert_eq!(clean_map.len(), clean_length);

    Ok(JunkFilter {
        jf_cleanTupType: clean_tup_type,
        jf_cleanMap: slice_borrow_in(mcx, &clean_map)?,
        jf_resultSlot: slot,
    })
}

/// `ExecFilterJunk` (execJunk.c): per output tuple; one deform + a map copy,
/// no allocations.
pub fn exec_filter_junk<'mcx>(estate: &mut EStateData<'mcx>, slot: ExecSlotId) -> ExecSlotId {
    let jf = estate.es_junkFilter.as_ref().expect("ExecFilterJunk without es_junkFilter");
    let result_slot = jf.jf_resultSlot;
    let clean_map = jf.jf_cleanMap;

    let mcx = estate.es_query_cxt;
    let table = &mut estate.es_tupleTable;
    let (s, r) = (slot.0 as usize, result_slot.0 as usize);
    assert!(s < table.len() && r < table.len() && s != r);
    let base = table.as_mut_ptr();
    // SAFETY: s and r are bounds-checked, distinct elements of one live slice.
    let (old_slot, result) = unsafe { (&mut *base.add(s), &mut *base.add(r)) };

    slot_getallattrs(old_slot);
    let old = old_slot.base();

    exec_clear_tuple(result, mcx);
    let rb = result.base_mut();
    for (i, &j) in clean_map.iter().enumerate() {
        if j == 0 {
            rb.tts_values[i] = Datum::null();
            rb.tts_isnull[i] = true;
        } else {
            rb.tts_values[i] = old.tts_values[j as usize - 1];
            rb.tts_isnull[i] = old.tts_isnull[j as usize - 1];
        }
    }
    exec_store_virtual_tuple(result);
    result_slot
}

#[cfg(test)]
mod tests;
