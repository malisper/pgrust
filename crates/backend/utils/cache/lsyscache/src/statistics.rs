use datum::Datum;
use mcx::{Mcx, PgVec};
use std::cell::Cell;
use types_core::{AttrNumber, InvalidOid, Oid};
use types_error::PgResult;
use types_tuple::HeapTupleData;

// pg_statistic.h
pub const STATISTIC_NUM_SLOTS: usize = 5;
// lsyscache.h
pub const ATTSTATSSLOT_VALUES: i32 = 0x01;
pub const ATTSTATSSLOT_NUMBERS: i32 = 0x02;

pub type GetAttAvgWidthHook = fn(Oid, AttrNumber) -> i32;

thread_local! {
    static GET_ATTAVGWIDTH_HOOK: Cell<Option<GetAttAvgWidthHook>> = const { Cell::new(None) };
}

pub fn set_get_attavgwidth_hook(hook: Option<GetAttAvgWidthHook>) -> Option<GetAttAvgWidthHook> {
    GET_ATTAVGWIDTH_HOOK.with(|cell| cell.replace(hook))
}

pub fn get_attavgwidth(relid: Oid, attnum: AttrNumber) -> PgResult<i32> {
    if let Some(hook) = GET_ATTAVGWIDTH_HOOK.with(|cell| cell.get()) {
        let stawidth = hook(relid, attnum);
        if stawidth > 0 {
            return Ok(stawidth);
        }
    }
    if let Some(stawidth) = syscache_seams::pg_statistic_stawidth::call(relid, attnum, false)? {
        if stawidth > 0 {
            return Ok(stawidth);
        }
    }
    Ok(0)
}

// lsyscache.h AttStatsSlot; C's values_arr/numbers_arr bookkeeping is
// subsumed by the mcx allocations.
#[derive(Debug)]
pub struct AttStatsSlot<'mcx> {
    pub staop: Oid,
    pub stacoll: Oid,
    pub valuetype: Oid,
    pub values: PgVec<'mcx, Datum>,
    pub numbers: PgVec<'mcx, f32>,
}

pub fn get_attstatsslot<'mcx>(
    mcx: Mcx<'mcx>,
    statstuple: &HeapTupleData<'_>,
    reqkind: i32,
    reqop: Oid,
    flags: i32,
) -> PgResult<Option<AttStatsSlot<'mcx>>> {
    let stats = syscache_seams::pg_statistic_slot_shape::call(statstuple);
    let Some(i) = (0..STATISTIC_NUM_SLOTS).find(|&i| {
        stats.stakind[i] as i32 == reqkind && (reqop == InvalidOid || stats.staop[i] == reqop)
    }) else {
        return Ok(None);
    };
    if flags & ATTSTATSSLOT_VALUES != 0 {
        panic!("get_attstatsslot(ATTSTATSSLOT_VALUES): DatumGetArrayTypePCopy/deconstruct_array unported (arrayfuncs.c)");
    }
    if flags & ATTSTATSSLOT_NUMBERS != 0 {
        panic!("get_attstatsslot(ATTSTATSSLOT_NUMBERS): DatumGetArrayTypePCopy unported (arrayfuncs.c)");
    }
    Ok(Some(AttStatsSlot {
        staop: stats.staop[i],
        stacoll: stats.stacoll[i],
        valuetype: InvalidOid,
        values: PgVec::new_in(mcx),
        numbers: PgVec::new_in(mcx),
    }))
}

// C pfrees the deconstructed arrays; dropping the slot's PgVecs is the mirror.
pub fn free_attstatsslot(sslot: AttStatsSlot<'_>) {
    drop(sslot);
}
