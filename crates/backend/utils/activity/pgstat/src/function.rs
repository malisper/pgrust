// pgstat_function.c — per-backend function-call usage (pending counts in ns
// ticks, shared entry in microseconds) and the recursion-compensating
// total_func_time accounting.

use core::cell::Cell;

use init_small::globals::MyDatabaseId;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_UNDEFINED_FUNCTION};

use crate::pending::{self, PendingData, PgStat_HashKey, PGSTAT_KIND_FUNCTION};
use crate::xact;
use crate::PgStat_Counter;

#[derive(Clone, Copy, Default, PartialEq, Debug)]
pub struct PgStat_FunctionCounts {
    pub numcalls: PgStat_Counter,
    // ns ticks; converted to microseconds at flush (INSTR_TIME_GET_MICROSEC)
    pub total_time: i64,
    pub self_time: i64,
}

#[derive(Clone, Copy, Default, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_StatFuncEntry {
    pub numcalls: PgStat_Counter,
    // microseconds
    pub total_time: PgStat_Counter,
    pub self_time: PgStat_Counter,
}

#[derive(Debug)]
pub struct PgStat_FunctionCallUsage {
    key: PgStat_HashKey,
    save_f_total_time: i64,
    save_total: i64,
    start: i64,
}

thread_local! {
    static TOTAL_FUNC_TIME: Cell<i64> = const { Cell::new(0) };
}

use crate::now_ns;

fn function_key(func_oid: Oid) -> PgStat_HashKey {
    PgStat_HashKey {
        kind: PGSTAT_KIND_FUNCTION,
        dboid: MyDatabaseId(),
        objid: func_oid as u64,
    }
}

pub fn pgstat_create_function(proid: Oid) {
    xact::pgstat_create_transactional(PGSTAT_KIND_FUNCTION, MyDatabaseId(), proid as u64);
}

pub fn pgstat_drop_function(proid: Oid) {
    xact::pgstat_drop_transactional(PGSTAT_KIND_FUNCTION, MyDatabaseId(), proid as u64);
}

// Caller holds C's `pgstat_track_functions <= flinfo->fn_stats` branch
// (execexpr compiles it into the step choice).
pub fn pgstat_init_function_usage(fn_oid: Oid) -> PgResult<PgStat_FunctionCallUsage> {
    let key = function_key(fn_oid);
    let (created_entry, save_f_total_time) = pending::with_state(|st| {
        let created = !st.have_pending(key);
        let PendingData::Function(f) = st.prep_pending_entry(key) else {
            unreachable!("function key holds non-function pending data")
        };
        (created, f.total_time)
    });

    // A newly-created entry may be for a concurrently dropped function: plain
    // function calls don't process invalidations, so probe pg_proc before
    // creating stats for it.
    if created_entry {
        inval_seams::accept_invalidation_messages::call()?;
        if !syscache_seams::search_syscache_exists_procoid::call(fn_oid)? {
            pending::with_state(|st| st.delete_pending_entry(key));
            crate::shmem::drop_entry(key);
            return Err(Box::new(
                PgError::error("function call to dropped function")
                    .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
            ));
        }
    }

    Ok(PgStat_FunctionCallUsage {
        key,
        save_f_total_time,
        save_total: TOTAL_FUNC_TIME.with(|c| c.get()),
        start: now_ns(),
    })
}

pub fn pgstat_end_function_usage(fcu: &PgStat_FunctionCallUsage, finalize: bool) {
    let total = now_ns() - fcu.start;
    let others = TOTAL_FUNC_TIME.with(|c| c.get()) - fcu.save_total;
    let self_time = total - others;
    TOTAL_FUNC_TIME.with(|c| c.set(c.get() + self_time));
    // total_time is assigned (pre-call value + elapsed), not accumulated, so
    // recursive calls of the same function aren't double-counted.
    let total = total + fcu.save_f_total_time;

    pending::with_state(|st| {
        let Some(PendingData::Function(fs)) = st.pending.get_mut(&fcu.key) else {
            // the pending entry can be dropped mid-call (DROP FUNCTION in a
            // recursive call); C keeps a dangling pointer alive via the entry
            // ref, the keyed model just drops the update
            return;
        };
        if finalize {
            fs.numcalls += 1;
        }
        fs.total_time = total;
        fs.self_time += self_time;
    });
}

pub fn find_funcstat_entry(func_id: Oid) -> Option<PgStat_FunctionCounts> {
    let key = function_key(func_id);
    pending::with_state(|st| match st.pending.get(&key) {
        Some(PendingData::Function(f)) => Some(*f),
        Some(_) => unreachable!("function key holds non-function pending data"),
        None => None,
    })
}

pub fn pgstat_fetch_stat_funcentry(func_id: Oid) -> Option<PgStat_StatFuncEntry> {
    match crate::shmem::fetch_entry(function_key(func_id)) {
        Some(crate::shmem::SharedEntry::Function(f)) => Some(f),
        Some(_) => unreachable!("function key holds non-function shared entry"),
        None => None,
    }
}

thread_local! {
    static TRACK_FUNCTIONS: Cell<i32> = const { Cell::new(0) };
}

pub fn pgstat_track_functions() -> i32 {
    TRACK_FUNCTIONS.with(|c| c.get())
}

pub fn set_pgstat_track_functions(v: i32) {
    TRACK_FUNCTIONS.with(|c| c.set(v));
}
