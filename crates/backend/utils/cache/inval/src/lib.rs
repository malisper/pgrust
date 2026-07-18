#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

#[cfg(feature = "bench-internals")]
pub mod bench_internals;
pub mod eoxact;
pub mod invalidate;
pub mod local;
mod msgs;
mod registration;
#[cfg(test)]
mod tests;

use std::cell::{Cell, RefCell};
use std::mem::ManuallyDrop;

use datum::Datum;
use mcx::{bind, Mcx, McxOwned, MemoryContext, PgVec};
use types_core::Oid;
use types_storage::SharedInvalidationMessage;

pub use msgs::InvalidationMsgsGroup;

pub type SyscacheCallbackFunction = fn(arg: Datum, cacheid: i32, hashvalue: u32);
pub type RelcacheCallbackFunction = fn(arg: Datum, relid: Oid);
pub type RelSyncCallbackFunction = fn(arg: Datum, relid: Oid);

pub(crate) const CAT_CACHE_MSGS: usize = 0;
pub(crate) const REL_CACHE_MSGS: usize = 1;

pub(crate) const MAX_SYSCACHE_CALLBACKS: usize = 64;
pub(crate) const MAX_RELCACHE_CALLBACKS: usize = 10;
pub(crate) const MAX_RELSYNC_CALLBACKS: usize = 10;
// syscache_ids.h: SysCacheSize == USERMAPPINGUSERSERVER + 1.
pub(crate) const SYS_CACHE_SIZE: usize = 85;
// procnumber.h; CacheInvalidateSmgr packs the ProcNumber into three bytes.
pub(crate) const MAX_BACKENDS_BITS: i32 = 18;

#[derive(Clone, Copy)]
pub(crate) struct SyscacheCallbackItem {
    pub(crate) id: i16,
    pub(crate) link: i16,
    pub(crate) function: SyscacheCallbackFunction,
    pub(crate) arg: Datum,
}

#[derive(Clone, Copy)]
pub(crate) struct RelcacheCallbackItem {
    pub(crate) function: RelcacheCallbackFunction,
    pub(crate) arg: Datum,
}

#[derive(Clone, Copy)]
pub(crate) struct RelsyncCallbackItem {
    pub(crate) function: RelSyncCallbackFunction,
    pub(crate) arg: Datum,
}

// C's static callback arrays verbatim: fixed capacity, Copy items, one
// indirect call per dispatch (non-per-row). The registrant set is closed in C
// but lives outside this crate, so rule-4 enum dispatch cannot enumerate it.
pub(crate) struct CallbackTables {
    pub(crate) syscache_list: [Option<SyscacheCallbackItem>; MAX_SYSCACHE_CALLBACKS],
    pub(crate) syscache_count: usize,
    pub(crate) relcache_list: [Option<RelcacheCallbackItem>; MAX_RELCACHE_CALLBACKS],
    pub(crate) relcache_count: usize,
    pub(crate) relsync_list: [Option<RelsyncCallbackItem>; MAX_RELSYNC_CALLBACKS],
    pub(crate) relsync_count: usize,
}

thread_local! {
    // Apart from the message state: dispatch copies one Copy item per step
    // under a short borrow (re-reading count/link like C) and invokes with NO
    // borrow held — callbacks re-enter inval (fabled's ResetPlanCache).
    pub(crate) static CALLBACKS: RefCell<CallbackTables> = const {
        RefCell::new(CallbackTables {
            syscache_list: [None; MAX_SYSCACHE_CALLBACKS],
            syscache_count: 0,
            relcache_list: [None; MAX_RELCACHE_CALLBACKS],
            relcache_count: 0,
            relsync_list: [None; MAX_RELSYNC_CALLBACKS],
            relsync_count: 0,
        })
    };
    // Cells, not part of the RefCell table: the per-message head-link probe
    // in CallSyscacheCallbacks must cost one load, like C's static array.
    pub(crate) static SYSCACHE_LINKS: [Cell<i16>; SYS_CACHE_SIZE] =
        const { [const { Cell::new(0) }; SYS_CACHE_SIZE] };
}

pub(crate) struct InvalState<'mcx> {
    pub(crate) mcx: Mcx<'mcx>,
    // Cursor-written: an aborted subxact can leave dead slots past the live
    // group's `nextmsg`; later adds overwrite them in place.
    pub(crate) msg_arrays: [PgVec<'mcx, SharedInvalidationMessage>; 2],
    pub(crate) trans_stack: PgVec<'mcx, registration::TransInvalidationInfo>,
    pub(crate) inplace_info: Option<registration::InvalidationInfo>,
    // LogLogicalInvalidations wire-image buffers, capacity retained.
    pub(crate) wal_scratch: [PgVec<'mcx, u8>; 2],
}

bind!(pub(crate) InvalStateTy => InvalState<'mcx>);

thread_local! {
    // ManuallyDrop keeps the TLS payload !needs_drop (fabled-lessons §8); the
    // C statics live in TopMemoryContext for the backend's whole life anyway.
    pub(crate) static STATE: RefCell<Option<ManuallyDrop<McxOwned<InvalStateTy>>>> =
        const { RefCell::new(None) };
    pub(crate) static DEBUG_DISCARD_CACHES: Cell<i32> = const { Cell::new(0) };
    pub(crate) static ACCEPT_RECURSION_DEPTH: Cell<i32> = const { Cell::new(0) };
}

// Single borrow per entry. Seams invoked under it must be ones whose C bodies
// never re-enter inval (sinval send, xlog insert, xact/relcache/catalog
// reads); anything that can re-enter (user callbacks, catcache prepare,
// LocalExecuteInvalidationMessage) runs with the borrow released.
pub(crate) fn with_state<R>(f: impl for<'mcx> FnOnce(&mut InvalState<'mcx>) -> R) -> R {
    STATE.with(|cell| {
        let mut slot = cell.borrow_mut();
        if slot.is_none() {
            init_state(&mut slot);
        }
        slot.as_mut().unwrap().with_mut(f)
    })
}

#[cold]
#[inline(never)]
fn init_state(slot: &mut Option<ManuallyDrop<McxOwned<InvalStateTy>>>) {
    let owned =
        McxOwned::<InvalStateTy>::try_new(MemoryContext::new("CacheInvalidation"), |mcx| {
            Ok(InvalState {
                mcx,
                msg_arrays: [PgVec::new_in(mcx), PgVec::new_in(mcx)],
                trans_stack: PgVec::new_in(mcx),
                inplace_info: None,
                wal_scratch: [PgVec::new_in(mcx), PgVec::new_in(mcx)],
            })
        })
        .expect("CacheInvalidation context allocation");
    *slot = Some(ManuallyDrop::new(owned));
    // Session-memory teardown (FPBUDGET-1): freed at clean task end.
    ::mcx::register_session_cleanup(Box::new(|| {
        STATE.with(|cell| {
            if let Some(owned) = cell.borrow_mut().take() {
                drop(ManuallyDrop::into_inner(owned));
            }
        });
    }));
}

/// True when the current transaction holds registered invalidation messages
/// not yet broadcast to the shared queue (uncommitted DDL). Retention
/// (wretain): a pooled parallel worker's sinval drain cannot see these, so a
/// leader with pending messages flags the launch and workers fall back to
/// C's fresh-process InvalidateSystemCaches.
pub fn TransactionHasPendingInvalidationMessages() -> bool {
    with_state(|state| {
        state.trans_stack.iter().any(|t| {
            t.ii.current_cmd_invalid_msgs.num_in_group() > 0
                || t.prior_cmd_invalid_msgs.num_in_group() > 0
        }) || state
            .inplace_info
            .as_ref()
            .is_some_and(|i| i.current_cmd_invalid_msgs.num_in_group() > 0)
    })
}

pub fn set_debug_discard_caches(value: i32) {
    DEBUG_DISCARD_CACHES.set(value);
}

pub fn debug_discard_caches() -> i32 {
    DEBUG_DISCARD_CACHES.get()
}

pub fn init_seams() {
    guc_tables::vars::debug_discard_caches.install(guc_tables::GucVarAccessors {
        get: debug_discard_caches,
        set: set_debug_discard_caches,
    });
    inval_seams::accept_invalidation_messages::set(local::AcceptInvalidationMessages);
}
