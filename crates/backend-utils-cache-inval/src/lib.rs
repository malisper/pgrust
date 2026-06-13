#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::result_large_err)]
// Not every outward seam re-export / shared-state field is exercised on every
// code path, so the unused-* lints fire crate-wide.
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(dead_code)]

//! `backend/utils/cache/inval.c` — transactional cache invalidation dispatcher.
//!
//! This crate is decomposed into family modules mirroring the structure of
//! `inval.c`, with every public entry point present. The shared
//! transaction-local state types (the chunked
//! [`msgs::InvalMessageArray`] storage, the [`msgs::InvalidationMsgsGroup`]
//! index bookkeeping, the command/subtransaction stack of
//! [`registration::TransInvalidationInfo`], the inplace
//! [`registration::InvalidationInfo`], and the registered
//! syscache/relcache/relsync callback tables) live here field-for-field with
//! the C, and the per-backend process globals are modelled as one
//! `thread_local!` cell.
//!
//! Memory discipline is plain [`mcx::Mcx`] / [`mcx::PgVec`] (the base crate's
//! `FreeIn`/charged-spine model is intentionally dropped): the dispatcher owns
//! one [`mcx::MemoryContext`] ("CacheInvalidation") holding the persistent
//! state, and per-call snapshots are ordinary `PgVec`/`Vec` allocations.
//!
//! Family modules:
//! - [`msgs`] — SI message array / group construction (`Add*InvalidationMessage`,
//!   the subgroup/group append + walk helpers).
//! - [`registration`] — `Register*Invalidation`, `Prepare*InvalidationState`,
//!   the callback-registration entry points, and the `InfoRef` selector.
//! - [`local_list`] — `LocalExecuteInvalidationMessage`,
//!   `AcceptInvalidationMessages`, `InvalidateSystemCaches[Extended]`, and the
//!   `ProcessInvalidationMessages[Multi]` collectors.
//! - [`cache_invalidate`] — `CacheInvalidate{HeapTuple,Catalog,Relcache,Smgr,
//!   Relmap,RelSync,...}` plus `CallSyscacheCallbacks`/`CallRelSyncCallbacks`.
//! - [`at_eoxact`] — `AtEOXact_Inval`, `AtEOSubXact_Inval`,
//!   `CommandEndInvalidationMessages`, the inplace (`PreInplace_Inval` /
//!   `AtInplace_Inval` / `ForgetInplace_Inval`) and 2PC / recovery
//!   (`PostPrepare_Inval`, `xactGetCommittedInvalidationMessages`,
//!   `ProcessCommittedInvalidationMessages`, `LogLogicalInvalidations`) paths.

use std::cell::{Cell, RefCell};

use mcx::{bind, Mcx, McxOwned, MemoryContext, PgVec};
use types_cache::{RelcacheCallbackFunction, SyscacheCallbackFunction};
use types_datum::Datum;

// Outward seams to other owners.
use backend_catalog_catalog_seams as catalog_seams;
use backend_storage_ipc_sinval_seams as sinval_seams;
use backend_storage_smgr_seams as smgr_seams;
use backend_utils_cache_catcache_seams as catcache_seams;
use backend_utils_cache_relcache_seams as relcache_seams;
use backend_utils_cache_relmapper_seams as relmapper_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_init_miscinit_seams as miscinit_seams;
use backend_utils_init_small_seams as init_small_seams;
use backend_utils_time_snapmgr_seams as snapmgr_seams;
use backend_access_transam_xact_seams as xact_seams;
use backend_access_transam_xlog_seams as xlog_seams;

pub mod at_eoxact;
pub mod cache_invalidate;
pub mod local_list;
pub mod msgs;
pub mod registration;

/// `RelSyncCallbackFunction` — `void (*)(Datum arg, Oid relid)` (inval.h),
/// the logical-decoding RelationSyncCache invalidation callback. (Unlike the
/// syscache/relcache callback types, this one is not yet in `types-cache`, so
/// inval.c — its owner — defines it here.)
pub type RelSyncCallbackFunction = fn(arg: Datum, relid: types_core::Oid);

/* ------------------------------------------------------------------------
 *  Subgroup indices (inval.c: CatCacheMsgs / RelCacheMsgs)
 * ------------------------------------------------------------------------ */

/// `CatCacheMsgs` subgroup index.
pub(crate) const CAT_CACHE_MSGS: usize = 0;
/// `RelCacheMsgs` subgroup index.
pub(crate) const REL_CACHE_MSGS: usize = 1;

/// `MAX_BACKENDS_BITS` (procnumber.h) — the `CacheInvalidateSmgr` optimization
/// stores only three bytes of the `ProcNumber`, so this must stay `<= 23`.
pub(crate) const MAX_BACKENDS_BITS: i32 = 18;

pub(crate) const MAX_SYSCACHE_CALLBACKS: usize = 64;
pub(crate) const MAX_RELCACHE_CALLBACKS: usize = 10;
pub(crate) const MAX_RELSYNC_CALLBACKS: usize = 10;

/// `SysCacheSize` — number of distinct system caches (mirrors
/// `utils/syscache.h`'s enum count); sizes `syscache_callback_links`.
pub(crate) const SYS_CACHE_SIZE: usize = 85;

/* ------------------------------------------------------------------------
 *  Dynamically-registered callback tables (inval.c)
 * ------------------------------------------------------------------------ */

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

/* ------------------------------------------------------------------------
 *  Process-global state (C statics), modelled per-backend (thread-local)
 * ------------------------------------------------------------------------ */

/// All of inval.c's file-scope statics, gathered into one per-backend struct.
///
/// The state co-owns its [`Mcx`] handle (the `CacheInvalidation` context) so
/// every working buffer — the dense SI message arrays, the (sub)transaction
/// stack, and the callback tables — is a plain `PgVec` charged to that one
/// context, with no `FreeIn`/charged-spine bookkeeping.
pub(crate) struct InvalState<'mcx> {
    /// The owning context handle (the persistent-state-owns-its-context
    /// pattern via [`McxOwned`]).
    pub(crate) mcx: Mcx<'mcx>,

    /// `InvalMessageArrays[2]`.
    pub(crate) message_arrays: [msgs::InvalMessageArray<'mcx>; 2],

    /// `transInvalInfo` chain as a stack: the last element is the current
    /// (deepest) `transInvalInfo`; the element before it is its parent.
    pub(crate) trans_inval_stack: PgVec<'mcx, registration::TransInvalidationInfo>,

    /// `inplaceInvalInfo`.
    pub(crate) inplace_inval_info: Option<registration::InvalidationInfo>,

    /// `syscache_callback_list` / `_links` / `_count`.
    pub(crate) syscache_callback_list: PgVec<'mcx, SyscacheCallbackItem>,
    pub(crate) syscache_callback_links: [i16; SYS_CACHE_SIZE],

    /// `relcache_callback_list`.
    pub(crate) relcache_callback_list: PgVec<'mcx, RelcacheCallbackItem>,

    /// `relsync_callback_list`.
    pub(crate) relsync_callback_list: PgVec<'mcx, RelsyncCallbackItem>,
}

bind!(pub(crate) InvalStateTy => InvalState<'mcx>);

thread_local! {
    /// The per-backend invalidation state, created with its owning context on
    /// first use (the C statics living in `TopMemoryContext`/`CacheMemoryContext`).
    pub(crate) static STATE: RefCell<Option<McxOwned<InvalStateTy>>> =
        const { RefCell::new(None) };
}

/// Run `f` over the backend-local state, creating it (and its owning
/// `CacheInvalidation` context) on first use.
///
/// The C statics live in `TopMemoryContext`/`CacheMemoryContext` and are simply
/// present for the life of the backend; allocation failure here corresponds to
/// the process-start `MemoryContextAlloc` that C treats as `FATAL`, so this
/// keeps the infallible `-> R` shape and `.expect()`s the one-time build.
pub(crate) fn with_state<R>(f: impl for<'mcx> FnOnce(&mut InvalState<'mcx>) -> R) -> R {
    STATE.with(|cell| {
        {
            // Lazily build the owned state + its context on first use.
            let mut slot = cell.borrow_mut();
            if slot.is_none() {
                let owned = McxOwned::<InvalStateTy>::try_new(
                    MemoryContext::new("CacheInvalidation"),
                    |mcx| {
                        Ok(InvalState {
                            mcx,
                            message_arrays: [
                                msgs::InvalMessageArray::new(mcx),
                                msgs::InvalMessageArray::new(mcx),
                            ],
                            trans_inval_stack: PgVec::new_in(mcx),
                            inplace_inval_info: None,
                            syscache_callback_list: PgVec::new_in(mcx),
                            // C zero-inits the links; 0 means "no entry", and a
                            // populated link stores (index + 1).
                            syscache_callback_links: [0; SYS_CACHE_SIZE],
                            relcache_callback_list: PgVec::new_in(mcx),
                            relsync_callback_list: PgVec::new_in(mcx),
                        })
                    },
                )
                .expect("CacheInvalidation context allocation");
                *slot = Some(owned);
            }
        }
        let mut slot = cell.borrow_mut();
        slot.as_mut().unwrap().with_mut(f)
    })
}

thread_local! {
    pub(crate) static DEBUG_DISCARD_CACHES: Cell<i32> = const { Cell::new(0) };
    /// `recursion_depth` static inside `AcceptInvalidationMessages`'s
    /// DISCARD_CACHES_ENABLED block.
    pub(crate) static ACCEPT_RECURSION_DEPTH: Cell<i32> = const { Cell::new(0) };
}

/// `debug_discard_caches` GUC storage.
pub fn set_debug_discard_caches(value: i32) {
    DEBUG_DISCARD_CACHES.with(|c| c.set(value));
}

/// Read the `debug_discard_caches` GUC.
pub fn debug_discard_caches() -> i32 {
    DEBUG_DISCARD_CACHES.with(|c| c.get())
}

/// Install this unit's inward seams (the public API siblings call across the
/// cycle-breaking `backend-utils-cache-inval-seams` crate).
pub fn init_seams() {
    use backend_utils_cache_inval_seams as seams;

    seams::cache_register_syscache_callback::set(cache_invalidate::CacheRegisterSyscacheCallback);
    seams::cache_register_relcache_callback::set(cache_invalidate::CacheRegisterRelcacheCallback);
    seams::accept_invalidation_messages::set(local_list::AcceptInvalidationMessages);
    seams::command_end_invalidation_messages::set(at_eoxact::CommandEndInvalidationMessages);
    seams::at_eoxact_inval::set(at_eoxact::AtEOXact_Inval);
    seams::at_eosubxact_inval::set(at_eoxact::AtEOSubXact_Inval);
    seams::post_prepare_inval::set(|| at_eoxact::PostPrepare_Inval().expect("PostPrepare_Inval"));
    seams::log_logical_invalidations::set(at_eoxact::LogLogicalInvalidations);
    seams::invalidate_system_caches::set(local_list::InvalidateSystemCaches);
    seams::call_syscache_callbacks::set(cache_invalidate::CallSyscacheCallbacks);

    // These two seams' installed signatures differ from at_eoxact's native
    // shape (the seam folds C's `nmsgs` out-param into the slice / returns a
    // `mcx`-charged `PgVec`), so they're wired through small adapters.
    seams::process_committed_invalidation_messages::set(
        |msgs, relcache_init_file_inval, dbid, tsid| {
            at_eoxact::ProcessCommittedInvalidationMessages(
                msgs,
                msgs.len() as i32,
                relcache_init_file_inval,
                dbid,
                tsid,
            )
        },
    );
    seams::xact_get_committed_invalidation_messages::set(|mcx| {
        let (msgs, relcache_init_file_inval) =
            at_eoxact::xactGetCommittedInvalidationMessages()?;
        // C allocates the array in CurTransactionContext; here we copy the
        // collected messages into the caller's `mcx`.
        let mut out = PgVec::new_in(mcx);
        out.try_reserve(msgs.len())
            .map_err(|_| types_error::PgError::error("out of memory"))?;
        out.extend_from_slice(&msgs);
        Ok((out, relcache_init_file_inval))
    });

    // Seams installed by other owners (relcache.c, sinval.c) — referenced here
    // only so the cross-cycle linkage is documented; not this unit's to wire.
    let _ = (
        seams::relcache_init_file_pre_invalidate::is_installed(),
        seams::relcache_init_file_post_invalidate::is_installed(),
        seams::send_shared_invalid_messages::is_installed(),
    );
}
