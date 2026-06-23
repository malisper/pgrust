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
use cache::{RelcacheCallbackFunction, SyscacheCallbackFunction};
use types_core::Oid;
// Bare-word machine-word `Datum` (`datum::Datum`), aliased `ScalarWord`.
// The callback `arg` is C's opaque `Datum arg` registration cookie: inval.c
// stores it verbatim and hands it back to the user callback untouched, never
// deforming it. It therefore stays the audited bare word rather than the
// canonical `types_tuple::Datum<'mcx>` enum — matching the `SyscacheCallback`
// / `RelcacheCallbackFunction` signatures in `types-cache`, whose `arg` is this
// same bare word. (Datum unification: opaque-passthrough cookies keep the
// scalar word; only deformed tuple values move to the canonical enum.)
use datum::Datum as ScalarWord;

// Outward seams to other owners.
use catalog_seams as catalog_seams;
use sinval_seams as sinval_seams;
use smgr_seams as smgr_seams;
use catcache_seams as catcache_seams;
use relcache_seams as relcache_seams;
use relmapper_seams as relmapper_seams;
use syscache_seams as syscache_seams;
use miscinit_seams as miscinit_seams;
use init_small_seams as init_small_seams;
use snapmgr_seams as snapmgr_seams;
use transam_xact_seams as xact_seams;
use transam_xlog_seams as xlog_seams;

pub mod at_eoxact;
pub mod cache_invalidate;
pub mod local_list;
pub mod msgs;
pub mod registration;

/// `RelSyncCallbackFunction` — `void (*)(Datum arg, Oid relid)` (inval.h),
/// the logical-decoding RelationSyncCache invalidation callback. (Unlike the
/// syscache/relcache callback types, this one is not yet in `types-cache`, so
/// inval.c — its owner — defines it here.)
pub type RelSyncCallbackFunction = fn(arg: ScalarWord, relid: types_core::Oid);

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

/// A registered syscache callback. C stores a single `void (*)(Datum, int,
/// uint32)` plus its `arg`. Plancache registers through the cycle-breaking
/// `*-pc-seams` crate with the projected `fn(cacheid, hashvalue)` shape (its
/// `Datum arg` is always 0 and so dropped at the seam boundary); both kinds
/// invoke identically, so we keep one item type with a tagged callback.
#[derive(Clone, Copy)]
pub(crate) enum SyscacheCallback {
    /// `void (*)(Datum arg, int cacheid, uint32 hashvalue)` — the full C shape.
    Full(SyscacheCallbackFunction),
    /// Plancache's projected `void (*)(int cacheid, uint32 hashvalue)`.
    Plancache(types_plancache::SyscacheCallbackFn),
}

#[derive(Clone, Copy)]
pub(crate) struct SyscacheCallbackItem {
    pub(crate) id: i16,
    pub(crate) link: i16,
    pub(crate) function: SyscacheCallback,
    pub(crate) arg: ScalarWord,
}

impl SyscacheCallbackItem {
    /// Invoke the callback exactly as C does: `ccitem->function(ccitem->arg,
    /// cacheid, hashvalue)`. The plancache projection drops the unused arg.
    pub(crate) fn invoke(&self, cacheid: i32, hashvalue: u32) {
        match self.function {
            SyscacheCallback::Full(f) => f(self.arg, cacheid, hashvalue),
            SyscacheCallback::Plancache(f) => f(cacheid, hashvalue),
        }
    }
}

/// A registered relcache callback (see [`SyscacheCallback`] for the
/// full-vs-plancache split).
#[derive(Clone, Copy)]
pub(crate) enum RelcacheCallback {
    /// `void (*)(Datum arg, Oid relid)` — the full C shape.
    Full(RelcacheCallbackFunction),
    /// Plancache's projected `void (*)(Oid relid)`.
    Plancache(types_plancache::RelcacheCallbackFn),
}

#[derive(Clone, Copy)]
pub(crate) struct RelcacheCallbackItem {
    pub(crate) function: RelcacheCallback,
    pub(crate) arg: ScalarWord,
}

impl RelcacheCallbackItem {
    /// Invoke as C does: `ccitem->function(ccitem->arg, relid)`.
    pub(crate) fn invoke(&self, relid: Oid) {
        match self.function {
            RelcacheCallback::Full(f) => f(self.arg, relid),
            RelcacheCallback::Plancache(f) => f(relid),
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RelsyncCallbackItem {
    pub(crate) function: RelSyncCallbackFunction,
    pub(crate) arg: ScalarWord,
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
    use inval_pc_seams as pc_seams;
    use inval_seams as seams;

    // `int debug_discard_caches` (inval.c) is a plain GUC variable: the GUC
    // machinery reads/writes it through `conf->variable` (C: `&debug_discard_caches`),
    // and AcceptInvalidationMessages reads it via `recursion_depth < debug_discard_caches`.
    // Install this owner's accessors over its backing store so the GUC slot is
    // backed (mirrors NBuffers' GucVarAccessors install in init-small).
    guc_tables::vars::debug_discard_caches.install(
        guc_tables::GucVarAccessors {
            get: debug_discard_caches,
            set: set_debug_discard_caches,
        },
    );

    seams::cache_register_syscache_callback::set(cache_invalidate::CacheRegisterSyscacheCallback);
    seams::cache_register_relcache_callback::set(cache_invalidate::CacheRegisterRelcacheCallback);
    seams::accept_invalidation_messages::set(local_list::AcceptInvalidationMessages);
    seams::command_end_invalidation_messages::set(at_eoxact::CommandEndInvalidationMessages);
    seams::at_eoxact_inval::set(at_eoxact::AtEOXact_Inval);
    seams::at_eosubxact_inval::set(at_eoxact::AtEOSubXact_Inval);
    seams::post_prepare_inval::set(|| at_eoxact::PostPrepare_Inval().expect("PostPrepare_Inval"));
    seams::log_logical_invalidations::set(at_eoxact::LogLogicalInvalidations);
    seams::invalidate_system_caches::set(local_list::InvalidateSystemCaches);
    // ParallelWorkerMain calls InvalidateSystemCaches through its own rt-seam.
    parallel_rt_seams::invalidate_system_caches::set(
        local_list::InvalidateSystemCaches,
    );
    seams::call_syscache_callbacks::set(cache_invalidate::CallSyscacheCallbacks);

    // CLUSTER catalog-invalidation entry points (consumed by
    // backend-commands-cluster / backend-utils-cache-relmapper). Both bodies'
    // signatures match the seam exactly, so they wire directly.
    seams::cache_invalidate_catalog::set(cache_invalidate::CacheInvalidateCatalog);
    seams::cache_invalidate_relmap::set(cache_invalidate::CacheInvalidateRelmap);

    // Relcache invalidation by OID (relcache initfile path) and by an
    // already-deformed pg_class row (CLUSTER swap_relation_files). The by-pg-
    // class seam carries `(relid, &PgClassForm)`; the impl mirrors
    // CacheInvalidateRelcacheByTuple.
    seams::cache_invalidate_relcache::set(cache_invalidate::CacheInvalidateRelcacheByRelid);
    // Immediate smgr-close invalidation broadcast (consumed by visibilitymap.c
    // `vm_extend` / freespace.c `fsm_extend` after a fork extension).
    seams::cache_invalidate_smgr::set(cache_invalidate::CacheInvalidateSmgr);
    // Single-message local execution (consumed by logical decoding's
    // ReorderBufferExecuteInvalidations).
    seams::local_execute_invalidation_message::set(local_list::LocalExecuteInvalidationMessage);
    seams::cache_invalidate_relcache_by_pg_class::set(
        cache_invalidate::CacheInvalidateRelcacheByPgClass,
    );

    // typecmds' ALTER DOMAIN paths send an sinval for a pg_type row they did
    // not change (so dependent cached plans rebuild). The seam carries only the
    // (classId, objectId) pair; the wrapper re-opens the catalog relation and
    // re-fetches the syscache tuple, then runs the shared
    // CacheInvalidateHeapTuple body. C: table_open + SearchSysCache1 +
    // CacheInvalidateHeapTuple(rel, tup, NULL).
    seams::cache_invalidate_heap_tuple::set(cache_invalidate::CacheInvalidateHeapTupleByOid);

    // twophase's FinishPreparedTransaction replays the 2PC state file's raw
    // serialized SharedInvalidationMessage[] buffer; this owner decodes it and
    // forwards to sinval (C: `SendSharedInvalidMessages((SI *) buf, nmsgs)`).
    seams::send_shared_invalid_messages::set(cache_invalidate::SendSharedInvalidMessagesRaw);

    // plancache's InitPlanCache registers its relcache/syscache callbacks via
    // the cycle-breaking *-pc-seams crate; the owner installs them here.
    pc_seams::register_relcache_callback::set(
        cache_invalidate::CacheRegisterRelcacheCallbackPlanCache,
    );
    pc_seams::register_syscache_callback::set(
        cache_invalidate::CacheRegisterSyscacheCallbackPlanCache,
    );

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

    // `RelationCacheInitFilePre/PostInvalidate` (relcache.c) re-exposed through
    // the inval dispatcher's seam crate so twophase's FinishPreparedTransaction
    // can bracket its SI replay (C calls the relcache routines directly). The
    // bodies live in relcache; forward to its seams.
    seams::relcache_init_file_pre_invalidate::set(|| {
        relcache_seams::relation_cache_init_file_pre_invalidate::call()
    });
    seams::relcache_init_file_post_invalidate::set(|| {
        relcache_seams::relation_cache_init_file_post_invalidate::call()
    });
}
