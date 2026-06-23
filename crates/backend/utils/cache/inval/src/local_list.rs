//! Local-list processing (inval.c `LocalExecuteInvalidationMessage`,
//! `AcceptInvalidationMessages`, `InvalidateSystemCaches[Extended]`, and the
//! `ProcessInvalidationMessages[Multi]` public collectors that snapshot a
//! group's messages out before the seam may re-enter).

use ::mcx::Mcx;
use types_core::{InvalidOid, ProcNumber};
use ::types_error::PgResult;
use types_storage::{
    RelFileLocatorBackend, SharedInvalidationMessage, SHAREDINVALCATALOG_ID,
    SHAREDINVALRELCACHE_ID, SHAREDINVALRELMAP_ID, SHAREDINVALRELSYNC_ID, SHAREDINVALSMGR_ID,
    SHAREDINVALSNAPSHOT_ID,
};

use crate::cache_invalidate::{CallRelSyncCallbacks, CallSyscacheCallbacks};
use crate::msgs::{
    process_invalidation_messages_group, process_invalidation_messages_multi, InvalMessageArray,
    InvalidationMsgsGroup,
};
use crate::with_state;

// Outward seams to other owners.
use sinval_seams as sinval_seams;
use smgr_seams as smgr_seams;
use catcache_seams as catcache_seams;
use relcache_seams as relcache_seams;
use relmapper_seams as relmapper_seams;
use init_small_seams as init_small_seams;
use snapmgr_seams as snapmgr_seams;

/// The leading `id` discriminant common to every SI message variant.
///
/// In C this is the union's first `int8 id` field: zero-or-positive for a
/// catcache message (where it doubles as the cache id), and the negative
/// `SHAREDINVAL*_ID` codes otherwise.
pub(crate) fn msg_id(msg: &SharedInvalidationMessage) -> i8 {
    match *msg {
        SharedInvalidationMessage::Catcache(m) => m.id,
        SharedInvalidationMessage::Catalog(_) => SHAREDINVALCATALOG_ID,
        SharedInvalidationMessage::Relcache(_) => SHAREDINVALRELCACHE_ID,
        SharedInvalidationMessage::Smgr(_) => SHAREDINVALSMGR_ID,
        SharedInvalidationMessage::Relmap(_) => SHAREDINVALRELMAP_ID,
        SharedInvalidationMessage::Snapshot(_) => SHAREDINVALSNAPSHOT_ID,
        SharedInvalidationMessage::RelSync(_) => SHAREDINVALRELSYNC_ID,
    }
}

/// `LocalExecuteInvalidationMessage` — process one inbound SI message, flushing
/// only the local caches (the big id-dispatch switch). Does not transmit the
/// message to other backends.
pub fn LocalExecuteInvalidationMessage(msg: &SharedInvalidationMessage) -> PgResult<()> {
    let my_database_id = init_small_seams::my_database_id::call();

    match *msg {
        SharedInvalidationMessage::Catcache(m) => {
            // msg->id >= 0
            if m.dbId == my_database_id || m.dbId == InvalidOid {
                snapmgr_seams::invalidate_catalog_snapshot::call();

                catcache_seams::syscache_invalidate::call(m.id as i32, m.hashValue)?;

                CallSyscacheCallbacks(m.id as i32, m.hashValue)?;
            }
        }
        SharedInvalidationMessage::Catalog(m) => {
            // SHAREDINVALCATALOG_ID
            if m.dbId == my_database_id || m.dbId == InvalidOid {
                snapmgr_seams::invalidate_catalog_snapshot::call();

                catcache_seams::catalog_cache_flush_catalog::call(m.catId)?;

                /* CatalogCacheFlushCatalog calls CallSyscacheCallbacks as needed */
            }
        }
        SharedInvalidationMessage::Relcache(m) => {
            // SHAREDINVALRELCACHE_ID
            if m.dbId == my_database_id || m.dbId == InvalidOid {
                if m.relId == InvalidOid {
                    relcache_seams::relation_cache_invalidate::call(false)?;
                } else {
                    relcache_seams::relation_cache_invalidate_entry::call(m.relId)?;
                }

                // Snapshot the relcache callbacks out before invoking them, so a
                // re-entrant registration does not alias the state borrow.
                let callbacks =
                    with_state(|s| s.relcache_callback_list.iter().copied().collect::<Vec<_>>());
                for ccitem in callbacks {
                    ccitem.invoke(m.relId);
                }
            }
        }
        SharedInvalidationMessage::Smgr(m) => {
            // SHAREDINVALSMGR_ID
            //
            // We could have smgr entries for relations of other databases, so no
            // short-circuit test is possible here.
            let rlocator = RelFileLocatorBackend {
                locator: m.rlocator,
                backend: (((m.backend_hi as i32) << 16) | (m.backend_lo as i32)) as ProcNumber,
            };
            smgr_seams::smgr_release_rellocator::call(rlocator)?;
        }
        SharedInvalidationMessage::Relmap(m) => {
            // SHAREDINVALRELMAP_ID — we only care about our own database and
            // shared catalogs.
            if m.dbId == InvalidOid {
                relmapper_seams::relation_map_invalidate::call(true)?;
            } else if m.dbId == my_database_id {
                relmapper_seams::relation_map_invalidate::call(false)?;
            }
        }
        SharedInvalidationMessage::Snapshot(m) => {
            // SHAREDINVALSNAPSHOT_ID — we only care about our own database and
            // shared catalogs.
            if m.dbId == InvalidOid {
                snapmgr_seams::invalidate_catalog_snapshot::call();
            } else if m.dbId == my_database_id {
                snapmgr_seams::invalidate_catalog_snapshot::call();
            }
        }
        SharedInvalidationMessage::RelSync(m) => {
            // SHAREDINVALRELSYNC_ID — we only care about our own database.
            if m.dbId == my_database_id {
                CallRelSyncCallbacks(m.relid)?;
            }
        }
    }
    // The C `else` arm `elog(FATAL, "unrecognized SI message ID")` is
    // structurally unreachable here: a `SharedInvalidationMessage` only ever
    // holds a valid variant (unrecognized ids are rejected at decode time).
    Ok(())
}

/// `InvalidateSystemCachesExtended`.
pub fn InvalidateSystemCachesExtended(debug_discard: bool) -> PgResult<()> {
    snapmgr_seams::invalidate_catalog_snapshot::call();
    catcache_seams::reset_catalog_caches_ext::call(debug_discard)?;
    relcache_seams::relation_cache_invalidate::call(debug_discard)?; /* gets smgr and relmap too */

    // Snapshot each callback table out before invoking, mirroring the C loops
    // over `syscache_callback_count` / `relcache_callback_count` /
    // `relsync_callback_count`.
    let (syscache, relcache, relsync) = with_state(|s| {
        (
            s.syscache_callback_list.iter().copied().collect::<Vec<_>>(),
            s.relcache_callback_list.iter().copied().collect::<Vec<_>>(),
            s.relsync_callback_list.iter().copied().collect::<Vec<_>>(),
        )
    });

    for ccitem in syscache {
        ccitem.invoke(ccitem.id as i32, 0);
    }

    for ccitem in relcache {
        ccitem.invoke(InvalidOid);
    }

    for ccitem in relsync {
        (ccitem.function)(ccitem.arg, InvalidOid);
    }

    Ok(())
}

/// `InvalidateSystemCaches`.
///
/// This blows away all tuples in the system catalog caches and all the cached
/// relation descriptors and smgr cache entries. We call this when we see a
/// shared-inval-queue overflow signal.
pub fn InvalidateSystemCaches() -> PgResult<()> {
    InvalidateSystemCachesExtended(false)
}

/// `AcceptInvalidationMessages` — read and process the shared invalidation
/// message queue (then the `debug_discard_caches` recursion guard).
pub fn AcceptInvalidationMessages() -> PgResult<()> {
    // USE_ASSERT_CHECKING block (message handlers shall access catalogs only
    // during transactions) is debug-only and omitted.

    // ReceiveSharedInvalidMessages(LocalExecuteInvalidationMessage,
    //                              InvalidateSystemCaches)
    //
    // The seam invokes its callbacks with `void` return; capture any error
    // raised by a callback and re-raise it after the drain returns.
    let mut inval_err: Option<::types_error::PgError> = None;
    let mut reset_err: Option<::types_error::PgError> = None;
    {
        let inval_err = &mut inval_err;
        let reset_err = &mut reset_err;
        let mut inval_function = |msg: &SharedInvalidationMessage| {
            if inval_err.is_none() {
                if let Err(e) = LocalExecuteInvalidationMessage(msg) {
                    *inval_err = Some(e);
                }
            }
        };
        let mut reset_function = || {
            if reset_err.is_none() {
                if let Err(e) = InvalidateSystemCaches() {
                    *reset_err = Some(e);
                }
            }
        };
        sinval_seams::receive_shared_invalid_messages::call(
            &mut inval_function,
            &mut reset_function,
        )?;
    }
    if let Some(e) = inval_err {
        return Err(e);
    }
    if let Some(e) = reset_err {
        return Err(e);
    }

    // DISCARD_CACHES_ENABLED test-only block: force cache flushes anytime a
    // flush could happen, bounded by `debug_discard_caches` recursion depth.
    let depth = crate::ACCEPT_RECURSION_DEPTH.with(|c| c.get());
    if depth < crate::debug_discard_caches() {
        crate::ACCEPT_RECURSION_DEPTH.with(|c| c.set(depth + 1));
        let result = InvalidateSystemCachesExtended(true);
        crate::ACCEPT_RECURSION_DEPTH.with(|c| c.set(depth));
        result?;
    }

    Ok(())
}

/// `ProcessInvalidationMessages(group, func)` (public) — run `func` for every
/// message in `group`, catcache entries first.
///
/// Siblings call this against a group they have collected; it walks the dense
/// arrays in the backend-local state.
pub fn ProcessInvalidationMessages(
    group: &InvalidationMsgsGroup,
    func: &mut dyn FnMut(&SharedInvalidationMessage) -> PgResult<()>,
) -> PgResult<()> {
    with_state(|s| process_invalidation_messages_group(&s.message_arrays, group, |msg| func(msg)))
}

/// Snapshot a group's messages (catcache subgroup first) into a plain `Vec`
/// before releasing the state borrow and calling a seam.
pub(crate) fn collect_group_messages<'mcx>(
    _mcx: Mcx<'mcx>,
    arrays: &[InvalMessageArray<'mcx>; 2],
    group: &InvalidationMsgsGroup,
) -> PgResult<Vec<SharedInvalidationMessage>> {
    let mut out = Vec::with_capacity(group.num_messages_in_group());
    process_invalidation_messages_group(arrays, group, |msg| {
        out.push(*msg);
        Ok(())
    })?;
    Ok(out)
}

/// Snapshot a group's messages as one batch per non-empty subgroup.
pub(crate) fn collect_group_messages_multi<'mcx>(
    _mcx: Mcx<'mcx>,
    arrays: &[InvalMessageArray<'mcx>; 2],
    group: &InvalidationMsgsGroup,
) -> PgResult<Vec<Vec<SharedInvalidationMessage>>> {
    let mut out: Vec<Vec<SharedInvalidationMessage>> = Vec::new();
    process_invalidation_messages_multi(arrays, group, |msgs| {
        out.push(msgs.to_vec());
        Ok(())
    })?;
    Ok(out)
}
