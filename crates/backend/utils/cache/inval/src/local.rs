use types_core::{InvalidOid, ProcNumber};
use types_error::PgResult;
use types_storage::{RelFileLocatorBackend, SharedInvalidationMessage};

use crate::invalidate::{CallRelSyncCallbacks, CallSyscacheCallbacks};
use crate::msgs::InvalidationMsgsGroup;
use crate::CALLBACKS;

// Catcache is the dominant replay kind (every catalog tuple change); its arm
// stays inline in the walk like C's switch, the rest go through one call.
#[inline]
pub fn LocalExecuteInvalidationMessage(msg: &SharedInvalidationMessage) -> PgResult<()> {
    match *msg {
        SharedInvalidationMessage::Catcache(m) => {
            if m.dbId == init_small::globals::MyDatabaseId() || m.dbId == InvalidOid {
                snapmgr_seams::invalidate_catalog_snapshot::call();
                syscache_seams::sys_cache_invalidate::call(m.id as i32, m.hashValue)?;
                CallSyscacheCallbacks(m.id as i32, m.hashValue)?;
            }
            Ok(())
        }
        _ => local_execute_other(msg),
    }
}

#[inline(never)]
fn local_execute_other(msg: &SharedInvalidationMessage) -> PgResult<()> {
    let my_database_id = init_small::globals::MyDatabaseId();

    match *msg {
        SharedInvalidationMessage::Catcache(_) => unreachable!("dispatched inline"),
        SharedInvalidationMessage::Catalog(m) => {
            if m.dbId == my_database_id || m.dbId == InvalidOid {
                snapmgr_seams::invalidate_catalog_snapshot::call();
                // CatalogCacheFlushCatalog calls CallSyscacheCallbacks as needed.
                catcache_seams::catalog_cache_flush_catalog::call(m.catId)?;
            }
        }
        SharedInvalidationMessage::Relcache(m) => {
            if m.dbId == my_database_id || m.dbId == InvalidOid {
                if m.relId == InvalidOid {
                    relcache_seams::relation_cache_invalidate::call(false)?;
                } else {
                    relcache_seams::relation_cache_invalidate_entry::call(m.relId)?;
                }

                let mut i = 0usize;
                while let Some(ccitem) = CALLBACKS.with(|c| {
                    let t = c.borrow();
                    if i < t.relcache_count { t.relcache_list[i] } else { None }
                }) {
                    (ccitem.function)(ccitem.arg, m.relId);
                    i += 1;
                }
            }
        }
        SharedInvalidationMessage::Smgr(m) => {
            let rlocator = RelFileLocatorBackend {
                locator: m.rlocator,
                backend: (((m.backend_hi as i32) << 16) | (m.backend_lo as i32)) as ProcNumber,
            };
            smgr_seams::smgr_release_rel_locator::call(rlocator)?;
        }
        SharedInvalidationMessage::Relmap(m) => {
            if m.dbId == InvalidOid {
                relmapper_seams::relation_map_invalidate::call(true)?;
            } else if m.dbId == my_database_id {
                relmapper_seams::relation_map_invalidate::call(false)?;
            }
        }
        SharedInvalidationMessage::Snapshot(m) => {
            if m.dbId == InvalidOid || m.dbId == my_database_id {
                snapmgr_seams::invalidate_catalog_snapshot::call();
            }
        }
        SharedInvalidationMessage::RelSync(m) => {
            if m.dbId == my_database_id {
                CallRelSyncCallbacks(m.relid)?;
            }
        }
    }
    Ok(())
}

pub fn InvalidateSystemCachesExtended(debug_discard: bool) -> PgResult<()> {
    snapmgr_seams::invalidate_catalog_snapshot::call();
    catcache_seams::reset_catalog_caches_ext::call(debug_discard)?;
    relcache_seams::relation_cache_invalidate::call(debug_discard)?; /* gets smgr and relmap too */

    let mut i = 0usize;
    while let Some(ccitem) = CALLBACKS.with(|c| {
        let t = c.borrow();
        if i < t.syscache_count { t.syscache_list[i] } else { None }
    }) {
        (ccitem.function)(ccitem.arg, ccitem.id as i32, 0);
        i += 1;
    }

    let mut i = 0usize;
    while let Some(ccitem) = CALLBACKS.with(|c| {
        let t = c.borrow();
        if i < t.relcache_count { t.relcache_list[i] } else { None }
    }) {
        (ccitem.function)(ccitem.arg, InvalidOid);
        i += 1;
    }

    let mut i = 0usize;
    while let Some(ccitem) = CALLBACKS.with(|c| {
        let t = c.borrow();
        if i < t.relsync_count { t.relsync_list[i] } else { None }
    }) {
        (ccitem.function)(ccitem.arg, InvalidOid);
        i += 1;
    }

    Ok(())
}

pub fn InvalidateSystemCaches() -> PgResult<()> {
    InvalidateSystemCachesExtended(false)
}

pub fn AcceptInvalidationMessages() -> PgResult<()> {
    sinval_seams::receive_shared_invalid_messages::call(
        &mut |msg| LocalExecuteInvalidationMessage(msg),
        &mut || InvalidateSystemCaches(),
    )?;

    // DISCARD_CACHES_ENABLED test block.
    if cfg!(debug_assertions) {
        let depth = crate::ACCEPT_RECURSION_DEPTH.get();
        if depth < crate::debug_discard_caches() {
            crate::ACCEPT_RECURSION_DEPTH.set(depth + 1);
            let result = InvalidateSystemCachesExtended(true);
            crate::ACCEPT_RECURSION_DEPTH.set(depth);
            result?;
        }
    }

    Ok(())
}

pub fn ProcessInvalidationMessages(
    group: &InvalidationMsgsGroup,
    func: &mut dyn FnMut(&SharedInvalidationMessage) -> PgResult<()>,
) -> PgResult<()> {
    crate::eoxact::process_group_with(group, func)
}
