//! `CacheInvalidate*` entry points (inval.c) plus the callback registration
//! and dispatch (`CacheRegister*Callback`, `CallSyscacheCallbacks`,
//! `CallRelSyncCallbacks`).

use types_core::primitive::{OidIsValid, InvalidOid};
use types_core::Oid;
// Bare-word machine-word `Datum` (`types_datum::Datum`), aliased `ScalarWord`:
// the opaque callback-registration cookie, stored and handed back verbatim
// (never deformed). See the crate-root note; the canonical `Datum<'mcx>` enum
// is for deformed tuple values, not passthrough args.
use types_datum::Datum as ScalarWord;
use types_error::{PgError, PgResult, FATAL};
use types_rel::RelationData;
use types_storage::{
    RelFileLocatorBackend, SharedInvalidationMessage, SharedInvalRelmapMsg, SharedInvalSmgrMsg,
};
use types_tuple::HeapTupleData;

use crate::registration::{
    prepare_inplace_invalidation_state, prepare_invalidation_state, register_catalog_invalidation,
    register_catcache_invalidation, register_relcache_invalidation, register_relsync_invalidation,
    register_snapshot_invalidation, InfoRef,
};
use crate::RelSyncCallbackFunction;
use crate::{
    with_state, RelcacheCallbackItem, RelsyncCallbackItem, SyscacheCallbackItem,
    MAX_RELCACHE_CALLBACKS, MAX_RELSYNC_CALLBACKS, MAX_SYSCACHE_CALLBACKS, SYS_CACHE_SIZE,
};
use types_cache::{RelcacheCallbackFunction, SyscacheCallbackFunction};

use backend_catalog_catalog_seams as catalog_seams;
use backend_storage_ipc_sinval_seams as sinval_seams;
use backend_utils_cache_catcache_seams as catcache_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_init_miscinit_seams as miscinit_seams;
use backend_utils_init_small_seams as init_small_seams;

/// `CacheInvalidateHeapTupleCommon` — shared end-of-command / inplace logic.
///
/// In C `prepare_callback` selects either `PrepareInvalidationState` (the
/// transactional path, `use_inplace == false`) or
/// `PrepareInplaceInvalidationState` (`use_inplace == true`).
pub(crate) fn cache_invalidate_heap_tuple_common(
    relation: &RelationData<'_>,
    tuple: &HeapTupleData<'_>,
    newtuple: Option<&HeapTupleData<'_>>,
    use_inplace: bool,
) -> PgResult<()> {
    /* PrepareToInvalidateCacheTuple() needs relcache */
    /* AssertCouldGetRelation(); -- assertion only */

    /* Do nothing during bootstrap */
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }

    /*
     * We only need to worry about invalidation for tuples that are in system
     * catalogs; user-relation tuples are never in catcaches and can't affect
     * the relcache either.
     */
    if !catalog_seams::is_catalog_relation::call(relation) {
        return Ok(());
    }

    /*
     * IsCatalogRelation() will return true for TOAST tables of system
     * catalogs, but we don't care about those, either.
     */
    if catalog_seams::is_toast_relation::call(relation) {
        return Ok(());
    }

    with_state(|state| {
        let mcx = state.mcx;

        /* Allocate any required resources. */
        let info = if use_inplace {
            prepare_inplace_invalidation_state(mcx, state)
        } else {
            prepare_invalidation_state(mcx, state)?
        };

        /*
         * First let the catcache do its thing
         */
        let tuple_rel_id = relation.rd_id; /* RelationGetRelid(relation) */
        if catalog_seams::relation_invalidates_snapshots_only::call(tuple_rel_id) {
            let database_id = if catalog_seams::is_shared_relation::call(tuple_rel_id) {
                InvalidOid
            } else {
                init_small_seams::my_database_id::call()
            };
            register_snapshot_invalidation(mcx, state, info, database_id, tuple_rel_id)?;
        } else {
            /*
             * PrepareToInvalidateCacheTuple(relation, tuple, newtuple,
             *     RegisterCatcacheInvalidation, (void *) info)
             *
             * The owned model returns one request per callback invocation in
             * the same order; replay each through RegisterCatcacheInvalidation.
             */
            let reqs =
                catcache_seams::prepare_to_invalidate_cache_tuple::call(mcx, relation, tuple, newtuple)?;
            for req in reqs.iter() {
                register_catcache_invalidation(
                    mcx,
                    state,
                    info,
                    req.cache_id,
                    req.hash_value,
                    req.db_id,
                )?;
            }
        }

        /*
         * Now, is this tuple one of the primary definers of a relcache entry?
         *
         * Note we ignore newtuple here; we assume an update cannot move a tuple
         * from being part of one relcache entry to being part of another.
         */
        let (relation_id, database_id) = if tuple_rel_id == types_core::catalog::RELATION_RELATION_ID
        {
            let classtup = syscache_seams::pg_class_shape::call(tuple);
            let database_id = if classtup.relisshared {
                InvalidOid
            } else {
                init_small_seams::my_database_id::call()
            };
            (classtup.oid, database_id)
        } else if tuple_rel_id == types_core::catalog::ATTRIBUTE_RELATION_ID {
            let relation_id = syscache_seams::pg_attribute_attrelid::call(tuple);
            /*
             * KLUGE ALERT: we always send the relcache event with MyDatabaseId,
             * even if the rel in question is shared (which we can't easily tell).
             */
            (relation_id, init_small_seams::my_database_id::call())
        } else if tuple_rel_id == types_core::catalog::INDEX_RELATION_ID {
            /*
             * When a pg_index row is updated, we should send out a relcache inval
             * for the index relation.
             */
            let relation_id = syscache_seams::pg_index_indexrelid::call(tuple);
            (relation_id, init_small_seams::my_database_id::call())
        } else if tuple_rel_id == types_core::catalog::CONSTRAINT_RELATION_ID {
            /*
             * Foreign keys are part of relcache entries, too, so send out an
             * inval for the table that the FK applies to.
             */
            match syscache_seams::pg_constraint_fk_target::call(tuple) {
                Some(conrelid) if OidIsValid(conrelid) => {
                    (conrelid, init_small_seams::my_database_id::call())
                }
                _ => return Ok(()),
            }
        } else {
            return Ok(());
        };

        /*
         * Yes.  We need to register a relcache invalidation event.
         */
        register_relcache_invalidation(mcx, state, info, database_id, relation_id)
    })
}

/// `CacheInvalidateHeapTuple`.
pub fn CacheInvalidateHeapTuple(
    relation: &RelationData<'_>,
    tuple: &HeapTupleData<'_>,
    newtuple: Option<&HeapTupleData<'_>>,
) -> PgResult<()> {
    cache_invalidate_heap_tuple_common(relation, tuple, newtuple, false)
}

/// `CacheInvalidateHeapTupleInplace`.
pub fn CacheInvalidateHeapTupleInplace(
    relation: &RelationData<'_>,
    key_equivalent_tuple: &HeapTupleData<'_>,
) -> PgResult<()> {
    cache_invalidate_heap_tuple_common(relation, key_equivalent_tuple, None, true)
}

/// `CacheInvalidateCatalog`.
pub fn CacheInvalidateCatalog(catalogId: Oid) -> PgResult<()> {
    let database_id = if catalog_seams::is_shared_relation::call(catalogId) {
        InvalidOid
    } else {
        init_small_seams::my_database_id::call()
    };

    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(mcx, state)?;
        register_catalog_invalidation(mcx, state, info, database_id, catalogId)
    })
}

/// `CacheInvalidateRelcache`.
pub fn CacheInvalidateRelcache(relation: &RelationData<'_>) -> PgResult<()> {
    let relation_id = relation.rd_id; /* RelationGetRelid(relation) */
    /*
     * C reads `relation->rd_rel->relisshared`; the trimmed relcache copy does
     * not carry that field, so use the equivalent OID-range probe
     * `IsSharedRelation(relationId)`.
     */
    let database_id = if catalog_seams::is_shared_relation::call(relation_id) {
        InvalidOid
    } else {
        init_small_seams::my_database_id::call()
    };

    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(mcx, state)?;
        register_relcache_invalidation(mcx, state, info, database_id, relation_id)
    })
}

/// `CacheInvalidateRelcacheAll`.
pub fn CacheInvalidateRelcacheAll() -> PgResult<()> {
    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(mcx, state)?;
        register_relcache_invalidation(mcx, state, info, InvalidOid, InvalidOid)
    })
}

/// `CacheInvalidateRelcacheByTuple`.
pub fn CacheInvalidateRelcacheByTuple(classTuple: &HeapTupleData<'_>) -> PgResult<()> {
    let classtup = syscache_seams::pg_class_shape::call(classTuple);
    let relation_id = classtup.oid;
    let database_id = if classtup.relisshared {
        InvalidOid
    } else {
        init_small_seams::my_database_id::call()
    };

    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(mcx, state)?;
        register_relcache_invalidation(mcx, state, info, database_id, relation_id)
    })
}

/// `CacheInvalidateRelcacheByRelid`.
pub fn CacheInvalidateRelcacheByRelid(relid: Oid) -> PgResult<()> {
    /*
     * tup = SearchSysCache1(RELOID, ObjectIdGetDatum(relid));
     * if (!HeapTupleIsValid(tup)) elog(ERROR, "cache lookup failed ...");
     * CacheInvalidateRelcacheByTuple(tup); ReleaseSysCache(tup);
     *
     * The owned syscache lookup returns the already-projected pg_class shape
     * (oid + relisshared); the installer owns the ReleaseSysCache. Folding
     * the body of CacheInvalidateRelcacheByTuple in directly avoids needing a
     * HeapTuple to re-pass.
     */
    let classtup = match syscache_seams::lookup_pg_class_by_relid::call(relid)? {
        Some(shape) => shape,
        None => {
            return Err(PgError::error(format!(
                "cache lookup failed for relation {relid}"
            )));
        }
    };

    let relation_id = classtup.oid;
    let database_id = if classtup.relisshared {
        InvalidOid
    } else {
        init_small_seams::my_database_id::call()
    };

    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(mcx, state)?;
        register_relcache_invalidation(mcx, state, info, database_id, relation_id)
    })
}

/// `CacheInvalidateRelSync`.
pub fn CacheInvalidateRelSync(relid: Oid) -> PgResult<()> {
    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(mcx, state)?;
        register_relsync_invalidation(
            mcx,
            state,
            info,
            init_small_seams::my_database_id::call(),
            relid,
        )
    })
}

/// `CacheInvalidateRelSyncAll`.
pub fn CacheInvalidateRelSyncAll() -> PgResult<()> {
    CacheInvalidateRelSync(InvalidOid)
}

/// `CacheInvalidateSmgr` — broadcast an smgr-close invalidation immediately.
///
/// Note: these messages are nontransactional, so they are sent immediately
/// without any queuing. In order to avoid bloating SharedInvalidationMessage,
/// we store only three bytes of the ProcNumber using what would otherwise be
/// padding space; thus the maximum possible ProcNumber is 2^23-1.
pub fn CacheInvalidateSmgr(rlocator: RelFileLocatorBackend) -> PgResult<()> {
    /* verify optimization stated above stays valid */
    const _: () = assert!(crate::MAX_BACKENDS_BITS <= 23);

    let msg = SharedInvalidationMessage::Smgr(SharedInvalSmgrMsg {
        backend_hi: (rlocator.backend >> 16) as i8,
        backend_lo: (rlocator.backend & 0xffff) as u16,
        rlocator: rlocator.locator,
    });

    sinval_seams::send_shared_invalid_messages::call(&[msg])
}

/// `CacheInvalidateRelmap` — broadcast a relmap-change invalidation immediately.
///
/// Note: these messages are nontransactional, so they are sent immediately.
pub fn CacheInvalidateRelmap(databaseId: Oid) -> PgResult<()> {
    let msg = SharedInvalidationMessage::Relmap(SharedInvalRelmapMsg { dbId: databaseId });

    sinval_seams::send_shared_invalid_messages::call(&[msg])
}

/* ------------------------------------------------------------------------
 *  Callback registration / dispatch
 * ------------------------------------------------------------------------ */

/// `CacheRegisterSyscacheCallback`.
pub fn CacheRegisterSyscacheCallback(
    cacheid: i32,
    func: SyscacheCallbackFunction,
    arg: ScalarWord,
) -> PgResult<()> {
    if cacheid < 0 || cacheid >= SYS_CACHE_SIZE as i32 {
        /* elog(FATAL, "invalid cache ID: %d", cacheid) */
        return Err(PgError::new(FATAL, format!("invalid cache ID: {cacheid}")));
    }

    with_state(|state| {
        if state.syscache_callback_list.len() >= MAX_SYSCACHE_CALLBACKS {
            /* elog(FATAL, "out of syscache_callback_list slots") */
            return Err(PgError::new(
                FATAL,
                "out of syscache_callback_list slots".to_string(),
            ));
        }

        let count = state.syscache_callback_list.len() as i16;
        let cidx = cacheid as usize;

        if state.syscache_callback_links[cidx] == 0 {
            /* first callback for this cache */
            state.syscache_callback_links[cidx] = count + 1;
        } else {
            /* add to end of chain, so that older callbacks are called first */
            let mut i = (state.syscache_callback_links[cidx] - 1) as usize;
            while state.syscache_callback_list[i].link > 0 {
                i = (state.syscache_callback_list[i].link - 1) as usize;
            }
            state.syscache_callback_list[i].link = count + 1;
        }

        state.syscache_callback_list.push(SyscacheCallbackItem {
            id: cacheid as i16,
            link: 0,
            function: func,
            arg,
        });

        Ok(())
    })
}

/// `CacheRegisterRelcacheCallback`.
pub fn CacheRegisterRelcacheCallback(func: RelcacheCallbackFunction, arg: ScalarWord) -> PgResult<()> {
    with_state(|state| {
        if state.relcache_callback_list.len() >= MAX_RELCACHE_CALLBACKS {
            /* elog(FATAL, "out of relcache_callback_list slots") */
            return Err(PgError::new(
                FATAL,
                "out of relcache_callback_list slots".to_string(),
            ));
        }

        state
            .relcache_callback_list
            .push(RelcacheCallbackItem { function: func, arg });

        Ok(())
    })
}

/// `CacheRegisterRelSyncCallback`.
pub fn CacheRegisterRelSyncCallback(func: RelSyncCallbackFunction, arg: ScalarWord) -> PgResult<()> {
    with_state(|state| {
        if state.relsync_callback_list.len() >= MAX_RELSYNC_CALLBACKS {
            /* elog(FATAL, "out of relsync_callback_list slots") */
            return Err(PgError::new(
                FATAL,
                "out of relsync_callback_list slots".to_string(),
            ));
        }

        state
            .relsync_callback_list
            .push(RelsyncCallbackItem { function: func, arg });

        Ok(())
    })
}

/// `CallSyscacheCallbacks`.
///
/// This is exported so that CatalogCacheFlushCatalog can call it, saving this
/// module from knowing which catcache IDs correspond to which catalogs.
pub fn CallSyscacheCallbacks(cacheid: i32, hashvalue: u32) -> PgResult<()> {
    if cacheid < 0 || cacheid >= SYS_CACHE_SIZE as i32 {
        return Err(PgError::error(format!("invalid cache ID: {cacheid}")));
    }

    /*
     * Snapshot the callbacks to invoke (id, link chain head). We must not hold
     * the borrow of the state across the user callback, which may re-enter the
     * inval machinery; collect the (function, arg) pairs first.
     */
    let to_call: Vec<(SyscacheCallbackFunction, ScalarWord)> = with_state(|state| {
        let mut out = Vec::new();
        let mut i = (state.syscache_callback_links[cacheid as usize] - 1) as i32;
        while i >= 0 {
            let ccitem = state.syscache_callback_list[i as usize];
            debug_assert!(ccitem.id == cacheid as i16);
            out.push((ccitem.function, ccitem.arg));
            i = ccitem.link as i32 - 1;
        }
        out
    });

    for (function, arg) in to_call {
        function(arg, cacheid, hashvalue);
    }

    Ok(())
}

/// `CallRelSyncCallbacks`.
pub fn CallRelSyncCallbacks(relid: Oid) -> PgResult<()> {
    let to_call: Vec<(RelSyncCallbackFunction, ScalarWord)> = with_state(|state| {
        state
            .relsync_callback_list
            .iter()
            .map(|ccitem| (ccitem.function, ccitem.arg))
            .collect()
    });

    for (function, arg) in to_call {
        function(arg, relid);
    }

    Ok(())
}
