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
use backend_utils_cache_relcache_seams as relcache_seams;
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
    tuple_data: &[u8],
    newtuple: Option<&HeapTupleData<'_>>,
    newtuple_data: Option<&[u8]>,
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

    let tuple_rel_id = relation.rd_id; /* RelationGetRelid(relation) */

    /*
     * First let the catcache do its thing.
     *
     * PrepareToInvalidateCacheTuple() may lazily initialize a not-yet-built
     * catcache (CatalogCacheInitializeCache), which in turn fires syscache
     * callbacks (CallSyscacheCallbacks) — i.e. it RE-ENTERS the invalidation
     * machinery. We must therefore run it BEFORE taking the `with_state`
     * borrow; doing it inside the borrow would make the re-entrant
     * CallSyscacheCallbacks take a second `borrow_mut()` and panic
     * ("already borrowed"). C's flat file-statics re-enter freely, so this is
     * a port-introduced escalation. The request structs are plain Copy values,
     * so we collect them into an owned Vec here and replay them under the
     * borrow below. The scratch context hosts the transient PgVec the seam
     * allocates (mirroring C's PrepareToInvalidateCacheTuple workspace).
     */
    let catcache_reqs: Vec<types_storage::PrepareToInvalidateCacheTuple> =
        if catalog_seams::relation_invalidates_snapshots_only::call(tuple_rel_id) {
            Vec::new()
        } else {
            let ctx = mcx::MemoryContext::new("PrepareToInvalidateCacheTuple");
            let reqs = catcache_seams::prepare_to_invalidate_cache_tuple::call(
                ctx.mcx(),
                relation,
                tuple,
                tuple_data,
                newtuple,
                newtuple_data,
            )?;
            reqs.iter().copied().collect()
        };

    with_state(|state| {
        let mcx = state.mcx;

        /* Allocate any required resources. */
        let info = if use_inplace {
            prepare_inplace_invalidation_state(mcx, state)
        } else {
            prepare_invalidation_state(mcx, state)?
        };

        if catalog_seams::relation_invalidates_snapshots_only::call(tuple_rel_id) {
            let database_id = if catalog_seams::is_shared_relation::call(tuple_rel_id) {
                InvalidOid
            } else {
                init_small_seams::my_database_id::call()
            };
            register_snapshot_invalidation(mcx, state, info, database_id, tuple_rel_id)?;
        } else {
            /*
             * The owned model returns one request per callback invocation in
             * the same order; replay each through RegisterCatcacheInvalidation.
             */
            for req in &catcache_reqs {
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
            let classtup = syscache_seams::pg_class_shape::call(tuple, tuple_data);
            let database_id = if classtup.relisshared {
                InvalidOid
            } else {
                init_small_seams::my_database_id::call()
            };
            (classtup.oid, database_id)
        } else if tuple_rel_id == types_core::catalog::ATTRIBUTE_RELATION_ID {
            let relation_id = syscache_seams::pg_attribute_attrelid::call(tuple, tuple_data);
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
            let relation_id = syscache_seams::pg_index_indexrelid::call(tuple, tuple_data);
            (relation_id, init_small_seams::my_database_id::call())
        } else if tuple_rel_id == types_core::catalog::CONSTRAINT_RELATION_ID {
            /*
             * Foreign keys are part of relcache entries, too, so send out an
             * inval for the table that the FK applies to.
             */
            match syscache_seams::pg_constraint_fk_target::call(tuple, tuple_data) {
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
///
/// `tuple_data` / `newtuple_data` are the respective tuples' user-data areas
/// (`(char *) t_data + t_hoff`), i.e. the caller's `FormedTuple.data`, threaded
/// through so the cache-key columns can be deformed during invalidation.
pub fn CacheInvalidateHeapTuple(
    relation: &RelationData<'_>,
    tuple: &HeapTupleData<'_>,
    tuple_data: &[u8],
    newtuple: Option<&HeapTupleData<'_>>,
    newtuple_data: Option<&[u8]>,
) -> PgResult<()> {
    cache_invalidate_heap_tuple_common(
        relation,
        tuple,
        tuple_data,
        newtuple,
        newtuple_data,
        false,
    )
}

/// `CacheInvalidateHeapTuple(rel, tuple, NULL)` reduced to the
/// `(classId, objectId)` shape the typecmds ALTER DOMAIN paths use: they send
/// an sinval for a catalog row they did not themselves change (so dependent
/// cached plans get rebuilt). In C the caller already holds the open relation
/// and a freshly fetched syscache tuple:
///
/// ```c
/// rel = table_open(TypeRelationId, ...Lock);
/// tup = SearchSysCacheCopy1(TYPEOID, ObjectIdGetDatum(domainoid));
/// ...
/// CacheInvalidateHeapTuple(rel, tup, NULL);
/// ```
///
/// The seam hands only the OID pair, so re-fetch both here: open the catalog
/// relcache entry (`RelationIdGetRelation`) and the live syscache tuple
/// (`SearchSysCache1`), then run the shared `CacheInvalidateHeapTuple` body
/// over them. Only `TypeRelationId` is exercised (both ALTER DOMAIN call
/// sites), which maps to the `TYPEOID` syscache; other catalogs would need
/// their class->cacheid mapping added here.
pub fn CacheInvalidateHeapTupleByOid(class_id: Oid, object_id: Oid) -> PgResult<()> {
    /* SysCacheIdentifier for the catalog: only pg_type is reached today. */
    const TYPEOID: i32 = 82;
    let cache_id = if class_id == types_core::catalog::TYPE_RELATION_ID {
        TYPEOID
    } else {
        return Err(PgError::error(format!(
            "CacheInvalidateHeapTuple: unsupported catalog {class_id}"
        )));
    };

    /*
     * The re-fetched relation copy and syscache tuple only need to live for the
     * duration of this call (mirroring C's SearchSysCache tuple, which the
     * caller releases right after CacheInvalidateHeapTuple). Host them in a
     * short-lived context.
     */
    let ctx = mcx::MemoryContext::new("CacheInvalidateHeapTuple");
    let mcx = ctx.mcx();

    /* rel = table_open(classId, AccessShareLock) */
    let relation = match relcache_seams::relation_id_get_relation::call(mcx, class_id)? {
        Some(rel) => rel,
        None => {
            return Err(PgError::error(format!(
                "could not open relation with OID {class_id}"
            )));
        }
    };

    /* tup = SearchSysCache1(cacheId, ObjectIdGetDatum(objectId)) */
    let result = (|| {
        let key = types_cache::SysCacheKey::Value(ScalarWord::from_oid(object_id));
        let formed = catcache_seams::search_cat_cache_1::call(mcx, cache_id, key)?;
        match formed {
            Some(tuple) => cache_invalidate_heap_tuple_common(
                &relation,
                &tuple.tuple,
                &tuple.data,
                None,
                None,
                false,
            ),
            None => Err(PgError::error(format!(
                "cache lookup failed for object {object_id} in catalog {class_id}"
            ))),
        }
    })();

    /* table_close(rel, AccessShareLock) — drop the relcache pin either way. */
    relcache_seams::relation_close::call(class_id)?;
    result
}

/// `CacheInvalidateHeapTupleInplace`.
pub fn CacheInvalidateHeapTupleInplace(
    relation: &RelationData<'_>,
    key_equivalent_tuple: &HeapTupleData<'_>,
    key_equivalent_data: &[u8],
) -> PgResult<()> {
    cache_invalidate_heap_tuple_common(
        relation,
        key_equivalent_tuple,
        key_equivalent_data,
        None,
        None,
        true,
    )
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

/// `CacheInvalidateRelcacheByTuple`. `class_data` is the tuple's user-data
/// area (`(char *) t_data + t_hoff`) for the `GETSTRUCT` deform.
pub fn CacheInvalidateRelcacheByTuple(
    classTuple: &HeapTupleData<'_>,
    class_data: &[u8],
) -> PgResult<()> {
    let classtup = syscache_seams::pg_class_shape::call(classTuple, class_data);
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

/// `CacheInvalidateRelcacheByTuple`, but with the pg_class row already
/// deformed by the caller into `(relid, form)`. Mirrors the C body exactly:
/// `relationId = classtup->oid`; `databaseId = relisshared ? InvalidOid :
/// MyDatabaseId`; `RegisterRelcacheInvalidation(PrepareInvalidationState(),
/// databaseId, relationId)`. The trimmed `PgClassForm` lacks the system `oid`
/// column, so the caller supplies the relation OID directly.
pub fn CacheInvalidateRelcacheByPgClass(
    relid: Oid,
    form: &types_cluster::PgClassForm,
) -> PgResult<()> {
    let database_id = if form.relisshared {
        InvalidOid
    } else {
        init_small_seams::my_database_id::call()
    };

    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(mcx, state)?;
        register_relcache_invalidation(mcx, state, info, database_id, relid)
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

/// `SendSharedInvalidMessages((SharedInvalidationMessage *) bufptr, nmsgs)`
/// from a still-serialized `SharedInvalidationMessage[]` byte buffer.
///
/// `FinishPreparedTransaction` (twophase.c) replays the invalidation messages
/// that the 2PC state file carries as a raw on-disk array; in C it casts the
/// buffer pointer straight to `SharedInvalidationMessage *`. The byte layout
/// may be unaligned here, so decode each 16-byte entry on the fly via
/// `SharedInvalMessages` and forward the typed slice to sinval, exactly as the
/// C dispatcher does.
pub fn SendSharedInvalidMessagesRaw(msgs: &[u8], nmsgs: i32) -> PgResult<()> {
    let view = types_storage::SharedInvalMessages::from_bytes(msgs);
    let mut decoded: Vec<SharedInvalidationMessage> = Vec::with_capacity(nmsgs as usize);
    for i in 0..nmsgs as usize {
        if let Some(m) = view.get(i) {
            decoded.push(m);
        }
    }
    sinval_seams::send_shared_invalid_messages::call(&decoded)
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
    register_syscache_callback_tagged(cacheid, crate::SyscacheCallback::Full(func), arg)
}

/// Shared body of `CacheRegisterSyscacheCallback` that takes an already-tagged
/// [`crate::SyscacheCallback`] (the `Full` C path and plancache's projected
/// path differ only in the stored callback shape).
fn register_syscache_callback_tagged(
    cacheid: i32,
    function: crate::SyscacheCallback,
    arg: ScalarWord,
) -> PgResult<()> {
    if cacheid < 0 || cacheid >= SYS_CACHE_SIZE as i32 {
        /* elog(FATAL, "invalid cache ID: %d", cacheid) */
        return Err(PgError::new(FATAL, format!("invalid cache ID: {cacheid}")));
    }

    with_state(|state| {
        if state.syscache_callback_list.len() >= MAX_SYSCACHE_CALLBACKS {
            return Err(PgError::new(
                FATAL,
                "out of syscache_callback_list slots".to_string(),
            ));
        }

        let count = state.syscache_callback_list.len() as i16;
        let cidx = cacheid as usize;

        if state.syscache_callback_links[cidx] == 0 {
            state.syscache_callback_links[cidx] = count + 1;
        } else {
            let mut i = (state.syscache_callback_links[cidx] - 1) as usize;
            while state.syscache_callback_list[i].link > 0 {
                i = (state.syscache_callback_list[i].link - 1) as usize;
            }
            state.syscache_callback_list[i].link = count + 1;
        }

        state.syscache_callback_list.push(SyscacheCallbackItem {
            id: cacheid as i16,
            link: 0,
            function,
            arg,
        });

        Ok(())
    })
}

/// Plancache's `CacheRegisterSyscacheCallback(cacheid, func, (Datum) 0)`,
/// routed through the `*-pc-seams` boundary with the `arg` projected out.
/// Mirrors C: plancache always passes a zero `Datum`.
pub fn CacheRegisterSyscacheCallbackPlanCache(
    cacheid: i32,
    func: types_plancache::SyscacheCallbackFn,
) -> PgResult<()> {
    register_syscache_callback_tagged(cacheid, crate::SyscacheCallback::Plancache(func), ScalarWord::null())
}

/// `CacheRegisterRelcacheCallback`.
pub fn CacheRegisterRelcacheCallback(func: RelcacheCallbackFunction, arg: ScalarWord) -> PgResult<()> {
    register_relcache_callback_tagged(crate::RelcacheCallback::Full(func), arg)
}

/// Shared body of `CacheRegisterRelcacheCallback` over a tagged callback.
fn register_relcache_callback_tagged(
    function: crate::RelcacheCallback,
    arg: ScalarWord,
) -> PgResult<()> {
    with_state(|state| {
        if state.relcache_callback_list.len() >= MAX_RELCACHE_CALLBACKS {
            return Err(PgError::new(
                FATAL,
                "out of relcache_callback_list slots".to_string(),
            ));
        }

        state
            .relcache_callback_list
            .push(RelcacheCallbackItem { function, arg });

        Ok(())
    })
}

/// Plancache's `CacheRegisterRelcacheCallback(func, (Datum) 0)`, routed through
/// the `*-pc-seams` boundary with the `arg` projected out.
pub fn CacheRegisterRelcacheCallbackPlanCache(
    func: types_plancache::RelcacheCallbackFn,
) -> PgResult<()> {
    register_relcache_callback_tagged(crate::RelcacheCallback::Plancache(func), ScalarWord::null())
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
    let to_call: Vec<SyscacheCallbackItem> = with_state(|state| {
        let mut out = Vec::new();
        let mut i = (state.syscache_callback_links[cacheid as usize] - 1) as i32;
        while i >= 0 {
            let ccitem = state.syscache_callback_list[i as usize];
            debug_assert!(ccitem.id == cacheid as i16);
            out.push(ccitem);
            i = ccitem.link as i32 - 1;
        }
        out
    });

    for ccitem in to_call {
        ccitem.invoke(cacheid, hashvalue);
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
