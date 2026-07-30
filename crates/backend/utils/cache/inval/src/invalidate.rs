use std::cell::RefCell;

use datum::Datum;
use mcx::{Mcx, MemoryContext, PgVec};
use types_core::catalog::{
    ATTRIBUTE_RELATION_ID, CONSTRAINT_RELATION_ID, INDEX_RELATION_ID, RELATION_RELATION_ID,
};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERROR, FATAL};
use types_rel::RelationData;
use types_storage::{
    RelFileLocatorBackend, SharedInvalRelmapMsg, SharedInvalSmgrMsg, SharedInvalidationMessage,
};
use types_tuple::HeapTupleData;

use crate::registration::{
    self, prepare_inplace_invalidation_state, prepare_invalidation_state,
};
use crate::{
    with_state, RelSyncCallbackFunction, RelcacheCallbackFunction, RelcacheCallbackItem,
    RelsyncCallbackItem, SyscacheCallbackFunction, SyscacheCallbackItem, CALLBACKS,
    MAX_RELCACHE_CALLBACKS, MAX_RELSYNC_CALLBACKS, MAX_SYSCACHE_CALLBACKS, SYS_CACHE_SIZE,
};

fn MyDatabaseId() -> Oid {
    init_small::globals::MyDatabaseId()
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_invalid_cache_id(level: types_error::ErrorLevel, cacheid: i32) -> Box<PgError> {
    Box::new(PgError::new(level, format!("invalid cache ID: {cacheid}")))
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_out_of_slots(list: &str) -> Box<PgError> {
    Box::new(PgError::new(FATAL, format!("out of {list} slots")))
}

thread_local! {
    static TUPLE_INVAL_SCRATCH: RefCell<MemoryContext> =
        RefCell::new(MemoryContext::new("PrepareToInvalidateCacheTuple scratch"));
}

// Reset-per-acquisition scratch; a re-entrant invalidation inside the seam
// falls back to a fresh context so the buffer is never aliased across nesting.
fn with_tuple_inval_scratch<R>(f: impl for<'s> FnOnce(Mcx<'s>) -> PgResult<R>) -> PgResult<R> {
    TUPLE_INVAL_SCRATCH.with(|cell| match cell.try_borrow_mut() {
        Ok(mut ctx) => {
            ctx.reset();
            f(ctx.mcx())
        }
        Err(_) => {
            let ctx = MemoryContext::new("PrepareToInvalidateCacheTuple (reentrant)");
            f(ctx.mcx())
        }
    })
}

fn cache_invalidate_heap_tuple_common(
    relation: &RelationData<'_>,
    tuple: &HeapTupleData<'_>,
    newtuple: Option<&HeapTupleData<'_>>,
    use_inplace: bool,
) -> PgResult<()> {
    if miscinit_seams::is_bootstrap_processing_mode::call() {
        return Ok(());
    }

    if !catalog_seams::is_catalog_relation::call(relation) {
        return Ok(());
    }
    if catalog_seams::is_toast_relation::call(relation) {
        return Ok(());
    }

    let tuple_rel_id = relation.rd_id;
    let snapshots_only = syscache_seams::relation_invalidates_snapshots_only::call(tuple_rel_id);

    with_tuple_inval_scratch(|scratch| {
        // The catcache walk may lazily init a catcache and re-enter via
        // CallSyscacheCallbacks, so it runs BEFORE the state borrow (C order
        // differs: prepare_callback first); its Copy requests replay under it.
        let reqs = if snapshots_only {
            PgVec::new_in(scratch)
        } else {
            catcache_seams::prepare_to_invalidate_cache_tuple::call(
                scratch, relation, tuple, newtuple,
            )?
        };

        let rel_target: Option<(Oid, Oid)> = if tuple_rel_id == RELATION_RELATION_ID {
            let classtup = syscache_seams::pg_class_shape::call(tuple);
            let database_id = if classtup.relisshared { InvalidOid } else { MyDatabaseId() };
            Some((classtup.oid, database_id))
        } else if tuple_rel_id == ATTRIBUTE_RELATION_ID {
            // KLUGE ALERT (C): always MyDatabaseId, even for shared rels.
            Some((syscache_seams::pg_attribute_attrelid::call(tuple), MyDatabaseId()))
        } else if tuple_rel_id == INDEX_RELATION_ID {
            Some((syscache_seams::pg_index_indexrelid::call(tuple), MyDatabaseId()))
        } else if tuple_rel_id == CONSTRAINT_RELATION_ID {
            syscache_seams::pg_constraint_fk_target::call(tuple)
                .map(|conrelid| (conrelid, MyDatabaseId()))
        } else {
            None
        };

        with_state(|state| {
            let mcx = state.mcx;
            let info = if use_inplace {
                prepare_inplace_invalidation_state(state)
            } else {
                prepare_invalidation_state(state)?
            };

            if snapshots_only {
                let database_id = if catalog_seams::is_shared_relation::call(tuple_rel_id) {
                    InvalidOid
                } else {
                    MyDatabaseId()
                };
                registration::register_snapshot_invalidation(
                    mcx, state, info, database_id, tuple_rel_id,
                )?;
            } else {
                for req in reqs.iter() {
                    registration::register_catcache_invalidation(
                        mcx, state, info, req.cache_id, req.hash_value, req.db_id,
                    )?;
                }
            }

            match rel_target {
                Some((relation_id, database_id)) => registration::register_relcache_invalidation(
                    mcx, state, info, database_id, relation_id,
                ),
                None => Ok(()),
            }
        })
    })
}

pub fn CacheInvalidateHeapTuple(
    relation: &RelationData<'_>,
    tuple: &HeapTupleData<'_>,
    newtuple: Option<&HeapTupleData<'_>>,
) -> PgResult<()> {
    cache_invalidate_heap_tuple_common(relation, tuple, newtuple, false)
}

pub fn CacheInvalidateHeapTupleInplace(
    relation: &RelationData<'_>,
    key_equivalent_tuple: &HeapTupleData<'_>,
) -> PgResult<()> {
    cache_invalidate_heap_tuple_common(relation, key_equivalent_tuple, None, true)
}

pub fn CacheInvalidateCatalog(catalogId: Oid) -> PgResult<()> {
    let database_id = if catalog_seams::is_shared_relation::call(catalogId) {
        InvalidOid
    } else {
        MyDatabaseId()
    };

    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(state)?;
        registration::register_catalog_invalidation(mcx, state, info, database_id, catalogId)
    })
}

pub fn CacheInvalidateRelcache(relation: &RelationData<'_>) -> PgResult<()> {
    let relation_id = relation.rd_id;
    let database_id = if relation.rd_rel.relisshared {
        InvalidOid
    } else {
        MyDatabaseId()
    };

    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(state)?;
        registration::register_relcache_invalidation(mcx, state, info, database_id, relation_id)
    })
}

pub fn CacheInvalidateRelcacheAll() -> PgResult<()> {
    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(state)?;
        registration::register_relcache_invalidation(mcx, state, info, InvalidOid, InvalidOid)
    })
}

pub fn CacheInvalidateRelcacheByTuple(classTuple: &HeapTupleData<'_>) -> PgResult<()> {
    let classtup = syscache_seams::pg_class_shape::call(classTuple);
    let relation_id = classtup.oid;
    let database_id = if classtup.relisshared { InvalidOid } else { MyDatabaseId() };

    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(state)?;
        registration::register_relcache_invalidation(mcx, state, info, database_id, relation_id)
    })
}

pub fn CacheInvalidateRelcacheByRelid(relid: Oid) -> PgResult<()> {
    let classtup = match syscache_seams::lookup_pg_class_by_relid::call(relid)? {
        Some(shape) => shape,
        None => return Err(err_cache_lookup_failed(relid)),
    };
    let relation_id = classtup.oid;
    let database_id = if classtup.relisshared { InvalidOid } else { MyDatabaseId() };

    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(state)?;
        registration::register_relcache_invalidation(mcx, state, info, database_id, relation_id)
    })
}

#[track_caller]
#[cold]
#[inline(never)]
fn err_cache_lookup_failed(relid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for relation {relid}")))
}

pub fn CacheInvalidateRelSync(relid: Oid) -> PgResult<()> {
    with_state(|state| {
        let mcx = state.mcx;
        let info = prepare_invalidation_state(state)?;
        registration::register_relsync_invalidation(mcx, state, info, MyDatabaseId(), relid)
    })
}

pub fn CacheInvalidateRelSyncAll() -> PgResult<()> {
    CacheInvalidateRelSync(InvalidOid)
}

// Nontransactional, sent immediately; three ProcNumber bytes travel in union
// padding, hence the MAX_BACKENDS_BITS bound.
pub fn CacheInvalidateSmgr(rlocator: RelFileLocatorBackend) -> PgResult<()> {
    const { assert!(crate::MAX_BACKENDS_BITS <= 23) };

    let msg = SharedInvalidationMessage::Smgr(SharedInvalSmgrMsg {
        backend_hi: (rlocator.backend >> 16) as i8,
        backend_lo: (rlocator.backend & 0xffff) as u16,
        rlocator: rlocator.locator,
    });
    sinval_seams::send_shared_invalid_messages::call(&[msg])
}

pub fn CacheInvalidateRelmap(databaseId: Oid) -> PgResult<()> {
    let msg = SharedInvalidationMessage::Relmap(SharedInvalRelmapMsg { dbId: databaseId });
    sinval_seams::send_shared_invalid_messages::call(&[msg])
}

pub fn CacheRegisterSyscacheCallback(
    cacheid: i32,
    func: SyscacheCallbackFunction,
    arg: Datum,
) -> PgResult<()> {
    if cacheid < 0 || cacheid >= SYS_CACHE_SIZE as i32 {
        return Err(err_invalid_cache_id(FATAL, cacheid));
    }
    CALLBACKS.with(|cell| {
        let t = &mut *cell.borrow_mut();
        if t.syscache_count >= MAX_SYSCACHE_CALLBACKS {
            return Err(err_out_of_slots("syscache_callback_list"));
        }

        let count = t.syscache_count as i16;
        let head = crate::SYSCACHE_LINKS.with(|links| links[cacheid as usize].get());
        if head == 0 {
            crate::SYSCACHE_LINKS.with(|links| links[cacheid as usize].set(count + 1));
        } else {
            let mut i = (head - 1) as usize;
            while t.syscache_list[i].expect("linked slot populated").link > 0 {
                i = (t.syscache_list[i].expect("linked slot populated").link - 1) as usize;
            }
            t.syscache_list[i].as_mut().expect("linked slot populated").link = count + 1;
        }

        t.syscache_list[t.syscache_count] = Some(SyscacheCallbackItem {
            id: cacheid as i16,
            link: 0,
            function: func,
            arg,
        });
        t.syscache_count += 1;
        Ok(())
    })
}

pub fn CacheRegisterRelcacheCallback(func: RelcacheCallbackFunction, arg: Datum) -> PgResult<()> {
    CALLBACKS.with(|cell| {
        let t = &mut *cell.borrow_mut();
        if t.relcache_count >= MAX_RELCACHE_CALLBACKS {
            return Err(err_out_of_slots("relcache_callback_list"));
        }
        t.relcache_list[t.relcache_count] = Some(RelcacheCallbackItem { function: func, arg });
        t.relcache_count += 1;
        Ok(())
    })
}

pub fn CacheRegisterRelSyncCallback(func: RelSyncCallbackFunction, arg: Datum) -> PgResult<()> {
    CALLBACKS.with(|cell| {
        let t = &mut *cell.borrow_mut();
        if t.relsync_count >= MAX_RELSYNC_CALLBACKS {
            return Err(err_out_of_slots("relsync_callback_list"));
        }
        t.relsync_list[t.relsync_count] = Some(RelsyncCallbackItem { function: func, arg });
        t.relsync_count += 1;
        Ok(())
    })
}

#[inline]
pub fn CallSyscacheCallbacks(cacheid: i32, hashvalue: u32) -> PgResult<()> {
    if cacheid < 0 || cacheid >= SYS_CACHE_SIZE as i32 {
        return Err(err_invalid_cache_id(ERROR, cacheid));
    }

    let head = crate::SYSCACHE_LINKS.with(|links| links[cacheid as usize].get());
    if head > 0 {
        call_syscache_chain(head, cacheid, hashvalue);
    }
    Ok(())
}

#[inline(never)]
fn call_syscache_chain(head: i16, cacheid: i32, hashvalue: u32) {
    let mut i = head as i32 - 1;
    while i >= 0 {
        let ccitem = CALLBACKS
            .with(|c| c.borrow().syscache_list[i as usize])
            .expect("linked slot populated");
        debug_assert_eq!(ccitem.id, cacheid as i16);
        (ccitem.function)(ccitem.arg, cacheid, hashvalue);
        // C re-reads ccitem->link after the callback (it may have registered).
        i = CALLBACKS.with(|c| {
            c.borrow().syscache_list[i as usize]
                .expect("linked slot populated")
                .link
        }) as i32
            - 1;
    }
}

pub fn CallRelSyncCallbacks(relid: Oid) -> PgResult<()> {
    let mut i = 0usize;
    while let Some(ccitem) = CALLBACKS.with(|c| {
        let t = c.borrow();
        if i < t.relsync_count { t.relsync_list[i] } else { None }
    }) {
        (ccitem.function)(ccitem.arg, relid);
        i += 1;
    }
    Ok(())
}
