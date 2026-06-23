//! `RemoveRelations` (tablecmds.c:1538) + `RangeVarCallbackForDropRelation`
//! (1702) — DROP TABLE/INDEX/SEQUENCE/VIEW/MATVIEW/FOREIGN TABLE.

#![allow(non_snake_case)]

use std::cell::RefCell;

use ::utils_error::ereport;
use ::mcx::Mcx;

use ::types_acl::ACLCHECK_NOT_OWNER;
use ::types_core::primitive::{InvalidOid, Oid, OidIsValid};
use types_error::{PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERROR};
use ::nodes::ddlnodes::DropStmt;
use ::nodes::nodes::Node;
use ::nodes::parsenodes::{
    DROP_CASCADE, OBJECT_FOREIGN_TABLE, OBJECT_INDEX, OBJECT_MATVIEW, OBJECT_SEQUENCE, OBJECT_TABLE,
    OBJECT_VIEW,
};
use ::types_storage::lock::{
    AccessExclusiveLock, ShareUpdateExclusiveLock, LOCKMODE,
};
use ::types_tuple::access::{
    RangeVar as AccessRangeVar, RELKIND_FOREIGN_TABLE, RELKIND_INDEX, RELKIND_MATVIEW,
    RELKIND_PARTITIONED_INDEX, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION, RELKIND_SEQUENCE,
    RELKIND_VIEW, RELPERSISTENCE_TEMP,
};

use aclchk_seams as aclchk_seam;
use dependency_seams as dep_seam;
use catalog_namespace::{makeRangeVarFromNameList, RangeVarGetRelidExtended, RVR_MISSING_OK};
use objectaddress_seams as objaddr_seam;
use partition_seams as partition_seam;
use index_seams as index_seam;
use pg_inherits_seams as inherits_seam;
use lmgr::{LockRelationOid, UnlockRelationOid};
use inval_seams as inval_seam;
use miscinit_seams as miscinit_seam;

use tablecmds_seams as seam;

use crate::helpers::{
    here, namelist_of_nodes, object_address_set, DropErrorMsgNonExistent, DropErrorMsgWrongType,
    NamespaceRelationId, RelationRelationId,
};

/// `struct DropRelationCallbackState` (tablecmds.c:314).
struct DropRelationCallbackState {
    /* set by RemoveRelations */
    expected_relkind: u8,
    heap_lockmode: LOCKMODE,
    /* state to track which subsidiary locks are held */
    heap_oid: Oid,
    part_parent_oid: Oid,
    /* passed back by the callback */
    actual_relkind: u8,
    actual_relpersistence: u8,
}

/// `RemoveRelations(DropStmt *drop)` (tablecmds.c:1538).
pub fn remove_relations<'mcx>(mcx: Mcx<'mcx>, drop: &DropStmt<'mcx>) -> PgResult<()> {
    let mut flags: i32 = 0;
    let mut lockmode: LOCKMODE = AccessExclusiveLock;

    /* DROP CONCURRENTLY uses a weaker lock, and has some restrictions */
    if drop.concurrent {
        lockmode = ShareUpdateExclusiveLock;
        debug_assert_eq!(drop.removeType, OBJECT_INDEX);
        if drop.objects.len() != 1 {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("DROP INDEX CONCURRENTLY does not support dropping multiple objects")
                .finish(here("RemoveRelations"));
        }
        if drop.behavior == DROP_CASCADE {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("DROP INDEX CONCURRENTLY does not support CASCADE")
                .finish(here("RemoveRelations"));
        }
    }

    /* Determine required relkind */
    let relkind: u8 = match drop.removeType {
        OBJECT_TABLE => RELKIND_RELATION,
        OBJECT_INDEX => RELKIND_INDEX,
        OBJECT_SEQUENCE => RELKIND_SEQUENCE,
        OBJECT_VIEW => RELKIND_VIEW,
        OBJECT_MATVIEW => RELKIND_MATVIEW,
        OBJECT_FOREIGN_TABLE => RELKIND_FOREIGN_TABLE,
        other => {
            return ereport(ERROR)
                .errmsg_internal(format!("unrecognized drop object type: {}", other as i32))
                .finish(here("RemoveRelations"));
        }
    };

    /* Lock and validate each relation; build a list of object addresses */
    let mut objects = dep_seam::new_object_addresses::call()?;

    for cell in drop.objects.iter() {
        /* makeRangeVarFromNameList((List *) lfirst(cell)) */
        let names = match cell.as_list() {
            Some(list) => {
                let nodes: Vec<Node> = list
                    .iter()
                    .map(|n| (**n).clone_in(mcx))
                    .collect::<PgResult<Vec<_>>>()?;
                namelist_of_nodes(&nodes)
            }
            None => unreachable!("DropStmt object is a Node::List namelist"),
        };
        let rel = makeRangeVarFromNameList(&names)?;

        /*
         * Check for shared-cache-inval messages before trying to access the
         * relation.
         */
        inval_seam::accept_invalidation_messages::call()?;

        /* Look up the appropriate relation using namespace search. */
        let state = RefCell::new(DropRelationCallbackState {
            expected_relkind: relkind,
            heap_lockmode: if drop.concurrent {
                ShareUpdateExclusiveLock
            } else {
                AccessExclusiveLock
            },
            heap_oid: InvalidOid,
            part_parent_oid: InvalidOid,
            actual_relkind: 0,
            actual_relpersistence: 0,
        });

        let mut callback = |callback_rel: &AccessRangeVar, rel_oid: Oid, old_rel_oid: Oid| {
            RangeVarCallbackForDropRelation(callback_rel, rel_oid, old_rel_oid, &state)
        };
        let rel_oid = RangeVarGetRelidExtended(
            mcx,
            &rel,
            lockmode,
            RVR_MISSING_OK,
            Some(&mut callback),
        )?;

        /* Not there? */
        if !OidIsValid(rel_oid) {
            DropErrorMsgNonExistent(&rel, relkind, drop.missing_ok)?;
            continue;
        }

        let st = state.borrow();

        /*
         * Decide if concurrent mode needs to be used here or not.
         */
        if drop.concurrent && st.actual_relpersistence != RELPERSISTENCE_TEMP {
            debug_assert!(drop.objects.len() == 1 && drop.removeType == OBJECT_INDEX);
            flags |= dep_seam::PERFORM_DELETION_CONCURRENTLY;
        }

        /*
         * Concurrent index drop cannot be used with partitioned indexes.
         */
        if (flags & dep_seam::PERFORM_DELETION_CONCURRENTLY) != 0
            && st.actual_relkind == RELKIND_PARTITIONED_INDEX
        {
            return ereport(ERROR)
                .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg(format!(
                    "cannot drop partitioned index \"{}\" concurrently",
                    rel.relname
                ))
                .finish(here("RemoveRelations"));
        }

        /*
         * If we're told to drop a partitioned index, we must acquire lock on
         * all the children of its parent partitioned table before proceeding.
         */
        if st.actual_relkind == RELKIND_PARTITIONED_INDEX {
            let _ = inherits_seam::find_all_inheritors::call(mcx, st.heap_oid, st.heap_lockmode)?;
        }

        /* OK, we're ready to delete this one */
        let obj = object_address_set(RelationRelationId, rel_oid);
        core::mem::drop(st);

        dep_seam::add_exact_object_address::call(obj, &mut objects)?;
    }

    dep_seam::perform_multiple_deletions::call(&objects.refs, drop.behavior, flags)?;

    dep_seam::free_object_addresses::call(objects)?;
    Ok(())
}

/// `RangeVarCallbackForDropRelation(rel, relOid, oldRelOid, arg)`
/// (tablecmds.c:1702).
fn RangeVarCallbackForDropRelation(
    rel: &AccessRangeVar,
    rel_oid: Oid,
    old_rel_oid: Oid,
    state: &RefCell<DropRelationCallbackState>,
) -> PgResult<()> {
    let heap_lockmode = state.borrow().heap_lockmode;

    /*
     * If we previously locked some other index's heap, and the name we're
     * looking up no longer refers to that relation, release the now-useless
     * lock.
     */
    {
        let mut st = state.borrow_mut();
        if rel_oid != old_rel_oid && OidIsValid(st.heap_oid) {
            UnlockRelationOid(st.heap_oid, heap_lockmode)?;
            st.heap_oid = InvalidOid;
        }

        /*
         * Similarly, if we previously locked some other partition's heap, and
         * the name we're looking up no longer refers to that relation, release.
         */
        if rel_oid != old_rel_oid && OidIsValid(st.part_parent_oid) {
            UnlockRelationOid(st.part_parent_oid, AccessExclusiveLock)?;
            st.part_parent_oid = InvalidOid;
        }
    }

    /* Didn't find a relation, so no need for locking or permission checks. */
    if !OidIsValid(rel_oid) {
        return Ok(());
    }

    let info = match seam::get_pg_class_drop_info::call(rel_oid)? {
        Some(info) => info,
        None => return Ok(()), /* concurrently dropped, so nothing to do */
    };
    let is_partition = info.relispartition;

    /* Pass back some data to save lookups in RemoveRelations */
    {
        let mut st = state.borrow_mut();
        st.actual_relkind = info.relkind;
        st.actual_relpersistence = info.relpersistence;
    }

    /*
     * Both RELKIND_RELATION and RELKIND_PARTITIONED_TABLE are OBJECT_TABLE.
     */
    let expected_relkind = if info.relkind == RELKIND_PARTITIONED_TABLE {
        RELKIND_RELATION
    } else if info.relkind == RELKIND_PARTITIONED_INDEX {
        RELKIND_INDEX
    } else {
        info.relkind
    };

    let st_expected_relkind = state.borrow().expected_relkind;
    if st_expected_relkind != expected_relkind {
        DropErrorMsgWrongType(&rel.relname, info.relkind, st_expected_relkind)?;
    }

    /* Allow DROP to either table owner or schema owner */
    if !aclchk_seam::object_ownercheck::call(
        RelationRelationId,
        rel_oid,
        miscinit_seam::get_user_id::call(),
    )? && !aclchk_seam::object_ownercheck::call(
        NamespaceRelationId,
        info.relnamespace,
        miscinit_seam::get_user_id::call(),
    )? {
        aclchk_seam::aclcheck_error::call(
            ACLCHECK_NOT_OWNER,
            objaddr_seam::get_relkind_objtype::call(info.relkind),
            Some(rel.relname.clone()),
        )?;
    }

    /*
     * Check the case of a system index that might have been invalidated by a
     * failed concurrent process and allow its drop.
     */
    let mut invalid_system_index = false;
    if seam::is_system_class_relid::call(rel_oid, info.relkind, info.relnamespace)?
        && info.relkind == RELKIND_INDEX
    {
        if let Some(indisvalid) = seam::get_index_isvalid::call(rel_oid)? {
            /* Mark object as being an invalid index of system catalogs */
            if !indisvalid {
                invalid_system_index = true;
            }
        } else {
            return Ok(());
        }
    }

    /* In the case of an invalid index, it is fine to bypass this check */
    if !invalid_system_index
        && !tablespace_globals_seams::allowSystemTableMods::call()?
        && seam::is_system_class_relid::call(rel_oid, info.relkind, info.relnamespace)?
    {
        return ereport(ERROR)
            .errcode(::types_error::ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!(
                "permission denied: \"{}\" is a system catalog",
                rel.relname
            ))
            .finish(here("RangeVarCallbackForDropRelation"));
    }

    /*
     * In DROP INDEX, attempt to acquire lock on the parent table before
     * locking the index.
     */
    if expected_relkind == RELKIND_INDEX && rel_oid != old_rel_oid {
        let heap_oid = index_seam::index_get_relation::call(rel_oid, true)?;
        state.borrow_mut().heap_oid = heap_oid;
        if OidIsValid(heap_oid) {
            LockRelationOid(heap_oid, heap_lockmode)?;
        }
    }

    /*
     * Similarly, if the relation is a partition, we must acquire lock on its
     * parent before locking the partition.
     */
    if is_partition && rel_oid != old_rel_oid {
        let part_parent_oid = partition_seam::get_partition_parent::call(rel_oid, true)?;
        state.borrow_mut().part_parent_oid = part_parent_oid;
        if OidIsValid(part_parent_oid) {
            LockRelationOid(part_parent_oid, AccessExclusiveLock)?;
        }
    }

    Ok(())
}
