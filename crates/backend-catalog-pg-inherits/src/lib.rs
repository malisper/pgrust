//! `src/backend/catalog/pg_inherits.c` (PostgreSQL 18.3) — the pg_inherits
//! catalog and inheritance queries: `find_inheritance_children` /
//! `find_inheritance_children_extended`, `find_all_inheritors`,
//! `has_subclass`, `has_superclass`, `typeInheritsFrom`,
//! `StoreSingleInheritance`, `DeleteInheritsTuple`, `PartitionHasPendingDetach`.
//!
//! Signature mapping:
//! * C `List *` of OIDs is `PgVec<'mcx, Oid>` allocated in the caller's `mcx`
//!   (the C `NIL` is an empty vec). `find_all_inheritors`' `numparents`
//!   out-`List **` is the optional second tuple element (computed iff the
//!   caller wants it).
//! * `find_inheritance_children_extended`'s `*detached_exist` / `*detached_xmin`
//!   out-params are `Option<&mut bool>` / `Option<&mut TransactionId>`.
//! * `DeleteInheritsTuple`'s `const char *childname` is `Option<&str>` (the C
//!   NULL renders as `"unknown relation"`).
//! * `table_open`..`table_close` spans are `Relation` guard scopes: the
//!   explicit `close(lockmode)` is the C `table_close`, and any `?` inside the
//!   span releases through `Drop`.
//! * Per-child locks acquired in `find_inheritance_children_extended` are
//!   [`LockGuard`]s: `keep()` for the C "hold until transaction end" default,
//!   `release()` for the "release useless lock" path after a vanished
//!   relation (AGENTS.md locks discipline).

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

extern crate alloc;

use alloc::format;

use mcx::{vec_with_capacity_in, Mcx, MemoryContext, PgHashMap, PgVec};
use types_catalog::pg_inherits::{
    Anum_pg_inherits_inhparent, Anum_pg_inherits_inhrelid, FormData_pg_inherits, Natts_pg_inherits,
    PgInheritsInsertRow, InheritsParentIndexId, InheritsRelationId, InheritsRelidSeqnoIndexId,
};
use types_core::fmgr::F_OIDEQ;
use types_core::primitive::{AttrNumber, InvalidOid, Oid, OidIsValid};
use types_core::xact::InvalidTransactionId;
use types_core::TransactionId;
use types_error::{ErrorLocation, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR, WARNING};
use types_rel::{Relation, RelationData};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};
use types_storage::lock::{AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE};
use types_tuple::backend_access_common_heaptuple::Datum;
use types_tuple::heaptuple::{HeapTupleData, HeapTupleHeaderChoice, ItemPointerData};

use backend_access_common_heaptuple::heap_deform_tuple;
use backend_access_common_scankey::ScanKeyInit;
use backend_utils_error::ereport;
use backend_access_index_genam_seams as genam_seams;
use backend_access_table_table as table;
use backend_catalog_indexing_seams as indexing_seams;
use backend_parser_parse_type as parse_type;
use backend_storage_lmgr_lmgr_seams as lmgr_seams;
use backend_utils_cache_syscache_seams as syscache_seams;
use backend_utils_time_snapmgr_seams as snapmgr_seams;

/// `ErrorLocation` for `ereport(...).finish(...)` / `into_error()`.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("pg_inherits.c", 0, funcname)
}

/// `TransactionIdFollows(id1, id2)` (access/transam.h) — id1 logically follows
/// id2 in the circular xid space: `(int32)(id1 - id2) > 0`.
#[inline]
fn TransactionIdFollows(id1: TransactionId, id2: TransactionId) -> bool {
    (id1.wrapping_sub(id2) as i32) > 0
}

/// `HeapTupleHeaderGetXmin(tuple->t_data)` — the scanned row's xmin, read off
/// the owned tuple header (`access/htup_details.h`).
fn heap_tuple_get_xmin(tuple: &HeapTupleData<'_>) -> TransactionId {
    match &tuple.t_data {
        Some(td) => match &td.t_choice {
            HeapTupleHeaderChoice::THeap(f) => f.t_xmin,
            HeapTupleHeaderChoice::TDatum(_) => InvalidTransactionId,
        },
        None => InvalidTransactionId,
    }
}

/// `(Form_pg_inherits) GETSTRUCT(tup)` — interpret one deformed pg_inherits
/// row. Every pg_inherits column is fixed-width and NOT NULL.
fn form_pg_inherits(values: &[Datum<'_>]) -> FormData_pg_inherits {
    debug_assert_eq!(values.len(), Natts_pg_inherits);
    let col = |attno: AttrNumber| &values[attno as usize - 1];
    FormData_pg_inherits {
        inhrelid: col(Anum_pg_inherits_inhrelid).as_oid(),
        inhparent: col(Anum_pg_inherits_inhparent).as_oid(),
        inhseqno: col(3).as_i32(),
        inhdetachpending: col(4).as_bool(),
    }
}

/// `table_open(InheritsRelationId, lockmode)` — the guard's `Drop` is the
/// error-path `table_close`; the success path closes explicitly.
fn open_inherits(mcx: Mcx<'_>, lockmode: LOCKMODE) -> PgResult<Relation<'_>> {
    table::table_open(mcx, InheritsRelationId, lockmode)
}

/// `ScanKeyInit(&key, attno, BTEqualStrategyNumber, F_OIDEQ,
/// ObjectIdGetDatum(value))`. The eager fmgr resolution crosses the fmgr seam.
fn oid_key<'mcx>(attno: AttrNumber, value: Oid) -> PgResult<ScanKeyData<'mcx>> {
    let mut key = ScanKeyData::empty();
    ScanKeyInit(
        &mut key,
        attno,
        BTEqualStrategyNumber,
        F_OIDEQ,
        Datum::from_oid(value),
    )?;
    Ok(key)
}

/// One scanned pg_inherits row: the deformed `Form_pg_inherits` columns plus
/// the two header fields the C reads directly off the tuple — `xmin` (the
/// detached-partition snapshot check) and `t_self` (the ctid passed to
/// `CatalogTupleDelete`).
struct ScannedRow {
    form: FormData_pg_inherits,
    xmin: TransactionId,
    t_self: ItemPointerData,
}

/// `systable_beginscan(rel, indexId, true, NULL, 1, &key)` + the
/// `while ((tup = systable_getnext(scan)))` loop + `systable_endscan(scan)`:
/// invoke `body` once per matching row, in scan order. `body` returning
/// `Ok(true)` continues, `Ok(false)` stops early (the C `break`); an `Err`
/// propagates after the scan is ended (the scan guard's `Drop` covers the
/// error path). The deformed-row scratch context drops at the end of each
/// iteration.
fn systable_scan_foreach(
    rel: &RelationData<'_>,
    index_id: Oid,
    key: ScanKeyData,
    mut body: impl FnMut(&ScannedRow) -> PgResult<bool>,
) -> PgResult<()> {
    let keys = [key];
    let mut scan = genam_seams::systable_beginscan::call(rel, index_id, true, None, &keys)?;
    loop {
        let scratch = MemoryContext::new("pg_inherits systable_scan_foreach row");
        let smcx = scratch.mcx();
        let Some(tup) = genam_seams::systable_getnext::call(smcx, scan.desc_mut())? else {
            break;
        };
        let xmin = heap_tuple_get_xmin(&tup.tuple);
        let t_self = tup.tuple.t_self;
        // (Form_pg_inherits) GETSTRUCT(tup): deform the whole row (every
        // pg_inherits column is fixed-width and NOT NULL, so by-value).
        let cols = heap_deform_tuple(smcx, &tup.tuple, &rel.rd_att, &tup.data)?;
        let mut values: PgVec<'_, Datum<'_>> = vec_with_capacity_in(smcx, cols.len())?;
        for (value, _null) in cols.iter() {
            values.push(value.clone());
        }
        let row = ScannedRow {
            form: form_pg_inherits(&values),
            xmin,
            t_self,
        };
        let keep_going = body(&row)?;
        if !keep_going {
            break;
        }
        // `scratch` drops here (declared before the borrows of it).
    }
    scan.end()
}

/*
 * find_inheritance_children
 *
 * Returns a list containing the OIDs of all relations which
 * inherit *directly* from the relation with OID 'parentrelId'.
 *
 * The specified lock type is acquired on each child relation (but not on the
 * given rel; caller should already have locked it).  If lockmode is NoLock
 * then no locks are acquired, but caller must beware of race conditions
 * against possible DROPs of child relations.
 *
 * Partitions marked as being detached are omitted; see
 * find_inheritance_children_extended for details.
 */
pub fn find_inheritance_children<'mcx>(
    mcx: Mcx<'mcx>,
    parentrelId: Oid,
    lockmode: LOCKMODE,
) -> PgResult<PgVec<'mcx, Oid>> {
    find_inheritance_children_extended(mcx, parentrelId, true, lockmode, None, None)
}

/*
 * find_inheritance_children_extended
 *
 * As find_inheritance_children, with more options regarding detached
 * partitions.
 *
 * If a partition's pg_inherits row is marked "detach pending",
 * *detached_exist (if not null) is set true.
 *
 * If omit_detached is true and there is an active snapshot (not the same as
 * the catalog snapshot used to scan pg_inherits!) and a pg_inherits tuple
 * marked "detach pending" is visible to that snapshot, then that partition is
 * omitted from the output list.  In addition, *detached_xmin (if not null) is
 * set to the xmin of the row of the detached partition.
 */
pub fn find_inheritance_children_extended<'mcx>(
    mcx: Mcx<'mcx>,
    parentrelId: Oid,
    omit_detached: bool,
    lockmode: LOCKMODE,
    mut detached_exist: Option<&mut bool>,
    mut detached_xmin: Option<&mut TransactionId>,
) -> PgResult<PgVec<'mcx, Oid>> {
    /*
     * Can skip the scan if pg_class shows the relation has never had a
     * subclass.
     */
    if !has_subclass(parentrelId)? {
        return vec_with_capacity_in(mcx, 0);
    }

    /*
     * Scan pg_inherits and build a working array of subclass OIDs.  (The C
     * preallocates 32 Oids and repalloc-doubles; a Vec is the faithful
     * equivalent — the maxoids/repalloc bookkeeping has no observable effect.)
     */
    let mut oidarr: alloc::vec::Vec<Oid> = alloc::vec::Vec::new();

    let scan_ctx = MemoryContext::new("pg_inherits find_children scan");
    let relation = open_inherits(scan_ctx.mcx(), AccessShareLock)?;
    let key = oid_key(Anum_pg_inherits_inhparent, parentrelId)?;

    systable_scan_foreach(&relation, InheritsParentIndexId, key, |row| {
        /*
         * Cope with partitions concurrently being detached.  When we see a
         * partition marked "detach pending", we omit it from the returned set
         * of visible partitions if caller requested that and the tuple's xmin
         * does not appear in progress to the active snapshot.
         */
        if row.form.inhdetachpending {
            if let Some(de) = detached_exist.as_deref_mut() {
                *de = true;
            }

            if omit_detached && snapmgr_seams::active_snapshot_set::call() {
                // xmin = HeapTupleHeaderGetXmin(inheritsTuple->t_data);
                let xmin: TransactionId = row.xmin;
                // snap = GetActiveSnapshot();
                let snap = snapmgr_seams::get_active_snapshot::call()?;

                // if (!XidInMVCCSnapshot(xmin, snap))
                let in_progress = match &snap {
                    Some(s) => snapmgr_seams::xid_in_mvcc_snapshot::call(xmin, s)?,
                    None => false,
                };
                if !in_progress {
                    if let Some(dx) = detached_xmin.as_deref_mut() {
                        /*
                         * Two detached partitions should not occur (see checks
                         * in MarkInheritDetached), but if they do, track the
                         * newer of the two.  Warn the user so they can clean
                         * up; since this is just a cross-check against
                         * potentially corrupt catalogs, it is not a
                         * full-fledged error.
                         */
                        if *dx != InvalidTransactionId {
                            ereport(WARNING)
                                .errmsg(format!(
                                    "more than one partition pending detach found for table with OID {parentrelId}"
                                ))
                                .finish(here("find_inheritance_children_extended"))?;
                            if TransactionIdFollows(xmin, *dx) {
                                *dx = xmin;
                            }
                        } else {
                            *dx = xmin;
                        }
                    }

                    /* Don't add the partition to the output list */
                    return Ok(true);
                }
            }
        }

        // inhrelid = ((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhrelid;
        oidarr.push(row.form.inhrelid);
        Ok(true)
    })?;

    // table_close(relation, AccessShareLock);
    relation.close(AccessShareLock)?;

    /*
     * If we found more than one child, sort them by OID.  This ensures
     * reasonably consistent behavior regardless of the vagaries of an
     * indexscan.  This is important since we need to be sure all backends lock
     * children in the same order to avoid needless deadlocks.
     */
    if oidarr.len() > 1 {
        // qsort(oidarr, numoids, sizeof(Oid), oid_cmp);
        oidarr.sort_unstable();
    }

    /*
     * Acquire locks and build the result list.
     */
    let mut list: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, oidarr.len())?;
    for &inhrelid in oidarr.iter() {
        if lockmode != NoLock {
            /* Get the lock to synchronize against concurrent drop */
            let guard = lmgr_seams::lock_relation_oid::call(inhrelid, lockmode)?;

            /*
             * Now that we have the lock, double-check to see if the relation
             * really exists or not.  If not, assume it was dropped while we
             * waited to acquire lock, and ignore it.
             */
            if !syscache_seams::search_syscache_exists_reloid::call(inhrelid)? {
                /* Release useless lock */
                guard.release()?;
                /* And ignore this relation */
                continue;
            }

            /* Hold the lock until transaction end (the C default). */
            guard.keep();
        }

        // list = lappend_oid(list, inhrelid);
        list.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
        list.push(inhrelid);
    }

    // pfree(oidarr);  -- Vec drop
    Ok(list)
}

/*
 * find_all_inheritors -
 *		Returns a list of relation OIDs including the given rel plus all
 *		relations that inherit from it, directly or indirectly.  Optionally,
 *		it also returns the number of parents found for each such relation
 *		within the inheritance tree rooted at the given rel.
 *
 * The specified lock type is acquired on all child relations (but not on the
 * given rel; caller should already have locked it).  If lockmode is NoLock
 * then no locks are acquired, but caller must beware of race conditions
 * against possible DROPs of child relations.
 *
 * In C `numparents` is an out-`List **`; here it is returned as the optional
 * second element of the tuple (computed iff `want_numparents`).
 */
pub fn find_all_inheritors<'mcx>(
    mcx: Mcx<'mcx>,
    parentrelId: Oid,
    lockmode: LOCKMODE,
    want_numparents: bool,
) -> PgResult<(PgVec<'mcx, Oid>, Option<PgVec<'mcx, i32>>)> {
    /* hash table for O(1) rel_oid -> rel_numparents-cell (list index) lookup */
    let mut seen_rels: PgHashMap<'mcx, Oid, usize> = PgHashMap::new_in(mcx);

    /*
     * We build a list starting with the given rel and adding all direct and
     * indirect children.  We use a single list as both the record of
     * already-found rels and the agenda of rels yet to be scanned: the index
     * walk grows the list during iteration without fetching the next element
     * until the bottom of the loop.  We can't keep pointers into the lists, but
     * an index is sufficient.
     */
    // rels_list = list_make1_oid(parentrelId);
    // rel_numparents = list_make1_int(0);
    let mut rels_list: PgVec<'mcx, Oid> = vec_with_capacity_in(mcx, 1)?;
    rels_list.push(parentrelId);
    let mut rel_numparents: PgVec<'mcx, i32> = vec_with_capacity_in(mcx, 1)?;
    rel_numparents.push(0);

    // foreach(l, rels_list)
    let mut idx = 0usize;
    while idx < rels_list.len() {
        let currentrel = rels_list[idx];

        /* Get the direct children of this rel */
        let child_ctx = MemoryContext::new("pg_inherits find_all_inheritors children");
        let currentchildren = find_inheritance_children(child_ctx.mcx(), currentrel, lockmode)?;

        /*
         * Add to the queue only those children not already seen. This avoids
         * making duplicate entries in case of multiple inheritance paths from
         * the same parent.  (It also keeps us from getting into an infinite
         * loop, though there can't be any cycles in the inheritance graph
         * anyway.)
         */
        // foreach(lc, currentchildren)
        for &child_oid in currentchildren.iter() {
            // hash_entry = hash_search(seen_rels, &child_oid, HASH_ENTER, &found);
            match seen_rels.get(&child_oid).copied() {
                Some(list_index) => {
                    /* already there: bump number-of-parents counter */
                    rel_numparents[list_index] += 1;
                }
                None => {
                    /* not there: add it, expect 1 parent initially */
                    let list_index = rels_list.len();
                    seen_rels
                        .try_reserve(1)
                        .map_err(|_| mcx.oom(core::mem::size_of::<(Oid, usize)>()))?;
                    seen_rels.insert(child_oid, list_index);
                    rels_list.try_reserve(1).map_err(|_| mcx.oom(core::mem::size_of::<Oid>()))?;
                    rels_list.push(child_oid);
                    rel_numparents
                        .try_reserve(1)
                        .map_err(|_| mcx.oom(core::mem::size_of::<i32>()))?;
                    rel_numparents.push(1);
                }
            }
        }

        idx += 1;
    }

    // if (numparents) *numparents = rel_numparents; else list_free(rel_numparents);
    let numparents = if want_numparents {
        Some(rel_numparents)
    } else {
        None
    };

    // hash_destroy(seen_rels);  -- PgHashMap drop
    // return rels_list;
    Ok((rels_list, numparents))
}

/*
 * has_subclass - does this relation have any children?
 *
 * In the current implementation, has_subclass returns whether a particular
 * class *might* have a subclass.  It will not return the correct result if a
 * class had a subclass which was later dropped, because relhassubclass in
 * pg_class is not updated immediately when a subclass is dropped, primarily
 * because of concurrency concerns.  Currently has_subclass is only used as an
 * efficiency hack to skip unnecessary inheritance searches, so this is OK.
 *
 * Although this doesn't actually touch pg_inherits, it is kept here since it's
 * normally used with the other routines here.
 */
pub fn has_subclass(relationId: Oid) -> PgResult<bool> {
    // tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));
    // if (!HeapTupleIsValid(tuple))
    //     elog(ERROR, "cache lookup failed for relation %u", relationId);
    // result = ((Form_pg_class) GETSTRUCT(tuple))->relhassubclass;
    // ReleaseSysCache(tuple);
    let ctx = MemoryContext::new("pg_inherits has_subclass");
    let result = match syscache_seams::search_pg_class_full_form::call(ctx.mcx(), relationId)? {
        Some(form) => form.relhassubclass,
        None => {
            return Err(ereport(ERROR)
                .errmsg(format!("cache lookup failed for relation {relationId}"))
                .into_error())
        }
    };
    Ok(result)
}

/*
 * has_superclass - does this relation inherit from another?
 *
 * Unlike has_subclass, this can be relied on to give an accurate answer.
 * However, the caller must hold a lock on the given relation so that it can't
 * be concurrently added to or removed from an inheritance hierarchy.
 */
pub fn has_superclass(relationId: Oid) -> PgResult<bool> {
    let ctx = MemoryContext::new("pg_inherits has_superclass");
    let catalog = open_inherits(ctx.mcx(), AccessShareLock)?;
    let key = oid_key(Anum_pg_inherits_inhrelid, relationId)?;

    // result = HeapTupleIsValid(systable_getnext(scan));
    let mut result = false;
    systable_scan_foreach(&catalog, InheritsRelidSeqnoIndexId, key, |_row| {
        result = true;
        Ok(false) /* the C reads only the first tuple */
    })?;

    // systable_endscan(scan); table_close(catalog, AccessShareLock);
    catalog.close(AccessShareLock)?;

    Ok(result)
}

/// The pg_inherits dup-parent check + max-`inhseqno` scan at the head of
/// `CreateInheritance` (tablecmds.c:17390-17415): scan pg_inherits by
/// `child_relid`, reject a row whose `inhparent == parent_relid` (the child
/// would inherit from the parent more than once), and return the highest
/// `inhseqno` seen (0 if none). The caller adds 1 for the new row's seqno.
/// `parent_name` is used only for the duplicate-parent error message.
pub fn next_inheritance_seqno_checked<'mcx>(
    mcx: Mcx<'mcx>,
    child_relid: Oid,
    parent_relid: Oid,
    parent_name: &str,
) -> PgResult<i32> {
    // Note: RowExclusiveLock because the caller will write pg_inherits next.
    let catalog = open_inherits(mcx, RowExclusiveLock)?;
    let key = oid_key(Anum_pg_inherits_inhrelid, child_relid)?;

    // inhseqno sequences start at 1.
    let mut inhseqno = 0i32;
    let mut dup_parent = false;
    systable_scan_foreach(&catalog, InheritsRelidSeqnoIndexId, key, |row| {
        if row.form.inhparent == parent_relid {
            dup_parent = true;
            return Ok(false);
        }
        if row.form.inhseqno > inhseqno {
            inhseqno = row.form.inhseqno;
        }
        Ok(true)
    })?;

    catalog.close(RowExclusiveLock)?;

    if dup_parent {
        return Err(ereport(ERROR)
            .errcode(types_error::ERRCODE_DUPLICATE_TABLE)
            .errmsg(format!(
                "relation \"{parent_name}\" would be inherited from more than once"
            ))
            .into_error());
    }

    Ok(inhseqno)
}

/*
 * Given two type OIDs, determine whether the first is a complex type (class
 * type) that inherits from the second.
 *
 * This essentially asks whether the first type is guaranteed to be coercible to
 * the second.  Therefore, we allow the first type to be a domain over a complex
 * type that inherits from the second; that creates no difficulties.  But the
 * second type cannot be a domain.
 */
pub fn typeInheritsFrom(subclassTypeId: Oid, superclassTypeId: Oid) -> PgResult<bool> {
    let mut result = false;

    /* We need to work with the associated relation OIDs */
    // subclassRelid = typeOrDomainTypeRelid(subclassTypeId);
    let subclassRelid = parse_type::typeOrDomainTypeRelid(subclassTypeId)?;
    if subclassRelid == InvalidOid {
        return Ok(false); /* not a complex type or domain over one */
    }
    // superclassRelid = typeidTypeRelid(superclassTypeId);
    let superclassRelid = parse_type::typeidTypeRelid(superclassTypeId)?;
    if superclassRelid == InvalidOid {
        return Ok(false); /* not a complex type */
    }

    /* No point in searching if the superclass has no subclasses */
    if !has_subclass(superclassRelid)? {
        return Ok(false);
    }

    /*
     * Begin the search at the relation itself, so add its relid to the queue.
     */
    // queue = list_make1_oid(subclassRelid);  visited = NIL;
    let bfs_ctx = MemoryContext::new("pg_inherits typeInheritsFrom");
    let bmcx = bfs_ctx.mcx();
    let mut queue: PgVec<'_, Oid> = vec_with_capacity_in(bmcx, 1)?;
    queue.push(subclassRelid);
    let mut visited: PgVec<'_, Oid> = vec_with_capacity_in(bmcx, 0)?;

    // inhrel = table_open(InheritsRelationId, AccessShareLock);
    let inhrel = open_inherits(bmcx, AccessShareLock)?;

    /*
     * Use queue to do a breadth-first traversal of the inheritance graph from
     * the relid supplied up to the root.  We append to the queue inside the
     * loop --- okay because the foreach() macro doesn't advance queue_item
     * until the next iteration begins.
     */
    // foreach(queue_item, queue)
    let mut qidx = 0usize;
    while qidx < queue.len() {
        let this_relid = queue[qidx];

        /*
         * If we've seen this relid already, skip it.  This avoids extra work in
         * multiple-inheritance scenarios, and protects against an infinite loop
         * if there's a cycle in pg_inherits (which theoretically can't happen).
         */
        // if (list_member_oid(visited, this_relid)) continue;
        if visited.iter().any(|&v| v == this_relid) {
            qidx += 1;
            continue;
        }

        /*
         * Okay, a not-yet-seen relid.  Add it to the visited list, then find
         * all the types this relid inherits from and add them to the queue.
         */
        // visited = lappend_oid(visited, this_relid);
        visited.try_reserve(1).map_err(|_| bmcx.oom(core::mem::size_of::<Oid>()))?;
        visited.push(this_relid);

        let key = oid_key(Anum_pg_inherits_inhrelid, this_relid)?;
        // while ((inhtup = systable_getnext(inhscan)) != NULL)
        systable_scan_foreach(&inhrel, InheritsRelidSeqnoIndexId, key, |row| {
            // Oid inhparent = ((Form_pg_inherits) GETSTRUCT(inhtup))->inhparent;
            let inhparent = row.form.inhparent;

            /* If this is the target superclass, we're done */
            if inhparent == superclassRelid {
                result = true;
                return Ok(false);
            }

            /* Else add to queue */
            // queue = lappend_oid(queue, inhparent);
            queue.try_reserve(1).map_err(|_| bmcx.oom(core::mem::size_of::<Oid>()))?;
            queue.push(inhparent);
            Ok(true)
        })?;

        if result {
            break;
        }

        qidx += 1;
    }

    /* clean up ... */
    // table_close(inhrel, AccessShareLock); list_free(visited); list_free(queue)
    inhrel.close(AccessShareLock)?;

    Ok(result)
}

/*
 * Create a single pg_inherits row with the given data
 */
pub fn StoreSingleInheritance(relationId: Oid, parentOid: Oid, seqNumber: i32) -> PgResult<()> {
    // inhRelation = table_open(InheritsRelationId, RowExclusiveLock);
    let ctx = MemoryContext::new("pg_inherits StoreSingleInheritance");
    let inhRelation = open_inherits(ctx.mcx(), RowExclusiveLock)?;

    /*
     * Make the pg_inherits entry.
     *
     * values[Anum_pg_inherits_inhrelid - 1]         = ObjectIdGetDatum(relationId);
     * values[Anum_pg_inherits_inhparent - 1]        = ObjectIdGetDatum(parentOid);
     * values[Anum_pg_inherits_inhseqno - 1]         = Int32GetDatum(seqNumber);
     * values[Anum_pg_inherits_inhdetachpending - 1] = BoolGetDatum(false);
     * memset(nulls, 0, sizeof(nulls));
     *
     * tuple = heap_form_tuple(RelationGetDescr(inhRelation), values, nulls);
     * CatalogTupleInsert(inhRelation, tuple);
     * heap_freetuple(tuple);
     */
    let row = PgInheritsInsertRow {
        inhrelid: relationId,
        inhparent: parentOid,
        inhseqno: seqNumber,
        inhdetachpending: false,
    };
    indexing_seams::catalog_tuple_insert_pg_inherits::call(ctx.mcx(), &inhRelation, &row)?;

    // table_close(inhRelation, RowExclusiveLock);
    inhRelation.close(RowExclusiveLock)?;

    Ok(())
}

/*
 * DeleteInheritsTuple
 *
 * Delete pg_inherits tuples with the given inhrelid.  inhparent may be given as
 * InvalidOid, in which case all tuples matching inhrelid are deleted; otherwise
 * only delete tuples with the specified inhparent.
 *
 * expect_detach_pending is the expected state of the inhdetachpending flag.  If
 * the catalog row does not match that state, an error is raised.
 *
 * childname is the partition name, if a table; pass None for regular
 * inheritance or when working with other relation kinds.
 *
 * Returns whether at least one row was deleted.
 */
pub fn DeleteInheritsTuple(
    inhrelid: Oid,
    inhparent: Oid,
    expect_detach_pending: bool,
    childname: Option<&str>,
) -> PgResult<bool> {
    let mut found = false;

    /*
     * Find pg_inherits entries by inhrelid.
     */
    // catalogRelation = table_open(InheritsRelationId, RowExclusiveLock);
    let ctx = MemoryContext::new("pg_inherits DeleteInheritsTuple");
    let catalogRelation = open_inherits(ctx.mcx(), RowExclusiveLock)?;
    let key = oid_key(Anum_pg_inherits_inhrelid, inhrelid)?;

    // while (HeapTupleIsValid(inheritsTuple = systable_getnext(scan)))
    systable_scan_foreach(&catalogRelation, InheritsRelidSeqnoIndexId, key, |row| {
        /* Compare inhparent if it was given, and do the actual deletion. */
        // parent = ((Form_pg_inherits) GETSTRUCT(inheritsTuple))->inhparent;
        let parent = row.form.inhparent;
        if !OidIsValid(inhparent) || parent == inhparent {
            // detach_pending = GETSTRUCT(inheritsTuple)->inhdetachpending;
            let detach_pending = row.form.inhdetachpending;

            /*
             * Raise error depending on state.  This should only happen for
             * partitions, but we have no way to cross-check.
             */
            if detach_pending && !expect_detach_pending {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "cannot detach partition \"{}\"",
                        childname.unwrap_or("unknown relation")
                    ))
                    .errdetail(
                        "The partition is being detached concurrently or has an unfinished detach.",
                    )
                    .errhint(
                        "Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete the pending detach operation.",
                    )
                    .into_error());
            }
            if !detach_pending && expect_detach_pending {
                return Err(ereport(ERROR)
                    .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                    .errmsg(format!(
                        "cannot complete detaching partition \"{}\"",
                        childname.unwrap_or("unknown relation")
                    ))
                    .errdetail("There's no pending concurrent detach.")
                    .into_error());
            }

            // CatalogTupleDelete(catalogRelation, &inheritsTuple->t_self);
            indexing_seams::catalog_tuple_delete::call(&catalogRelation, row.t_self)?;
            found = true;
        }
        Ok(true)
    })?;

    /* Done */
    // systable_endscan(scan); table_close(catalogRelation, RowExclusiveLock);
    catalogRelation.close(RowExclusiveLock)?;

    Ok(found)
}

/*
 * Return whether the pg_inherits tuple for a partition has the "detach pending"
 * flag set.
 */
pub fn PartitionHasPendingDetach(partoid: Oid) -> PgResult<bool> {
    /* We don't have a good way to verify it is in fact a partition */

    /*
     * Find the pg_inherits entry by inhrelid.  (There should only be one.)
     */
    // catalogRelation = table_open(InheritsRelationId, RowExclusiveLock);
    let ctx = MemoryContext::new("pg_inherits PartitionHasPendingDetach");
    let catalogRelation = open_inherits(ctx.mcx(), RowExclusiveLock)?;
    let key = oid_key(Anum_pg_inherits_inhrelid, partoid)?;

    // while (HeapTupleIsValid(inheritsTuple = systable_getnext(scan)))
    //
    // The C returns on the first matching tuple; faithfully a loop that only
    // ever runs one iteration.
    let mut detached: Option<bool> = None;
    systable_scan_foreach(&catalogRelation, InheritsRelidSeqnoIndexId, key, |row| {
        // detached = GETSTRUCT(inheritsTuple)->inhdetachpending;
        detached = Some(row.form.inhdetachpending);
        Ok(false)
    })?;

    if let Some(d) = detached {
        // systable_endscan(scan); table_close(catalogRelation, RowExclusiveLock);
        catalogRelation.close(RowExclusiveLock)?;
        return Ok(d);
    }

    // elog(ERROR, "relation %u is not a partition", partoid);
    //
    // Faithful to the C: in the no-row case the error is raised *without* an
    // explicit table_close — the C `elog(ERROR)` longjmps and the open relation
    // is released by transaction-abort resource cleanup. Here the error
    // propagates as `Err`; dropping `catalogRelation` on the way out is the
    // RAII analog of that abort-time release.
    drop(catalogRelation);
    Err(ereport(ERROR)
        .errmsg(format!("relation {partoid} is not a partition"))
        .into_error())
}

/// Install the pg_inherits inward seams (the two cross-cycle entry points
/// consumed by `backend-commands-cluster` and `backend-parser-coerce`).
pub fn init_seams() {
    backend_catalog_pg_inherits_seams::find_all_inheritors::set(|mcx, parent_rel_id, lockmode| {
        let (rels, _numparents) = find_all_inheritors(mcx, parent_rel_id, lockmode, false)?;
        Ok(rels)
    });
    backend_catalog_pg_inherits_seams::type_inherits_from::set(typeInheritsFrom);
    // index_create (catalog/index.c) partition-index parent link.
    backend_catalog_pg_inherits_seams::store_single_inheritance::set(StoreSingleInheritance);
    // index_drop (catalog/index.c) partition-index parent-link cleanup.
    backend_catalog_pg_inherits_seams::delete_inherits_tuple::set(DeleteInheritsTuple);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `TransactionIdFollows` is `(int32)(id1 - id2) > 0` over the circular xid
    /// space, matching access/transam.h.
    #[test]
    fn transaction_id_follows_circular() {
        assert!(TransactionIdFollows(100, 50));
        assert!(!TransactionIdFollows(50, 100));
        assert!(!TransactionIdFollows(50, 50));
        // Wraparound: a small id "follows" a near-max id.
        assert!(TransactionIdFollows(5, u32::MAX - 5));
    }

    /// The hardwired catalog/index/attribute OIDs match catalog/pg_inherits.h.
    #[test]
    fn catalog_constants_match_headers() {
        assert_eq!(InheritsRelationId, 2611);
        assert_eq!(InheritsRelidSeqnoIndexId, 2680);
        assert_eq!(InheritsParentIndexId, 2187);
        assert_eq!(Anum_pg_inherits_inhrelid, 1);
        assert_eq!(Anum_pg_inherits_inhparent, 2);
        assert_eq!(Natts_pg_inherits, 4);
        assert_eq!(BTEqualStrategyNumber, 3);
        assert_eq!(F_OIDEQ, 184);
        assert_eq!(AccessShareLock, 1);
        assert_eq!(RowExclusiveLock, 3);
        assert_eq!(NoLock, 0);
    }
}
