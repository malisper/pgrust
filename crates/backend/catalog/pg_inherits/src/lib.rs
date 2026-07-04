// pg_inherits.c: StoreSingleInheritance + find_inheritance_children[_extended]
// + find_all_inheritors + has_superclass + DeleteInheritsTuple, plus
// get_partition_parent + get_partition_ancestors (C: catalog/partition.c;
// hosted here for the pg_inherits scan machinery).
#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::fmgr::{F_INT4EQ, F_OIDEQ};
use types_core::{AttrNumber, InvalidOid, Oid, RegProcedure};
use types_error::{PgError, PgResult, ERROR};
use types_rel::{AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

pub fn init_seams() {
    pg_inherits_seams::type_inherits_from::set(typeInheritsFrom);
}

pub const InheritsRelationId: Oid = 2611;
pub const InheritsRelidSeqnoIndexId: Oid = 2680;
pub const InheritsParentIndexId: Oid = 2187;

pub const Anum_pg_inherits_inhrelid: AttrNumber = 1;
pub const Anum_pg_inherits_inhparent: AttrNumber = 2;
pub const Anum_pg_inherits_inhseqno: AttrNumber = 3;
pub const Anum_pg_inherits_inhdetachpending: AttrNumber = 4;
pub const Natts_pg_inherits: usize = 4;

fn eq_key(attno: AttrNumber, func: RegProcedure, arg: Datum) -> ScanKeyData {
    let mut key = ScanKeyData::empty();
    key.sk_attno = attno;
    key.sk_strategy = BTEqualStrategyNumber;
    key.sk_collation = types_core::C_COLLATION_OID;
    key.sk_func = fmgr_seams::fmgr_info::call(func)
        .unwrap_or_else(|e| panic!("fmgr_info({func}) failed: {e:?}"));
    key.sk_argument = arg;
    key
}

pub fn StoreSingleInheritance<'mcx>(
    mcx: Mcx<'mcx>,
    relation_id: Oid,
    parent_oid: Oid,
    seq_number: i32,
) -> PgResult<()> {
    let rel = table::table_open(mcx, InheritsRelationId, RowExclusiveLock)?;
    let values = [
        Datum::from_oid(relation_id),
        Datum::from_oid(parent_oid),
        Datum::from_i32(seq_number),
        Datum::from_bool(false),
    ];
    let nulls = [false; Natts_pg_inherits];
    let mut tup = heaptuple::heap_form_tuple(mcx, rel.descr(), &values, &nulls)?;
    catalog_indexing::CatalogTupleInsert(mcx, &rel, &mut tup)?;
    rel.close(RowExclusiveLock)
}

// Children sorted by OID, then locked in that order (C's deadlock-avoidance
// contract), rechecking existence after each lock: a child seen by the scan
// can be dropped while we wait for its lock (pg_inherits.c:206).
pub fn find_inheritance_children<'mcx>(
    mcx: Mcx<'mcx>,
    parent_rel_id: Oid,
    lockmode: LOCKMODE,
) -> PgResult<PgVec<'mcx, Oid>> {
    find_inheritance_children_extended(mcx, parent_rel_id, true, lockmode, None, None)
}

// Detach-pending rows are omitted only when the pending flag's inserter is
// visible-as-committed to the active snapshot (pg_inherits.c:82-186): RI
// queries under RR/SERIALIZABLE snapshots must keep seeing the partition.
pub fn find_inheritance_children_extended<'mcx>(
    mcx: Mcx<'mcx>,
    parent_rel_id: Oid,
    omit_detached: bool,
    lockmode: LOCKMODE,
    mut detached_exist: Option<&mut bool>,
    mut detached_xmin: Option<&mut types_core::TransactionId>,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut result: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    if !has_subclass(parent_rel_id)? {
        return Ok(result);
    }
    let rel = table::table_open(mcx, InheritsRelationId, AccessShareLock)?;
    let keys = [eq_key(Anum_pg_inherits_inhparent, F_OIDEQ, Datum::from_oid(parent_rel_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, InheritsParentIndexId, true, None, &keys)?;
    let desc = rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_inherits columns under its descriptor.
        let pending = unsafe {
            types_tuple::heap_getattr(
                tup,
                Anum_pg_inherits_inhdetachpending as i32,
                desc,
                &mut isnull,
            )
        }
        .as_bool();
        if pending {
            if let Some(exist) = detached_exist.as_deref_mut() {
                *exist = true;
            }
            if omit_detached && snapmgr::ActiveSnapshotSet() {
                let xmin = tup.t_data().xmin();
                let snap = snapmgr::GetActiveSnapshot();
                if !snapmgr::XidInMVCCSnapshot(xmin, &snap)? {
                    if let Some(out) = detached_xmin.as_deref_mut() {
                        if *out != types_core::InvalidTransactionId {
                            elog_seams::ereport_msg::call(
                                types_error::WARNING,
                                format!(
                                    "more than one partition pending detach found for \
                                     table with OID {parent_rel_id}"
                                ),
                                None,
                            )?;
                            if types_core::TransactionIdFollows(xmin, *out) {
                                *out = xmin;
                            }
                        } else {
                            *out = xmin;
                        }
                    }
                    continue;
                }
            }
        }
        // SAFETY: as above.
        let inhrelid = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_inherits_inhrelid as i32, desc, &mut isnull)
        }
        .as_oid();
        result.push(inhrelid);
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    result.sort_unstable();
    if lockmode != NoLock {
        let mut live: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
        for &child in result.iter() {
            lmgr::LockRelationOid(child, lockmode)?;
            if syscache_seams::search_syscache_exists_reloid::call(child)? {
                live.push(child);
            } else {
                lmgr::UnlockRelationOid(child, lockmode)?;
            }
        }
        return Ok(live);
    }
    Ok(result)
}

pub fn find_all_inheritors<'mcx>(
    mcx: Mcx<'mcx>,
    parent_rel_id: Oid,
    lockmode: LOCKMODE,
) -> PgResult<PgVec<'mcx, Oid>> {
    Ok(find_all_inheritors_numparents(mcx, parent_rel_id, lockmode)?.0)
}

// find_all_inheritors with the numparents out-list: per rel, how many of its
// parents lie inside the returned hierarchy.
pub fn find_all_inheritors_numparents<'mcx>(
    mcx: Mcx<'mcx>,
    parent_rel_id: Oid,
    lockmode: LOCKMODE,
) -> PgResult<(PgVec<'mcx, Oid>, PgVec<'mcx, i32>)> {
    let mut rels_list: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut rel_numparents: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    rels_list.push(parent_rel_id);
    rel_numparents.push(0);
    let mut i = 0;
    while i < rels_list.len() {
        let currentrel = rels_list[i];
        let children = find_inheritance_children(mcx, currentrel, lockmode)?;
        for &child in children.iter() {
            match rels_list.iter().position(|&r| r == child) {
                None => {
                    rels_list.push(child);
                    rel_numparents.push(1);
                }
                Some(pos) => rel_numparents[pos] += 1,
            }
        }
        i += 1;
    }
    Ok((rels_list, rel_numparents))
}

// has_subclass (lsyscache.c): pg_class.relhassubclass via syscache.
pub fn has_subclass(relation_id: Oid) -> PgResult<bool> {
    lsyscache::get_rel_relhassubclass(relation_id)
}

pub fn has_superclass(mcx: Mcx<'_>, relation_id: Oid) -> PgResult<bool> {
    let rel = table::table_open(mcx, InheritsRelationId, AccessShareLock)?;
    let keys = [eq_key(Anum_pg_inherits_inhrelid, F_OIDEQ, Datum::from_oid(relation_id))];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, InheritsRelidSeqnoIndexId, true, None, &keys)?;
    let found = genam::systable_getnext(mcx, &mut scan)?.is_some();
    genam::systable_endscan(mcx, scan)?;
    rel.close(AccessShareLock)?;
    Ok(found)
}

// BFS up the inheritance graph; subclass side may be a domain over a complex
// type, superclass may not. Cold path: scans run in a local scratch context.
pub fn typeInheritsFrom(subclass_type_id: Oid, superclass_type_id: Oid) -> PgResult<bool> {
    let subclass_relid =
        lsyscache::get_typ_typrelid(lsyscache::getBaseType(subclass_type_id)?)?;
    if subclass_relid == InvalidOid {
        return Ok(false);
    }
    let superclass_relid = lsyscache::get_typ_typrelid(superclass_type_id)?;
    if superclass_relid == InvalidOid {
        return Ok(false);
    }
    if !has_subclass(superclass_relid)? {
        return Ok(false);
    }

    let cx = mcx::MemoryContext::new_bump("typeInheritsFrom");
    let mcx = cx.mcx();
    let mut result = false;
    let mut queue: PgVec<'_, Oid> = PgVec::new_in(mcx);
    let mut visited: PgVec<'_, Oid> = PgVec::new_in(mcx);
    queue.push(subclass_relid);

    let rel = table::table_open(mcx, InheritsRelationId, AccessShareLock)?;
    let desc = rel.descr();
    let mut i = 0;
    'search: while i < queue.len() {
        let this_relid = queue[i];
        i += 1;
        if visited.iter().any(|&r| r == this_relid) {
            continue;
        }
        visited.push(this_relid);
        let keys = [eq_key(Anum_pg_inherits_inhrelid, F_OIDEQ, Datum::from_oid(this_relid))];
        let mut scan =
            genam::systable_beginscan(mcx, &rel, InheritsRelidSeqnoIndexId, true, None, &keys)?;
        while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
            let mut isnull = false;
            // SAFETY: fixed NOT NULL pg_inherits columns under its descriptor.
            let inhparent = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    Anum_pg_inherits_inhparent as i32,
                    desc,
                    &mut isnull,
                )
            }
            .as_oid();
            if inhparent == superclass_relid {
                result = true;
                genam::systable_endscan(mcx, scan)?;
                break 'search;
            }
            queue.push(inhparent);
        }
        genam::systable_endscan(mcx, scan)?;
    }
    rel.close(AccessShareLock)?;
    Ok(result)
}

pub fn DeleteInheritsTuple<'mcx>(
    mcx: Mcx<'mcx>,
    inhrelid: Oid,
    inhparent: Oid,
    expect_detach_pending: bool,
    childname: Option<&str>,
) -> PgResult<bool> {
    let mut found = false;
    let rel = table::table_open(mcx, InheritsRelationId, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_inherits_inhrelid, F_OIDEQ, Datum::from_oid(inhrelid))];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, InheritsRelidSeqnoIndexId, true, None, &keys)?;
    let desc = rel.descr();
    while let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_inherits columns under its descriptor.
        let parent = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_inherits_inhparent as i32, desc, &mut isnull)
        }
        .as_oid();
        if inhparent == InvalidOid || parent == inhparent {
            // SAFETY: as above.
            let detach_pending = unsafe {
                types_tuple::heap_getattr(
                    tup,
                    Anum_pg_inherits_inhdetachpending as i32,
                    desc,
                    &mut isnull,
                )
            }
            .as_bool();
            if detach_pending != expect_detach_pending {
                let name = childname.unwrap_or("unknown relation");
                let e = if detach_pending {
                    PgError::new(ERROR, format!("cannot detach partition \"{name}\""))
                        .with_detail(
                            "The partition is being detached concurrently or has an \
                             unfinished detach.",
                        )
                        .with_hint(
                            "Use ALTER TABLE ... DETACH PARTITION ... FINALIZE to complete \
                             the pending detach operation.",
                        )
                } else {
                    PgError::new(
                        ERROR,
                        format!("cannot complete detaching partition \"{name}\""),
                    )
                    .with_detail("There's no pending concurrent detach.")
                };
                return Err(Box::new(e.with_sqlstate(
                    types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
                )));
            }
            let tid = tup.t_self;
            catalog_indexing::CatalogTupleDelete(&rel, &tid)?;
            found = true;
        }
    }
    genam::systable_endscan(mcx, scan)?;
    rel.close(RowExclusiveLock)?;
    Ok(found)
}

pub fn get_partition_parent(mcx: Mcx<'_>, relid: Oid, even_if_detached: bool) -> PgResult<Oid> {
    let rel = table::table_open(mcx, InheritsRelationId, AccessShareLock)?;
    let (result, detach_pending) = get_partition_parent_worker(mcx, &rel, relid)?;
    rel.close(AccessShareLock)?;
    if result == InvalidOid {
        panic!("could not find tuple for parent of relation {relid}");
    }
    if detach_pending && !even_if_detached {
        panic!("relation {relid} has no parent because it's being detached");
    }
    Ok(result)
}

// C: partition.c get_partition_ancestors — bottom-up parent chain, stopping
// at a detach-pending edge.
pub fn get_partition_ancestors<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let rel = table::table_open(mcx, InheritsRelationId, AccessShareLock)?;
    let mut ancestors: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut current = relid;
    loop {
        let (parent, detach_pending) = get_partition_parent_worker(mcx, &rel, current)?;
        if parent == InvalidOid || detach_pending {
            break;
        }
        ancestors.push(parent);
        current = parent;
    }
    rel.close(AccessShareLock)?;
    Ok(ancestors)
}

pub fn PartitionHasPendingDetach(mcx: Mcx<'_>, partoid: Oid) -> PgResult<bool> {
    let rel = table::table_open(mcx, InheritsRelationId, RowExclusiveLock)?;
    let keys = [eq_key(Anum_pg_inherits_inhrelid, F_OIDEQ, Datum::from_oid(partoid))];
    let mut scan =
        genam::systable_beginscan(mcx, &rel, InheritsRelidSeqnoIndexId, true, None, &keys)?;
    let desc = rel.descr();
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_inherits columns under its descriptor.
        let detached = unsafe {
            types_tuple::heap_getattr(
                tup,
                Anum_pg_inherits_inhdetachpending as i32,
                desc,
                &mut isnull,
            )
        }
        .as_bool();
        genam::systable_endscan(mcx, scan)?;
        rel.close(RowExclusiveLock)?;
        return Ok(detached);
    }
    panic!("relation {partoid} is not a partition");
}

// C: partition.c index_get_partition; takes the partition's relid (Rust
// relcache hands out index lists by relid).
pub fn index_get_partition(mcx: Mcx<'_>, partition_relid: Oid, index_id: Oid) -> PgResult<Oid> {
    let idxlist = relcache_seams::relation_get_index_list::call(mcx, partition_relid)?;
    for &part_idx in idxlist.iter() {
        if !lsyscache::get_rel_relispartition(part_idx)? {
            continue;
        }
        if get_partition_parent(mcx, part_idx, false)? == index_id {
            return Ok(part_idx);
        }
    }
    Ok(InvalidOid)
}

fn get_partition_parent_worker<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &types_rel::Relation<'mcx>,
    relid: Oid,
) -> PgResult<(Oid, bool)> {
    let keys = [
        eq_key(Anum_pg_inherits_inhrelid, F_OIDEQ, Datum::from_oid(relid)),
        eq_key(Anum_pg_inherits_inhseqno, F_INT4EQ, Datum::from_i32(1)),
    ];
    let mut scan =
        genam::systable_beginscan(mcx, rel, InheritsRelidSeqnoIndexId, true, None, &keys)?;
    let desc = rel.descr();
    let mut result = InvalidOid;
    let mut detach_pending = false;
    if let Some(tup) = genam::systable_getnext(mcx, &mut scan)? {
        let mut isnull = false;
        // SAFETY: fixed NOT NULL pg_inherits columns under its descriptor.
        result = unsafe {
            types_tuple::heap_getattr(tup, Anum_pg_inherits_inhparent as i32, desc, &mut isnull)
        }
        .as_oid();
        // SAFETY: as above.
        detach_pending = unsafe {
            types_tuple::heap_getattr(
                tup,
                Anum_pg_inherits_inhdetachpending as i32,
                desc,
                &mut isnull,
            )
        }
        .as_bool();
    }
    genam::systable_endscan(mcx, scan)?;
    Ok((result, detach_pending))
}
