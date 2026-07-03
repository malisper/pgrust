// pg_inherits.c: StoreSingleInheritance + find_inheritance_children +
// find_all_inheritors (DETACH CONCURRENTLY loud).
#![allow(non_snake_case, non_upper_case_globals)]

use datum::Datum;
use mcx::{Mcx, PgVec};
use types_core::fmgr::F_OIDEQ;
use types_core::{AttrNumber, Oid, RegProcedure};
use types_error::PgResult;
use types_rel::{AccessShareLock, NoLock, RowExclusiveLock, LOCKMODE};
use types_scan::scankey::{BTEqualStrategyNumber, ScanKeyData};

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

// find_inheritance_children (children sorted by OID, then locked in that
// order as C does to avoid deadlock; the concurrent-DROP recheck arm is
// subsumed by the lock model — a locked child cannot vanish before open).
pub fn find_inheritance_children<'mcx>(
    mcx: Mcx<'mcx>,
    parent_rel_id: Oid,
    lockmode: LOCKMODE,
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
            panic!("pg_inherits: DETACH CONCURRENTLY pending partitions unported");
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
        for &child in result.iter() {
            lmgr::LockRelationOid(child, lockmode)?;
        }
    }
    Ok(result)
}

// find_all_inheritors (numparents callers unported; BFS agenda order kept).
pub fn find_all_inheritors<'mcx>(
    mcx: Mcx<'mcx>,
    parent_rel_id: Oid,
    lockmode: LOCKMODE,
) -> PgResult<PgVec<'mcx, Oid>> {
    let mut rels_list: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    rels_list.push(parent_rel_id);
    let mut i = 0;
    while i < rels_list.len() {
        let currentrel = rels_list[i];
        let children = find_inheritance_children(mcx, currentrel, lockmode)?;
        for &child in children.iter() {
            if !rels_list.contains(&child) {
                rels_list.push(child);
            }
        }
        i += 1;
    }
    Ok(rels_list)
}

// has_subclass (lsyscache.c): pg_class.relhassubclass via syscache.
pub fn has_subclass(relation_id: Oid) -> PgResult<bool> {
    lsyscache::get_rel_relhassubclass(relation_id)
}
