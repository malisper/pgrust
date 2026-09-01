use mcx::{Mcx, PgVec};
use types_core::{InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_INTERNAL_ERROR};
use types_rel::{
    RdIndexList, RelationData, RELKIND_PARTITIONED_TABLE, REPLICA_IDENTITY_DEFAULT,
    REPLICA_IDENTITY_INDEX,
};

use crate::{cache_mcx, store};

#[track_caller]
#[cold]
#[inline(never)]
fn not_open(relid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "RelationGetIndexList: relation {relid} not in relcache"
        ))
        .with_sqlstate(ERRCODE_INTERNAL_ERROR),
    )
}

fn copy_list<'mcx>(mcx: Mcx<'mcx>, list: &[Oid]) -> PgResult<PgVec<'mcx, Oid>> {
    let mut out = mcx::vec_with_capacity_in(mcx, list.len())?;
    out.extend_from_slice(list);
    Ok(out)
}

// RelationGetIndexList (relcache.c). The caller holds the relation open; the
// returned list is a caller-context copy, the cache copy lives with the entry.
pub fn RelationGetIndexList<'mcx>(mcx: Mcx<'mcx>, relid: Oid) -> PgResult<PgVec<'mcx, Oid>> {
    let rel = store::RelationIdGetRelation(relid)?.ok_or_else(|| not_open(relid))?;
    if let Some(cached) = rel.rd_indexlist.borrow().as_ref() {
        return copy_list(mcx, &cached.list);
    }
    rebuild_index_list(mcx, &rel)
}

// RelationGetReplicaIndex's oid resolution (relcache.c), reading through the
// CURRENT cache entry. C fills rd_replidindex on the single in-place relcache
// struct, so a caller-held relation always observes the freshly built value;
// our rebuild replaces the Rc, so a caller holding a rebuilt-away predecessor
// (entry invalidated between open and use) would see its own rd_indexlist
// stay None forever and misread "no replica index" (round-18 soak, class-6
// disposition: with a delete-publishing publication present that misread
// escalates to a B-only 55000 where C succeeds). Resolving via the current
// entry restores C's in-place semantics.
pub fn RelationGetReplicaIndexOid(mcx: Mcx<'_>, relid: Oid) -> PgResult<Oid> {
    let rel = store::RelationIdGetRelation(relid)?.ok_or_else(|| not_open(relid))?;
    if rel.rd_indexlist.borrow().is_none() {
        let _ = rebuild_index_list(mcx, &rel)?;
    }
    let replidindex = rel
        .rd_indexlist
        .borrow()
        .as_ref()
        .map(|l| l.replidindex)
        .unwrap_or(InvalidOid);
    Ok(replidindex)
}

// dropconstraint_internal's rd_pkindex/rd_replidindex read, through the
// CURRENT cache entry for the same reason as RelationGetReplicaIndexOid
// above: a rebuild replaces the Rc, so a caller-held predecessor entry can
// have rd_indexlist == None even right after RelationGetIndexList succeeded
// (r21 portals soak: ALTER TABLE .. DROP NOT NULL panicked on that expect
// when an invalidation landed inside the pg_index scan).
pub fn RelationGetPkReplidIndexes(mcx: Mcx<'_>, relid: Oid) -> PgResult<(Oid, Oid)> {
    let rel = store::RelationIdGetRelation(relid)?.ok_or_else(|| not_open(relid))?;
    if rel.rd_indexlist.borrow().is_none() {
        let _ = rebuild_index_list(mcx, &rel)?;
    }
    let pair = rel
        .rd_indexlist
        .borrow()
        .as_ref()
        .map(|l| (l.pkindex, l.replidindex))
        .unwrap_or((InvalidOid, InvalidOid));
    Ok(pair)
}

// The scan may process invalidations (it re-enters the relcache), so no
// rd_indexlist borrow is held across it; the result is built in the caller's
// context and installed on the entry only after the scan completes, as in C.
fn rebuild_index_list<'mcx>(
    mcx: Mcx<'mcx>,
    rel: &RelationData<'static>,
) -> PgResult<PgVec<'mcx, Oid>> {
    let rows = relcache_build_seams::scan_pg_index_shapes::call(mcx, rel.rd_id)?;

    let mut result: PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, rows.len())?;
    let mut pkey_index = InvalidOid;
    let mut candidate_index = InvalidOid;
    let mut pkdeferrable = false;
    for row in rows.iter() {
        if !row.indislive {
            continue;
        }
        result.push(row.indexrelid);
        if !row.indisunique || row.has_indpred {
            continue;
        }
        if row.indisprimary
            && (row.indisvalid || rel.rd_rel.relkind == RELKIND_PARTITIONED_TABLE)
        {
            pkey_index = row.indexrelid;
            pkdeferrable = !row.indimmediate;
        }
        if !row.indimmediate || !row.indisvalid {
            continue;
        }
        if row.indisreplident {
            candidate_index = row.indexrelid;
        }
    }
    result.sort_unstable();

    let replident = rel.rd_rel.relreplident;
    let replidindex = if replident == REPLICA_IDENTITY_DEFAULT
        && pkey_index != InvalidOid
        && !pkdeferrable
    {
        pkey_index
    } else if replident == REPLICA_IDENTITY_INDEX && candidate_index != InvalidOid {
        candidate_index
    } else {
        InvalidOid
    };

    *rel.rd_indexlist.borrow_mut() = Some(RdIndexList {
        list: copy_list(cache_mcx(), &result)?,
        pkindex: pkey_index,
        ispkdeferrable: pkdeferrable,
        replidindex,
    });
    Ok(result)
}
