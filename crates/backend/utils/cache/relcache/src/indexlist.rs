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
