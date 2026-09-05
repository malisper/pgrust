//! spccache.c: per-tablespace option lookups (pg_tablespace.spcoptions).
//! Divergence owned here (the attoptcache precedent): C keeps a
//! syscache-invalidated hash of parsed TableSpaceOpts (TableSpaceCacheHash +
//! InvalidateTableSpaceCacheCallback, spccache.c:55-97); this port re-reads
//! the TABLESPACEOID syscache on every call, so an ALTER TABLESPACE ...
//! SET/RESET is observed at once with no callback to register.
use mcx::Mcx;
use reloptions::TableSpaceOpts;
use types_core::{InvalidOid, Oid};
use types_error::PgResult;

// get_tablespace (spccache.c:98-170). spcid always comes from a pg_class
// tuple, so InvalidOid means the database's default tablespace; a missing
// pg_tablespace row (!HeapTupleIsValid) or a null spcoptions reads as "no
// options specified" (opts == NULL).
pub fn get_tablespace(mcx: Mcx<'_>, spcid: Oid) -> PgResult<Option<TableSpaceOpts>> {
    let spcid = if spcid == InvalidOid {
        init_small::globals::MyDatabaseTableSpace()
    } else {
        spcid
    };
    // A catalog-less harness (unit fixtures install no TABLESPACEOID
    // projection) has no pg_tablespace row to read: C's invalid-tuple arm.
    if !syscache_seams::pg_tablespace_spcoptions::is_installed() {
        return Ok(None);
    }
    let Some(Some(datum)) = syscache_seams::pg_tablespace_spcoptions::call(mcx, spcid)? else {
        return Ok(None);
    };
    let p = datum.as_usize() as *const u8;
    // SAFETY: not-null spcoptions datum copied out of the syscache tuple into
    // mcx — a live varlena image readable through its varsize_any extent.
    let image = unsafe { core::slice::from_raw_parts(p, types_tuple::varatt::varsize_any(p)) };
    // tablespace_reloptions(datum, false): unset options read as -1.
    reloptions::tablespace_reloptions(mcx, Some(image), false)
}

// get_tablespace_page_costs (spccache.c:182-205): the tablespace's own
// random_page_cost / seq_page_cost when set (>= 0), else the session GUC
// values the caller passes (costsize owns those cells).
pub fn get_tablespace_page_costs(
    mcx: Mcx<'_>,
    spcid: Oid,
    random_page_cost: f64,
    seq_page_cost: f64,
) -> PgResult<(f64, f64)> {
    let opts = get_tablespace(mcx, spcid)?;
    let spc_random_page_cost = match opts {
        Some(o) if o.random_page_cost >= 0.0 => o.random_page_cost,
        _ => random_page_cost,
    };
    let spc_seq_page_cost = match opts {
        Some(o) if o.seq_page_cost >= 0.0 => o.seq_page_cost,
        _ => seq_page_cost,
    };
    Ok((spc_random_page_cost, spc_seq_page_cost))
}

// get_tablespace_io_concurrency (spccache.c:215-224).
pub fn get_tablespace_io_concurrency(mcx: Mcx<'_>, spcid: Oid) -> PgResult<i32> {
    Ok(match get_tablespace(mcx, spcid)? {
        Some(o) if o.effective_io_concurrency >= 0 => o.effective_io_concurrency,
        _ => guc_tables::vars::effective_io_concurrency.read(),
    })
}

// get_tablespace_maintenance_io_concurrency (spccache.c:230-237).
pub fn get_tablespace_maintenance_io_concurrency(mcx: Mcx<'_>, spcid: Oid) -> PgResult<i32> {
    Ok(match get_tablespace(mcx, spcid)? {
        Some(o) if o.maintenance_io_concurrency >= 0 => o.maintenance_io_concurrency,
        _ => guc_tables::vars::maintenance_io_concurrency.read(),
    })
}

#[cfg(test)]
mod tests;
