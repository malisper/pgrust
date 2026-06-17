#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
// Every fallible function returns the shared `types_error::PgResult`
// (== `Result<_, PgError>`), the project-wide error contract; we accept the
// large-`Err` lint crate-wide.
#![allow(clippy::result_large_err)]

//! Catalog-read owner for `pg_authid` startup probes.
//!
//! Hosts the `pg_authid` heap-scan reads that `postinit.c` / `dbcommands.c`
//! need during backend startup but which belong to the catalog-read layer
//! (the relcache/heap-scan machinery), not to the orchestration. The sole
//! routine currently ported is [`ThereIsAtLeastOneRole`] (postinit.c).
//!
//! `pg_authid` is a nailed bootstrap catalog (built via `formrdesc` in
//! `RelationCacheInitializePhase2`), so `table_open(AuthIdRelationId, ...)`
//! resolves its relcache entry without a recursive catalog scan; the row scan
//! itself is the real catalog heap scan via the genam iterator.

use mcx::{Mcx, MemoryContext};
use types_catalog::catalog::AUTH_ID_RELATION_ID;
use types_core::primitive::InvalidOid;
use types_error::PgResult;
use types_storage::lock::AccessShareLock;

use backend_access_index_genam_seams as genam;
use backend_access_table_table as table;

/// `ThereIsAtLeastOneRole(void)` (postinit.c): returns true if at least one
/// role is defined.
///
/// ```c
/// pg_authid_rel = table_open(AuthIdRelationId, AccessShareLock);
/// scan = table_beginscan_catalog(pg_authid_rel, 0, NULL);
/// result = (heap_getnext(scan, ForwardScanDirection) != NULL);
/// table_endscan(scan);
/// table_close(pg_authid_rel, AccessShareLock);
/// return result;
/// ```
///
/// The catalog heap scan is expressed through `systable_beginscan` with
/// `index_ok = false` and no keys: that opens no index and runs
/// `table_beginscan_strat(..., allow_sync = false)`, exactly what
/// `table_beginscan_catalog(rel, 0, NULL)` does (the same boundary
/// `pg_db_role_setting`'s `DropSetting` uses). `heap_getnext != NULL` is the
/// first `systable_getnext` returning `Some`.
pub fn ThereIsAtLeastOneRole(mcx: Mcx<'_>) -> PgResult<bool> {
    // pg_authid_rel = table_open(AuthIdRelationId, AccessShareLock);
    let pg_authid_rel = table::table_open(mcx, AUTH_ID_RELATION_ID, AccessShareLock)?;

    // scan = table_beginscan_catalog(pg_authid_rel, 0, NULL);
    let mut scan =
        genam::systable_beginscan::call(&pg_authid_rel, InvalidOid, false, None, &[])?;

    // result = (heap_getnext(scan, ForwardScanDirection) != NULL);
    let row_mcx = MemoryContext::new("ThereIsAtLeastOneRole row");
    let result = genam::systable_getnext::call(row_mcx.mcx(), scan.desc_mut())?.is_some();

    // table_endscan(scan);
    scan.end()?;

    // table_close(pg_authid_rel, AccessShareLock);
    pg_authid_rel.close(AccessShareLock)?;

    Ok(result)
}

/// Install this unit's inward seams.
pub fn init_seams() {
    backend_catalog_pg_authid_seams::there_is_at_least_one_role::set(ThereIsAtLeastOneRole);
}
