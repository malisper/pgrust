use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, RootIndexBuildServices, map_access_error,
};
use crate::backend::access::{RootAccessRuntime, RootAccessServices, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext, IndexBuildResult,
    IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInsertContext, IndexVacuumContext,
};
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};
use crate::include::access::tidbitmap::TidBitmap;
use crate::include::catalog::BRIN_AM_OID;

fn brin_routine(
    am_oid: u32,
) -> Result<crate::include::access::amapi::IndexAmRoutine, CatalogError> {
    if am_oid == BRIN_AM_OID {
        return Ok(crate::backend::access::brin::brin_am_handler());
    }
    Err(CatalogError::Corrupt("unknown index access method"))
}

pub fn index_build_stub(
    ctx: &IndexBuildContext,
    am_oid: u32,
) -> Result<IndexBuildResult, CatalogError> {
    if pgrust_access::index::indexam::supports_index_am(am_oid) {
        // :HACK: root compatibility adapter while generic index dispatch is
        // owned by `pgrust_access` but expression-index projection still lives
        // in root executor/analyzer services.
        let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
        let mut index_services = RootIndexBuildServices::new(ctx, &mut key_projector);
        let heap_services = RootAccessRuntime::heap(
            &ctx.pool,
            &ctx.txns,
            Some(ctx.interrupts.as_ref()),
            ctx.client_id,
        );
        return pgrust_access::index::indexam::index_build_stub(
            &ctx.to_access_context(),
            am_oid,
            &heap_services,
            &mut index_services,
            &heap_services,
            &RootAccessServices,
            &RootAccessWal {
                pool: ctx.pool.as_ref(),
            },
        )
        .map_err(map_access_error);
    }
    let routine = brin_routine(am_oid)?;
    if let Some(ambuild) = routine.ambuild {
        ambuild(ctx)
    } else {
        Ok(IndexBuildResult::default())
    }
}

pub fn index_insert_stub(ctx: &IndexInsertContext, am_oid: u32) -> Result<bool, CatalogError> {
    if pgrust_access::index::indexam::supports_index_am(am_oid) {
        // :HACK: root compatibility adapter while access runtime still needs
        // root-owned transaction wait and interrupt services.
        let runtime = RootAccessRuntime {
            pool: Some(&ctx.pool),
            txns: Some(&ctx.txns),
            txn_waiter: ctx.txn_waiter.as_deref(),
            interrupts: Some(ctx.interrupts.as_ref()),
            client_id: ctx.client_id,
        };
        return pgrust_access::index::indexam::index_insert_stub(
            &ctx.to_access_context(),
            am_oid,
            &runtime,
            &RootAccessServices,
            &RootAccessWal {
                pool: ctx.pool.as_ref(),
            },
        )
        .map_err(map_access_error);
    }
    let routine = brin_routine(am_oid)?;
    let aminsert = routine
        .aminsert
        .ok_or(CatalogError::Corrupt("missing index insert callback"))?;
    aminsert(ctx)
}

pub fn index_build_empty_stub(
    ctx: &IndexBuildEmptyContext,
    am_oid: u32,
) -> Result<(), CatalogError> {
    if pgrust_access::index::indexam::supports_index_am(am_oid) {
        // :HACK: root compatibility adapter while WAL ownership remains in root.
        return pgrust_access::index::indexam::index_build_empty_stub(
            &ctx.to_access_context(),
            am_oid,
            &RootAccessWal {
                pool: ctx.pool.as_ref(),
            },
        )
        .map_err(map_access_error);
    }
    let routine = brin_routine(am_oid)?;
    let ambuildempty = routine
        .ambuildempty
        .ok_or(CatalogError::Corrupt("missing index buildempty callback"))?;
    ambuildempty(ctx)
}

pub fn index_beginscan(
    ctx: &IndexBeginScanContext,
    am_oid: u32,
) -> Result<IndexScanDesc, CatalogError> {
    if pgrust_access::index::indexam::supports_index_am(am_oid) {
        // :HACK: root compatibility adapter while scan callers use root paths.
        return pgrust_access::index::indexam::index_beginscan(
            &ctx.to_access_context(),
            am_oid,
            &RootAccessServices,
        )
        .map_err(map_access_error);
    }
    let routine = brin_routine(am_oid)?;
    let ambeginscan = routine
        .ambeginscan
        .ok_or(CatalogError::Corrupt("missing index beginscan callback"))?;
    ambeginscan(ctx)
}

pub fn index_rescan(
    scan: &mut IndexScanDesc,
    am_oid: u32,
    keys: &[crate::include::access::scankey::ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    if pgrust_access::index::indexam::supports_index_am(am_oid) {
        // :HACK: root compatibility adapter while scan callers use root paths.
        return pgrust_access::index::indexam::index_rescan(
            scan,
            am_oid,
            keys,
            direction,
            &RootAccessServices,
        )
        .map_err(map_access_error);
    }
    let routine = brin_routine(am_oid)?;
    let amrescan = routine
        .amrescan
        .ok_or(CatalogError::Corrupt("missing index rescan callback"))?;
    amrescan(scan, keys, direction)
}

pub fn index_getnext(scan: &mut IndexScanDesc, am_oid: u32) -> Result<bool, CatalogError> {
    if pgrust_access::index::indexam::supports_index_am(am_oid) {
        // :HACK: root compatibility adapter while scan callers use root paths.
        return pgrust_access::index::indexam::index_getnext(scan, am_oid, &RootAccessServices)
            .map_err(map_access_error);
    }
    let routine = brin_routine(am_oid)?;
    let amgettuple = routine
        .amgettuple
        .ok_or(CatalogError::Corrupt("missing index gettuple callback"))?;
    amgettuple(scan)
}

pub fn index_getbitmap(
    scan: &mut IndexScanDesc,
    am_oid: u32,
    bitmap: &mut TidBitmap,
) -> Result<i64, CatalogError> {
    if pgrust_access::index::indexam::supports_index_am(am_oid) {
        // :HACK: root compatibility adapter while scan callers use root paths.
        let pool = scan.pool.clone();
        let heap_services = RootAccessRuntime::heap_storage(&pool, scan.client_id);
        return pgrust_access::index::indexam::index_getbitmap(
            scan,
            am_oid,
            bitmap,
            &heap_services,
            &RootAccessServices,
        )
        .map_err(map_access_error);
    }
    let routine = brin_routine(am_oid)?;
    let amgetbitmap = routine
        .amgetbitmap
        .ok_or(CatalogError::Corrupt("missing index getbitmap callback"))?;
    amgetbitmap(scan, bitmap)
}

pub fn index_endscan(scan: IndexScanDesc, am_oid: u32) -> Result<(), CatalogError> {
    if pgrust_access::index::indexam::supports_index_am(am_oid) {
        // :HACK: root compatibility adapter while scan callers use root paths.
        return pgrust_access::index::indexam::index_endscan(scan, am_oid)
            .map_err(map_access_error);
    }
    let routine = brin_routine(am_oid)?;
    let amendscan = routine
        .amendscan
        .ok_or(CatalogError::Corrupt("missing index endscan callback"))?;
    amendscan(scan)
}

pub fn index_bulk_delete(
    ctx: &IndexVacuumContext,
    am_oid: u32,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    if pgrust_access::index::indexam::supports_index_am(am_oid) {
        // :HACK: root compatibility adapter while VACUUM still owns root
        // transaction and interrupt state.
        let runtime = RootAccessRuntime::heap(
            &ctx.pool,
            &ctx.txns,
            Some(ctx.interrupts.as_ref()),
            ctx.client_id,
        );
        return pgrust_access::index::indexam::index_bulk_delete(
            &ctx.to_access_context(),
            am_oid,
            callback,
            stats,
            &runtime,
            &RootAccessWal {
                pool: ctx.pool.as_ref(),
            },
        )
        .map_err(map_access_error);
    }
    let routine = brin_routine(am_oid)?;
    let ambulkdelete = routine
        .ambulkdelete
        .ok_or(CatalogError::Corrupt("missing index bulkdelete callback"))?;
    ambulkdelete(ctx, callback, stats)
}

pub fn index_vacuum_cleanup(
    ctx: &IndexVacuumContext,
    am_oid: u32,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    if pgrust_access::index::indexam::supports_index_am(am_oid) {
        // :HACK: root compatibility adapter while VACUUM still owns root
        // transaction and WAL state.
        let runtime = RootAccessRuntime::heap(
            &ctx.pool,
            &ctx.txns,
            Some(ctx.interrupts.as_ref()),
            ctx.client_id,
        );
        return pgrust_access::index::indexam::index_vacuum_cleanup(
            &ctx.to_access_context(),
            am_oid,
            stats,
            &runtime,
            &RootAccessWal {
                pool: ctx.pool.as_ref(),
            },
        )
        .map_err(map_access_error);
    }
    let routine = brin_routine(am_oid)?;
    let amvacuumcleanup = routine.amvacuumcleanup.ok_or(CatalogError::Corrupt(
        "missing index vacuumcleanup callback",
    ))?;
    amvacuumcleanup(ctx, stats)
}
