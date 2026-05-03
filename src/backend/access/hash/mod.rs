pub(crate) mod support;
pub mod wal;

use crate::backend::access::index::buildkeys::{
    IndexBuildKeyProjector, RootIndexBuildServices, map_access_error, map_catalog_error_to_access,
    materialize_heap_row_values,
};
use crate::backend::access::{RootAccessRuntime, RootAccessServices, RootAccessWal};
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::{
    IndexAmRoutine, IndexBeginScanContext, IndexBuildContext, IndexBuildEmptyContext,
    IndexBuildResult, IndexBulkDeleteCallback, IndexBulkDeleteResult, IndexInsertContext,
    IndexVacuumContext,
};
use crate::include::access::hash::{hash_tuple_hash, hash_tuple_key_values};
use crate::include::access::htup::AttributeCompression;
use crate::include::access::itup::IndexTupleData;
use crate::include::access::relscan::{IndexScanDesc, ScanDirection};
use crate::include::access::scankey::ScanKeyData;
use crate::include::access::tidbitmap::TidBitmap;
use crate::include::nodes::datum::Value;
use crate::include::nodes::primnodes::RelationDesc;
use pgrust_access::{AccessError, AccessHeapServices, AccessIndexServices, hash as access_hash};

pub(crate) use support::{
    HASH_PARTITION_SEED, hash_bytes_extended, hash_combine64, hash_value_extended,
};

fn encode_hash_tuple_payload(
    desc: &RelationDesc,
    key_values: &[Value],
    hash: u32,
    default_toast_compression: AttributeCompression,
) -> Result<Vec<u8>, CatalogError> {
    crate::include::access::hash::encode_hash_tuple_payload(
        desc,
        key_values,
        hash,
        default_toast_compression,
        &RootAccessServices,
    )
    .map_err(map_access_error)
}

fn tuple_hash(tuple: &IndexTupleData) -> Result<u32, CatalogError> {
    hash_tuple_hash(tuple).map_err(map_access_error)
}

fn tuple_key_values(
    desc: &RelationDesc,
    tuple: &IndexTupleData,
) -> Result<Vec<Value>, CatalogError> {
    hash_tuple_key_values(desc, tuple, &RootAccessServices).map_err(map_access_error)
}

fn hashbuild(ctx: &IndexBuildContext) -> Result<IndexBuildResult, CatalogError> {
    let attr_descs = ctx.heap_desc.attribute_descs();
    let mut key_projector = IndexBuildKeyProjector::new(ctx)?;
    let mut index_services = RootIndexBuildServices::new(ctx, &mut key_projector);
    let heap_services = crate::backend::access::RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    let mut heap_tuples = 0;
    let mut pending = Vec::new();
    heap_services
        .for_each_visible_heap_tuple(
            ctx.heap_relation,
            ctx.snapshot.clone(),
            &mut |tid, tuple| {
                let datums = tuple
                    .deform(&attr_descs)
                    .map_err(|err| AccessError::Scalar(format!("heap deform failed: {err:?}")))?;
                let row_values = materialize_heap_row_values(&ctx.heap_desc, &datums)
                    .map_err(map_catalog_error_to_access)?;
                heap_tuples += 1;
                if let Some(key_values) =
                    index_services.project_index_row(&ctx.index_meta, &row_values, tid)?
                {
                    pending.push((tid, key_values));
                }
                Ok(())
            },
        )
        .map_err(map_access_error)?;

    drop(index_services);
    access_hash::hashbuild_projected(
        &ctx.to_access_context(),
        heap_tuples,
        pending,
        &heap_services,
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

fn hashbuildempty(ctx: &IndexBuildEmptyContext) -> Result<(), CatalogError> {
    access_hash::hashbuildempty(
        &ctx.to_access_context(),
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

fn hashinsert(ctx: &IndexInsertContext) -> Result<bool, CatalogError> {
    access_hash::hashinsert(
        &ctx.to_access_context(),
        &RootAccessServices,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

fn hashbeginscan(ctx: &IndexBeginScanContext) -> Result<IndexScanDesc, CatalogError> {
    // :HACK: root compatibility adapter while hash scan runtime moves into
    // `pgrust_access`; old AM callbacks still use root context types.
    access_hash::hashbeginscan(&ctx.to_access_context(), &RootAccessServices)
        .map_err(map_access_error)
}

fn hashrescan(
    scan: &mut IndexScanDesc,
    keys: &[ScanKeyData],
    direction: ScanDirection,
) -> Result<(), CatalogError> {
    access_hash::hashrescan(scan, keys, direction, &RootAccessServices).map_err(map_access_error)
}

fn hashgettuple(scan: &mut IndexScanDesc) -> Result<bool, CatalogError> {
    access_hash::hashgettuple(scan, &RootAccessServices).map_err(map_access_error)
}

fn hashgetbitmap(scan: &mut IndexScanDesc, bitmap: &mut TidBitmap) -> Result<i64, CatalogError> {
    access_hash::hashgetbitmap(scan, bitmap, &RootAccessServices).map_err(map_access_error)
}

fn hashendscan(scan: IndexScanDesc) -> Result<(), CatalogError> {
    access_hash::hashendscan(scan).map_err(map_access_error)
}

fn hashbulkdelete(
    ctx: &IndexVacuumContext,
    callback: &IndexBulkDeleteCallback<'_>,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    let runtime = RootAccessRuntime::heap(
        &ctx.pool,
        &ctx.txns,
        Some(ctx.interrupts.as_ref()),
        ctx.client_id,
    );
    access_hash::hashbulkdelete(
        &ctx.to_access_context(),
        callback,
        stats,
        &runtime,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

fn hashvacuumcleanup(
    ctx: &IndexVacuumContext,
    stats: Option<IndexBulkDeleteResult>,
) -> Result<IndexBulkDeleteResult, CatalogError> {
    access_hash::hashvacuumcleanup(
        &ctx.to_access_context(),
        stats,
        &RootAccessWal {
            pool: ctx.pool.as_ref(),
        },
    )
    .map_err(map_access_error)
}

pub fn hash_am_handler() -> IndexAmRoutine {
    IndexAmRoutine {
        amstrategies: 1,
        amsupport: 1,
        amcanorder: false,
        amcanorderbyop: false,
        amcanhash: true,
        amconsistentordering: false,
        amcanbackward: true,
        amcanunique: false,
        amcanmulticol: false,
        amoptionalkey: false,
        amsearcharray: false,
        amsearchnulls: false,
        amstorage: false,
        amclusterable: false,
        ampredlocks: true,
        amsummarizing: false,
        ambuild: Some(hashbuild),
        ambuildempty: Some(hashbuildempty),
        aminsert: Some(hashinsert),
        ambeginscan: Some(hashbeginscan),
        amrescan: Some(hashrescan),
        amgettuple: Some(hashgettuple),
        amgetbitmap: Some(hashgetbitmap),
        amendscan: Some(hashendscan),
        ambulkdelete: Some(hashbulkdelete),
        amvacuumcleanup: Some(hashvacuumcleanup),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::catalog::column_desc;
    use crate::backend::parser::{SqlType, SqlTypeKind};
    use crate::include::access::itemptr::ItemPointerData;

    #[test]
    fn hash_handler_advertises_postgres_like_capabilities() {
        let am = hash_am_handler();

        assert_eq!(am.amstrategies, 1);
        assert_eq!(am.amsupport, 1);
        assert!(am.amcanhash);
        assert!(!am.amcanunique);
        assert!(!am.amcanmulticol);
        assert!(!am.amoptionalkey);
        assert!(am.amgetbitmap.is_some());
    }

    #[test]
    fn hash_tuple_payload_roundtrips_hash_and_key() {
        let desc = RelationDesc {
            columns: vec![column_desc("id", SqlType::new(SqlTypeKind::Int4), false)],
        };
        let payload =
            encode_hash_tuple_payload(&desc, &[Value::Int32(10)], 123, AttributeCompression::Pglz)
                .unwrap();
        let tuple =
            IndexTupleData::new_raw(ItemPointerData::default(), false, true, false, payload);

        assert_eq!(tuple_hash(&tuple).unwrap(), 123);
        assert_eq!(
            tuple_key_values(&desc, &tuple).unwrap(),
            vec![Value::Int32(10)]
        );
    }
}
