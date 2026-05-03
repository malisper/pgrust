use pgrust_access::index::unique as access_unique;

use crate::backend::access::RootAccessRuntime;
use crate::backend::access::index::buildkeys::map_access_error;
use crate::backend::catalog::CatalogError;
use crate::include::access::amapi::IndexInsertContext;
use crate::include::nodes::datum::Value;

pub use access_unique::UniqueProbeConflict;

pub fn probe_unique_conflict(
    ctx: &IndexInsertContext,
    key_values: &[Value],
) -> Result<Option<UniqueProbeConflict>, CatalogError> {
    // :HACK: root compatibility adapter while unique probing uses the generic
    // `pgrust_access` index dispatcher but root still owns transaction waits.
    let access_runtime = RootAccessRuntime {
        pool: Some(&ctx.pool),
        txns: Some(&ctx.txns),
        txn_waiter: ctx.txn_waiter.as_deref(),
        interrupts: Some(ctx.interrupts.as_ref()),
        client_id: ctx.client_id,
    };
    access_unique::probe_unique_conflict(
        &ctx.to_access_context(),
        key_values,
        &access_runtime,
        &crate::backend::access::RootAccessServices,
    )
    .map_err(map_access_error)
}
