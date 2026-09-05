// Subscription-side replication-origin helpers. The origin.c engine lives in
// the `origin` crate (round-4 inc B); this module keeps only the
// subscription-specific name mapping and thin delegating wrappers.
use mcx::Mcx;
use types_core::{Oid, RepOriginId};
use types_error::PgResult;

// ReplicationOriginNameForLogicalRep (worker.c).
pub(crate) fn ReplicationOriginNameForLogicalRep(subid: Oid, relid: Oid) -> String {
    if relid != types_core::InvalidOid {
        format!("pg_{subid}_{relid}")
    } else {
        format!("pg_{subid}")
    }
}

pub(crate) fn replorigin_create(mcx: Mcx<'_>, roname: &str) -> PgResult<RepOriginId> {
    ::origin::replorigin_create(mcx, roname)
}

pub(crate) fn replorigin_by_name(roname: &str, missing_ok: bool) -> PgResult<RepOriginId> {
    ::origin::replorigin_by_name(roname, missing_ok)
}

// replorigin_get_progress (origin.c): the origin's remote_lsn, as read by
// ALTER SUBSCRIPTION ... SKIP (subscriptioncmds.c:1562).
pub(crate) fn replorigin_get_progress(
    node: RepOriginId,
    flush: bool,
) -> PgResult<types_core::primitive::XLogRecPtr> {
    ::origin::replorigin_get_progress(node, flush)
}

pub(crate) fn replorigin_drop_by_name(
    mcx: Mcx<'_>,
    name: &str,
    missing_ok: bool,
) -> PgResult<()> {
    // C DropSubscription/AlterSubscription pass nowait=false.
    ::origin::replorigin_drop_by_name(mcx, name, missing_ok, false)
}
