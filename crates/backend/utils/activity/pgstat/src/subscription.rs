// pgstat_subscription.c read half; report/create/drop land with logical
// replication apply.

use types_core::{InvalidOid, Oid, TimestampTz};

use crate::pending::{PgStat_HashKey, PGSTAT_KIND_SUBSCRIPTION};
use crate::PgStat_Counter;

pub const CONFLICT_NUM_TYPES: usize = 7;

#[derive(Clone, Copy, Default, PartialEq, Debug)]
#[repr(C)]
pub struct PgStat_StatSubEntry {
    pub apply_error_count: PgStat_Counter,
    pub sync_error_count: PgStat_Counter,
    pub conflict_count: [PgStat_Counter; CONFLICT_NUM_TYPES],
    pub stat_reset_timestamp: TimestampTz,
}

fn subscription_key(subid: Oid) -> PgStat_HashKey {
    PgStat_HashKey { kind: PGSTAT_KIND_SUBSCRIPTION, dboid: InvalidOid, objid: subid as u64 }
}

pub fn pgstat_fetch_stat_subscription(subid: Oid) -> Option<PgStat_StatSubEntry> {
    match crate::shmem::fetch_entry(subscription_key(subid)) {
        Some(crate::shmem::SharedEntry::Subscription(e)) => Some(e),
        Some(_) => unreachable!("subscription key holds non-subscription shared entry"),
        None => None,
    }
}
