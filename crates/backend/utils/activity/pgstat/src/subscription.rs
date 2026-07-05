// pgstat_subscription.c.

use types_core::{InvalidOid, Oid, TimestampTz};

use crate::pending::{PendingData, PgStat_HashKey, PGSTAT_KIND_SUBSCRIPTION};
use crate::PgStat_Counter;

// replication/conflict.h: CT_MULTIPLE_UNIQUE_CONFLICTS + 1.
pub const CONFLICT_NUM_TYPES: usize = 7;

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
pub struct PgStat_StatSubEntry {
    pub apply_error_count: PgStat_Counter,
    pub sync_error_count: PgStat_Counter,
    pub conflict_count: [PgStat_Counter; CONFLICT_NUM_TYPES],
    pub stat_reset_timestamp: TimestampTz,
}

#[derive(Clone, Copy, Default, Debug)]
pub struct PgStat_BackendSubEntry {
    pub apply_error_count: PgStat_Counter,
    pub sync_error_count: PgStat_Counter,
    pub conflict_count: [PgStat_Counter; CONFLICT_NUM_TYPES],
}

fn sub_key(subid: Oid) -> PgStat_HashKey {
    PgStat_HashKey {
        kind: PGSTAT_KIND_SUBSCRIPTION,
        dboid: InvalidOid,
        objid: subid as u64,
    }
}

fn with_pending(subid: Oid, f: impl FnOnce(&mut PgStat_BackendSubEntry)) {
    crate::pending::with_state(|st| {
        let PendingData::Subscription(pending) = st.prep_pending_entry(sub_key(subid)) else {
            unreachable!("subscription key holds non-subscription pending entry")
        };
        f(pending);
    });
}

pub fn pgstat_report_subscription_error(subid: Oid, is_apply_error: bool) {
    with_pending(subid, |p| {
        if is_apply_error {
            p.apply_error_count += 1;
        } else {
            p.sync_error_count += 1;
        }
    });
}

pub fn pgstat_report_subscription_conflict(subid: Oid, conflict_type: usize) {
    with_pending(subid, |p| p.conflict_count[conflict_type] += 1);
}

pub fn pgstat_create_subscription(subid: Oid) {
    // Ensures that stats are dropped if the transaction rolls back.
    crate::xact::pgstat_create_transactional(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid as u64);
    // C: get_entry_ref(create) + pgstat_reset_entry(ts = 0) = a zeroed entry.
    crate::shmem::update_subscription_entry(sub_key(subid), |e| *e = Default::default());
}

pub fn pgstat_drop_subscription(subid: Oid) {
    crate::xact::pgstat_drop_transactional(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid as u64);
}

pub fn pgstat_fetch_stat_subscription(subid: Oid) -> Option<PgStat_StatSubEntry> {
    match crate::shmem::fetch_entry(sub_key(subid)) {
        Some(crate::shmem::SharedEntry::Subscription(e)) => Some(e),
        _ => None,
    }
}
