//! Port of `src/backend/utils/activity/pgstat_subscription.c` (PostgreSQL 18.3).
//!
//! Implementation of subscription statistics (`PGSTAT_KIND_SUBSCRIPTION`, a
//! variable-numbered stats kind that uses backend-local pending data). Kept
//! separate from `pgstat.c` to enforce the line between the statistics
//! access/storage implementation and the details of individual kinds.
//!
//! The kind's callbacks (`flush_pending_cb`, `reset_timestamp_cb`) are
//! registered into the pgstat core's per-kind table via [`KindInfoBuilder`] from
//! [`init_seams`]; the only outward seam with a live caller —
//! `pgstat_report_subscription_conflict` (from `conflict.c`) — is installed
//! there too. The remaining entry points (`pgstat_report_subscription_error`,
//! `pgstat_create_subscription`, `pgstat_drop_subscription`,
//! `pgstat_fetch_stat_subscription`) are ported as public functions; their
//! callers (worker.c / subscriptioncmds.c / the pg_stat_subscription_stats
//! views) are not yet ported, so they have no seam to install yet.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use ::activity_pgstat::entry_ref::PgStat_EntryRef;
use ::activity_pgstat::kind_info::KindInfoBuilder;
use ::activity_pgstat::pgstat_core;
use ::activity_pgstat::registry;
use ::activity_pgstat::shmem;
use activity_xact as xact;
use ::types_core::primitive::{InvalidOid, Oid};
use ::types_error::PgResult;
use ::replication::conflict::{ConflictType, CONFLICT_NUM_TYPES};
use ::types_pgstat::activity_pgstat::{
    PgStat_BackendSubEntry, PgStat_StatSubEntry, PGSTAT_KIND_SUBSCRIPTION,
};
use ::types_pgstat::pgstat_internal::{
    PgStat_KindInfo, PgStatShared_Common, PgStatShared_Subscription,
};

/// The backend-local pending block for a subscription entry: C allocates a
/// zeroed `PgStat_BackendSubEntry` (`pending_size`) when the entry has no
/// pending yet. Mirrors `pgstat_prep_pending_entry(..., NULL)`'s default
/// allocation.
fn new_pending_sub() -> Box<dyn core::any::Any> {
    Box::new(PgStat_BackendSubEntry::default())
}

// ---------------------------------------------------------------------------
// Report (error / conflict).
// ---------------------------------------------------------------------------

/// Port of `void pgstat_report_subscription_error(Oid subid, bool
/// is_apply_error)` — report a subscription error.
pub fn pgstat_report_subscription_error(subid: Oid, is_apply_error: bool) -> PgResult<()> {
    // entry_ref = pgstat_prep_pending_entry(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid, NULL);
    let entry_ref = pgstat_core::pgstat_prep_pending_entry(
        PGSTAT_KIND_SUBSCRIPTION,
        InvalidOid,
        subid as u64,
        new_pending_sub,
    )?;

    // pending = entry_ref->pending;
    // SAFETY: just-prepped live reference; its pending was just ensured present.
    let er = unsafe { entry_ref.get() };
    let pending = downcast_pending(er);

    if is_apply_error {
        pending.apply_error_count += 1;
    } else {
        pending.sync_error_count += 1;
    }
    Ok(())
}

/// Port of `void pgstat_report_subscription_conflict(Oid subid, ConflictType
/// type)` — report a subscription conflict.
pub fn pgstat_report_subscription_conflict(subid: Oid, type_: ConflictType) -> PgResult<()> {
    let entry_ref = pgstat_core::pgstat_prep_pending_entry(
        PGSTAT_KIND_SUBSCRIPTION,
        InvalidOid,
        subid as u64,
        new_pending_sub,
    )?;

    // SAFETY: just-prepped live reference.
    let er = unsafe { entry_ref.get() };
    let pending = downcast_pending(er);
    pending.conflict_count[type_ as usize] += 1;
    Ok(())
}

/// `entry_ref->pending` as the typed `PgStat_BackendSubEntry`.
fn downcast_pending(er: &mut PgStat_EntryRef) -> &mut PgStat_BackendSubEntry {
    er.pending
        .as_mut()
        .expect("subscription entry_ref has no pending after prep")
        .downcast_mut::<PgStat_BackendSubEntry>()
        .expect("subscription pending is not a PgStat_BackendSubEntry")
}

// ---------------------------------------------------------------------------
// Create / drop.
// ---------------------------------------------------------------------------

/// Port of `void pgstat_create_subscription(Oid subid)` — report creating the
/// subscription.
pub fn pgstat_create_subscription(subid: Oid) -> PgResult<()> {
    // Ensures that stats are dropped if transaction rolls back.
    xact::pgstat_create_transactional(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid as u64)?;

    // Create and initialize the subscription stats entry.
    shmem::pgstat_get_entry_ref(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid as u64, true, None)?;
    pgstat_core::pgstat_reset_entry(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid as u64, 0)?;
    Ok(())
}

/// Port of `void pgstat_drop_subscription(Oid subid)` — report dropping the
/// subscription. Ensures that stats are dropped if the transaction commits.
pub fn pgstat_drop_subscription(subid: Oid) -> PgResult<()> {
    xact::pgstat_drop_transactional(PGSTAT_KIND_SUBSCRIPTION, InvalidOid, subid as u64)
}

// ---------------------------------------------------------------------------
// Fetch.
// ---------------------------------------------------------------------------

/// Port of `PgStat_StatSubEntry *pgstat_fetch_stat_subscription(Oid subid)` —
/// the collected statistics for one subscription, or `None`.
///
/// The variable-numbered fetch (`pgstat_fetch_entry`) bottoms out on the
/// not-yet-ported variable-snapshot/cache subsystem in `pgstat.c`; the seam it
/// calls panics until that lands.
pub fn pgstat_fetch_stat_subscription(subid: Oid) -> PgResult<Option<PgStat_StatSubEntry>> {
    let bytes = pgstat_seams::pgstat_fetch_entry::call(
        PGSTAT_KIND_SUBSCRIPTION,
        InvalidOid,
        subid as u64,
    )?;
    Ok(bytes.map(|b| decode_sub_entry(&b)))
}

/// Decode the `shared_data_len` bytes `pgstat_fetch_entry` copies out into the
/// typed `PgStat_StatSubEntry` (C's `(PgStat_StatSubEntry *) ...`).
fn decode_sub_entry(bytes: &[u8]) -> PgStat_StatSubEntry {
    assert_eq!(
        bytes.len(),
        core::mem::size_of::<PgStat_StatSubEntry>(),
        "pgstat_fetch_stat_subscription: unexpected stats blob size"
    );
    // SAFETY: the blob is exactly a `PgStat_StatSubEntry` (a Copy, pointer-free
    // POD), copied byte-for-byte by pgstat_fetch_entry.
    unsafe { core::ptr::read_unaligned(bytes.as_ptr() as *const PgStat_StatSubEntry) }
}

// ---------------------------------------------------------------------------
// Callbacks.
// ---------------------------------------------------------------------------

/// Port of `bool pgstat_subscription_flush_cb(PgStat_EntryRef *entry_ref, bool
/// nowait)` — flush out pending stats for the entry.
///
/// Returns `Ok(false)` (C `false`) if `nowait` and the lock could not be
/// acquired (no flush); `Ok(true)` (C `true`) otherwise.
pub fn pgstat_subscription_flush_cb(entry_ref: &mut PgStat_EntryRef, nowait: bool) -> PgResult<bool> {
    // localent = (PgStat_BackendSubEntry *) entry_ref->pending; (non-zero content)
    let localent: PgStat_BackendSubEntry = *entry_ref
        .pending
        .as_ref()
        .expect("subscription flush: entry_ref has no pending")
        .downcast_ref::<PgStat_BackendSubEntry>()
        .expect("subscription pending is not a PgStat_BackendSubEntry");

    // if (!pgstat_lock_entry(entry_ref, nowait)) return false;
    if !shmem::pgstat_lock_entry(entry_ref, nowait)? {
        return Ok(false);
    }

    // shsubent = (PgStatShared_Subscription *) entry_ref->shared_stats;
    // SAFETY: shared_stats points at a live PgStatShared_Subscription (header
    // first); the content lock is now held.
    let shsubent = unsafe { &mut *(entry_ref.shared_stats as *mut PgStatShared_Subscription) };

    // SUB_ACC(fld): shsubent->stats.fld += localent->fld
    shsubent.stats.apply_error_count += localent.apply_error_count;
    shsubent.stats.sync_error_count += localent.sync_error_count;
    for i in 0..CONFLICT_NUM_TYPES {
        shsubent.stats.conflict_count[i] += localent.conflict_count[i];
    }

    shmem::pgstat_unlock_entry(entry_ref)?;
    Ok(true)
}

/// Port of `void pgstat_subscription_reset_timestamp_cb(PgStatShared_Common
/// *header, TimestampTz ts)`.
fn pgstat_subscription_reset_timestamp_cb(
    header: &mut PgStatShared_Common,
    ts: ::types_core::TimestampTz,
) {
    // ((PgStatShared_Subscription *) header)->stats.stat_reset_timestamp = ts;
    // SAFETY: the kind table only hands this cb the PgStatShared_Common embedded
    // as the first field of a PgStatShared_Subscription.
    let shsub =
        unsafe { &mut *((header as *mut PgStatShared_Common) as *mut PgStatShared_Subscription) };
    shsub.stats.stat_reset_timestamp = ts;
}

// ---------------------------------------------------------------------------
// Kind registration + seam installation.
// ---------------------------------------------------------------------------

/// The `PgStat_KindInfo` metadata for `PGSTAT_KIND_SUBSCRIPTION`
/// (`pgstat.c:pgstat_kind_builtin_infos[PGSTAT_KIND_SUBSCRIPTION]`).
fn subscription_kind_info() -> PgStat_KindInfo {
    PgStat_KindInfo {
        fixed_amount: false,
        // so pg_stat_subscription_stats entries can be seen in all databases
        accessed_across_databases: true,
        write_to_file: true,
        shared_size: core::mem::size_of::<PgStatShared_Subscription>() as u32,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        shared_data_off: core::mem::offset_of!(PgStatShared_Subscription, stats) as u32,
        shared_data_len: core::mem::size_of::<PgStat_StatSubEntry>() as u32,
        pending_size: core::mem::size_of::<PgStat_BackendSubEntry>() as u32,
        name: "subscription",
    }
}

/// Register `PGSTAT_KIND_SUBSCRIPTION` and install the subscription outward
/// seam.
///
/// Must run before `::activity_pgstat::init_seams()` seals the
/// per-kind table.
pub fn init_seams() {
    registry::register(
        KindInfoBuilder::new(PGSTAT_KIND_SUBSCRIPTION, subscription_kind_info())
            .flush_pending_cb(pgstat_subscription_flush_cb)
            .reset_timestamp_cb(pgstat_subscription_reset_timestamp_cb)
            // On-disk (de)serialization of the `PgStat_StatSubEntry` body.
            .read_var_cb(|header, bytes| {
                // SAFETY: header points at a live PgStatShared_Subscription body.
                let sh = unsafe { &mut *(header as *mut PgStatShared_Subscription) };
                sh.stats = ::activity_pgstat::kind_info::pgstat_deserialize_pod::<
                    PgStat_StatSubEntry,
                >(bytes);
                Ok(())
            })
            .write_var_cb(|header| {
                // SAFETY: header points at a live PgStatShared_Subscription body.
                let sh = unsafe { &*(header as *const PgStatShared_Subscription) };
                ::activity_pgstat::kind_info::pgstat_serialize_pod(&sh.stats)
            }),
    );

    // The one outward seam with a live caller (conflict.c).
    stat_seams::pgstat_report_subscription_conflict::set(
        pgstat_report_subscription_conflict,
    );
}
