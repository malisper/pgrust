//! Port of `src/backend/utils/activity/pgstat_replslot.c` (PostgreSQL 18.3).
//!
//! Implementation of replication-slot statistics (`PGSTAT_KIND_REPLSLOT`, a
//! variable-numbered stats kind). Kept separate from `pgstat.c` to enforce the
//! line between the statistics access/storage implementation and the details of
//! individual kinds.
//!
//! Replication-slot stats work a bit differently from other variable-numbered
//! stats. Slots have no OIDs (so they can exist on physical replicas); the slot
//! *array index* is used as the object id while running, and the slot *name* is
//! used when (de)serializing — after a restart the index can change, and slots
//! may have been dropped while shut down, which is why stats for slots that
//! cannot be found by name on startup are not restored.
//!
//! The kind's callbacks (`reset_timestamp_cb`, `to_serialized_name`,
//! `from_serialized_name`) are registered into the pgstat core's per-kind table
//! via [`KindInfoBuilder`] from [`init_seams`]; the slot.c → pgstat outward
//! seams (`pgstat_create_replslot` / `_acquire_replslot` / `_drop_replslot` /
//! `pgstat_report_replslot`) are installed there too.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use backend_replication_slot as slot;
use backend_utils_activity_pgstat::kind_info::KindInfoBuilder;
use backend_utils_activity_pgstat::registry;
use backend_utils_activity_pgstat::shmem;
use backend_utils_error::{ereport, elog};
use types_core::primitive::InvalidOid;
use types_error::{PgResult, ErrorLocation, ERROR, ERRCODE_INVALID_PARAMETER_VALUE};
use types_logical::ReorderBufferStats;
use types_pgstat::activity_pgstat::{PgStat_StatReplSlotEntry, PGSTAT_KIND_REPLSLOT};
use types_pgstat::pgstat_internal::{
    PgStat_HashKey, PgStat_KindInfo, PgStatShared_Common, PgStatShared_ReplSlot,
};
use types_tuple::heaptuple::NameData;

const REPLSLOT_C: &str = "pgstat_replslot.c";

fn loc(lineno: i32, funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new(REPLSLOT_C, lineno, funcname)
}

// ---------------------------------------------------------------------------
// Reset (SQL-callable pg_stat_reset_replication_slot).
// ---------------------------------------------------------------------------

/// Port of `void pgstat_reset_replslot(const char *name)` — reset counters for
/// a single replication slot.
///
/// Permission checking is managed through the normal GRANT system.
///
/// C holds `ReplicationSlotControlLock` (shared) across the lookup and the
/// `pgstat_reset`. The slot crate does not expose the bare control-lock
/// primitive, so the lookup uses [`slot::snapshot_slot_by_name`], which itself
/// takes/releases the control lock for the named-slot search (and snapshots the
/// slot's persistent data so the logical/physical test can be applied).
pub fn pgstat_reset_replslot(name: &str) -> PgResult<()> {
    // SearchNamedReplicationSlot(name, false) under ReplicationSlotControlLock.
    let snap = slot::snapshot_slot_by_name(name)?;

    let snap = match snap {
        Some(s) => s,
        None => {
            // ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            //         errmsg("replication slot \"%s\" does not exist", name))
            return ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!("replication slot \"{name}\" does not exist"))
                .finish(loc(56, "pgstat_reset_replslot"));
        }
    };

    // Reset stats if it is a logical slot. Nothing to do for physical slots as
    // we collect stats only for logical slots.
    if slot::snapshot_is_logical(&snap) {
        backend_utils_activity_pgstat::pgstat_core::pgstat_reset(
            PGSTAT_KIND_REPLSLOT,
            InvalidOid,
            snap.slotno as u64,
        )?;
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Report / create / acquire / drop (driven by slot.c via outward seams).
// ---------------------------------------------------------------------------

/// Port of `void pgstat_report_replslot(ReplicationSlot *slot, const
/// PgStat_StatReplSlotEntry *repSlotStat)` — report a slot's decoding stats.
///
/// The slot is identified to pgstat by its array index. The stat fields come
/// from the reorder buffer; the outward seam carries them as a
/// [`ReorderBufferStats`] (the same field set `logical.c`'s
/// `UpdateDecodingStats` copies into a `PgStat_StatReplSlotEntry`).
///
/// We can rely on the stats entry to exist and to belong to this slot: we only
/// get here if `pgstat_create_replslot` / `pgstat_acquire_replslot` already ran.
pub fn pgstat_report_replslot(slot_index: i32, rep_slot_stat: &PgStat_StatReplSlotEntry) -> PgResult<()> {
    // entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_REPLSLOT, InvalidOid,
    //                                         ReplicationSlotIndex(slot), false);
    let entry_ref = shmem::pgstat_get_entry_ref_locked(
        PGSTAT_KIND_REPLSLOT,
        InvalidOid,
        slot_index as u64,
        false,
    )?
    .expect("pgstat_report_replslot: stats entry for an acquired slot must exist");

    // SAFETY: just-resolved live, content-locked reference; its shared_stats
    // points at a live PgStatShared_ReplSlot (header first), so the cast and
    // the `&mut stats` are sound.
    let er = unsafe { entry_ref.get() };
    let statent = unsafe { &mut (*(er.shared_stats as *mut PgStatShared_ReplSlot)).stats };

    // Update the replication slot statistics (REPLSLOT_ACC).
    statent.spill_txns += rep_slot_stat.spill_txns;
    statent.spill_count += rep_slot_stat.spill_count;
    statent.spill_bytes += rep_slot_stat.spill_bytes;
    statent.stream_txns += rep_slot_stat.stream_txns;
    statent.stream_count += rep_slot_stat.stream_count;
    statent.stream_bytes += rep_slot_stat.stream_bytes;
    statent.total_txns += rep_slot_stat.total_txns;
    statent.total_bytes += rep_slot_stat.total_bytes;

    shmem::pgstat_unlock_entry(er)?;
    Ok(())
}

/// Map a reorder-buffer stats snapshot to a `PgStat_StatReplSlotEntry`, exactly
/// as `logical.c`'s `UpdateDecodingStats` does before calling
/// `pgstat_report_replslot`. `stat_reset_timestamp` is not set by the reporter
/// (it is owned by the reset path); it stays at its `Default` zero.
fn rep_slot_stat_from_reorder(s: &ReorderBufferStats) -> PgStat_StatReplSlotEntry {
    PgStat_StatReplSlotEntry {
        spill_txns: s.spill_txns,
        spill_count: s.spill_count,
        spill_bytes: s.spill_bytes,
        stream_txns: s.stream_txns,
        stream_count: s.stream_count,
        stream_bytes: s.stream_bytes,
        total_txns: s.total_txns,
        total_bytes: s.total_bytes,
        stat_reset_timestamp: 0,
    }
}

/// Port of `void pgstat_create_replslot(ReplicationSlot *slot)` — report
/// replication-slot creation.
///
/// NB: in C this is called with `ReplicationSlotAllocationLock` already held.
pub fn pgstat_create_replslot(slot_index: i32, _name: NameData) -> PgResult<()> {
    // entry_ref = pgstat_get_entry_ref_locked(PGSTAT_KIND_REPLSLOT, InvalidOid,
    //                                         ReplicationSlotIndex(slot), false);
    let entry_ref = shmem::pgstat_get_entry_ref_locked(
        PGSTAT_KIND_REPLSLOT,
        InvalidOid,
        slot_index as u64,
        false,
    )?
    .expect("pgstat_create_replslot: get_entry_ref_locked(create) returned None");

    // SAFETY: just-resolved live, content-locked reference.
    let er = unsafe { entry_ref.get() };

    // NB: need to accept that there might be stats from an older slot, e.g. if
    // we previously crashed after dropping a slot.
    // memset(&shstatent->stats, 0, sizeof(shstatent->stats));
    let shstatent = unsafe { &mut *(er.shared_stats as *mut PgStatShared_ReplSlot) };
    shstatent.stats = PgStat_StatReplSlotEntry::default();

    shmem::pgstat_unlock_entry(er)?;
    Ok(())
}

/// Port of `void pgstat_acquire_replslot(ReplicationSlot *slot)` — report a
/// replication slot has been acquired.
///
/// This guarantees a stats entry exists for later `pgstat_report_replslot`
/// calls.
pub fn pgstat_acquire_replslot(slot_index: i32) -> PgResult<()> {
    shmem::pgstat_get_entry_ref(
        PGSTAT_KIND_REPLSLOT,
        InvalidOid,
        slot_index as u64,
        true,
        None,
    )?;
    Ok(())
}

/// Port of `void pgstat_drop_replslot(ReplicationSlot *slot)` — report
/// replication-slot drop.
///
/// In C this is called with `ReplicationSlotAllocationLock` already held.
pub fn pgstat_drop_replslot(slot_index: i32) -> PgResult<()> {
    if !shmem::pgstat_drop_entry(PGSTAT_KIND_REPLSLOT, InvalidOid, slot_index as u64)? {
        shmem::pgstat_request_entry_refs_gc()?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Fetch (SQL-callable support).
// ---------------------------------------------------------------------------

/// Port of `PgStat_StatReplSlotEntry *pgstat_fetch_replslot(NameData slotname)`
/// — return the replication-slot statistics for a named slot, or `None`.
///
/// The variable-numbered fetch (`pgstat_fetch_entry`) bottoms out on the
/// not-yet-ported variable-snapshot/cache subsystem in `pgstat.c`; the seam it
/// calls panics until that lands. The lookup (`get_replslot_index`) is real.
pub fn pgstat_fetch_replslot(slotname: NameData) -> PgResult<Option<PgStat_StatReplSlotEntry>> {
    // LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);
    // idx = get_replslot_index(NameStr(slotname), false);
    let name = String::from_utf8_lossy(slotname.name_str()).into_owned();
    let idx = get_replslot_index(&name, true)?;

    if idx == -1 {
        return Ok(None);
    }

    // slotentry = (PgStat_StatReplSlotEntry *) pgstat_fetch_entry(...);
    let bytes = backend_utils_activity_pgstat_seams::pgstat_fetch_entry::call(
        PGSTAT_KIND_REPLSLOT,
        InvalidOid,
        idx as u64,
    )?;
    Ok(bytes.map(|b| decode_replslot_entry(&b)))
}

/// Decode the `shared_data_len` bytes `pgstat_fetch_entry` copies out into the
/// typed `PgStat_StatReplSlotEntry` (C's `(PgStat_StatReplSlotEntry *) ...`).
fn decode_replslot_entry(bytes: &[u8]) -> PgStat_StatReplSlotEntry {
    assert_eq!(
        bytes.len(),
        core::mem::size_of::<PgStat_StatReplSlotEntry>(),
        "pgstat_fetch_replslot: unexpected stats blob size"
    );
    // SAFETY: the blob is exactly a `PgStat_StatReplSlotEntry` (a Copy,
    // pointer-free POD), copied byte-for-byte by pgstat_fetch_entry.
    unsafe { core::ptr::read_unaligned(bytes.as_ptr() as *const PgStat_StatReplSlotEntry) }
}

// ---------------------------------------------------------------------------
// Serialized-name callbacks (on-disk (de)serialization).
// ---------------------------------------------------------------------------

/// Port of `void pgstat_replslot_to_serialized_name_cb(const PgStat_HashKey
/// *key, const PgStatShared_Common *header, NameData *name)`.
///
/// This is only called late during shutdown, when the set of existing slots is
/// not allowed to change, so a slot is assumed to exist at the offset.
fn pgstat_replslot_to_serialized_name_cb(key: &PgStat_HashKey, _header: &PgStatShared_Common) -> String {
    // if (!ReplicationSlotName(key->objid, name)) elog(ERROR, ...)
    match slot::ReplicationSlotName(key.objid as i32) {
        Ok((true, name)) => String::from_utf8_lossy(name.name_str()).into_owned(),
        _ => {
            // elog(ERROR, "could not find name for replication slot index ...").
            // The kind-table callback type is infallible (returns String); the
            // miss is asserted impossible by the C comment (called only at
            // shutdown when the slot set is frozen). Surface it loudly through
            // the elog path's panic-on-Err for fidelity.
            let _ = elog(
                ERROR,
                format!(
                    "could not find name for replication slot index {}",
                    key.objid
                ),
            );
            String::new()
        }
    }
}

/// Port of `bool pgstat_replslot_from_serialized_name_cb(const NameData *name,
/// PgStat_HashKey *key)` — parse a serialized name back into a hash key.
fn pgstat_replslot_from_serialized_name_cb(name: &str) -> Option<PgStat_HashKey> {
    // idx = get_replslot_index(NameStr(*name), true);
    let idx = match get_replslot_index(name, true) {
        Ok(i) => i,
        // The C cb is `bool`; a failed lookup is the documented false path.
        Err(_) => return None,
    };

    // slot might have been deleted
    if idx == -1 {
        return None;
    }

    Some(PgStat_HashKey {
        kind: PGSTAT_KIND_REPLSLOT,
        dboid: InvalidOid,
        objid: idx as u64,
    })
}

/// Port of `void pgstat_replslot_reset_timestamp_cb(PgStatShared_Common
/// *header, TimestampTz ts)`.
fn pgstat_replslot_reset_timestamp_cb(header: &mut PgStatShared_Common, ts: types_core::TimestampTz) {
    // ((PgStatShared_ReplSlot *) header)->stats.stat_reset_timestamp = ts;
    // SAFETY: the kind table only ever hands this cb a header that is the
    // PgStatShared_Common embedded as the first field of a PgStatShared_ReplSlot.
    let shslot = unsafe { &mut *((header as *mut PgStatShared_Common) as *mut PgStatShared_ReplSlot) };
    shslot.stats.stat_reset_timestamp = ts;
}

// ---------------------------------------------------------------------------
// Internal helpers.
// ---------------------------------------------------------------------------

/// Port of `static int get_replslot_index(const char *name, bool need_lock)` —
/// the slot array index for a named slot, or `-1` if not found.
fn get_replslot_index(name: &str, need_lock: bool) -> PgResult<i32> {
    // slot = SearchNamedReplicationSlot(name, need_lock);
    let slot = slot::SearchNamedReplicationSlot(name, need_lock)?;
    Ok(match slot {
        // return ReplicationSlotIndex(slot);
        Some(i) => slot::ReplicationSlotIndex(i),
        // if (!slot) return -1;
        None => -1,
    })
}

// ---------------------------------------------------------------------------
// Kind registration + seam installation.
// ---------------------------------------------------------------------------

/// The `PgStat_KindInfo` metadata for `PGSTAT_KIND_REPLSLOT`
/// (`pgstat.c:pgstat_kind_builtin_infos[PGSTAT_KIND_REPLSLOT]`).
///
/// The C byte offsets (`*_off`) are used only by the on-disk (de)serializer; the
/// idiomatic port dispatches the runtime callbacks via typed field projection
/// instead, so they are left 0 here.
fn replslot_kind_info() -> PgStat_KindInfo {
    PgStat_KindInfo {
        fixed_amount: false,
        // so pg_replication_slots stats can be seen in all databases
        accessed_across_databases: true,
        write_to_file: true,
        shared_size: core::mem::size_of::<PgStatShared_ReplSlot>() as u32,
        snapshot_ctl_off: 0,
        shared_ctl_off: 0,
        shared_data_off: 0,
        shared_data_len: core::mem::size_of::<PgStat_StatReplSlotEntry>() as u32,
        pending_size: 0,
        name: "replslot",
    }
}

/// Register `PGSTAT_KIND_REPLSLOT` and install the slot.c → pgstat outward
/// seams.
///
/// Must run before `backend_utils_activity_pgstat::init_seams()` seals the
/// per-kind table.
pub fn init_seams() {
    // Register the variable kind's callbacks in pgstat_kind_builtin_infos[].
    registry::register(
        KindInfoBuilder::new(PGSTAT_KIND_REPLSLOT, replslot_kind_info())
            .reset_timestamp_cb(pgstat_replslot_reset_timestamp_cb)
            .to_serialized_name(pgstat_replslot_to_serialized_name_cb)
            .from_serialized_name(pgstat_replslot_from_serialized_name_cb),
    );

    // slot.c → pgstat outward seams (keyed by ReplicationSlotIndex; slot.c holds
    // the relevant slot locks when it calls these).
    use backend_utils_activity_pgstat_replslot_seams as s;
    s::pgstat_create_replslot::set(|slot_index, name| {
        // void seam: a create error (OOM/LWLock) is fatal on the slot path; the
        // C callback is void, so surface the Err by panicking through expect.
        pgstat_create_replslot(slot_index, name)
            .expect("pgstat_create_replslot failed")
    });
    s::pgstat_acquire_replslot::set(|slot_index| {
        pgstat_acquire_replslot(slot_index).expect("pgstat_acquire_replslot failed")
    });
    s::pgstat_drop_replslot::set(|slot_index| {
        pgstat_drop_replslot(slot_index).expect("pgstat_drop_replslot failed")
    });
    s::pgstat_report_replslot::set(|slot_index, stats| {
        // The `UpdateDecodingStats` DEBUG2 trace + the rb->stats reset live in
        // logical.c; here we only map the carried reorder-buffer stats to the
        // PgStat_StatReplSlotEntry and accumulate. A report error is fatal;
        // surface it.
        let rep = rep_slot_stat_from_reorder(&stats);
        pgstat_report_replslot(slot_index, &rep).expect("pgstat_report_replslot failed")
    });
}
