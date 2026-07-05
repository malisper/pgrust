// pgstat_replslot.c. Slots have no oids: the slot index is the objid while
// running; the name is the (de-)serialization key (file.rs 'N' records), so
// stats for slots dropped while shut down are discarded at restore.

use types_core::{InvalidOid, TimestampTz};
use types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};

use crate::pending::{PgStat_HashKey, PGSTAT_KIND_REPLSLOT};
use crate::PgStat_Counter;

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
pub struct PgStat_StatReplSlotEntry {
    pub spill_txns: PgStat_Counter,
    pub spill_count: PgStat_Counter,
    pub spill_bytes: PgStat_Counter,
    pub stream_txns: PgStat_Counter,
    pub stream_count: PgStat_Counter,
    pub stream_bytes: PgStat_Counter,
    pub total_txns: PgStat_Counter,
    pub total_bytes: PgStat_Counter,
    pub stat_reset_timestamp: TimestampTz,
}

pub(crate) fn replslot_key(index: u64) -> PgStat_HashKey {
    PgStat_HashKey {
        kind: PGSTAT_KIND_REPLSLOT,
        dboid: InvalidOid,
        objid: index,
    }
}

pub fn pgstat_reset_replslot(name: &str) -> PgResult<()> {
    // C holds ReplicationSlotControlLock across lookup + reset; the lock
    // window narrows to the seam's lookup here (the store mutex serializes
    // the reset itself).
    let (index, is_logical) = slot_seams::named_replication_slot_info::call(name, true)?;
    if index < 0 {
        return Err(Box::new(
            PgError::error(format!("replication slot \"{name}\" does not exist"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
        ));
    }
    // Stats are only collected for logical slots.
    if is_logical {
        crate::shmem::pgstat_reset(PGSTAT_KIND_REPLSLOT, InvalidOid, index as u64);
    }
    Ok(())
}

// C takes the slot pointer; callers resolve ReplicationSlotIndex so this
// crate stays free of slot types (slot -> pgstat is the dependency edge).
pub fn pgstat_report_replslot(index: i32, rep: &PgStat_StatReplSlotEntry) {
    crate::shmem::update_replslot_entry(replslot_key(index as u64), |s| {
        s.spill_txns += rep.spill_txns;
        s.spill_count += rep.spill_count;
        s.spill_bytes += rep.spill_bytes;
        s.stream_txns += rep.stream_txns;
        s.stream_count += rep.stream_count;
        s.stream_bytes += rep.stream_bytes;
        s.total_txns += rep.total_txns;
        s.total_bytes += rep.total_bytes;
    });
}

pub fn pgstat_create_replslot(index: i32) {
    // Stats from an older slot can exist if we crashed after dropping it.
    crate::shmem::update_replslot_entry(replslot_key(index as u64), |s| *s = Default::default());
}

pub fn pgstat_acquire_replslot(index: i32) {
    crate::shmem::update_replslot_entry(replslot_key(index as u64), |_| {});
}

pub fn pgstat_drop_replslot(index: i32) {
    crate::shmem::drop_entry(replslot_key(index as u64));
}

pub fn pgstat_fetch_replslot(name: &str) -> PgResult<Option<PgStat_StatReplSlotEntry>> {
    let (index, _) = slot_seams::named_replication_slot_info::call(name, true)?;
    if index < 0 {
        return Ok(None);
    }
    Ok(
        match crate::shmem::fetch_entry(replslot_key(index as u64)) {
            Some(crate::shmem::SharedEntry::ReplSlot(e)) => Some(e),
            _ => None,
        },
    )
}
