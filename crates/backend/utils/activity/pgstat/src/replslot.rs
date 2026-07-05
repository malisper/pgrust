// pgstat_replslot.c read half; slot index resolution stays with the slot
// crate's callers (report/create/drop land with logical decoding).

use types_core::{InvalidOid, TimestampTz};

use crate::pending::{PgStat_HashKey, PGSTAT_KIND_REPLSLOT};
use crate::PgStat_Counter;

#[derive(Clone, Copy, Default, PartialEq, Debug)]
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

fn replslot_key(idx: i32) -> PgStat_HashKey {
    PgStat_HashKey { kind: PGSTAT_KIND_REPLSLOT, dboid: InvalidOid, objid: idx as u64 }
}

pub fn pgstat_fetch_replslot_by_index(idx: i32) -> Option<PgStat_StatReplSlotEntry> {
    match crate::shmem::fetch_entry(replslot_key(idx)) {
        Some(crate::shmem::SharedEntry::ReplSlot(e)) => Some(e),
        Some(_) => unreachable!("replslot key holds non-replslot shared entry"),
        None => None,
    }
}

pub fn pgstat_reset_replslot_by_index(idx: i32) {
    crate::shmem::pgstat_reset(PGSTAT_KIND_REPLSLOT, InvalidOid, idx as u64);
}
