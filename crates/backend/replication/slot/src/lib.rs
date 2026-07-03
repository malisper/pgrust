#![allow(non_snake_case)]

use types_error::PgResult;

pub fn ReplicationSlotInitialize() -> PgResult<()> {
    ipc_seams::before_shmem_exit::call(ReplicationSlotShmemExit, datum::Datum::from_usize(0))
}

fn ReplicationSlotShmemExit(_code: i32, _arg: datum::Datum) -> PgResult<()> {
    Ok(())
}

pub fn init_seams() {
    slot_seams::replication_slot_initialize::set(ReplicationSlotInitialize);
}
