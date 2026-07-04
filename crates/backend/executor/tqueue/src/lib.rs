use elog::ereport;
use shm_mq::{ShmMqHandle, ShmMqRecv, ShmMqResult};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};

pub const PARALLEL_TUPLE_QUEUE_SIZE: usize = 65536;

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("tqueue.c", 0, funcname)
}

// tqueueReceiveSlot's queue-facing core; phase 2 wires the DestReceiver and
// owns the slot -> MinimalTuple materialization. Returns false when the queue
// is detached.
pub fn tqueue_send_bytes(queue: &mut ShmMqHandle, tuple: &[u8]) -> PgResult<bool> {
    let result = queue.send(tuple, false, false)?;

    if result == ShmMqResult::Detached {
        return Ok(false);
    }
    if result != ShmMqResult::Success {
        ereport(ERROR)
            .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
            .errmsg("could not send tuple to shared-memory queue")
            .finish(loc("tqueueReceiveSlot"))?;
    }
    Ok(true)
}

pub struct TupleQueueReader {
    queue: ShmMqHandle,
}

impl TupleQueueReader {
    pub fn new(queue: ShmMqHandle) -> Self {
        Self { queue }
    }

    // TupleQueueReaderNext: the raw MinimalTuple byte image borrowed from the
    // queue, valid until the next call (phase 2 owns the MinimalTuple typing).
    // Detached => done = true and None; nowait with nothing ready => None.
    pub fn next(&mut self, nowait: bool, done: &mut bool) -> PgResult<Option<&[u8]>> {
        *done = false;
        match self.queue.receive(nowait)? {
            ShmMqRecv::Detached => {
                *done = true;
                Ok(None)
            }
            ShmMqRecv::WouldBlock => Ok(None),
            ShmMqRecv::Success(data) => Ok(Some(data)),
        }
    }
}

#[cfg(test)]
mod tests;
