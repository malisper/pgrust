use elog::ereport;
use mcx::MemoryContext;
use shm_mq::{ShmMqHandle, ShmMqRecv, ShmMqResult};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERROR,
};
use types_slot::SlotData;

pub const PARALLEL_TUPLE_QUEUE_SIZE: usize = 65536;

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("tqueue.c", 0, funcname)
}

pub struct DrTqueue {
    queue: Option<ShmMqHandle>,
    scratch: Option<MemoryContext>,
}

/// `CreateTupleQueueDestReceiver` (tqueue.c).
pub fn tqueue_create_DR(queue: ShmMqHandle) -> DrTqueue {
    DrTqueue { queue: Some(queue), scratch: None }
}

impl DrTqueue {
    pub fn startup(&mut self, _operation: i32, _typeinfo: &types_tuple::TupleDescData<'_>) {}

    /// `tqueueReceiveSlot`: false = queue detached, stop early.
    pub fn receive_slot(&mut self, slot: &mut SlotData<'_>) -> PgResult<bool> {
        let queue = self.queue.as_mut().expect("tqueueReceiveSlot after shutdown");
        // ExecFetchSlotMinimalTuple's no-copy arm.
        if let SlotData::Minimal(m) = &*slot {
            if let Some(p) = m.mintuple {
                // SAFETY: a stored minimal tuple is a live flat image of t_len bytes.
                let bytes = unsafe {
                    let t_len = p.as_ref().t_len as usize;
                    core::slice::from_raw_parts(p.as_ptr().cast::<u8>(), t_len)
                };
                return tqueue_send_bytes(queue, bytes);
            }
        }
        exectuples::slot_getallattrs(slot);
        let ctx = self.scratch.get_or_insert_with(|| MemoryContext::new_bump("tqueue"));
        let sent = {
            let mcx = ctx.mcx();
            let base = slot.base();
            let desc = base
                .tts_tupleDescriptor
                .as_ref()
                .expect("tqueueReceiveSlot: slot without descriptor");
            let natts = desc.natts as usize;
            let tup = heaptuple::heap_form_minimal_tuple(
                mcx,
                desc,
                &base.tts_values[..natts],
                &base.tts_isnull[..natts],
                0,
            )?;
            // SAFETY: a formed minimal tuple is a live flat image of t_len bytes.
            let bytes =
                unsafe { core::slice::from_raw_parts(tup.as_ptr(), tup.t_len() as usize) };
            tqueue_send_bytes(queue, bytes)?
        };
        ctx.reset();
        Ok(sent)
    }

    /// `tqueueShutdownReceiver`: detach from the queue.
    pub fn shutdown(&mut self) {
        if let Some(mut q) = self.queue.take() {
            q.detach();
        }
    }
}

// tqueueReceiveSlot's queue-facing core; false = queue detached.
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
