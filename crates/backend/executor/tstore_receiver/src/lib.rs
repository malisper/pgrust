// tstoreReceiver.c; detoast + tupmap arms are loud panics naming their lanes.
#![allow(non_snake_case)]

use ::types_error::PgResult;
use ::types_portal::TuplestoreHandle;
use ::types_slot::SlotData;
use ::types_tuple::TupleDescData;

#[cfg(test)]
mod tests;

pub struct DrTstore {
    tstore: TuplestoreHandle,
    detoast: bool,
}

pub fn tstore_create_DR() -> DrTstore {
    DrTstore { tstore: TuplestoreHandle::NULL, detoast: false }
}

// C's tContext lives inside the store behind the handle.
pub fn set_params(myState: &mut DrTstore, tstore: TuplestoreHandle, detoast: bool) {
    myState.tstore = tstore;
    myState.detoast = detoast;
}

impl DrTstore {
    pub fn startup(&mut self, _operation: i32, typeinfo: &TupleDescData<'_>) -> PgResult<()> {
        if self.detoast {
            let natts = typeinfo.natts as usize;
            for attr in &typeinfo.compact_attrs[..natts] {
                if !attr.attisdropped && attr.attlen == -1 {
                    panic!(
                        "tstoreReceiveSlot_detoast: forced detoast \
                         (WITH HOLD cursor lane, tstoreReceiver.c) not ported"
                    );
                }
            }
        }
        Ok(())
    }

    pub fn receive_slot(&mut self, slot: &mut SlotData<'_>) -> PgResult<bool> {
        tuplestore::hold::puttupleslot(self.tstore, slot)?;
        Ok(true)
    }

    pub fn shutdown(&mut self) {}
}
