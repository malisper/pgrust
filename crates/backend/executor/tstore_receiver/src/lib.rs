// tstoreReceiver.c; the tupmap arm is a loud panic naming its lane.
#![allow(non_snake_case)]

use ::datum::Datum;
use ::mcx::MemoryContext;
use ::types_error::PgResult;
use ::types_portal::TuplestoreHandle;
use ::types_slot::SlotData;
use ::types_tuple::varatt::{
    varatt_is_1b, varatt_is_1b_e, varsize_1b, varsize_4b, vartag_size, VARHDRSZ_EXTERNAL,
};
use ::types_tuple::TupleDescData;

#[cfg(test)]
mod tests;

pub struct DrTstore {
    tstore: TuplestoreHandle,
    detoast: bool,
    needtoast: bool,
    scratch: Option<MemoryContext>,
    /// SE-R41 (notes/se-r41-retire.md §3.3): the §4.2 row-identity sidecar
    /// of a capture-batchable eligible cursor-store fill. Set ONLY by
    /// `fill_portal_store_to`'s capture-batch arm (knob-ON, store-armed
    /// portals); every other producer leaves it NULL. Carried on the
    /// receiver — its lifetime IS the fill call — so the run seam can arm
    /// per-accept capture without estate/TLS state.
    capture_sidecar: TuplestoreHandle,
}

pub fn tstore_create_DR() -> DrTstore {
    DrTstore {
        tstore: TuplestoreHandle::NULL,
        detoast: false,
        needtoast: false,
        scratch: None,
        capture_sidecar: TuplestoreHandle::NULL,
    }
}

// C's tContext lives inside the store behind the handle.
pub fn set_params(myState: &mut DrTstore, tstore: TuplestoreHandle, detoast: bool) {
    myState.tstore = tstore;
    myState.detoast = detoast;
}

/// SE-R41: arm/read the capture sidecar (see the field doc).
pub fn set_capture_sidecar(myState: &mut DrTstore, sidecar: TuplestoreHandle) {
    myState.capture_sidecar = sidecar;
}

pub fn capture_sidecar(myState: &DrTstore) -> Option<TuplestoreHandle> {
    if myState.capture_sidecar.is_null() {
        None
    } else {
        Some(myState.capture_sidecar)
    }
}

impl DrTstore {
    pub fn startup(&mut self, _operation: i32, typeinfo: &TupleDescData<'_>) -> PgResult<()> {
        let natts = typeinfo.natts as usize;
        self.needtoast = self.detoast
            && typeinfo.compact_attrs[..natts]
                .iter()
                .any(|attr| !attr.attisdropped && attr.attlen == -1);
        if self.needtoast && self.scratch.is_none() {
            self.scratch = Some(MemoryContext::new_bump("tstoreReceiver detoast"));
        }
        Ok(())
    }

    pub fn receive_slot(&mut self, slot: &mut SlotData<'_>) -> PgResult<bool> {
        if !self.needtoast {
            tuplestore::hold::puttupleslot(self.tstore, slot)?;
            return Ok(true);
        }
        exectuples::slot_getallattrs(slot);
        let ctx = self.scratch.as_mut().expect("startup ran before receive_slot");
        {
            let mcx = ctx.mcx();
            let base = slot.base();
            let desc = base
                .tts_tupleDescriptor
                .as_ref()
                .expect("tstoreReceiveSlot_detoast: slot without descriptor");
            let natts = desc.natts as usize;
            let mut outvalues = ::mcx::vec_with_capacity_in(mcx, natts)?;
            for i in 0..natts {
                let mut val = base.tts_values[i];
                let attr = &desc.compact_attrs[i];
                if !attr.attisdropped && attr.attlen == -1 && !base.tts_isnull[i] {
                    // SAFETY: non-null deformed varlena datum.
                    if unsafe { varatt_is_1b_e(val.as_usize() as *const u8) } {
                        // SAFETY: as above.
                        let flat = detoast::detoast_external_attr(mcx, unsafe { va_slice(val) })?;
                        val = Datum::from_usize(flat.leak().as_ptr() as usize);
                    }
                }
                outvalues.push(val);
            }
            tuplestore::hold::putvalues(self.tstore, desc, &outvalues, &base.tts_isnull[..natts])?;
        }
        ctx.reset();
        Ok(true)
    }

    pub fn shutdown(&mut self) {}
}

/// # Safety
/// `d` is a pointer datum to a live varlena.
unsafe fn va_slice<'a>(d: Datum) -> &'a [u8] {
    let p = d.as_usize() as *const u8;
    // SAFETY: caller contract.
    unsafe {
        let len = if varatt_is_1b_e(p) {
            VARHDRSZ_EXTERNAL + vartag_size(*p.add(1))
        } else if varatt_is_1b(p) {
            varsize_1b(p)
        } else {
            varsize_4b(p)
        };
        core::slice::from_raw_parts(p, len)
    }
}
