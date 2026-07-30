// trigfuncs.c: suppress_redundant_updates_trigger.
#![no_std]

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;

use datum::Datum;
use types_error::{PgError, PgResult, ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED, ERROR};
use types_fmgr::{FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};
use types_trigger::{
    TRIGGER_FIRED_BY_UPDATE, TRIGGER_FIRED_FOR_ROW, TRIGGER_EVENT_BEFORE, TRIGGER_EVENT_TIMINGMASK,
};
use types_trigger_call::trigger_data_from_fcinfo;
use types_tuple::{HeapTupleData, HEAP_XACT_MASK, SizeofHeapTupleHeader};

#[track_caller]
#[cold]
#[inline(never)]
fn protocol_err(msg: &str) -> Box<PgError> {
    Box::new(
        PgError::new(ERROR, format!("suppress_redundant_updates_trigger: {msg}"))
            .with_sqlstate(ERRCODE_E_R_I_E_TRIGGER_PROTOCOL_VIOLATED),
    )
}

pub fn fc_suppress_redundant_updates_trigger(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: the trigger call machinery keeps the TriggerData live for the
    // duration of the call.
    let Some(td) = (unsafe { trigger_data_from_fcinfo(fcinfo) }) else {
        return Err(protocol_err("must be called as trigger"));
    };
    if !TRIGGER_FIRED_BY_UPDATE(td.tg_event) {
        return Err(protocol_err("must be called on update"));
    }
    if td.tg_event & TRIGGER_EVENT_TIMINGMASK != TRIGGER_EVENT_BEFORE {
        return Err(protocol_err("must be called before update"));
    }
    if !TRIGGER_FIRED_FOR_ROW(td.tg_event) {
        return Err(protocol_err("must be called for each row"));
    }

    let newptr = td.tg_newtuple.expect("BEFORE UPDATE has a new tuple");
    let oldptr = td.tg_trigtuple.expect("BEFORE UPDATE has an old tuple");
    // SAFETY: live tuples per the trigger call contract.
    let (newtuple, oldtuple): (&HeapTupleData<'_>, &HeapTupleData<'_>) =
        unsafe { (newptr.as_ref(), oldptr.as_ref()) };

    let (nh, oh) = (newtuple.t_data(), oldtuple.t_data());
    let same = newtuple.t_len == oldtuple.t_len
        && nh.t_hoff == oh.t_hoff
        && nh.natts() == oh.natts()
        && (nh.t_infomask & !HEAP_XACT_MASK) == (oh.t_infomask & !HEAP_XACT_MASK)
        && {
            let n = newtuple.t_len as usize - SizeofHeapTupleHeader;
            // SAFETY: the image invariant guarantees t_len readable bytes
            // from the header, t_len >= SizeofHeapTupleHeader.
            unsafe {
                core::slice::from_raw_parts(newtuple.header_ptr().add(SizeofHeapTupleHeader), n)
                    == core::slice::from_raw_parts(
                        oldtuple.header_ptr().add(SizeofHeapTupleHeader),
                        n,
                    )
            }
        };

    if same {
        Ok(Datum::from_usize(0))
    } else {
        Ok(Datum::from_usize(newptr.as_ptr() as usize))
    }
}

const fn b(foid: types_core::Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

pub const TRIGFUNCS_BUILTINS: &[FmgrBuiltin] =
    &[b(1291, "suppress_redundant_updates_trigger", 0, fc_suppress_redundant_updates_trigger)];
