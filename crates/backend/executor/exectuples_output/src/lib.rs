// execTuples.c tuple-output surface (TupOutputState); lives outside
// exectuples because DestReceiver's crate depends on exectuples via printtup.
#![allow(non_snake_case)]

use std::rc::Rc;

use datum::Datum;
use mcx::Mcx;
use tcop_dest::DestReceiver;
use types_error::PgResult;
use types_nodes::nodes_enums::CmdType;
use types_slot::{SlotData, TupleSlotKind};
use types_tuple::TupleDescData;

#[cfg(test)]
mod tests;

pub struct TupOutputState<'d, 'mcx> {
    slot: SlotData<'mcx>,
    dest: &'d mut DestReceiver<'mcx>,
}

pub fn begin_tup_output_tupdesc<'d, 'mcx>(
    mcx: Mcx<'mcx>,
    dest: &'d mut DestReceiver<'mcx>,
    tupdesc: Rc<TupleDescData<'mcx>>,
) -> PgResult<TupOutputState<'d, 'mcx>> {
    let slot =
        exectuples::make_tuple_table_slot(mcx, TupleSlotKind::Virtual, Some(tupdesc.clone()));
    dest.startup(CmdType::CMD_SELECT as i32, &tupdesc)?;
    Ok(TupOutputState { slot, dest })
}

pub fn do_tup_output<'mcx>(
    tstate: &mut TupOutputState<'_, 'mcx>,
    mcx: Mcx<'mcx>,
    values: &[Datum],
    isnull: &[bool],
) -> PgResult<()> {
    let natts = tstate
        .slot
        .base()
        .tts_tupleDescriptor
        .as_ref()
        .expect("do_tup_output slot has a descriptor")
        .natts as usize;
    debug_assert!(values.len() == natts && isnull.len() == natts);
    exectuples::exec_clear_tuple(&mut tstate.slot, mcx);
    let base = tstate.slot.base_mut();
    base.tts_values[..natts].copy_from_slice(values);
    base.tts_isnull[..natts].copy_from_slice(isnull);
    exectuples::exec_store_virtual_tuple(&mut tstate.slot);
    tstate.dest.receive_slot(&mut tstate.slot)?;
    exectuples::exec_clear_tuple(&mut tstate.slot, mcx);
    Ok(())
}

pub fn do_text_output_multiline<'mcx>(
    tstate: &mut TupOutputState<'_, 'mcx>,
    mcx: Mcx<'mcx>,
    txt: &str,
) -> PgResult<()> {
    let mut rest = txt;
    while !rest.is_empty() {
        let (line, tail) = match rest.find('\n') {
            Some(i) => (&rest[..i], &rest[i + 1..]),
            None => (rest, ""),
        };
        do_text_output_line(tstate, mcx, line)?;
        rest = tail;
    }
    Ok(())
}

// executor.h do_text_output_oneline.
pub fn do_text_output_oneline<'mcx>(
    tstate: &mut TupOutputState<'_, 'mcx>,
    mcx: Mcx<'mcx>,
    text_to_emit: &str,
) -> PgResult<()> {
    do_text_output_line(tstate, mcx, text_to_emit)
}

fn do_text_output_line<'mcx>(
    tstate: &mut TupOutputState<'_, 'mcx>,
    mcx: Mcx<'mcx>,
    line: &str,
) -> PgResult<()> {
    let v = varlena::cstring_to_text(mcx, line.as_bytes())?;
    let values = [Datum::from_usize(v.as_bytes().as_ptr() as usize)];
    do_tup_output(tstate, mcx, &values, &[false])?;
    drop(v);
    Ok(())
}

pub fn end_tup_output(tstate: TupOutputState<'_, '_>) -> PgResult<()> {
    let TupOutputState { slot, dest } = tstate;
    dest.shutdown()?;
    drop(slot);
    Ok(())
}
