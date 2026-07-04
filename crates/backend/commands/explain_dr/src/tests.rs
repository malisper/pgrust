use super::*;
use ::datum::varlena::set_varsize_4b;
use ::datum::Datum;
use ::mcx::MemoryContext;
use ::types_core::Oid;
use ::types_fmgr::FunctionCallInfoBaseData;
use ::types_slot::{TupleSlotKind, TupleTableSlot, VirtualTupleTableSlot};
use ::types_tuple::{CompactAttribute, FormData_pg_attribute, NameData, TYPALIGN_INT};
use std::cell::Cell;
use std::rc::Rc;
use std::sync::Once;

const INT4OUT: Oid = 43;
const INT4SEND: Oid = 2407;
const INT4OID: Oid = 23;

thread_local! {
    static FMGR_INFO_CALLS: Cell<u32> = const { Cell::new(0) };
}

fn int4out_fn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let s = format!("{}\0", fcinfo.arg(0).as_i32());
    Ok(Datum::from_usize(
        Box::leak(s.into_boxed_str()).as_ptr() as usize
    ))
}

fn int4send_fn(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut FunctionCallInfoBaseData,
) -> PgResult<Datum> {
    let mut img = Vec::with_capacity(8);
    img.extend_from_slice(&set_varsize_4b(8));
    img.extend_from_slice(&fcinfo.arg(0).as_i32().to_be_bytes());
    Ok(Datum::from_usize(
        Box::leak(img.into_boxed_slice()).as_ptr() as usize
    ))
}

fn install_fixtures() {
    static INSTALL: Once = Once::new();
    INSTALL.call_once(|| {
        mbutils_seams::pg_server_to_client::set(|_mcx, _s| Ok(None));
        lsyscache_seams::get_type_output_info::set(|oid| match oid {
            INT4OID => Ok((INT4OUT, false)),
            _ => panic!("get_type_output_info: unexpected oid {oid}"),
        });
        lsyscache_seams::get_type_binary_output_info::set(|oid| match oid {
            INT4OID => Ok((INT4SEND, false)),
            _ => panic!("get_type_binary_output_info: unexpected oid {oid}"),
        });
        fmgr_seams::fmgr_info::set(|oid| {
            FMGR_INFO_CALLS.with(|c| c.set(c.get() + 1));
            match oid {
                INT4OUT => Ok(FmgrInfo::new(int4out_fn, INT4OUT, 1, true, false)),
                INT4SEND => Ok(FmgrInfo::new(int4send_fn, INT4SEND, 1, true, false)),
                _ => panic!("fmgr_info: unexpected oid {oid}"),
            }
        });
    });
}

fn setup() -> MemoryContext {
    install_fixtures();
    FMGR_INFO_CALLS.with(|c| c.set(0));
    MemoryContext::new("explain-dr-test")
}

fn int4_attr(i: i16) -> FormData_pg_attribute {
    let mut attname = NameData::default();
    attname.namestrcpy(&format!("c{}", i + 1));
    FormData_pg_attribute {
        attname,
        attnum: i + 1,
        atttypid: INT4OID,
        atttypmod: -1,
        attlen: 4,
        attbyval: true,
        attalign: TYPALIGN_INT,
        ..Default::default()
    }
}

fn int4_desc(mcx: Mcx<'_>, n: i16) -> Rc<TupleDescData<'_>> {
    let mut attrs = mcx::PgVec::new_in(mcx);
    let mut compact = mcx::PgVec::new_in(mcx);
    for att in (0..n).map(int4_attr) {
        compact.push(CompactAttribute::populate_from(&att));
        attrs.push(att);
    }
    Rc::new(TupleDescData {
        natts: n as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: -1,
        constr: None,
        compact_attrs: compact,
        attrs,
    })
}

fn make_slot<'mcx>(
    mcx: Mcx<'mcx>,
    desc: Rc<TupleDescData<'mcx>>,
    values: &[(Datum, bool)],
) -> SlotData<'mcx> {
    let mut base = TupleTableSlot::new_in(mcx, TupleSlotKind::Virtual);
    base.set_descriptor(mcx, desc);
    for (i, &(v, isnull)) in values.iter().enumerate() {
        base.tts_values[i] = v;
        base.tts_isnull[i] = isnull;
    }
    base.tts_nvalid = values.len() as i16;
    base.mark_not_empty();
    SlotData::Virtual(VirtualTupleTableSlot {
        base,
        data: mcx::PgVec::new_in(mcx),
    })
}

// DataRow payload: int16 natts + per column int32 len + bytes; text "1" = 7.
#[test]
fn text_rows_counted_not_sent() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let desc = int4_desc(mcx, 1);
    let mut dr = CreateExplainSerializeDestReceiver(mcx, false, false, false);
    dr.startup(0, &desc).unwrap();

    let mut slot = make_slot(mcx, desc.clone(), &[(Datum::from_i32(1), false)]);
    assert!(dr.receive_slot(&mut slot).unwrap());
    assert_eq!(dr.metrics.bytesSent, 7);

    let mut slot = make_slot(mcx, desc.clone(), &[(Datum::from_i32(42), false)]);
    assert!(dr.receive_slot(&mut slot).unwrap());
    assert_eq!(dr.metrics.bytesSent, 7 + 8);

    let mut slot = make_slot(mcx, desc, &[(Datum::from_i32(0), true)]);
    assert!(dr.receive_slot(&mut slot).unwrap());
    assert_eq!(dr.metrics.bytesSent, 7 + 8 + 6);

    // One descriptor, one fmgr resolution.
    assert_eq!(FMGR_INFO_CALLS.with(|c| c.get()), 1);
    dr.shutdown();
}

#[test]
fn binary_rows_counted() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let desc = int4_desc(mcx, 2);
    let mut dr = CreateExplainSerializeDestReceiver(mcx, true, false, false);
    dr.startup(0, &desc).unwrap();

    let mut slot = make_slot(
        mcx,
        desc,
        &[(Datum::from_i32(7), false), (Datum::from_i32(9), false)],
    );
    assert!(dr.receive_slot(&mut slot).unwrap());
    // 2 + 2 * (4 + 4)
    assert_eq!(dr.metrics.bytesSent, 18);
}

#[test]
fn descriptor_change_reprepares() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let desc1 = int4_desc(mcx, 1);
    let desc2 = int4_desc(mcx, 1);
    let mut dr = CreateExplainSerializeDestReceiver(mcx, false, false, false);
    dr.startup(0, &desc1).unwrap();

    let mut slot = make_slot(mcx, desc1, &[(Datum::from_i32(1), false)]);
    dr.receive_slot(&mut slot).unwrap();
    let mut slot = make_slot(mcx, desc2, &[(Datum::from_i32(1), false)]);
    dr.receive_slot(&mut slot).unwrap();
    assert_eq!(FMGR_INFO_CALLS.with(|c| c.get()), 2);
}

#[test]
fn timing_accumulates() {
    let ctx = setup();
    let mcx = ctx.mcx();
    let desc = int4_desc(mcx, 1);
    let mut dr = CreateExplainSerializeDestReceiver(mcx, false, true, false);
    dr.startup(0, &desc).unwrap();
    let mut slot = make_slot(mcx, desc, &[(Datum::from_i32(1), false)]);
    dr.receive_slot(&mut slot).unwrap();
    assert!(!dr.metrics.timeSpent.is_zero());
}
