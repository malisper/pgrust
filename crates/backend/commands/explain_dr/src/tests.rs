//! Tests for the `explain_dr.c` port. The genuine externals (catalog
//! type-output lookups + fmgr lookup, the executor slot deconstruction, and the
//! fmgr output-function calls) are reached through the real per-owner `-seams`
//! crates, exactly as production does. For the unit tests we install those
//! seams once (process-wide, guarded by a [`Once`]) with deterministic test
//! implementations that read a per-thread [`Script`]; each test sets its own
//! script before driving the receive path. The timing clock and `pgBufferUsage`
//! are read directly from the real `portability-instr-time` /
//! `backend-executor-instrument` crates (not scriptable), so the
//! timing/buffers test only asserts that the measured path runs and produces
//! non-negative accumulations.
//!
//! The text-send path (`pq_sendcountedtext`) routes through pqformat's
//! `pg_server_to_client` mbutils seam (a cross-crate concern installed by
//! `mbutils`), so the row-encoding assertions exercise the binary path, which
//! drives the identical control flow without that dependency.

use super::*;
use core::cell::RefCell;
use std::sync::Once;

use mcx::{MemoryContext, PgVec};
use ::types_core::Oid;
use ::types_tuple::heaptuple::Datum as TupleDatum;
use ::types_tuple::heaptuple::{CompactAttribute, FormData_pg_attribute, TupleDescData};

// --- per-thread script the installed test seams read --------------------------

#[derive(Default, Clone)]
struct Script {
    is_null: Vec<bool>,
    binary_out: Vec<u8>,
}

thread_local! {
    static SCRIPT: RefCell<Script> = RefCell::new(Script::default());
}

fn set_script(s: Script) {
    SCRIPT.with(|c| *c.borrow_mut() = s);
}

static INSTALL: Once = Once::new();

/// Install the outward seams this crate now calls with deterministic test
/// bodies. Process-wide (the seam slots are `OnceLock`s); the per-test variation
/// rides on the thread-local [`Script`].
fn install_test_seams() {
    INSTALL.call_once(|| {
        // getTypeOutputInfo / getTypeBinaryOutputInfo: echo the type oid as the
        // resolved function oid, non-varlena.
        lsyscache_s::get_type_output_info::set(|typid| Ok((typid, false)));
        lsyscache_s::get_type_binary_output_info::set(|typid| Ok((typid, false)));
        // fmgr_info lookup half: always resolves.
        fmgr_s::fmgr_info_check::set(|_oid| Ok(()));
        // OutputFunctionCall / SendFunctionCall: return the scripted bytes.
        fmgr_s::output_function_call::set(|mcx, _finfo, _val| {
            let bytes = SCRIPT.with(|c| c.borrow().binary_out.clone());
            let mut v = PgVec::new_in(mcx);
            v.extend_from_slice(&bytes);
            Ok(v)
        });
        fmgr_s::send_function_call::set(|mcx, _finfo, _val| {
            let bytes = SCRIPT.with(|c| c.borrow().binary_out.clone());
            let mut v = PgVec::new_in(mcx);
            v.extend_from_slice(&bytes);
            Ok(v)
        });
        // slot_getallattrs: hand back one (value, isnull) per scripted column.
        exectuples_s::slot_getallattrs::set(|mcx, _slot| {
            let is_null = SCRIPT.with(|c| c.borrow().is_null.clone());
            let mut cols = PgVec::new_in(mcx);
            for n in is_null {
                cols.push((TupleDatum::ByVal(1), n));
            }
            Ok(cols)
        });
    });
}

// --- helpers -----------------------------------------------------------------

fn make_slot<'mcx>(mcx: Mcx<'mcx>) -> SlotData<'mcx> {
    SlotData::Virtual(nodes::tuptable::VirtualTupleTableSlot {
        base: types_slot::TupleTableSlot::new_in(mcx),
        data: PgVec::new_in(mcx),
    })
}

fn make_tupdesc<'mcx>(mcx: Mcx<'mcx>, typoids: &[Oid]) -> TupleDescData<'mcx> {
    let mut attrs = PgVec::new_in(mcx);
    let mut compact = PgVec::new_in(mcx);
    for (i, &oid) in typoids.iter().enumerate() {
        let mut a = FormData_pg_attribute {
            atttypid: oid,
            attnum: (i + 1) as i16,
            ..FormData_pg_attribute::default()
        };
        a.attlen = -1;
        attrs.push(a);
        compact.push(CompactAttribute {
            attlen: a.attlen,
            attbyval: a.attbyval,
            ..CompactAttribute::default()
        });
    }
    TupleDescData {
        natts: typoids.len() as i32,
        tdtypeid: 0,
        tdtypmod: -1,
        tdrefcount: 0,
        constr: None,
        compact_attrs: compact,
        attrs,
    }
}

fn make_es<'mcx>(
    mcx: Mcx<'mcx>,
    serialize: ExplainSerializeOption,
    timing: bool,
    buffers: bool,
) -> ExplainState<'mcx> {
    let mut es = ExplainState::new_in(mcx);
    es.serialize = serialize;
    es.timing = timing;
    es.buffers = buffers;
    es
}

// --- tests -------------------------------------------------------------------

#[test]
fn create_records_mydest() {
    let cx = MemoryContext::new("t");
    let es = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT,
        false,
        false,
    );
    let r = CreateExplainSerializeDestReceiver(&es);
    assert_eq!(r.mydest, CommandDest::ExplainSerialize);
    assert_eq!(r.format, 0);
}

#[test]
fn startup_selects_format_and_zeroes_metrics() {
    let cx = MemoryContext::new("t");

    let es_text = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT,
        false,
        false,
    );
    let mut r = CreateExplainSerializeDestReceiver(&es_text);
    r.metrics.bytesSent = 999; // dirty it
    let _ = serializeAnalyzeStartup(&mut r, cx.mcx(), 0, &make_tupdesc(cx.mcx(), &[])).unwrap();
    assert_eq!(r.format, 0);
    assert_eq!(r.metrics, SerializeMetrics::default());

    let es_bin = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY,
        false,
        false,
    );
    let mut r2 = CreateExplainSerializeDestReceiver(&es_bin);
    let _ = serializeAnalyzeStartup(&mut r2, cx.mcx(), 0, &make_tupdesc(cx.mcx(), &[])).unwrap();
    assert_eq!(r2.format, 1);

    // Shutdown clears finfos / attrinfo / nattrs.
    r2.finfos.push(FmgrInfo::default());
    r2.nattrs = 3;
    serializeAnalyzeShutdown(&mut r2).unwrap();
    assert!(r2.finfos.is_empty());
    assert_eq!(r2.nattrs, 0);
}

#[test]
fn binary_path_lengths_and_byte_count() {
    install_test_seams();
    set_script(Script {
        is_null: vec![false],
        binary_out: vec![0xde, 0xad, 0xbe, 0xef],
    });

    let cx = MemoryContext::new("t");
    let es = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY,
        false,
        false,
    );
    let mut receiver = CreateExplainSerializeDestReceiver(&es);

    let typeinfo = make_tupdesc(cx.mcx(), &[17]);
    let mut buf = serializeAnalyzeStartup(&mut receiver, cx.mcx(), 0, &typeinfo).unwrap();
    assert_eq!(receiver.format, 1);

    let mut slot = make_slot(cx.mcx());
    let ok =
        serializeAnalyzeReceive(&mut receiver, cx.mcx(), &mut buf, &typeinfo, &mut slot).unwrap();
    assert!(ok);

    // Binary DataRow body: int16 natts, int32 payload length, payload bytes.
    let mut expected: Vec<u8> = Vec::new();
    expected.extend_from_slice(&1u16.to_be_bytes());
    expected.extend_from_slice(&4u32.to_be_bytes());
    expected.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(buf.as_bytes(), &expected[..]);
    assert_eq!(receiver.metrics.bytesSent, expected.len() as u64);
}

#[test]
fn null_column_emits_minus_one_and_accumulates() {
    install_test_seams();
    set_script(Script {
        is_null: vec![false, true],
        binary_out: vec![0xaa],
    });

    let cx = MemoryContext::new("t");
    let es = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY,
        false,
        false,
    );
    let mut receiver = CreateExplainSerializeDestReceiver(&es);

    let typeinfo = make_tupdesc(cx.mcx(), &[17, 25]);
    let mut buf = serializeAnalyzeStartup(&mut receiver, cx.mcx(), 0, &typeinfo).unwrap();

    let mut slot = make_slot(cx.mcx());
    serializeAnalyzeReceive(&mut receiver, cx.mcx(), &mut buf, &typeinfo, &mut slot).unwrap();

    let mut expected: Vec<u8> = Vec::new();
    expected.extend_from_slice(&2u16.to_be_bytes()); // natts
    expected.extend_from_slice(&1u32.to_be_bytes()); // col0 len
    expected.push(0xaa); // col0 payload
    expected.extend_from_slice(&(-1i32 as u32).to_be_bytes()); // col1 NULL
    assert_eq!(buf.as_bytes(), &expected[..]);
    assert_eq!(receiver.metrics.bytesSent, expected.len() as u64);

    // A second row accumulates onto bytesSent.
    serializeAnalyzeReceive(&mut receiver, cx.mcx(), &mut buf, &typeinfo, &mut slot).unwrap();
    assert_eq!(receiver.metrics.bytesSent, (expected.len() * 2) as u64);
}

#[test]
fn timing_and_buffers_paths_run() {
    install_test_seams();
    set_script(Script {
        is_null: vec![false],
        binary_out: vec![0x07],
    });

    let cx = MemoryContext::new("t");
    let es = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY,
        true,
        true,
    );
    let mut receiver = CreateExplainSerializeDestReceiver(&es);

    let typeinfo = make_tupdesc(cx.mcx(), &[17]);
    let mut buf = serializeAnalyzeStartup(&mut receiver, cx.mcx(), 0, &typeinfo).unwrap();

    let mut slot = make_slot(cx.mcx());
    // With timing+buffers on, the measurement code reads the real clock and
    // pgBufferUsage twice each; the path must run and the accumulations stay
    // non-negative (a monotonic clock can give a 0-tick delta on a fast machine).
    serializeAnalyzeReceive(&mut receiver, cx.mcx(), &mut buf, &typeinfo, &mut slot).unwrap();
    assert!(receiver.metrics.timeSpent.ticks >= 0);
    // Binary body: int16 natts (2) + int32 len (4) + 1 payload byte = 7.
    assert_eq!(receiver.metrics.bytesSent, 7);
}

#[test]
fn unsupported_format_errors() {
    // The format-7 branch errors before touching any seam (the loop's
    // format dispatch hits its `else` arm first).
    let cx = MemoryContext::new("t");
    let es = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT,
        false,
        false,
    );
    let mut receiver = CreateExplainSerializeDestReceiver(&es);
    receiver.format = 7; // C `else` branch

    let typeinfo = make_tupdesc(cx.mcx(), &[23]);
    let err = serialize_prepare_info(&mut receiver, &typeinfo, 1).unwrap_err();
    assert!(err.message().contains("unsupported format code: 7"));
}

#[test]
fn get_serialization_metrics_handles_other_receiver() {
    let cx = MemoryContext::new("t");
    assert_eq!(GetSerializationMetrics(None), SerializeMetrics::default());

    let es = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT,
        false,
        false,
    );
    let mut receiver = CreateExplainSerializeDestReceiver(&es);
    receiver.metrics.bytesSent = 42;
    assert_eq!(GetSerializationMetrics(Some(&receiver)).bytesSent, 42);

    // A receiver whose mydest is not ExplainSerialize falls into the else branch.
    receiver.mydest = CommandDest::None;
    assert_eq!(
        GetSerializationMetrics(Some(&receiver)),
        SerializeMetrics::default()
    );
}
