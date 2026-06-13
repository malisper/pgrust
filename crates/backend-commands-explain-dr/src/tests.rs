//! Tests for the `explain_dr.c` port. The genuine externals (catalog
//! type-output lookups + fmgr lookup, the executor slot accessors, the fmgr
//! output-function calls, the tmpcontext discipline, and the timing/buffer
//! instrumentation) are the stateful [`SerializeRuntime`] trait, scripted per
//! test via [`ScriptRuntime`].
//!
//! The text-send path (`pq_sendcountedtext`) routes through pqformat's
//! `pg_server_to_client` mbutils seam (a cross-crate concern installed by
//! `mbutils`), so the row-encoding assertions exercise the binary path, which
//! drives the identical control flow without that dependency.

use super::*;
use core::cell::RefCell;
use mcx::MemoryContext;
use types_core::Oid;
use types_tuple::heaptuple::{
    CompactAttribute, FormData_pg_attribute, TupleDescData,
};

// --- scripted runtime --------------------------------------------------------

#[derive(Default)]
struct Script {
    is_null: Vec<bool>,
    values: Vec<Datum>,
    binary_out: Vec<u8>,
    clock: Vec<i64>,
    clock_pos: usize,
    bufusage: Vec<BufferUsage>,
    bufusage_pos: usize,
    getallattrs_calls: u32,
    enter_calls: u32,
    exit_calls: u32,
    create_calls: u32,
    delete_calls: u32,
}

struct ScriptRuntime {
    s: RefCell<Script>,
}

impl ScriptRuntime {
    fn new(s: Script) -> Self {
        ScriptRuntime { s: RefCell::new(s) }
    }
}

fn finfo_for(oid: Oid) -> FmgrInfo {
    let mut f = FmgrInfo::empty();
    f.fn_oid = oid;
    f
}

impl SerializeRuntime for ScriptRuntime {
    fn prepare_text(&self, atttypid: Oid) -> PgResult<FmgrInfo> {
        Ok(finfo_for(atttypid))
    }
    fn prepare_binary(&self, atttypid: Oid) -> PgResult<FmgrInfo> {
        Ok(finfo_for(atttypid))
    }
    fn slot_getallattrs(&self) -> PgResult<()> {
        self.s.borrow_mut().getallattrs_calls += 1;
        Ok(())
    }
    fn is_null(&self, attnum: usize) -> PgResult<bool> {
        Ok(self.s.borrow().is_null[attnum])
    }
    fn value(&self, attnum: usize) -> PgResult<Datum> {
        Ok(self.s.borrow().values[attnum])
    }
    fn send_function_call(&self, _finfo: &FmgrInfo, _attr: Datum) -> PgResult<OutputBytes> {
        Ok(self.s.borrow().binary_out.clone())
    }
    fn create_tmpcontext(&self) -> PgResult<()> {
        self.s.borrow_mut().create_calls += 1;
        Ok(())
    }
    fn enter_tmpcontext(&self) -> PgResult<()> {
        self.s.borrow_mut().enter_calls += 1;
        Ok(())
    }
    fn exit_tmpcontext(&self) -> PgResult<()> {
        self.s.borrow_mut().exit_calls += 1;
        Ok(())
    }
    fn delete_tmpcontext(&self) -> PgResult<()> {
        self.s.borrow_mut().delete_calls += 1;
        Ok(())
    }
    fn instr_time_current(&self) -> PgResult<instr_time> {
        let mut s = self.s.borrow_mut();
        let t = s.clock[s.clock_pos];
        s.clock_pos += 1;
        Ok(instr_time { ticks: t })
    }
    fn pg_buffer_usage(&self) -> PgResult<BufferUsage> {
        let mut s = self.s.borrow_mut();
        let b = s.bufusage[s.bufusage_pos];
        s.bufusage_pos += 1;
        Ok(b)
    }
}

// --- helpers -----------------------------------------------------------------

fn make_tupdesc<'mcx>(mcx: Mcx<'mcx>, typoids: &[Oid]) -> TupleDescData<'mcx> {
    let mut attrs = mcx::PgVec::new_in(mcx);
    let mut compact = mcx::PgVec::new_in(mcx);
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
    let rt = ScriptRuntime::new(Script::default());
    let _ = serializeAnalyzeStartup(&mut r, cx.mcx(), 0, &make_tupdesc(cx.mcx(), &[]), &rt).unwrap();
    assert_eq!(r.format, 0);
    assert_eq!(r.metrics, SerializeMetrics::default());
    assert_eq!(rt.s.borrow().create_calls, 1);

    let es_bin = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY,
        false,
        false,
    );
    let mut r2 = CreateExplainSerializeDestReceiver(&es_bin);
    let _ =
        serializeAnalyzeStartup(&mut r2, cx.mcx(), 0, &make_tupdesc(cx.mcx(), &[]), &rt).unwrap();
    assert_eq!(r2.format, 1);

    // Shutdown clears finfos / attrinfo / nattrs and deletes the context.
    r2.finfos.push(FmgrInfo::empty());
    r2.nattrs = 3;
    serializeAnalyzeShutdown(&mut r2, &rt).unwrap();
    assert!(r2.finfos.is_empty());
    assert_eq!(r2.nattrs, 0);
    assert!(rt.s.borrow().delete_calls >= 1);
}

#[test]
fn binary_path_lengths_and_byte_count() {
    let cx = MemoryContext::new("t");
    let es = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY,
        false,
        false,
    );
    let mut receiver = CreateExplainSerializeDestReceiver(&es);

    let rt = ScriptRuntime::new(Script {
        is_null: alloc_vec(&[false]),
        values: vec![Datum::from_usize(1)],
        binary_out: vec![0xde, 0xad, 0xbe, 0xef],
        ..Script::default()
    });

    let typeinfo = make_tupdesc(cx.mcx(), &[17]);
    let mut buf =
        serializeAnalyzeStartup(&mut receiver, cx.mcx(), 0, &typeinfo, &rt).unwrap();
    assert_eq!(receiver.format, 1);

    let ok = serializeAnalyzeReceive(&mut receiver, &mut buf, &typeinfo, &rt).unwrap();
    assert!(ok);

    // Binary DataRow body: int16 natts, int32 payload length, payload bytes.
    let mut expected: Vec<u8> = Vec::new();
    expected.extend_from_slice(&1u16.to_be_bytes());
    expected.extend_from_slice(&4u32.to_be_bytes());
    expected.extend_from_slice(&[0xde, 0xad, 0xbe, 0xef]);
    assert_eq!(buf.as_bytes(), &expected[..]);
    assert_eq!(receiver.metrics.bytesSent, expected.len() as u64);

    // Slot deconstructed once; per-row context entered then exited once.
    assert_eq!(rt.s.borrow().getallattrs_calls, 1);
    assert_eq!(rt.s.borrow().enter_calls, 1);
    assert_eq!(rt.s.borrow().exit_calls, 1);
}

#[test]
fn null_column_emits_minus_one_and_accumulates() {
    let cx = MemoryContext::new("t");
    let es = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY,
        false,
        false,
    );
    let mut receiver = CreateExplainSerializeDestReceiver(&es);

    let rt = ScriptRuntime::new(Script {
        is_null: alloc_vec(&[false, true]),
        values: vec![Datum::from_usize(1), Datum::from_usize(0)],
        binary_out: vec![0xaa],
        ..Script::default()
    });

    let typeinfo = make_tupdesc(cx.mcx(), &[17, 25]);
    let mut buf =
        serializeAnalyzeStartup(&mut receiver, cx.mcx(), 0, &typeinfo, &rt).unwrap();

    serializeAnalyzeReceive(&mut receiver, &mut buf, &typeinfo, &rt).unwrap();

    let mut expected: Vec<u8> = Vec::new();
    expected.extend_from_slice(&2u16.to_be_bytes()); // natts
    expected.extend_from_slice(&1u32.to_be_bytes()); // col0 len
    expected.push(0xaa); // col0 payload
    expected.extend_from_slice(&(-1i32 as u32).to_be_bytes()); // col1 NULL
    assert_eq!(buf.as_bytes(), &expected[..]);
    assert_eq!(receiver.metrics.bytesSent, expected.len() as u64);

    // A second row accumulates onto bytesSent.
    serializeAnalyzeReceive(&mut receiver, &mut buf, &typeinfo, &rt).unwrap();
    assert_eq!(receiver.metrics.bytesSent, (expected.len() * 2) as u64);
}

#[test]
fn timing_and_buffers_accumulate() {
    let cx = MemoryContext::new("t");
    let es = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_BINARY,
        true,
        true,
    );
    let mut receiver = CreateExplainSerializeDestReceiver(&es);

    let before = BufferUsage {
        shared_blks_hit: 100,
        ..BufferUsage::default()
    };
    let after = BufferUsage {
        shared_blks_hit: 105,
        ..BufferUsage::default()
    };

    let rt = ScriptRuntime::new(Script {
        is_null: alloc_vec(&[false]),
        values: vec![Datum::from_usize(7)],
        binary_out: vec![0x07],
        clock: vec![1_000, 1_750],
        bufusage: vec![before, after],
        ..Script::default()
    });

    let typeinfo = make_tupdesc(cx.mcx(), &[17]);
    let mut buf =
        serializeAnalyzeStartup(&mut receiver, cx.mcx(), 0, &typeinfo, &rt).unwrap();

    serializeAnalyzeReceive(&mut receiver, &mut buf, &typeinfo, &rt).unwrap();

    assert_eq!(receiver.metrics.timeSpent.ticks, 750);
    assert_eq!(receiver.metrics.bufferUsage.shared_blks_hit, 5);
}

#[test]
fn unsupported_format_errors() {
    let cx = MemoryContext::new("t");
    let es = make_es(
        cx.mcx(),
        ExplainSerializeOption::EXPLAIN_SERIALIZE_TEXT,
        false,
        false,
    );
    let mut receiver = CreateExplainSerializeDestReceiver(&es);
    receiver.format = 7; // C `else` branch

    let rt = ScriptRuntime::new(Script::default());
    let typeinfo = make_tupdesc(cx.mcx(), &[23]);
    let err = serialize_prepare_info(&mut receiver, &typeinfo, 1, &rt).unwrap_err();
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

fn alloc_vec(v: &[bool]) -> Vec<bool> {
    v.to_vec()
}
