#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use std::fmt::Write as _;
use std::rc::Rc;

use datum::Datum;
use elog::ereport;
use logical::{
    OutputPluginCallbacks, OutputPluginContext, OutputPluginOutputType, OutputPluginPrepareWrite,
    OutputPluginWrite,
};
use mcx::{Mcx, MemoryContext};
use reorderbuffer::{ReorderBuffer, ReorderBufferChange, ReorderBufferChangeData, TxnId};
use types_core::{
    InvalidOid, Oid, RepOriginId, TransactionId, XLogRecPtr, BITOID, BOOLOID, FLOAT4OID,
    FLOAT8OID, INT2OID, INT4OID, INT8OID, NUMERICOID, OIDOID, VARBITOID,
};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_INVALID_PARAMETER_VALUE, ERROR,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_rel::RelationData;
use types_tuple::{varatt, HeapTupleData, TupleDescData};

const LIBRARY: &str = "test_decoding";

#[track_caller]
fn loc(func: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, func)
}

#[cold]
#[inline(never)]
fn unported(what: &str) -> ! {
    panic!("unported callee reached from test_decoding.c: {what}")
}

struct TestDecodingData {
    include_xids: bool,
    include_timestamp: bool,
    skip_empty_xacts: bool,
    only_local: bool,
}

struct TestDecodingTxnData {
    xact_wrote_changes: bool,
    // Whether the current streamed block wrote anything (per-chunk reset).
    stream_wrote_changes: bool,
}

fn data_from(opc: &OutputPluginContext) -> &'static mut TestDecodingData {
    debug_assert!(opc.output_plugin_private != 0);
    // SAFETY: set in pg_decode_startup; freed only in pg_decode_shutdown.
    unsafe { &mut *(opc.output_plugin_private as *mut TestDecodingData) }
}

fn txndata_from(rb: &ReorderBuffer, txn: TxnId) -> &'static mut TestDecodingTxnData {
    let p = rb.txn(txn).output_plugin_private;
    debug_assert!(p != 0);
    // SAFETY: set in pg_decode_begin_txn; freed only in pg_decode_commit_txn.
    unsafe { &mut *(p as *mut TestDecodingTxnData) }
}

fn fc__pg_output_plugin_init(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // SAFETY: arg 0 carries the callbacks pointer from LoadOutputPlugin.
    let cb = unsafe { &mut *(fcinfo.arg(0).as_usize() as *mut OutputPluginCallbacks) };
    cb.startup_cb = Some(pg_decode_startup);
    cb.begin_cb = Some(pg_decode_begin_txn);
    cb.change_cb = Some(pg_decode_change);
    cb.truncate_cb = Some(pg_decode_truncate);
    cb.commit_cb = Some(pg_decode_commit_txn);
    cb.filter_prepare_cb = Some(pg_decode_filter_prepare);
    cb.begin_prepare_cb = Some(pg_decode_begin_prepare_txn);
    cb.prepare_cb = Some(pg_decode_prepare_txn);
    cb.commit_prepared_cb = Some(pg_decode_commit_prepared_txn);
    cb.rollback_prepared_cb = Some(pg_decode_rollback_prepared_txn);
    cb.filter_by_origin_cb = Some(pg_decode_filter);
    cb.shutdown_cb = Some(pg_decode_shutdown);
    cb.message_cb = Some(pg_decode_message);
    cb.stream_start_cb = Some(pg_decode_stream_start);
    cb.stream_stop_cb = Some(pg_decode_stream_stop);
    cb.stream_abort_cb = Some(pg_decode_stream_abort);
    cb.stream_commit_cb = Some(pg_decode_stream_commit);
    cb.stream_change_cb = Some(pg_decode_stream_change);
    cb.stream_message_cb = Some(pg_decode_stream_message);
    cb.stream_truncate_cb = Some(pg_decode_stream_truncate);
    // C also registers stream_prepare_cb; streamed two-phase is deferred
    // (GL-LOGDEC-1 ASK-1) — reaching it errors in the wrapper.
    Ok(Datum::from_usize(0))
}

fn parse_bool_option(name: &str, value: Option<&str>, target: &mut bool) -> PgResult<()> {
    match value {
        None => {
            *target = true;
            Ok(())
        }
        Some(v) => match adt_bool::parse_bool(v) {
            Some(b) => {
                *target = b;
                Ok(())
            }
            None => ereport(ERROR)
                .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                .errmsg(format!(
                    "could not parse value \"{v}\" for parameter \"{name}\""
                ))
                .finish(loc("pg_decode_startup")),
        },
    }
}

fn pg_decode_startup(opc: &mut OutputPluginContext, _is_init: bool) -> PgResult<()> {
    let mut data = Box::new(TestDecodingData {
        include_xids: true,
        include_timestamp: false,
        skip_empty_xacts: false,
        only_local: false,
    });

    opc.options.output_type = OutputPluginOutputType::Textual;
    opc.options.receive_rewrites = false;

    let mut enable_streaming = false;
    let options = std::mem::take(&mut opc.output_plugin_options);
    for (name, value) in &options {
        let value_str = value.as_deref();
        match name.as_str() {
            "include-xids" => parse_bool_option(name, value_str, &mut data.include_xids)?,
            "include-timestamp" => {
                parse_bool_option(name, value_str, &mut data.include_timestamp)?
            }
            "force-binary" => {
                let mut force_binary = false;
                if value_str.is_some() {
                    parse_bool_option(name, value_str, &mut force_binary)?;
                    if force_binary {
                        opc.options.output_type = OutputPluginOutputType::Binary;
                    }
                }
            }
            "skip-empty-xacts" => parse_bool_option(name, value_str, &mut data.skip_empty_xacts)?,
            "only-local" => parse_bool_option(name, value_str, &mut data.only_local)?,
            "include-rewrites" => {
                if value_str.is_some() {
                    let mut v = opc.options.receive_rewrites;
                    parse_bool_option(name, value_str, &mut v)?;
                    opc.options.receive_rewrites = v;
                }
            }
            "stream-changes" => {
                if value_str.is_some() {
                    parse_bool_option(name, value_str, &mut enable_streaming)?;
                }
            }
            _ => {
                return ereport(ERROR)
                    .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
                    .errmsg(format!(
                        "option \"{}\" = \"{}\" is unknown",
                        name,
                        value_str.unwrap_or("(null)")
                    ))
                    .finish(loc("pg_decode_startup"));
            }
        }
    }
    opc.output_plugin_options = options;

    opc.streaming &= enable_streaming;

    opc.output_plugin_private = Box::into_raw(data) as usize;
    Ok(())
}

fn pg_decode_shutdown(opc: &mut OutputPluginContext) -> PgResult<()> {
    let p = opc.output_plugin_private;
    opc.output_plugin_private = 0;
    if p != 0 {
        // SAFETY: exclusive owner of the startup allocation.
        unsafe { drop(Box::from_raw(p as *mut TestDecodingData)) };
    }
    Ok(())
}

fn pg_decode_begin_txn(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
) -> PgResult<()> {
    let data = data_from(opc);
    let txndata = Box::new(TestDecodingTxnData {
        xact_wrote_changes: false,
        stream_wrote_changes: false,
    });
    rb.txn_mut(txn).output_plugin_private = Box::into_raw(txndata) as usize;

    if data.skip_empty_xacts {
        return Ok(());
    }
    pg_output_begin(opc, rb, txn, true)
}

fn pg_output_begin(
    opc: &mut OutputPluginContext,
    rb: &ReorderBuffer,
    txn: TxnId,
    last_write: bool,
) -> PgResult<()> {
    let include_xids = data_from(opc).include_xids;
    OutputPluginPrepareWrite(opc, last_write)?;
    if include_xids {
        let _ = write!(opc.out, "BEGIN {}", rb.txn(txn).xid);
    } else {
        opc.out.push_str("BEGIN");
    }
    OutputPluginWrite(opc, last_write)
}

fn timestamptz_str(ts: types_core::TimestampTz) -> PgResult<String> {
    let mut buf: adt_timestamp::TsBuf = [0; core::mem::size_of::<adt_timestamp::TsBuf>()];
    let len = adt_timestamp::timestamptz_out(ts, &mut buf)?;
    Ok(String::from_utf8_lossy(&buf[..len]).into_owned())
}

fn pg_decode_commit_txn(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    _commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    let data = data_from(opc);

    let p = rb.txn(txn).output_plugin_private;
    rb.txn_mut(txn).output_plugin_private = 0;
    debug_assert!(p != 0);
    // SAFETY: exclusive owner of the begin-callback allocation.
    let txndata = unsafe { Box::from_raw(p as *mut TestDecodingTxnData) };
    let xact_wrote_changes = txndata.xact_wrote_changes;
    drop(txndata);

    if data.skip_empty_xacts && !xact_wrote_changes {
        return Ok(());
    }

    OutputPluginPrepareWrite(opc, true)?;
    if data.include_xids {
        let _ = write!(opc.out, "COMMIT {}", rb.txn(txn).xid);
    } else {
        opc.out.push_str("COMMIT");
    }

    if data.include_timestamp {
        let ts = timestamptz_str(rb.txn(txn).xact_time)?;
        let _ = write!(opc.out, " (at {ts})");
    }

    OutputPluginWrite(opc, true)
}

fn pg_decode_filter(opc: &mut OutputPluginContext, origin_id: RepOriginId) -> PgResult<bool> {
    let data = data_from(opc);
    Ok(data.only_local && origin_id != 0)
}

// quote_literal_cstr (quote.c): single-quote, doubling ' and \, with an E
// prefix when a backslash is present.
fn quote_literal_str(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len() * 2 + 4);
    if raw.contains('\\') {
        out.push('E');
    }
    out.push('\'');
    for ch in raw.chars() {
        if ch == '\'' || ch == '\\' {
            out.push(ch);
        }
        out.push(ch);
    }
    out.push('\'');
    out
}

// pg_decode_begin_prepare_txn (test_decoding.c:349).
fn pg_decode_begin_prepare_txn(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
) -> PgResult<()> {
    let data = data_from(opc);
    let txndata = Box::new(TestDecodingTxnData {
        xact_wrote_changes: false,
        stream_wrote_changes: false,
    });
    rb.txn_mut(txn).output_plugin_private = Box::into_raw(txndata) as usize;

    // If asked to skip empty transactions, BEGIN is emitted at the first
    // operation instead.
    if data.skip_empty_xacts {
        return Ok(());
    }
    pg_output_begin(opc, rb, txn, true)
}

// pg_decode_prepare_txn (test_decoding.c:370).
fn pg_decode_prepare_txn(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    _prepare_lsn: XLogRecPtr,
) -> PgResult<()> {
    let data = data_from(opc);

    // PREPARE ends this transaction's output: reclaim the begin-prepare
    // allocation now (C leaves it to the decoding context's teardown). An
    // empty prepared txn skips ProcessTXN entirely, so begin_prepare never
    // ran and there is nothing to reclaim.
    let p = rb.txn(txn).output_plugin_private;
    rb.txn_mut(txn).output_plugin_private = 0;
    let xact_wrote_changes = if p != 0 {
        // SAFETY: exclusive owner of the begin-prepare-callback allocation.
        let txndata = unsafe { Box::from_raw(p as *mut TestDecodingTxnData) };
        txndata.xact_wrote_changes
    } else {
        false
    };

    if data.skip_empty_xacts && !xact_wrote_changes {
        return Ok(());
    }

    let gid = rb.txn(txn).gid.clone().expect("prepared txn carries a gid");
    OutputPluginPrepareWrite(opc, true)?;
    let _ = write!(opc.out, "PREPARE TRANSACTION {}", quote_literal_str(&gid));
    if data.include_xids {
        let _ = write!(opc.out, ", txid {}", rb.txn(txn).xid);
    }
    if data.include_timestamp {
        let ts = timestamptz_str(rb.txn(txn).xact_time)?;
        let _ = write!(opc.out, " (at {ts})");
    }
    OutputPluginWrite(opc, true)
}

// pg_decode_commit_prepared_txn (test_decoding.c:400).
fn pg_decode_commit_prepared_txn(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    _commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    let data = data_from(opc);
    let gid = rb.txn(txn).gid.clone().expect("prepared txn carries a gid");

    OutputPluginPrepareWrite(opc, true)?;
    let _ = write!(opc.out, "COMMIT PREPARED {}", quote_literal_str(&gid));
    if data.include_xids {
        let _ = write!(opc.out, ", txid {}", rb.txn(txn).xid);
    }
    if data.include_timestamp {
        let ts = timestamptz_str(rb.txn(txn).xact_time)?;
        let _ = write!(opc.out, " (at {ts})");
    }
    OutputPluginWrite(opc, true)
}

// pg_decode_rollback_prepared_txn (test_decoding.c:422).
fn pg_decode_rollback_prepared_txn(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    _prepare_end_lsn: XLogRecPtr,
    _prepare_time: types_core::TimestampTz,
) -> PgResult<()> {
    let data = data_from(opc);
    let gid = rb.txn(txn).gid.clone().expect("prepared txn carries a gid");

    OutputPluginPrepareWrite(opc, true)?;
    let _ = write!(opc.out, "ROLLBACK PREPARED {}", quote_literal_str(&gid));
    if data.include_xids {
        let _ = write!(opc.out, ", txid {}", rb.txn(txn).xid);
    }
    if data.include_timestamp {
        let ts = timestamptz_str(rb.txn(txn).xact_time)?;
        let _ = write!(opc.out, " (at {ts})");
    }
    OutputPluginWrite(opc, true)
}

// pg_decode_filter_prepare (test_decoding.c:452): filter out gids containing
// "_nodecode" (they decode as regular transactions at COMMIT PREPARED).
fn pg_decode_filter_prepare(
    _opc: &mut OutputPluginContext,
    _xid: TransactionId,
    gid: &str,
) -> PgResult<bool> {
    Ok(gid.contains("_nodecode"))
}

fn print_literal(s: &mut logical::OutBuf, typid: Oid, outputstr: &str) {
    match typid {
        INT2OID | INT4OID | INT8OID | OIDOID | FLOAT4OID | FLOAT8OID | NUMERICOID => {
            s.push_str(outputstr);
        }
        BITOID | VARBITOID => {
            let _ = write!(s, "B'{outputstr}'");
        }
        BOOLOID => {
            if outputstr == "t" {
                s.push_str("true");
            } else {
                s.push_str("false");
            }
        }
        _ => {
            s.push('\'');
            for ch in outputstr.chars() {
                if ch == '\'' {
                    s.push(ch);
                }
                s.push(ch);
            }
            s.push('\'');
        }
    }
}

fn oid_output_function_call(mcx: Mcx<'_>, typoutput: Oid, val: Datum) -> PgResult<String> {
    let mut flinfo = fmgr_seams::fmgr_info::call(typoutput)?;
    let d = types_fmgr::function_call1_coll_in(&mut flinfo, InvalidOid, mcx, val)?;
    // SAFETY: output functions return a NUL-terminated cstring datum.
    let bytes = unsafe { core::ffi::CStr::from_ptr((d.as_usize() as *const u8).cast()) }.to_bytes();
    Ok(String::from_utf8_lossy(bytes).into_owned())
}

fn varatt_is_external_ondisk(val: Datum) -> bool {
    let p = val.as_usize() as *const u8;
    // SAFETY: a live varlena datum; the first two bytes classify it.
    unsafe { *p == 0x01 && *p.add(1) == varatt::VARTAG_ONDISK }
}

fn tuple_to_stringinfo(
    s: &mut logical::OutBuf,
    mcx: Mcx<'_>,
    tupdesc: &TupleDescData<'static>,
    tuple: &HeapTupleData<'_>,
    skip_nulls: bool,
) -> PgResult<()> {
    for natt in 0..tupdesc.natts as usize {
        let attr = &tupdesc.attrs[natt];

        if attr.attisdropped || attr.attnum < 0 {
            continue;
        }

        let typid = attr.atttypid;

        let mut isnull = false;
        // SAFETY: natt+1 <= natts; tupdesc matches the tuple's relation.
        let origval = unsafe {
            types_tuple::getattr::heap_getattr(tuple, natt as i32 + 1, tupdesc, &mut isnull)
        };

        if isnull && skip_nulls {
            continue;
        }

        s.push(' ');
        let attname = String::from_utf8_lossy(attr.attname.name_str()).into_owned();
        s.push_str(&format_type::quote_identifier(&attname));

        s.push('[');
        s.push_str(&format_type::format_type_be(typid)?);
        s.push(']');

        let (typoutput, typisvarlena) = lsyscache::getTypeOutputInfo(typid)?;

        s.push(':');

        if isnull {
            s.push_str("null");
        } else if typisvarlena && varatt_is_external_ondisk(origval) {
            s.push_str("unchanged-toast-datum");
        } else if !typisvarlena {
            print_literal(s, typid, &oid_output_function_call(mcx, typoutput, origval)?);
        } else {
            let p = origval.as_usize() as *const u8;
            // SAFETY: a live varlena readable through its full VARSIZE_ANY.
            let raw = unsafe { core::slice::from_raw_parts(p, varatt::varsize_any(p)) };
            let detoasted = detoast::detoast_attr(mcx, raw)?;
            let val = Datum::from_usize(detoasted.as_ptr() as usize);
            let out = oid_output_function_call(mcx, typoutput, val)?;
            core::mem::forget(detoasted);
            print_literal(s, typid, &out);
        }
    }
    Ok(())
}

fn pg_decode_change(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    relation: &RelationData<'static>,
    change: &mut ReorderBufferChange,
) -> PgResult<()> {
    let data = data_from(opc);
    let txndata = txndata_from(rb, txn);

    if data.skip_empty_xacts && !txndata.xact_wrote_changes {
        pg_output_begin(opc, rb, txn, false)?;
    }
    txndata.xact_wrote_changes = true;

    let class_form = &relation.rd_rel;
    let tupdesc = &relation.rd_att;

    // C resets a private context per change; dropping this arena matches.
    let change_ctx = MemoryContext::new("text conversion context");
    let mcx = change_ctx.mcx();

    OutputPluginPrepareWrite(opc, true)?;

    opc.out.push_str("table ");
    let nspname = lsyscache::get_namespace_name(
        mcx,
        lsyscache::get_rel_namespace(relation.rd_id)?,
    )?
    .expect("relation namespace exists");
    // C prefers get_rel_name(relrewrite) for rewrite relations; relrewrite is
    // not carried by this port's FormData_pg_class and rewrite changes only
    // reach plugins with receive_rewrites (unreachable without that field).
    let relname = String::from_utf8_lossy(class_form.relname.name_str()).into_owned();
    opc.out
        .push_str(&ruleutils::quote_qualified_identifier(
            Some(nspname.as_str()),
            &relname,
        ));
    opc.out.push(':');

    match &change.data {
        ReorderBufferChangeData::Tp {
            oldtuple, newtuple, ..
        } => match change.action {
            reorderbuffer::Insert => {
                opc.out.push_str(" INSERT:");
                match newtuple {
                    None => opc.out.push_str(" (no-tuple-data)"),
                    Some(t) => tuple_to_stringinfo(&mut opc.out, mcx, tupdesc, t, false)?,
                }
            }
            reorderbuffer::Update => {
                opc.out.push_str(" UPDATE:");
                if let Some(t) = oldtuple {
                    opc.out.push_str(" old-key:");
                    tuple_to_stringinfo(&mut opc.out, mcx, tupdesc, t, true)?;
                    opc.out.push_str(" new-tuple:");
                }
                match newtuple {
                    None => opc.out.push_str(" (no-tuple-data)"),
                    Some(t) => tuple_to_stringinfo(&mut opc.out, mcx, tupdesc, t, false)?,
                }
            }
            reorderbuffer::Delete => {
                opc.out.push_str(" DELETE:");
                match oldtuple {
                    None => opc.out.push_str(" (no-tuple-data)"),
                    Some(t) => tuple_to_stringinfo(&mut opc.out, mcx, tupdesc, t, true)?,
                }
            }
            _ => unreachable!("unexpected change action in change callback"),
        },
        _ => unreachable!("change callback with non-tuple payload"),
    }

    OutputPluginWrite(opc, true)
}

fn pg_decode_truncate(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    relations: &[Rc<RelationData<'static>>],
    change: &mut ReorderBufferChange,
) -> PgResult<()> {
    let data = data_from(opc);
    let txndata = txndata_from(rb, txn);

    if data.skip_empty_xacts && !txndata.xact_wrote_changes {
        pg_output_begin(opc, rb, txn, false)?;
    }
    txndata.xact_wrote_changes = true;

    let change_ctx = MemoryContext::new("text conversion context");
    let mcx = change_ctx.mcx();

    OutputPluginPrepareWrite(opc, true)?;

    opc.out.push_str("table ");

    for (i, rel) in relations.iter().enumerate() {
        if i > 0 {
            opc.out.push_str(", ");
        }
        let nspname = lsyscache::get_namespace_name(mcx, rel.rd_rel.relnamespace)?
            .expect("relation namespace exists");
        let relname = String::from_utf8_lossy(rel.rd_rel.relname.name_str()).into_owned();
        opc.out
            .push_str(&ruleutils::quote_qualified_identifier(
                Some(nspname.as_str()),
                &relname,
            ));
    }

    opc.out.push_str(": TRUNCATE:");

    let (restart_seqs, cascade) = match &change.data {
        ReorderBufferChangeData::Truncate {
            cascade,
            restart_seqs,
            ..
        } => (*restart_seqs, *cascade),
        _ => unreachable!("truncate callback with non-truncate payload"),
    };

    if restart_seqs || cascade {
        if restart_seqs {
            opc.out.push_str(" restart_seqs");
        }
        if cascade {
            opc.out.push_str(" cascade");
        }
    } else {
        opc.out.push_str(" (no-flags)");
    }

    OutputPluginWrite(opc, true)
}

fn pg_decode_message(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: Option<TxnId>,
    _lsn: XLogRecPtr,
    transactional: bool,
    prefix: &str,
    message: &[u8],
) -> PgResult<()> {
    let data = data_from(opc);

    if transactional {
        let txn = txn.expect("transactional message has a txn");
        let txndata = txndata_from(rb, txn);
        if data.skip_empty_xacts && !txndata.xact_wrote_changes {
            pg_output_begin(opc, rb, txn, false)?;
        }
        txndata.xact_wrote_changes = true;
    }

    OutputPluginPrepareWrite(opc, true)?;
    let _ = write!(
        opc.out,
        "message: transactional: {} prefix: {}, sz: {} content:",
        transactional as i32,
        prefix,
        message.len()
    );
    opc.out.push_str(&String::from_utf8_lossy(message));
    OutputPluginWrite(opc, true)
}

// pg_output_stream_start (test_decoding.c:672).
fn pg_output_stream_start(
    opc: &mut OutputPluginContext,
    rb: &ReorderBuffer,
    txn: TxnId,
    last_write: bool,
) -> PgResult<()> {
    let include_xids = data_from(opc).include_xids;
    OutputPluginPrepareWrite(opc, last_write)?;
    if include_xids {
        let _ = write!(
            opc.out,
            "opening a streamed block for transaction TXN {}",
            rb.txn(txn).xid
        );
    } else {
        opc.out.push_str("opening a streamed block for transaction");
    }
    OutputPluginWrite(opc, last_write)
}

// pg_decode_stream_start (test_decoding.c:648): streamed txns skip begin_cb,
// so the txn plugin data allocates lazily on the first stream.
fn pg_decode_stream_start(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
) -> PgResult<()> {
    let data = data_from(opc);
    if rb.txn(txn).output_plugin_private == 0 {
        let txndata = Box::new(TestDecodingTxnData {
            xact_wrote_changes: false,
            stream_wrote_changes: false,
        });
        rb.txn_mut(txn).output_plugin_private = Box::into_raw(txndata) as usize;
    }
    txndata_from(rb, txn).stream_wrote_changes = false;
    if data.skip_empty_xacts {
        return Ok(());
    }
    pg_output_stream_start(opc, rb, txn, true)
}

// pg_decode_stream_stop (test_decoding.c:684).
fn pg_decode_stream_stop(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
) -> PgResult<()> {
    let data = data_from(opc);
    if data.skip_empty_xacts && !txndata_from(rb, txn).stream_wrote_changes {
        return Ok(());
    }
    OutputPluginPrepareWrite(opc, true)?;
    if data.include_xids {
        let _ = write!(
            opc.out,
            "closing a streamed block for transaction TXN {}",
            rb.txn(txn).xid
        );
    } else {
        opc.out.push_str("closing a streamed block for transaction");
    }
    OutputPluginWrite(opc, true)
}

// pg_decode_stream_abort (test_decoding.c:702): the abort may name a
// subtransaction, but the plugin data lives on the toplevel txn.
fn pg_decode_stream_abort(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    _abort_lsn: XLogRecPtr,
) -> PgResult<()> {
    let data = data_from(opc);
    let toptxn = rb.toptxn_id(txn);
    let xact_wrote_changes = txndata_from(rb, toptxn).xact_wrote_changes;

    if toptxn == txn {
        let p = rb.txn(txn).output_plugin_private;
        rb.txn_mut(txn).output_plugin_private = 0;
        debug_assert!(p != 0);
        // SAFETY: exclusive owner of the stream-start allocation.
        unsafe { drop(Box::from_raw(p as *mut TestDecodingTxnData)) };
    }

    if data.skip_empty_xacts && !xact_wrote_changes {
        return Ok(());
    }
    OutputPluginPrepareWrite(opc, true)?;
    if data.include_xids {
        let _ = write!(
            opc.out,
            "aborting streamed (sub)transaction TXN {}",
            rb.txn(txn).xid
        );
    } else {
        opc.out.push_str("aborting streamed (sub)transaction");
    }
    OutputPluginWrite(opc, true)
}

// pg_decode_stream_commit (test_decoding.c:740).
fn pg_decode_stream_commit(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    _commit_lsn: XLogRecPtr,
) -> PgResult<()> {
    let data = data_from(opc);
    let p = rb.txn(txn).output_plugin_private;
    rb.txn_mut(txn).output_plugin_private = 0;
    debug_assert!(p != 0);
    // SAFETY: exclusive owner of the stream-start allocation.
    let txndata = unsafe { Box::from_raw(p as *mut TestDecodingTxnData) };
    let xact_wrote_changes = txndata.xact_wrote_changes;
    drop(txndata);

    if data.skip_empty_xacts && !xact_wrote_changes {
        return Ok(());
    }
    OutputPluginPrepareWrite(opc, true)?;
    if data.include_xids {
        let _ = write!(opc.out, "committing streamed transaction TXN {}", rb.txn(txn).xid);
    } else {
        opc.out.push_str("committing streamed transaction");
    }
    if data.include_timestamp {
        let ts = timestamptz_str(rb.txn(txn).xact_time)?;
        let _ = write!(opc.out, " (at {ts})");
    }
    OutputPluginWrite(opc, true)
}

// pg_decode_stream_change (test_decoding.c:771). In a streamed block the
// change data may already be gone (spilled and consumed); only the fact of
// the change is printed.
fn pg_decode_stream_change(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    _relation: &RelationData<'static>,
    _change: &mut ReorderBufferChange,
) -> PgResult<()> {
    let data = data_from(opc);
    if data.skip_empty_xacts && !txndata_from(rb, txn).stream_wrote_changes {
        pg_output_stream_start(opc, rb, txn, false)?;
    }
    {
        let txndata = txndata_from(rb, txn);
        txndata.xact_wrote_changes = true;
        txndata.stream_wrote_changes = true;
    }
    OutputPluginPrepareWrite(opc, true)?;
    if data.include_xids {
        let _ = write!(opc.out, "streaming change for TXN {}", rb.txn(txn).xid);
    } else {
        opc.out.push_str("streaming change for transaction");
    }
    OutputPluginWrite(opc, true)
}

// pg_decode_stream_message (test_decoding.c:797).
fn pg_decode_stream_message(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: Option<TxnId>,
    _lsn: XLogRecPtr,
    transactional: bool,
    prefix: &str,
    message: &[u8],
) -> PgResult<()> {
    let data = data_from(opc);
    if transactional {
        let txn = txn.expect("transactional message has a txn");
        if data.skip_empty_xacts && !txndata_from(rb, txn).stream_wrote_changes {
            pg_output_stream_start(opc, rb, txn, false)?;
        }
        let txndata = txndata_from(rb, txn);
        txndata.xact_wrote_changes = true;
        txndata.stream_wrote_changes = true;
    }

    OutputPluginPrepareWrite(opc, true)?;
    if transactional {
        // The message content is not output: it may already be gone.
        let _ = write!(
            opc.out,
            "streaming message: transactional: {} prefix: {}, sz: {}",
            transactional as i32,
            prefix,
            message.len()
        );
    } else {
        let _ = write!(
            opc.out,
            "streaming message: transactional: {} prefix: {}, sz: {} content:",
            transactional as i32,
            prefix,
            message.len()
        );
        opc.out.push_str(&String::from_utf8_lossy(message));
    }
    OutputPluginWrite(opc, true)
}

// pg_decode_stream_truncate (test_decoding.c:835).
fn pg_decode_stream_truncate(
    opc: &mut OutputPluginContext,
    rb: &mut ReorderBuffer,
    txn: TxnId,
    _relations: &[Rc<RelationData<'static>>],
    _change: &mut ReorderBufferChange,
) -> PgResult<()> {
    let data = data_from(opc);
    if data.skip_empty_xacts && !txndata_from(rb, txn).stream_wrote_changes {
        pg_output_stream_start(opc, rb, txn, false)?;
    }
    {
        let txndata = txndata_from(rb, txn);
        txndata.xact_wrote_changes = true;
        txndata.stream_wrote_changes = true;
    }
    OutputPluginPrepareWrite(opc, true)?;
    if data.include_xids {
        let _ = write!(opc.out, "streaming truncate for TXN {}", rb.txn(txn).xid);
    } else {
        opc.out.push_str("streaming truncate for transaction");
    }
    OutputPluginWrite(opc, true)
}

fn lookup(function: &str) -> Option<types_fmgr::PGFunction> {
    match function {
        "_PG_output_plugin_init" => Some(fc__pg_output_plugin_init),
        _ => None,
    }
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        pg_init: None,
    });
}
