// copyto.c, text/CSV/binary formats to file or frontend, from a relation scan
// or a query executor run (DestCopyOut). The callback destination
// (data_dest_cb) has no producer here.

use core::ffi::CStr;
use std::rc::Rc;

use backend_progress::progress::{
    PROGRESS_COPY_BYTES_PROCESSED, PROGRESS_COPY_COMMAND, PROGRESS_COPY_COMMAND_TO,
    PROGRESS_COPY_TUPLES_PROCESSED, PROGRESS_COPY_TYPE, PROGRESS_COPY_TYPE_FILE,
    PROGRESS_COPY_TYPE_PIPE,
};
use backend_progress::{
    pgstat_progress_end_command, pgstat_progress_start_command, pgstat_progress_update_multi_param,
    pgstat_progress_update_param, PROGRESS_COMMAND_COPY,
};
use datum::Datum;
use elog::ereport;
use mcx::{Mcx, MemoryContext, PgVec};
use stringinfo::StringInfo;
use types_core::primitive::InvalidOid;
use types_dest::CommandDest;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_INVALID_NAME,
    ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE, ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_fmgr::{function_call1_coll_in, send_function_call, FmgrInfo, PackedVarlena};
use types_nodes::nodes_enums::CmdType;
use types_nodes::rawnodes::{CreateTableAsStmt, RawStmt};
use types_nodes::{NodeList, QuerySource};
use types_portal::{ParamListHandle, QueryDescHandle, QueryEnvHandle, CURSOR_OPT_PARALLEL_OK};
use types_rel::Relation;
use types_scan::ForwardScanDirection;
use types_slot::SlotData;
use types_tuple::TupleDescData;

use crate::{
    force_flags, unported, CopyFormatOptions, CopyGetAttnums, ProcessCopyOptions, RELKIND_RELATION,
};

// C buffers per-row fwrite in libc's FILE; here fe_msgbuf retains rows until
// this watermark, so the write cadence (not per-row syscalls) matches C.
const FILE_FLUSH_THRESHOLD: usize = 65536;

enum CopyDest<'s> {
    File { fd: i32, filename: &'s str },
    Frontend,
}

pub struct CopyToState<'mcx, 's> {
    fe_msgbuf: StringInfo<'mcx>,
    dest: CopyDest<'s>,
    pub opts: CopyFormatOptions<'s>,
    attnumlist: PgVec<'mcx, i16>,
    force_quote_flags: PgVec<'mcx, bool>,
    file_encoding: i32,
    need_transcoding: bool,
    bytes_processed: u64,
    rowcx: MemoryContext,
    query_desc: Option<QueryDescHandle>,
    tupdesc: Option<Rc<TupleDescData<'static>>>,
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

/// `BeginCopyTo` (copyto.c), relation and query source arms. `query_rel_id`
/// is the OID of the base relation an RLS COPY was converted from.
pub fn BeginCopyTo<'mcx, 's>(
    mcx: Mcx<'mcx>,
    rel: Option<&Relation<'mcx>>,
    raw_query: Option<&RawStmt<'mcx>>,
    query_rel_id: types_core::Oid,
    filename: Option<&'s str>,
    attnamelist: &NodeList<'_>,
    options: &NodeList<'s>,
    source_text: Option<&str>,
) -> PgResult<CopyToState<'mcx, 's>> {
    if let Some(rel) = rel {
        if rel.rd_rel.relkind != RELKIND_RELATION
            && !(rel.rd_rel.relkind == b'm' && rel.rd_rel.relispopulated)
        {
            return Err(cannot_copy_from_relkind(rel));
        }
    }

    let opts = ProcessCopyOptions(false, options, source_text)?;

    let (query_desc, tupdesc) = match raw_query {
        None => (None, None),
        Some(raw_query) => {
            debug_assert!(rel.is_none());
            let query_string = source_text.expect("COPY (query) carries its source text");
            let (qd, td) = begin_copy_query(mcx, raw_query, query_rel_id, query_string)?;
            (Some(qd), Some(td))
        }
    };
    let tup_desc: &TupleDescData<'_> = match rel {
        Some(rel) => &rel.rd_att,
        None => tupdesc.as_deref().expect("ExecutorStart computed a result tupDesc"),
    };

    let attnumlist = CopyGetAttnums(mcx, tup_desc, rel, attnamelist)?;
    let force_quote_flags = force_flags(
        mcx,
        tup_desc,
        rel,
        &attnumlist,
        opts.force_quote,
        opts.force_quote_all,
        "FORCE_QUOTE",
    )?;

    let file_encoding = if opts.file_encoding < 0 {
        mbutils::pg_get_client_encoding()
    } else {
        opts.file_encoding
    };
    let need_transcoding = !(file_encoding == mbutils::GetDatabaseEncoding()
        || file_encoding == wchar::PG_SQL_ASCII);
    if !opts.binary && file_encoding >= wchar::PG_SJIS {
        // unported: COPY TO with a client-only encoding (pg_encoding_mblen escape walk)
        return Err(Box::new(
            PgError::error("COPY TO with a client-only encoding is not supported yet".to_string())
                .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }

    let dest = match filename {
        Some(filename) => {
            if !filename.starts_with('/') {
                return Err(Box::new(
                    PgError::error("relative path not allowed for COPY to file")
                        .with_sqlstate(ERRCODE_INVALID_NAME),
                ));
            }
            // SAFETY: process-global umask swap around open, as C's BeginCopyTo.
            // wasm32: no umask on WASI (files carry no mode bits).
            #[cfg(not(target_family = "wasm"))]
            let oumask = unsafe { libc::umask(0o022) };
            let copy_file = fd::AllocateFile(filename, "wb");
            // SAFETY: restore saved umask.
            #[cfg(not(target_family = "wasm"))]
            unsafe { libc::umask(oumask) };
            let copy_file = copy_file?;
            if copy_file < 0 {
                ereport(ERROR)
                    .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not open file \"{filename}\" for writing: %m"))
                    .errhint(
                        "COPY TO instructs the PostgreSQL server process to write a file. You \
                         may want a client-side facility such as psql's \\copy.",
                    )
                    .finish(loc("BeginCopyTo"))?;
            }
            let is_dir = fd::with_allocated_stdio(copy_file, |f| {
                f.metadata().map(|m| m.is_dir()).unwrap_or(false)
            })
            .unwrap_or(false);
            if is_dir {
                return Err(Box::new(
                    PgError::error(format!("\"{filename}\" is a directory"))
                        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
            CopyDest::File { fd: copy_file, filename }
        }
        None => {
            if elog::config::where_to_send_output() != CommandDest::Remote {
                unported("TO STDOUT outside a remote session (stdout file arm)");
            }
            CopyDest::Frontend
        }
    };

    pgstat_progress_start_command(
        PROGRESS_COMMAND_COPY,
        rel.map(|r| r.rd_id).unwrap_or(InvalidOid),
    );
    let progress_type = match dest {
        CopyDest::File { .. } => PROGRESS_COPY_TYPE_FILE,
        CopyDest::Frontend => PROGRESS_COPY_TYPE_PIPE,
    };
    pgstat_progress_update_multi_param(
        &[PROGRESS_COPY_COMMAND, PROGRESS_COPY_TYPE],
        &[PROGRESS_COPY_COMMAND_TO, progress_type],
    );

    Ok(CopyToState {
        fe_msgbuf: StringInfo::new_in(mcx)?,
        dest,
        opts,
        attnumlist,
        force_quote_flags,
        file_encoding,
        need_transcoding,
        bytes_processed: 0,
        rowcx: MemoryContext::new_bump("COPY TO"),
        query_desc,
        tupdesc,
    })
}

// BeginCopyTo (copyto.c), the raw_query arm: analyze/rewrite, plan, snapshot
// push, DestCopyOut QueryDesc, ExecutorStart.
fn begin_copy_query<'mcx>(
    mcx: Mcx<'mcx>,
    raw_query: &RawStmt<'mcx>,
    query_rel_id: types_core::Oid,
    query_string: &str,
) -> PgResult<(QueryDescHandle, Rc<TupleDescData<'static>>)> {
    let rewritten = postgres::simple_query::pg_analyze_and_rewrite_fixedparams(
        mcx,
        raw_query,
        query_string,
        &[],
        QueryEnvHandle::NULL,
    )?;

    if rewritten.is_empty() {
        return Err(feature_not_supported(
            "DO INSTEAD NOTHING rules are not supported for COPY",
        ));
    }
    if rewritten.len() > 1 {
        for q in rewritten.iter() {
            if q.querySource == QuerySource::QSRC_QUAL_INSTEAD_RULE {
                return Err(feature_not_supported(
                    "conditional DO INSTEAD rules are not supported for COPY",
                ));
            }
            if q.querySource == QuerySource::QSRC_NON_INSTEAD_RULE {
                return Err(feature_not_supported(
                    "DO ALSO rules are not supported for COPY",
                ));
            }
        }
        return Err(feature_not_supported(
            "multi-statement DO INSTEAD rules are not supported for COPY",
        ));
    }

    let query = rewritten.into_iter().next().expect("checked non-empty");

    if let Some(u) = query.utilityStmt {
        if u.as_variant::<CreateTableAsStmt>().is_some() {
            return Err(feature_not_supported("COPY (SELECT INTO) is not supported"));
        }
        return Err(feature_not_supported("COPY query must not be a utility command"));
    }
    if query.commandType != CmdType::CMD_SELECT && query.returningList.is_nil() {
        return Err(feature_not_supported("COPY query must have a RETURNING clause"));
    }

    let plan = postgres::simple_query::pg_plan_query(
        mcx,
        mcx::leak_in(mcx::alloc_in(mcx, query)?),
        query_string,
        CURSOR_OPT_PARALLEL_OK,
        ParamListHandle::NULL,
    )?
    .expect("planner handles non-utility commands");

    // An RLS-converted COPY passed in the relid it checked policies on and
    // locked; the planner looked the name up again, so re-verify it resolved
    // to the same relation (copyto.c BeginCopyTo).
    if query_rel_id != InvalidOid && !plan.relationOids.iter().any(|o| o == query_rel_id) {
        return Err(Box::new(
            PgError::error("relation referenced by COPY statement has changed")
                .with_sqlstate(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
        ));
    }

    // Arena-pin the plan: create_query_desc's retention contract holds it
    // until free_query_desc, past this frame.
    let plan = mcx::leak_in(mcx::alloc_in(mcx, plan)?);

    snapmgr::PushCopiedSnapshot(&snapmgr::GetActiveSnapshot())?;
    snapmgr::UpdateActiveSnapshotCommandId()?;

    let qd = execmain_seams::create_query_desc::call(
        plan,
        query_string,
        Some(snapmgr::GetActiveSnapshot()),
        None,
        CommandDest::CopyOut,
        ParamListHandle::NULL,
        QueryEnvHandle::NULL,
        0,
    )?;
    execmain_seams::executor_start::call(qd, 0)?;
    let tupdesc = execmain_seams::query_desc_result_tupdesc::call(qd)
        .expect("ExecutorStart computed a result tupDesc");
    Ok((qd, tupdesc))
}

#[track_caller]
#[cold]
#[inline(never)]
fn feature_not_supported(msg: &str) -> Box<PgError> {
    Box::new(PgError::error(msg.to_string()).with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED))
}

/// `DoCopyTo` (copyto.c): scan the relation or run the query plan, emit every
/// visible row.
pub fn DoCopyTo<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyToState<'mcx, '_>,
    rel: Option<&Relation<'mcx>>,
) -> PgResult<u64> {
    let query_tupdesc = cstate.tupdesc.clone();
    let tup_desc: &TupleDescData<'_> = match rel {
        Some(rel) => &rel.rd_att,
        None => query_tupdesc.as_deref().expect("query COPY carries the executor tupDesc"),
    };

    // FmgrInfo carries droppy fn_extra, so PgVec::new_in (printtup precedent);
    // resolve-once, never per row (rule 4).
    let mut out_functions: PgVec<'mcx, FmgrInfo> = PgVec::new_in(mcx);
    out_functions.try_reserve_exact(cstate.attnumlist.len()).map_err(|_| {
        mcx.oom(cstate.attnumlist.len() * core::mem::size_of::<FmgrInfo>())
    })?;
    for &attnum in cstate.attnumlist.iter() {
        let attr = tup_desc.attr(attnum as usize - 1);
        let (func_oid, _is_varlena) = if cstate.opts.binary {
            lsyscache::typ::getTypeBinaryOutputInfo(attr.atttypid)?
        } else {
            lsyscache::typ::getTypeOutputInfo(attr.atttypid)?
        };
        out_functions.push(fmgr_core::fmgr_info(func_oid)?);
    }

    if matches!(cstate.dest, CopyDest::Frontend) {
        send_copy_begin(mcx, cstate.attnumlist.len(), cstate.opts.binary)?;
    }

    if cstate.opts.binary {
        // CopyToBinaryStart: signature, flags, no header extension.
        cstate.fe_msgbuf.append_bytes(&BINARY_SIGNATURE)?;
        cstate.fe_msgbuf.append_bytes(&0i32.to_be_bytes())?;
        cstate.fe_msgbuf.append_bytes(&0i32.to_be_bytes())?;
    }

    if cstate.opts.header_line != crate::CopyHeaderChoice::False {
        let mut hdr_delim = false;
        let single_attr = cstate.attnumlist.len() == 1;
        for &attnum in cstate.attnumlist.iter() {
            if hdr_delim {
                cstate.fe_msgbuf.append_byte(cstate.opts.delim)?;
            }
            hdr_delim = true;
            let colname = tup_desc.attr(attnum as usize - 1).attname;
            if cstate.opts.csv_mode {
                copy_attribute_out_csv(
                    &mut cstate.fe_msgbuf,
                    colname.name_str(),
                    &cstate.opts,
                    false,
                    single_attr,
                )?;
            } else {
                copy_attribute_out_text(
                    &mut cstate.fe_msgbuf,
                    colname.name_str(),
                    cstate.opts.delim,
                )?;
            }
        }
        end_of_row(cstate)?;
    }

    let processed: u64 = match rel {
        Some(rel) => {
            let snapshot = Some(snapmgr::GetActiveSnapshot());
            let mut scandesc =
                tableam::table_beginscan(mcx, rel, snapshot, 0, PgVec::new_in(mcx))?;
            let mut slot = tableam::table_slot_create(mcx, rel)?;

            let mut processed: u64 = 0;
            while tableam::table_scan_getnextslot(
                mcx,
                &mut scandesc,
                ForwardScanDirection,
                &mut slot,
            )? {
                exectuples::slot_getallattrs(&mut slot);
                CopyOneRowTo(cstate, &mut slot, &mut out_functions)?;
                processed += 1;
                pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, processed as i64);
            }

            tableam::table_endscan(scandesc)?;
            processed
        }
        None => {
            let qd = cstate.query_desc.expect("query COPY has a QueryDesc");
            let mut frame = QueryFrame { cstate, out_functions: &mut out_functions };
            let mut dest = tcop_dest::DestReceiver::CopyOut(copy_seams::CopyDestState::new(
                (&mut frame as *mut QueryFrame<'_, '_, '_>).cast(),
            ));
            execmain_seams::executor_run::call(
                qd,
                types_scan::sdir::ScanDirection::ForwardScanDirection,
                0,
                &mut dest,
            )?;
            let tcop_dest::DestReceiver::CopyOut(st) = &dest else { unreachable!() };
            st.processed
        }
    };
    if cstate.opts.binary {
        // CopyToBinaryEnd: -1 tuple-count trailer, then flush.
        cstate.fe_msgbuf.append_bytes(&(-1i16).to_be_bytes())?;
        send_end_of_row(cstate)?;
    }
    match cstate.dest {
        CopyDest::File { .. } => flush_to_file(cstate)?,
        CopyDest::Frontend => {
            // SendCopyEnd: no unsent data, then CopyDone.
            debug_assert!(cstate.fe_msgbuf.is_empty());
            pqformat::pq_putemptymessage(b'c')?;
        }
    }
    Ok(processed)
}

struct QueryFrame<'f, 'mcx, 's> {
    cstate: &'f mut CopyToState<'mcx, 's>,
    out_functions: &'f mut [FmgrInfo],
}

// copy_dest_receive (copyto.c); installed on copy_seams::copy_dest_receive.
pub(crate) fn copy_dest_receive<'mcx>(
    state: &mut copy_seams::CopyDestState,
    slot: &mut SlotData<'mcx>,
) -> PgResult<bool> {
    // SAFETY: `frame` points at DoCopyTo's QueryFrame, live for the whole
    // executor_run window; lifetimes re-derived under that retention contract.
    let frame = unsafe { &mut *(state.frame as *mut QueryFrame<'_, 'mcx, '_>) };
    exectuples::slot_getallattrs(slot);
    CopyOneRowTo(frame.cstate, slot, frame.out_functions)?;
    state.processed += 1;
    pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, state.processed as i64);
    Ok(true)
}

// SendCopyBegin (copyto.c): CopyOutResponse.
fn send_copy_begin(mcx: Mcx<'_>, natts: usize, binary: bool) -> PgResult<()> {
    let format: u16 = if binary { 1 } else { 0 };
    let mut buf = pqformat::pq_beginmessage(mcx, b'H')?;
    pqformat::pq_sendbyte(&mut buf, format as u8)?;
    pqformat::pq_sendint16(&mut buf, natts as u16)?;
    for _ in 0..natts {
        pqformat::pq_sendint16(&mut buf, format)?;
    }
    pqformat::pq_endmessage(buf)
}

const BINARY_SIGNATURE: [u8; 11] = *b"PGCOPY\n\xff\r\n\0";

/// `CopyOneRowTo` + `CopyToTextLikeOneRow` (copyto.c).
fn CopyOneRowTo<'mcx>(
    cstate: &mut CopyToState<'mcx, '_>,
    slot: &mut SlotData<'mcx>,
    out_functions: &mut [FmgrInfo],
) -> PgResult<()> {
    let CopyToState {
        rowcx,
        fe_msgbuf,
        opts,
        attnumlist,
        force_quote_flags,
        need_transcoding,
        file_encoding,
        ..
    } = cstate;
    rowcx.reset();
    let rmcx = rowcx.mcx();

    let base = slot.base();
    if opts.binary {
        fe_msgbuf.append_bytes(&(attnumlist.len() as i16).to_be_bytes())?;
        for (i, &attnum) in attnumlist.iter().enumerate() {
            let m = attnum as usize - 1;
            if base.tts_isnull[m] {
                fe_msgbuf.append_bytes(&(-1i32).to_be_bytes())?;
                continue;
            }
            let out = send_function_call(&mut out_functions[i], base.tts_values[m], rmcx)?;
            // SAFETY: send fns return an untoasted bytea image (C's
            // DatumGetByteaP); external/compressed panics in from_ptr.
            let v = unsafe { PackedVarlena::from_ptr(out.as_usize() as *const u8) };
            let data = v.data();
            fe_msgbuf.append_bytes(&(data.len() as i32).to_be_bytes())?;
            fe_msgbuf.append_bytes(data)?;
        }
        return send_end_of_row(cstate);
    }

    let single_attr = attnumlist.len() == 1;
    let mut need_delim = false;
    for (i, &attnum) in attnumlist.iter().enumerate() {
        let m = attnum as usize - 1;
        if need_delim {
            fe_msgbuf.append_byte(opts.delim)?;
        }
        need_delim = true;

        if base.tts_isnull[m] {
            // null_print is validated ASCII-safe (no \r/\n/delim); C converts
            // it once per COPY, identity under the live encodings.
            fe_msgbuf.append_bytes(opts.null_print.as_bytes())?;
            continue;
        }
        let value: Datum = base.tts_values[m];
        let out = function_call1_coll_in(&mut out_functions[i], InvalidOid, rmcx, value)?;
        // SAFETY: text output fns return a NUL-terminated cstring datum
        // (printtup precedent).
        let s = unsafe { CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) }.to_bytes();
        let s: &[u8] = if *need_transcoding {
            match mbutils::pg_server_to_any(rmcx, s, *file_encoding)? {
                Some(converted) => {
                    let (ptr, len) = (converted.as_ptr(), converted.len());
                    // SAFETY: converted lives in rmcx until the next row reset.
                    unsafe { core::slice::from_raw_parts(ptr, len) }
                }
                None => s,
            }
        } else {
            s
        };
        if opts.csv_mode {
            copy_attribute_out_csv(fe_msgbuf, s, opts, force_quote_flags[m], single_attr)?;
        } else {
            copy_attribute_out_text(fe_msgbuf, s, opts.delim)?;
        }
    }
    end_of_row(cstate)
}

/// `CopyAttributeOutCSV` (copyto.c). The null_print comparison happens before
/// transcoding in C; the transcoded-vs-raw distinction is inert under the
/// supported (non-ASCII-embedding) encodings, so `s` here is post-conversion.
pub(crate) fn copy_attribute_out_csv(
    buf: &mut StringInfo<'_>,
    s: &[u8],
    opts: &CopyFormatOptions<'_>,
    use_quote: bool,
    single_attr: bool,
) -> PgResult<()> {
    let delimc = opts.delim;
    let quotec = opts.quote;
    let escapec = opts.escape;

    let mut use_quote = use_quote || s == opts.null_print.as_bytes();
    if !use_quote {
        // Quote a lone \. so older versions and PQgetline keep loading it.
        if single_attr && s == b"\\." {
            use_quote = true;
        } else {
            use_quote = s
                .iter()
                .any(|&c| c == delimc || c == quotec || c == b'\n' || c == b'\r');
        }
    }

    if use_quote {
        buf.append_byte(quotec)?;
        let mut start = 0usize;
        for (i, &c) in s.iter().enumerate() {
            if c == quotec || c == escapec {
                buf.append_bytes(&s[start..i])?;
                buf.append_byte(escapec)?;
                start = i;
            }
        }
        buf.append_bytes(&s[start..])?;
        buf.append_byte(quotec)?;
    } else {
        buf.append_bytes(s)?;
    }
    Ok(())
}

// CopySendTextLikeEndOfRow.
fn end_of_row(cstate: &mut CopyToState<'_, '_>) -> PgResult<()> {
    cstate.fe_msgbuf.append_byte(b'\n')?;
    send_end_of_row(cstate)
}

// CopySendEndOfRow.
fn send_end_of_row(cstate: &mut CopyToState<'_, '_>) -> PgResult<()> {
    match cstate.dest {
        CopyDest::File { .. } => {
            if cstate.fe_msgbuf.len() >= FILE_FLUSH_THRESHOLD {
                flush_to_file(cstate)?;
            }
        }
        CopyDest::Frontend => {
            pqcomm::pq_putmessage(b'd', cstate.fe_msgbuf.as_bytes())?;
            cstate.bytes_processed += cstate.fe_msgbuf.len() as u64;
            pgstat_progress_update_param(
                PROGRESS_COPY_BYTES_PROCESSED,
                cstate.bytes_processed as i64,
            );
            cstate.fe_msgbuf.reset();
        }
    }
    Ok(())
}

fn flush_to_file(cstate: &mut CopyToState<'_, '_>) -> PgResult<()> {
    let CopyDest::File { fd, .. } = cstate.dest else {
        panic!("COPY TO: flush_to_file on non-file destination")
    };
    if cstate.fe_msgbuf.is_empty() {
        return Ok(());
    }
    let bytes = cstate.fe_msgbuf.as_bytes();
    let wrote = fd::with_allocated_stdio(fd, |f| {
        use std::io::Write;
        f.write_all(bytes)
    });
    match wrote {
        Some(Ok(())) => {}
        Some(Err(e)) => {
            ereport(ERROR)
                .with_saved_errno(e.raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg("could not write to COPY file: %m")
                .finish(loc("CopySendEndOfRow"))?;
        }
        None => panic!("COPY TO: AllocateFile index {fd} vanished"),
    }
    cstate.bytes_processed += cstate.fe_msgbuf.len() as u64;
    pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, cstate.bytes_processed as i64);
    cstate.fe_msgbuf.reset();
    Ok(())
}

/// `EndCopyTo` + `EndCopy` (copyto.c).
pub fn EndCopyTo(mut cstate: CopyToState<'_, '_>) -> PgResult<()> {
    if let Some(qd) = cstate.query_desc.take() {
        execmain_seams::executor_finish::call(qd)?;
        execmain_seams::executor_end::call(qd)?;
        execmain_seams::free_query_desc::call(qd);
        snapmgr::PopActiveSnapshot()?;
    }
    if let CopyDest::File { fd, filename } = cstate.dest {
        flush_to_file(&mut cstate)?;
        if fd::FreeFile(fd)? != 0 {
            ereport(ERROR)
                .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not close file \"{filename}\": %m"))
                .finish(loc("EndCopy"))?;
        }
    }
    pgstat_progress_end_command();
    Ok(())
}

/// `CopyAttributeOutText` (copyto.c), server-encoding arm: chunked dump with
/// the exact C escape table (\b \f \n \r \t \v, backslash, delimiter).
pub fn copy_attribute_out_text(
    buf: &mut StringInfo<'_>,
    s: &[u8],
    delimc: u8,
) -> PgResult<()> {
    let mut start = 0usize;
    let mut ptr = 0usize;
    while ptr < s.len() {
        let c = s[ptr];
        // Bitwise-OR'd predicate: one predicted branch per clean byte (the
        // 3-branch ladder stalled 65% of cycles on V2 — see copy_cmd record).
        if (c < 0x20) | (c == b'\\') | (c == delimc) {
            if c < 0x20 {
                let esc = match c {
                    b'\x08' => b'b',
                    b'\x0c' => b'f',
                    b'\n' => b'n',
                    b'\r' => b'r',
                    b'\t' => b't',
                    b'\x0b' => b'v',
                    _ => {
                        if c == delimc {
                            c
                        } else {
                            ptr += 1;
                            continue;
                        }
                    }
                };
                if ptr > start {
                    buf.append_bytes(&s[start..ptr])?;
                }
                buf.append_byte(b'\\')?;
                buf.append_byte(esc)?;
                ptr += 1;
                start = ptr;
            } else {
                if ptr > start {
                    buf.append_bytes(&s[start..ptr])?;
                }
                buf.append_byte(b'\\')?;
                start = ptr;
                ptr += 1;
            }
        } else {
            ptr += 1;
        }
    }
    if ptr > start {
        buf.append_bytes(&s[start..ptr])?;
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_copy_from_relkind(rel: &Relation<'_>) -> Box<PgError> {
    let name = rel.name();
    let (msg, hint): (String, Option<&str>) = match rel.rd_rel.relkind {
        b'v' => (
            format!("cannot copy from view \"{name}\""),
            Some("Try the COPY (SELECT ...) TO variant."),
        ),
        b'm' => (
            format!("cannot copy from unpopulated materialized view \"{name}\""),
            Some("Use the REFRESH MATERIALIZED VIEW command."),
        ),
        b'f' => (
            format!("cannot copy from foreign table \"{name}\""),
            Some("Try the COPY (SELECT ...) TO variant."),
        ),
        b'S' => (format!("cannot copy from sequence \"{name}\""), None),
        b'p' => (
            format!("cannot copy from partitioned table \"{name}\""),
            Some("Try the COPY (SELECT ...) TO variant."),
        ),
        _ => (format!("cannot copy from non-table relation \"{name}\""), None),
    };
    let mut e = PgError::error(msg).with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE);
    if let Some(h) = hint {
        e = e.with_hint(h);
    }
    Box::new(e)
}
