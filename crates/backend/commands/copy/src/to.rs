// copyto.c, text/CSV/binary formats to file or frontend, from a relation scan
// or a query executor run (DestCopyOut). The callback destination
// (data_dest_cb) has no producer here.

use core::ffi::CStr;
use std::rc::Rc;

use backend_progress::progress::{
    PROGRESS_COPY_BYTES_PROCESSED, PROGRESS_COPY_COMMAND, PROGRESS_COPY_COMMAND_TO,
    PROGRESS_COPY_TUPLES_PROCESSED, PROGRESS_COPY_TYPE, PROGRESS_COPY_TYPE_FILE,
    PROGRESS_COPY_TYPE_PIPE, PROGRESS_COPY_TYPE_PROGRAM,
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
    force_flags, CopyFormatOptions, CopyGetAttnums, ProcessCopyOptions, RELKIND_RELATION,
};

// C buffers per-row fwrite in libc's FILE; here fe_msgbuf retains rows until
// this watermark, so the write cadence (not per-row syscalls) matches C.
const FILE_FLUSH_THRESHOLD: usize = 65536;

// Final mode of a COPY TO server file. C's BeginCopyTo opens under a temporary
// umask of S_IWGRP|S_IWOTH (0o022), so the fopen default 0666 resolves to
// 0666 & ~0o022 = 0o644. We set this explicitly (see BeginCopyTo) rather than
// mutate the process-global umask, which is shared across all backend threads.
#[cfg(not(target_family = "wasm"))]
const COPY_TO_FILE_MODE: u32 = 0o644;

enum CopyDest<'s> {
    File { fd: i32, filename: &'s str },
    // COPY TO PROGRAM (copyto.c is_program arm): `fd` is an OpenPipeStream
    // index; rows stream into the child's stdin. Closed via
    // ClosePipeToProgram in EndCopyTo (any nonzero exit is an error).
    Program { fd: i32, filename: &'s str },
    Frontend,
    // copyto.c:919 (`cstate->copy_file = stdout`): the pipe destination of a
    // non-remote session — single-user mode's COPY ... TO STDOUT. Raw copy
    // bytes go to the process's stdout, interleaved with debugtup rows.
    Stdout,
}

// Owning handle for the query-COPY QueryDesc registry entry (mirrors
// pquery::QueryDescOwner; the execmain audit E-4 law): both Err returns and
// loud panics between BeginCopyTo's create and EndCopyTo's free must release
// the entry, or the EState's relcache refs survive the statement and every
// later CheckTableNotInUse in the session sees a phantom "active query"
// (a fuzzing round inuse-guard: COPY (query) TO erroring mid-run left VACUUM
// FULL raising 55006 where C succeeds). EndCopyTo disarms on its clean path.
struct OwnedQueryDesc(QueryDescHandle);

impl OwnedQueryDesc {
    fn disarm(&mut self) {
        self.0 = QueryDescHandle::NULL;
    }
}

impl Drop for OwnedQueryDesc {
    fn drop(&mut self) {
        if !self.0.is_null() {
            execmain_seams::release_query_desc::call(self.0);
        }
    }
}

pub struct CopyToState<'mcx, 's> {
    fe_msgbuf: StringInfo<'mcx>,
    dest: CopyDest<'s>,
    // C's copy_file is a stdio FILE: rows accumulate in its st_blksize buffer
    // and reach the fd only when that fills (fwrite) or at fclose (EndCopy),
    // which is where a write error on a short COPY surfaces ("could not
    // close file", copyto.c:596) rather than per row. `file_buf` mirrors that
    // buffer for the File destination; `file_bufsize` is the stream's
    // st_blksize (BUFSIZ when unknown, as __smakebuf).
    file_buf: Vec<u8>,
    file_bufsize: usize,
    pub opts: CopyFormatOptions<'s>,
    attnumlist: PgVec<'mcx, i16>,
    force_quote_flags: PgVec<'mcx, bool>,
    file_encoding: i32,
    need_transcoding: bool,
    // ASCII can be a non-first byte of a multibyte character (client-only
    // encodings): the escape walks must skip whole characters.
    encoding_embeds_ascii: bool,
    // opts.null_print converted to the file encoding (CopyToTextLikeStart);
    // None while no conversion applies.
    null_print_client: Option<PgVec<'mcx, u8>>,
    bytes_processed: u64,
    // Prefix of fe_msgbuf already counted into bytes_processed: the buffered
    // destinations defer the write past C's per-row fwrite, but the progress
    // accounting stays per row (copyto.c:494-497).
    fe_msgbuf_accounted: usize,
    rowcx: MemoryContext,
    query_desc: Option<OwnedQueryDesc>,
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
pub fn BeginCopyTo<'mcx: 's, 's>(
    mcx: Mcx<'mcx>,
    rel: Option<&Relation<'mcx>>,
    raw_query: Option<&RawStmt<'mcx>>,
    query_rel_id: types_core::Oid,
    filename: Option<&'s str>,
    is_program: bool,
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

    let opts = ProcessCopyOptions(mcx, false, options, source_text)?;

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
    let encoding_embeds_ascii = wchar::pg_encoding_is_client_only(file_encoding);

    // stdio buffer size of the File destination (see CopyToState).
    let mut file_bufsize = stdio_bufsize(0);
    let dest = match filename {
        Some(filename) if is_program => {
            // copyto.c is_program arm: popen the command, write its stdin.
            let copy_file = fd::OpenPipeStream(filename, "w")?;
            if copy_file < 0 {
                ereport(ERROR)
                    .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not execute command \"{filename}\": %m"))
                    .finish(loc("BeginCopyTo"))?;
            }
            CopyDest::Program { fd: copy_file, filename }
        }
        Some(filename) => {
            if !filename.starts_with('/') {
                return Err(Box::new(
                    PgError::error("relative path not allowed for COPY to file")
                        .with_sqlstate(ERRCODE_INVALID_NAME),
                ));
            }
            // C's BeginCopyTo temporarily sets the process umask to
            // S_IWGRP|S_IWOTH (0o022) around this open (copyto.c:952-961), so
            // a file the fopen CREATES lands at the fopen default 0666 masked
            // to COPY_TO_FILE_MODE (0o644) — while a file that already exists
            // is only truncated: fopen never touches its mode, whoever owns it.
            // umask(2) is process-global; in pgrust every backend is a thread
            // of one process (unlike C's process-per-backend), so mutating it
            // would race every other thread's concurrent file creation, and
            // interleaved save/restore pairs could corrupt the process umask
            // durably. Instead note whether the target pre-exists, open under
            // the server's own umask, and set the exact resulting mode on the
            // fd only for a file this open created — exactly as syslogger's
            // logfile_open does for this same hazard. (metadata follows
            // symlinks, as fopen does.)
            #[cfg_attr(target_family = "wasm", allow(unused_variables))]
            let preexisting = std::fs::metadata(filename).is_ok();
            let copy_file = fd::AllocateFile(filename, "wb")?;
            if copy_file < 0 {
                // copy errno because ereport subfunctions might change it
                let save_errno = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
                let mut e = ereport(ERROR)
                    .with_saved_errno(save_errno)
                    .errcode_for_file_access()
                    .errmsg(format!("could not open file \"{filename}\" for writing: %m"));
                if crate::open_failure_hint_applies(save_errno) {
                    e = e.errhint(
                        "COPY TO instructs the PostgreSQL server process to write a file. You \
                         may want a client-side facility such as psql's \\copy.",
                    );
                }
                e.finish(loc("BeginCopyTo"))?;
            }
            // fchmod a file we created to C's resulting mode without touching
            // process-global umask; a pre-existing file keeps its mode (C's
            // fopen truncates it and nothing else — a 0600 file stays 0600,
            // and a group-writable file owned by another user, where fchmod
            // would fail with EPERM, is written as C writes it).
            // wasm32/WASI carries no mode bits, so this is a no-op there.
            #[cfg(not(target_family = "wasm"))]
            {
                use std::os::unix::fs::PermissionsExt;
                let set = if preexisting {
                    None
                } else {
                    fd::with_allocated_stdio(copy_file, |f| {
                        f.set_permissions(std::fs::Permissions::from_mode(COPY_TO_FILE_MODE))
                    })
                };
                if let Some(Err(e)) = set {
                    ereport(ERROR)
                        .with_saved_errno(e.raw_os_error().unwrap_or(0))
                        .errcode_for_file_access()
                        .errmsg(format!("could not set mode of file \"{filename}\": %m"))
                        .finish(loc("BeginCopyTo"))?;
                }
            }
            // fstat (copyto.c:976): a failure is an error, never a silent
            // "not a directory, default buffer".
            let (is_dir, blksize) = match fd::with_allocated_stdio(copy_file, |f| f.metadata()) {
                Some(Ok(m)) => {
                    #[cfg(not(target_family = "wasm"))]
                    let blksize = {
                        use std::os::unix::fs::MetadataExt;
                        m.blksize() as usize
                    };
                    // wasi stat carries no st_blksize: stdio_bufsize's default.
                    #[cfg(target_family = "wasm")]
                    let blksize = 0usize;
                    (m.is_dir(), blksize)
                }
                Some(Err(e)) => return Err(crate::could_not_stat_file(filename, &e)),
                None => panic!("COPY TO: AllocateFile index {copy_file} vanished"),
            };
            if is_dir {
                return Err(Box::new(
                    PgError::error(format!("\"{filename}\" is a directory"))
                        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
            file_bufsize = stdio_bufsize(blksize);
            CopyDest::File { fd: copy_file, filename }
        }
        None => {
            // copyto.c:918: a pipe COPY outside a remote session (single-user
            // mode) writes to the process's stdout instead of the frontend.
            if elog::config::where_to_send_output() != CommandDest::Remote {
                CopyDest::Stdout
            } else {
                CopyDest::Frontend
            }
        }
    };

    pgstat_progress_start_command(
        PROGRESS_COMMAND_COPY,
        rel.map(|r| r.rd_id).unwrap_or(InvalidOid),
    );
    let progress_type = match dest {
        CopyDest::File { .. } => PROGRESS_COPY_TYPE_FILE,
        CopyDest::Program { .. } => PROGRESS_COPY_TYPE_PROGRAM,
        // C sets PIPE for the whole pipe arm, stdout included (copyto.c:915).
        CopyDest::Frontend | CopyDest::Stdout => PROGRESS_COPY_TYPE_PIPE,
    };
    pgstat_progress_update_multi_param(
        &[PROGRESS_COPY_COMMAND, PROGRESS_COPY_TYPE],
        &[PROGRESS_COPY_COMMAND_TO, progress_type],
    );

    Ok(CopyToState {
        fe_msgbuf: StringInfo::new_in(mcx)?,
        file_buf: Vec::new(),
        file_bufsize,
        dest,
        opts,
        attnumlist,
        force_quote_flags,
        encoding_embeds_ascii,
        null_print_client: None,
        file_encoding,
        need_transcoding,
        bytes_processed: 0,
        fe_msgbuf_accounted: 0,
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
) -> PgResult<(OwnedQueryDesc, Rc<TupleDescData<'static>>)> {
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
    // Own the entry from creation: an ExecutorStart error — or any error
    // between here and EndCopyTo's clean shutdown — must release it, or its
    // EState's relcache pins outlive the statement (see OwnedQueryDesc).
    let owner = OwnedQueryDesc(qd);
    execmain_seams::executor_start::call(qd, 0)?;
    let tupdesc = execmain_seams::query_desc_result_tupdesc::call(qd)
        .expect("ExecutorStart computed a result tupDesc");
    Ok((owner, tupdesc))
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

    // copyto.c:1035: CopyOutResponse goes out BEFORE the per-column output
    // function lookup, so a type without a binary send function fails inside
    // the copy (CopyOutResponse, then ErrorResponse). libpq-side readers
    // depend on that order: the logical tablesync worker sees COPY_OUT, starts
    // its own BeginCopyFrom, and reports "no binary input function" (its
    // side) first — src/test/subscription 014_binary waits for that line.
    if matches!(cstate.dest, CopyDest::Frontend) {
        send_copy_begin(mcx, cstate.attnumlist.len(), cstate.opts.binary)?;
    }

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

    if cstate.opts.binary {
        // CopyToBinaryStart: signature, flags, no header extension.
        cstate.fe_msgbuf.append_bytes(&BINARY_SIGNATURE)?;
        cstate.fe_msgbuf.append_bytes(&0i32.to_be_bytes())?;
        cstate.fe_msgbuf.append_bytes(&0i32.to_be_bytes())?;
    }

    // CopyToTextLikeStart: convert null_print to the file encoding once; it
    // is sent as-is for NULL columns.
    if !cstate.opts.binary && cstate.need_transcoding {
        cstate.null_print_client = mbutils::pg_server_to_any(
            mcx,
            cstate.opts.null_print.as_bytes(),
            cstate.file_encoding,
        )?;
    }

    if cstate.opts.header_line != crate::CopyHeaderChoice::False {
        let mut hdr_delim = false;
        let single_attr = cstate.attnumlist.len() == 1;
        let mb_encoding = cstate.encoding_embeds_ascii.then_some(cstate.file_encoding);
        for &attnum in cstate.attnumlist.iter() {
            if hdr_delim {
                cstate.fe_msgbuf.append_byte(cstate.opts.delim)?;
            }
            hdr_delim = true;
            let colname = tup_desc.attr(attnum as usize - 1).attname;
            // C's writers transcode internally; the null_print force-quote
            // comparison happens on the pre-conversion bytes.
            let matches_null = colname.name_str() == cstate.opts.null_print.as_bytes();
            let converted;
            let name_bytes: &[u8] = if cstate.need_transcoding {
                match mbutils::pg_server_to_any(mcx, colname.name_str(), cstate.file_encoding)? {
                    Some(c) => {
                        converted = c;
                        &converted
                    }
                    None => colname.name_str(),
                }
            } else {
                colname.name_str()
            };
            if cstate.opts.csv_mode {
                copy_attribute_out_csv(
                    &mut cstate.fe_msgbuf,
                    name_bytes,
                    &cstate.opts,
                    matches_null,
                    single_attr,
                    mb_encoding,
                )?;
            } else if let Some(enc) = mb_encoding {
                copy_attribute_out_text_embedded(
                    &mut cstate.fe_msgbuf,
                    name_bytes,
                    cstate.opts.delim,
                    enc,
                )?;
            } else {
                copy_attribute_out_text(&mut cstate.fe_msgbuf, name_bytes, cstate.opts.delim)?;
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
                // copyto.c:1082: per row, not only at the scan's block
                // boundaries — a cancel lands on the very next tuple.
                postgres_seams::check_for_interrupts::call()?;

                exectuples::slot_getallattrs(&mut slot);
                CopyOneRowTo(cstate, &mut slot, &mut out_functions)?;
                processed += 1;
                pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, processed as i64);
            }

            tableam::table_endscan(scandesc)?;
            processed
        }
        None => {
            let qd = cstate.query_desc.as_ref().expect("query COPY has a QueryDesc").0;
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
        CopyDest::Program { .. } => flush_to_program(cstate)?,
        CopyDest::Stdout => flush_to_stdout(cstate)?,
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
        encoding_embeds_ascii,
        null_print_client,
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
            // null_print_client: converted once per COPY (CopyToTextLikeStart).
            let np: &[u8] = match null_print_client {
                Some(v) => v,
                None => opts.null_print.as_bytes(),
            };
            fe_msgbuf.append_bytes(np)?;
            continue;
        }
        let value: Datum = base.tts_values[m];
        let out = function_call1_coll_in(&mut out_functions[i], InvalidOid, rmcx, value)?;
        // SAFETY: text output fns return a NUL-terminated cstring datum
        // (printtup precedent).
        let s = unsafe { CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) }.to_bytes();
        // C compares against null_print before conversion (copyto.c:1311).
        let matches_null = opts.csv_mode && s == opts.null_print.as_bytes();
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
        let mb_encoding = encoding_embeds_ascii.then_some(*file_encoding);
        if opts.csv_mode {
            copy_attribute_out_csv(
                fe_msgbuf,
                s,
                opts,
                force_quote_flags[m] || matches_null,
                single_attr,
                mb_encoding,
            )?;
        } else if let Some(enc) = mb_encoding {
            copy_attribute_out_text_embedded(fe_msgbuf, s, opts.delim, enc)?;
        } else {
            copy_attribute_out_text(fe_msgbuf, s, opts.delim)?;
        }
    }
    end_of_row(cstate)
}

// The next multibyte-aware step from byte i of s: pg_encoding_mblen for a
// high-bit lead byte under an ASCII-embedding encoding, else 1. C's walk can
// step past a truncated final character (and reads on until a NUL); the
// clamp keeps the same dump boundary without the overread.
#[inline]
fn mb_step(mblen_encoding: Option<i32>, s: &[u8], i: usize) -> usize {
    match mblen_encoding {
        Some(enc) if s[i] & 0x80 != 0 => {
            (wchar::pg_encoding_mblen(enc, &s[i..]) as usize).min(s.len() - i)
        }
        _ => 1,
    }
}

/// `CopyAttributeOutCSV` (copyto.c). C compares the pre-conversion string
/// against null_print for forced quoting; callers fold that comparison into
/// `use_quote` since `s` here is post-conversion. `mblen_encoding` is set for
/// ASCII-embedding (client-only) file encodings.
pub(crate) fn copy_attribute_out_csv(
    buf: &mut StringInfo<'_>,
    s: &[u8],
    opts: &CopyFormatOptions<'_>,
    use_quote: bool,
    single_attr: bool,
    mblen_encoding: Option<i32>,
) -> PgResult<()> {
    let delimc = opts.delim;
    let quotec = opts.quote;
    let escapec = opts.escape;

    let mut use_quote = use_quote;
    if !use_quote {
        // Quote a lone \. so older versions and PQgetline keep loading it.
        if single_attr && s == b"\\." {
            use_quote = true;
        } else {
            let mut i = 0usize;
            while i < s.len() {
                let c = s[i];
                if c == delimc || c == quotec || c == b'\n' || c == b'\r' {
                    use_quote = true;
                    break;
                }
                i += mb_step(mblen_encoding, s, i);
            }
        }
    }

    if use_quote {
        buf.append_byte(quotec)?;
        let mut start = 0usize;
        let mut i = 0usize;
        while i < s.len() {
            let c = s[i];
            if c == quotec || c == escapec {
                buf.append_bytes(&s[start..i])?;
                buf.append_byte(escapec)?;
                start = i;
            }
            i += mb_step(mblen_encoding, s, i);
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

// The progress update of CopySendEndOfRow (copyto.c:494-497): everything in
// fe_msgbuf not yet counted joins bytes_processed, and pg_stat_progress_copy
// sees it now — per row, on every destination, whether or not the bytes
// have reached the file yet.
fn account_fe_msgbuf(cstate: &mut CopyToState<'_, '_>) {
    let unaccounted = cstate.fe_msgbuf.len() - cstate.fe_msgbuf_accounted;
    if unaccounted == 0 {
        return;
    }
    cstate.bytes_processed += unaccounted as u64;
    cstate.fe_msgbuf_accounted = cstate.fe_msgbuf.len();
    pgstat_progress_update_param(PROGRESS_COPY_BYTES_PROCESSED, cstate.bytes_processed as i64);
}

// fe_msgbuf handed off (written or sent): nothing left to account.
fn reset_fe_msgbuf(cstate: &mut CopyToState<'_, '_>) {
    cstate.fe_msgbuf.reset();
    cstate.fe_msgbuf_accounted = 0;
}

// CopySendEndOfRow.
fn send_end_of_row(cstate: &mut CopyToState<'_, '_>) -> PgResult<()> {
    match cstate.dest {
        CopyDest::File { .. } => {
            account_fe_msgbuf(cstate);
            if cstate.fe_msgbuf.len() >= FILE_FLUSH_THRESHOLD {
                flush_to_file(cstate)?;
            }
        }
        CopyDest::Program { .. } => {
            account_fe_msgbuf(cstate);
            if cstate.fe_msgbuf.len() >= FILE_FLUSH_THRESHOLD {
                flush_to_program(cstate)?;
            }
        }
        CopyDest::Stdout => {
            account_fe_msgbuf(cstate);
            if cstate.fe_msgbuf.len() >= FILE_FLUSH_THRESHOLD {
                flush_to_stdout(cstate)?;
            }
        }
        CopyDest::Frontend => {
            pqcomm::pq_putmessage(b'd', cstate.fe_msgbuf.as_bytes())?;
            account_fe_msgbuf(cstate);
            reset_fe_msgbuf(cstate);
        }
    }
    Ok(())
}

// The stdio buffer size C's copy_file FILE gets: st_blksize when the stream
// reports one, else BUFSIZ (__smakebuf / _IO_file_doallocate).
fn stdio_bufsize(st_blksize: usize) -> usize {
    if st_blksize > 0 { st_blksize } else { 1024 }
}

// stdio fwrite over `file_buf`: the row is appended, and every time the
// buffer fills to `file_bufsize` that chunk goes to the fd (__sfvwrite
// flushes on a full buffer). Returns the write error, if any.
fn stdio_buffered_write(cstate: &mut CopyToState<'_, '_>, fd: i32, bytes: &[u8]) -> std::io::Result<()> {
    cstate.file_buf.extend_from_slice(bytes);
    while cstate.file_buf.len() >= cstate.file_bufsize {
        let chunk = cstate.file_bufsize;
        let r = fd::with_allocated_stdio(fd, |f| {
            use std::io::Write;
            f.write_all(&cstate.file_buf[..chunk])
        });
        match r {
            Some(Ok(())) => cstate.file_buf.drain(..chunk),
            Some(Err(e)) => return Err(e),
            None => panic!("COPY TO: AllocateFile index {fd} vanished"),
        };
    }
    Ok(())
}

// fflush at fclose: whatever the buffer still holds.
fn stdio_final_flush(cstate: &mut CopyToState<'_, '_>, fd: i32) -> std::io::Result<()> {
    if cstate.file_buf.is_empty() {
        return Ok(());
    }
    let r = fd::with_allocated_stdio(fd, |f| {
        use std::io::Write;
        f.write_all(&cstate.file_buf)
    });
    cstate.file_buf.clear();
    match r {
        Some(r) => r,
        None => panic!("COPY TO: AllocateFile index {fd} vanished"),
    }
}

fn flush_to_file(cstate: &mut CopyToState<'_, '_>) -> PgResult<()> {
    let CopyDest::File { fd, .. } = cstate.dest else {
        panic!("COPY TO: flush_to_file on non-file destination")
    };
    if cstate.fe_msgbuf.is_empty() {
        return Ok(());
    }
    let bytes = cstate.fe_msgbuf.as_bytes().to_vec();
    if let Err(e) = stdio_buffered_write(cstate, fd, &bytes) {
        ereport(ERROR)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg("could not write to COPY file: %m")
            .finish(loc("CopySendEndOfRow"))?;
    }
    account_fe_msgbuf(cstate);
    reset_fe_msgbuf(cstate);
    Ok(())
}

// The PROGRAM arm of C's CopySendEndOfRow COPY_FILE fwrite. On EPIPE the
// pipe is closed first: the subprocess' exit status usually carries a better
// error than "Broken pipe"; if the child in fact exited cleanly after
// closing its stdin early, the EPIPE write error is still an error
// (copyto.c:456-477).
fn flush_to_program(cstate: &mut CopyToState<'_, '_>) -> PgResult<()> {
    let CopyDest::Program { fd, filename } = cstate.dest else {
        panic!("COPY TO: flush_to_program on non-program destination")
    };
    if cstate.fe_msgbuf.is_empty() {
        return Ok(());
    }
    match fd::PipeStreamWrite(fd, cstate.fe_msgbuf.as_bytes()) {
        Ok(()) => {}
        Err(en) => {
            if en == libc::EPIPE {
                close_pipe_to_program(fd, filename)?;
                // ClosePipeToProgram didn't throw: the program terminated
                // normally but closed the pipe first. Throw the EPIPE error.
            }
            ereport(ERROR)
                .with_saved_errno(en)
                .errcode_for_file_access()
                .errmsg("could not write to COPY program: %m")
                .finish(loc("CopySendEndOfRow"))?;
        }
    }
    account_fe_msgbuf(cstate);
    reset_fe_msgbuf(cstate);
    Ok(())
}

// ClosePipeToProgram (copyto.c): pclose and check the wait status; unlike
// COPY FROM PROGRAM, any nonzero exit (SIGPIPE included) is an error.
fn close_pipe_to_program(fd: i32, filename: &str) -> PgResult<()> {
    let pclose_rc = fd::ClosePipeStream(fd)?;
    if pclose_rc == -1 {
        ereport(ERROR)
            .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg("could not close pipe to external command: %m")
            .finish(loc("ClosePipeToProgram"))?;
    } else if pclose_rc != 0 {
        return Err(Box::new(
            PgError::error(format!("program \"{filename}\" failed"))
                .with_sqlstate(types_error::ERRCODE_EXTERNAL_ROUTINE_EXCEPTION)
                .with_detail(wait_error::wait_result_to_str(pclose_rc)),
        ));
    }
    Ok(())
}

// The stdout arm of C's CopySendEndOfRow COPY_FILE fwrite (copy_file =
// stdout, copyto.c:919). std::io::stdout() is the same buffered handle the
// debugtup receiver and the "backend> " prompt print through, so ordering
// with the surrounding session output is preserved exactly as C's shared
// stdio FILE buffer preserves it.
fn flush_to_stdout(cstate: &mut CopyToState<'_, '_>) -> PgResult<()> {
    debug_assert!(matches!(cstate.dest, CopyDest::Stdout));
    if cstate.fe_msgbuf.is_empty() {
        return Ok(());
    }
    use std::io::Write;
    if let Err(e) = std::io::stdout().write_all(cstate.fe_msgbuf.as_bytes()) {
        ereport(ERROR)
            .with_saved_errno(e.raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg("could not write to COPY file: %m")
            .finish(loc("CopySendEndOfRow"))?;
    }
    account_fe_msgbuf(cstate);
    reset_fe_msgbuf(cstate);
    Ok(())
}

/// `EndCopyTo` + `EndCopy` (copyto.c).
pub fn EndCopyTo(mut cstate: CopyToState<'_, '_>) -> PgResult<()> {
    if let Some(mut owner) = cstate.query_desc.take() {
        let qd = owner.0;
        // A finish/end error releases the entry via the owner drop (the
        // abort path keeps the executor bundle's drop glue as teardown,
        // PortalCleanup's error contract); the clean path frees it.
        execmain_seams::executor_finish::call(qd)?;
        execmain_seams::executor_end::call(qd)?;
        owner.disarm();
        execmain_seams::free_query_desc::call(qd);
        snapmgr::PopActiveSnapshot()?;
    }
    // EndCopy never closes stdout (FreeFile runs only for the filename arm);
    // flush what DoCopyTo's error paths may have left buffered.
    if matches!(cstate.dest, CopyDest::Stdout) {
        flush_to_stdout(&mut cstate)?;
    }
    if let CopyDest::File { fd, filename } = cstate.dest {
        flush_to_file(&mut cstate)?;
        // fclose flushes the stdio buffer first; a write failure there is
        // fclose's failure, reported as "could not close file" (copyto.c:596).
        let flushed = stdio_final_flush(&mut cstate, fd);
        let close_errno = match (&flushed, fd::FreeFile(fd)?) {
            (Err(e), _) => Some(e.raw_os_error().unwrap_or(0)),
            (Ok(()), rc) if rc != 0 => {
                Some(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
            }
            _ => None,
        };
        if let Some(errno) = close_errno {
            ereport(ERROR)
                .with_saved_errno(errno)
                .errcode_for_file_access()
                .errmsg(format!("could not close file \"{filename}\": %m"))
                .finish(loc("EndCopy"))?;
        }
    }
    if let CopyDest::Program { fd, filename } = cstate.dest {
        flush_to_program(&mut cstate)?;
        close_pipe_to_program(fd, filename)?;
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

/// `CopyAttributeOutText` (copyto.c), ASCII-embedding (client-only encoding)
/// arm: identical escape table, but high-bit lead bytes advance by
/// pg_encoding_mblen so ASCII-looking trail bytes are never escaped.
pub fn copy_attribute_out_text_embedded(
    buf: &mut StringInfo<'_>,
    s: &[u8],
    delimc: u8,
    file_encoding: i32,
) -> PgResult<()> {
    let enc = Some(file_encoding);
    let mut start = 0usize;
    let mut ptr = 0usize;
    while ptr < s.len() {
        let c = s[ptr];
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
        } else if c == b'\\' || c == delimc {
            if ptr > start {
                buf.append_bytes(&s[start..ptr])?;
            }
            buf.append_byte(b'\\')?;
            start = ptr;
            ptr += 1;
        } else {
            ptr += mb_step(enc, s, ptr);
        }
    }
    if ptr > start {
        buf.append_bytes(&s[start..ptr.min(s.len())])?;
    }
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_copy_from_relkind(rel: &Relation<'_>) -> Box<PgError> {
    let name = rel.name();
    let (msg, hint, sqlstate) = match rel.rd_rel.relkind {
        b'v' => (
            format!("cannot copy from view \"{name}\""),
            Some("Try the COPY (SELECT ...) TO variant."),
            ERRCODE_WRONG_OBJECT_TYPE,
        ),
        b'm' => (
            format!("cannot copy from unpopulated materialized view \"{name}\""),
            Some("Use the REFRESH MATERIALIZED VIEW command."),
            ERRCODE_FEATURE_NOT_SUPPORTED,
        ),
        b'f' => (
            format!("cannot copy from foreign table \"{name}\""),
            Some("Try the COPY (SELECT ...) TO variant."),
            ERRCODE_WRONG_OBJECT_TYPE,
        ),
        b'S' => (
            format!("cannot copy from sequence \"{name}\""),
            None,
            ERRCODE_WRONG_OBJECT_TYPE,
        ),
        b'p' => (
            format!("cannot copy from partitioned table \"{name}\""),
            Some("Try the COPY (SELECT ...) TO variant."),
            ERRCODE_WRONG_OBJECT_TYPE,
        ),
        _ => (
            format!("cannot copy from non-table relation \"{name}\""),
            None,
            ERRCODE_WRONG_OBJECT_TYPE,
        ),
    };
    let mut e = PgError::error(msg).with_sqlstate(sqlstate);
    if let Some(h) = hint {
        e = e.with_hint(h);
    }
    Box::new(e)
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use super::COPY_TO_FILE_MODE;
    use std::os::unix::fs::PermissionsExt;

    // The fix replaces C's process-global umask swap with an explicit fchmod on
    // the created file (see BeginCopyTo). This asserts the mechanism: setting
    // COPY_TO_FILE_MODE on the fd yields exactly C's resulting mode (0o644)
    // regardless of the current process umask, and without mutating it — which
    // in pgrust's thread-per-backend model would race other sessions' files.
    #[test]
    fn explicit_mode_matches_c_and_leaves_umask_untouched() {
        assert_eq!(COPY_TO_FILE_MODE, 0o644, "must match C's 0666 & ~0o022");

        // Force a restrictive umask (the server's PG_MODE_MASK_OWNER = 0o077),
        // as would be in effect when a backend runs COPY TO.
        let saved = unsafe { libc::umask(0o077) };

        let mut path = std::env::temp_dir();
        path.push(format!("pgrust_copyto_mode_{}.tmp", std::process::id()));
        let file = std::fs::File::create(&path).expect("create temp file");
        // Default create honors umask 0o077 -> 0o600; explicit set overrides it.
        file.set_permissions(std::fs::Permissions::from_mode(COPY_TO_FILE_MODE))
            .expect("set_permissions");

        let mode = file.metadata().unwrap().permissions().mode() & 0o777;

        // Restore umask before asserting so a failure can't leak global state.
        let after = unsafe { libc::umask(saved) };
        let _ = std::fs::remove_file(&path);

        assert_eq!(mode, 0o644, "explicit fchmod must reach C's mode");
        // Our code path never calls umask(2): the process mask is unchanged.
        assert_eq!(after, 0o077, "process umask must be untouched by the fix");
    }
}
