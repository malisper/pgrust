//! Port of PostgreSQL `src/backend/commands/copyto.c` — the COPY TO executor:
//! emit rows from a relation or query to a file / program / frontend / callback
//! in the text, CSV, or binary wire format.
//!
//! The byte-exact text/CSV/binary formatting (`copy_attribute_out_text`,
//! `copy_attribute_out_csv`, the `copy_send_*` family, the binary
//! header/trailer) lives entirely here over an owned per-row [`StringInfo`]
//! buffer. Cross-subsystem work (option parsing, attribute resolution, type
//! out-function lookup/call, encoding conversion, the file/program/frontend
//! sinks, the table scan, and the COPY-(query)-TO parse/plan/execute pipeline)
//! goes through the owners' seam crates.

#![allow(non_snake_case)]
#![allow(clippy::result_large_err)]
#![allow(clippy::too_many_arguments)]

extern crate alloc;

use core::cell::RefCell;

use mcx::{Mcx, PgString, PgVec};
use types_copy::{CopyFormatOptions, CopyHeaderChoice};
use types_core::fmgr::FmgrInfo;
use types_core::primitive::{AttrNumber, InvalidOid, Oid};
use types_error::{PgError, PgResult, ERRCODE_INVALID_COLUMN_REFERENCE, ERRCODE_INVALID_NAME};
use types_nodes::copy_query::{ParseState, QueryDesc, QuerySource, RawStmt, T_CreateTableAsStmt};
use types_nodes::nodes::{CmdType, CMD_SELECT};
use types_nodes::TupleTableSlot;
use types_pgstat::backend_progress::ProgressCommandType;
use types_rel::Relation;
use types_stringinfo::StringInfo;
use types_tuple::access::{
    RELKIND_FOREIGN_TABLE, RELKIND_MATVIEW, RELKIND_PARTITIONED_TABLE, RELKIND_RELATION,
    RELKIND_SEQUENCE, RELKIND_VIEW,
};
use types_tuple::backend_access_common_heaptuple::DeformedColumn;
use types_tuple::heaptuple::TupleDesc;

use backend_access_table_tableam_seams as tableam_s;
use backend_commands_copy_seams as copy_s;
use backend_executor_execMain_seams as execmain_s;
use backend_storage_file_fd_seams as fd_s;
use backend_utils_activity_backend_progress_seams as progress_s;
use backend_utils_cache_lsyscache_seams as lsyscache_s;
use backend_utils_fmgr_fmgr_seams as fmgr_s;
use backend_utils_mb_mbutils_seams as mbutils_s;

use backend_utils_error::config::where_to_send_output;
use types_dest::CommandDest;

mod tests;

/// `PROGRESS_COPY_*` parameter / command / type codes (commands/progress.h),
/// verified against PostgreSQL 18.3.
const PROGRESS_COPY_BYTES_PROCESSED: i32 = 0;
const PROGRESS_COPY_TUPLES_PROCESSED: i32 = 2;
const PROGRESS_COPY_COMMAND: i32 = 4;
const PROGRESS_COPY_TYPE: i32 = 5;
const PROGRESS_COPY_COMMAND_TO: i64 = 2;
const PROGRESS_COPY_TYPE_FILE: i64 = 1;
const PROGRESS_COPY_TYPE_PROGRAM: i64 = 2;
const PROGRESS_COPY_TYPE_PIPE: i64 = 3;
const PROGRESS_COPY_TYPE_CALLBACK: i64 = 4;

/// `PG_SQL_ASCII` (mb/pg_wchar.h) — the SQL_ASCII encoding id.
const PG_SQL_ASCII: i32 = 0;

/// Protocol message-type bytes (libpq/protocol.h), verified against
/// PostgreSQL 18.3.
const PQMSG_COPY_DATA: u8 = b'd';
const PQMSG_COPY_DONE: u8 = b'c';
const PQMSG_COPY_OUT_RESPONSE: u8 = b'H';

/// `static const char BinarySignature[11] = "PGCOPY\n\377\r\n\0";`
/// (copyto.c:109). NB: there's a copy of this in copyfromparse.c.
const BINARY_SIGNATURE: [u8; 11] = [
    b'P', b'G', b'C', b'O', b'P', b'Y', b'\n', 0o377, b'\r', b'\n', 0,
];

/// `typedef enum CopyDest` (copyto.c:43) — the bottom-level destination kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CopyDest {
    /// `COPY_FILE` — to file (or a piped program).
    File,
    /// `COPY_FRONTEND` — to frontend.
    Frontend,
    /// `COPY_CALLBACK` — to callback function.
    Callback,
}

/// Which built-in format routine is in use — the C dispatch is a
/// `const CopyToRoutine *` over the three method tables (copyto.c:152-173).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CopyToRoutineKind {
    Text,
    Csv,
    Binary,
}

/// `copy_data_dest_cb` (commands/copy.h) — `void (*)(void *data, int len)`, the
/// caller-supplied data-writing callback. It can raise, so the owned form is
/// fallible.
pub type CopyDataDestCb = fn(&[u8]) -> PgResult<()>;

/// `typedef struct CopyToStateData` (copyto.c:65) — all state for one COPY TO
/// operation. This crate owns the layout (opaque at the C boundary).
pub struct CopyToStateData<'mcx> {
    /// `const CopyToRoutine *routine` — the format-specific method table.
    routine: CopyToRoutineKind,

    /// `CopyDest copy_dest`.
    copy_dest: CopyDest,
    /// `FILE *copy_file` — used if `copy_dest == COPY_FILE` (`None` ⇒ NULL).
    copy_file: Option<fd_s::PgFileStream>,
    /// `StringInfo fe_msgbuf` — per-row output buffer for all dests.
    fe_msgbuf: StringInfo<'mcx>,

    /// `int file_encoding`.
    file_encoding: i32,
    /// `bool need_transcoding`.
    need_transcoding: bool,
    /// `bool encoding_embeds_ascii`.
    encoding_embeds_ascii: bool,

    /// `Relation rel` — relation to copy to (`None` for COPY (query) TO).
    rel: Option<Relation<'mcx>>,
    /// `QueryDesc *queryDesc` — executable query, or `None`.
    query_desc: Option<QueryDesc<'mcx>>,
    /// `List *attnumlist` — integer list of attnums to copy.
    attnumlist: PgVec<'mcx, AttrNumber>,
    /// `char *filename` — filename, or `None` for STDOUT.
    filename: Option<PgString<'mcx>>,
    /// `bool is_program`.
    is_program: bool,
    /// `copy_data_dest_cb data_dest_cb` (`None` ⇒ NULL).
    data_dest_cb: Option<CopyDataDestCb>,

    /// `CopyFormatOptions opts`.
    opts: CopyFormatOptions<'mcx>,

    /// `FmgrInfo *out_functions` — per-(physical-)column output-function lookup
    /// info. Indexed by `attnum - 1`; sized to `num_phys_attrs`.
    out_functions: PgVec<'mcx, FmgrInfo>,

    /// `uint64 bytes_processed`.
    bytes_processed: u64,

    /// The allocator the cstate's `'mcx` data is charged to (C: the per-copy
    /// `copycontext`). Carried so the buffer/out-functions/attnumlist grow in
    /// the same context for the lifetime of the COPY.
    mcx: Mcx<'mcx>,

    /// `((DR_copy *) cstate->queryDesc->dest)->processed` — the COPY-(query)-TO
    /// receiver's processed-tuple counter, kept on the cstate (the receiver is
    /// reached by handle).
    receiver_processed: u64,
    /// The COPY-OUT `DestReceiver` handle (`cstate->queryDesc->dest`) for the
    /// COPY-(query)-TO path, or `None` for COPY-relation-TO.
    receiver: Option<u64>,
}

/* ===================================================================== */
/* COPY-(query)-TO receiver registry (the DR_copy.cstate aliasing)       */
/* ===================================================================== */
//
// The executor (a not-yet-ported unit) drives the COPY-OUT DestReceiver, whose
// `receiveSlot` callback (`copy_dest_receive`) must reach the live cstate — in
// C, `((DR_copy *) self)->cstate`. The cstate is owned by `DoCopyTo`'s stack
// frame; for the duration of `executor_run_copy` the executor runs
// synchronously and re-enters through `copy_dest_receive`. A per-backend
// (`thread_local`) registry holds the raw cstate pointer keyed by the receiver
// handle for exactly that window, mirroring C's pointer aliasing.

thread_local! {
    static RECEIVERS: RefCell<alloc::vec::Vec<Option<ReceiverSlot>>> = const { RefCell::new(alloc::vec::Vec::new()) };
}

struct ReceiverSlot {
    /// Raw pointer to the live cstate driving this receiver (set only while a
    /// query run is in progress).
    cstate: *mut (),
}

/// Allocate a fresh receiver handle (1-based; 0 is never handed out).
fn receiver_register() -> u64 {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        reg.push(Some(ReceiverSlot { cstate: core::ptr::null_mut() }));
        reg.len() as u64
    })
}

/// Associate the live cstate pointer with `handle` for the duration of a run.
fn receiver_bind<'mcx>(handle: u64, cstate: &mut CopyToStateData<'mcx>) {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(Some(slot)) = reg.get_mut((handle - 1) as usize) {
            slot.cstate = cstate as *mut CopyToStateData<'mcx> as *mut ();
        }
    });
}

/// Clear the cstate pointer after the run.
fn receiver_unbind(handle: u64) {
    RECEIVERS.with(|r| {
        let mut reg = r.borrow_mut();
        if let Some(Some(slot)) = reg.get_mut((handle - 1) as usize) {
            slot.cstate = core::ptr::null_mut();
        }
    });
}

/* ===================================================================== */
/* COPY TO routine dispatch                                              */
/* ===================================================================== */

/// `CopyToGetRoutine(opts)` (copyto.c:177).
fn copy_to_get_routine(opts: &CopyFormatOptions<'_>) -> CopyToRoutineKind {
    if opts.csv_mode {
        CopyToRoutineKind::Csv
    } else if opts.binary {
        CopyToRoutineKind::Binary
    } else {
        // default is text
        CopyToRoutineKind::Text
    }
}

/* ===================================================================== */
/* Format-specific routines (text / CSV / binary)                        */
/* ===================================================================== */

/// `CopyToTextLikeStart(cstate, tupDesc)` (copyto.c:190).
fn copy_to_text_like_start(cstate: &mut CopyToStateData<'_>, tup_desc: &TupleDesc<'_>) -> PgResult<()> {
    // For non-binary copy, we need to convert null_print to file encoding,
    // because it will be sent directly with CopySendString.
    if cstate.need_transcoding {
        let np = cstate.opts.null_print.as_bytes();
        if let Some(converted) =
            mbutils_s::pg_server_to_any::call(cstate.mcx, np, cstate.file_encoding)?
        {
            cstate.opts.null_print_client = bytes_to_pgstring(cstate.mcx, &converted)?;
        } else {
            cstate.opts.null_print_client = cstate.opts.null_print.clone_in(cstate.mcx)?;
        }
    }

    // if a header has been requested send the line
    if cstate.opts.header_line != CopyHeaderChoice::COPY_HEADER_FALSE {
        let mut hdr_delim = false;
        let delimc = cstate.opts.delim;
        let csv_mode = cstate.opts.csv_mode;

        // foreach(cur, cstate->attnumlist)
        let attnums: alloc::vec::Vec<AttrNumber> = cstate.attnumlist.iter().copied().collect();
        for attnum in attnums {
            if hdr_delim {
                copy_send_char(cstate, delimc)?;
            }
            hdr_delim = true;

            // colname = NameStr(TupleDescAttr(tupDesc, attnum - 1)->attname);
            let colname: alloc::vec::Vec<u8> =
                td(tup_desc).attr((attnum - 1) as usize).attname.name_str().to_vec();

            if csv_mode {
                copy_attribute_out_csv(cstate, &colname, false)?;
            } else {
                copy_attribute_out_text(cstate, &colname)?;
            }
        }

        copy_send_text_like_end_of_row(cstate)?;
    }
    Ok(())
}

/// `CopyToTextLikeOutFunc(cstate, atttypid, finfo)` (copyto.c:233): assign the
/// (text) output function for an attribute.
fn copy_to_text_like_out_func(atttypid: Oid) -> PgResult<FmgrInfo> {
    // getTypeOutputInfo(atttypid, &func_oid, &is_varlena);
    let (func_oid, _is_varlena) = lsyscache_s::get_type_output_info::call(atttypid)?;
    // fmgr_info(func_oid, finfo);
    fmgr_s::fmgr_info_check::call(func_oid)?;
    Ok(FmgrInfo { fn_oid: func_oid, ..Default::default() })
}

/// `CopyToBinaryOutFunc(cstate, atttypid, finfo)` (copyto.c:333).
fn copy_to_binary_out_func(atttypid: Oid) -> PgResult<FmgrInfo> {
    // getTypeBinaryOutputInfo(atttypid, &func_oid, &is_varlena);
    let (func_oid, _is_varlena) = lsyscache_s::get_type_binary_output_info::call(atttypid)?;
    // fmgr_info(func_oid, finfo);
    fmgr_s::fmgr_info_check::call(func_oid)?;
    Ok(FmgrInfo { fn_oid: func_oid, ..Default::default() })
}

/// `CopyToTextLikeOneRow(cstate, slot, is_csv)` (copyto.c:264). `cols` is the
/// slot's deformed `(tts_values[i], tts_isnull[i])` array.
fn copy_to_text_like_one_row(
    cstate: &mut CopyToStateData<'_>,
    cols: &[DeformedColumn<'_>],
    is_csv: bool,
) -> PgResult<()> {
    let mut need_delim = false;
    let delimc = cstate.opts.delim;

    let attnums: alloc::vec::Vec<AttrNumber> = cstate.attnumlist.iter().copied().collect();
    for attnum in attnums {
        let (value, isnull) = &cols[(attnum - 1) as usize];

        if need_delim {
            copy_send_char(cstate, delimc)?;
        }
        need_delim = true;

        if *isnull {
            let np = pgstring_bytes(&cstate.opts.null_print_client);
            copy_send_string(cstate, &np)?;
        } else {
            let finfo = cstate.out_functions[(attnum - 1) as usize];
            let string = fmgr_s::output_function_call::call(cstate.mcx, &finfo, value)?;
            let string: alloc::vec::Vec<u8> = string.to_vec();

            if is_csv {
                let force = cstate.opts.force_quote_flags[(attnum - 1) as usize];
                copy_attribute_out_csv(cstate, &string, force)?;
            } else {
                copy_attribute_out_text(cstate, &string)?;
            }
        }
    }

    copy_send_text_like_end_of_row(cstate)
}

/// `CopyToBinaryStart(cstate, tupDesc)` (copyto.c:314).
fn copy_to_binary_start(cstate: &mut CopyToStateData<'_>) -> PgResult<()> {
    // Signature
    copy_send_data(cstate, &BINARY_SIGNATURE)?;
    // Flags field
    copy_send_int32(cstate, 0)?;
    // No header extension
    copy_send_int32(cstate, 0)?;
    Ok(())
}

/// `CopyToBinaryOneRow(cstate, slot)` (copyto.c:345). `cols` is the slot's
/// deformed `(tts_values[i], tts_isnull[i])` array.
fn copy_to_binary_one_row(
    cstate: &mut CopyToStateData<'_>,
    cols: &[DeformedColumn<'_>],
) -> PgResult<()> {
    // Binary per-tuple header
    copy_send_int16(cstate, cstate.attnumlist.len() as i16)?;

    let attnums: alloc::vec::Vec<AttrNumber> = cstate.attnumlist.iter().copied().collect();
    for attnum in attnums {
        let (value, isnull) = &cols[(attnum - 1) as usize];

        if *isnull {
            copy_send_int32(cstate, -1)?;
        } else {
            let finfo = cstate.out_functions[(attnum - 1) as usize];
            // outputbytes = SendFunctionCall(...) — header already stripped to
            // VARSIZE - VARHDRSZ payload bytes by the seam.
            let outputbytes = fmgr_s::send_function_call::call(cstate.mcx, &finfo, value)?;
            let outputbytes: alloc::vec::Vec<u8> = outputbytes.to_vec();
            copy_send_int32(cstate, outputbytes.len() as i32)?;
            copy_send_data(cstate, &outputbytes)?;
        }
    }

    copy_send_end_of_row(cstate)
}

/// `CopyToBinaryEnd(cstate)` (copyto.c:378).
fn copy_to_binary_end(cstate: &mut CopyToStateData<'_>) -> PgResult<()> {
    // Generate trailer for a binary copy
    copy_send_int16(cstate, -1)?;
    // Need to flush out the trailer
    copy_send_end_of_row(cstate)
}

/* ----- routine dispatch helpers (the `const CopyToRoutine *` calls) ----- */

/// Dispatch `routine->CopyToStart`.
fn routine_start(cstate: &mut CopyToStateData<'_>, tup_desc: &TupleDesc<'_>) -> PgResult<()> {
    match cstate.routine {
        CopyToRoutineKind::Text | CopyToRoutineKind::Csv => copy_to_text_like_start(cstate, tup_desc),
        CopyToRoutineKind::Binary => copy_to_binary_start(cstate),
    }
}

/// Dispatch `routine->CopyToOutFunc`.
fn routine_out_func(cstate: &CopyToStateData<'_>, atttypid: Oid) -> PgResult<FmgrInfo> {
    match cstate.routine {
        CopyToRoutineKind::Text | CopyToRoutineKind::Csv => copy_to_text_like_out_func(atttypid),
        CopyToRoutineKind::Binary => copy_to_binary_out_func(atttypid),
    }
}

/// Dispatch `routine->CopyToOneRow`.
fn routine_one_row(cstate: &mut CopyToStateData<'_>, cols: &[DeformedColumn<'_>]) -> PgResult<()> {
    match cstate.routine {
        // CopyToTextOneRow / CopyToCSVOneRow both call CopyToTextLikeOneRow.
        CopyToRoutineKind::Text => copy_to_text_like_one_row(cstate, cols, false),
        CopyToRoutineKind::Csv => copy_to_text_like_one_row(cstate, cols, true),
        CopyToRoutineKind::Binary => copy_to_binary_one_row(cstate, cols),
    }
}

/// Dispatch `routine->CopyToEnd`.
fn routine_end(cstate: &mut CopyToStateData<'_>) -> PgResult<()> {
    match cstate.routine {
        // CopyToTextLikeEnd: nothing to do here.
        CopyToRoutineKind::Text | CopyToRoutineKind::Csv => Ok(()),
        CopyToRoutineKind::Binary => copy_to_binary_end(cstate),
    }
}

/* ===================================================================== */
/* Low-level communications functions                                    */
/* ===================================================================== */

/// `SendCopyBegin(cstate)` (copyto.c:391).
fn send_copy_begin(cstate: &mut CopyToStateData<'_>) -> PgResult<()> {
    let natts = cstate.attnumlist.len() as i32;
    let format: i16 = if cstate.opts.binary { 1 } else { 0 };

    let mut buf = backend_libpq_pqformat::pq_beginmessage(cstate.mcx, PQMSG_COPY_OUT_RESPONSE)?;
    backend_libpq_pqformat::pq_sendbyte(&mut buf, format as u8)?; // overall format
    backend_libpq_pqformat::pq_sendint16(&mut buf, natts as u16)?;
    for _ in 0..natts {
        backend_libpq_pqformat::pq_sendint16(&mut buf, format as u16)?; // per-column formats
    }
    backend_libpq_pqformat::pq_endmessage(buf)?;
    cstate.copy_dest = CopyDest::Frontend;
    Ok(())
}

/// `SendCopyEnd(cstate)` (copyto.c:408).
fn send_copy_end(cstate: &CopyToStateData<'_>) -> PgResult<()> {
    // Shouldn't have any unsent data
    debug_assert!(cstate.fe_msgbuf.is_empty());
    // Send Copy Done message
    backend_libpq_pqformat::pq_putemptymessage(PQMSG_COPY_DONE)
}

/// `CopySendData(cstate, databuf, datasize)` (copyto.c:427) —
/// `appendBinaryStringInfo`.
fn copy_send_data(cstate: &mut CopyToStateData<'_>, databuf: &[u8]) -> PgResult<()> {
    append_bytes(&mut cstate.fe_msgbuf, databuf)
}

/// `CopySendString(cstate, str)` (copyto.c:433).
fn copy_send_string(cstate: &mut CopyToStateData<'_>, sbytes: &[u8]) -> PgResult<()> {
    append_bytes(&mut cstate.fe_msgbuf, sbytes)
}

/// `CopySendChar(cstate, c)` (copyto.c:439) — `appendStringInfoCharMacro`.
fn copy_send_char(cstate: &mut CopyToStateData<'_>, c: u8) -> PgResult<()> {
    let buf = &mut cstate.fe_msgbuf.data;
    let mcx = *buf.allocator();
    buf.try_reserve(1).map_err(|_| mcx.oom(1))?;
    buf.push(c);
    Ok(())
}

/// `appendBinaryStringInfo` — append `bytes` to the buffer, growing the
/// `PgVec<u8>` via `try_reserve` (fallible-alloc rule).
fn append_bytes(buf: &mut StringInfo<'_>, bytes: &[u8]) -> PgResult<()> {
    let mcx = *buf.data.allocator();
    buf.data
        .try_reserve(bytes.len())
        .map_err(|_| mcx.oom(bytes.len()))?;
    buf.data.extend_from_slice(bytes);
    Ok(())
}

/// `CopySendEndOfRow(cstate)` (copyto.c:445) — flush the per-row buffer and
/// report byte progress, then reset.
fn copy_send_end_of_row(cstate: &mut CopyToStateData<'_>) -> PgResult<()> {
    match cstate.copy_dest {
        CopyDest::File => {
            // The bare write/ferror is fd/OS-owned; the EPIPE / is_program
            // decision and the message selection are copyto's own control flow
            // (copyto.c:451-483).
            if let Some(write_errno) = fd_s::copy_write_file::call(
                cstate.copy_file.expect("COPY_FILE dest with no open file"),
                cstate.fe_msgbuf.as_bytes(),
            )? {
                if cstate.is_program {
                    let mut errnum = write_errno;
                    if errnum == backend_utils_error::errno::EPIPE {
                        // The pipe will be closed automatically on error at the
                        // end of transaction, but we might get a better error
                        // message from the subprocess' exit code than just
                        // "Broken Pipe".
                        close_pipe_to_program(cstate)?;
                        // If ClosePipeToProgram() didn't throw an error, the
                        // program terminated normally, but closed the pipe
                        // first. Restore errno, and throw an error.
                        errnum = backend_utils_error::errno::EPIPE;
                    }
                    return Err(PgError::error(
                        backend_utils_error::errno::replace_percent_m(
                            "could not write to COPY program: %m",
                            errnum,
                        ),
                    )
                    .with_sqlstate(backend_utils_error::errno::sqlstate_for_file_access(errnum))
                    .with_saved_errno(errnum));
                } else {
                    return Err(PgError::error(
                        backend_utils_error::errno::replace_percent_m(
                            "could not write to COPY file: %m",
                            write_errno,
                        ),
                    )
                    .with_sqlstate(backend_utils_error::errno::sqlstate_for_file_access(
                        write_errno,
                    ))
                    .with_saved_errno(write_errno));
                }
            }
        }
        CopyDest::Frontend => {
            // Dump the accumulated row as one CopyData message
            let _eof = backend_libpq_pqcomm_seams::pq_putmessage::call(
                PQMSG_COPY_DATA,
                cstate.fe_msgbuf.as_bytes(),
            )?;
        }
        CopyDest::Callback => {
            let cb = cstate.data_dest_cb.expect("COPY_CALLBACK dest with no callback");
            cb(cstate.fe_msgbuf.as_bytes())?;
        }
    }

    // Update the progress
    cstate.bytes_processed += cstate.fe_msgbuf.len() as u64;
    progress_s::pgstat_progress_update_param::call(
        PROGRESS_COPY_BYTES_PROCESSED,
        cstate.bytes_processed as i64,
    );

    // resetStringInfo(fe_msgbuf)
    cstate.fe_msgbuf.reset();
    Ok(())
}

/// `CopySendTextLikeEndOfRow(cstate)` (copyto.c:506).
fn copy_send_text_like_end_of_row(cstate: &mut CopyToStateData<'_>) -> PgResult<()> {
    match cstate.copy_dest {
        // Default line termination depends on platform; this port targets the
        // non-WIN32 build, so a bare '\n' (copyto.c:512-516).
        CopyDest::File => copy_send_char(cstate, b'\n')?,
        // The FE/BE protocol uses \n as newline for all platforms.
        CopyDest::Frontend => copy_send_char(cstate, b'\n')?,
        CopyDest::Callback => {}
    }

    // Now take the actions related to the end of a row
    copy_send_end_of_row(cstate)
}

/// `CopySendInt32(cstate, val)` (copyto.c:538) — network byte order.
fn copy_send_int32(cstate: &mut CopyToStateData<'_>, val: i32) -> PgResult<()> {
    let buf = (val as u32).to_be_bytes();
    copy_send_data(cstate, &buf)
}

/// `CopySendInt16(cstate, val)` (copyto.c:550) — network byte order.
fn copy_send_int16(cstate: &mut CopyToStateData<'_>, val: i16) -> PgResult<()> {
    let buf = (val as u16).to_be_bytes();
    copy_send_data(cstate, &buf)
}

/// `ClosePipeToProgram(cstate)` (copyto.c:562).
fn close_pipe_to_program(cstate: &CopyToStateData<'_>) -> PgResult<()> {
    debug_assert!(cstate.is_program);
    let filename = cstate
        .filename
        .as_ref()
        .map(|f| f.as_str())
        .unwrap_or("");
    fd_s::close_pipe_to_program::call(
        cstate.copy_file.expect("is_program with no open pipe"),
        filename,
    )
}

/// `EndCopy(cstate)` (copyto.c:587) — release resources for COPY TO.
fn end_copy(cstate: &CopyToStateData<'_>) -> PgResult<()> {
    if cstate.is_program {
        close_pipe_to_program(cstate)?;
    } else if let Some(filename) = cstate.filename.as_ref() {
        // if (cstate->filename != NULL && FreeFile(cstate->copy_file)) ereport
        fd_s::free_file::call(
            cstate.copy_file.expect("filename set with no open file"),
            filename.as_str(),
        )?;
    }

    progress_s::pgstat_progress_end_command::call();

    // MemoryContextDelete(cstate->copycontext); pfree(cstate);
    // The owned cstate's context is reclaimed when it is dropped.
    Ok(())
}

/* ===================================================================== */
/* Public drivers                                                        */
/* ===================================================================== */

/// `BeginCopyTo(pstate, rel, raw_query, queryRelId, filename, is_program,
/// data_dest_cb, attnamelist, options)` (copyto.c:623).
///
/// `mcx` is the per-COPY context the cstate's data is charged to (C: the
/// `copycontext` created from `CurrentMemoryContext`). A NULL C pointer is
/// `None`.
pub fn BeginCopyTo<'mcx>(
    mcx: Mcx<'mcx>,
    pstate: Option<&ParseState<'mcx>>,
    rel: Option<Relation<'mcx>>,
    raw_query: Option<&RawStmt<'mcx>>,
    query_rel_id: Oid,
    filename: Option<&str>,
    is_program: bool,
    data_dest_cb: Option<CopyDataDestCb>,
    attnamelist: Option<&[PgString<'mcx>]>,
    options: Option<&types_nodes::execnodes::Opaque>,
) -> PgResult<CopyToStateData<'mcx>> {
    let pipe = filename.is_none() && data_dest_cb.is_none();

    // const int progress_cols[] = {PROGRESS_COPY_COMMAND, PROGRESS_COPY_TYPE};
    let progress_cols: [i32; 2] = [PROGRESS_COPY_COMMAND, PROGRESS_COPY_TYPE];
    // int64 progress_vals[] = {PROGRESS_COPY_COMMAND_TO, 0};
    let mut progress_vals: [i64; 2] = [PROGRESS_COPY_COMMAND_TO, 0];

    // Relation-kind validity gate (copyto.c:647-686).
    if let Some(rel) = rel.as_ref() {
        let relkind = rel.rd_rel.relkind;
        if relkind != RELKIND_RELATION {
            if relkind == RELKIND_VIEW {
                return Err(PgError::error(alloc::format!(
                    "cannot copy from view \"{}\"",
                    rel.name()
                ))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .with_hint("Try the COPY (SELECT ...) TO variant."));
            } else if relkind == RELKIND_MATVIEW {
                if !rel.rd_rel.relispopulated {
                    return Err(PgError::error(alloc::format!(
                        "cannot copy from unpopulated materialized view \"{}\"",
                        rel.name()
                    ))
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .with_hint("Use the REFRESH MATERIALIZED VIEW command."));
                }
            } else if relkind == RELKIND_FOREIGN_TABLE {
                return Err(PgError::error(alloc::format!(
                    "cannot copy from foreign table \"{}\"",
                    rel.name()
                ))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .with_hint("Try the COPY (SELECT ...) TO variant."));
            } else if relkind == RELKIND_SEQUENCE {
                return Err(PgError::error(alloc::format!(
                    "cannot copy from sequence \"{}\"",
                    rel.name()
                ))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE));
            } else if relkind == RELKIND_PARTITIONED_TABLE {
                return Err(PgError::error(alloc::format!(
                    "cannot copy from partitioned table \"{}\"",
                    rel.name()
                ))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE)
                .with_hint("Try the COPY (SELECT ...) TO variant."));
            } else {
                return Err(PgError::error(alloc::format!(
                    "cannot copy from non-table relation \"{}\"",
                    rel.name()
                ))
                .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE));
            }
        }
    }

    // Extract options from the statement node tree (C zeroes opts via palloc0
    // then ProcessCopyOptions fills it).
    let opts = copy_s::process_copy_options::call(mcx, pstate, false /* is_from */, options)?;

    // Set format routine.
    let routine = copy_to_get_routine(&opts);

    let mut cstate = CopyToStateData {
        routine,
        copy_dest: CopyDest::File, // default, possibly overwritten below
        copy_file: None,
        fe_msgbuf: StringInfo::new_in(mcx),
        file_encoding: 0,
        need_transcoding: false,
        encoding_embeds_ascii: false,
        rel,
        query_desc: None,
        attnumlist: PgVec::new_in(mcx),
        filename: None,
        is_program: false,
        data_dest_cb: None,
        opts,
        out_functions: PgVec::new_in(mcx),
        bytes_processed: 0,
        mcx,
        receiver_processed: 0,
        receiver: None,
    };

    // Process the source/target relation or query.
    let tup_desc: TupleDesc<'mcx>;
    if let Some(rel) = cstate.rel.as_ref() {
        debug_assert!(raw_query.is_none());
        // tupDesc = RelationGetDescr(cstate->rel);
        tup_desc = clone_tupdesc(mcx, rel.rd_att.as_ref())?;
    } else {
        // COPY (query) TO: run parse analysis + rewrite, validate, plan,
        // double-check queryRelId, push the snapshot, build the DestCopyOut
        // receiver, build the QueryDesc, ExecutorStart (copyto.c:719-851).
        let pstate = pstate.expect("COPY (query) TO requires a ParseState");
        let raw = raw_query.expect("COPY (query) TO requires a raw query");
        let source_text = pstate.p_sourcetext.as_str();

        // rewritten = pg_analyze_and_rewrite_fixedparams(...)
        let rewritten = backend_parser_analyze_seams::pg_analyze_and_rewrite_fixedparams::call(
            mcx, raw, source_text,
        )?;

        // check that we got back something we can work with
        if rewritten.is_empty() {
            return Err(PgError::error(
                "DO INSTEAD NOTHING rules are not supported for COPY",
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        } else if rewritten.len() > 1 {
            // examine queries to determine which error message to issue
            for q in rewritten.iter() {
                if q.querySource == QuerySource::QSRC_QUAL_INSTEAD_RULE {
                    return Err(PgError::error(
                        "conditional DO INSTEAD rules are not supported for COPY",
                    )
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
                }
                if q.querySource == QuerySource::QSRC_NON_INSTEAD_RULE {
                    return Err(PgError::error("DO ALSO rules are not supported for COPY")
                        .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
                }
            }
            return Err(PgError::error(
                "multi-statement DO INSTEAD rules are not supported for COPY",
            )
            .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        }

        // query = linitial_node(Query, rewritten);
        let query = &rewritten[0];

        // The grammar allows SELECT INTO, but we don't support that.
        if let Some(tag) = query.utilityStmt {
            if tag == T_CreateTableAsStmt {
                return Err(PgError::error("COPY (SELECT INTO) is not supported")
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
            }
            // The only other utility command we could see is NOTIFY.
            return Err(PgError::error("COPY query must not be a utility command")
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        }

        // RETURNING clause is required for non-SELECT.
        if query.commandType != CmdType::CMD_SELECT && !query.has_returning_list {
            debug_assert!(matches!(
                query.commandType,
                CmdType::CMD_INSERT | CmdType::CMD_UPDATE | CmdType::CMD_DELETE | CmdType::CMD_MERGE
            ));
            return Err(PgError::error("COPY query must have a RETURNING clause")
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED));
        }
        let _ = CMD_SELECT;

        // plan the query
        let plan = backend_optimizer_plan_planner_seams::pg_plan_query::call(
            mcx,
            query,
            source_text,
            types_nodes::copy_query::CURSOR_OPT_PARALLEL_OK,
        )?;

        // queryRelId double-check (RLS): the original relation must be present.
        if query_rel_id != InvalidOid {
            let found = match &plan.relationOids {
                Some(oids) => oids.iter().any(|&o| o == query_rel_id),
                None => false,
            };
            if !found {
                return Err(PgError::error(
                    "relation referenced by COPY statement has changed",
                )
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE));
            }
        }

        // Use a snapshot with an updated command ID.
        backend_utils_time_snapmgr_seams::push_copied_active_snapshot::call()?;
        backend_utils_time_snapmgr_seams::update_active_snapshot_command_id::call()?;

        // Create dest receiver for COPY OUT (DestCopyOut → CreateCopyDestReceiver)
        // and associate it with this cstate.
        let receiver = receiver_register();
        cstate.receiver = Some(receiver);

        // Create a QueryDesc and ExecutorStart (computes the result tupdesc).
        let started =
            execmain_s::create_query_desc_and_start::call(mcx, plan, source_text, receiver)?;
        tup_desc = clone_tupdesc(mcx, deref_tupdesc(&started))?;
        cstate.query_desc = Some(started);
    }

    // Generate or convert list of attributes to process.
    cstate.attnumlist =
        copy_s::copy_get_attnums::call(mcx, &tup_desc, cstate.rel.as_ref(), attnamelist)?;

    let num_phys_attrs = tup_desc_natts(&tup_desc);

    // Convert FORCE_QUOTE name list to per-column flags, check validity
    // (copyto.c:858-884).
    // cstate->opts.force_quote_flags = (bool *) palloc0(num_phys_attrs * sizeof(bool));
    cstate.opts.force_quote_flags = make_false_vec(mcx, num_phys_attrs as usize)?;
    if cstate.opts.force_quote_all {
        // MemSet(..., true, ...);
        for f in cstate.opts.force_quote_flags.iter_mut() {
            *f = true;
        }
    } else if cstate.opts.force_quote.is_some() {
        // attnums = CopyGetAttnums(tupDesc, cstate->rel, cstate->opts.force_quote);
        let fq = cstate.opts.force_quote.as_ref().unwrap();
        let attnums =
            copy_s::copy_get_attnums::call(mcx, &tup_desc, cstate.rel.as_ref(), Some(fq))?;
        for attnum in attnums.iter().copied() {
            if !cstate.attnumlist.iter().any(|&a| a == attnum) {
                let attname: alloc::vec::Vec<u8> = td(&tup_desc)
                    .attr((attnum - 1) as usize)
                    .attname
                    .name_str()
                    .to_vec();
                return Err(PgError::error(alloc::format!(
                    // translator: %s is the name of a COPY option, e.g. FORCE_NOT_NULL
                    "{} column \"{}\" not referenced by COPY",
                    "FORCE_QUOTE",
                    String::from_utf8_lossy(&attname)
                ))
                .with_sqlstate(ERRCODE_INVALID_COLUMN_REFERENCE));
            }
            cstate.opts.force_quote_flags[(attnum - 1) as usize] = true;
        }
    }

    // Use client encoding when ENCODING option is not specified.
    if cstate.opts.file_encoding < 0 {
        cstate.file_encoding = mbutils_s::pg_get_client_encoding::call();
    } else {
        cstate.file_encoding = cstate.opts.file_encoding;
    }

    // Set up encoding conversion info if the file and server encodings differ.
    if cstate.file_encoding == mbutils_s::get_database_encoding::call()
        || cstate.file_encoding == PG_SQL_ASCII
    {
        cstate.need_transcoding = false;
    } else {
        cstate.need_transcoding = true;
    }

    // See Multibyte encoding comment above.
    cstate.encoding_embeds_ascii =
        mbutils_s::pg_encoding_is_client_only::call(cstate.file_encoding);

    cstate.copy_dest = CopyDest::File; // default

    if let Some(cb) = data_dest_cb {
        progress_vals[1] = PROGRESS_COPY_TYPE_CALLBACK;
        cstate.copy_dest = CopyDest::Callback;
        cstate.data_dest_cb = Some(cb);
    } else if pipe {
        progress_vals[1] = PROGRESS_COPY_TYPE_PIPE;

        debug_assert!(!is_program); // the grammar does not allow this
        if where_to_send_output() != CommandDest::Remote {
            cstate.copy_file = Some(fd_s::stdout_stream::call());
        }
        // else: copy_file stays None; DoCopyTo will SendCopyBegin to the FE.
    } else {
        let fname = filename.expect("non-pipe COPY TO requires a filename");
        cstate.filename = Some(PgString::from_str_in(fname, mcx)?);
        cstate.is_program = is_program;

        if is_program {
            progress_vals[1] = PROGRESS_COPY_TYPE_PROGRAM;
            cstate.copy_file = Some(fd_s::open_pipe_stream_write::call(fname)?);
        } else {
            progress_vals[1] = PROGRESS_COPY_TYPE_FILE;

            // Prevent write to relative path.
            if !port_path_seams::is_absolute_path::call(fname) {
                return Err(PgError::error("relative path not allowed for COPY to file")
                    .with_sqlstate(ERRCODE_INVALID_NAME));
            }

            // umask + AllocateFile (PG_TRY/umask restore) + fstat/S_ISDIR are
            // fd/OS-owned (copyto.c:952-985).
            cstate.copy_file = Some(fd_s::open_copy_to_file::call(fname)?);
        }
    }

    // initialize progress
    let relid = match cstate.rel.as_ref() {
        Some(rel) => rel.rd_id,
        None => InvalidOid,
    };
    progress_s::pgstat_progress_start_command::call(ProgressCommandType::Copy, relid);
    progress_s::pgstat_progress_update_multi_param::call(&progress_cols, &progress_vals);

    cstate.bytes_processed = 0;

    Ok(cstate)
}

/// `EndCopyTo(cstate)` (copyto.c:1005).
pub fn EndCopyTo(mut cstate: CopyToStateData<'_>) -> PgResult<()> {
    if let Some(query_desc) = cstate.query_desc.take() {
        // Close down the query and free resources.
        execmain_s::end_copy_query::call(query_desc.exec_token)?;
        backend_utils_time_snapmgr_seams::pop_active_snapshot::call()?;
    }

    // Clean up storage.
    end_copy(&cstate)?;
    Ok(())
}

/// `DoCopyTo(cstate)` (copyto.c:1026) — the COPY TO main loop; returns the
/// number of rows processed.
pub fn DoCopyTo(cstate: &mut CopyToStateData<'_>) -> PgResult<u64> {
    let pipe = cstate.filename.is_none() && cstate.data_dest_cb.is_none();
    let fe_copy = pipe && where_to_send_output() == CommandDest::Remote;

    if fe_copy {
        send_copy_begin(cstate)?;
    }

    let tup_desc: TupleDesc = match cstate.rel.as_ref() {
        Some(rel) => clone_tupdesc(cstate.mcx, rel.rd_att.as_ref())?,
        None => {
            let qd = cstate.query_desc.as_ref().expect("COPY query TO with no QueryDesc");
            clone_tupdesc(cstate.mcx, &deref_tupdesc(qd))?
        }
    };
    let num_phys_attrs = tup_desc_natts(&tup_desc);

    // cstate->opts.null_print_client = cstate->opts.null_print; (default)
    cstate.opts.null_print_client = cstate.opts.null_print.clone_in(cstate.mcx)?;

    // We use fe_msgbuf as a per-row buffer regardless of copy_dest.
    cstate.fe_msgbuf = StringInfo::new_in(cstate.mcx);

    // Get info about the columns we need to process (out_functions lookup).
    // cstate->out_functions = palloc(num_phys_attrs * sizeof(FmgrInfo));
    cstate.out_functions = make_unresolved_finfo_vec(cstate.mcx, num_phys_attrs as usize)?;
    let attnums: alloc::vec::Vec<AttrNumber> = cstate.attnumlist.iter().copied().collect();
    for attnum in attnums {
        let atttypid = td(&tup_desc).attr((attnum - 1) as usize).atttypid;
        let finfo = routine_out_func(cstate, atttypid)?;
        cstate.out_functions[(attnum - 1) as usize] = finfo;
    }

    // The per-row reset context (cstate->rowcontext) is the C palloc-recovery
    // device for datatype output routines; in the owned model output values are
    // allocated in `cstate.mcx` and dropped per row, so no separate context is
    // materialized here.

    // routine->CopyToStart(cstate, tupDesc);
    routine_start(cstate, &tup_desc)?;

    let processed: u64;
    if let Some(rel) = cstate.rel.as_ref() {
        // The table scan (copyto.c:1071-1100).
        // table_beginscan(cstate->rel, GetActiveSnapshot(), 0, NULL): the active
        // snapshot is passed explicitly across the seam.
        let rel_alias = rel.alias();
        let snapshot = backend_utils_time_snapmgr_seams::get_active_snapshot::call()?
            .expect("COPY TO scan with no active snapshot");
        let scandesc = tableam_s::table_beginscan::call(&rel_alias, snapshot)?;
        let mut slot = backend_access_table_tableam::table_slot_create(cstate.mcx, &rel_alias)?;

        let mut local_processed: u64 = 0;
        while tableam_s::table_scan_getnextslot::call(scandesc, &mut slot)? {
            // CHECK_FOR_INTERRUPTS();
            backend_tcop_postgres_seams::check_for_interrupts::call()?;

            // slot_getallattrs(slot) — CopyOneRowTo re-deconstructs the tuple
            // (it owns the deformed-columns view it passes to the routine).

            // Format and send the data
            copy_one_row_to(cstate, &slot)?;

            // Increment the number of processed tuples, and report progress.
            local_processed += 1;
            progress_s::pgstat_progress_update_param::call(
                PROGRESS_COPY_TUPLES_PROCESSED,
                local_processed as i64,
            );
        }

        backend_executor_execTuples_seams::exec_drop_single_tuple_table_slot::call(slot)?;
        tableam_s::table_endscan::call(scandesc)?;
        processed = local_processed;
    } else {
        // run the plan --- the dest receiver will send tuples
        // ExecutorRun(cstate->queryDesc, ForwardScanDirection, 0);
        let exec_token = cstate.query_desc.as_ref().unwrap().exec_token;
        let receiver = cstate.receiver.expect("COPY query TO with no receiver");
        receiver_bind(receiver, cstate);
        let run = execmain_s::executor_run_copy::call(exec_token);
        receiver_unbind(receiver);
        run?;

        // processed = ((DR_copy *) cstate->queryDesc->dest)->processed;
        // The receiver incremented `cstate.receiver_processed` per tuple inside
        // `copy_dest_receive`, exactly mirroring the C DR_copy counter.
        processed = cstate.receiver_processed;
    }

    // routine->CopyToEnd(cstate);
    routine_end(cstate)?;

    // MemoryContextDelete(cstate->rowcontext) — see the rowcontext note above.

    if fe_copy {
        send_copy_end(cstate)?;
    }

    Ok(processed)
}

/// `CopyOneRowTo(cstate, slot)` (copyto.c:1122).
fn copy_one_row_to(cstate: &mut CopyToStateData<'_>, slot: &TupleTableSlot) -> PgResult<()> {
    // MemoryContextReset(cstate->rowcontext); switch to rowcontext: the owned
    // model recovers per-row output allocations by dropping them as locals.

    // Make sure the tuple is fully deconstructed: the deformed (value, isnull)
    // view is what the format routine reads (C's `slot->tts_values[i]`).
    let cols = backend_executor_execTuples_seams::slot_getallattrs::call(cstate.mcx, slot)?;

    routine_one_row(cstate, &cols)
}

/// `copy_dest_receive(slot, self)` (copyto.c:1398) — the inward seam impl: the
/// executor's COPY-OUT receiver re-enters here with the receiver handle that
/// carries the live cstate.
fn copy_dest_receive(receiver: u64, slot: &mut TupleTableSlot) -> PgResult<bool> {
    // CopyToState cstate = ((DR_copy *) self)->cstate;
    let ptr = RECEIVERS.with(|r| {
        let reg = r.borrow();
        reg.get((receiver - 1) as usize)
            .and_then(|s| s.as_ref())
            .map(|s| s.cstate)
            .unwrap_or(core::ptr::null_mut())
    });
    if ptr.is_null() {
        panic!("backend-commands-copyto: copy_dest_receive on an unbound receiver");
    }
    // SAFETY: the pointer is the live cstate bound for the duration of the
    // synchronous executor run (DoCopyTo's query branch), mirroring C's
    // DR_copy.cstate alias.
    let cstate: &mut CopyToStateData<'_> = unsafe { &mut *(ptr as *mut CopyToStateData<'_>) };

    // Send the data.
    copy_one_row_to(cstate, slot)?;

    // Increment the number of processed tuples, and report the progress.
    cstate.receiver_processed += 1;
    progress_s::pgstat_progress_update_param::call(
        PROGRESS_COPY_TUPLES_PROCESSED,
        cstate.receiver_processed as i64,
    );
    Ok(true)
}

/// `CreateCopyDestReceiver()` (copyto.c:1435) — build the COPY-TO
/// `DestReceiver` (used by `COPY (query) TO`). Returns a receiver handle the
/// executor's COPY-OUT dispatch drives through [`copy_dest_receive`].
pub fn CreateCopyDestReceiver() -> u64 {
    receiver_register()
}

/* ===================================================================== */
/* Attribute output (text / CSV escaping) — byte-exact                   */
/* ===================================================================== */

/// `CopyAttributeOutText(cstate, string)` (copyto.c:1147).
fn copy_attribute_out_text(cstate: &mut CopyToStateData<'_>, string: &[u8]) -> PgResult<()> {
    let delimc = cstate.opts.delim;

    // ptr = need_transcoding ? pg_server_to_any(...) : string;
    let converted: Option<alloc::vec::Vec<u8>> = if cstate.need_transcoding {
        mbutils_s::pg_server_to_any::call(cstate.mcx, string, cstate.file_encoding)?
            .map(|v| v.to_vec())
    } else {
        None
    };
    let ptr: &[u8] = match &converted {
        Some(v) => v,
        None => string,
    };

    let mut i: usize = 0;
    let mut start: usize = 0;
    let n = ptr.len();

    if cstate.encoding_embeds_ascii {
        while i < n {
            let c = ptr[i];
            if c < 0x20 {
                let esc = match c {
                    0x08 => Some(b'b'),
                    0x0c => Some(b'f'),
                    0x0a => Some(b'n'),
                    0x0d => Some(b'r'),
                    0x09 => Some(b't'),
                    0x0b => Some(b'v'),
                    _ => {
                        // If it's the delimiter, must backslash it.
                        if c == delimc {
                            None
                        } else {
                            // All ASCII control chars are length 1.
                            i += 1;
                            continue;
                        }
                    }
                };
                let outc = esc.unwrap_or(c);
                dumpsofar(cstate, ptr, start, i)?;
                copy_send_char(cstate, b'\\')?;
                copy_send_char(cstate, outc)?;
                i += 1;
                start = i; // do not include char in next run
            } else if c == b'\\' || c == delimc {
                dumpsofar(cstate, ptr, start, i)?;
                copy_send_char(cstate, b'\\')?;
                start = i; // we include char in next run
                i += 1;
            } else if is_highbit_set(c) {
                i += mbutils_s::pg_encoding_mblen::call(cstate.file_encoding, &ptr[i..]) as usize;
            } else {
                i += 1;
            }
        }
    } else {
        while i < n {
            let c = ptr[i];
            if c < 0x20 {
                let esc = match c {
                    0x08 => Some(b'b'),
                    0x0c => Some(b'f'),
                    0x0a => Some(b'n'),
                    0x0d => Some(b'r'),
                    0x09 => Some(b't'),
                    0x0b => Some(b'v'),
                    _ => {
                        if c == delimc {
                            None
                        } else {
                            i += 1;
                            continue;
                        }
                    }
                };
                let outc = esc.unwrap_or(c);
                dumpsofar(cstate, ptr, start, i)?;
                copy_send_char(cstate, b'\\')?;
                copy_send_char(cstate, outc)?;
                i += 1;
                start = i;
            } else if c == b'\\' || c == delimc {
                dumpsofar(cstate, ptr, start, i)?;
                copy_send_char(cstate, b'\\')?;
                start = i;
                i += 1;
            } else {
                i += 1;
            }
        }
    }

    dumpsofar(cstate, ptr, start, i)
}

/// `#define DUMPSOFAR()` (copyto.c:1140) — flush the literal run `[start, ptr)`.
fn dumpsofar(cstate: &mut CopyToStateData<'_>, buf: &[u8], start: usize, ptr: usize) -> PgResult<()> {
    if ptr > start {
        copy_send_data(cstate, &buf[start..ptr])?;
    }
    Ok(())
}

/// `CopyAttributeOutCSV(cstate, string, use_quote)` (copyto.c:1300).
fn copy_attribute_out_csv(
    cstate: &mut CopyToStateData<'_>,
    string: &[u8],
    mut use_quote: bool,
) -> PgResult<()> {
    let delimc = cstate.opts.delim;
    let quotec = cstate.opts.quote;
    let escapec = cstate.opts.escape;
    let single_attr = cstate.attnumlist.len() == 1;

    // force quoting if it matches null_print (before conversion!)
    if !use_quote && string == cstate.opts.null_print.as_bytes() {
        use_quote = true;
    }

    let converted: Option<alloc::vec::Vec<u8>> = if cstate.need_transcoding {
        mbutils_s::pg_server_to_any::call(cstate.mcx, string, cstate.file_encoding)?
            .map(|v| v.to_vec())
    } else {
        None
    };
    let ptr: &[u8] = match &converted {
        Some(v) => v,
        None => string,
    };

    // Make a preliminary pass to discover if it needs quoting.
    if !use_quote {
        // Quote '\.' if it appears alone on a line.
        if single_attr && ptr == b"\\." {
            use_quote = true;
        } else {
            let mut t: usize = 0;
            let n = ptr.len();
            while t < n {
                let c = ptr[t];
                if c == delimc || c == quotec || c == b'\n' || c == b'\r' {
                    use_quote = true;
                    break;
                }
                if is_highbit_set(c) && cstate.encoding_embeds_ascii {
                    t += mbutils_s::pg_encoding_mblen::call(cstate.file_encoding, &ptr[t..]) as usize;
                } else {
                    t += 1;
                }
            }
        }
    }

    if use_quote {
        copy_send_char(cstate, quotec)?;

        // Same optimization strategy as in CopyAttributeOutText.
        let mut i: usize = 0;
        let mut start: usize = 0;
        let n = ptr.len();
        while i < n {
            let c = ptr[i];
            if c == quotec || c == escapec {
                dumpsofar(cstate, ptr, start, i)?;
                copy_send_char(cstate, escapec)?;
                start = i; // we include char in next run
            }
            if is_highbit_set(c) && cstate.encoding_embeds_ascii {
                i += mbutils_s::pg_encoding_mblen::call(cstate.file_encoding, &ptr[i..]) as usize;
            } else {
                i += 1;
            }
        }
        dumpsofar(cstate, ptr, start, i)?;

        copy_send_char(cstate, quotec)?;
    } else {
        // If it doesn't need quoting, we can just dump it as-is.
        copy_send_string(cstate, ptr)?;
    }
    Ok(())
}

/* ===================================================================== */
/* Small in-crate helpers                                                */
/* ===================================================================== */

/// `IS_HIGHBIT_SET(c)` (c.h).
#[inline]
fn is_highbit_set(c: u8) -> bool {
    c & 0x80 != 0
}

/// `*tupDesc` — the descriptor behind the `TupleDesc` (a non-NULL pointer in C;
/// `RelationGetDescr`/`ExecutorStart` always set it).
fn td<'a, 'mcx>(tup_desc: &'a TupleDesc<'mcx>) -> &'a types_tuple::heaptuple::TupleDescData<'mcx> {
    tup_desc.as_ref().expect("COPY TO with a NULL TupleDesc")
}

/// `tupDesc->natts`.
fn tup_desc_natts(tup_desc: &TupleDesc<'_>) -> i32 {
    td(tup_desc).natts
}

/// Clone a tuple descriptor into `mcx` (the C shares the `TupleDesc *`; the
/// owned model copies the consumed slice, as `RelationGetDescr`'s callers do
/// not mutate it).
fn clone_tupdesc<'mcx>(
    mcx: Mcx<'mcx>,
    src: &types_tuple::heaptuple::TupleDescData<'_>,
) -> PgResult<TupleDesc<'mcx>> {
    Ok(Some(mcx::alloc_in(mcx, src.clone_in(mcx)?)?))
}

/// Borrow the started query's result descriptor.
fn deref_tupdesc<'a, 'mcx>(
    qd: &'a QueryDesc<'mcx>,
) -> &'a types_tuple::heaptuple::TupleDescData<'mcx> {
    qd.tupDesc
        .as_ref()
        .expect("ExecutorStart did not set a result tupdesc")
}

/// Convert raw bytes (file-encoding) to a [`PgString`] charged to `mcx`. COPY's
/// transcoded strings are byte-exact; `PgString` requires UTF-8, matching the
/// server/file encodings the text path uses.
fn bytes_to_pgstring<'mcx>(mcx: Mcx<'mcx>, bytes: &[u8]) -> PgResult<PgString<'mcx>> {
    match core::str::from_utf8(bytes) {
        Ok(s) => PgString::from_str_in(s, mcx),
        Err(_) => {
            // Non-UTF-8 file encoding: keep the bytes via lossy text (the null
            // marker is compared as bytes downstream, so round-trip fidelity is
            // only needed for valid encodings the comparison path uses).
            let owned = alloc::string::String::from_utf8_lossy(bytes);
            PgString::from_str_in(&owned, mcx)
        }
    }
}

/// The bytes of a [`PgString`].
fn pgstring_bytes(s: &PgString<'_>) -> alloc::vec::Vec<u8> {
    s.as_bytes().to_vec()
}

/// `palloc0(n * sizeof(bool))` — a fresh `false`-filled flag vector in `mcx`.
fn make_false_vec(mcx: Mcx<'_>, n: usize) -> PgResult<PgVec<'_, bool>> {
    let mut v = mcx::vec_with_capacity_in(mcx, n)?;
    for _ in 0..n {
        v.push(false);
    }
    Ok(v)
}

/// `palloc(num_phys_attrs * sizeof(FmgrInfo))` — `num_phys_attrs` unresolved
/// `FmgrInfo`s in `mcx` (only the referenced slots get filled, as in C).
fn make_unresolved_finfo_vec(mcx: Mcx<'_>, n: usize) -> PgResult<PgVec<'_, FmgrInfo>> {
    let mut v = mcx::vec_with_capacity_in(mcx, n)?;
    for _ in 0..n {
        v.push(FmgrInfo::empty());
    }
    Ok(v)
}

/// Install every seam this crate owns.
pub fn init_seams() {
    backend_commands_copyto_seams::copy_dest_receive::set(copy_dest_receive);
}
