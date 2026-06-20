//! Port of PostgreSQL `src/backend/commands/copyfrom.c` — the COPY FROM
//! executor: bulk-load rows from an input source into a relation.
//!
//! This is the **driver** half of the COPY FROM module (the byte-exact input
//! codec lives in the sibling `backend-commands-copyfromparse` parser crate,
//! reached directly). The driver constructs and owns the
//! [`CopyParseState`](types_copy::CopyParseState) (the repo's owned-value model
//! of the C `CopyFromStateData`), drives `NextCopyFrom` per row, and inserts the
//! produced tuples into the target relation through the regular executor
//! machinery (an owned [`EStateData`], a `ResultRelInfo`, `table_tuple_insert`,
//! `ExecConstraints`, `ExecInsertIndexTuples`).
//!
//! Faithful 1:1 port of the control flow of `BeginCopyFrom` (copyfrom.c:1529),
//! `CopyFrom` (copyfrom.c:779) and `EndCopyFrom` (copyfrom.c:1914). The runtime
//! path exercised end to end is the **plain-table, text-format, all-columns,
//! no-default, single-insert** case (`CIM_SINGLE`); the partition-routing /
//! FDW-batch / multi-insert-buffer / trigger / volatile-default branches are
//! ported faithfully in structure but reach machinery that is gated behind a
//! loud `ereport(ERROR)` (those paths bottom out on keystone-blocked
//! subsystems — partition tuple routing, the FDW api, the trigger F1 front-half,
//! and the `expression_planner` plan-layer gate for column defaults).
//!
//! # By-reference input values (resolved)
//!
//! `NextCopyFrom` returns each field as an
//! [`AttrValue`](types_copy::AttrValue) whose `datum` is the canonical rich
//! `types_tuple::Datum<'mcx>` (`ByVal`/`ByRef`/`Cstring`/… arms) — the same type
//! the target slot's `tts_values` carry. A pass-by-value type (`int4`, `oid`, …)
//! rides the `ByVal` word; a pass-by-reference value (`text`, `varchar`, every
//! varlena) rides the `ByRef`/`Cstring` arm verbatim. The input-function seam
//! (`input_function_call_safe`) bridges `FmgrOut` → the rich `Datum` via
//! [`fmgr_out_to_datum`], so `COPY t(a int, b text)` flows end to end with no
//! information loss across the parser→driver boundary.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::collections::HashMap;

use mcx::Mcx;
use types_copy::{
    AttrValue, CopyFileHandle, CopyGetDataResult, CopyParseOptions, CopyParseState,
    CopySource, EolType, INPUT_BUF_SIZE, RAW_BUF_SIZE,
};
use types_core::primitive::{AttrNumber, Oid};
use types_tuple::backend_access_common_heaptuple::Datum as RichDatum;
use backend_utils_error::ereport;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_WRONG_OBJECT_TYPE,
    ERROR,
};
use types_nodes::{RriId, SlotId};
use types_rel::Relation;

use backend_commands_copyfromparse as parse;

mod init;
pub use init::init_seams;

/* ---------------------------------------------------------------------------
 * Constants (copyfrom.c:54-75 + commands/progress.h).
 * ------------------------------------------------------------------------- */

/// `TABLE_INSERT_SKIP_FSM` (access/tableam.h, bit 1).
const TABLE_INSERT_SKIP_FSM: i32 = 1 << 1;
/// `TABLE_INSERT_FROZEN` (access/tableam.h, bit 2).
const TABLE_INSERT_FROZEN: i32 = 1 << 2;

/// `PG_SQL_ASCII` (mb/pg_wchar.h) — encoding id 0.
const PG_SQL_ASCII: i32 = 0;

const RELKIND_RELATION: u8 = b'r';
const RELKIND_VIEW: u8 = b'v';
const RELKIND_MATVIEW: u8 = b'm';
const RELKIND_SEQUENCE: u8 = b'S';
const RELKIND_FOREIGN_TABLE: u8 = b'f';
const RELKIND_PARTITIONED_TABLE: u8 = b'p';

/// `ErrorLocation` for `ereport(...).finish(...)`.
fn here(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("copyfrom.c", 0, funcname)
}

/// `RELKIND_HAS_STORAGE(relkind)` (pg_class.h): r/i/S/t/m.
fn relkind_has_storage(relkind: u8) -> bool {
    const RELKIND_TOASTVALUE: u8 = b't';
    matches!(
        relkind,
        RELKIND_RELATION | RELKIND_SEQUENCE | RELKIND_TOASTVALUE | RELKIND_MATVIEW
    )
}

/* ===========================================================================
 * The COPY input data source registry (the FILE* stand-in).
 *
 * The C `cstate->copy_file` is a `FILE *`; the owned model carries a
 * [`CopyFileHandle`] token, and this crate owns the actual byte source it names
 * (a buffered file image, or the single-user backend's stdin). The
 * `copy_get_data_file` seam reads from here.
 * =========================================================================== */

/// One open COPY input source.
enum CopySourceReader {
    /// `cstate->copy_file = stdin` (single-user backend): read from the
    /// program's standard input.
    Stdin,
    /// `AllocateFile(filename, PG_BINARY_R)`: the whole file image, read once,
    /// consumed front to back.
    File { data: Vec<u8>, pos: usize },
    /// `copy_src == COPY_FRONTEND`: COPY data arriving over the libpq wire as
    /// `CopyData`/`CopyDone` protocol messages. `residual` holds bytes from the
    /// last `CopyData` message not yet consumed; `eof` is set when `CopyDone`
    /// (or the EOF marker) was seen.
    Frontend { residual: Vec<u8>, pos: usize, eof: bool },
}

thread_local! {
    /// `CopyFileHandle` → its open reader. Keyed by the token the driver stamps
    /// into `cstate.copy_file`.
    static SOURCES: RefCell<HashMap<u64, CopySourceReader>> = RefCell::new(HashMap::new());
    /// Monotone allocator for `CopyFileHandle` tokens.
    static NEXT_HANDLE: RefCell<u64> = const { RefCell::new(1) };
}

/// Register a new reader, returning its handle token.
fn register_source(reader: CopySourceReader) -> CopyFileHandle {
    let h = NEXT_HANDLE.with(|n| {
        let mut n = n.borrow_mut();
        let h = *n;
        *n += 1;
        h
    });
    SOURCES.with(|s| s.borrow_mut().insert(h, reader));
    CopyFileHandle(h)
}

/// Drop a reader (the C `FreeFile` / closing `stdin` is a no-op).
fn release_source(handle: CopyFileHandle) {
    SOURCES.with(|s| s.borrow_mut().remove(&handle.0));
}

/// `CopyGetData` `COPY_FILE` leg (copyfromparse.c:251-259) — read up to
/// `maxread` bytes from the named source. This is the installed body of the
/// `copy_get_data_file` seam.
fn copy_get_data_file_impl(
    cstate: &CopyParseState<'_>,
    maxread: i32,
) -> PgResult<CopyGetDataResult> {
    let handle = match cstate.copy_file {
        Some(h) => h,
        None => {
            return Err(PgError::error(
                "COPY FROM: data source is not open (copy_file is NULL)",
            ))
        }
    };
    let maxread = maxread.max(0) as usize;

    SOURCES.with(|s| {
        let mut map = s.borrow_mut();
        let reader = map.get_mut(&handle.0).ok_or_else(|| {
            PgError::error("COPY FROM: data source handle is not registered")
        })?;
        match reader {
            CopySourceReader::File { data, pos } => {
                let end = (*pos + maxread).min(data.len());
                let chunk = data[*pos..end].to_vec();
                *pos = end;
                let reached_eof = *pos >= data.len();
                Ok(CopyGetDataResult {
                    data: chunk,
                    reached_eof,
                })
            }
            CopySourceReader::Stdin => {
                use std::io::Read;
                let mut buf = vec![0u8; maxread];
                let n = std::io::stdin().read(&mut buf).map_err(|e| {
                    PgError::error(format!("could not read from COPY FROM stdin: {e}"))
                })?;
                buf.truncate(n);
                Ok(CopyGetDataResult {
                    data: buf,
                    // In single-user mode a zero-length read means EOF.
                    reached_eof: n == 0,
                })
            }
            CopySourceReader::Frontend { .. } => {
                // Frontend reads are served by `copy_get_data_frontend_impl`
                // (the COPY_FRONTEND seam); `copy_get_data_file` is the
                // COPY_FILE leg only and must never see a frontend source.
                Err(PgError::error(
                    "COPY FROM: COPY_FILE read leg invoked on a COPY_FRONTEND source",
                ))
            }
        }
    })
}

/// `CopyGetData` `COPY_FRONTEND` leg (copyfromparse.c:260-342) — pull COPY data
/// off the libpq wire. Drains any residual bytes from the last `CopyData`
/// message; when empty and not at EOF, reads the next protocol message
/// (`CopyData` → its body; `CopyDone` → EOF; `CopyFail` → ERROR; `Flush`/`Sync`
/// → skipped). Installed as the `copy_get_data_frontend` seam body.
fn copy_get_data_frontend_impl(
    cstate: &CopyParseState<'_>,
    maxread: i32,
) -> PgResult<CopyGetDataResult> {
    use backend_libpq_pqcomm as pqcomm;

    let handle = cstate
        .copy_file
        .ok_or_else(|| PgError::error("COPY FROM: frontend source is not open"))?;
    let maxread = maxread.max(0) as usize;

    // First, try to serve from the residual of the current message.
    let served = SOURCES.with(|s| -> PgResult<Option<CopyGetDataResult>> {
        let mut map = s.borrow_mut();
        let reader = map
            .get_mut(&handle.0)
            .ok_or_else(|| PgError::error("COPY FROM: frontend handle not registered"))?;
        match reader {
            CopySourceReader::Frontend { residual, pos, eof } => {
                if *pos < residual.len() {
                    let end = (*pos + maxread).min(residual.len());
                    let chunk = residual[*pos..end].to_vec();
                    *pos = end;
                    let drained = *pos >= residual.len();
                    Ok(Some(CopyGetDataResult {
                        data: chunk,
                        // EOF only when the frontend signalled CopyDone AND we
                        // have drained the last buffered message.
                        reached_eof: *eof && drained,
                    }))
                } else if *eof {
                    Ok(Some(CopyGetDataResult { data: Vec::new(), reached_eof: true }))
                } else {
                    Ok(None)
                }
            }
            _ => Err(PgError::error(
                "COPY FROM: COPY_FRONTEND read leg invoked on a non-frontend source",
            )),
        }
    })?;
    if let Some(r) = served {
        return Ok(r);
    }

    // Residual exhausted and not at EOF — read the next protocol message.
    // PqMsg type bytes (protocol.h): CopyData='d', CopyDone='c', CopyFail='f',
    // Flush='H', Sync='S'.
    const PQMSG_COPY_DATA: i32 = b'd' as i32;
    const PQMSG_COPY_DONE: i32 = b'c' as i32;
    const PQMSG_COPY_FAIL: i32 = b'f' as i32;
    const PQMSG_FLUSH: i32 = b'H' as i32;
    const PQMSG_SYNC: i32 = b'S' as i32;
    // PQ_LARGE_MESSAGE_LIMIT / PQ_SMALL_MESSAGE_LIMIT (libpq-be.h).
    const PQ_LARGE_MESSAGE_LIMIT: i32 = 0x3fff_ffff;
    const PQ_SMALL_MESSAGE_LIMIT: i32 = 10_000;
    const EOF: i32 = -1;

    let mcx = *cstate.attnumlist.allocator();
    loop {
        pqcomm::pq_startmsgread()?;
        let mtype = pqcomm::pq_getbyte()?;
        if mtype == EOF {
            return Err(PgError::error(
                "unexpected EOF on client connection with an open transaction",
            )
            .with_sqlstate(types_error::ERRCODE_CONNECTION_FAILURE));
        }
        let maxmsglen = match mtype {
            PQMSG_COPY_DATA => PQ_LARGE_MESSAGE_LIMIT,
            PQMSG_COPY_DONE | PQMSG_COPY_FAIL | PQMSG_FLUSH | PQMSG_SYNC => PQ_SMALL_MESSAGE_LIMIT,
            other => {
                return Err(PgError::error(format!(
                    "unexpected message type 0x{other:02X} during COPY from stdin"
                ))
                .with_sqlstate(types_error::ERRCODE_PROTOCOL_VIOLATION))
            }
        };
        let mut msg = types_stringinfo::StringInfo::new_in(mcx);
        if pqcomm::pq_getmessage(&mut msg, maxmsglen)? == EOF {
            return Err(PgError::error(
                "unexpected EOF on client connection with an open transaction",
            )
            .with_sqlstate(types_error::ERRCODE_CONNECTION_FAILURE));
        }
        match mtype {
            PQMSG_COPY_DATA => {
                let body: Vec<u8> = msg.data.as_slice().to_vec();
                // Stash the message body as the new residual and serve from it.
                let chunk_end = maxread.min(body.len());
                let chunk = body[..chunk_end].to_vec();
                let drained = chunk_end >= body.len();
                SOURCES.with(|s| {
                    if let Some(CopySourceReader::Frontend { residual, pos, .. }) =
                        s.borrow_mut().get_mut(&handle.0)
                    {
                        *residual = body;
                        *pos = chunk_end;
                    }
                    let _ = drained;
                });
                return Ok(CopyGetDataResult { data: chunk, reached_eof: false });
            }
            PQMSG_COPY_DONE => {
                SOURCES.with(|s| {
                    if let Some(CopySourceReader::Frontend { residual, pos, eof }) =
                        s.borrow_mut().get_mut(&handle.0)
                    {
                        residual.clear();
                        *pos = 0;
                        *eof = true;
                    }
                });
                return Ok(CopyGetDataResult { data: Vec::new(), reached_eof: true });
            }
            PQMSG_COPY_FAIL => {
                let m = String::from_utf8_lossy(msg.data.as_slice())
                    .trim_end_matches('\0')
                    .to_string();
                return Err(PgError::error(format!("COPY from stdin failed: {m}"))
                    .with_sqlstate(types_error::ERRCODE_QUERY_CANCELED));
            }
            // Flush/Sync: ignore and read the next message.
            PQMSG_FLUSH | PQMSG_SYNC => continue,
            _ => unreachable!(),
        }
    }
}

/// `ReceiveCopyBegin(cstate)` (copyfromparse.c:169-187) — build and send the
/// `CopyInResponse` (`'G'`) message announcing the per-column wire formats, then
/// flush so the frontend knows it may start sending COPY data.
fn receive_copy_begin_impl(mcx: Mcx<'_>, natts: i32, binary: bool) -> PgResult<()> {
    use backend_libpq_pqformat as pqf;

    let format: u16 = if binary { 1 } else { 0 };
    // pq_beginmessage(&buf, PqMsg_CopyInResponse='G');
    let mut buf = pqf::pq_beginmessage(mcx, b'G')?;
    // pq_sendbyte(&buf, format);       /* overall format */
    pqf::pq_sendbyte(&mut buf, format as u8)?;
    // pq_sendint16(&buf, natts);
    pqf::pq_sendint16(&mut buf, natts as u16)?;
    // for (i = 0; i < natts; i++) pq_sendint16(&buf, format);  /* per-col */
    for _ in 0..natts {
        pqf::pq_sendint16(&mut buf, format)?;
    }
    // pq_endmessage(&buf);
    pqf::pq_endmessage(buf)?;
    // pq_flush();  (FE must know it can send)
    backend_libpq_pqcomm::pq_flush()?;
    Ok(())
}

/* ===========================================================================
 * The owned driver state.
 *
 * The C `CopyFromStateData` is split in this repo: the parse-relevant subset is
 * `types_copy::CopyParseState` (owned by, and threaded through, the parser);
 * the executor extras the driver needs (range table, perminfos, the owned
 * EState) live alongside it here.
 * =========================================================================== */

/// `CopyFromState` — the owned driver state. Wraps the parser's
/// [`CopyParseState`] together with the executor state the insert loop needs.
pub struct CopyFromStateData<'mcx> {
    /// The parse-relevant cstate the parser drives.
    pub cstate: CopyParseState<'mcx>,
    /// `List *range_table` (== `pstate->p_rtable`).
    pub range_table: mcx::PgVec<'mcx, types_nodes::RangeTblEntry<'mcx>>,
    /// `List *rteperminfos` (== `pstate->p_rteperminfos`).
    pub rteperminfos: mcx::PgVec<'mcx, types_nodes::RTEPermissionInfo<'mcx>>,
    /// `bool volatile_defexprs`.
    pub volatile_defexprs: bool,
    /// `Node *whereClause` — preprocessed WHERE qual (`None` ⇒ no WHERE).
    pub where_clause: bool,
    /// `bool is_program`.
    pub is_program: bool,
    /// Per physical attribute, the *unplanned* default-value `Expr` returned by
    /// `build_column_default` (`None` ⇒ the column has no default). In C
    /// `BeginCopyFrom` runs `expression_planner` + `ExecInitExpr(defexpr, NULL)`
    /// in the copy context immediately; the owned `ExecInitExpr` needs an
    /// `EState`'s per-query context, which only exists once `CopyFrom` creates
    /// the executor state. We therefore carry the raw default expressions here
    /// and compile them into `cstate.defexprs` (+ build `defmap`/`num_defaults`)
    /// in `CopyFrom`, before the row loop. This is a faithful split of the same
    /// C steps — no behavior change, only the allocation context differs.
    pub raw_defexprs: mcx::PgVec<'mcx, Option<mcx::PgBox<'mcx, types_nodes::Expr>>>,
}

/* ===========================================================================
 * BeginCopyFrom (copyfrom.c:1529)
 * =========================================================================== */

/// `BeginCopyFrom(pstate, rel, whereClause, filename, is_program,
/// data_source_cb, attnamelist, options)` (copyfrom.c:1529) — set up to read
/// tuples from a file/stdin for COPY FROM.
///
/// In this repo the option processing + attnum resolution + RTE setup already
/// happened in `DoCopy` (commands/copy.c); the driver receives the already-open
/// `rel`, the resolved `attnumlist`, the parsed `opts`, the range table and the
/// per-column input/typioparam catalog info it computes here.
#[allow(clippy::too_many_arguments)]
pub fn BeginCopyFrom<'mcx>(
    mcx: Mcx<'mcx>,
    rel: Relation<'mcx>,
    opts: CopyParseOptions,
    file_encoding_opt: i32,
    attnumlist: mcx::PgVec<'mcx, AttrNumber>,
    range_table: mcx::PgVec<'mcx, types_nodes::RangeTblEntry<'mcx>>,
    rteperminfos: mcx::PgVec<'mcx, types_nodes::RTEPermissionInfo<'mcx>>,
    filename: Option<&str>,
    is_program: bool,
    has_where: bool,
) -> PgResult<CopyFromStateData<'mcx>> {
    let binary = opts.binary;

    // tupDesc = RelationGetDescr(cstate->rel); num_phys_attrs = tupDesc->natts;
    let num_phys_attrs = rel.rd_att.natts as usize;

    // Build the per-attribute catalog info: the input function and typioparam
    // for each physical attribute (copyfrom.c:1752-1819). Defaults
    // (build_column_default → expression_planner → ExecInitExpr) are the
    // #159 plan-layer keystone; if any column is missing from the column list
    // and is not generated, we'd need a default — which we cannot build yet, so
    // we raise rather than silently insert a wrong value (see below).
    let mut in_functions: mcx::PgVec<'mcx, types_fmgr::FmgrInfo> =
        mcx::vec_with_capacity_in(mcx, num_phys_attrs)?;
    let mut typioparams: mcx::PgVec<'mcx, Oid> = mcx::vec_with_capacity_in(mcx, num_phys_attrs)?;
    let mut defexprs: mcx::PgVec<'mcx, Option<mcx::PgBox<'mcx, types_nodes::execexpr::ExprState<'mcx>>>> =
        mcx::vec_with_capacity_in(mcx, num_phys_attrs)?;
    // Raw (unplanned) default Exprs carried to CopyFrom (see CopyFromStateData).
    let mut raw_defexprs: mcx::PgVec<'mcx, Option<mcx::PgBox<'mcx, types_nodes::Expr>>> =
        mcx::vec_with_capacity_in(mcx, num_phys_attrs)?;

    for attnum in 1..=num_phys_attrs {
        let att = &rel.rd_att.attrs[attnum - 1];
        if att.attisdropped {
            // We don't need info for dropped attributes; install a placeholder.
            in_functions.push(types_fmgr::FmgrInfo::empty());
            typioparams.push(0);
            defexprs.push(None);
            raw_defexprs.push(None);
            continue;
        }

        // CopyFromInFunc: getTypeInputInfo(atttypid, &infunc, &typioparam) +
        // fmgr_info(infunc, &in_functions[m]); (binary uses getTypeBinaryInputInfo
        // + the receive function — gated below.)
        if binary {
            return Err(unsupported(
                "BeginCopyFrom: binary-format COPY FROM input function setup \
                 (getTypeBinaryInputInfo) is not yet wired",
            ));
        }
        let (infunc_oid, typioparam) =
            backend_utils_cache_lsyscache_seams::get_type_input_info::call(att.atttypid)?;
        let resolved = backend_utils_fmgr_core::fmgr_info(mcx, infunc_oid)?;
        in_functions.push(resolved.finfo);
        typioparams.push(typioparam);

        defexprs.push(None);

        // Get default info if available (copyfrom.c:1769-1819).
        //
        // We only need the default values for columns that do not appear in the
        // column list, unless the DEFAULT option was given. We never need default
        // values for generated columns.
        let in_list = attnumlist.iter().any(|&a| a as usize == attnum);
        let mut raw = None;
        if (opts.default_print.is_some() || !in_list) && att.attgenerated == 0 {
            // defexpr = (Expr *) build_column_default(cstate->rel, attnum);
            //
            // build_column_default returns NULL when the column has no default
            // (e.g. `operand_f8` in the numeric `width_bucket_test` COPY): the
            // column is then simply left NULL on input. A non-NULL result is a
            // real default to compile and (if not copied from input) record in
            // defmap. The expression_planner + ExecInitExpr compile is deferred
            // to CopyFrom (where the EState's per-query context exists); see the
            // `raw_defexprs` field comment.
            raw = backend_rewrite_rewritehandler_seams::build_column_default::call(
                mcx,
                rel.alias(),
                attnum as i32,
            )?;
        }
        raw_defexprs.push(raw);
    }

    // Look up encoding conversion function (copyfrom.c:1685-1688):
    // `if (cstate->opts.file_encoding < 0) cstate->file_encoding =
    //  pg_get_client_encoding(); else cstate->file_encoding =
    //  cstate->opts.file_encoding;`
    let database_encoding = backend_utils_mb_mbutils_seams::get_database_encoding::call();
    let file_encoding = if file_encoding_opt < 0 {
        backend_utils_mb_mbutils_seams::pg_get_client_encoding::call()
    } else {
        file_encoding_opt
    };

    // Look up encoding conversion function (copyfrom.c:1693-1710).
    let need_transcoding = !(file_encoding == database_encoding
        || file_encoding == PG_SQL_ASCII
        || database_encoding == PG_SQL_ASCII);
    let conversion_proc = if need_transcoding {
        let proc = backend_utils_mb_mbutils_seams::find_default_conversion_proc::call(
            file_encoding,
            database_encoding,
        )?;
        if proc == types_core::InvalidOid {
            return Err(PgError::error(format!(
                "default conversion function for encoding \"{}\" to \"{}\" does not exist",
                common_encnames_seams::pg_encoding_to_char::call(file_encoding),
                common_encnames_seams::pg_encoding_to_char::call(database_encoding),
            ))
            .with_sqlstate(types_error::ERRCODE_UNDEFINED_FUNCTION));
        }
        proc
    } else {
        types_core::InvalidOid
    };

    // Build the parse state. raw_buf is RAW_BUF_SIZE+1; the codec fills it.
    let mut cstate = CopyParseState {
        opts,
        rel,
        attnumlist,
        copy_src: CopySource::COPY_FILE,
        copy_file: None,
        fe_msgbuf: None,
        data_source_cb: None,
        escontext: None,
        estate: None,
        econtext: None,
        file_encoding,
        need_transcoding,
        conversion_proc,
        bytes_processed: 0,
        cur_lineno: 0,
        eol_type: EolType::EOL_UNKNOWN,
        line_buf_valid: false,
        raw_buf: vec![0u8; (RAW_BUF_SIZE + 1) as usize],
        raw_buf_index: 0,
        raw_buf_len: 0,
        raw_reached_eof: false,
        input_is_raw: !need_transcoding,
        input_buf: Vec::new(),
        input_buf_index: 0,
        input_buf_len: 0,
        input_reached_eof: false,
        input_reached_error: false,
        line_buf: Vec::new(),
        attribute_buf: Vec::new(),
        attribute_cursor: 0,
        max_fields: 0,
        raw_fields: Vec::new(),
        in_functions,
        typioparams,
        defexprs,
        convert_select_flags: None,
        force_notnull_flags: vec![false; num_phys_attrs],
        force_null_flags: vec![false; num_phys_attrs],
        defaults: vec![false; num_phys_attrs],
        num_defaults: 0,
        defmap: Vec::new(),
        num_errors: 0,
        relname_only: false,
        cur_attname: None,
        cur_attval: None,
    };
    // raw_buf starts empty (len 0); we pre-sized the Vec only to mirror the C
    // palloc — the codec tracks the live length via raw_buf_len.
    cstate.raw_buf.clear();
    cstate.raw_buf.resize((RAW_BUF_SIZE + 1) as usize, 0);
    cstate.raw_buf_len = 0;

    // C (copyfrom.c BeginCopyFrom): `input_buf` is a distinct INPUT_BUF_SIZE+1
    // buffer only when transcoding; otherwise it aliases `raw_buf`
    // (`input_is_raw`). The codec tracks the live length via input_buf_len.
    if need_transcoding {
        cstate.input_buf.resize((INPUT_BUF_SIZE + 1) as usize, 0);
    }
    cstate.input_buf_index = 0;
    cstate.input_buf_len = 0;

    // Open the data source: stdin (pipe) or a server-side file.
    let pipe = filename.is_none();
    if pipe {
        // C: if (whereToSendOutput == DestRemote) ReceiveCopyBegin(cstate);
        //    else cstate->copy_file = stdin;
        if backend_utils_error::config::where_to_send_output() == types_dest::CommandDest::Remote {
            // ReceiveCopyBegin: send the CopyInResponse and switch the source to
            // the libpq frontend (COPY_FRONTEND).
            cstate.copy_file = Some(register_source(CopySourceReader::Frontend {
                residual: Vec::new(),
                pos: 0,
                eof: false,
            }));
            cstate.copy_src = CopySource::COPY_FRONTEND;
            let natts = cstate.attnumlist.len() as i32;
            receive_copy_begin_impl(mcx, natts, cstate.opts.binary)?;
        } else {
            // single-user backend: cstate->copy_file = stdin.
            cstate.copy_file = Some(register_source(CopySourceReader::Stdin));
        }
    } else {
        let filename = filename.expect("checked not-None above");
        if is_program {
            return Err(unsupported(
                "BeginCopyFrom: COPY FROM PROGRAM (OpenPipeStream) is not yet wired",
            ));
        }
        // AllocateFile(filename, PG_BINARY_R) + fstat: read the whole file image.
        let data =
            backend_storage_file_fd_seams::read_server_file::call(mcx, filename, 0, -1, false)?;
        let data = match data {
            Some(v) => v.to_vec(),
            None => {
                return Err(PgError::error(format!(
                    "could not open file \"{filename}\" for reading"
                )))
            }
        };
        cstate.copy_file = Some(register_source(CopySourceReader::File { data, pos: 0 }));
    }

    // cstate->routine->CopyFromStart(cstate, tupDesc): the text/CSV start
    // callback. It allocates the line_buf / raw_fields workspace and (for
    // transcoding) the input_buf; we run the no-transcoding text branch here.
    copy_from_start(&mut cstate)?;

    Ok(CopyFromStateData {
        cstate,
        range_table,
        rteperminfos,
        volatile_defexprs: false,
        where_clause: has_where,
        is_program,
        raw_defexprs,
    })
}

/// `CopyFromTextLikeStart` (copyfrom.c:169) — the text/CSV start callback
/// (no-transcoding branch). Binary's `CopyFromBinaryStart` (the header read) is
/// gated upstream.
fn copy_from_start(cstate: &mut CopyParseState<'_>) -> PgResult<()> {
    // NB: `cstate.need_transcoding` may be set — `BeginCopyFrom` configures the
    // file→server conversion (conversion_proc + sized input_buf) when the file
    // encoding differs from the database encoding, and the text/CSV read loop
    // (copyfromparse `CopyConvertBuf`) handles it. C's `CopyFromTextLikeStart`
    // has no transcoding restriction here either. (A stale debug_assert that
    // forbade transcoding used to panic every transcoding COPY FROM — e.g.
    // `COPY ... FROM ... WITH (ENCODING 'LATIN1')` — in debug builds.)
    // input_buf aliases raw_buf only when input_is_raw (no transcoding); when
    // transcoding it is a separate sized buffer, both set in BeginCopyFrom.
    cstate.input_reached_eof = false;
    // initStringInfo(&cstate->line_buf);
    cstate.line_buf = Vec::new();
    // attr_count = list_length(cstate->attnumlist); max_fields = attr_count;
    let attr_count = cstate.attnumlist.len() as i32;
    cstate.max_fields = attr_count;
    // raw_fields workspace.
    cstate.raw_fields = vec![None; attr_count as usize];
    Ok(())
}

/* ===========================================================================
 * EndCopyFrom (copyfrom.c:1914)
 * =========================================================================== */

/// `EndCopyFrom(cstate)` (copyfrom.c:1914) — close the data source and free the
/// COPY context.
pub fn EndCopyFrom(state: CopyFromStateData<'_>) -> PgResult<()> {
    // EndCopy(cstate): if copy_src == COPY_FILE && copy_file is a real file,
    // FreeFile / ClosePipeFromProgram. Here: drop the registered reader.
    if let Some(handle) = state.cstate.copy_file {
        release_source(handle);
    }
    Ok(())
}

/* ===========================================================================
 * CopyFrom (copyfrom.c:779) — the per-row insert loop (CIM_SINGLE path).
 * =========================================================================== */

/// `CopyFrom(cstate)` (copyfrom.c:779) — copy FROM file/stdin into the target
/// relation. Returns the number of rows processed.
///
/// This is the `CIM_SINGLE` (single-insert) realization: one
/// `table_tuple_insert` per row, with `ExecConstraints` + index maintenance.
/// The multi-insert-buffer / partition-routing / FDW-batch / BEFORE-trigger
/// branches of the C are gated (they reach keystone-blocked subsystems).
pub fn CopyFrom<'mcx>(mcx: Mcx<'mcx>, state: &mut CopyFromStateData<'mcx>) -> PgResult<u64> {
    let relkind = state.cstate.rel.rd_rel.relkind;

    // The target must be a plain, foreign, or partitioned relation, or an
    // INSTEAD OF INSERT view. We support plain tables; the rest is gated.
    if relkind != RELKIND_RELATION
        && relkind != RELKIND_FOREIGN_TABLE
        && relkind != RELKIND_PARTITIONED_TABLE
    {
        let name = state.cstate.rel.rd_rel.relname.as_str();
        if relkind == RELKIND_VIEW {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("cannot copy to view \"{name}\""))
                .errhint("To enable copying to a view, provide an INSTEAD OF INSERT trigger.")
                .finish(here("CopyFrom"))
                .map(|()| 0u64);
        } else if relkind == RELKIND_MATVIEW {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("cannot copy to materialized view \"{name}\""))
                .finish(here("CopyFrom"))
                .map(|()| 0u64);
        } else if relkind == RELKIND_SEQUENCE {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("cannot copy to sequence \"{name}\""))
                .finish(here("CopyFrom"))
                .map(|()| 0u64);
        } else {
            return ereport(ERROR)
                .errcode(ERRCODE_WRONG_OBJECT_TYPE)
                .errmsg(format!("cannot copy to non-table relation \"{name}\""))
                .finish(here("CopyFrom"))
                .map(|()| 0u64);
        }
    }

    if relkind == RELKIND_FOREIGN_TABLE || relkind == RELKIND_PARTITIONED_TABLE {
        return Err(unsupported(
            "CopyFrom: foreign-table / partitioned-table COPY FROM (FDW api / \
             partition tuple routing) is not yet wired",
        ));
    }
    if state.where_clause {
        return Err(unsupported(
            "CopyFrom: COPY FROM ... WHERE (ExecInitQual/ExecQual) is not yet wired",
        ));
    }
    if state.cstate.rel.rd_trigdesc.is_some() {
        return Err(unsupported(
            "CopyFrom: COPY FROM into a table with triggers \
             (trigger F1 front-half) is not yet wired",
        ));
    }

    // CommandId mycid = GetCurrentCommandId(true);
    let mycid = backend_access_transam_xact_seams::get_current_command_id::call(true)?;
    // int ti_options = 0. (We faithfully omit the new-in-subxact SKIP_FSM
    // optimization — the relcache subid fields are not carried — and COPY FREEZE
    // is gated upstream; both only affect performance/visibility, not the result
    // of the common path.)
    let ti_options: i32 = 0;
    let _ = (TABLE_INSERT_SKIP_FSM, TABLE_INSERT_FROZEN, relkind_has_storage);

    // EState *estate = CreateExecutorState();
    let mut estate_owned = backend_executor_execUtils::create_executor_state_in(mcx)?;
    let processed = {
        let estate: &mut types_nodes::EStateData<'mcx> = &mut estate_owned;

        // ExecInitRangeTable(estate, cstate->range_table, cstate->rteperminfos,
        //                    bms_make_singleton(1));
        let unpruned = backend_nodes_core::bitmapset::bms_make_singleton(mcx, 1)?;
        let range_table = core::mem::replace(&mut state.range_table, mcx::PgVec::new_in(mcx));
        let perminfos = core::mem::replace(&mut state.rteperminfos, mcx::PgVec::new_in(mcx));
        backend_executor_execUtils::ExecInitRangeTable(
            estate,
            range_table,
            perminfos,
            Some(unpruned),
        )?;

        // resultRelInfo = makeNode(ResultRelInfo); ExecInitResultRelation(estate, rri, 1);
        let rri: RriId = estate.add_result_rel(types_nodes::ResultRelInfo::default())?;
        backend_executor_execUtils::ExecInitResultRelation(estate, rri, 1, false)?;

        // CheckValidResultRel(resultRelInfo, CMD_INSERT, ONCONFLICT_NONE, NIL);
        backend_executor_execMain_seams::check_valid_result_rel::call(
            estate,
            rri,
            types_nodes::nodes::CmdType::CMD_INSERT,
            types_nodes::nodes::OnConflictAction::ONCONFLICT_NONE,
            &[], /* mergeActions: COPY is INSERT-only */
        )?;

        // ExecOpenIndices(resultRelInfo, false);
        backend_executor_execIndexing_seams::exec_open_indices::call(estate, rri, false)?;

        // We do not set up a ModifyTableState / FDW init / batch size — CIM_SINGLE
        // plain table. AfterTriggerBeginQuery / BS/AS statement triggers and the
        // transition-capture setup are no-ops with no trigger descriptor; we skip
        // them faithfully (the has-trigger case was gated above).

        // singleslot = table_slot_create(rri->ri_RelationDesc, &estate->es_tupleTable);
        // bistate = GetBulkInsertState();
        let rel_alias = relation_alias(estate, rri);
        let slot = backend_access_table_tableam::table_slot_create(mcx, &rel_alias)?;
        let singleslot: SlotId = estate.push_slot_data(slot)?;
        let mut bistate = backend_access_heap_heapam::GetBulkInsertState()?;

        // GetPerTupleExprContext(estate): make the per-tuple ExprContext.
        let econtext = backend_executor_execUtils::MakePerTupleExprContext(estate)?;

        // Compile the default-value expressions (copyfrom.c:1769-1819 deferred
        // half). In C `BeginCopyFrom` runs expression_planner + ExecInitExpr in
        // the copy context; in the owned model `ExecInitExpr` needs the EState's
        // per-query context, which only exists now. For each physical attribute
        // with a non-NULL `build_column_default` result, compile it via
        // ExecPrepareExpr (= expression_planner + ExecInitExpr(node, NULL)) and,
        // if the column is not copied from input, record it in `defmap`.
        {
            let raw = core::mem::replace(&mut state.raw_defexprs, mcx::PgVec::new_in(mcx));
            for (i, maybe) in raw.into_iter().enumerate() {
                let defexpr = match maybe {
                    Some(e) => e,
                    None => continue,
                };
                // defexpr = expression_planner(defexpr);
                // defexprs[i] = ExecInitExpr(defexpr, NULL);
                let compiled =
                    backend_executor_execExpr_seams::exec_prepare_expr::call(&defexpr, estate)?;
                state.cstate.defexprs[i] = Some(compiled);
                // if (!list_member_int(cstate->attnumlist, attnum)) { defmap... }
                let attnum = (i + 1) as AttrNumber;
                if !state.cstate.attnumlist.iter().any(|&a| a == attnum) {
                    state.cstate.defmap.push(i as i32);
                    state.cstate.num_defaults += 1;
                }
                // volatile_defexprs tracking only governs the (unported)
                // multi-insert optimization; the single-insert path used here is
                // unaffected, so we leave state.volatile_defexprs as-is.
            }
        }

        // Wire the per-tuple ExprContext and the EState back-link the default
        // evaluator (exec_eval_expr seam) reads. These are only dereferenced
        // when `num_defaults > 0` (i.e. a real default exists); the common COPY
        // with a full column list never touches them.
        state.cstate.econtext = Some(econtext);
        state.cstate.estate = Some(types_nodes::execnodes::EStateLink::from_ref(estate));

        let result_oid = relation_alias(estate, rri).rd_id;

        let mut processed: u64 = 0;
        loop {
            // ResetPerTupleExprContext(estate);
            backend_executor_execUtils::ResetPerTupleExprContext(estate);

            // ExecClearTuple(myslot);
            backend_executor_execTuples::slot_store_fetch::ExecClearTuple(estate.slot_data_mut(singleslot))?;

            // NextCopyFrom(cstate, econtext, myslot->tts_values, myslot->tts_isnull):
            // pull one row of AttrValues from the parser. The C
            // `CopyFromErrorCallback` is active across this call; attach its
            // context line on error propagation (copyfrom.c:251).
            let row = parse::NextCopyFrom(&mut state.cstate)
                .map_err(|e| e.add_context(copy_from_error_context(&state.cstate)))?;
            let row = match row {
                Some(r) => r,
                None => break,
            };

            // Store the values/nulls into the slot (ExecStoreVirtualTuple).
            store_row_into_slot(estate, singleslot, &row)?;

            // (re-)initialize tts_tableOid before constraints.
            estate.slot_mut(singleslot).tts_tableOid = result_oid;

            // ExecComputeStoredGenerated / ExecConstraints: if the relation has
            // constraints, check them. (No generated-stored columns on the
            // common path; ExecComputeStoredGenerated is gated by the descriptor
            // flag inside the executor.)
            if relation_alias(estate, rri).rd_att.constr.is_some() {
                backend_executor_execMain_seams::exec_constraints::call(estate, rri, singleslot)?;
            }

            // table_tuple_insert(rel, myslot, mycid, ti_options, bistate);
            {
                let rel = relation_alias(estate, rri);
                let slot_ref = estate.slot_data_mut(singleslot);
                backend_access_table_tableam::table_tuple_insert(
                    mcx,
                    &rel,
                    slot_ref,
                    mycid,
                    ti_options,
                    Some(&mut bistate),
                )?;
            }

            // index entries.
            if estate.result_rel(rri).ri_NumIndices > 0 {
                let _recheck = backend_executor_execIndexing_seams::exec_insert_index_tuples::call(
                    mcx,
                    estate,
                    rri,
                    singleslot,
                    false,
                    false,
                    None,
                    &[],
                    false,
                )?;
            }

            // ExecARInsertTriggers: no-op with no trigger descriptor.

            processed += 1;
        }

        // FreeBulkInsertState(bistate).
        backend_access_heap_heapam::FreeBulkInsertState(&mut bistate);

        // AfterTriggerEndQuery / ExecASInsertTriggers: no-op (no triggers).
        // ExecResetTupleTable / ExecCloseResultRelations / ExecCloseRangeTableRelations.
        backend_executor_execUtils::ExecResetTupleTable(estate, false)?;
        backend_executor_execUtils::ExecCloseResultRelations(estate)?;
        backend_executor_execUtils::ExecCloseRangeTableRelations(estate)?;

        processed
    };

    // FreeExecutorState(estate);
    backend_executor_execUtils::free_executor_state_in(estate_owned)?;

    Ok(processed)
}

/// Store one row of [`AttrValue`]s into the slot's `tts_values`/`tts_isnull`,
/// then `ExecStoreVirtualTuple`. Mirrors the C "directly store the values/nulls
/// array in the slot" + `ExecStoreVirtualTuple`.
fn store_row_into_slot<'mcx>(
    estate: &mut types_nodes::EStateData<'mcx>,
    slot: SlotId,
    row: &[AttrValue<'mcx>],
) -> PgResult<()> {
    let s = estate.slot_data_mut(slot);
    let base = s.base_mut();
    // Ensure the value/null arrays are sized to the descriptor.
    let natts = row.len();
    base.tts_values.clear();
    base.tts_isnull.clear();
    base.tts_values
        .try_reserve(natts)
        .map_err(|_| PgError::error("COPY FROM: out of memory sizing slot values"))?;
    base.tts_isnull
        .try_reserve(natts)
        .map_err(|_| PgError::error("COPY FROM: out of memory sizing slot nulls"))?;
    for av in row.iter() {
        // The AttrValue datum is already the canonical rich slot Datum
        // (`ByVal`/`ByRef`/`Cstring`/…); move a clone straight into tts_values.
        // A by-reference (varlena `text`) value flows through verbatim.
        base.tts_values.push(av.datum.clone());
        base.tts_isnull.push(av.isnull);
    }
    backend_executor_execTuples::slot_store_fetch::ExecStoreVirtualTuple(s)
}

/* ===========================================================================
 * Seam body: input_function_call_safe (the fmgr value layer).
 *
 * The parser calls this per text field. We resolve `&cstate.in_functions[m]`,
 * `cstate.typioparams[m]`, the typmod and `cstate.escontext`, and dispatch the
 * real `InputFunctionCallSafe`. A by-reference (varlena) result is the keystone
 * wall — we raise a clear error rather than drop the bytes.
 * =========================================================================== */

fn input_function_call_safe_impl<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyParseState<'mcx>,
    m: i32,
    string: Option<&str>,
    typmod: i32,
) -> PgResult<Option<RichDatum<'mcx>>> {
    let idx = m as usize;
    let flinfo = cstate.in_functions[idx].clone();
    let typioparam = cstate.typioparams[idx];
    // Re-derive the resolution from the resolved fn_oid (deterministic; the
    // builtin fast path). C stores fn_addr on the FmgrInfo; here the resolution
    // is keyed on fn_oid.
    let resolved = backend_utils_fmgr_core::fmgr_info(mcx, flinfo.fn_oid)?;

    let out = backend_utils_fmgr_core::input_function_call_safe_typed(
        mcx,
        &resolved.resolution,
        resolved.finfo,
        string,
        typioparam,
        typmod,
        cstate.escontext.as_mut(),
    )?;

    match out {
        // Soft error trapped (ON_ERROR IGNORE): C returns false ⇒ None.
        None => Ok(None),
        // The fmgr result is already the canonical rich Datum carrier. A
        // by-reference (varlena `text`/`varchar`) result flows through verbatim
        // via its `ByRef`/`Cstring` arm — exactly the slot `tts_values` element.
        Some(fmgr_out) => Ok(Some(fmgr_out_to_datum(mcx, fmgr_out)?)),
    }
}

/// `FmgrOut<'mcx>` → the canonical rich [`RichDatum`]. By-value carries the
/// machine word; a by-reference payload becomes the matching owned arm. Mirrors
/// the established `fmgr_out_to_datum` bridge (jsonfuncs/populate.rs).
fn fmgr_out_to_datum<'mcx>(
    mcx: Mcx<'mcx>,
    out: types_fmgr::FmgrOut<'mcx>,
) -> PgResult<RichDatum<'mcx>> {
    use types_fmgr::boundary::RefPayload;
    match out {
        types_fmgr::FmgrOut::ByVal(d) => Ok(d),
        types_fmgr::FmgrOut::Ref(payload) => match payload {
            RefPayload::Varlena(b) => {
                let mut v = mcx::vec_with_capacity_in::<u8>(mcx, b.len())?;
                v.extend_from_slice(&b);
                Ok(RichDatum::ByRef(v))
            }
            RefPayload::Cstring(s) => Ok(RichDatum::Cstring(s)),
            RefPayload::Composite(image) => Ok(RichDatum::Composite(
                types_tuple::backend_access_common_heaptuple::FormedTuple::from_datum_image(
                    mcx, &image,
                )?,
            )),
            RefPayload::Expanded(eo) => Ok(RichDatum::Expanded(eo)),
            RefPayload::Internal(state) => Ok(RichDatum::Internal(state)),
        },
    }
}

/* ===========================================================================
 * Small helpers + the remaining (gated) seam bodies.
 * =========================================================================== */

fn unsupported(msg: &str) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        .errmsg(msg.to_string())
        .into_error()
}

/// `CopyLimitPrintoutLength(str)` (copyfrom.c:329) — cap a value printed in an
/// error context at `MAX_COPY_DATA_DISPLAY` (1024) bytes (truncated on a
/// character boundary), appending `...` when shortened.
fn copy_limit_printout_length(s: &str) -> String {
    const MAX_COPY_DATA_DISPLAY: usize = 1024;
    let slen = s.len();
    let len = backend_utils_mb_mbutils_seams::pg_mbcliplen::call(
        s.as_bytes(),
        slen as i32,
        MAX_COPY_DATA_DISPLAY as i32,
    ) as usize;
    if len == slen {
        s.to_string()
    } else {
        format!("{}...", &s[..len])
    }
}

/// `CopyFromErrorCallback(arg)` (copyfrom.c:251) — the COPY FROM error-context
/// callback, rendered as the single context line C's `errcontext()` would
/// append. The port attaches it on error propagation (the sanctioned
/// replacement for C's `error_context_stack` callback) around the per-row
/// `NextCopyFrom` call.
fn copy_from_error_context(cstate: &CopyParseState<'_>) -> String {
    let cur_relname = cstate.rel.rd_rel.relname.as_str();

    if cstate.relname_only {
        return format!("COPY {cur_relname}");
    }
    if cstate.opts.binary {
        // Can't usefully display the data.
        return match &cstate.cur_attname {
            Some(att) => format!(
                "COPY {cur_relname}, line {}, column {att}",
                cstate.cur_lineno
            ),
            None => format!("COPY {cur_relname}, line {}", cstate.cur_lineno),
        };
    }
    match (&cstate.cur_attname, &cstate.cur_attval) {
        (Some(att), Some(val)) => {
            // Error is relevant to a particular column.
            let attval = copy_limit_printout_length(val);
            format!(
                "COPY {cur_relname}, line {}, column {att}: \"{attval}\"",
                cstate.cur_lineno
            )
        }
        (Some(att), None) => {
            // Error is relevant to a particular column, value is NULL.
            format!(
                "COPY {cur_relname}, line {}, column {att}: null input",
                cstate.cur_lineno
            )
        }
        (None, _) => {
            // Error is relevant to a particular line; print it if line_buf is
            // still valid.
            if cstate.line_buf_valid {
                let lineval =
                    copy_limit_printout_length(&String::from_utf8_lossy(&cstate.line_buf));
                format!(
                    "COPY {cur_relname}, line {}: \"{lineval}\"",
                    cstate.cur_lineno
                )
            } else {
                format!("COPY {cur_relname}, line {}", cstate.cur_lineno)
            }
        }
    }
}

/// `ResultRelInfo.ri_RelationDesc` as a fresh alias (the open relation the
/// EState owns).
fn relation_alias<'mcx>(estate: &types_nodes::EStateData<'mcx>, rri: RriId) -> Relation<'mcx> {
    estate
        .result_rel(rri)
        .ri_RelationDesc
        .as_ref()
        .expect("ExecInitResultRelation opened ri_RelationDesc")
        .alias()
}
