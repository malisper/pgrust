// copyfrom.c, text/CSV from file or frontend, CIM_MULTI lane
// (heap_multi_insert buffering with a shared BulkInsertState, matching C's
// default insert method and its bulk relation-extension page geometry).

use backend_progress::progress::{
    PROGRESS_COPY_BYTES_TOTAL, PROGRESS_COPY_COMMAND, PROGRESS_COPY_COMMAND_FROM,
    PROGRESS_COPY_TUPLES_EXCLUDED, PROGRESS_COPY_TUPLES_PROCESSED, PROGRESS_COPY_TUPLES_SKIPPED,
    PROGRESS_COPY_TYPE, PROGRESS_COPY_TYPE_CALLBACK, PROGRESS_COPY_TYPE_FILE,
    PROGRESS_COPY_TYPE_PIPE,
};
use backend_progress::{
    pgstat_progress_end_command, pgstat_progress_start_command, pgstat_progress_update_multi_param,
    pgstat_progress_update_param, PROGRESS_COMMAND_COPY,
};
use elog::ereport;
use mcx::{vec_from_elem_in, Mcx, MemoryContext, PgVec};
use stringinfo::StringInfo;
use types_core::Oid;
use types_dest::CommandDest;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_fmgr::FmgrInfo;
use types_nodes::nodes_enums::CmdType;
use types_nodes::NodeList;
use types_rel::Relation;
use types_tuple::NameData;

use crate::fromparse::{EolType, INPUT_BUF_SIZE, RAW_BUF_SIZE};
use crate::{
    force_flags, unported, CopyFormatOptions, CopyGetAttnums, ProcessCopyOptions, RELKIND_RELATION,
};

pub(crate) enum CopySrc<'mcx, 's> {
    File { fd: i32, filename: &'s str },
    Frontend { msgbuf: StringInfo<'mcx> },
    // COPY_CALLBACK (copyfrom_internal.h): tablesync pulls bytes from the
    // publisher's COPY OUT stream. cb(buf, minread) fills up to buf.len()
    // bytes, at least minread unless the stream ends; 0 = EOF.
    // Lifetime-erased to 'static (SAFETY at construction): CopyFromState is
    // bounded by 's regardless, and a non-'static dyn's conservative dropck
    // ("Drop may observe borrows") breaks parallel.rs's worker teardown.
    Callback { cb: Box<dyn FnMut(&mut [u8], usize) -> PgResult<usize> + 'static> },
    /// Parallel COPY worker: one segmentator-cut input chunk (whole rows,
    /// in-memory). EOF at the chunk's end.
    Chunk(crate::parallel::ChunkCursor),
    /// FORMAT 'parquet': typed column batches from a server-side file (the
    /// reader owns the file handle; drop closes it on every exit path).
    Parquet(Box<crate::fromparquet::ParquetSrc>),
}

pub struct CopyFromState<'mcx, 's> {
    pub opts: CopyFormatOptions<'s>,
    pub(crate) src: CopySrc<'mcx, 's>,
    pub(crate) raw_buf: PgVec<'mcx, u8>,
    pub(crate) raw_buf_index: usize,
    pub(crate) raw_buf_len: usize,
    pub(crate) raw_reached_eof: bool,
    pub(crate) input_reached_eof: bool,
    pub(crate) input_reached_error: bool,
    pub(crate) input_buf: Option<PgVec<'mcx, u8>>,
    pub(crate) input_buf_index: usize,
    pub(crate) input_buf_len: usize,
    pub(crate) line_buf: PgVec<'mcx, u8>,
    pub(crate) line_buf_valid: bool,
    pub(crate) attribute_buf: PgVec<'mcx, u8>,
    pub(crate) binary_attr_buf: StringInfo<'mcx>,
    pub(crate) raw_fields: PgVec<'mcx, i32>,
    pub(crate) max_fields: usize,
    pub(crate) eol_type: EolType,
    pub cur_lineno: u64,
    pub(crate) cur_attidx: Option<usize>,
    pub(crate) cur_attval_off: Option<i32>,
    pub(crate) file_encoding: i32,
    pub(crate) need_transcoding: bool,
    pub(crate) conversion_proc: Oid,
    pub(crate) convertcx: MemoryContext,
    pub(crate) attnumlist: PgVec<'mcx, i16>,
    pub(crate) in_functions: PgVec<'mcx, FmgrInfo>,
    pub(crate) typioparams: PgVec<'mcx, Oid>,
    pub(crate) atttypmods: PgVec<'mcx, i32>,
    pub(crate) attnames: PgVec<'mcx, NameData>,
    pub(crate) force_notnull_flags: PgVec<'mcx, bool>,
    pub(crate) force_null_flags: PgVec<'mcx, bool>,
    pub(crate) convert_select_flags: Option<PgVec<'mcx, bool>>,
    // Per physical attribute; defmap lists attrs absent from attnumlist whose
    // default fills the column, defaults[] carries per-row DEFAULT markers.
    pub(crate) defexprs: PgVec<'mcx, Option<mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>>>,
    pub(crate) defmap: PgVec<'mcx, usize>,
    pub(crate) defaults: PgVec<'mcx, bool>,
    pub(crate) where_clause: NodeList<'mcx>,
    pub(crate) relname: String,
    pub(crate) escontext: Option<Box<types_fmgr::ErrorSaveNode>>,
    pub(crate) num_errors: u64,
    pub(crate) bytes_processed: u64,
    pub(crate) volatile_defexprs: bool,
}

impl CopyFromState<'_, '_> {
    pub(crate) fn attname(&self, m: usize) -> String {
        String::from_utf8_lossy(self.attnames[m].name_str()).into_owned()
    }

    pub fn num_errors(&self) -> u64 {
        self.num_errors
    }

    /// `cstate->escontext->error_occurred` (file_fdw's soft-error probe).
    pub fn soft_error_occurred(&self) -> bool {
        self.escontext.as_ref().is_some_and(|n| n.ctx.error_occurred())
    }

    pub fn reset_soft_error(&mut self) {
        if let Some(n) = self.escontext.as_mut() {
            n.ctx.reset_error_occurred();
        }
    }
}

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

/// `BeginCopyFrom` (copyfrom.c), text/CSV from file or frontend.
pub fn BeginCopyFrom<'mcx, 's>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    where_clause: NodeList<'mcx>,
    filename: Option<&'s str>,
    attnamelist: &NodeList<'_>,
    options: &NodeList<'s>,
    source_text: Option<&str>,
) -> PgResult<CopyFromState<'mcx, 's>> {
    begin_copy_from_guts(mcx, rel, where_clause, filename, None, attnamelist, options, source_text)
}

// BeginCopyFrom's data_source_cb form (COPY_CALLBACK): tablesync feeds bytes
// from the publisher's COPY OUT stream. cb(buf, minread) -> bytes written,
// 0 at end of stream.
pub fn BeginCopyFromCallback<'mcx, 's>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    attnamelist: &NodeList<'_>,
    options: &NodeList<'s>,
    cb: Box<dyn FnMut(&mut [u8], usize) -> PgResult<usize> + 's>,
) -> PgResult<CopyFromState<'mcx, 's>> {
    begin_copy_from_guts(mcx, rel, NodeList::nil(), None, Some(cb), attnamelist, options, None)
}

#[allow(clippy::too_many_arguments)]
fn begin_copy_from_guts<'mcx, 's>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    where_clause: NodeList<'mcx>,
    filename: Option<&'s str>,
    mut data_source_cb: Option<Box<dyn FnMut(&mut [u8], usize) -> PgResult<usize> + 's>>,
    attnamelist: &NodeList<'_>,
    options: &NodeList<'s>,
    source_text: Option<&str>,
) -> PgResult<CopyFromState<'mcx, 's>> {
    let opts = ProcessCopyOptions(true, options, source_text)?;
    let tup_desc = &rel.rd_att;
    let attnumlist = CopyGetAttnums(mcx, tup_desc, Some(rel), attnamelist)?;
    let num_phys_attrs = tup_desc.natts as usize;

    let force_notnull_flags = force_flags(
        mcx,
        tup_desc,
        Some(rel),
        &attnumlist,
        opts.force_notnull,
        opts.force_notnull_all,
        "FORCE_NOT_NULL",
    )?;
    let force_null_flags = force_flags(
        mcx,
        tup_desc,
        Some(rel),
        &attnumlist,
        opts.force_null,
        opts.force_null_all,
        "FORCE_NULL",
    )?;

    // C builds these after the force_notnull/force_null flags (observable as
    // error precedence when several option lists name un-referenced columns).
    let convert_select_flags = if opts.convert_selectively {
        let mut flags = vec_from_elem_in(mcx, false, num_phys_attrs);
        let empty = NodeList::nil();
        let sel = CopyGetAttnums(mcx, tup_desc, Some(rel), opts.convert_select.unwrap_or(&empty))?;
        for &attnum in sel.iter() {
            if !attnumlist.contains(&attnum) {
                let att = tup_desc.attr(attnum as usize - 1);
                return Err(Box::new(
                    PgError::error(format!(
                        "selected column \"{}\" not referenced by COPY",
                        String::from_utf8_lossy(att.attname.name_str())
                    ))
                    .with_sqlstate(types_error::ERRCODE_INVALID_COLUMN_REFERENCE),
                ));
            }
            flags[attnum as usize - 1] = true;
        }
        Some(flags)
    } else {
        None
    };

    let file_encoding = if opts.file_encoding < 0 {
        mbutils::pg_get_client_encoding()
    } else {
        opts.file_encoding
    };
    let db_encoding = mbutils::GetDatabaseEncoding();
    let need_transcoding = !(file_encoding == db_encoding
        || file_encoding == wchar::PG_SQL_ASCII
        || db_encoding == wchar::PG_SQL_ASCII);
    let conversion_proc = if need_transcoding {
        let p = namespace_seams::find_default_conversion_proc::call(file_encoding, db_encoding)?;
        if p == 0 {
            return Err(Box::new(
                PgError::error(format!(
                    "default conversion function for encoding \"{}\" to \"{}\" does not exist",
                    mbutils::pg_encoding_to_char(file_encoding),
                    mbutils::pg_encoding_to_char(db_encoding),
                ))
                .with_sqlstate(ERRCODE_UNDEFINED_FUNCTION),
            ));
        }
        p
    } else {
        0
    };

    let mut in_functions: PgVec<'mcx, FmgrInfo> = PgVec::new_in(mcx);
    let mut typioparams: PgVec<'mcx, Oid> = PgVec::new_in(mcx);
    let mut atttypmods: PgVec<'mcx, i32> = PgVec::new_in(mcx);
    let mut attnames: PgVec<'mcx, NameData> = PgVec::new_in(mcx);
    for &attnum in attnumlist.iter() {
        let att = tup_desc.attr(attnum as usize - 1);
        let (func_oid, typioparam) = if opts.binary {
            lsyscache::typ::getTypeBinaryInputInfo(att.atttypid)?
        } else {
            lsyscache::typ::getTypeInputInfo(att.atttypid)?
        };
        in_functions.push(fmgr_core::fmgr_info(func_oid)?);
        typioparams.push(typioparam);
        atttypmods.push(att.atttypmod);
    }
    let mut defexprs: PgVec<'mcx, Option<mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>>> =
        PgVec::new_in(mcx);
    let mut defmap: PgVec<'mcx, usize> = PgVec::new_in(mcx);
    let mut volatile_defexprs = false;
    for i in 0..num_phys_attrs {
        let att = tup_desc.attr(i);
        attnames.push(att.attname);
        defexprs.push(None);
        if att.attisdropped {
            continue;
        }
        let in_list = attnumlist.contains(&(i as i16 + 1));
        if (opts.default_print.is_some() || !in_list) && att.attgenerated == 0 {
            let Some(defexpr) = rewrite_handler::build_column_default(mcx, rel, i + 1)? else {
                continue;
            };
            let defexpr = clauses::eval_const_expressions(mcx, defexpr)?;
            nodes_core::fix_opfuncids(defexpr)?;
            let mut state = execexpr::exec_init_expr(mcx, Some(defexpr), execexpr::ParamBind::NONE)?
                .expect("column default expression");
            // SAFETY: default results land in the statement mcx, which
            // outlives every next_copy_from call (C per-tuple econtext;
            // WATCH: unbounded for very large loads, as the input values).
            unsafe { state.arm_result_mcx_raw(mcx) };
            defexprs[i] = Some(state);
            if !in_list {
                defmap.push(i);
            }
            if !volatile_defexprs {
                volatile_defexprs = clauses::contain_volatile_functions_not_nextval(defexpr)?;
            }
        }
    }
    pgstat_progress_start_command(PROGRESS_COMMAND_COPY, rel.rd_id);
    let mut progress_type = PROGRESS_COPY_TYPE_PIPE;
    let mut progress_bytes_total: i64 = 0;
    let src = if opts.parquet {
        // Server-side file only in this increment (STDIN and callback
        // sources error cleanly inside open_source / here).
        if data_source_cb.is_some() {
            return Err(Box::new(
                PgError::error(
                    "COPY FROM with parquet format only supports reading from a file",
                )
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        let (psrc, file_len) = crate::fromparquet::open_source(
            filename,
            tup_desc,
            &attnumlist,
            opts.parquet_match_by_name,
            opts.parquet_coerce_epoch,
        )?;
        progress_type = PROGRESS_COPY_TYPE_FILE;
        progress_bytes_total = file_len as i64;
        CopySrc::Parquet(psrc)
    } else if let Some(cb) = data_source_cb.take() {
        progress_type = PROGRESS_COPY_TYPE_CALLBACK;
        // SAFETY (dropck erasure only): CopyFromState<'mcx, 's> still carries
        // 's, so the state (and this box) cannot outlive the closure's
        // captures; erasing 's from the trait object relaxes nothing but
        // dropck's conservative dyn-Drop borrow extension.
        let cb: Box<dyn FnMut(&mut [u8], usize) -> PgResult<usize> + 'static> =
            unsafe { core::mem::transmute(cb) };
        CopySrc::Callback { cb }
    } else {
        match filename {
        Some(filename) => {
            let fd = fd::AllocateFile(filename, "rb")?;
            if fd < 0 {
                ereport(ERROR)
                    .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
                    .errcode_for_file_access()
                    .errmsg(format!("could not open file \"{filename}\" for reading: %m"))
                    .errhint(
                        "COPY FROM instructs the PostgreSQL server process to read a file. You \
                         may want a client-side facility such as psql's \\copy.",
                    )
                    .finish(loc("BeginCopyFrom"))?;
            }
            progress_type = PROGRESS_COPY_TYPE_FILE;
            let (is_dir, size) = fd::with_allocated_stdio(fd, |f| {
                f.metadata().map(|m| (m.is_dir(), m.len())).unwrap_or((false, 0))
            })
            .unwrap_or((false, 0));
            if is_dir {
                return Err(Box::new(
                    PgError::error(format!("\"{filename}\" is a directory"))
                        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
            progress_bytes_total = size as i64;
            CopySrc::File { fd, filename }
        }
        None => {
            if elog::config::where_to_send_output() != CommandDest::Remote {
                unported("FROM STDIN outside a remote session (stdin file arm)");
            }
            receive_copy_begin(mcx, attnumlist.len(), opts.binary)?
        }
        }
    };
    pgstat_progress_update_multi_param(
        &[PROGRESS_COPY_COMMAND, PROGRESS_COPY_TYPE, PROGRESS_COPY_BYTES_TOTAL],
        &[PROGRESS_COPY_COMMAND_FROM, progress_type, progress_bytes_total],
    );

    let max_fields = attnumlist.len();
    let opts_on_error = opts.on_error;
    let is_binary = opts.binary;
    let mut cstate = CopyFromState {
        opts,
        src,
        raw_buf: vec_from_elem_in(mcx, 0u8, RAW_BUF_SIZE + 1),
        raw_buf_index: 0,
        raw_buf_len: 0,
        raw_reached_eof: false,
        input_reached_eof: false,
        input_reached_error: false,
        input_buf: need_transcoding.then(|| vec_from_elem_in(mcx, 0u8, INPUT_BUF_SIZE + 1)),
        input_buf_index: 0,
        input_buf_len: 0,
        line_buf: PgVec::new_in(mcx),
        line_buf_valid: false,
        attribute_buf: PgVec::new_in(mcx),
        binary_attr_buf: StringInfo::new_in(mcx)?,
        raw_fields: PgVec::new_in(mcx),
        max_fields,
        eol_type: EolType::Unknown,
        cur_lineno: 0,
        cur_attidx: None,
        cur_attval_off: None,
        file_encoding,
        need_transcoding,
        conversion_proc,
        convertcx: MemoryContext::new("COPY convert"),
        attnumlist,
        in_functions,
        typioparams,
        atttypmods,
        attnames,
        force_notnull_flags,
        force_null_flags,
        convert_select_flags,
        defexprs,
        defmap,
        defaults: vec_from_elem_in(mcx, false, num_phys_attrs),
        where_clause,
        relname: rel.name().to_string(),
        escontext: (opts_on_error == crate::CopyOnErrorChoice::Ignore)
            .then(|| Box::new(types_fmgr::ErrorSaveNode::new(false))),
        num_errors: 0,
        bytes_processed: 0,
        volatile_defexprs,
    };
    if is_binary {
        cstate.receive_copy_binary_header()?;
    }
    Ok(cstate)
}

// ReceiveCopyBegin (copyfromparse.c): CopyInResponse, then flush so the
// frontend knows it can send.
fn receive_copy_begin<'mcx, 's>(
    mcx: Mcx<'mcx>,
    natts: usize,
    binary: bool,
) -> PgResult<CopySrc<'mcx, 's>> {
    let format: u16 = if binary { 1 } else { 0 };
    let mut buf = pqformat::pq_beginmessage(mcx, b'G')?;
    pqformat::pq_sendbyte(&mut buf, format as u8)?;
    pqformat::pq_sendint16(&mut buf, natts as u16)?;
    for _ in 0..natts {
        pqformat::pq_sendint16(&mut buf, format)?;
    }
    pqformat::pq_endmessage(buf)?;
    let msgbuf = StringInfo::new_in(mcx)?;
    pqcomm::pq_flush()?;
    Ok(CopySrc::Frontend { msgbuf })
}

// copyfrom.c MAX_BUFFERED_TUPLES / MAX_BUFFERED_BYTES.
const MAX_BUFFERED_TUPLES: usize = 1000;
const MAX_BUFFERED_BYTES: usize = 65535;

/// `CopyFrom` (copyfrom.c): read rows, insert into the heap + indexes. Every
/// CIM_SINGLE trigger in C (BEFORE/INSTEAD triggers, FDW, volatile defaults,
/// volatile WHERE) is unported-loud upstream, so this is always CIM_MULTI.
pub fn CopyFrom<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyFromState<'mcx, '_>,
    rel: &Relation<'mcx>,
) -> PgResult<u64> {
    let relkind = rel.rd_rel.relkind;
    let trigdesc = if rel.rd_hastriggers {
        relcache::RelationGetTriggerDesc(rel.rd_id)?
    } else {
        None
    };
    let has_instead = trigdesc.as_ref().is_some_and(|td| td.trig_insert_instead_row);
    // A non-table target is allowed only with an INSTEAD OF INSERT row
    // trigger (copyfrom.c:809-841).
    if relkind != RELKIND_RELATION
        && relkind != types_rel::RELKIND_FOREIGN_TABLE
        && relkind != types_rel::RELKIND_PARTITIONED_TABLE
        && !has_instead
    {
        return Err(cannot_copy_to_relkind(rel));
    }
    // New-in-transaction storage: probing the FSM is a waste of time
    // (RELKIND_HAS_STORAGE arm; partitioned roots have none).
    let mut ti_options = if relkind == RELKIND_RELATION
        && (rel.rd_createSubid.get() != types_core::xact::InvalidSubTransactionId
            || rel.rd_firstRelfilelocatorSubid.get()
                != types_core::xact::InvalidSubTransactionId)
    {
        tableam_vocab::TABLE_INSERT_SKIP_FSM
    } else {
        0
    };
    if cstate.opts.freeze {
        if relkind == types_rel::RELKIND_PARTITIONED_TABLE {
            return Err(Box::new(
                PgError::error("cannot perform COPY FREEZE on a partitioned table")
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        if relkind == types_rel::RELKIND_FOREIGN_TABLE {
            return Err(Box::new(
                PgError::error("cannot perform COPY FREEZE on a foreign table")
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
            ));
        }
        snapmgr::InvalidateCatalogSnapshot();
        if !snapmgr::ThereAreNoPriorRegisteredSnapshots() || !portalmem::ThereAreNoReadyPortals()
        {
            return Err(Box::new(
                PgError::error("cannot perform COPY FREEZE because of prior transaction activity")
                    .with_sqlstate(types_error::ERRCODE_INVALID_TRANSACTION_STATE),
            ));
        }
        let cur = xact::GetCurrentSubTransactionId();
        if rel.rd_createSubid.get() != cur && rel.rd_newRelfilelocatorSubid.get() != cur {
            return Err(Box::new(
                PgError::error(
                    "cannot perform COPY FREEZE because the table was not created or truncated \
                     in the current subtransaction",
                )
                .with_sqlstate(types_error::ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            ));
        }
        ti_options |= tableam_vocab::TABLE_INSERT_FROZEN;
    }
    // C dispatches through ri_FdwRoutine (BeginForeignInsert) after
    // CheckValidResultRel; no in-tree FDW models ExecForeignInsert, so the
    // CheckValidResultRel error is the invariant outcome.
    if relkind == types_rel::RELKIND_FOREIGN_TABLE {
        return Err(Box::new(
            PgError::error(format!("cannot insert into foreign table \"{}\"", rel.name()))
                .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
        ));
    }
    // Morsel-parallel COPY (parallel.rs; PGRUST_PARALLEL_COPY=1, pgrcolumnar
    // text loads): admission refusals (triggers included — fail-closed,
    // traced) fall through to the serial body, byte-identically. Errors
    // return UNWRAPPED — the workers attached the exact line contexts
    // already, and the leader-side cstate's read cursor never moved (a
    // second copy_from_error_context would fabricate "line 0"). Engagement
    // implies no triggers, so the early return skips no trigger work;
    // partitioned targets never reach here.
    if relkind == RELKIND_RELATION {
        if let Some(processed) =
            crate::parallel::copy_from_parallel(mcx, cstate, rel, trigdesc.is_some())?
        {
            debug_assert!(trigdesc.is_none(), "parallel COPY engaged on a trigger-bearing rel");
            return Ok(processed);
        }
    }
    // C forces CIM_SINGLE on a partitioned target with BEFORE/INSTEAD row
    // triggers (copyfrom.c:995-1005) or statement-level transition tables
    // (copyfrom.c:1016-1027) — the flush path can't fire either correctly.
    let part_force_single = relkind == types_rel::RELKIND_PARTITIONED_TABLE
        && trigdesc.as_ref().is_some_and(|td| {
            td.trig_insert_before_row || td.trig_insert_instead_row || td.trig_insert_new_table
        });
    // AfterTriggerBeginQuery precedes MakeTransitionCaptureState (copyfrom.c
    // 961/972): the capture registry keys off the query depth it opens. A
    // triggerless partitioned target still needs it — routed-into leaves may
    // carry their own row triggers.
    let open_trigger_query =
        trigdesc.is_some() || relkind == types_rel::RELKIND_PARTITIONED_TABLE;
    if open_trigger_query {
        trigger::AfterTriggerBeginQuery();
    }
    let transition_capture = match &trigdesc {
        Some(td) => trigger::MakeTransitionCaptureState(td, rel.rd_id, CmdType::CMD_INSERT)?,
        None => None,
    };
    let mut trig_fmgr = trigger::TriggerFmgrCache::default();
    let mut trig_when = trigger::TriggerWhenCache::default();
    if let Some(td) = &trigdesc {
        let mut when =
            trigger::TriggerWhenEval { mcx, cache: &mut trig_when, modified_cols: None };
        trigger::ExecBSInsertTriggers(mcx, rel, td, &mut trig_fmgr, &mut when)?;
    }
    // CopyFromErrorCallback scope: C installs error_context_stack here, after
    // the relkind + FREEZE checks; buffered-but-unflushed slots on the Err
    // path are simply dropped, as C's are (the aborted xact kills flushed
    // ones).
    let body = if relkind == types_rel::RELKIND_PARTITIONED_TABLE {
        copy_from_partitioned_body(
            mcx,
            cstate,
            rel,
            ti_options,
            part_force_single,
            transition_capture.as_ref(),
        )
    } else {
        let mut trig = trigdesc.as_ref().map(|td| CopyTrig {
            td,
            tc: transition_capture.as_ref(),
            when: &mut trig_when,
            fmgr: &mut trig_fmgr,
        });
        copy_from_body(mcx, cstate, rel, ti_options, trig.as_mut())
    };
    match body {
        Ok(n) => {
            if let Some(td) = &trigdesc {
                let mut when =
                    trigger::TriggerWhenEval { mcx, cache: &mut trig_when, modified_cols: None };
                trigger::ExecASInsertTriggers(rel, td, transition_capture.as_ref(), Some(&mut when))?;
            }
            if open_trigger_query {
                trigger::AfterTriggerEndQuery()?;
            }
            Ok(n)
        }
        Err(e) => Err(copy_from_error_context(cstate, e)),
    }
}

// The row-trigger slice of C's resultRelInfo that the insert loop and
// CopyMultiInsertBufferFlush touch.
struct CopyTrig<'a, 'mcx> {
    td: &'a types_trigger::TriggerDesc<'static>,
    tc: Option<&'a trigger::TransitionCaptureState>,
    when: &'a mut trigger::TriggerWhenCache<'mcx>,
    fmgr: &'a mut trigger::TriggerFmgrCache,
}

fn copy_from_body<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyFromState<'mcx, '_>,
    rel: &Relation<'mcx>,
    ti_options: i32,
    mut trig: Option<&mut CopyTrig<'_, 'mcx>>,
) -> PgResult<u64> {
    let mycid = xact::GetCurrentCommandId(true)?;

    let mut index_state = execindexing::ExecOpenIndices(mcx, rel, false)?;

    // The DoCopy perminfo's insertedCols (copy.c): constraint-error DETAILs
    // always include the columns the user provided data for (execMain.c
    // ExecBuildSlotValueDescription).
    let inserted_cols = {
        const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
        let mut b = types_nodes::Bitmapset::empty();
        for &a in cstate.attnumlist.iter() {
            b.add_member(mcx, a as i32 - FLIHAN)?;
        }
        b
    };
    let mut qualexpr = init_where_qual(mcx, cstate)?;
    let has_br = trig.as_ref().is_some_and(|t| t.td.trig_insert_before_row);
    let has_ir = trig.as_ref().is_some_and(|t| t.td.trig_insert_instead_row);
    // CIM_SINGLE arms of copyfrom.c:996-1052: BEFORE/INSTEAD row triggers and
    // volatile expressions may query the table mid-load, so every row must be
    // visible as soon as stored.
    let single_insert =
        cstate.volatile_defexprs || where_clause_volatile(cstate)? || has_br || has_ir;
    let mut single_eval_cx = MemoryContext::new_bump("CopySingleInsertEval");
    let mut check_exprs = None;

    let has_generated_stored =
        rel.rd_att.constr.as_deref().is_some_and(|c| c.has_generated_stored);
    let mut generated_exprs = None;
    let mut virtual_nn_exprs = None;

    // std Vec: SlotData owns droppy state via the arena-erased views; the
    // slot pool itself is per-statement (CopyMultiInsertBuffer.slots).
    let mut slots: Vec<types_slot::SlotData<'mcx>> = Vec::new();
    let mut linenos: Vec<u64> = Vec::new();
    let mut bistate = heapam::GetBulkInsertState();
    let mut nused = 0usize;
    let mut buffered_bytes = 0usize;

    let mut processed: u64 = 0;
    let mut excluded: i64 = 0;
    let mut flushed: i64 = 0;
    loop {
        postgres_seams::check_for_interrupts::call()?;

        if nused == slots.len() {
            slots.push(tableam::table_slot_create(mcx, rel)?);
            linenos.push(0);
        }
        let slot = &mut slots[nused];
        exectuples::exec_clear_tuple(slot, mcx);

        // Input-function results and the materialized tuple land in the
        // statement mcx and are reclaimed at statement end (nodemodifytable
        // ExecInsert precedent); WATCH: unbounded for very large loads.
        {
            let base = slot.base_mut();
            if !cstate.next_copy_from(mcx, &mut base.tts_values, &mut base.tts_isnull)? {
                break;
            }
        }
        if cstate.escontext.as_ref().is_some_and(|n| n.ctx.error_occurred()) {
            cstate.escontext.as_mut().unwrap().ctx.reset_error_occurred();
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_SKIPPED, cstate.num_errors as i64);
            if cstate.opts.reject_limit > 0 && cstate.num_errors > cstate.opts.reject_limit as u64
            {
                return Err(reject_limit_exceeded(cstate.opts.reject_limit));
            }
            continue;
        }

        exectuples::exec_store_virtual_tuple(slot);
        slot.base_mut().tts_tableOid = rel.rd_id;

        if qualexpr.is_some() {
            let mut eval = execexpr::EvalSlots { scan: Some(slot), inner: None, outer: None };
            if !execexpr::exec_qual(qualexpr.as_deref_mut(), &mut eval)? {
                excluded += 1;
                pgstat_progress_update_param(PROGRESS_COPY_TUPLES_EXCLUDED, excluded);
                continue;
            }
        }

        // BEFORE ROW INSERT triggers (copyfrom.c:1327-1331); a NULL return
        // suppresses the row and it is not counted.
        if has_br {
            let t = trig.as_deref_mut().expect("has_br implies trig");
            let mut when =
                trigger::TriggerWhenEval { mcx, cache: &mut *t.when, modified_cols: None };
            if !trigger::ExecBRInsertTriggers(mcx, rel, t.td, t.fmgr, &mut when, slot)? {
                continue;
            }
        }

        // An INSTEAD OF INSERT row trigger owns the row; nothing is stored
        // (copyfrom.c:1340-1343).
        if has_ir {
            let t = trig.as_deref_mut().expect("has_ir implies trig");
            let mut when =
                trigger::TriggerWhenEval { mcx, cache: &mut *t.when, modified_cols: None };
            trigger::ExecIRInsertTriggers(mcx, rel, t.td, t.fmgr, &mut when, slot)?;
            processed += 1;
            flushed += 1;
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, flushed);
            continue;
        }

        if has_generated_stored {
            nodemodifytable::exec_compute_stored_generated(mcx, &mut generated_exprs, rel, slot)?;
        }

        // ExecConstraints (copyfrom.c:1352-1358): NOT NULL + CHECK.
        nodemodifytable::exec_constraints(
            mcx,
            &mut check_exprs,
            &mut virtual_nn_exprs,
            rel,
            slot,
            None,
            Some(&inserted_cols),
        )?;

        if single_insert {
            single_eval_cx.reset();
            tableam::table_tuple_insert(mcx, rel, slot, mycid, ti_options, Some(&mut bistate))?;
            let recheck_indexes = if index_state.num_indices() > 0 {
                execindexing::ExecInsertIndexTuples(
                    mcx,
                    single_eval_cx.mcx(),
                    &mut index_state,
                    rel,
                    slot,
                    false,
                    None,
                    &[],
                    false,
                )?
            } else {
                PgVec::new_in(mcx)
            };
            if let Some(t) = trig.as_deref_mut() {
                let mut when =
                    trigger::TriggerWhenEval { mcx, cache: &mut *t.when, modified_cols: None };
                trigger::ExecARInsertTriggers(
                    mcx,
                    rel,
                    Some(t.td),
                    slot.base().tts_tid,
                    &recheck_indexes,
                    t.tc,
                    Some(&mut when),
                    // COPY's target rel is not a child result rel here, so
                    // there is no child->root capture map (C ri_ChildToRootMap
                    // is NULL for a non-child result relation).
                    None,
                )?;
            }
            processed += 1;
            flushed += 1;
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, flushed);
            continue;
        }

        exectuples::exec_materialize_slot(slot, mcx)?;
        linenos[nused] = cstate.cur_lineno;
        nused += 1;
        buffered_bytes += cstate.line_buf.len();
        processed += 1;

        if nused >= MAX_BUFFERED_TUPLES || buffered_bytes >= MAX_BUFFERED_BYTES {
            flush_multi_insert(
                mcx,
                cstate,
                rel,
                &mut slots[..nused],
                &linenos[..nused],
                mycid,
                ti_options,
                &mut bistate,
                &mut index_state,
                trig.as_deref_mut(),
            )?;
            flushed += nused as i64;
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, flushed);
            nused = 0;
            buffered_bytes = 0;
        }
    }

    if nused > 0 {
        flush_multi_insert(
            mcx,
            cstate,
            rel,
            &mut slots[..nused],
            &linenos[..nused],
            mycid,
            ti_options,
            &mut bistate,
            &mut index_state,
            trig.as_deref_mut(),
        )?;
        flushed += nused as i64;
        pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, flushed);
    }

    // A view target (INSTEAD OF INSERT trigger) has no table AM; C never
    // reaches table AM calls on that path (copyfrom.c:1340-1343).
    if rel.rd_rel.relkind == RELKIND_RELATION {
        tableam::table_finish_bulk_insert(rel, ti_options)?;
    }

    skipped_rows_notice(cstate)?;
    Ok(processed)
}

// The whereClause volatility arm of C's insert-method selection
// (copyfrom.c:1041).
fn where_clause_volatile(cstate: &CopyFromState<'_, '_>) -> PgResult<bool> {
    for wc in cstate.where_clause.iter() {
        if clauses::contain_volatile_functions(wc)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn init_where_qual<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &CopyFromState<'mcx, '_>,
) -> PgResult<Option<mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>>> {
    let mut qualexpr =
        execexpr::exec_init_qual(mcx, &cstate.where_clause, execexpr::ParamBind::NONE)?;
    if let Some(q) = qualexpr.as_mut() {
        // SAFETY: qual scratch results land in the statement mcx, which
        // outlives every per-row evaluation.
        unsafe { q.arm_result_mcx_raw(mcx) };
    }
    Ok(qualexpr)
}

fn skipped_rows_notice(cstate: &CopyFromState<'_, '_>) -> PgResult<()> {
    if cstate.num_errors > 0
        && cstate.opts.log_verbosity >= crate::CopyLogVerbosityChoice::Default
    {
        let n = cstate.num_errors;
        let msg = if n == 1 {
            format!("{n} row was skipped due to data type incompatibility")
        } else {
            format!("{n} rows were skipped due to data type incompatibility")
        };
        ereport(types_error::NOTICE).errmsg(msg).finish(loc("CopyFrom"))?;
    }
    Ok(())
}

// CopyMultiInsertBuffer, plain-table arm (foreign partitions are loud).
struct PartBuffer<'mcx> {
    leaf: usize,
    // std Vec: SlotData owns droppy arena-erased views; the pool lives for
    // the statement (CopyMultiInsertBuffer.slots).
    slots: Vec<types_slot::SlotData<'mcx>>,
    linenos: Vec<u64>,
    nused: usize,
    bistate: tableam_vocab::BulkInsertStateData,
}

// copyfrom.c MAX_PARTITION_BUFFERS: trim to this many after each flush.
const MAX_PARTITION_BUFFERS: usize = 32;

// The trigger slice of a routed leaf's resultRelInfo (ExecFindPartition's
// ExecInitPartitionInfo leg): trigdesc, per-rel caches, the leaf->root
// attmap for transition capture (C ri_ChildToRootMap), and C's
// leafpart_use_multi_insert (copyfrom.c:1232-1236).
struct LeafTrig<'mcx> {
    td: Option<std::rc::Rc<types_trigger::TriggerDesc<'static>>>,
    fmgr: trigger::TriggerFmgrCache,
    when: trigger::TriggerWhenCache<'mcx>,
    c2r: Option<mcx::PgVec<'mcx, i16>>,
    pcheck: Option<mcx::PgBox<'mcx, execexpr::ExprState<'mcx>>>,
    use_multi: bool,
}

// CopyFrom, partitioned arm: CIM_MULTI_CONDITIONAL (each routed leaf batches
// through its own CopyMultiInsertBuffer) with the CIM_SINGLE fallbacks of
// copyfrom.c:995-1052 — force_single covers the target's BR/IR triggers and
// transition tables, volatile defaults/WHERE are computed here, and a leaf
// with BR/IR triggers takes the single-insert arm per row.
fn copy_from_partitioned_body<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyFromState<'mcx, '_>,
    rel: &Relation<'mcx>,
    ti_options: i32,
    force_single: bool,
    transition_capture: Option<&trigger::TransitionCaptureState>,
) -> PgResult<u64> {
    let mycid = xact::GetCurrentCommandId(true)?;

    // The DoCopy perminfo's insertedCols (copy.c): constraint-error DETAILs
    // always include the columns the user provided data for (execMain.c
    // ExecBuildSlotValueDescription), root-numbered.
    let inserted_cols = {
        const FLIHAN: i32 = types_tuple::htup::FirstLowInvalidHeapAttributeNumber;
        let mut b = types_nodes::Bitmapset::empty();
        for &a in cstate.attnumlist.iter() {
            b.add_member(mcx, a as i32 - FLIHAN)?;
        }
        b
    };
    let mut qualexpr = init_where_qual(mcx, cstate)?;
    let mut router = execpartition::PartitionTupleRouting::new(mcx, rel)?;
    // C GetPerTupleExprContext: expression partition keys evaluate here,
    // reset per row.
    let mut route_eval_cx = MemoryContext::new_bump("CopyRouteEvalPerTuple");
    let mut rootslot = tableam::table_slot_create(mcx, rel)?;
    let mut buffers: Vec<PartBuffer<'mcx>> = Vec::new();
    let mut leaf_indexes: Vec<Option<execindexing::ResultRelIndexState<'mcx>>> = Vec::new();
    let mut leaf_checks: Vec<Option<mcx::PgVec<'mcx, nodemodifytable::CheckExpr<'mcx>>>> =
        Vec::new();
    let mut buffered_tuples = 0usize;
    let mut buffered_bytes = 0usize;
    let mut processed: u64 = 0;
    let mut excluded: i64 = 0;
    let mut flushed: i64 = 0;
    // CIM_SINGLE arms of copyfrom.c:1028-1052; the non-batch bistate exists
    // for CIM_MULTI_CONDITIONAL too (copyfrom.c:1083), pin released on
    // partition switch (copyfrom.c:1258-1260).
    let single_insert =
        force_single || cstate.volatile_defexprs || where_clause_volatile(cstate)?;
    let mut single_bistate = heapam::GetBulkInsertState();
    let mut prev_leaf: Option<usize> = None;
    let mut leaf_slots: Vec<Option<types_slot::SlotData<'mcx>>> = Vec::new();
    let mut leaf_trig: Vec<Option<LeafTrig<'mcx>>> = Vec::new();

    loop {
        postgres_seams::check_for_interrupts::call()?;

        exectuples::exec_clear_tuple(&mut rootslot, mcx);
        {
            let base = rootslot.base_mut();
            if !cstate.next_copy_from(mcx, &mut base.tts_values, &mut base.tts_isnull)? {
                break;
            }
        }
        if cstate.escontext.as_ref().is_some_and(|n| n.ctx.error_occurred()) {
            cstate.escontext.as_mut().unwrap().ctx.reset_error_occurred();
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_SKIPPED, cstate.num_errors as i64);
            if cstate.opts.reject_limit > 0 && cstate.num_errors > cstate.opts.reject_limit as u64
            {
                return Err(reject_limit_exceeded(cstate.opts.reject_limit));
            }
            continue;
        }

        exectuples::exec_store_virtual_tuple(&mut rootslot);
        rootslot.base_mut().tts_tableOid = rel.rd_id;

        if qualexpr.is_some() {
            let mut eval =
                execexpr::EvalSlots { scan: Some(&mut rootslot), inner: None, outer: None };
            if !execexpr::exec_qual(qualexpr.as_deref_mut(), &mut eval)? {
                excluded += 1;
                pgstat_progress_update_param(PROGRESS_COPY_TUPLES_EXCLUDED, excluded);
                continue;
            }
        }

        route_eval_cx.reset();
        let leaf = router.find_partition(&mut rootslot, route_eval_cx.mcx())?;
        if leaf_checks.len() <= leaf {
            leaf_checks.resize_with(leaf + 1, || None);
            leaf_indexes.resize_with(leaf + 1, || None);
        }
        if leaf_trig.len() <= leaf {
            leaf_trig.resize_with(leaf + 1, || None);
        }
        if leaf_trig[leaf].is_none() {
            let lrel = router.leaf_rel(leaf);
            if lrel.rd_rel.relkind != RELKIND_RELATION {
                // CheckValidResultRel (execMain.c): no in-tree FDW models
                // ExecForeignInsert, so a routed foreign leaf always errors.
                return Err(Box::new(
                    PgError::error(format!(
                        "cannot insert into foreign table \"{}\"",
                        lrel.name()
                    ))
                    .with_sqlstate(types_error::ERRCODE_FEATURE_NOT_SUPPORTED),
                ));
            }
            if lrel
                .rd_att
                .constr
                .as_deref()
                .is_some_and(|c| c.has_generated_stored || c.has_generated_virtual)
            {
                panic!("CopyFrom: generated columns on a routed-into partition not ported");
            }
            let td = if lrel.rd_hastriggers {
                relcache::RelationGetTriggerDesc(lrel.rd_id)?
            } else {
                None
            };
            // C leafpart_use_multi_insert (copyfrom.c:1232-1236): a leaf with
            // BEFORE/INSTEAD row triggers takes the single-insert arm.
            let use_multi = !td.as_ref().is_some_and(|t| {
                t.trig_insert_before_row || t.trig_insert_instead_row
            });
            let c2r =
                tupdesc::build_attrmap_by_name_if_req(mcx, &lrel.rd_att, &rel.rd_att, false)?;
            leaf_trig[leaf] = Some(LeafTrig {
                td,
                fmgr: trigger::TriggerFmgrCache::default(),
                when: trigger::TriggerWhenCache::default(),
                c2r,
                pcheck: None,
                use_multi,
            });
        }
        let leaf_use_multi = !single_insert
            && leaf_trig[leaf].as_ref().expect("leaf initialized").use_multi;

        if prev_leaf != Some(leaf) {
            // copyfrom.c:1245-1260: flush pending inserts before a
            // non-batchable partition's row so its triggers see them, and
            // release the bistate pin on partition switch.
            if !leaf_use_multi && buffered_tuples > 0 {
                flushed += flush_part_buffers(
                    mcx,
                    cstate,
                    &router,
                    &mut buffers,
                    &mut leaf_indexes,
                    &mut leaf_trig,
                    transition_capture,
                    mycid,
                    ti_options,
                )?;
                pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, flushed);
                buffered_tuples = 0;
                buffered_bytes = 0;
            }
            heapam::ReleaseBulkInsertStatePin(&mut single_bistate);
            prev_leaf = Some(leaf);
        }

        if !leaf_use_multi {
            let lrel = router.leaf_rel(leaf);
            if leaf_slots.len() <= leaf {
                leaf_slots.resize_with(leaf + 1, || None);
            }
            let use_slot: &mut types_slot::SlotData<'mcx> = match router.leaf_attrmap(leaf) {
                Some(map) => {
                    if leaf_slots[leaf].is_none() {
                        leaf_slots[leaf] = Some(tableam::table_slot_create(mcx, lrel)?);
                    }
                    let ls = leaf_slots[leaf].as_mut().expect("just created");
                    exectuples::exec_clear_tuple(ls, mcx);
                    exectuples::execute_attr_map_slot(map, &mut rootslot, ls, mcx);
                    ls
                }
                None => &mut rootslot,
            };
            use_slot.base_mut().tts_tableOid = lrel.rd_id;
            let LeafTrig { td, fmgr, when: when_cache, c2r, pcheck, .. } =
                leaf_trig[leaf].as_mut().expect("leaf initialized");
            let has_br = td.as_ref().is_some_and(|t| t.trig_insert_before_row);
            // BEFORE ROW INSERT triggers on the leaf (copyfrom.c:1327-1331);
            // a NULL return suppresses the row and it is not counted.
            if has_br {
                let mut when =
                    trigger::TriggerWhenEval { mcx, cache: &mut *when_cache, modified_cols: None };
                if !trigger::ExecBRInsertTriggers(
                    mcx,
                    lrel,
                    td.as_deref().expect("has_br implies trigdesc"),
                    fmgr,
                    &mut when,
                    use_slot,
                )? {
                    continue;
                }
            }
            nodemodifytable::exec_constraints(
                mcx,
                &mut leaf_checks[leaf],
                &mut None,
                lrel,
                use_slot,
                Some(rel),
                Some(&inserted_cols),
            )?;
            // ExecPartitionCheck (copyfrom.c:1361-1368): a routed row is
            // re-checked only when a BR trigger could have moved it.
            if has_br && !execpartition::exec_partition_check(mcx, pcheck, lrel, use_slot)? {
                return Err(execpartition::partition_constraint_violation(
                    mcx,
                    lrel,
                    use_slot,
                    None,
                    Some(rel),
                ));
            }
            tableam::table_tuple_insert(
                mcx,
                lrel,
                use_slot,
                mycid,
                ti_options,
                Some(&mut single_bistate),
            )?;
            if leaf_indexes[leaf].is_none() {
                leaf_indexes[leaf] = Some(execindexing::ExecOpenIndices(mcx, lrel, false)?);
            }
            let idx = leaf_indexes[leaf].as_mut().expect("just opened");
            let recheck_indexes = if idx.num_indices() > 0 {
                execindexing::ExecInsertIndexTuples(
                    mcx,
                    route_eval_cx.mcx(),
                    idx,
                    lrel,
                    use_slot,
                    false,
                    None,
                    &[],
                    false,
                )?
            } else {
                PgVec::new_in(mcx)
            };
            // AFTER ROW INSERT triggers (copyfrom.c:1441); capture converts
            // leaf-format tuples through the leaf->root map.
            if td.is_some() || transition_capture.is_some() {
                let conv = c2r.as_ref().map(|map| trigger::ChildToRoot {
                    map,
                    child_desc: lrel.rd_att.as_ref(),
                    root_desc: rel.rd_att.as_ref(),
                });
                let mut when =
                    trigger::TriggerWhenEval { mcx, cache: &mut *when_cache, modified_cols: None };
                trigger::ExecARInsertTriggers(
                    mcx,
                    lrel,
                    td.as_deref(),
                    use_slot.base().tts_tid,
                    &recheck_indexes,
                    transition_capture,
                    Some(&mut when),
                    conv.as_ref(),
                )?;
            }
            processed += 1;
            flushed += 1;
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, flushed);
            continue;
        }

        let bidx = match buffers.iter().position(|b| b.leaf == leaf) {
            Some(i) => i,
            None => {
                buffers.push(PartBuffer {
                    leaf,
                    slots: Vec::new(),
                    linenos: Vec::new(),
                    nused: 0,
                    bistate: heapam::GetBulkInsertState(),
                });
                buffers.len() - 1
            }
        };
        {
            let lrel = router.leaf_rel(leaf);
            let buf = &mut buffers[bidx];
            if buf.nused == buf.slots.len() {
                buf.slots.push(tableam::table_slot_create(mcx, lrel)?);
                buf.linenos.push(0);
            }
            let slot = &mut buf.slots[buf.nused];
            // ExecPrepareTupleRouting: attno-remapped leaves take the tuple
            // converted (then materialized off rootslot's memory).
            match router.leaf_attrmap(leaf) {
                Some(map) => {
                    exectuples::execute_attr_map_slot(map, &mut rootslot, slot, mcx);
                    exectuples::exec_materialize_slot(slot, mcx)?;
                }
                None => exectuples::exec_copy_slot(slot, &mut rootslot, mcx, mcx)?,
            }
            slot.base_mut().tts_tableOid = lrel.rd_id;
            // Virtual-generated columns on a routed-into partition panic above,
            // so the virtual-NN compile cache is never populated.
            nodemodifytable::exec_constraints(
                mcx,
                &mut leaf_checks[leaf],
                &mut None,
                lrel,
                slot,
                Some(rel),
                Some(&inserted_cols),
            )?;
            // Routed rows skip ExecPartitionCheck (bound proven on descent;
            // the DEFAULT-partition re-check runs inside find_partition).
            buf.linenos[buf.nused] = cstate.cur_lineno;
            buf.nused += 1;
        }
        buffered_tuples += 1;
        buffered_bytes += cstate.line_buf.len();
        processed += 1;

        if buffered_tuples >= MAX_BUFFERED_TUPLES || buffered_bytes >= MAX_BUFFERED_BYTES {
            flushed += flush_part_buffers(
                mcx,
                cstate,
                &router,
                &mut buffers,
                &mut leaf_indexes,
                &mut leaf_trig,
                transition_capture,
                mycid,
                ti_options,
            )?;
            pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, flushed);
            buffered_tuples = 0;
            buffered_bytes = 0;
            while buffers.len() > MAX_PARTITION_BUFFERS {
                if buffers[0].leaf == leaf {
                    let cur = buffers.remove(0);
                    buffers.push(cur);
                }
                let evicted = buffers.remove(0);
                tableam::table_finish_bulk_insert(router.leaf_rel(evicted.leaf), ti_options)?;
            }
        }
    }

    flushed += flush_part_buffers(
        mcx,
        cstate,
        &router,
        &mut buffers,
        &mut leaf_indexes,
        &mut leaf_trig,
        transition_capture,
        mycid,
        ti_options,
    )?;
    pgstat_progress_update_param(PROGRESS_COPY_TUPLES_PROCESSED, flushed);
    for buf in buffers.iter() {
        tableam::table_finish_bulk_insert(router.leaf_rel(buf.leaf), ti_options)?;
    }

    skipped_rows_notice(cstate)?;
    Ok(processed)
}

// CopyMultiInsertInfoFlush over every tracked buffer, in creation order.
#[allow(clippy::too_many_arguments)]
fn flush_part_buffers<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyFromState<'mcx, '_>,
    router: &execpartition::PartitionTupleRouting<'mcx>,
    buffers: &mut [PartBuffer<'mcx>],
    leaf_indexes: &mut [Option<execindexing::ResultRelIndexState<'mcx>>],
    leaf_trig: &mut [Option<LeafTrig<'mcx>>],
    transition_capture: Option<&trigger::TransitionCaptureState>,
    mycid: types_core::CommandId,
    ti_options: i32,
) -> PgResult<i64> {
    let mut flushed: i64 = 0;
    for buf in buffers.iter_mut() {
        if buf.nused == 0 {
            continue;
        }
        let lrel = router.leaf_rel(buf.leaf);
        if leaf_indexes[buf.leaf].is_none() {
            leaf_indexes[buf.leaf] = Some(execindexing::ExecOpenIndices(mcx, lrel, false)?);
        }
        let index_state = leaf_indexes[buf.leaf].as_mut().expect("just opened");
        // AFTER ROW triggers on a batchable leaf fire at flush time
        // (CopyMultiInsertBufferFlush, copyfrom.c:562-598); transition
        // capture never reaches here — it forces the single-insert arm.
        let LeafTrig { td, fmgr, when, .. } =
            leaf_trig[buf.leaf].as_mut().expect("leaf initialized");
        let mut trig =
            td.as_deref().map(|td| CopyTrig { td, tc: transition_capture, when, fmgr });
        flush_multi_insert(
            mcx,
            cstate,
            lrel,
            &mut buf.slots[..buf.nused],
            &buf.linenos[..buf.nused],
            mycid,
            ti_options,
            &mut buf.bistate,
            index_state,
            trig.as_mut(),
        )?;
        flushed += buf.nused as i64;
        buf.nused = 0;
    }
    Ok(flushed)
}

// CopyMultiInsertBufferFlush (copyfrom.c), single non-partitioned table.
// Errors here report the buffered tuple's line number, not the read cursor's
// (C saves/restores cur_lineno and clears line_buf_valid around the flush).
#[allow(clippy::too_many_arguments)]
fn flush_multi_insert<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyFromState<'mcx, '_>,
    rel: &Relation<'mcx>,
    slots: &mut [types_slot::SlotData<'mcx>],
    linenos: &[u64],
    mycid: types_core::CommandId,
    ti_options: i32,
    bistate: &mut tableam_vocab::BulkInsertStateData,
    index_state: &mut execindexing::ResultRelIndexState<'mcx>,
    mut trig: Option<&mut CopyTrig<'_, 'mcx>>,
) -> PgResult<()> {
    let save_cur_lineno = cstate.cur_lineno;
    let save_line_buf_valid = cstate.line_buf_valid;
    cstate.line_buf_valid = false;

    let mut refs: Vec<&mut types_slot::SlotData<'mcx>> = slots.iter_mut().collect();
    tableam::table_multi_insert(mcx, rel, &mut refs, mycid, ti_options, Some(bistate))?;

    if index_state.num_indices() > 0 || trig.is_some() {
        // C resets the per-tuple econtext per buffered row (CopyMultiInsertBufferFlush).
        let mut eval_cx = MemoryContext::new_bump("CopyIndexEvalPerTuple");
        for (i, slot) in refs.into_iter().enumerate() {
            eval_cx.reset();
            cstate.cur_lineno = linenos[i];
            let recheck_indexes = if index_state.num_indices() > 0 {
                execindexing::ExecInsertIndexTuples(
                    mcx,
                    eval_cx.mcx(),
                    index_state,
                    rel,
                    slot,
                    false,
                    None,
                    &[],
                    false,
                )?
            } else {
                PgVec::new_in(mcx)
            };
            if let Some(t) = trig.as_deref_mut() {
                let mut when =
                    trigger::TriggerWhenEval { mcx, cache: &mut *t.when, modified_cols: None };
                trigger::ExecARInsertTriggers(
                    mcx,
                    rel,
                    Some(t.td),
                    slot.base().tts_tid,
                    &recheck_indexes,
                    t.tc,
                    Some(&mut when),
                    // COPY's target rel is not a child result rel here, so
                    // there is no child->root capture map (C ri_ChildToRootMap
                    // is NULL for a non-child result relation).
                    None,
                )?;
            }
        }
    }

    cstate.line_buf_valid = save_line_buf_valid;
    cstate.cur_lineno = save_cur_lineno;
    Ok(())
}

// CopyFromErrorCallback + CopyLimitPrintoutLength (copyfrom.c), text arm,
// attached on Err propagation instead of via error_context_stack.
#[cold]
#[inline(never)]
pub fn copy_from_error_context(
    cstate: &CopyFromState<'_, '_>,
    e: Box<PgError>,
) -> Box<PgError> {
    let relname = &cstate.relname;
    let lineno = cstate.cur_lineno;
    if cstate.opts.parquet {
        // Parquet rows are not lines and carry no displayable raw text.
        let ctx = match cstate.cur_attidx {
            Some(m) => {
                let attname = cstate.attname(m);
                format!("COPY {relname}, row {lineno}, column {attname}")
            }
            None => format!("COPY {relname}, row {lineno}"),
        };
        return Box::new(e.add_context(ctx));
    }
    if cstate.opts.binary {
        // C's binary arm: the raw data is not usefully displayable.
        let ctx = match cstate.cur_attidx {
            Some(m) => {
                let attname = cstate.attname(m);
                format!("COPY {relname}, line {lineno}, column {attname}")
            }
            None => format!("COPY {relname}, line {lineno}"),
        };
        return Box::new(e.add_context(ctx));
    }
    let ctx = match cstate.cur_attidx {
        Some(m) => {
            let attname = cstate.attname(m);
            match cstate.cur_attval_off {
                Some(off) => {
                    let bytes = &cstate.attribute_buf[off as usize..];
                    let nul = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
                    let attval = limit_printout_length(&bytes[..nul]);
                    format!("COPY {relname}, line {lineno}, column {attname}: \"{attval}\"")
                }
                None => {
                    format!("COPY {relname}, line {lineno}, column {attname}: null input")
                }
            }
        }
        None => {
            if cstate.line_buf_valid {
                let lineval = limit_printout_length(&cstate.line_buf);
                format!("COPY {relname}, line {lineno}: \"{lineval}\"")
            } else {
                format!("COPY {relname}, line {lineno}")
            }
        }
    };
    Box::new(e.add_context(ctx))
}

const MAX_COPY_DATA_DISPLAY: i32 = 100;

pub(crate) fn limit_printout_length(bytes: &[u8]) -> String {
    let slen = bytes.len() as i32;
    if slen <= MAX_COPY_DATA_DISPLAY {
        return String::from_utf8_lossy(bytes).into_owned();
    }
    let len = mbutils::pg_mbcliplen(bytes, slen, MAX_COPY_DATA_DISPLAY) as usize;
    let mut s = String::from_utf8_lossy(&bytes[..len]).into_owned();
    s.push_str("...");
    s
}

/// `EndCopyFrom` (copyfrom.c).
pub fn EndCopyFrom(cstate: CopyFromState<'_, '_>) -> PgResult<()> {
    if let CopySrc::File { fd, filename } = &cstate.src {
        if fd::FreeFile(*fd)? != 0 {
            ereport(ERROR)
                .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not close file \"{filename}\": %m"))
                .finish(loc("EndCopyFrom"))?;
        }
    }
    pgstat_progress_end_command();
    Ok(())
}

#[track_caller]
#[cold]
#[inline(never)]
fn reject_limit_exceeded(limit: i64) -> Box<PgError> {
    Box::new(
        PgError::error(format!(
            "skipped more than REJECT_LIMIT ({limit}) rows due to data type incompatibility"
        ))
        .with_sqlstate(types_error::ERRCODE_INVALID_TEXT_REPRESENTATION),
    )
}

#[track_caller]
#[cold]
#[inline(never)]
fn cannot_copy_to_relkind(rel: &Relation<'_>) -> Box<PgError> {
    let name = rel.name();
    let (msg, hint): (String, Option<&str>) = match rel.rd_rel.relkind {
        b'v' => (
            format!("cannot copy to view \"{name}\""),
            Some("To enable copying to a view, provide an INSTEAD OF INSERT trigger."),
        ),
        b'm' => (format!("cannot copy to materialized view \"{name}\""), None),
        b'S' => (format!("cannot copy to sequence \"{name}\""), None),
        _ => (format!("cannot copy to non-table relation \"{name}\""), None),
    };
    let mut e = PgError::error(msg).with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE);
    if let Some(h) = hint {
        e = e.with_hint(h);
    }
    Box::new(e)
}
