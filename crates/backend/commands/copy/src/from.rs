// copyfrom.c, text/CSV from file or frontend, CIM_MULTI lane
// (heap_multi_insert buffering with a shared BulkInsertState, matching C's
// default insert method and its bulk relation-extension page geometry).

use elog::ereport;
use mcx::{vec_from_elem_in, Mcx, MemoryContext, PgVec};
use stringinfo::StringInfo;
use types_core::Oid;
use types_dest::CommandDest;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_NOT_NULL_VIOLATION, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_fmgr::FmgrInfo;
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
}

impl CopyFromState<'_, '_> {
    pub(crate) fn attname(&self, m: usize) -> String {
        String::from_utf8_lossy(self.attnames[m].name_str()).into_owned()
    }
}

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("copyfrom.c", 0, funcname)
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
    let opts = ProcessCopyOptions(true, options, source_text)?;
    if opts.binary {
        unported("FORMAT binary (text-only lane)");
    }
    if opts.freeze {
        unported("FREEZE (multi-insert/frozen lane)");
    }
    let tup_desc = &rel.rd_att;
    let attnumlist = CopyGetAttnums(mcx, tup_desc, rel, attnamelist)?;
    let num_phys_attrs = tup_desc.natts as usize;

    let force_notnull_flags = force_flags(
        mcx,
        tup_desc,
        rel,
        &attnumlist,
        opts.force_notnull,
        opts.force_notnull_all,
        "FORCE_NOT_NULL",
    )?;
    let force_null_flags = force_flags(
        mcx,
        tup_desc,
        rel,
        &attnumlist,
        opts.force_null,
        opts.force_null_all,
        "FORCE_NULL",
    )?;

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
        let (func_oid, typioparam) = lsyscache::typ::getTypeInputInfo(att.atttypid)?;
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
        if (opts.default_print.is_some() || !in_list)
            && att.attgenerated == 0
            && (att.atthasdef || att.attidentity != 0)
        {
            let defexpr = rewrite_handler::build_column_default(mcx, rel, i + 1)?;
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
    if volatile_defexprs {
        unported("FROM with volatile default expressions (CIM_SINGLE lane)");
    }
    if tup_desc
        .constr
        .as_deref()
        .is_some_and(|c| c.has_generated_stored || c.has_generated_virtual)
    {
        unported("FROM with generated columns (ExecComputeStoredGenerated lane)");
    }

    let src = match filename {
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
            let is_dir = fd::with_allocated_stdio(fd, |f| {
                f.metadata().map(|m| m.is_dir()).unwrap_or(false)
            })
            .unwrap_or(false);
            if is_dir {
                return Err(Box::new(
                    PgError::error(format!("\"{filename}\" is a directory"))
                        .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
                ));
            }
            CopySrc::File { fd, filename }
        }
        None => {
            if elog::config::where_to_send_output() != CommandDest::Remote {
                unported("FROM STDIN outside a remote session (stdin file arm)");
            }
            receive_copy_begin(mcx, attnumlist.len())?
        }
    };

    let max_fields = attnumlist.len();
    let opts_on_error = opts.on_error;
    Ok(CopyFromState {
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
        defexprs,
        defmap,
        defaults: vec_from_elem_in(mcx, false, num_phys_attrs),
        where_clause,
        relname: rel.name().to_string(),
        escontext: (opts_on_error == crate::CopyOnErrorChoice::Ignore)
            .then(|| Box::new(types_fmgr::ErrorSaveNode::new(false))),
        num_errors: 0,
        bytes_processed: 0,
    })
}

// ReceiveCopyBegin (copyfromparse.c): CopyInResponse, then flush so the
// frontend knows it can send.
fn receive_copy_begin<'mcx, 's>(mcx: Mcx<'mcx>, natts: usize) -> PgResult<CopySrc<'mcx, 's>> {
    let mut buf = pqformat::pq_beginmessage(mcx, b'G')?;
    pqformat::pq_sendbyte(&mut buf, 0)?;
    pqformat::pq_sendint16(&mut buf, natts as u16)?;
    for _ in 0..natts {
        pqformat::pq_sendint16(&mut buf, 0)?;
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
    if rel.rd_rel.relkind != RELKIND_RELATION {
        return Err(cannot_copy_to_relkind(rel));
    }
    // CopyFromErrorCallback scope: C installs error_context_stack here, after
    // the relkind checks; buffered-but-unflushed slots on the Err path are
    // simply dropped, as C's are (the aborted xact kills flushed ones).
    match copy_from_body(mcx, cstate, rel) {
        Ok(n) => Ok(n),
        Err(e) => Err(copy_from_error_context(cstate, rel, e)),
    }
}

fn copy_from_body<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyFromState<'mcx, '_>,
    rel: &Relation<'mcx>,
) -> PgResult<u64> {
    let mycid = xact::GetCurrentCommandId(true)?;
    // New-in-transaction storage: probing the FSM is a waste of time
    // (relkind has storage: CopyFrom rejects everything but RELKIND_RELATION).
    let ti_options = if rel.rd_createSubid.get() != types_core::xact::InvalidSubTransactionId
        || rel.rd_firstRelfilelocatorSubid.get() != types_core::xact::InvalidSubTransactionId
    {
        tableam_vocab::TABLE_INSERT_SKIP_FSM
    } else {
        0
    };

    let mut index_state = execindexing::ExecOpenIndices(mcx, rel, false)?;

    for wc in cstate.where_clause.iter() {
        if clauses::contain_volatile_functions(wc)? {
            unported("FROM ... WHERE with volatile functions (CIM_SINGLE lane)");
        }
    }
    let mut qualexpr =
        execexpr::exec_init_qual(mcx, &cstate.where_clause, execexpr::ParamBind::NONE)?;
    if let Some(q) = qualexpr.as_mut() {
        // SAFETY: qual scratch results land in the statement mcx, which
        // outlives every per-row evaluation.
        unsafe { q.arm_result_mcx_raw(mcx) };
    }

    // std Vec: SlotData owns droppy state via the arena-erased views; the
    // slot pool itself is per-statement (CopyMultiInsertBuffer.slots).
    let mut slots: Vec<types_slot::SlotData<'mcx>> = Vec::new();
    let mut linenos: Vec<u64> = Vec::new();
    let mut bistate = heapam::GetBulkInsertState();
    let mut nused = 0usize;
    let mut buffered_bytes = 0usize;

    let mut processed: u64 = 0;
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
                continue;
            }
        }

        if let Some(constr) = rel.rd_att.constr.as_deref() {
            if constr.num_check > 0 || !constr.check.is_empty() {
                panic!("ExecConstraints (execMain.c): CHECK constraints not ported");
            }
            if constr.has_not_null {
                not_null_constraints(rel, slot)?;
            }
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
            )?;
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
        )?;
    }

    tableam::table_finish_bulk_insert(rel, ti_options)?;

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
    Ok(processed)
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
) -> PgResult<()> {
    let save_cur_lineno = cstate.cur_lineno;
    let save_line_buf_valid = cstate.line_buf_valid;
    cstate.line_buf_valid = false;

    let mut refs: Vec<&mut types_slot::SlotData<'mcx>> = slots.iter_mut().collect();
    tableam::table_multi_insert(mcx, rel, &mut refs, mycid, ti_options, Some(bistate))?;

    if index_state.num_indices() > 0 {
        // C resets the per-tuple econtext per buffered row (CopyMultiInsertBufferFlush).
        let mut eval_cx = MemoryContext::new_bump("CopyIndexEvalPerTuple");
        for (i, slot) in refs.into_iter().enumerate() {
            eval_cx.reset();
            cstate.cur_lineno = linenos[i];
            execindexing::ExecInsertIndexTuples(
                mcx,
                eval_cx.mcx(),
                index_state,
                rel,
                slot,
                false,
                None,
                &[],
            )?;
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
fn copy_from_error_context(
    cstate: &CopyFromState<'_, '_>,
    rel: &Relation<'_>,
    e: Box<PgError>,
) -> Box<PgError> {
    let relname = rel.name();
    let lineno = cstate.cur_lineno;
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

fn not_null_constraints<'mcx>(
    rel: &Relation<'mcx>,
    slot: &mut types_slot::SlotData<'mcx>,
) -> PgResult<()> {
    for i in 0..rel.rd_att.natts as usize {
        let att = rel.rd_att.attr(i);
        if att.attnotnull && exectuples::slot_attisnull(slot, i as i32 + 1) {
            return Err(not_null_violation(rel, i));
        }
    }
    Ok(())
}

#[cold]
#[inline(never)]
fn not_null_violation(rel: &Relation<'_>, attidx: usize) -> Box<PgError> {
    let att = rel.rd_att.attr(attidx);
    let col = String::from_utf8_lossy(att.attname.name_str()).into_owned();
    Box::new(
        PgError::error(format!(
            "null value in column \"{col}\" of relation \"{}\" violates not-null constraint",
            rel.name()
        ))
        .with_sqlstate(ERRCODE_NOT_NULL_VIOLATION),
    )
}

/// `EndCopyFrom` (copyfrom.c).
pub fn EndCopyFrom(cstate: CopyFromState<'_, '_>) -> PgResult<()> {
    if let CopySrc::File { fd, filename } = cstate.src {
        if fd::FreeFile(fd)? != 0 {
            ereport(ERROR)
                .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
                .errcode_for_file_access()
                .errmsg(format!("could not close file \"{filename}\": %m"))
                .finish(loc("EndCopyFrom"))?;
        }
    }
    Ok(())
}

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
        b'f' | b'p' => {
            unported("FROM into foreign/partitioned relations (FDW/tuple-routing lanes)")
        }
        _ => (format!("cannot copy to non-table relation \"{name}\""), None),
    };
    let mut e = PgError::error(msg).with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE);
    if let Some(h) = hint {
        e = e.with_hint(h);
    }
    Box::new(e)
}
