// copyfrom.c, text format from file, single-insert lane. C's default here is
// CIM_MULTI (heap_multi_insert buffering); tableam::multi_insert is loud in
// heapam, so this port runs the CIM_SINGLE shape — tracked as an M3 perf gap
// in CATALOG.tsv, not a silent behavior change.

use elog::ereport;
use mcx::{vec_from_elem_in, Mcx, MemoryContext, PgVec};
use types_core::Oid;
use types_error::{
    ErrorLocation, PgError, PgResult, ERRCODE_NOT_NULL_VIOLATION, ERRCODE_UNDEFINED_FUNCTION,
    ERRCODE_WRONG_OBJECT_TYPE, ERROR,
};
use types_fmgr::FmgrInfo;
use types_nodes::NodeList;
use types_rel::Relation;
use types_tuple::NameData;

use crate::fromparse::{EolType, INPUT_BUF_SIZE, RAW_BUF_SIZE};
use crate::{unported, CopyFormatOptions, CopyGetAttnums, ProcessCopyOptions, RELKIND_RELATION};

pub struct CopyFromState<'mcx, 's> {
    pub opts: CopyFormatOptions<'s>,
    pub(crate) copy_file: i32,
    pub(crate) filename: &'s str,
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
    pub(crate) attribute_buf: PgVec<'mcx, u8>,
    pub(crate) raw_fields: PgVec<'mcx, i32>,
    pub(crate) max_fields: usize,
    pub(crate) eol_type: EolType,
    pub cur_lineno: u64,
    pub(crate) file_encoding: i32,
    pub(crate) need_transcoding: bool,
    pub(crate) conversion_proc: Oid,
    pub(crate) convertcx: MemoryContext,
    pub(crate) attnumlist: PgVec<'mcx, i16>,
    pub(crate) in_functions: PgVec<'mcx, FmgrInfo>,
    pub(crate) typioparams: PgVec<'mcx, Oid>,
    pub(crate) atttypmods: PgVec<'mcx, i32>,
    pub(crate) attnames: PgVec<'mcx, NameData>,
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

/// `BeginCopyFrom` (copyfrom.c), text-from-file arm.
pub fn BeginCopyFrom<'mcx, 's>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    filename: &'s str,
    attnamelist: &NodeList<'_>,
    options: &NodeList<'s>,
) -> PgResult<CopyFromState<'mcx, 's>> {
    let opts = ProcessCopyOptions(true, options)?;
    let tup_desc = &rel.rd_att;
    let attnumlist = CopyGetAttnums(mcx, tup_desc, rel, attnamelist)?;
    let num_phys_attrs = tup_desc.natts as usize;

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
    for i in 0..num_phys_attrs {
        let att = tup_desc.attr(i);
        attnames.push(att.attname);
        // build_column_default/ExecInitExpr are the rewrite gap: a column the
        // input does not supply keeps NULL here, which silently diverges when
        // a default exists.
        if att.atthasdef && !attnumlist.contains(&(i as i16 + 1)) {
            unported("FROM with omitted defaulted column (build_column_default lane)");
        }
    }
    if tup_desc
        .constr
        .as_deref()
        .is_some_and(|c| c.has_generated_stored || c.has_generated_virtual)
    {
        unported("FROM with generated columns (ExecComputeStoredGenerated lane)");
    }

    let copy_file = fd::AllocateFile(filename, "rb")?;
    if copy_file < 0 {
        ereport(ERROR)
            .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{filename}\" for reading: %m"))
            .errhint(
                "COPY FROM instructs the PostgreSQL server process to read a file. You may \
                 want a client-side facility such as psql's \\copy.",
            )
            .finish(loc("BeginCopyFrom"))?;
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

    let max_fields = attnumlist.len();
    Ok(CopyFromState {
        opts,
        copy_file,
        filename,
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
        attribute_buf: PgVec::new_in(mcx),
        raw_fields: PgVec::new_in(mcx),
        max_fields,
        eol_type: EolType::Unknown,
        cur_lineno: 0,
        file_encoding,
        need_transcoding,
        conversion_proc,
        convertcx: MemoryContext::new("COPY convert"),
        attnumlist,
        in_functions,
        typioparams,
        atttypmods,
        attnames,
        bytes_processed: 0,
    })
}

/// `CopyFrom` (copyfrom.c): read rows, insert into the heap + indexes.
pub fn CopyFrom<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyFromState<'mcx, '_>,
    rel: &Relation<'mcx>,
) -> PgResult<u64> {
    if rel.rd_rel.relkind != RELKIND_RELATION {
        return Err(cannot_copy_to_relkind(rel));
    }

    let mycid = xact::GetCurrentCommandId(true)?;
    let ti_options = 0;

    let mut index_state = execindexing::ExecOpenIndices(mcx, rel, false)?;
    let mut slot = tableam::table_slot_create(mcx, rel)?;

    let mut processed: u64 = 0;
    loop {
        exectuples::exec_clear_tuple(&mut slot, mcx);

        // Input-function results and the materialized tuple land in the
        // statement mcx and are reclaimed at statement end (nodemodifytable
        // ExecInsert precedent); WATCH: unbounded for very large loads.
        {
            let base = slot.base_mut();
            if !cstate.next_copy_from(mcx, &mut base.tts_values, &mut base.tts_isnull)? {
                break;
            }
        }
        exectuples::exec_store_virtual_tuple(&mut slot);
        slot.base_mut().tts_tableOid = rel.rd_id;

        if let Some(constr) = rel.rd_att.constr.as_deref() {
            if constr.num_check > 0 || !constr.check.is_empty() {
                panic!("ExecConstraints (execMain.c): CHECK constraints not ported");
            }
            if constr.has_not_null {
                not_null_constraints(rel, &mut slot)?;
            }
        }

        tableam::table_tuple_insert(mcx, rel, &mut slot, mycid, ti_options, None)?;

        if index_state.num_indices() > 0 {
            execindexing::ExecInsertIndexTuples(mcx, &mut index_state, rel, &mut slot)?;
        }
        processed += 1;
    }

    Ok(processed)
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
    if fd::FreeFile(cstate.copy_file)? != 0 {
        ereport(ERROR)
            .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{}\": %m", cstate.filename))
            .finish(loc("EndCopyFrom"))?;
    }
    Ok(())
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
