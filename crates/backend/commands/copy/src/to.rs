// copyto.c, text format to file. Frontend/callback destinations and the CSV/
// binary routines are loud in lib.rs before this module is reached.

use core::ffi::CStr;

use datum::Datum;
use elog::ereport;
use mcx::{Mcx, MemoryContext, PgVec};
use stringinfo::StringInfo;
use types_core::primitive::InvalidOid;
use types_error::{ErrorLocation, PgError, PgResult, ERRCODE_INVALID_NAME, ERRCODE_WRONG_OBJECT_TYPE, ERROR};
use types_fmgr::{function_call1_coll_in, FmgrInfo};
use types_nodes::NodeList;
use types_rel::Relation;
use types_scan::ForwardScanDirection;
use types_slot::SlotData;

use crate::{unported, CopyFormatOptions, CopyGetAttnums, ProcessCopyOptions, RELKIND_RELATION};

// C buffers per-row fwrite in libc's FILE; here fe_msgbuf retains rows until
// this watermark, so the write cadence (not per-row syscalls) matches C.
const FILE_FLUSH_THRESHOLD: usize = 65536;

pub struct CopyToState<'mcx, 's> {
    fe_msgbuf: StringInfo<'mcx>,
    copy_file: i32,
    filename: &'s str,
    pub opts: CopyFormatOptions<'s>,
    attnumlist: PgVec<'mcx, i16>,
    file_encoding: i32,
    need_transcoding: bool,
    bytes_processed: u64,
    rowcx: MemoryContext,
}

fn loc(funcname: &'static str) -> ErrorLocation {
    ErrorLocation::new("copyto.c", 0, funcname)
}

/// `BeginCopyTo` (copyto.c), relation-to-file arm.
pub fn BeginCopyTo<'mcx, 's>(
    mcx: Mcx<'mcx>,
    rel: &Relation<'mcx>,
    filename: &'s str,
    attnamelist: &NodeList<'_>,
    options: &NodeList<'s>,
) -> PgResult<CopyToState<'mcx, 's>> {
    if rel.rd_rel.relkind != RELKIND_RELATION
        && !(rel.rd_rel.relkind == b'm' && rel.rd_rel.relispopulated)
    {
        return Err(cannot_copy_from_relkind(rel));
    }

    let opts = ProcessCopyOptions(false, options)?;
    let attnumlist = CopyGetAttnums(mcx, &rel.rd_att, rel, attnamelist)?;

    let file_encoding = if opts.file_encoding < 0 {
        mbutils::pg_get_client_encoding()
    } else {
        opts.file_encoding
    };
    let need_transcoding = !(file_encoding == mbutils::GetDatabaseEncoding()
        || file_encoding == wchar::PG_SQL_ASCII);
    if file_encoding >= wchar::PG_SJIS {
        unported("TO with a client-only encoding (pg_encoding_mblen escape walk)");
    }

    if !filename.starts_with('/') {
        return Err(Box::new(
            PgError::error("relative path not allowed for COPY to file")
                .with_sqlstate(ERRCODE_INVALID_NAME),
        ));
    }
    // SAFETY: process-global umask swap around open, as C's BeginCopyTo.
    let oumask = unsafe { libc::umask(0o022) };
    let copy_file = fd::AllocateFile(filename, "wb");
    // SAFETY: restore saved umask.
    unsafe { libc::umask(oumask) };
    let copy_file = copy_file?;
    if copy_file < 0 {
        ereport(ERROR)
            .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not open file \"{filename}\" for writing: %m"))
            .errhint(
                "COPY TO instructs the PostgreSQL server process to write a file. You may \
                 want a client-side facility such as psql's \\copy.",
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

    Ok(CopyToState {
        fe_msgbuf: StringInfo::new_in(mcx)?,
        copy_file,
        filename,
        opts,
        attnumlist,
        file_encoding,
        need_transcoding,
        bytes_processed: 0,
        rowcx: MemoryContext::new("COPY TO"),
    })
}

/// `DoCopyTo` (copyto.c): scan the relation, emit every visible row.
pub fn DoCopyTo<'mcx>(
    mcx: Mcx<'mcx>,
    cstate: &mut CopyToState<'mcx, '_>,
    rel: &Relation<'mcx>,
) -> PgResult<u64> {
    let tup_desc = &rel.rd_att;

    // FmgrInfo carries droppy fn_extra, so PgVec::new_in (printtup precedent);
    // resolve-once, never per row (rule 4).
    let mut out_functions: PgVec<'mcx, FmgrInfo> = PgVec::new_in(mcx);
    out_functions.try_reserve_exact(cstate.attnumlist.len()).map_err(|_| {
        mcx.oom(cstate.attnumlist.len() * core::mem::size_of::<FmgrInfo>())
    })?;
    for &attnum in cstate.attnumlist.iter() {
        let attr = tup_desc.attr(attnum as usize - 1);
        let (func_oid, _is_varlena) = lsyscache::typ::getTypeOutputInfo(attr.atttypid)?;
        out_functions.push(fmgr_core::fmgr_info(func_oid)?);
    }

    if cstate.opts.header_line {
        let mut hdr_delim = false;
        for &attnum in cstate.attnumlist.iter() {
            if hdr_delim {
                cstate.fe_msgbuf.append_byte(cstate.opts.delim)?;
            }
            hdr_delim = true;
            let colname = tup_desc.attr(attnum as usize - 1).attname;
            copy_attribute_out_text(
                &mut cstate.fe_msgbuf,
                colname.name_str(),
                cstate.opts.delim,
            )?;
        }
        end_of_row(cstate)?;
    }

    let snapshot = Some(snapmgr::GetActiveSnapshot());
    let mut scandesc = tableam::table_beginscan(mcx, rel, snapshot, 0, PgVec::new_in(mcx))?;
    let mut slot = tableam::table_slot_create(mcx, rel)?;

    let mut processed: u64 = 0;
    while tableam::table_scan_getnextslot(mcx, &mut scandesc, ForwardScanDirection, &mut slot)? {
        exectuples::slot_getallattrs(&mut slot);
        CopyOneRowTo(cstate, &mut slot, &mut out_functions)?;
        processed += 1;
    }

    tableam::table_endscan(scandesc)?;
    flush_to_file(cstate)?;
    Ok(processed)
}

/// `CopyOneRowTo` + `CopyToTextLikeOneRow` (copyto.c).
fn CopyOneRowTo<'mcx>(
    cstate: &mut CopyToState<'mcx, '_>,
    slot: &mut SlotData<'mcx>,
    out_functions: &mut [FmgrInfo],
) -> PgResult<()> {
    let CopyToState { rowcx, fe_msgbuf, opts, attnumlist, need_transcoding, file_encoding, .. } =
        cstate;
    rowcx.reset();
    let rmcx = rowcx.mcx();

    let base = slot.base();
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
        copy_attribute_out_text(fe_msgbuf, s, opts.delim)?;
    }
    end_of_row(cstate)
}

// CopySendTextLikeEndOfRow + CopySendEndOfRow, COPY_FILE arm.
fn end_of_row(cstate: &mut CopyToState<'_, '_>) -> PgResult<()> {
    cstate.fe_msgbuf.append_byte(b'\n')?;
    if cstate.fe_msgbuf.len() >= FILE_FLUSH_THRESHOLD {
        flush_to_file(cstate)?;
    }
    Ok(())
}

fn flush_to_file(cstate: &mut CopyToState<'_, '_>) -> PgResult<()> {
    if cstate.fe_msgbuf.is_empty() {
        return Ok(());
    }
    let bytes = cstate.fe_msgbuf.as_bytes();
    let wrote = fd::with_allocated_stdio(cstate.copy_file, |f| {
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
        None => panic!("COPY TO: AllocateFile index {} vanished", cstate.copy_file),
    }
    cstate.bytes_processed += cstate.fe_msgbuf.len() as u64;
    cstate.fe_msgbuf.reset();
    Ok(())
}

/// `EndCopyTo` + `EndCopy` (copyto.c).
pub fn EndCopyTo(mut cstate: CopyToState<'_, '_>) -> PgResult<()> {
    flush_to_file(&mut cstate)?;
    if fd::FreeFile(cstate.copy_file)? != 0 {
        ereport(ERROR)
            .with_saved_errno(std::io::Error::last_os_error().raw_os_error().unwrap_or(0))
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{}\": %m", cstate.filename))
            .finish(loc("EndCopy"))?;
    }
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
            ptr += 1;
        }
    }
    if ptr > start {
        buf.append_bytes(&s[start..ptr])?;
    }
    Ok(())
}

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
