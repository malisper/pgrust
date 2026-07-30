//! `contrib/pg_walinspect/pg_walinspect.c` — SQL functions to inspect the
//! contents of the write-ahead log, over the ported xlogreader engine
//! (LocalPageRead's no-wait callbacks) and xlogstats.
//!
//! C's per-tuple temporary memory context is not reproduced: row datums are
//! allocated in the arming (per-query) context, which the executor resets
//! after the call.

#![allow(non_snake_case)]

use ::mcx::Mcx;
use ::stringinfo::StringInfo;
use ::types_core::{XLogRecPtr, TEXTOID};
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_fmgr::{
    varlena_result, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction,
};
use datum::Datum;
use xlogreader::{LocalPageRead, XLogReaderState};
use xlogreader_seams::XLOG_BLCKSZ;
use xlogstats::{XLogStats, MAX_XLINFO_TYPES, RM_MAX_ID};

const LIBRARY: &str = "pg_walinspect";
const BLCKSZ: usize = 8192;

// bimg_info flags (xlogrecord.h).
const BKPIMAGE_HAS_HOLE: u8 = 0x01;
const BKPIMAGE_COMPRESS_PGLZ: u8 = 0x04;
const BKPIMAGE_COMPRESS_LZ4: u8 = 0x08;
const BKPIMAGE_COMPRESS_ZSTD: u8 = 0x10;

const XLR_INFO_MASK: u8 = 0x0F;

fn lsn_fmt(lsn: XLogRecPtr) -> String {
    format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32)
}

#[track_caller]
#[cold]
fn param_err(msg: impl Into<String>) -> Box<PgError> {
    Box::new(PgError::error(msg.into()).with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE))
}

fn GetCurrentLSN() -> XLogRecPtr {
    if !transam_xlog::RecoveryInProgress() {
        transam_xlog::GetFlushRecPtr(None)
    } else {
        xlogrecovery::GetXLogReplayRecPtr().0
    }
}

fn ValidateInputLSNs(start_lsn: XLogRecPtr, end_lsn: &mut XLogRecPtr) -> PgResult<()> {
    let curr_lsn = GetCurrentLSN();
    if start_lsn > curr_lsn {
        return Err(Box::new(
            PgError::error("WAL start LSN must be less than current LSN")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!(
                    "Current WAL LSN on the database system is at {}.",
                    lsn_fmt(curr_lsn)
                )),
        ));
    }
    if start_lsn > *end_lsn {
        return Err(param_err("WAL start LSN must be less than end LSN"));
    }
    if *end_lsn > curr_lsn {
        *end_lsn = curr_lsn;
    }
    Ok(())
}

fn InitXLogReaderState<'m>(
    mcx: Mcx<'m>,
    lsn: XLogRecPtr,
) -> PgResult<(XLogReaderState<'m>, LocalPageRead)> {
    if lsn < XLOG_BLCKSZ as u64 {
        return Err(param_err(format!("could not read WAL at LSN {}", lsn_fmt(lsn))));
    }

    let mut reader = XLogReaderState::allocate(mcx, transam_xlog::wal_segment_size())?;
    let mut routine = LocalPageRead { wait_for_wal: false };

    let first_valid_record = reader.XLogFindNextRecord(&mut routine, lsn)?;
    if first_valid_record == 0 {
        return Err(Box::new(PgError::error(format!(
            "could not find a valid record after {}",
            lsn_fmt(lsn)
        ))));
    }
    reader.XLogBeginRead(first_valid_record);

    Ok((reader, routine))
}

fn ReadNextXLogRecord(
    reader: &mut XLogReaderState<'_>,
    routine: &mut LocalPageRead,
) -> PgResult<bool> {
    match reader.XLogReadRecord(routine)? {
        Some(_) => Ok(true),
        None => {
            if reader.v.private_end_of_wal {
                return Ok(false);
            }
            let at = lsn_fmt(reader.v.EndRecPtr);
            Err(Box::new(PgError::error(match reader.errormsg() {
                Some(msg) => format!("could not read WAL at {at}: {msg}"),
                None => format!("could not read WAL at {at}"),
            })))
        }
    }
}

fn text_datum(mcx: Mcx<'_>, s: &[u8]) -> PgResult<Datum> {
    Ok(varlena_result(varlena::cstring_to_text(mcx, s)?))
}

fn bytea_datum(mcx: Mcx<'_>, data: &[u8]) -> PgResult<Datum> {
    let mut image = Vec::with_capacity(data.len() + 4);
    image.extend_from_slice(&(((data.len() + 4) as u32) << 2).to_ne_bytes());
    image.extend_from_slice(data);
    ::types_fmgr::byref_result(mcx, &image)
}

fn record_type(desc: &rmgr::RmgrData, info: u8) -> PgResult<String> {
    Ok(match (desc.rm_identify)(info) {
        Some(id) => id.to_string(),
        None => format!("UNKNOWN ({:x})", info & !XLR_INFO_MASK),
    })
}

fn GetWALRecordInfo(
    mcx: Mcx<'_>,
    reader: &XLogReaderState<'_>,
) -> PgResult<([Datum; 11], [bool; 11])> {
    let desc = rmgr::GetRmgr(reader.XLogRecGetRmid())?;
    let rec_type = record_type(desc, reader.XLogRecGetInfo())?;

    let mut rec_desc = StringInfo::new_in(mcx)?;
    (desc.rm_desc)(&mut rec_desc, &reader.v)?;

    let mut fpi_len: u32 = 0;
    let has_block_refs = reader.XLogRecHasAnyBlockRefs();
    let mut rec_blk_ref = StringInfo::new_in(mcx)?;
    if has_block_refs {
        rmgrdesc::xlogdesc::XLogRecGetBlockRefInfo(
            &reader.v,
            false,
            true,
            &mut rec_blk_ref,
            Some(&mut fpi_len),
        )?;
    }

    let mut values = [Datum::null(); 11];
    let mut nulls = [false; 11];
    values[0] = Datum::from_u64(reader.v.ReadRecPtr);
    values[1] = Datum::from_u64(reader.v.EndRecPtr);
    values[2] = Datum::from_u64(reader.XLogRecGetPrev());
    values[3] = Datum::from_transaction_id(reader.XLogRecGetXid());
    values[4] = text_datum(mcx, desc.rm_name.as_bytes())?;
    values[5] = text_datum(mcx, rec_type.as_bytes())?;
    values[6] = Datum::from_u32(reader.XLogRecGetTotalLen());
    values[7] = Datum::from_u32(reader.XLogRecGetDataLen());
    values[8] = Datum::from_u32(fpi_len);
    if rec_desc.len() > 0 {
        values[9] = text_datum(mcx, rec_desc.as_bytes())?;
    } else {
        nulls[9] = true;
    }
    if has_block_refs {
        values[10] = text_datum(mcx, rec_blk_ref.as_bytes())?;
    } else {
        nulls[10] = true;
    }
    Ok((values, nulls))
}

fn fc_pg_get_wal_record_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_wal_record_info: resolved FmgrInfo required");
    let lsn: XLogRecPtr = fcinfo.arg(0).as_u64();
    let curr_lsn = GetCurrentLSN();

    if lsn > curr_lsn {
        return Err(Box::new(
            PgError::error("WAL input LSN must be less than current LSN")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!(
                    "Current WAL LSN on the database system is at {}.",
                    lsn_fmt(curr_lsn)
                )),
        ));
    }

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(PgError::error("return type must be a row type")));
    }
    let tupdesc = resolved.result_tuple_desc.expect("composite result has tupdesc");

    let (mut reader, mut routine) = InitXLogReaderState(mcx, lsn)?;

    if !ReadNextXLogRecord(&mut reader, &mut routine)? {
        return Err(param_err(format!(
            "could not read WAL at {}",
            lsn_fmt(reader.v.EndRecPtr)
        )));
    }

    let (values, nulls) = GetWALRecordInfo(mcx, &reader)?;

    let tup = heaptuple::heap_form_tuple(mcx, &tupdesc, &values, &nulls)?;
    let d = Datum::from_usize(tup.header_ptr() as usize);
    core::mem::forget(tup); // leak into the arming context (C palloc ownership)
    Ok(d)
}

fn GetWALRecordsInfo(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
) -> PgResult<Datum> {
    debug_assert!(start_lsn <= end_lsn);

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let (mut reader, mut routine) = InitXLogReaderState(mcx, start_lsn)?;

    while ReadNextXLogRecord(&mut reader, &mut routine)? && reader.v.EndRecPtr <= end_lsn {
        let (values, nulls) = GetWALRecordInfo(mcx, &reader)?;
        srf.putvalues(&values, &nulls)?;
        postgres_seams::check_for_interrupts::call()?;
    }

    Ok(srf.finish(fcinfo))
}

fn fc_pg_get_wal_records_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_wal_records_info: resolved FmgrInfo required");
    let start_lsn: XLogRecPtr = fcinfo.arg(0).as_u64();
    let mut end_lsn: XLogRecPtr = fcinfo.arg(1).as_u64();

    ValidateInputLSNs(start_lsn, &mut end_lsn)?;
    GetWALRecordsInfo(flinfo, fcinfo, start_lsn, end_lsn)
}

// Removed in 1.1; kept for compatibility with extension version 1.0.
fn fc_pg_get_wal_records_info_till_end_of_wal(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo =
        flinfo.expect("pg_get_wal_records_info_till_end_of_wal: resolved FmgrInfo required");
    let start_lsn: XLogRecPtr = fcinfo.arg(0).as_u64();
    let end_lsn = GetCurrentLSN();

    if start_lsn > end_lsn {
        return Err(Box::new(
            PgError::error("WAL start LSN must be less than current LSN")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!(
                    "Current WAL LSN on the database system is at {}.",
                    lsn_fmt(end_lsn)
                )),
        ));
    }

    GetWALRecordsInfo(flinfo, fcinfo, start_lsn, end_lsn)
}

#[allow(clippy::too_many_arguments)]
fn FillXLogStatsRow(
    mcx: Mcx<'_>,
    name: &str,
    n: u64,
    total_count: u64,
    rec_len: u64,
    total_rec_len: u64,
    fpi_len: u64,
    total_fpi_len: u64,
    tot_len: u64,
    total_len: u64,
) -> PgResult<[Datum; 9]> {
    let pct = |part: u64, total: u64| -> f64 {
        if total != 0 {
            100.0 * part as f64 / total as f64
        } else {
            0.0
        }
    };
    Ok([
        text_datum(mcx, name.as_bytes())?,
        Datum::from_i64(n as i64),
        Datum::from_f64(pct(n, total_count)),
        Datum::from_i64(rec_len as i64),
        Datum::from_f64(pct(rec_len, total_rec_len)),
        Datum::from_i64(fpi_len as i64),
        Datum::from_f64(pct(fpi_len, total_fpi_len)),
        Datum::from_i64(tot_len as i64),
        Datum::from_f64(pct(tot_len, total_len)),
    ])
}

fn GetXLogSummaryStats(
    mcx: Mcx<'_>,
    srf: &mut funcapi::MaterializedSRF<'_>,
    stats: &XLogStats,
    stats_per_record: bool,
) -> PgResult<()> {
    // Totals pass over RmgrIdIsValid ids only (builtin + custom 128..=255;
    // the gap between them is skipped, as in C).
    let mut total_count: u64 = 0;
    let mut total_rec_len: u64 = 0;
    let mut total_fpi_len: u64 = 0;
    for ri in 0..=RM_MAX_ID {
        if !rmgr::RmgrIdIsBuiltin(ri as i32) && !rmgr::RmgrIdIsCustom(ri as i32) {
            continue;
        }
        total_count += stats.rmgr_stats[ri].count;
        total_rec_len += stats.rmgr_stats[ri].rec_len;
        total_fpi_len += stats.rmgr_stats[ri].fpi_len;
    }
    let total_len = total_rec_len + total_fpi_len;

    for ri in 0..=RM_MAX_ID {
        if !rmgr::RmgrIdExists(ri as u8) {
            continue;
        }
        let desc = rmgr::GetRmgr(ri as u8)?;

        if stats_per_record {
            for rj in 0..MAX_XLINFO_TYPES {
                let s = &stats.record_stats[ri][rj];
                if s.count == 0 {
                    continue;
                }
                let id = match (desc.rm_identify)((rj << 4) as u8) {
                    Some(id) => id.to_string(),
                    None => format!("UNKNOWN ({:x})", rj << 4),
                };
                let values = FillXLogStatsRow(
                    mcx,
                    &format!("{}/{}", desc.rm_name, id),
                    s.count,
                    total_count,
                    s.rec_len,
                    total_rec_len,
                    s.fpi_len,
                    total_fpi_len,
                    s.rec_len + s.fpi_len,
                    total_len,
                )?;
                srf.putvalues(&values, &[false; 9])?;
            }
        } else {
            let s = &stats.rmgr_stats[ri];
            let values = FillXLogStatsRow(
                mcx,
                desc.rm_name,
                s.count,
                total_count,
                s.rec_len,
                total_rec_len,
                s.fpi_len,
                total_fpi_len,
                s.rec_len + s.fpi_len,
                total_len,
            )?;
            srf.putvalues(&values, &[false; 9])?;
        }
    }
    Ok(())
}

fn GetWalStats(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    start_lsn: XLogRecPtr,
    end_lsn: XLogRecPtr,
    stats_per_record: bool,
) -> PgResult<Datum> {
    debug_assert!(start_lsn <= end_lsn);

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let (mut reader, mut routine) = InitXLogReaderState(mcx, start_lsn)?;

    let mut stats = Box::new(XLogStats::ZEROED);
    while ReadNextXLogRecord(&mut reader, &mut routine)? && reader.v.EndRecPtr <= end_lsn {
        xlogstats::XLogRecStoreStats(&mut stats, &reader.v);
        postgres_seams::check_for_interrupts::call()?;
    }

    GetXLogSummaryStats(mcx, &mut srf, &stats, stats_per_record)?;

    Ok(srf.finish(fcinfo))
}

fn fc_pg_get_wal_stats(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_wal_stats: resolved FmgrInfo required");
    let start_lsn: XLogRecPtr = fcinfo.arg(0).as_u64();
    let mut end_lsn: XLogRecPtr = fcinfo.arg(1).as_u64();
    let stats_per_record = fcinfo.arg(2).as_bool();

    ValidateInputLSNs(start_lsn, &mut end_lsn)?;
    GetWalStats(flinfo, fcinfo, start_lsn, end_lsn, stats_per_record)
}

// Removed in 1.1; kept for compatibility with extension version 1.0.
fn fc_pg_get_wal_stats_till_end_of_wal(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_wal_stats_till_end_of_wal: resolved FmgrInfo required");
    let start_lsn: XLogRecPtr = fcinfo.arg(0).as_u64();
    let stats_per_record = fcinfo.arg(1).as_bool();
    let end_lsn = GetCurrentLSN();

    if start_lsn > end_lsn {
        return Err(Box::new(
            PgError::error("WAL start LSN must be less than current LSN")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!(
                    "Current WAL LSN on the database system is at {}.",
                    lsn_fmt(end_lsn)
                )),
        ));
    }

    GetWalStats(flinfo, fcinfo, start_lsn, end_lsn, stats_per_record)
}

fn GetWALBlockInfo(
    mcx: Mcx<'_>,
    srf: &mut funcapi::MaterializedSRF<'_>,
    reader: &mut XLogReaderState<'_>,
    show_data: bool,
) -> PgResult<()> {
    debug_assert!(reader.XLogRecHasAnyBlockRefs());

    let desc = rmgr::GetRmgr(reader.XLogRecGetRmid())?;
    let rec_type = record_type(desc, reader.XLogRecGetInfo())?;

    let mut rec_desc = StringInfo::new_in(mcx)?;
    (desc.rm_desc)(&mut rec_desc, &reader.v)?;

    let max_block_id = reader.XLogRecMaxBlockId();
    debug_assert!(max_block_id >= 0);
    for block_id in 0..=max_block_id as u8 {
        if !reader.XLogRecHasBlockRef(block_id) {
            continue;
        }

        let Some((rnode, forknum, blkno, _)) = reader.XLogRecGetBlockTagExtended(block_id)
        else {
            continue;
        };
        let blk = *reader.v.block(block_id);

        let block_data_len: u32 = if blk.has_data { blk.data_len as u32 } else { 0 };

        let mut block_fpi_len: u32 = 0;
        let mut block_fpi_info: Option<Datum> = None;
        if blk.has_image {
            block_fpi_len = blk.bimg_len as u32;

            let mut flags: Vec<Datum> = Vec::with_capacity(5);
            if blk.bimg_info & BKPIMAGE_HAS_HOLE != 0 {
                flags.push(text_datum(mcx, b"HAS_HOLE")?);
            }
            if blk.apply_image {
                flags.push(text_datum(mcx, b"APPLY")?);
            }
            if blk.bimg_info & BKPIMAGE_COMPRESS_PGLZ != 0 {
                flags.push(text_datum(mcx, b"COMPRESS_PGLZ")?);
            }
            if blk.bimg_info & BKPIMAGE_COMPRESS_LZ4 != 0 {
                flags.push(text_datum(mcx, b"COMPRESS_LZ4")?);
            }
            if blk.bimg_info & BKPIMAGE_COMPRESS_ZSTD != 0 {
                flags.push(text_datum(mcx, b"COMPRESS_ZSTD")?);
            }

            let image = if flags.is_empty() {
                datum::array_build::construct_empty_array_image(mcx, TEXTOID)?
            } else {
                datum::array_build::construct_array_image(mcx, &flags, TEXTOID, -1, false, b'i')?
            };
            block_fpi_info = Some(::types_fmgr::byref_result(mcx, &image)?);
        }

        let mut values = [Datum::null(); 20];
        let mut nulls = [false; 20];
        let mut i = 0usize;

        values[i] = Datum::from_u64(reader.v.ReadRecPtr);
        i += 1;
        values[i] = Datum::from_u64(reader.v.EndRecPtr);
        i += 1;
        values[i] = Datum::from_u64(reader.XLogRecGetPrev());
        i += 1;
        values[i] = Datum::from_i16(block_id as i16);
        i += 1;

        values[i] = Datum::from_oid(rnode.spcOid);
        i += 1;
        values[i] = Datum::from_oid(rnode.dbOid);
        i += 1;
        values[i] = Datum::from_oid(rnode.relNumber);
        i += 1;
        values[i] = Datum::from_i16(forknum as i16);
        i += 1;
        values[i] = Datum::from_i64(blkno as i64);
        i += 1;

        values[i] = Datum::from_transaction_id(reader.XLogRecGetXid());
        i += 1;
        values[i] = text_datum(mcx, desc.rm_name.as_bytes())?;
        i += 1;
        values[i] = text_datum(mcx, rec_type.as_bytes())?;
        i += 1;

        values[i] = Datum::from_u32(reader.XLogRecGetTotalLen());
        i += 1;
        values[i] = Datum::from_u32(reader.XLogRecGetDataLen());
        i += 1;
        values[i] = Datum::from_u32(block_data_len);
        i += 1;
        values[i] = Datum::from_u32(block_fpi_len);
        i += 1;

        match block_fpi_info {
            Some(d) => values[i] = d,
            None => nulls[i] = true,
        }
        i += 1;

        if rec_desc.len() > 0 {
            values[i] = text_datum(mcx, rec_desc.as_bytes())?;
        } else {
            nulls[i] = true;
        }
        i += 1;

        match reader.XLogRecGetBlockData(block_id) {
            Some(data) if show_data => {
                let owned = data.to_vec();
                values[i] = bytea_datum(mcx, &owned)?;
            }
            _ => nulls[i] = true,
        }
        i += 1;

        if blk.has_image && show_data {
            let mut page = vec![0u8; BLCKSZ];
            if !reader.RestoreBlockImage(block_id, &mut page) {
                return Err(Box::new(PgError::error(
                    reader
                        .errormsg_buf_raw()
                        .unwrap_or("could not restore block image")
                        .to_string(),
                )));
            }
            values[i] = bytea_datum(mcx, &page)?;
        } else {
            nulls[i] = true;
        }
        i += 1;

        debug_assert_eq!(i, 20);

        srf.putvalues(&values, &nulls)?;
    }

    Ok(())
}

fn fc_pg_get_wal_block_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_wal_block_info: resolved FmgrInfo required");
    let start_lsn: XLogRecPtr = fcinfo.arg(0).as_u64();
    let mut end_lsn: XLogRecPtr = fcinfo.arg(1).as_u64();
    let show_data = fcinfo.arg(2).as_bool();

    ValidateInputLSNs(start_lsn, &mut end_lsn)?;

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let (mut reader, mut routine) = InitXLogReaderState(mcx, start_lsn)?;

    while ReadNextXLogRecord(&mut reader, &mut routine)? && reader.v.EndRecPtr <= end_lsn {
        postgres_seams::check_for_interrupts::call()?;

        if !reader.XLogRecHasAnyBlockRefs() {
            continue;
        }
        GetWALBlockInfo(mcx, &mut srf, &mut reader, show_data)?;
    }

    Ok(srf.finish(fcinfo))
}

fn lookup(function: &str) -> Option<PGFunction> {
    Some(match function {
        "pg_get_wal_record_info" => fc_pg_get_wal_record_info,
        "pg_get_wal_records_info" => fc_pg_get_wal_records_info,
        "pg_get_wal_records_info_till_end_of_wal" => fc_pg_get_wal_records_info_till_end_of_wal,
        "pg_get_wal_stats" => fc_pg_get_wal_stats,
        "pg_get_wal_stats_till_end_of_wal" => fc_pg_get_wal_stats_till_end_of_wal,
        "pg_get_wal_block_info" => fc_pg_get_wal_block_info,
        _ => return None,
    })
}

pub fn init_seams() {
    dfmgr::register_builtin_library(dfmgr::BuiltinLibraryEntry {
        name: LIBRARY,
        lookup,
        // pg_walinspect.c's PG_MODULE_MAGIC_EXT has no _PG_init.
        pg_init: None,
    });
}
