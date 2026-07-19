//! rawpage.c — get_raw_page family, page_header, page_checksum.

use crate::*;
use types_core::{BlockNumber, ForkNumber, MaxBlockNumber, INT2OID, INT4OID};
use types_error::ERRCODE_WRONG_OBJECT_TYPE;
use types_rel::pg_class::RELKIND_HAS_STORAGE;

fn forkname_to_number(name: &str) -> PgResult<ForkNumber> {
    Ok(match name {
        "main" => ForkNumber::MAIN_FORKNUM,
        "fsm" => ForkNumber::FSM_FORKNUM,
        "vm" => ForkNumber::VISIBILITYMAP_FORKNUM,
        "init" => ForkNumber::INIT_FORKNUM,
        _ => {
            return Err(Box::new(
                PgError::error("invalid fork name")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_hint("Valid fork names are \"main\", \"fsm\", \"vm\", and \"init\"."),
            ))
        }
    })
}

fn get_raw_page_internal(
    fcinfo: &mut Fcinfo,
    relname_arg: usize,
    forknum: ForkNumber,
    blkno: BlockNumber,
) -> PgResult<Datum> {
    require_superuser("raw page")?;

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let rel = relation_open_by_text_arg(mcx, fcinfo, relname_arg, types_rel::AccessShareLock)?;

    if !RELKIND_HAS_STORAGE(rel.rd_rel.relkind) {
        let detail = pg_class_seams::errdetail_relkind_not_supported::call(rel.rd_rel.relkind)?;
        return Err(Box::new(
            PgError::error(format!("cannot get raw page from relation \"{}\"", rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE)
                .with_detail(detail),
        ));
    }

    if relation_is_other_temp(&rel) {
        return Err(other_temp_err());
    }

    if blkno >= bufmgr::RelationGetNumberOfBlocksInFork(&rel, forknum)? {
        return Err(param_err(format!(
            "block number {} is out of range for relation \"{}\"",
            blkno,
            rel.name()
        )));
    }

    let buf = bufmgr::ReadBufferExtended(
        &rel,
        forknum,
        blkno,
        types_storage::storage::ReadBufferMode::Normal,
        None,
    )?;
    bufmgr::LockBuffer(buf, bufmgr::BUFFER_LOCK_SHARE)?;
    let ptr = bufmgr::BufferGetPagePtr(buf);
    let mut image = vec![0u8; BLCKSZ];
    // SAFETY: a locked, pinned buffer page is BLCKSZ readable.
    unsafe {
        core::ptr::copy_nonoverlapping(ptr.as_ptr(), image.as_mut_ptr(), BLCKSZ);
    }
    bufmgr::UnlockReleaseBuffer(buf)?;

    rel.close(types_rel::AccessShareLock)?;

    bytea_datum(mcx, &image)
}

pub(crate) fn fc_get_raw_page_1_9(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let blkno = fcinfo.arg(1).as_i64();
    if blkno < 0 || blkno > MaxBlockNumber as i64 {
        return Err(param_err("invalid block number"));
    }
    get_raw_page_internal(fcinfo, 0, ForkNumber::MAIN_FORKNUM, blkno as BlockNumber)
}

pub(crate) fn fc_get_raw_page(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    // Early 8.4 betas redefined get_raw_page() with three arguments.
    if fcinfo.nargs() != 2 {
        return Err(Box::new(
            PgError::error("wrong number of arguments to get_raw_page()")
                .with_hint("Run the updated pageinspect.sql script."),
        ));
    }
    let blkno = fcinfo.arg(1).as_i32() as u32;
    get_raw_page_internal(fcinfo, 0, ForkNumber::MAIN_FORKNUM, blkno)
}

pub(crate) fn fc_get_raw_page_fork_1_9(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let forkname = {
        // SAFETY: STRICT text arg.
        let v = unsafe { fcinfo.arg_varlena_packed(1)? };
        String::from_utf8_lossy(v.data()).into_owned()
    };
    let forknum = forkname_to_number(&forkname)?;
    let blkno = fcinfo.arg(2).as_i64();
    if blkno < 0 || blkno > MaxBlockNumber as i64 {
        return Err(param_err("invalid block number"));
    }
    get_raw_page_internal(fcinfo, 0, forknum, blkno as BlockNumber)
}

pub(crate) fn fc_get_raw_page_fork(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let forkname = {
        // SAFETY: STRICT text arg.
        let v = unsafe { fcinfo.arg_varlena_packed(1)? };
        String::from_utf8_lossy(v.data()).into_owned()
    };
    let forknum = forkname_to_number(&forkname)?;
    let blkno = fcinfo.arg(2).as_i32() as u32;
    get_raw_page_internal(fcinfo, 0, forknum, blkno)
}

pub(crate) fn fc_page_header(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("page_header: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let page = RawPage::arg(fcinfo, 0)?;
    let b = page.bytes();

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let mut values = [Datum::null(); 9];
    let nulls = [false; 9];

    let lsn = page_lsn(b);
    // pageinspect >= 1.2 uses pg_lsn instead of text for the LSN field.
    if tupdesc.attr(0).atttypid == TEXTOID {
        let s = format!("{:X}/{:X}", (lsn >> 32) as u32, lsn as u32);
        values[0] = text_datum(mcx, s.as_bytes())?;
    } else {
        values[0] = Datum::from_u64(lsn);
    }
    values[1] = Datum::from_i16(pd_checksum(b) as i16);
    values[2] = Datum::from_i16(pd_flags(b) as i16);

    // pageinspect >= 1.10 uses int4 instead of int2 for those fields.
    match tupdesc.attr(3).atttypid {
        INT2OID => {
            values[3] = Datum::from_i16(pd_lower(b) as i16);
            values[4] = Datum::from_i16(pd_upper(b) as i16);
            values[5] = Datum::from_i16(pd_special(b) as i16);
            values[6] = Datum::from_i16(page_size(b) as i16);
        }
        INT4OID => {
            values[3] = Datum::from_i32(pd_lower(b) as i32);
            values[4] = Datum::from_i32(pd_upper(b) as i32);
            values[5] = Datum::from_i32(pd_special(b) as i32);
            values[6] = Datum::from_i32(page_size(b) as i32);
        }
        _ => return Err(Box::new(PgError::error("incorrect output types"))),
    }

    values[7] = Datum::from_i16(page_layout_version(b) as i16);
    values[8] = Datum::from_transaction_id(pd_prune_xid(b));

    composite_result(mcx, &tupdesc, &values, &nulls)
}

// pg_checksum_page (storage/checksum_impl.h): FNV-1a-derived block checksum
// with pd_checksum treated as zero.
pub(crate) fn pg_checksum_page(page: &[u8], blkno: u32) -> u16 {
    const N_SUMS: usize = 32;
    const FNV_PRIME: u32 = 16777619;
    const CHECKSUM_BASE_OFFSETS: [u32; N_SUMS] = [
        0x5B1F36E9, 0xB8525960, 0x02AB50AA, 0x1DE66D2A, 0x79FF467A, 0x9BB9F8A3, 0x217E7CD2,
        0x83E13D2C, 0xF8D4474F, 0xE39EB970, 0x42C6AE16, 0x993216FA, 0x7B093B5D, 0x98DAFF3C,
        0xF718902A, 0x0B1C9CDB, 0xE58F764B, 0x187636BC, 0x5D7B3BB1, 0xE73DE7DE, 0x92BEC979,
        0xCCA6C0B2, 0x304A0979, 0x85AA43D4, 0x783125BB, 0x6CA8EAA2, 0xE407EAC6, 0x4B5CFC3E,
        0x9FBF8C76, 0x15CA20BE, 0xF2CA9FD3, 0x959BD756,
    ];
    #[inline(always)]
    fn comp(sum: &mut u32, value: u32) {
        let tmp = *sum ^ value;
        *sum = tmp.wrapping_mul(FNV_PRIME) ^ (tmp >> 17);
    }
    debug_assert_eq!(page.len(), BLCKSZ);
    let mut sums = CHECKSUM_BASE_OFFSETS;
    let rows = BLCKSZ / (4 * N_SUMS);
    for row in 0..rows {
        for (lane, sum) in sums.iter_mut().enumerate() {
            let off = (row * N_SUMS + lane) * 4;
            // pd_checksum (bytes 8..10) is computed as zero.
            let v = if off == 8 {
                u32::from_ne_bytes([0, 0, page[10], page[11]])
            } else {
                u32::from_ne_bytes(page[off..off + 4].try_into().unwrap())
            };
            comp(sum, v);
        }
    }
    for _ in 0..2 {
        for sum in sums.iter_mut() {
            comp(sum, 0);
        }
    }
    let mut checksum: u32 = sums.into_iter().fold(0, |a, s| a ^ s);
    checksum ^= blkno;
    (checksum % 65535 + 1) as u16
}

fn page_checksum_internal(fcinfo: &mut Fcinfo, version: PiVersion) -> PgResult<Datum> {
    require_superuser("raw page")?;

    let blkno: i64 = if version == PiVersion::V1_8 {
        fcinfo.arg(1).as_i32() as u32 as i64
    } else {
        fcinfo.arg(1).as_i64()
    };
    if blkno < 0 || blkno > MaxBlockNumber as i64 {
        return Err(param_err("invalid block number"));
    }

    let page = RawPage::arg(fcinfo, 0)?;
    if page_is_new(page.bytes()) {
        return Ok(fcinfo.return_null());
    }

    Ok(Datum::from_i16(
        pg_checksum_page(page.bytes(), blkno as u32) as i16
    ))
}

pub(crate) fn fc_page_checksum_1_9(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    page_checksum_internal(fcinfo, PiVersion::V1_9)
}

pub(crate) fn fc_page_checksum(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    page_checksum_internal(fcinfo, PiVersion::V1_8)
}
