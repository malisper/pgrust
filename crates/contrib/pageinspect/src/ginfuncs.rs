//! ginfuncs.c — gin_metapage_info, gin_page_opaque_info, gin_leafpage_items.

use crate::*;
use gin_vocab::{
    GIN_COMPRESSED, GIN_DATA, GIN_DELETED, GIN_INCOMPLETE_SPLIT, GIN_LEAF, GIN_LIST,
    GIN_LIST_FULLROW, GIN_META,
};

const GIN_OPAQUE_SIZE: usize = 8;

struct GinOpaque {
    rightlink: u32,
    maxoff: u16,
    flags: u16,
}

fn gin_opaque(b: &[u8]) -> GinOpaque {
    let sp = pd_special(b) as usize;
    if sp + GIN_OPAQUE_SIZE > b.len() {
        return GinOpaque { rightlink: 0, maxoff: 0, flags: 0 };
    }
    GinOpaque {
        rightlink: r_u32(b, sp),
        maxoff: r_u16(b, sp + 4),
        flags: r_u16(b, sp + 6),
    }
}

fn check_special(b: &[u8], what: &str) -> PgResult<()> {
    if page_special_size(b) as usize != maxalign(GIN_OPAQUE_SIZE) {
        return Err(Box::new(
            PgError::error(format!("input page is not a valid {what}"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!(
                    "Expected special size {}, got {}.",
                    maxalign(GIN_OPAQUE_SIZE),
                    page_special_size(b)
                )),
        ));
    }
    Ok(())
}

pub(crate) fn fc_gin_metapage_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("gin_metapage_info: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let page = RawPage::arg(fcinfo, 0)?;
    let b = page.bytes();
    if page_is_new(b) {
        return Ok(fcinfo.return_null());
    }
    check_special(b, "GIN metapage")?;

    let opaq = gin_opaque(b);
    if opaq.flags != GIN_META {
        return Err(Box::new(
            PgError::error("input page is not a GIN metapage")
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!("Flags {:04X}, expected {:04X}", opaq.flags, GIN_META)),
        ));
    }

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    // GinMetaPageData at PageGetContents.
    let m = SizeOfPageHeaderData;
    let values = [
        Datum::from_i64(r_u32(b, m) as i64),      // head
        Datum::from_i64(r_u32(b, m + 4) as i64),  // tail
        Datum::from_i32(r_u32(b, m + 8) as i32),  // tailFreeSize
        Datum::from_i64(r_u32(b, m + 12) as i64), // nPendingPages
        Datum::from_i64(r_u64(b, m + 16) as i64), // nPendingHeapTuples
        Datum::from_i64(r_u32(b, m + 24) as i64), // nTotalPages
        Datum::from_i64(r_u32(b, m + 28) as i64), // nEntryPages
        Datum::from_i64(r_u32(b, m + 32) as i64), // nDataPages
        Datum::from_i64(r_u64(b, m + 40) as i64), // nEntries
        Datum::from_i32(r_u32(b, m + 48) as i32), // ginVersion
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 10])
}

pub(crate) fn fc_gin_page_opaque_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("gin_page_opaque_info: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let page = RawPage::arg(fcinfo, 0)?;
    let b = page.bytes();
    if page_is_new(b) {
        return Ok(fcinfo.return_null());
    }
    check_special(b, "GIN data leaf page")?;

    let opaq = gin_opaque(b);

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let mut flags: Vec<Datum> = Vec::new();
    let mut flagbits = opaq.flags;
    for (bit, name) in [
        (GIN_DATA, &b"data"[..]),
        (GIN_LEAF, b"leaf"),
        (GIN_DELETED, b"deleted"),
        (GIN_META, b"meta"),
        (GIN_LIST, b"list"),
        (GIN_LIST_FULLROW, b"list_fullrow"),
        (GIN_INCOMPLETE_SPLIT, b"incomplete_split"),
        (GIN_COMPRESSED, b"compressed"),
    ] {
        if flagbits & bit != 0 {
            flags.push(text_datum(mcx, name)?);
        }
    }
    flagbits &= !(GIN_DATA
        | GIN_LEAF
        | GIN_DELETED
        | GIN_META
        | GIN_LIST
        | GIN_LIST_FULLROW
        | GIN_INCOMPLETE_SPLIT
        | GIN_COMPRESSED);
    if flagbits != 0 {
        // Unrecognized flags print in hex (to_hex32).
        flags.push(text_datum(mcx, format!("{flagbits:x}").as_bytes())?);
    }

    let values = [
        Datum::from_i64(opaq.rightlink as i64),
        Datum::from_i32(opaq.maxoff as i32),
        text_array_datum(mcx, &flags)?,
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 3])
}

pub(crate) fn fc_gin_leafpage_items(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("gin_leafpage_items: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let rows = if !flinfo.has_fn_extra() {
        // SAFETY: the arming context outlives this call.
        let mcx = unsafe { fcinfo.result_mcx_detached() };
        let page = RawPage::arg(fcinfo, 0)?;
        let b = page.bytes();
        if page_is_new(b) {
            return Ok(fcinfo.return_null());
        }
        check_special(b, "GIN data leaf page")?;

        let opaq = gin_opaque(b);
        if opaq.flags != (GIN_DATA | GIN_LEAF | GIN_COMPRESSED) {
            return Err(Box::new(
                PgError::error("input page is not a compressed GIN data leaf page")
                    .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                    .with_detail(format!(
                        "Flags {:04X}, expected {:04X}",
                        opaq.flags,
                        GIN_DATA | GIN_LEAF | GIN_COMPRESSED
                    )),
            ));
        }

        let tupdesc = composite_tupdesc(mcx, flinfo)?;

        // GinDataLeafPageGetPostingList / ...Size.
        let start = SizeOfPageHeaderData + maxalign(6);
        let size = (pd_lower(b) as usize)
            .saturating_sub(maxalign(SizeOfPageHeaderData))
            .saturating_sub(maxalign(6));
        let end = (start + size).min(b.len());

        let mut rows = Vec::new();
        let mut segoff = start;
        while segoff + gin_vocab::SizeOfGinPostingListHeader <= end {
            let nbytes = gin::postinglist::seg_nbytes(&b[segoff..]);
            let segsize = gin_vocab::size_of_gin_posting_list(nbytes);
            if segoff + segsize > end {
                break;
            }
            let seg = &b[segoff..segoff + segsize];

            let first = gin::postinglist::seg_first(seg);
            let mut decoded = mcx::vec_new_in::<types_tuple::itemptr::ItemPointerData>(mcx);
            gin::postinglist::ginPostingListDecodeAllSegments(seg, &mut decoded)?;
            let mut tids = Vec::with_capacity(decoded.len());
            for ip in decoded.iter() {
                tids.push(tid_datum(mcx, &tid_bytes(*ip))?);
            }

            let values = [
                tid_datum(mcx, &tid_bytes(first))?,
                Datum::from_i16(nbytes as i16),
                tid_array_datum(mcx, &tids)?,
            ];
            rows.push(tuple_image(mcx, &tupdesc, &values, &[false; 3])?);

            segoff += segsize;
        }
        Some(rows)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, rows)
}
