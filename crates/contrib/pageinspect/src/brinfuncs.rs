//! brinfuncs.c — brin_page_type, brin_page_items, brin_metapage_info,
//! brin_revmap_data.

use crate::*;
use types_brin::{
    BrinPageType, BRIN_PAGETYPE_META, BRIN_PAGETYPE_REGULAR, BRIN_PAGETYPE_REVMAP,
    REVMAP_PAGE_MAXITEMS,
};
use types_core::{BRIN_AM_OID, INT4OID, INT8OID, InvalidOid};
use types_error::ERRCODE_WRONG_OBJECT_TYPE;
use types_rel::pg_class::RELKIND_INDEX;

const BRIN_SPECIAL_SIZE: usize = 8;

fn check_special(b: &[u8]) -> PgResult<()> {
    if page_special_size(b) as usize != BRIN_SPECIAL_SIZE {
        return Err(Box::new(
            PgError::error(format!("input page is not a valid {} page", "BRIN"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!(
                    "Expected special size {}, got {}.",
                    BRIN_SPECIAL_SIZE,
                    page_special_size(b)
                )),
        ));
    }
    Ok(())
}

fn verify_brin_page(page: &RawPage, typ: u16, strtype: &str) -> PgResult<()> {
    let b = page.bytes();
    if page_is_new(b) {
        return Ok(());
    }
    check_special(b)?;
    let got = BrinPageType(&page.page_ref());
    if got != typ {
        return Err(Box::new(
            PgError::error(format!("page is not a BRIN page of type \"{strtype}\""))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!("Expected special type {typ:08x}, got {got:08x}.")),
        ));
    }
    Ok(())
}

pub(crate) fn fc_brin_page_type(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    require_superuser("raw page")?;

    let page = RawPage::arg(fcinfo, 0)?;
    let b = page.bytes();
    if page_is_new(b) {
        return Ok(fcinfo.return_null());
    }
    check_special(b)?;

    let typ = match BrinPageType(&page.page_ref()) {
        BRIN_PAGETYPE_META => "meta".to_string(),
        BRIN_PAGETYPE_REVMAP => "revmap".to_string(),
        BRIN_PAGETYPE_REGULAR => "regular".to_string(),
        other => format!("unknown ({other:02x})"),
    };

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    text_datum(mcx, typ.as_bytes())
}

pub(crate) fn fc_brin_metapage_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("brin_metapage_info: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let page = RawPage::arg(fcinfo, 0)?;
    verify_brin_page(&page, BRIN_PAGETYPE_META, "metapage")?;
    if page_is_new(page.bytes()) {
        return Ok(fcinfo.return_null());
    }

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let meta = types_brin::brin_meta_read(&page.page_ref());
    let values = [
        text_datum(mcx, format!("0x{:08X}", meta.brinMagic).as_bytes())?,
        Datum::from_i32(meta.brinVersion as i32),
        Datum::from_i32(meta.pagesPerRange as i32),
        Datum::from_i64(meta.lastRevmapPage as i64),
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 4])
}

pub(crate) fn fc_brin_revmap_data(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("brin_revmap_data: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let rows = if !flinfo.has_fn_extra() {
        let page = RawPage::arg(fcinfo, 0)?;
        verify_brin_page(&page, BRIN_PAGETYPE_REVMAP, "revmap")?;
        if page_is_new(page.bytes()) {
            return Ok(fcinfo.return_null());
        }
        let mut rows = Vec::with_capacity(REVMAP_PAGE_MAXITEMS);
        for idx in 0..REVMAP_PAGE_MAXITEMS {
            let ip = types_brin::revmap_get_tid(&page.page_ref(), idx);
            rows.push(tid_bytes(ip).to_vec());
        }
        Some(rows)
    } else {
        None
    };
    srf_stream(flinfo, fcinfo, rows)
}

pub(crate) fn fc_brin_page_items(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("brin_page_items: resolved FmgrInfo required");
    require_superuser("raw page")?;

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    // 1.12 added the "empty" column in the middle; refuse older definitions.
    if (srf.tupdesc.natts as usize) < 8 {
        return Err(Box::new(
            PgError::error("function has wrong number of declared columns")
                .with_sqlstate(types_error::ERRCODE_INVALID_FUNCTION_DEFINITION)
                .with_hint(
                    "To resolve the problem, update the \"pageinspect\" extension to the latest version.",
                ),
        ));
    }

    let index_relid = fcinfo.arg(1).as_oid();
    let index_rel = relation::relation_open(mcx, index_relid, types_rel::AccessShareLock)?;
    if index_rel.rd_rel.relkind != RELKIND_INDEX {
        return Err(Box::new(
            PgError::error(format!("\"{}\" is not an index", index_rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    if index_rel.rd_rel.relam != BRIN_AM_OID {
        return Err(Box::new(
            PgError::error(format!("\"{}\" is not a {} index", index_rel.name(), "BRIN"))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    let bdesc = brin::brin_build_desc(mcx, &index_rel)?;

    let page = RawPage::arg(fcinfo, 0)?;
    verify_brin_page(&page, BRIN_PAGETYPE_REGULAR, "regular")?;
    let b = page.bytes();

    if page_is_new(b) {
        index_rel.close(types_rel::AccessShareLock)?;
        // Materialize mode is armed: C's PG_RETURN_NULL yields an empty set.
        return Ok(srf.finish(fcinfo));
    }

    let natts = bdesc.bd_tupdesc.natts as usize;

    // Resolve output functions for every stored type once.
    let mut out_fns: Vec<Vec<FmgrInfo>> = Vec::with_capacity(natts);
    for attno in 1..=natts {
        let col = &bdesc.bd_info[attno - 1];
        let mut fns = Vec::with_capacity(col.oi_nstored as usize);
        for i in 0..col.oi_nstored as usize {
            let (output, _isvarlena) = lsyscache::getTypeOutputInfo(col.oi_typids[i])?;
            fns.push(fmgr_core::fmgr_info(output)?);
        }
        out_fns.push(fns);
    }

    let blknum_typid = srf.tupdesc.attr(1).atttypid;
    let maxoff = page_max_offset_number(b);
    let mut dtup: Option<types_brin::BrinMemTuple> = None;
    let mut offset = 1usize;
    let mut attno = 1usize;
    let mut unused_item = false;

    // C's for(;;) loop bottom-tests `offset > maxoff`, so an empty page
    // still emits one unused-item row for offset 1.
    loop {
            let mut values = [Datum::null(); 8];
            let mut nulls = [false; 8];

            // One iteration per attribute of every tuple on the page; a None
            // dtup signals decoding the next item.
            if dtup.is_none() {
                let item_id = page_item_id(b, offset);
                if item_id.is_used()
                    && item_id.has_storage()
                    && (item_id.off as usize) + (item_id.len as usize) <= b.len()
                {
                    let tuple = &b[item_id.off as usize..(item_id.off + item_id.len) as usize];
                    let mut mem = brin_tuple::brin_new_memtuple(&bdesc);
                    brin_tuple::brin_deform_tuple(&bdesc, tuple, &mut mem)?;
                    dtup = Some(mem);
                    attno = 1;
                    unused_item = false;
                } else {
                    unused_item = true;
                }
            } else {
                attno += 1;
            }

            if unused_item {
                values[0] = Datum::from_i16(offset as i16);
                for n in nulls.iter_mut().skip(1) {
                    *n = true;
                }
            } else {
                let d = dtup.as_ref().expect("decoded tuple");
                let att = attno - 1;
                values[0] = Datum::from_i16(offset as i16);
                values[1] = match blknum_typid {
                    INT8OID => Datum::from_i64(d.bt_blkno as i64),
                    // Old extension versions used int4.
                    INT4OID => Datum::from_i32(d.bt_blkno as i32),
                    _ => return Err(Box::new(PgError::error("incorrect output types"))),
                };
                values[2] = Datum::from_i16(attno as i16);
                values[3] = Datum::from_bool(d.bt_columns[att].bv_allnulls);
                values[4] = Datum::from_bool(d.bt_columns[att].bv_hasnulls);
                values[5] = Datum::from_bool(d.bt_placeholder);
                values[6] = Datum::from_bool(d.bt_empty_range);
                if !d.bt_columns[att].bv_allnulls {
                    let mut s = String::new();
                    s.push('{');
                    for i in 0..out_fns[att].len() {
                        if i > 0 {
                            s.push_str(" .. ");
                        }
                        let val = types_fmgr::function_call1_coll_in(
                            &mut out_fns[att][i],
                            InvalidOid,
                            mcx,
                            d.bt_columns[att].bv_values[i],
                        )?;
                        // SAFETY: output functions return NUL-terminated cstrings.
                        let cs = unsafe {
                            core::ffi::CStr::from_ptr(val.as_usize() as *const core::ffi::c_char)
                        };
                        s.push_str(&String::from_utf8_lossy(cs.to_bytes()));
                    }
                    s.push('}');
                    values[7] = text_datum(mcx, s.as_bytes())?;
                } else {
                    nulls[7] = true;
                }
            }

            srf.putvalues(&values, &nulls)?;

            if unused_item {
                offset += 1;
            } else if attno >= natts {
                dtup = None;
                offset += 1;
            }

        if offset > maxoff {
            break;
        }
    }

    index_rel.close(types_rel::AccessShareLock)?;
    Ok(srf.finish(fcinfo))
}
