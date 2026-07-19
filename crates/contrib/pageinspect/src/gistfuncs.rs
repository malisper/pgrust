//! gistfuncs.c — gist_page_opaque_info, gist_page_items(_bytea).

use crate::*;
use types_core::{GIST_AM_OID, InvalidOid};
use types_error::ERRCODE_WRONG_OBJECT_TYPE;
use types_gist::{GistPageIsDeleted, GIST_PAGE_ID, F_DELETED, F_FOLLOW_RIGHT, F_HAS_GARBAGE, F_LEAF, F_TUPLES_DELETED};
use types_rel::pg_class::RELKIND_INDEX;

const GIST_OPAQUE_SIZE: usize = 16;

fn verify_gist_page(page: &RawPage) -> PgResult<()> {
    let b = page.bytes();
    if page_is_new(b) {
        return Ok(());
    }
    if page_special_size(b) as usize != maxalign(GIST_OPAQUE_SIZE) {
        return Err(Box::new(
            PgError::error(format!("input page is not a valid {} page", "GiST"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!(
                    "Expected special size {}, got {}.",
                    maxalign(GIST_OPAQUE_SIZE),
                    page_special_size(b)
                )),
        ));
    }
    let opaq = types_gist::page_opaque(&page.page_ref());
    if opaq.gist_page_id != GIST_PAGE_ID {
        return Err(Box::new(
            PgError::error(format!("input page is not a valid {} page", "GiST"))
                .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE)
                .with_detail(format!(
                    "Expected {:08x}, got {:08x}.",
                    GIST_PAGE_ID, opaq.gist_page_id
                )),
        ));
    }
    Ok(())
}

pub(crate) fn fc_gist_page_opaque_info(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("gist_page_opaque_info: resolved FmgrInfo required");
    require_superuser("raw page")?;

    let page = RawPage::arg(fcinfo, 0)?;
    verify_gist_page(&page)?;
    let b = page.bytes();
    if page_is_new(b) {
        return Ok(fcinfo.return_null());
    }

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let tupdesc = composite_tupdesc(mcx, flinfo)?;

    let opaq = types_gist::page_opaque(&page.page_ref());

    let mut flags: Vec<Datum> = Vec::new();
    let mut flagbits = opaq.flags;
    for (bit, name) in [
        (F_LEAF, &b"leaf"[..]),
        (F_DELETED, b"deleted"),
        (F_TUPLES_DELETED, b"tuples_deleted"),
        (F_FOLLOW_RIGHT, b"follow_right"),
        (F_HAS_GARBAGE, b"has_garbage"),
    ] {
        if flagbits & bit != 0 {
            flags.push(text_datum(mcx, name)?);
        }
    }
    flagbits &= !(F_LEAF | F_DELETED | F_TUPLES_DELETED | F_FOLLOW_RIGHT | F_HAS_GARBAGE);
    if flagbits != 0 {
        flags.push(text_datum(mcx, format!("{flagbits:x}").as_bytes())?);
    }

    let values = [
        Datum::from_u64(page_lsn(b)),
        Datum::from_u64(opaq.nsn),
        Datum::from_i64(opaq.rightlink as i64),
        text_array_datum(mcx, &flags)?,
    ];
    composite_result(mcx, &tupdesc, &values, &[false; 4])
}

pub(crate) fn fc_gist_page_items_bytea(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("gist_page_items_bytea: resolved FmgrInfo required");
    require_superuser("raw page")?;

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let page = RawPage::arg(fcinfo, 0)?;
    verify_gist_page(&page)?;
    let b = page.bytes();

    if page_is_new(b) {
        return Ok(srf.finish(fcinfo));
    }

    let maxoff = if GistPageIsDeleted(&page.page_ref()) {
        notice("page is deleted")?;
        0
    } else {
        page_max_offset_number(b)
    };

    for offnum in 1..=maxoff {
        let id = page_item_id(b, offnum);
        if !id.is_valid() {
            return Err(Box::new(PgError::error("invalid ItemId")));
        }
        let pos = id.off as usize;
        if pos + 8 > b.len() {
            return Err(Box::new(PgError::error("invalid ItemId")));
        }
        // SAFETY: header read bounded above; tuple copy bounded below.
        let itup = unsafe { b.as_ptr().add(pos) };
        let tuple_len = unsafe { nbtree::itup::index_tuple_size(itup) };
        if pos + tuple_len > b.len() {
            return Err(Box::new(PgError::error("invalid ItemId")));
        }

        let values = [
            Datum::from_i16(offnum as i16),
            tid_datum(mcx, &b[pos..pos + 6])?,
            Datum::from_i16(tuple_len as i16),
            Datum::from_bool(id.is_dead()),
            bytea_datum(mcx, &b[pos..pos + tuple_len])?,
        ];
        srf.putvalues(&values, &[false; 5])?;
    }

    Ok(srf.finish(fcinfo))
}

fn output_fn_text(finfo: &mut FmgrInfo, mcx: Mcx<'_>, val: Datum) -> PgResult<String> {
    let d = types_fmgr::function_call1_coll_in(finfo, InvalidOid, mcx, val)?;
    // SAFETY: type output functions return a NUL-terminated cstring datum.
    let cs = unsafe { core::ffi::CStr::from_ptr(d.as_usize() as *const core::ffi::c_char) };
    Ok(String::from_utf8_lossy(cs.to_bytes()).into_owned())
}

pub(crate) fn fc_gist_page_items(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("gist_page_items: resolved FmgrInfo required");
    require_superuser("raw page")?;

    // SAFETY: the arming context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };
    let mut srf = funcapi::InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;

    let index_relid = fcinfo.arg(1).as_oid();
    let index_rel = relation::relation_open(mcx, index_relid, types_rel::AccessShareLock)?;
    if index_rel.rd_rel.relkind != RELKIND_INDEX {
        // C index_open rejects non-indexes before the AM check.
        return Err(Box::new(
            PgError::error(format!("\"{}\" is not an index", index_rel.name()))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }
    if index_rel.rd_rel.relam != GIST_AM_OID {
        return Err(Box::new(
            PgError::error(format!("\"{}\" is not a {} index", index_rel.name(), "GiST"))
                .with_sqlstate(ERRCODE_WRONG_OBJECT_TYPE),
        ));
    }

    let page = RawPage::arg(fcinfo, 0)?;
    verify_gist_page(&page)?;
    let b = page.bytes();

    if page_is_new(b) {
        index_rel.close(types_rel::AccessShareLock)?;
        return Ok(srf.finish(fcinfo));
    }

    let flagbits = types_gist::page_opaque(&page.page_ref()).flags;

    // Leaf pages carry included attributes; non-leaf pages only key ones.
    let nkeyatts = index_rel.indnkeyatts();
    let (tupdesc, keys_only) = if flagbits & F_LEAF != 0 {
        (tupdesc::CreateTupleDescCopy(mcx, index_rel.rd_att.as_ref())?, false)
    } else {
        (
            tupdesc::CreateTupleDescTruncatedCopy(mcx, index_rel.rd_att.as_ref(), nkeyatts)?,
            true,
        )
    };

    // pg_get_indexdef_columns_extended(indexRelid, RULE_INDEXDEF_PRETTY [|KEYS_ONLY]).
    let index_columns = ruleutils::pg_get_indexdef_worker(
        mcx,
        index_relid,
        0,
        None,
        true,
        keys_only,
        false,
        false,
        ruleutils::PRETTYFLAG_PAREN | ruleutils::PRETTYFLAG_INDENT | ruleutils::PRETTYFLAG_SCHEMA,
        false,
    )?;

    let natts = tupdesc.natts as usize;
    let mut out_fns: Vec<Option<FmgrInfo>> = Vec::with_capacity(natts);
    for i in 0..natts {
        let (foutoid, _isvarlena) = lsyscache::getTypeOutputInfo(tupdesc.attr(i).atttypid)?;
        out_fns.push(Some(fmgr_core::fmgr_info(foutoid)?));
    }

    let maxoff = if GistPageIsDeleted(&page.page_ref()) {
        notice("page is deleted")?;
        0
    } else {
        page_max_offset_number(b)
    };

    for offnum in 1..=maxoff {
        let id = page_item_id(b, offnum);
        if !id.is_valid() {
            return Err(Box::new(PgError::error("invalid ItemId")));
        }
        let pos = id.off as usize;
        if pos + 8 > b.len() {
            return Err(Box::new(PgError::error("invalid ItemId")));
        }
        // SAFETY: header read bounded above; attribute reads stay inside the
        // tuple, whose length is bounded below.
        let itup = unsafe { b.as_ptr().add(pos) };
        let tuple_len = unsafe { nbtree::itup::index_tuple_size(itup) };
        if pos + tuple_len > b.len() {
            return Err(Box::new(PgError::error("invalid ItemId")));
        }

        let mut values = [Datum::null(); 5];
        let mut nulls = [false; 5];
        values[0] = Datum::from_i16(offnum as i16);
        values[1] = tid_datum(mcx, &b[pos..pos + 6])?;
        values[2] = Datum::from_i32(tuple_len as i32);
        values[3] = Datum::from_bool(id.is_dead());

        if let Some(ref index_columns) = index_columns {
            let mut buf = String::new();
            buf.push('(');
            buf.push_str(index_columns);
            buf.push_str(")=(");

            // Mostly copied from record_out().
            for i in 0..natts {
                let mut isnull = false;
                // SAFETY: itup points at a tuple_len-byte in-bounds image.
                let val_datum =
                    unsafe { nbtree::itup::index_getattr(itup, (i + 1) as i16, &tupdesc, &mut isnull) };
                let value = if isnull {
                    "null".to_string()
                } else {
                    output_fn_text(out_fns[i].as_mut().expect("resolved"), mcx, val_datum)?
                };

                if i == nkeyatts as usize {
                    buf.push_str(") INCLUDE (");
                } else if i > 0 {
                    buf.push_str(", ");
                }

                let mut nq = value.is_empty();
                for ch in value.chars() {
                    if ch == '"'
                        || ch == '\\'
                        || ch == '('
                        || ch == ')'
                        || ch == ','
                        || ch.is_ascii_whitespace()
                    {
                        nq = true;
                        break;
                    }
                }
                if nq {
                    buf.push('"');
                }
                for ch in value.chars() {
                    if ch == '"' || ch == '\\' {
                        buf.push(ch);
                    }
                    buf.push(ch);
                }
                if nq {
                    buf.push('"');
                }
            }
            buf.push(')');
            values[4] = text_datum(mcx, buf.as_bytes())?;
        } else {
            nulls[4] = true;
        }

        srf.putvalues(&values, &nulls)?;
    }

    index_rel.close(types_rel::AccessShareLock)?;
    Ok(srf.finish(fcinfo))
}
