//! tsvector_op.c ts_stat support: ts_stat1/ts_stat2 SRFs over an SPI cursor.
//! The C StatEntry binary tree is replaced by a map + one descending sort —
//! same aggregation, same output order (C's in-order walk descends because
//! greater keys go left).

use std::collections::HashMap;

use ::adt_tsvector_core::layout::{ts_compare_string, wep_getweight, TsVec};
use ::datum::Datum;
use ::mcx::Mcx;
use ::types_core::Oid;
use ::types_error::{PgError, PgResult, ERRCODE_INVALID_PARAMETER_VALUE};
use ::types_fmgr::{
    byref_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};
use ::types_tuple::varatt;

pub(crate) const TSVECTOROID: Oid = 3614;
pub(crate) const TEXTOID: Oid = 25;
const INT4OID: Oid = 23;
const RECORDOID: Oid = 2249;

fn text_data(fcinfo: &Fcinfo, i: usize) -> PgResult<&[u8]> {
    // SAFETY: catalog arg type at index `i` is text (strict fns).
    Ok(unsafe { fcinfo.arg_varlena_packed(i) }?.data())
}

// PG_DETOAST_DATUM: full 4B-header image.
pub(crate) fn detoasted_image<'m>(mcx: Mcx<'m>, d: Datum) -> PgResult<&'m [u8]> {
    let p = d.as_usize() as *const u8;
    // SAFETY: non-null varlena datum readable through its header.
    unsafe {
        if varatt::varatt_is_4b_u(p) {
            Ok(core::slice::from_raw_parts(p, varatt::varsize_4b(p)))
        } else if varatt::varatt_is_1b(p) && !varatt::varatt_is_1b_e(p) {
            let src = core::slice::from_raw_parts(
                p.add(varatt::VARHDRSZ_SHORT),
                varatt::varsize_1b(p) - varatt::VARHDRSZ_SHORT,
            );
            let total = 4 + src.len();
            let mut buf = ::mcx::vec_with_capacity_in(mcx, total)?;
            ::mcx::vec_append_bytes(&mut buf, &varatt::set_varsize_4b_word(total as u32).to_ne_bytes())?;
            ::mcx::vec_append_bytes(&mut buf, src)?;
            let out = core::slice::from_raw_parts(buf.as_ptr(), buf.len());
            core::mem::forget(buf);
            Ok(out)
        } else {
            let raw = core::slice::from_raw_parts(p, varatt::varsize_any(p));
            let flat = ::detoast::detoast_attr(mcx, raw)?;
            let out = core::slice::from_raw_parts(flat.as_ptr(), flat.len());
            core::mem::forget(flat);
            Ok(out)
        }
    }
}

#[track_caller]
#[cold]
#[inline(never)]
fn one_tsvector_column() -> Box<PgError> {
    Box::new(
        PgError::error("ts_stat query must return one tsvector column".to_string())
            .with_sqlstate(ERRCODE_INVALID_PARAMETER_VALUE),
    )
}

// C ts_stat_sql weight parsing: single-byte chars map A/B/C/D (either case);
// everything else, including whole multibyte chars, contributes nothing.
fn parse_weight(ws: &[u8]) -> u16 {
    let mut weight = 0u16;
    let mut i = 0usize;
    while i < ws.len() {
        let len = ::mbutils::pg_mblen(&ws[i..]) as usize;
        if len == 1 {
            match ws[i] {
                b'A' | b'a' => weight |= 1 << 3,
                b'B' | b'b' => weight |= 1 << 2,
                b'C' | b'c' => weight |= 1 << 1,
                b'D' | b'd' => weight |= 1,
                _ => {}
            }
        }
        i += len.max(1);
    }
    weight
}

// C ts_accum over one tsvector value.
fn ts_accum(acc: &mut HashMap<Vec<u8>, (i32, i32)>, weight: u16, img: &[u8]) {
    let v = TsVec { payload: &img[4..] };
    for i in 0..v.size() {
        let we = v.entry(i);
        let n = if weight == 0 {
            if we.haspos() {
                v.positions(we).len() as i32
            } else {
                1
            }
        } else if we.haspos() {
            v.positions(we)
                .iter()
                .filter(|&&p| weight & (1 << wep_getweight(p)) != 0)
                .count() as i32
        } else {
            0
        };
        if n == 0 {
            continue;
        }
        let e = acc.entry(v.lexeme(we).to_vec()).or_insert((0, 0));
        e.0 += 1;
        e.1 += n;
    }
}

fn ts_stat_sql(mcx: Mcx<'_>, txt: &[u8], ws: Option<&[u8]>) -> PgResult<Vec<(Vec<u8>, i32, i32)>> {
    let query = core::str::from_utf8(txt).map_err(|_| {
        Box::new(PgError::error("ts_stat query is not valid UTF-8".to_string()))
    })?;
    let plan = ::spi::SPI_prepare(query, &[])?;
    if plan == ::spi::SpiPlanPtr::NULL {
        return Err(Box::new(PgError::error(format!(
            "SPI_prepare(\"{query}\") failed"
        ))));
    }
    let portal = ::spi::SPI_cursor_open(None, plan, &[], &[], true)?;
    ::spi::SPI_cursor_fetch(&portal, true, 100)?;

    // C validates the shape from the first fetch's tuptable, rows or not.
    let ok = match ::spi::SPI_tuptable() {
        None => false,
        Some(h) => ::spi::tuptable_with(h, |t| -> PgResult<bool> {
            Ok(t.tupdesc.natts == 1
                && ::coerce::IsBinaryCoercible(::spi::SPI_gettypeid(&t.tupdesc, 1), TSVECTOROID)?)
        })?,
    };
    if !ok {
        return Err(one_tsvector_column());
    }

    let weight = ws.map_or(0, parse_weight);
    let mut acc: HashMap<Vec<u8>, (i32, i32)> = HashMap::new();

    while ::spi::SPI_processed() > 0 {
        let Some(h) = ::spi::SPI_tuptable() else {
            break;
        };
        ::spi::tuptable_with(h, |t| -> PgResult<()> {
            for tup in t.vals.iter() {
                let (d, isnull) = ::spi::SPI_getbinval(tup, &t.tupdesc, 1);
                if !isnull {
                    let img = detoasted_image(mcx, d)?;
                    ts_accum(&mut acc, weight, img);
                }
            }
            Ok(())
        })?;
        ::spi::SPI_freetuptable(h)?;
        ::spi::SPI_cursor_fetch(&portal, true, 100)?;
    }
    if let Some(h) = ::spi::SPI_tuptable() {
        ::spi::SPI_freetuptable(h)?;
    }
    ::spi::SPI_cursor_close(portal)?;
    ::spi::SPI_freeplan(plan);

    let mut rows: Vec<(Vec<u8>, i32, i32)> = acc
        .into_iter()
        .map(|(k, (ndoc, nentry))| (k, ndoc, nentry))
        .collect();
    rows.sort_unstable_by(|a, b| ts_compare_string(&b.0, &a.0, false).cmp(&0));
    Ok(rows)
}

fn stat_rows(fcinfo: &Fcinfo, ws_arg: bool) -> PgResult<Vec<Vec<u8>>> {
    let mcx = fcinfo.result_mcx();
    ::spi::SPI_connect()?;
    let result = (|| {
        let txt = text_data(fcinfo, 0)?;
        let ws = if ws_arg { Some(text_data(fcinfo, 1)?) } else { None };
        ts_stat_sql(mcx, txt, ws)
    })();
    ::spi::SPI_finish()?;
    let stats = result?;

    let mut desc = ::tupdesc::CreateTemplateTupleDesc(mcx, 3)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 1, Some("word"), TEXTOID, -1, 0)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 2, Some("ndoc"), INT4OID, -1, 0)?;
    ::tupdesc::TupleDescInitEntry(&mut desc, 3, Some("nentry"), INT4OID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    desc.tdtypmod = -1;
    // BlessTupleDesc: consumers of the composite datums (put_composite_row's
    // rowtype lookup, record_out) need the registered typmod.
    ::typcache_seams::assign_record_type_typmod::call(&mut desc)?;

    let mut rows = Vec::with_capacity(stats.len());
    for (word, ndoc, nentry) in stats {
        let w = varlena_result(::varlena::cstring_to_text(mcx, &word)?);
        let tuple = ::heaptuple::heap_form_tuple(
            mcx,
            &desc,
            &[w, Datum::from_i32(ndoc), Datum::from_i32(nentry)],
            &[false, false, false],
        )?;
        rows.push(tuple.image().to_vec());
    }
    Ok(rows)
}

fn srf_drive(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
    name: &'static str,
    collect: impl FnOnce(&Fcinfo) -> PgResult<Vec<Vec<u8>>>,
) -> PgResult<Datum> {
    let flinfo = flinfo.unwrap_or_else(|| panic!("{name}: NULL flinfo"));
    if !flinfo.has_fn_extra() {
        let rows = collect(fcinfo)?;
        let fctx = ::funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = ::funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("SRF rows set at first call")
        .downcast_ref::<Vec<Vec<u8>>>()
        .expect("user_fctx is the SRF row list");
    match rows.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(::funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(::funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

pub fn fc_ts_stat1(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "ts_stat1", |fcinfo| stat_rows(fcinfo, false))
}

pub fn fc_ts_stat2(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    srf_drive(flinfo, fcinfo, "ts_stat2", |fcinfo| stat_rows(fcinfo, true))
}

const fn srf(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: true, func }
}

pub const TS_STAT_BUILTINS: &[FmgrBuiltin] = &[
    srf(3689, "ts_stat1", 1, fc_ts_stat1),
    srf(3690, "ts_stat2", 2, fc_ts_stat2),
    FmgrBuiltin {
        foid: 3752,
        name: "tsvector_update_trigger_byid",
        nargs: 0,
        strict: false,
        retset: false,
        func: trigger::fc_tsvector_update_trigger_byid,
    },
];

pub mod trigger;

#[cfg(test)]
mod tests;
