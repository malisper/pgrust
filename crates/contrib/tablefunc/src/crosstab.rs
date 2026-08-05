//! `tablefunc.c` crosstab (positional) + crosstab_hash (categories query).

use datum::Datum;
use funcapi::{InitMaterializedSRF, MaterializedSRF, MAT_SRF_USE_EXPECTED_DESC};
use mcx::Mcx;
use rustc_hash::FxHashMap;
use types_error::{
    PgError, PgResult, ERRCODE_CARDINALITY_VIOLATION, ERRCODE_DATATYPE_MISMATCH,
    ERRCODE_INVALID_PARAMETER_VALUE, ERRCODE_NULL_VALUE_NOT_ALLOWED,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_tuple::TupleDescData;

use crate::tupbuild::AttInMetadata;

#[track_caller]
#[cold]
fn err(msg: &str, detail: &str, sqlstate: types_error::SqlState) -> Box<PgError> {
    let mut e = PgError::error(msg.to_string()).with_sqlstate(sqlstate);
    if !detail.is_empty() {
        e = e.with_detail(detail.to_string());
    }
    Box::new(e)
}

fn arg_sql(fcinfo: &Fcinfo, i: usize) -> PgResult<String> {
    // SAFETY: catalog args are non-null text varlenas (STRICT fns). Copied to
    // owned so the immutable arg borrow doesn't block the &mut fcinfo below.
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(core::str::from_utf8(v.data()).expect("SQL text arg is valid UTF-8").to_string())
}

// compatCrosstabTupleDescs: ret[0] must match sql[0]; ret[1..] must match sql[2].
fn compat_crosstab_tupdescs(
    ret: &TupleDescData<'_>,
    sql: &TupleDescData<'_>,
) -> PgResult<()> {
    if ret.natts < 2 {
        return Err(err(
            "invalid crosstab return type",
            "Return row must have at least two columns.",
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    debug_assert_eq!(sql.natts, 3);

    let ra0 = &ret.attrs[0];
    let sa0 = &sql.attrs[0];
    if ra0.atttypid != sa0.atttypid || (ra0.atttypmod >= 0 && ra0.atttypmod != sa0.atttypmod) {
        return Err(err(
            "invalid crosstab return type",
            &format!(
                "Source row_name datatype {} does not match return row_name datatype {}.",
                format_type::format_type_with_typemod(sa0.atttypid, sa0.atttypmod)?,
                format_type::format_type_with_typemod(ra0.atttypid, ra0.atttypmod)?,
            ),
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }

    let sval_typid = sql.attrs[2].atttypid;
    let sval_typmod = sql.attrs[2].atttypmod;
    for i in 1..ret.natts as usize {
        let ra = &ret.attrs[i];
        if ra.atttypid != sval_typid || (ra.atttypmod >= 0 && ra.atttypmod != sval_typmod) {
            return Err(err(
                "invalid crosstab return type",
                &format!(
                    "Source value datatype {} does not match return value datatype {} in column {}.",
                    format_type::format_type_with_typemod(sval_typid, sval_typmod)?,
                    format_type::format_type_with_typemod(ra.atttypid, ra.atttypmod)?,
                    i + 1,
                ),
                ERRCODE_DATATYPE_MISMATCH,
            ));
        }
    }
    Ok(())
}

pub(crate) fn fc_crosstab(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("crosstab: NULL flinfo");
    let sql = arg_sql(fcinfo, 0)?;

    // SAFETY: the arming (per-query) context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    spi::SPI_connect()?;
    let ret = spi::SPI_execute(&sql, true, 0)?;
    let proc = spi::SPI_processed();

    if ret != spi::SPI_OK_SELECT || proc == 0 {
        spi::SPI_finish()?;
        if let Some(rsi) = fcinfo.rsinfo_mut() {
            rsi.isDone = types_fmgr::ExprDoneCond::ExprEndResult;
        }
        fcinfo.isnull = true;
        return Ok(Datum::null());
    }

    // InitMaterializedSRF resolves the composite/record result tupdesc
    // (get_call_result_type) and begins the tuplestore; do it here so the
    // "return type must be a row type" leg fires like C's get_call_result_type.
    let mut srf = InitMaterializedSRF(mcx, flinfo, fcinfo, 0)?;
    let num_categories = srf.tupdesc.natts as usize - 1;

    let h = spi::SPI_tuptable().expect("SELECT leaves a tuptable");
    let result = spi::tuptable_with(h, |t| -> PgResult<()> {
        if t.tupdesc.natts != 3 {
            return Err(err(
                "invalid crosstab source data query",
                "The query must return 3 columns: row_name, category, and value.",
                ERRCODE_INVALID_PARAMETER_VALUE,
            ));
        }
        compat_crosstab_tupdescs(&srf.tupdesc, &t.tupdesc)?;
        let mut attinmeta = AttInMetadata::new(&srf.tupdesc)?;

        let max_calls = proc;
        let mut firstpass = true;
        let mut lastrowid: Option<Vec<u8>> = None;
        let mut call_cntr: u64 = 0;

        while call_cntr < max_calls {
            let mut skip_tuple = false;
            // values[0] = rowid, values[1..] = one per category (None = NULL).
            let mut values: Vec<Option<Vec<u8>>> = vec![None; 1 + num_categories];

            let mut i = 0;
            while i < num_categories {
                if call_cntr >= max_calls {
                    break;
                }
                let spi_tuple = &t.vals[call_cntr as usize];
                let rowid = spi::SPI_getvalue(mcx, spi_tuple, &t.tupdesc, 1)?;

                if i == 0 {
                    values[0] = rowid.map(|s| s.to_vec());
                    if !firstpass && xstreq(lastrowid.as_deref(), rowid) {
                        skip_tuple = true;
                        break;
                    }
                }

                if xstreq(values[0].as_deref(), rowid) {
                    values[1 + i] = spi::SPI_getvalue(mcx, spi_tuple, &t.tupdesc, 3)?
                        .map(|s| s.to_vec());
                    if i < num_categories - 1 {
                        call_cntr += 1;
                    }
                } else {
                    call_cntr -= 1;
                    break;
                }
                i += 1;
            }

            if !skip_tuple {
                let val_refs: Vec<Option<&[u8]>> =
                    values.iter().map(|v| v.as_deref()).collect();
                let (d, n) = attinmeta.build(mcx, &val_refs)?;
                srf.putvalues(&d, &n)?;
            }

            lastrowid = values[0].clone();
            firstpass = false;
            call_cntr += 1;
        }
        Ok(())
    });
    result?;

    spi::SPI_finish()?;
    Ok(srf.finish(fcinfo))
}

fn xstreq(a: Option<&[u8]>, b: Option<&[u8]>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => x == y,
        _ => false,
    }
}

// ===========================================================================
// crosstab_hash — categories loaded from a second query into a hash table.
// ===========================================================================

const MAX_CATNAME_LEN: usize = 64; // NAMEDATALEN

fn cat_key(name: &[u8]) -> [u8; MAX_CATNAME_LEN] {
    // C snprintf(key, MAX_CATNAME_LEN - 1, "%s", catname): NUL-padded,
    // truncated to 62 payload bytes.
    let mut key = [0u8; MAX_CATNAME_LEN];
    let n = name.len().min(MAX_CATNAME_LEN - 2);
    key[..n].copy_from_slice(&name[..n]);
    key
}

fn load_categories_hash(
    mcx: Mcx<'_>,
    cats_sql: &str,
) -> PgResult<FxHashMap<[u8; MAX_CATNAME_LEN], usize>> {
    let mut hash: FxHashMap<[u8; MAX_CATNAME_LEN], usize> = FxHashMap::default();

    spi::SPI_connect()?;
    let ret = spi::SPI_execute(cats_sql, true, 0)?;
    let proc = spi::SPI_processed();

    if ret == spi::SPI_OK_SELECT && proc > 0 {
        let h = spi::SPI_tuptable().expect("SELECT leaves a tuptable");
        let r = spi::tuptable_with(h, |t| -> PgResult<()> {
            if t.tupdesc.natts != 1 {
                return Err(err(
                    "invalid crosstab categories query",
                    "The query must return one column.",
                    ERRCODE_INVALID_PARAMETER_VALUE,
                ));
            }
            for i in 0..proc as usize {
                let catname = spi::SPI_getvalue(mcx, &t.vals[i], &t.tupdesc, 1)?;
                let Some(catname) = catname else {
                    return Err(err(
                        "crosstab category value must not be null",
                        "",
                        ERRCODE_NULL_VALUE_NOT_ALLOWED,
                    ));
                };
                let key = cat_key(catname);
                if hash.insert(key, i).is_some() {
                    return Err(err("duplicate category name", "", types_error::ERRCODE_DUPLICATE_OBJECT));
                }
            }
            Ok(())
        });
        r?;
    }

    if spi::SPI_finish()? != spi::SPI_OK_FINISH {
        panic!("load_categories_hash: SPI_finish() failed");
    }
    Ok(hash)
}

pub(crate) fn fc_crosstab_hash(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let flinfo = flinfo.expect("crosstab_hash: NULL flinfo");
    let sql = arg_sql(fcinfo, 0)?;
    let cats_sql = arg_sql(fcinfo, 1)?;

    // SAFETY: the arming (per-query) context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    // MAT_SRF_USE_EXPECTED_DESC: crosstab_hash uses rsinfo->expectedDesc
    // directly (requires a column-definition list) and validates natts>=2.
    let mut srf = InitMaterializedSRF(mcx, flinfo, fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;
    if srf.tupdesc.natts < 2 {
        return Err(err(
            "invalid crosstab return type",
            "Return row must have at least two columns.",
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }

    let crosstab_hash = load_categories_hash(mcx, &cats_sql)?;
    get_crosstab_tuplestore(mcx, &sql, &crosstab_hash, &mut srf)?;
    Ok(srf.finish(fcinfo))
}

fn get_crosstab_tuplestore(
    mcx: Mcx<'_>,
    sql: &str,
    crosstab_hash: &FxHashMap<[u8; MAX_CATNAME_LEN], usize>,
    srf: &mut MaterializedSRF<'_>,
) -> PgResult<()> {
    let num_categories = crosstab_hash.len();
    let mut attinmeta = AttInMetadata::new(&srf.tupdesc)?;
    let tupdesc_natts = srf.tupdesc.natts as usize;

    spi::SPI_connect()?;
    let ret = spi::SPI_execute(sql, true, 0)?;
    let proc = spi::SPI_processed();

    if ret == spi::SPI_OK_SELECT && proc > 0 {
        let h = spi::SPI_tuptable().expect("SELECT leaves a tuptable");
        let r = spi::tuptable_with(h, |t| -> PgResult<()> {
            let ncols = t.tupdesc.natts as usize;
            if num_categories == 0 {
                return Err(err(
                    "crosstab categories query must return at least one row",
                    "",
                    ERRCODE_CARDINALITY_VIOLATION,
                ));
            }
            if ncols < 3 {
                return Err(err(
                    "invalid crosstab source data query",
                    "The query must return at least 3 columns: row_name, category, and value.",
                    ERRCODE_INVALID_PARAMETER_VALUE,
                ));
            }
            let result_ncols = (ncols - 2) + num_categories;
            if tupdesc_natts != result_ncols {
                return Err(err(
                    "invalid crosstab return type",
                    &format!(
                        "Return row must have {result_ncols} columns, not {tupdesc_natts}."
                    ),
                    ERRCODE_DATATYPE_MISMATCH,
                ));
            }

            let mut values: Vec<Option<Vec<u8>>> = vec![None; result_ncols];
            let mut lastrowid: Option<Vec<u8>> = None;
            let mut firstpass = true;

            for i in 0..proc as usize {
                let spi_tuple = &t.vals[i];
                let rowid = spi::SPI_getvalue(mcx, spi_tuple, &t.tupdesc, 1)?
                    .map(|s| s.to_vec());

                if firstpass || !xstreq(lastrowid.as_deref(), rowid.as_deref()) {
                    if !firstpass {
                        flush_row(mcx, &mut attinmeta, &values, srf)?;
                        for v in values.iter_mut() {
                            *v = None;
                        }
                    }
                    values[0] = rowid.clone();
                    for j in 1..ncols - 2 {
                        values[j] = spi::SPI_getvalue(mcx, spi_tuple, &t.tupdesc, (j + 1) as i32)?
                            .map(|s| s.to_vec());
                    }
                    firstpass = false;
                }

                let catname = spi::SPI_getvalue(mcx, spi_tuple, &t.tupdesc, (ncols - 1) as i32)?;
                if let Some(catname) = catname {
                    if let Some(&attidx) = crosstab_hash.get(&cat_key(catname)) {
                        values[attidx + ncols - 2] =
                            spi::SPI_getvalue(mcx, spi_tuple, &t.tupdesc, ncols as i32)?
                                .map(|s| s.to_vec());
                    }
                }

                lastrowid = rowid;
            }

            flush_row(mcx, &mut attinmeta, &values, srf)?;
            Ok(())
        });
        r?;
    }

    if spi::SPI_finish()? != spi::SPI_OK_FINISH {
        panic!("get_crosstab_tuplestore: SPI_finish() failed");
    }
    Ok(())
}

fn flush_row(
    mcx: Mcx<'_>,
    attinmeta: &mut AttInMetadata,
    values: &[Option<Vec<u8>>],
    srf: &mut MaterializedSRF<'_>,
) -> PgResult<()> {
    let val_refs: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
    let (d, n) = attinmeta.build(mcx, &val_refs)?;
    srf.putvalues(&d, &n)
}
