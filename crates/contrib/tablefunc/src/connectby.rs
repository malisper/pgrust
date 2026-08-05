//! `tablefunc.c` connectby_text / connectby_text_serial — recursive walk of a
//! parent/child table with cycle detection.

use datum::Datum;
use funcapi::{InitMaterializedSRF, MaterializedSRF, MAT_SRF_USE_EXPECTED_DESC};
use mcx::Mcx;
use types_core::{INT4OID, TEXTOID};
use types_error::{
    PgError, PgResult, ERRCODE_DATATYPE_MISMATCH, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_INVALID_RECURSION,
};
use types_fmgr::{FmgrInfo, FunctionCallInfoBaseData as Fcinfo};
use types_tuple::TupleDescData;

use crate::tupbuild::AttInMetadata;

const CONNECTBY_NCOLS: usize = 4;
const CONNECTBY_NCOLS_NOBRANCH: usize = 3;

#[track_caller]
#[cold]
fn err(msg: &str, detail: &str, sqlstate: types_error::SqlState) -> Box<PgError> {
    let mut e = PgError::error(msg.to_string()).with_sqlstate(sqlstate);
    if !detail.is_empty() {
        e = e.with_detail(detail.to_string());
    }
    Box::new(e)
}

fn arg_str<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a str> {
    // SAFETY: catalog args are non-null text varlenas (STRICT fns).
    let v = unsafe { fcinfo.arg_varlena_packed(i)? };
    Ok(core::str::from_utf8(v.data()).expect("text arg is valid UTF-8"))
}

struct ConnectbyParams {
    relname: String,
    key_fld: String,
    parent_key_fld: String,
    orderby_fld: Option<String>,
    branch_delim: String,
    show_branch: bool,
    show_serial: bool,
    max_depth: i32,
}

pub(crate) fn fc_connectby_text(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let show_branch = fcinfo.nargs() == 6;
    let branch_delim = if show_branch {
        arg_str(fcinfo, 5)?.to_string()
    } else {
        "~".to_string()
    };
    let params = ConnectbyParams {
        relname: arg_str(fcinfo, 0)?.to_string(),
        key_fld: arg_str(fcinfo, 1)?.to_string(),
        parent_key_fld: arg_str(fcinfo, 2)?.to_string(),
        orderby_fld: None,
        branch_delim,
        show_branch,
        show_serial: false,
        max_depth: fcinfo.arg_i32(4),
    };
    let start_with = arg_str(fcinfo, 3)?.to_string();
    connectby_body(flinfo.expect("connectby_text: NULL flinfo"), fcinfo, params, start_with)
}

pub(crate) fn fc_connectby_text_serial(
    flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let show_branch = fcinfo.nargs() == 7;
    let branch_delim = if show_branch {
        arg_str(fcinfo, 6)?.to_string()
    } else {
        "~".to_string()
    };
    let params = ConnectbyParams {
        relname: arg_str(fcinfo, 0)?.to_string(),
        key_fld: arg_str(fcinfo, 1)?.to_string(),
        parent_key_fld: arg_str(fcinfo, 2)?.to_string(),
        orderby_fld: Some(arg_str(fcinfo, 3)?.to_string()),
        branch_delim,
        show_branch,
        show_serial: true,
        max_depth: fcinfo.arg_i32(5),
    };
    let start_with = arg_str(fcinfo, 4)?.to_string();
    connectby_body(
        flinfo.expect("connectby_text_serial: NULL flinfo"),
        fcinfo,
        params,
        start_with,
    )
}

fn validate_connectby_tupdesc(
    td: &TupleDescData<'_>,
    show_branch: bool,
    show_serial: bool,
) -> PgResult<()> {
    let mut expected_cols = if show_branch {
        CONNECTBY_NCOLS
    } else {
        CONNECTBY_NCOLS_NOBRANCH
    };
    if show_serial {
        expected_cols += 1;
    }
    if td.natts as usize != expected_cols {
        return Err(err(
            "invalid connectby return type",
            &format!("Return row must have {expected_cols} columns, not {}.", td.natts),
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    if td.attrs[2].atttypid != INT4OID {
        return Err(err(
            "invalid connectby return type",
            &format!(
                "Third return column (depth) must be type {}.",
                format_type::format_type_be(INT4OID)?
            ),
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    if show_branch && td.attrs[3].atttypid != TEXTOID {
        return Err(err(
            "invalid connectby return type",
            &format!(
                "Fourth return column (branch) must be type {}.",
                format_type::format_type_be(TEXTOID)?
            ),
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    if show_branch && show_serial && td.attrs[4].atttypid != INT4OID {
        return Err(err(
            "invalid connectby return type",
            &format!(
                "Fifth return column (serial) must be type {}.",
                format_type::format_type_be(INT4OID)?
            ),
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    if !show_branch && show_serial && td.attrs[3].atttypid != INT4OID {
        return Err(err(
            "invalid connectby return type",
            &format!(
                "Fourth return column (serial) must be type {}.",
                format_type::format_type_be(INT4OID)?
            ),
            ERRCODE_DATATYPE_MISMATCH,
        ));
    }
    Ok(())
}

fn compat_connectby_tupdescs(
    ret: &TupleDescData<'_>,
    sql: &TupleDescData<'_>,
) -> PgResult<()> {
    if sql.natts < 2 {
        return Err(err(
            "invalid connectby source data query",
            "The query must return at least two columns.",
            ERRCODE_INVALID_PARAMETER_VALUE,
        ));
    }
    for (idx, label) in [(0usize, "key"), (1usize, "parent key")] {
        let ra = &ret.attrs[idx];
        let sa = &sql.attrs[idx];
        if ra.atttypid != sa.atttypid || (ra.atttypmod >= 0 && ra.atttypmod != sa.atttypmod) {
            let detail = if idx == 0 {
                format!(
                    "Source key type {} does not match return key type {}.",
                    format_type::format_type_with_typemod(sa.atttypid, sa.atttypmod)?,
                    format_type::format_type_with_typemod(ra.atttypid, ra.atttypmod)?,
                )
            } else {
                format!(
                    "Source parent key type {} does not match return parent key type {}.",
                    format_type::format_type_with_typemod(sa.atttypid, sa.atttypmod)?,
                    format_type::format_type_with_typemod(ra.atttypid, ra.atttypmod)?,
                )
            };
            let _ = label;
            return Err(err("invalid connectby return type", &detail, ERRCODE_DATATYPE_MISMATCH));
        }
    }
    Ok(())
}

fn connectby_body(
    flinfo: &mut FmgrInfo,
    fcinfo: &mut Fcinfo,
    params: ConnectbyParams,
    start_with: String,
) -> PgResult<Datum> {
    // SAFETY: the arming (per-query) context outlives this call.
    let mcx = unsafe { fcinfo.result_mcx_detached() };

    let mut srf = InitMaterializedSRF(mcx, flinfo, fcinfo, MAT_SRF_USE_EXPECTED_DESC)?;
    validate_connectby_tupdesc(&srf.tupdesc, params.show_branch, params.show_serial)?;
    let mut attinmeta = AttInMetadata::new(&srf.tupdesc)?;

    spi::SPI_connect()?;
    let mut serial: i32 = 1;
    build_tuplestore_recursively(
        mcx,
        &params,
        &start_with,
        &start_with,
        0,
        &mut serial,
        &mut attinmeta,
        &mut srf,
    )?;
    spi::SPI_finish()?;

    Ok(srf.finish(fcinfo))
}

#[allow(clippy::too_many_arguments)]
fn build_tuplestore_recursively(
    mcx: Mcx<'_>,
    params: &ConnectbyParams,
    start_with: &str,
    branch: &str,
    level: i32,
    serial: &mut i32,
    attinmeta: &mut AttInMetadata,
    srf: &mut MaterializedSRF<'_>,
) -> PgResult<()> {
    if params.max_depth > 0 && level > params.max_depth {
        return Ok(());
    }

    let quoted_start = quote_literal_cstr(mcx, start_with)?;
    let sql = if !params.show_serial {
        format!(
            "SELECT {}, {} FROM {} WHERE {} = {} AND {} IS NOT NULL AND {} <> {}",
            params.key_fld,
            params.parent_key_fld,
            params.relname,
            params.parent_key_fld,
            quoted_start,
            params.key_fld,
            params.key_fld,
            params.parent_key_fld,
        )
    } else {
        format!(
            "SELECT {}, {} FROM {} WHERE {} = {} AND {} IS NOT NULL AND {} <> {} ORDER BY {}",
            params.key_fld,
            params.parent_key_fld,
            params.relname,
            params.parent_key_fld,
            quoted_start,
            params.key_fld,
            params.key_fld,
            params.parent_key_fld,
            params.orderby_fld.as_deref().expect("serial mode carries orderby_fld"),
        )
    };

    let mut level = level;
    // First time through, emit the root row.
    if level == 0 {
        let mut values = build_values(
            params,
            Some(start_with.as_bytes()),
            None,
            level,
            Some(start_with),
            serial,
        );
        let val_refs: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
        let (d, n) = attinmeta.build(mcx, &val_refs)?;
        srf.putvalues(&d, &n)?;
        values.clear();
        level += 1;
    }

    let ret = spi::SPI_execute(&sql, true, 0)?;
    let proc = spi::SPI_processed();

    if ret != spi::SPI_OK_SELECT || proc == 0 {
        return Ok(());
    }

    let h = spi::SPI_tuptable().expect("SELECT leaves a tuptable");
    // Copy result rows out of the tuptable so recursion (which runs new SPI
    // queries and overwrites SPI_tuptable) reads from owned data.
    struct Row {
        current_key: Option<Vec<u8>>,
        parent_key: Option<Vec<u8>>,
    }
    let rows = spi::tuptable_with(h, |t| -> PgResult<Vec<Row>> {
        compat_connectby_tupdescs(&srf.tupdesc, &t.tupdesc)?;
        let mut rows = Vec::with_capacity(proc as usize);
        for i in 0..proc as usize {
            let current_key =
                spi::SPI_getvalue(mcx, &t.vals[i], &t.tupdesc, 1)?.map(|s| s.to_vec());
            let parent_key =
                spi::SPI_getvalue(mcx, &t.vals[i], &t.tupdesc, 2)?.map(|s| s.to_vec());
            rows.push(Row { current_key, parent_key });
        }
        Ok(rows)
    })?;

    for row in rows {
        // Cycle detection: chk_branchstr = delim + branch + delim, and the
        // current key wrapped in delimiters must not already appear in it.
        if let Some(ck) = row.current_key.as_deref() {
            let chk_branch = format!(
                "{d}{b}{d}",
                d = params.branch_delim,
                b = branch
            );
            let ck_str = String::from_utf8_lossy(ck);
            let chk_current = format!(
                "{d}{k}{d}",
                d = params.branch_delim,
                k = ck_str
            );
            if chk_branch.contains(&chk_current) {
                return Err(err("infinite recursion detected", "", ERRCODE_INVALID_RECURSION));
            }
        }

        // Extend the branch with the current key.
        let current_branch = match row.current_key.as_deref() {
            Some(ck) => {
                format!("{branch}{}{}", params.branch_delim, String::from_utf8_lossy(ck))
            }
            None => branch.to_string(),
        };

        let values = build_values(
            params,
            row.current_key.as_deref(),
            row.parent_key.as_deref(),
            level,
            Some(&current_branch),
            serial,
        );
        let val_refs: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
        let (d, n) = attinmeta.build(mcx, &val_refs)?;
        srf.putvalues(&d, &n)?;

        if let Some(ck) = row.current_key.as_deref() {
            let ck_str = core::str::from_utf8(ck).expect("key is text");
            build_tuplestore_recursively(
                mcx,
                params,
                ck_str,
                &current_branch,
                level + 1,
                serial,
                attinmeta,
                srf,
            )?;
        }
    }

    Ok(())
}

// Assemble the per-row C-string column vector for one output tuple.
fn build_values(
    params: &ConnectbyParams,
    current_key: Option<&[u8]>,
    parent_key: Option<&[u8]>,
    level: i32,
    current_branch: Option<&str>,
    serial: &mut i32,
) -> Vec<Option<Vec<u8>>> {
    let ncols = if params.show_branch {
        CONNECTBY_NCOLS
    } else {
        CONNECTBY_NCOLS_NOBRANCH
    } + params.show_serial as usize;
    let mut values: Vec<Option<Vec<u8>>> = vec![None; ncols];
    values[0] = current_key.map(|s| s.to_vec());
    values[1] = parent_key.map(|s| s.to_vec());
    values[2] = Some(level.to_string().into_bytes());
    if params.show_branch {
        values[3] = current_branch.map(|s| s.as_bytes().to_vec());
    }
    if params.show_serial {
        let s = *serial;
        *serial += 1;
        let idx = if params.show_branch { 4 } else { 3 };
        values[idx] = Some(s.to_string().into_bytes());
    }
    values
}

fn quote_literal_cstr(mcx: Mcx<'_>, s: &str) -> PgResult<String> {
    let v = adt_quote::quote_literal(mcx, s.as_bytes())?;
    Ok(String::from_utf8(v.data().to_vec()).expect("quote_literal yields valid UTF-8"))
}
