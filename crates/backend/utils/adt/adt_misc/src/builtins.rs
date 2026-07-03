use datum::Datum;
use types_core::{InvalidOid, Oid, PG_CATALOG_NAMESPACE, RELATION_RELATION_ID};
use types_error::PgResult;
use types_fmgr::{
    byref_result, cstring_result, varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction,
};

pub fn fc_version(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    Ok(varlena_result(varlena::cstring_to_text(
        mcx,
        crate::introspect::PG_VERSION_STR.as_bytes(),
    )?))
}

pub fn fc_current_database(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let db = crate::introspect::current_database()?;
    byref_result(fcinfo.result_mcx(), &db.data)
}

fn description_result(
    fcinfo: &mut Fcinfo,
    objoid: Oid,
    classoid: Oid,
    objsubid: i32,
) -> PgResult<Datum> {
    let found = crate::introspect::get_description(fcinfo.result_mcx(), objoid, classoid, objsubid)?
        .map(varlena_result);
    match found {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
}

// Unknown catalog name yields NULL, not an error (the SQL body's subquery).
pub fn fc_obj_description(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let objoid = fcinfo.arg_oid(0);
    // SAFETY: catalog arg 1 of obj_description is a non-null name (strict fn).
    let catalogname = unsafe { fcinfo.arg_name(1) };
    let len = catalogname.iter().position(|&b| b == 0).unwrap_or(catalogname.len());
    let catalogname = core::str::from_utf8(&catalogname[..len])
        .expect("catalog names are valid UTF-8");
    let classoid = lsyscache::get_relname_relid(catalogname, PG_CATALOG_NAMESPACE)?;
    if classoid == InvalidOid {
        return Ok(fcinfo.return_null());
    }
    description_result(fcinfo, objoid, classoid, 0)
}

pub fn fc_col_description(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let objoid = fcinfo.arg_oid(0);
    let attnum = fcinfo.arg_i32(1);
    description_result(fcinfo, objoid, RELATION_RELATION_ID, attnum)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin {
        foid,
        name,
        nargs,
        strict: true,
        retset: false,
        func,
    }
}

// 1215/1216 are absent from the canonical table (SQL-language in C, STRICT).

// varchar.c typmodout slice hosted here until the varchar unit lands.
fn typmod_paren(fcinfo: &mut Fcinfo, s: String) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    let mut v = mcx::vec_with_capacity_in(mcx, s.len() + 1)?;
    mcx::vec_append_bytes(&mut v, s.as_bytes())?;
    mcx::vec_append_bytes(&mut v, &[0])?;
    Ok(cstring_result(v))
}

// utils/mb/mbutils.c PG_encoding_to_char, hosted with the misc slice.
pub fn fc_pg_encoding_to_char(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let name = mbutils::pg_encoding_to_char(fcinfo.arg_i32(0));
    let mut n = types_tuple::NameData::default();
    n.namestrcpy(name);
    byref_result(fcinfo.result_mcx(), &n.data)
}

pub fn fc_anychar_typmodout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(0);
    let out = if typmod > 4 { format!("({})", typmod - 4) } else { String::new() };
    typmod_paren(fcinfo, out)
}

pub fn fc_numerictypmodout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let typmod = fcinfo.arg_i32(0);
    let out = if adt_numeric::ops::is_valid_numeric_typmod(typmod) {
        format!(
            "({},{})",
            adt_numeric::ops::numeric_typmod_precision(typmod),
            adt_numeric::ops::numeric_typmod_scale(typmod)
        )
    } else {
        String::new()
    };
    typmod_paren(fcinfo, out)
}

pub const MISC_BUILTINS: &[FmgrBuiltin] = &[
    b(89, "pgsql_version", 0, fc_version),
    b(2918, "numerictypmodout", 1, fc_numerictypmodout),
    b(861, "current_database", 0, fc_current_database),
    b(1215, "obj_description", 2, fc_obj_description),
    b(1597, "PG_encoding_to_char", 1, fc_pg_encoding_to_char),
    b(1216, "col_description", 2, fc_col_description),
];
