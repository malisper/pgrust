//! fmgr wrappers (`fc_*`) + `XMLMAP_BUILTINS` (xml.c mapping-family pg_proc rows).

use datum::{Datum, Varlena};
use mcx::Mcx;
use types_core::Oid;
use types_error::PgResult;
use types_fmgr::{varlena_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo, PGFunction};

fn arg_str<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a str> {
    // SAFETY: catalog arg i is a non-null text/refcursor varlena (strict fn).
    let bytes = unsafe { fcinfo.arg_varlena_packed(i)? }.data();
    Ok(core::str::from_utf8(bytes).unwrap_or_else(|_| panic!("non-UTF-8 text argument")))
}

fn arg_name<'a>(fcinfo: &'a Fcinfo, i: usize) -> &'a str {
    let p = fcinfo.arg(i).as_usize() as *const u8;
    // SAFETY: catalog arg i is a NameData pointer (64-byte NUL-terminated).
    unsafe {
        let mut len = 0usize;
        while len < 64 && *p.add(len) != 0 {
            len += 1;
        }
        core::str::from_utf8(core::slice::from_raw_parts(p, len))
            .unwrap_or_else(|_| panic!("non-UTF-8 name argument"))
    }
}

fn ret_xml<'mcx>(mcx: Mcx<'mcx>, payload: &str) -> PgResult<Datum> {
    let payload = payload.as_bytes();
    let mut image: mcx::PgVec<u8> = mcx::vec_with_capacity_in(mcx, payload.len() + datum::VARHDRSZ)?;
    image.resize(datum::VARHDRSZ, 0);
    mcx::vec_append_bytes(&mut image, payload)?;
    Ok(varlena_result(Varlena::from_image(image)))
}

fn table_fam(
    fcinfo: &mut Fcinfo,
    f: fn(Oid, bool, bool, &str) -> PgResult<String>,
) -> PgResult<Datum> {
    let relid = fcinfo.arg(0).as_oid();
    let nulls = fcinfo.arg(1).as_bool();
    let tableforest = fcinfo.arg(2).as_bool();
    let targetns = arg_str(fcinfo, 3)?;
    let out = f(relid, nulls, tableforest, targetns)?;
    ret_xml(fcinfo.result_mcx(), &out)
}

fn str_fam(
    fcinfo: &mut Fcinfo,
    f: fn(&str, bool, bool, &str) -> PgResult<String>,
) -> PgResult<Datum> {
    let arg0 = arg_str(fcinfo, 0)?;
    let nulls = fcinfo.arg(1).as_bool();
    let tableforest = fcinfo.arg(2).as_bool();
    let targetns = arg_str(fcinfo, 3)?;
    let out = f(arg0, nulls, tableforest, targetns)?;
    ret_xml(fcinfo.result_mcx(), &out)
}

fn name_fam(
    fcinfo: &mut Fcinfo,
    f: fn(&str, bool, bool, &str) -> PgResult<String>,
) -> PgResult<Datum> {
    let schemaname = arg_name(fcinfo, 0);
    let nulls = fcinfo.arg(1).as_bool();
    let tableforest = fcinfo.arg(2).as_bool();
    let targetns = arg_str(fcinfo, 3)?;
    let out = f(schemaname, nulls, tableforest, targetns)?;
    ret_xml(fcinfo.result_mcx(), &out)
}

fn db_fam(fcinfo: &mut Fcinfo, f: fn(bool, bool, &str) -> PgResult<String>) -> PgResult<Datum> {
    let nulls = fcinfo.arg(0).as_bool();
    let tableforest = fcinfo.arg(1).as_bool();
    let targetns = arg_str(fcinfo, 2)?;
    let out = f(nulls, tableforest, targetns)?;
    ret_xml(fcinfo.result_mcx(), &out)
}

pub fn fc_table_to_xml(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    table_fam(fcinfo, crate::table_to_xml)
}

pub fn fc_query_to_xml(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    str_fam(fcinfo, crate::query_to_xml)
}

pub fn fc_cursor_to_xml(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let name = arg_str(fcinfo, 0)?;
    let count = fcinfo.arg(1).as_i32();
    let nulls = fcinfo.arg(2).as_bool();
    let tableforest = fcinfo.arg(3).as_bool();
    let targetns = arg_str(fcinfo, 4)?;
    let out = crate::cursor_to_xml(name, count, nulls, tableforest, targetns)?;
    ret_xml(fcinfo.result_mcx(), &out)
}

pub fn fc_table_to_xmlschema(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    table_fam(fcinfo, crate::table_to_xmlschema)
}

pub fn fc_query_to_xmlschema(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    str_fam(fcinfo, crate::query_to_xmlschema)
}

pub fn fc_cursor_to_xmlschema(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    str_fam(fcinfo, crate::cursor_to_xmlschema)
}

pub fn fc_table_to_xml_and_xmlschema(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    table_fam(fcinfo, crate::table_to_xml_and_xmlschema)
}

pub fn fc_query_to_xml_and_xmlschema(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    str_fam(fcinfo, crate::query_to_xml_and_xmlschema)
}

pub fn fc_schema_to_xml(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    name_fam(fcinfo, crate::schema_to_xml)
}

pub fn fc_schema_to_xmlschema(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    name_fam(fcinfo, crate::schema_to_xmlschema)
}

pub fn fc_schema_to_xml_and_xmlschema(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    name_fam(fcinfo, crate::schema_to_xml_and_xmlschema)
}

pub fn fc_database_to_xml(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    db_fam(fcinfo, crate::database_to_xml)
}

pub fn fc_database_to_xmlschema(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    db_fam(fcinfo, crate::database_to_xmlschema)
}

pub fn fc_database_to_xml_and_xmlschema(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    db_fam(fcinfo, crate::database_to_xml_and_xmlschema)
}

const fn b(foid: Oid, name: &'static str, nargs: i16, func: PGFunction) -> FmgrBuiltin {
    FmgrBuiltin { foid, name, nargs, strict: true, retset: false, func }
}

// pg_proc.dat rows for xml.c's SQL/XML mapping section.
pub const XMLMAP_BUILTINS: &[FmgrBuiltin] = &[
    b(2923, "table_to_xml", 4, fc_table_to_xml),
    b(2924, "query_to_xml", 4, fc_query_to_xml),
    b(2925, "cursor_to_xml", 5, fc_cursor_to_xml),
    b(2926, "table_to_xmlschema", 4, fc_table_to_xmlschema),
    b(2927, "query_to_xmlschema", 4, fc_query_to_xmlschema),
    b(2928, "cursor_to_xmlschema", 4, fc_cursor_to_xmlschema),
    b(2929, "table_to_xml_and_xmlschema", 4, fc_table_to_xml_and_xmlschema),
    b(2930, "query_to_xml_and_xmlschema", 4, fc_query_to_xml_and_xmlschema),
    b(2933, "schema_to_xml", 4, fc_schema_to_xml),
    b(2934, "schema_to_xmlschema", 4, fc_schema_to_xmlschema),
    b(2935, "schema_to_xml_and_xmlschema", 4, fc_schema_to_xml_and_xmlschema),
    b(2936, "database_to_xml", 3, fc_database_to_xml),
    b(2937, "database_to_xmlschema", 3, fc_database_to_xmlschema),
    b(2938, "database_to_xml_and_xmlschema", 3, fc_database_to_xml_and_xmlschema),
];
