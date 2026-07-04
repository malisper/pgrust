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

pub fn fc_shobj_description(
    _flinfo: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let objoid = fcinfo.arg_oid(0);
    // SAFETY: catalog arg 1 of shobj_description is a non-null name (strict fn).
    let catalogname = unsafe { fcinfo.arg_name(1) };
    let len = catalogname.iter().position(|&b| b == 0).unwrap_or(catalogname.len());
    let catalogname = core::str::from_utf8(&catalogname[..len])
        .expect("catalog names are valid UTF-8");
    let classoid = lsyscache::get_relname_relid(catalogname, PG_CATALOG_NAMESPACE)?;
    if classoid == InvalidOid {
        return Ok(fcinfo.return_null());
    }
    let found = crate::introspect::get_shared_description(fcinfo.result_mcx(), objoid, classoid)?
        .map(varlena_result);
    match found {
        Some(d) => Ok(d),
        None => Ok(fcinfo.return_null()),
    }
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

struct KeywordRows {
    tuples: Vec<Vec<u8>>,
}

fn collect_keyword_rows(flinfo: &FmgrInfo, fcinfo: &Fcinfo) -> PgResult<KeywordRows> {
    let mcx = fcinfo.result_mcx();
    let resolved = funcapi::get_call_result_type(mcx, flinfo, None)?;
    if resolved.class != funcapi::TypeFuncClass::Composite {
        return Err(Box::new(types_error::PgError::error(
            "return type must be a row type",
        )));
    }
    let desc = resolved.result_tuple_desc.expect("composite result carries a tupdesc");

    let n = keywords::ScanKeywords.num_keywords as usize;
    let mut tuples = Vec::with_capacity(n);
    for i in 0..n {
        let word = keywords::GetScanKeyword(i, &keywords::ScanKeywords).expect("index < n");
        let (catcode, catdesc): (u8, &str) = match keywords::ScanKeywordCategories[i] {
            keywords::KeywordCategory::Unreserved => (b'U', "unreserved"),
            keywords::KeywordCategory::ColName => {
                (b'C', "unreserved (cannot be function or type name)")
            }
            keywords::KeywordCategory::TypeFuncName => {
                (b'T', "reserved (can be function or type name)")
            }
            keywords::KeywordCategory::Reserved => (b'R', "reserved"),
        };
        let barelabel = keywords::ScanKeywordBareLabel[i];
        let baredesc = if barelabel { "can be bare label" } else { "requires AS" };
        let values = [
            varlena_result(varlena::cstring_to_text(mcx, word)?),
            Datum::from_char(catcode as i8),
            Datum::from_bool(barelabel),
            varlena_result(varlena::cstring_to_text(mcx, catdesc.as_bytes())?),
            varlena_result(varlena::cstring_to_text(mcx, baredesc.as_bytes())?),
        ];
        let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &[false; 5])?;
        tuples.push(tuple.image().to_vec());
    }
    Ok(KeywordRows { tuples })
}

pub fn fc_pg_get_keywords(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("pg_get_keywords: NULL flinfo");
    if !flinfo.has_fn_extra() {
        let rows = collect_keyword_rows(flinfo, fcinfo)?;
        let fctx = funcapi::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("pg_get_keywords: rows set at first call")
        .downcast_ref::<KeywordRows>()
        .expect("pg_get_keywords: user_fctx is KeywordRows");
    match rows.tuples.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(funcapi::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi::srf_return_done(flinfo, fcinfo)),
    }
}

pub const MISC_BUILTINS: &[FmgrBuiltin] = &[
    b(89, "pgsql_version", 0, fc_version),
    b(2918, "numerictypmodout", 1, fc_numerictypmodout),
    b(861, "current_database", 0, fc_current_database),
    b(1215, "obj_description", 2, fc_obj_description),
    b(1597, "PG_encoding_to_char", 1, fc_pg_encoding_to_char),
    b(1216, "col_description", 2, fc_col_description),
    FmgrBuiltin {
        foid: 1686,
        name: "pg_get_keywords",
        nargs: 0,
        strict: true,
        retset: true,
        func: fc_pg_get_keywords,
    },
    b(1993, "shobj_description", 2, fc_shobj_description),
];
