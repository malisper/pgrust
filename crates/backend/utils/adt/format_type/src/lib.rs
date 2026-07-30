#![allow(non_snake_case)]

#[cfg(test)]
mod tests;


pub mod builtins;
use datum::Datum;
use keywords::{KeywordCategory, ScanKeywordCategories, ScanKeywordLookup, ScanKeywords};
use types_core::primitive::InvalidOid;
use types_core::catalog::{
    BITOID, BOOLOID, BPCHAROID, FLOAT4OID, FLOAT8OID, INT2OID, INT4OID,
    INT8OID, INTERVALOID, JSONOID, NUMERICOID, TIMEOID, TIMESTAMPOID, TIMESTAMPTZOID, TIMETZOID,
    VARBITOID, VARCHAROID,
};
use types_core::Oid;
use types_error::{PgError, PgResult};

const TYPSTORAGE_PLAIN: i8 = b'p' as i8;
const F_ARRAY_SUBSCRIPT_HANDLER: Oid = 6179;

// IsTrueArrayType (pg_type.h); local copy — lsyscache deps this crate
// (type_maximum_size), so the edge must point downward.
fn is_true_array_type(typelem: Oid, typsubscript: Oid) -> bool {
    typelem != InvalidOid && typsubscript == F_ARRAY_SUBSCRIPT_HANDLER
}

#[track_caller]
#[cold]
#[inline(never)]
fn type_lookup_failed(typid: Oid) -> Box<PgError> {
    Box::new(PgError::error(format!("cache lookup failed for type {typid}")))
}

pub const FORMAT_TYPE_TYPEMOD_GIVEN: u16 = 0x01;
pub const FORMAT_TYPE_ALLOW_INVALID: u16 = 0x02;
pub const FORMAT_TYPE_FORCE_QUALIFY: u16 = 0x04;
pub const FORMAT_TYPE_INVALID_AS_NULL: u16 = 0x08;

/// C `format_type_be` = `format_type_extended(type_oid, -1, 0)`.
pub fn format_type_be(type_oid: Oid) -> PgResult<String> {
    Ok(format_type_extended(type_oid, -1, 0)?.expect("no FORMAT_TYPE_INVALID_AS_NULL"))
}

pub fn format_type_be_qualified(type_oid: Oid) -> PgResult<String> {
    Ok(format_type_extended(type_oid, -1, FORMAT_TYPE_FORCE_QUALIFY)?
        .expect("no FORMAT_TYPE_INVALID_AS_NULL"))
}

pub fn format_type_with_typemod(type_oid: Oid, typemod: i32) -> PgResult<String> {
    Ok(format_type_extended(type_oid, typemod, FORMAT_TYPE_TYPEMOD_GIVEN)?
        .expect("no FORMAT_TYPE_INVALID_AS_NULL"))
}

pub fn format_type_extended(
    type_oid: Oid,
    typemod: i32,
    flags: u16,
) -> PgResult<Option<String>> {
    let typemod_given = flags & FORMAT_TYPE_TYPEMOD_GIVEN != 0;
    if type_oid == InvalidOid {
        if flags & FORMAT_TYPE_INVALID_AS_NULL != 0 {
            return Ok(None);
        }
        if flags & FORMAT_TYPE_ALLOW_INVALID != 0 {
            return Ok(Some("-".to_string()));
        }
    }
    let Some(mut shape) = syscache_seams::lookup_pg_type_typcache_shape::call(type_oid)? else {
        if flags & FORMAT_TYPE_INVALID_AS_NULL != 0 {
            return Ok(None);
        }
        if flags & FORMAT_TYPE_ALLOW_INVALID != 0 {
            return Ok(Some("???".to_string()));
        }
        return Err(type_lookup_failed(type_oid));
    };
    let mut named_oid = type_oid;
    let mut is_array = false;
    if is_true_array_type(shape.typelem, shape.typsubscript)
        && shape.typstorage != TYPSTORAGE_PLAIN
    {
        named_oid = shape.typelem;
        shape = match syscache_seams::lookup_pg_type_typcache_shape::call(named_oid)? {
            Some(s) => s,
            None if flags & FORMAT_TYPE_INVALID_AS_NULL != 0 => return Ok(None),
            None if flags & FORMAT_TYPE_ALLOW_INVALID != 0 => {
                return Ok(Some("???[]".to_string()))
            }
            None => return Err(type_lookup_failed(type_oid)),
        };
        is_array = true;
    }

    let with_typemod = typemod_given && typemod >= 0;

    let special: Option<String> = match named_oid {
        // bit/bpchar with TYPEMOD_GIVEN and typemod -1 fall to the quoted
        // catalog name (BIT means BIT(1), CHARACTER means CHARACTER(1)).
        BITOID if with_typemod => Some(print_typmod("bit", typemod, named_oid)?),
        BITOID if typemod_given => None,
        BITOID => Some("bit".to_string()),
        BOOLOID => Some("boolean".to_string()),
        BPCHAROID if with_typemod => Some(print_typmod("character", typemod, named_oid)?),
        BPCHAROID if typemod_given => None,
        BPCHAROID => Some("character".to_string()),
        FLOAT4OID => Some("real".to_string()),
        FLOAT8OID => Some("double precision".to_string()),
        INT2OID => Some("smallint".to_string()),
        INT4OID => Some("integer".to_string()),
        INT8OID => Some("bigint".to_string()),
        NUMERICOID if with_typemod => Some(print_typmod("numeric", typemod, named_oid)?),
        NUMERICOID => Some("numeric".to_string()),
        INTERVALOID if with_typemod => Some(print_typmod("interval", typemod, named_oid)?),
        INTERVALOID => Some("interval".to_string()),
        TIMEOID if with_typemod => Some(print_typmod("time", typemod, named_oid)?),
        TIMEOID => Some("time without time zone".to_string()),
        TIMETZOID if with_typemod => Some(print_typmod("time", typemod, named_oid)?),
        TIMETZOID => Some("time with time zone".to_string()),
        TIMESTAMPOID if with_typemod => Some(print_typmod("timestamp", typemod, named_oid)?),
        TIMESTAMPOID => Some("timestamp without time zone".to_string()),
        TIMESTAMPTZOID if with_typemod => Some(print_typmod("timestamp", typemod, named_oid)?),
        TIMESTAMPTZOID => Some("timestamp with time zone".to_string()),
        VARBITOID if with_typemod => Some(print_typmod("bit varying", typemod, named_oid)?),
        VARBITOID => Some("bit varying".to_string()),
        VARCHAROID if with_typemod => Some(print_typmod("character varying", typemod, named_oid)?),
        VARCHAROID => Some("character varying".to_string()),
        JSONOID => Some("json".to_string()),
        _ => None,
    };

    let mut buf = match special {
        Some(name) => name,
        None => {
            let name = core::str::from_utf8(shape.typname.name_str())
                .unwrap_or_else(|_| panic!("non-UTF-8 pg_type.typname"));
            // C: quote_qualified_identifier(NULL-if-visible nspname, typname).
            let mut quoted = String::new();
            // C qualifies purely on visibility (no OID gate): initdb-created
            // types (information_schema domains, oid < FirstNormalObjectId)
            // must qualify when their schema is not on search_path.
            if flags & FORMAT_TYPE_FORCE_QUALIFY != 0
                || !namespace_seams::type_is_visible::call(named_oid)?
            {
                let t = syscache_seams::pg_type_domain_shape::call(named_oid)?
                    .ok_or_else(|| type_lookup_failed(named_oid))?;
                // get_namespace_name_or_temp (lsyscache.c) over the two
                // seams; lsyscache deps this crate so a direct dep cycles.
                if namespace_seams::is_temp_namespace::call(t.typnamespace) {
                    quoted.push_str("pg_temp");
                } else {
                    let nsp = syscache_seams::pg_namespace_nspname::call(t.typnamespace)?
                        .ok_or_else(|| type_lookup_failed(named_oid))?;
                    let nsp = core::str::from_utf8(nsp.name_str())
                        .unwrap_or_else(|_| panic!("non-UTF-8 pg_namespace.nspname"));
                    quoted.push_str(&quote_identifier(nsp));
                }
                quoted.push('.');
            }
            quoted.push_str(&quote_identifier(name));
            if with_typemod {
                print_typmod(&quoted, typemod, named_oid)?
            } else {
                quoted
            }
        }
    };
    if is_array {
        buf.push_str("[]");
    }
    Ok(Some(buf))
}

/// C `printTypmod`; takes the type oid instead of a pre-fetched typmodout.
fn print_typmod(typname: &str, typmod: i32, type_oid: Oid) -> PgResult<String> {
    debug_assert!(typmod >= 0);
    let typmodout = match syscache_seams::pg_type_io_shape::call(type_oid)? {
        Some(t) => t.typmodout,
        None => InvalidOid,
    };
    if typmodout == InvalidOid {
        return Ok(format!("{typname}({typmod})"));
    }
    let mut finfo = fmgr_seams::fmgr_info::call(typmodout)?;
    let ctx = mcx::MemoryContext::new("print_typmod");
    let mut fcinfo = types_fmgr::LocalFcinfo::<1>::fresh(InvalidOid);
    // SAFETY: ctx outlives the call; the cstring is copied into the format!
    // below before ctx drops.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    fcinfo.set_arg(0, Datum::from_i32(typmod));
    let out = finfo.invoke(&mut fcinfo)?;
    // SAFETY: typmodout fns return a NUL-terminated cstring datum.
    let s = unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const core::ffi::c_char) };
    Ok(format!("{typname}{}", s.to_str().expect("typmodout output is ASCII")))
}

pub fn type_maximum_size(type_oid: Oid, typemod: i32) -> i32 {
    if typemod < 0 {
        return -1;
    }
    const VARHDRSZ: i32 = 4;
    match type_oid {
        BPCHAROID | VARCHAROID => {
            let max_len =
                wchar::pg_encoding_max_length(mbutils_seams::get_database_encoding::call());
            (typemod - VARHDRSZ) * max_len + VARHDRSZ
        }
        NUMERICOID => {
            // numeric_maximum_size (numeric.c) inlined: a direct adt_numeric dep
            // closes the cycle arrayfuncs->lsyscache->format_type->adt_numeric->arrayfuncs.
            if typemod < 4 {
                return -1;
            }
            let precision = ((typemod - 4) >> 16) & 0xffff;
            let numeric_digits = (precision + 2 * (4 - 1)) / 4;
            8 + numeric_digits * 2
        }
        VARBITOID | BITOID => (typemod + 7) / 8 + 2 * 4,
        _ => -1,
    }
}

/// C `quote_identifier` (ruleutils.c) minus the quote_all_identifiers GUC.
pub fn quote_identifier(ident: &str) -> std::borrow::Cow<'_, str> {
    let bytes = ident.as_bytes();
    let mut safe = matches!(bytes.first(), Some(b'a'..=b'z' | b'_'));
    if safe {
        safe = bytes
            .iter()
            .all(|&b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_'));
    }
    if safe {
        let kwnum = ScanKeywordLookup(bytes, &ScanKeywords);
        if kwnum >= 0 && ScanKeywordCategories[kwnum as usize] != KeywordCategory::Unreserved {
            safe = false;
        }
    }
    if safe {
        return std::borrow::Cow::Borrowed(ident);
    }
    let mut quoted = String::with_capacity(ident.len() + 2);
    quoted.push('"');
    for ch in ident.chars() {
        if ch == '"' {
            quoted.push('"');
        }
        quoted.push(ch);
    }
    quoted.push('"');
    std::borrow::Cow::Owned(quoted)
}
