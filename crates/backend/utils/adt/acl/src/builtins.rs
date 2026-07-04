use datum::Datum;
use types_core::Oid;
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_UNDEFINED_TABLE};
use types_fmgr::{
    byref_result, cstring_result, FmgrBuiltin, FmgrInfo, FunctionCallInfoBaseData as Fcinfo,
    PGFunction, ACLITEM_LEN,
};

use crate::ops::{convert_any_priv_string, PrivMapEntry};
use crate::varlena::acl_image;
use crate::{
    acl_grant_option_for, acldefault, aclitem_set_privs_goptions, get_role_oid_or_public,
    AclItem, AclObjectType, ACL_DELETE, ACL_INSERT, ACL_MAINTAIN, ACL_NO_RIGHTS, ACL_REFERENCES,
    ACL_SELECT, ACL_TRIGGER, ACL_TRUNCATE, ACL_UPDATE,
};

const ACLCHECK_OK: i32 = 0;

#[inline]
fn arg_aclitem(fcinfo: &Fcinfo, i: usize) -> AclItem {
    // SAFETY: catalog arg type aclitem — non-null 16-byte by-ref (strict fn).
    let b = unsafe { fcinfo.arg_fixed(i, ACLITEM_LEN) };
    let mut g = [0u8; 4];
    let mut r = [0u8; 4];
    let mut p = [0u8; 8];
    g.copy_from_slice(&b[0..4]);
    r.copy_from_slice(&b[4..8]);
    p.copy_from_slice(&b[8..16]);
    AclItem {
        ai_grantee: u32::from_le_bytes(g),
        ai_grantor: u32::from_le_bytes(r),
        ai_privs: u64::from_le_bytes(p),
    }
}

fn aclitem_result(fcinfo: &Fcinfo, item: &AclItem) -> PgResult<Datum> {
    let mut b = [0u8; ACLITEM_LEN];
    b[0..4].copy_from_slice(&item.ai_grantee.to_le_bytes());
    b[4..8].copy_from_slice(&item.ai_grantor.to_le_bytes());
    b[8..16].copy_from_slice(&item.ai_privs.to_le_bytes());
    byref_result(fcinfo.result_mcx(), &b)
}

fn arg_text_str<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a str> {
    // SAFETY: catalog arg type text — non-null varlena (strict fn).
    let v = unsafe { fcinfo.arg_varlena_packed(i) }?;
    core::str::from_utf8(v.data())
        .map_err(|_| Box::new(PgError::error("invalid UTF-8 in text argument")))
}

fn arg_name_str<'a>(fcinfo: &'a Fcinfo, i: usize) -> PgResult<&'a str> {
    // SAFETY: catalog arg type name — non-null 64-byte Name (strict fn).
    let b = unsafe { fcinfo.arg_name(i) };
    let end = b.iter().position(|&c| c == 0).unwrap_or(b.len());
    core::str::from_utf8(&b[..end])
        .map_err(|_| Box::new(PgError::error("invalid UTF-8 in name argument")))
}

fn fc_aclitemin(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    // SAFETY: catalog arg 0 of aclitemin is a non-null cstring.
    let s = unsafe { fcinfo.arg_cstring(0) };
    // SAFETY: fcinfo.context, if set, is a live ErrorSaveNode armed for this call.
    let esc = unsafe { fcinfo.error_save_node() };
    match crate::io::aclitemin(s.to_bytes(), esc)? {
        Some(item) => aclitem_result(fcinfo, &item),
        None => Ok(fcinfo.return_null()),
    }
}

// Out-function contract: the returned cstring aliases backend-thread scratch
// (the nameout precedent) so array_out's unarmed per-element calls work.
std::thread_local! {
    static ACLITEMOUT_SCRATCH: core::cell::RefCell<Vec<u8>> =
        const { core::cell::RefCell::new(Vec::new()) };
}

fn fc_aclitemout(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let item = arg_aclitem(fcinfo, 0);
    ACLITEMOUT_SCRATCH.with(|c| {
        let mut buf = c.borrow_mut();
        buf.clear();
        crate::io::aclitemout_into(&item, &mut buf)?;
        buf.push(0);
        Ok(Datum::from_usize(buf.as_ptr() as usize))
    })
}

fn fc_aclitem_eq(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a1 = arg_aclitem(fcinfo, 0);
    let a2 = arg_aclitem(fcinfo, 1);
    Ok(Datum::from_bool(a1 == a2))
}

fn fc_hash_aclitem(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_aclitem(fcinfo, 0);
    let sum = (a.ai_privs as u32)
        .wrapping_add(a.ai_grantee)
        .wrapping_add(a.ai_grantor);
    Ok(Datum::from_i32(sum as i32))
}

fn fc_hash_aclitem_extended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_aclitem(fcinfo, 0);
    let seed = fcinfo.arg_i64(1) as u64;
    let sum = (a.ai_privs as u32)
        .wrapping_add(a.ai_grantee)
        .wrapping_add(a.ai_grantor);
    let h = if seed == 0 {
        sum as u64
    } else {
        hash_uint32_extended(sum, seed)
    };
    Ok(Datum::from_i64(h as i64))
}

// hash_bytes_uint32_extended (common/hashfn.c).
fn hash_uint32_extended(k: u32, seed: u64) -> u64 {
    let init: u32 = 0x9e37_79b9u32.wrapping_add(4).wrapping_add(3923095);
    let (mut a, mut b, mut c) = (init, init, init);
    if seed != 0 {
        a = a.wrapping_add((seed >> 32) as u32);
        b = b.wrapping_add(seed as u32);
        (a, b, c) = mix(a, b, c);
    }
    a = a.wrapping_add(k);
    let (_, b, c) = final_mix(a, b, c);
    ((b as u64) << 32) | (c as u64)
}

fn mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    a = a.wrapping_sub(c); a ^= c.rotate_left(4);  c = c.wrapping_add(b);
    b = b.wrapping_sub(a); b ^= a.rotate_left(6);  a = a.wrapping_add(c);
    c = c.wrapping_sub(b); c ^= b.rotate_left(8);  b = b.wrapping_add(a);
    a = a.wrapping_sub(c); a ^= c.rotate_left(16); c = c.wrapping_add(b);
    b = b.wrapping_sub(a); b ^= a.rotate_left(19); a = a.wrapping_add(c);
    c = c.wrapping_sub(b); c ^= b.rotate_left(4);  b = b.wrapping_add(a);
    (a, b, c)
}

fn final_mix(mut a: u32, mut b: u32, mut c: u32) -> (u32, u32, u32) {
    c ^= b; c = c.wrapping_sub(b.rotate_left(14));
    a ^= c; a = a.wrapping_sub(c.rotate_left(11));
    b ^= a; b = b.wrapping_sub(a.rotate_left(25));
    c ^= b; c = c.wrapping_sub(b.rotate_left(16));
    a ^= c; a = a.wrapping_sub(c.rotate_left(4));
    b ^= a; b = b.wrapping_sub(a.rotate_left(14));
    c ^= b; c = c.wrapping_sub(b.rotate_left(24));
    (a, b, c)
}

fn fc_aclcontains(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: catalog arg 0 of aclcontains is a non-null aclitem[] varlena.
    let v = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let acl = crate::varlena::decode_acl_payload(mcx, v.data())?;
    let aip = arg_aclitem(fcinfo, 1);
    Ok(Datum::from_bool(crate::ops::aclcontains(&acl, &aip)))
}

#[cold]
fn no_longer_supported(what: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("{what} is no longer supported"))
            .with_sqlstate(ERRCODE_FEATURE_NOT_SUPPORTED),
    )
}

fn fc_aclinsert(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(no_longer_supported("aclinsert"))
}

fn fc_aclremove(_flinfo: Option<&mut FmgrInfo>, _fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    Err(no_longer_supported("aclremove"))
}

const MAKEACLITEM_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "SELECT", value: ACL_SELECT },
    PrivMapEntry { name: "INSERT", value: ACL_INSERT },
    PrivMapEntry { name: "UPDATE", value: ACL_UPDATE },
    PrivMapEntry { name: "DELETE", value: ACL_DELETE },
    PrivMapEntry { name: "TRUNCATE", value: ACL_TRUNCATE },
    PrivMapEntry { name: "REFERENCES", value: ACL_REFERENCES },
    PrivMapEntry { name: "TRIGGER", value: ACL_TRIGGER },
    PrivMapEntry { name: "EXECUTE", value: crate::ACL_EXECUTE },
    PrivMapEntry { name: "USAGE", value: crate::ACL_USAGE },
    PrivMapEntry { name: "CREATE", value: crate::ACL_CREATE },
    PrivMapEntry { name: "TEMP", value: crate::ACL_CREATE_TEMP },
    PrivMapEntry { name: "TEMPORARY", value: crate::ACL_CREATE_TEMP },
    PrivMapEntry { name: "CONNECT", value: crate::ACL_CONNECT },
    PrivMapEntry { name: "SET", value: crate::ACL_SET },
    PrivMapEntry { name: "ALTER SYSTEM", value: crate::ACL_ALTER_SYSTEM },
    PrivMapEntry { name: "MAINTAIN", value: ACL_MAINTAIN },
];

fn fc_makeaclitem(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let grantee = fcinfo.arg_oid(0);
    let grantor = fcinfo.arg_oid(1);
    let privtext = arg_text_str(fcinfo, 2)?;
    let goption = fcinfo.arg_bool(3);
    let privs = convert_any_priv_string(privtext, MAKEACLITEM_PRIV_MAP)?;
    let mut item = AclItem {
        ai_grantee: grantee,
        ai_grantor: grantor,
        ai_privs: 0,
    };
    aclitem_set_privs_goptions(&mut item, privs, if goption { privs } else { ACL_NO_RIGHTS });
    aclitem_result(fcinfo, &item)
}

fn fc_acldefault_sql(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let objtypec = fcinfo.arg_char(0) as u8;
    let owner = fcinfo.arg_oid(1);
    let objtype = match objtypec {
        b'c' => AclObjectType::Column,
        b'r' => AclObjectType::Table,
        b's' => AclObjectType::Sequence,
        b'd' => AclObjectType::Database,
        b'f' => AclObjectType::Function,
        b'l' => AclObjectType::Language,
        b'L' => AclObjectType::LargeObject,
        b'n' => AclObjectType::Schema,
        b'p' => AclObjectType::ParameterAcl,
        b't' => AclObjectType::Tablespace,
        b'F' => AclObjectType::Fdw,
        b'S' => AclObjectType::ForeignServer,
        b'T' => AclObjectType::Type,
        other => {
            return Err(Box::new(PgError::error(format!(
                "unrecognized object type abbreviation: {}",
                other as char
            ))))
        }
    };
    let mcx = fcinfo.result_mcx();
    let acl = acldefault(objtype, owner);
    let img = acl_image(mcx, acl.as_slice())?;
    let d = Datum::from_usize(img.as_ptr() as usize);
    core::mem::forget(img);
    Ok(d)
}

const TABLE_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "SELECT", value: ACL_SELECT },
    PrivMapEntry { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
    PrivMapEntry { name: "INSERT", value: ACL_INSERT },
    PrivMapEntry { name: "INSERT WITH GRANT OPTION", value: acl_grant_option_for(ACL_INSERT) },
    PrivMapEntry { name: "UPDATE", value: ACL_UPDATE },
    PrivMapEntry { name: "UPDATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_UPDATE) },
    PrivMapEntry { name: "DELETE", value: ACL_DELETE },
    PrivMapEntry { name: "DELETE WITH GRANT OPTION", value: acl_grant_option_for(ACL_DELETE) },
    PrivMapEntry { name: "TRUNCATE", value: ACL_TRUNCATE },
    PrivMapEntry { name: "TRUNCATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_TRUNCATE) },
    PrivMapEntry { name: "REFERENCES", value: ACL_REFERENCES },
    PrivMapEntry {
        name: "REFERENCES WITH GRANT OPTION",
        value: acl_grant_option_for(ACL_REFERENCES),
    },
    PrivMapEntry { name: "TRIGGER", value: ACL_TRIGGER },
    PrivMapEntry { name: "TRIGGER WITH GRANT OPTION", value: acl_grant_option_for(ACL_TRIGGER) },
    PrivMapEntry { name: "MAINTAIN", value: ACL_MAINTAIN },
    PrivMapEntry { name: "MAINTAIN WITH GRANT OPTION", value: acl_grant_option_for(ACL_MAINTAIN) },
];

fn convert_table_priv_string(priv_type: &str) -> PgResult<u64> {
    convert_any_priv_string(priv_type, TABLE_PRIV_MAP)
}

fn convert_table_name(fcinfo: &Fcinfo, i: usize) -> PgResult<Oid> {
    use types_error::ERRCODE_INVALID_NAME;
    let mcx = fcinfo.result_mcx();
    let rawname = arg_text_str(fcinfo, i)?;
    let encoding = if mbutils_seams::get_database_encoding::is_installed() {
        mbutils_seams::get_database_encoding::call()
    } else {
        wchar::PG_SQL_ASCII
    };
    let names = ::varlena::split_identifier_string(mcx, rawname, b'.', encoding)?
        .filter(|l| !l.is_empty())
        .ok_or_else(|| {
            Box::new(PgError::error("invalid name syntax").with_sqlstate(ERRCODE_INVALID_NAME))
        })?;
    let (catalogname, schemaname, relname) = match names.as_slice() {
        [r] => (None, None, r.as_str()),
        [s, r] => (None, Some(s.as_str()), r.as_str()),
        [c, s, r] => (Some(c.as_str()), Some(s.as_str()), r.as_str()),
        _ => {
            return Err(Box::new(
                PgError::error(format!(
                    "improper relation name (too many dotted names): {rawname}"
                ))
                .with_sqlstate(types_error::ERRCODE_SYNTAX_ERROR),
            ))
        }
    };
    let rv = rel_vocab::RangeVar {
        catalogname,
        schemaname,
        relname,
        inh: true,
        relpersistence: types_core::catalog::RELPERSISTENCE_PERMANENT,
        location: -1,
    };
    // We might not even have permissions on this relation; don't lock it.
    catalog_namespace::RangeVarGetRelid(&rv, 0, false)
}

#[cold]
#[inline(never)]
fn undefined_table_oid(oid: Oid) -> Box<PgError> {
    Box::new(
        PgError::error(format!("relation with OID {oid} does not exist"))
            .with_sqlstate(ERRCODE_UNDEFINED_TABLE),
    )
}

fn table_priv_check(roleid: Oid, tableoid: Oid, mode: u64) -> PgResult<Datum> {
    let (aclresult, is_missing) = aclchk_seams::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if is_missing {
        return Err(undefined_table_oid(tableoid));
    }
    Ok(Datum::from_bool(aclresult == ACLCHECK_OK))
}

fn table_priv_check_ext(fcinfo: &mut Fcinfo, roleid: Oid, tableoid: Oid, mode: u64) -> PgResult<Datum> {
    let (aclresult, is_missing) = aclchk_seams::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if is_missing {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_bool(aclresult == ACLCHECK_OK))
}

fn fc_has_table_privilege_name_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let tableoid = convert_table_name(fcinfo, 1)?;
    let mode = convert_table_priv_string(arg_text_str(fcinfo, 2)?)?;
    table_priv_check(roleid, tableoid, mode)
}

fn fc_has_table_privilege_name(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let tableoid = convert_table_name(fcinfo, 0)?;
    let mode = convert_table_priv_string(arg_text_str(fcinfo, 1)?)?;
    table_priv_check(roleid, tableoid, mode)
}

fn fc_has_table_privilege_name_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let tableoid = fcinfo.arg_oid(1);
    let mode = convert_table_priv_string(arg_text_str(fcinfo, 2)?)?;
    table_priv_check_ext(fcinfo, roleid, tableoid, mode)
}

fn fc_has_table_privilege_id(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let tableoid = fcinfo.arg_oid(0);
    let mode = convert_table_priv_string(arg_text_str(fcinfo, 1)?)?;
    table_priv_check_ext(fcinfo, roleid, tableoid, mode)
}

fn fc_has_table_privilege_id_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let tableoid = convert_table_name(fcinfo, 1)?;
    let mode = convert_table_priv_string(arg_text_str(fcinfo, 2)?)?;
    table_priv_check(roleid, tableoid, mode)
}

fn fc_has_table_privilege_id_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let tableoid = fcinfo.arg_oid(1);
    let mode = convert_table_priv_string(arg_text_str(fcinfo, 2)?)?;
    table_priv_check_ext(fcinfo, roleid, tableoid, mode)
}

const DATABASE_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "CREATE", value: crate::ACL_CREATE },
    PrivMapEntry { name: "CREATE WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_CREATE) },
    PrivMapEntry { name: "TEMPORARY", value: crate::ACL_CREATE_TEMP },
    PrivMapEntry {
        name: "TEMPORARY WITH GRANT OPTION",
        value: acl_grant_option_for(crate::ACL_CREATE_TEMP),
    },
    PrivMapEntry { name: "TEMP", value: crate::ACL_CREATE_TEMP },
    PrivMapEntry { name: "TEMP WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_CREATE_TEMP) },
    PrivMapEntry { name: "CONNECT", value: crate::ACL_CONNECT },
    PrivMapEntry { name: "CONNECT WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_CONNECT) },
];

const FUNCTION_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "EXECUTE", value: crate::ACL_EXECUTE },
    PrivMapEntry { name: "EXECUTE WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_EXECUTE) },
];

const USAGE_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "USAGE", value: crate::ACL_USAGE },
    PrivMapEntry { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_USAGE) },
];

const SCHEMA_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "CREATE", value: crate::ACL_CREATE },
    PrivMapEntry { name: "CREATE WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_CREATE) },
    PrivMapEntry { name: "USAGE", value: crate::ACL_USAGE },
    PrivMapEntry { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_USAGE) },
];

const LARGEOBJECT_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "SELECT", value: ACL_SELECT },
    PrivMapEntry { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
    PrivMapEntry { name: "UPDATE", value: ACL_UPDATE },
    PrivMapEntry { name: "UPDATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_UPDATE) },
];

fn object_priv_check(classid: Oid, objectid: Oid, roleid: Oid, mode: u64) -> PgResult<Datum> {
    let r = aclchk_seams::object_aclcheck::call(classid, objectid, roleid, mode)?;
    Ok(Datum::from_bool(r == ACLCHECK_OK))
}

fn object_priv_check_ext(
    fcinfo: &mut Fcinfo,
    classid: Oid,
    objectid: Oid,
    roleid: Oid,
    mode: u64,
) -> PgResult<Datum> {
    let (r, is_missing) = aclchk_seams::object_aclcheck_ext::call(classid, objectid, roleid, mode)?;
    if is_missing {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_bool(r == ACLCHECK_OK))
}

fn convert_database_name(fcinfo: &Fcinfo, i: usize) -> PgResult<Oid> {
    dbcommands_seams::get_database_oid::call(fcinfo.result_mcx(), arg_text_str(fcinfo, i)?, false)
}

fn convert_schema_name(fcinfo: &Fcinfo, i: usize) -> PgResult<Oid> {
    catalog_namespace::get_namespace_oid(arg_text_str(fcinfo, i)?, false)
}

fn convert_language_name(fcinfo: &Fcinfo, i: usize) -> PgResult<Oid> {
    crate::get_language_oid(arg_text_str(fcinfo, i)?, false)
}

fn convert_function_name(fcinfo: &Fcinfo, i: usize) -> PgResult<Oid> {
    let s = arg_text_str(fcinfo, i)?;
    let oid = adt_regproc::regprocedurein(fcinfo.result_mcx(), s, None)?.unwrap_or(0);
    if oid == 0 {
        return Err(Box::new(
            PgError::error(format!("function \"{s}\" does not exist"))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_FUNCTION),
        ));
    }
    Ok(oid)
}

fn convert_type_name(fcinfo: &Fcinfo, i: usize) -> PgResult<Oid> {
    let s = arg_text_str(fcinfo, i)?;
    let oid = adt_regproc::regtypein(fcinfo.result_mcx(), s, None)?.unwrap_or(0);
    if oid == 0 {
        return Err(Box::new(
            PgError::error(format!("type \"{s}\" does not exist"))
                .with_sqlstate(types_error::ERRCODE_UNDEFINED_OBJECT),
        ));
    }
    Ok(oid)
}

macro_rules! has_priv_family {
    ($classid:expr, $map:expr, $convert:ident,
     $nn:ident, $ni:ident, $in_:ident, $ii:ident, $n:ident, $i:ident) => {
        fn $nn(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
            let objoid = $convert(fcinfo, 1)?;
            let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, $map)?;
            object_priv_check($classid, objoid, roleid, mode)
        }
        fn $ni(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
            let objoid = fcinfo.arg_oid(1);
            let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, $map)?;
            object_priv_check_ext(fcinfo, $classid, objoid, roleid, mode)
        }
        fn $in_(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let roleid = fcinfo.arg_oid(0);
            let objoid = $convert(fcinfo, 1)?;
            let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, $map)?;
            object_priv_check($classid, objoid, roleid, mode)
        }
        fn $ii(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let roleid = fcinfo.arg_oid(0);
            let objoid = fcinfo.arg_oid(1);
            let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, $map)?;
            object_priv_check_ext(fcinfo, $classid, objoid, roleid, mode)
        }
        fn $n(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let roleid = miscinit_seams::get_user_id::call();
            let objoid = $convert(fcinfo, 0)?;
            let mode = convert_any_priv_string(arg_text_str(fcinfo, 1)?, $map)?;
            object_priv_check($classid, objoid, roleid, mode)
        }
        fn $i(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
            let roleid = miscinit_seams::get_user_id::call();
            let objoid = fcinfo.arg_oid(0);
            let mode = convert_any_priv_string(arg_text_str(fcinfo, 1)?, $map)?;
            object_priv_check_ext(fcinfo, $classid, objoid, roleid, mode)
        }
    };
}

has_priv_family!(
    types_core::catalog::DATABASE_RELATION_ID,
    DATABASE_PRIV_MAP,
    convert_database_name,
    fc_has_database_privilege_name_name,
    fc_has_database_privilege_name_id,
    fc_has_database_privilege_id_name,
    fc_has_database_privilege_id_id,
    fc_has_database_privilege_name,
    fc_has_database_privilege_id
);

has_priv_family!(
    types_core::catalog::PROCEDURE_RELATION_ID,
    FUNCTION_PRIV_MAP,
    convert_function_name,
    fc_has_function_privilege_name_name,
    fc_has_function_privilege_name_id,
    fc_has_function_privilege_id_name,
    fc_has_function_privilege_id_id,
    fc_has_function_privilege_name,
    fc_has_function_privilege_id
);

has_priv_family!(
    types_core::catalog::LANGUAGE_RELATION_ID,
    USAGE_PRIV_MAP,
    convert_language_name,
    fc_has_language_privilege_name_name,
    fc_has_language_privilege_name_id,
    fc_has_language_privilege_id_name,
    fc_has_language_privilege_id_id,
    fc_has_language_privilege_name,
    fc_has_language_privilege_id
);

has_priv_family!(
    types_core::catalog::NAMESPACE_RELATION_ID,
    SCHEMA_PRIV_MAP,
    convert_schema_name,
    fc_has_schema_privilege_name_name,
    fc_has_schema_privilege_name_id,
    fc_has_schema_privilege_id_name,
    fc_has_schema_privilege_id_id,
    fc_has_schema_privilege_name,
    fc_has_schema_privilege_id
);

has_priv_family!(
    types_core::catalog::TYPE_RELATION_ID,
    USAGE_PRIV_MAP,
    convert_type_name,
    fc_has_type_privilege_name_name,
    fc_has_type_privilege_name_id,
    fc_has_type_privilege_id_name,
    fc_has_type_privilege_id_id,
    fc_has_type_privilege_name,
    fc_has_type_privilege_id
);

fn lo_priv_result(fcinfo: &mut Fcinfo, roleid: Oid, lobj: Oid, mode: u64) -> PgResult<Datum> {
    let (result, is_missing) = aclchk_seams::has_lo_priv_byid::call(roleid, lobj, mode)?;
    if is_missing {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_bool(result))
}

fn fc_has_largeobject_privilege_name_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let lobj = fcinfo.arg_oid(1);
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, LARGEOBJECT_PRIV_MAP)?;
    lo_priv_result(fcinfo, roleid, lobj, mode)
}

fn fc_has_largeobject_privilege_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let lobj = fcinfo.arg_oid(0);
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 1)?, LARGEOBJECT_PRIV_MAP)?;
    lo_priv_result(fcinfo, roleid, lobj, mode)
}

fn fc_has_largeobject_privilege_id_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let lobj = fcinfo.arg_oid(1);
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, LARGEOBJECT_PRIV_MAP)?;
    lo_priv_result(fcinfo, roleid, lobj, mode)
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

pub const ACL_BUILTINS: &[FmgrBuiltin] = &[
    b(329, "hash_aclitem", 1, fc_hash_aclitem),
    b(777, "hash_aclitem_extended", 2, fc_hash_aclitem_extended),
    b(1031, "aclitemin", 1, fc_aclitemin),
    b(1032, "aclitemout", 1, fc_aclitemout),
    b(1035, "aclinsert", 2, fc_aclinsert),
    b(1036, "aclremove", 2, fc_aclremove),
    b(1037, "aclcontains", 2, fc_aclcontains),
    b(1062, "aclitem_eq", 2, fc_aclitem_eq),
    b(1365, "makeaclitem", 4, fc_makeaclitem),
    b(1922, "has_table_privilege_name_name", 3, fc_has_table_privilege_name_name),
    b(1923, "has_table_privilege_name_id", 3, fc_has_table_privilege_name_id),
    b(1924, "has_table_privilege_id_name", 3, fc_has_table_privilege_id_name),
    b(1925, "has_table_privilege_id_id", 3, fc_has_table_privilege_id_id),
    b(1926, "has_table_privilege_name", 2, fc_has_table_privilege_name),
    b(1927, "has_table_privilege_id", 2, fc_has_table_privilege_id),
    b(2250, "has_database_privilege_name_name", 3, fc_has_database_privilege_name_name),
    b(2251, "has_database_privilege_name_id", 3, fc_has_database_privilege_name_id),
    b(2252, "has_database_privilege_id_name", 3, fc_has_database_privilege_id_name),
    b(2253, "has_database_privilege_id_id", 3, fc_has_database_privilege_id_id),
    b(2254, "has_database_privilege_name", 2, fc_has_database_privilege_name),
    b(2255, "has_database_privilege_id", 2, fc_has_database_privilege_id),
    b(2256, "has_function_privilege_name_name", 3, fc_has_function_privilege_name_name),
    b(2257, "has_function_privilege_name_id", 3, fc_has_function_privilege_name_id),
    b(2258, "has_function_privilege_id_name", 3, fc_has_function_privilege_id_name),
    b(2259, "has_function_privilege_id_id", 3, fc_has_function_privilege_id_id),
    b(2260, "has_function_privilege_name", 2, fc_has_function_privilege_name),
    b(2261, "has_function_privilege_id", 2, fc_has_function_privilege_id),
    b(2262, "has_language_privilege_name_name", 3, fc_has_language_privilege_name_name),
    b(2263, "has_language_privilege_name_id", 3, fc_has_language_privilege_name_id),
    b(2264, "has_language_privilege_id_name", 3, fc_has_language_privilege_id_name),
    b(2265, "has_language_privilege_id_id", 3, fc_has_language_privilege_id_id),
    b(2266, "has_language_privilege_name", 2, fc_has_language_privilege_name),
    b(2267, "has_language_privilege_id", 2, fc_has_language_privilege_id),
    b(2268, "has_schema_privilege_name_name", 3, fc_has_schema_privilege_name_name),
    b(2269, "has_schema_privilege_name_id", 3, fc_has_schema_privilege_name_id),
    b(2270, "has_schema_privilege_id_name", 3, fc_has_schema_privilege_id_name),
    b(2271, "has_schema_privilege_id_id", 3, fc_has_schema_privilege_id_id),
    b(2272, "has_schema_privilege_name", 2, fc_has_schema_privilege_name),
    b(2273, "has_schema_privilege_id", 2, fc_has_schema_privilege_id),
    b(3138, "has_type_privilege_name_name", 3, fc_has_type_privilege_name_name),
    b(3139, "has_type_privilege_name_id", 3, fc_has_type_privilege_name_id),
    b(3140, "has_type_privilege_id_name", 3, fc_has_type_privilege_id_name),
    b(3141, "has_type_privilege_id_id", 3, fc_has_type_privilege_id_id),
    b(3142, "has_type_privilege_name", 2, fc_has_type_privilege_name),
    b(3143, "has_type_privilege_id", 2, fc_has_type_privilege_id),
    b(6348, "has_largeobject_privilege_name_id", 3, fc_has_largeobject_privilege_name_id),
    b(6349, "has_largeobject_privilege_id", 2, fc_has_largeobject_privilege_id),
    b(6350, "has_largeobject_privilege_id_id", 3, fc_has_largeobject_privilege_id_id),
    b(3943, "acldefault_sql", 2, fc_acldefault_sql),
];
