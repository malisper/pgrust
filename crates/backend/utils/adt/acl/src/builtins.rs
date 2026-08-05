use datum::Datum;
use types_core::{Oid, BOOLOID, OIDOID, RECORDOID, TEXTOID};
use types_error::{PgError, PgResult, ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_UNDEFINED_TABLE};
use types_fmgr::{
    byref_result, cstring_result, varlena_result, FmgrBuiltin, FmgrInfo,
    FunctionCallInfoBaseData as Fcinfo, PGFunction, ACLITEM_LEN,
};

use crate::ops::{convert_any_priv_string, PrivMapEntry};
use crate::varlena::acl_image;
use crate::{
    acl_grant_option_for, acldefault, aclitem_get_goptions, aclitem_get_privs,
    aclitem_set_privs_goptions, get_role_oid_or_public, AclItem, AclObjectType, ACL_ALTER_SYSTEM,
    ACL_CONNECT, ACL_CREATE, ACL_CREATE_TEMP, ACL_DELETE, ACL_EXECUTE, ACL_INSERT, ACL_MAINTAIN,
    ACL_NO_RIGHTS, ACL_REFERENCES, ACL_SELECT, ACL_SET, ACL_TRIGGER, ACL_TRUNCATE, ACL_UPDATE,
    ACL_USAGE, N_ACL_RIGHTS,
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

// pub for the proofs suite (proofs/state-seam-probe); not part of the crate API.
pub fn fc_aclitem_eq(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a1 = arg_aclitem(fcinfo, 0);
    let a2 = arg_aclitem(fcinfo, 1);
    Ok(Datum::from_bool(a1 == a2))
}

// pub for the proofs suite (proofs/state-seam-probe); not part of the crate API.
pub fn fc_hash_aclitem(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let a = arg_aclitem(fcinfo, 0);
    let sum = (a.ai_privs as u32)
        .wrapping_add(a.ai_grantee)
        .wrapping_add(a.ai_grantor);
    Ok(Datum::from_i32(sum as i32))
}

// pub for the proofs suite (proofs/state-seam-probe); not part of the crate API.
pub fn fc_hash_aclitem_extended(_flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
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

#[track_caller]
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
    convert_table_name_str(fcinfo.result_mcx(), arg_text_str(fcinfo, i)?)
}

// textToQualifiedNameList + makeRangeVarFromNameList + no-lock RangeVarGetRelid.
pub fn convert_table_name_str(mcx: mcx::Mcx<'_>, rawname: &str) -> PgResult<Oid> {
    use types_error::ERRCODE_INVALID_NAME;
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

#[track_caller]
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


pub(crate) const TABLESPACE_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "CREATE", value: crate::ACL_CREATE },
    PrivMapEntry { name: "CREATE WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_CREATE) },
];

pub(crate) const SEQUENCE_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "USAGE", value: crate::ACL_USAGE },
    PrivMapEntry { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_USAGE) },
    PrivMapEntry { name: "SELECT", value: ACL_SELECT },
    PrivMapEntry { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
    PrivMapEntry { name: "UPDATE", value: ACL_UPDATE },
    PrivMapEntry { name: "UPDATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_UPDATE) },
];

pub(crate) const PARAMETER_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "SET", value: crate::ACL_SET },
    PrivMapEntry { name: "SET WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_SET) },
    PrivMapEntry { name: "ALTER SYSTEM", value: crate::ACL_ALTER_SYSTEM },
    PrivMapEntry {
        name: "ALTER SYSTEM WITH GRANT OPTION",
        value: acl_grant_option_for(crate::ACL_ALTER_SYSTEM),
    },
];

// MEMBER has no ACL bit; ACL_CREATE stands in, shared only with pg_role_aclcheck.
pub(crate) const ROLE_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "USAGE", value: crate::ACL_USAGE },
    PrivMapEntry { name: "MEMBER", value: crate::ACL_CREATE },
    PrivMapEntry { name: "SET", value: crate::ACL_SET },
    PrivMapEntry { name: "USAGE WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_CREATE) },
    PrivMapEntry { name: "USAGE WITH ADMIN OPTION", value: acl_grant_option_for(crate::ACL_CREATE) },
    PrivMapEntry { name: "MEMBER WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_CREATE) },
    PrivMapEntry { name: "MEMBER WITH ADMIN OPTION", value: acl_grant_option_for(crate::ACL_CREATE) },
    PrivMapEntry { name: "SET WITH GRANT OPTION", value: acl_grant_option_for(crate::ACL_CREATE) },
    PrivMapEntry { name: "SET WITH ADMIN OPTION", value: acl_grant_option_for(crate::ACL_CREATE) },
];

fn convert_tablespace_name(fcinfo: &Fcinfo, i: usize) -> PgResult<Oid> {
    tablespace_seams::get_tablespace_oid::call(fcinfo.result_mcx(), arg_text_str(fcinfo, i)?, false)
}

fn convert_foreign_data_wrapper_name(fcinfo: &Fcinfo, i: usize) -> PgResult<Oid> {
    foreigncmds_seams::get_foreign_data_wrapper_oid::call(arg_text_str(fcinfo, i)?, false)
}

fn convert_server_name(fcinfo: &Fcinfo, i: usize) -> PgResult<Oid> {
    foreigncmds_seams::get_foreign_server_oid::call(arg_text_str(fcinfo, i)?, false)
}

has_priv_family!(
    types_core::catalog::TABLE_SPACE_RELATION_ID,
    TABLESPACE_PRIV_MAP,
    convert_tablespace_name,
    fc_has_tablespace_privilege_name_name,
    fc_has_tablespace_privilege_name_id,
    fc_has_tablespace_privilege_id_name,
    fc_has_tablespace_privilege_id_id,
    fc_has_tablespace_privilege_name,
    fc_has_tablespace_privilege_id
);

has_priv_family!(
    types_core::catalog::FOREIGN_DATA_WRAPPER_RELATION_ID,
    USAGE_PRIV_MAP,
    convert_foreign_data_wrapper_name,
    fc_has_fdw_privilege_name_name,
    fc_has_fdw_privilege_name_id,
    fc_has_fdw_privilege_id_name,
    fc_has_fdw_privilege_id_id,
    fc_has_fdw_privilege_name,
    fc_has_fdw_privilege_id
);

has_priv_family!(
    types_core::catalog::FOREIGN_SERVER_RELATION_ID,
    USAGE_PRIV_MAP,
    convert_server_name,
    fc_has_server_privilege_name_name,
    fc_has_server_privilege_name_id,
    fc_has_server_privilege_id_name,
    fc_has_server_privilege_id_id,
    fc_has_server_privilege_name,
    fc_has_server_privilege_id
);

fn convert_sequence_priv_string(priv_type: &str) -> PgResult<u64> {
    convert_any_priv_string(priv_type, SEQUENCE_PRIV_MAP)
}

#[track_caller]
#[cold]
#[inline(never)]
fn not_a_sequence(name: &str) -> Box<PgError> {
    Box::new(
        PgError::error(format!("\"{name}\" is not a sequence"))
            .with_sqlstate(types_error::ERRCODE_WRONG_OBJECT_TYPE),
    )
}

fn sequence_priv_byname(fcinfo: &Fcinfo, roleid: Oid, nameidx: usize, mode: u64) -> PgResult<Datum> {
    let sequenceoid = convert_table_name(fcinfo, nameidx)?;
    if lsyscache::get_rel_relkind(sequenceoid)? as u8 != types_rel::pg_class::RELKIND_SEQUENCE {
        return Err(not_a_sequence(arg_text_str(fcinfo, nameidx)?));
    }
    table_priv_check(roleid, sequenceoid, mode)
}

fn sequence_priv_byid(fcinfo: &mut Fcinfo, roleid: Oid, oididx: usize, mode: u64) -> PgResult<Datum> {
    let sequenceoid = fcinfo.arg_oid(oididx);
    let relkind = lsyscache::get_rel_relkind(sequenceoid)? as u8;
    if relkind == 0 {
        return Ok(fcinfo.return_null());
    }
    if relkind != types_rel::pg_class::RELKIND_SEQUENCE {
        let relname = syscache_seams::pg_class_relname::call(sequenceoid)?
            .map(|n| String::from_utf8_lossy(n.name_str()).into_owned())
            .unwrap_or_default();
        return Err(not_a_sequence(&relname));
    }
    table_priv_check_ext(fcinfo, roleid, sequenceoid, mode)
}

fn fc_has_sequence_privilege_name_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let mode = convert_sequence_priv_string(arg_text_str(fcinfo, 2)?)?;
    sequence_priv_byname(fcinfo, roleid, 1, mode)
}

fn fc_has_sequence_privilege_name_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let mode = convert_sequence_priv_string(arg_text_str(fcinfo, 2)?)?;
    sequence_priv_byid(fcinfo, roleid, 1, mode)
}

fn fc_has_sequence_privilege_id_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let mode = convert_sequence_priv_string(arg_text_str(fcinfo, 2)?)?;
    sequence_priv_byname(fcinfo, roleid, 1, mode)
}

fn fc_has_sequence_privilege_id_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let mode = convert_sequence_priv_string(arg_text_str(fcinfo, 2)?)?;
    sequence_priv_byid(fcinfo, roleid, 1, mode)
}

fn fc_has_sequence_privilege_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let mode = convert_sequence_priv_string(arg_text_str(fcinfo, 1)?)?;
    sequence_priv_byname(fcinfo, roleid, 0, mode)
}

fn fc_has_sequence_privilege_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let mode = convert_sequence_priv_string(arg_text_str(fcinfo, 1)?)?;
    sequence_priv_byid(fcinfo, roleid, 0, mode)
}

fn parameter_priv_check(fcinfo: &Fcinfo, roleid: Oid, paramidx: usize, mode: u64) -> PgResult<Datum> {
    let r = aclchk_seams::pg_parameter_aclcheck::call(arg_text_str(fcinfo, paramidx)?, roleid, mode)?;
    Ok(Datum::from_bool(r == ACLCHECK_OK))
}

fn fc_has_parameter_privilege_name_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, PARAMETER_PRIV_MAP)?;
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    parameter_priv_check(fcinfo, roleid, 1, mode)
}

fn fc_has_parameter_privilege_id_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, PARAMETER_PRIV_MAP)?;
    parameter_priv_check(fcinfo, roleid, 1, mode)
}

fn fc_has_parameter_privilege_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 1)?, PARAMETER_PRIV_MAP)?;
    let roleid = miscinit_seams::get_user_id::call();
    parameter_priv_check(fcinfo, roleid, 0, mode)
}

const ACLCHECK_NO_PRIV: i32 = 1;

fn pg_role_aclcheck(role_oid: Oid, roleid: Oid, mode: u64) -> PgResult<i32> {
    if mode & acl_grant_option_for(crate::ACL_CREATE) != 0
        && crate::is_admin_of_role(roleid, role_oid)?
    {
        return Ok(ACLCHECK_OK);
    }
    if mode & crate::ACL_CREATE != 0 && crate::is_member_of_role(roleid, role_oid)? {
        return Ok(ACLCHECK_OK);
    }
    if mode & crate::ACL_USAGE != 0 && crate::has_privs_of_role(roleid, role_oid)? {
        return Ok(ACLCHECK_OK);
    }
    if mode & crate::ACL_SET != 0 && crate::member_can_set_role(roleid, role_oid)? {
        return Ok(ACLCHECK_OK);
    }
    Ok(ACLCHECK_NO_PRIV)
}

fn role_priv_result(role_oid: Oid, roleid: Oid, mode: u64) -> PgResult<Datum> {
    Ok(Datum::from_bool(pg_role_aclcheck(role_oid, roleid, mode)? == ACLCHECK_OK))
}

fn fc_pg_has_role_name_name(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let roleid = crate::get_role_oid(arg_name_str(fcinfo, 0)?, false)?;
    let role_oid = crate::get_role_oid(arg_name_str(fcinfo, 1)?, false)?;
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, ROLE_PRIV_MAP)?;
    role_priv_result(role_oid, roleid, mode)
}

fn fc_pg_has_role_name_id(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let roleid = crate::get_role_oid(arg_name_str(fcinfo, 0)?, false)?;
    let role_oid = fcinfo.arg_oid(1);
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, ROLE_PRIV_MAP)?;
    role_priv_result(role_oid, roleid, mode)
}

fn fc_pg_has_role_id_name(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let role_oid = crate::get_role_oid(arg_name_str(fcinfo, 1)?, false)?;
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, ROLE_PRIV_MAP)?;
    role_priv_result(role_oid, roleid, mode)
}

fn fc_pg_has_role_id_id(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let role_oid = fcinfo.arg_oid(1);
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 2)?, ROLE_PRIV_MAP)?;
    role_priv_result(role_oid, roleid, mode)
}

fn fc_pg_has_role_name(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let role_oid = crate::get_role_oid(arg_name_str(fcinfo, 0)?, false)?;
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 1)?, ROLE_PRIV_MAP)?;
    role_priv_result(role_oid, roleid, mode)
}

fn fc_pg_has_role_id(_f: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let role_oid = fcinfo.arg_oid(0);
    let mode = convert_any_priv_string(arg_text_str(fcinfo, 1)?, ROLE_PRIV_MAP)?;
    role_priv_result(role_oid, roleid, mode)
}

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

const COLUMN_PRIV_MAP: &[PrivMapEntry] = &[
    PrivMapEntry { name: "SELECT", value: ACL_SELECT },
    PrivMapEntry { name: "SELECT WITH GRANT OPTION", value: acl_grant_option_for(ACL_SELECT) },
    PrivMapEntry { name: "INSERT", value: ACL_INSERT },
    PrivMapEntry { name: "INSERT WITH GRANT OPTION", value: acl_grant_option_for(ACL_INSERT) },
    PrivMapEntry { name: "UPDATE", value: ACL_UPDATE },
    PrivMapEntry { name: "UPDATE WITH GRANT OPTION", value: acl_grant_option_for(ACL_UPDATE) },
    PrivMapEntry { name: "REFERENCES", value: ACL_REFERENCES },
    PrivMapEntry {
        name: "REFERENCES WITH GRANT OPTION",
        value: acl_grant_option_for(ACL_REFERENCES),
    },
];

fn convert_column_priv_string(priv_type: &str) -> PgResult<u64> {
    convert_any_priv_string(priv_type, COLUMN_PRIV_MAP)
}

// convert_column_name (acl.c): InvalidAttrNumber (0) for a dropped column or
// vanished table, error for a bad column name on a live table.
fn convert_column_name(fcinfo: &Fcinfo, tableoid: Oid, i: usize) -> PgResult<i16> {
    use cache_syscache::cacheinfo::ATTNAME;
    use cache_syscache::{ReleaseSysCache, SearchSysCache2, SysCacheGetAttrNotNull, SysCacheKey};
    const ANUM_PG_ATTRIBUTE_ATTNUM: i32 = 5;
    const ANUM_PG_ATTRIBUTE_ATTISDROPPED: i32 = 17;

    let colname = arg_text_str(fcinfo, i)?;
    match SearchSysCache2(
        ATTNAME,
        SysCacheKey::Value(Datum::from_oid(tableoid)),
        SysCacheKey::Str(colname),
    )? {
        Some(tuple) => {
            let dropped =
                SysCacheGetAttrNotNull(ATTNAME, &tuple, ANUM_PG_ATTRIBUTE_ATTISDROPPED)?.as_bool();
            let attnum = if dropped {
                0
            } else {
                SysCacheGetAttrNotNull(ATTNAME, &tuple, ANUM_PG_ATTRIBUTE_ATTNUM)?.as_i16()
            };
            ReleaseSysCache(tuple);
            Ok(attnum)
        }
        None => {
            if let Some(relname) = syscache_seams::pg_class_relname::call(tableoid)? {
                let relname = core::str::from_utf8(relname.name_str()).unwrap_or("");
                return Err(Box::new(
                    PgError::error(format!(
                        "column \"{colname}\" of relation \"{relname}\" does not exist"
                    ))
                    .with_sqlstate(types_error::ERRCODE_UNDEFINED_COLUMN),
                ));
            }
            Ok(0)
        }
    }
}

// column_privilege_check (acl.c): 1 has, 0 lacks, -1 dropped/missing → NULL.
fn column_privilege_check(tableoid: Oid, attnum: i16, roleid: Oid, mode: u64) -> PgResult<i32> {
    if attnum == 0 {
        return Ok(-1);
    }
    let (aclresult, is_missing) =
        aclchk_seams::pg_attribute_aclcheck_ext::call(tableoid, attnum, roleid, mode)?;
    if aclresult == ACLCHECK_OK {
        return Ok(1);
    }
    if is_missing {
        return Ok(-1);
    }
    let (aclresult, is_missing) =
        aclchk_seams::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if aclresult == ACLCHECK_OK {
        Ok(1)
    } else if is_missing {
        Ok(-1)
    } else {
        Ok(0)
    }
}

fn column_priv_result(fcinfo: &mut Fcinfo, privresult: i32) -> PgResult<Datum> {
    if privresult < 0 {
        return Ok(fcinfo.return_null());
    }
    Ok(Datum::from_bool(privresult > 0))
}

fn fc_has_column_privilege_name_name_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let tableoid = convert_table_name(fcinfo, 1)?;
    let colattnum = convert_column_name(fcinfo, tableoid, 2)?;
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 3)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_name_name_attnum(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let tableoid = convert_table_name(fcinfo, 1)?;
    let colattnum = fcinfo.arg_i16(2);
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 3)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_name_id_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let tableoid = fcinfo.arg_oid(1);
    let colattnum = convert_column_name(fcinfo, tableoid, 2)?;
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 3)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_name_id_attnum(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let tableoid = fcinfo.arg_oid(1);
    let colattnum = fcinfo.arg_i16(2);
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 3)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_id_name_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let tableoid = convert_table_name(fcinfo, 1)?;
    let colattnum = convert_column_name(fcinfo, tableoid, 2)?;
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 3)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_id_name_attnum(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let tableoid = convert_table_name(fcinfo, 1)?;
    let colattnum = fcinfo.arg_i16(2);
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 3)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_id_id_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let tableoid = fcinfo.arg_oid(1);
    let colattnum = convert_column_name(fcinfo, tableoid, 2)?;
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 3)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_id_id_attnum(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let tableoid = fcinfo.arg_oid(1);
    let colattnum = fcinfo.arg_i16(2);
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 3)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_name_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let tableoid = convert_table_name(fcinfo, 0)?;
    let colattnum = convert_column_name(fcinfo, tableoid, 1)?;
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 2)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_name_attnum(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let tableoid = convert_table_name(fcinfo, 0)?;
    let colattnum = fcinfo.arg_i16(1);
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 2)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_id_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let tableoid = fcinfo.arg_oid(0);
    let colattnum = convert_column_name(fcinfo, tableoid, 1)?;
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 2)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn fc_has_column_privilege_id_attnum(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let tableoid = fcinfo.arg_oid(0);
    let colattnum = fcinfo.arg_i16(1);
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 2)?)?;
    let r = column_privilege_check(tableoid, colattnum, roleid, mode)?;
    column_priv_result(fcinfo, r)
}

fn any_column_priv_check(roleid: Oid, tableoid: Oid, mode: u64) -> PgResult<Datum> {
    let mut aclresult = aclchk_seams::pg_class_aclmask::call(tableoid, roleid, mode, false)
        .map(|m| if m != 0 { ACLCHECK_OK } else { 1 })?;
    if aclresult != ACLCHECK_OK {
        aclresult = aclchk_seams::pg_attribute_aclcheck_all::call(tableoid, roleid, mode, false)?;
    }
    Ok(Datum::from_bool(aclresult == ACLCHECK_OK))
}

fn any_column_priv_check_ext(
    fcinfo: &mut Fcinfo,
    roleid: Oid,
    tableoid: Oid,
    mode: u64,
) -> PgResult<Datum> {
    let (mut aclresult, is_missing) =
        aclchk_seams::pg_class_aclcheck_ext::call(tableoid, roleid, mode)?;
    if aclresult != ACLCHECK_OK {
        if is_missing {
            return Ok(fcinfo.return_null());
        }
        let (r, is_missing) =
            aclchk_seams::pg_attribute_aclcheck_all_ext::call(tableoid, roleid, mode, false)?;
        if is_missing {
            return Ok(fcinfo.return_null());
        }
        aclresult = r;
    }
    Ok(Datum::from_bool(aclresult == ACLCHECK_OK))
}

fn fc_has_any_column_privilege_name_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let tableoid = convert_table_name(fcinfo, 1)?;
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 2)?)?;
    any_column_priv_check(roleid, tableoid, mode)
}

fn fc_has_any_column_privilege_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let tableoid = convert_table_name(fcinfo, 0)?;
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 1)?)?;
    any_column_priv_check(roleid, tableoid, mode)
}

fn fc_has_any_column_privilege_name_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = get_role_oid_or_public(arg_name_str(fcinfo, 0)?)?;
    let tableoid = fcinfo.arg_oid(1);
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 2)?)?;
    any_column_priv_check_ext(fcinfo, roleid, tableoid, mode)
}

fn fc_has_any_column_privilege_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = miscinit_seams::get_user_id::call();
    let tableoid = fcinfo.arg_oid(0);
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 1)?)?;
    any_column_priv_check_ext(fcinfo, roleid, tableoid, mode)
}

fn fc_has_any_column_privilege_id_name(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let tableoid = convert_table_name(fcinfo, 1)?;
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 2)?)?;
    any_column_priv_check(roleid, tableoid, mode)
}

fn fc_has_any_column_privilege_id_id(
    _f: Option<&mut FmgrInfo>,
    fcinfo: &mut Fcinfo,
) -> PgResult<Datum> {
    let roleid = fcinfo.arg_oid(0);
    let tableoid = fcinfo.arg_oid(1);
    let mode = convert_column_priv_string(arg_text_str(fcinfo, 2)?)?;
    any_column_priv_check_ext(fcinfo, roleid, tableoid, mode)
}

pub(crate) fn convert_aclright_to_string(aclright: u64) -> &'static str {
    match aclright {
        ACL_INSERT => "INSERT",
        ACL_SELECT => "SELECT",
        ACL_UPDATE => "UPDATE",
        ACL_DELETE => "DELETE",
        ACL_TRUNCATE => "TRUNCATE",
        ACL_REFERENCES => "REFERENCES",
        ACL_TRIGGER => "TRIGGER",
        ACL_EXECUTE => "EXECUTE",
        ACL_USAGE => "USAGE",
        ACL_CREATE => "CREATE",
        ACL_CREATE_TEMP => "TEMPORARY",
        ACL_CONNECT => "CONNECT",
        ACL_SET => "SET",
        ACL_ALTER_SYSTEM => "ALTER SYSTEM",
        ACL_MAINTAIN => "MAINTAIN",
        _ => panic!("unrecognized aclright: {aclright}"),
    }
}

struct AclExplodeRows {
    tuples: Vec<Vec<u8>>,
}

fn collect_aclexplode_rows(fcinfo: &Fcinfo) -> PgResult<AclExplodeRows> {
    let mcx = fcinfo.result_mcx();
    // SAFETY: strict fn, arg 0 is a non-null aclitem[] varlena.
    let v = unsafe { fcinfo.arg_varlena_packed(0) }?;
    let n = crate::varlena::check_acl_payload(v.data())?;

    let mut desc = tupdesc::CreateTemplateTupleDesc(mcx, 4)?;
    tupdesc::TupleDescInitEntry(&mut desc, 1, Some("grantor"), OIDOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 2, Some("grantee"), OIDOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 3, Some("privilege_type"), TEXTOID, -1, 0)?;
    tupdesc::TupleDescInitEntry(&mut desc, 4, Some("is_grantable"), BOOLOID, -1, 0)?;
    desc.tdtypeid = RECORDOID;
    typcache_seams::assign_record_type_typmod::call(&mut desc)?;

    let mut tuples = Vec::new();
    for i in 0..n {
        let item = crate::varlena::read_acl_item(v.data(), i);
        for right in 0..N_ACL_RIGHTS {
            let priv_bit = 1u64 << right;
            if aclitem_get_privs(&item) & priv_bit == 0 {
                continue;
            }
            let ptext = varlena_result(varlena::cstring_to_text(
                mcx,
                convert_aclright_to_string(priv_bit).as_bytes(),
            )?);
            let values = [
                Datum::from_oid(item.ai_grantor),
                Datum::from_oid(item.ai_grantee),
                ptext,
                Datum::from_bool(aclitem_get_goptions(&item) & priv_bit != 0),
            ];
            let tuple = heaptuple::heap_form_tuple(mcx, &desc, &values, &[false; 4])?;
            tuples.push(tuple.image().to_vec());
        }
    }
    Ok(AclExplodeRows { tuples })
}

fn fc_aclexplode(flinfo: Option<&mut FmgrInfo>, fcinfo: &mut Fcinfo) -> PgResult<Datum> {
    let flinfo = flinfo.expect("aclexplode: resolved FmgrInfo required");
    if !flinfo.has_fn_extra() {
        let rows = collect_aclexplode_rows(fcinfo)?;
        let fctx = funcapi_srf::init_MultiFuncCall(flinfo, fcinfo)?;
        fctx.user_fctx = Some(Box::new(rows));
    }
    let fctx = funcapi_srf::per_MultiFuncCall(flinfo);
    let idx = fctx.call_cntr as usize;
    let rows = fctx
        .user_fctx
        .as_ref()
        .expect("aclexplode: rows set at first call")
        .downcast_ref::<AclExplodeRows>()
        .expect("aclexplode: user_fctx is AclExplodeRows");
    match rows.tuples.get(idx) {
        Some(img) => {
            let d = byref_result(fcinfo.result_mcx(), img)?;
            Ok(funcapi_srf::srf_return_next(flinfo, fcinfo, d))
        }
        None => Ok(funcapi_srf::srf_return_done(flinfo, fcinfo)),
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
    FmgrBuiltin {
        foid: 1689,
        name: "aclexplode",
        nargs: 1,
        strict: true,
        retset: true,
        func: fc_aclexplode,
    },
    b(2181, "has_sequence_privilege_name_name", 3, fc_has_sequence_privilege_name_name),
    b(2182, "has_sequence_privilege_name_id", 3, fc_has_sequence_privilege_name_id),
    b(2183, "has_sequence_privilege_id_name", 3, fc_has_sequence_privilege_id_name),
    b(2184, "has_sequence_privilege_id_id", 3, fc_has_sequence_privilege_id_id),
    b(2185, "has_sequence_privilege_name", 2, fc_has_sequence_privilege_name),
    b(2186, "has_sequence_privilege_id", 2, fc_has_sequence_privilege_id),
    b(2390, "has_tablespace_privilege_name_name", 3, fc_has_tablespace_privilege_name_name),
    b(2391, "has_tablespace_privilege_name_id", 3, fc_has_tablespace_privilege_name_id),
    b(2392, "has_tablespace_privilege_id_name", 3, fc_has_tablespace_privilege_id_name),
    b(2393, "has_tablespace_privilege_id_id", 3, fc_has_tablespace_privilege_id_id),
    b(2394, "has_tablespace_privilege_name", 2, fc_has_tablespace_privilege_name),
    b(2395, "has_tablespace_privilege_id", 2, fc_has_tablespace_privilege_id),
    b(2705, "pg_has_role_name_name", 3, fc_pg_has_role_name_name),
    b(2706, "pg_has_role_name_id", 3, fc_pg_has_role_name_id),
    b(2707, "pg_has_role_id_name", 3, fc_pg_has_role_id_name),
    b(2708, "pg_has_role_id_id", 3, fc_pg_has_role_id_id),
    b(2709, "pg_has_role_name", 2, fc_pg_has_role_name),
    b(2710, "pg_has_role_id", 2, fc_pg_has_role_id),
    b(3000, "has_foreign_data_wrapper_privilege_name_name", 3, fc_has_fdw_privilege_name_name),
    b(3001, "has_foreign_data_wrapper_privilege_name_id", 3, fc_has_fdw_privilege_name_id),
    b(3002, "has_foreign_data_wrapper_privilege_id_name", 3, fc_has_fdw_privilege_id_name),
    b(3003, "has_foreign_data_wrapper_privilege_id_id", 3, fc_has_fdw_privilege_id_id),
    b(3004, "has_foreign_data_wrapper_privilege_name", 2, fc_has_fdw_privilege_name),
    b(3005, "has_foreign_data_wrapper_privilege_id", 2, fc_has_fdw_privilege_id),
    b(3006, "has_server_privilege_name_name", 3, fc_has_server_privilege_name_name),
    b(3007, "has_server_privilege_name_id", 3, fc_has_server_privilege_name_id),
    b(3008, "has_server_privilege_id_name", 3, fc_has_server_privilege_id_name),
    b(3009, "has_server_privilege_id_id", 3, fc_has_server_privilege_id_id),
    b(3010, "has_server_privilege_name", 2, fc_has_server_privilege_name),
    b(3011, "has_server_privilege_id", 2, fc_has_server_privilege_id),
    b(6205, "has_parameter_privilege_name_name", 3, fc_has_parameter_privilege_name_name),
    b(6206, "has_parameter_privilege_id_name", 3, fc_has_parameter_privilege_id_name),
    b(6207, "has_parameter_privilege_name", 2, fc_has_parameter_privilege_name),
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
    b(3012, "has_column_privilege_name_name_name", 4, fc_has_column_privilege_name_name_name),
    b(3013, "has_column_privilege_name_name_attnum", 4, fc_has_column_privilege_name_name_attnum),
    b(3014, "has_column_privilege_name_id_name", 4, fc_has_column_privilege_name_id_name),
    b(3015, "has_column_privilege_name_id_attnum", 4, fc_has_column_privilege_name_id_attnum),
    b(3016, "has_column_privilege_id_name_name", 4, fc_has_column_privilege_id_name_name),
    b(3017, "has_column_privilege_id_name_attnum", 4, fc_has_column_privilege_id_name_attnum),
    b(3018, "has_column_privilege_id_id_name", 4, fc_has_column_privilege_id_id_name),
    b(3019, "has_column_privilege_id_id_attnum", 4, fc_has_column_privilege_id_id_attnum),
    b(3020, "has_column_privilege_name_name", 3, fc_has_column_privilege_name_name),
    b(3021, "has_column_privilege_name_attnum", 3, fc_has_column_privilege_name_attnum),
    b(3022, "has_column_privilege_id_name", 3, fc_has_column_privilege_id_name),
    b(3023, "has_column_privilege_id_attnum", 3, fc_has_column_privilege_id_attnum),
    b(3024, "has_any_column_privilege_name_name", 3, fc_has_any_column_privilege_name_name),
    b(3025, "has_any_column_privilege_name_id", 3, fc_has_any_column_privilege_name_id),
    b(3026, "has_any_column_privilege_id_name", 3, fc_has_any_column_privilege_id_name),
    b(3027, "has_any_column_privilege_id_id", 3, fc_has_any_column_privilege_id_id),
    b(3028, "has_any_column_privilege_name", 2, fc_has_any_column_privilege_name),
    b(3029, "has_any_column_privilege_id", 2, fc_has_any_column_privilege_id),
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
