use syscache_seams::PgTypeTypcacheShape;
use types_core::catalog::{BPCHAROID, INT4OID, TEXTOID, VARCHAROID};
use types_core::InvalidOid;
use types_tuple::NameData;

use crate::{format_type_be, format_type_with_typemod, quote_identifier};

const VARCHARTYPMODOUT: types_core::Oid = 2915;

const CHAROID: types_core::Oid = 18;
const INT4ARRAYOID: types_core::Oid = 1007;
const F_ARRAY_SUBSCRIPT_HANDLER: types_core::Oid = 6179;

fn shape(name: &str, typelem: types_core::Oid, typsubscript: types_core::Oid) -> PgTypeTypcacheShape {
    let mut typname = NameData::default();
    typname.namestrcpy(name);
    PgTypeTypcacheShape {
        typname,
        typlen: 4,
        typbyval: true,
        typalign: b'i' as i8,
        typstorage: if typelem != InvalidOid { b'x' as i8 } else { b'p' as i8 },
        typtype: b'b' as i8,
        typisdefined: true,
        typrelid: InvalidOid,
        typsubscript,
        typelem,
        typarray: InvalidOid,
        typcollation: InvalidOid,
    }
}

fn install_fixture() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_type_typcache_shape::set(|typid| {
            Ok(match typid {
                INT4OID => Some(shape("int4", InvalidOid, InvalidOid)),
                TEXTOID => Some(shape("text", InvalidOid, InvalidOid)),
                CHAROID => Some(shape("char", InvalidOid, InvalidOid)),
                BPCHAROID => Some(shape("bpchar", InvalidOid, InvalidOid)),
                VARCHAROID => Some(shape("varchar", InvalidOid, InvalidOid)),
                INT4ARRAYOID => Some(shape("_int4", INT4OID, F_ARRAY_SUBSCRIPT_HANDLER)),
                20000 => Some(shape("mytype", InvalidOid, InvalidOid)),
                20001 => Some(shape("othertype", InvalidOid, InvalidOid)),
                _ => None,
            })
        });
        syscache_seams::pg_type_io_shape::set(|typid| {
            Ok(Some(syscache_seams::PgTypeIoShape {
                oid: typid,
                typinput: InvalidOid,
                typoutput: InvalidOid,
                typreceive: InvalidOid,
                typsend: InvalidOid,
                typmodin: InvalidOid,
                typmodout: if typid == VARCHAROID { VARCHARTYPMODOUT } else { InvalidOid },
                typelem: InvalidOid,
                typlen: 4,
                typbyval: true,
                typalign: b'i' as i8,
                typdelim: b',' as i8,
                typisdefined: true,
            }))
        });
        // C TypeIsVisible semantics for the fixture: builtins and 20000 are
        // on the path; 20001 (initdb-band oid 13000 nsp) is not.
        namespace_seams::type_is_visible::set(|typid| Ok(typid != 20001));
        namespace_seams::is_temp_namespace::set(|_| false);
        syscache_seams::pg_type_domain_shape::set(|typid| {
            let mut n = types_tuple::NameData::default();
            let name: &[u8] = match typid {
                20001 => b"othertype",
                INT4OID => b"int4",
                TEXTOID => b"text",
                CHAROID => b"char",
                BPCHAROID => b"bpchar",
                VARCHAROID => b"varchar",
                _ => b"mytype",
            };
            n.data[..name.len()].copy_from_slice(name);
            Ok(Some(syscache_seams::PgTypeDomainShape {
                typname: n,
                typnamespace: 13000,
                typtype: b'b' as i8,
                typnotnull: false,
                typbasetype: types_core::InvalidOid,
            }))
        });
        syscache_seams::pg_namespace_nspname::set(|nspid| {
            assert_eq!(nspid, 13000);
            let mut n = types_tuple::NameData::default();
            n.data[..18].copy_from_slice(b"information_schema");
            Ok(Some(n))
        });
        fmgr_seams::fmgr_info::set(|oid| match oid {
            VARCHARTYPMODOUT => Ok(types_fmgr::FmgrInfo::new(
                varchartypmodout_fn,
                VARCHARTYPMODOUT,
                1,
                true,
                false,
            )),
            _ => panic!("fmgr_info: unexpected oid {oid}"),
        });
    });
}

fn varchartypmodout_fn(
    _flinfo: Option<&mut types_fmgr::FmgrInfo>,
    fcinfo: &mut types_fmgr::FunctionCallInfoBaseData,
) -> types_error::PgResult<datum::Datum> {
    let mut s = format!("({})", fcinfo.arg(0).as_i32() - 4).into_bytes();
    s.push(0);
    Ok(datum::Datum::from_usize(
        Box::leak(s.into_boxed_slice()).as_ptr() as usize,
    ))
}

#[test]
fn builtin_special_cases_and_default_arm() {
    install_fixture();
    assert_eq!(format_type_be(INT4OID).unwrap(), "integer");
    assert_eq!(format_type_be(TEXTOID).unwrap(), "text");
    // "char" is a TypeFuncName keyword -> C quote_identifier doubles it.
    assert_eq!(format_type_be(CHAROID).unwrap(), "\"char\"");
    assert_eq!(format_type_be(INT4ARRAYOID).unwrap(), "integer[]");
}

#[test]
fn unknown_oid_is_cache_lookup_error() {
    install_fixture();
    let err = format_type_be(31337).unwrap_err();
    assert_eq!(err.message(), "cache lookup failed for type 31337");
}

#[test]
fn user_type_renders_via_type_is_visible() {
    install_fixture();
    assert_eq!(format_type_be(20000).unwrap(), "mytype");
}

// C qualifies purely on visibility, no OID gate: an initdb-band type whose
// schema is off the search_path renders schema-qualified (the
// information_schema.cardinal_number domain-violation message shape).
#[test]
fn invisible_initdb_band_type_renders_qualified() {
    install_fixture();
    assert_eq!(format_type_be(20001).unwrap(), "information_schema.othertype");
}

#[test]
fn with_typemod_matches_c() {
    install_fixture();
    assert_eq!(format_type_with_typemod(INT4OID, -1).unwrap(), "integer");
    assert_eq!(format_type_with_typemod(TEXTOID, -1).unwrap(), "text");
    assert_eq!(format_type_with_typemod(VARCHAROID, 36).unwrap(), "character varying(32)");
    assert_eq!(format_type_with_typemod(TEXTOID, 5).unwrap(), "text(5)");
    // bpchar with TYPEMOD_GIVEN and typemod -1 renders the raw catalog name.
    assert_eq!(format_type_with_typemod(BPCHAROID, -1).unwrap(), "bpchar");
    assert_eq!(format_type_be(BPCHAROID).unwrap(), "character");
}

#[test]
fn quote_identifier_matches_ruleutils() {
    assert_eq!(quote_identifier("text"), "text");
    assert_eq!(quote_identifier("mixedCase"), "\"mixedCase\"");
    assert_eq!(quote_identifier("select"), "\"select\"");
    assert_eq!(quote_identifier("has space"), "\"has space\"");
    assert_eq!(quote_identifier("qu\"ote"), "\"qu\"\"ote\"");
    // unreserved keywords stay bare; col-name keywords are quoted.
    assert_eq!(quote_identifier("abort"), "abort");
    assert_eq!(quote_identifier("interval"), "\"interval\"");
}
