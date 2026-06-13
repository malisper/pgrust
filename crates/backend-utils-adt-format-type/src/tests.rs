//! Unit tests for the `format_type.c` port.
//!
//! The catalog / fmgr / encoding / numeric call-outs are cross-crate `seam!`
//! slots (process-global `OnceLock`s). All tests run under one serialization
//! mutex and install deterministic mocks once. The mock catalog is keyed by OID
//! through a test-serialized static table.

extern crate std;

use super::*;
use alloc::borrow::ToOwned;
use mcx::MemoryContext;
use std::sync::Mutex;
use std::vec;
use std::vec::Vec;

// A minimal catalog the seam mocks resolve against. Each entry is the
// `TypeFormInfo` fields (typename is owned as a `&'static str`, copied into the
// caller's mcx by the mock). Serialized by TEST_LOCK.
#[derive(Clone)]
struct MockType {
    typelem: Oid,
    typsubscript: Oid,
    typstorage: i8,
    typmodout: Oid,
    typnamespace: Oid,
    typname: &'static str,
}

static MOCK_TYPES: Mutex<Vec<(Oid, MockType)>> = Mutex::new(Vec::new());
static TEST_LOCK: Mutex<()> = Mutex::new(());

fn mock_type_form<'mcx>(mcx: Mcx<'mcx>, type_oid: Oid) -> PgResult<Option<TypeFormInfo<'mcx>>> {
    let table = MOCK_TYPES.lock().unwrap();
    match table.iter().find(|(oid, _)| *oid == type_oid) {
        None => Ok(None),
        Some((_, m)) => Ok(Some(TypeFormInfo {
            typelem: m.typelem,
            typsubscript: m.typsubscript,
            typstorage: m.typstorage,
            typmodout: m.typmodout,
            typnamespace: m.typnamespace,
            typname: PgString::from_str_in(m.typname, mcx)?,
        })),
    }
}

fn mock_type_is_visible(_mcx: Mcx<'_>, _type_oid: Oid) -> PgResult<bool> {
    Ok(true)
}

fn mock_namespace_name_or_temp<'mcx>(
    mcx: Mcx<'mcx>,
    _nspid: Oid,
) -> PgResult<Option<PgString<'mcx>>> {
    Ok(Some(PgString::from_str_in("public", mcx)?))
}

fn mock_quote_qualified_identifier<'mcx>(
    mcx: Mcx<'mcx>,
    qualifier: Option<&str>,
    ident: &str,
) -> PgResult<PgString<'mcx>> {
    // Faithful enough for tests: prefix the schema with a dot when present.
    let mut s = PgString::new_in(mcx);
    if let Some(q) = qualifier {
        s.try_push_str(q)?;
        s.try_push('.')?;
    }
    s.try_push_str(ident)?;
    Ok(s)
}

fn mock_typmod_out<'mcx>(mcx: Mcx<'mcx>, _typmodout: Oid, typmod: i32) -> PgResult<PgString<'mcx>> {
    // Stand in for a type-specific typmodout: "(N)".
    let mut s = PgString::from_str_in("(", mcx)?;
    push_i32(&mut s, typmod)?;
    s.try_push(')')?;
    Ok(s)
}

fn mock_database_encoding_max_length() -> i32 {
    4 // UTF8
}

fn mock_numeric_maximum_size(typmod: i32) -> i32 {
    // Not exercised by these tests beyond presence; mirror numeric.c's -1 for
    // an unconstrained typmod and a plausible value otherwise.
    if typmod < 0 {
        -1
    } else {
        typmod
    }
}

/// Install the deterministic mocks. Idempotent across the process (the same fn
/// ptrs each time), but `OnceLock::set` panics on re-set, so guard with a flag.
fn install_mocks() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        backend_utils_cache_syscache_seams::type_form::set(mock_type_form);
        backend_catalog_namespace_seams::type_is_visible::set(mock_type_is_visible);
        backend_utils_cache_lsyscache_seams::get_namespace_name_or_temp::set(
            mock_namespace_name_or_temp,
        );
        backend_utils_adt_ruleutils_seams::quote_qualified_identifier::set(
            mock_quote_qualified_identifier,
        );
        backend_utils_fmgr_fmgr_seams::typmod_out::set(mock_typmod_out);
        backend_utils_mb_mbutils_seams::pg_database_encoding_max_length::set(
            mock_database_encoding_max_length,
        );
        backend_utils_adt_numeric_seams::numeric_maximum_size::set(mock_numeric_maximum_size);
    });
}

fn type_form(
    typelem: Oid,
    typsubscript: Oid,
    typstorage: i8,
    typmodout: Oid,
    typname: &'static str,
) -> MockType {
    MockType {
        typelem,
        typsubscript,
        typstorage,
        typmodout,
        typnamespace: 2200, // pg_catalog-ish
        typname,
    }
}

fn set_catalog(entries: Vec<(Oid, MockType)>) {
    *MOCK_TYPES.lock().unwrap() = entries;
}

#[test]
fn special_case_scalar_names() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    set_catalog(vec![
        (BOOLOID, type_form(0, 0, b'p' as i8, 0, "bool")),
        (INT4OID, type_form(0, 0, b'p' as i8, 0, "int4")),
        (FLOAT8OID, type_form(0, 0, b'p' as i8, 0, "float8")),
        (JSONOID, type_form(0, 0, b'x' as i8, 0, "json")),
    ]);

    assert_eq!(
        format_type_extended(mcx, BOOLOID, -1, 0)
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("boolean")
    );
    assert_eq!(
        format_type_extended(mcx, INT4OID, -1, 0)
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("integer")
    );
    assert_eq!(
        format_type_extended(mcx, FLOAT8OID, -1, 0)
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("double precision")
    );
    assert_eq!(
        format_type_extended(mcx, JSONOID, -1, 0)
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("json")
    );
}

#[test]
fn typmod_decoration_default_and_via_typmodout() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    // numeric: typmodout = InvalidOid -> default "(N)" decoration with the name.
    // varchar: typmodout != InvalidOid -> uses the mock typmodout "(N)".
    set_catalog(vec![
        (NUMERICOID, type_form(0, 0, b'm' as i8, 0, "numeric")),
        (VARCHAROID, type_form(0, 0, b'x' as i8, 100, "varchar")),
    ]);

    assert_eq!(
        format_type_with_typemod(mcx, NUMERICOID, 5).unwrap().as_str(),
        "numeric(5)"
    );
    assert_eq!(
        format_type_with_typemod(mcx, VARCHAROID, 10)
            .unwrap()
            .as_str(),
        "character varying(10)"
    );
    assert_eq!(
        format_type(mcx, Some(VARCHAROID), None)
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("character varying")
    );
}

#[test]
fn bpchar_typemod_given_vs_null_quirk() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    set_catalog(vec![(BPCHAROID, type_form(0, 0, b'x' as i8, 0, "bpchar"))]);

    // typemod NULL -> "character".
    assert_eq!(
        format_type(mcx, Some(BPCHAROID), None)
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("character")
    );
    // typemod -1 with TYPEMOD_GIVEN -> empty special-case branch leaves buf
    // None, falls through to the quoted catalog name "bpchar".
    assert_eq!(
        format_type(mcx, Some(BPCHAROID), Some(-1))
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("bpchar")
    );
    // typemod 1 -> "character(1)".
    assert_eq!(
        format_type(mcx, Some(BPCHAROID), Some(1))
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("character(1)")
    );
}

#[test]
fn true_array_type_is_deconstructed() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    set_catalog(vec![
        (
            1007, // INT4ARRAYOID
            type_form(INT4OID, F_ARRAY_SUBSCRIPT_HANDLER, b'x' as i8, 0, "_int4"),
        ),
        (INT4OID, type_form(0, 0, b'p' as i8, 0, "int4")),
    ]);

    assert_eq!(
        format_type_extended(mcx, 1007, -1, 0)
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("integer[]")
    );
}

#[test]
fn plain_storage_array_not_deconstructed() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    // oidvector: typelem=OID, subscript handler, but PLAIN storage -> NOT
    // deconstructed; falls through to the quoted catalog name.
    set_catalog(vec![(
        30, // OIDVECTOROID
        type_form(26, F_ARRAY_SUBSCRIPT_HANDLER, b'p' as i8, 0, "oidvector"),
    )]);

    assert_eq!(
        format_type_extended(mcx, 30, -1, 0)
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("oidvector")
    );
}

#[test]
fn invalid_oid_paths() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    set_catalog(vec![]);

    assert_eq!(
        format_type_extended(mcx, InvalidOid, -1, FORMAT_TYPE_ALLOW_INVALID)
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("-")
    );
    assert_eq!(
        format_type_extended(mcx, InvalidOid, -1, FORMAT_TYPE_INVALID_AS_NULL)
            .unwrap()
            .map(|s| s.as_str().to_owned()),
        None
    );
    assert_eq!(
        format_type_extended(mcx, 99999, -1, FORMAT_TYPE_ALLOW_INVALID)
            .unwrap()
            .map(|s| s.as_str().to_owned())
            .as_deref(),
        Some("???")
    );
    assert!(format_type_extended(mcx, 99999, -1, 0).is_err());
}

#[test]
fn format_type_null_first_arg_returns_none() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    assert_eq!(
        format_type(mcx, None, Some(5))
            .unwrap()
            .map(|s| s.as_str().to_owned()),
        None
    );
}

#[test]
fn force_qualify_uses_namespace() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    set_catalog(vec![(50000, type_form(0, 0, b'p' as i8, 0, "mytype"))]);

    assert_eq!(format_type_be(mcx, 50000).unwrap().as_str(), "mytype");
    assert_eq!(
        format_type_be_qualified(mcx, 50000).unwrap().as_str(),
        "public.mytype"
    );
}

#[test]
fn type_maximum_size_arithmetic() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mocks();

    assert_eq!(type_maximum_size(BPCHAROID, -1).unwrap(), -1);
    // varchar(10): (10 - 4) * 4 + 4 = 28.
    assert_eq!(type_maximum_size(VARCHAROID, 10).unwrap(), 28);
    // bit(9): (9 + 7) / 8 + 8 = 2 + 8 = 10.
    assert_eq!(type_maximum_size(BITOID, 9).unwrap(), 10);
    // unknown -> -1.
    assert_eq!(type_maximum_size(INT4OID, 5).unwrap(), -1);
}

#[test]
fn oidvectortypes_joins_with_comma() {
    let _g = TEST_LOCK.lock().unwrap();
    install_mocks();
    let cx = MemoryContext::new("t");
    let mcx = cx.mcx();
    set_catalog(vec![
        (INT4OID, type_form(0, 0, b'p' as i8, 0, "int4")),
        (BOOLOID, type_form(0, 0, b'p' as i8, 0, "bool")),
    ]);

    assert_eq!(oidvectortypes(mcx, &[]).unwrap().as_str(), "");
    assert_eq!(oidvectortypes(mcx, &[INT4OID]).unwrap().as_str(), "integer");
    assert_eq!(
        oidvectortypes(mcx, &[INT4OID, BOOLOID]).unwrap().as_str(),
        "integer, boolean"
    );
    // A missing OID under ALLOW_INVALID renders "???" (never errors here).
    assert_eq!(
        oidvectortypes(mcx, &[INT4OID, 88888]).unwrap().as_str(),
        "integer, ???"
    );
}
