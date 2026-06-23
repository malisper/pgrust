//! Tests for the `misc.c` port.
//!
//! Pure-logic tests (`count_nulls`, the identifier-class helpers, the
//! `REQ_EVENTS` constant, `unpack_sql_state`, `pg_get_keywords`) touch no seam.
//! `parse_ident` reaches `small1::downcase_identifier`, which
//! reads the database-encoding seam; those tests install it under [`SEAM_LOCK`]
//! (the seam slot is process-global).

use super::*;

use ::mcx::MemoryContext;
use std::sync::Mutex;

static SEAM_LOCK: Mutex<()> = Mutex::new(());

// -------------------------------------------------------------------
// count_nulls / num_nulls / num_nonnulls (pure logic)
// -------------------------------------------------------------------

#[test]
fn count_nulls_separate_args() {
    let args = CountNullsArgs::Separate(&[false, true, false, true, true]);
    assert_eq!(count_nulls(&args), Some((5, 3)));
    assert_eq!(pg_num_nulls(&args), Some(3));
    assert_eq!(pg_num_nonnulls(&args), Some(2));
}

#[test]
fn count_nulls_variadic_null_arg_returns_none() {
    let args = CountNullsArgs::Variadic {
        arg_is_null: true,
        nitems: 0,
        bitmap: None,
    };
    assert_eq!(count_nulls(&args), None);
    assert_eq!(pg_num_nulls(&args), None);
    assert_eq!(pg_num_nonnulls(&args), None);
}

#[test]
fn count_nulls_variadic_no_bitmap_means_no_nulls() {
    let args = CountNullsArgs::Variadic {
        arg_is_null: false,
        nitems: 4,
        bitmap: None,
    };
    assert_eq!(count_nulls(&args), Some((4, 0)));
}

#[test]
fn count_nulls_variadic_bitmap_scan_matches_c() {
    // 10 items; bitmap bit set => element NON-null, clear => NULL.
    // Set bits for items 0,2,4,6,8 -> those non-null; 1,3,5,7,9 null.
    let bitmap = [0x55u8, 0x01u8];
    let args = CountNullsArgs::Variadic {
        arg_is_null: false,
        nitems: 10,
        bitmap: Some(&bitmap),
    };
    assert_eq!(count_nulls(&args), Some((10, 5)));
}

// -------------------------------------------------------------------
// identifier-class helpers (pure logic)
// -------------------------------------------------------------------

#[test]
fn ident_class_helpers_match_scan_l() {
    assert!(is_ident_start(b'_'));
    assert!(is_ident_start(b'a'));
    assert!(is_ident_start(b'Z'));
    assert!(is_ident_start(0xC3)); // high-bit set
    assert!(!is_ident_start(b'1'));
    assert!(!is_ident_start(b'$'));

    assert!(is_ident_cont(b'1'));
    assert!(is_ident_cont(b'$'));
    assert!(is_ident_cont(b'a'));
    assert!(!is_ident_cont(b'.'));

    for ch in [b' ', b'\t', b'\n', b'\r', 0x0b, 0x0c] {
        assert!(scanner_isspace(ch));
    }
    assert!(!scanner_isspace(b'a'));
    assert!(!scanner_isspace(0));
}

// -------------------------------------------------------------------
// REQ_EVENTS / FirstLowInvalidHeapAttributeNumber (pure constants)
// -------------------------------------------------------------------

#[test]
fn req_events_constant_matches_c() {
    // (1 << CMD_UPDATE) | (1 << CMD_DELETE) == (1<<2)|(1<<4) == 0x14.
    assert_eq!(REQ_EVENTS, 0x14);
}

#[test]
fn first_low_invalid_heap_attribute_number_matches_c() {
    assert_eq!(FirstLowInvalidHeapAttributeNumber, -7);
}

// -------------------------------------------------------------------
// unpack_sql_state (pure bit-twiddling)
// -------------------------------------------------------------------

#[test]
fn unpack_sql_state_matches_c() {
    // ERRCODE_INVALID_PARAMETER_VALUE is "22023".
    assert_eq!(unpack_sql_state(ERRCODE_INVALID_PARAMETER_VALUE.0), "22023");
    assert_eq!(unpack_sql_state(ERRCODE_DATATYPE_MISMATCH.0), "42804");
}

// -------------------------------------------------------------------
// pg_get_keywords (real grammar keyword table, no seam)
// -------------------------------------------------------------------

#[test]
fn pg_get_keywords_renders_table() {
    let rows = pg_get_keywords();
    // 494 keywords in PG 18 (common-keywords ScanKeywords).
    assert_eq!(rows.len(), keywords::ScanKeywords.num_keywords());

    // "select" is a fully reserved keyword (kwlist.h: RESERVED_KEYWORD,
    // BARE_LABEL).
    let select = rows
        .iter()
        .find(|r| r.word == b"select")
        .expect("select keyword present");
    assert_eq!(select.catcode, Some("R"));
    assert_eq!(select.catdesc, Some("reserved"));
    assert_eq!(select.barelabel, "true");
    assert_eq!(select.baredesc, "can be bare label");

    // "abort" is unreserved and can be a bare label (kwlist.h:
    // UNRESERVED_KEYWORD, BARE_LABEL).
    let abort = rows
        .iter()
        .find(|r| r.word == b"abort")
        .expect("abort keyword present");
    assert_eq!(abort.catcode, Some("U"));
    assert_eq!(abort.barelabel, "true");
}

// -------------------------------------------------------------------
// pg_typeof / pg_column_is_updatable short-circuit / any_value_transfn
// -------------------------------------------------------------------

#[test]
fn pg_typeof_is_identity() {
    assert_eq!(pg_typeof(23), 23);
    assert_eq!(pg_typeof(InvalidOid), InvalidOid);
}

#[test]
fn column_is_updatable_system_columns_short_circuit() {
    // attnum <= 0 returns false without consulting any (uninstalled) seam.
    assert!(!pg_column_is_updatable(123, 0, false).unwrap());
    assert!(!pg_column_is_updatable(123, -1, true).unwrap());
}

#[test]
fn any_value_transfn_is_identity() {
    assert_eq!(any_value_transfn(7i32), 7);
    assert_eq!(any_value_transfn("x"), "x");
}

// -------------------------------------------------------------------
// pg_current_logfile format validation (rejection path needs no seam)
// -------------------------------------------------------------------

#[test]
fn current_logfile_rejects_bad_format() {
    let ctx = MemoryContext::new("misc-test");
    let mcx = ctx.mcx();
    let e = pg_current_logfile(mcx, Some(b"bogus")).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert!(e.message().contains("not supported"));
    assert!(e.hint().unwrap().contains("stderr"));
}

// -------------------------------------------------------------------
// pg_basetype domain-stack walk (lookup supplied as a closure)
// -------------------------------------------------------------------

#[test]
fn basetype_walks_domain_stack_and_handles_bogus_oid() {
    // 4000 (domain) -> 4001 (domain) -> 23 (base); 99999 is bogus.
    let lookup = |typid: Oid| -> PgResult<Option<TypeBaseStep>> {
        Ok(match typid {
            4000 => Some(TypeBaseStep {
                is_domain: true,
                typbasetype: 4001,
            }),
            4001 => Some(TypeBaseStep {
                is_domain: true,
                typbasetype: 23,
            }),
            23 => Some(TypeBaseStep {
                is_domain: false,
                typbasetype: 0,
            }),
            99999 => None,
            other => Some(TypeBaseStep {
                is_domain: false,
                typbasetype: other,
            }),
        })
    };
    assert_eq!(pg_basetype(4000, lookup).unwrap(), Some(23));
    assert_eq!(pg_basetype(23, lookup).unwrap(), Some(23));
    assert_eq!(pg_basetype(99999, lookup).unwrap(), None);
}

// -------------------------------------------------------------------
// parse_ident — exercises the full scanner against the real
// downcase_identifier (needs the database-encoding seam installed).
// -------------------------------------------------------------------

fn install_encoding_seam() {
    // C-locale single-byte encoding (max length 1) -> ASCII-only downcasing.
    // The seam slot is process-global and installing it twice panics, so do it
    // exactly once across the whole test binary.
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        mbutils_seams::pg_database_encoding_max_length::set(|| 1);
    });
}

fn parts_to_vecs(parts: Vec<::mcx::PgVec<'_, u8>>) -> Vec<Vec<u8>> {
    parts.into_iter().map(|p| p.to_vec()).collect()
}

#[test]
fn parse_ident_simple_qualified() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_encoding_seam();
    let ctx = MemoryContext::new("misc-test");
    let parts = parse_ident(ctx.mcx(), b"Foo.bar", true).unwrap();
    assert_eq!(parts_to_vecs(parts), vec![b"foo".to_vec(), b"bar".to_vec()]);
}

#[test]
fn parse_ident_quoted_preserves_case_and_unescapes() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_encoding_seam();
    let ctx = MemoryContext::new("misc-test");
    let parts = parse_ident(ctx.mcx(), b"\"Foo\".\"a\"\"b\"", true).unwrap();
    assert_eq!(parts_to_vecs(parts), vec![b"Foo".to_vec(), b"a\"b".to_vec()]);
}

#[test]
fn parse_ident_multiple_doubled_quotes_unescape() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_encoding_seam();
    let ctx = MemoryContext::new("misc-test");
    let parts = parse_ident(ctx.mcx(), b"\"a\"\"b\"\"c\"", true).unwrap();
    assert_eq!(parts_to_vecs(parts), vec![b"a\"b\"c".to_vec()]);
}

#[test]
fn parse_ident_leading_and_internal_whitespace() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_encoding_seam();
    let ctx = MemoryContext::new("misc-test");
    let parts = parse_ident(ctx.mcx(), b"  a . b  ", false).unwrap();
    assert_eq!(parts_to_vecs(parts), vec![b"a".to_vec(), b"b".to_vec()]);
}

#[test]
fn parse_ident_errors_match_c() {
    let _g = SEAM_LOCK.lock().unwrap();
    install_encoding_seam();
    let ctx = MemoryContext::new("misc-test");
    let mcx = ctx.mcx();

    let e = parse_ident(mcx, b"\"abc", true).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert!(e.detail().unwrap().contains("unclosed double quotes"));

    let e = parse_ident(mcx, b"\"\"", true).unwrap_err();
    assert!(e.detail().unwrap().contains("must not be empty"));

    let e = parse_ident(mcx, b".a", true).unwrap_err();
    assert!(e.detail().unwrap().contains("before \".\""));

    let e = parse_ident(mcx, b"a.", true).unwrap_err();
    assert!(e.detail().unwrap().contains("after \".\""));

    // Strict: trailing junk is an error.
    let e = parse_ident(mcx, b"a b", true).unwrap_err();
    assert_eq!(e.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    assert!(e.detail().is_none());

    // Non-strict: trailing junk tolerated, only the first ident kept.
    let parts = parse_ident(mcx, b"a b", false).unwrap();
    assert_eq!(parts_to_vecs(parts), vec![b"a".to_vec()]);
}

// -------------------------------------------------------------------
// pg_get_catalog_foreign_keys() / sys_fk_relationships[] (pure data)
// -------------------------------------------------------------------

#[test]
fn catalog_foreign_keys_row_count_matches_header() {
    // system_fk_info.h has 219 sys_fk_relationships[] entries.
    let rows = build_sys_fk_rows();
    assert_eq!(rows.len(), 219);
}

#[test]
fn catalog_foreign_keys_representative_rows() {
    let rows = build_sys_fk_rows();

    // First entry: pg_proc(1255).pronamespace -> pg_namespace(2615).oid
    let first = &rows[0];
    assert_eq!(first.fktable, 1255);
    assert_eq!(first.pktable, 2615);
    assert_eq!(first.fkcols, vec![b"pronamespace".to_vec()]);
    assert_eq!(first.pkcols, vec![b"oid".to_vec()]);
    assert!(!first.is_array);
    assert!(!first.is_opt);

    // Multi-column entry: pg_attrdef(2604).{adrelid, adnum}
    //   -> pg_attribute(1249).{attrelid, attnum}
    let multi = rows
        .iter()
        .find(|r| r.fktable == 2604 && r.fkcols.len() == 2)
        .expect("pg_attrdef multi-column FK present");
    assert_eq!(multi.pktable, 1249);
    assert_eq!(
        multi.fkcols,
        vec![b"adrelid".to_vec(), b"adnum".to_vec()]
    );
    assert_eq!(
        multi.pkcols,
        vec![b"attrelid".to_vec(), b"attnum".to_vec()]
    );
    assert!(!multi.is_array);
    assert!(!multi.is_opt);

    // Last entry: pg_subscription_rel(6102).srrelid -> pg_class(1259).oid
    let last = rows.last().unwrap();
    assert_eq!(last.fktable, 6102);
    assert_eq!(last.pktable, 1259);
    assert_eq!(last.fkcols, vec![b"srrelid".to_vec()]);
}
