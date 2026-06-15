//! Unit tests for the seam-light `backend-parser-small1` logic: the
//! `scanner_isspace` predicate, identifier downcasing/truncation, the
//! `parser_errposition` cursor conversion, the fixed-parameter hook range
//! checks, and the extern-param probe walker.

extern crate std;

use super::*;
use std::sync::Once;

/// Install the small set of outward seams the tested code paths reach, with
/// deterministic test bodies. Idempotent across tests.
fn install_test_seams() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // Single-byte server encoding so the high-bit downcasing path is live.
        mb::pg_database_encoding_max_length::set(|| 1);
        // ASCII source text: char index == byte length.
        mb::pg_mbstrlen_with_len::set(|s, limit| {
            core::str::from_utf8(&s[..limit as usize])
                .map(|s| s.chars().count() as i32)
                .unwrap_or(limit)
        });
        // Single-byte clip: just the limit.
        mb::pg_mbcliplen::set(|_s, _len, limit| limit);
        // No type carries a collation in these tests.
        lsyscache::get_typcollation::set(|_oid| Ok(InvalidOid));
    });
}

fn ps<'mcx>(mcx: Mcx<'mcx>) -> ParseState<'mcx> {
    ParseState::new(mcx).unwrap()
}

#[test]
fn scanner_isspace_matches_flex_space_set() {
    for &c in &[b' ', b'\t', b'\n', b'\r', 0x0b, 0x0c] {
        assert!(scanner_isspace(c), "byte {c:#x} must be space");
    }
    for &c in &[b'a', b'0', 0u8, 0x7f, b'_'] {
        assert!(!scanner_isspace(c), "byte {c:#x} must not be space");
    }
}

#[test]
fn downcase_identifier_folds_ascii() {
    install_test_seams();
    let ctx = mcx::MemoryContext::new("test");
    let out = downcase_identifier(ctx.mcx(), b"FooBAR_1", false, true).unwrap();
    assert_eq!(out.as_slice(), b"foobar_1");
}

#[test]
fn downcase_identifier_high_bit_single_byte() {
    install_test_seams();
    let ctx = mcx::MemoryContext::new("test");
    // The ASCII byte folds; the high-bit byte's fold is locale-aware (libc),
    // so under the test runner's locale it is either folded by tolower or left
    // unchanged — never corrupted. We only assert the deterministic ASCII fold
    // and that the high-bit byte equals libc's tolower of it.
    let hb = 0xC0u8;
    let out = downcase_identifier(ctx.mcx(), &[b'A', hb], false, true).unwrap();
    let expected_hb = if hb & 0x80 != 0 && unsafe { libc::isupper(i32::from(hb)) != 0 } {
        unsafe { libc::tolower(i32::from(hb)) as u8 }
    } else {
        hb
    };
    assert_eq!(out.as_slice(), &[b'a', expected_hb]);
}

#[test]
fn downcase_truncate_clips_over_namedatalen() {
    install_test_seams();
    let ctx = mcx::MemoryContext::new("test");
    let long = alloc::vec![b'X'; NAMEDATALEN + 10];
    let out = downcase_truncate_identifier(ctx.mcx(), &long, false).unwrap();
    // pg_mbcliplen test stub returns the limit (NAMEDATALEN - 1).
    assert_eq!(out.len(), NAMEDATALEN - 1);
    assert!(out.iter().all(|&b| b == b'x'));
}

#[test]
fn truncate_identifier_leaves_short_unchanged() {
    install_test_seams();
    let ctx = mcx::MemoryContext::new("test");
    let out = truncate_identifier(ctx.mcx(), b"short", false).unwrap();
    assert_eq!(out.as_slice(), b"short");
}

#[test]
fn parser_errposition_negative_and_no_text() {
    install_test_seams();
    let ctx = mcx::MemoryContext::new("test");
    let mut pstate = ps(ctx.mcx());
    // location < 0 -> 0
    assert_eq!(parser_errposition(&pstate, -1), 0);
    // no source text -> 0
    assert_eq!(parser_errposition(&pstate, 5), 0);
    // with source text -> char index + 1
    pstate.p_sourcetext = Some(mcx::PgString::from_str_in("select 1", ctx.mcx()).unwrap());
    assert_eq!(parser_errposition(&pstate, 6), 7);
}

#[test]
fn fixed_paramref_hook_resolves_in_range() {
    install_test_seams();
    let ctx = mcx::MemoryContext::new("test");
    let pstate = ps(ctx.mcx());
    let types = [INT4OID, BOOLOID];
    let parstate = setup_parse_fixed_parameters(&types);
    let pref = ParamRef { number: 2, location: -1 };
    let param = fixed_paramref_hook(&pstate, &parstate, &pref).unwrap();
    assert_eq!(param.paramkind, PARAM_EXTERN);
    assert_eq!(param.paramid, 2);
    assert_eq!(param.paramtype, BOOLOID);
    assert_eq!(param.paramtypmod, -1);
}

#[test]
fn fixed_paramref_hook_rejects_out_of_range() {
    install_test_seams();
    let ctx = mcx::MemoryContext::new("test");
    let pstate = ps(ctx.mcx());
    let types = [INT4OID];
    let parstate = setup_parse_fixed_parameters(&types);
    // paramno 0, > numParams, and an InvalidOid slot are all errors.
    for n in [0, 2] {
        let pref = ParamRef { number: n, location: -1 };
        assert!(fixed_paramref_hook(&pstate, &parstate, &pref).is_err());
    }
    let inv = [InvalidOid];
    let parstate2 = setup_parse_fixed_parameters(&inv);
    let pref = ParamRef { number: 1, location: -1 };
    assert!(fixed_paramref_hook(&pstate, &parstate2, &pref).is_err());
}

#[test]
fn node_param_projection() {
    // The extern-param probe's IsA(node, Param) projection: a PARAM_EXTERN Param
    // node is detected as containing an extern param; a PARAM_EXEC one is not.
    let extern_param = Node::Expr(Expr::Param(Param {
        paramkind: PARAM_EXTERN,
        paramid: 1,
        paramtype: INT4OID,
        paramtypmod: -1,
        paramcollid: InvalidOid,
        location: -1,
    }));
    assert!(query_contains_extern_params_walker(&extern_param));

    let exec_param = Node::Expr(Expr::Param(Param {
        paramkind: types_nodes::primnodes::PARAM_EXEC,
        paramid: 1,
        paramtype: INT4OID,
        paramtypmod: -1,
        paramcollid: InvalidOid,
        location: -1,
    }));
    assert!(!query_contains_extern_params_walker(&exec_param));
}
