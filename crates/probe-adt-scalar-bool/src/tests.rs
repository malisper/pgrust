use super::*;

#[test]
fn parse_recognizes_canonical_spellings() {
    assert_eq!(parse_bool("true"), Some(true));
    assert_eq!(parse_bool("false"), Some(false));
    assert_eq!(parse_bool("YES"), Some(true));
    assert_eq!(parse_bool("No"), Some(false));
    assert_eq!(parse_bool("on"), Some(true));
    assert_eq!(parse_bool("OFF"), Some(false));
    assert_eq!(parse_bool("1"), Some(true));
    assert_eq!(parse_bool("0"), Some(false));
}

#[test]
fn parse_accepts_prefixes_like_c() {
    // C `strncasecmp("tr", "true", 2) == 0`.
    assert_eq!(parse_bool("t"), Some(true));
    assert_eq!(parse_bool("tr"), Some(true));
    assert_eq!(parse_bool("tru"), Some(true));
    assert_eq!(parse_bool("f"), Some(false));
    assert_eq!(parse_bool("ye"), Some(true));
}

#[test]
fn parse_rejects_garbage_and_overlong() {
    assert_eq!(parse_bool(""), None);
    assert_eq!(parse_bool("truex"), None);
    assert_eq!(parse_bool("2"), None);
    assert_eq!(parse_bool("10"), None);
    // 'o' needs at least 2 chars: a bare "o" must not match "on".
    assert_eq!(parse_bool("o"), None);
}

#[test]
fn boolin_trims_whitespace() {
    assert_eq!(boolin("  TRUE  ", None).unwrap(), true);
    assert_eq!(boolin("\tno\n", None).unwrap(), false);
}

#[test]
fn boolin_bad_syntax_hard_throws_without_context() {
    let err = boolin("maybe", None).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
}

#[test]
fn boolin_bad_syntax_soft_routes_to_context() {
    let mut ctx = SoftErrorContext::new(true);
    // Soft path returns the suppress-warning false and records the error.
    assert_eq!(boolin("maybe", Some(&mut ctx)).unwrap(), false);
    assert!(ctx.error_occurred());
}

#[test]
fn boolout_t_f() {
    assert_eq!(boolout(true), "t");
    assert_eq!(boolout(false), "f");
}

#[test]
fn comparisons_match_c_false_lt_true() {
    assert!(booleq(true, true));
    assert!(!booleq(true, false));
    assert!(boolne(true, false));
    // false < true
    assert!(boollt(false, true));
    assert!(!boollt(true, false));
    assert!(!boollt(true, true));
    assert!(boolgt(true, false));
    assert!(!boolgt(false, true));
    assert!(boolle(false, true));
    assert!(boolle(true, true));
    assert!(!boolle(true, false));
    assert!(boolge(true, false));
    assert!(boolge(true, true));
    assert!(!boolge(false, true));
}

#[test]
fn statefuncs() {
    assert!(booland_statefunc(true, true));
    assert!(!booland_statefunc(true, false));
    assert!(boolor_statefunc(true, false));
    assert!(!boolor_statefunc(false, false));
}

#[test]
fn agg_accum_and_finals() {
    let ctx = mcx::MemoryContext::new("test");
    let mcx = ctx.mcx();

    // First call with NULL state creates it.
    let s = bool_accum(Some(mcx), None, Some(true)).unwrap();
    assert_eq!(s.aggcount, 1);
    assert_eq!(s.aggtrue, 1);
    // A null value is skipped.
    let s = bool_accum(Some(mcx), Some(s), None).unwrap();
    assert_eq!(s.aggcount, 1);
    assert_eq!(s.aggtrue, 1);
    let s = bool_accum(Some(mcx), Some(s), Some(false)).unwrap();
    assert_eq!(s.aggcount, 2);
    assert_eq!(s.aggtrue, 1);

    // all-true is false (one false seen), any-true is true.
    assert_eq!(bool_alltrue(Some(s)), Some(false));
    assert_eq!(bool_anytrue(Some(s)), Some(true));

    // Inverse transition undoes the false.
    let s = bool_accum_inv(Some(s), Some(false)).unwrap();
    assert_eq!(s.aggcount, 1);
    assert_eq!(s.aggtrue, 1);
    assert_eq!(bool_alltrue(Some(s)), Some(true));

    // No non-null values => NULL finals.
    let empty = BoolAggState::default();
    assert_eq!(bool_alltrue(Some(empty)), None);
    assert_eq!(bool_anytrue(Some(empty)), None);
    assert_eq!(bool_alltrue(None), None);
    assert_eq!(bool_anytrue(None), None);
}

#[test]
fn agg_non_aggregate_context_errors() {
    assert!(make_bool_agg_state(None).is_err());
    assert!(bool_accum(None, None, Some(true)).is_err());
    assert!(bool_accum_inv(None, Some(true)).is_err());
}
