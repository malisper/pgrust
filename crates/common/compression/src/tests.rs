use super::*;

fn parse(alg: PgCompressAlgorithm, spec: Option<&str>) -> PgCompressSpecification {
    parse_compress_specification(alg, spec)
}

#[test]
fn algorithm_names_case_sensitive() {
    assert_eq!(parse_compress_algorithm("gzip"), Some(PgCompressAlgorithm::Gzip));
    assert_eq!(parse_compress_algorithm("lz4"), Some(PgCompressAlgorithm::Lz4));
    assert_eq!(parse_compress_algorithm("zstd"), Some(PgCompressAlgorithm::Zstd));
    assert_eq!(parse_compress_algorithm("none"), Some(PgCompressAlgorithm::None));
    // C uses strcmp: case matters.
    assert_eq!(parse_compress_algorithm("GZIP"), None);
    assert_eq!(parse_compress_algorithm("Gzip"), None);
    assert_eq!(parse_compress_algorithm(""), None);
}

#[test]
fn defaults_per_algorithm() {
    assert_eq!(parse(PgCompressAlgorithm::None, None).level, 0);
    assert_eq!(parse(PgCompressAlgorithm::Gzip, None).level, Z_DEFAULT_COMPRESSION);
    assert_eq!(parse(PgCompressAlgorithm::Lz4, None).level, 0);
    #[cfg(not(target_family = "wasm"))]
    assert_eq!(parse(PgCompressAlgorithm::Zstd, None).level, 3); // ZSTD_CLEVEL_DEFAULT
}

#[test]
fn bare_integer_specification() {
    let s = parse(PgCompressAlgorithm::Gzip, Some("5"));
    assert_eq!(s.level, 5);
    assert!(s.parse_error.is_none());
    assert!(validate_compress_specification(&s).is_none());
}

#[test]
fn level_keyword() {
    let s = parse(PgCompressAlgorithm::Gzip, Some("level=7"));
    assert_eq!(s.level, 7);
    assert!(validate_compress_specification(&s).is_none());
}

#[test]
fn gzip_level_out_of_range() {
    let s = parse(PgCompressAlgorithm::Gzip, Some("level=12"));
    assert!(s.parse_error.is_none()); // parses OK...
    assert_eq!(
        validate_compress_specification(&s).as_deref(),
        Some("compression algorithm \"gzip\" expects a compression level between 1 and 9 (default at -1)")
    );
    // ...and gzip level 0 is also rejected (default is -1, not 0).
    let s0 = parse(PgCompressAlgorithm::Gzip, Some("0"));
    assert!(validate_compress_specification(&s0).is_some());
}

#[test]
fn lz4_level_range() {
    let ok = parse(PgCompressAlgorithm::Lz4, Some("level=12"));
    assert!(validate_compress_specification(&ok).is_none());
    let bad = parse(PgCompressAlgorithm::Lz4, Some("level=13"));
    assert_eq!(
        validate_compress_specification(&bad).as_deref(),
        Some("compression algorithm \"lz4\" expects a compression level between 1 and 12 (default at 0)")
    );
}

#[cfg(not(target_family = "wasm"))]
#[test]
fn zstd_workers_and_long() {
    let s = parse(PgCompressAlgorithm::Zstd, Some("workers=3,level=4,long"));
    assert!(s.parse_error.is_none());
    assert_eq!(s.workers, 3);
    assert_eq!(s.level, 4);
    assert!(s.long_distance);
    assert_eq!(s.options, PG_COMPRESSION_OPTION_WORKERS | PG_COMPRESSION_OPTION_LONG_DISTANCE);
    assert!(validate_compress_specification(&s).is_none());
    // Negative zstd levels are legal down to ZSTD_minCLevel().
    let neg = parse(PgCompressAlgorithm::Zstd, Some("level=-3"));
    assert!(validate_compress_specification(&neg).is_none());
}

#[test]
fn workers_rejected_for_non_zstd() {
    let s = parse(PgCompressAlgorithm::Gzip, Some("workers=3"));
    assert_eq!(
        validate_compress_specification(&s).as_deref(),
        Some("compression algorithm \"gzip\" does not accept a worker count")
    );
}

#[test]
fn long_rejected_for_non_zstd() {
    let s = parse(PgCompressAlgorithm::Lz4, Some("long"));
    assert_eq!(
        validate_compress_specification(&s).as_deref(),
        Some("compression algorithm \"lz4\" does not support long-distance mode")
    );
}

#[test]
fn none_rejects_level() {
    let s = parse(PgCompressAlgorithm::None, Some("level=1"));
    assert_eq!(
        validate_compress_specification(&s).as_deref(),
        Some("compression algorithm \"none\" does not accept a compression level")
    );
    // But an explicit level of 0 is accepted for "none" (C validates level != 0).
    let s0 = parse(PgCompressAlgorithm::None, Some("level=0"));
    assert!(validate_compress_specification(&s0).is_none());
}

#[test]
fn parse_error_strings() {
    let s = parse(PgCompressAlgorithm::Gzip, Some("bogus=1"));
    assert_eq!(s.parse_error.as_deref(), Some("unrecognized compression option: \"bogus\""));

    let s = parse(PgCompressAlgorithm::Gzip, Some("level"));
    assert_eq!(s.parse_error.as_deref(), Some("compression option \"level\" requires a value"));

    let s = parse(PgCompressAlgorithm::Gzip, Some("level=abc"));
    assert_eq!(
        s.parse_error.as_deref(),
        Some("value for compression option \"level\" must be an integer")
    );

    let s = parse(PgCompressAlgorithm::Zstd, Some("long=maybe"));
    #[cfg(not(target_family = "wasm"))]
    assert_eq!(
        s.parse_error.as_deref(),
        Some("value for compression option \"long\" must be a Boolean value")
    );
    #[cfg(target_family = "wasm")]
    let _ = s;

    let s = parse(PgCompressAlgorithm::Gzip, Some(",level=1"));
    assert_eq!(
        s.parse_error.as_deref(),
        Some("found empty string where a compression option was expected")
    );

    // parse_error short-circuits validation.
    let s = parse(PgCompressAlgorithm::Gzip, Some("bogus=1"));
    assert_eq!(
        validate_compress_specification(&s).as_deref(),
        Some("unrecognized compression option: \"bogus\"")
    );
}

#[test]
fn boolean_forms() {
    for v in ["yes", "on", "1", "YES", "On"] {
        let s = parse(PgCompressAlgorithm::Zstd, Some(&format!("long={v}")));
        #[cfg(not(target_family = "wasm"))]
        {
            assert!(s.parse_error.is_none(), "long={v}");
            assert!(s.long_distance, "long={v}");
        }
        #[cfg(target_family = "wasm")]
        let _ = s;
    }
    for v in ["no", "off", "0"] {
        let s = parse(PgCompressAlgorithm::Zstd, Some(&format!("long={v}")));
        #[cfg(not(target_family = "wasm"))]
        {
            assert!(s.parse_error.is_none(), "long={v}");
            assert!(!s.long_distance, "long={v}");
        }
        #[cfg(target_family = "wasm")]
        let _ = s;
    }
}

// C `parse_compress_options` (compression.c:426, FRONTEND): the -Z/--compress
// splitter used by pg_dump / pg_basebackup / pg_receivewal.
#[test]
fn parse_options_bare_integer_zero_is_none() {
    // compression.c:441-446: a bare 0 means "none" with no detail.
    assert_eq!(parse_compress_options("0"), ("none".to_string(), None));
    assert_eq!(parse_compress_options("-0"), ("none".to_string(), None));
    // strtol("") converts nothing, endp == option, *endp == '\0', result 0.
    assert_eq!(parse_compress_options(""), ("none".to_string(), None));
}

#[test]
fn parse_options_bare_integer_nonzero_is_gzip() {
    // compression.c:447-451: any other bare integer implies gzip, and the
    // detail is the ORIGINAL option text (pstrdup(option)), not a
    // re-rendering of the parsed value.
    assert_eq!(parse_compress_options("5"), ("gzip".to_string(), Some("5".to_string())));
    assert_eq!(parse_compress_options("+7"), ("gzip".to_string(), Some("+7".to_string())));
    assert_eq!(parse_compress_options(" 9"), ("gzip".to_string(), Some(" 9".to_string())));
    assert_eq!(parse_compress_options("-1"), ("gzip".to_string(), Some("-1".to_string())));
}

#[test]
fn parse_options_method_only() {
    // compression.c:459-463: no ':' → the whole option is the algorithm.
    assert_eq!(parse_compress_options("gzip"), ("gzip".to_string(), None));
    assert_eq!(parse_compress_options("zstd"), ("zstd".to_string(), None));
    // Not an integer and not a known name: C does not validate here.
    assert_eq!(parse_compress_options("bogus"), ("bogus".to_string(), None));
    assert_eq!(parse_compress_options("5x"), ("5x".to_string(), None));
    // strtol converts nothing on a bare sign; *endp is '+', so ':' lookup.
    assert_eq!(parse_compress_options("+"), ("+".to_string(), None));
}

#[test]
fn parse_options_method_and_detail() {
    // compression.c:464-474: split at the FIRST ':'; detail is everything
    // after it (may itself contain ':' or be empty).
    assert_eq!(
        parse_compress_options("gzip:5"),
        ("gzip".to_string(), Some("5".to_string()))
    );
    assert_eq!(
        parse_compress_options("zstd:level=3,long"),
        ("zstd".to_string(), Some("level=3,long".to_string()))
    );
    assert_eq!(parse_compress_options("lz4:"), ("lz4".to_string(), Some(String::new())));
    assert_eq!(parse_compress_options(":5"), (String::new(), Some("5".to_string())));
    assert_eq!(
        parse_compress_options("a:b:c"),
        ("a".to_string(), Some("b:c".to_string()))
    );
}

#[test]
fn parse_options_feeds_specification_like_pg_dump() {
    // pg_dump.c:683-700 shape: split, then parse_compress_algorithm +
    // parse_compress_specification on the pieces.
    let (alg, detail) = parse_compress_options("gzip:7");
    let alg = parse_compress_algorithm(&alg).expect("gzip");
    let spec = parse_compress_specification(alg, detail.as_deref());
    assert_eq!(spec.algorithm, PgCompressAlgorithm::Gzip);
    assert_eq!(spec.level, 7);
    assert!(validate_compress_specification(&spec).is_none());

    let (alg, detail) = parse_compress_options("0");
    let alg = parse_compress_algorithm(&alg).expect("none");
    let spec = parse_compress_specification(alg, detail.as_deref());
    assert_eq!(spec.algorithm, PgCompressAlgorithm::None);
    assert_eq!(spec.level, 0);
    assert!(validate_compress_specification(&spec).is_none());
}
