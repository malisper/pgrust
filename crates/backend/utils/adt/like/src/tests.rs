use super::*;
use types_core::catalog::C_COLLATION_OID;
use wchar::{PG_LATIN1, PG_UTF8};

const C: Oid = C_COLLATION_OID;

fn utf8() {
    mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
}

fn latin1() {
    mbutils::SetDatabaseEncoding(PG_LATIN1).unwrap();
}

#[test]
fn like_basic() {
    utf8();
    for (s, p, want) in [
        ("hello", "hello", true),
        ("hello", "world", false),
        ("hello", "h%", true),
        ("hello", "%o", true),
        ("hello", "%ell%", true),
        ("hello", "h_llo", true),
        ("hello", "h__lo", true),
        ("hello", "h__l_o", false),
        ("hello", "_____", true),
        ("hello", "______", false),
        ("hello", "____", false),
        ("hello", "%", true),
        ("", "%", true),
        ("", "", true),
        ("hello", "", false),
        ("", "_", false),
        ("hello", "%%%", true),
        ("hello", "h%x", false),
        ("abc", "a%b%c", true),
        ("abc", "%_%", true),
        ("abc", "a__", true),
        ("abcdef", "a%c_e_", true),
        ("abcdef", "a%c_e__", false),
        ("hello", "HELLO", false),
        ("indio", "in%dio", true),
        ("indio", "%d%", true),
    ] {
        assert_eq!(
            textlike(s.as_bytes(), p.as_bytes(), C).unwrap(),
            want,
            "{s:?} LIKE {p:?}"
        );
        assert_eq!(
            textnlike(s.as_bytes(), p.as_bytes(), C).unwrap(),
            !want,
            "{s:?} NOT LIKE {p:?}"
        );
    }
}

#[test]
fn like_escapes() {
    utf8();
    for (s, p, want) in [
        ("50%", "50\\%", true),
        ("50x", "50\\%", false),
        ("a_c", "a\\_c", true),
        ("abc", "a\\_c", false),
        ("a\\c", "a\\\\c", true),
        ("%", "\\%", true),
        ("_", "\\_", true),
        ("\\", "\\\\", true),
        ("a%bc", "a\\%b%", true),
    ] {
        assert_eq!(
            textlike(s.as_bytes(), p.as_bytes(), C).unwrap(),
            want,
            "{s:?} LIKE {p:?}"
        );
    }
    // Text exhausted before the trailing escape is reached: falsy, no error.
    assert!(!textlike(b"abc", b"abc\\", C).unwrap());
    for p in ["\\", "%\\", "ab%\\"] {
        let err = textlike(b"abc", p.as_bytes(), C).unwrap_err();
        assert_eq!(err.sqlstate(), ERRCODE_INVALID_ESCAPE_SEQUENCE, "pattern {p:?}");
        assert!(err.message().contains("must not end with escape character"));
    }
}

#[test]
fn like_utf8_char_semantics() {
    utf8();
    // '_' consumes one multibyte character, '%' scans char-synced.
    assert!(textlike("héllo".as_bytes(), "h_llo".as_bytes(), C).unwrap());
    assert!(textlike("héllo".as_bytes(), "h_l%".as_bytes(), C).unwrap());
    assert!(!textlike("héllo".as_bytes(), "h__llo".as_bytes(), C).unwrap());
    assert!(textlike("日本語".as_bytes(), "___".as_bytes(), C).unwrap());
    assert!(!textlike("日本語".as_bytes(), "__".as_bytes(), C).unwrap());
    assert!(textlike("日本語".as_bytes(), "%語".as_bytes(), C).unwrap());
    assert!(textlike("日本語".as_bytes(), "日%".as_bytes(), C).unwrap());
    assert!(!textlike("日本語".as_bytes(), "%本".as_bytes(), C).unwrap());
}

#[test]
fn like_single_byte_encoding() {
    latin1();
    assert!(textlike(b"h\xe9llo", b"h_llo", C).unwrap());
    assert!(textlike(b"h\xe9llo", b"%llo", C).unwrap());
    assert!(!textlike(b"h\xe9llo", b"h\xe8%", C).unwrap());
    utf8();
}

#[test]
fn ilike_utf8_ascii_fold() {
    utf8();
    let ctx = mcx::MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut scratch = IcScratch::default();
    for (s, p, want) in [
        ("HELLO", "hello", true),
        ("hello", "HeLLo", true),
        ("Hello World", "hello w%", true),
        ("Hello", "h_LLO", true),
        ("Hello", "world", false),
        ("ÄBC", "äbc", false),
    ] {
        assert_eq!(
            texticlike(mcx, s.as_bytes(), p.as_bytes(), C, &mut scratch).unwrap(),
            want,
            "{s:?} ILIKE {p:?}"
        );
        assert_eq!(
            texticnlike(mcx, s.as_bytes(), p.as_bytes(), C, &mut scratch).unwrap(),
            !want
        );
    }
}

#[test]
fn ilike_single_byte_fold() {
    latin1();
    let ctx = mcx::MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut scratch = IcScratch::default();
    assert!(texticlike(mcx, b"HELLO", b"hello", C, &mut scratch).unwrap());
    assert!(texticlike(mcx, b"HeLLo", b"%ell%", C, &mut scratch).unwrap());
    // C ctype: high-bit bytes don't fold.
    assert!(!texticlike(mcx, b"\xc4bc", b"\xe4bc", C, &mut scratch).unwrap());
    utf8();
}

#[test]
fn bytea_like() {
    utf8();
    assert!(bytealike(b"ab\x00cd", b"ab\x00cd").unwrap());
    assert!(bytealike(b"ab\x00cd", b"ab%").unwrap());
    assert!(bytealike(b"ab\x00cd", b"ab_cd").unwrap());
    assert!(!bytealike(b"ab\x00cd", b"ab\x01cd").unwrap());
    assert!(byteanlike(b"abc", b"abd").unwrap());
    // bytea matching is bytewise even under a multibyte database encoding.
    assert!(bytealike("日本".as_bytes(), b"______").unwrap());
}

#[test]
fn name_like() {
    utf8();
    let ctx = mcx::MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut name = [0u8; 64];
    name[..8].copy_from_slice(b"pg_class");
    assert!(namelike(&name, b"pg\\_%", C).unwrap());
    assert!(namelike(&name, b"pg_class", C).unwrap());
    assert!(!namelike(&name, b"pg_class_", C).unwrap());
    assert!(namenlike(&name, b"pg_index", C).unwrap());
    let mut scratch = IcScratch::default();
    assert!(nameiclike(mcx, &name, b"PG\\_CLASS", C, &mut scratch).unwrap());
    assert!(nameicnlike(mcx, &name, b"PG\\_INDEX", C, &mut scratch).unwrap());
}

#[test]
fn indeterminate_and_abort_paths() {
    utf8();
    let ctx = mcx::MemoryContext::new("t");
    let mcx = ctx.mcx();
    let err = textlike(b"a", b"a", 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INDETERMINATE_COLLATION);
    assert!(err.message().contains("for LIKE"));
    let mut scratch = IcScratch::default();
    let err = texticlike(mcx, b"a", b"a", 0, &mut scratch).unwrap_err();
    assert!(err.message().contains("for ILIKE"));

    // LIKE_ABORT propagation: late-% pattern longer than text.
    assert_eq!(sb_match_text(b"ab", b"a%cdX", None).unwrap(), LIKE_ABORT);
    assert_eq!(sb_match_text(b"ab", b"%_._", None).unwrap(), LIKE_ABORT);
}

#[test]
fn like_escape_conversion() {
    utf8();
    let run = |pat: &[u8], esc: &[u8]| -> PgResult<Vec<u8>> {
        let mut out = Vec::new();
        like_escape_into(pat, esc, &mut out)?;
        Ok(out)
    };
    assert_eq!(run(b"50#%", b"#").unwrap(), b"50\\%");
    assert_eq!(run(b"a#_c", b"#").unwrap(), b"a\\_c");
    assert_eq!(run(b"a\\c", b"#").unwrap(), b"a\\\\c");
    assert_eq!(run(b"##", b"#").unwrap(), b"\\#");
    assert_eq!(run(b"#\\", b"#").unwrap(), b"\\\\");
    assert_eq!(run(b"a\\c", b"").unwrap(), b"a\\\\c");
    assert_eq!(run(b"abc", b"\\").unwrap(), b"abc");
    assert_eq!(run("é%".as_bytes(), "é".as_bytes()).unwrap(), b"\\%");
    let err = run(b"abc", b"xy").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_ESCAPE_SEQUENCE);
    assert!(err.message().contains("invalid escape string"));
    // Multibyte escape char must count as ONE character.
    assert!(run(b"abc", "é".as_bytes()).is_ok());

    let mut out = Vec::new();
    like_escape_bytea_into(b"50#%", b"#", &mut out).unwrap();
    assert_eq!(out, b"50\\%");
}

#[test]
fn mb_encoding_arm_is_clean_feature_error() {
    mbutils::SetDatabaseEncoding(wchar::PG_EUC_JP).unwrap();
    let r = textlike(b"a", b"a", C);
    utf8();
    let err = r.unwrap_err();
    assert_eq!(err.sqlstate(), ::types_error::ERRCODE_FEATURE_NOT_SUPPORTED);
    assert!(err.message().contains("not yet implemented"), "{}", err.message());
}

#[test]
fn fc_wrappers_and_oids() {
    use types_fmgr::LocalFcinfo;
    utf8();
    let text = |s: &[u8]| -> Vec<u8> {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + s.len()));
        v.extend_from_slice(s);
        v
    };
    let s = text(b"Hello");
    let p = text(b"h%");
    let ctx = mcx::MemoryContext::new("t");
    let mut fcinfo = LocalFcinfo::<2>::new(C);
    // SAFETY: ctx outlives every call through this frame.
    unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
    fcinfo.set_arg(0, datum::Datum::from_usize(s.as_ptr() as usize));
    fcinfo.set_arg(1, datum::Datum::from_usize(p.as_ptr() as usize));
    assert!(!builtins::fc_textlike(None, &mut fcinfo)
        .unwrap()
        .as_bool());

    let mut flinfo = types_fmgr::FmgrInfo::new(builtins::fc_texticlike, 1633, 2, true, false);
    assert!(builtins::fc_texticlike(Some(&mut flinfo), &mut fcinfo)
        .unwrap()
        .as_bool());

    let mut flinfo = types_fmgr::FmgrInfo::new(builtins::fc_like_escape, 1637, 2, true, false);
    let pat = text(b"50#%");
    let esc = text(b"#");
    fcinfo.set_arg(0, datum::Datum::from_usize(pat.as_ptr() as usize));
    fcinfo.set_arg(1, datum::Datum::from_usize(esc.as_ptr() as usize));
    let d = builtins::fc_like_escape(Some(&mut flinfo), &mut fcinfo).unwrap();
    let out = unsafe { types_fmgr::PackedVarlena::from_ptr(d.as_usize() as *const u8) };
    assert_eq!(out.data(), b"50\\%");

    // pg_proc.dat parity: OID-keyed spot checks + table shape.
    let by_oid = |oid: Oid| {
        builtins::LIKE_BUILTINS
            .iter()
            .find(|b| b.foid == oid)
            .unwrap()
    };
    assert_eq!(by_oid(850).name, "textlike");
    assert_eq!(by_oid(1633).name, "texticlike");
    assert_eq!(by_oid(1637).name, "like_escape");
    assert_eq!(by_oid(2005).name, "bytealike");
    assert_eq!(by_oid(2009).name, "like_escape");
    assert_eq!(builtins::LIKE_BUILTINS.len(), 27);
    for b in builtins::LIKE_BUILTINS {
        assert!(b.strict && !b.retset);
    }
}

#[test]
fn like_support_rows_are_loud() {
    use types_fmgr::LocalFcinfo;
    // Selectivity/IndexCondition requests stay loud.
    let tag = types_nodes::NodeTag::T_SupportRequestSelectivity;
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, datum::Datum::from_usize(&tag as *const _ as usize));
    let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        builtins::fc_textlike_support(None, &mut fcinfo)
    }));
    let msg = *r.unwrap_err().downcast::<&'static str>().unwrap();
    assert!(msg.contains("textlike_support"));

    // Requests C's like_regex_support ignores return NULL (Datum 0).
    let tag = types_nodes::NodeTag::T_SupportRequestSimplify;
    let mut fcinfo = LocalFcinfo::<1>::new(0);
    fcinfo.set_arg(0, datum::Datum::from_usize(&tag as *const _ as usize));
    let r = builtins::fc_textlike_support(None, &mut fcinfo).unwrap();
    assert_eq!(r.as_usize(), 0);
}

const COLL_LIBC_LATIN1: Oid = 40001;
const COLL_BUILTIN_CUTF8: Oid = 40002;

fn install_collation_stub() {
    use std::sync::Once;
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_collation_locale_row::set(|mcx, collid| {
            let s = |v: &str| mcx::PgString::from_str_in(v, mcx);
            let mut collname = types_tuple::NameData::default();
            collname.namestrcpy("like_test");
            let (provider, collate, ctype, locale) = match collid {
                COLL_LIBC_LATIN1 => (
                    pg_locale::COLLPROVIDER_LIBC,
                    Some("en_US.ISO8859-1"),
                    Some("en_US.ISO8859-1"),
                    None,
                ),
                COLL_BUILTIN_CUTF8 => (pg_locale::COLLPROVIDER_BUILTIN, None, None, Some("C.UTF-8")),
                _ => return Ok(None),
            };
            Ok(Some(syscache_seams::PgCollationLocaleRow {
                collname,
                collnamespace: 11,
                collprovider: provider,
                collisdeterministic: true,
                collencoding: -1,
                collcollate: collate.map(s).transpose()?,
                collctype: ctype.map(s).transpose()?,
                colllocale: locale.map(s).transpose()?,
                collicurules: None,
                collversion: None,
            }))
        });
    });
}

// Expectations verified against live PG 18.3 (builtin C.UTF-8 database).
#[test]
fn ilike_non_c_ctype_folds() {
    utf8();
    install_collation_stub();
    let ctx = mcx::MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut scratch = IcScratch::default();
    for (s, p, want) in [
        ("ΣΟΦΟΣ", "σοφοσ", true),
        ("İstanbul", "i%", true),
        ("STRASSE", "straße", false),
        ("Ёлка", "ёлка", true),
        ("Wörld", "w_rld", true),
    ] {
        assert_eq!(
            texticlike(mcx, s.as_bytes(), p.as_bytes(), COLL_BUILTIN_CUTF8, &mut scratch).unwrap(),
            want,
            "{s:?} ILIKE {p:?}"
        );
    }
}

#[test]
fn ilike_sb_tolower_l_fold() {
    latin1();
    install_collation_stub();
    let ctx = mcx::MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut scratch = IcScratch::default();
    // 0xC4/0xE4 = Ä/ä in LATIN1; tolower_l folds them under en_US.ISO8859-1.
    match texticlike(mcx, b"\xc4bc", b"\xe4bc", COLL_LIBC_LATIN1, &mut scratch) {
        Ok(v) => assert!(v),
        Err(e) => {
            eprintln!("SKIP: en_US.ISO8859-1 locale not available on this host ({e})");
            utf8();
            return;
        }
    }
    assert!(texticlike(mcx, b"\xc4bc", b"\xe4b_", COLL_LIBC_LATIN1, &mut scratch).unwrap());
    assert!(!texticlike(mcx, b"\xc4bc", b"\xe9bc", COLL_LIBC_LATIN1, &mut scratch).unwrap());
    utf8();
}
