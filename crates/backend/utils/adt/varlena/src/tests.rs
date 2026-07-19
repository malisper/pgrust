use mcx::MemoryContext;
use types_core::{C_COLLATION_OID, POSIX_COLLATION_OID};

use crate::*;

const C: u32 = C_COLLATION_OID;

#[test]
fn cstring_text_round_trip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let t = cstring_to_text(mcx, b"hello").unwrap();
    assert_eq!(t.data(), b"hello");
    assert_eq!(t.varsize(), 5 + VARHDRSZ);
    let c = text_to_cstring(mcx, t.data()).unwrap();
    assert_eq!(&c[..], b"hello\0");
    let empty = cstring_to_text(mcx, b"").unwrap();
    assert_eq!(empty.data(), b"");
    assert_eq!(empty.varsize(), VARHDRSZ);
}

#[test]
fn open_image_forms() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let t = cstring_to_text(mcx, b"abc").unwrap();
    match open_image(mcx, t.as_bytes()).unwrap() {
        VarPayload::Inline(p) => assert_eq!(p, b"abc"),
        _ => panic!("expected inline"),
    }
    // 1B short form: header (len<<1)|1, len = total including the header byte.
    let short = [((4usize << 1) | 1) as u8, b'x', b'y', b'z'];
    match open_image(mcx, &short).unwrap() {
        VarPayload::Inline(p) => assert_eq!(p, b"xyz"),
        _ => panic!("expected inline"),
    };
}

#[test]
#[should_panic(expected = "seam not installed")]
fn open_image_external_is_loud_until_detoast_lands() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let external = [0x01u8, 18, 0, 0];
    let _ = open_image(mcx, &external);
}

#[test]
fn fastcmp_c_matches_memcmp_semantics() {
    assert_eq!(varstrfastcmp_c(b"abc", b"abc"), 0);
    assert!(varstrfastcmp_c(b"abc", b"abd") < 0);
    assert!(varstrfastcmp_c(b"abd", b"abc") > 0);
    assert!(varstrfastcmp_c(b"ab", b"abc") < 0);
    assert!(varstrfastcmp_c(b"abc", b"ab") > 0);
    assert_eq!(varstrfastcmp_c(b"", b""), 0);
    assert!(varstrfastcmp_c(b"", b"a") < 0);
    // NUL bytes are data, not terminators.
    assert!(varstrfastcmp_c(b"a\0b", b"a\0c") < 0);
}

#[test]
fn bpchar_fastcmp_trims_trailing_blanks_only() {
    assert_eq!(bpcharfastcmp_c(b"ab  ", b"ab"), 0);
    assert_eq!(bpcharfastcmp_c(b"ab", b"ab   "), 0);
    assert!(bpcharfastcmp_c(b" ab", b"ab") < 0);
    assert!(bpcharfastcmp_c(b"ab c", b"ab") > 0);
    assert_eq!(bpcharfastcmp_c(b"   ", b""), 0);
}

#[test]
fn text_cmp_family_c_collation() {
    assert_eq!(text_cmp(b"a", b"b", C).unwrap(), -1);
    assert_eq!(varstr_cmp(b"same", b"same", POSIX_COLLATION_OID).unwrap(), 0);
    assert!(texteq(b"x", b"x", C).unwrap());
    assert!(!texteq(b"x", b"xx", C).unwrap());
    assert!(textne(b"x", b"y", C).unwrap());
    assert!(text_lt(b"a", b"b", C).unwrap());
    assert!(text_le(b"a", b"a", C).unwrap());
    assert!(text_gt(b"b", b"a", C).unwrap());
    assert!(text_ge(b"b", b"b", C).unwrap());
    assert_eq!(bttextcmp(b"aa", b"ab", C).unwrap(), -1);
    assert_eq!(text_larger(b"a", b"b", C).unwrap(), b"b");
    assert_eq!(text_larger(b"a", b"a", C).unwrap(), b"a");
    assert_eq!(text_smaller(b"a", b"b", C).unwrap(), b"a");
    assert!(btvarstrequalimage(C).unwrap());
}

#[test]
fn invalid_collation_errors() {
    let err = text_cmp(b"a", b"b", 0).unwrap_err();
    let msg = format!("{err:?}");
    assert!(msg.contains("could not determine which collation"), "{msg}");
}

#[test]
fn catenate_and_lengths() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let t = text_catenate(mcx, b"foo", b"bar").unwrap();
    assert_eq!(t.data(), b"foobar");
    assert_eq!(textoctetlen(b"foobar"), 6);
    assert_eq!(bytea::byteaoctetlen(b"ab"), 2);
}

#[test]
fn replace_text_cases() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(
        replace_text(mcx, b"foobarbaz", b"bar", b"XX", C).unwrap().data(),
        b"fooXXbaz"
    );
    assert_eq!(
        replace_text(mcx, b"aaaa", b"a", b"bb", C).unwrap().data(),
        b"bbbbbbbb"
    );
    // empty source or empty pattern: unmodified copy.
    assert_eq!(replace_text(mcx, b"", b"a", b"b", C).unwrap().data(), b"");
    assert_eq!(replace_text(mcx, b"abc", b"", b"x", C).unwrap().data(), b"abc");
    // pattern not found: unmodified copy.
    assert_eq!(replace_text(mcx, b"abc", b"z", b"x", C).unwrap().data(), b"abc");
}

// text/bytea_overlay substring their first argument through the
// detoast_attr_slice fetch, so t1 is a raw varlena image (t2 stays payload).
fn image_4b(s: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + s.len());
    v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + s.len()));
    v.extend_from_slice(s);
    v
}

#[test]
fn text_overlay_cases() {
    install_detoast_seams();
    install_mb_for_levenshtein();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    // overlay('Txxxxas' placing 'hom' from 2 for 4) = 'Thomas'.
    assert_eq!(
        text_overlay(mcx, &image_4b(b"Txxxxas"), b"hom", 2, 4).unwrap().data(),
        b"Thomas"
    );
    // no_len defaults sl to length(t2); C caller path exercised via fc_textoverlay_no_len.
    assert_eq!(
        text_overlay(mcx, &image_4b(b"Txxxas"), b"hom", 2, 3).unwrap().data(),
        b"Thomas"
    );
    let err = text_overlay(mcx, &image_4b(b"abc"), b"x", 0, 1).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SUBSTRING_ERROR);
    let err = text_overlay(mcx, &image_4b(b"abc"), b"x", i32::MAX, 1).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
}

#[test]
fn bytea_overlay_and_bit_count() {
    install_detoast_seams();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(
        bytea::bytea_overlay(mcx, &image_4b(b"Txxxxas"), b"hom", 2, 4).unwrap().data(),
        b"Thomas"
    );
    let err = bytea::bytea_overlay(mcx, &image_4b(b"abc"), b"x", 0, 1).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SUBSTRING_ERROR);

    assert_eq!(bytea::bytea_bit_count(b""), 0);
    assert_eq!(bytea::bytea_bit_count(&[0xffu8]), 8);
    assert_eq!(bytea::bytea_bit_count(&[0x01u8, 0x03]), 3);
}

#[test]
fn convert_to_base_cases() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(convert_to_base(mcx, 0, 2).unwrap().data(), b"0");
    assert_eq!(convert_to_base(mcx, 5, 2).unwrap().data(), b"101");
    assert_eq!(convert_to_base(mcx, 8, 8).unwrap().data(), b"10");
    assert_eq!(convert_to_base(mcx, 255, 16).unwrap().data(), b"ff");
    // negative ints print as their unsigned bit pattern (C casts before conversion).
    assert_eq!(
        convert_to_base(mcx, (-1i32 as u32) as u64, 16).unwrap().data(),
        b"ffffffff"
    );
    assert_eq!(
        convert_to_base(mcx, -1i64 as u64, 16).unwrap().data(),
        b"ffffffffffffffff"
    );
}

#[test]
fn wire_io_round_trips() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Identity client<->server conversion (single-encoding test setup).
        mbutils_seams::pg_server_to_client::set(|_, _| Ok(None));
    });
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let sent = textsend(mcx, b"wire").unwrap();
    assert_eq!(sent.data(), b"wire");

    let mut buf = stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(b"payload").unwrap();
    let got = textrecv(mcx, &mut buf).unwrap();
    assert_eq!(got.data(), b"payload");

    let mut buf = stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(b"raw\x01bytes").unwrap();
    let got = bytea::bytearecv(mcx, &mut buf).unwrap();
    assert_eq!(got.data(), b"raw\x01bytes");

    let b = bytea::byteasend(mcx, b"copy").unwrap();
    assert_eq!(b.data(), b"copy");

    assert_eq!(&unknownin(mcx, b"u\0trailing").unwrap()[..], b"u\0");
    let us = unknownsend(mcx, b"unk\0").unwrap();
    assert_eq!(us.data(), b"unk");
}

#[test]
fn byteain_hex_and_escape() {
    install_mb_for_levenshtein();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = bytea::byteain(mcx, b"\\xDEADbeef", None).unwrap().unwrap();
    assert_eq!(v.data(), &[0xde, 0xad, 0xbe, 0xef]);
    let v = bytea::byteain(mcx, b"\\x de ad ", None).unwrap().unwrap();
    assert_eq!(v.data(), &[0xde, 0xad]);
    let v = bytea::byteain(mcx, b"\\x", None).unwrap().unwrap();
    assert_eq!(v.data(), b"");

    let v = bytea::byteain(mcx, b"ab\\\\c\\001", None).unwrap().unwrap();
    assert_eq!(v.data(), &[b'a', b'b', b'\\', b'c', 1]);
    let v = bytea::byteain(mcx, b"\\377", None).unwrap().unwrap();
    assert_eq!(v.data(), &[0xff]);

    assert!(bytea::byteain(mcx, b"\\xgg", None).is_err());
    assert!(bytea::byteain(mcx, b"\\xa", None).is_err());
    assert!(bytea::byteain(mcx, b"bad\\9", None).is_err());
    assert!(bytea::byteain(mcx, b"trail\\", None).is_err());

    // Soft-error context captures instead of failing (C ereturn).
    let mut soft = types_error::SoftErrorContext::new(true);
    let r = bytea::byteain(mcx, b"\\xzz", Some(&mut soft)).unwrap();
    assert!(r.is_none());
    assert!(soft.error_occurred());
}

#[test]
fn byteaout_hex_and_escape() {
    let mut buf = Vec::new();
    bytea::byteaout_into(
        &[0xde, 0xad, 0x01],
        guc_tables::consts::BYTEA_OUTPUT_HEX,
        &mut buf,
    )
    .unwrap();
    assert_eq!(&buf[..], b"\\xdead01\0");

    bytea::byteaout_into(
        &[b'a', b'\\', 0x01, 0x7f],
        guc_tables::consts::BYTEA_OUTPUT_ESCAPE,
        &mut buf,
    )
    .unwrap();
    assert_eq!(&buf[..], b"a\\\\\\001\\177\0");

    assert!(bytea::byteaout_into(b"x", 99, &mut buf).is_err());
}

#[test]
fn byteain_hex_digit_message_is_c_exact() {
    install_mb_for_levenshtein();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let err = bytea::byteain(mcx, b"\\xzz", None).unwrap_err();
    assert_eq!(err.message, "invalid hexadecimal digit: \"z\"");
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);
    let err = bytea::byteain(mcx, b"\\xa", None).unwrap_err();
    assert_eq!(err.message, "invalid hexadecimal data: odd number of digits");
}

#[test]
fn bytea_substring_and_pos() {
    install_detoast_seams();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut img = vec![0u8; 4];
    img.extend_from_slice(&[0u8, 1, 2, 3, 4, 5]);
    let hdr = datum::varlena::set_varsize_4b(img.len());
    img[..4].copy_from_slice(&hdr);
    let s: &[u8] = &img;
    // 1-based; substring(s from 2 for 3) = bytes at index 1..4.
    assert_eq!(bytea::bytea_substring(mcx, s, 2, 3, false).unwrap().data(), &[1, 2, 3]);
    // no length -> to end.
    assert_eq!(bytea::bytea_substring(mcx, s, 3, -1, true).unwrap().data(), &[2, 3, 4, 5]);
    // start <= 0 shifts window; length trims per SQL end position.
    assert_eq!(bytea::bytea_substring(mcx, s, -1, 3, false).unwrap().data(), &[0]);
    // start past end -> empty.
    assert_eq!(bytea::bytea_substring(mcx, s, 10, 2, false).unwrap().data(), b"");
    // E < 1 -> empty.
    assert_eq!(bytea::bytea_substring(mcx, s, 0, 0, false).unwrap().data(), b"");
    // negative length -> error 22011.
    let err = bytea::bytea_substring(mcx, s, 1, -2, false).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_SUBSTRING_ERROR);

    assert_eq!(bytea::byteapos(b"abcabc", b"bc"), 2);
    assert_eq!(bytea::byteapos(b"abc", b"xy"), 0);
    assert_eq!(bytea::byteapos(b"abc", b""), 1);
    assert_eq!(bytea::byteapos(b"a", b"abc"), 0);
}

#[test]
fn bytea_get_set_byte_and_bit() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = &[0x00u8, 0xff, 0x10];
    assert_eq!(bytea::bytea_get_byte(v, 1).unwrap(), 255);
    assert_eq!(bytea::bytea_get_byte(v, 2).unwrap(), 16);
    assert_eq!(
        bytea::bytea_get_byte(v, 3).unwrap_err().sqlstate(),
        types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR
    );
    // bit 0 of byte 1 (0xff) is the LSB.
    assert_eq!(bytea::bytea_get_bit(v, 8).unwrap(), 1);
    // byte 2 = 0x10 = bit 4 set; global bit index 16+4 = 20.
    assert_eq!(bytea::bytea_get_bit(v, 20).unwrap(), 1);
    assert_eq!(bytea::bytea_get_bit(v, 21).unwrap(), 0);
    assert_eq!(
        bytea::bytea_get_bit(v, 24).unwrap_err().sqlstate(),
        types_error::ERRCODE_ARRAY_SUBSCRIPT_ERROR
    );

    let r = bytea::bytea_set_byte(mcx, v, 0, 0xab).unwrap();
    assert_eq!(r.data(), &[0xab, 0xff, 0x10]);
    let r = bytea::bytea_set_bit(mcx, v, 0, 1).unwrap();
    assert_eq!(r.data(), &[0x01, 0xff, 0x10]);
    let r = bytea::bytea_set_bit(mcx, v, 8, 0).unwrap();
    assert_eq!(r.data(), &[0x00, 0xfe, 0x10]);
    let err = bytea::bytea_set_bit(mcx, v, 0, 2).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(err.message, "new bit must be 0 or 1");
}

#[test]
fn bytea_cmp_family() {
    use crate::bytea::*;
    assert!(byteaeq(b"a\0b", b"a\0b"));
    assert!(byteane(b"a", b"b"));
    assert!(bytealt(b"a", b"ab"));
    assert!(byteale(b"a", b"a"));
    assert!(byteagt(b"b", b"a"));
    assert!(byteage(b"b", b"b"));
    // Raw memcmp magnitude, like C (C 18.3: byteacmp('\xff','\x01') → 254).
    assert_eq!(byteacmp(b"\xff", b"\x01"), 254);
    assert_eq!(bytea_larger(b"a", b"b"), b"b");
    assert_eq!(bytea_smaller(b"a", b"b"), b"a");
}

#[test]
fn fc_wrappers_dispatch() {
    use datum::Datum;
    use types_fmgr::LocalFcinfo;

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let a = cstring_to_text(mcx, b"aa").unwrap();
    let b = cstring_to_text(mcx, b"ab").unwrap();

    let mut fcinfo = LocalFcinfo::<2>::new(C);
    fcinfo.set_arg(0, Datum::from_usize(a.as_bytes().as_ptr() as usize));
    fcinfo.set_arg(1, Datum::from_usize(b.as_bytes().as_ptr() as usize));

    assert!(!crate::builtins::fc_texteq(None, &mut fcinfo).unwrap().as_bool());
    assert!(crate::builtins::fc_text_lt(None, &mut fcinfo).unwrap().as_bool());
    assert_eq!(
        crate::builtins::fc_bttextcmp(None, &mut fcinfo).unwrap().as_i32(),
        -1
    );
    // larger returns arg1's pointer word (C pointer identity).
    let larger = crate::builtins::fc_text_larger(None, &mut fcinfo).unwrap();
    assert_eq!(larger.as_usize(), b.as_bytes().as_ptr() as usize);

    let mut flinfo = types_fmgr::FmgrInfo::unresolved();
    let out = crate::builtins::fc_textout(Some(&mut flinfo), &mut fcinfo).unwrap();
    let cstr = unsafe { core::ffi::CStr::from_ptr(out.as_usize() as *const _) };
    assert_eq!(cstr.to_bytes(), b"aa");
}

#[test]
fn builtin_table_matches_declared_arity() {
    let non_strict = [3535u32, 3536, 3543, 3544, 6299, 394, 376, 6160, 6161];
    let retset = [6160u32, 6161];
    for row in crate::builtins::VARLENA_BUILTINS {
        assert_eq!(row.strict, !non_strict.contains(&row.foid), "{}", row.name);
        assert_eq!(row.retset, retset.contains(&row.foid), "{}", row.name);
        // unicode_version/icu_unicode_version are 0-arg catalog functions.
        assert!((0..=4).contains(&row.nargs), "{}", row.name);
    }
}

fn install_mb_for_levenshtein() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Real mbutils fns: tests flip encodings via SetDatabaseEncoding.
        mbutils_seams::pg_database_encoding_max_length::set(
            mbutils::pg_database_encoding_max_length,
        );
        mbutils_seams::pg_mbstrlen_with_len::set(mbutils::pg_mbstrlen_with_len);
        mbutils_seams::pg_mblen_range::set(mbutils::pg_mblen_range);
    });
}

fn install_detoast_seams() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(detoast::init_seams);
}

fn install_text_type_shape() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        syscache_seams::lookup_pg_type_shape::set(|typid| {
            Ok((typid == types_core::TEXTOID).then_some(types_tuple::tupdesc::PgTypeShape {
                typlen: -1,
                typbyval: false,
                typalign: b'i' as i8,
                typstorage: b'x' as i8,
                typcollation: 100,
            }))
        });
    });
}

#[test]
fn levenshtein_matches_c_values() {
    install_mb_for_levenshtein();
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let ln = |s: &str, t: &str| {
        levenshtein::varstr_levenshtein(mcx, s.as_bytes(), t.as_bytes(), 1, 1, 1, false).unwrap()
    };
    assert_eq!(ln("kitten", "sitting"), 3);
    assert_eq!(ln("", "abc"), 3);
    assert_eq!(ln("abc", ""), 3);
    assert_eq!(ln("same", "same"), 0);
    assert_eq!(ln("ctid", "cttid"), 1);
    // Pinned against live PG 18.3 fuzzystrmatch: levenshtein('extensive','exhaustive',2,1,5).
    assert_eq!(
        levenshtein::varstr_levenshtein(mcx, b"extensive", b"exhaustive", 2, 1, 5, false)
            .unwrap(),
        11
    );
}

#[test]
fn levenshtein_less_equal_bound_and_multibyte() {
    install_mb_for_levenshtein();
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let lle = |s: &str, t: &str, max_d: i32| {
        levenshtein::varstr_levenshtein_less_equal(
            mcx,
            s.as_bytes(),
            t.as_bytes(),
            1,
            1,
            1,
            max_d,
            true,
        )
        .unwrap()
    };
    assert_eq!(lle("kitten", "sitting", 2), 3);
    assert_eq!(lle("kitten", "sitting", 3), 3);
    assert_eq!(lle("kitten", "sitting", 10), 3);
    // Pinned against live PG 18.3: levenshtein_less_equal('extensive','exhaustive',2) = 3.
    assert_eq!(lle("extensive", "exhaustive", 2), 3);
    assert_eq!(lle("café", "cafe", 4), 1);
    assert_eq!(lle("日本語", "日本", 4), 1);
    assert_eq!(lle("colname", "colname", 3), 0);
    assert_eq!(lle("a", "zzzzzzzz", 3), 4);
}

#[test]
fn levenshtein_untrusted_length_cap_is_22023() {
    install_mb_for_levenshtein();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let long = "x".repeat(256);
    let err = levenshtein::varstr_levenshtein(mcx, long.as_bytes(), b"y", 1, 1, 1, false)
        .unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);
    assert_eq!(
        err.message,
        "levenshtein argument exceeds maximum length of 255 characters"
    );
    assert!(
        levenshtein::varstr_levenshtein(mcx, long.as_bytes(), b"y", 1, 1, 1, true).is_ok()
    );
}

mod fc_results {
    use datum::{Datum, VarlenaRef};
    use mcx::MemoryContext;
    use types_fmgr::{
        direct_function_call1_coll_in, direct_function_call2_coll_in,
        direct_function_call3_coll_in, LocalFcinfo,
    };

    use crate::builtins::*;

    fn text_image(s: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + s.len()));
        v.extend_from_slice(s);
        v
    }

    fn text_of(d: Datum) -> &'static [u8] {
        // SAFETY: test results are live 4B-header varlenas kept in the ctx.
        unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) }.data()
    }

    #[test]
    fn textcat_and_byteacat() {
        let ctx = MemoryContext::new_bump("t");
        let a = text_image(b"foo");
        let b = text_image(b"bar");
        let d = direct_function_call2_coll_in(
            fc_textcat,
            0,
            ctx.mcx(),
            Datum::from_usize(a.as_ptr() as usize),
            Datum::from_usize(b.as_ptr() as usize),
        )
        .unwrap();
        assert_eq!(text_of(d), b"foobar");
        let d = direct_function_call2_coll_in(
            fc_byteacat,
            0,
            ctx.mcx(),
            Datum::from_usize(a.as_ptr() as usize),
            Datum::from_usize(b.as_ptr() as usize),
        )
        .unwrap();
        assert_eq!(text_of(d), b"foobar");
    }

    #[test]
    fn byteain_hex() {
        let ctx = MemoryContext::new_bump("t");
        let d = direct_function_call1_coll_in(
            fc_byteain,
            0,
            ctx.mcx(),
            Datum::from_usize(b"\\x6465616462656566\0".as_ptr() as usize),
        )
        .unwrap();
        assert_eq!(text_of(d), b"deadbeef");
    }

    #[test]
    fn unknownin_copies_cstring() {
        let ctx = MemoryContext::new_bump("t");
        let src = b"who knows\0";
        let d = direct_function_call1_coll_in(
            fc_unknownin,
            0,
            ctx.mcx(),
            Datum::from_usize(src.as_ptr() as usize),
        )
        .unwrap();
        let p = d.as_usize() as *const u8;
        assert_ne!(p, src.as_ptr());
        // SAFETY: unknownin result is a live NUL-terminated cstring in ctx.
        let got = unsafe { core::ffi::CStr::from_ptr(p.cast()) };
        assert_eq!(got.to_bytes(), b"who knows");
    }

    #[test]
    #[should_panic(expected = "never armed")]
    fn textcat_unarmed_panics() {
        let a = text_image(b"x");
        let _ = types_fmgr::direct_function_call2_coll(
            fc_textcat,
            0,
            Datum::from_usize(a.as_ptr() as usize),
            Datum::from_usize(a.as_ptr() as usize),
        );
    }

    #[test]
    fn replace_text_wrapper() {
        let ctx = MemoryContext::new_bump("t");
        let src = text_image(b"foobarbaz");
        let from = text_image(b"bar");
        let to = text_image(b"XX");
        let d = direct_function_call3_coll_in(
            fc_replace_text,
            types_core::C_COLLATION_OID,
            ctx.mcx(),
            Datum::from_usize(src.as_ptr() as usize),
            Datum::from_usize(from.as_ptr() as usize),
            Datum::from_usize(to.as_ptr() as usize),
        )
        .unwrap();
        assert_eq!(text_of(d), b"fooXXbaz");
    }

    #[test]
    fn overlay_wrappers() {
        let ctx = MemoryContext::new_bump("t");
        let t1 = text_image(b"Txxxxas");
        let t2 = text_image(b"hom");

        let mut fcinfo = LocalFcinfo::<4>::new(0);
        // SAFETY: ctx outlives this call.
        unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
        fcinfo.set_arg(0, Datum::from_usize(t1.as_ptr() as usize));
        fcinfo.set_arg(1, Datum::from_usize(t2.as_ptr() as usize));
        fcinfo.set_arg(2, Datum::from_i32(2));
        fcinfo.set_arg(3, Datum::from_i32(4));
        let d = fc_textoverlay(None, &mut fcinfo).unwrap();
        assert_eq!(text_of(d), b"Thomas");

        let d = fc_byteaoverlay(None, &mut fcinfo).unwrap();
        assert_eq!(text_of(d), b"Thomas");

        // no_len defaults sl to length(t2) = 3: overlay('Txxxas' placing
        // 'hom' from 2) = 'Thomas'.
        let t1_short = text_image(b"Txxxas");
        let mut fcinfo3 = LocalFcinfo::<3>::new(0);
        // SAFETY: ctx outlives this call.
        unsafe { fcinfo3.set_result_mcx(ctx.mcx()) };
        fcinfo3.set_arg(0, Datum::from_usize(t1_short.as_ptr() as usize));
        fcinfo3.set_arg(1, Datum::from_usize(t2.as_ptr() as usize));
        fcinfo3.set_arg(2, Datum::from_i32(2));
        let d = fc_textoverlay_no_len(None, &mut fcinfo3).unwrap();
        assert_eq!(text_of(d), b"Thomas");
    }

    #[test]
    fn bytea_bit_count_wrapper() {
        let img = text_image(&[0xffu8, 0x01]);
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        fcinfo.set_arg(0, Datum::from_usize(img.as_ptr() as usize));
        assert_eq!(fc_bytea_bit_count(None, &mut fcinfo).unwrap().as_i64(), 9);
    }

    #[test]
    fn convert_to_base_wrappers() {
        let ctx = MemoryContext::new_bump("t");
        let d = direct_function_call1_coll_in(fc_to_hex32, 0, ctx.mcx(), Datum::from_i32(255))
            .unwrap();
        assert_eq!(text_of(d), b"ff");
        let d = direct_function_call1_coll_in(fc_to_bin64, 0, ctx.mcx(), Datum::from_i64(5))
            .unwrap();
        assert_eq!(text_of(d), b"101");
        let d = direct_function_call1_coll_in(fc_to_oct32, 0, ctx.mcx(), Datum::from_i32(8))
            .unwrap();
        assert_eq!(text_of(d), b"10");
    }

    #[test]
    fn text_to_table_srf_value_per_call() {
        use types_fmgr::{ExprDoneCond, FmgrInfo, ReturnSetInfo, SFRM_ValuePerCall};

        let ctx = MemoryContext::new_bump("t");
        let input = text_image(b"a,,b");
        let sep = text_image(b",");

        let mut flinfo = FmgrInfo::new(crate::split_text::fc_text_to_table, 6160, 2, false, true);
        let mut rsinfo = ReturnSetInfo::new(SFRM_ValuePerCall);
        let mut fci = LocalFcinfo::<2>::new(types_core::C_COLLATION_OID);
        // SAFETY: ctx outlives the call loop.
        unsafe { fci.set_result_mcx(ctx.mcx()) };
        fci.set_arg(0, Datum::from_usize(input.as_ptr() as usize));
        fci.set_arg(1, Datum::from_usize(sep.as_ptr() as usize));

        let mut out: Vec<Vec<u8>> = Vec::new();
        loop {
            fci.isnull = false;
            rsinfo.isDone = ExprDoneCond::ExprSingleResult;
            // Re-arm per invoke: the isDone write above invalidates a
            // previously armed pointer's provenance (miri F6).
            fci.resultinfo = rsinfo.as_fmnode_ptr();
            let d = flinfo.invoke(&mut fci).unwrap();
            if rsinfo.isDone == ExprDoneCond::ExprEndResult {
                assert!(fci.isnull);
                break;
            }
            assert_eq!(rsinfo.isDone, ExprDoneCond::ExprMultipleResult);
            assert!(!fci.isnull);
            out.push(text_of(d).to_vec());
        }
        assert_eq!(out, vec![b"a".to_vec(), b"".to_vec(), b"b".to_vec()]);
        assert!(!flinfo.has_fn_extra(), "SRF_RETURN_DONE tears down the multi-call frame");

        // NULL input string: split_text's early-false return, zero rows.
        let mut flinfo2 = FmgrInfo::new(crate::split_text::fc_text_to_table, 6160, 2, false, true);
        let mut fci2 = LocalFcinfo::<2>::new(0);
        // SAFETY: ctx outlives this call.
        unsafe { fci2.set_result_mcx(ctx.mcx()) };
        fci2.set_arg_null(0);
        fci2.set_arg_null(1);
        rsinfo.isDone = ExprDoneCond::ExprSingleResult;
        fci2.isnull = false;
        // Arm after the isDone write (miri F6).
        fci2.resultinfo = rsinfo.as_fmnode_ptr();
        let _ = flinfo2.invoke(&mut fci2).unwrap();
        assert_eq!(rsinfo.isDone, ExprDoneCond::ExprEndResult);
    }

    #[test]
    fn text_to_table_null_string() {
        use types_fmgr::{ExprDoneCond, FmgrInfo, ReturnSetInfo, SFRM_ValuePerCall};

        let ctx = MemoryContext::new_bump("t");
        let input = text_image(b"a,N,b");
        let sep = text_image(b",");
        let nullstr = text_image(b"N");

        let mut flinfo =
            FmgrInfo::new(crate::split_text::fc_text_to_table, 6161, 3, false, true);
        let mut rsinfo = ReturnSetInfo::new(SFRM_ValuePerCall);
        let mut fci = LocalFcinfo::<3>::new(types_core::C_COLLATION_OID);
        // SAFETY: ctx outlives the call loop.
        unsafe { fci.set_result_mcx(ctx.mcx()) };
        fci.set_arg(0, Datum::from_usize(input.as_ptr() as usize));
        fci.set_arg(1, Datum::from_usize(sep.as_ptr() as usize));
        fci.set_arg(2, Datum::from_usize(nullstr.as_ptr() as usize));

        let mut out: Vec<Option<Vec<u8>>> = Vec::new();
        loop {
            fci.isnull = false;
            rsinfo.isDone = ExprDoneCond::ExprSingleResult;
            // Re-arm per invoke: the isDone write above invalidates a
            // previously armed pointer's provenance (miri F6).
            fci.resultinfo = rsinfo.as_fmnode_ptr();
            let d = flinfo.invoke(&mut fci).unwrap();
            if rsinfo.isDone == ExprDoneCond::ExprEndResult {
                break;
            }
            out.push(if fci.isnull { None } else { Some(text_of(d).to_vec()) });
        }
        assert_eq!(out, vec![Some(b"a".to_vec()), None, Some(b"b".to_vec())]);
    }
}

mod text_surface {
    use mcx::MemoryContext;
    use types_core::C_COLLATION_OID;
    use wchar::{PG_SQL_ASCII, PG_UTF8};

    use crate::*;

    const C: u32 = C_COLLATION_OID;

    fn text_image(t: &str) -> Vec<u8> {
        let mut img = vec![0u8; 4];
        img.extend_from_slice(t.as_bytes());
        let hdr = datum::varlena::set_varsize_4b(img.len());
        img[..4].copy_from_slice(&hdr);
        img
    }

    fn substr(mcx: Mcx<'_>, t: &str, s: i32, l: i32) -> String {
        crate::tests::install_mb_for_levenshtein();
        crate::tests::install_detoast_seams();
        let img = text_image(t);
        String::from_utf8(text_substring(mcx, &img, s, l, false).unwrap().data().to_vec())
            .unwrap()
    }

    fn substr_no_len(mcx: Mcx<'_>, t: &str, s: i32) -> String {
        crate::tests::install_mb_for_levenshtein();
        crate::tests::install_detoast_seams();
        let img = text_image(t);
        String::from_utf8(text_substring(mcx, &img, s, -1, true).unwrap().data().to_vec())
            .unwrap()
    }

    #[test]
    fn text_substring_single_byte_arms() {
        mbutils::SetDatabaseEncoding(PG_SQL_ASCII).unwrap();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        assert_eq!(substr(mcx, "hello", 2, 3), "ell");
        assert_eq!(substr(mcx, "hello", -2, 5), "he");
        assert_eq!(substr(mcx, "hello", -5, 3), "");
        assert_eq!(substr(mcx, "hello", 2, i32::MAX), "ello");
        assert_eq!(substr(mcx, "hello", i32::MIN, i32::MAX), "");
        assert_eq!(substr(mcx, "hello", 99, 1), "");
        assert_eq!(substr(mcx, "hello", 1, 0), "");
        assert_eq!(substr(mcx, "", 1, 3), "");
        assert_eq!(substr_no_len(mcx, "hello", 3), "llo");
        assert_eq!(substr_no_len(mcx, "hello", -7), "hello");
        assert_eq!(substr_no_len(mcx, "hello", i32::MIN), "hello");
        let err = text_substring(mcx, b"hello", 1, -2, false).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_SUBSTRING_ERROR);
        assert_eq!(err.message, "negative substring length not allowed");
    }

    #[test]
    fn text_substring_multibyte_arms() {
        mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        assert_eq!(substr(mcx, "日本語abc", 2, 2), "本語");
        assert_eq!(substr(mcx, "日本語", 1, 1), "日");
        assert_eq!(substr(mcx, "héllo", 2, 3), "éll");
        assert_eq!(substr(mcx, "a😀b", 2, 1), "😀");
        assert_eq!(substr(mcx, "abc", -3, 4), "");
        assert_eq!(substr(mcx, "日本語", 2, i32::MAX), "本語");
        assert_eq!(substr(mcx, "日本語", 99, 1), "");
        assert_eq!(substr(mcx, "日本語", -1, 3), "日");
        assert_eq!(substr_no_len(mcx, "日本語", 2), "本語");
        assert_eq!(substr_no_len(mcx, "日本語", -5), "日本語");
        let err = text_substring(mcx, "日本語".as_bytes(), 1, -1, false).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_SUBSTRING_ERROR);
        assert_eq!(err.message, "negative substring length not allowed");
    }

    #[test]
    fn textpos_arms() {
        mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
        let p = |h: &str, n: &str| textpos(h.as_bytes(), n.as_bytes(), C).unwrap();
        assert_eq!(p("abcabc", "bc"), 2);
        assert_eq!(p("abcabc", ""), 1);
        assert_eq!(p("ab", "abc"), 0);
        assert_eq!(p("abc", "xy"), 0);
        assert_eq!(p("abc", "c"), 3);
        assert_eq!(p("日本語", "語"), 3);
        assert_eq!(p("日本語abc日本語", "本"), 2);
        assert_eq!(p("xxx", "xx"), 1);
        assert_eq!(p("", "a"), 0);
        let long = "z".repeat(5000) + "needle" + &"z".repeat(100);
        assert_eq!(p(&long, "needle"), 5001);
        assert_eq!(p(&long, "absent-needle"), 0);
    }

    #[test]
    fn text_position_next_skips_matched_portion() {
        mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
        let mut state = text_position_setup(b"xxx", b"xx", C).unwrap();
        assert!(text_position_next(&mut state).unwrap());
        assert_eq!(text_position_get_match_off(&state), 0);
        assert!(!text_position_next(&mut state).unwrap());
        text_position_reset(&mut state);
        assert!(text_position_next(&mut state).unwrap());
        assert_eq!(text_position_get_match_pos(&mut state).unwrap(), 1);
    }

    #[test]
    fn split_part_arms() {
        mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let sp = |s: &str, sep: &str, n: i32| {
            String::from_utf8(
                split_part(mcx, s.as_bytes(), sep.as_bytes(), n, C).unwrap().data().to_vec(),
            )
            .unwrap()
        };
        assert_eq!(sp("abc~@~def~@~ghi", "~@~", 1), "abc");
        assert_eq!(sp("abc~@~def~@~ghi", "~@~", 2), "def");
        assert_eq!(sp("abc~@~def~@~ghi", "~@~", 3), "ghi");
        assert_eq!(sp("abc~@~def~@~ghi", "~@~", 4), "");
        assert_eq!(sp("abc~@~def~@~ghi", "~@~", -1), "ghi");
        assert_eq!(sp("abc~@~def~@~ghi", "~@~", -3), "abc");
        assert_eq!(sp("abc~@~def~@~ghi", "~@~", -4), "");
        assert_eq!(sp("abc,def", ",", -2), "abc");
        assert_eq!(sp("abc", ",", 1), "abc");
        assert_eq!(sp("abc", ",", -1), "abc");
        assert_eq!(sp("abc", ",", 2), "");
        assert_eq!(sp("abc", "", 1), "abc");
        assert_eq!(sp("abc", "", -1), "abc");
        assert_eq!(sp("abc", "", 2), "");
        assert_eq!(sp("", ",", 1), "");
        assert_eq!(sp("a,,b", ",", 2), "");
        assert_eq!(sp("日、本、語", "、", 2), "本");
        let err = split_part(mcx, b"abc", b",", 0, C).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_PARAMETER_VALUE);
        assert_eq!(err.message, "field position must not be zero");
    }

    #[test]
    fn replace_text_arms() {
        mbutils::SetDatabaseEncoding(PG_UTF8).unwrap();
        let ctx = MemoryContext::new("t");
        let mcx = ctx.mcx();
        let rp = |s: &str, from: &str, to: &str| {
            String::from_utf8(
                replace_text(mcx, s.as_bytes(), from.as_bytes(), to.as_bytes(), C)
                    .unwrap()
                    .data()
                    .to_vec(),
            )
            .unwrap()
        };
        assert_eq!(rp("abcdef", "cd", "XX"), "abXXef");
        assert_eq!(rp("yabadabadoo", "ba", "123"), "ya123da123doo");
        assert_eq!(rp("abab", "ab", ""), "");
        assert_eq!(rp("abc", "xyz", "q"), "abc");
        assert_eq!(rp("", "a", "b"), "");
        assert_eq!(rp("abc", "", "q"), "abc");
        assert_eq!(rp("aaa", "a", "aa"), "aaaaaa");
        assert_eq!(rp("日本語", "本", "外"), "日外語");
    }
}

mod string_agg_fns {
    use datum::{Datum, VarlenaRef};
    use mcx::MemoryContext;
    use types_fmgr::{AggStateNode, LocalFcinfo};

    use crate::builtins::*;

    fn text_image(s: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + s.len()));
        v.extend_from_slice(s);
        v
    }

    fn run_string_agg(rows: &[Option<&str>], delim: Option<&str>) -> Option<String> {
        let agg_ctx = MemoryContext::new_bump("aggcontext");
        let mut node = AggStateNode::new(agg_ctx);
        let result_ctx = MemoryContext::new_bump("per-tuple");

        let delim_img = delim.map(|d| text_image(d.as_bytes()));
        let mut state = Datum::null();
        let mut state_null = true;
        for row in rows {
            let mut fcinfo = LocalFcinfo::<3>::new(0);
            fcinfo.context = node.fm_node_ptr();
            if !state_null {
                fcinfo.set_arg(0, state);
            }
            let img = row.map(|v| text_image(v.as_bytes()));
            if let Some(img) = &img {
                fcinfo.set_arg(1, Datum::from_usize(img.as_ptr() as usize));
            }
            if let Some(d) = &delim_img {
                fcinfo.set_arg(2, Datum::from_usize(d.as_ptr() as usize));
            }
            state = fc_string_agg_transfn(None, &mut fcinfo).unwrap();
            state_null = fcinfo.isnull;
        }

        let mut fcinfo = LocalFcinfo::<1>::new(0);
        fcinfo.context = node.fm_node_ptr();
        // SAFETY: result_ctx outlives the call below.
        unsafe { fcinfo.set_result_mcx(result_ctx.mcx()) };
        if !state_null {
            fcinfo.set_arg(0, state);
        }
        let d = fc_string_agg_finalfn(None, &mut fcinfo).unwrap();
        if fcinfo.isnull {
            return None;
        }
        // SAFETY: the finalfn result is a live 4B-header varlena in result_ctx.
        let bytes = unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) }.data().to_vec();
        Some(String::from_utf8(bytes).unwrap())
    }

    #[test]
    fn string_agg_basic_and_null_handling() {
        assert_eq!(run_string_agg(&[Some("a"), Some("b"), Some("c")], Some(",")).unwrap(), "a,b,c");
        assert_eq!(run_string_agg(&[Some("a"), None, Some("c")], Some("+")).unwrap(), "a+c");
        assert_eq!(run_string_agg(&[Some("solo")], Some(",")).unwrap(), "solo");
        assert_eq!(run_string_agg(&[Some("a"), Some("b")], None).unwrap(), "ab");
        assert_eq!(run_string_agg(&[None, None], Some(",")), None);
        assert_eq!(run_string_agg(&[], Some(",")), None);
        assert_eq!(run_string_agg(&[Some(""), Some("")], Some(",")).unwrap(), ",");
        assert_eq!(
            run_string_agg(&[Some("日本"), Some("語")], Some("、")).unwrap(),
            "日本、語"
        );
        let big: Vec<Option<&str>> = vec![Some("0123456789abcdef"); 200];
        assert_eq!(run_string_agg(&big, Some("|")).unwrap().len(), 200 * 16 + 199);
    }

    #[test]
    fn string_agg_transfn_outside_agg_context_errors() {
        let img = text_image(b"x");
        let mut fcinfo = LocalFcinfo::<3>::new(0);
        fcinfo.set_arg(1, Datum::from_usize(img.as_ptr() as usize));
        fcinfo.set_arg_null(0);
        fcinfo.set_arg_null(2);
        let err = fc_string_agg_transfn(None, &mut fcinfo).unwrap_err();
        assert_eq!(err.message, "string_agg_transfn called in non-aggregate context");
    }

    #[test]
    fn bytea_string_agg_matches_text_shape() {
        let agg_ctx = MemoryContext::new_bump("aggcontext");
        let mut node = AggStateNode::new(agg_ctx);
        let result_ctx = MemoryContext::new_bump("per-tuple");
        let vals = [text_image(&[0xde, 0xad]), text_image(&[0xbe, 0xef])];
        let delim = text_image(&[0x00]);
        let mut state = Datum::null();
        let mut state_null = true;
        for v in &vals {
            let mut fcinfo = LocalFcinfo::<3>::new(0);
            fcinfo.context = node.fm_node_ptr();
            if !state_null {
                fcinfo.set_arg(0, state);
            }
            fcinfo.set_arg(1, Datum::from_usize(v.as_ptr() as usize));
            fcinfo.set_arg(2, Datum::from_usize(delim.as_ptr() as usize));
            state = fc_bytea_string_agg_transfn(None, &mut fcinfo).unwrap();
            state_null = fcinfo.isnull;
        }
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        fcinfo.context = node.fm_node_ptr();
        // SAFETY: result_ctx outlives the call below.
        unsafe { fcinfo.set_result_mcx(result_ctx.mcx()) };
        fcinfo.set_arg(0, state);
        let d = fc_bytea_string_agg_finalfn(None, &mut fcinfo).unwrap();
        // SAFETY: live 4B-header varlena in result_ctx.
        let out = unsafe { VarlenaRef::from_ptr(d.as_usize() as *const u8) }.data().to_vec();
        assert_eq!(out, vec![0xde, 0xad, 0x00, 0xbe, 0xef]);
    }

    #[test]
    #[should_panic(expected = "abbreviated-key SortSupport unported")]
    fn bttextsortsupport_is_loud() {
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        let _ = fc_bttextsortsupport(None, &mut fcinfo);
    }

    #[test]
    fn string_agg_combine_rejects_non_aggregate_context() {
        let mut fcinfo = LocalFcinfo::<2>::new(0);
        let err = fc_string_agg_combine(None, &mut fcinfo).unwrap_err();
        assert!(err.message().contains("aggregate function called in non-aggregate context"));
    }
}

// unistr rows diffed vs live C 18.3 (psql, 2026-07-03); server encoding UTF8.
#[test]
fn unistr_rows() {
    mbutils::SetDatabaseEncoding(wchar::PG_UTF8).unwrap();
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let u = |s: &str| {
        crate::unistr(mcx, s.as_bytes()).map(|v| String::from_utf8_lossy(v.data()).into_owned())
    };
    assert_eq!(u(r"d\0061t\+000061 \\ A \U0001F603").unwrap(), "data \\ A \u{1F603}");
    assert_eq!(u(r"perl \0441\043B\043E\043D").unwrap(), "perl слон");
    assert_eq!(u(r"\D83D\DE03").unwrap(), "\u{1F603}");
    assert_eq!(u("plain").unwrap(), "plain");
    assert_eq!(u(r"\D83D").unwrap_err().to_string(), "invalid Unicode surrogate pair");
    assert_eq!(u(r"\DE03\D83D").unwrap_err().to_string(), "invalid Unicode surrogate pair");
    assert_eq!(u(r"\D83Dx").unwrap_err().to_string(), "invalid Unicode surrogate pair");
    assert_eq!(u(r"\xyz").unwrap_err().to_string(), "invalid Unicode escape");
    assert_eq!(u(r"\+00D800").unwrap_err().to_string(), "invalid Unicode surrogate pair");
    assert_eq!(u(r"\0000").unwrap_err().to_string(), "invalid Unicode code point: 0000");
}

#[test]
fn text_to_array_arms() {
    use datum::Datum;
    use mcx::Mcx;
    use types_fmgr::LocalFcinfo;

    install_text_type_shape();

    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    fn run(
        mcx: Mcx<'_>,
        input: Option<&[u8]>,
        sep: Option<&[u8]>,
        ns: Option<&[u8]>,
    ) -> Option<std::vec::Vec<Option<std::string::String>>> {
        let mut fcinfo = LocalFcinfo::<3>::new(C);
        // SAFETY: mcx outlives the call.
        unsafe { fcinfo.set_result_mcx(mcx) };
        let mut hold = std::vec::Vec::new();
        for (i, v) in [input, sep, ns].into_iter().enumerate() {
            match v {
                Some(b) => {
                    let t = cstring_to_text(mcx, b).unwrap();
                    fcinfo.set_arg(i, Datum::from_usize(t.as_bytes().as_ptr() as usize));
                    hold.push(t);
                }
                None => fcinfo.set_arg_null(i),
            }
        }
        let d = crate::split_text::fc_text_to_array(None, &mut fcinfo).unwrap();
        if fcinfo.isnull {
            return None;
        }
        let p = d.as_usize() as *const u8;
        let img = unsafe {
            core::slice::from_raw_parts(p, arrayfuncs::foundation::varsize_any(p))
        };
        let (elems, nulls) =
            arrayfuncs::deconstruct_array_builtin(mcx, img, types_core::TEXTOID, true).unwrap();
        Some(
            elems
                .iter()
                .zip(nulls.iter())
                .map(|(e, &isnull)| {
                    if isnull {
                        None
                    } else {
                        let ep = e.as_usize() as *const u8;
                        let n = arrayfuncs::foundation::varsize_any(ep);
                        let payload =
                            unsafe { core::slice::from_raw_parts(ep.add(4), n - 4) };
                        Some(std::string::String::from_utf8(payload.to_vec()).unwrap())
                    }
                })
                .collect(),
        )
    }

    let s = |x: &str| Some(x.to_string());
    assert_eq!(
        run(mcx, Some(b"1|2|3"), Some(b"|"), None),
        Some(vec![s("1"), s("2"), s("3")])
    );
    assert_eq!(run(mcx, Some(b""), Some(b"|"), None), Some(vec![]));
    assert_eq!(run(mcx, Some(b"abc"), Some(b""), None), Some(vec![s("abc")]));
    assert_eq!(
        run(mcx, Some(b"abc"), None, None),
        Some(vec![s("a"), s("b"), s("c")])
    );
    assert_eq!(
        run(mcx, Some(b"1|NULL|3"), Some(b"|"), Some(b"NULL")),
        Some(vec![s("1"), None, s("3")])
    );
    assert_eq!(
        run(mcx, Some(b"1||2"), Some(b"|"), None),
        Some(vec![s("1"), s(""), s("2")])
    );
    assert_eq!(run(mcx, None, Some(b"|"), None), None);
    // NULL separator + null_string still applies per-character
    assert_eq!(
        run(mcx, Some(b"ab"), None, Some(b"b")),
        Some(vec![s("a"), None])
    );
}

#[test]
fn bytea_int_conversions() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    assert_eq!(bytea::bytea_int2(&[]).unwrap(), 0);
    assert_eq!(bytea::bytea_int2(&[0x12]).unwrap(), 0x12);
    assert_eq!(bytea::bytea_int2(&[0x80, 0x00]).unwrap(), i16::MIN);
    assert_eq!(bytea::bytea_int2(&[0xff, 0xff]).unwrap(), -1);
    let err = bytea::bytea_int2(&[1, 2, 3]).unwrap_err();
    assert_eq!(err.sqlstate(), types_error::ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
    assert_eq!(err.message(), "smallint out of range");

    assert_eq!(bytea::bytea_int4(&[0xde, 0xad, 0xbe, 0xef]).unwrap(), -559038737);
    let err = bytea::bytea_int4(&[0; 5]).unwrap_err();
    assert_eq!(err.message(), "integer out of range");

    assert_eq!(bytea::bytea_int8(&[0xff; 8]).unwrap(), -1);
    let err = bytea::bytea_int8(&[0; 9]).unwrap_err();
    assert_eq!(err.message(), "bigint out of range");

    let v = bytea::int_bytea(mcx, &0x1234i16.to_be_bytes()).unwrap();
    assert_eq!(v.data(), &[0x12, 0x34]);
    assert_eq!(bytea::bytea_int2(v.data()).unwrap(), 0x1234);
    let v = bytea::int_bytea(mcx, &(-1i64).to_be_bytes()).unwrap();
    assert_eq!(v.data(), &[0xff; 8]);
}

#[test]
fn bytea_reverse_cases() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    assert_eq!(bytea::bytea_reverse(mcx, b"abc").unwrap().data(), b"cba");
    assert_eq!(bytea::bytea_reverse(mcx, b"").unwrap().data(), b"");
    assert_eq!(bytea::bytea_reverse(mcx, b"x").unwrap().data(), b"x");
}

// format(VARIADIC text[]): element Datums borrow the detoasted array image
// (FormatArgs._array keepalive). Run under both allocator disciplines: Aset
// free-list header clobber AND bump rewind-reuse corrupt a dropped image.
mod format_variadic {
    use super::*;
    use datum::Datum;
    use types_fmgr::{FmgrInfo, LocalFcinfo};

    const TEXTOID: types_core::Oid = 25;
    const TEXTOUT: types_core::Oid = 46;

    fn install() {
        static ONCE: std::sync::Once = std::sync::Once::new();
        ONCE.call_once(|| {
            install_detoast_seams();
            install_text_type_shape();
            syscache_seams::pg_type_io_shape::set(|typid| {
                Ok((typid == TEXTOID).then_some(syscache_seams::PgTypeIoShape {
                    oid: TEXTOID,
                    typinput: 1,
                    typoutput: TEXTOUT,
                    typreceive: 1,
                    typsend: 1,
                    typmodin: 0,
                    typmodout: 0,
                    typelem: 0,
                    typlen: -1,
                    typbyval: false,
                    typalign: b'i' as i8,
                    typdelim: b',' as i8,
                    typisdefined: true,
                }))
            });
            fmgr_seams::fmgr_info::set(|oid| {
                assert_eq!(oid, TEXTOUT);
                Ok(FmgrInfo::new(crate::builtins::fc_textout, TEXTOUT, 1, true, false))
            });
            fmgr_seams::get_fn_expr_variadic::set(|_flinfo| true);
            fmgr_seams::get_fn_expr_argtype::set(|_flinfo, _argnum| TEXTOID);
        });
    }

    fn run(armed: &MemoryContext) {
        install();
        let ctx = MemoryContext::new("format args");
        let mcx = ctx.mcx();
        let elems = [
            cstring_to_text(mcx, b"a").unwrap(),
            cstring_to_text(mcx, b"bb").unwrap(),
            cstring_to_text(mcx, b"ccc").unwrap(),
        ];
        let datums: Vec<Datum> = elems
            .iter()
            .map(|t| Datum::from_usize(t.as_bytes().as_ptr() as usize))
            .collect();
        let array =
            arrayfuncs::construct_array(mcx, &datums, TEXTOID, -1, false, b'i').unwrap();
        let fmt = cstring_to_text(mcx, b"%s+%s+%s").unwrap();

        let mut fcinfo = LocalFcinfo::<2>::new(C);
        fcinfo.set_arg(0, Datum::from_usize(fmt.as_bytes().as_ptr() as usize));
        fcinfo.set_arg(1, Datum::from_usize(array.as_ptr() as usize));
        // SAFETY: armed outlives the call and the result reads below.
        unsafe { fcinfo.set_result_mcx(armed.mcx()) };

        let mut flinfo = FmgrInfo::unresolved();
        let out = crate::concat_format::fc_text_format(Some(&mut flinfo), &mut fcinfo).unwrap();
        let p = out.as_usize() as *const u8;
        // SAFETY: live text varlena result.
        let (len, data) = unsafe {
            let n = types_tuple::varatt::varsize_any(p);
            (n, core::slice::from_raw_parts(p.add(4), n - 4))
        };
        assert_eq!(len - 4, 8);
        assert_eq!(data, b"a+bb+ccc", "element datums read after the detoast temporary's scope");
    }

    #[test]
    fn survives_aset_free_list() {
        let armed = MemoryContext::new("t");
        run(&armed);
    }

    #[test]
    fn survives_bump_rewind() {
        let armed = MemoryContext::new_bump("t");
        run(&armed);
    }
}

mod pg_column_funcs {
    use datum::Datum;
    use mcx::MemoryContext;
    use types_fmgr::{FmgrInfo, LocalFcinfo, PGFunction};

    use crate::builtins::*;

    fn install() {
        super::install_detoast_seams();
        super::install_text_type_shape();
        if !fmgr_seams::get_fn_expr_argtype::is_installed() {
            fmgr_seams::get_fn_expr_argtype::set(|_flinfo, _argnum| types_core::TEXTOID);
        }
    }

    fn text_image(s: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(4 + s.len());
        v.extend_from_slice(&datum::varlena::set_varsize_4b(4 + s.len()));
        v.extend_from_slice(s);
        v
    }

    fn call(func: PGFunction, image: &[u8]) -> (Datum, bool) {
        install();
        let ctx = MemoryContext::new("t");
        let mut flinfo = FmgrInfo::unresolved();
        let mut fcinfo = LocalFcinfo::<1>::new(0);
        // SAFETY: ctx outlives the call.
        unsafe { fcinfo.set_result_mcx(ctx.mcx()) };
        fcinfo.set_arg(0, Datum::from_usize(image.as_ptr() as usize));
        let d = func(Some(&mut flinfo), &mut fcinfo).unwrap();
        (d, fcinfo.isnull)
    }

    #[test]
    fn pg_column_size_inline_varlena() {
        let image = text_image(b"hello");
        let (d, isnull) = call(fc_pg_column_size, &image);
        assert!(!isnull);
        assert_eq!(d.as_i32(), image.len() as i32);
    }

    #[test]
    fn pg_column_compression_uncompressed_is_null() {
        let image = text_image(b"hello");
        let (_, isnull) = call(fc_pg_column_compression, &image);
        assert!(isnull);
    }

    #[test]
    fn pg_column_toast_chunk_id_inline_is_null() {
        let image = text_image(b"hello");
        let (_, isnull) = call(fc_pg_column_toast_chunk_id, &image);
        assert!(isnull);
    }
}

// fnconf batch-1, OIDs 246/253 (btnametextcmp/bttextnamecmp) and the
// bttextcmp/bpcharcmp siblings: C's varstr_cmp returns the RAW memcmp
// difference under C collation (varlena.c); only the equal-prefix length
// tie-break is ±1. C 18.3: btnametextcmp('a'::name, 'c') → -2.
// Red at base: pgrust sign-normalized every result to ±1.
#[test]
fn varstrfastcmp_c_returns_raw_memcmp_magnitude() {
    assert_eq!(varstrfastcmp_c(b"a", b"c"), -2);
    assert_eq!(varstrfastcmp_c(b"c", b"a"), 2);
    assert_eq!(varstrfastcmp_c(b"abz", b"abd"), b'z' as i32 - b'd' as i32);
    assert_eq!(varstrfastcmp_c(b"\x00", b"\xff"), -255);
    // Equal-prefix length tie-break stays normalized, like C.
    assert_eq!(varstrfastcmp_c(b"ab", b"abcd"), -1);
    assert_eq!(varstrfastcmp_c(b"abcd", b"ab"), 1);
    assert_eq!(varstrfastcmp_c(b"", b""), 0);
}
