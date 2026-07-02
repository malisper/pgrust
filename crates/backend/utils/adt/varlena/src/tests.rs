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
fn wire_io_round_trips() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        // Identity client<->server conversion (single-encoding test setup).
        mbutils_seams::pg_client_to_server::set(|_, _| Ok(None));
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
fn bytea_cmp_family() {
    use crate::bytea::*;
    assert!(byteaeq(b"a\0b", b"a\0b"));
    assert!(byteane(b"a", b"b"));
    assert!(bytealt(b"a", b"ab"));
    assert!(byteale(b"a", b"a"));
    assert!(byteagt(b"b", b"a"));
    assert!(byteage(b"b", b"b"));
    assert_eq!(byteacmp(b"\xff", b"\x01"), 1);
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
    for row in crate::builtins::VARLENA_BUILTINS {
        assert!(row.strict && !row.retset);
        assert!(row.nargs == 1 || row.nargs == 2, "{}", row.name);
    }
}
