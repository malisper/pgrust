use mcx::MemoryContext;

use crate::*;

fn install_mb() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        mbutils_seams::pg_mblen_range::set(|s| {
            Ok(std::str::from_utf8(s)
                .ok()
                .and_then(|t| t.chars().next())
                .map(|c| c.len_utf8() as i32)
                .unwrap_or(1))
        });
    });
}

fn enc(data: &[u8], name: &[u8]) -> Vec<u8> {
    let ctx = MemoryContext::new("t");
    let v = binary_encode(ctx.mcx(), data, name).unwrap().data().to_vec();
    v
}

fn dec(data: &[u8], name: &[u8]) -> Vec<u8> {
    let ctx = MemoryContext::new("t");
    let v = binary_decode(ctx.mcx(), data, name).unwrap().data().to_vec();
    v
}

#[test]
fn find_encoding_case_insensitive() {
    assert_eq!(pg_find_encoding(b"hex"), Some(Codec::Hex));
    assert_eq!(pg_find_encoding(b"HEX"), Some(Codec::Hex));
    assert_eq!(pg_find_encoding(b"Base64"), Some(Codec::Base64));
    assert_eq!(pg_find_encoding(b"ESCAPE"), Some(Codec::Escape));
    assert_eq!(pg_find_encoding(b"hex\0trailing"), Some(Codec::Hex));
    assert_eq!(pg_find_encoding(b"rot13"), None);
    assert_eq!(pg_find_encoding(b"hexx"), None);
}

#[test]
fn hex_round_trip() {
    assert_eq!(enc(b"abc", b"hex"), b"616263");
    assert_eq!(enc(&[0, 1, 2, 0xff], b"hex"), b"000102ff");
    assert_eq!(enc(b"", b"hex"), b"");
    assert_eq!(dec(b"616263", b"hex"), b"abc");
    // whitespace tolerated on decode.
    assert_eq!(dec(b"61 62\n63", b"hex"), b"abc");
}

#[test]
fn hex_decode_errors() {
    install_mb();
    let ctx = MemoryContext::new("t");
    let err = binary_decode(ctx.mcx(), b"6g", b"hex").unwrap_err();
    assert_eq!(err.message, "invalid hexadecimal digit: \"g\"");
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    let err = binary_decode(ctx.mcx(), b"616", b"hex").unwrap_err();
    assert_eq!(err.message, "invalid hexadecimal data: odd number of digits");
}

#[test]
fn base64_round_trip_rfc_vectors() {
    assert_eq!(enc(b"", b"base64"), b"");
    assert_eq!(enc(b"f", b"base64"), b"Zg==");
    assert_eq!(enc(b"fo", b"base64"), b"Zm8=");
    assert_eq!(enc(b"foo", b"base64"), b"Zm9v");
    assert_eq!(enc(b"foob", b"base64"), b"Zm9vYg==");
    assert_eq!(enc(b"fooba", b"base64"), b"Zm9vYmE=");
    assert_eq!(enc(b"foobar", b"base64"), b"Zm9vYmFy");
    for v in [
        &b""[..],
        b"f",
        b"fo",
        b"foo",
        b"foob",
        b"fooba",
        b"foobar",
    ] {
        assert_eq!(dec(&enc(v, b"base64"), b"base64"), v);
    }
}

#[test]
fn base64_wraps_at_76_columns() {
    // 60 input bytes -> 80 base64 chars -> one embedded newline after col 76.
    let data = vec![0x41u8; 60];
    let out = enc(&data, b"base64");
    let nl = out.iter().filter(|&&c| c == b'\n').count();
    assert_eq!(nl, 1);
    assert_eq!(out[76], b'\n');
    // Decoder skips the newline and round-trips.
    assert_eq!(dec(&out, b"base64"), data);
}

#[test]
fn base64_decode_errors() {
    install_mb();
    let ctx = MemoryContext::new("t");
    // stray '=' where no padding is expected.
    let err = binary_decode(ctx.mcx(), b"=", b"base64").unwrap_err();
    assert_eq!(err.message, "unexpected \"=\" while decoding base64 sequence");
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    // invalid symbol.
    let err = binary_decode(ctx.mcx(), b"Zm9*", b"base64").unwrap_err();
    assert_eq!(
        err.message,
        "invalid symbol \"*\" found while decoding base64 sequence"
    );
    // truncated (pos != 0 at end).
    let err = binary_decode(ctx.mcx(), b"Zm9", b"base64").unwrap_err();
    assert_eq!(err.message, "invalid base64 end sequence");
}

#[test]
fn escape_round_trip() {
    // 0x00 -> \000, 0x5c -> \\, 0x80 -> \200, printable stays literal.
    assert_eq!(enc(&[0x00, b'a', 0x5c, 0x80], b"escape"), b"\\000a\\\\\\200");
    assert_eq!(dec(b"\\000a\\\\\\200", b"escape"), &[0x00, b'a', 0x5c, 0x80]);
    // full byte range round-trips.
    let all: Vec<u8> = (0u8..=255).collect();
    assert_eq!(dec(&enc(&all, b"escape"), b"escape"), all);
}

#[test]
fn escape_decode_errors() {
    let ctx = MemoryContext::new("t");
    // lone backslash not followed by valid octal or backslash.
    let err = binary_decode(ctx.mcx(), b"a\\9", b"escape").unwrap_err();
    assert_eq!(err.message, "invalid input syntax for type bytea");
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
    let err = binary_decode(ctx.mcx(), b"trail\\", b"escape").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);
}

#[test]
fn unrecognized_codec_is_22023() {
    let ctx = MemoryContext::new("t");
    let err = binary_encode(ctx.mcx(), b"x", b"rot13").unwrap_err();
    assert_eq!(err.message, "unrecognized encoding: \"rot13\"");
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
    let err = binary_decode(ctx.mcx(), b"x", b"nope").unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
}

#[test]
fn all_codecs_round_trip_random_binary() {
    // A pseudo-random binary blob across every codec.
    let mut data = Vec::new();
    let mut x: u32 = 0x1234_5678;
    for _ in 0..1000 {
        x = x.wrapping_mul(1103515245).wrapping_add(12345);
        data.push((x >> 16) as u8);
    }
    for name in [&b"hex"[..], b"base64", b"escape"] {
        assert_eq!(dec(&enc(&data, name), name), data, "codec {name:?}");
    }
}

#[test]
fn builtin_table_arity() {
    for row in builtins::ENCODE_BUILTINS {
        assert_eq!(row.nargs, 2);
        assert!(row.strict && !row.retset);
    }
}
