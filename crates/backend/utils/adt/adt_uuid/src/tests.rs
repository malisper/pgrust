use super::*;
use ::types_error::SoftErrorContext;

const NIL: pg_uuid_t = pg_uuid_t {
    data: [0u8; UUID_LEN],
};

fn parse(s: &str) -> pg_uuid_t {
    uuid_in(s.as_bytes(), None).expect("valid uuid")
}

fn render(u: &pg_uuid_t) -> String {
    let mut b = uuid_out(u);
    assert_eq!(b.last(), Some(&0), "uuid_out must NUL-terminate");
    b.pop();
    String::from_utf8(b).unwrap()
}

#[test]
fn in_out_roundtrip_canonical() {
    let s = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11";
    let u = parse(s);
    assert_eq!(render(&u), s);
    assert_eq!(
        u.data,
        [
            0xa0, 0xee, 0xbc, 0x99, 0x9c, 0x0b, 0x4e, 0xf8, 0xbb, 0x6d, 0x6b, 0xb9, 0xbd, 0x38,
            0x0a, 0x11
        ]
    );
}

#[test]
fn in_accepts_relaxed_forms() {
    let canon = "a0eebc999c0b4ef8bb6d6bb9bd380a11";
    let braces = "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}";
    let upper = "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11";
    let expect = parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    assert_eq!(parse(canon), expect);
    assert_eq!(parse(braces), expect);
    assert_eq!(parse(upper), expect);
}

#[test]
fn in_rejects_bad_syntax_hard() {
    for bad in [
        "",
        "not-a-uuid",
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a1",  // too short
        "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a111", // too long
        "g0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",  // non-hex
        "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", // unclosed brace
    ] {
        let r = uuid_in(bad.as_bytes(), None);
        assert!(r.is_err(), "{bad:?} should hard-error");
        assert_eq!(
            r.unwrap_err().sqlstate(),
            ::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION
        );
    }
}

#[test]
fn in_soft_error_saves_not_throws() {
    let mut ctx = SoftErrorContext::new(true);
    let r = uuid_in(b"bad", Some(&mut ctx));
    assert!(r.is_ok(), "soft context must not throw");
    assert!(ctx.error_occurred());
    let e = ctx.take_error().expect("error saved");
    assert_eq!(e.sqlstate(), ::types_error::ERRCODE_INVALID_TEXT_REPRESENTATION);
    assert_eq!(
        e.message(),
        "invalid input syntax for type uuid: \"bad\""
    );
}

#[test]
fn comparisons_match_memcmp() {
    let a = parse("00000000-0000-0000-0000-000000000000");
    let b = parse("00000000-0000-0000-0000-000000000001");
    assert!(uuid_lt(&a, &b));
    assert!(uuid_le(&a, &b));
    assert!(uuid_ne(&a, &b));
    assert!(uuid_gt(&b, &a));
    assert!(uuid_ge(&b, &a));
    assert!(uuid_eq(&a, &a));
    assert_eq!(uuid_cmp(&a, &a), 0);
    assert!(uuid_internal_cmp(&a, &b) < 0);
    assert!(uuid_internal_cmp(&b, &a) > 0);
    // memcmp byte-difference, not normalized.
    let hi = parse("ff000000-0000-0000-0000-000000000000");
    assert_eq!(uuid_internal_cmp(&hi, &a), 0xff);
}

#[test]
fn hash_is_hash_any_of_data() {
    let u = parse("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    assert_eq!(uuid_hash(&u), hashfn::hash_bytes(&u.data));
    assert_eq!(
        uuid_hash_extended(&u, 0x1234),
        hashfn::hash_bytes_extended(&u.data, 0x1234)
    );
}

#[test]
fn extract_version_and_timestamp_v7() {
    // RFC 9562 v7: version nibble 7, variant 0b10.
    let u = parse("018f0c0e-9c0b-7ef8-bb6d-6bb9bd380a11");
    assert_eq!(uuid_extract_version(&u), Some(7));
    let ts = uuid_extract_timestamp(&u).expect("v7 has a timestamp");
    // Recompute expected: ms big-endian in bytes 0..6.
    let ms: u64 = ((u.data[0] as u64) << 40)
        | ((u.data[1] as u64) << 32)
        | ((u.data[2] as u64) << 24)
        | ((u.data[3] as u64) << 16)
        | ((u.data[4] as u64) << 8)
        | (u.data[5] as u64);
    let expect = (ms * US_PER_MS as u64) as i64
        - (POSTGRES_EPOCH_JDATE as i64 - UNIX_EPOCH_JDATE as i64)
            * SECS_PER_DAY as i64
            * USECS_PER_SEC;
    assert_eq!(ts, expect);
}

#[test]
fn extract_returns_none_for_non_rfc_variant() {
    // variant bits not 0b10 -> NULL.
    let mut u = NIL;
    u.data[8] = 0x00;
    assert_eq!(uuid_extract_version(&u), None);
    assert_eq!(uuid_extract_timestamp(&u), None);
}

#[test]
fn extract_timestamp_none_for_v4() {
    // v4 (random): version 4, variant ok -> version Some(4) but no timestamp.
    let mut u = NIL;
    u.data[6] = 0x40; // version 4
    u.data[8] = 0x80; // variant 0b10
    assert_eq!(uuid_extract_version(&u), Some(4));
    assert_eq!(uuid_extract_timestamp(&u), None);
}

#[test]
fn increment_decrement_inverse_and_wrap() {
    let mut u = [0u8; UUID_LEN];
    u[UUID_LEN - 1] = 5;
    assert!(!uuid_increment(&mut u));
    assert_eq!(u[UUID_LEN - 1], 6);
    assert!(!uuid_decrement(&mut u));
    assert_eq!(u[UUID_LEN - 1], 5);

    // carry across bytes
    let mut c = [0u8; UUID_LEN];
    c[UUID_LEN - 1] = 0xFF;
    assert!(!uuid_increment(&mut c));
    assert_eq!(c[UUID_LEN - 1], 0x00);
    assert_eq!(c[UUID_LEN - 2], 0x01);

    // overflow: all 0xFF -> true
    let mut max = [0xFFu8; UUID_LEN];
    assert!(uuid_increment(&mut max));
    // underflow: all 0x00 -> true
    let mut min = [0x00u8; UUID_LEN];
    assert!(uuid_decrement(&mut min));
}

#[test]
fn skipsupport_boundaries() {
    let s = uuid_skipsupport();
    assert_eq!(s.low_elem.data, [0x00; UUID_LEN]);
    assert_eq!(s.high_elem.data, [0xFF; UUID_LEN]);
}

#[test]
fn set_version_sets_version_and_variant() {
    let mut d = [0xFFu8; UUID_LEN];
    uuid_set_version(&mut d, 4);
    assert_eq!(d[6] >> 4, 4); // version nibble
    assert_eq!(d[6] & 0x0f, 0x0f); // low nibble preserved
    assert_eq!(d[8] & 0xc0, 0x80); // variant 0b10
    assert_eq!(d[8] & 0x3f, 0x3f); // low six bits preserved
}
