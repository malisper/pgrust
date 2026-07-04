use mcx::MemoryContext;

use crate::*;

fn view<'a>(v: &'a ::datum::Varlena<'_>) -> SnapView<'a> {
    SnapView::new(v.data())
}

fn parse<'m>(mcx: ::mcx::Mcx<'m>, s: &str) -> PgResult<Option<::datum::Varlena<'m>>> {
    parse_snapshot(mcx, s, None)
}

#[test]
fn strtou64_libc_semantics() {
    assert_eq!(strtou64(b"12:13:"), (12, 2));
    assert_eq!(strtou64(b"  42"), (42, 4));
    assert_eq!(strtou64(b"+7x"), (7, 2));
    assert_eq!(strtou64(b"-1"), (u64::MAX, 2));
    assert_eq!(strtou64(b"18446744073709551616"), (u64::MAX, 20));
    assert_eq!(strtou64(b"x1"), (0, 0));
    assert_eq!(strtou64(b""), (0, 0));
    assert_eq!(strtou64(b"18446744073709551615"), (u64::MAX, 20));
}

#[test]
fn parse_out_round_trip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    for s in [
        "12:13:",
        "12:18:14,16",
        "12:16:14,14",
        "31:31:",
        "8589934593:8589934593:", // epoch 2
        "8589934593:8589934595:8589934594",
    ] {
        let v = parse(mcx, s).unwrap().unwrap();
        let out = snapshot_out_bytes(mcx, &view(&v)).unwrap();
        let expect = if s == "12:16:14,14" { "12:16:14" } else { s };
        assert_eq!(core::str::from_utf8(&out).unwrap(), expect, "input {s}");
    }
}

#[test]
fn parse_errors_22p02() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    for s in [
        "31:12:",   // xmax < xmin
        "0:1:",     // invalid xmin
        "12:13:0",  // xip < xmin
        "12:16:14,13", // out of order
        "12:16:14,,16",
        "12:16:14 16",
        "12",
        "12:",
        "12:13",
        ":",
        "",
        "12:13:14", // xip >= xmax
    ] {
        let err = parse(mcx, s).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_TEXT_REPRESENTATION, "input {s}");
        assert!(
            err.message().contains("invalid input syntax for type pg_snapshot"),
            "input {s}: {}",
            err.message()
        );
    }
}

#[test]
fn parse_soft_error_returns_none() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mut esc = types_error::SoftErrorContext::new(false);
    let r = parse_snapshot(mcx, "31:12:", Some(&mut esc)).unwrap();
    assert!(r.is_none());
    assert!(esc.error_occurred());
}

#[test]
fn image_layout_matches_c() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = parse(mcx, "12:18:14,16").unwrap().unwrap();
    // varlena total = 24-byte header block + 2 * 8.
    assert_eq!(v.varsize(), 24 + 16);
    let s = view(&v);
    assert_eq!(s.nxip(), 2);
    assert_eq!(s.xmin(), 12);
    assert_eq!(s.xmax(), 18);
    assert_eq!(s.xip(0), 14);
    assert_eq!(s.xip(1), 16);
}

#[test]
fn visible_fxid_linear_and_bsearch() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let v = parse(mcx, "10:100:20,30,40").unwrap().unwrap();
    let s = view(&v);
    assert!(is_visible_fxid(9, &s));
    assert!(is_visible_fxid(10, &s)); // not in xip
    assert!(!is_visible_fxid(100, &s));
    assert!(!is_visible_fxid(200, &s));
    assert!(!is_visible_fxid(30, &s));
    assert!(is_visible_fxid(31, &s));

    // > USE_BSEARCH_IF_NXIP_GREATER xips takes the bsearch arm.
    let xips: Vec<u64> = (100..164).step_by(2).collect();
    let img = snapshot_image(mcx, 100, 1000, &xips).unwrap();
    let s = view(&img);
    assert_eq!(s.nxip() as usize, xips.len());
    assert!(s.nxip() > USE_BSEARCH_IF_NXIP_GREATER);
    for &x in &xips {
        assert!(!is_visible_fxid(x, &s));
        assert!(is_visible_fxid(x + 1, &s));
    }
    assert!(is_visible_fxid(99, &s));
    assert!(is_visible_fxid(999, &s));
    assert!(!is_visible_fxid(1000, &s));
}

#[test]
fn recv_vectors() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    let mk = |nxip: i32, xmin: u64, xmax: u64, xips: &[u64]| {
        let mut b = Vec::new();
        b.extend_from_slice(&nxip.to_be_bytes());
        b.extend_from_slice(&xmin.to_be_bytes());
        b.extend_from_slice(&xmax.to_be_bytes());
        for &x in xips {
            b.extend_from_slice(&x.to_be_bytes());
        }
        b
    };

    let si_of = |wire: &[u8]| {
        let mut si = stringinfo::StringInfo::new_in(mcx).unwrap();
        si.append_bytes(wire).unwrap();
        si
    };

    let mut si = si_of(&mk(2, 12, 18, &[14, 16]));
    let v = snapshot_recv(mcx, &mut si).unwrap();
    let s = view(&v);
    assert_eq!((s.nxip(), s.xmin(), s.xmax()), (2, 12, 18));

    // duplicate xip collapses (C's i--/nxip-- dance).
    let mut si = si_of(&mk(3, 12, 18, &[14, 14, 16]));
    let v = snapshot_recv(mcx, &mut si).unwrap();
    let s = view(&v);
    assert_eq!(s.nxip(), 2);
    assert_eq!((s.xip(0), s.xip(1)), (14, 16));

    for bad in [
        mk(-1, 12, 18, &[]),
        mk(0, 18, 12, &[]),
        mk(0, 0, 12, &[]),
        mk(1, 12, 18, &[11]), // xip < xmin
        mk(1, 12, 18, &[19]), // xip > xmax
        mk(2, 12, 18, &[16, 14]), // out of order
    ] {
        let mut si = si_of(&bad);
        let err = snapshot_recv(mcx, &mut si).unwrap_err();
        assert_eq!(err.sqlstate(), types_error::ERRCODE_INVALID_BINARY_REPRESENTATION);
    }
}

#[test]
fn allowable_at_epochs() {
    // xid at or below next's low word: same epoch.
    assert_eq!(full_xid_from_allowable_at((3 << 32) | 100, 50), (3 << 32) | 50);
    // xid above next's low word: previous epoch.
    assert_eq!(full_xid_from_allowable_at((3 << 32) | 100, 200), (2 << 32) | 200);
    // special xids keep epoch 0.
    assert_eq!(full_xid_from_allowable_at((3 << 32) | 100, 2), 2);
    assert_eq!(full_xid_from_allowable_at((3 << 32) | 100, 0), 0);
}
