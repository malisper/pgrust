use ::mcx::MemoryContext;
use ::types_core::geo::{Point, CIRCLE, LINE, LSEG};
use ::types_error::{ERRCODE_INVALID_TEXT_REPRESENTATION, ERRCODE_PROGRAM_LIMIT_EXCEEDED};

use crate::*;

fn p(x: f64, y: f64) -> Point {
    Point { x, y }
}

fn out_str(f: impl FnOnce(&mut Vec<u8>)) -> String {
    let mut out = Vec::new();
    f(&mut out);
    String::from_utf8(out).unwrap()
}

fn parse_path(ctx: &MemoryContext, s: &str) -> (bool, Vec<Point>) {
    let v = io::path_in(ctx.mcx(), s, None).unwrap();
    let r = PathRef::from_payload(v.data());
    (r.closed, (0..r.n()).map(|i| r.pt(i)).collect())
}

#[test]
fn lseg_io() {
    let ls = io::lseg_in("[(0,0),(1,1)]", None).unwrap();
    assert_eq!(ls.p, [p(0.0, 0.0), p(1.0, 1.0)]);
    assert_eq!(out_str(|o| io::lseg_out(&ls, o)), "[(0,0),(1,1)]");
    assert!(io::lseg_in("[(0,0),(1,1)", None).is_err());
    let ls = io::lseg_in("(0,0),(6e300,Infinity)", None).unwrap();
    assert_eq!(
        out_str(|o| io::lseg_out(&ls, o)),
        "[(0,0),(6e+300,Infinity)]"
    );
}

#[test]
fn line_io() {
    let l = io::line_in("{1,2,3}", None).unwrap();
    assert_eq!(l, LINE { A: 1.0, B: 2.0, C: 3.0 });
    assert_eq!(out_str(|o| io::line_out(&l, o)), "{1,2,3}");

    let err = io::line_in("{0,0,5}", None).unwrap_err();
    assert_eq!(
        err.message(),
        "invalid line specification: A and B cannot both be zero"
    );
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TEXT_REPRESENTATION);

    let l2 = io::line_in("[(0,0),(1,0)]", None).unwrap();
    assert_eq!(l2, LINE { A: 0.0, B: -1.0, C: 0.0 });
    let err = io::line_in("[(0,0),(0,0)]", None).unwrap_err();
    assert_eq!(
        err.message(),
        "invalid line specification: must be two distinct points"
    );
    assert!(io::line_in("{1,2}", None).is_err());
    assert!(io::line_in("{nan,nan,nan}", None).is_ok());
}

#[test]
fn circle_io() {
    let c = io::circle_in("<(1,2),3>", None).unwrap();
    assert_eq!(c.center, p(1.0, 2.0));
    assert_eq!(c.radius, 3.0);
    assert_eq!(out_str(|o| io::circle_out(&c, o)), "<(1,2),3>");
    for s in ["((1,2),3)", "1,2,3", "(1,2),3"] {
        assert_eq!(io::circle_in(s, None).unwrap(), c);
    }
    assert!(io::circle_in("<(1,2),-1>", None).is_err());
    assert!(io::circle_in("<(1,2),NaN>", None).unwrap().radius.is_nan());
    assert!(io::circle_in("<(1,2),3> x", None).is_err());
}

#[test]
fn path_io() {
    let ctx = MemoryContext::new("t");
    let (closed, pts) = parse_path(&ctx, "((0,0),(1,0),(1,1))");
    assert!(closed);
    assert_eq!(pts, vec![p(0.0, 0.0), p(1.0, 0.0), p(1.0, 1.0)]);

    let (closed, pts) = parse_path(&ctx, "[(0,0),(1,0)]");
    assert!(!closed);
    assert_eq!(pts.len(), 2);

    let v = io::path_in(ctx.mcx(), "((0,0),(1,0),(1,1))", None).unwrap();
    let r = PathRef::from_payload(v.data());
    assert_eq!(out_str(|o| io::path_out(&r, o)), "((0,0),(1,0),(1,1))");

    assert!(io::path_in(ctx.mcx(), "(0,0),(1,1)x", None).is_err());
    assert!(io::path_in(ctx.mcx(), "", None).is_err());
    assert!(io::path_in(ctx.mcx(), "(0,0",  None).is_err());
}

#[test]
fn poly_io_and_boundbox() {
    let ctx = MemoryContext::new("t");
    let v = io::poly_in(ctx.mcx(), "((0,0),(2,0),(2,2),(0,2))", None).unwrap();
    let r = PolyRef::from_payload(v.data());
    assert_eq!(r.n(), 4);
    assert_eq!(r.boundbox.low, p(0.0, 0.0));
    assert_eq!(r.boundbox.high, p(2.0, 2.0));
    assert_eq!(
        out_str(|o| io::poly_out(&r, o)),
        "((0,0),(2,0),(2,2),(0,2))"
    );
    assert!(io::poly_in(ctx.mcx(), "((0,0),(1,1)", None).is_err());
    assert!(io::poly_in(ctx.mcx(), "0,0", None).is_ok());
}

#[test]
fn recv_send_roundtrip() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();

    let pt = p(1.5, -2.5);
    let sent = io::point_send(mcx, &pt).unwrap();
    let mut buf = ::stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(sent.data()).unwrap();
    assert_eq!(io::point_recv(&mut buf).unwrap(), pt);

    let v = io::path_in(mcx, "((0,0),(1,1))", None).unwrap();
    let r = PathRef::from_payload(v.data());
    let sent = io::path_send(mcx, &r).unwrap();
    let mut buf = ::stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(sent.data()).unwrap();
    let back = io::path_recv(mcx, &mut buf).unwrap();
    assert_eq!(back.as_bytes(), v.as_bytes());

    let mut buf = ::stringinfo::StringInfo::new_in(mcx).unwrap();
    buf.append_bytes(&[0u8, 255, 255, 255, 255]).unwrap();
    assert!(io::path_recv(mcx, &mut buf).is_err());

    let mut buf = ::stringinfo::StringInfo::new_in(mcx).unwrap();
    for v in [0.0f64, 0.0, -1.0] {
        buf.append_bytes(&v.to_bits().to_be_bytes()).unwrap();
    }
    let err = io::circle_recv(&mut buf).unwrap_err();
    assert_eq!(err.message(), "invalid radius in external \"circle\" value");

    let mut buf = ::stringinfo::StringInfo::new_in(mcx).unwrap();
    for v in [0.0f64, 0.0, 0.0] {
        buf.append_bytes(&v.to_bits().to_be_bytes()).unwrap();
    }
    let err = io::line_recv(&mut buf).unwrap_err();
    assert_eq!(
        err.message(),
        "invalid line specification: A and B cannot both be zero"
    );
}

#[test]
fn point_arithmetic() {
    assert_eq!(
        point::point_mul_point(&p(1.0, 2.0), &p(3.0, 4.0)).unwrap(),
        p(-5.0, 10.0)
    );
    assert_eq!(
        point::point_add_point(&p(1.0, 2.0), &p(3.0, 4.0)).unwrap(),
        p(4.0, 6.0)
    );
    assert_eq!(
        point::point_div_point(&p(-5.0, 10.0), &p(3.0, 4.0)).unwrap(),
        p(1.0, 2.0)
    );
    let err = point::point_div_point(&p(1.0, 1.0), &p(0.0, 0.0)).unwrap_err();
    assert_eq!(err.message(), "division by zero");
}

#[test]
fn lseg_ops() {
    let l1 = io::lseg_in("[(0,0),(2,0)]", None).unwrap();
    let l2 = io::lseg_in("[(0,1),(2,1)]", None).unwrap();
    assert_eq!(proximity::lseg_closept_lseg(None, &l1, &l2).unwrap(), 1.0);
    assert_eq!(proximity::close_lseg(&l1, &l2).unwrap(), None);
    assert!(lseg::lseg_parallel(&l1, &l2).unwrap());

    let l3 = io::lseg_in("[(0,0),(2,2)]", None).unwrap();
    let l4 = io::lseg_in("[(0,2),(2,0)]", None).unwrap();
    assert_eq!(lseg::lseg_interpt(&l3, &l4).unwrap(), Some(p(1.0, 1.0)));
    assert!(lseg::lseg_intersect(&l3, &l4).unwrap());
}

#[test]
fn box_point_proximity() {
    let b = box_in("(0,0),(2,2)").unwrap();
    let d = proximity::dist_pb(&p(5.0, 5.0), &b).unwrap();
    assert!((d - 4.242_640_687_119_286).abs() < 1e-12);
    assert_eq!(
        proximity::close_pb(&p(5.0, 1.0), &b).unwrap(),
        Some(p(2.0, 1.0))
    );
    assert_eq!(boxes::box_cn(&b).unwrap(), p(1.0, 1.0));
    assert_eq!(boxes::box_ar(&b).unwrap(), 4.0);
}

#[test]
fn poly_ops() {
    let ctx = MemoryContext::new("t");
    let mcx = ctx.mcx();
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| ::postgres_seams::check_for_interrupts::set(|| Ok(())));

    let a = io::poly_in(mcx, "((0,0),(2,0),(2,2),(0,2))", None).unwrap();
    let b = io::poly_in(mcx, "((1,1),(3,1),(3,3),(1,3))", None).unwrap();
    let (ra, rb) = (
        PolyRef::from_payload(a.data()),
        PolyRef::from_payload(b.data()),
    );
    assert!(poly::poly_overlap(&ra, &rb).unwrap());

    let outer = io::poly_in(mcx, "((0,0),(10,0),(10,10),(0,10))", None).unwrap();
    let inner = io::poly_in(mcx, "((2,2),(3,2),(3,3),(2,3))", None).unwrap();
    let (ro, ri) = (
        PolyRef::from_payload(outer.data()),
        PolyRef::from_payload(inner.data()),
    );
    assert!(poly::poly_contain(&ro, &ri).unwrap());
    assert!(!poly::poly_contain(&ri, &ro).unwrap());
    assert!(poly::poly_contain_pt(&ro, &p(5.0, 5.0)).unwrap());
    assert!(!poly::poly_contain_pt(&ri, &p(5.0, 5.0)).unwrap());
    assert!(poly::poly_same(&ra, &ra));
    assert!(!poly::poly_same(&ra, &rb));

    let c = CIRCLE {
        center: p(10.0, 10.0),
        radius: 1.0,
    };
    let d = proximity::dist_cpoly(&c, &ra).unwrap();
    assert!((d - 10.313_708_498_984_761).abs() < 1e-12);
}

#[test]
fn circle_poly_conversion() {
    let ctx = MemoryContext::new("t");
    let c = CIRCLE {
        center: p(0.0, 0.0),
        radius: 4.0,
    };
    let err = circle::circle_poly_checks(i32::MAX, &c).unwrap_err();
    assert_eq!(err.message(), "too many points requested");
    assert_eq!(err.sqlstate(), ERRCODE_PROGRAM_LIMIT_EXCEEDED);
    assert!(circle::circle_poly_checks(1, &c).is_err());
    assert!(circle::circle_poly_checks(8, &c).is_ok());

    let v = io::poly_in(ctx.mcx(), "((0,0),(4,0),(2,3))", None).unwrap();
    let r = PolyRef::from_payload(v.data());
    let cc = poly::poly_to_circle(&r).unwrap();
    assert_eq!(cc.center, p(2.0, 1.0));
}

#[test]
fn path_measures() {
    let ctx = MemoryContext::new("t");
    let v = io::path_in(ctx.mcx(), "((0,0),(1,0),(1,1),(0,1))", None).unwrap();
    let r = PathRef::from_payload(v.data());
    assert_eq!(path::path_area(&r).unwrap(), Some(1.0));
    assert_eq!(path::path_length(&r).unwrap(), 4.0);

    let open = io::path_in(ctx.mcx(), "[(0,0),(1,0),(1,1)]", None).unwrap();
    let ro = PathRef::from_payload(open.data());
    assert_eq!(path::path_area(&ro).unwrap(), None);
    assert_eq!(path::path_length(&ro).unwrap(), 2.0);
    assert!(proximity::on_ppath(&p(0.5, 0.0), &ro).unwrap());
    assert!(!proximity::on_ppath(&p(0.5, 0.5), &ro).unwrap());
}

#[test]
fn line_geometry() {
    let l1 = io::line_in("{1,-1,0}", None).unwrap();
    let l2 = io::line_in("{1,-1,2}", None).unwrap();
    assert!(line::line_parallel(&l1, &l2).unwrap());
    let d = line::line_distance(&l1, &l2).unwrap();
    assert!((d - core::f64::consts::SQRT_2).abs() < 1e-12);
    let l3 = io::line_in("{1,1,0}", None).unwrap();
    assert!(line::line_perp(&l1, &l3).unwrap());
    assert_eq!(line::line_interpt(&l1, &l3).unwrap(), Some(p(0.0, 0.0)));
    assert!(line::line_eq(&l1, &io::line_in("{2,-2,0}", None).unwrap()).unwrap());
}

#[test]
fn soft_error_capture() {
    let mut esc = ::types_error::SoftErrorContext::new(true);
    let r = io::point_in("(bogus,1)", Some(&mut esc)).unwrap();
    assert!(esc.error_occurred());
    assert_eq!(r, Point::default());

    let mut esc = ::types_error::SoftErrorContext::new(true);
    let r = io::circle_in("<(1,2),-1>", Some(&mut esc)).unwrap();
    assert!(esc.error_occurred());
    assert_eq!(r, CIRCLE::default());
}

#[test]
fn lseg_line_codecs() {
    let ls = LSEG {
        p: [p(1.0, 2.0), p(3.0, 4.0)],
    };
    assert_eq!(LSEG::from_datum_bytes(&ls.to_datum_bytes()), ls);
    let l = LINE {
        A: 1.0,
        B: -2.0,
        C: 3.5,
    };
    assert_eq!(LINE::from_datum_bytes(&l.to_datum_bytes()), l);
}
