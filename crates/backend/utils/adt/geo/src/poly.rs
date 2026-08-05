use ::adt_float::{float8_div, float8_lt, float8_pl};
use ::types_core::geo::{Point, BOX, CIRCLE, LSEG};
use ::types_error::PgResult;

use crate::lseg::{lseg_contain_point, lseg_interpt_lseg, statlseg_construct};
use crate::point::point_add_point;
use crate::proximity::lseg_closept_lseg;
use crate::{box_contain_box, box_ov, plist_same, point_dt, point_eq_point, point_inside, PolyRef, Pts};

pub fn poly_left(a: &PolyRef<'_>, b: &PolyRef<'_>) -> bool {
    a.boundbox.high.x < b.boundbox.low.x
}

pub fn poly_overleft(a: &PolyRef<'_>, b: &PolyRef<'_>) -> bool {
    a.boundbox.high.x <= b.boundbox.high.x
}

pub fn poly_right(a: &PolyRef<'_>, b: &PolyRef<'_>) -> bool {
    a.boundbox.low.x > b.boundbox.high.x
}

pub fn poly_overright(a: &PolyRef<'_>, b: &PolyRef<'_>) -> bool {
    a.boundbox.low.x >= b.boundbox.low.x
}

pub fn poly_below(a: &PolyRef<'_>, b: &PolyRef<'_>) -> bool {
    a.boundbox.high.y < b.boundbox.low.y
}

pub fn poly_overbelow(a: &PolyRef<'_>, b: &PolyRef<'_>) -> bool {
    a.boundbox.high.y <= b.boundbox.high.y
}

pub fn poly_above(a: &PolyRef<'_>, b: &PolyRef<'_>) -> bool {
    a.boundbox.low.y > b.boundbox.high.y
}

pub fn poly_overabove(a: &PolyRef<'_>, b: &PolyRef<'_>) -> bool {
    a.boundbox.low.y >= b.boundbox.low.y
}

pub fn poly_same(a: &PolyRef<'_>, b: &PolyRef<'_>) -> bool {
    if a.n() != b.n() {
        false
    } else {
        plist_same(a, b)
    }
}

pub fn poly_overlap_internal(a: &PolyRef<'_>, b: &PolyRef<'_>) -> PgResult<bool> {
    debug_assert!(a.n() != 0 && b.n() != 0);

    if !box_ov(&a.boundbox, &b.boundbox) {
        return Ok(false);
    }

    let na = a.n();
    let nb = b.n();
    let mut sa = LSEG {
        p: [a.pt(na - 1), Point::default()],
    };

    for ia in 0..na {
        sa.p[1] = a.pt(ia);

        let mut sb = LSEG {
            p: [b.pt(nb - 1), Point::default()],
        };
        for ib in 0..nb {
            sb.p[1] = b.pt(ib);
            if lseg_interpt_lseg(None, &sa, &sb)? {
                return Ok(true);
            }
            sb.p[0] = sb.p[1];
        }
        sa.p[0] = sa.p[1];
    }

    Ok(point_inside(&a.pt(0), b)? != 0 || point_inside(&b.pt(0), a)? != 0)
}

pub fn poly_overlap(a: &PolyRef<'_>, b: &PolyRef<'_>) -> PgResult<bool> {
    poly_overlap_internal(a, b)
}

fn touched_lseg_inside_poly(
    a: &Point,
    b: &Point,
    s: &LSEG,
    poly: &PolyRef<'_>,
    start: usize,
) -> PgResult<bool> {
    // a is on s, b is not.
    let t = LSEG { p: [*a, *b] };

    if point_eq_point(a, &s.p[0]) {
        if lseg_contain_point(&t, &s.p[1])? {
            return lseg_inside_poly(b, &s.p[1], poly, start);
        }
    } else if point_eq_point(a, &s.p[1]) {
        if lseg_contain_point(&t, &s.p[0])? {
            return lseg_inside_poly(b, &s.p[0], poly, start);
        }
    } else if lseg_contain_point(&t, &s.p[0])? {
        return lseg_inside_poly(b, &s.p[0], poly, start);
    } else if lseg_contain_point(&t, &s.p[1])? {
        return lseg_inside_poly(b, &s.p[1], poly, start);
    }

    Ok(true)
}

fn lseg_inside_poly(a: &Point, b: &Point, poly: &PolyRef<'_>, start: usize) -> PgResult<bool> {
    ::stack_depth::check_stack_depth()?;

    let t = LSEG { p: [*a, *b] };
    let mut res = true;
    let mut intersection = false;

    let npts = poly.n();
    let first = if start == 0 { npts - 1 } else { start - 1 };
    let mut s = LSEG {
        p: [poly.pt(first), Point::default()],
    };

    let mut i = start;
    while i < npts && res {
        ::postgres_seams::check_for_interrupts::call()?;

        s.p[1] = poly.pt(i);

        if lseg_contain_point(&s, &t.p[0])? {
            if lseg_contain_point(&s, &t.p[1])? {
                return Ok(true);
            }
            res = touched_lseg_inside_poly(&t.p[0], &t.p[1], &s, poly, i + 1)?;
        } else if lseg_contain_point(&s, &t.p[1])? {
            res = touched_lseg_inside_poly(&t.p[1], &t.p[0], &s, poly, i + 1)?;
        } else {
            let mut interpt = Point::default();
            if lseg_interpt_lseg(Some(&mut interpt), &t, &s)? {
                intersection = true;
                res = lseg_inside_poly(&t.p[0], &interpt, poly, i + 1)?;
                if res {
                    res = lseg_inside_poly(&t.p[1], &interpt, poly, i + 1)?;
                }
            }
        }

        s.p[0] = s.p[1];
        i += 1;
    }

    if res && !intersection {
        let p = Point {
            x: float8_div(float8_pl(t.p[0].x, t.p[1].x)?, 2.0)?,
            y: float8_div(float8_pl(t.p[0].y, t.p[1].y)?, 2.0)?,
        };
        res = point_inside(&p, poly)? != 0;
    }

    Ok(res)
}

pub fn poly_contain_poly(contains: &PolyRef<'_>, contained: &PolyRef<'_>) -> PgResult<bool> {
    debug_assert!(contains.n() != 0 && contained.n() != 0);

    if !box_contain_box(&contains.boundbox, &contained.boundbox) {
        return Ok(false);
    }

    let nb = contained.n();
    let mut s = LSEG {
        p: [contained.pt(nb - 1), Point::default()],
    };
    for i in 0..nb {
        s.p[1] = contained.pt(i);
        if !lseg_inside_poly(&s.p[0], &s.p[1], contains, 0)? {
            return Ok(false);
        }
        s.p[0] = s.p[1];
    }

    Ok(true)
}

pub fn poly_contain(a: &PolyRef<'_>, b: &PolyRef<'_>) -> PgResult<bool> {
    poly_contain_poly(a, b)
}

pub fn poly_contained(a: &PolyRef<'_>, b: &PolyRef<'_>) -> PgResult<bool> {
    poly_contain_poly(b, a)
}

pub fn poly_contain_pt(poly: &PolyRef<'_>, p: &Point) -> PgResult<bool> {
    Ok(point_inside(p, poly)? != 0)
}

pub fn pt_contained_poly(p: &Point, poly: &PolyRef<'_>) -> PgResult<bool> {
    Ok(point_inside(p, poly)? != 0)
}

pub fn poly_distance(a: &PolyRef<'_>, b: &PolyRef<'_>) -> PgResult<Option<f64>> {
    if poly_overlap_internal(a, b)? {
        return Ok(Some(0.0));
    }

    let mut min = 0.0;
    let mut have_min = false;
    let na = a.n();
    let nb = b.n();

    for i in 0..na {
        let iprev = if i > 0 { i - 1 } else { na - 1 };
        for j in 0..nb {
            let jprev = if j > 0 { j - 1 } else { nb - 1 };
            let seg1 = statlseg_construct(&a.pt(iprev), &a.pt(i));
            let seg2 = statlseg_construct(&b.pt(jprev), &b.pt(j));
            let tmp = lseg_closept_lseg(None, &seg1, &seg2)?;
            if !have_min || float8_lt(tmp, min) {
                min = tmp;
                have_min = true;
            }
        }
    }

    if !have_min {
        return Ok(None);
    }
    Ok(Some(min))
}

pub fn poly_npoints(poly: &PolyRef<'_>) -> i32 {
    poly.n() as i32
}

pub fn poly_center(poly: &PolyRef<'_>) -> PgResult<Point> {
    Ok(poly_to_circle(poly)?.center)
}

pub fn poly_box(poly: &PolyRef<'_>) -> BOX {
    poly.boundbox
}

pub fn poly_to_circle(poly: &PolyRef<'_>) -> PgResult<CIRCLE> {
    debug_assert!(poly.n() != 0);

    let npts = poly.n() as f64;
    let mut center = Point { x: 0.0, y: 0.0 };
    for i in 0..poly.n() {
        center = point_add_point(&center, &poly.pt(i))?;
    }
    center.x = float8_div(center.x, npts)?;
    center.y = float8_div(center.y, npts)?;

    let mut radius = 0.0;
    for i in 0..poly.n() {
        radius = float8_pl(radius, point_dt(&poly.pt(i), &center)?)?;
    }
    radius = float8_div(radius, npts)?;

    Ok(CIRCLE { center, radius })
}

pub fn poly_circle(poly: &PolyRef<'_>) -> PgResult<CIRCLE> {
    poly_to_circle(poly)
}
