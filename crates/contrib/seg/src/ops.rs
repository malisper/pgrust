//! contrib/seg operators (seg.c): comparison with its boundary-type
//! ordering, R-tree relations, union/intersection, sizes.

use types_error::{PgError, PgResult};

use crate::repr::Seg;

/// `seg_cmp`: lower boundary position first, then boundary type, then
/// significant digits, then approximation flag; mirrored for the upper
/// boundary.
pub fn seg_cmp(a: &Seg, b: &Seg) -> PgResult<i32> {
    // lower boundary position
    if a.lower < b.lower {
        return Ok(-1);
    }
    if a.lower > b.lower {
        return Ok(1);
    }

    // type of boundary: '-' < '<' < (exact/'~') < '>'
    if a.l_ext != b.l_ext {
        if a.l_ext == b'-' {
            return Ok(-1);
        }
        if b.l_ext == b'-' {
            return Ok(1);
        }
        if a.l_ext == b'<' {
            return Ok(-1);
        }
        if b.l_ext == b'<' {
            return Ok(1);
        }
        if a.l_ext == b'>' {
            return Ok(1);
        }
        if b.l_ext == b'>' {
            return Ok(-1);
        }
    }

    // fewer significant digits = more blurred = smaller
    if a.l_sigd < b.l_sigd {
        return Ok(-1);
    }
    if a.l_sigd > b.l_sigd {
        return Ok(1);
    }

    // same digit count: approximate ('~') is more blurred than exact
    if a.l_ext != b.l_ext {
        if a.l_ext == b'~' {
            return Ok(-1);
        }
        if b.l_ext == b'~' {
            return Ok(1);
        }
        // can't get here unless data is corrupt
        return Err(Box::new(PgError::error(format!(
            "bogus lower boundary types {} {}",
            a.l_ext as i32, b.l_ext as i32
        ))));
    }

    // upper boundary position
    if a.upper < b.upper {
        return Ok(-1);
    }
    if a.upper > b.upper {
        return Ok(1);
    }

    // type of boundary: '<' < (exact/'~') < '>' < '-'
    if a.u_ext != b.u_ext {
        if a.u_ext == b'-' {
            return Ok(1);
        }
        if b.u_ext == b'-' {
            return Ok(-1);
        }
        if a.u_ext == b'<' {
            return Ok(-1);
        }
        if b.u_ext == b'<' {
            return Ok(1);
        }
        if a.u_ext == b'>' {
            return Ok(1);
        }
        if b.u_ext == b'>' {
            return Ok(-1);
        }
    }

    // converse of the lower-boundary cases
    if a.u_sigd < b.u_sigd {
        return Ok(1);
    }
    if a.u_sigd > b.u_sigd {
        return Ok(-1);
    }

    if a.u_ext != b.u_ext {
        if a.u_ext == b'~' {
            return Ok(1);
        }
        if b.u_ext == b'~' {
            return Ok(-1);
        }
        return Err(Box::new(PgError::error(format!(
            "bogus upper boundary types {} {}",
            a.u_ext as i32, b.u_ext as i32
        ))));
    }

    Ok(0)
}

pub fn seg_contains(a: &Seg, b: &Seg) -> bool {
    a.lower <= b.lower && a.upper >= b.upper
}

pub fn seg_overlap(a: &Seg, b: &Seg) -> bool {
    (a.upper >= b.upper && a.lower <= b.upper) || (b.upper >= a.upper && b.lower <= a.upper)
}

/// `seg_over_left` — right edge of (a) at or left of the right edge of (b).
pub fn seg_over_left(a: &Seg, b: &Seg) -> bool {
    a.upper <= b.upper
}

/// `seg_left` — (a) entirely left of (b).
pub fn seg_left(a: &Seg, b: &Seg) -> bool {
    a.upper < b.lower
}

/// `seg_right` — (a) entirely right of (b).
pub fn seg_right(a: &Seg, b: &Seg) -> bool {
    a.lower > b.upper
}

/// `seg_over_right` — left edge of (a) at or right of the left edge of (b).
pub fn seg_over_right(a: &Seg, b: &Seg) -> bool {
    a.lower >= b.lower
}

/// `seg_union`: max upper / min lower, boundary metadata rides along.
pub fn seg_union(a: &Seg, b: &Seg) -> Seg {
    let (upper, u_sigd, u_ext) = if a.upper > b.upper {
        (a.upper, a.u_sigd, a.u_ext)
    } else {
        (b.upper, b.u_sigd, b.u_ext)
    };
    let (lower, l_sigd, l_ext) = if a.lower < b.lower {
        (a.lower, a.l_sigd, a.l_ext)
    } else {
        (b.lower, b.l_sigd, b.l_ext)
    };
    Seg {
        lower,
        upper,
        l_sigd,
        u_sigd,
        l_ext,
        u_ext,
    }
}

/// `seg_inter`: min upper / max lower.
pub fn seg_inter(a: &Seg, b: &Seg) -> Seg {
    let (upper, u_sigd, u_ext) = if a.upper < b.upper {
        (a.upper, a.u_sigd, a.u_ext)
    } else {
        (b.upper, b.u_sigd, b.u_ext)
    };
    let (lower, l_sigd, l_ext) = if a.lower > b.lower {
        (a.lower, a.l_sigd, a.l_ext)
    } else {
        (b.lower, b.l_sigd, b.l_ext)
    };
    Seg {
        lower,
        upper,
        l_sigd,
        u_sigd,
        l_ext,
        u_ext,
    }
}

/// `rt_seg_size` (GiST penalty metric).
pub fn rt_seg_size(a: &Seg) -> f32 {
    if a.upper <= a.lower {
        0.0
    } else {
        (a.upper - a.lower).abs()
    }
}

/// `seg_size` (the SQL-visible one — no degenerate clamp).
pub fn seg_size(a: &Seg) -> f32 {
    (a.upper - a.lower).abs()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg(lower: f32, upper: f32) -> Seg {
        Seg {
            lower,
            upper,
            l_sigd: 1,
            u_sigd: 1,
            l_ext: 0,
            u_ext: 0,
        }
    }

    #[test]
    fn relations() {
        let a = seg(1.0, 5.0);
        let b = seg(2.0, 3.0);
        assert!(seg_contains(&a, &b));
        assert!(!seg_contains(&b, &a));
        assert!(seg_overlap(&a, &b));
        assert!(seg_left(&seg(0.0, 1.0), &seg(2.0, 3.0)));
        assert!(seg_right(&seg(2.0, 3.0), &seg(0.0, 1.0)));
        assert!(seg_over_left(&b, &a));
        assert!(seg_over_right(&b, &a));
    }

    #[test]
    fn cmp_boundary_type_order() {
        // '-' lower bound sorts below an exact one at the same position
        let open = Seg { l_ext: b'-', ..seg(1.0, 2.0) };
        let exact = seg(1.0, 2.0);
        assert_eq!(seg_cmp(&open, &exact).unwrap(), -1);
        assert_eq!(seg_cmp(&exact, &open).unwrap(), 1);
        // fewer significant digits sorts first
        let blurred = Seg { l_sigd: 1, ..seg(1.0, 2.0) };
        let sharp = Seg { l_sigd: 3, ..seg(1.0, 2.0) };
        assert_eq!(seg_cmp(&blurred, &sharp).unwrap(), -1);
        // upper boundary: '-' sorts above
        let uopen = Seg { u_ext: b'-', ..seg(1.0, 2.0) };
        assert_eq!(seg_cmp(&uopen, &exact).unwrap(), 1);
        assert_eq!(seg_cmp(&exact, &exact).unwrap(), 0);
    }

    #[test]
    fn union_inter_size() {
        let a = seg(1.0, 3.0);
        let b = seg(2.0, 5.0);
        let u = seg_union(&a, &b);
        assert_eq!((u.lower, u.upper), (1.0, 5.0));
        let i = seg_inter(&a, &b);
        assert_eq!((i.lower, i.upper), (2.0, 3.0));
        assert_eq!(rt_seg_size(&a), 2.0);
        assert_eq!(rt_seg_size(&seg(3.0, 1.0)), 0.0);
        assert_eq!(seg_size(&seg(3.0, 1.0)), 2.0);
    }
}
