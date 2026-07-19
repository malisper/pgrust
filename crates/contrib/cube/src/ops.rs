//! contrib/cube's internal algebra (cube.c): comparison, containment,
//! overlap, union/intersection, size and the three distance metrics. Pure
//! functions over `CubeView`s; constructors return full varlena images.
//!
//! Every Min/Max in cube.c is the C macro (second operand on false
//! comparisons, NaN included) — `c_min`/`c_max` here, never `f64::min`.

use crate::repr::{build_image_auto, c_max, c_min, CubeView};

/// `cube_cmp_v0`: the "made up" total-ish ordering used by the btree
/// opclass and GiST same.
pub fn cube_cmp_v0(a: &CubeView<'_>, b: &CubeView<'_>) -> i32 {
    let dim = a.dim().min(b.dim());

    // compare the common dimensions
    for i in 0..dim {
        if c_min(a.ll(i), a.ur(i)) > c_min(b.ll(i), b.ur(i)) {
            return 1;
        }
        if c_min(a.ll(i), a.ur(i)) < c_min(b.ll(i), b.ur(i)) {
            return -1;
        }
    }
    for i in 0..dim {
        if c_max(a.ll(i), a.ur(i)) > c_max(b.ll(i), b.ur(i)) {
            return 1;
        }
        if c_max(a.ll(i), a.ur(i)) < c_max(b.ll(i), b.ur(i)) {
            return -1;
        }
    }

    // compare extra dimensions to zero
    if a.dim() > b.dim() {
        for i in dim..a.dim() {
            if c_min(a.ll(i), a.ur(i)) > 0.0 {
                return 1;
            }
            if c_min(a.ll(i), a.ur(i)) < 0.0 {
                return -1;
            }
        }
        for i in dim..a.dim() {
            if c_max(a.ll(i), a.ur(i)) > 0.0 {
                return 1;
            }
            if c_max(a.ll(i), a.ur(i)) < 0.0 {
                return -1;
            }
        }
        // all common dimensions equal: more dimensions wins
        return 1;
    }
    if a.dim() < b.dim() {
        for i in dim..b.dim() {
            if c_min(b.ll(i), b.ur(i)) > 0.0 {
                return -1;
            }
            if c_min(b.ll(i), b.ur(i)) < 0.0 {
                return 1;
            }
        }
        for i in dim..b.dim() {
            if c_max(b.ll(i), b.ur(i)) > 0.0 {
                return -1;
            }
            if c_max(b.ll(i), b.ur(i)) < 0.0 {
                return 1;
            }
        }
        return -1;
    }

    0
}

/// `cube_contains_v0`: Box(A) contains Box(B).
pub fn cube_contains_v0(a: &CubeView<'_>, b: &CubeView<'_>) -> bool {
    if a.dim() < b.dim() {
        // the excess dimensions of (b) must all be zero
        for i in a.dim()..b.dim() {
            if b.ll(i) != 0.0 {
                return false;
            }
            if b.ur(i) != 0.0 {
                return false;
            }
        }
    }

    for i in 0..a.dim().min(b.dim()) {
        if c_min(a.ll(i), a.ur(i)) > c_min(b.ll(i), b.ur(i)) {
            return false;
        }
        if c_max(a.ll(i), a.ur(i)) < c_max(b.ll(i), b.ur(i)) {
            return false;
        }
    }

    true
}

/// `cube_overlap_v0`.
pub fn cube_overlap_v0(a: &CubeView<'_>, b: &CubeView<'_>) -> bool {
    // swap so that 'a' has the higher dimensionality
    let (a, b) = if a.dim() < b.dim() { (b, a) } else { (a, b) };

    for i in 0..b.dim() {
        if c_min(a.ll(i), a.ur(i)) > c_max(b.ll(i), b.ur(i)) {
            return false;
        }
        if c_max(a.ll(i), a.ur(i)) < c_min(b.ll(i), b.ur(i)) {
            return false;
        }
    }

    for i in b.dim()..a.dim() {
        if c_min(a.ll(i), a.ur(i)) > 0.0 {
            return false;
        }
        if c_max(a.ll(i), a.ur(i)) < 0.0 {
            return false;
        }
    }

    true
}

/// `cube_union_v0` — the non-trivial path (the C `a == b` pointer shortcut
/// is handled by callers, who return the input unchanged). Result collapses
/// to a point image when applicable, exactly like the C SET_VARSIZE shrink.
pub fn cube_union_v0(a: &CubeView<'_>, b: &CubeView<'_>) -> Vec<u8> {
    // swap so that 'a' is always the larger-dimensional one
    let (a, b) = if a.dim() < b.dim() { (b, a) } else { (a, b) };
    let dim = a.dim();

    let mut coords = vec![0f64; 2 * dim];
    for i in 0..b.dim() {
        coords[i] = c_min(c_min(a.ll(i), a.ur(i)), c_min(b.ll(i), b.ur(i)));
        coords[i + dim] = c_max(c_max(a.ll(i), a.ur(i)), c_max(b.ll(i), b.ur(i)));
    }
    for i in b.dim()..dim {
        coords[i] = c_min(0.0, c_min(a.ll(i), a.ur(i)));
        coords[i + dim] = c_max(0.0, c_max(a.ll(i), a.ur(i)));
    }

    build_image_auto(dim, &coords)
}

/// `cube_inter`'s core (argument swap and point collapse included).
pub fn cube_inter_v0(a: &CubeView<'_>, b: &CubeView<'_>) -> Vec<u8> {
    let (a, b) = if a.dim() < b.dim() { (b, a) } else { (a, b) };
    let dim = a.dim();

    let mut coords = vec![0f64; 2 * dim];
    for i in 0..b.dim() {
        coords[i] = c_max(c_min(a.ll(i), a.ur(i)), c_min(b.ll(i), b.ur(i)));
        coords[i + dim] = c_min(c_max(a.ll(i), a.ur(i)), c_max(b.ll(i), b.ur(i)));
    }
    for i in b.dim()..dim {
        coords[i] = c_max(0.0, c_min(a.ll(i), a.ur(i)));
        coords[i + dim] = c_min(0.0, c_max(a.ll(i), a.ur(i)));
    }

    build_image_auto(dim, &coords)
}

/// `rt_cube_size` for a present cube (the GiST NULL special case is the
/// caller's business and never arises here — union results are non-null).
pub fn rt_cube_size(a: &CubeView<'_>) -> f64 {
    if a.is_point() || a.dim() == 0 {
        0.0
    } else {
        let mut result = 1.0;
        for i in 0..a.dim() {
            result *= (a.ur(i) - a.ll(i)).abs();
        }
        result
    }
}

/// `distance_1D`.
fn distance_1d(a1: f64, a2: f64, b1: f64, b2: f64) -> f64 {
    // interval (a) is entirely on the left of (b)
    if a1 <= b1 && a2 <= b1 && a1 <= b2 && a2 <= b2 {
        return c_min(b1, b2) - c_max(a1, a2);
    }
    // interval (a) is entirely on the right of (b)
    if a1 > b1 && a2 > b1 && a1 > b2 && a2 > b2 {
        return c_min(a1, a2) - c_max(b1, b2);
    }
    // all sorts of intersections
    0.0
}

/// `cube_distance` (Euclidean).
pub fn cube_distance(a: &CubeView<'_>, b: &CubeView<'_>) -> f64 {
    let (a, b) = if a.dim() < b.dim() { (b, a) } else { (a, b) };
    let mut distance = 0.0;
    for i in 0..b.dim() {
        let d = distance_1d(a.ll(i), a.ur(i), b.ll(i), b.ur(i));
        distance += d * d;
    }
    for i in b.dim()..a.dim() {
        let d = distance_1d(a.ll(i), a.ur(i), 0.0, 0.0);
        distance += d * d;
    }
    distance.sqrt()
}

/// `distance_taxicab`.
pub fn distance_taxicab(a: &CubeView<'_>, b: &CubeView<'_>) -> f64 {
    let (a, b) = if a.dim() < b.dim() { (b, a) } else { (a, b) };
    let mut distance = 0.0;
    for i in 0..b.dim() {
        distance += distance_1d(a.ll(i), a.ur(i), b.ll(i), b.ur(i)).abs();
    }
    for i in b.dim()..a.dim() {
        distance += distance_1d(a.ll(i), a.ur(i), 0.0, 0.0).abs();
    }
    distance
}

/// `distance_chebyshev`.
pub fn distance_chebyshev(a: &CubeView<'_>, b: &CubeView<'_>) -> f64 {
    let (a, b) = if a.dim() < b.dim() { (b, a) } else { (a, b) };
    let mut distance = 0.0;
    for i in 0..b.dim() {
        let d = distance_1d(a.ll(i), a.ur(i), b.ll(i), b.ur(i)).abs();
        if d > distance {
            distance = d;
        }
    }
    for i in b.dim()..a.dim() {
        let d = distance_1d(a.ll(i), a.ur(i), 0.0, 0.0).abs();
        if d > distance {
            distance = d;
        }
    }
    distance
}

/// The `~>` / KNN-coordinate getter shared by `cube_coord_llur` and
/// `g_cube_distance` strategy 15. `coord` is 1-based and non-zero; caller
/// validated. For non-leaf internal pages `g_cube_distance` wants the lower
/// bound (or upper for inversed) — `internal_page` + `inverse` reproduce
/// that.
pub fn coord_llur(cube: &CubeView<'_>, coord: i32, internal_page: bool) -> f64 {
    let inverse = coord < 0;
    let coord = coord.unsigned_abs() as usize;
    let mut result;
    if coord <= 2 * cube.dim() {
        let index = (coord - 1) / 2;
        let upper = (coord - 1) % 2 == 1;
        if cube.is_point() {
            result = cube.x(index);
        } else if internal_page {
            // non-leaf: always the bound that can only shrink under us
            if !inverse {
                result = c_min(cube.x(index), cube.x(index + cube.dim()));
            } else {
                result = c_max(cube.x(index), cube.x(index + cube.dim()));
            }
        } else if upper {
            result = c_max(cube.x(index), cube.x(index + cube.dim()));
        } else {
            result = c_min(cube.x(index), cube.x(index + cube.dim()));
        }
    } else {
        result = 0.0;
    }
    if inverse {
        result = -result;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::repr::build_image;

    fn cube(coords: &[f64]) -> Vec<u8> {
        let dim = coords.len() / 2;
        build_image_auto(dim, coords)
    }
    fn point(coords: &[f64]) -> Vec<u8> {
        build_image(coords.len(), true, coords)
    }
    fn v(img: &[u8]) -> CubeView<'_> {
        CubeView::from_payload(&img[4..])
    }

    #[test]
    fn cmp_matches_c_semantics() {
        let a = cube(&[0.0, 0.0, 1.0, 1.0]);
        let b = cube(&[0.0, 0.0, 2.0, 2.0]);
        assert_eq!(cube_cmp_v0(&v(&a), &v(&b)), -1);
        assert_eq!(cube_cmp_v0(&v(&b), &v(&a)), 1);
        assert_eq!(cube_cmp_v0(&v(&a), &v(&a)), 0);
        // more dimensions wins when common ones tie
        let c = point(&[0.0, 0.0]);
        let d = point(&[0.0]);
        assert_eq!(cube_cmp_v0(&v(&c), &v(&d)), 1);
        // ... unless the extra dimension is negative
        let e = point(&[0.0, -1.0]);
        assert_eq!(cube_cmp_v0(&v(&e), &v(&d)), -1);
    }

    #[test]
    fn contains_overlap_mixed_dims() {
        let big = cube(&[0.0, 0.0, 10.0, 10.0]);
        let small = cube(&[1.0, 1.0, 2.0, 2.0]);
        assert!(cube_contains_v0(&v(&big), &v(&small)));
        assert!(!cube_contains_v0(&v(&small), &v(&big)));
        assert!(cube_overlap_v0(&v(&big), &v(&small)));
        // lower-dim a contains higher-dim b only when b's extras are zero
        let a1 = cube(&[0.0, 10.0]);
        let b2z = point(&[5.0, 0.0]);
        let b2nz = point(&[5.0, 1.0]);
        assert!(cube_contains_v0(&v(&a1), &v(&b2z)));
        assert!(!cube_contains_v0(&v(&a1), &v(&b2nz)));
        // unnormalized coordinates (ll > ur) still work via Min/Max
        let w = cube(&[2.0, 2.0, 0.0, 0.0]);
        assert!(cube_contains_v0(&v(&w), &v(&small)));
    }

    #[test]
    fn union_inter_normalize_and_collapse() {
        let a = cube(&[2.0, 0.0]); // 1-D box (2)..(0), unnormalized
        let b = cube(&[1.0, 3.0]);
        let u = cube_union_v0(&v(&a), &v(&b));
        assert_eq!((v(&u).ll(0), v(&u).ur(0)), (0.0, 3.0));
        let i = cube_inter_v0(&v(&a), &v(&b));
        assert_eq!((v(&i).ll(0), v(&i).ur(0)), (1.0, 2.0));
        // union of a point with itself collapses back to the point image
        let p = point(&[4.0]);
        let up = cube_union_v0(&v(&p), &v(&p));
        assert_eq!(up, p);
        // mixed dimensionality unions against implicit zero
        let p2 = point(&[1.0, 1.0]);
        let p1 = point(&[2.0]);
        let u2 = cube_union_v0(&v(&p2), &v(&p1));
        assert_eq!(v(&u2).dim(), 2);
        assert_eq!((v(&u2).ll(0), v(&u2).ur(0)), (1.0, 2.0));
        assert_eq!((v(&u2).ll(1), v(&u2).ur(1)), (0.0, 1.0));
    }

    #[test]
    fn distances_match_c_formulas() {
        let a = point(&[0.0, 0.0]);
        let b = point(&[3.0, 4.0]);
        assert_eq!(cube_distance(&v(&a), &v(&b)), 5.0);
        assert_eq!(distance_taxicab(&v(&a), &v(&b)), 7.0);
        assert_eq!(distance_chebyshev(&v(&a), &v(&b)), 4.0);
        // overlapping projections contribute zero
        let c = cube(&[0.0, 0.0, 10.0, 10.0]);
        let d = cube(&[5.0, 5.0, 6.0, 6.0]);
        assert_eq!(cube_distance(&v(&c), &v(&d)), 0.0);
        // dimension mismatch measures to zero on the extra axes
        let e = point(&[0.0]);
        let f = point(&[0.0, 2.0]);
        assert_eq!(cube_distance(&v(&e), &v(&f)), 2.0);
    }

    #[test]
    fn size_and_coord_llur() {
        let a = cube(&[0.0, 0.0, 2.0, 3.0]);
        assert_eq!(rt_cube_size(&v(&a)), 6.0);
        assert_eq!(rt_cube_size(&v(&point(&[1.0]))), 0.0);
        // llur getter on an unnormalized cube ((2,1),(1,2))
        let w = cube(&[2.0, 1.0, 1.0, 2.0]);
        assert_eq!(coord_llur(&v(&w), 1, false), 1.0); // lower of dim 1
        assert_eq!(coord_llur(&v(&w), 2, false), 2.0); // upper of dim 1
        assert_eq!(coord_llur(&v(&w), 3, false), 1.0);
        assert_eq!(coord_llur(&v(&w), 4, false), 2.0);
        assert_eq!(coord_llur(&v(&w), -1, false), -1.0);
        assert_eq!(coord_llur(&v(&w), 9, false), 0.0); // out of range -> 0
    }
}
