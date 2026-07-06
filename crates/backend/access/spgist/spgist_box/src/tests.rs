use super::*;
use ::types_core::geo::Point;

fn mkbox(lx: f64, ly: f64, hx: f64, hy: f64) -> BOX {
    BOX { high: Point { x: hx, y: hy }, low: Point { x: lx, y: ly } }
}

#[test]
fn quadrant_bits() {
    let c = mkbox(0.0, 0.0, 10.0, 10.0);
    assert_eq!(getQuadrant(&c, &mkbox(0.0, 0.0, 10.0, 10.0)), 0);
    assert_eq!(getQuadrant(&c, &mkbox(1.0, 1.0, 11.0, 11.0)), 0xF);
    assert_eq!(getQuadrant(&c, &mkbox(1.0, 0.0, 10.0, 10.0)), 0x8);
    assert_eq!(getQuadrant(&c, &mkbox(0.0, 0.0, 11.0, 10.0)), 0x4);
    assert_eq!(getQuadrant(&c, &mkbox(0.0, 1.0, 10.0, 10.0)), 0x2);
    assert_eq!(getQuadrant(&c, &mkbox(0.0, 0.0, 10.0, 11.0)), 0x1);
    assert_eq!(getQuadrant(&c, &mkbox(-1.0, -1.0, 9.0, 9.0)), 0);
}

#[test]
fn next_rect_box_partitions() {
    let root = initRectBox();
    let centroid = getRangeBox(&mkbox(0.0, 0.0, 10.0, 10.0));
    let q15 = nextRectBox(&root, &centroid, 0xF);
    assert_eq!(q15.range_box_x.left.low, 0.0);
    assert_eq!(q15.range_box_x.left.high, f64::INFINITY);
    assert_eq!(q15.range_box_x.right.low, 10.0);
    assert_eq!(q15.range_box_y.left.low, 0.0);
    assert_eq!(q15.range_box_y.right.low, 10.0);
    let q0 = nextRectBox(&root, &centroid, 0);
    assert_eq!(q0.range_box_x.left.high, 0.0);
    assert_eq!(q0.range_box_x.left.low, f64::NEG_INFINITY);
    assert_eq!(q0.range_box_x.right.high, 10.0);
    assert_eq!(q0.range_box_y.left.high, 0.0);
    assert_eq!(q0.range_box_y.right.high, 10.0);
}

#[test]
fn predicates_4d() {
    let root = initRectBox();
    let centroid = getRangeBox(&mkbox(0.0, 0.0, 10.0, 10.0));
    let q0 = nextRectBox(&root, &centroid, 0);
    let q15 = nextRectBox(&root, &centroid, 0xF);

    // Existential semantics: "can ANY box in the quadrant satisfy the op" —
    // q15 (all lows past the centroid) still admits boxes wholly left of
    // x=100, and q0 (lows unbounded below) admits boxes containing `small`.
    let far_right = getRangeBox(&mkbox(100.0, 0.0, 110.0, 10.0));
    assert!(left4D(&q0, &far_right));
    assert!(!right4D(&q0, &far_right));
    assert!(overLeft4D(&q0, &far_right));
    assert!(left4D(&q15, &far_right));
    assert!(overlap4D(&q15, &far_right));

    let far_left = getRangeBox(&mkbox(-200.0, 0.0, -190.0, 10.0));
    assert!(!left4D(&q15, &far_left));
    assert!(right4D(&q15, &far_left));

    let small = getRangeBox(&mkbox(1.0, 1.0, 2.0, 2.0));
    assert!(contain4D(&q15, &small));
    assert!(overlap4D(&q0, &small));
    assert!(contain4D(&q0, &small));
    assert!(!contain4D(&q15, &getRangeBox(&mkbox(-5.0, -5.0, 2.0, 2.0))));

    let huge = getRangeBox(&mkbox(-100.0, -100.0, 100.0, 100.0));
    assert!(contained4D(&q0, &huge));
}

#[test]
fn bbox_exactness() {
    for s in [1u16, 2, 4, 5, 9, 10, 11, 12] {
        assert!(is_bounding_box_test_exact(s));
    }
    for s in [3u16, 6, 7, 8] {
        assert!(!is_bounding_box_test_exact(s));
    }
}
