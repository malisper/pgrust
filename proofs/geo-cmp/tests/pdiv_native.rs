// Native differential for the point_div/point_mul pow2-plane FAILED
// (kissat 255.9s, playback walled): shipped Rust vs the vendored C
// driver's dump (scratchpad pdiv_c.txt regenerated per run).
use types_core::geo::Point;
use types_error::{ERRCODE_DIVISION_BY_ZERO, ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE};

#[test]
fn dump_rust_side() {
    let specials: [f64; 20] = [
        0.0, -0.0, 0.5, -0.5, 1.0, -1.0, 2.0, -2.0, 3.0, 1e300, -1e300, 1e-300, 5e-324,
        1.7976931348623157e308, f64::INFINITY, f64::NEG_INFINITY, f64::NAN, 1e-160, -1e-160,
        1e160,
    ];
    let cells: [[f64; 2]; 8] = [
        [1.0, 0.0], [0.0, 1.0], [-2.0, 0.0], [0.0, -0.5], [0.5, 0.0], [0.0, 0.0],
        [f64::INFINITY, 0.0], [f64::NAN, 0.0],
    ];
    for (a, &x1) in specials.iter().enumerate() {
        for (b, &y1) in specials.iter().enumerate() {
            for (c, cell) in cells.iter().enumerate() {
                let p1 = Point { x: x1, y: y1 };
                let p2 = Point { x: cell[0], y: cell[1] };
                for (tag, r) in [
                    ("D", adt_geo::point::point_div_point(&p1, &p2)),
                    ("M", adt_geo::point::point_mul_point(&p1, &p2)),
                ] {
                    match r {
                        Ok(p) => println!("{} {} {} {} 0 {:x} {:x}", tag, a, b, c, p.x.to_bits(), p.y.to_bits()),
                        Err(e) => {
                            let f = if e.sqlstate == ERRCODE_DIVISION_BY_ZERO { 3 }
                                else if e.sqlstate == ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE { 9 } // 9 = 22003: matches C flag 1 or 2
                                else { 99 };
                            println!("{} {} {} {} {} - -", tag, a, b, c, f);
                        }
                    }
                }
            }
        }
    }
}
