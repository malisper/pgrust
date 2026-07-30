// Native counterexample replays (ground-truth law): tool-artifact triage.
use types_core::geo::Point;

#[test]
fn replay_point_distance_hslice_cex() {
    let x1 = f64::from_bits(288230376151711744);
    let x2 = f64::from_bits(9660221200708665344);
    let y = f64::from_bits(18442240474082181120);
    let r = adt_geo::point::point_distance(&Point { x: x1, y }, &Point { x: x2, y }).unwrap();
    println!("Rust: bits={:016x}", r.to_bits());
    assert_eq!(r.to_bits(), 0x7ff8000000000000, "must match native C canonical NaN");
}
