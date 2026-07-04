use super::*;

#[test]
fn cutoff_limits() {
    let mut s = Tsm::Bernoulli.init_state();
    s.begin_sample_scan(&[Datum::from_f32(0.0)], 7).unwrap();
    let TsmState::Bernoulli(b) = &s else { unreachable!() };
    assert_eq!(b.cutoff, 0);
    let mut s = Tsm::Bernoulli.init_state();
    s.begin_sample_scan(&[Datum::from_f32(100.0)], 7).unwrap();
    let TsmState::Bernoulli(b) = &s else { unreachable!() };
    assert_eq!(b.cutoff, 1u64 << 32);
}

#[test]
fn bad_percent_is_2202h() {
    let mut s = Tsm::System.init_state();
    let err = s.begin_sample_scan(&[Datum::from_f32(-1.0)], 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TABLESAMPLE_ARGUMENT);
    let mut s = Tsm::System.init_state();
    let err = s.begin_sample_scan(&[Datum::from_f32(f32::NAN)], 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TABLESAMPLE_ARGUMENT);
}

#[test]
fn bernoulli_deterministic_and_full_at_100() {
    let mut s = Tsm::Bernoulli.init_state();
    s.begin_sample_scan(&[Datum::from_f32(100.0)], 42).unwrap();
    for off in 1..=20u16 {
        assert_eq!(s.next_sample_tuple(3, 20), off);
    }
    assert_eq!(s.next_sample_tuple(3, 20), InvalidOffsetNumber);

    let run = |seed: u32| {
        let mut s = Tsm::Bernoulli.init_state();
        s.begin_sample_scan(&[Datum::from_f32(30.0)], seed).unwrap();
        let mut picked = vec![];
        loop {
            let off = s.next_sample_tuple(5, 200);
            if off == InvalidOffsetNumber {
                break;
            }
            picked.push(off);
        }
        picked
    };
    assert_eq!(run(1234), run(1234));
    assert_ne!(run(1234), run(1235));
}

#[test]
fn system_blocks_deterministic() {
    let run = || {
        let mut s = Tsm::System.init_state();
        s.begin_sample_scan(&[Datum::from_f32(40.0)], 99).unwrap();
        let mut blocks = vec![];
        loop {
            let b = s.next_sample_block(50);
            if b == types_core::InvalidBlockNumber {
                break;
            }
            blocks.push(b);
        }
        blocks
    };
    let blocks = run();
    assert_eq!(blocks, run());
    assert!(blocks.windows(2).all(|w| w[0] < w[1]));
    assert!(!blocks.is_empty() && blocks.len() < 50);
    // All tuples of a selected block come back in order.
    let mut s = Tsm::System.init_state();
    s.begin_sample_scan(&[Datum::from_f32(40.0)], 99).unwrap();
    for off in 1..=5u16 {
        assert_eq!(s.next_sample_tuple(0, 5), off);
    }
    assert_eq!(s.next_sample_tuple(0, 5), InvalidOffsetNumber);
}
