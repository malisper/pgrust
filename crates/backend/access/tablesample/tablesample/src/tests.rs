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
        assert_eq!(s.next_sample_tuple(3, 20, 0), off);
    }
    assert_eq!(s.next_sample_tuple(3, 20, 0), InvalidOffsetNumber);

    let run = |seed: u32| {
        let mut s = Tsm::Bernoulli.init_state();
        s.begin_sample_scan(&[Datum::from_f32(30.0)], seed).unwrap();
        let mut picked = vec![];
        loop {
            let off = s.next_sample_tuple(5, 200, 0);
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
            let b = s.next_sample_block(50, 0);
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
        assert_eq!(s.next_sample_tuple(0, 5, 0), off);
    }
    assert_eq!(s.next_sample_tuple(0, 5, 0), InvalidOffsetNumber);
}

#[test]
fn registry_dispatch() {
    assert_eq!(Tsm::from_handler(F_TSM_BERNOULLI_HANDLER), Some(Tsm::Bernoulli));
    assert_eq!(Tsm::from_handler(F_TSM_SYSTEM_HANDLER), Some(Tsm::System));
    assert_eq!(Tsm::from_handler(9999), None);
    assert_eq!(Tsm::from_symbol(b"tsm_system_rows_handler"), Some(Tsm::SystemRows));
    assert_eq!(Tsm::from_symbol(b"tsm_system_time_handler"), Some(Tsm::SystemTime));
    assert_eq!(Tsm::from_symbol(b"blhandler"), None);
    assert_eq!(Tsm::from_symbol(b""), None);
}

#[test]
fn unknown_handler_is_clean_error() {
    let err = not_a_tsm_routine(4242);
    assert_eq!(
        err.message(),
        "tablesample handler function 4242 did not return a TsmRoutine struct"
    );
}

#[test]
fn method_properties_match_c_vtables() {
    use types_core::catalog::{FLOAT8OID, INT8OID};
    for tsm in [Tsm::Bernoulli, Tsm::System] {
        assert_eq!(tsm.parameter_types(), &[FLOAT4OID]);
        assert!(tsm.repeatable_across_queries());
        assert!(tsm.repeatable_across_scans());
    }
    assert!(!Tsm::Bernoulli.has_next_sample_block());
    assert!(Tsm::System.has_next_sample_block());

    assert_eq!(Tsm::SystemRows.parameter_types(), &[INT8OID]);
    assert!(!Tsm::SystemRows.repeatable_across_queries());
    assert!(Tsm::SystemRows.repeatable_across_scans());
    assert!(Tsm::SystemRows.has_next_sample_block());

    assert_eq!(Tsm::SystemTime.parameter_types(), &[FLOAT8OID]);
    assert!(!Tsm::SystemTime.repeatable_across_queries());
    assert!(!Tsm::SystemTime.repeatable_across_scans());
    assert!(Tsm::SystemTime.has_next_sample_block());
}

#[test]
fn extension_states_route_params() {
    let mut s = Tsm::SystemRows.init_state();
    let (bulkread, pagemode) = s.begin_sample_scan(&[Datum::from_i64(3)], 11).unwrap();
    assert!(bulkread && pagemode);
    let b = s.next_sample_block(4, 0);
    assert!(b < 4);
    assert_eq!(s.next_sample_tuple(b, 2, 0), FirstOffsetNumber);
    assert_eq!(s.next_sample_tuple(b, 2, 3), InvalidOffsetNumber);
    let err = Tsm::SystemRows.init_state().begin_sample_scan(&[Datum::from_i64(-1)], 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TABLESAMPLE_ARGUMENT);

    let mut s = Tsm::SystemTime.init_state();
    let (bulkread, pagemode) = s.begin_sample_scan(&[Datum::from_f64(0.0)], 11).unwrap();
    assert!(bulkread && pagemode);
    assert_eq!(s.next_sample_block(4, 0), types_core::InvalidBlockNumber);
    let err = Tsm::SystemTime.init_state().begin_sample_scan(&[Datum::from_f64(-1.0)], 0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_TABLESAMPLE_ARGUMENT);
}
