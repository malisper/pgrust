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

// ---------------------------------------------------------------------------
// Exception-row witnesses (phase-1 100%-coverage campaign, lane p1-wavea).
// The census carves tablesample_diff cannot reach stay EXECUTABLE via these
// tests (exception rows in proofs/coverage/phase1-exceptions.tsv).
// ---------------------------------------------------------------------------

/// Witness for the defensive Bernoulli NextSampleBlock panic arm
/// (lib.rs TsmState::next_sample_block): C encodes the same contract as
/// NextSampleBlock == NULL in bernoulli.c's vtable — the executor never
/// calls it when has_next_sample_block() is false (tsmapi.h).
#[test]
#[should_panic(expected = "NextSampleBlock called on a TSM without one")]
fn bernoulli_next_sample_block_panics() {
    let mut s = Tsm::Bernoulli.init_state();
    s.next_sample_block(1, 0);
}

/// Witness for the Tsm::get census carve (GetTsmRoutine syscache/fmgr
/// seam, tablesample.c 26-40): builtin fast path, extension-symbol hit,
/// unknown-symbol elog, and no-prosrc elog — all four arms executable.
#[test]
fn tsm_get_carve_witness() {
    syscache_seams::lookup_pg_proc_prosrc::set(|mcx, funcid| {
        Ok(match funcid {
            111 => Some(mcx::PgString::from_str_in("tsm_system_rows_handler", mcx)?),
            222 => Some(mcx::PgString::from_str_in("tsm_system_time_handler", mcx)?),
            333 => Some(mcx::PgString::from_str_in("not_a_handler", mcx)?),
            _ => None,
        })
    });
    let cx = mcx::MemoryContext::new("tsm_get_test");
    let m = cx.mcx();
    assert_eq!(Tsm::get(m, F_TSM_BERNOULLI_HANDLER).unwrap(), Tsm::Bernoulli);
    assert_eq!(Tsm::get(m, F_TSM_SYSTEM_HANDLER).unwrap(), Tsm::System);
    assert_eq!(Tsm::get(m, 111).unwrap(), Tsm::SystemRows);
    assert_eq!(Tsm::get(m, 222).unwrap(), Tsm::SystemTime);
    let err = Tsm::get(m, 333).unwrap_err();
    assert_eq!(
        err.message(),
        "tablesample handler function 333 did not return a TsmRoutine struct"
    );
    let err = Tsm::get(m, 444).unwrap_err();
    assert_eq!(
        err.message(),
        "tablesample handler function 444 did not return a TsmRoutine struct"
    );
}

/// Witness for the sample_scan_get_sample_size census carve (planner fold;
/// bernoulli.c 85-121 / system.c 88-124 + costsize.c clamp_row_est): every
/// dispatch arm plus the extract_fraction bogus/default branches and the
/// clamp_row_est <=1 / round / huge / NaN arms stay executable.
#[test]
fn sample_scan_get_sample_size_carve_witness() {
    use types_core::catalog::FLOAT8OID;
    let cx = mcx::MemoryContext::new("tsm_size_test");
    let m = cx.mcx();
    let f4 = |v: f32| {
        NodeList::make1(
            m,
            Node::mk_const(m, FLOAT4OID, -1, 0, 4, Datum::from_f32(v), false, true).unwrap(),
        )
        .unwrap()
    };
    // Bernoulli: pages passthrough, tuples = clamp(t * fract).
    let (pages, tuples) =
        Tsm::Bernoulli.sample_scan_get_sample_size(m, &f4(50.0), 100, 1000.0, 4.0).unwrap();
    assert_eq!((pages, tuples), (100, 500.0));
    // System: pages also scaled.
    let (pages, tuples) =
        Tsm::System.sample_scan_get_sample_size(m, &f4(50.0), 100, 1000.0, 4.0).unwrap();
    assert_eq!((pages, tuples), (50, 500.0));
    // Bogus percent -> 0.1 default fraction (extract_fraction else arm).
    let (pages, tuples) =
        Tsm::Bernoulli.sample_scan_get_sample_size(m, &f4(150.0), 100, 1000.0, 4.0).unwrap();
    assert_eq!((pages, tuples), (100, 100.0));
    // NaN percent -> same default.
    let (_, tuples) = Tsm::Bernoulli
        .sample_scan_get_sample_size(m, &f4(f32::NAN), 100, 1000.0, 4.0)
        .unwrap();
    assert_eq!(tuples, 100.0);
    // Null Const -> default fraction (non-Const arm of extract_fraction).
    let nullc = NodeList::make1(
        m,
        Node::mk_const(m, FLOAT4OID, -1, 0, 4, Datum::from_f32(0.0), true, true).unwrap(),
    )
    .unwrap();
    let (_, tuples) =
        Tsm::System.sample_scan_get_sample_size(m, &nullc, 100, 1000.0, 4.0).unwrap();
    assert_eq!(tuples, 100.0);
    // clamp_row_est arms: <= 1.0 floor; > 1e100 / NaN cap.
    let (_, tuples) =
        Tsm::Bernoulli.sample_scan_get_sample_size(m, &f4(0.0), 100, 1000.0, 4.0).unwrap();
    assert_eq!(tuples, 1.0);
    let (_, tuples) =
        Tsm::Bernoulli.sample_scan_get_sample_size(m, &f4(100.0), 100, 2e100, 4.0).unwrap();
    assert_eq!(tuples, 1e100);
    let (_, tuples) = Tsm::Bernoulli
        .sample_scan_get_sample_size(m, &f4(50.0), 100, f64::NAN, 4.0)
        .unwrap();
    assert_eq!(tuples, 1e100);
    // SystemRows: INT8 Const limit (delegates to the contrib crate).
    let i8list = NodeList::make1(
        m,
        Node::mk_const(m, INT8OID, -1, 0, 8, Datum::from_i64(500), false, true).unwrap(),
    )
    .unwrap();
    let (pages, tuples) =
        Tsm::SystemRows.sample_scan_get_sample_size(m, &i8list, 64, 1000.0, 4.0).unwrap();
    assert!(pages > 0 && tuples > 0.0);
    // SystemRows null Const -> None limit arm.
    let i8null = NodeList::make1(
        m,
        Node::mk_const(m, INT8OID, -1, 0, 8, Datum::from_i64(0), true, true).unwrap(),
    )
    .unwrap();
    Tsm::SystemRows.sample_scan_get_sample_size(m, &i8null, 64, 1000.0, 4.0).unwrap();
    // SystemTime: FLOAT8 Const limit + null Const arm.
    let f8list = NodeList::make1(
        m,
        Node::mk_const(m, FLOAT8OID, -1, 0, 8, Datum::from_f64(1000.0), false, true).unwrap(),
    )
    .unwrap();
    Tsm::SystemTime.sample_scan_get_sample_size(m, &f8list, 64, 1000.0, 4.0).unwrap();
    let f8null = NodeList::make1(
        m,
        Node::mk_const(m, FLOAT8OID, -1, 0, 8, Datum::from_f64(0.0), true, true).unwrap(),
    )
    .unwrap();
    Tsm::SystemTime.sample_scan_get_sample_size(m, &f8null, 64, 1000.0, 4.0).unwrap();
}
