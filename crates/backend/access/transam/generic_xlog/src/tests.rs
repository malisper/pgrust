//! Unit tests for the `generic_xlog.c` port.
//!
//! The pure byte-level delta computation/application (`write_fragment`,
//! `compute_region_delta`, `compute_delta`, `apply_page_redo`) are tested
//! directly. The seam-crossing entry points (`GenericXLogFinish`,
//! `GenericXLogRegisterBuffer`, `generic_mask`) are tested against a
//! thread-local test runtime installed into the per-owner seams. These tests
//! must run single-threaded (`--test-threads=1`) because the seams are
//! process-global slots.

use super::*;

use std::cell::RefCell;
use std::collections::HashMap;

use ::mcx::MemoryContext;

// ---------------------------------------------------------------------------
// Page builders shared by the byte-logic tests.
// ---------------------------------------------------------------------------

fn make_page(pd_lower: u16, pd_upper: u16, lower_fill: u8, upper_fill: u8, hole_fill: u8) -> Vec<u8> {
    let mut p = vec![hole_fill; BLCKSZ];
    for b in p.iter_mut().take(pd_lower as usize) {
        *b = lower_fill;
    }
    for b in p.iter_mut().take(BLCKSZ).skip(pd_upper as usize) {
        *b = upper_fill;
    }
    p[PD_LOWER_OFFSET..PD_LOWER_OFFSET + 2].copy_from_slice(&pd_lower.to_ne_bytes());
    p[PD_UPPER_OFFSET..PD_UPPER_OFFSET + 2].copy_from_slice(&pd_upper.to_ne_bytes());
    p
}

// ===========================================================================
// Pure byte-level logic.
// ===========================================================================

#[test]
fn delta_round_trip_reproduces_target() {
    let cur = make_page(100, 7000, 0xAA, 0xBB, 0x00);
    let mut target = make_page(120, 6800, 0xCC, 0xDD, 0x00);
    target[40] = 0x11;
    target[41] = 0x22;
    target[7900] = 0x33;

    let mut delta = vec![0u8; MAX_DELTA_SIZE];
    let mut delta_len = 0usize;
    compute_delta(&mut delta, &mut delta_len, &cur, &target);

    let mut applied = cur.clone();
    apply_page_redo(&mut applied, &delta[..delta_len]);

    let target_lower = page_pd_lower(&target) as usize;
    let target_upper = page_pd_upper(&target) as usize;
    assert_eq!(&applied[..target_lower], &target[..target_lower]);
    assert_eq!(&applied[target_upper..], &target[target_upper..]);
}

#[test]
fn identical_pages_yield_empty_delta() {
    let cur = make_page(100, 7000, 0xAA, 0xBB, 0x00);
    let target = cur.clone();
    let mut delta = vec![0u8; MAX_DELTA_SIZE];
    let mut delta_len = 0usize;
    compute_delta(&mut delta, &mut delta_len, &cur, &target);
    assert_eq!(delta_len, 0);
}

#[test]
fn fragment_header_layout() {
    let mut delta = vec![0u8; MAX_DELTA_SIZE];
    let mut delta_len = 0usize;
    let data = [0x10u8, 0x20, 0x30];
    write_fragment(&mut delta, &mut delta_len, 5, 3, &data);
    assert_eq!(delta_len, OFFSET_NUMBER_SIZE * 2 + 3);
    assert_eq!(u16::from_ne_bytes([delta[0], delta[1]]), 5);
    assert_eq!(u16::from_ne_bytes([delta[2], delta[3]]), 3);
    assert_eq!(&delta[4..7], &data);
}

// ===========================================================================
// Seam-backed test runtime (process-global; single-threaded tests only).
// ===========================================================================

#[derive(Default)]
struct TestRuntime {
    is_logged: bool,
    pages: HashMap<Buffer, Vec<u8>>,
    dirty: Vec<Buffer>,
    registered: Vec<(u8, Buffer, u8)>,
    buf_data: HashMap<u8, Vec<u8>>,
    inserted_lsn: u64,
}

thread_local! {
    static RT: RefCell<TestRuntime> = RefCell::new(TestRuntime::default());
}

fn with_rt<R>(f: impl FnOnce(&mut TestRuntime) -> R) -> R {
    RT.with(|rt| f(&mut rt.borrow_mut()))
}

/// Install the seams this crate calls outward, backed by the thread-local
/// runtime. The seam slots panic on a second `set`, so guard with a flag.
fn install_seams() {
    use std::sync::Once;
    static INSTALLED: Once = Once::new();
    INSTALLED.call_once(|| {
        relcache_seams::relation_needs_wal::set(|_rel| {
            with_rt(|rt| rt.is_logged)
        });
        bufmgr_seams::with_buffer_page::set(|buf, f| {
            // Run the callback over the runtime's page, writing changes back.
            let mut page = with_rt(|rt| rt.pages.get(&buf).cloned().unwrap());
            f(&mut page)?;
            with_rt(|rt| {
                rt.pages.insert(buf, page);
            });
            Ok(())
        });
        bufmgr_seams::mark_buffer_dirty::set(|buf| {
            with_rt(|rt| rt.dirty.push(buf))
        });
        bufmgr_seams::unlock_release_buffer::set(|_buf| {});
        xloginsert_seams::xlog_begin_insert::set(|| Ok(()));
        xloginsert_seams::xlog_register_buffer::set(|bid, buf, flags| {
            with_rt(|rt| rt.registered.push((bid, buf, flags)));
            Ok(())
        });
        xloginsert_seams::xlog_register_buf_data::set(|bid, data| {
            with_rt(|rt| {
                rt.buf_data.insert(bid, data.to_vec());
            });
            Ok(())
        });
        xloginsert_seams::xlog_insert_record::set(|_rmid, _info| {
            Ok(with_rt(|rt| rt.inserted_lsn))
        });
        bufmask_seams::mask_page_lsn_and_checksum::set(|page| {
            for b in page.iter_mut().take(10) {
                *b = 0;
            }
        });
        bufmask_seams::mask_unused_space::set(|page| {
            let lower =
                u16::from_ne_bytes([page[PD_LOWER_OFFSET], page[PD_LOWER_OFFSET + 1]]) as usize;
            let upper =
                u16::from_ne_bytes([page[PD_UPPER_OFFSET], page[PD_UPPER_OFFSET + 1]]) as usize;
            for b in page.iter_mut().take(upper).skip(lower) {
                *b = 0;
            }
            Ok(())
        });
    });
}

fn reset_rt(is_logged: bool) {
    install_seams();
    with_rt(|rt| {
        *rt = TestRuntime {
            is_logged,
            inserted_lsn: 0x1234_5678_9abc_def0,
            ..TestRuntime::default()
        };
    });
}

/// A `GenericXLogState` built directly (the seam supplies `is_logged`).
fn start_state<'mcx>(mcx: Mcx<'mcx>, is_logged: bool) -> GenericXLogState<'mcx> {
    let mut pages = vec_with_capacity_in(mcx, MAX_GENERIC_XLOG_PAGES).unwrap();
    for _ in 0..MAX_GENERIC_XLOG_PAGES {
        pages.push(GenericXLogPageData::new(mcx).unwrap());
    }
    GenericXLogState { pages, is_logged }
}

// ===========================================================================
// GenericXLogFinish / GenericXLogRegisterBuffer.
// ===========================================================================

#[test]
fn logged_finish_applies_image_zeroes_hole_and_emits_delta() {
    reset_rt(true);
    let ctx = MemoryContext::new("gx-test");
    let buffer: Buffer = 1;
    with_rt(|rt| {
        rt.pages.insert(buffer, make_page(100, 7000, 0xAA, 0xBB, 0x77));
    });

    let mut state = start_state(ctx.mcx(), true);
    let block_id = GenericXLogRegisterBuffer(&mut state, buffer, 0).unwrap();
    assert_eq!(block_id, 0);

    {
        let img = state.page_image_mut(block_id);
        img[40] = 0x11;
        img[41] = 0x22;
    }

    let lsn = GenericXLogFinish(state).unwrap();
    assert_eq!(lsn, 0x1234_5678_9abc_def0);

    with_rt(|rt| {
        let page = rt.pages.get(&buffer).cloned().unwrap();
        assert_eq!(page[40], 0x11);
        assert_eq!(page[41], 0x22);
        assert!(page[100..7000].iter().all(|&b| b == 0));
        // pd_lsn stamped in-page.
        assert_eq!(u64::from_ne_bytes(page[0..8].try_into().unwrap()), lsn);
        assert!(rt.dirty.contains(&buffer));
        assert_eq!(rt.registered[0], (0u8, buffer, REGBUF_STANDARD));
        assert!(!rt.buf_data.get(&0).unwrap().is_empty());
    });
}

#[test]
fn full_image_flag_forces_image_and_skips_delta() {
    reset_rt(true);
    let ctx = MemoryContext::new("gx-test");
    let buffer: Buffer = 1;
    with_rt(|rt| {
        rt.pages.insert(buffer, make_page(100, 7000, 0xAA, 0xBB, 0x77));
    });

    let mut state = start_state(ctx.mcx(), true);
    let block_id = GenericXLogRegisterBuffer(&mut state, buffer, GENERIC_XLOG_FULL_IMAGE).unwrap();
    state.page_image_mut(block_id)[40] = 0x11;

    GenericXLogFinish(state).unwrap();
    with_rt(|rt| {
        assert_eq!(
            rt.registered[0],
            (0u8, buffer, REGBUF_FORCE_IMAGE | REGBUF_STANDARD)
        );
        assert!(rt.buf_data.get(&0).is_none());
    });
}

#[test]
fn unlogged_finish_copies_full_image_returns_invalid_lsn() {
    reset_rt(false);
    let ctx = MemoryContext::new("gx-test");
    let buffer: Buffer = 1;
    with_rt(|rt| {
        rt.pages.insert(buffer, make_page(100, 7000, 0xAA, 0xBB, 0x77));
    });

    let mut state = start_state(ctx.mcx(), false);
    assert!(!state.is_logged());
    let block_id = GenericXLogRegisterBuffer(&mut state, buffer, 0).unwrap();
    state.page_image_mut(block_id)[40] = 0x11;

    let lsn = GenericXLogFinish(state).unwrap();
    assert_eq!(lsn, types_core::xact::InvalidXLogRecPtr);
    with_rt(|rt| {
        let page = rt.pages.get(&buffer).cloned().unwrap();
        assert_eq!(page[40], 0x11);
        // Unlogged: hole is NOT zeroed (full image copy), keeps 0x77.
        assert_eq!(page[200], 0x77);
        assert!(rt.dirty.contains(&buffer));
        assert!(rt.registered.is_empty());
    });
}

#[test]
fn register_dedups_existing_buffer() {
    reset_rt(true);
    let ctx = MemoryContext::new("gx-test");
    let buffer: Buffer = 1;
    with_rt(|rt| {
        rt.pages.insert(buffer, make_page(100, 7000, 0xAA, 0xBB, 0x77));
    });
    let mut state = start_state(ctx.mcx(), true);
    let a = GenericXLogRegisterBuffer(&mut state, buffer, 0).unwrap();
    let b = GenericXLogRegisterBuffer(&mut state, buffer, 0).unwrap();
    assert_eq!(a, b);
}

#[test]
fn register_overflow_errors() {
    reset_rt(true);
    let ctx = MemoryContext::new("gx-test");
    let mut state = start_state(ctx.mcx(), true);
    for i in 0..MAX_GENERIC_XLOG_PAGES {
        let buffer = (i + 1) as Buffer;
        with_rt(|rt| {
            rt.pages.insert(buffer, make_page(100, 7000, 0xAA, 0xBB, 0x77));
        });
        GenericXLogRegisterBuffer(&mut state, buffer, 0).unwrap();
    }
    let extra = (MAX_GENERIC_XLOG_PAGES + 100) as Buffer;
    with_rt(|rt| {
        rt.pages.insert(extra, make_page(100, 7000, 0xAA, 0xBB, 0x77));
    });
    let err = GenericXLogRegisterBuffer(&mut state, extra, 0).unwrap_err();
    assert!(err.message().contains("maximum number"));
}

// ===========================================================================
// generic_mask.
// ===========================================================================

#[test]
fn generic_mask_zeroes_header_lsn_checksum_and_hole() {
    reset_rt(true);
    let mut page = make_page(100, 7000, 0xAA, 0xBB, 0x77);
    for b in page.iter_mut().take(10) {
        *b = 0xEE;
    }
    generic_mask(&mut page, 0 as BlockNumber).unwrap();
    assert!(page[0..10].iter().all(|&b| b == 0));
    assert!(page[100..7000].iter().all(|&b| b == 0));
    assert_eq!(page[20], 0xAA);
}
