//! audit-18.6 b095 (a186-candidate-fp-smgr-smgr-a919133d77807bfc2f0a-1):
//! smgr.c:753-761 smgrstartreadv brackets the md call with HOLD_INTERRUPTS /
//! RESUME_INTERRUPTS, and C's errfinish (elog.c:528) zeroes
//! InterruptHoldoffCount on every ERROR — so after an error escapes the
//! bracket and is caught (plpgsql EXCEPTION, PG_TRY) the backend takes
//! interrupts again. The Rust port only resumed on Ok; an Err from inside the
//! bracket left the holdoff live for the rest of the session (statement_timeout,
//! pg_cancel_backend and pg_terminate_backend all ignored).

mod common;

use types_core::primitive::{ForkNumber, INVALID_PROC_NUMBER};
use types_core::BLCKSZ;
use types_storage::smgr::RELSEG_SIZE;
use types_storage::{RelFileLocator, RelFileLocatorBackend};

#[test]
fn startreadv_error_inside_hold_bracket_leaves_interrupts_resumed() {
    let dir = common::setup("holdoff");
    let key = RelFileLocatorBackend {
        locator: RelFileLocator { spcOid: 1663, dbOid: 5, relNumber: 16384 },
        backend: INVALID_PROC_NUMBER,
    };
    let fork = ForkNumber::MAIN_FORKNUM;

    smgr::smgropen(key.locator, key.backend).unwrap();
    smgr::smgrcreate(key, fork, false).unwrap();
    let block = [0u8; BLCKSZ];
    smgr::smgrextend(key, fork, 0, &block, false).unwrap();

    assert_eq!(init_small::globals::InterruptHoldoffCount(), 0);

    // A read in segment 1 of a 1-block relation: _mdfd_getseg(EXTENSION_FAIL)
    // ereports "previous segment is only 1 blocks" (md.c:1839) from INSIDE the
    // hold bracket — the corrupted-index-TID heap fetch of the SQL repro.
    let mut page = [0u8; BLCKSZ];
    let pages = [page.as_mut_ptr()];
    let r = smgr_seams::smgr_startreadv::call(key, fork, RELSEG_SIZE, &pages);
    let err = r.expect_err("read past the last segment must ereport");
    assert!(
        err.message.contains("previous segment is only 1 blocks"),
        "unexpected error: {}",
        err.message
    );

    // C: errfinish zeroed the count before the longjmp; the catcher sees 0.
    assert_eq!(
        init_small::globals::InterruptHoldoffCount(),
        0,
        "smgrstartreadv error path must not leak HOLD_INTERRUPTS"
    );

    let _ = std::fs::remove_dir_all(dir);
}
