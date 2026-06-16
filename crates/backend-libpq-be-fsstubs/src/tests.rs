//! Unit tests for the parts of be-fsstubs that don't require an installed
//! runtime seam: the FD-cookie-table validation guard and its SQLSTATE.

use crate::state::with_state;
use crate::*;
use types_error::ERRCODE_UNDEFINED_OBJECT;

/// Reset the process-local cookie table between tests (the `thread_local!`
/// state persists across tests on the same thread).
fn reset_state() {
    with_state(|s| {
        s.clear_cookies();
        s.set_lo_cleanup_needed(false);
    });
}

#[test]
fn close_invalid_fd_raises_undefined_object() {
    reset_state();
    // No FDs are open: any fd is invalid.
    let err = be_lo_close(0).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
}

#[test]
fn lseek_negative_fd_raises_undefined_object() {
    reset_state();
    let err = be_lo_lseek(-1, 0, SEEK_SET).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
}

#[test]
fn tell_out_of_range_fd_raises_undefined_object() {
    reset_state();
    let err = be_lo_tell(999).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
}

#[test]
fn loread_invalid_fd_raises_undefined_object() {
    reset_state();
    // len is clamped to >= 0, then lo_read validates the fd.
    let err = be_loread(5, 16).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
}

#[test]
fn loread_negative_len_clamped_then_invalid_fd() {
    reset_state();
    // Negative len -> 0; fd still invalid -> ERRCODE_UNDEFINED_OBJECT (not a
    // panic from a negative allocation).
    let err = be_loread(0, -10).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_UNDEFINED_OBJECT);
}

#[test]
fn get_fragment_negative_nbytes_raises_invalid_parameter() {
    reset_state();
    let err = be_lo_get_fragment(1, 0, -1).unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_INVALID_PARAMETER_VALUE);
}

#[test]
fn at_eoxact_without_lo_activity_is_noop() {
    reset_state();
    // lo_cleanup_needed == false -> early return Ok(()), no seam calls.
    assert!(AtEOXact_LargeObject(true).is_ok());
    assert!(AtEOXact_LargeObject(false).is_ok());
}

#[test]
fn at_eosubxact_without_fscxt_is_noop() {
    reset_state();
    // has_fscxt() == false (cookies_size == 0) -> early return Ok(()).
    assert!(AtEOSubXact_LargeObject(true, 3, 2).is_ok());
    assert!(AtEOSubXact_LargeObject(false, 3, 2).is_ok());
}

#[test]
fn newlofd_state_growth_first_then_double() {
    reset_state();
    // First newLOfd: grows to 64, returns slot 0.
    let fd0 = super::newLOfd();
    assert_eq!(fd0, 0);
    let size_after_first = with_state(|s| s.cookies_size());
    assert_eq!(size_after_first, 64);
    // lo_cleanup_needed is set by newLOfd.
    assert!(with_state(|s| s.lo_cleanup_needed()));

    // With slot 0 still free (no descriptor stored), newLOfd reuses slot 0.
    let fd_reuse = super::newLOfd();
    assert_eq!(fd_reuse, 0);
    assert_eq!(with_state(|s| s.cookies_size()), 64);

    reset_state();
}

#[test]
fn newlofd_doubles_when_full() {
    reset_state();
    // Fill the table so every slot is occupied, forcing the double path.
    super::newLOfd();
    with_state(|s| {
        // Occupy all 64 slots (LargeObjectDesc is not Clone; build fresh boxes).
        for i in 0..s.cookies_size() {
            let desc = Box::new(types_storage::large_object::LargeObjectDesc {
                id: 1,
                snapshot: None,
                subid: 0,
                offset: 0,
                flags: types_storage::large_object::IFS_RDLOCK,
            });
            s.set_cookie(i, desc);
        }
    });
    let fd = super::newLOfd();
    assert_eq!(fd, 64);
    assert_eq!(with_state(|s| s.cookies_size()), 128);
    reset_state();
}
