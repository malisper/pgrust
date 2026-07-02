use std::sync::Once;

use types_guc::GucSource;

use super::*;

fn setup() {
    static ONCE: Once = Once::new();
    ONCE.call_once(init_seams);
}

#[test]
fn defaults_match_c() {
    assert_eq!(max_stack_depth(), 100);
    assert_eq!(max_stack_depth_bytes(), 100 * 1024);
    assert_eq!(STACK_DEPTH_SLOP, 512 * 1024);
}

#[test]
fn unarmed_base_never_trips() {
    assert!(!stack_is_too_deep());
    check_stack_depth().unwrap();
}

#[test]
fn depth_check_trips_past_limit() {
    // A synthetic base max_stack_depth_bytes + slack away from the live stack
    // trips the check in both growth directions; restore disarms.
    let here = set_stack_base();
    assert_eq!(here, 0);

    let base = set_stack_base();
    restore_stack_base(base + 200 * 1024);
    assert!(stack_is_too_deep());
    let err = check_stack_depth().unwrap_err();
    assert_eq!(err.sqlstate(), ERRCODE_STATEMENT_TOO_COMPLEX);
    assert_eq!(err.message(), "stack depth limit exceeded");
    assert_eq!(
        err.hint(),
        Some(
            "Increase the configuration parameter \"max_stack_depth\" (currently 100kB), \
             after ensuring the platform's stack depth limit is adequate."
        )
    );

    restore_stack_base(base.saturating_sub(200 * 1024));
    assert!(stack_is_too_deep());

    restore_stack_base(base);
    assert!(!stack_is_too_deep());
    restore_stack_base(0);
}

#[test]
fn assign_updates_bytes_only() {
    assign_max_stack_depth(2048);
    assert_eq!(max_stack_depth_bytes(), 2048 * 1024);
    assert_eq!(max_stack_depth(), 100);
    assign_max_stack_depth(100);
}

#[test]
fn rlimit_check_hook() {
    setup();
    let rlimit = get_stack_depth_rlimit();
    assert_eq!(rlimit, get_stack_depth_rlimit());

    assert!(check_max_stack_depth(100, GucSource::PGC_S_DEFAULT));

    if rlimit > 0 && rlimit < isize::MAX {
        let over = ((rlimit - STACK_DEPTH_SLOP) / 1024 + 1) as i32;
        assert!(!check_max_stack_depth(over, GucSource::PGC_S_SESSION));
        let check = guc::take_guc_check_error();
        assert_eq!(
            check.detail.as_deref(),
            Some(format!(
                "\"max_stack_depth\" must not exceed {}kB.",
                (rlimit - STACK_DEPTH_SLOP) / 1024
            ))
            .as_deref()
        );
        assert_eq!(
            check.hint.as_deref(),
            Some("Increase the platform's stack depth limit via \"ulimit -s\" or local equivalent.")
        );
    }
}

#[test]
fn guc_slots_installed() {
    setup();
    let check = guc_tables::hooks::check_max_stack_depth.get();
    let mut newval = 100;
    let mut extra = None;
    assert!(check(&mut newval, &mut extra, GucSource::PGC_S_DEFAULT).unwrap());

    guc_tables::vars::max_stack_depth.write(150);
    assert_eq!(guc_tables::vars::max_stack_depth.read(), 150);
    assert_eq!(max_stack_depth(), 150);
    let assign = guc_tables::hooks::assign_max_stack_depth.get();
    assign(150, None);
    assert_eq!(max_stack_depth_bytes(), 150 * 1024);

    guc_tables::vars::max_stack_depth.write(100);
    assign(100, None);
}
