use super::*;
use std::sync::Once;

static SETUP: Once = Once::new();

fn setup() {
    SETUP.call_once(|| {
        guc_tables::init_seams();
        init_seams();
        save_ps_display_args();
    });
    // IsUnderPostmaster and ps state are thread-local; arm per test thread.
    init_small::globals::SetIsUnderPostmaster(true);
    set_update_process_title(true);
}

fn full_title() -> String {
    STATE.with(|s| {
        let s = s.borrow();
        String::from_utf8_lossy(&s.buffer[..s.cur_len]).into_owned()
    })
}

fn activity() -> String {
    get_ps_display(|b| String::from_utf8_lossy(b).into_owned())
}

#[test]
fn fixed_prefix_and_activity() {
    setup();
    init_ps_display(Some("test backend"));
    assert_eq!(full_title(), "postgres: test backend ");
    assert_eq!(activity(), "");

    set_ps_display("SELECT");
    assert_eq!(full_title(), "postgres: test backend SELECT");
    assert_eq!(activity(), "SELECT");

    set_ps_display("idle");
    assert_eq!(full_title(), "postgres: test backend idle");
    assert_eq!(activity(), "idle");
}

#[test]
fn suffix_append_overwrite_remove() {
    setup();
    init_ps_display(Some("suffix backend"));
    set_ps_display("SELECT");

    set_ps_display_suffix("waiting");
    assert_eq!(activity(), "SELECT waiting");
    set_ps_display_suffix("waiting for lock");
    assert_eq!(activity(), "SELECT waiting for lock");

    set_ps_display_remove_suffix();
    assert_eq!(activity(), "SELECT");
    set_ps_display_remove_suffix();
    assert_eq!(activity(), "SELECT");
}

#[test]
fn set_ps_display_wipes_suffix() {
    setup();
    init_ps_display(Some("wipe backend"));
    set_ps_display("SELECT");
    set_ps_display_suffix("waiting");
    set_ps_display("COMMIT");
    assert_eq!(activity(), "COMMIT");
    set_ps_display_remove_suffix();
    assert_eq!(activity(), "COMMIT");
}

#[test]
fn update_process_title_off_freezes_display() {
    setup();
    init_ps_display(Some("guc backend"));
    set_ps_display("before");
    set_update_process_title(false);
    set_ps_display("after");
    set_ps_display_suffix("waiting");
    assert_eq!(activity(), "before");
    set_update_process_title(true);
}

#[test]
fn guc_accessors_route_to_thread_state() {
    setup();
    guc_tables::vars::update_process_title.write(false);
    assert!(!update_process_title());
    guc_tables::vars::update_process_title.read();
    guc_tables::vars::update_process_title.write(true);
    assert!(update_process_title());
}

#[test]
fn activity_truncates_at_buffer_bound() {
    setup();
    init_ps_display(Some("trunc backend"));
    let long = "x".repeat(PS_BUFFER_SIZE * 2);
    set_ps_display(&long);
    let a = activity();
    assert_eq!(a.len(), PS_BUFFER_SIZE - 1 - "postgres: trunc backend ".len());
    assert!(a.bytes().all(|b| b == b'x'));

    set_ps_display("SELECT");
    let suffix = "y".repeat(PS_BUFFER_SIZE * 2);
    set_ps_display_suffix(&suffix);
    assert_eq!(full_title().len(), PS_BUFFER_SIZE - 1);
    set_ps_display_remove_suffix();
    assert_eq!(activity(), "SELECT");
}

#[test]
fn seams_route_to_crate() {
    setup();
    ps_status_seams::init_ps_display::call(Some("seam backend"));
    ps_status_seams::set_ps_display::call("startup");
    assert_eq!(activity(), "startup");
    ps_status_seams::set_ps_display_suffix::call("waiting");
    assert_eq!(activity(), "startup waiting");
    ps_status_seams::set_ps_display_remove_suffix::call();
    assert_eq!(activity(), "startup");
}
