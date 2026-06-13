//! `globals.c` (unit: backend-utils-init globals) — per-backend process
//! globals. Boot values are the C initializers.

use core::cell::Cell;
use std::cell::RefCell;

thread_local! {
    /// `CritSectionCount` (miscadmin.h; storage in globals.c).
    static CRIT_SECTION_COUNT: Cell<u32> = const { Cell::new(0) };
    /// `ExitOnAnyError` (globals.c); initdb sets it.
    static EXIT_ON_ANY_ERROR: Cell<bool> = const { Cell::new(false) };
    /// `IsUnderPostmaster` (globals.c).
    static IS_UNDER_POSTMASTER: Cell<bool> = const { Cell::new(false) };
    /// `FrontendProtocol` (globals.c); 0 = not yet negotiated.
    static FRONTEND_PROTOCOL: Cell<u32> = const { Cell::new(0) };
    /// `OutputFileName` (globals.c); `None` mirrors the empty boot string.
    static OUTPUT_FILE_NAME: RefCell<Option<String>> = const { RefCell::new(None) };
}

pub fn crit_section_count() -> u32 {
    CRIT_SECTION_COUNT.with(Cell::get)
}
pub fn set_crit_section_count(count: u32) {
    CRIT_SECTION_COUNT.with(|c| c.set(count));
}
pub fn exit_on_any_error() -> bool {
    EXIT_ON_ANY_ERROR.with(Cell::get)
}
pub fn set_exit_on_any_error(value: bool) {
    EXIT_ON_ANY_ERROR.with(|c| c.set(value));
}
pub fn is_under_postmaster() -> bool {
    IS_UNDER_POSTMASTER.with(Cell::get)
}
pub fn set_is_under_postmaster(value: bool) {
    IS_UNDER_POSTMASTER.with(|c| c.set(value));
}
pub fn frontend_protocol() -> u32 {
    FRONTEND_PROTOCOL.with(Cell::get)
}
pub fn set_frontend_protocol(version: u32) {
    FRONTEND_PROTOCOL.with(|c| c.set(version));
}
pub fn output_file_name() -> Option<String> {
    OUTPUT_FILE_NAME.with(|c| c.borrow().clone())
}
pub fn set_output_file_name(name: Option<String>) {
    OUTPUT_FILE_NAME.with(|c| *c.borrow_mut() = name);
}
