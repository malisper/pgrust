//! GUC-backed datetime formatting settings.
//!
//! In PostgreSQL these are the C globals `DateStyle`, `DateOrder`, and
//! `IntervalStyle` (declared in `globals.c`, set from GUC).  They are
//! per-backend state (assigned per session by SET), so they are modeled as
//! `thread_local!` here — never process-wide shared statics.

use core::cell::Cell;

use ::types_datetime::{DATEORDER_MDY, INTSTYLE_POSTGRES, USE_ISO_DATES};

thread_local! {
    /// `DateStyle`: output format for dates/timestamps (USE_ISO_DATES, ...).
    static DATE_STYLE: Cell<i32> = const { Cell::new(USE_ISO_DATES) };
    /// `DateOrder`: field order for ambiguous dates (DATEORDER_MDY, ...).
    static DATE_ORDER: Cell<i32> = const { Cell::new(DATEORDER_MDY) };
    /// `IntervalStyle`: output format for intervals (INTSTYLE_POSTGRES, ...).
    static INTERVAL_STYLE: Cell<i32> = const { Cell::new(INTSTYLE_POSTGRES) };
}

/// Read the current `DateStyle` (C global `DateStyle`).
#[inline]
pub fn date_style() -> i32 {
    DATE_STYLE.with(Cell::get)
}

/// Set `DateStyle` (one of the `USE_*_DATES` constants).
#[inline]
pub fn set_date_style(style: i32) {
    DATE_STYLE.with(|c| c.set(style));
}

/// Read the current `DateOrder` (C global `DateOrder`).
#[inline]
pub fn date_order() -> i32 {
    DATE_ORDER.with(Cell::get)
}

/// Set `DateOrder` (one of the `DATEORDER_*` constants).
#[inline]
pub fn set_date_order(order: i32) {
    DATE_ORDER.with(|c| c.set(order));
}

/// Read the current `IntervalStyle` (C global `IntervalStyle`).
#[inline]
pub fn interval_style() -> i32 {
    INTERVAL_STYLE.with(Cell::get)
}

/// Set `IntervalStyle` (one of the `INTSTYLE_*` constants).
#[inline]
pub fn set_interval_style(style: i32) {
    INTERVAL_STYLE.with(|c| c.set(style));
}

/// Test-only serialization lock for the `DateOrder`/`DateStyle` settings.
///
/// These mirror per-backend C globals; the default cargo test runner shares
/// threads across tests, so a test that mutates `DateOrder` and then asserts on
/// a derived value would race a sibling test that set a different order. Such
/// tests hold this single crate-wide lock so each observes only its own writes.
/// `into_inner()` recovers from a poisoned lock left by an unrelated failure.
#[cfg(test)]
pub(crate) static DATE_ORDER_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_postgres() {
        assert_eq!(date_style(), USE_ISO_DATES);
        assert_eq!(date_order(), DATEORDER_MDY);
        assert_eq!(interval_style(), INTSTYLE_POSTGRES);
    }

    #[test]
    fn setters_round_trip() {
        set_date_order(DATEORDER_MDY);
        assert_eq!(date_order(), DATEORDER_MDY);
    }
}
