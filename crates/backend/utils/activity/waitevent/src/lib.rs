//! wait_event.c core: the my_wait_event_info indirection. Before
//! pgstat_set_wait_event_storage redirects to the PGPROC slot, writes go to
//! C's never-read process-local fallback — dropped here (same observable
//! behavior). Wait-event NAME resolution and custom events defer.

use core::cell::Cell;
use core::sync::atomic::{AtomicU32, Ordering::Relaxed};

#[cfg(test)]
mod tests;

thread_local! {
    static MY_WAIT_EVENT_INFO: Cell<Option<&'static AtomicU32>> = const { Cell::new(None) };
}

pub fn pgstat_set_wait_event_storage(slot: &'static AtomicU32) {
    MY_WAIT_EVENT_INFO.set(Some(slot));
}

pub fn pgstat_reset_wait_event_storage() {
    MY_WAIT_EVENT_INFO.set(None);
}

#[inline]
pub fn pgstat_report_wait_start(wait_event_info: u32) {
    if let Some(slot) = MY_WAIT_EVENT_INFO.get() {
        slot.store(wait_event_info, Relaxed);
    }
}

#[inline]
pub fn pgstat_report_wait_end() {
    pgstat_report_wait_start(0);
}

pub fn init_seams() {
    waitevent_seams::pgstat_report_wait_start::set(pgstat_report_wait_start);
    waitevent_seams::pgstat_report_wait_end::set(pgstat_report_wait_end);
    waitevent_seams::pgstat_set_wait_event_storage::set(pgstat_set_wait_event_storage);
    waitevent_seams::pgstat_reset_wait_event_storage::set(pgstat_reset_wait_event_storage);
}
