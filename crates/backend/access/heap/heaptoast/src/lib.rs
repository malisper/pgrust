//! heaptoast.c + toast_helper.c + the toast_internals.c write half, plus
//! detoast.c's chunk-fetch statics (installed on toast_internals_seams).
//! Whole path is per-oversized-value cold. Loud arms: toast-index insertion
//! (nbtree insert lane is phase 2), speculative-abort delete, the rd_toastoid
//! table-rewrite lane (rewriteheap unported; RelationData carries no
//! rd_toastoid, so C's OidIsValid(rd_toastoid) is const-false here).
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

mod fetch;
mod helper;
mod internals;
mod toast;

#[cfg(test)]
mod tests;

pub use fetch::{heap_fetch_toast_slice, toast_fetch_datum, toast_fetch_datum_slice};
pub use helper::*;
pub use internals::*;
pub use toast::*;

use ::types_core::BLCKSZ;
use ::types_tuple::{SizeofHeapTupleHeader, MAXALIGN};

const SizeOfPageHeaderData: usize = 24;
const SizeOfItemIdData: usize = 4;

const fn maxalign_down(len: usize) -> usize {
    len & !7
}

pub const fn MaximumBytesPerTuple(tuples_per_page: usize) -> usize {
    maxalign_down(
        (BLCKSZ - MAXALIGN(SizeOfPageHeaderData + tuples_per_page * SizeOfItemIdData))
            / tuples_per_page,
    )
}

pub const TOAST_TUPLES_PER_PAGE: usize = 4;
pub const TOAST_TUPLE_THRESHOLD: usize = MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE);
pub const TOAST_TUPLE_TARGET: usize = TOAST_TUPLE_THRESHOLD;
pub const TOAST_TUPLES_PER_PAGE_MAIN: usize = 1;
pub const TOAST_TUPLE_TARGET_MAIN: usize = MaximumBytesPerTuple(TOAST_TUPLES_PER_PAGE_MAIN);
pub const EXTERN_TUPLES_PER_PAGE: usize = 4;
pub const EXTERN_TUPLE_MAX_SIZE: usize = MaximumBytesPerTuple(EXTERN_TUPLES_PER_PAGE);
pub const TOAST_MAX_CHUNK_SIZE: usize =
    EXTERN_TUPLE_MAX_SIZE - MAXALIGN(SizeofHeapTupleHeader) - 4 - 4 - 4;

// Values C computes for BLCKSZ 8192 (heaptoast.h); initdb-frozen.
const _: () = assert!(TOAST_TUPLE_THRESHOLD == 2032);
const _: () = assert!(TOAST_TUPLE_TARGET_MAIN == 8160);
const _: () = assert!(TOAST_MAX_CHUNK_SIZE == 1996);

#[cold]
#[inline(never)]
pub(crate) fn unported(what: &str) -> ! {
    panic!("unported: {what}")
}

pub(crate) fn check_for_interrupts() {
    if init_small::globals::InterruptPending() {
        unported("ProcessInterrupts (tcop/postgres.c)");
    }
}

pub fn init_seams() {
    heaptoast_seams::heap_toast_insert_or_update::set(toast::heap_toast_insert_or_update);
    heaptoast_seams::heap_toast_delete::set(toast::heap_toast_delete);
    heaptoast_seams::toast_compress_datum::set(internals::toast_compress_datum);
    heaptoast_seams::toast_flatten_tuple::set(toast::toast_flatten_tuple);
    toast_internals_seams::toast_fetch_datum::set(fetch::toast_fetch_datum);
    toast_internals_seams::toast_fetch_datum_slice::set(fetch::toast_fetch_datum_slice);
}
