//! Outward seam declarations for `utils/adt/mac.c`.
//!
//! `mac.c` owns `macaddr_sortsupport`, but the *external* substrate it reaches
//! into lives in not-yet-ported neighbour subsystems: installing the comparator
//! / abbreviation callbacks into the live `SortSupportData` node, initializing
//! the HyperLogLog cardinality estimator (`lib/hyperloglog`) used by the
//! abbreviation abort cost model, and reading the `trace_sort` GUC for the
//! `macaddr_abbrev` LOG lines (tuplesort / guc). `backend-utils-adt-mac` CALLS
//! this slot (it never installs it), so it stays uninstalled — a loud panic —
//! until the real owner subsystem lands. That is mirror-PG-and-panic.
//!
//! The pure parts of `macaddr_sortsupport` — the comparator
//! ([`backend_utils_adt_mac::macaddr_fast_cmp`]) and the abbreviated-key packing
//! ([`backend_utils_adt_mac::macaddr_abbrev_convert_bits`]) — are in-crate and
//! pure; only the node mutation + estimator wiring crosses here.

#![allow(non_snake_case)]

pub mod sortsupport {
    seam_core::seam!(
        /// Install the SortSupport comparator + abbreviation callbacks into the
        /// live `SortSupportData` node (and, when `ssup.abbreviate`, allocate the
        /// `macaddr_sortsupport_state` in `ssup_cxt` and initialize the
        /// HyperLogLog estimator with `initHyperLogLog(&uss->abbr_card, 10)`).
        /// Returns whether a registrar was wired; the default (uninstalled) is a
        /// faithful no-op, exactly as if sortsupport were never registered (the
        /// btree AM falls back to the ordinary `macaddr_cmp` ordering proc).
        pub fn register() -> bool
    );
}
