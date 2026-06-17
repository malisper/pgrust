//! Seam declarations for the `backend-replication-libpqwalreceiver` unit
//! (`replication/libpqwalreceiver/libpqwalreceiver.c`).
//!
//! These outward seams were removed: the owning crate
//! `backend_replication_libpqwalreceiver` is a clean single-owner leaf, so its
//! `walrcv_*` routines are now called directly by their consumers
//! (walreceiver, slotsync, slotfuncs) instead of through a fn-ptr seam
//! indirection. The behavior is identical (a direct call replaces the seam
//! call); this is faithful de-indirection. The crate is retained as an empty
//! shell so existing workspace/dependency wiring stays valid.
