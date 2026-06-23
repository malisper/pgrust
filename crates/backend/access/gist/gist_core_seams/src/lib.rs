//! Seam declarations for the `backend-access-gist-core` unit (`gistxlog.c`):
//! the rmgr-table callbacks it owns (slots of `RmgrTable`, populated from
//! `access/rmgrlist.h` by `access/transam/rmgr.c`) and the GiST WAL-write
//! routines the insert spine reaches.
//!
//! These outward seams were removed: the owning crate
//! `gist_core` is a clean single-owner leaf, so its `gist_*`
//! routines (in the `gistxlog` module) are now called directly by their
//! consumers (the rmgr table) and by the owner's own insert/vacuum spine,
//! instead of through a fn-ptr seam indirection. The behavior is identical (a
//! direct call replaces the seam call); this is faithful de-indirection. The
//! crate is retained as an empty shell so existing workspace/dependency wiring
//! stays valid.
