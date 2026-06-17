//! Seam declarations for the `backend-access-transam-parallel` unit
//! (`access/transam/parallel.c`).
//!
//! These outward seams were removed: the owning crate
//! `backend_access_transam_parallel` is a clean single-owner leaf (no consumer
//! is in its dependency closure), so its routines are now called directly by
//! their consumers instead of through a fn-ptr seam indirection. The behavior
//! is identical; this is faithful de-indirection. The crate is retained as an
//! empty shell so existing workspace/dependency wiring stays valid.
