//! Inward seam declarations for the `backend-access-hash-core` unit
//! (the combined `hashinsert.c` / `hashovfl.c` / `hashpage.c` /
//! `hashsearch.c` / `hashutil.c` module of the hash access method).
//!
//! The five hash-core modules live in a single crate and call each other
//! directly (no seams needed within the crate). The cross-crate consumers
//! (`hash.c` in `backend-access-hash-entry`, and `hashbucketcleanup` /
//! `hashbulkdelete` which reach back into hashovfl/hashpage) take a direct
//! crate dependency on `backend-access-hash-core` since the dependency graph
//! is acyclic. Consequently this crate currently declares no inward seams;
//! it exists to hold any future cross-cycle decls and is wired through
//! `seams-init` (`init_seams()` is a no-op, mirroring `functioncmds`).

#![allow(non_snake_case)]
