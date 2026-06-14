//! `backend-lib-algorithms` — the reusable utility data-structure modules of
//! this catalog unit: the binary heap (`src/common/binaryheap.c`) and the
//! intrusive linked lists (`src/backend/lib/ilist.c`).
//!
//! Each module already lives in its own dedicated leaf crate
//! (`backend-lib-binaryheap`, `backend-lib-ilist`) holding the single canonical
//! port of its C source. To avoid the divergence hazard of two near-identical
//! copies, this aggregator re-exports those crates rather than re-implementing
//! them; the public surface (`backend_lib_algorithms::binaryheap::BinaryHeap`,
//! the crate-root glob, …) matches one-to-one.

#![no_std]

pub mod binaryheap {
    //! See [`backend_lib_binaryheap`].
    pub use backend_lib_binaryheap::*;
}

pub mod ilist {
    //! See [`backend_lib_ilist`].
    pub use backend_lib_ilist::*;
}

pub use binaryheap::*;
pub use ilist::*;
