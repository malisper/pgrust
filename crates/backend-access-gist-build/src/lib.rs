//! Port of the GiST index build engine: `src/backend/access/gist/gistbuild.c`
//! and `src/backend/access/gist/gistbuildbuffers.c` (PostgreSQL 18.3).
//!
//! [`gistbuild`] drives the table scan that builds a GiST index, choosing
//! between the sorted bottom-up build and the (optionally buffered) insert
//! build; [`gistbuildempty`] writes an empty init-fork index. Both are plain
//! `pub fn`s matching the build-scan contract; the `index.c` owner that
//! dispatches them via the `IndexAmRoutine` `ambuild`/`ambuildempty` slots is
//! not ported yet, so — like the sibling AM builds (`hashbuild`, `spgbuild`,
//! `brinbuild`) — they are exported but not yet wired into a vtable.
//!
//! The buffering build node-buffer engine lives in [`gistbuildbuffers`]; the
//! drivers and callbacks in [`gistbuild`].
//!
//! The GiST AM `ambuild`/`ambuildempty` vtable slots live in the lower
//! `backend-access-gist-core` crate (the `IndexAmRoutine` handler), which sits
//! BELOW this crate in the dep graph (gist-build depends on gist-core), so its
//! adapters cannot call [`gistbuild`]/[`gistbuildempty`] directly. The
//! cross-crate edge is bridged through the
//! `backend-access-gist-am-seams::{gistbuild,gistbuildempty}` build-dispatch
//! seams, which this crate installs from [`init_seams`] (mirroring the nbtree
//! `btbuild` seam, owned/installed by `backend-access-nbtree-nbtsort`).

#![allow(non_snake_case)]

extern crate alloc;

use backend_access_gist_am_seams as am_seams;

pub mod gistbuild;
pub mod gistbuildbuffers;

pub use gistbuild::{gistbuild, gistbuildempty};

// ===========================================================================
// init_seams — gist-build owns the `gistbuild` / `gistbuildempty`
// build-dispatch seams (declared in `backend-access-gist-am-seams`).
//
// `gistbuild`/`gistbuildempty` (the GiST AM `ambuild`/`ambuildempty` entries)
// live here, ABOVE the AM-vtable crate (`backend-access-gist-core`) in the dep
// graph, so the vtable's adapters (`gistbuild_am`/`gistbuildempty_am`) cannot
// call them directly. The cross-crate edge is bridged through the
// `gistbuild`/`gistbuildempty` seams, which this crate installs here: the
// adapter passes the `IndexInfoCarrier` (#342) through, and this installer
// downcasts it back to the real `types_nodes::execnodes::IndexInfo<'mcx>`
// before invoking the build.
// ===========================================================================

/// Install this crate's inward (build-dispatch) seams.
pub fn init_seams() {
    am_seams::gistbuild::set(|mcx, heap, index, index_info| {
        // The dispatch layer (index.c) wraps the caller's owned
        // `&mut IndexInfo<'mcx>` in the carrier; recover the concrete struct
        // (tag-checked downcast — a NULL/wrong-type carrier is the C
        // NULL-pointer programming error).
        let info = index_info
            .downcast_mut::<types_nodes::execnodes::IndexInfo<'_>>()
            .unwrap_or_else(|| {
                panic!("gistbuild: IndexInfoCarrier did not carry the expected IndexInfo")
            });
        gistbuild::gistbuild(mcx, heap, index, info)
    });
    am_seams::gistbuildempty::set(|mcx, index| gistbuild::gistbuildempty(mcx, index));
}

#[cfg(test)]
mod tests;
