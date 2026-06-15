#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `PgError` is a large owned `Err`; the un-boxed return is the project error
// contract, so accept `clippy::result_large_err` crate-wide.
#![allow(clippy::result_large_err)]

//! Buffer-pool support — the buffer manager's helper modules:
//!
//!  * `buf_table.c` — the shared buffer lookup hash ([`buf_table`]),
//!  * `freelist.c` — the clock-sweep victim selection and free list
//!    ([`freelist`]) plus the backend-private `BufferAccessStrategy` ring
//!    ([`strategy`]),
//!  * `localbuf.c` — the temp-relation local buffer manager ([`localbuf`]).
//!
//! These provide the substrate the main buffer manager
//! (`backend-storage-buffer-bufmgr`) sits on.
//!
//! `buf_table.c` and `freelist.c`'s control block are SHARED-MEMORY
//! subsystems. The shared structs are modeled field-for-field as owned Rust
//! values with the real spinlock + atomic semantics; the SUBSTRATE they rest on
//! — the shared-memory allocator (`ShmemInitStruct`), the bufmgr-owned
//! per-buffer header array (`LockBufHdr`/`UnlockBufHdr`/`freeNext`), and the
//! bgwriter wakeup latch — is reached through the owners' seam crates, because
//! that infrastructure is not yet ported. The `buffer_strategy_lock` is a real
//! [`backend_storage_lmgr_s_lock::Spinlock`] (a direct dep). The in-crate
//! ALGORITHMS (clock sweep, open-addressing lookup/insert/delete, free-list
//! pop, ring policy, local clock sweep) stay in-crate.
//!
//! `localbuf.c` is BACKEND-LOCAL (temp-table buffers are never shared); its
//! pool is an owned [`localbuf::LocalBufferManager`] value, and its externals
//! are the temp-relation `smgr` I/O entry points (seamed).

extern crate alloc;

use std::cell::RefCell;

use types_error::PgResult;
use types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};

mod buf_table;
mod freelist;
mod localbuf;
mod strategy;

pub use buf_table::{buf_table_hash_code, buf_table_hash_partition, BufTable, BufTableShmemSize};
pub use freelist::{BufferStrategyControl, ClockSweep, StrategyShmemSize};
pub use localbuf::{check_temp_buffers, LocalBufferManager};
pub use strategy::FreeAccessStrategy;

// Re-export the shared signature types from types-storage so callers reach them
// through this crate's surface.
pub use types_storage::buf::{
    IOContext, LocalBufferLookupEnt, Victim, FREENEXT_END_OF_LIST, FREENEXT_NOT_IN_LIST,
};
pub use types_storage::PrefetchBufferResult;

/// `BUF_STATE_GET_REFCOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_refcount(buf_state: u32) -> u32 {
    buf_state & types_storage::buf::BUF_REFCOUNT_MASK
}

/// `BUF_STATE_GET_USAGECOUNT(buf_state)` (buf_internals.h).
#[inline]
fn buf_state_get_usagecount(buf_state: u32) -> u32 {
    (buf_state & types_storage::buf::BUF_USAGECOUNT_MASK) / types_storage::buf::BUF_USAGECOUNT_ONE
}

// ---------------------------------------------------------------------------
// Backend-private `BufferAccessStrategy` registry.
//
// The inward `get_access_strategy`/`free_access_strategy` bufmgr seams carry the
// opaque `types_storage::buf::BufferAccessStrategy { id: u32 }`, not the ring
// struct. Strategy objects are BACKEND-PRIVATE, so this crate owns a
// per-backend slab mapping `id -> ring`. `id == 0` is the C NULL (default,
// no-ring) strategy; nonzero ids name a live ring.
// ---------------------------------------------------------------------------

thread_local! {
    /// `id - 1` indexes the slab; `None` entries are freed slots. The id `0` is
    /// reserved for the C NULL strategy and is never stored here.
    static STRATEGIES: RefCell<alloc::vec::Vec<Option<strategy::BufferAccessStrategy>>> =
        const { RefCell::new(alloc::vec::Vec::new()) };
}

/// `GetAccessStrategy(btype)` (freelist.c) installed inward seam. Builds the
/// ring, stores it in the backend-private slab, and returns its id; id 0 is the
/// C NULL/default strategy (returned when `GetAccessStrategy` yields no ring).
pub fn get_access_strategy(btype: BufferAccessStrategyType) -> PgResult<BufferAccessStrategy> {
    let nbuffers_total = backend_utils_init_small_seams::nbuffers::call();
    let ring = strategy::BufferAccessStrategy::GetAccessStrategy(btype, nbuffers_total)?;
    match ring {
        None => Ok(BufferAccessStrategy { id: 0 }),
        Some(ring) => {
            let id = STRATEGIES.with(|slab| {
                let mut slab = slab.borrow_mut();
                // Reuse a freed slot if one exists.
                if let Some(pos) = slab.iter().position(|s| s.is_none()) {
                    slab[pos] = Some(ring);
                    (pos as u32) + 1
                } else {
                    slab.push(Some(ring));
                    slab.len() as u32
                }
            });
            Ok(BufferAccessStrategy { id })
        }
    }
}

/// `FreeAccessStrategy(strategy)` (freelist.c) installed inward seam. A NULL
/// (`id == 0`) strategy is a no-op (C's guard).
pub fn free_access_strategy(strategy: BufferAccessStrategy) {
    if strategy.id == 0 {
        return;
    }
    STRATEGIES.with(|slab| {
        let mut slab = slab.borrow_mut();
        let idx = (strategy.id - 1) as usize;
        if let Some(slot) = slab.get_mut(idx) {
            *slot = None;
        }
    });
}

/// Install this crate's inward seams (the two `BufferAccessStrategy` bufmgr
/// seams). The per-buffer header / shmem / smgr / GUC / latch seams this crate
/// CONSUMES are installed by their own owners.
pub fn init_seams() {
    backend_storage_buffer_bufmgr_seams::get_access_strategy::set(get_access_strategy);
    backend_storage_buffer_bufmgr_seams::free_access_strategy::set(free_access_strategy);
}

#[cfg(test)]
pub(crate) mod test_support;
