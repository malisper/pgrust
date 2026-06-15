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

use alloc::rc::Rc;
use core::cell::RefCell;

use types_error::PgResult;
use types_storage::buf::{BufferAccessStrategy, BufferAccessStrategyType};

mod buf_table;
mod freelist;
mod localbuf;
mod strategy;

pub use buf_table::{buf_table_hash_code, buf_table_hash_partition, BufTable, BufTableShmemSize};
pub use freelist::{BufferStrategyControl, ClockSweep, StrategyShmemSize};
pub use localbuf::{check_temp_buffers, LocalBufferManager};
pub use strategy::{
    get_access_strategy_ring, get_access_strategy_with_size_ring, BufferAccessStrategyRing,
    FreeAccessStrategy,
};

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
// Backend-private `BufferAccessStrategy` ring.
//
// `BufferAccessStrategyData` is a backend-private object that C's
// `GetAccessStrategy` `palloc`s and hands BACK BY POINTER (`typedef struct
// BufferAccessStrategyData *BufferAccessStrategy`); callers hold it directly and
// mutate the ring through the pointer until `FreeAccessStrategy` `pfree`s it.
// The faithful Rust model of that single shared/mutated heap object is an
// `Rc<RefCell<_>>` (see the `BufferAccessStrategy` alias in types-storage); the
// C `NULL` (default, no-ring) strategy is `None`. There is no id-keyed lookup
// table — the handle IS the object.
// ---------------------------------------------------------------------------

/// `GetAccessStrategy(btype)` (freelist.c) installed inward seam. Builds the
/// ring (or `None` for the default/no-ring strategy) and returns it as the
/// by-pointer handle (`Rc<RefCell<_>>`), mirroring C's `palloc`'d object.
pub fn get_access_strategy(btype: BufferAccessStrategyType) -> PgResult<BufferAccessStrategy> {
    let nbuffers_total = backend_utils_init_small_seams::nbuffers::call();
    let ring = strategy::get_access_strategy_ring(btype, nbuffers_total)?;
    Ok(ring.map(|ring| Rc::new(RefCell::new(ring))))
}

/// `FreeAccessStrategy(strategy)` (freelist.c) installed inward seam. A NULL
/// (`None`) strategy is a no-op (C's guard); otherwise dropping the handle frees
/// the ring once the last reference is gone (C's `pfree`).
pub fn free_access_strategy(strategy: BufferAccessStrategy) {
    drop(strategy);
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
