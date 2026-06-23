//! Handle registries backing the `walrcv_*` function-table seams.
//!
//! The rest of the backend (walreceiver, the logical apply worker, tablesync,
//! slotsync) calls libpq exclusively through the `walrcv_*` function table,
//! whose inward seams (`libpqwalreceiver_seams::*`) are
//! typed over opaque integer handles (`types_walreceiver::WalReceiverConn(usize)`
//! / `WalRcvExecResult(usize)` / `WalRcvResultTupslot(usize)`).  But the
//! *provider* side — the `libpqrcv_*` functions in this crate — works with live
//! owned objects (the analog of C's `WalReceiverConn *streamConn` /
//! `WalRcvExecResult *`).  Those objects cannot cross a `seam!`
//! function-pointer boundary by value and remain addressable across the many
//! `walrcv_receive`/`getattr` calls that drive a single connection or iterate a
//! single result.  So a registry — the integer handle is the seam-crossing
//! token, the live object stays parked here — is the right shape, exactly as
//! the WAL receiver in C keeps its `WalReceiverConn *` as backend-local
//! `palloc`'d memory (NOT shared memory; single-threaded per backend).
//!
//! `with` panics LOUDLY on an unknown handle: a `walrcv_*` call with a
//! stale/forged handle is a hard programming error (the direct analog of C
//! dereferencing a dangling pointer).  `0` is the NULL handle (no object),
//! matching the C convention.

use std::collections::BTreeMap;

use crate::WalRcvExecResult as OwnedExecResult;
use crate::WalReceiverConn;

/// One iterating slot over a result's tuplestore.  In C this is a
/// `TupleTableSlot *` made by `MakeTupleTableSlot`; here it is the owner-resident
/// handle the execTuples subsystem hands back through the seam, paired with the
/// result it iterates.
#[derive(Clone, Copy, Debug)]
pub struct ResultTupslot {
    /// The `TupleTableSlot` handle (execTuples-owned, via the seam).
    pub slot: fe_seams::ResultTupslotId,
    /// The result-registry handle whose tuplestore this slot iterates.
    pub result: usize,
}

/// Process-local connection / result / slot tables plus the monotonic handle
/// allocator.  Single-threaded per backend (matching C's backend-local
/// `WalReceiverConn` memory and the single-threaded WAL-receive / logical-decode
/// loops), so no lock is required.
struct Registry {
    conns: BTreeMap<usize, WalReceiverConn>,
    results: BTreeMap<usize, OwnedExecResult>,
    tupslots: BTreeMap<usize, ResultTupslot>,
    /// Next handle to hand out. Starts at 1 so `0` stays the NULL sentinel.
    next: usize,
}

static mut REGISTRY: Registry = Registry {
    conns: BTreeMap::new(),
    results: BTreeMap::new(),
    tupslots: BTreeMap::new(),
    next: 1,
};

/// Run `f` with exclusive access to the process-local registry.
///
/// SAFETY: libpqwalreceiver is driven single-threaded per backend (the WAL
/// receiver daemon, the apply worker, tablesync, slotsync each run as one
/// process with one decode/receive loop), so there is never concurrent access.
fn with_registry<R>(f: impl FnOnce(&mut Registry) -> R) -> R {
    unsafe { f(&mut *std::ptr::addr_of_mut!(REGISTRY)) }
}

fn alloc_id(r: &mut Registry) -> usize {
    let id = r.next;
    r.next += 1;
    id
}

// ---------------------------------------------------------------------------
// Connections.
// ---------------------------------------------------------------------------

/// Insert a freshly-connected [`WalReceiverConn`] and return its non-zero handle.
pub fn insert_conn(conn: WalReceiverConn) -> usize {
    with_registry(|r| {
        let id = alloc_id(r);
        r.conns.insert(id, conn);
        id
    })
}

/// Remove the connection for `handle` and return it (so the caller can run the
/// real disconnect on the owned value and drop it). `None` if unknown / NULL.
pub fn remove_conn(handle: usize) -> Option<WalReceiverConn> {
    if handle == 0 {
        return None;
    }
    with_registry(|r| r.conns.remove(&handle))
}

/// Borrow the live connection for `handle` and run `f` against it.
pub fn with_conn<R>(handle: usize, f: impl FnOnce(&WalReceiverConn) -> R) -> R {
    with_registry(|r| {
        let conn = r
            .conns
            .get(&handle)
            .unwrap_or_else(|| panic!("walrcv: unknown WalReceiverConn handle {handle}"));
        f(conn)
    })
}

/// Like [`with_conn`], but borrows the connection mutably (for `walrcv_receive`,
/// which refills `conn.recvBuf`).
pub fn with_conn_mut<R>(handle: usize, f: impl FnOnce(&mut WalReceiverConn) -> R) -> R {
    with_registry(|r| {
        let conn = r
            .conns
            .get_mut(&handle)
            .unwrap_or_else(|| panic!("walrcv: unknown WalReceiverConn handle {handle}"));
        f(conn)
    })
}

// ---------------------------------------------------------------------------
// Exec results.
// ---------------------------------------------------------------------------

/// Park a `walrcv_exec` result and return its non-zero handle.
pub fn insert_result(res: OwnedExecResult) -> usize {
    with_registry(|r| {
        let id = alloc_id(r);
        r.results.insert(id, res);
        id
    })
}

/// Borrow the parked result for `handle`.
pub fn with_result<R>(handle: usize, f: impl FnOnce(&OwnedExecResult) -> R) -> R {
    with_registry(|r| {
        let res = r
            .results
            .get(&handle)
            .unwrap_or_else(|| panic!("walrcv: unknown WalRcvExecResult handle {handle}"));
        f(res)
    })
}

/// Remove and return the parked result (for `walrcv_clear_result`). `None` if
/// unknown / NULL.
pub fn remove_result(handle: usize) -> Option<OwnedExecResult> {
    if handle == 0 {
        return None;
    }
    with_registry(|r| r.results.remove(&handle))
}

// ---------------------------------------------------------------------------
// Result-iteration slots.
// ---------------------------------------------------------------------------

/// Park an iterating slot and return its non-zero handle.
pub fn insert_tupslot(tupslot: ResultTupslot) -> usize {
    with_registry(|r| {
        let id = alloc_id(r);
        r.tupslots.insert(id, tupslot);
        id
    })
}

/// Resolve a tupslot handle.
pub fn get_tupslot(handle: usize) -> ResultTupslot {
    with_registry(|r| {
        *r.tupslots
            .get(&handle)
            .unwrap_or_else(|| panic!("walrcv: unknown WalRcvResultTupslot handle {handle}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_conn(tag: usize) -> WalReceiverConn {
        WalReceiverConn {
            streamConn: tag,
            logical: tag % 2 == 0,
            recvBuf: Vec::new(),
        }
    }

    #[test]
    fn conn_handles_are_nonzero_distinct() {
        let a = insert_conn(fresh_conn(10));
        let b = insert_conn(fresh_conn(20));
        assert_ne!(a, 0);
        assert_ne!(b, 0);
        assert_ne!(a, b);
    }

    #[test]
    fn with_conn_resolves_and_mutates() {
        let h = insert_conn(fresh_conn(42));
        assert_eq!(with_conn(h, |c| c.streamConn), 42);
        with_conn_mut(h, |c| c.recvBuf = Vec::from(&b"hi"[..]));
        assert_eq!(with_conn(h, |c| c.recvBuf.clone()), b"hi");
    }

    #[test]
    fn remove_conn_forgets() {
        let h = insert_conn(fresh_conn(99));
        assert_eq!(remove_conn(h).expect("present").streamConn, 99);
        assert!(remove_conn(h).is_none());
        assert!(remove_conn(0).is_none());
    }

    #[test]
    #[should_panic(expected = "unknown WalReceiverConn handle")]
    fn unknown_conn_handle_panics() {
        with_conn(usize::MAX, |_| ());
    }
}
