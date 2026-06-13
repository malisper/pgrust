//! Tuple-hash-table vocabulary (`executor/execGrouping.c` /
//! `nodes/execnodes.h`), trimmed to the handle/iterator types the grouping
//! node units (`nodeSetOp`, `nodeAgg`, `nodeRecursiveunion`, ...) store and
//! pass back to execGrouping.
//!
//! `TupleHashTable` is C's `tuplehash_hash *` — a pointer to the
//! simplehash-generated table that execGrouping owns and the consumer only
//! holds and threads back into `LookupTupleHashEntry`/`ScanTupleHashTable`/
//! `ResetTupleHashTable`. `TupleHashEntry` is `TupleHashEntryData *`, an
//! entry handle the consumer passes to `TupleHashEntryGetAdditional` /
//! `TupleHashEntryGetTuple`. Neither's internals are inspected by the
//! consumer, so the owned model carries them as opaque tokens the execGrouping
//! owner resolves; the per-group `additional` payload (which the consumer
//! *does* read/write) crosses the execGrouping seam as its real type.

#![no_std]

extern crate alloc;

/// `TupleHashTable` (`nodes/execnodes.h`) — a handle to the execGrouping-owned
/// hash table (`tuplehash_hash *`). The consumer never inspects it; it stores
/// the handle and threads it back through the execGrouping seams. The payload
/// is a boxed `dyn Any` the owner downcasts (genuinely owner-private state;
/// `None` is the C `NULL`).
#[derive(Default)]
pub struct TupleHashTable(pub Option<alloc::boxed::Box<dyn core::any::Any>>);

impl core::fmt::Debug for TupleHashTable {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self.0 {
            Some(_) => f.write_str("TupleHashTable(<set>)"),
            None => f.write_str("TupleHashTable(<null>)"),
        }
    }
}

/// `TupleHashEntry` (`nodes/execnodes.h`) — `TupleHashEntryData *`, an opaque
/// `Copy` handle to one entry in a [`TupleHashTable`]. The consumer obtains it
/// from `LookupTupleHashEntry` / `ScanTupleHashTable` and passes it to
/// `TupleHashEntryGetAdditional` / `TupleHashEntryGetTuple`; it never
/// dereferences it. The `u64` is the execGrouping owner's entry identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct TupleHashEntry(pub u64);

/// `TupleHashIterator` (`nodes/execnodes.h`) — iteration state for
/// `ScanTupleHashTable`, reset by `ResetTupleHashIterator`. C's
/// `tuplehash_iterator` is `{ uint32 cur; uint32 end; bool done; }`; the
/// consumer only zero-initializes it (`ResetTupleHashIterator`) and threads it
/// into the scan, so the owned model carries the same words for the owner to
/// drive.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TupleHashIterator {
    /// `uint32 cur` — current bucket cursor.
    pub cur: u32,
    /// `uint32 end` — end bucket.
    pub end: u32,
    /// `bool done` — iteration finished?
    pub done: bool,
}
