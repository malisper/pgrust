//! Executor node types (`nodes/execnodes.h`) that the node-construction helpers
//! in `makefuncs.c` create directly.
//!
//! Only [`IndexInfo`] lives here: it is `pg_node_attr(no_copy_equal, no_read,
//! no_query_jumble)` (executor working state, never traversed by
//! copy/equal/read), but `makeIndexInfo` constructs it, so its exact `#[repr(C)]`
//! layout is modelled here. The executor-internal pointee types it references
//! (`ExprState`, the exclusion/unique operator arrays, the AM cache,
//! `MemoryContext`) are never deep-copied here; they are carried as raw pointers
//! with the same ABI width as the C struct, exactly like the `copy_as_scalar`
//! opaque-pointee pattern used in [`crate::pathnodes`].

use core::ffi::{c_int, c_void};

use pg_ffi_fgram::{AttrNumber, ExprState, List, MemoryContext, NodeTag, Oid};

use crate::pathnodes::INDEX_MAX_KEYS;

/// `IndexInfo` (`nodes/execnodes.h`, `pg_node_attr(no_copy_equal, no_read,
/// no_query_jumble)`) - information about one index, as understood by the
/// executor's index-build/insertion machinery. Constructed by `makeIndexInfo`.
///
/// The three fixed-size `ii_IndexAttrNumbers` slots are `INDEX_MAX_KEYS`-wide.
/// `ii_Expressions`/`ii_Predicate` hold `Expr` lists; `ii_ExpressionsState`/
/// `ii_PredicateState` hold compiled executor state; the `ii_Exclusion*`/
/// `ii_Unique*` members are per-column arrays the planner/executor fill in
/// later; `ii_AmCache` and `ii_Context` are AM- and memory-context-private.
#[repr(C)]
pub struct IndexInfo {
    pub type_: NodeTag,
    /// total number of columns in index
    pub ii_NumIndexAttrs: c_int,
    /// number of key columns in index
    pub ii_NumIndexKeyAttrs: c_int,
    pub ii_IndexAttrNumbers: [AttrNumber; INDEX_MAX_KEYS],
    /// list of Expr
    pub ii_Expressions: *mut List,
    /// list of ExprState
    pub ii_ExpressionsState: *mut List,
    /// list of Expr
    pub ii_Predicate: *mut List,
    pub ii_PredicateState: *mut ExprState,
    /// array with one entry per column
    pub ii_ExclusionOps: *mut Oid,
    /// array with one entry per column
    pub ii_ExclusionProcs: *mut Oid,
    /// array with one entry per column
    pub ii_ExclusionStrats: *mut u16,
    /// array with one entry per column
    pub ii_UniqueOps: *mut Oid,
    /// array with one entry per column
    pub ii_UniqueProcs: *mut Oid,
    /// array with one entry per column
    pub ii_UniqueStrats: *mut u16,
    pub ii_Unique: bool,
    pub ii_NullsNotDistinct: bool,
    pub ii_ReadyForInserts: bool,
    pub ii_CheckedUnchanged: bool,
    pub ii_IndexUnchanged: bool,
    pub ii_Concurrent: bool,
    pub ii_BrokenHotChain: bool,
    pub ii_Summarizing: bool,
    pub ii_WithoutOverlaps: bool,
    pub ii_ParallelWorkers: c_int,
    pub ii_Am: Oid,
    pub ii_AmCache: *mut c_void,
    pub ii_Context: MemoryContext,
}

// ---------------------------------------------------------------------------
// Compile-time exact-layout assertions (LP64), matching `struct IndexInfo`.
// ---------------------------------------------------------------------------

const _: () = {
    use core::mem::{offset_of, size_of};

    // NodeTag header at offset 0.
    assert!(offset_of!(IndexInfo, type_) == 0);
    assert!(offset_of!(IndexInfo, ii_NumIndexAttrs) == 4);
    assert!(offset_of!(IndexInfo, ii_NumIndexKeyAttrs) == 8);
    // [AttrNumber; 32] = 64 bytes at offset 12.
    assert!(offset_of!(IndexInfo, ii_IndexAttrNumbers) == 12);
    // First pointer field, padded from 76 up to 8-alignment.
    assert!(offset_of!(IndexInfo, ii_Expressions) == 80);
    assert!(offset_of!(IndexInfo, ii_ExpressionsState) == 88);
    assert!(offset_of!(IndexInfo, ii_Predicate) == 96);
    assert!(offset_of!(IndexInfo, ii_PredicateState) == 104);
    assert!(offset_of!(IndexInfo, ii_ExclusionOps) == 112);
    assert!(offset_of!(IndexInfo, ii_ExclusionProcs) == 120);
    assert!(offset_of!(IndexInfo, ii_ExclusionStrats) == 128);
    assert!(offset_of!(IndexInfo, ii_UniqueOps) == 136);
    assert!(offset_of!(IndexInfo, ii_UniqueProcs) == 144);
    assert!(offset_of!(IndexInfo, ii_UniqueStrats) == 152);
    // Nine consecutive bools.
    assert!(offset_of!(IndexInfo, ii_Unique) == 160);
    assert!(offset_of!(IndexInfo, ii_WithoutOverlaps) == 168);
    // int after the bool run, padded to 4-alignment from 169.
    assert!(offset_of!(IndexInfo, ii_ParallelWorkers) == 172);
    assert!(offset_of!(IndexInfo, ii_Am) == 176);
    // pointer, padded from 180 to 8-alignment.
    assert!(offset_of!(IndexInfo, ii_AmCache) == 184);
    assert!(offset_of!(IndexInfo, ii_Context) == 192);
    assert!(size_of::<IndexInfo>() == 200);
};
