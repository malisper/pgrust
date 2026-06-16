//! Seam declarations for `backend-utils-adt-mcxtfuncs`
//! (`utils/adt/mcxtfuncs.c`: the backend memory-context introspection
//! functions).
//!
//! The *algorithmic logic* of mcxtfuncs.c — the breadth-first walk over the
//! live `MemoryContext` tree, the transient `context_id` assignment, the
//! ancestor `path` construction, the dynahash relabeling, the identifier
//! clipping, and the per-column row layout — is ported in-crate in the owner
//! crate. mcxtfuncs.c is otherwise a thin SQL wrapper that reaches into
//! genuinely-external subsystems, each crossing one of the seams declared here.
//!
//! The seams owned by the **unported `utils/mmgr/mcxt.c` remainder** carry the
//! live `MemoryContext` tree. The C `MemoryContext` is a `MemoryContextData *`
//! used here only as a dynahash key and a tree-walk cursor; until that owner
//! lands the cursor is an opaque [`MemoryContextRef`] handle the owner mints and
//! resolves (inherited opacity for an unported owner, not an invented one).
//!
//!  * **`utils/mmgr/mcxt.c`** — `TopMemoryContext`
//!    ([`top_memory_context`]), the tree fields and node tag
//!    ([`context_node`]), and the per-context `methods->stats` call
//!    ([`context_stats`]).
//!  * **`utils/fmgr/funcapi.c` / `utils/sort/tuplestore.c`** — the
//!    `tuplestore_putvalues` of an `InitMaterializedSRF`-prepared
//!    `ReturnSetInfo`, plus the `CStringGetTextDatum` / `construct_array_builtin`
//!    Datum assembly it consumes; folded into [`tuplestore_putvalues`], which
//!    receives the already-computed owned [`McxtRow`].
//!  * **`storage/ipc/procarray.c` / `storage/lmgr/proc.c`** —
//!    `BackendPidGetProc` / `AuxiliaryPidGetProc` / `GetNumberFromPGProc`,
//!    folded into [`pid_get_proc`]. (`SendProcSignal` is the existing
//!    `backend-storage-ipc-procsignal-seams::send_proc_signal`.)

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;

use types_core::ProcNumber;
use types_error::PgResult;

/// `MemoryContext` — a `MemoryContextData *` in C, here an opaque cursor handle
/// the (unported) `mcxt.c` owner mints and resolves. Used only as a dynahash
/// key (identity comparison) and a tree-walk cursor.
pub type MemoryContextRef = usize;

/// `MemoryContextCounters` (`utils/mmgr/memnodes.h`) — the per-context stats the
/// `(*context->methods->stats)` callback fills in. Field order and meaning match
/// the C struct exactly; the SRF emits `totalspace`, `nblocks`, `freespace`,
/// `freechunks`, and `totalspace - freespace`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MemoryContextCounters {
    /// `nblocks` — total number of malloc blocks.
    pub nblocks: usize,
    /// `freechunks` — total number of free chunks.
    pub freechunks: usize,
    /// `totalspace` — total bytes requested from malloc.
    pub totalspace: usize,
    /// `freespace` — the unused portion of `totalspace`.
    pub freespace: usize,
}

/// The kind of memory-context backend, resolved from C's `context->type`
/// `NodeTag` (`T_AllocSetContext` / `T_GenerationContext` / `T_SlabContext` /
/// `T_BumpContext`). Anything else maps to [`MemoryContextType::Unknown`]
/// (C's `"???"`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryContextType {
    /// `T_AllocSetContext` -> `"AllocSet"`.
    AllocSet,
    /// `T_GenerationContext` -> `"Generation"`.
    Generation,
    /// `T_SlabContext` -> `"Slab"`.
    Slab,
    /// `T_BumpContext` -> `"Bump"`.
    Bump,
    /// any other `NodeTag` -> `"???"`.
    Unknown,
}

/// The tree-cursor view of a live `MemoryContext`: the fields mcxtfuncs.c reads
/// off `MemoryContextData` while walking — `parent`/`firstchild`/`nextchild`
/// links, the `name`/`ident` identifiers (raw server-encoding bytes, no lossy
/// UTF-8 conversion), and the resolved context `type`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryContextNode {
    /// `context->parent`.
    pub parent: Option<MemoryContextRef>,
    /// `context->firstchild`.
    pub firstchild: Option<MemoryContextRef>,
    /// `context->nextchild`.
    pub nextchild: Option<MemoryContextRef>,
    /// `context->name` (NULL -> `None`).
    pub name: Option<Vec<u8>>,
    /// `context->ident` (NULL -> `None`).
    pub ident: Option<Vec<u8>>,
    /// `context->type` resolved to a [`MemoryContextType`].
    pub context_type: MemoryContextType,
}

/// One output row of `pg_get_backend_memory_contexts` — the already-computed
/// `values[]`/`nulls[]` of `PutMemoryContextsStatsTupleStore`, in column order.
/// The provider side of [`tuplestore_putvalues`] turns these owned values into
/// the `text` / `int4` / `int4[]` / `int8` Datums (`CStringGetTextDatum`,
/// `Int32GetDatum`, `construct_array_builtin`, `Int64GetDatum`) and appends them
/// to the SRF tuplestore.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct McxtRow {
    /// `values[0]` `name` (`None` -> `nulls[0]`), raw server-encoding bytes.
    pub name: Option<Vec<u8>>,
    /// `values[1]` `ident` (`None` -> `nulls[1]`), clipped, raw bytes.
    pub ident: Option<Vec<u8>>,
    /// `values[2]` `type`.
    pub context_type: Vec<u8>,
    /// `values[3]` `level` = `list_length(path)`.
    pub level: i32,
    /// `values[4]` `path` = `int_list_to_array(path)`.
    pub path: Vec<i32>,
    /// `values[5]` `total_bytes` = `stat.totalspace`.
    pub total_bytes: i64,
    /// `values[6]` `total_nblocks` = `stat.nblocks`.
    pub n_blocks: i64,
    /// `values[7]` `free_bytes` = `stat.freespace`.
    pub free_bytes: i64,
    /// `values[8]` `free_chunks` = `stat.freechunks`.
    pub free_chunks: i64,
    /// `values[9]` `used_bytes` = `stat.totalspace - stat.freespace`.
    pub used_bytes: i64,
}

/// The result of `BackendPidGetProc`/`AuxiliaryPidGetProc` plus
/// `GetNumberFromPGProc` — the only `PGPROC` information
/// `pg_log_backend_memory_contexts` needs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct McxtSignalTarget {
    /// `GetNumberFromPGProc(proc)`.
    pub proc_number: ProcNumber,
}

seam_core::seam!(
    /// `TopMemoryContext` (`mcxt.c`) — the root of the live context tree, the
    /// start of `pg_get_backend_memory_contexts`'s breadth-first walk
    /// (`list_make1(TopMemoryContext)`). Infallible read of a backend global.
    pub fn top_memory_context() -> PgResult<MemoryContextRef>
);

seam_core::seam!(
    /// Read the tree-cursor fields of a live `MemoryContext`
    /// (`parent`/`firstchild`/`nextchild`/`name`/`ident`/`type`). Infallible
    /// (pointer dereference of a valid context); the `PgResult` carries the
    /// owner's `MemoryContextIsValid` assertion surface.
    pub fn context_node(context: MemoryContextRef) -> PgResult<MemoryContextNode>
);

seam_core::seam!(
    /// `(*context->methods->stats)(context, NULL, NULL, &stat, true)`
    /// (`mcxt.c`): fill `MemoryContextCounters` for one context (no recursion,
    /// no printing). `Err` carries any `ereport(ERROR)` the stats method path
    /// can raise.
    pub fn context_stats(context: MemoryContextRef) -> PgResult<MemoryContextCounters>
);

seam_core::seam!(
    /// `tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls)`
    /// against the `InitMaterializedSRF`-prepared `ReturnSetInfo`, folded with
    /// the `CStringGetTextDatum`/`construct_array_builtin`/`Int*GetDatum` Datum
    /// assembly of the SRF's `values[]`/`nulls[]`. Receives the already-computed
    /// owned [`McxtRow`]. `Err` carries the palloc `ereport(ERROR)` of tuple
    /// forming / append.
    pub fn tuplestore_putvalues(row: McxtRow) -> PgResult<()>
);

seam_core::seam!(
    /// `BackendPidGetProc(pid)` and, on `NULL`, `AuxiliaryPidGetProc(pid)`
    /// (`procarray.c`), followed by `GetNumberFromPGProc(proc)` (`proc.h`) —
    /// resolve a backend/auxiliary PID to its [`McxtSignalTarget`], or `None`
    /// if no such process. `Err` carries any shared-memory access
    /// `ereport(ERROR)`.
    pub fn pid_get_proc(pid: i32) -> PgResult<Option<McxtSignalTarget>>
);
