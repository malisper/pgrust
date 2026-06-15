//! `backend/commands/matview.c` file-local C-ABI definitions.
//!
//! Only `matview.c`'s own private DestReceiver subtype crosses an ABI boundary
//! (the executor stores result tuples into it through the `DestReceiver`
//! function pointers).  Everything else `matview.c` touches —
//! `RefreshMatViewStmt`, `DestReceiver`, `QueryCompletion`, `RangeVar`,
//! `ObjectAddress`, `CommandId`, `BulkInsertState`, the lock-mode / RELKIND /
//! RELPERSISTENCE / SQLSTATE constants — already lives in the existing
//! `pgrust-pg-ffi` modules and is reused as-is.
//!
//! Referenced by path (`pgrust_pg_ffi::matview::DR_transientrel`); deliberately
//! NOT in the crate-root glob to avoid ambiguous-glob collisions with the
//! widely-named `DestReceiver` / `Relation` / `BulkInsertState` types.

use crate::executor::DestReceiver;
use crate::heap::BulkInsertState;
use crate::{CommandId, Oid};

/// `Relation` as used by `matview.c` — a relcache entry pointer.  Mirrors the
/// boundary `pub type Relation = *mut c_void` convention used elsewhere in the
/// FFI crate; the relcache internals are owned by the relation-cache subsystem
/// and reached through the matview seam.
pub type Relation = *mut core::ffi::c_void;

/// `DR_transientrel` (matview.c lines 45-54) — the private state of the
/// transient-relation `DestReceiver` used to bulk-load the regenerated matview
/// data into the freshly created transient heap.
///
/// ```c
/// typedef struct
/// {
///     DestReceiver pub;           /* publicly-known function pointers */
///     Oid         transientoid;   /* OID of new heap into which to store */
///     /* These fields are filled by transientrel_startup: */
///     Relation    transientrel;   /* relation to write to */
///     CommandId   output_cid;     /* cmin to insert in output tuples */
///     int         ti_options;     /* table_tuple_insert performance options */
///     BulkInsertState bistate;    /* bulk insert state */
/// } DR_transientrel;
/// ```
///
/// The leading `DestReceiver pub` member makes a `*mut DR_transientrel`
/// castable to/from `*mut DestReceiver`, exactly as the C relies on.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DR_transientrel {
    /// `DestReceiver pub` — publicly-known function pointers (must be first so
    /// the struct is layout-compatible with a bare `DestReceiver`).
    pub pub_: DestReceiver,
    /// `Oid transientoid` — OID of the new heap into which to store rows.
    pub transientoid: Oid,
    /// `Relation transientrel` — relation to write to (filled by
    /// `transientrel_startup`).
    pub transientrel: Relation,
    /// `CommandId output_cid` — cmin to insert in output tuples.
    pub output_cid: CommandId,
    /// `int ti_options` — `table_tuple_insert` performance options.
    pub ti_options: core::ffi::c_int,
    /// `BulkInsertState bistate` — bulk-insert state.
    pub bistate: BulkInsertState,
}
