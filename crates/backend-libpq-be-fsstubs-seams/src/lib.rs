//! Seam declarations for the `backend-libpq-be-fsstubs` unit
//! (`libpq/be-fsstubs.c`, large-object descriptors).
//!
//! Two groups of seams live here:
//!
//!   * the **outward** `at_eoxact_large_object` / `at_eosubxact_large_object`
//!     transaction-end hooks: the owning `backend-libpq-be-fsstubs` unit
//!     installs these from its `init_seams()`, and `access/xact.c` consumes
//!     them.
//!
//!   * the **inward** snapshot-on-top-owner registration
//!     (`register_snapshot_on_top_owner` / `unregister_snapshot_from_top_owner`)
//!     and the server-file I/O halves of `lo_import` / `lo_export`
//!     (`import_server_file` / `export_server_file`). These are the genuine
//!     externals be-fsstubs depends on that have no faithful direct call in this
//!     repo yet: `RegisterSnapshotOnOwner(snapshot, TopTransactionResourceOwner)`
//!     needs the `TopTransactionResourceOwner` resowner argument (snapmgr's
//!     repo port is the `NoOwner` core over the incompatible `SnapHandle`
//!     registry model, not the `Rc<SnapshotData>` carried by `LargeObjectDesc`),
//!     and the raw `read()`/`write()` byte loops + `umask` dance of the
//!     import/export file transfer are the server-file primitive. Until their
//!     owners land, a call panics loudly.

use types_core::SubTransactionId;
use types_error::PgResult;

seam_core::seam!(
    /// `AtEOXact_LargeObject(isCommit)` — close large-object descriptors at
    /// transaction end.
    pub fn at_eoxact_large_object(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `AtEOSubXact_LargeObject(isCommit, mySubid, parentSubid)`.
    pub fn at_eosubxact_large_object(
        is_commit: bool,
        my_subid: SubTransactionId,
        parent_subid: SubTransactionId,
    ) -> PgResult<()>
);

// NOTE: `register_snapshot_on_top_owner` / `unregister_snapshot_from_top_owner`
// were re-homed to `backend-utils-time-snapmgr-seams` (their true C owner is
// `utils/time/snapmgr.c`); be-fsstubs now calls them through that crate.

seam_core::seam!(
    /// `lo_import_internal` server-file half (be-fsstubs.c:423-479): open
    /// `filename` (`OpenTransientFile(O_RDONLY | PG_BINARY)`) and stream its
    /// contents in `BUFSIZE` chunks into the just-opened write descriptor via
    /// repeated `inv_write`, then close the file. `write_chunk` is invoked for
    /// each chunk read from the file (it performs `inv_write(lobj, buf, nbytes)`
    /// in the crate, against the descriptor it owns); the `could not
    /// open/read/close server file` errors are raised here exactly as the C
    /// does.
    pub fn import_server_file(
        filename: &[u8],
        write_chunk: &mut dyn FnMut(&[u8]) -> PgResult<i32>,
    ) -> PgResult<()>
);

seam_core::seam!(
    /// `be_lo_export` server-file half (be-fsstubs.c:485-551): create `filename`
    /// (`OpenTransientFilePerm`, with the friendlier 022 umask) and stream the
    /// LO into it in `BUFSIZE` chunks via repeated `inv_read` (driven by
    /// `read_chunk`, which fills a `BUFSIZE` buffer and returns the byte count),
    /// then close the file. The `could not create/write/close server file`
    /// errors are raised here as in the C.
    pub fn export_server_file(
        filename: &[u8],
        read_chunk: &mut dyn FnMut(&mut [u8]) -> PgResult<i32>,
    ) -> PgResult<()>
);
