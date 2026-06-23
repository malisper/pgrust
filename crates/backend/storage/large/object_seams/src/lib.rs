//! Seam declarations for the `backend-storage-large-object` unit
//! (`storage/large_object/inv_api.c`, the server-side inversion-fs large-object
//! byte API).
//!
//! These are the *outward-consumed* entry points: `close_lo_relation` is called
//! by `access/transam/xact.c` at main-transaction end, and the `inv_*` byte API
//! is called by `libpq/be-fsstubs.c` and the large-object SQL functions
//! (`utils/adt/`/`catalog/`). The owning unit installs them from its
//! `init_seams()`; until those consumers land a call panics loudly.
//!
//! The open descriptor crosses as the owned
//! [`::types_storage::large_object::LargeObjectDesc`]; read/write buffers cross as
//! owned slices (the C `char *buf` + `int nbytes`).

use ::types_core::{int64, Oid};
use ::types_error::PgResult;
use ::types_storage::large_object::LargeObjectDesc;

seam_core::seam!(
    /// `close_lo_relation(isCommit)` (inv_api.c): clean up the single
    /// `pg_largeobject` relation reference at main-transaction end. Called by
    /// `xact.c`. `Err` carries the relation-close error surface.
    pub fn close_lo_relation(is_commit: bool) -> PgResult<()>
);

seam_core::seam!(
    /// `inv_create(lobjId)` (inv_api.c): create a new large object, returning
    /// its OID. `lobjId == InvalidOid` picks one.
    pub fn inv_create(lobj_id: Oid) -> PgResult<Oid>
);

seam_core::seam!(
    /// `inv_open(lobjId, flags, mcxt)` (inv_api.c): open an existing large
    /// object, returning a filled-in descriptor (owned; the C allocates it in
    /// the caller's memory context).
    pub fn inv_open(lobj_id: Oid, flags: i32) -> PgResult<Box<LargeObjectDesc>>
);

seam_core::seam!(
    /// `inv_close(obj_desc)` (inv_api.c): close a descriptor made by
    /// [`inv_open`] (the C `pfree`; here `Drop` of the owned descriptor).
    pub fn inv_close(obj_desc: Box<LargeObjectDesc>) -> PgResult<()>
);

seam_core::seam!(
    /// `inv_drop(lobjId)` (inv_api.c): destroy an existing large object.
    /// Returns 1 on success (historical).
    pub fn inv_drop(lobj_id: Oid) -> PgResult<i32>
);

seam_core::seam!(
    /// `inv_seek(obj_desc, offset, whence)` (inv_api.c): reposition the seek
    /// pointer, returning the new offset.
    pub fn inv_seek(obj_desc: &mut LargeObjectDesc, offset: int64, whence: i32) -> PgResult<int64>
);

seam_core::seam!(
    /// `inv_tell(obj_desc)` (inv_api.c): the current seek offset.
    pub fn inv_tell(obj_desc: &LargeObjectDesc) -> PgResult<int64>
);

seam_core::seam!(
    /// `inv_read(obj_desc, buf, nbytes)` (inv_api.c): read up to `buf.len()`
    /// bytes into `buf`, returning the count read.
    pub fn inv_read(obj_desc: &mut LargeObjectDesc, buf: &mut [u8]) -> PgResult<i32>
);

seam_core::seam!(
    /// `inv_write(obj_desc, buf, nbytes)` (inv_api.c): write `buf.len()` bytes,
    /// returning the count written.
    pub fn inv_write(obj_desc: &mut LargeObjectDesc, buf: &[u8]) -> PgResult<i32>
);

seam_core::seam!(
    /// `inv_truncate(obj_desc, len)` (inv_api.c): truncate the large object to
    /// `len` bytes.
    pub fn inv_truncate(obj_desc: &mut LargeObjectDesc, len: int64) -> PgResult<()>
);
