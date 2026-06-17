#![allow(non_snake_case)]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(clippy::result_large_err)]
// The C declares locals up top and assigns later; keep that decl-then-assign
// shape so the port reads 1:1 against be-fsstubs.c.
#![allow(clippy::needless_late_init)]

//! `backend/libpq/be-fsstubs.c` — built-in functions for open/close/read/write
//! operations on large objects (the SQL-callable `lo_*` interface).
//!
//! Faithful 1:1 port of be-fsstubs.c. Every C function is present with its
//! original name, FD-cookie-table semantics, descriptor-state guards, lock-mode
//! handling, branch order, and error codes / messages / SQLSTATE preserved. The
//! process-local FD-cookie table (`cookies` / `cookies_size` /
//! `lo_cleanup_needed`) is per-backend state (correct: LO FDs are only good
//! within a transaction), kept in [`state`].
//!
//! SQL-callable entry points (the `be_lo_*` functions): each is ported as a
//! plain Rust function over already-unwrapped arguments — the fmgr/`Datum`
//! (`PG_FUNCTION_ARGS`, `PG_GETARG_*` / `PG_RETURN_*`) glue is the accepted
//! project-wide deferral, so these take/return Rust values (`bytea` results are
//! owned `Vec<u8>`):
//!
//!   * [`be_lo_open`], [`be_lo_close`], [`be_lo_lseek`], [`be_lo_lseek64`],
//!     [`be_lo_creat`], [`be_lo_create`], [`be_lo_tell`], [`be_lo_tell64`],
//!     [`be_lo_unlink`], [`be_loread`], [`be_lowrite`], [`be_lo_import`],
//!     [`be_lo_import_with_oid`], [`be_lo_export`], [`be_lo_truncate`],
//!     [`be_lo_truncate64`], [`be_lo_get`], [`be_lo_get_fragment`],
//!     [`be_lo_from_bytea`], [`be_lo_put`].
//!
//! Bare (non-fmgr) ops + helpers ported in-crate: [`lo_read`], [`lo_write`],
//! `lo_truncate_internal`, `lo_import_internal`, `lo_get_fragment_internal`,
//! [`AtEOXact_LargeObject`], [`AtEOSubXact_LargeObject`], `newLOfd`, `closeLOfd`.
//!
//! ## Idiomatic owned style vs the faithful raw-pointer C
//!
//!   * the cookie slots OWN the boxed [`LargeObjectDesc`]
//!     (`Option<Box<LargeObjectDesc>>`) — exactly the box `inv_open` hands back;
//!     the C `LargeObjectDesc *` allocated in `fscxt` is replaced by Rust
//!     ownership, so the `fscxt` `MemoryContext` (whose only job was to own those
//!     allocations for the xact) disappears (see [`state`]);
//!   * the bare-read/write `(char *buf, int len)` becomes the owned slice
//!     `&mut [u8]` / `&[u8]`; the bytea results are owned `Vec<u8>`;
//!   * `closeLOfd` takes the owned descriptor out of its slot, hands the owned
//!     snapshot to `UnregisterSnapshotFromOwner`, then `inv_close` consumes the
//!     descriptor (the C `pfree` is `Drop`).
//!
//! ## Dependencies
//!
//! Foundation crates called directly: `backend-storage-large-object`
//! (`inv_*` / `close_lo_relation`), `backend-catalog-pg-largeobject`
//! (`LargeObjectExists`), `backend-catalog-aclchk-seams` (`object_ownercheck`),
//! `backend-utils-init-miscinit` (`GetUserId`), `backend-utils-misc-guc-tables`
//! (the `lo_compat_privileges` GUC), `backend-access-transam-xact-seams`
//! (`PreventCommandIfReadOnly` / `GetCurrentSubTransactionId`).
//!
//! Genuine externals cross the unit's own seams
//! ([`backend_libpq_be_fsstubs_seams`], panic until their owners land): the
//! snapshot register/unregister against `TopTransactionResourceOwner`
//! (`RegisterSnapshotOnOwner` / `UnregisterSnapshotFromOwner` — snapmgr's repo
//! port is the `NoOwner` core over the incompatible `SnapHandle` registry, not
//! the `Rc<SnapshotData>` carried by `LargeObjectDesc`), and the server-file I/O
//! of `lo_import` / `lo_export` (`import_server_file` / `export_server_file`).
//!
//! This crate installs its outward-consumed `AtEOXact_LargeObject` /
//! `AtEOSubXact_LargeObject` seams (consumed by `access/xact.c`) from
//! [`init_seams`].

use backend_catalog_aclchk_seams::object_ownercheck;
use backend_catalog_pg_largeobject::LargeObjectExists;
use backend_storage_large_object::{
    close_lo_relation, inv_close, inv_create, inv_drop, inv_open, inv_read, inv_seek, inv_tell,
    inv_truncate, inv_write,
};
use backend_utils_error::{ereport, PgError, PgResult};
use backend_utils_init_miscinit::GetUserId;
use backend_libpq_be_fsstubs_seams as seam;
use backend_utils_time_snapmgr_seams as snapmgr_seam;
use types_core::xact::SubTransactionId;
use types_core::{int64, InvalidOid, Oid};
use types_error::{
    ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_storage::large_object::{IFS_RDLOCK, IFS_WRLOCK, LargeObjectDesc};

pub mod fmgr_builtins;
pub mod state;

#[cfg(test)]
mod tests;

use crate::state::with_state;

/// `BUFSIZE` — chunk size for `lo_import` / `lo_export` transfers
/// (be-fsstubs.c:61).
const BUFSIZE: usize = 8192;

/// `INV_WRITE` (`libpq/libpq-fs.h`).
const INV_WRITE: i32 = 0x0002_0000;
/// `INV_READ` (`libpq/libpq-fs.h`).
const INV_READ: i32 = 0x0004_0000;

/// `SEEK_SET` (`<stdio.h>`).
const SEEK_SET: i32 = 0;
/// `SEEK_END` (`<stdio.h>`).
const SEEK_END: i32 = 2;

/// `MaxAllocSize` (`utils/memutils.h`) — `0x3fffffff`.
const MaxAllocSize: int64 = 0x3FFF_FFFF;

/// `LargeObjectRelationId` — `pg_largeobject` (`pg_largeobject_d.h`).
const LargeObjectRelationId: Oid = types_catalog::catalog::LARGE_OBJECT_RELATION_ID;

/// `VARHDRSZ` (`c.h`) — the varlena header size, used by the `lo_get`
/// allocation-limit guard.
const VARHDRSZ: int64 = core::mem::size_of::<i32>() as int64;

/*****************************************************************************
 *	File Interfaces for Large Objects
 *****************************************************************************/

/* ===========================================================================
 * be_lo_open (be-fsstubs.c:86-123)
 * ========================================================================= */

/// `lo_open(lobjId, mode)` — open a large object and return a (process-local)
/// FD into the cookie table.
pub fn be_lo_open(lobjId: Oid, mode: i32) -> PgResult<i32> {
    let mut lobjDesc: Box<LargeObjectDesc>;
    let fd: i32;

    if mode & INV_WRITE != 0 {
        backend_access_transam_xact_seams::prevent_command_if_read_only::call("lo_open(INV_WRITE)")?;
    }

    /*
     * Allocate a large object descriptor first.  This will also create 'fscxt'
     * if this is the first LO opened in this transaction.
     */
    fd = newLOfd();

    // lobjDesc = inv_open(lobjId, mode, fscxt);
    lobjDesc = inv_open(lobjId, mode)?;
    // lobjDesc->subid = GetCurrentSubTransactionId();
    lobjDesc.subid = backend_access_transam_xact_seams::get_current_sub_transaction_id::call();

    /*
     * We must register the snapshot in TopTransaction's resowner so that it
     * stays alive until the LO is closed rather than until the current portal
     * shuts down.
     */
    if let Some(snapshot) = lobjDesc.snapshot.take() {
        lobjDesc.snapshot = Some(snapmgr_seam::register_snapshot_on_top_owner::call(snapshot)?);
    }

    // Assert(cookies[fd] == NULL); cookies[fd] = lobjDesc;
    with_state(|s| {
        debug_assert!(!s.cookie_is_some(fd));
        s.set_cookie(fd, lobjDesc);
    });

    Ok(fd)
}

/* ===========================================================================
 * be_lo_close (be-fsstubs.c:125-142)
 * ========================================================================= */

/// `lo_close(fd)`.
pub fn be_lo_close(fd: i32) -> PgResult<i32> {
    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }

    closeLOfd(fd)?;

    Ok(0)
}

/*****************************************************************************
 *	Bare Read/Write operations --- these are not fmgr-callable!
 *
 *	We assume the large object supports byte oriented reads and seeks so
 *	that our work is easier.
 *****************************************************************************/

/* ===========================================================================
 * lo_read (be-fsstubs.c:153-179)
 * ========================================================================= */

/// `lo_read(fd, buf, len)` — read up to `buf.len()` bytes from the FD into
/// `buf`; returns the number of bytes read. (The C `(char *buf, int len)` is
/// the owned slice `buf: &mut [u8]`; `len == buf.len()`.)
pub fn lo_read(fd: i32, buf: &mut [u8]) -> PgResult<i32> {
    let status: i32;

    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }

    /*
     * Check state.  inv_read() would throw an error anyway, but we want the
     * error to be about the FD's state not the underlying privilege; it might be
     * that the privilege exists but user forgot to ask for read mode.
     */
    // lobj = cookies[fd]; if ((lobj->flags & IFS_RDLOCK) == 0) ...; status = inv_read(lobj, buf, len);
    status = with_state(|s| {
        let lobj = s.cookie_mut(fd).expect("guarded by fd_is_valid");
        if (lobj.flags & IFS_RDLOCK) == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "large object descriptor {fd} was not opened for reading"
                ))
                .into_error());
        }
        inv_read(lobj, buf)
    })?;

    Ok(status)
}

/* ===========================================================================
 * lo_write (be-fsstubs.c:181-203)
 * ========================================================================= */

/// `lo_write(fd, buf, len)` — write `buf.len()` bytes from `buf` to the FD;
/// returns the number of bytes written. (The C `(const char *buf, int len)` is
/// the owned slice `buf: &[u8]`; `len == buf.len()`.)
pub fn lo_write(fd: i32, buf: &[u8]) -> PgResult<i32> {
    let status: i32;

    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }

    /* see comment in lo_read() */
    status = with_state(|s| {
        let lobj = s.cookie_mut(fd).expect("guarded by fd_is_valid");
        if (lobj.flags & IFS_WRLOCK) == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "large object descriptor {fd} was not opened for writing"
                ))
                .into_error());
        }
        inv_write(lobj, buf)
    })?;

    Ok(status)
}

/* ===========================================================================
 * be_lo_lseek (be-fsstubs.c:205-228)
 * ========================================================================= */

/// `lo_lseek(fd, offset, whence)` — 32-bit seek with overflow guard.
pub fn be_lo_lseek(fd: i32, offset: i32, whence: i32) -> PgResult<i32> {
    let status: int64;

    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }

    status = with_state(|s| inv_seek(s.cookie_mut(fd).unwrap(), offset as int64, whence))?;

    /* guard against result overflow */
    if status != status as i32 as int64 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg(format!(
                "lo_lseek result out of range for large-object descriptor {fd}"
            ))
            .into_error());
    }

    Ok(status as i32)
}

/* ===========================================================================
 * be_lo_lseek64 (be-fsstubs.c:230-246)
 * ========================================================================= */

/// `lo_lseek64(fd, offset, whence)`.
pub fn be_lo_lseek64(fd: i32, offset: int64, whence: i32) -> PgResult<int64> {
    let status: int64;

    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }

    status = with_state(|s| inv_seek(s.cookie_mut(fd).unwrap(), offset, whence))?;

    Ok(status)
}

/* ===========================================================================
 * be_lo_creat / be_lo_create (be-fsstubs.c:248-272)
 * ========================================================================= */

/// `lo_creat()` — create an LO with a system-assigned OID.
pub fn be_lo_creat() -> PgResult<Oid> {
    let lobjId: Oid;

    backend_access_transam_xact_seams::prevent_command_if_read_only::call("lo_creat()")?;

    with_state(|s| s.set_lo_cleanup_needed(true));
    lobjId = inv_create(InvalidOid)?;

    Ok(lobjId)
}

/// `lo_create(lobjId)` — create an LO with a caller-specified OID.
pub fn be_lo_create(mut lobjId: Oid) -> PgResult<Oid> {
    backend_access_transam_xact_seams::prevent_command_if_read_only::call("lo_create()")?;

    with_state(|s| s.set_lo_cleanup_needed(true));
    lobjId = inv_create(lobjId)?;

    Ok(lobjId)
}

/* ===========================================================================
 * be_lo_tell / be_lo_tell64 (be-fsstubs.c:274-311)
 * ========================================================================= */

/// `lo_tell(fd)` — 32-bit tell with overflow guard.
pub fn be_lo_tell(fd: i32) -> PgResult<i32> {
    let offset: int64;

    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }

    offset = with_state(|s| inv_tell(s.cookie_mut(fd).unwrap()))?;

    /* guard against result overflow */
    if offset != offset as i32 as int64 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg(format!(
                "lo_tell result out of range for large-object descriptor {fd}"
            ))
            .into_error());
    }

    Ok(offset as i32)
}

/// `lo_tell64(fd)`.
pub fn be_lo_tell64(fd: i32) -> PgResult<int64> {
    let offset: int64;

    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }

    offset = with_state(|s| inv_tell(s.cookie_mut(fd).unwrap()))?;

    Ok(offset)
}

/* ===========================================================================
 * be_lo_unlink (be-fsstubs.c:313-355)
 * ========================================================================= */

/// `lo_unlink(lobjId)` — drop a large object.
pub fn be_lo_unlink(lobjId: Oid) -> PgResult<i32> {
    backend_access_transam_xact_seams::prevent_command_if_read_only::call("lo_unlink()")?;

    if !LargeObjectExists(lobjId)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("large object {lobjId} does not exist"))
            .into_error());
    }

    /*
     * Must be owner of the large object.  It would be cleaner to check this in
     * inv_drop(), but we want to throw the error before not after closing
     * relevant FDs.
     */
    if !backend_utils_misc_guc_tables::vars::lo_compat_privileges.read()
        && !object_ownercheck::call(LargeObjectRelationId, lobjId, GetUserId())?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("must be owner of large object {lobjId}"))
            .into_error());
    }

    /*
     * If there are any open LO FDs referencing that ID, close 'em.
     */
    // if (fscxt != NULL) { for (i ...) if (cookies[i] && cookies[i]->id == lobjId) closeLOfd(i); }
    let to_close: Vec<i32> = with_state(|s| {
        let mut v = Vec::new();
        if s.has_fscxt() {
            for i in 0..s.cookies_size() {
                if s.cookie_id(i) == Some(lobjId) {
                    v.push(i);
                }
            }
        }
        v
    });
    for i in to_close {
        closeLOfd(i)?;
    }

    /*
     * inv_drop does not create a need for end-of-transaction cleanup and hence
     * we don't need to set lo_cleanup_needed.
     */
    inv_drop(lobjId)
}

/*****************************************************************************
 *	Read/Write using bytea
 *****************************************************************************/

/* ===========================================================================
 * be_loread (be-fsstubs.c:361-377)
 * ========================================================================= */

/// `loread(fd, len)` — read up to `len` bytes and return them as a `bytea`
/// (here, the read bytes as a `Vec<u8>`; the `palloc`/`SET_VARSIZE` bytea
/// framing is the fmgr value-layer deferral).
pub fn be_loread(fd: i32, mut len: i32) -> PgResult<Vec<u8>> {
    let totalread: i32;

    if len < 0 {
        len = 0;
    }

    // retval = (bytea *) palloc(VARHDRSZ + len);
    let mut retval = vec![0u8; len as usize];
    // totalread = lo_read(fd, VARDATA(retval), len);
    totalread = lo_read(fd, &mut retval)?;
    // SET_VARSIZE(retval, totalread + VARHDRSZ);
    retval.truncate(totalread as usize);

    Ok(retval)
}

/* ===========================================================================
 * be_lowrite (be-fsstubs.c:379-392)
 * ========================================================================= */

/// `lowrite(fd, wbuf)` — write the bytea `wbuf` to the FD; returns the byte
/// count written.
pub fn be_lowrite(fd: i32, wbuf: &[u8]) -> PgResult<i32> {
    let bytestowrite: i32;
    let totalwritten: i32;

    backend_access_transam_xact_seams::prevent_command_if_read_only::call("lowrite()")?;

    // bytestowrite = VARSIZE_ANY_EXHDR(wbuf);
    bytestowrite = wbuf.len() as i32;
    // totalwritten = lo_write(fd, VARDATA_ANY(wbuf), bytestowrite);
    totalwritten = lo_write(fd, &wbuf[..bytestowrite as usize])?;
    Ok(totalwritten)
}

/*****************************************************************************
 *	 Import/Export of Large Object
 *****************************************************************************/

/* ===========================================================================
 * be_lo_import / be_lo_import_with_oid (be-fsstubs.c:402-421)
 * ========================================================================= */

/// `lo_import(filename)` — import a server file as an (inversion) large object.
pub fn be_lo_import(filename: &[u8]) -> PgResult<Oid> {
    lo_import_internal(filename, InvalidOid)
}

/// `lo_import_with_oid(filename, oid)` — import specifying the LO's OID.
pub fn be_lo_import_with_oid(filename: &[u8], oid: Oid) -> PgResult<Oid> {
    lo_import_internal(filename, oid)
}

/* ===========================================================================
 * lo_import_internal (be-fsstubs.c:423-479)
 * ========================================================================= */

/// The guts of `lo_import` — open the server file, create the LO, and stream
/// the file into it. The server-file open/read/close is the seam's
/// `import_server_file`; the `inv_create` / `inv_open` / `inv_write` /
/// `inv_close` are in-crate against the owned descriptor.
fn lo_import_internal(filename: &[u8], lobjOid: Oid) -> PgResult<Oid> {
    let mut lobj: Box<LargeObjectDesc>;
    let oid: Oid;

    backend_access_transam_xact_seams::prevent_command_if_read_only::call("lo_import()")?;

    /*
     * create an inversion object
     */
    with_state(|s| s.set_lo_cleanup_needed(true));
    oid = inv_create(lobjOid)?;

    /*
     * read in from the filesystem and write to the inversion object
     */
    // lobj = inv_open(oid, INV_WRITE, CurrentMemoryContext);
    lobj = inv_open(oid, INV_WRITE)?;

    // while ((nbytes = read(fd, buf, BUFSIZE)) > 0) { tmp = inv_write(lobj, buf, nbytes); }
    seam::import_server_file::call(filename, &mut |buf: &[u8]| {
        let tmp = inv_write(&mut lobj, buf)?;
        debug_assert_eq!(tmp, buf.len() as i32);
        Ok(tmp)
    })?;

    // inv_close(lobj);
    inv_close(lobj)?;

    Ok(oid)
}

/* ===========================================================================
 * be_lo_export (be-fsstubs.c:485-551)
 * ========================================================================= */

/// `lo_export(lobjId, filename)` — export a large object to a server file.
pub fn be_lo_export(lobjId: Oid, filename: &[u8]) -> PgResult<i32> {
    let mut lobj: Box<LargeObjectDesc>;

    /*
     * open the inversion object (no need to test for failure)
     */
    with_state(|s| s.set_lo_cleanup_needed(true));
    lobj = inv_open(lobjId, INV_READ)?;

    /*
     * open the file to be written to, then read from the inversion file and
     * write to the filesystem (the umask dance + transient-file create/close is
     * in the seam).
     */
    // while ((nbytes = inv_read(lobj, buf, BUFSIZE)) > 0) { write(fd, buf, nbytes); }
    seam::export_server_file::call(filename, &mut |buf: &mut [u8]| {
        debug_assert_eq!(buf.len(), BUFSIZE);
        inv_read(&mut lobj, buf)
    })?;

    // inv_close(lobj);
    inv_close(lobj)?;

    Ok(1)
}

/* ===========================================================================
 * lo_truncate_internal / be_lo_truncate / be_lo_truncate64
 * (be-fsstubs.c:557-600)
 * ========================================================================= */

/// `lo_truncate_internal(fd, len)` — the shared guts of `lo_truncate` /
/// `lo_truncate64`.
fn lo_truncate_internal(fd: i32, len: int64) -> PgResult<()> {
    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }

    /* see comment in lo_read() */
    with_state(|s| {
        let lobj = s.cookie_mut(fd).expect("guarded by fd_is_valid");
        if (lobj.flags & IFS_WRLOCK) == 0 {
            return Err(ereport(ERROR)
                .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
                .errmsg(format!(
                    "large object descriptor {fd} was not opened for writing"
                ))
                .into_error());
        }
        inv_truncate(lobj, len)
    })?;
    Ok(())
}

/// `lo_truncate(fd, len)` — 32-bit truncate.
pub fn be_lo_truncate(fd: i32, len: i32) -> PgResult<i32> {
    backend_access_transam_xact_seams::prevent_command_if_read_only::call("lo_truncate()")?;

    lo_truncate_internal(fd, len as int64)?;
    Ok(0)
}

/// `lo_truncate64(fd, len)`.
pub fn be_lo_truncate64(fd: i32, len: int64) -> PgResult<i32> {
    backend_access_transam_xact_seams::prevent_command_if_read_only::call("lo_truncate64()")?;

    lo_truncate_internal(fd, len)?;
    Ok(0)
}

/* ===========================================================================
 * AtEOXact_LargeObject (be-fsstubs.c:606-643)
 * ========================================================================= */

/// Prepares large objects for transaction commit (or abort) — close LO FDs,
/// clear the cookie array, and let inv_api clean up.
pub fn AtEOXact_LargeObject(isCommit: bool) -> PgResult<()> {
    if !with_state(|s| s.lo_cleanup_needed()) {
        return Ok(()); /* no LO operations in this xact */
    }

    /*
     * Close LO fds and clear cookies array so that LO fds are no longer good.
     * The memory context and resource owner holding them are going away at the
     * end-of-transaction anyway, but on commit, we need to close them to avoid
     * warnings about leaked resources at commit.  On abort we can skip this
     * step.
     */
    if isCommit {
        let to_close: Vec<i32> = with_state(|s| {
            (0..s.cookies_size())
                .filter(|&i| s.cookie_is_some(i))
                .collect()
        });
        for i in to_close {
            closeLOfd(i)?;
        }
    }

    /*
     * Needn't actually pfree since we're about to zap context — clear the cookie
     * array.  (In the idiomatic port the descriptors are owned by the slots, so
     * dropping the array drops any still-open descriptors on abort, which is the
     * `MemoryContextDelete(fscxt)` analogue.)
     */
    // cookies = NULL; cookies_size = 0;  +  if (fscxt) MemoryContextDelete(fscxt); fscxt = NULL;
    with_state(|s| s.clear_cookies());

    /* Give inv_api.c a chance to clean up, too */
    close_lo_relation(isCommit)?;

    with_state(|s| s.set_lo_cleanup_needed(false));

    Ok(())
}

/* ===========================================================================
 * AtEOSubXact_LargeObject (be-fsstubs.c:652-673)
 * ========================================================================= */

/// Take care of large objects at subtransaction commit/abort: reassign LOs
/// created/opened during a committing subtransaction to the parent
/// subtransaction; on abort, close them.
pub fn AtEOSubXact_LargeObject(
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) -> PgResult<()> {
    if !with_state(|s| s.has_fscxt()) {
        return Ok(()); /* no LO operations in this xact */
    }

    // for (i = 0; i < cookies_size; i++) { lo = cookies[i]; if (lo && lo->subid == mySubid) {...} }
    let cookies_size = with_state(|s| s.cookies_size());
    for i in 0..cookies_size {
        let subid = with_state(|s| s.cookie_subid(i));

        if subid == Some(mySubid) {
            if isCommit {
                with_state(|s| s.set_cookie_subid(i, parentSubid));
            } else {
                closeLOfd(i)?;
            }
        }
    }

    Ok(())
}

/*****************************************************************************
 *	Support routines for this file
 *****************************************************************************/

/* ===========================================================================
 * newLOfd (be-fsstubs.c:679-718)
 * ========================================================================= */

/// Allocate a free FD slot in the cookie table (growing it as needed).
/// (The C `fscxt` creation on first use is subsumed by the cookie array's
/// first allocation — the slots own the descriptors directly.)
fn newLOfd() -> i32 {
    with_state(|s| {
        s.set_lo_cleanup_needed(true);

        /* Try to find a free slot */
        for i in 0..s.cookies_size() {
            if !s.cookie_is_some(i) {
                return i;
            }
        }

        /* No free slot, so make the array bigger */
        // The C MemoryContextAllocZero / repalloc0_array of the cookies array is
        // performed by the process-local state (first time => 64, else doubled,
        // new slots empty).
        if s.cookies_size() <= 0 {
            s.grow_cookies(64);
            0
        } else {
            let i = s.cookies_size();
            let newsize = s.cookies_size() * 2;
            s.grow_cookies(newsize);
            i
        }
    })
}

/* ===========================================================================
 * closeLOfd (be-fsstubs.c:720-736)
 * ========================================================================= */

/// Close the LO FD `fd`: clear the cookie slot, unregister the snapshot, and
/// `inv_close` the descriptor.
fn closeLOfd(fd: i32) -> PgResult<()> {
    let mut lobj: Box<LargeObjectDesc>;

    /*
     * Make sure we do not try to free twice if this errors out for some reason.
     * Better a leak than a crash.
     */
    // lobj = cookies[fd]; cookies[fd] = NULL;
    lobj = match with_state(|s| s.take_cookie(fd)) {
        Some(l) => l,
        None => return Ok(()),
    };

    // if (lobj->snapshot) UnregisterSnapshotFromOwner(lobj->snapshot, TopTransactionResourceOwner);
    if let Some(snapshot) = lobj.snapshot.take() {
        snapmgr_seam::unregister_snapshot_from_top_owner::call(snapshot)?;
    }
    // inv_close(lobj);
    inv_close(lobj)?;

    Ok(())
}

/*****************************************************************************
 *	Wrappers oriented toward SQL callers
 *****************************************************************************/

/* ===========================================================================
 * lo_get_fragment_internal (be-fsstubs.c:745-791)
 * ========================================================================= */

/// Read `[offset, offset+nbytes)` within the LO; when `nbytes` is -1, read to
/// end. Returns the read bytes (the bytea `palloc`/`SET_VARSIZE` framing is the
/// fmgr value-layer deferral).
fn lo_get_fragment_internal(loOid: Oid, offset: int64, nbytes: i32) -> PgResult<Vec<u8>> {
    let mut loDesc: Box<LargeObjectDesc>;
    let loSize: int64;
    let result_length: int64;

    with_state(|s| s.set_lo_cleanup_needed(true));
    loDesc = inv_open(loOid, INV_READ)?;

    /*
     * Compute number of bytes we'll actually read, accommodating nbytes == -1
     * and reads beyond the end of the LO.
     */
    loSize = inv_seek(&mut loDesc, 0, SEEK_END)?;
    if loSize > offset {
        if nbytes as int64 >= 0 && nbytes as int64 <= loSize - offset {
            result_length = nbytes as int64; /* request is wholly inside LO */
        } else {
            result_length = loSize - offset; /* adjust to end of LO */
        }
    } else {
        result_length = 0; /* request is wholly outside LO */
    }

    /*
     * A result_length calculated from loSize may not fit in a size_t.  Check
     * that the size will satisfy this and subsequently-enforced size limits.
     */
    if result_length > MaxAllocSize - VARHDRSZ {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("large object read request is too large")
            .into_error());
    }

    // result = (bytea *) palloc(VARHDRSZ + result_length);
    let mut result = vec![0u8; result_length as usize];

    inv_seek(&mut loDesc, offset, SEEK_SET)?;
    // total_read = inv_read(loDesc, VARDATA(result), result_length);
    let total_read = inv_read(&mut loDesc, &mut result)?;
    debug_assert_eq!(total_read as int64, result_length);
    // SET_VARSIZE(result, result_length + VARHDRSZ);  -- result already sized.

    inv_close(loDesc)?;

    Ok(result)
}

/* ===========================================================================
 * be_lo_get / be_lo_get_fragment (be-fsstubs.c:796-826)
 * ========================================================================= */

/// `lo_get(loOid)` — read the entire LO.
pub fn be_lo_get(loOid: Oid) -> PgResult<Vec<u8>> {
    lo_get_fragment_internal(loOid, 0, -1)
}

/// `lo_get(loOid, offset, nbytes)` — read a range within the LO.
pub fn be_lo_get_fragment(loOid: Oid, offset: int64, nbytes: i32) -> PgResult<Vec<u8>> {
    if nbytes < 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("requested length cannot be negative")
            .into_error());
    }

    lo_get_fragment_internal(loOid, offset, nbytes)
}

/* ===========================================================================
 * be_lo_from_bytea (be-fsstubs.c:831-849)
 * ========================================================================= */

/// `lo_from_bytea(loOid, str)` — create an LO with initial contents `str`.
pub fn be_lo_from_bytea(mut loOid: Oid, str: &[u8]) -> PgResult<Oid> {
    let mut loDesc: Box<LargeObjectDesc>;

    backend_access_transam_xact_seams::prevent_command_if_read_only::call("lo_from_bytea()")?;

    with_state(|s| s.set_lo_cleanup_needed(true));
    loOid = inv_create(loOid)?;
    loDesc = inv_open(loOid, INV_WRITE)?;
    let written = inv_write(&mut loDesc, str)?;
    debug_assert_eq!(written, str.len() as i32);
    inv_close(loDesc)?;

    Ok(loOid)
}

/* ===========================================================================
 * be_lo_put (be-fsstubs.c:854-873)
 * ========================================================================= */

/// `lo_put(loOid, offset, str)` — update a range within the LO.
pub fn be_lo_put(loOid: Oid, offset: int64, str: &[u8]) -> PgResult<()> {
    let mut loDesc: Box<LargeObjectDesc>;

    backend_access_transam_xact_seams::prevent_command_if_read_only::call("lo_put()")?;

    with_state(|s| s.set_lo_cleanup_needed(true));
    loDesc = inv_open(loOid, INV_WRITE)?;
    inv_seek(&mut loDesc, offset, SEEK_SET)?;
    let written = inv_write(&mut loDesc, str)?;
    debug_assert_eq!(written, str.len() as i32);
    inv_close(loDesc)?;

    Ok(())
}

/* ===========================================================================
 * Shared FD-validation helpers (the
 * `fd < 0 || fd >= cookies_size || cookies[fd] == NULL` guard +
 * `errmsg("invalid large-object descriptor: %d", fd)` raiser).
 * ========================================================================= */

/// `fd >= 0 && fd < cookies_size && cookies[fd] != NULL`.
fn fd_is_valid(fd: i32) -> bool {
    with_state(|s| s.cookie_is_some(fd))
}

/// `ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("invalid
/// large-object descriptor: %d", fd)))`.
fn invalid_descriptor(fd: i32) -> PgError {
    ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_OBJECT)
        .errmsg(format!("invalid large-object descriptor: {fd}"))
        .into_error()
}

/* ===========================================================================
 * init_seams — install the outward transaction-end hooks consumed by xact.c.
 * ========================================================================= */

/// Install the `AtEOXact_LargeObject` / `AtEOSubXact_LargeObject` seams, and
/// register the SQL-callable `lo_*` fmgr builtins (C: `fmgr_builtins[]` rows).
pub fn init_seams() {
    backend_libpq_be_fsstubs_seams::at_eoxact_large_object::set(AtEOXact_LargeObject);
    backend_libpq_be_fsstubs_seams::at_eosubxact_large_object::set(AtEOSubXact_LargeObject);
    crate::fmgr_builtins::register_be_fsstubs_builtins();
}
