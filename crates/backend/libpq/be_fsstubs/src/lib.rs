// be-fsstubs.c: SQL-callable lo_* interface over the per-backend FD table.
#![allow(non_snake_case, non_upper_case_globals)]

use core::cell::RefCell;
use core::mem::ManuallyDrop;

use datum::Varlena;
use elog::ereport;
use large_object::{
    inv_close, inv_create, inv_drop, inv_open, inv_read, inv_seek, inv_tell, inv_truncate,
    inv_write, LargeObjectDesc, INV_READ, INV_WRITE, SEEK_END, SEEK_SET,
};
use mcx::{Mcx, PgVec};
use types_core::{int64, InvalidOid, Oid, SubTransactionId};
use types_error::{
    PgResult, ERRCODE_INSUFFICIENT_PRIVILEGE, ERRCODE_INVALID_PARAMETER_VALUE,
    ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE, ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE,
    ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERRCODE_UNDEFINED_OBJECT, ERROR,
};
use types_storage::large_object::{IFS_RDLOCK, IFS_WRLOCK};

pub mod fmgr_builtins;

const MaxAllocSize: int64 = 0x3FFF_FFFF;
const VARHDRSZ: int64 = 4;

struct FsState {
    // C's cookies[] in fscxt: the slots own the descriptors.
    cookies: Vec<Option<LargeObjectDesc>>,
    lo_cleanup_needed: bool,
}

thread_local! {
    // ManuallyDrop keeps the TLS payload dtor-free (backend-lifetime in C too);
    // AtEOXact_LargeObject empties it at every transaction end.
    static STATE: RefCell<ManuallyDrop<FsState>> = const {
        RefCell::new(ManuallyDrop::new(FsState {
            cookies: Vec::new(),
            lo_cleanup_needed: false,
        }))
    };
}

fn with_state<R>(f: impl FnOnce(&mut FsState) -> R) -> R {
    STATE.with(|s| f(&mut s.borrow_mut()))
}

#[cold]
fn invalid_descriptor(fd: i32) -> Box<types_error::PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_UNDEFINED_OBJECT)
        .errmsg(format!("invalid large-object descriptor: {fd}"))
        .into_error()
        .into()
}

fn fd_is_valid(fd: i32) -> bool {
    fd >= 0
        && with_state(|s| {
            (fd as usize) < s.cookies.len() && s.cookies[fd as usize].is_some()
        })
}

fn newLOfd() -> i32 {
    with_state(|s| {
        s.lo_cleanup_needed = true;
        for (i, c) in s.cookies.iter().enumerate() {
            if c.is_none() {
                return i as i32;
            }
        }
        // No free slot: first allocation is 64 slots, else double (C's
        // MemoryContextAllocZero / repalloc0_array growth).
        if s.cookies.is_empty() {
            s.cookies.resize_with(64, || None);
            0
        } else {
            let i = s.cookies.len();
            s.cookies.resize_with(i * 2, || None);
            i as i32
        }
    })
}

fn closeLOfd(fd: i32) -> PgResult<()> {
    // Clear the slot first so an error can't double-free (C: better a leak
    // than a crash).
    let mut lobj = match with_state(|s| s.cookies[fd as usize].take()) {
        Some(l) => l,
        None => return Ok(()),
    };
    if let Some(snapshot) = lobj.snapshot.take() {
        snapmgr::UnregisterSnapshotFromOwner(
            &snapshot,
            resowner_seams::top_transaction_resource_owner::call(),
        );
    }
    inv_close(lobj)
}

pub fn be_lo_open<'mcx>(mcx: Mcx<'mcx>, lobjId: Oid, mode: i32) -> PgResult<i32> {
    if mode & INV_WRITE != 0 {
        xact::PreventCommandIfReadOnly("lo_open(INV_WRITE)")?;
    }

    let fd = newLOfd();

    let mut lobjDesc = inv_open(mcx, lobjId, mode)?;
    lobjDesc.subid = xact::GetCurrentSubTransactionId();

    // Register the snapshot in TopTransaction's resowner so it stays alive
    // until the LO is closed rather than until the current portal shuts down.
    if let Some(snapshot) = lobjDesc.snapshot.take() {
        lobjDesc.snapshot = Some(snapmgr::RegisterSnapshotOnOwner(
            &snapshot,
            resowner_seams::top_transaction_resource_owner::call(),
        )?);
    }

    with_state(|s| {
        debug_assert!(s.cookies[fd as usize].is_none());
        s.cookies[fd as usize] = Some(lobjDesc);
    });

    Ok(fd)
}

pub fn be_lo_close(fd: i32) -> PgResult<i32> {
    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }
    closeLOfd(fd)?;
    Ok(0)
}

#[cold]
fn not_opened(fd: i32, what: &str) -> Box<types_error::PgError> {
    ereport(ERROR)
        .errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE)
        .errmsg(format!("large object descriptor {fd} was not opened for {what}"))
        .into_error()
        .into()
}

// Bare (non-fmgr) read/write ops, shared with the fastpath protocol callers.
pub fn lo_read<'mcx>(mcx: Mcx<'mcx>, fd: i32, buf: &mut [u8]) -> PgResult<i32> {
    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }
    // Check state first so the error is about the FD, not the privilege.
    with_state(|s| {
        let lobj = s.cookies[fd as usize].as_mut().expect("fd_is_valid");
        if (lobj.flags & IFS_RDLOCK) == 0 {
            return Err(not_opened(fd, "reading"));
        }
        inv_read(mcx, lobj, buf)
    })
}

pub fn lo_write<'mcx>(mcx: Mcx<'mcx>, fd: i32, buf: &[u8]) -> PgResult<i32> {
    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }
    with_state(|s| {
        let lobj = s.cookies[fd as usize].as_mut().expect("fd_is_valid");
        if (lobj.flags & IFS_WRLOCK) == 0 {
            return Err(not_opened(fd, "writing"));
        }
        inv_write(mcx, lobj, buf)
    })
}

pub fn be_lo_lseek<'mcx>(mcx: Mcx<'mcx>, fd: i32, offset: i32, whence: i32) -> PgResult<i32> {
    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }
    let status = with_state(|s| {
        inv_seek(mcx, s.cookies[fd as usize].as_mut().expect("fd_is_valid"), offset as int64, whence)
    })?;
    if status != status as i32 as int64 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg(format!(
                "lo_lseek result out of range for large-object descriptor {fd}"
            ))
            .into_error()
            .into());
    }
    Ok(status as i32)
}

pub fn be_lo_lseek64<'mcx>(mcx: Mcx<'mcx>, fd: i32, offset: int64, whence: i32) -> PgResult<int64> {
    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }
    with_state(|s| {
        inv_seek(mcx, s.cookies[fd as usize].as_mut().expect("fd_is_valid"), offset, whence)
    })
}

pub fn be_lo_creat<'mcx>(mcx: Mcx<'mcx>) -> PgResult<Oid> {
    xact::PreventCommandIfReadOnly("lo_creat()")?;
    with_state(|s| s.lo_cleanup_needed = true);
    inv_create(mcx, InvalidOid)
}

pub fn be_lo_create<'mcx>(mcx: Mcx<'mcx>, lobjId: Oid) -> PgResult<Oid> {
    xact::PreventCommandIfReadOnly("lo_create()")?;
    with_state(|s| s.lo_cleanup_needed = true);
    inv_create(mcx, lobjId)
}

pub fn be_lo_tell(fd: i32) -> PgResult<i32> {
    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }
    let offset = with_state(|s| inv_tell(s.cookies[fd as usize].as_ref().expect("fd_is_valid")))?;
    if offset != offset as i32 as int64 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE)
            .errmsg(format!(
                "lo_tell result out of range for large-object descriptor {fd}"
            ))
            .into_error()
            .into());
    }
    Ok(offset as i32)
}

pub fn be_lo_tell64(fd: i32) -> PgResult<int64> {
    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }
    with_state(|s| inv_tell(s.cookies[fd as usize].as_ref().expect("fd_is_valid")))
}

pub fn be_lo_unlink<'mcx>(mcx: Mcx<'mcx>, lobjId: Oid) -> PgResult<i32> {
    xact::PreventCommandIfReadOnly("lo_unlink()")?;

    if !pg_largeobject::LargeObjectExists(mcx, lobjId)? {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("large object {lobjId} does not exist"))
            .into_error()
            .into());
    }

    // Must be owner; checked here rather than in inv_drop so the error comes
    // before closing relevant FDs.
    if !guc_tables::vars::lo_compat_privileges.read()
        && !aclchk::object_ownercheck_lo(mcx, lobjId, miscinit::GetUserId())?
    {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INSUFFICIENT_PRIVILEGE)
            .errmsg(format!("must be owner of large object {lobjId}"))
            .into_error()
            .into());
    }

    // If there are any open LO FDs referencing that ID, close 'em.
    let to_close: Vec<i32> = with_state(|s| {
        s.cookies
            .iter()
            .enumerate()
            .filter(|(_, c)| c.as_ref().is_some_and(|l| l.id == lobjId))
            .map(|(i, _)| i as i32)
            .collect()
    });
    for i in to_close {
        closeLOfd(i)?;
    }

    // inv_drop creates no need for end-of-transaction cleanup.
    inv_drop(mcx, lobjId)
}

pub fn be_loread<'mcx>(mcx: Mcx<'mcx>, fd: i32, mut len: i32) -> PgResult<Varlena<'mcx>> {
    if len < 0 {
        len = 0;
    }
    let mut image: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    image
        .try_reserve_exact(VARHDRSZ as usize + len as usize)
        .map_err(|_| mcx.oom(VARHDRSZ as usize + len as usize))?;
    image.resize(VARHDRSZ as usize + len as usize, 0);
    let totalread = lo_read(mcx, fd, &mut image[VARHDRSZ as usize..])?;
    image.truncate(VARHDRSZ as usize + totalread as usize);
    Ok(Varlena::from_image(image))
}

pub fn be_lowrite<'mcx>(mcx: Mcx<'mcx>, fd: i32, wbuf: &[u8]) -> PgResult<i32> {
    xact::PreventCommandIfReadOnly("lowrite()")?;
    lo_write(mcx, fd, wbuf)
}

// BUFSIZE (be-fsstubs.c).
const BUFSIZE: usize = 8192;

fn to_fnamebuf(filename: &[u8]) -> String {
    // text_to_cstring_buffer's encoding (server encoding bytes, NUL-terminated
    // at MAXPGPATH); paths are opaque bytes to the OS so lossless round-trip
    // matters more than utf8 validity, but regress fixtures are ASCII.
    String::from_utf8_lossy(filename).into_owned()
}

fn lo_import_internal<'mcx>(mcx: Mcx<'mcx>, filename: &[u8], lobjOid: Oid) -> PgResult<Oid> {
    xact::PreventCommandIfReadOnly("lo_import()")?;

    let fnamebuf = to_fnamebuf(filename);

    let fd = fd::desc::OpenTransientFile(&fnamebuf, libc::O_RDONLY)?;
    if fd < 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(elog::errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not open server file \"{fnamebuf}\": %m"))
            .into_error()
            .into());
    }

    with_state(|s| s.lo_cleanup_needed = true);
    let oid = inv_create(mcx, lobjOid)?;

    let result = (|| -> PgResult<()> {
        let mut lobj = inv_open(mcx, oid, INV_WRITE)?;
        let mut buf = [0u8; BUFSIZE];
        loop {
            // SAFETY: buf is a live BUFSIZE-byte buffer; fd is the transient
            // file just opened above.
            let nbytes = unsafe { libc::read(fd, buf.as_mut_ptr().cast(), BUFSIZE) };
            if nbytes < 0 {
                return Err(ereport(ERROR)
                    .with_saved_errno(elog::errno::current_errno())
                    .errcode_for_file_access()
                    .errmsg(format!("could not read server file \"{fnamebuf}\": %m"))
                    .into_error()
                    .into());
            }
            if nbytes == 0 {
                break;
            }
            let written = inv_write(mcx, &mut lobj, &buf[..nbytes as usize])?;
            debug_assert_eq!(written as usize, nbytes as usize);
        }
        inv_close(lobj)
    })();

    if fd::desc::CloseTransientFile(fd) != 0 {
        result?;
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{fnamebuf}\": %m"))
            .into_error()
            .into());
    }
    result?;

    Ok(oid)
}

pub fn be_lo_import<'mcx>(mcx: Mcx<'mcx>, filename: &[u8]) -> PgResult<Oid> {
    lo_import_internal(mcx, filename, InvalidOid)
}

pub fn be_lo_import_with_oid<'mcx>(mcx: Mcx<'mcx>, filename: &[u8], oid: Oid) -> PgResult<Oid> {
    lo_import_internal(mcx, filename, oid)
}

pub fn be_lo_export<'mcx>(mcx: Mcx<'mcx>, lobjId: Oid, filename: &[u8]) -> PgResult<i32> {
    with_state(|s| s.lo_cleanup_needed = true);
    let mut lobj = inv_open(mcx, lobjId, INV_READ)?;

    let fnamebuf = to_fnamebuf(filename);

    // C reduces the backend's normal 077 umask to 022 for the duration of the
    // open so exported files aren't world-writable; restored via a guard so
    // an early return still resets it.
    struct UmaskGuard(#[allow(dead_code)] libc::mode_t);
    impl Drop for UmaskGuard {
        fn drop(&mut self) {
            // SAFETY: process-wide umask restore, single-threaded backend.
            // wasm32: no umask on WASI (files carry no mode bits) — no-op.
            #[cfg(not(target_family = "wasm"))]
            unsafe {
                libc::umask(self.0);
            }
        }
    }
    // SAFETY: process-wide umask swap, single-threaded backend.
    #[cfg(not(target_family = "wasm"))]
    let oumask = unsafe { libc::umask(libc::S_IWGRP | libc::S_IWOTH) };
    #[cfg(target_family = "wasm")]
    let oumask = 0;
    let _restore = UmaskGuard(oumask);
    let fd = fd::desc::OpenTransientFilePerm(
        &fnamebuf,
        libc::O_CREAT | libc::O_WRONLY | libc::O_TRUNC,
        (libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IROTH) as u32,
    )?;
    drop(_restore);
    if fd < 0 {
        return Err(ereport(ERROR)
            .with_saved_errno(elog::errno::current_errno())
            .errcode_for_file_access()
            .errmsg(format!("could not create server file \"{fnamebuf}\": %m"))
            .into_error()
            .into());
    }

    let result = (|| -> PgResult<()> {
        let mut buf = [0u8; BUFSIZE];
        loop {
            let nbytes = inv_read(mcx, &mut lobj, &mut buf)?;
            if nbytes <= 0 {
                break;
            }
            // SAFETY: buf's first nbytes bytes were just filled by inv_read.
            let written =
                unsafe { libc::write(fd, buf.as_ptr().cast(), nbytes as usize) };
            if written != nbytes as isize {
                return Err(ereport(ERROR)
                    .with_saved_errno(elog::errno::current_errno())
                    .errcode_for_file_access()
                    .errmsg(format!("could not write server file \"{fnamebuf}\": %m"))
                    .into_error()
                    .into());
            }
        }
        Ok(())
    })();

    if fd::desc::CloseTransientFile(fd) != 0 {
        result?;
        return Err(ereport(ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not close file \"{fnamebuf}\": %m"))
            .into_error()
            .into());
    }
    result?;

    inv_close(lobj)?;

    Ok(1)
}

fn lo_truncate_internal<'mcx>(mcx: Mcx<'mcx>, fd: i32, len: int64) -> PgResult<()> {
    if !fd_is_valid(fd) {
        return Err(invalid_descriptor(fd));
    }
    with_state(|s| {
        let lobj = s.cookies[fd as usize].as_mut().expect("fd_is_valid");
        if (lobj.flags & IFS_WRLOCK) == 0 {
            return Err(not_opened(fd, "writing"));
        }
        inv_truncate(mcx, lobj, len)
    })
}

pub fn be_lo_truncate<'mcx>(mcx: Mcx<'mcx>, fd: i32, len: i32) -> PgResult<i32> {
    xact::PreventCommandIfReadOnly("lo_truncate()")?;
    lo_truncate_internal(mcx, fd, len as int64)?;
    Ok(0)
}

pub fn be_lo_truncate64<'mcx>(mcx: Mcx<'mcx>, fd: i32, len: int64) -> PgResult<i32> {
    xact::PreventCommandIfReadOnly("lo_truncate64()")?;
    lo_truncate_internal(mcx, fd, len)?;
    Ok(0)
}

pub fn AtEOXact_LargeObject(isCommit: bool) -> PgResult<()> {
    if !with_state(|s| s.lo_cleanup_needed) {
        return Ok(());
    }

    // On commit close the FDs to avoid leaked-resource warnings; on abort the
    // clear below is the MemoryContextDelete(fscxt) analogue.
    if isCommit {
        let open: Vec<i32> = with_state(|s| {
            (0..s.cookies.len() as i32)
                .filter(|&i| s.cookies[i as usize].is_some())
                .collect()
        });
        for i in open {
            closeLOfd(i)?;
        }
    }

    with_state(|s| s.cookies = Vec::new());

    large_object::close_lo_relation(isCommit)?;

    with_state(|s| s.lo_cleanup_needed = false);

    Ok(())
}

pub fn AtEOSubXact_LargeObject(
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) -> PgResult<()> {
    if with_state(|s| s.cookies.is_empty()) {
        return Ok(()); // no LO operations in this xact
    }

    let n = with_state(|s| s.cookies.len() as i32);
    for i in 0..n {
        let subid = with_state(|s| s.cookies[i as usize].as_ref().map(|l| l.subid));
        if subid == Some(mySubid) {
            if isCommit {
                with_state(|s| {
                    s.cookies[i as usize].as_mut().expect("checked above").subid = parentSubid;
                });
            } else {
                closeLOfd(i)?;
            }
        }
    }

    Ok(())
}

fn lo_get_fragment_internal<'mcx>(
    mcx: Mcx<'mcx>,
    loOid: Oid,
    offset: int64,
    nbytes: i32,
) -> PgResult<Varlena<'mcx>> {
    with_state(|s| s.lo_cleanup_needed = true);
    let mut loDesc = inv_open(mcx, loOid, INV_READ)?;

    // Compute the byte count actually read, accommodating nbytes == -1 and
    // reads beyond the end of the LO.
    let loSize = inv_seek(mcx, &mut loDesc, 0, SEEK_END)?;
    let result_length: int64 = if loSize > offset {
        if nbytes as int64 >= 0 && (nbytes as int64) <= loSize - offset {
            nbytes as int64
        } else {
            loSize - offset
        }
    } else {
        0
    };

    if result_length > MaxAllocSize - VARHDRSZ {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg("large object read request is too large")
            .into_error()
            .into());
    }

    let total = VARHDRSZ as usize + result_length as usize;
    let mut image: PgVec<'mcx, u8> = PgVec::new_in(mcx);
    image.try_reserve_exact(total).map_err(|_| mcx.oom(total))?;
    image.resize(total, 0);

    inv_seek(mcx, &mut loDesc, offset, SEEK_SET)?;
    let total_read = inv_read(mcx, &mut loDesc, &mut image[VARHDRSZ as usize..])?;
    debug_assert_eq!(total_read as int64, result_length);

    inv_close(loDesc)?;

    Ok(Varlena::from_image(image))
}

pub fn be_lo_get<'mcx>(mcx: Mcx<'mcx>, loOid: Oid) -> PgResult<Varlena<'mcx>> {
    lo_get_fragment_internal(mcx, loOid, 0, -1)
}

pub fn be_lo_get_fragment<'mcx>(
    mcx: Mcx<'mcx>,
    loOid: Oid,
    offset: int64,
    nbytes: i32,
) -> PgResult<Varlena<'mcx>> {
    if nbytes < 0 {
        return Err(ereport(ERROR)
            .errcode(ERRCODE_INVALID_PARAMETER_VALUE)
            .errmsg("requested length cannot be negative")
            .into_error()
            .into());
    }
    lo_get_fragment_internal(mcx, loOid, offset, nbytes)
}

pub fn be_lo_from_bytea<'mcx>(mcx: Mcx<'mcx>, loOid: Oid, data: &[u8]) -> PgResult<Oid> {
    xact::PreventCommandIfReadOnly("lo_from_bytea()")?;

    with_state(|s| s.lo_cleanup_needed = true);
    let loOid = inv_create(mcx, loOid)?;
    let mut loDesc = inv_open(mcx, loOid, INV_WRITE)?;
    let written = inv_write(mcx, &mut loDesc, data)?;
    debug_assert_eq!(written, data.len() as i32);
    inv_close(loDesc)?;

    Ok(loOid)
}

pub fn be_lo_put<'mcx>(mcx: Mcx<'mcx>, loOid: Oid, offset: int64, data: &[u8]) -> PgResult<()> {
    xact::PreventCommandIfReadOnly("lo_put()")?;

    with_state(|s| s.lo_cleanup_needed = true);
    let mut loDesc = inv_open(mcx, loOid, INV_WRITE)?;
    inv_seek(mcx, &mut loDesc, offset, SEEK_SET)?;
    let written = inv_write(mcx, &mut loDesc, data)?;
    debug_assert_eq!(written, data.len() as i32);
    inv_close(loDesc)?;

    Ok(())
}

pub fn init_seams() {
    be_fsstubs_seams::at_eoxact_large_object::set(AtEOXact_LargeObject);
    be_fsstubs_seams::at_eosubxact_large_object::set(AtEOSubXact_LargeObject);
}
