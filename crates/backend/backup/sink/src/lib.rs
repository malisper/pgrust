//! Port of PostgreSQL's `basebackup_sink` (`src/backend/backup/basebackup_sink.c`
//! and `src/include/backup/basebackup_sink.h`).
//!
//! A base backup produces one archive per tablespace directory, plus a backup
//! manifest unless that feature has been disabled. A [`Bbsink`] is an object to
//! which those archives, and the manifest if present, can be sent. In practice
//! there is a *chain* of sinks rather than just one, with callbacks being
//! forwarded from one to the next, possibly with modification. Each sink is
//! responsible for a single task, e.g. command progress reporting, throttling,
//! or communication with the client.
//!
//! The C code uses a vtable of function pointers (`bbsink_ops`) plus a struct
//! of shared pointers (`bbs_buffer`, `bbs_next`, `bbs_state`). This port
//! replaces the vtable with the [`BbsinkOps`] trait and replaces the shared raw
//! buffer/next pointers with owned values: the buffer is an owned
//! [`PgVec<u8>`](::mcx::PgVec) charged to the surrounding memory context (the
//! C `palloc` analog), and a forwarding sink shares its successor's buffer by
//! delegating buffer access to the next sink in the chain. The shared backup
//! `state` is threaded explicitly through the dispatch functions rather than
//! stored as a back-pointer, preserving the "single shared state" behavior
//! while keeping borrow checking sound.
//!
//! The working buffer (`bbs_buffer`) is allocated by a concrete sink's
//! `begin_backup` callback with `palloc`, charging it to the surrounding
//! `MemoryContext`. Allocation is fallible: [`Bbsink::set_buffer`] takes an
//! [`Mcx`] handle and returns a [`PgResult`], surfacing OOM as a recoverable
//! error rather than aborting.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::boxed::Box;
use alloc::string::String;

use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::primitive::{Oid, Size, TimeLineID, XLogRecPtr, BLCKSZ};
use ::types_error::PgResult;

/// Information about a tablespace.
///
/// Mirrors C `tablespaceinfo` (`src/include/backup/basebackup.h`). In some
/// usages, `path` can be `None` to denote the `PGDATA` directory itself.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TablespaceInfo {
    /// Tablespace's OID.
    pub oid: Oid,
    /// Full path to tablespace's directory (`None` denotes `PGDATA` itself).
    pub path: Option<String>,
    /// Relative path if it's within `PGDATA`, else `None`.
    pub rpath: Option<String>,
    /// Total size as sent; `None` if not known (C uses `-1`).
    pub size: Option<i64>,
}

/// Overall backup state shared by all bbsink objects for a backup
/// (C `struct bbsink_state`).
///
/// Before calling [`bbsink_begin_backup`], the caller must initialize a
/// `BbsinkState` which will last for the lifetime of the backup, and must
/// thereafter update it as required before each new call to a bbsink method.
/// The bbsink retains a reference to the state object and consults it to
/// understand the progress of the backup.
#[derive(Clone, Debug, Default)]
pub struct BbsinkState {
    /// List of tablespaces (C `List *tablespaces`). Must be set before calling
    /// [`bbsink_begin_backup`] and must not be modified thereafter.
    pub tablespaces: alloc::vec::Vec<TablespaceInfo>,
    /// Index of the current tablespace within `tablespaces`.
    pub tablespace_num: i32,
    /// Number of bytes read so far from `PGDATA`.
    pub bytes_done: u64,
    /// Total number of bytes estimated to be present in `PGDATA`, if estimated.
    pub bytes_total: u64,
    /// `true` iff a proper estimate has been stored into `bytes_total`.
    pub bytes_total_is_valid: bool,
    /// Point in the WAL stream at which the backup began. Must be set before
    /// calling [`bbsink_begin_backup`] and must not be modified thereafter.
    pub startptr: XLogRecPtr,
    /// Timeline at which the backup began. Same modification rules as
    /// `startptr`.
    pub starttli: TimeLineID,
}

/// Callbacks for a base backup sink (C `struct bbsink_ops`).
///
/// This trait replaces the C vtable of function pointers. All of these
/// callbacks are required. If a particular callback just needs to forward the
/// call to the next sink, call the matching `bbsink_forward_*` free function
/// from the implementation (see [`bbsink_forward_begin_backup`] and friends).
///
/// Callers should always invoke these callbacks via the `bbsink_*` free
/// functions rather than calling them directly, because those functions perform
/// PostgreSQL's `Assert` checks first.
pub trait BbsinkOps<'mcx> {
    /// Invoked just once, at the very start of the backup. It must set the
    /// sink's buffer to a chunk of storage where at least `buffer_length` bytes
    /// of data can be written, via [`Bbsink::set_buffer`] (or, for a forwarding
    /// sink, [`bbsink_forward_begin_backup`]).
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()>;

    /// For each archive transmitted to a bbsink, there will be one call to
    /// `begin_archive`, some number of calls to `archive_contents`, and then
    /// one call to `end_archive`.
    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()>;

    /// Process `len` bytes of an archive's contents. Before this is invoked,
    /// the caller should copy `len` bytes into the buffer, but no more than the
    /// buffer length.
    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()>;

    /// Finish an archive.
    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()>;

    /// Begin the backup manifest.
    fn begin_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()>;

    /// Process `len` bytes of the manifest. Same buffer rules as
    /// [`BbsinkOps::archive_contents`].
    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()>;

    /// Finish the backup manifest.
    fn end_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()>;

    /// Invoked just once, after all archives and the manifest have been sent.
    fn end_backup(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        endptr: XLogRecPtr,
        endtli: TimeLineID,
    ) -> PgResult<()>;

    /// If a backup is aborted by an error, this callback is invoked before the
    /// bbsink object is destroyed, so it can release resources that would not
    /// be released automatically. If no error occurs, this callback is invoked
    /// after `end_backup`.
    fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()>;
}

/// Common data for any type of basebackup sink (C `struct bbsink`).
///
/// The C struct carries raw pointers `bbs_ops`, `bbs_buffer`, `bbs_next`, and
/// `bbs_state`. Here:
///
/// * `ops` owns the callback table as a boxed trait object.
/// * `buffer` is an owned byte buffer (the C `bbs_buffer`/`bbs_buffer_length`),
///   charged to the surrounding memory context via its [`Mcx`] handle (the
///   `palloc` accounting analog).
/// * `next` owns the successor sink in the chain (the C `bbs_next`).
/// * The shared backup `state` is threaded explicitly through the dispatch
///   functions rather than stored as a back-pointer.
///
/// A forwarding sink shares its successor's buffer: its own `buffer` stays
/// empty and `shares_next_buffer` is set, so buffer queries delegate to `next`.
pub struct Bbsink<'mcx> {
    ops: Box<dyn BbsinkOps<'mcx> + 'mcx>,
    buffer: PgVec<'mcx, u8>,
    /// Logical buffer length. For an owning sink this equals `buffer.len()`;
    /// during `begin_backup` it is set before the buffer is allocated, matching
    /// the C code which sets `bbs_buffer_length` before `begin_backup` fills in
    /// `bbs_buffer`.
    buffer_length: Size,
    /// When `true`, this sink forwards buffer access to `next` (it shares the
    /// successor's buffer instead of owning one).
    shares_next_buffer: bool,
    next: Option<Box<Bbsink<'mcx>>>,
}

impl<'mcx> Bbsink<'mcx> {
    /// Create a new sink with the given callbacks and optional successor sink.
    ///
    /// `mcx` is the surrounding memory context handle into which the sink's
    /// working buffer will be allocated by [`Bbsink::set_buffer`] (the C
    /// context that holds `palloc`-ed `bbs_buffer`).
    pub fn new(
        mcx: Mcx<'mcx>,
        ops: Box<dyn BbsinkOps<'mcx> + 'mcx>,
        next: Option<Box<Bbsink<'mcx>>>,
    ) -> Self {
        Self {
            ops,
            buffer: PgVec::new_in(mcx),
            buffer_length: 0,
            shares_next_buffer: false,
            next,
        }
    }

    /// The successor sink, if any.
    pub fn next(&self) -> Option<&Bbsink<'mcx>> {
        self.next.as_deref()
    }

    /// The successor sink, mutably, if any.
    pub fn next_mut(&mut self) -> Option<&mut Bbsink<'mcx>> {
        self.next.as_deref_mut()
    }

    /// Whether a buffer has been installed for this sink.
    pub fn has_buffer(&self) -> bool {
        if self.shares_next_buffer {
            self.next.as_deref().is_some_and(Bbsink::has_buffer)
        } else {
            !self.buffer.is_empty()
        }
    }

    /// The allocated length of the sink's buffer (C `bbs_buffer_length`).
    ///
    /// For a forwarding sink that shares its successor's buffer, this reports
    /// the successor's buffer length, mirroring how the C code copies
    /// `bbs_buffer`/`bbs_buffer_length` from the next sink.
    pub fn buffer_length(&self) -> Size {
        if self.shares_next_buffer {
            self.next.as_deref().map(Bbsink::buffer_length).unwrap_or(0)
        } else {
            self.buffer_length
        }
    }

    /// Read-only access to the first `len` bytes of the sink's buffer.
    ///
    /// Panics if `len` exceeds the buffer length or if no buffer is installed,
    /// matching the C contract that callers stay within `bbs_buffer_length`.
    pub fn buffer_slice(&self, len: Size) -> &[u8] {
        if self.shares_next_buffer {
            return self
                .next
                .as_deref()
                .expect("forwarding sink must have next sink")
                .buffer_slice(len);
        }
        assert!(len <= self.buffer.len(), "buffer length exceeded");
        assert!(!self.buffer.is_empty(), "bbsink buffer must be set");
        &self.buffer[..len]
    }

    /// Mutable access to the first `len` bytes of the sink's buffer.
    ///
    /// Panics under the same conditions as [`Bbsink::buffer_slice`].
    pub fn buffer_slice_mut(&mut self, len: Size) -> &mut [u8] {
        if self.shares_next_buffer {
            return self
                .next
                .as_deref_mut()
                .expect("forwarding sink must have next sink")
                .buffer_slice_mut(len);
        }
        assert!(len <= self.buffer.len(), "buffer length exceeded");
        assert!(!self.buffer.is_empty(), "bbsink buffer must be set");
        &mut self.buffer[..len]
    }

    /// Mutable access to the whole installed buffer, if any.
    pub fn buffer_mut(&mut self) -> Option<&mut [u8]> {
        if self.shares_next_buffer {
            return self.next.as_deref_mut().and_then(Bbsink::buffer_mut);
        }
        if self.buffer.is_empty() {
            None
        } else {
            Some(self.buffer.as_mut_slice())
        }
    }

    /// Install a zeroed buffer of `len` bytes for this sink (C `palloc0`).
    ///
    /// This is the idiomatic equivalent of a `begin_backup` callback allocating
    /// `bbs_buffer`. The allocation is fallible: it enforces `palloc`'s
    /// `MaxAllocSize` gate and uses fallible reservation, returning a
    /// [`PgError`](::types_error::PgError) instead of aborting on OOM. The bytes
    /// are charged to `mcx` (the surrounding context).
    ///
    /// Any previously installed buffer is replaced; its deallocation is charged
    /// back to its context through its [`Mcx`] handle.
    pub fn set_buffer(&mut self, mcx: Mcx<'mcx>, len: Size) -> PgResult<()> {
        let mut buffer = vec_with_capacity_in::<u8>(mcx, len)?;
        // `palloc0` semantics: `len` zero bytes. Capacity is reserved above, so
        // this does not reallocate.
        buffer.resize(len, 0);
        self.buffer = buffer;
        self.buffer_length = len;
        self.shares_next_buffer = false;
        Ok(())
    }

    /// Release this sink's buffer.
    pub fn clear_buffer(&mut self, mcx: Mcx<'mcx>) {
        self.buffer = PgVec::new_in(mcx);
        self.buffer_length = 0;
        self.shares_next_buffer = false;
    }
}

// --- Dispatch functions: the `bbsink_*` inline helpers from the C header. ---

/// Run a callback on `sink` while its boxed ops are temporarily moved out, so
/// the closure can borrow the sink's buffer/next fields mutably without
/// aliasing the ops box. The ops box is always restored, even on unwind.
fn dispatch<'mcx, R>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    f: impl FnOnce(&mut (dyn BbsinkOps<'mcx> + 'mcx), &mut Bbsink<'mcx>, &mut BbsinkState) -> R,
) -> R {
    struct OpsGuard<'a, 'mcx> {
        sink: &'a mut Bbsink<'mcx>,
        ops: Box<dyn BbsinkOps<'mcx> + 'mcx>,
    }
    impl<'mcx> Drop for OpsGuard<'_, 'mcx> {
        fn drop(&mut self) {
            self.sink.ops = core::mem::replace(
                &mut self.ops,
                Box::new(NoopOps) as Box<dyn BbsinkOps<'mcx> + 'mcx>,
            );
        }
    }
    let placeholder: Box<dyn BbsinkOps<'mcx> + 'mcx> = Box::new(NoopOps);
    let ops = core::mem::replace(&mut sink.ops, placeholder);
    let mut guard = OpsGuard { sink, ops };
    f(guard.ops.as_mut(), guard.sink, state)
}

/// Internal placeholder ops used only while a real ops box is temporarily moved
/// out during dispatch. It is never actually invoked.
struct NoopOps;

impl<'mcx> BbsinkOps<'mcx> for NoopOps {
    fn begin_backup(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn begin_archive(
        &mut self,
        _: &mut Bbsink<'mcx>,
        _: &mut BbsinkState,
        _: &str,
    ) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn archive_contents(
        &mut self,
        _: &mut Bbsink<'mcx>,
        _: &mut BbsinkState,
        _: Size,
    ) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn end_archive(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn begin_manifest(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn manifest_contents(
        &mut self,
        _: &mut Bbsink<'mcx>,
        _: &mut BbsinkState,
        _: Size,
    ) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn end_manifest(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn end_backup(
        &mut self,
        _: &mut Bbsink<'mcx>,
        _: &mut BbsinkState,
        _: XLogRecPtr,
        _: TimeLineID,
    ) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn cleanup(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
}

/// Begin a backup.
///
/// Sets the shared state and buffer length on `sink`, invokes the `begin_backup`
/// callback, and then verifies that a buffer of a length that is a positive
/// multiple of `BLCKSZ` was installed.
pub fn bbsink_begin_backup<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    buffer_length: Size,
) -> PgResult<()> {
    assert!(buffer_length > 0, "buffer_length must be positive");

    sink.buffer_length = buffer_length;
    dispatch(sink, state, |ops, sink, state| ops.begin_backup(sink, state))?;

    assert!(sink.has_buffer(), "begin_backup must set the buffer");
    assert!(
        sink.buffer_length() % BLCKSZ == 0,
        "buffer length must be a multiple of BLCKSZ"
    );
    Ok(())
}

/// Begin an archive.
pub fn bbsink_begin_archive<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    archive_name: &str,
) -> PgResult<()> {
    dispatch(sink, state, |ops, sink, state| {
        ops.begin_archive(sink, state, archive_name)
    })
}

/// Process some of the contents of an archive.
pub fn bbsink_archive_contents<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    len: Size,
) -> PgResult<()> {
    // The caller should make a reasonable attempt to fill the buffer before
    // calling this function, so it shouldn't be completely empty. Nor should it
    // be filled beyond capacity.
    assert!(
        len > 0 && len <= sink.buffer_length(),
        "archive content length must fit sink buffer"
    );
    dispatch(sink, state, |ops, sink, state| {
        ops.archive_contents(sink, state, len)
    })
}

/// Finish an archive.
pub fn bbsink_end_archive<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    dispatch(sink, state, |ops, sink, state| ops.end_archive(sink, state))
}

/// Begin the backup manifest.
pub fn bbsink_begin_manifest<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    dispatch(sink, state, |ops, sink, state| {
        ops.begin_manifest(sink, state)
    })
}

/// Process some of the manifest contents.
pub fn bbsink_manifest_contents<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    len: Size,
) -> PgResult<()> {
    // See comments in `bbsink_archive_contents`.
    assert!(
        len > 0 && len <= sink.buffer_length(),
        "manifest content length must fit sink buffer"
    );
    dispatch(sink, state, |ops, sink, state| {
        ops.manifest_contents(sink, state, len)
    })
}

/// Finish the backup manifest.
pub fn bbsink_end_manifest<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    dispatch(sink, state, |ops, sink, state| {
        ops.end_manifest(sink, state)
    })
}

/// Finish a backup.
///
/// Matches the C assertion that, by the time the backup ends, every tablespace
/// has been processed: `tablespace_num == list_length(tablespaces)`.
pub fn bbsink_end_backup<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    endptr: XLogRecPtr,
    endtli: TimeLineID,
) -> PgResult<()> {
    assert!(
        state.tablespace_num as i64 == state.tablespaces.len() as i64,
        "all tablespaces must be processed before end_backup"
    );
    dispatch(sink, state, |ops, sink, state| {
        ops.end_backup(sink, state, endptr, endtli)
    })
}

/// Release resources before destruction.
pub fn bbsink_cleanup<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    dispatch(sink, state, |ops, sink, state| ops.cleanup(sink, state))
}

// --- Forwarding callbacks: pass operations through to the next sink. ---

/// Forward `begin_backup` to the next sink.
///
/// Only use this from a `begin_backup` callback when you want the sink you're
/// implementing to share a buffer with the successor sink. After the successor
/// installs its buffer, this sink marks itself as sharing that buffer.
pub fn bbsink_forward_begin_backup<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    let buffer_length = sink.buffer_length;
    let next = sink
        .next
        .as_deref_mut()
        .expect("forwarding sink must have next sink");
    bbsink_begin_backup(next, state, buffer_length)?;
    // Share the successor's buffer (the C code copies `bbs_buffer`).
    sink.shares_next_buffer = true;
    Ok(())
}

/// Forward `begin_archive` to the next sink.
pub fn bbsink_forward_begin_archive<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    archive_name: &str,
) -> PgResult<()> {
    let next = sink
        .next
        .as_deref_mut()
        .expect("forwarding sink must have next sink");
    bbsink_begin_archive(next, state, archive_name)
}

/// Forward `archive_contents` to the next sink.
///
/// Code that wants to use this should arrange to share the successor sink's
/// buffer. In cases where the buffer isn't shared, the data needs to be copied
/// before forwarding; this function does not do that, mirroring the C code,
/// which asserts the buffers are identical.
pub fn bbsink_forward_archive_contents<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    len: Size,
) -> PgResult<()> {
    assert_shared_buffer(sink);
    let next = sink
        .next
        .as_deref_mut()
        .expect("forwarding sink must have next sink");
    bbsink_archive_contents(next, state, len)
}

/// Forward `end_archive` to the next sink.
pub fn bbsink_forward_end_archive<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    let next = sink
        .next
        .as_deref_mut()
        .expect("forwarding sink must have next sink");
    bbsink_end_archive(next, state)
}

/// Forward `begin_manifest` to the next sink.
pub fn bbsink_forward_begin_manifest<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    let next = sink
        .next
        .as_deref_mut()
        .expect("forwarding sink must have next sink");
    bbsink_begin_manifest(next, state)
}

/// Forward `manifest_contents` to the next sink.
///
/// As with the `archive_contents` callback, the buffer is expected to be shared.
pub fn bbsink_forward_manifest_contents<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    len: Size,
) -> PgResult<()> {
    assert_shared_buffer(sink);
    let next = sink
        .next
        .as_deref_mut()
        .expect("forwarding sink must have next sink");
    bbsink_manifest_contents(next, state, len)
}

/// Forward `end_manifest` to the next sink.
pub fn bbsink_forward_end_manifest<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    let next = sink
        .next
        .as_deref_mut()
        .expect("forwarding sink must have next sink");
    bbsink_end_manifest(next, state)
}

/// Forward `end_backup` to the next sink.
pub fn bbsink_forward_end_backup<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    endptr: XLogRecPtr,
    endtli: TimeLineID,
) -> PgResult<()> {
    let next = sink
        .next
        .as_deref_mut()
        .expect("forwarding sink must have next sink");
    bbsink_end_backup(next, state, endptr, endtli)
}

/// Forward `cleanup` to the next sink.
pub fn bbsink_forward_cleanup<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
) -> PgResult<()> {
    let next = sink
        .next
        .as_deref_mut()
        .expect("forwarding sink must have next sink");
    bbsink_cleanup(next, state)
}

/// Mirror of the C
/// `Assert(sink->bbs_buffer == sink->bbs_next->bbs_buffer)` and
/// `Assert(sink->bbs_buffer_length == sink->bbs_next->bbs_buffer_length)` used
/// by the forwarding contents callbacks. In this buffer-sharing model, a
/// forwarding sink records sharing as a flag, so the assertion checks that this
/// sink really is forwarding to a successor whose buffer it shares.
fn assert_shared_buffer(sink: &Bbsink<'_>) {
    assert!(sink.next.is_some(), "forwarding sink must have next sink");
    assert!(
        sink.shares_next_buffer,
        "forwarded content requires a shared buffer"
    );
    assert_eq!(
        sink.buffer_length(),
        sink.next.as_deref().map(Bbsink::buffer_length).unwrap_or(0),
        "forwarded content requires a shared buffer length"
    );
}

/// Install this crate's seams. This unit is a vtable leaf with no seams of its
/// own (no cross-cycle callers and no outward seamed calls), so there is
/// nothing to set; it is registered in `seams-init::init_all` for uniformity.
pub fn init_seams() {}

#[cfg(test)]
mod tests;
