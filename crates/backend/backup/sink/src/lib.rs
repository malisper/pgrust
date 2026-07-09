//! Port of `basebackup_sink.c` / `basebackup_sink.h` (PostgreSQL 18.3): the
//! bbsink chain. The C vtable (`bbsink_ops`) becomes the [`BbsinkOps`] trait;
//! the shared raw pointers become an owned `PgVec` buffer, an owned
//! `Box<Bbsink>` successor, and an explicitly threaded `&mut BbsinkState`. A
//! forwarding sink shares its successor's buffer, recorded as a flag.

#![no_std]
#![forbid(unsafe_code)]

extern crate alloc;

use alloc::boxed::Box;

use ::mcx::{vec_with_capacity_in, Mcx, PgVec};
use ::types_core::primitive::{Size, TimeLineID, XLogRecPtr, BLCKSZ};
use ::types_error::PgResult;

// TablespaceInfo is owned by xlogbackup (transam layer) to avoid a layering
// inversion; re-exported here so sink consumers keep a stable `sink::TablespaceInfo`.
pub use ::xlogbackup::TablespaceInfo;

/// C `bbsink_state`. `tablespaces`, `startptr`, `starttli` must be set before
/// [`bbsink_begin_backup`] and not modified thereafter.
#[derive(Clone, Debug, Default)]
pub struct BbsinkState {
    pub tablespaces: alloc::vec::Vec<TablespaceInfo>,
    pub tablespace_num: i32,
    pub bytes_done: u64,
    pub bytes_total: u64,
    pub bytes_total_is_valid: bool,
    pub startptr: XLogRecPtr,
    pub starttli: TimeLineID,
}

/// C `bbsink_ops`. A callback that only forwards should call the matching
/// `bbsink_forward_*` function. Callers invoke these via the `bbsink_*` free
/// functions, which run C's `Assert`s first.
pub trait BbsinkOps<'mcx> {
    fn begin_backup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()>;
    fn begin_archive(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        archive_name: &str,
    ) -> PgResult<()>;
    fn archive_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()>;
    fn end_archive(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()>;
    fn begin_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()>;
    fn manifest_contents(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        len: Size,
    ) -> PgResult<()>;
    fn end_manifest(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()>;
    fn end_backup(
        &mut self,
        sink: &mut Bbsink<'mcx>,
        state: &mut BbsinkState,
        endptr: XLogRecPtr,
        endtli: TimeLineID,
    ) -> PgResult<()>;
    fn cleanup(&mut self, sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()>;
}

/// C `struct bbsink`. Owns the callback table, the working buffer (charged to
/// its [`Mcx`]), and the successor sink. A forwarding sink leaves `buffer`
/// empty and sets `shares_next_buffer`, delegating buffer queries to `next`.
pub struct Bbsink<'mcx> {
    ops: Box<dyn BbsinkOps<'mcx> + 'mcx>,
    buffer: PgVec<'mcx, u8>,
    /// C `bbs_buffer_length`; set before the buffer is installed during
    /// `begin_backup`, as C sets the length before filling the pointer.
    buffer_length: Size,
    shares_next_buffer: bool,
    next: Option<Box<Bbsink<'mcx>>>,
}

impl<'mcx> Bbsink<'mcx> {
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

    pub fn next(&self) -> Option<&Bbsink<'mcx>> {
        self.next.as_deref()
    }

    pub fn next_mut(&mut self) -> Option<&mut Bbsink<'mcx>> {
        self.next.as_deref_mut()
    }

    pub fn has_buffer(&self) -> bool {
        if self.shares_next_buffer {
            self.next.as_deref().is_some_and(Bbsink::has_buffer)
        } else {
            !self.buffer.is_empty()
        }
    }

    pub fn buffer_length(&self) -> Size {
        if self.shares_next_buffer {
            self.next.as_deref().map(Bbsink::buffer_length).unwrap_or(0)
        } else {
            self.buffer_length
        }
    }

    /// Panics if `len` exceeds the buffer length or no buffer is installed
    /// (C's within-`bbs_buffer_length` contract).
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

    /// Install a zeroed `len`-byte buffer (C `palloc0`). Fallible: enforces
    /// `MaxAllocSize` and returns [`PgError`](::types_error::PgError) on OOM
    /// rather than aborting.
    pub fn set_buffer(&mut self, mcx: Mcx<'mcx>, len: Size) -> PgResult<()> {
        let mut buffer = vec_with_capacity_in::<u8>(mcx, len)?;
        buffer.resize(len, 0);
        self.buffer = buffer;
        self.buffer_length = len;
        self.shares_next_buffer = false;
        Ok(())
    }

    pub fn clear_buffer(&mut self, mcx: Mcx<'mcx>) {
        self.buffer = PgVec::new_in(mcx);
        self.buffer_length = 0;
        self.shares_next_buffer = false;
    }
}

// Dispatch: the `bbsink_*` inline helpers from the C header.

/// Move the boxed ops out for the callback so it can borrow the sink's
/// buffer/next fields without aliasing the ops box. Restored even on unwind.
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

struct NoopOps;

impl<'mcx> BbsinkOps<'mcx> for NoopOps {
    fn begin_backup(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn begin_archive(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState, _: &str) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn archive_contents(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState, _: Size) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn end_archive(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn begin_manifest(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState) -> PgResult<()> {
        unreachable!("placeholder ops invoked")
    }
    fn manifest_contents(&mut self, _: &mut Bbsink<'mcx>, _: &mut BbsinkState, _: Size) -> PgResult<()> {
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

/// Asserts a buffer of a positive `BLCKSZ` multiple was installed.
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

pub fn bbsink_begin_archive<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    archive_name: &str,
) -> PgResult<()> {
    dispatch(sink, state, |ops, sink, state| {
        ops.begin_archive(sink, state, archive_name)
    })
}

pub fn bbsink_archive_contents<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    len: Size,
) -> PgResult<()> {
    assert!(
        len > 0 && len <= sink.buffer_length(),
        "archive content length must fit sink buffer"
    );
    dispatch(sink, state, |ops, sink, state| {
        ops.archive_contents(sink, state, len)
    })
}

pub fn bbsink_end_archive<'mcx>(sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
    dispatch(sink, state, |ops, sink, state| ops.end_archive(sink, state))
}

pub fn bbsink_begin_manifest<'mcx>(sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
    dispatch(sink, state, |ops, sink, state| ops.begin_manifest(sink, state))
}

pub fn bbsink_manifest_contents<'mcx>(
    sink: &mut Bbsink<'mcx>,
    state: &mut BbsinkState,
    len: Size,
) -> PgResult<()> {
    assert!(
        len > 0 && len <= sink.buffer_length(),
        "manifest content length must fit sink buffer"
    );
    dispatch(sink, state, |ops, sink, state| {
        ops.manifest_contents(sink, state, len)
    })
}

pub fn bbsink_end_manifest<'mcx>(sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
    dispatch(sink, state, |ops, sink, state| ops.end_manifest(sink, state))
}

/// Asserts every tablespace has been processed (C `bbsink_end_backup`).
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

pub fn bbsink_cleanup<'mcx>(sink: &mut Bbsink<'mcx>, state: &mut BbsinkState) -> PgResult<()> {
    dispatch(sink, state, |ops, sink, state| ops.cleanup(sink, state))
}

// Forwarding callbacks: pass operations through to the next sink.

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
    // Share the successor's buffer (C copies `bbs_next->bbs_buffer`).
    sink.shares_next_buffer = true;
    Ok(())
}

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

/// C asserts the forwarding sink's buffer equals its successor's; the sharing
/// flag stands in for pointer equality here.
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

pub fn init_seams() {}

#[cfg(test)]
mod tests;
