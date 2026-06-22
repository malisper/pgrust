#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
// `PgError` is a large owned `Err`; the un-boxed return is the project error
// contract.
#![allow(clippy::result_large_err)]

//! `storage/aio/read_stream.c` — the look-ahead read-stream mechanism.
//!
//! A read stream issues a series of [`BufferManager::ReadBuffer`]-equivalent
//! pins for one fork of one relation, in the order a user-provided callback
//! dictates, while looking ahead to coalesce neighbouring blocks into larger
//! vectored reads (`StartReadBuffers`) and start them ahead of time. The full
//! C algorithm is ported here: the circular buffer queue, the parallel
//! in-progress-I/O queue, the adaptive look-ahead distance, the fast path for
//! all-cached scans, forwarded-buffer handling on split reads, and read-ahead
//! advice.
//!
//! This crate sits directly above [`backend_storage_buffer_bufmgr`]: it calls
//! `StartReadBuffer`/`StartReadBuffers`/`WaitReadBuffers` + the buffer
//! accessors directly (no seam — bufmgr is below read_stream). The look-ahead
//! depends only on the buffer manager being present; the actual block transfer
//! rides bufmgr's synchronous (IOMETHOD_SYNC) read path. The legs that require
//! the asynchronous pgaio engine (`pgaio_enter_batchmode`/`pgaio_exit_batchmode`
//! AIO batching) are seam-and-panic until that engine lands (#15, sanctioned).
//!
//! The C `ReadStream *` is an owned value here: `read_stream_begin_relation`
//! returns a `Box<ReadStream<'mcx>>` the consumer holds (no handle, no
//! registry). The C `ReadStreamBlockNumberCB` function pointer + `void*`
//! private data is a boxed `FnMut` closure carried in the stream.

extern crate alloc;

use alloc::boxed::Box;
use alloc::format;
use alloc::vec;
use alloc::vec::Vec;

use types_core::primitive::{BlockNumber, Buffer, ForkNumber, InvalidBlockNumber};
use types_error::{PgError, PgResult};
use backend_storage_buffer_support::{BufferAccessStrategyRing, LocalBufferManager};
use types_rel::Relation;
use types_storage::buf::BufferAccessStrategy;
use types_storage::storage::{BufferIsValid, InvalidBuffer};

use backend_storage_buffer_bufmgr::{BufferManager, ReadOp};
use backend_storage_buffer_bufmgr_seams as bufmgr_seams;

/// The ambient process buffer manager (`BufferManager::global`), expected to be
/// registered by `BufferManagerShmemInit` before any read stream runs.
fn bm_global() -> &'static BufferManager {
    BufferManager::global().expect("read_stream: the buffer manager is not registered for this process")
}

// === read_stream.h flags ====================================================

/// `READ_STREAM_DEFAULT` (read_stream.h) — reasonable default tuning.
pub const READ_STREAM_DEFAULT: i32 = 0x00;
/// `READ_STREAM_MAINTENANCE` (read_stream.h) — governed by
/// `maintenance_io_concurrency` instead of `effective_io_concurrency`.
pub const READ_STREAM_MAINTENANCE: i32 = 0x01;
/// `READ_STREAM_SEQUENTIAL` (read_stream.h) — disable prefetch advice.
pub const READ_STREAM_SEQUENTIAL: i32 = 0x02;
/// `READ_STREAM_FULL` (read_stream.h) — skip the look-ahead ramp-up.
pub const READ_STREAM_FULL: i32 = 0x04;
/// `READ_STREAM_USE_BATCHING` (read_stream.h) — opt in to AIO batch mode.
pub const READ_STREAM_USE_BATCHING: i32 = 0x08;

// === bufmgr.h read flags ====================================================

/// `READ_BUFFERS_ISSUE_ADVICE` (bufmgr.h) — issue read-ahead advice.
const READ_BUFFERS_ISSUE_ADVICE: u32 = 1 << 2;
/// `READ_BUFFERS_SYNCHRONOUSLY` (bufmgr.h) — force a synchronous read.
const READ_BUFFERS_SYNCHRONOUSLY: u32 = 1 << 4;

/// `PG_INT16_MAX` (c.h).
const PG_INT16_MAX: i32 = 32767;
/// `MAXIMUM_ALIGNOF` (pg_config.h) — not used for layout here (the queue is a
/// real `Vec`, not one `palloc` blob); kept for documentation parity.

// === The block-range general-use callback ==================================

/// `BlockRangeReadStreamPrivate` (read_stream.h) — private data for
/// [`block_range_read_stream_cb`], iterating `[current_blocknum,
/// last_exclusive)`.
#[derive(Clone, Copy, Debug, Default)]
pub struct BlockRangeReadStreamPrivate {
    /// `BlockNumber current_blocknum` — the next block the range will emit.
    pub current_blocknum: BlockNumber,
    /// `BlockNumber last_exclusive` — the exclusive upper bound.
    pub last_exclusive: BlockNumber,
}

/// The callback that returns the next block number to read, or
/// [`InvalidBlockNumber`] for end-of-stream. The C `ReadStreamBlockNumberCB` is
/// a function pointer over `void *callback_private_data` + a `void
/// *per_buffer_data` write area; here the closure owns whatever private state
/// the C `callback_private_data` held, and writes the per-buffer data area
/// directly.
pub type ReadStreamBlockNumberCB<'mcx> =
    Box<dyn FnMut(&mut [u8]) -> BlockNumber + 'mcx>;

// === InProgressIO ===========================================================

/// `InProgressIO` (read_stream.c) — a started-but-unwaited read, plus the queue
/// index of its head buffer.
struct InProgressIO {
    /// `int16 buffer_index` — index in the circular buffer queue of the head
    /// buffer this I/O covers.
    buffer_index: i16,
    /// `ReadBuffersOperation op` — the in-flight read operation; `None` between
    /// uses (the C value lives inline in the array, pre-initialised; here a
    /// fresh `ReadOp` is produced by each `StartReadBuffers` call and stored
    /// for the matching `WaitReadBuffers`).
    op: Option<ReadOp>,
    /// `op.blocknum` — captured so the advice-cancel check in
    /// `read_stream_next_buffer` can read it (the C reads `op.blocknum`
    /// directly; `ReadOp` is opaque here).
    op_blocknum: BlockNumber,
}

impl Default for InProgressIO {
    fn default() -> Self {
        InProgressIO {
            buffer_index: 0,
            op: None,
            op_blocknum: InvalidBlockNumber,
        }
    }
}

// === ReadStream =============================================================

/// `struct ReadStream` (read_stream.c) — state for a stream of look-ahead reads.
pub struct ReadStream<'mcx> {
    max_ios: i16,
    io_combine_limit: i16,
    ios_in_progress: i16,
    queue_size: i16,
    max_pinned_buffers: i16,
    forwarded_buffers: i16,
    pinned_buffers: i16,
    distance: i16,
    initialized_buffers: i16,
    read_buffers_flags: u32,
    sync_mode: bool,
    batch_mode: bool,
    advice_enabled: bool,
    temporary: bool,

    /// One-block buffer to support 'ungetting' a block number.
    buffered_blocknum: BlockNumber,

    /// The callback + (in C) opaque private pointer; folded into the closure.
    callback: ReadStreamBlockNumberCB<'mcx>,

    /// Next expected block, for detecting sequential access.
    seq_blocknum: BlockNumber,
    seq_until_processed: BlockNumber,

    /// The read operation we are currently preparing.
    pending_read_blocknum: BlockNumber,
    pending_read_nblocks: i16,

    /// Space for the optional per-buffer private data (`per_buffer_data_size *
    /// queue_size` bytes, indexed by buffer slot).
    per_buffer_data_size: usize,
    per_buffer_data: Vec<u8>,

    /// Read operations started but not waited for yet.
    ios: Vec<InProgressIO>,
    oldest_io_index: i16,
    next_io_index: i16,

    fast_path: bool,

    /// Circular queue of buffers (length `queue_size + queue_overflow`).
    oldest_buffer_index: i16,
    next_buffer_index: i16,
    buffers: Vec<Buffer>,

    /// The relation + fork being read (the read pipeline needs `&Relation`).
    /// The forwarded fields the C pre-stores in `ios[i].op` (rel/smgr/
    /// persistence/forknum/strategy) are supplied here on each StartReadBuffers
    /// call (bufmgr's StartReadBuffers takes them per-call).
    rel: &'mcx Relation<'mcx>,
    forknum: ForkNumber,
    /// `IOContextForStrategy(stream->ios[].op.strategy)` — the pg_stat_io context
    /// this stream's reads are accounted under (IOCONTEXT_NORMAL when no ring).
    io_context: types_storage::buf::IOContext,
}

impl<'mcx> ReadStream<'mcx> {
    /// `get_per_buffer_data(stream, buffer_index)` (read_stream.c:150) — the
    /// per-buffer data slice for one queue slot.
    fn per_buffer_data_slice(&mut self, buffer_index: i16) -> &mut [u8] {
        let sz = self.per_buffer_data_size;
        let start = sz * buffer_index as usize;
        &mut self.per_buffer_data[start..start + sz]
    }

    /// `read_stream_get_block(stream, per_buffer_data)` (read_stream.c:178) —
    /// ask the callback which block to read next, with the one-block unget
    /// buffer in front.
    fn read_stream_get_block(&mut self, buffer_index: Option<i16>) -> BlockNumber {
        let mut blocknum = self.buffered_blocknum;
        if blocknum != InvalidBlockNumber {
            self.buffered_blocknum = InvalidBlockNumber;
        } else {
            // VALGRIND_MAKE_MEM_UNDEFINED(per_buffer_data, ...) — no-op here.
            // The C passes a per-buffer write area; with no per-buffer data the
            // callback receives an empty slice.
            blocknum = match buffer_index {
                Some(bi) if self.per_buffer_data_size > 0 => {
                    // Borrow split: copy the slice ptr region out by calling
                    // the closure on the per-buffer slice.
                    let sz = self.per_buffer_data_size;
                    let start = sz * bi as usize;
                    // Temporarily take the callback to avoid a double &mut self
                    // borrow (callback is a field of self, and it needs the
                    // per-buffer slice which is also a field of self).
                    let mut cb = core::mem::replace(
                        &mut self.callback,
                        Box::new(|_: &mut [u8]| InvalidBlockNumber),
                    );
                    let r = cb(&mut self.per_buffer_data[start..start + sz]);
                    self.callback = cb;
                    r
                }
                _ => {
                    let mut empty: [u8; 0] = [];
                    (self.callback)(&mut empty)
                }
            };
        }
        blocknum
    }

    /// `read_stream_unget_block(stream, blocknum)` (read_stream.c:210).
    fn read_stream_unget_block(&mut self, blocknum: BlockNumber) {
        debug_assert_eq!(self.buffered_blocknum, InvalidBlockNumber);
        debug_assert_ne!(blocknum, InvalidBlockNumber);
        self.buffered_blocknum = blocknum;
    }

    /// `read_stream_start_pending_read(stream)` (read_stream.c:229) — start as
    /// much of the current pending read as the per-backend buffer limit and the
    /// buffer manager allow. Returns false if nothing could be started.
    fn read_stream_start_pending_read(&mut self) -> PgResult<bool> {
        let bm = bm_global();

        debug_assert!(self.pending_read_nblocks > 0);
        debug_assert!(self.pending_read_nblocks <= self.io_combine_limit);
        debug_assert!(self.pinned_buffers + self.pending_read_nblocks <= self.max_pinned_buffers);

        // Do we need to issue read-ahead advice?
        let mut flags = self.read_buffers_flags;
        if self.advice_enabled {
            if self.pending_read_blocknum == self.seq_blocknum {
                // Sequential: issue advice until the reads catch up.
                if self.seq_until_processed != InvalidBlockNumber {
                    flags |= READ_BUFFERS_ISSUE_ADVICE;
                }
            } else {
                // Random jump: note a new potential sequential region.
                self.seq_until_processed = self.pending_read_blocknum;
                if self.pinned_buffers > 0 {
                    flags |= READ_BUFFERS_ISSUE_ADVICE;
                }
            }
        }

        // How many more buffers is this backend allowed?
        let mut buffer_limit: i32 = if self.temporary {
            i32::min(bm_get_additional_local_pin_limit(), PG_INT16_MAX)
        } else {
            i32::min(bm_get_additional_pin_limit(bm), PG_INT16_MAX)
        };
        debug_assert!(self.forwarded_buffers <= self.pending_read_nblocks);

        buffer_limit += self.forwarded_buffers as i32;
        buffer_limit = i32::min(buffer_limit, PG_INT16_MAX);

        if buffer_limit == 0 && self.pinned_buffers == 0 {
            buffer_limit = 1; // guarantee progress
        }

        // Does the per-backend limit affect this read?
        let mut nblocks = self.pending_read_nblocks as i32;
        if buffer_limit < nblocks {
            // Shrink distance: no more look-ahead until buffers are released.
            let new_distance = self.pinned_buffers as i32 + buffer_limit;
            if (self.distance as i32) > new_distance {
                self.distance = new_distance as i16;
            }

            // Unless we have nothing to give the consumer, stop here.
            if self.pinned_buffers > 0 {
                return Ok(false);
            }

            // A short read is required to make progress.
            nblocks = buffer_limit;
        }

        // Initialize buffers to InvalidBuffer (= not forwarded) on first use,
        // and remember the original nblocks (requested) so forwarded buffers
        // can be detected as output.
        let buffer_index0 = self.next_buffer_index;
        let io_index = self.next_io_index;
        while (self.initialized_buffers as i32) < buffer_index0 as i32 + nblocks {
            self.buffers[self.initialized_buffers as usize] = InvalidBuffer;
            self.initialized_buffers += 1;
        }
        let requested_nblocks = nblocks;

        // StartReadBuffers(&ios[io_index].op, &buffers[buffer_index], blocknum,
        //                  &nblocks, flags)
        let mut nblocks_io = nblocks;
        let (op, need_wait) = {
            // The buffer manager operates on a contiguous slice of the queue;
            // the queue has room (queue_size + queue_overflow) so a read never
            // wraps mid-operation.
            let bstart = buffer_index0 as usize;
            let bend = bstart + nblocks as usize;
            bm.StartReadBuffers(
                self.rel,
                self.forknum,
                &mut self.buffers[bstart..bend],
                self.pending_read_blocknum,
                &mut nblocks_io,
                flags,
                self.io_context,
            )?
        };
        nblocks = nblocks_io;
        self.pinned_buffers += nblocks as i16;

        // Remember whether we need to wait before returning this buffer.
        if !need_wait {
            // Look-ahead distance decays, no I/O necessary.
            if self.distance > 1 {
                self.distance -= 1;
            }
            // op is dropped (no wait needed); it held only a 'hit' record.
            let _ = op;
        } else {
            // Remember to call WaitReadBuffers() before returning head buffer.
            self.ios[io_index as usize].buffer_index = buffer_index0;
            self.ios[io_index as usize].op_blocknum = self.pending_read_blocknum;
            self.ios[io_index as usize].op = Some(op);
            self.next_io_index += 1;
            if self.next_io_index == self.max_ios {
                self.next_io_index = 0;
            }
            debug_assert!(self.ios_in_progress < self.max_ios);
            self.ios_in_progress += 1;
            self.seq_blocknum = self.pending_read_blocknum.wrapping_add(nblocks as u32);
        }

        // How many pins were acquired but forwarded to the next call?
        let mut forwarded = 0i32;
        while nblocks + forwarded < requested_nblocks
            && self.buffers[(buffer_index0 as i32 + nblocks + forwarded) as usize] != InvalidBuffer
        {
            forwarded += 1;
        }
        self.forwarded_buffers = forwarded as i16;

        // We gave a contiguous range to StartReadBuffers(), but we want it to
        // wrap at queue_size. Copy overflowing buffers to the front of the
        // array where they'll be consumed, leaving a copy in the overflow zone
        // the I/O still references.
        let overflow = (buffer_index0 as i32 + nblocks + forwarded) - self.queue_size as i32;
        if overflow > 0 {
            debug_assert!(overflow < self.queue_size as i32); // can't overlap
            for k in 0..overflow as usize {
                self.buffers[k] = self.buffers[self.queue_size as usize + k];
            }
        }

        // Compute location of start of next read, without using %.
        let mut buffer_index = buffer_index0 as i32 + nblocks;
        if buffer_index >= self.queue_size as i32 {
            buffer_index -= self.queue_size as i32;
        }
        debug_assert!(buffer_index >= 0 && buffer_index < self.queue_size as i32);
        self.next_buffer_index = buffer_index as i16;

        // Adjust the pending read to cover the remaining portion, if any.
        self.pending_read_blocknum = self.pending_read_blocknum.wrapping_add(nblocks as u32);
        self.pending_read_nblocks -= nblocks as i16;

        Ok(true)
    }

    /// `read_stream_look_ahead(stream)` (read_stream.c:428).
    fn read_stream_look_ahead(&mut self) -> PgResult<()> {
        // Allow amortizing the cost of submitting IO over multiple IOs (AIO
        // batch mode). The asynchronous engine is unported (#15); a stream that
        // opts into batching reaches the engine here.
        if self.batch_mode {
            pgaio_enter_batchmode();
        }

        while self.ios_in_progress < self.max_ios
            && self.pinned_buffers + self.pending_read_nblocks < self.distance
        {
            if self.pending_read_nblocks == self.io_combine_limit {
                self.read_stream_start_pending_read()?;
                continue;
            }

            // The index of the Nth block of the pending read, with wrap-around.
            let mut buffer_index = self.next_buffer_index as i32 + self.pending_read_nblocks as i32;
            if buffer_index >= self.queue_size as i32 {
                buffer_index -= self.queue_size as i32;
            }
            debug_assert!(buffer_index >= 0 && buffer_index < self.queue_size as i32);
            let blocknum = self.read_stream_get_block(Some(buffer_index as i16));
            if blocknum == InvalidBlockNumber {
                // End of stream.
                self.distance = 0;
                break;
            }

            // Can we merge it with the pending read?
            if self.pending_read_nblocks > 0
                && self
                    .pending_read_blocknum
                    .wrapping_add(self.pending_read_nblocks as u32)
                    == blocknum
            {
                self.pending_read_nblocks += 1;
                continue;
            }

            // We have to start the pending read before building another.
            while self.pending_read_nblocks > 0 {
                if !self.read_stream_start_pending_read()?
                    || self.ios_in_progress == self.max_ios
                {
                    // We've hit the buffer or I/O limit. Rewind and stop here.
                    self.read_stream_unget_block(blocknum);
                    if self.batch_mode {
                        pgaio_exit_batchmode();
                    }
                    return Ok(());
                }
            }

            // This is the start of a new pending read.
            self.pending_read_blocknum = blocknum;
            self.pending_read_nblocks = 1;
        }

        // Start the pending read immediately if we've reached io_combine_limit,
        // or the distance limit with nothing pinned, or hit end-of-stream.
        if self.pending_read_nblocks > 0
            && (self.pending_read_nblocks == self.io_combine_limit
                || (self.pending_read_nblocks >= self.distance && self.pinned_buffers == 0)
                || self.distance == 0)
            && self.ios_in_progress < self.max_ios
        {
            self.read_stream_start_pending_read()?;
        }

        debug_assert!(self.pinned_buffers > 0 || self.distance == 0);

        if self.batch_mode {
            pgaio_exit_batchmode();
        }
        Ok(())
    }

    /// `read_stream_next_buffer(stream, per_buffer_data)` (read_stream.c:790) —
    /// pull one pinned buffer out of the stream. Returns [`InvalidBuffer`] at
    /// end-of-stream. When `want_per_buffer_data`, the returned slice index is
    /// the (now-consumed) oldest buffer slot; the caller's per-buffer data
    /// pointer would point into `self.per_buffer_data` at that slot.
    pub fn read_stream_next_buffer(&mut self) -> PgResult<(Buffer, Option<i16>)> {
        let bm = bm_global();
        let buffer;
        let oldest_buffer_index;

        // Fast path for all-cached scans: specialized for no I/O and no
        // per-buffer data, staying in the same buffer slot with singular
        // StartReadBuffer().
        if self.fast_path {
            debug_assert_eq!(self.ios_in_progress, 0);
            debug_assert_eq!(self.forwarded_buffers, 0);
            debug_assert_eq!(self.pinned_buffers, 1);
            debug_assert_eq!(self.distance, 1);
            debug_assert_eq!(self.pending_read_nblocks, 0);
            debug_assert_eq!(self.per_buffer_data_size, 0);
            debug_assert!(self.initialized_buffers > self.oldest_buffer_index);

            // Return the buffer we pinned last time.
            let obi = self.oldest_buffer_index;
            debug_assert_eq!(
                (obi as i32 + 1) % self.queue_size as i32,
                self.next_buffer_index as i32
            );
            buffer = self.buffers[obi as usize];
            debug_assert_ne!(buffer, InvalidBuffer);

            // Choose the next block to pin.
            let next_blocknum = self.read_stream_get_block(None);

            if next_blocknum != InvalidBlockNumber {
                let mut flags = self.read_buffers_flags;
                if self.advice_enabled {
                    flags |= READ_BUFFERS_ISSUE_ADVICE;
                }

                // Pin a buffer for the next call. Same buffer entry, arbitrary
                // I/O entry. No pin-limit check (always allowed one pin).
                let mut slot = self.buffers[obi as usize];
                let (op, need_wait) = bm.StartReadBuffer(
                    self.rel,
                    self.forknum,
                    &mut slot,
                    next_blocknum,
                    flags,
                    self.io_context,
                )?;
                self.buffers[obi as usize] = slot;

                if !need_wait {
                    // Fast return.
                    let _ = op;
                    self.fast_path = false;
                    return Ok((buffer, None));
                }

                // Next call must wait for I/O for the newly pinned buffer.
                self.oldest_io_index = 0;
                self.next_io_index = if self.max_ios > 1 { 1 } else { 0 };
                self.ios_in_progress = 1;
                self.ios[0].buffer_index = obi;
                self.ios[0].op_blocknum = next_blocknum;
                self.ios[0].op = Some(op);
                self.seq_blocknum = next_blocknum.wrapping_add(1);
            } else {
                // No more blocks, end of stream.
                self.distance = 0;
                self.oldest_buffer_index = self.next_buffer_index;
                self.pinned_buffers = 0;
                self.buffers[obi as usize] = InvalidBuffer;
            }

            self.fast_path = false;
            return Ok((buffer, None));
        }

        if self.pinned_buffers == 0 {
            debug_assert_eq!(self.oldest_buffer_index, self.next_buffer_index);

            // End of stream reached?
            if self.distance == 0 {
                return Ok((InvalidBuffer, None));
            }

            // Crank the handle to get started.
            self.read_stream_look_ahead()?;

            // End of stream reached?
            if self.pinned_buffers == 0 {
                debug_assert_eq!(self.distance, 0);
                return Ok((InvalidBuffer, None));
            }
        }

        // Grab the oldest pinned buffer and associated per-buffer data.
        debug_assert!(self.pinned_buffers > 0);
        oldest_buffer_index = self.oldest_buffer_index;
        debug_assert!(
            oldest_buffer_index >= 0 && oldest_buffer_index < self.queue_size
        );
        buffer = self.buffers[oldest_buffer_index as usize];
        debug_assert!(BufferIsValid(buffer));

        // Do we have to wait for an associated I/O first?
        if self.ios_in_progress > 0
            && self.ios[self.oldest_io_index as usize].buffer_index == oldest_buffer_index
        {
            let io_index = self.oldest_io_index;
            let op_blocknum = self.ios[io_index as usize].op_blocknum;

            // WaitReadBuffers(&ios[io_index].op).
            {
                let mut op = self.ios[io_index as usize].op.take().expect(
                    "read_stream: in-progress I/O has no stored ReadOp",
                );
                bm.WaitReadBuffers(&mut op)?;
                // op is consumed; the slot is now free for reuse.
            }

            debug_assert!(self.ios_in_progress > 0);
            self.ios_in_progress -= 1;
            self.oldest_io_index += 1;
            if self.oldest_io_index == self.max_ios {
                self.oldest_io_index = 0;
            }

            // Look-ahead distance ramps up rapidly after we do I/O.
            let mut distance = self.distance as i32 * 2;
            distance = i32::min(distance, self.max_pinned_buffers as i32);
            self.distance = distance as i16;

            // If we've reached the first block of a sequential region we're
            // issuing advice for, cancel that until the next jump.
            if self.advice_enabled && op_blocknum == self.seq_until_processed {
                self.seq_until_processed = InvalidBlockNumber;
            }
        }

        // Zap this queue entry, or it would appear as a forwarded buffer. If
        // it's potentially in the overflow zone, also zap the copy.
        self.buffers[oldest_buffer_index as usize] = InvalidBuffer;
        if (oldest_buffer_index as i32) < self.io_combine_limit as i32 - 1 {
            self.buffers[self.queue_size as usize + oldest_buffer_index as usize] = InvalidBuffer;
        }

        // (CLOBBER_FREED_MEMORY / USE_VALGRIND per-buffer wipe — no-op here.)

        // Pin transferred to caller.
        debug_assert!(self.pinned_buffers > 0);
        self.pinned_buffers -= 1;

        // Advance oldest buffer, with wrap-around.
        self.oldest_buffer_index += 1;
        if self.oldest_buffer_index == self.queue_size {
            self.oldest_buffer_index = 0;
        }

        // Prepare for the next call.
        self.read_stream_look_ahead()?;

        // See if we can take the fast path for all-cached scans next time.
        if self.ios_in_progress == 0
            && self.forwarded_buffers == 0
            && self.pinned_buffers == 1
            && self.distance == 1
            && self.pending_read_nblocks == 0
            && self.per_buffer_data_size == 0
        {
            // Clear any overflow copy the fast path won't clear out.
            if (self.oldest_buffer_index as i32) < self.io_combine_limit as i32 - 1 {
                self.buffers
                    [self.queue_size as usize + self.oldest_buffer_index as usize] = InvalidBuffer;
            }
            self.fast_path = true;
        }

        let pbd = if self.per_buffer_data_size > 0 {
            Some(oldest_buffer_index)
        } else {
            None
        };
        Ok((buffer, pbd))
    }

    /// Read access to one queue slot's per-buffer data (the C consumer reads the
    /// pointer returned by `read_stream_next_buffer`'s `*per_buffer_data`).
    pub fn per_buffer_data(&self, buffer_index: i16) -> &[u8] {
        let sz = self.per_buffer_data_size;
        let start = sz * buffer_index as usize;
        &self.per_buffer_data[start..start + sz]
    }

    /// `read_stream_next_block(stream, &strategy)` (read_stream.c:1030) — return
    /// and consume the next block the look-ahead would read, plus the strategy
    /// that would be used. Transitional support for callers that do I/O
    /// themselves.
    pub fn read_stream_next_block(&mut self) -> (BlockNumber, bool) {
        // *strategy = stream->ios[0].op.strategy; the op carries the stream-wide
        // strategy from begin time. The transitional caller only needs whether a
        // ring is in use (io_context != NORMAL).
        let has_strategy =
            self.io_context != types_storage::buf::IOContext::IOCONTEXT_NORMAL;
        let blocknum = self.read_stream_get_block(None);
        (blocknum, has_strategy)
    }

    /// `read_stream_reset(stream)` (read_stream.c:1043) — release queued buffers
    /// so the stream can be reused for different blocks.
    pub fn read_stream_reset(&mut self) -> PgResult<()> {
        let bm = bm_global();

        // Stop looking ahead.
        self.distance = 0;

        // Forget buffered block number and fast path state.
        self.buffered_blocknum = InvalidBlockNumber;
        self.fast_path = false;

        // Unpin anything that wasn't consumed.
        loop {
            let (buffer, _) = self.read_stream_next_buffer()?;
            if buffer == InvalidBuffer {
                break;
            }
            bm.ReleaseBuffer(buffer)?;
        }

        // Unpin any unused forwarded buffers.
        let mut index = self.next_buffer_index;
        while index < self.initialized_buffers && self.buffers[index as usize] != InvalidBuffer {
            let buffer = self.buffers[index as usize];
            debug_assert!(self.forwarded_buffers > 0);
            self.forwarded_buffers -= 1;
            bm.ReleaseBuffer(buffer)?;

            self.buffers[index as usize] = InvalidBuffer;
            if (index as i32) < self.io_combine_limit as i32 - 1 {
                self.buffers[self.queue_size as usize + index as usize] = InvalidBuffer;
            }

            index += 1;
            if index == self.queue_size {
                index = 0;
            }
        }

        debug_assert_eq!(self.forwarded_buffers, 0);
        debug_assert_eq!(self.pinned_buffers, 0);
        debug_assert_eq!(self.ios_in_progress, 0);

        // Start off assuming data is cached.
        self.distance = 1;
        Ok(())
    }
}

// === Begin ==================================================================

/// `read_stream_begin_impl(...)` (read_stream.c:537) — the shared constructor.
#[allow(clippy::too_many_arguments)]
fn read_stream_begin_impl<'mcx>(
    flags: i32,
    strategy: BufferAccessStrategy,
    rel: &'mcx Relation<'mcx>,
    spc_oid: types_core::Oid,
    rel_number: types_core::Oid,
    is_catalog_rel: bool,
    is_temp: bool,
    persistence: u8,
    forknum: ForkNumber,
    callback: ReadStreamBlockNumberCB<'mcx>,
    per_buffer_data_size: usize,
) -> PgResult<Box<ReadStream<'mcx>>> {
    let _ = persistence;

    // IOContextForStrategy(strategy) — the pg_stat_io context for this stream's
    // reads (IOCONTEXT_NORMAL when no ring).
    let io_context = {
        use types_storage::buf::{BufferAccessStrategyType as Bas, IOContext};
        match &strategy {
            None => IOContext::IOCONTEXT_NORMAL,
            Some(s) => match s.borrow().btype {
                Bas::BasNormal => IOContext::IOCONTEXT_NORMAL,
                Bas::BasBulkread => IOContext::IOCONTEXT_BULKREAD,
                Bas::BasBulkwrite => IOContext::IOCONTEXT_BULKWRITE,
                Bas::BasVacuum => IOContext::IOCONTEXT_VACUUM,
            },
        }
    };

    // Decide how many I/Os we will allow to run at the same time.
    let tablespace_id = spc_oid;
    let my_database_id = backend_utils_init_small_seams::my_database_id::call();
    let my_database_tablespace = backend_utils_init_small_seams::my_database_table_space::call();
    let oid_is_valid = my_database_id != types_core::InvalidOid;

    let mut max_ios: i32 = if !oid_is_valid
        || is_catalog_rel
        || backend_catalog_catalog::IsCatalogRelationOid(rel_number)
    {
        // Avoid circularity before spccache.c is ready.
        bufmgr_seams::effective_io_concurrency::call()
    } else if (flags & READ_STREAM_MAINTENANCE) != 0 {
        backend_utils_cache_spccache::get_tablespace_maintenance_io_concurrency(
            tablespace_id,
            my_database_tablespace,
            bufmgr_seams::maintenance_io_concurrency::call(),
        )?
    } else {
        backend_utils_cache_spccache::get_tablespace_io_concurrency(
            tablespace_id,
            my_database_tablespace,
            bufmgr_seams::effective_io_concurrency::call(),
        )?
    };

    // Cap to INT16_MAX to avoid overflowing below.
    max_ios = i32::min(max_ios, PG_INT16_MAX);

    let io_combine_limit = bufmgr_seams::io_combine_limit::call();

    // Maximum extra space we could need for overflowing buffers.
    let queue_overflow: i32 = io_combine_limit - 1;

    // Choose the maximum number of buffers we're prepared to pin.
    let mut max_pinned_buffers: i64 = (max_ios as i64 + 1) * io_combine_limit as i64;
    max_pinned_buffers = i64::min(
        max_pinned_buffers,
        (PG_INT16_MAX - queue_overflow - 1) as i64,
    );

    // Give the strategy a chance to limit the number of buffers we pin.
    let strategy_pin_limit = bm_get_access_strategy_pin_limit(&strategy);
    max_pinned_buffers = i64::min(strategy_pin_limit as i64, max_pinned_buffers);

    // Also limit to the maximum number of pins we could ever acquire.
    let bm = bm_global();
    let max_possible_buffer_limit: u32 = if is_temp {
        bm_get_local_pin_limit()
    } else {
        bm.GetPinLimit()
    };
    max_pinned_buffers = i64::min(max_pinned_buffers, max_possible_buffer_limit as i64);

    // Need at least one to make progress.
    max_pinned_buffers = i64::max(1, max_pinned_buffers);

    // One extra entry for buffers/per-buffer data: consumers keep access to the
    // per-buffer object until the next next_buffer() call.
    let queue_size: i32 = max_pinned_buffers as i32 + 1;

    let sync_mode = bufmgr_seams::io_method_sync::call();
    let batch_mode = (flags & READ_STREAM_USE_BATCHING) != 0;

    // Read-ahead advice simulating async I/O with synchronous calls. (USE_PREFETCH)
    let advice_enabled = sync_mode
        && !bufmgr_seams::io_direct_data::call()
        && (flags & READ_STREAM_SEQUENTIAL) == 0
        && max_ios > 0;

    // Setting max_ios to zero disables AIO + advice but still needs space for
    // one I/O. Bump to one, and remember to ask for synchronous I/O only.
    let mut read_buffers_flags = 0u32;
    if max_ios == 0 {
        max_ios = 1;
        read_buffers_flags = READ_BUFFERS_SYNCHRONOUSLY;
    }

    let ios_len = i32::max(1, max_ios) as usize;
    let buffers_len = (queue_size + queue_overflow) as usize;

    let mut ios = Vec::with_capacity(ios_len);
    for _ in 0..ios_len {
        ios.push(InProgressIO::default());
    }

    let distance = if (flags & READ_STREAM_FULL) != 0 {
        i32::min(max_pinned_buffers as i32, io_combine_limit) as i16
    } else {
        1
    };

    let stream = ReadStream {
        max_ios: max_ios as i16,
        io_combine_limit: io_combine_limit as i16,
        ios_in_progress: 0,
        queue_size: queue_size as i16,
        max_pinned_buffers: max_pinned_buffers as i16,
        forwarded_buffers: 0,
        pinned_buffers: 0,
        distance,
        initialized_buffers: 0,
        read_buffers_flags,
        sync_mode,
        batch_mode,
        advice_enabled,
        temporary: is_temp,
        buffered_blocknum: InvalidBlockNumber,
        callback,
        seq_blocknum: InvalidBlockNumber,
        seq_until_processed: InvalidBlockNumber,
        pending_read_blocknum: 0,
        pending_read_nblocks: 0,
        per_buffer_data_size,
        per_buffer_data: vec![0u8; per_buffer_data_size * queue_size as usize],
        ios,
        oldest_io_index: 0,
        next_io_index: 0,
        fast_path: false,
        oldest_buffer_index: 0,
        next_buffer_index: 0,
        buffers: vec![InvalidBuffer; buffers_len],
        rel,
        forknum,
        io_context,
    };

    Ok(Box::new(stream))
}

/// `read_stream_begin_relation(flags, strategy, rel, forknum, callback,
/// callback_private_data, per_buffer_data_size)` (read_stream.c:736) — begin a
/// read stream for one fork of a relation.
#[allow(clippy::too_many_arguments)]
pub fn read_stream_begin_relation<'mcx>(
    flags: i32,
    strategy: BufferAccessStrategy,
    rel: &'mcx Relation<'mcx>,
    forknum: ForkNumber,
    callback: ReadStreamBlockNumberCB<'mcx>,
    per_buffer_data_size: usize,
) -> PgResult<Box<ReadStream<'mcx>>> {
    // smgr = RelationGetSmgr(rel); persistence = rel->rd_rel->relpersistence.
    let spc_oid = rel.rd_locator.spcOid;
    let rel_number = rel.rd_locator.relNumber;
    let is_catalog_rel = backend_catalog_catalog::IsCatalogRelation(rel);
    let is_temp = rel.rd_backend != types_core::primitive::INVALID_PROC_NUMBER;
    read_stream_begin_impl(
        flags,
        strategy,
        rel,
        spc_oid,
        rel_number,
        is_catalog_rel,
        is_temp,
        rel.rd_rel.relpersistence,
        forknum,
        callback,
        per_buffer_data_size,
    )
}

/// `block_range_read_stream_cb(stream, callback_private_data, per_buffer_data)`
/// (read_stream.c:161) — the general-use callback for block-range scans over
/// `[current_blocknum, last_exclusive)`. Build a `ReadStreamBlockNumberCB`
/// closure over the shared private state so the consumer can update
/// `last_exclusive` between range passes.
pub fn block_range_read_stream_cb<'mcx>(
    private: alloc::rc::Rc<core::cell::RefCell<BlockRangeReadStreamPrivate>>,
) -> ReadStreamBlockNumberCB<'mcx> {
    Box::new(move |_per_buffer_data: &mut [u8]| {
        let mut p = private.borrow_mut();
        if p.current_blocknum < p.last_exclusive {
            let b = p.current_blocknum;
            p.current_blocknum += 1;
            b
        } else {
            InvalidBlockNumber
        }
    })
}

/// `read_stream_end(stream)` (read_stream.c:1088) — release and free the stream.
/// The `Box<ReadStream>` is dropped by the caller; this performs the
/// `read_stream_reset` that releases any still-pinned buffers.
pub fn read_stream_end(mut stream: Box<ReadStream>) -> PgResult<()> {
    stream.read_stream_reset()?;
    // pfree(stream): the Box is dropped on return.
    Ok(())
}

// === Helpers bridging the buffer-support pin-limit accessors ================

/// `GetAccessStrategyPinLimit(strategy)` (freelist.c) — the per-strategy pin
/// cap. A NULL (`None`) strategy returns `NBuffers` (effectively unbounded; the
/// real cap is `max_possible_buffer_limit` below). The non-NULL ring cap
/// (`BAS_BULKREAD` == ring size, others == ring/2) is the
/// [`BufferAccessStrategyRing::GetAccessStrategyPinLimit`] method on the ring
/// object, read here through the by-pointer handle (the ring is shared, so this
/// borrows it).
fn bm_get_access_strategy_pin_limit(strategy: &BufferAccessStrategy) -> i32 {
    match strategy {
        None => backend_utils_init_small_seams::nbuffers::call(),
        Some(ring) => ring.borrow().GetAccessStrategyPinLimit(),
    }
}

/// `GetAdditionalPinLimit()` (bufmgr.c) — how many more buffers this backend can
/// pin in shared memory.
fn bm_get_additional_pin_limit(bm: &BufferManager) -> i32 {
    let v = bm.GetAdditionalPinLimit();
    i32::try_from(v).unwrap_or(PG_INT16_MAX)
}

/// `GetAdditionalLocalPinLimit()` (localbuf.c) — the local (temp-relation)
/// buffer pin limit (pins available beyond those already held). A
/// temp-relation read stream reaches this here; it delegates to the real
/// localbuf accessor on THIS backend's ambient [`LocalBufferManager`]. The
/// local pool is established before any temp-relation read stream runs (the
/// stream pins the relation's local buffers), so the manager is published.
fn bm_get_additional_local_pin_limit() -> i32 {
    let lbm = LocalBufferManager::global().expect(
        "read_stream: the local (temp-relation) buffer manager is not registered \
         for this process",
    );
    i32::try_from(lbm.GetAdditionalLocalPinLimit()).unwrap_or(PG_INT16_MAX)
}

/// `GetLocalPinLimit()` (localbuf.c) — see [`bm_get_additional_local_pin_limit`].
fn bm_get_local_pin_limit() -> u32 {
    let lbm = LocalBufferManager::global().expect(
        "read_stream: the local (temp-relation) buffer manager is not registered \
         for this process",
    );
    lbm.GetLocalPinLimit()
}

// === AIO batch mode =========================================================
//
// `pgaio_enter_batchmode()` / `pgaio_exit_batchmode()` (aio.c) bracket a region
// in which I/O is staged for batched submission rather than submitted one at a
// time. Under the synchronous I/O method (IOMETHOD_SYNC), which is the engine
// the buffer manager rides here, there is no separately-staged AIO queue: every
// `StartReadBuffers` performs (or queues for `WaitReadBuffers`) its read inline,
// so `pgaio_have_staged()`/`pgaio_submit_staged()` reduce to constant no-ops —
// exactly the posture bufmgr's read path already takes for
// `ReadBuffersCanStartIO`. Entering/leaving batch mode is therefore a no-op at
// this layer. (When the asynchronous pgaio engine lands (#15), these become real
// `pgaio_enter_batchmode`/`pgaio_exit_batchmode` calls into it.)

/// `pgaio_enter_batchmode()` (aio.c) — no-op under the synchronous engine.
fn pgaio_enter_batchmode() {}

/// `pgaio_exit_batchmode()` (aio.c) — no-op under the synchronous engine.
fn pgaio_exit_batchmode() {}

/// This crate declares no inward seams (consumers depend on it directly). It is
/// a leaf above the buffer manager; nothing installs into it.
pub fn init_seams() {}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::rc::Rc;
    use core::cell::RefCell;

    /// `block_range_read_stream_cb` emits `[current, last_exclusive)` and stops
    /// at `InvalidBlockNumber`, advancing `current_blocknum` as it goes — the C
    /// `p->current_blocknum++` post-increment semantics.
    #[test]
    fn block_range_cb_iterates_and_stops() {
        let p = Rc::new(RefCell::new(BlockRangeReadStreamPrivate {
            current_blocknum: 5,
            last_exclusive: 8,
        }));
        let mut cb = block_range_read_stream_cb(p.clone());
        let mut empty: [u8; 0] = [];
        assert_eq!(cb(&mut empty), 5);
        assert_eq!(cb(&mut empty), 6);
        assert_eq!(cb(&mut empty), 7);
        assert_eq!(cb(&mut empty), InvalidBlockNumber);
        // current_blocknum advanced to last_exclusive.
        assert_eq!(p.borrow().current_blocknum, 8);
    }

    /// Updating `last_exclusive` between passes (as `btvacuumscan` does after
    /// re-checking the relation length) resumes emission from the current point.
    #[test]
    fn block_range_cb_resumes_after_extending_bound() {
        let p = Rc::new(RefCell::new(BlockRangeReadStreamPrivate {
            current_blocknum: 1,
            last_exclusive: 2,
        }));
        let mut cb = block_range_read_stream_cb(p.clone());
        let mut empty: [u8; 0] = [];
        assert_eq!(cb(&mut empty), 1);
        assert_eq!(cb(&mut empty), InvalidBlockNumber);
        // The consumer extends the bound and the same stream resumes.
        p.borrow_mut().last_exclusive = 4;
        assert_eq!(cb(&mut empty), 2);
        assert_eq!(cb(&mut empty), 3);
        assert_eq!(cb(&mut empty), InvalidBlockNumber);
    }
}

const _: fn() = || {
    // Keep `format`/`PgError` imported for the read-path error rendering the
    // bufmgr layer surfaces; no read-stream-local ereport beyond the bufmgr
    // pass-through exists.
    let _ = |s: &str| -> PgError { PgError::error(format!("{s}")) };
};
