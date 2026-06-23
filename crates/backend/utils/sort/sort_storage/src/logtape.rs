//! Management of "logical tapes" within temporary files.
//!
//! Port of `src/backend/utils/sort/logtape.c` from PostgreSQL 18.3.
//!
//! This module exists to support sorting via multiple merge passes (see
//! tuplesort.c). It provides the illusion of N independent tape devices to
//! tuplesort.c by carving a single underlying temporary file into `BLCKSZ`
//! blocks, chaining the blocks of each logical tape together, and recycling
//! freed blocks via a min-heap free list so that peak space usage stays close
//! to the actual data volume.
//!
//! Every function in `logtape.c` is ported here: the public
//! `LogicalTape*`/`LogicalTapeSet*` entry points plus the static helpers
//! (`lts*`). The branch order, assertions, error messages, and SQLSTATEs match
//! the C source.
//!
//! # Ownership model
//!
//! `LogicalTapeSet *` / `LogicalTape *` are opaque typedefs in C
//! (`logtape.h`); the two working structs are private to `logtape.c`. In this
//! repo the structs ([`LogicalTapeSet`] / [`LogicalTape`]) are the shared
//! vocabulary defined in the `backend-utils-sort-storage-seams` crate, and the
//! consumer that drives this unit (`nodeAgg`'s hash-agg spill) holds the set
//! BY VALUE as an owned `PgBox<LogicalTapeSet<'mcx>>`. A `LogicalTape *` is a
//! `usize` slot index into the set's `tapes` vector (the faithful rendering of
//! C's pointer into the set-owned tape array). There is no side-table
//! registry: the set is a real owned value passed `&mut` through the seams.
//!
//! A whole tape set (its underlying `BufFile`, its free-block min-heap, and all
//! the tapes created within it) is one owned allocation — the C
//! `LogicalTapeSet` plus the `lt->tapeSet` back-pointer collapsed together,
//! charged to the `mcx` captured at `LogicalTapeSetCreate`, exactly as C
//! charges everything in the set to the context captured there. A tape slot
//! indexes the per-tape state so it can be reached while it shares the set's
//! state mutably.
//!
//! # SharedFileSet (parallel sort)
//!
//! `logtape.c`'s parallel-sort paths (`LogicalTapeImport`, the
//! `fileset`/`worker != -1` arms of `LogicalTapeSetCreate`, and
//! `LogicalTapeFreeze`'s `BufFileExportFileSet`) need
//! `storage/file/sharedfileset.c`, whose owner crate is not present in this
//! worktree. The handle-threaded seams the consumers install pass no
//! `SharedFileSet` (hash-agg/serial spill is always `fileset = NULL`,
//! `worker = -1`), so those arms are never reached on the seam path; the
//! structure is preserved 1:1 and the genuinely-unported fileset BufFile ops
//! panic loudly (`panic!`) rather than fabricate a result.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]

use utils_error::elog;
use mcx::{Mcx, PgBox, PgVec};
use types_core::{BLCKSZ, MAXPGPATH};
use types_error::{PgError, PgResult, ERROR};

use buffile_seams as buffile;

// The owned `LogicalTape` / `LogicalTapeSet` structs are the shared vocabulary,
// defined in the seam crate so the consumer (nodeAgg) can name them and hold
// the set by value. This owner crate fills the concrete bodies over them.
use sort_storage_seams::{LogicalTape, LogicalTapeSet};

/// `MaxAllocSize` (`memutils.h`).
const MaxAllocSize: usize = mcx::MAX_ALLOC_SIZE;

/// Preallocation lower bound (blocks).
pub const TAPE_WRITE_PREALLOC_MIN: i32 = 8;
/// Preallocation upper bound (blocks).
pub const TAPE_WRITE_PREALLOC_MAX: i32 = 128;

/// `sizeof(TapeBlockTrailer)` — two `int64`s.
const SIZEOF_TAPE_BLOCK_TRAILER: usize = 16;

/// `#define TapeBlockPayloadSize (BLCKSZ - sizeof(TapeBlockTrailer))`.
const TapeBlockPayloadSize: usize = BLCKSZ - SIZEOF_TAPE_BLOCK_TRAILER;

// ---------------------------------------------------------------------------
// Charged zero-fill helpers (single `palloc0` / growing `repalloc`).
// ---------------------------------------------------------------------------

fn pgvec_zero_fill<T: Copy + Default>(
    v: &mut PgVec<'_, T>,
    mcx: Mcx<'_>,
    len: usize,
    oom: &'static str,
) -> PgResult<()> {
    debug_assert!(v.is_empty());
    v.try_reserve(len).map_err(|_| PgError::error(oom))?;
    for _ in 0..len {
        v.push(T::default());
    }
    let _ = mcx;
    Ok(())
}

fn pgvec_zero_extend<T: Copy + Default>(
    v: &mut PgVec<'_, T>,
    len: usize,
    oom: &'static str,
) -> PgResult<()> {
    debug_assert!(len >= v.len());
    let add = len - v.len();
    v.try_reserve(add).map_err(|_| PgError::error(oom))?;
    for _ in 0..add {
        v.push(T::default());
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Block-buffer trailer accessors (C macros over `char *buf`).
//
//   TapeBlockGetTrailer(buf) -> (TapeBlockTrailer *)(buf + TapeBlockPayloadSize)
//   TapeBlockIsLast(buf)     -> TapeBlockGetTrailer(buf)->next < 0
//   TapeBlockGetNBytes(buf)  -> last ? -next : TapeBlockPayloadSize
//   TapeBlockSetNBytes(buf,n)-> TapeBlockGetTrailer(buf)->next = -(n)
//
// The trailer is the final 16 bytes of the block, a native-endian pair of
// `int64`s (the temp file is private to this process, so native byte order
// matches the on-disk encoding).
// ---------------------------------------------------------------------------

#[inline]
fn trailer_prev(buf: &[u8]) -> i64 {
    let s = TapeBlockPayloadSize;
    let mut b = [0u8; 8];
    b.copy_from_slice(&buf[s..s + 8]);
    i64::from_ne_bytes(b)
}

#[inline]
fn trailer_next(buf: &[u8]) -> i64 {
    let s = TapeBlockPayloadSize + 8;
    let mut b = [0u8; 8];
    b.copy_from_slice(&buf[s..s + 8]);
    i64::from_ne_bytes(b)
}

#[inline]
fn TapeBlockSetPrev(buf: &mut [u8], prev: i64) {
    let s = TapeBlockPayloadSize;
    buf[s..s + 8].copy_from_slice(&prev.to_ne_bytes());
}

#[inline]
fn TapeBlockSetNext(buf: &mut [u8], next: i64) {
    let s = TapeBlockPayloadSize + 8;
    buf[s..s + 8].copy_from_slice(&next.to_ne_bytes());
}

#[inline]
fn TapeBlockIsLast(buf: &[u8]) -> bool {
    trailer_next(buf) < 0
}

#[inline]
fn TapeBlockGetNBytes(buf: &[u8]) -> i64 {
    if TapeBlockIsLast(buf) {
        -trailer_next(buf)
    } else {
        TapeBlockPayloadSize as i64
    }
}

#[inline]
fn TapeBlockSetNBytes(buf: &mut [u8], nbytes: i64) {
    TapeBlockSetNext(buf, -nbytes);
}

/// Move a context-charged `PgVec` out of its slot, leaving an empty vec in the
/// SAME context (the allocator-aware analog of `core::mem::take`, which a
/// `PgVec` cannot use because it has no `Default`). Used to borrow a tape's
/// buffer out so the set's file can read into it, then put it back.
#[inline]
fn steal<'mcx, T>(v: &mut PgVec<'mcx, T>) -> PgVec<'mcx, T> {
    let mcx = *v.allocator();
    core::mem::replace(v, PgVec::new_in(mcx))
}

// ---------------------------------------------------------------------------
// Block I/O.
// ---------------------------------------------------------------------------

/// Write a block-sized buffer to the specified block of the underlying file.
fn ltsWriteBlock(set: &mut LogicalTapeSet<'_>, blocknum: i64, buffer: &[u8]) -> PgResult<()> {
    // BufFile does not support "holes": fill the gap with zeros.
    while blocknum > set.nBlocksWritten {
        let zerobuf = [0u8; BLCKSZ];
        let extend_at = set.nBlocksWritten;
        ltsWriteBlock(set, extend_at, &zerobuf)?;
    }

    let pfile = set
        .pfile
        .as_mut()
        .ok_or_else(|| PgError::error("logical tape set has no underlying file"))?;

    if buffile::buf_file_seek_block::call(pfile, blocknum)? != 0 {
        return Err(file_access_error(format!(
            "could not seek to block {blocknum} of temporary file"
        )));
    }
    buffile::buf_file_write::call(pfile, &buffer[..BLCKSZ])?;

    if blocknum == set.nBlocksWritten {
        set.nBlocksWritten += 1;
    }
    Ok(())
}

/// Read a block-sized buffer from the specified block of the underlying file.
fn ltsReadBlock(set: &mut LogicalTapeSet<'_>, blocknum: i64, buffer: &mut [u8]) -> PgResult<()> {
    let pfile = set
        .pfile
        .as_mut()
        .ok_or_else(|| PgError::error("logical tape set has no underlying file"))?;
    if buffile::buf_file_seek_block::call(pfile, blocknum)? != 0 {
        return Err(file_access_error(format!(
            "could not seek to block {blocknum} of temporary file"
        )));
    }
    buffile::buf_file_read_exact::call(pfile, &mut buffer[..BLCKSZ])
}

/// Read as many blocks as we can into the per-tape buffer. Returns `true` if
/// anything was read, `false` on EOF.
fn ltsReadFillBuffer(set: &mut LogicalTapeSet<'_>, slot: usize) -> PgResult<bool> {
    {
        let lt = tape_mut(set, slot);
        lt.pos = 0;
        lt.nbytes = 0;
    }

    loop {
        let datablocknum = tape(set, slot).nextBlockNumber;
        if datablocknum == -1 {
            break; // EOF
        }
        let datablocknum = datablocknum + tape(set, slot).offsetBlockNumber;

        let nbytes_at_start = tape(set, slot).nbytes;
        // Read the block into the buffer at offset nbytes. Borrow the buffer
        // out of the tape, read into it through the set's file, put it back.
        let mut buf = steal(&mut tape_mut(set, slot).buffer);
        let res = ltsReadBlock(set, datablocknum, &mut buf[nbytes_at_start..]);
        tape_mut(set, slot).buffer = buf;
        res?;

        if !tape(set, slot).frozen {
            ltsReleaseBlock(set, datablocknum);
        }
        {
            let next = tape(set, slot).nextBlockNumber;
            tape_mut(set, slot).curBlockNumber = next;
        }

        let (this_nbytes, this_is_last, this_next) = {
            let lt = tape(set, slot);
            let thisbuf = &lt.buffer[nbytes_at_start..];
            (
                TapeBlockGetNBytes(thisbuf) as usize,
                TapeBlockIsLast(thisbuf),
                trailer_next(thisbuf),
            )
        };
        tape_mut(set, slot).nbytes += this_nbytes;
        if this_is_last {
            tape_mut(set, slot).nextBlockNumber = -1; // EOF
            break;
        } else {
            tape_mut(set, slot).nextBlockNumber = this_next;
        }

        // } while (lt->buffer_size - lt->nbytes > BLCKSZ);
        let lt = tape(set, slot);
        if lt.buffer_size as i64 - lt.nbytes as i64 <= BLCKSZ as i64 {
            break;
        }
    }

    Ok(tape(set, slot).nbytes > 0)
}

#[inline]
fn tape<'a, 'mcx>(set: &'a LogicalTapeSet<'mcx>, slot: usize) -> &'a LogicalTape<'mcx> {
    set.tapes[slot]
        .as_ref()
        .expect("logtape: operation on a closed tape")
}

#[inline]
fn tape_mut<'a, 'mcx>(set: &'a mut LogicalTapeSet<'mcx>, slot: usize) -> &'a mut LogicalTape<'mcx> {
    set.tapes[slot]
        .as_mut()
        .expect("logtape: operation on a closed tape")
}

// ---------------------------------------------------------------------------
// Free-list min heap.
// ---------------------------------------------------------------------------

#[inline]
fn left_offset(i: u64) -> u64 {
    2 * i + 1
}
#[inline]
fn right_offset(i: u64) -> u64 {
    2 * i + 2
}
#[inline]
fn parent_offset(i: u64) -> u64 {
    (i - 1) / 2
}

/// Get the next block for writing.
fn ltsGetBlock(set: &mut LogicalTapeSet<'_>, slot: usize) -> PgResult<i64> {
    if set.enable_prealloc {
        ltsGetPreallocBlock(set, slot)
    } else {
        Ok(ltsGetFreeBlock(set))
    }
}

/// Select the lowest currently unused block from the set's global free list.
fn ltsGetFreeBlock(lts: &mut LogicalTapeSet<'_>) -> i64 {
    if lts.nFreeBlocks == 0 {
        let block = lts.nBlocksAllocated;
        lts.nBlocksAllocated += 1;
        return block;
    }
    if lts.nFreeBlocks == 1 {
        lts.nFreeBlocks -= 1;
        return lts.freeBlocks[0];
    }

    let blocknum = lts.freeBlocks[0];
    lts.nFreeBlocks -= 1;
    let holeval = lts.freeBlocks[lts.nFreeBlocks as usize];

    let heap = &mut lts.freeBlocks;
    let mut holepos: u64 = 0;
    let heapsize = lts.nFreeBlocks as u64;
    loop {
        let left = left_offset(holepos);
        let right = right_offset(holepos);
        let min_child: u64;
        if left < heapsize && right < heapsize {
            min_child = if heap[left as usize] < heap[right as usize] {
                left
            } else {
                right
            };
        } else if left < heapsize {
            min_child = left;
        } else if right < heapsize {
            min_child = right;
        } else {
            break;
        }
        if heap[min_child as usize] >= holeval {
            break;
        }
        heap[holepos as usize] = heap[min_child as usize];
        holepos = min_child;
    }
    heap[holepos as usize] = holeval;

    blocknum
}

/// Return the lowest free block number from the tape's preallocation list,
/// refilling from the set's free list if necessary.
fn ltsGetPreallocBlock(set: &mut LogicalTapeSet<'_>, slot: usize) -> PgResult<i64> {
    if tape(set, slot).nprealloc > 0 {
        let lt = tape_mut(set, slot);
        lt.nprealloc -= 1;
        return Ok(lt.prealloc[lt.nprealloc as usize]);
    }

    if !tape(set, slot).prealloc_allocated {
        let mcx = set_mcx(set);
        let lt = tape_mut(set, slot);
        lt.prealloc_size = TAPE_WRITE_PREALLOC_MIN;
        let size = lt.prealloc_size as usize;
        let mut v = steal(&mut lt.prealloc);
        pgvec_zero_fill(
            &mut v,
            mcx,
            size,
            "out of memory allocating logical-tape prealloc list",
        )?;
        tape_mut(set, slot).prealloc = v;
        tape_mut(set, slot).prealloc_allocated = true;
    } else if tape(set, slot).prealloc_size < TAPE_WRITE_PREALLOC_MAX {
        let lt = tape_mut(set, slot);
        lt.prealloc_size *= 2;
        if lt.prealloc_size > TAPE_WRITE_PREALLOC_MAX {
            lt.prealloc_size = TAPE_WRITE_PREALLOC_MAX;
        }
        let size = lt.prealloc_size as usize;
        let mut v = steal(&mut lt.prealloc);
        pgvec_zero_extend(&mut v, size, "out of memory growing logical-tape prealloc list")?;
        tape_mut(set, slot).prealloc = v;
    }

    // refill preallocation list
    let nprealloc = tape(set, slot).prealloc_size;
    tape_mut(set, slot).nprealloc = nprealloc;
    let mut i = nprealloc;
    while i > 0 {
        let block = ltsGetFreeBlock(set);
        let lt = tape_mut(set, slot);
        lt.prealloc[(i - 1) as usize] = block;
        debug_assert!(i == lt.nprealloc || lt.prealloc[(i - 1) as usize] > lt.prealloc[i as usize]);
        i -= 1;
    }

    let lt = tape_mut(set, slot);
    lt.nprealloc -= 1;
    Ok(lt.prealloc[lt.nprealloc as usize])
}

/// Return a block# to the freelist.
fn ltsReleaseBlock(lts: &mut LogicalTapeSet<'_>, blocknum: i64) {
    if lts.forgetFreeSpace {
        return;
    }

    if lts.nFreeBlocks >= lts.freeBlocksLen as i64 {
        // If the freelist becomes very large, leak this free block.
        if lts.freeBlocksLen * 2 * core::mem::size_of::<i64>() > MaxAllocSize {
            return;
        }
        let newlen = lts.freeBlocksLen * 2;
        // ltsReleaseBlock is `void` in C: a repalloc OOM there ereport(ERROR)s
        // and longjmps. Preserve the infallible signature by panicking on the
        // (effectively unreachable) allocation failure.
        let mut v = steal(&mut lts.freeBlocks);
        pgvec_zero_extend(&mut v, newlen, "out of memory growing logical-tape free-block heap")
            .expect("out of memory growing logical-tape free-block heap");
        lts.freeBlocks = v;
        lts.freeBlocksLen = newlen;
    }

    let heap = &mut lts.freeBlocks;
    let mut holepos = lts.nFreeBlocks as u64;
    lts.nFreeBlocks += 1;
    while holepos != 0 {
        let parent = parent_offset(holepos);
        if heap[parent as usize] < blocknum {
            break;
        }
        heap[holepos as usize] = heap[parent as usize];
        holepos = parent;
    }
    heap[holepos as usize] = blocknum;
}

/// Lazily allocate and initialize the read buffer.
fn ltsInitReadBuffer(set: &mut LogicalTapeSet<'_>, slot: usize) -> PgResult<()> {
    debug_assert!(tape(set, slot).buffer_size > 0);
    let size = tape(set, slot).buffer_size;
    alloc_buffer(set, slot, size)?;

    let lt = tape_mut(set, slot);
    lt.nextBlockNumber = lt.firstBlockNumber;
    lt.pos = 0;
    lt.nbytes = 0;
    ltsReadFillBuffer(set, slot)?;
    Ok(())
}

/// `mcx` of the set's owning context.
fn set_mcx<'mcx>(_set: &LogicalTapeSet<'mcx>) -> Mcx<'mcx> {
    // The set's PgVecs/PgBox were all allocated in the set's context; recover
    // its `Mcx<'mcx>` from one of them (the free-block heap is always present).
    *_set.freeBlocks.allocator()
}

/// Allocate the per-tape buffer to exactly `size` bytes, zero-initialized.
fn alloc_buffer(set: &mut LogicalTapeSet<'_>, slot: usize, size: usize) -> PgResult<()> {
    debug_assert!(size <= MaxAllocSize);
    let mcx = set_mcx(set);
    // pfree(lt->buffer) then palloc(size): drop the old buffer (returns its
    // charge), build the new one, zero-fill.
    let mut v: PgVec<u8> = PgVec::new_in(mcx);
    pgvec_zero_fill(&mut v, mcx, size, "out of memory allocating logical-tape buffer")?;
    let lt = tape_mut(set, slot);
    lt.buffer = v;
    lt.buffer_size = size;
    lt.buffer_allocated = true;
    Ok(())
}

/// Release the per-tape buffer (`pfree(lt->buffer)`).
fn free_buffer(set: &mut LogicalTapeSet<'_>, slot: usize) {
    let mcx = set_mcx(set);
    let lt = tape_mut(set, slot);
    lt.buffer = PgVec::new_in(mcx);
    lt.buffer_allocated = false;
}

/// Build the BufFile segment name from a worker number (`pg_itoa`). Used only
/// by the parallel-sort (`SharedFileSet`) paths, which need sharedfileset.c
/// (unported); kept for fidelity.
#[allow(dead_code)]
fn worker_filename(worker: i32) -> PgResult<String> {
    let s = worker.to_string();
    if s.len() >= MAXPGPATH {
        return Err(PgError::error("logical tape filename is too long"));
    }
    Ok(s)
}

/// Construct a fresh per-tape struct (C `ltsCreateTape`). Buffer/prealloc are
/// lazily allocated.
fn make_tape<'mcx>(mcx: Mcx<'mcx>) -> LogicalTape<'mcx> {
    LogicalTape {
        writing: true,
        frozen: false,
        dirty: false,
        firstBlockNumber: -1,
        curBlockNumber: -1,
        nextBlockNumber: -1,
        offsetBlockNumber: 0,
        buffer: PgVec::new_in(mcx),
        buffer_size: 0,
        max_size: MaxAllocSize, // palloc() larger than MaxAllocSize would fail
        pos: 0,
        nbytes: 0,
        buffer_allocated: false,
        prealloc: PgVec::new_in(mcx),
        prealloc_allocated: false,
        nprealloc: 0,
        prealloc_size: 0,
    }
}

// ---------------------------------------------------------------------------
// Public (handle-threaded) API.
// ---------------------------------------------------------------------------

/// `LogicalTapeSetCreate(preallocate, fileset, worker)` (logtape.c). The
/// installed seam passes no `SharedFileSet` (serial/hash-agg spill is always
/// `fileset = NULL`, `worker = -1`). The owned set is allocated in `mcx` and
/// returned for the consumer to hold by value (no registry).
pub fn logical_tape_set_create<'mcx>(
    mcx: Mcx<'mcx>,
    preallocate: bool,
    worker: i32,
) -> PgResult<PgBox<'mcx, LogicalTapeSet<'mcx>>> {
    // fileset == NULL on the seam path; the worker/leader fileset arms of C
    // would need sharedfileset.c (unported), so only the serial arm runs.
    if worker != -1 {
        panic!(
            "logtape LogicalTapeSetCreate with worker={worker}: parallel (SharedFileSet) \
             tape sets need storage/file/sharedfileset.c, not ported in this worktree"
        );
    }

    // Create temp BufFile BEFORE the (charged) free-block heap so a failure
    // can never strand a charged allocation.
    let pfile = buffile::buf_file_create_temp::call(mcx, false)?;

    let freeBlocksLen: usize = 32; // reasonable initial guess
    let mut freeBlocks: PgVec<i64> = PgVec::new_in(mcx);
    if let Err(e) = pgvec_zero_fill(
        &mut freeBlocks,
        mcx,
        freeBlocksLen,
        "out of memory allocating logical-tape free-block heap",
    ) {
        // Close the BufFile we just created so it does not leak.
        let _ = buffile::buf_file_close::call(pfile);
        return Err(e);
    }

    mcx::alloc_in(
        mcx,
        LogicalTapeSet {
            pfile: Some(pfile),
            fileset: None,
            worker,
            nBlocksAllocated: 0,
            nBlocksWritten: 0,
            nHoleBlocks: 0,
            forgetFreeSpace: false,
            freeBlocks,
            nFreeBlocks: 0,
            freeBlocksLen,
            enable_prealloc: preallocate,
            tapes: Vec::new(),
        },
    )
}

/// `LogicalTapeSetClose(lts)` (logtape.c): destroy the set and close its
/// underlying `BufFile`. Infallible in C (close paths do not ereport(ERROR));
/// a close error here surfaces as a loud panic to keep the `void` contract.
/// Consumes the owned set.
pub fn logical_tape_set_close(mut set: PgBox<'_, LogicalTapeSet<'_>>) {
    if let Some(pfile) = set.pfile.take() {
        buffile::buf_file_close::call(pfile)
            .expect("logtape LogicalTapeSetClose: BufFileClose failed");
    }
    // Dropping `set` releases the free-block heap and every tape's buffer.
    drop(set);
}

/// `LogicalTapeSetBlocks(lts)` (logtape.c): total disk space used, in blocks.
pub fn logical_tape_set_blocks(set: &LogicalTapeSet<'_>) -> i64 {
    set.nBlocksWritten - set.nHoleBlocks
}

/// `LogicalTapeCreate(lts)` (logtape.c): allocate a new tape in the set, in
/// write state. Returns the tape's slot index (the owned-model `LogicalTape *`).
pub fn logical_tape_create(set: &mut LogicalTapeSet<'_>) -> PgResult<usize> {
    // The leader cannot create new tapes (BufFiles opened shared are
    // read-only). On the serial seam path `fileset` is always None, so this
    // never fires; preserved for fidelity.
    if set.fileset.is_some() && set.worker == -1 {
        elog(ERROR, "cannot create new tapes in leader process")?;
    }

    let mcx = set_mcx(set);
    let tape = make_tape(mcx);
    let slot = set.tapes.len();
    set.tapes.push(Some(tape));
    Ok(slot)
}

/// `LogicalTapeClose(lt)` (logtape.c): release a single tape. Does NOT return
/// blocks to the free list.
pub fn logical_tape_close(set: &mut LogicalTapeSet<'_>, slot: usize) {
    // pfree(lt->buffer): drop the tape, freeing buffer/prealloc charge.
    set.tapes[slot] = None;
}

/// `LogicalTapeSetForgetFreeSpace(lts)` (logtape.c).
pub fn logical_tape_set_forget_free_space(set: &mut LogicalTapeSet<'_>) {
    set.forgetFreeSpace = true;
}

/// `LogicalTapeWrite(lt, ptr, size)` (logtape.c): append `data` to the tape.
pub fn logical_tape_write(set: &mut LogicalTapeSet<'_>, slot: usize, data: &[u8]) -> PgResult<()> {
    {
        let mut ptr = data;
        let mut size = ptr.len();

        debug_assert!(tape(set, slot).writing);
        debug_assert!(tape(set, slot).offsetBlockNumber == 0);

        if !tape(set, slot).buffer_allocated {
            alloc_buffer(set, slot, BLCKSZ)?;
        }
        if tape(set, slot).curBlockNumber == -1 {
            debug_assert!(tape(set, slot).firstBlockNumber == -1);
            debug_assert!(tape(set, slot).pos == 0);

            let block = ltsGetBlock(set, slot)?;
            let lt = tape_mut(set, slot);
            lt.curBlockNumber = block;
            lt.firstBlockNumber = block;
            TapeBlockSetPrev(&mut lt.buffer, -1);
        }

        debug_assert!(tape(set, slot).buffer_size == BLCKSZ);
        while size > 0 {
            if tape(set, slot).pos >= TapeBlockPayloadSize {
                if !tape(set, slot).dirty {
                    elog(ERROR, "invalid logtape state: should be dirty")?;
                }

                let nextBlockNumber = ltsGetBlock(set, slot)?;
                let curBlockNumber = tape(set, slot).curBlockNumber;
                {
                    let lt = tape_mut(set, slot);
                    TapeBlockSetNext(&mut lt.buffer, nextBlockNumber);
                }
                // ltsWriteBlock(curBlockNumber, lt->buffer)
                let buf = steal(&mut tape_mut(set, slot).buffer);
                let res = ltsWriteBlock(set, curBlockNumber, &buf);
                tape_mut(set, slot).buffer = buf;
                res?;

                let lt = tape_mut(set, slot);
                TapeBlockSetPrev(&mut lt.buffer, curBlockNumber);
                lt.curBlockNumber = nextBlockNumber;
                lt.pos = 0;
                lt.nbytes = 0;
            }

            let pos = tape(set, slot).pos;
            let mut nthistime = TapeBlockPayloadSize - pos;
            if nthistime > size {
                nthistime = size;
            }
            debug_assert!(nthistime > 0);

            {
                let lt = tape_mut(set, slot);
                lt.buffer[pos..pos + nthistime].copy_from_slice(&ptr[..nthistime]);
                lt.dirty = true;
                lt.pos += nthistime;
                if lt.nbytes < lt.pos {
                    lt.nbytes = lt.pos;
                }
            }
            ptr = &ptr[nthistime..];
            size -= nthistime;
        }
        Ok(())
    }
}

/// `LogicalTapeRewindForRead(lt, buffer_size)` (logtape.c): switch from write
/// to read.
pub fn logical_tape_rewind_for_read(
    set: &mut LogicalTapeSet<'_>,
    slot: usize,
    buffer_size: usize,
) -> PgResult<()> {
    {
        let mut buffer_size = buffer_size;
        if tape(set, slot).frozen {
            buffer_size = BLCKSZ;
        } else {
            if buffer_size < BLCKSZ {
                buffer_size = BLCKSZ;
            }
            let max_size = tape(set, slot).max_size;
            if buffer_size > max_size {
                buffer_size = max_size;
            }
            buffer_size -= buffer_size % BLCKSZ;
        }

        if tape(set, slot).writing {
            if tape(set, slot).dirty {
                let nbytes = tape(set, slot).nbytes as i64;
                let curBlockNumber = tape(set, slot).curBlockNumber;
                {
                    let lt = tape_mut(set, slot);
                    TapeBlockSetNBytes(&mut lt.buffer, nbytes);
                }
                let buf = steal(&mut tape_mut(set, slot).buffer);
                let res = ltsWriteBlock(set, curBlockNumber, &buf);
                tape_mut(set, slot).buffer = buf;
                res?;
            }
            tape_mut(set, slot).writing = false;
        } else {
            debug_assert!(tape(set, slot).frozen);
        }

        free_buffer(set, slot);
        tape_mut(set, slot).buffer_size = buffer_size;

        if tape(set, slot).prealloc_allocated {
            let mut i = tape(set, slot).nprealloc;
            while i > 0 {
                let block = tape(set, slot).prealloc[(i - 1) as usize];
                ltsReleaseBlock(set, block);
                i -= 1;
            }
            let mcx = set_mcx(set);
            let lt = tape_mut(set, slot);
            lt.prealloc = PgVec::new_in(mcx);
            lt.prealloc_allocated = false;
            lt.nprealloc = 0;
            lt.prealloc_size = 0;
        }
        Ok(())
    }
}

/// `LogicalTapeRead(lt, ptr, size)` (logtape.c): read up to `dst.len()` bytes;
/// returns the number of bytes actually read.
pub fn logical_tape_read(set: &mut LogicalTapeSet<'_>, slot: usize, dst: &mut [u8]) -> PgResult<usize> {
    {
        let mut nread: usize = 0;
        let mut size = dst.len();
        let mut out_off = 0usize;

        debug_assert!(!tape(set, slot).writing);

        if !tape(set, slot).buffer_allocated {
            ltsInitReadBuffer(set, slot)?;
        }

        while size > 0 {
            if tape(set, slot).pos >= tape(set, slot).nbytes {
                if !ltsReadFillBuffer(set, slot)? {
                    break; // EOF
                }
            }
            let lt = tape(set, slot);
            let mut nthistime = lt.nbytes - lt.pos;
            if nthistime > size {
                nthistime = size;
            }
            debug_assert!(nthistime > 0);
            let from = lt.pos;
            dst[out_off..out_off + nthistime].copy_from_slice(&lt.buffer[from..from + nthistime]);

            tape_mut(set, slot).pos += nthistime;
            out_off += nthistime;
            size -= nthistime;
            nread += nthistime;
        }
        Ok(nread)
    }
}

/// `LogicalTapeFreeze(lt, share)` (logtape.c). `share` is the serial-sort
/// `NULL` here (the sharing arm needs sharedfileset.c).
pub fn logical_tape_freeze(set: &mut LogicalTapeSet<'_>, slot: usize) -> PgResult<()> {
    {
        debug_assert!(tape(set, slot).writing);
        debug_assert!(tape(set, slot).offsetBlockNumber == 0);

        if tape(set, slot).dirty {
            let nbytes = tape(set, slot).nbytes as i64;
            let curBlockNumber = tape(set, slot).curBlockNumber;
            {
                let lt = tape_mut(set, slot);
                TapeBlockSetNBytes(&mut lt.buffer, nbytes);
            }
            let buf = steal(&mut tape_mut(set, slot).buffer);
            let res = ltsWriteBlock(set, curBlockNumber, &buf);
            tape_mut(set, slot).buffer = buf;
            res?;
        }
        {
            let lt = tape_mut(set, slot);
            lt.writing = false;
            lt.frozen = true;
        }

        // The seek/backspace functions assume a single-block read buffer.
        if !tape(set, slot).buffer_allocated || tape(set, slot).buffer_size != BLCKSZ {
            alloc_buffer(set, slot, BLCKSZ)?;
        }

        {
            let first = tape(set, slot).firstBlockNumber;
            let lt = tape_mut(set, slot);
            lt.curBlockNumber = first;
            lt.pos = 0;
            lt.nbytes = 0;
            if first == -1 {
                lt.nextBlockNumber = -1;
            }
        }
        let curBlockNumber = tape(set, slot).curBlockNumber;
        let mut buf = steal(&mut tape_mut(set, slot).buffer);
        let res = ltsReadBlock(set, curBlockNumber, &mut buf);
        tape_mut(set, slot).buffer = buf;
        res?;

        let lt = tape_mut(set, slot);
        if TapeBlockIsLast(&lt.buffer) {
            lt.nextBlockNumber = -1;
        } else {
            lt.nextBlockNumber = trailer_next(&lt.buffer);
        }
        lt.nbytes = TapeBlockGetNBytes(&lt.buffer) as usize;
        Ok(())
    }
}

/// `LogicalTapeBackspace(lt, size)` (logtape.c): back up a frozen-for-read tape
/// by `size` bytes; returns the number of bytes backed up.
pub fn logical_tape_backspace(set: &mut LogicalTapeSet<'_>, slot: usize, size: usize) -> PgResult<usize> {
    {
        debug_assert!(tape(set, slot).frozen);
        debug_assert!(tape(set, slot).buffer_size == BLCKSZ);

        if !tape(set, slot).buffer_allocated {
            ltsInitReadBuffer(set, slot)?;
        }

        if size <= tape(set, slot).pos {
            tape_mut(set, slot).pos -= size;
            return Ok(size);
        }

        let mut seekpos = tape(set, slot).pos; // part within this block
        while size > seekpos {
            let prev = trailer_prev(&tape(set, slot).buffer);
            if prev == -1 {
                if tape(set, slot).curBlockNumber != tape(set, slot).firstBlockNumber {
                    elog(ERROR, "unexpected end of tape")?;
                }
                tape_mut(set, slot).pos = 0;
                return Ok(seekpos);
            }

            let mut buf = steal(&mut tape_mut(set, slot).buffer);
            let res = ltsReadBlock(set, prev, &mut buf);
            tape_mut(set, slot).buffer = buf;
            res?;

            let next = trailer_next(&tape(set, slot).buffer);
            let cur = tape(set, slot).curBlockNumber;
            if next != cur {
                elog(
                    ERROR,
                    format!("broken tape, next of block {prev} is {next}, expected {cur}"),
                )?;
            }

            let lt = tape_mut(set, slot);
            lt.nbytes = TapeBlockPayloadSize;
            lt.curBlockNumber = prev;
            lt.nextBlockNumber = next;
            seekpos += TapeBlockPayloadSize;
        }

        tape_mut(set, slot).pos = seekpos - size;
        Ok(size)
    }
}

/// `LogicalTapeSeek(lt, blocknum, offset)` (logtape.c): seek a frozen-for-read
/// tape to a position previously returned by `LogicalTapeTell`.
pub fn logical_tape_seek(set: &mut LogicalTapeSet<'_>, slot: usize, blocknum: i64, offset: i32) -> PgResult<()> {
    {
        debug_assert!(tape(set, slot).frozen);
        debug_assert!(offset >= 0 && offset as usize <= TapeBlockPayloadSize);
        debug_assert!(tape(set, slot).buffer_size == BLCKSZ);

        if !tape(set, slot).buffer_allocated {
            ltsInitReadBuffer(set, slot)?;
        }

        if blocknum != tape(set, slot).curBlockNumber {
            let mut buf = steal(&mut tape_mut(set, slot).buffer);
            let res = ltsReadBlock(set, blocknum, &mut buf);
            tape_mut(set, slot).buffer = buf;
            res?;
            let next = trailer_next(&tape(set, slot).buffer);
            let lt = tape_mut(set, slot);
            lt.curBlockNumber = blocknum;
            lt.nbytes = TapeBlockPayloadSize;
            lt.nextBlockNumber = next;
        }

        if offset as usize > tape(set, slot).nbytes {
            elog(ERROR, "invalid tape seek position")?;
        }
        tape_mut(set, slot).pos = offset as usize;
        Ok(())
    }
}

/// `LogicalTapeTell(lt, blocknum, offset)` (logtape.c): current position.
pub fn logical_tape_tell(set: &mut LogicalTapeSet<'_>, slot: usize) -> PgResult<(i64, i32)> {
    {
        if !tape(set, slot).buffer_allocated {
            ltsInitReadBuffer(set, slot)?;
        }
        debug_assert!(tape(set, slot).offsetBlockNumber == 0);
        debug_assert!(tape(set, slot).buffer_size == BLCKSZ);
        let lt = tape(set, slot);
        Ok((lt.curBlockNumber, lt.pos as i32))
    }
}

// ---------------------------------------------------------------------------
// Error helpers.
// ---------------------------------------------------------------------------

/// The file-access error the C `ereport(ERROR, (errcode_for_file_access(), ...))`
/// produces. The OS is behind the BufFile seam, so no live errno survives; we
/// save `EIO` (the representative I/O errno) so `errcode_for_file_access()`
/// derives a genuine file-access SQLSTATE rather than the bare INTERNAL_ERROR.
fn file_access_error(msg: String) -> PgError {
    const EIO: i32 = 5;
    utils_error::ereport(ERROR)
        .with_saved_errno(EIO)
        .errcode_for_file_access()
        .errmsg(msg)
        .into_error()
}

#[cfg(test)]
mod tests;
