// logtape.c, serial arm; block trailer = trailing (prev, next) i64 pair,
// next < 0 on the last block encodes -nbytes. Parallel (SharedFileSet
// import) and sharedtuplestore.c are unreached in serial sorts.
#![allow(non_snake_case)]

use ::mcx::{Mcx, PgVec};
use ::types_error::{PgError, PgResult};

use fd::BufFile;

#[cfg(test)]
mod tests;

const BLCKSZ: usize = 8192;
const TRAILER_SIZE: usize = 16;
const TAPE_BLOCK_PAYLOAD_SIZE: usize = BLCKSZ - TRAILER_SIZE;
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

const TAPE_WRITE_PREALLOC_MIN: usize = 8;
const TAPE_WRITE_PREALLOC_MAX: usize = 128;

/// The C `LogicalTape *`: a slot index into the owning set.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TapeIdx(u32);

#[inline]
fn trailer_prev(buf: &[u8]) -> i64 {
    i64::from_ne_bytes(
        buf[TAPE_BLOCK_PAYLOAD_SIZE..TAPE_BLOCK_PAYLOAD_SIZE + 8].try_into().unwrap(),
    )
}

#[inline]
fn trailer_next(buf: &[u8]) -> i64 {
    i64::from_ne_bytes(buf[TAPE_BLOCK_PAYLOAD_SIZE + 8..BLCKSZ].try_into().unwrap())
}

#[inline]
fn set_trailer_prev(buf: &mut [u8], prev: i64) {
    buf[TAPE_BLOCK_PAYLOAD_SIZE..TAPE_BLOCK_PAYLOAD_SIZE + 8].copy_from_slice(&prev.to_ne_bytes());
}

#[inline]
fn set_trailer_next(buf: &mut [u8], next: i64) {
    buf[TAPE_BLOCK_PAYLOAD_SIZE + 8..BLCKSZ].copy_from_slice(&next.to_ne_bytes());
}

#[inline]
fn block_is_last(buf: &[u8]) -> bool {
    trailer_next(buf) < 0
}

#[inline]
fn block_nbytes(buf: &[u8]) -> usize {
    if block_is_last(buf) {
        (-trailer_next(buf)) as usize
    } else {
        TAPE_BLOCK_PAYLOAD_SIZE
    }
}

struct LogicalTape<'m> {
    writing: bool,
    frozen: bool,
    dirty: bool,
    first_block_number: i64,
    cur_block_number: i64,
    next_block_number: i64,
    // Lazily sized; empty until the first write/read touches the tape.
    buffer: PgVec<'m, u8>,
    buffer_size: usize,
    max_size: usize,
    pos: usize,
    nbytes: usize,
    // Descending; consumed from the end (lowest block numbers first).
    prealloc: PgVec<'m, i64>,
}

struct TapeSetCore<'m> {
    mcx: Mcx<'m>,
    pfile: BufFile<'m>,
    n_blocks_allocated: i64,
    n_blocks_written: i64,
    forget_free_space: bool,
    // Min-heap; len() is C's nFreeBlocks.
    free_blocks: PgVec<'m, i64>,
    enable_prealloc: bool,
}

pub struct LogicalTapeSet<'m> {
    core: TapeSetCore<'m>,
    tapes: PgVec<'m, Option<LogicalTape<'m>>>,
}

impl<'m> TapeSetCore<'m> {
    fn write_block(&mut self, blocknum: i64, buffer: &[u8]) -> PgResult<()> {
        // BufFile has no holes: zero-fill up to a preallocated target block.
        while blocknum > self.n_blocks_written {
            let zerobuf = [0u8; BLCKSZ];
            let at = self.n_blocks_written;
            self.write_block_raw(at, &zerobuf)?;
        }
        self.write_block_raw(blocknum, buffer)
    }

    fn write_block_raw(&mut self, blocknum: i64, buffer: &[u8]) -> PgResult<()> {
        if self.pfile.seek_block(blocknum)? != 0 {
            return Err(seek_failed(blocknum));
        }
        self.pfile.write(&buffer[..BLCKSZ])?;
        if blocknum == self.n_blocks_written {
            self.n_blocks_written += 1;
        }
        Ok(())
    }

    fn read_block(&mut self, blocknum: i64, buffer: &mut [u8]) -> PgResult<()> {
        if self.pfile.seek_block(blocknum)? != 0 {
            return Err(seek_failed(blocknum));
        }
        self.pfile.read_exact(&mut buffer[..BLCKSZ])
    }

    fn get_free_block(&mut self) -> i64 {
        let n = self.free_blocks.len();
        if n == 0 {
            let b = self.n_blocks_allocated;
            self.n_blocks_allocated += 1;
            return b;
        }
        if n == 1 {
            let b = self.free_blocks[0];
            self.free_blocks.clear();
            return b;
        }
        let heap = &mut self.free_blocks;
        let blocknum = heap[0];
        let holeval = heap[n - 1];
        heap.truncate(n - 1);
        let heapsize = heap.len();
        let mut holepos = 0usize;
        loop {
            let left = 2 * holepos + 1;
            let right = 2 * holepos + 2;
            let min_child = if right < heapsize {
                if heap[left] < heap[right] {
                    left
                } else {
                    right
                }
            } else if left < heapsize {
                left
            } else {
                break;
            };
            if heap[min_child] >= holeval {
                break;
            }
            heap[holepos] = heap[min_child];
            holepos = min_child;
        }
        heap[holepos] = holeval;
        blocknum
    }

    fn release_block(&mut self, blocknum: i64) {
        if self.forget_free_space {
            return;
        }
        // C leaks the block rather than growing the heap past MaxAllocSize.
        if self.free_blocks.len() >= self.free_blocks.capacity()
            && self.free_blocks.capacity() * 2 * 8 > MAX_ALLOC_SIZE
        {
            return;
        }
        let heap = &mut self.free_blocks;
        let mut holepos = heap.len();
        heap.push(blocknum);
        while holepos != 0 {
            let parent = (holepos - 1) / 2;
            if heap[parent] < blocknum {
                break;
            }
            heap[holepos] = heap[parent];
            holepos = parent;
        }
        heap[holepos] = blocknum;
    }
}

impl<'m> LogicalTapeSet<'m> {
    /// `LogicalTapeSetCreate`, serial arm (`fileset = NULL`, `worker = -1`).
    pub fn create(mcx: Mcx<'m>, preallocate: bool) -> PgResult<LogicalTapeSet<'m>> {
        let pfile = fd::BufFileCreateTemp(mcx, false)?;
        let mut free_blocks: PgVec<'m, i64> = PgVec::new_in(mcx);
        free_blocks.reserve(32);
        Ok(LogicalTapeSet {
            core: TapeSetCore {
                mcx,
                pfile,
                n_blocks_allocated: 0,
                n_blocks_written: 0,
                forget_free_space: false,
                free_blocks,
                enable_prealloc: preallocate,
            },
            tapes: PgVec::new_in(mcx),
        })
    }

    /// `LogicalTapeSetClose`; tapes need not be closed first (their buffers
    /// are dropped with the set).
    pub fn close(self) -> PgResult<()> {
        self.core.pfile.close()
    }

    pub fn blocks(&self) -> i64 {
        self.core.n_blocks_written
    }

    pub fn forget_free_space(&mut self) {
        self.core.forget_free_space = true;
    }

    pub fn create_tape(&mut self) -> TapeIdx {
        let mcx = self.core.mcx;
        let slot = self.tapes.len();
        self.tapes.push(Some(LogicalTape {
            writing: true,
            frozen: false,
            dirty: false,
            first_block_number: -1,
            cur_block_number: -1,
            next_block_number: -1,
            buffer: PgVec::new_in(mcx),
            buffer_size: 0,
            max_size: MAX_ALLOC_SIZE,
            pos: 0,
            nbytes: 0,
            prealloc: PgVec::new_in(mcx),
        }));
        TapeIdx(slot as u32)
    }

    /// `LogicalTapeClose`: drops the tape's buffers; blocks are NOT returned
    /// to the free list (caller reads tapes to EOF first, as C does).
    pub fn close_tape(&mut self, tape: TapeIdx) {
        self.tapes[tape.0 as usize] = None;
    }

    #[inline]
    fn parts(&mut self, tape: TapeIdx) -> (&mut TapeSetCore<'m>, &mut LogicalTape<'m>) {
        let lt = self.tapes[tape.0 as usize]
            .as_mut()
            .expect("logtape: operation on a closed tape");
        (&mut self.core, lt)
    }

    pub fn write(&mut self, tape: TapeIdx, mut data: &[u8]) -> PgResult<()> {
        let (core, lt) = self.parts(tape);
        debug_assert!(lt.writing);

        if lt.buffer.is_empty() {
            lt.buffer.resize(BLCKSZ, 0);
            lt.buffer_size = BLCKSZ;
        }
        if lt.cur_block_number == -1 {
            debug_assert!(lt.first_block_number == -1 && lt.pos == 0);
            let block = get_block(core, lt);
            lt.cur_block_number = block;
            lt.first_block_number = block;
            set_trailer_prev(&mut lt.buffer, -1);
        }

        debug_assert!(lt.buffer_size == BLCKSZ);
        while !data.is_empty() {
            if lt.pos >= TAPE_BLOCK_PAYLOAD_SIZE {
                if !lt.dirty {
                    return Err(Box::new(PgError::error(
                        "invalid logtape state: should be dirty",
                    )));
                }
                let next = get_block(core, lt);
                set_trailer_next(&mut lt.buffer, next);
                core.write_block(lt.cur_block_number, &lt.buffer)?;
                set_trailer_prev(&mut lt.buffer, lt.cur_block_number);
                lt.cur_block_number = next;
                lt.pos = 0;
                lt.nbytes = 0;
            }

            let nthistime = (TAPE_BLOCK_PAYLOAD_SIZE - lt.pos).min(data.len());
            debug_assert!(nthistime > 0);
            lt.buffer[lt.pos..lt.pos + nthistime].copy_from_slice(&data[..nthistime]);
            lt.dirty = true;
            lt.pos += nthistime;
            if lt.nbytes < lt.pos {
                lt.nbytes = lt.pos;
            }
            data = &data[nthistime..];
        }
        Ok(())
    }

    pub fn rewind_for_read(&mut self, tape: TapeIdx, buffer_size: usize) -> PgResult<()> {
        let (core, lt) = self.parts(tape);
        let buffer_size = if lt.frozen {
            BLCKSZ
        } else {
            let b = buffer_size.max(BLCKSZ).min(lt.max_size);
            b - b % BLCKSZ
        };

        if lt.writing {
            if lt.dirty {
                set_trailer_next(&mut lt.buffer, -(lt.nbytes as i64));
                core.write_block(lt.cur_block_number, &lt.buffer)?;
            }
            lt.writing = false;
        } else {
            debug_assert!(lt.frozen);
        }

        lt.buffer.clear();
        lt.buffer.shrink_to_fit();
        lt.buffer_size = buffer_size;

        while let Some(block) = lt.prealloc.pop() {
            core.release_block(block);
        }
        lt.prealloc.shrink_to_fit();
        Ok(())
    }

    pub fn read(&mut self, tape: TapeIdx, dst: &mut [u8]) -> PgResult<usize> {
        let (core, lt) = self.parts(tape);
        debug_assert!(!lt.writing);

        if lt.buffer.is_empty() {
            init_read_buffer(core, lt)?;
        }

        let mut nread = 0usize;
        while nread < dst.len() {
            if lt.pos >= lt.nbytes {
                if !read_fill_buffer(core, lt)? {
                    break;
                }
            }
            let nthistime = (lt.nbytes - lt.pos).min(dst.len() - nread);
            debug_assert!(nthistime > 0);
            dst[nread..nread + nthistime].copy_from_slice(&lt.buffer[lt.pos..lt.pos + nthistime]);
            lt.pos += nthistime;
            nread += nthistime;
        }
        Ok(nread)
    }

    pub fn freeze(&mut self, tape: TapeIdx) -> PgResult<()> {
        let (core, lt) = self.parts(tape);
        debug_assert!(lt.writing);

        if lt.dirty {
            set_trailer_next(&mut lt.buffer, -(lt.nbytes as i64));
            core.write_block(lt.cur_block_number, &lt.buffer)?;
        }
        lt.writing = false;
        lt.frozen = true;

        if lt.buffer_size != BLCKSZ || lt.buffer.len() != BLCKSZ {
            lt.buffer.clear();
            lt.buffer.resize(BLCKSZ, 0);
            lt.buffer_size = BLCKSZ;
        }

        lt.cur_block_number = lt.first_block_number;
        lt.pos = 0;
        lt.nbytes = 0;
        if lt.first_block_number == -1 {
            lt.next_block_number = -1;
        }
        core.read_block(lt.cur_block_number, &mut lt.buffer)?;
        lt.next_block_number = if block_is_last(&lt.buffer) {
            -1
        } else {
            trailer_next(&lt.buffer)
        };
        lt.nbytes = block_nbytes(&lt.buffer);
        Ok(())
    }

    /// `LogicalTapeBackspace` (frozen tapes only); returns bytes backed up.
    pub fn backspace(&mut self, tape: TapeIdx, size: usize) -> PgResult<usize> {
        let (core, lt) = self.parts(tape);
        debug_assert!(lt.frozen && lt.buffer_size == BLCKSZ);

        if lt.buffer.is_empty() {
            init_read_buffer(core, lt)?;
        }

        if size <= lt.pos {
            lt.pos -= size;
            return Ok(size);
        }

        let mut seekpos = lt.pos;
        while size > seekpos {
            let prev = trailer_prev(&lt.buffer);
            if prev == -1 {
                if lt.cur_block_number != lt.first_block_number {
                    return Err(Box::new(PgError::error("unexpected end of tape")));
                }
                lt.pos = 0;
                return Ok(seekpos);
            }

            core.read_block(prev, &mut lt.buffer)?;
            let next = trailer_next(&lt.buffer);
            if next != lt.cur_block_number {
                return Err(Box::new(PgError::error(format!(
                    "broken tape, next of block {prev} is {next}, expected {}",
                    lt.cur_block_number
                ))));
            }
            lt.nbytes = TAPE_BLOCK_PAYLOAD_SIZE;
            lt.cur_block_number = prev;
            lt.next_block_number = next;
            seekpos += TAPE_BLOCK_PAYLOAD_SIZE;
        }

        lt.pos = seekpos - size;
        Ok(size)
    }

    /// `LogicalTapeSeek` (frozen tapes; position from [`Self::tell`]).
    pub fn seek(&mut self, tape: TapeIdx, blocknum: i64, offset: i32) -> PgResult<()> {
        let (core, lt) = self.parts(tape);
        debug_assert!(lt.frozen);
        debug_assert!(offset >= 0 && offset as usize <= TAPE_BLOCK_PAYLOAD_SIZE);
        debug_assert!(lt.buffer_size == BLCKSZ);

        if lt.buffer.is_empty() {
            init_read_buffer(core, lt)?;
        }

        if blocknum != lt.cur_block_number {
            core.read_block(blocknum, &mut lt.buffer)?;
            lt.cur_block_number = blocknum;
            lt.nbytes = TAPE_BLOCK_PAYLOAD_SIZE;
            lt.next_block_number = trailer_next(&lt.buffer);
        }

        if offset as usize > lt.nbytes {
            return Err(Box::new(PgError::error("invalid tape seek position")));
        }
        lt.pos = offset as usize;
        Ok(())
    }

    pub fn tell(&mut self, tape: TapeIdx) -> PgResult<(i64, i32)> {
        let (core, lt) = self.parts(tape);
        if lt.buffer.is_empty() {
            init_read_buffer(core, lt)?;
        }
        debug_assert!(lt.buffer_size == BLCKSZ);
        Ok((lt.cur_block_number, lt.pos as i32))
    }
}

fn get_block(core: &mut TapeSetCore<'_>, lt: &mut LogicalTape<'_>) -> i64 {
    if core.enable_prealloc {
        get_prealloc_block(core, lt)
    } else {
        core.get_free_block()
    }
}

/// `ltsGetPreallocBlock`: descending list consumed from the end; doubling
/// refill between TAPE_WRITE_PREALLOC_MIN and _MAX.
fn get_prealloc_block(core: &mut TapeSetCore<'_>, lt: &mut LogicalTape<'_>) -> i64 {
    if let Some(block) = lt.prealloc.pop() {
        return block;
    }
    let size = if lt.prealloc.capacity() == 0 {
        TAPE_WRITE_PREALLOC_MIN
    } else {
        (lt.prealloc.capacity() * 2).min(TAPE_WRITE_PREALLOC_MAX)
    };
    lt.prealloc.reserve(size);
    lt.prealloc.resize(size, 0);
    for i in (0..size).rev() {
        lt.prealloc[i] = core.get_free_block();
        debug_assert!(i + 1 == size || lt.prealloc[i] > lt.prealloc[i + 1]);
    }
    lt.prealloc.pop().unwrap()
}

/// `ltsInitReadBuffer`: lazily size the buffer and pull the first block(s).
fn init_read_buffer(core: &mut TapeSetCore<'_>, lt: &mut LogicalTape<'_>) -> PgResult<()> {
    debug_assert!(lt.buffer_size > 0);
    lt.buffer.reserve(lt.buffer_size);
    lt.buffer.resize(lt.buffer_size, 0);
    lt.next_block_number = lt.first_block_number;
    lt.pos = 0;
    lt.nbytes = 0;
    read_fill_buffer(core, lt)?;
    Ok(())
}

/// `ltsReadFillBuffer`: true if anything was read, false on EOF.
fn read_fill_buffer(core: &mut TapeSetCore<'_>, lt: &mut LogicalTape<'_>) -> PgResult<bool> {
    lt.pos = 0;
    lt.nbytes = 0;
    loop {
        let datablocknum = lt.next_block_number;
        if datablocknum == -1 {
            break;
        }
        let start = lt.nbytes;
        core.read_block(datablocknum, &mut lt.buffer[start..start + BLCKSZ])?;
        if !lt.frozen {
            core.release_block(datablocknum);
        }
        lt.cur_block_number = lt.next_block_number;

        let thisbuf = &lt.buffer[start..start + BLCKSZ];
        lt.nbytes += block_nbytes(thisbuf);
        if block_is_last(thisbuf) {
            lt.next_block_number = -1;
            break;
        }
        lt.next_block_number = trailer_next(thisbuf);

        if lt.buffer_size - lt.nbytes <= BLCKSZ {
            break;
        }
    }
    Ok(lt.nbytes > 0)
}

#[track_caller]
#[cold]
#[inline(never)]
fn seek_failed(blocknum: i64) -> Box<PgError> {
    Box::new(
        ::elog::ereport(::types_error::ERROR)
            .errcode_for_file_access()
            .errmsg(format!("could not seek to block {blocknum} of temporary file"))
            .into_error(),
    )
}
