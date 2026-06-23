//! `utils/sort/sharedtuplestore.c` — per-batch shared tuplestores for the
//! parallel hash join.
//!
//! A `SharedTuplestore` is a parallel-aware subset of `tuplestore.c`: multiple
//! backends write into it (each into its own per-participant `BufFile` in a
//! `SharedFileSet`), then multiple backends cooperatively read every tuple back
//! via a round-robin parallel scan, claiming chunks under each participant's
//! in-DSM `LWLock`.
//!
//! Two pieces of state:
//!  * the **shared** control object lives in DSM at the address named by a
//!    [`SharedTuplestoreHandle`]: a `#[repr(C)] SharedTuplestore` header
//!    followed by a flexible array of `SharedTuplestoreParticipant` (each with
//!    an embedded in-DSM `LWLock`). It is reached by raw pointer + the
//!    `offset_of!(SharedTuplestore, participants)` arithmetic, exactly as C
//!    does. Every Rust backend uses the same `#[repr(C)]` layout, so the offset
//!    arithmetic `sts_estimate` reports (consumed by nodeHash's
//!    `EstimateParallelHashJoinBatch` / `ParallelHashJoinBatch{Inner,Outer}`)
//!    is self-consistent across processes.
//!  * the **backend-local** accessor (read/write buffers, open `BufFile`s,
//!    cursor positions) lives in a `thread_local` slab keyed by a 1-based
//!    `usize` token — the [`SharedTuplestoreAccessorHandle`]. Both seam
//!    families (`&mut SharedTuplestoreAccessor` for nodeHashjoin and `*_handle`
//!    for nodeHash) resolve to the same slab entry via that token.

#![allow(non_snake_case)]

extern crate alloc;

use alloc::vec::Vec;
use core::cell::RefCell;

use buffile_seams as buffile;
use lwlock_seams as lwlock;
use sort_storage_seams as seams;
use ::types_core::{Size, ProcNumber};
use ::types_core::fmgr::NAMEDATALEN;
use ::types_core::primitive::BLCKSZ;
use ::types_error::{PgError, PgResult};
use ::execparallel::{
    FileSetHandle, SharedFileSetHandle, SharedTuplestoreAccessorHandle, SharedTuplestoreHandle,
};
use ::nodes::nodehashjoin::BufFile;
use ::types_storage::{LWLock, LWLockMode, LWTRANCHE_SHARED_TUPLESTORE};

/// `STS_CHUNK_PAGES` (sharedtuplestore.c:37) — chunk size in BLCKSZ pages.
const STS_CHUNK_PAGES: usize = 4;
/// `STS_CHUNK_HEADER_SIZE` = `offsetof(SharedTuplestoreChunk, data)`. The header
/// is `int ntuples; int overflow;` = 8 bytes on the LP64 ABI.
const STS_CHUNK_HEADER_SIZE: usize = 8;
/// `STS_CHUNK_DATA_SIZE` = `STS_CHUNK_PAGES * BLCKSZ - STS_CHUNK_HEADER_SIZE`.
const STS_CHUNK_DATA_SIZE: usize = STS_CHUNK_PAGES * BLCKSZ - STS_CHUNK_HEADER_SIZE;
/// `STS_CHUNK_PAGES * BLCKSZ` — a whole flushed chunk's byte size.
const STS_CHUNK_BYTES: usize = STS_CHUNK_PAGES * BLCKSZ;

/// `MAXALIGN(LEN)` (c.h) — round up to `MAXIMUM_ALIGNOF` (8 on LP64).
#[inline]
const fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

/// O_RDONLY (fcntl.h) — the mode `BufFileOpenFileSet` is called with for reads.
const O_RDONLY: i32 = 0;

// ===========================================================================
//   In-DSM shared structures (placed at the SharedTuplestoreHandle address).
// ===========================================================================

/// `SharedTuplestoreParticipant` (sharedtuplestore.c:50) — per-participant
/// shared state: an embedded in-DSM `LWLock` plus the shared read/write head.
/// `read_page`/`npages` are `BlockNumber` (u32). `#[repr(C)]` so its layout
/// matches across every Rust backend in the cohort.
#[repr(C)]
struct SharedTuplestoreParticipant {
    /// `LWLock lock`.
    lock: LWLock,
    /// `BlockNumber read_page` — page number for the next read.
    read_page: u32,
    /// `BlockNumber npages` — number of pages written.
    npages: u32,
    /// `bool writing` — used only for assertions.
    writing: bool,
}

/// `struct SharedTuplestore` (sharedtuplestore.c:59) — the control object in
/// shared memory, followed by `participants[FLEXIBLE_ARRAY_MEMBER]`.
#[repr(C)]
struct SharedTuplestore {
    /// `int nparticipants` — number of participants that can write.
    nparticipants: i32,
    /// `int flags` — `SHARED_TUPLESTORE_XXX` bits.
    flags: i32,
    /// `size_t meta_data_size` — size of the per-tuple header.
    meta_data_size: Size,
    /// `char name[NAMEDATALEN]`.
    name: [u8; NAMEDATALEN as usize],
    // Followed by `SharedTuplestoreParticipant participants[]`.
}

/// `offsetof(SharedTuplestore, participants)` — the byte offset of the flexible
/// participants array past the header. `#[repr(C)]` lays the header out as the
/// fields above; the array starts at the maxaligned end of the struct.
#[inline]
const fn participants_offset() -> usize {
    maxalign(core::mem::size_of::<SharedTuplestore>())
}

#[inline]
unsafe fn sts_header<'a>(sts: SharedTuplestoreHandle) -> &'a mut SharedTuplestore {
    &mut *(sts.0 as *mut SharedTuplestore)
}

#[inline]
unsafe fn participant_at<'a>(
    sts: SharedTuplestoreHandle,
    i: i32,
) -> &'a mut SharedTuplestoreParticipant {
    let base = sts.0 + participants_offset();
    let p = base + (i as usize) * core::mem::size_of::<SharedTuplestoreParticipant>();
    &mut *(p as *mut SharedTuplestoreParticipant)
}

// ===========================================================================
//   Backend-local accessor slab.
// ===========================================================================

/// `struct SharedTuplestoreAccessor` (sharedtuplestore.c:71) — the per-backend
/// state that lives in backend-local memory. Held in the thread_local slab; the
/// `SharedTuplestoreAccessorHandle` is `slot index + 1`.
struct AccessorState {
    /// `int participant` — my participant number.
    participant: i32,
    /// `SharedTuplestore *sts` — the shared control object's DSM address.
    sts: SharedTuplestoreHandle,
    /// `SharedFileSet *fileset` — the fileset holding the files (DSM address).
    fileset: SharedFileSetHandle,

    // ---- reading ----
    /// `int read_participant` — the current participant to read from.
    read_participant: i32,
    /// `BufFile *read_file` — the current file to read from.
    read_file: Option<BufFile>,
    /// `int read_ntuples_available` — number of tuples in the current chunk.
    read_ntuples_available: i32,
    /// `int read_ntuples` — how many tuples have we read from the chunk.
    read_ntuples: i32,
    /// `size_t read_bytes` — how many bytes have we read from the chunk.
    read_bytes: usize,
    /// `char *read_buffer` — a buffer for loading tuples (grown lazily).
    read_buffer: Vec<u8>,
    /// `BlockNumber read_next_page` — lowest block we'll consider reading.
    read_next_page: u32,

    // ---- writing ----
    /// `SharedTuplestoreChunk *write_chunk` — the in-flight write buffer (whole
    /// `STS_CHUNK_BYTES` chunk image: `[ntuples:i32][overflow:i32][data...]`).
    write_chunk: Option<Vec<u8>>,
    /// `BufFile *write_file` — the current file to write to.
    write_file: Option<BufFile>,
    /// `char *write_pointer` — current write offset within `write_chunk->data`,
    /// expressed as an absolute byte offset into the chunk image. `usize::MAX`
    /// models C's `write_pointer == NULL` (no chunk yet).
    write_pointer: usize,
    /// `char *write_end` — one past the end of the chunk image (`STS_CHUNK_BYTES`
    /// once a chunk is allocated).
    write_end: usize,
}

thread_local! {
    static SLAB: RefCell<Vec<Option<AccessorState>>> = const { RefCell::new(Vec::new()) };
}

/// Insert an accessor into the slab, returning its 1-based handle.
fn slab_insert(acc: AccessorState) -> SharedTuplestoreAccessorHandle {
    SLAB.with(|s| {
        let mut v = s.borrow_mut();
        v.push(Some(acc));
        SharedTuplestoreAccessorHandle(v.len())
    })
}

/// Run `f` with a mutable borrow of the accessor named by `handle`.
fn with_accessor<R>(
    handle: SharedTuplestoreAccessorHandle,
    f: impl FnOnce(&mut AccessorState) -> R,
) -> R {
    SLAB.with(|s| {
        let mut v = s.borrow_mut();
        let acc = v[handle.0 - 1]
            .as_mut()
            .expect("sharedtuplestore: accessor slot is empty");
        f(acc)
    })
}

/// `MyProcNumber` for the LWLock seam — the caller's real PGPROC slot index
/// (valid in both the leader and every worker), NOT `ParallelWorkerNumber`.
/// `LWLockAcquire` needs the real proc number to queue/wait on contention.
#[inline]
fn current_proc_number() -> ProcNumber {
    init_small_seams::my_proc_number::call()
}

/// The backend-local `FileSet *` handle for this accessor's `SharedFileSet`:
/// `&fileset->fs`. The `SharedFileSet` lives in DSM at `fileset.0`; its `fs`
/// field is the first member, so `&fileset->fs` is the same address.
#[inline]
fn fileset_fs(fileset: SharedFileSetHandle) -> FileSetHandle {
    let sfs = fileset.0 as *const ::types_storage::fileset::SharedFileSet;
    // `fs` is the first field of `SharedFileSet` (#[repr(C)]), so its address is
    // the struct's address.
    let fs = unsafe { core::ptr::addr_of!((*sfs).fs) } as usize;
    FileSetHandle(fs)
}

// ===========================================================================
//   sts_filename — the per-participant BufFile name.
// ===========================================================================

/// `sts_filename(name, accessor, participant)` (sharedtuplestore.c:598):
/// `"<sts.name>.p<participant>"`.
fn sts_filename(sts: SharedTuplestoreHandle, participant: i32) -> alloc::string::String {
    let header = unsafe { sts_header(sts) };
    // The name is a NUL-terminated C string in a `[u8; NAMEDATALEN]`.
    let end = header.name.iter().position(|&b| b == 0).unwrap_or(NAMEDATALEN as usize);
    let base = core::str::from_utf8(&header.name[..end])
        .expect("sharedtuplestore: name is not valid UTF-8");
    alloc::format!("{base}.p{participant}")
}

// ===========================================================================
//   sts_estimate / sts_initialize / sts_attach
// ===========================================================================

/// `sts_estimate(participants)` (sharedtuplestore.c:103).
fn sts_estimate(participants: i32) -> Size {
    participants_offset()
        + core::mem::size_of::<SharedTuplestoreParticipant>() * participants as usize
}

/// `sts_initialize(...)` (sharedtuplestore.c:125): initialize the shared object
/// in place and return this backend's accessor handle.
fn sts_initialize(
    sts: SharedTuplestoreHandle,
    participants: i32,
    my_participant_number: i32,
    meta_data_size: Size,
    flags: i32,
    fileset: SharedFileSetHandle,
    name: &str,
) -> PgResult<SharedTuplestoreAccessorHandle> {
    debug_assert!(my_participant_number < participants);

    let header = unsafe { sts_header(sts) };
    header.nparticipants = participants;
    header.meta_data_size = meta_data_size;
    header.flags = flags;

    if name.len() > NAMEDATALEN as usize - 1 {
        return Err(PgError::error("SharedTuplestore name too long"));
    }
    // strcpy(sts->name, name): copy the bytes + a trailing NUL.
    header.name = [0u8; NAMEDATALEN as usize];
    header.name[..name.len()].copy_from_slice(name.as_bytes());

    // Limit meta-data so it + tuple size always fits into a single chunk.
    if meta_data_size + core::mem::size_of::<u32>() >= STS_CHUNK_DATA_SIZE {
        return Err(PgError::error("meta-data too long"));
    }

    for i in 0..participants {
        let p = unsafe { participant_at(sts, i) };
        lwlock::lwlock_initialize::call(&mut p.lock, LWTRANCHE_SHARED_TUPLESTORE);
        p.read_page = 0;
        p.npages = 0;
        p.writing = false;
    }

    let acc = AccessorState {
        participant: my_participant_number,
        sts,
        fileset,
        read_participant: 0,
        read_file: None,
        read_ntuples_available: 0,
        read_ntuples: 0,
        read_bytes: 0,
        read_buffer: Vec::new(),
        read_next_page: 0,
        write_chunk: None,
        write_file: None,
        write_pointer: usize::MAX,
        write_end: 0,
    };
    Ok(slab_insert(acc))
}

/// `sts_attach(...)` (sharedtuplestore.c:177): attach to an already-initialized
/// shared tuplestore.
fn sts_attach(
    sts: SharedTuplestoreHandle,
    my_participant_number: i32,
    fileset: SharedFileSetHandle,
) -> PgResult<SharedTuplestoreAccessorHandle> {
    let header = unsafe { sts_header(sts) };
    debug_assert!(my_participant_number < header.nparticipants);
    let acc = AccessorState {
        participant: my_participant_number,
        sts,
        fileset,
        read_participant: 0,
        read_file: None,
        read_ntuples_available: 0,
        read_ntuples: 0,
        read_bytes: 0,
        read_buffer: Vec::new(),
        read_next_page: 0,
        write_chunk: None,
        write_file: None,
        write_pointer: usize::MAX,
        write_end: 0,
    };
    Ok(slab_insert(acc))
}

// ===========================================================================
//   sts_flush_chunk / sts_end_write
// ===========================================================================

/// `sts_flush_chunk(accessor)` (sharedtuplestore.c:195): write the in-flight
/// chunk image to the write file, zero it, and bump this participant's npages.
fn sts_flush_chunk(acc: &mut AccessorState) -> PgResult<()> {
    let chunk = acc
        .write_chunk
        .as_mut()
        .expect("sts_flush_chunk: write_chunk is NULL");
    let file = acc
        .write_file
        .as_mut()
        .expect("sts_flush_chunk: write_file is NULL");
    buffile::buf_file_write::call(file, &chunk[..STS_CHUNK_BYTES])?;
    for b in chunk[..STS_CHUNK_BYTES].iter_mut() {
        *b = 0;
    }
    // write_pointer = &write_chunk->data[0].
    acc.write_pointer = STS_CHUNK_HEADER_SIZE;
    let p = unsafe { participant_at(acc.sts, acc.participant) };
    p.npages += STS_CHUNK_PAGES as u32;
    Ok(())
}

/// `sts_end_write(accessor)` (sharedtuplestore.c:212): flush and close the write
/// file, making the partition readable.
fn sts_end_write(acc: &mut AccessorState) -> PgResult<()> {
    if acc.write_file.is_some() {
        sts_flush_chunk(acc)?;
        let mut file = acc.write_file.take().unwrap();
        buffile::buf_file_close_ref::call(&mut file)?;
        acc.write_chunk = None;
        acc.write_pointer = usize::MAX;
        acc.write_end = 0;
        let p = unsafe { participant_at(acc.sts, acc.participant) };
        p.writing = false;
    }
    Ok(())
}

// ===========================================================================
//   sts_reinitialize / sts_begin_parallel_scan / sts_end_parallel_scan
// ===========================================================================

/// `sts_reinitialize(accessor)` (sharedtuplestore.c:233): reset every
/// participant's shared read head.
fn sts_reinitialize(acc: &mut AccessorState) {
    let header = unsafe { sts_header(acc.sts) };
    let n = header.nparticipants;
    for i in 0..n {
        let p = unsafe { participant_at(acc.sts, i) };
        p.read_page = 0;
    }
}

/// `sts_end_parallel_scan(accessor)` (sharedtuplestore.c:280): free the
/// backend-local read file.
fn sts_end_parallel_scan(acc: &mut AccessorState) -> PgResult<()> {
    if let Some(mut file) = acc.read_file.take() {
        buffile::buf_file_close_ref::call(&mut file)?;
    }
    Ok(())
}

/// `sts_begin_parallel_scan(accessor)` (sharedtuplestore.c:252).
fn sts_begin_parallel_scan(acc: &mut AccessorState) -> PgResult<()> {
    // End any existing scan that was in progress.
    sts_end_parallel_scan(acc)?;

    // We start out reading the file THIS backend wrote.
    acc.read_participant = acc.participant;
    acc.read_file = None;
    acc.read_next_page = 0;
    Ok(())
}

// ===========================================================================
//   sts_puttuple
// ===========================================================================

/// `sts_puttuple(accessor, meta_data, tuple)` (sharedtuplestore.c:299). `tuple`
/// is the flat C `MinimalTuple` byte image (its `t_len` is the leading u32, and
/// `tuple.len() == t_len`).
fn sts_puttuple(acc: &mut AccessorState, meta_data: &[u8], tuple: &[u8]) -> PgResult<()> {
    let header = unsafe { sts_header(acc.sts) };
    let meta_data_size = header.meta_data_size;
    let t_len = tuple.len();
    debug_assert_eq!(meta_data.len(), meta_data_size);

    // Do we have our own file yet?
    if acc.write_file.is_none() {
        let name = sts_filename(acc.sts, acc.participant);
        let fs = fileset_fs(acc.fileset);
        let mcx = mcxt_seams::top_memory_context::call();
        let file = buffile::buf_file_create_fileset::call(mcx, fs, &name)?;
        acc.write_file = Some(mcx::box_into_inner_leak(file));
        let p = unsafe { participant_at(acc.sts, acc.participant) };
        p.writing = true;
    }

    // Do we have space? size = meta_data_size + tuple->t_len.
    let mut size = meta_data_size + t_len;
    if acc.write_pointer == usize::MAX || acc.write_pointer + size > acc.write_end {
        if acc.write_chunk.is_none() {
            // First time through. Allocate chunk (zeroed).
            let mut chunk = alloc::vec![0u8; STS_CHUNK_BYTES];
            // write_chunk->ntuples = 0 (already zero).
            write_i32(&mut chunk, 0, 0);
            acc.write_chunk = Some(chunk);
            acc.write_pointer = STS_CHUNK_HEADER_SIZE; // &data[0]
            acc.write_end = STS_CHUNK_BYTES;
        } else {
            // See if flushing helps.
            sts_flush_chunk(acc)?;
        }

        // It may still not be enough in the case of a gigantic tuple.
        if acc.write_pointer + size > acc.write_end {
            // Oversized tuple: write the beginning here, the rest in overflow
            // chunks. (sts_initialize verified tuple+meta always fits a chunk.)
            debug_assert!(acc.write_pointer + meta_data_size + core::mem::size_of::<u32>() < acc.write_end);

            // Write the meta-data.
            {
                let chunk = acc.write_chunk.as_mut().unwrap();
                if meta_data_size > 0 {
                    chunk[acc.write_pointer..acc.write_pointer + meta_data_size]
                        .copy_from_slice(meta_data);
                }
                // Write as much of the tuple as fits (includes leading t_len).
                let written = acc.write_end - acc.write_pointer - meta_data_size;
                chunk[acc.write_pointer + meta_data_size..acc.write_end]
                    .copy_from_slice(&tuple[..written]);
                bump_ntuples(chunk);
                size -= meta_data_size;
                size -= written;

                // Now write the rest in overflow chunks. `written` tracks bytes
                // of `tuple` already emitted.
                let mut written_total = written;
                while size > 0 {
                    sts_flush_chunk(acc)?;
                    let chunk = acc.write_chunk.as_mut().unwrap();
                    // overflow = ceil(size / STS_CHUNK_DATA_SIZE).
                    let overflow = (size + STS_CHUNK_DATA_SIZE - 1) / STS_CHUNK_DATA_SIZE;
                    write_i32(chunk, 4, overflow as i32);
                    let avail = acc.write_end - acc.write_pointer;
                    let written_this_chunk = avail.min(size);
                    chunk[acc.write_pointer..acc.write_pointer + written_this_chunk]
                        .copy_from_slice(&tuple[written_total..written_total + written_this_chunk]);
                    acc.write_pointer += written_this_chunk;
                    size -= written_this_chunk;
                    written_total += written_this_chunk;
                    // re-borrow chunk on next loop iteration via index
                    let _ = chunk;
                }
            }
            return Ok(());
        }
    }

    // Copy meta-data and tuple into the buffer.
    let chunk = acc.write_chunk.as_mut().unwrap();
    if meta_data_size > 0 {
        chunk[acc.write_pointer..acc.write_pointer + meta_data_size].copy_from_slice(meta_data);
    }
    chunk[acc.write_pointer + meta_data_size..acc.write_pointer + meta_data_size + t_len]
        .copy_from_slice(tuple);
    acc.write_pointer += size;
    bump_ntuples(chunk);
    Ok(())
}

/// Write an i32 at byte `off` in the chunk image (native endianness — this is
/// the same process family's own scratch/file format).
#[inline]
fn write_i32(chunk: &mut [u8], off: usize, v: i32) {
    chunk[off..off + 4].copy_from_slice(&v.to_ne_bytes());
}

#[inline]
fn read_i32(buf: &[u8], off: usize) -> i32 {
    let mut b = [0u8; 4];
    b.copy_from_slice(&buf[off..off + 4]);
    i32::from_ne_bytes(b)
}

/// `++write_chunk->ntuples`.
#[inline]
fn bump_ntuples(chunk: &mut [u8]) {
    let n = read_i32(chunk, 0);
    write_i32(chunk, 0, n + 1);
}

// ===========================================================================
//   sts_read_tuple / sts_parallel_scan_next
// ===========================================================================

/// `sts_read_tuple(accessor, meta_data)` (sharedtuplestore.c:415): read one
/// tuple (and its meta-data) from the current read file into `read_buffer`,
/// returning the flat MinimalTuple image. `meta_out` receives the meta-data.
fn sts_read_tuple(acc: &mut AccessorState, meta_out: &mut [u8]) -> PgResult<Vec<u8>> {
    let meta_data_size = unsafe { sts_header(acc.sts) }.meta_data_size;

    let file = acc
        .read_file
        .as_mut()
        .expect("sts_read_tuple: read_file is NULL");

    if meta_data_size > 0 {
        buffile::buf_file_read_exact::call(file, &mut meta_out[..meta_data_size])?;
        acc.read_bytes += meta_data_size;
    }
    let mut size_bytes = [0u8; 4];
    buffile::buf_file_read_exact::call(file, &mut size_bytes)?;
    let size = u32::from_ne_bytes(size_bytes) as usize;
    acc.read_bytes += core::mem::size_of::<u32>();

    if size > acc.read_buffer.len() {
        let new_size = size.max(acc.read_buffer.len() * 2);
        acc.read_buffer.resize(new_size, 0);
    }

    let mut remaining_size = size - core::mem::size_of::<u32>();
    let mut this_chunk_size = remaining_size.min(STS_CHUNK_BYTES - acc.read_bytes);
    let mut dest = core::mem::size_of::<u32>(); // destination = read_buffer + sizeof(uint32)
    {
        let file = acc.read_file.as_mut().unwrap();
        buffile::buf_file_read_exact::call(file, &mut acc.read_buffer[dest..dest + this_chunk_size])?;
    }
    acc.read_bytes += this_chunk_size;
    remaining_size -= this_chunk_size;
    dest += this_chunk_size;
    acc.read_ntuples += 1;

    // Read any overflow chunks.
    while remaining_size > 0 {
        // Positioned at the start of an overflow chunk; read its header.
        let mut chunk_header = [0u8; STS_CHUNK_HEADER_SIZE];
        {
            let file = acc.read_file.as_mut().unwrap();
            buffile::buf_file_read_exact::call(file, &mut chunk_header)?;
        }
        acc.read_bytes = STS_CHUNK_HEADER_SIZE;
        let overflow = read_i32(&chunk_header, 4);
        if overflow == 0 {
            return Err(PgError::error(
                "unexpected chunk in shared tuplestore temporary file",
            ));
        }
        acc.read_next_page += STS_CHUNK_PAGES as u32;
        this_chunk_size = remaining_size.min(STS_CHUNK_BYTES - STS_CHUNK_HEADER_SIZE);
        {
            let file = acc.read_file.as_mut().unwrap();
            buffile::buf_file_read_exact::call(file, &mut acc.read_buffer[dest..dest + this_chunk_size])?;
        }
        acc.read_bytes += this_chunk_size;
        remaining_size -= this_chunk_size;
        dest += this_chunk_size;

        // Count regular tuples following the oversized tuple in this chunk.
        acc.read_ntuples = 0;
        acc.read_ntuples_available = read_i32(&chunk_header, 0);
    }

    // tuple = read_buffer; tuple->t_len = size.
    let mut out = acc.read_buffer[..size].to_vec();
    out[0..4].copy_from_slice(&(size as u32).to_ne_bytes());
    Ok(out)
}

/// `sts_parallel_scan_next(accessor, meta_data)` (sharedtuplestore.c:495).
/// Returns `(flat_tuple_image, meta_filled)` or `None` at end of the whole
/// store. `meta_out` is filled in with the per-tuple meta-data.
fn sts_parallel_scan_next(
    acc: &mut AccessorState,
    meta_out: &mut [u8],
) -> PgResult<Option<Vec<u8>>> {
    let (nparticipants, my_participant) = {
        let header = unsafe { sts_header(acc.sts) };
        (header.nparticipants, acc.participant)
    };

    loop {
        // Can we read more tuples from the current chunk?
        if acc.read_ntuples < acc.read_ntuples_available {
            return Ok(Some(sts_read_tuple(acc, meta_out)?));
        }

        // Find the location of a new chunk to read, under the participant lock.
        let read_participant = acc.read_participant;
        let (eof, read_page) = {
            let p = unsafe { participant_at(acc.sts, read_participant) };
            let guard = lwlock::lwlock_acquire::call(
                &p.lock,
                LWLockMode::LW_EXCLUSIVE,
                current_proc_number(),
            )?;
            // Skip directly past overflow pages we already know about.
            if p.read_page < acc.read_next_page {
                p.read_page = acc.read_next_page;
            }
            let eof = p.read_page >= p.npages;
            let read_page = if !eof {
                let rp = p.read_page;
                p.read_page += STS_CHUNK_PAGES as u32;
                acc.read_next_page = p.read_page;
                rp
            } else {
                0
            };
            guard.release()?;
            (eof, read_page)
        };

        if !eof {
            // Make sure we have the file open.
            if acc.read_file.is_none() {
                let name = sts_filename(acc.sts, read_participant);
                let fs = fileset_fs(acc.fileset);
                let mcx = mcxt_seams::top_memory_context::call();
                let opened =
                    buffile::buf_file_open_fileset::call(mcx, fs, &name, O_RDONLY, false)?;
                let file = opened.expect("sts_parallel_scan_next: read file missing");
                acc.read_file = Some(mcx::box_into_inner_leak(file));
            }

            // Seek and load the chunk header.
            let mut chunk_header = [0u8; STS_CHUNK_HEADER_SIZE];
            {
                let file = acc.read_file.as_mut().unwrap();
                let rc = buffile::buf_file_seek_block::call(file, read_page as i64)?;
                if rc != 0 {
                    return Err(PgError::error(alloc::format!(
                        "could not seek to block {read_page} in shared tuplestore temporary file"
                    )));
                }
                buffile::buf_file_read_exact::call(file, &mut chunk_header)?;
            }

            let overflow = read_i32(&chunk_header, 4);
            if overflow > 0 {
                // Skip this and all following overflow chunks at once.
                acc.read_next_page = read_page + overflow as u32 * STS_CHUNK_PAGES as u32;
                continue;
            }

            acc.read_ntuples = 0;
            acc.read_ntuples_available = read_i32(&chunk_header, 0);
            acc.read_bytes = STS_CHUNK_HEADER_SIZE;
            // Go around again to get a tuple from this chunk.
        } else {
            if let Some(mut file) = acc.read_file.take() {
                buffile::buf_file_close_ref::call(&mut file)?;
            }
            // Try the next participant's file; if full circle, we're done.
            acc.read_participant = (acc.read_participant + 1) % nparticipants;
            if acc.read_participant == my_participant {
                break;
            }
            acc.read_next_page = 0;
            // Go around again to get a chunk from this file.
        }
    }

    Ok(None)
}

// ===========================================================================
//   Seam installation.
// ===========================================================================

/// Install every shared-tuplestore seam.
pub fn init_seams() {
    // ---- `&mut SharedTuplestoreAccessor`-threaded family (nodeHashjoin) ----
    // The accessor token is boxed inside the `Opaque` payload; recover it.
    seams::sts_begin_parallel_scan::set(|accessor| {
        let h = accessor_handle(accessor);
        with_accessor(h, sts_begin_parallel_scan)
    });
    seams::sts_end_parallel_scan::set(|accessor| {
        let h = accessor_handle(accessor);
        with_accessor(h, sts_end_parallel_scan)
    });
    seams::sts_parallel_scan_next::set(|mcx, accessor| {
        let h = accessor_handle(accessor);
        let mut meta = [0u8; 4];
        let next = with_accessor(h, |acc| sts_parallel_scan_next(acc, &mut meta))?;
        match next {
            None => Ok(None),
            Some(blob) => {
                let hashvalue = u32::from_ne_bytes(meta);
                let mut v = mcx::vec_with_capacity_in(mcx, blob.len())?;
                v.extend_from_slice(&blob);
                Ok(Some((v, hashvalue)))
            }
        }
    });
    seams::sts_puttuple::set(|accessor, hashvalue, tuple| {
        let h = accessor_handle(accessor);
        let meta = hashvalue.to_ne_bytes();
        with_accessor(h, |acc| sts_puttuple(acc, &meta, tuple))
    });
    seams::sts_end_write::set(|accessor| {
        let h = accessor_handle(accessor);
        with_accessor(h, sts_end_write)
    });

    // ---- `SharedTuplestoreAccessorHandle`-threaded family (nodeHash) ----
    seams::sts_estimate::set(sts_estimate);
    seams::sts_initialize::set(
        |sts, participants, my, meta, flags, fileset, name| {
            sts_initialize(sts, participants, my, meta, flags, fileset, name)
        },
    );
    seams::sts_attach::set(|sts, my, fileset| sts_attach(sts, my, fileset));
    seams::sts_reinitialize::set(|accessor| {
        with_accessor(accessor, sts_reinitialize);
        Ok(())
    });
    seams::sts_begin_parallel_scan_handle::set(|accessor| {
        with_accessor(accessor, sts_begin_parallel_scan)
    });
    seams::sts_end_parallel_scan_handle::set(|accessor| {
        let _ = with_accessor(accessor, sts_end_parallel_scan);
    });
    seams::sts_puttuple_handle::set(|accessor, meta, tuple| {
        with_accessor(accessor, |acc| sts_puttuple(acc, meta, tuple))
    });
    seams::sts_end_write_handle::set(|accessor| with_accessor(accessor, sts_end_write));
    seams::sts_parallel_scan_next_handle::set(|mcx, accessor, meta| {
        let next = with_accessor(accessor, |acc| sts_parallel_scan_next(acc, meta))?;
        match next {
            None => Ok(None),
            Some(blob) => {
                let mut v = mcx::vec_with_capacity_in(mcx, blob.len())?;
                v.extend_from_slice(&blob);
                Ok(Some(v))
            }
        }
    });
}

/// Recover the 1-based accessor handle from the `Opaque` payload of a
/// `SharedTuplestoreAccessor` (the token box nodeHash put there in `box_accessor`).
fn accessor_handle(
    accessor: &::nodes::nodehash::SharedTuplestoreAccessor,
) -> SharedTuplestoreAccessorHandle {
    *accessor
        .0
         .0
        .as_ref()
        .and_then(|any| any.downcast_ref::<SharedTuplestoreAccessorHandle>())
        .expect("sharedtuplestore: accessor Opaque does not carry a handle token")
}
