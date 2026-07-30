// sharedtuplestore.c, thread-native: multiple participants write, then all
// scan a page-chunked union of the files. The shared control block is an
// Arc'd struct (C: shm following ParallelHashJoinBatch); the per-participant
// LWLock is a Mutex. File sharing rides fd's FileSet (participants open each
// other's files by name; VFD tables are thread-local, so every accessor holds
// its own handles).
#![allow(non_snake_case)]

use core::ptr::NonNull;
use std::sync::{Arc, Mutex};

use ::elog::ereport;
use ::fd::fileset::FileSet;
use ::fd::BufFile;
use ::mcx::{Mcx, PgVec};
use ::types_error::{ErrorLocation, PgResult, ERROR};
use ::types_tuple::MinimalTupleData;

pub fn init_seams() {}

const BLCKSZ: usize = 8192;
const STS_CHUNK_PAGES: u32 = 4;
const STS_CHUNK_HEADER_SIZE: usize = 8; // offsetof(SharedTuplestoreChunk, data)
const STS_CHUNK_SIZE: usize = STS_CHUNK_PAGES as usize * BLCKSZ;
const STS_CHUNK_DATA_SIZE: usize = STS_CHUNK_SIZE - STS_CHUNK_HEADER_SIZE;

/// `SHARED_TUPLESTORE_SINGLE_PASS` is the only flag and is advisory in C too.
pub const SHARED_TUPLESTORE_SINGLE_PASS: i32 = 0x01;

#[track_caller]
fn loc(funcname: &'static str) -> ErrorLocation {
    // pgrust is Rust: report where in OUR source this was raised.
    // #[track_caller] resolves to the call site, not this helper.
    let site = core::panic::Location::caller();
    ErrorLocation::new(site.file(), site.line() as i32, funcname)
}

struct StsParticipant {
    read_page: u32,
    npages: u32,
    writing: bool,
}

/// The shared control object (C `SharedTuplestore`).
pub struct SharedTuplestore {
    nparticipants: i32,
    meta_data_size: usize,
    name: String,
    participants: Box<[Mutex<StsParticipant>]>,
}

impl SharedTuplestore {
    /// `sts_initialize` (shared part); accessors attach separately.
    pub fn new(nparticipants: i32, meta_data_size: usize, name: &str) -> SharedTuplestore {
        assert!(
            meta_data_size + core::mem::size_of::<u32>() < STS_CHUNK_DATA_SIZE,
            "meta-data too long"
        );
        SharedTuplestore {
            nparticipants,
            meta_data_size,
            name: name.to_string(),
            participants: (0..nparticipants.max(0))
                .map(|_| Mutex::new(StsParticipant { read_page: 0, npages: 0, writing: false }))
                .collect(),
        }
    }

    /// `sts_reinitialize`: reset every participant's shared read head. Only
    /// one participant may call this, not concurrently with a scan.
    pub fn reinitialize(&self) {
        for p in self.participants.iter() {
            p.lock().unwrap_or_else(|e| e.into_inner()).read_page = 0;
        }
    }
}

/// Per-participant, per-thread state (C `SharedTuplestoreAccessor`).
pub struct SharedTuplestoreAccessor<'mcx> {
    participant: i32,
    sts: Arc<SharedTuplestore>,
    fileset: Arc<FileSet>,
    mcx: Mcx<'mcx>,

    read_participant: i32,
    read_file: Option<BufFile<'mcx>>,
    read_ntuples_available: i32,
    read_ntuples: i32,
    read_bytes: usize,
    // u64 backing keeps the tuple image MAXALIGNed.
    read_buffer: PgVec<'mcx, u64>,
    read_next_page: u32,

    write_chunk: Option<PgVec<'mcx, u8>>,
    write_file: Option<BufFile<'mcx>>,
    write_pointer: usize,
}

fn sts_filename(name: &str, participant: i32) -> String {
    format!("{name}.p{participant}")
}

impl<'mcx> SharedTuplestoreAccessor<'mcx> {
    /// `sts_initialize`/`sts_attach` accessor part.
    pub fn attach(
        sts: Arc<SharedTuplestore>,
        fileset: Arc<FileSet>,
        participant: i32,
        mcx: Mcx<'mcx>,
    ) -> SharedTuplestoreAccessor<'mcx> {
        debug_assert!(participant < sts.nparticipants);
        SharedTuplestoreAccessor {
            participant,
            sts,
            fileset,
            mcx,
            read_participant: 0,
            read_file: None,
            read_ntuples_available: 0,
            read_ntuples: 0,
            read_bytes: 0,
            read_buffer: PgVec::new_in(mcx),
            read_next_page: 0,
            write_chunk: None,
            write_file: None,
            write_pointer: 0,
        }
    }

    fn participant_mut(
        &self,
        i: i32,
    ) -> std::sync::MutexGuard<'_, StsParticipant> {
        self.sts.participants[i as usize].lock().unwrap_or_else(|e| e.into_inner())
    }

    // `sts_flush_chunk`.
    fn flush_chunk(&mut self) -> PgResult<()> {
        let chunk = self.write_chunk.as_mut().expect("flush without a write chunk");
        self.write_file
            .as_mut()
            .expect("flush without a write file")
            .write(&chunk[..])?;
        chunk.fill(0);
        self.write_pointer = STS_CHUNK_HEADER_SIZE;
        self.participant_mut(self.participant).npages += STS_CHUNK_PAGES;
        Ok(())
    }

    /// `sts_end_write`: every writer must call this before anyone scans.
    pub fn end_write(&mut self) -> PgResult<()> {
        if self.write_file.is_some() {
            self.flush_chunk()?;
            self.write_file.take().expect("just checked").close()?;
            self.write_chunk = None;
            self.participant_mut(self.participant).writing = false;
        }
        Ok(())
    }

    /// `sts_begin_parallel_scan`.
    pub fn begin_parallel_scan(&mut self) -> PgResult<()> {
        self.end_parallel_scan()?;
        #[cfg(debug_assertions)]
        for p in self.sts.participants.iter() {
            debug_assert!(!p.lock().unwrap_or_else(|e| e.into_inner()).writing);
        }
        // Start with this participant's own file (cache locality, as C).
        self.read_participant = self.participant;
        self.read_file = None;
        self.read_next_page = 0;
        self.read_ntuples_available = 0;
        self.read_ntuples = 0;
        Ok(())
    }

    /// `sts_end_parallel_scan`.
    pub fn end_parallel_scan(&mut self) -> PgResult<()> {
        if let Some(f) = self.read_file.take() {
            f.close()?;
        }
        Ok(())
    }

    /// `sts_puttuple`: `tuple` is a full minimal-tuple image (t_len leading).
    pub fn put_tuple(&mut self, meta_data: &[u8], tuple: &[u8]) -> PgResult<()> {
        debug_assert_eq!(meta_data.len(), self.sts.meta_data_size);
        if self.write_file.is_none() {
            let name = sts_filename(&self.sts.name, self.participant);
            self.write_file = Some(::fd::BufFileCreateFileSet(self.mcx, &self.fileset, &name)?);
            self.participant_mut(self.participant).writing = true;
        }
        if self.write_chunk.is_none() {
            let mut chunk: PgVec<'mcx, u8> = ::mcx::vec_with_capacity_in(self.mcx, STS_CHUNK_SIZE)?;
            chunk.resize(STS_CHUNK_SIZE, 0);
            self.write_chunk = Some(chunk);
            self.write_pointer = STS_CHUNK_HEADER_SIZE;
        }

        let mut size = self.sts.meta_data_size + tuple.len();
        if self.write_pointer + size > STS_CHUNK_SIZE {
            // Flush and retry; a gigantic tuple then spills into overflow
            // chunks, meta + leading bytes first (C's layout exactly).
            self.flush_chunk()?;
            if self.write_pointer + size > STS_CHUNK_SIZE {
                let meta_size = self.sts.meta_data_size;
                debug_assert!(
                    self.write_pointer + meta_size + core::mem::size_of::<u32>() < STS_CHUNK_SIZE
                );
                {
                    let chunk = self.write_chunk.as_mut().expect("chunk allocated");
                    chunk[self.write_pointer..self.write_pointer + meta_size]
                        .copy_from_slice(meta_data);
                }
                let mut written = STS_CHUNK_SIZE - self.write_pointer - meta_size;
                {
                    let wp = self.write_pointer + meta_size;
                    let chunk = self.write_chunk.as_mut().expect("chunk allocated");
                    chunk[wp..wp + written].copy_from_slice(&tuple[..written]);
                    let ntuples =
                        i32::from_ne_bytes(chunk[0..4].try_into().unwrap()) + 1;
                    chunk[0..4].copy_from_slice(&ntuples.to_ne_bytes());
                }
                size -= meta_size;
                size -= written;
                while size > 0 {
                    self.flush_chunk()?;
                    let overflow =
                        ((size + STS_CHUNK_DATA_SIZE - 1) / STS_CHUNK_DATA_SIZE) as i32;
                    let chunk = self.write_chunk.as_mut().expect("chunk allocated");
                    chunk[4..8].copy_from_slice(&overflow.to_ne_bytes());
                    let written_this_chunk = size.min(STS_CHUNK_SIZE - self.write_pointer);
                    chunk[self.write_pointer..self.write_pointer + written_this_chunk]
                        .copy_from_slice(&tuple[written..written + written_this_chunk]);
                    self.write_pointer += written_this_chunk;
                    size -= written_this_chunk;
                    written += written_this_chunk;
                }
                return Ok(());
            }
        }

        let wp = self.write_pointer;
        let meta_size = self.sts.meta_data_size;
        let chunk = self.write_chunk.as_mut().expect("chunk allocated");
        chunk[wp..wp + meta_size].copy_from_slice(meta_data);
        chunk[wp + meta_size..wp + meta_size + tuple.len()].copy_from_slice(tuple);
        self.write_pointer += size;
        let ntuples = i32::from_ne_bytes(chunk[0..4].try_into().unwrap()) + 1;
        chunk[0..4].copy_from_slice(&ntuples.to_ne_bytes());
        Ok(())
    }

    // `sts_read_tuple`: the image is valid until the next call.
    fn read_tuple(&mut self, meta_data: &mut [u8]) -> PgResult<NonNull<MinimalTupleData>> {
        let meta_size = self.sts.meta_data_size;
        let file = self.read_file.as_mut().expect("read_tuple without an open file");
        if meta_size > 0 {
            file.read_exact(meta_data)?;
            self.read_bytes += meta_size;
        }
        let mut size_buf = [0u8; 4];
        file.read_exact(&mut size_buf)?;
        self.read_bytes += 4;
        let size = u32::from_ne_bytes(size_buf) as usize;
        self.read_buffer.clear();
        self.read_buffer.resize(size.div_ceil(8), 0);
        // SAFETY: u64 backing reinterpreted as bytes; length covers size.
        let image: &mut [u8] = unsafe {
            core::slice::from_raw_parts_mut(self.read_buffer.as_mut_ptr().cast(), size)
        };
        image[0..4].copy_from_slice(&size_buf);
        let mut remaining_size = size - 4;
        let mut this_chunk_size = remaining_size.min(STS_CHUNK_SIZE - self.read_bytes);
        let mut dest = 4usize;
        file.read_exact(&mut image[dest..dest + this_chunk_size])?;
        self.read_bytes += this_chunk_size;
        remaining_size -= this_chunk_size;
        dest += this_chunk_size;
        self.read_ntuples += 1;

        while remaining_size > 0 {
            // Positioned at the start of an overflow chunk.
            let mut header = [0u8; STS_CHUNK_HEADER_SIZE];
            file.read_exact(&mut header)?;
            self.read_bytes = STS_CHUNK_HEADER_SIZE;
            let ntuples = i32::from_ne_bytes(header[0..4].try_into().unwrap());
            let overflow = i32::from_ne_bytes(header[4..8].try_into().unwrap());
            if overflow == 0 {
                ereport(ERROR)
                    .errcode_for_file_access()
                    .errmsg("unexpected chunk in shared tuplestore temporary file")
                    .errdetail_internal("Expected overflow chunk.")
                    .finish(loc("sts_read_tuple"))?;
            }
            self.read_next_page += STS_CHUNK_PAGES;
            this_chunk_size = remaining_size.min(STS_CHUNK_SIZE - STS_CHUNK_HEADER_SIZE);
            file.read_exact(&mut image[dest..dest + this_chunk_size])?;
            self.read_bytes += this_chunk_size;
            remaining_size -= this_chunk_size;
            dest += this_chunk_size;
            // Regular tuples may follow the spilled one in this chunk.
            self.read_ntuples = 0;
            self.read_ntuples_available = ntuples;
        }

        Ok(NonNull::new(image.as_mut_ptr().cast::<MinimalTupleData>())
            .expect("read buffer is non-null"))
    }

    /// `sts_parallel_scan_next`: None when this participant's scan is done.
    pub fn parallel_scan_next(
        &mut self,
        meta_data: &mut [u8],
    ) -> PgResult<Option<NonNull<MinimalTupleData>>> {
        loop {
            if self.read_ntuples < self.read_ntuples_available {
                return Ok(Some(self.read_tuple(meta_data)?));
            }

            if init_small::globals::InterruptPending() {
                postgres_seams::check_for_interrupts::call()?;
            }

            let (eof, read_page) = {
                let mut p = self.sts.participants[self.read_participant as usize]
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                // Skip directly past overflow pages we know about.
                if p.read_page < self.read_next_page {
                    p.read_page = self.read_next_page;
                }
                let eof = p.read_page >= p.npages;
                if !eof {
                    let page = p.read_page;
                    p.read_page += STS_CHUNK_PAGES;
                    self.read_next_page = p.read_page;
                    (false, page)
                } else {
                    (true, 0)
                }
            };

            if !eof {
                if self.read_file.is_none() {
                    let name = sts_filename(&self.sts.name, self.read_participant);
                    self.read_file = Some(::fd::BufFileOpenFileSet(
                        self.mcx,
                        &self.fileset,
                        &name,
                        true,
                    )?);
                }
                let file = self.read_file.as_mut().expect("just opened");
                if file.seek_block(read_page as i64)? != 0 {
                    ereport(ERROR)
                        .errcode_for_file_access()
                        .errmsg(format!(
                            "could not seek to block {read_page} in shared tuplestore temporary file"
                        ))
                        .finish(loc("sts_parallel_scan_next"))?;
                }
                let mut header = [0u8; STS_CHUNK_HEADER_SIZE];
                file.read_exact(&mut header)?;
                let ntuples = i32::from_ne_bytes(header[0..4].try_into().unwrap());
                let overflow = i32::from_ne_bytes(header[4..8].try_into().unwrap());
                if overflow > 0 {
                    // Skip the whole overflow run at once.
                    self.read_next_page = read_page + overflow as u32 * STS_CHUNK_PAGES;
                    continue;
                }
                self.read_ntuples = 0;
                self.read_ntuples_available = ntuples;
                self.read_bytes = STS_CHUNK_HEADER_SIZE;
                // Go around to pull a tuple from this chunk.
            } else {
                if let Some(f) = self.read_file.take() {
                    f.close()?;
                }
                self.read_participant =
                    (self.read_participant + 1) % self.sts.nparticipants;
                if self.read_participant == self.participant {
                    break;
                }
                self.read_next_page = 0;
            }
        }
        Ok(None)
    }
}

// Exempt: BufFiles are closed by end_write/end_parallel_scan; the error path
// leaks fds until resowner cleanup, as BufFile's own convention.
mcx::forget_safe_struct!(
    SharedTuplestoreAccessor<'_> { participant, mcx, read_participant,
        read_ntuples_available, read_ntuples, read_bytes, read_buffer,
        read_next_page, write_pointer;
        sts, fileset, read_file, write_file, write_chunk },
);
