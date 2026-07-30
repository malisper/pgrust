// tuplestore.c: TSS_INMEM + the TSS_WRITEFILE/TSS_READFILE spill arms over
// BufFile.
#![allow(non_snake_case)]

use core::mem;

use ::datum::Datum;
use ::mcx::{bind, Mcx, McxOwned, MemoryContext, PgVec};
use ::types_error::{PgError, PgResult};
use ::types_slot::{SlotData, EXEC_FLAG_BACKWARD, EXEC_FLAG_REWIND};
use ::types_tuple::htup::MINIMAL_TUPLE_DATA_OFFSET;
use ::types_tuple::{MinimalTupleData, TupleDescData};

use fd::buffile::SEEK_CUR;
use fd::buffile::SEEK_SET;
use fd::BufFile;

pub mod hold;

#[cfg(test)]
mod tests;

pub fn init_seams() {
    hold::install_seams();
}

// C: Max(16384 / sizeof(void*), ALLOCSET_SEPARATE_THRESHOLD / sizeof(void*) + 1).
const INITIAL_MEMTUPSIZE: usize = 2048;

#[inline]
const fn maxalign(len: usize) -> usize {
    (len + 7) & !7
}

const PTR_SIZE: usize = mem::size_of::<*mut MinimalTupleData>();

// C availMem is GetMemoryChunkSpace: generation chunks for tuples, aset for
// memtuples — tuplestore_get_stats byte-parity depends on these exact shapes.
const CHUNKHDRSZ: i64 = 8;
const ALLOC_CHUNK_LIMIT: usize = 8192;

#[inline]
fn generation_chunk_space(len: usize) -> i64 {
    maxalign(len) as i64 + CHUNKHDRSZ
}

#[inline]
fn aset_chunk_space(len: usize) -> i64 {
    if len > ALLOC_CHUNK_LIMIT {
        maxalign(len) as i64 + CHUNKHDRSZ
    } else {
        len.next_power_of_two().max(8) as i64 + CHUNKHDRSZ
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TupStoreStatus {
    InMem,
    WriteFile,
    ReadFile,
}

#[derive(Clone, Copy)]
struct ReadPointer {
    eflags: i32,
    eof_reached: bool,
    current: usize,
    // File position, valid in the file states (C TSReadPointer.file/offset).
    file: i32,
    offset: i64,
}

pub struct TuplestoreData<'m> {
    mcx: Mcx<'m>,
    tuplecontext: MemoryContext,
    status: TupStoreStatus,
    eflags: i32,
    backward: bool,
    inter_xact: bool,
    truncated: bool,
    used_disk: bool,
    allowed_mem: i64,
    avail_mem: i64,
    grow_memtuples: bool,
    tuples: i64,
    max_space: i64,
    myfile: Option<BufFile<'m>>,
    writepos_file: i32,
    writepos_offset: i64,
    memtuples: PgVec<'m, *mut MinimalTupleData>,
    memtupdeleted: usize,
    readptrs: PgVec<'m, ReadPointer>,
    activeptr: usize,
    // Retained flat-image scratch for the READFILE readtup.
    read_scratch: PgVec<'m, u8>,
}

bind!(pub TuplestoreTy => TuplestoreData<'mcx>);

// Drop is the fd guard: a spilled store owns an open temp-file VFD that must
// close before the query's resowner cross-check (C closes in tuplestore_end).
pub struct Tuplestore(McxOwned<TuplestoreTy>);

impl Drop for Tuplestore {
    fn drop(&mut self) {
        self.0.with_mut(|st| {
            if let Some(file) = st.myfile.take() {
                let _ = file.close();
            }
        })
    }
}

#[inline]
fn cfi() -> PgResult<()> {
    if init_small::globals::InterruptPending() {
        return postgres_seams::check_for_interrupts::call();
    }
    Ok(())
}

const RP0: ReadPointer =
    ReadPointer { eflags: 0, eof_reached: false, current: 0, file: 0, offset: 0 };

#[track_caller]
#[cold]
#[inline(never)]
fn seek_failed() -> Box<PgError> {
    Box::new(PgError::error("could not seek in tuplestore temporary file"))
}

// C's tuplestore_begin_heap creates no memory context; our two-context shell
// must not be a per-call create+destroy pair (nested stores fall back to
// fresh construction).
mod ts_pool {
    thread_local! {
        static SLOT: core::cell::RefCell<Option<super::Tuplestore>> =
            const { core::cell::RefCell::new(None) };
    }

    pub(crate) fn take() -> Option<super::Tuplestore> {
        SLOT.with(|s| s.borrow_mut().take())
    }

    pub(crate) fn park(ts: super::Tuplestore) {
        SLOT.with(|s| {
            let mut slot = s.borrow_mut();
            if slot.is_none() {
                *slot = Some(ts);
            }
        });
    }
}

impl Tuplestore {
    pub fn begin_heap(random_access: bool, inter_xact: bool, max_kbytes: i32) -> Tuplestore {
        let eflags = if random_access {
            EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND
        } else {
            EXEC_FLAG_REWIND
        };
        if let Some(mut ts) = ts_pool::take() {
            ts.0.with_mut(|st| {
                debug_assert!(st.tuples == 0 && st.memtuples.is_empty() && st.myfile.is_none());
                let allowed_mem = i64::from(max_kbytes) * 1024;
                st.status = TupStoreStatus::InMem;
                st.eflags = eflags;
                st.inter_xact = inter_xact;
                st.truncated = false;
                st.used_disk = false;
                st.allowed_mem = allowed_mem;
                st.avail_mem = allowed_mem - aset_chunk_space(st.memtuples.capacity() * PTR_SIZE);
                st.grow_memtuples = true;
                st.max_space = 0;
                st.memtupdeleted = 0;
                st.readptrs.clear();
                st.readptrs.push(ReadPointer { eflags, ..RP0 });
                st.activeptr = 0;
            });
            return ts;
        }
        let owned = McxOwned::try_new(MemoryContext::new("tuplestore"), |mcx| {
            let allowed_mem = i64::from(max_kbytes) * 1024;
            let memtuples = PgVec::with_capacity_in(INITIAL_MEMTUPSIZE, mcx);
            let avail_mem = allowed_mem - aset_chunk_space(memtuples.capacity() * PTR_SIZE);
            let mut readptrs = PgVec::with_capacity_in(8, mcx);
            readptrs.push(ReadPointer { eflags, ..RP0 });
            Ok(TuplestoreData {
                mcx,
                // C: generation context (FIFO pfree); nothing here frees
                // per-tuple, so a wholesale-reset bump arena matches cost.
                tuplecontext: mcx.context().new_child_bump("tuplestore tuples"),
                status: TupStoreStatus::InMem,
                eflags,
                backward: false,
                inter_xact,
                truncated: false,
                used_disk: false,
                allowed_mem,
                avail_mem,
                grow_memtuples: true,
                tuples: 0,
                max_space: 0,
                myfile: None,
                writepos_file: 0,
                writepos_offset: 0,
                memtuples,
                memtupdeleted: 0,
                readptrs,
                activeptr: 0,
                read_scratch: PgVec::new_in(mcx),
            })
        })
        .expect("tuplestore context construction is infallible");
        Tuplestore(owned)
    }

    pub fn puttupleslot<'q>(
        &mut self,
        slot: &mut SlotData<'q>,
        slot_mcx: Mcx<'q>,
    ) -> PgResult<()> {
        self.0.with_mut(|st| {
            let mtup =
                exectuples::exec_copy_slot_minimal_tuple(slot, slot_mcx, st.tuplecontext.mcx(), 0)?;
            let t_len = mtup.t_len() as usize;
            let tuple = mtup.as_ptr().cast_mut().cast::<MinimalTupleData>();
            // Ownership moves to tuplecontext (bulk-freed at clear/end); the
            // wrapper must not run its deallocating Drop.
            mem::forget(mtup);
            st.puttuple_common(tuple, generation_chunk_space(t_len))
        })
    }

    pub fn put_heap_tuple(&mut self, htup: &::types_tuple::HeapTupleData<'_>) -> PgResult<()> {
        self.0.with_mut(|st| {
            let mtup =
                heaptuple::minimal_tuple_from_heap_tuple(st.tuplecontext.mcx(), htup, 0)?;
            let t_len = mtup.t_len() as usize;
            let tuple = mtup.as_ptr().cast_mut().cast::<MinimalTupleData>();
            mem::forget(mtup);
            st.puttuple_common(tuple, generation_chunk_space(t_len))
        })
    }

    pub fn putvalues(
        &mut self,
        tdesc: &TupleDescData<'_>,
        values: &[Datum],
        isnull: &[bool],
    ) -> PgResult<()> {
        self.0.with_mut(|st| {
            let mtup =
                heaptuple::heap_form_minimal_tuple(st.tuplecontext.mcx(), tdesc, values, isnull, 0)?;
            let t_len = mtup.t_len() as usize;
            let tuple = mtup.as_ptr().cast_mut().cast::<MinimalTupleData>();
            mem::forget(mtup);
            st.puttuple_common(tuple, generation_chunk_space(t_len))
        })
    }

    /// With `copy == false` the slot borrows the store's image: valid until
    /// clear/end (C's shouldFree=false contract).
    pub fn gettupleslot<'q>(
        &mut self,
        forward: bool,
        copy: bool,
        slot: &mut SlotData<'q>,
        slot_mcx: Mcx<'q>,
    ) -> PgResult<bool> {
        self.0.with_mut(|st| {
            let tuple = match st.gettuple(forward)? {
                StoreTuple::None => {
                    exectuples::exec_clear_tuple(slot, slot_mcx);
                    return Ok(false);
                }
                StoreTuple::File => {
                    // File tuples are always fresh copies (C should_free).
                    let owned =
                        heaptuple::heap_copy_minimal_tuple(slot_mcx, &st.read_scratch, 0)?;
                    exectuples::exec_store_minimal_tuple_owned(slot, slot_mcx, owned);
                    return Ok(true);
                }
                StoreTuple::Mem(tuple) => tuple,
            };
            if copy {
                // SAFETY: live tuplecontext image of t_len bytes.
                let bytes = unsafe {
                    core::slice::from_raw_parts(
                        tuple.cast_const().cast::<u8>(),
                        (*tuple).t_len as usize,
                    )
                };
                let owned = heaptuple::heap_copy_minimal_tuple(slot_mcx, bytes, 0)?;
                exectuples::exec_store_minimal_tuple_owned(slot, slot_mcx, owned);
            } else {
                // SAFETY: live t_len-byte tuplecontext image, held until
                // clear/end (caller contract above); full-image provenance —
                // a &MinimalTupleData here would shrink it to the header.
                unsafe {
                    exectuples::exec_store_minimal_tuple_ptr(
                        slot,
                        slot_mcx,
                        core::ptr::NonNull::new_unchecked(tuple),
                    )
                };
            }
            Ok(true)
        })
    }

    pub fn clear(&mut self) {
        self.0.with_mut(|st| {
            st.updatemax();
            if let Some(file) = st.myfile.take() {
                file.close()
                    .expect("tuplestore_clear: closing temp file failed");
            }
            st.tuplecontext.reset();
            st.avail_mem = st.allowed_mem - aset_chunk_space(st.memtuples.capacity() * PTR_SIZE);
            st.status = TupStoreStatus::InMem;
            st.truncated = false;
            st.memtuples.clear();
            st.memtupdeleted = 0;
            st.tuples = 0;
            for rp in st.readptrs.iter_mut() {
                rp.eof_reached = false;
                rp.current = 0;
            }
        })
    }

    pub fn rescan(&mut self) -> PgResult<()> {
        self.0.with_mut(|st| {
            let active = st.activeptr;
            debug_assert!(st.readptrs[active].eflags & EXEC_FLAG_REWIND != 0);
            debug_assert!(!st.truncated);
            match st.status {
                TupStoreStatus::InMem => {
                    let rp = &mut st.readptrs[active];
                    rp.eof_reached = false;
                    rp.current = 0;
                }
                TupStoreStatus::WriteFile => {
                    let rp = &mut st.readptrs[active];
                    rp.eof_reached = false;
                    rp.file = 0;
                    rp.offset = 0;
                }
                TupStoreStatus::ReadFile => {
                    st.readptrs[active].eof_reached = false;
                    let file = st.myfile.as_mut().expect("ReadFile without file");
                    if file.seek(0, 0, SEEK_SET)? != 0 {
                        return Err(seek_failed());
                    }
                }
            }
            Ok(())
        })
    }

    pub fn end(mut self) {
        // A grown store takes the destroy path so a big result can't pin memory.
        let park = self.0.with_mut(|st| {
            st.memtuples.capacity() <= INITIAL_MEMTUPSIZE
                && st.readptrs.capacity() <= 8
                && st.read_scratch.capacity() == 0
        });
        if park {
            self.clear();
            ts_pool::park(self);
        }
    }

    pub fn tuple_count(&self) -> i64 {
        self.0.with(|st| st.tuples)
    }

    pub fn ateof(&self) -> bool {
        self.0.with(|st| st.readptrs[st.activeptr].eof_reached)
    }

    /// `tuplestore_in_memory`.
    pub fn in_memory(&self) -> bool {
        self.0.with(|st| st.status == TupStoreStatus::InMem)
    }

    /// `tuplestore_get_stats`.
    pub fn get_stats(&mut self) -> types_core::instrument::TuplestoreInstrumentation {
        self.0.with_mut(|st| {
            st.updatemax();
            types_core::instrument::TuplestoreInstrumentation {
                space_type: if st.used_disk {
                    types_core::instrument::TuplesortSpaceType::Disk
                } else {
                    types_core::instrument::TuplesortSpaceType::Memory
                },
                max_space: st.max_space,
            }
        })
    }

    pub fn set_eflags(&mut self, eflags: i32) {
        self.0.with_mut(|st| {
            assert!(
                st.status == TupStoreStatus::InMem && st.memtuples.is_empty(),
                "too late to call tuplestore_set_eflags"
            );
            st.readptrs[0].eflags = eflags;
            let mut all = eflags;
            for rp in st.readptrs.iter().skip(1) {
                all |= rp.eflags;
            }
            st.eflags = all;
        })
    }

    /// New pointer copies pointer 0's position (C contract).
    pub fn alloc_read_pointer(&mut self, eflags: i32) -> i32 {
        self.0.with_mut(|st| {
            if st.status != TupStoreStatus::InMem || !st.memtuples.is_empty() {
                assert!(
                    (st.eflags | eflags) == st.eflags,
                    "too late to require new tuplestore eflags"
                );
            }
            let mut rp = st.readptrs[0];
            rp.eflags = eflags;
            st.readptrs.push(rp);
            st.eflags |= eflags;
            (st.readptrs.len() - 1) as i32
        })
    }

    /// C `tuplestore_advance`.
    pub fn advance(&mut self, forward: bool) -> PgResult<bool> {
        self.0
            .with_mut(|st| Ok(!matches!(st.gettuple(forward)?, StoreTuple::None)))
    }

    /// C `tuplestore_skiptuples`.
    pub fn skiptuples(&mut self, ntuples: i64, forward: bool) -> PgResult<bool> {
        if ntuples <= 0 {
            return Ok(true);
        }
        let n = ntuples as usize;
        self.0.with_mut(|st| {
            if st.status != TupStoreStatus::InMem {
                for _ in 0..ntuples {
                    if matches!(st.gettuple(forward)?, StoreTuple::None) {
                        return Ok(false);
                    }
                    cfi()?;
                }
                return Ok(true);
            }
            let count = st.memtuples.len();
            let memtupdeleted = st.memtupdeleted;
            let rp = &mut st.readptrs[st.activeptr];
            if forward {
                if rp.eof_reached {
                    return Ok(false);
                }
                if rp.current + n <= count {
                    rp.current += n;
                    return Ok(true);
                }
                rp.current = count;
                rp.eof_reached = true;
                Ok(false)
            } else {
                debug_assert!(rp.eflags & EXEC_FLAG_BACKWARD != 0);
                // C tuplestore.c:1213-1227: the first backward step from EOF
                // re-reads the last tuple without moving (ntuples--); the
                // floor is memtupdeleted, not 0.
                let mut n = n;
                if rp.eof_reached {
                    rp.current = count;
                    rp.eof_reached = false;
                    n -= 1;
                }
                if rp.current - memtupdeleted > n {
                    rp.current -= n;
                    return Ok(true);
                }
                rp.current = memtupdeleted;
                Ok(false)
            }
        })
    }

    /// `tuplestore_copy_read_pointer`.
    pub fn copy_read_pointer(&mut self, srcptr: i32, destptr: i32) -> PgResult<()> {
        self.0.with_mut(|st| {
            let (s, d) = (srcptr as usize, destptr as usize);
            debug_assert!(s < st.readptrs.len() && d < st.readptrs.len());
            if s == d {
                return Ok(());
            }
            let recompute = st.readptrs[d].eflags != st.readptrs[s].eflags;
            st.readptrs[d] = st.readptrs[s];
            if recompute {
                let mut eflags = st.readptrs[0].eflags;
                for rp in st.readptrs.iter().skip(1) {
                    eflags |= rp.eflags;
                }
                st.eflags = eflags;
            }
            if st.status == TupStoreStatus::ReadFile {
                // The active pointer's position lives in the seek point, not
                // its variables: assigning TO the active seeks, assigning
                // FROM the active tells (except at EOF).
                if d == st.activeptr {
                    let rp = st.readptrs[d];
                    let (f, off) = if rp.eof_reached {
                        (st.writepos_file, st.writepos_offset)
                    } else {
                        (rp.file, rp.offset)
                    };
                    let file = st.myfile.as_mut().expect("ReadFile without file");
                    if file.seek(f, off, SEEK_SET)? != 0 {
                        return Err(seek_failed());
                    }
                } else if s == st.activeptr && !st.readptrs[d].eof_reached {
                    let file = st.myfile.as_mut().expect("ReadFile without file");
                    let (f, off) = file.tell();
                    st.readptrs[d].file = f;
                    st.readptrs[d].offset = off;
                }
            }
            Ok(())
        })
    }

    /// `tuplestore_trim`, TSS_INMEM arm. C DIVERGENCE: the bump tuplecontext
    /// cannot free one tuple, so C's pfree is mirrored in the accounting only
    /// (spill decisions match C; the arena holds the bytes until reset).
    pub fn trim(&mut self) {
        self.0.with_mut(|st| {
            if st.eflags & EXEC_FLAG_REWIND != 0 {
                return;
            }
            // C: temp files are not worth trimming.
            if st.status != TupStoreStatus::InMem {
                return;
            }
            let count = st.memtuples.len();
            let mut oldest = count;
            for rp in st.readptrs.iter() {
                if !rp.eof_reached {
                    oldest = oldest.min(rp.current);
                }
            }
            let Some(nremove) = oldest.checked_sub(1).filter(|&n| n > 0) else {
                return;
            };
            debug_assert!(nremove >= st.memtupdeleted && nremove <= count);
            st.updatemax();
            for i in st.memtupdeleted..nremove {
                // SAFETY: live tuplecontext image; header read.
                let len = unsafe { (*st.memtuples[i]).t_len } as usize;
                st.avail_mem += generation_chunk_space(len);
            }
            st.memtupdeleted = nremove;
            if nremove < count / 8 {
                return;
            }
            st.memtuples.copy_within(nremove.., 0);
            st.memtuples.truncate(count - nremove);
            st.memtupdeleted = 0;
            for rp in st.readptrs.iter_mut() {
                if !rp.eof_reached {
                    rp.current -= nremove;
                }
            }
        })
    }

    /// `tuplestore_select_read_pointer`.
    pub fn select_read_pointer(&mut self, ptr: i32) -> PgResult<()> {
        self.0.with_mut(|st| {
            let p = ptr as usize;
            debug_assert!(p < st.readptrs.len());
            if p == st.activeptr {
                return Ok(());
            }
            if st.status == TupStoreStatus::ReadFile {
                let old = st.activeptr;
                if !st.readptrs[old].eof_reached {
                    let file = st.myfile.as_mut().expect("ReadFile without file");
                    let (f, off) = file.tell();
                    st.readptrs[old].file = f;
                    st.readptrs[old].offset = off;
                }
                let rp = st.readptrs[p];
                let (f, off) = if rp.eof_reached {
                    (st.writepos_file, st.writepos_offset)
                } else {
                    (rp.file, rp.offset)
                };
                let file = st.myfile.as_mut().expect("ReadFile without file");
                if file.seek(f, off, SEEK_SET)? != 0 {
                    return Err(seek_failed());
                }
            }
            st.activeptr = p;
            Ok(())
        })
    }
}

enum StoreTuple {
    None,
    Mem(*mut MinimalTupleData),
    // Flat image staged in read_scratch.
    File,
}

impl<'m> TuplestoreData<'m> {
    fn puttuple_common(&mut self, tuple: *mut MinimalTupleData, used: i64) -> PgResult<()> {
        self.tuples += 1;

        match self.status {
            TupStoreStatus::InMem => {
                self.avail_mem -= used;

                // Per the C API spec the ACTIVE eof reader stays at EOF
                // (advances with the write pointer); inactive eof readers
                // point at this tuple.
                let count = self.memtuples.len();
                for (i, rp) in self.readptrs.iter_mut().enumerate() {
                    if rp.eof_reached && i != self.activeptr {
                        rp.eof_reached = false;
                        rp.current = count;
                    }
                }
                if self.memtuples.len() >= self.memtuples.capacity() - 1 {
                    self.grow_memtuples();
                    debug_assert!(self.memtuples.len() < self.memtuples.capacity());
                }
                self.memtuples.push(tuple);

                if self.memtuples.len() < self.memtuples.capacity() && self.avail_mem >= 0 {
                    return Ok(());
                }

                // Switch to tape-based operation.
                let myfile = fd::BufFileCreateTemp(self.mcx, self.inter_xact)?;
                self.myfile = Some(myfile);
                self.backward = (self.eflags & EXEC_FLAG_BACKWARD) != 0;
                self.updatemax();
                self.status = TupStoreStatus::WriteFile;
                self.dumptuples()?;
                // C's WRITETUP pfrees each dumped tuple; the bump arena
                // releases them wholesale instead.
                self.tuplecontext.reset();
                self.avail_mem = self.allowed_mem
                    - aset_chunk_space(self.memtuples.capacity() * PTR_SIZE);
                Ok(())
            }
            TupStoreStatus::WriteFile => {
                let file = self.myfile.as_mut().expect("WriteFile without file");
                for (i, rp) in self.readptrs.iter_mut().enumerate() {
                    if rp.eof_reached && i != self.activeptr {
                        rp.eof_reached = false;
                        let (f, off) = file.tell();
                        rp.file = f;
                        rp.offset = off;
                    }
                }
                self.writetup(tuple)?;
                self.tuplecontext.reset();
                Ok(())
            }
            TupStoreStatus::ReadFile => {
                // Switch from reading to writing.
                let active = self.activeptr;
                let file = self.myfile.as_mut().expect("ReadFile without file");
                if !self.readptrs[active].eof_reached {
                    let (f, off) = file.tell();
                    self.readptrs[active].file = f;
                    self.readptrs[active].offset = off;
                }
                if file.seek(self.writepos_file, self.writepos_offset, SEEK_SET)? != 0 {
                    return Err(seek_failed());
                }
                self.status = TupStoreStatus::WriteFile;

                for (i, rp) in self.readptrs.iter_mut().enumerate() {
                    if rp.eof_reached && i != self.activeptr {
                        rp.eof_reached = false;
                        rp.file = self.writepos_file;
                        rp.offset = self.writepos_offset;
                    }
                }
                self.writetup(tuple)?;
                self.tuplecontext.reset();
                Ok(())
            }
        }
    }

    /// `dumptuples`: write the in-memory tuples out, converting read-pointer
    /// positions from index to file/offset form as they are passed.
    fn dumptuples(&mut self) -> PgResult<()> {
        let count = self.memtuples.len();
        for i in self.memtupdeleted..=count {
            let file = self.myfile.as_mut().expect("dumptuples without file");
            let (f, off) = file.tell();
            for rp in self.readptrs.iter_mut() {
                if i == rp.current && !rp.eof_reached {
                    rp.file = f;
                    rp.offset = off;
                }
            }
            if i >= count {
                break;
            }
            let tuple = self.memtuples[i];
            self.writetup(tuple)?;
            // Track the deletion synchronously: a later writetup failure
            // (e.g. temp_file_limit) must not leave a corrupt tuplestore,
            // which matters for persistent stores like a Portal holdStore
            // (C adb7873).
            self.memtupdeleted += 1;
        }
        self.memtupdeleted = 0;
        self.memtuples.clear();
        Ok(())
    }

    /// `writetup_heap`; the arena copy is released by the caller's
    /// tuplecontext reset rather than C's per-tuple pfree.
    fn writetup(&mut self, tuple: *mut MinimalTupleData) -> PgResult<()> {
        let file = self.myfile.as_mut().expect("writetup without file");
        // SAFETY: live tuplecontext image of t_len bytes.
        let (body, bodylen) = unsafe {
            let t_len = (*tuple).t_len as usize;
            (
                tuple.cast_const().cast::<u8>().add(MINIMAL_TUPLE_DATA_OFFSET),
                t_len - MINIMAL_TUPLE_DATA_OFFSET,
            )
        };
        let tuplen = (bodylen + mem::size_of::<u32>()) as u32;
        file.write(&tuplen.to_ne_bytes())?;
        // SAFETY: body/bodylen bound the live image.
        file.write(unsafe { core::slice::from_raw_parts(body, bodylen) })?;
        if self.backward {
            file.write(&tuplen.to_ne_bytes())?;
        }
        Ok(())
    }

    /// `getlen`.
    fn getlen(&mut self, eof_ok: bool) -> PgResult<u32> {
        let file = self.myfile.as_mut().expect("getlen without file");
        let mut buf = [0u8; 4];
        let nbytes = file.read_maybe_eof(&mut buf, eof_ok)?;
        if nbytes == 0 {
            return Ok(0);
        }
        Ok(u32::from_ne_bytes(buf))
    }

    /// `readtup_heap` into `read_scratch` as a flat minimal-tuple image.
    fn readtup(&mut self, len: u32) -> PgResult<()> {
        let bodylen = len as usize - mem::size_of::<u32>();
        let t_len = bodylen + MINIMAL_TUPLE_DATA_OFFSET;
        // Bytes 4..10 (header padding) stay zero across reuse: resize only
        // ever zero-fills growth, and no write below touches them.
        if self.read_scratch.len() < t_len {
            self.read_scratch.resize(t_len, 0);
        } else {
            self.read_scratch.truncate(t_len);
        }
        self.read_scratch[..4].copy_from_slice(&(t_len as u32).to_ne_bytes());
        let TuplestoreData { myfile, read_scratch, .. } = self;
        let file = myfile.as_mut().expect("readtup without file");
        file.read_exact(&mut read_scratch[MINIMAL_TUPLE_DATA_OFFSET..])?;
        if self.backward {
            let mut trail = [0u8; 4];
            let file = self.myfile.as_mut().expect("readtup without file");
            file.read_exact(&mut trail)?;
        }
        Ok(())
    }

    fn grow_memtuples(&mut self) -> bool {
        let memtupsize = self.memtuples.capacity();
        let mem_now_used = self.allowed_mem - self.avail_mem;

        if !self.grow_memtuples {
            return false;
        }

        let newmemtupsize = if mem_now_used <= self.avail_mem {
            if memtupsize < (i32::MAX / 2) as usize {
                memtupsize * 2
            } else {
                self.grow_memtuples = false;
                i32::MAX as usize
            }
        } else {
            let grow_ratio = self.allowed_mem as f64 / mem_now_used as f64;
            let newsize = ((memtupsize as f64 * grow_ratio) as usize).min(i32::MAX as usize);
            self.grow_memtuples = false;
            newsize
        };

        if newmemtupsize <= memtupsize
            || self.avail_mem < ((newmemtupsize - memtupsize) * PTR_SIZE) as i64
        {
            self.grow_memtuples = false;
            return false;
        }

        self.avail_mem += aset_chunk_space(memtupsize * PTR_SIZE);
        self.memtuples.reserve_exact(newmemtupsize - self.memtuples.len());
        self.avail_mem -= aset_chunk_space(self.memtuples.capacity() * PTR_SIZE);
        assert!(self.avail_mem >= 0, "unexpected out-of-memory situation in tuplestore");
        true
    }

    /// `tuplestore_updatemax`. C DIVERGENCE: a BufFileSize failure (fstat on
    /// an open temp fd) panics instead of ereporting.
    fn updatemax(&mut self) {
        if self.status == TupStoreStatus::InMem {
            self.max_space = self.max_space.max(self.allowed_mem - self.avail_mem);
        } else {
            let size = self
                .myfile
                .as_ref()
                .expect("file state without file")
                .size()
                .expect("tuplestore_updatemax: BufFileSize failed");
            self.max_space = self.max_space.max(size);
            self.used_disk = true;
        }
    }

    fn gettuple(&mut self, forward: bool) -> PgResult<StoreTuple> {
        debug_assert!(
            forward || self.readptrs[self.activeptr].eflags & EXEC_FLAG_BACKWARD != 0
        );
        match self.status {
            TupStoreStatus::InMem => {
                let count = self.memtuples.len();
                let rp = &mut self.readptrs[self.activeptr];
                if !forward {
                    if rp.eof_reached {
                        rp.current = count;
                        rp.eof_reached = false;
                    } else {
                        if rp.current <= self.memtupdeleted {
                            debug_assert!(!self.truncated);
                            return Ok(StoreTuple::None);
                        }
                        rp.current -= 1;
                    }
                    if rp.current <= self.memtupdeleted {
                        debug_assert!(!self.truncated);
                        return Ok(StoreTuple::None);
                    }
                    return Ok(StoreTuple::Mem(self.memtuples[rp.current - 1]));
                }
                if rp.eof_reached {
                    return Ok(StoreTuple::None);
                }
                if rp.current < count {
                    let t = self.memtuples[rp.current];
                    rp.current += 1;
                    return Ok(StoreTuple::Mem(t));
                }
                rp.eof_reached = true;
                Ok(StoreTuple::None)
            }
            TupStoreStatus::WriteFile => {
                let active = self.activeptr;
                if self.readptrs[active].eof_reached && forward {
                    return Ok(StoreTuple::None);
                }
                // Switch from writing to reading.
                let file = self.myfile.as_mut().expect("WriteFile without file");
                let (f, off) = file.tell();
                self.writepos_file = f;
                self.writepos_offset = off;
                if !self.readptrs[active].eof_reached
                    && file.seek(
                        self.readptrs[active].file,
                        self.readptrs[active].offset,
                        SEEK_SET,
                    )? != 0
                {
                    return Err(seek_failed());
                }
                self.status = TupStoreStatus::ReadFile;
                self.gettuple_readfile(forward)
            }
            TupStoreStatus::ReadFile => self.gettuple_readfile(forward),
        }
    }

    fn gettuple_readfile(&mut self, forward: bool) -> PgResult<StoreTuple> {
        let active = self.activeptr;
        if forward {
            let tuplen = self.getlen(true)?;
            if tuplen != 0 {
                self.readtup(tuplen)?;
                return Ok(StoreTuple::File);
            }
            self.readptrs[active].eof_reached = true;
            return Ok(StoreTuple::None);
        }

        // Backward: back up to the previously-returned tuple's trailing
        // length word; a failed seek means start of file.
        let file = self.myfile.as_mut().expect("ReadFile without file");
        if file.seek(0, -(mem::size_of::<u32>() as i64), SEEK_CUR)? != 0 {
            // Even a failed backwards fetch gets you out of eof state.
            self.readptrs[active].eof_reached = false;
            debug_assert!(!self.truncated);
            return Ok(StoreTuple::None);
        }
        let mut tuplen = self.getlen(false)?;

        if self.readptrs[active].eof_reached {
            self.readptrs[active].eof_reached = false;
        } else {
            let file = self.myfile.as_mut().expect("ReadFile without file");
            let back = (tuplen as i64) + 2 * mem::size_of::<u32>() as i64;
            if file.seek(0, -back, SEEK_CUR)? != 0 {
                // Prev tuple is the first in the file: back up so it becomes
                // next to read forward (matches the in-memory case).
                let back = (tuplen as i64) + mem::size_of::<u32>() as i64;
                if file.seek(0, -back, SEEK_CUR)? != 0 {
                    return Err(seek_failed());
                }
                debug_assert!(!self.truncated);
                return Ok(StoreTuple::None);
            }
            tuplen = self.getlen(false)?;
        }

        let file = self.myfile.as_mut().expect("ReadFile without file");
        if file.seek(0, -(tuplen as i64), SEEK_CUR)? != 0 {
            return Err(seek_failed());
        }
        self.readtup(tuplen)?;
        Ok(StoreTuple::File)
    }
}
