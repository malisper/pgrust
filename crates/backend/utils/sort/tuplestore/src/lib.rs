// tuplestore.c, TSS_INMEM arms only. Spill to tape (TSS_WRITEFILE/READFILE
// over BufFile) and trim are loud panics naming their C lanes.
#![allow(non_snake_case)]

use core::mem;

use ::datum::Datum;
use ::mcx::{bind, Mcx, McxOwned, MemoryContext, PgVec};
use ::types_error::PgResult;
use ::types_slot::{SlotData, EXEC_FLAG_BACKWARD, EXEC_FLAG_REWIND};
use ::types_tuple::{MinimalTupleData, TupleDescData};

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

#[derive(Clone, Copy)]
struct ReadPointer {
    eflags: i32,
    eof_reached: bool,
    current: usize,
}

pub struct TuplestoreData<'m> {
    tuplecontext: MemoryContext,
    eflags: i32,
    allowed_mem: i64,
    avail_mem: i64,
    grow_memtuples: bool,
    tuples: i64,
    memtuples: PgVec<'m, *mut MinimalTupleData>,
    readptrs: PgVec<'m, ReadPointer>,
    activeptr: usize,
}

bind!(pub TuplestoreTy => TuplestoreData<'mcx>);

pub struct Tuplestore(McxOwned<TuplestoreTy>);

#[cold]
#[inline(never)]
fn spill_unported(allowed_mem: i64) -> ! {
    panic!(
        "tuplestore: work_mem ({allowed_mem}B) exceeded; spill to tape \
         (TSS_WRITEFILE dumptuples over BufFile, tuplestore.c) not ported"
    )
}

impl Tuplestore {
    /// `inter_xact` only matters to the BufFile arm, which is loud.
    pub fn begin_heap(random_access: bool, _inter_xact: bool, max_kbytes: i32) -> Tuplestore {
        let eflags = if random_access {
            EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND
        } else {
            EXEC_FLAG_REWIND
        };
        let owned = McxOwned::try_new(MemoryContext::new("tuplestore"), |mcx| {
            let allowed_mem = i64::from(max_kbytes) * 1024;
            let memtuples = PgVec::with_capacity_in(INITIAL_MEMTUPSIZE, mcx);
            let avail_mem = allowed_mem - (memtuples.capacity() * PTR_SIZE) as i64;
            let mut readptrs = PgVec::with_capacity_in(8, mcx);
            readptrs.push(ReadPointer { eflags, eof_reached: false, current: 0 });
            Ok(TuplestoreData {
                // C: generation context (FIFO pfree); nothing here frees
                // per-tuple, so a wholesale-reset bump arena matches cost.
                tuplecontext: mcx.context().new_child_bump("tuplestore tuples"),
                eflags,
                allowed_mem,
                avail_mem,
                grow_memtuples: true,
                tuples: 0,
                memtuples,
                readptrs,
                activeptr: 0,
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
            st.puttuple_common(tuple, maxalign(t_len) as i64)
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
            st.puttuple_common(tuple, maxalign(t_len) as i64)
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
            let Some(tuple) = st.gettuple(forward) else {
                exectuples::exec_clear_tuple(slot, slot_mcx);
                return Ok(false);
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
                // SAFETY: lifetime laundered to the slot's, as C stores the
                // borrowed pointer with shouldFree=false; the image lives in
                // tuplecontext until clear/end (caller contract above).
                let mtref: &'q MinimalTupleData = unsafe { &*tuple };
                exectuples::exec_store_minimal_tuple(slot, slot_mcx, mtref);
            }
            Ok(true)
        })
    }

    pub fn clear(&mut self) {
        self.0.with_mut(|st| {
            st.tuplecontext.reset();
            st.avail_mem = st.allowed_mem - (st.memtuples.capacity() * PTR_SIZE) as i64;
            st.memtuples.clear();
            st.tuples = 0;
            for rp in st.readptrs.iter_mut() {
                rp.eof_reached = false;
                rp.current = 0;
            }
        })
    }

    pub fn rescan(&mut self) {
        self.0.with_mut(|st| {
            let active = st.activeptr;
            let rp = &mut st.readptrs[active];
            debug_assert!(rp.eflags & EXEC_FLAG_REWIND != 0);
            rp.eof_reached = false;
            rp.current = 0;
        })
    }

    pub fn end(self) {}

    pub fn tuple_count(&self) -> i64 {
        self.0.with(|st| st.tuples)
    }

    pub fn ateof(&self) -> bool {
        self.0.with(|st| st.readptrs[st.activeptr].eof_reached)
    }

    /// Spill is loud, so always true.
    pub fn in_memory(&self) -> bool {
        true
    }

    pub fn set_eflags(&mut self, eflags: i32) {
        self.0.with_mut(|st| {
            assert!(st.memtuples.is_empty(), "too late to call tuplestore_set_eflags");
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
            if !st.memtuples.is_empty() {
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

    /// C `tuplestore_advance`: move the active pointer one tuple without
    /// returning it.
    pub fn advance(&mut self, forward: bool) -> bool {
        self.0.with_mut(|st| st.gettuple(forward).is_some())
    }

    /// C `tuplestore_skiptuples`, TSS_INMEM arm: position arithmetic, no
    /// tuple reads.
    pub fn skiptuples(&mut self, ntuples: i64, forward: bool) -> bool {
        if ntuples <= 0 {
            return true;
        }
        let n = ntuples as usize;
        self.0.with_mut(|st| {
            let count = st.memtuples.len();
            let rp = &mut st.readptrs[st.activeptr];
            if forward {
                if rp.eof_reached {
                    return false;
                }
                if rp.current + n <= count {
                    rp.current += n;
                    return true;
                }
                rp.current = count;
                rp.eof_reached = true;
                false
            } else {
                debug_assert!(rp.eflags & EXEC_FLAG_BACKWARD != 0);
                let cur = if rp.eof_reached { count } else { rp.current };
                // C: n+1 backward steps then one forward re-read; net effect
                // is current -= n with the tuple floor at position 1.
                if cur > n {
                    rp.eof_reached = false;
                    rp.current = cur - n;
                    return true;
                }
                rp.eof_reached = false;
                rp.current = 0;
                false
            }
        })
    }

    /// TSS_INMEM select is a pure index swap; READFILE seek save/restore is
    /// the spill lane's problem.
    pub fn select_read_pointer(&mut self, ptr: i32) {
        self.0.with_mut(|st| {
            debug_assert!((ptr as usize) < st.readptrs.len());
            st.activeptr = ptr as usize;
        })
    }
}

impl<'m> TuplestoreData<'m> {
    fn puttuple_common(&mut self, tuple: *mut MinimalTupleData, used: i64) -> PgResult<()> {
        self.avail_mem -= used;
        self.tuples += 1;

        // Per the C API spec the ACTIVE eof reader stays at EOF (advances
        // with the write pointer); inactive eof readers point at this tuple.
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
        spill_unported(self.allowed_mem)
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

        self.avail_mem += (memtupsize * PTR_SIZE) as i64;
        self.memtuples.reserve_exact(newmemtupsize - self.memtuples.len());
        self.avail_mem -= (self.memtuples.capacity() * PTR_SIZE) as i64;
        assert!(self.avail_mem >= 0, "unexpected out-of-memory situation in tuplestore");
        true
    }

    fn gettuple(&mut self, forward: bool) -> Option<*mut MinimalTupleData> {
        let count = self.memtuples.len();
        let rp = &mut self.readptrs[self.activeptr];
        if !forward {
            debug_assert!(rp.eflags & EXEC_FLAG_BACKWARD != 0);
            // C's memtupdeleted floor is 0 here (trim unported).
            if rp.eof_reached {
                rp.current = count;
                rp.eof_reached = false;
            } else {
                if rp.current == 0 {
                    return None;
                }
                rp.current -= 1;
            }
            if rp.current == 0 {
                return None;
            }
            return Some(self.memtuples[rp.current - 1]);
        }
        if rp.eof_reached {
            return None;
        }
        if rp.current < count {
            let t = self.memtuples[rp.current];
            rp.current += 1;
            return Some(t);
        }
        rp.eof_reached = true;
        None
    }
}
