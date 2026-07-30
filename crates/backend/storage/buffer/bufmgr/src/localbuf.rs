//! localbuf.c: backend-local buffers for temp relations. One backend = one
//! thread, so every structure is TLS and `state` uses unsynchronized
//! load/store (C's pg_atomic_unlocked_write_u32 contract); the header lock is
//! never taken on a local descriptor.

use core::cell::{Cell, UnsafeCell};
use core::mem::ManuallyDrop;
use core::sync::atomic::Ordering;

use elog::ereport;
use mcx::{MemoryContext, PgFxHashMap};
use types_core::{
    BlockNumber, Buffer, ForkNumber, MaxBlockNumber, BLCKSZ,
};
use types_error::{
    ErrorLocation, PgResult, ERRCODE_INSUFFICIENT_RESOURCES, ERRCODE_PROGRAM_LIMIT_EXCEEDED, ERROR,
};
use pgstat::io::{
    pgstat_count_io_op, pgstat_count_io_op_time, pgstat_prepare_io_time, IOObject, IOOp,
};
use types_storage::buf::{
    buftag, IOContext, BM_DIRTY, BM_JUST_DIRTIED, BM_MAX_USAGE_COUNT, BM_TAG_VALID, BM_VALID,
    BUF_FLAG_MASK, BUF_REFCOUNT_ONE, BUF_USAGECOUNT_MASK, BUF_USAGECOUNT_ONE,
};
use types_storage::{RelFileLocator, RelFileLocatorBackend};

use crate::buf_hdr::{cleared_buftag, BufferDesc, BufferDescriptorGetBuffer, PG_IO_ALIGN_SIZE};
use crate::counters;
use crate::pin::{buffer_refcount, buffer_usagecount};

const MAX_ALLOC_SIZE: usize = 0x3fffffff;

struct LocalBufs {
    descs: &'static [BufferDesc],
    blocks: &'static [Cell<*mut u8>],
    ref_counts: &'static [Cell<i32>],
    hash: ManuallyDrop<PgFxHashMap<'static, buftag, i32>>,
    next_free: i32,
    pinned: i32,
    cur_block: *mut u8,
    next_buf_in_block: usize,
    num_bufs_in_block: usize,
    total_bufs_allocated: usize,
}

thread_local! {
    static LOCAL: UnsafeCell<Option<LocalBufs>> = const { UnsafeCell::new(None) };
}

#[inline]
fn with<R>(f: impl FnOnce(&mut LocalBufs) -> R) -> R {
    LOCAL.with(|l| {
        // SAFETY: one backend = one thread; no callee below re-enters this
        // module through the same TLS slot (internal helpers take &mut in).
        let slot = unsafe { &mut *l.get() };
        if slot.is_none() {
            init_local_buffers(slot);
        }
        f(slot.as_mut().expect("local buffers initialized"))
    })
}

#[inline]
fn with_inited<R>(default: R, f: impl FnOnce(&mut LocalBufs) -> R) -> R {
    LOCAL.with(|l| {
        // SAFETY: as in `with`.
        match unsafe { &mut *l.get() } {
            Some(lb) => f(lb),
            None => default,
        }
    })
}

fn num_temp_buffers() -> i32 {
    if guc_tables::vars::num_temp_buffers.installed() {
        guc_tables::vars::num_temp_buffers.read()
    } else {
        1024
    }
}

/// NLocBuffer: 0 until local buffers are first initialized (localbuf.c).
pub fn n_loc_buffer() -> i32 {
    with_inited(0, |lb| lb.descs.len() as i32)
}

#[cold]
fn init_local_buffers(slot: &mut Option<LocalBufs>) {
    if parallel_seams::is_parallel_worker::is_installed()
        && parallel_seams::is_parallel_worker::call()
    {
        panic!("cannot access temporary tables during a parallel operation");
    }
    let nbufs = num_temp_buffers().max(1) as usize;
    let mut descs = Vec::with_capacity(nbufs);
    for i in 0..nbufs {
        descs.push(BufferDesc::initial(-(i as i32) - 2, 0));
    }
    let cx: &'static MemoryContext = ::mcx::session_root("LocalBufferLookup");
    slot.replace(LocalBufs {
        descs: Box::leak(descs.into_boxed_slice()),
        blocks: Box::leak(vec![Cell::new(core::ptr::null_mut()); nbufs].into_boxed_slice()),
        ref_counts: Box::leak(vec![Cell::new(0); nbufs].into_boxed_slice()),
        hash: ManuallyDrop::new(PgFxHashMap::with_hasher_in(Default::default(), cx.mcx())),
        next_free: 0,
        pinned: 0,
        cur_block: core::ptr::null_mut(),
        next_buf_in_block: 0,
        num_bufs_in_block: 0,
        total_bufs_allocated: 0,
    });
}

#[inline]
pub(crate) fn local_bufid(buffer: Buffer) -> usize {
    debug_assert!(buffer < 0);
    (-buffer - 1) as usize
}

pub(crate) fn local_desc(buffer: Buffer) -> &'static BufferDesc {
    with(|lb| {
        let descs: &'static [BufferDesc] = lb.descs;
        &descs[local_bufid(buffer)]
    })
}

pub(crate) fn local_ref_count(buffer: Buffer) -> i32 {
    with_inited(0, |lb| lb.ref_counts[local_bufid(buffer)].get())
}

pub(crate) fn incr_local_ref_count(buffer: Buffer) {
    with(|lb| {
        let rc = &lb.ref_counts[local_bufid(buffer)];
        debug_assert!(rc.get() > 0);
        rc.set(rc.get() + 1);
    })
}

pub(crate) fn local_block_ptr(buffer: Buffer) -> *mut u8 {
    with(|lb| lb.blocks[local_bufid(buffer)].get())
}

fn init_buffer_tag(rlocator: RelFileLocator, forknum: ForkNumber, blkno: BlockNumber) -> buftag {
    buftag {
        spcOid: rlocator.spcOid,
        dbOid: rlocator.dbOid,
        relNumber: rlocator.relNumber,
        forkNum: forknum,
        blockNum: blkno,
    }
}

#[inline]
fn state_of(desc: &BufferDesc) -> u32 {
    desc.state.load(Ordering::Relaxed)
}

#[inline]
fn set_state(desc: &BufferDesc, v: u32) {
    desc.state.store(v, Ordering::Relaxed);
}

fn pin_local(lb: &mut LocalBufs, id: usize, adjust_usagecount: bool) -> bool {
    let desc = &lb.descs[id];
    let mut state = state_of(desc);
    if lb.ref_counts[id].get() == 0 {
        lb.pinned += 1;
        state += BUF_REFCOUNT_ONE;
        if adjust_usagecount && buffer_usagecount(state) < BM_MAX_USAGE_COUNT {
            state += BUF_USAGECOUNT_ONE;
        }
        set_state(desc, state);
    }
    lb.ref_counts[id].set(lb.ref_counts[id].get() + 1);
    crate::pin::RememberBufferPin(BufferDescriptorGetBuffer(desc));
    state & BM_VALID != 0
}

pub(crate) fn PinLocalBuffer(buffer: Buffer, adjust_usagecount: bool) -> bool {
    with(|lb| pin_local(lb, local_bufid(buffer), adjust_usagecount))
}

pub(crate) fn UnpinLocalBuffer(buffer: Buffer) {
    UnpinLocalBufferNoOwner(buffer);
    // Same never-panic contract as pin::ForgetBufferPin: unpins can run
    // from drop guards during unwind.
    crate::pin::ForgetBufferPin(buffer);
}

pub(crate) fn UnpinLocalBufferNoOwner(buffer: Buffer) {
    with(|lb| {
        let id = local_bufid(buffer);
        debug_assert!(lb.ref_counts[id].get() > 0);
        debug_assert!(lb.pinned > 0);
        lb.ref_counts[id].set(lb.ref_counts[id].get() - 1);
        if lb.ref_counts[id].get() == 0 {
            let desc = &lb.descs[id];
            lb.pinned -= 1;
            let state = state_of(desc);
            debug_assert!(buffer_refcount(state) > 0);
            set_state(desc, state - BUF_REFCOUNT_ONE);
        }
    })
}

/// LocalBufferAlloc: find or create; (buffer, found). Only default access
/// strategy exists locally, so usage_count always advances on a hit.
pub(crate) fn LocalBufferAlloc(
    smgr: RelFileLocatorBackend,
    forknum: ForkNumber,
    blkno: BlockNumber,
) -> PgResult<(Buffer, bool)> {
    let new_tag = init_buffer_tag(smgr.locator, forknum, blkno);
    resowner::ResourceOwnerEnlarge(resowner::CurrentResourceOwner())?;
    if let Some(buffer) = with(|lb| {
        lb.hash.get(&new_tag).copied().map(|id| {
            debug_assert!(lb.descs[id as usize].tag() == new_tag);
            BufferDescriptorGetBuffer(&lb.descs[id as usize])
        })
    }) {
        let valid = PinLocalBuffer(buffer, true);
        return Ok((buffer, valid));
    }

    let victim = GetLocalVictimBuffer()?;
    with(|lb| {
        let id = local_bufid(victim);
        let prev = lb.hash.insert(new_tag, id as i32);
        assert!(prev.is_none(), "local buffer hash table corrupted");
        let desc = &lb.descs[id];
        // SAFETY: single-threaded local descriptor; only reference is our pin.
        unsafe { desc.set_tag(new_tag) };
        let mut state = state_of(desc);
        state &= !(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
        state |= BM_TAG_VALID | BUF_USAGECOUNT_ONE;
        set_state(desc, state);
    });
    Ok((victim, false))
}

pub(crate) fn FlushLocalBuffer(buffer: Buffer) -> PgResult<()> {
    debug_assert!(local_ref_count(buffer) > 0);
    if !StartLocalBufferIO(buffer, false) {
        return Ok(());
    }
    let (tag, block) = with(|lb| {
        let id = local_bufid(buffer);
        (lb.descs[id].tag(), lb.blocks[id].get())
    });
    // SAFETY: pinned local block; sole thread of access.
    let page = unsafe { &mut *(block as *mut [u8; BLCKSZ]) };
    crate::write::PageSetChecksumInplace(page, tag.blockNum);
    let reln = RelFileLocatorBackend {
        locator: RelFileLocator::new(tag.spcOid, tag.dbOid, tag.relNumber),
        backend: init_small::globals::MyProcNumber(),
    };
    let io_start = pgstat_prepare_io_time(crate::gucs::track_io_timing());
    // Local buffers are this thread's private blocks (the debug_assert above
    // is the pin), so an exclusive-image chunk is exactly right here.
    smgr_seams::smgr_write::call(
        reln,
        tag.forkNum,
        tag.blockNum,
        types_storage::WriteChunk::from_slice(&page[..]),
        false,
    )?;
    pgstat_count_io_op_time(
        IOObject::TempRelation,
        IOContext::IOCONTEXT_NORMAL,
        IOOp::Write,
        io_start,
        1,
        BLCKSZ as u64,
    );
    TerminateLocalBufferIO(buffer, true, 0);
    counters::local_written();
    Ok(())
}

pub(crate) fn GetLocalVictimBuffer() -> PgResult<Buffer> {
    resowner::ResourceOwnerEnlarge(resowner::CurrentResourceOwner())?;
    let buffer = with(|lb| {
        let n = lb.descs.len() as i32;
        let mut trycounter = n;
        loop {
            let victim = lb.next_free;
            lb.next_free += 1;
            if lb.next_free >= n {
                lb.next_free = 0;
            }
            let desc = &lb.descs[victim as usize];
            if lb.ref_counts[victim as usize].get() == 0 {
                let state = state_of(desc);
                if buffer_usagecount(state) > 0 {
                    set_state(desc, state - BUF_USAGECOUNT_ONE);
                    trycounter = n;
                } else {
                    pin_local(lb, victim as usize, false);
                    break Ok(BufferDescriptorGetBuffer(desc));
                }
            } else {
                trycounter -= 1;
                if trycounter == 0 {
                    break Err(());
                }
            }
        }
    });
    let buffer = match buffer {
        Ok(b) => b,
        Err(()) => {
            ereport(ERROR)
                .errcode(ERRCODE_INSUFFICIENT_RESOURCES)
                .errmsg("no empty local buffer available")
                .finish(ErrorLocation::new(file!(), line!() as i32, "GetLocalVictimBuffer"))?;
            unreachable!("ERROR reported");
        }
    };

    let id = local_bufid(buffer);
    with(|lb| {
        if lb.blocks[id].get().is_null() {
            lb.blocks[id].set(get_local_buffer_storage(lb));
        }
    });
    let state = with(|lb| state_of(&lb.descs[id]));
    if state & BM_DIRTY != 0 {
        FlushLocalBuffer(buffer)?;
    }
    if state & BM_TAG_VALID != 0 {
        InvalidateLocalBuffer(buffer, false)?;
        pgstat_count_io_op(
            IOObject::TempRelation,
            IOContext::IOCONTEXT_NORMAL,
            IOOp::Evict,
            1,
            0,
        );
    }
    Ok(buffer)
}

fn InvalidateLocalBuffer(buffer: Buffer, check_unreferenced: bool) -> PgResult<()> {
    with(|lb| {
        let id = local_bufid(buffer);
        let desc = &lb.descs[id];
        let tag = desc.tag();
        if check_unreferenced && lb.ref_counts[id].get() != 0 {
            return Err(Box::new(
                types_error::PgError::new(
                    ERROR,
                    format!(
                        "block {} of relation base/{}/t{}_{} is still referenced (local {})",
                        tag.blockNum,
                        tag.dbOid,
                        init_small::globals::MyProcNumber(),
                        tag.relNumber,
                        lb.ref_counts[id].get()
                    ),
                )
                .with_error_location(ErrorLocation::new(file!(), line!() as i32, "InvalidateLocalBuffer")),
            ));
        }
        let removed = lb.hash.remove(&tag);
        assert!(removed.is_some(), "local buffer hash table corrupted");
        // SAFETY: single-threaded local descriptor.
        unsafe { desc.set_tag(cleared_buftag()) };
        let mut state = state_of(desc);
        state &= !(BUF_FLAG_MASK | BUF_USAGECOUNT_MASK);
        set_state(desc, state);
        Ok(())
    })
}

pub(crate) fn MarkLocalBufferDirty(buffer: Buffer) {
    debug_assert!(local_ref_count(buffer) > 0);
    with(|lb| {
        let desc = &lb.descs[local_bufid(buffer)];
        let state = state_of(desc);
        if state & BM_DIRTY == 0 {
            counters::local_dirtied();
        }
        set_state(desc, state | BM_DIRTY);
    })
}

/// StartLocalBufferIO with the AIO wait arm collapsed (io_method=sync).
pub(crate) fn StartLocalBufferIO(buffer: Buffer, for_input: bool) -> bool {
    let state = with(|lb| state_of(&lb.descs[local_bufid(buffer)]));
    !(if for_input {
        state & BM_VALID != 0
    } else {
        state & BM_DIRTY == 0
    })
}

pub(crate) fn TerminateLocalBufferIO(buffer: Buffer, clear_dirty: bool, set_flag_bits: u32) {
    with(|lb| {
        let desc = &lb.descs[local_bufid(buffer)];
        let mut state = state_of(desc);
        state &= !types_storage::buf::BM_IO_ERROR;
        if clear_dirty {
            state &= !BM_DIRTY;
        }
        state |= set_flag_bits;
        set_state(desc, state);
    })
}

pub fn DropRelationLocalBuffers(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
    first_del_block: BlockNumber,
) -> PgResult<()> {
    let n = n_loc_buffer();
    for i in 0..n {
        let buffer = -i - 1;
        let (state, tag) = with(|lb| {
            let desc = &lb.descs[i as usize];
            (state_of(desc), desc.tag())
        });
        if state & BM_TAG_VALID != 0
            && tag.spcOid == rlocator.spcOid
            && tag.dbOid == rlocator.dbOid
            && tag.relNumber == rlocator.relNumber
            && tag.forkNum == forknum
            && tag.blockNum >= first_del_block
        {
            InvalidateLocalBuffer(buffer, true)?;
        }
    }
    Ok(())
}

pub fn DropRelationAllLocalBuffers(rlocator: RelFileLocator) -> PgResult<()> {
    let n = n_loc_buffer();
    for i in 0..n {
        let buffer = -i - 1;
        let (state, tag) = with(|lb| {
            let desc = &lb.descs[i as usize];
            (state_of(desc), desc.tag())
        });
        if state & BM_TAG_VALID != 0
            && tag.spcOid == rlocator.spcOid
            && tag.dbOid == rlocator.dbOid
            && tag.relNumber == rlocator.relNumber
        {
            InvalidateLocalBuffer(buffer, true)?;
        }
    }
    Ok(())
}

/// FlushRelationBuffers' RelationUsesLocalBuffers arm.
pub(crate) fn FlushRelationLocalBuffers(rlocator: RelFileLocator) -> PgResult<()> {
    let n = n_loc_buffer();
    for i in 0..n {
        let buffer = -i - 1;
        let (state, tag) = with(|lb| {
            let desc = &lb.descs[i as usize];
            (state_of(desc), desc.tag())
        });
        if state & BM_TAG_VALID != 0
            && tag.spcOid == rlocator.spcOid
            && tag.dbOid == rlocator.dbOid
            && tag.relNumber == rlocator.relNumber
            && state & (BM_VALID | BM_DIRTY) == (BM_VALID | BM_DIRTY)
        {
            resowner::ResourceOwnerEnlarge(resowner::CurrentResourceOwner())?;
            PinLocalBuffer(buffer, false);
            FlushLocalBuffer(buffer)?;
            UnpinLocalBuffer(buffer);
        }
    }
    Ok(())
}

pub(crate) fn LimitAdditionalLocalPins(additional_pins: &mut u32) {
    if *additional_pins <= 1 {
        return;
    }
    let max_pins = (num_temp_buffers() - with_inited(0, |lb| lb.pinned)).max(0) as u32;
    if *additional_pins >= max_pins {
        *additional_pins = max_pins;
    }
}

/// ExtendBufferedRelLocal: victim-per-page, zeroed images, one smgrzeroextend.
pub(crate) fn ExtendBufferedRelLocal(
    smgr: RelFileLocatorBackend,
    fork: ForkNumber,
    mut extend_by: u32,
    extend_upto: BlockNumber,
    buffers: &mut [Buffer],
) -> PgResult<(BlockNumber, u32)> {
    LimitAdditionalLocalPins(&mut extend_by);
    debug_assert!(extend_by > 0);

    for slot in buffers.iter_mut().take(extend_by as usize) {
        *slot = GetLocalVictimBuffer()?;
        // SAFETY: pinned, tag-invalid victim block owned by this thread.
        unsafe { core::ptr::write_bytes(local_block_ptr(*slot), 0, BLCKSZ) };
    }

    let first_block = smgr_seams::smgr_nblocks::call(smgr, fork)?;

    if extend_upto != types_core::InvalidBlockNumber {
        debug_assert!(first_block <= extend_upto);
        debug_assert!(first_block as u64 + extend_by as u64 <= extend_upto as u64);
    }

    if first_block as u64 + extend_by as u64 >= MaxBlockNumber as u64 {
        ereport(ERROR)
            .errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED)
            .errmsg(format!(
                "cannot extend relation base/{}/t{}_{} beyond {} blocks",
                smgr.locator.dbOid,
                smgr.backend,
                smgr.locator.relNumber,
                MaxBlockNumber
            ))
            .finish(ErrorLocation::new(file!(), line!() as i32, "ExtendBufferedRelLocal"))?;
    }

    for i in 0..extend_by as usize {
        let victim = buffers[i];
        let tag = init_buffer_tag(smgr.locator, fork, first_block + i as u32);
        resowner::ResourceOwnerEnlarge(resowner::CurrentResourceOwner())?;
        let existing = with(|lb| lb.hash.get(&tag).copied());
        if let Some(existing_id) = existing {
            // Leftover from a failed extension: reuse it, clearing BM_VALID.
            UnpinLocalBuffer(victim);
            let existing_buf = with(|lb| {
                let desc = &lb.descs[existing_id as usize];
                let state = state_of(desc);
                debug_assert!(state & BM_TAG_VALID != 0);
                debug_assert!(state & BM_DIRTY == 0);
                set_state(desc, state & !BM_VALID);
                BufferDescriptorGetBuffer(desc)
            });
            PinLocalBuffer(existing_buf, false);
            buffers[i] = existing_buf;
        } else {
            with(|lb| {
                let id = local_bufid(victim);
                let desc = &lb.descs[id];
                let state = state_of(desc);
                debug_assert!(
                    state & (BM_VALID | BM_TAG_VALID | BM_DIRTY | BM_JUST_DIRTIED) == 0
                );
                // SAFETY: single-threaded local descriptor; only our pin.
                unsafe { desc.set_tag(tag) };
                set_state(desc, state | BM_TAG_VALID | BUF_USAGECOUNT_ONE);
                lb.hash.insert(tag, id as i32);
            });
        }
    }

    let io_start = pgstat_prepare_io_time(crate::gucs::track_io_timing());

    smgr_seams::smgr_zeroextend::call(smgr, fork, first_block, extend_by as i32, false)?;

    pgstat_count_io_op_time(
        IOObject::TempRelation,
        IOContext::IOCONTEXT_NORMAL,
        IOOp::Extend,
        io_start,
        1,
        extend_by as u64 * BLCKSZ as u64,
    );

    for buf in buffers.iter().take(extend_by as usize) {
        with(|lb| {
            let desc = &lb.descs[local_bufid(*buf)];
            set_state(desc, state_of(desc) | BM_VALID);
        });
        counters::local_written();
    }

    Ok((first_block, extend_by))
}

fn get_local_buffer_storage(lb: &mut LocalBufs) -> *mut u8 {
    debug_assert!(lb.total_bufs_allocated < lb.descs.len());
    if lb.next_buf_in_block >= lb.num_bufs_in_block {
        let mut num_bufs = (lb.num_bufs_in_block * 2).max(16);
        num_bufs = num_bufs.min(lb.descs.len() - lb.total_bufs_allocated);
        num_bufs = num_bufs.min(MAX_ALLOC_SIZE / BLCKSZ);
        let layout =
            core::alloc::Layout::from_size_align(num_bufs * BLCKSZ, PG_IO_ALIGN_SIZE)
                .expect("local buffer chunk layout");
        // SAFETY: non-zero layout; chunk is session-lifetime (C never frees it).
        let chunk = unsafe { std::alloc::alloc(layout) };
        assert!(!chunk.is_null(), "out of memory");
        lb.cur_block = chunk;
        lb.next_buf_in_block = 0;
        lb.num_bufs_in_block = num_bufs;
    }
    // SAFETY: in-bounds offset within the chunk sized above.
    let this_buf = unsafe { lb.cur_block.add(lb.next_buf_in_block * BLCKSZ) };
    lb.next_buf_in_block += 1;
    lb.total_bufs_allocated += 1;
    this_buf
}

fn CheckForLocalBufferLeaks() {
    if cfg!(debug_assertions) {
        let leaks = with_inited(0, |lb| {
            let mut errors = 0;
            for (i, rc) in lb.ref_counts.iter().enumerate() {
                if rc.get() != 0 {
                    let _ = elog::elog(
                        types_error::WARNING,
                        format!("local buffer refcount leak: [{}] (refcount={})", -(i as i32) - 1, rc.get()),
                    );
                    errors += 1;
                }
            }
            errors
        });
        debug_assert!(leaks == 0, "local buffer refcount leaks detected");
    }
}

pub fn AtEOXact_LocalBuffers(_is_commit: bool) {
    CheckForLocalBufferLeaks();
}

pub fn AtProcExit_LocalBuffers() {
    CheckForLocalBufferLeaks();
}

pub(crate) fn install_check_temp_buffers_hook() {
    fn hook(
        newval: &mut i32,
        _extra: &mut Option<guc_tables::GucHookExtra>,
        source: types_guc::GucSource,
    ) -> PgResult<bool> {
        let n = n_loc_buffer();
        if source != types_guc::GucSource::PGC_S_TEST && n != 0 && n != *newval {
            guc_seams::guc_check_errdetail::call(
                "\"temp_buffers\" cannot be changed after any temporary tables have been accessed in the session.".into(),
            );
            return Ok(false);
        }
        Ok(true)
    }
    guc_tables::hooks::check_temp_buffers.install(hook);
}
