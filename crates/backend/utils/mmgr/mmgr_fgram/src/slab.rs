#[cfg(target_family = "wasm")]
#[allow(unused_imports)]
use wasm_libc_shim as libc;
use std::cell::RefCell;
use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr::{self, NonNull};

use ::error_fgram::{PgError, PgResult};
use ::pg_ffi_fgram::{
    MemoryChunk, MemoryContext, MemoryContextCounters, MemoryContextData, MemoryContextMethods,
    MemoryStatsPrintFunc, Size, MCTX_SLAB_ID, MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO,
    MEMORYCHUNK_MAX_BLOCKOFFSET, MEMORYCHUNK_MAX_VALUE, T_SLAB_CONTEXT,
};

const SLAB_BLOCKLIST_COUNT: usize = 3;
const SLAB_MAXIMUM_EMPTY_BLOCKS: u32 = 10;

type SlabSet = *mut SlabContext;
type SlabBlock = *mut SlabBlockData;

#[repr(C)]
struct SlabContext {
    header: MemoryContextData,
    chunkSize: u32,
    fullChunkSize: u32,
    blockSize: u32,
    chunksPerBlock: i32,
    curBlocklistIndex: i32,
    blocklist_shift: i32,
    emptyblocks: DCListHead,
    blocklist: [DListHead; SLAB_BLOCKLIST_COUNT],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DListNode {
    prev: *mut DListNode,
    next: *mut DListNode,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct DListHead {
    head: DListNode,
}

#[repr(C)]
struct DCListHead {
    dlist: DListHead,
    count: u32,
}

#[repr(C)]
struct SlabBlockData {
    slab: SlabSet,
    nfree: i32,
    nunused: i32,
    freehead: *mut MemoryChunk,
    unused: *mut MemoryChunk,
    node: DListNode,
}

struct SlabMetadata {
    context: MemoryContext,
    owned_name: *mut c_char,
    owned_ident: *mut c_char,
}

thread_local! {
    static OWNED_METADATA: RefCell<Vec<SlabMetadata>> = const {
        RefCell::new(Vec::new())
    };
}

pub fn methods() -> *const MemoryContextMethods {
    &SLAB_METHODS
}

pub fn create_context(
    parent: Option<MemoryContext>,
    name: String,
    block_size: Size,
    chunk_size: Size,
) -> PgResult<MemoryContext> {
    let (block_size, chunk_size, full_chunk_size, chunks_per_block) =
        validate_create_params(block_size, chunk_size)?;
    let name = CString::new(name).map_err(|error| PgError::error(error.to_string()))?;
    let name = name.into_raw();

    let slab = unsafe { libc::malloc(std::mem::size_of::<SlabContext>()).cast::<SlabContext>() };
    if slab.is_null() {
        let context_name = unsafe { CStr::from_ptr(name) }
            .to_string_lossy()
            .into_owned();
        unsafe {
            drop(CString::from_raw(name));
        }
        return Err(out_of_memory().with_detail(format!(
            "Failed while creating memory context \"{context_name}\"."
        )));
    }

    unsafe {
        (*slab).chunkSize = chunk_size as u32;
        (*slab).fullChunkSize = full_chunk_size as u32;
        (*slab).blockSize = block_size as u32;
        (*slab).chunksPerBlock = chunks_per_block;
        (*slab).curBlocklistIndex = 0;
        (*slab).blocklist_shift = blocklist_shift(chunks_per_block);
        dclist_init(&mut (*slab).emptyblocks);
        for blocklist in &mut (*slab).blocklist {
            dlist_init(blocklist);
        }
        crate::raw::memory_context_create_common(
            slab.cast(),
            T_SLAB_CONTEXT,
            MCTX_SLAB_ID,
            parent,
            name,
        )?;
    }

    if let Err(error) = register_owned_metadata(slab.cast(), name, ptr::null_mut()) {
        unsafe {
            libc::free(slab.cast());
            drop(CString::from_raw(name));
        }
        return Err(error);
    }

    Ok(slab.cast())
}

pub fn set_identifier(context: MemoryContext, ident: CString) -> PgResult<()> {
    let ident = ident.into_raw();
    if let Err(error) = replace_owned_identifier(context, ident) {
        unsafe {
            drop(CString::from_raw(ident));
        }
        return Err(error);
    }
    unsafe {
        (*context).ident = ident;
    }
    Ok(())
}

pub unsafe fn alloc(
    context: MemoryContext,
    size: Size,
    flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    let slab = context.cast::<SlabContext>();
    if unsafe { size != (*slab).chunkSize as Size } {
        return Err(invalid_alloc_size(context, size));
    }
    let pointer = unsafe { slab_alloc_internal(context, size, flags) };
    NonNull::new(pointer).map_or_else(
        || allocation_failure(context, size, flags),
        |ptr| {
            if flags & MCXT_ALLOC_ZERO != 0 {
                unsafe { ptr::write_bytes(ptr.as_ptr(), 0, size) };
            }
            Ok(Some((ptr, size)))
        },
    )
}

pub unsafe fn free(pointer: *mut c_void) -> PgResult<()> {
    unsafe { slab_free_internal(pointer) }
}

pub unsafe fn realloc(
    pointer: *mut c_void,
    size: Size,
    _flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    let context = unsafe { slab_get_chunk_context_internal(pointer) };
    let slab = context.cast::<SlabContext>();
    unsafe {
        if size == (*slab).chunkSize as Size {
            return Ok(NonNull::new(pointer).map(|ptr| (ptr, size)));
        }
    }
    Err(PgError::error("slab allocator does not support realloc()"))
}

pub unsafe fn reset(context: MemoryContext) -> PgResult<()> {
    unsafe { slab_reset_internal(context) };
    Ok(())
}

pub unsafe fn delete(context: MemoryContext) -> PgResult<()> {
    unsafe { slab_delete_internal(context) };
    Ok(())
}

pub unsafe fn get_chunk_context(pointer: *mut c_void) -> PgResult<MemoryContext> {
    Ok(unsafe { slab_get_chunk_context_internal(pointer) })
}

pub unsafe fn get_chunk_space(pointer: *mut c_void) -> PgResult<Size> {
    Ok(unsafe { slab_get_chunk_space_internal(pointer) })
}

pub unsafe fn is_empty(context: MemoryContext) -> PgResult<bool> {
    Ok(unsafe { slab_is_empty_internal(context) })
}

pub unsafe fn stats(context: MemoryContext) -> PgResult<MemoryContextCounters> {
    let mut totals = MemoryContextCounters::default();
    unsafe { slab_stats_internal(context, None, ptr::null_mut(), &mut totals, false) };
    Ok(totals)
}

pub unsafe fn check(context: MemoryContext) -> PgResult<()> {
    unsafe { slab_check_internal(context) }
}

unsafe extern "C" fn method_alloc(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    unsafe { slab_alloc_internal(context, size, flags) }
}

unsafe extern "C" fn method_free(pointer: *mut c_void) {
    let _ = unsafe { slab_free_internal(pointer) };
}

unsafe extern "C" fn method_realloc(pointer: *mut c_void, size: Size, _flags: i32) -> *mut c_void {
    let context = unsafe { slab_get_chunk_context_internal(pointer) };
    if context.is_null() {
        return ptr::null_mut();
    }
    let slab = context.cast::<SlabContext>();
    unsafe {
        if size == (*slab).chunkSize as Size {
            pointer
        } else {
            ptr::null_mut()
        }
    }
}

unsafe extern "C" fn method_reset(context: MemoryContext) {
    unsafe { slab_reset_internal(context) };
}

unsafe extern "C" fn method_delete(context: MemoryContext) {
    unsafe { slab_delete_internal(context) };
}

unsafe extern "C" fn method_get_chunk_context(pointer: *mut c_void) -> MemoryContext {
    unsafe { slab_get_chunk_context_internal(pointer) }
}

unsafe extern "C" fn method_get_chunk_space(pointer: *mut c_void) -> Size {
    unsafe { slab_get_chunk_space_internal(pointer) }
}

unsafe extern "C" fn method_is_empty(context: MemoryContext) -> bool {
    unsafe { slab_is_empty_internal(context) }
}

unsafe extern "C" fn method_stats(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    unsafe { slab_stats_internal(context, printfunc, passthru, totals, print_to_stderr) };
}

const SLAB_METHODS: MemoryContextMethods = MemoryContextMethods {
    alloc: Some(method_alloc),
    free_p: Some(method_free),
    realloc: Some(method_realloc),
    reset: Some(method_reset),
    delete_context: Some(method_delete),
    get_chunk_context: Some(method_get_chunk_context),
    get_chunk_space: Some(method_get_chunk_space),
    is_empty: Some(method_is_empty),
    stats: Some(method_stats),
};

unsafe fn slab_reset_internal(context: MemoryContext) {
    let slab = context.cast::<SlabContext>();
    unsafe {
        while !dclist_is_empty(&(*slab).emptyblocks) {
            let node = dclist_pop_head_node(&mut (*slab).emptyblocks);
            let block = slab_block_from_node(node);
            libc::free(block.cast());
            (*context).mem_allocated = (*context)
                .mem_allocated
                .saturating_sub((*slab).blockSize as Size);
        }
        for blocklist in &mut (*slab).blocklist {
            while !dlist_is_empty(blocklist) {
                let node = dlist_pop_head_node(blocklist);
                let block = slab_block_from_node(node);
                libc::free(block.cast());
                (*context).mem_allocated = (*context)
                    .mem_allocated
                    .saturating_sub((*slab).blockSize as Size);
            }
        }
        (*slab).curBlocklistIndex = 0;
    }
}

unsafe fn slab_delete_internal(context: MemoryContext) {
    unsafe {
        slab_reset_internal(context);
        release_owned_metadata(context);
        libc::free(context.cast());
    }
}

unsafe fn slab_alloc_internal(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    let slab = context.cast::<SlabContext>();
    unsafe {
        if size != (*slab).chunkSize as Size {
            return ptr::null_mut();
        }
        if (*slab).curBlocklistIndex == 0 {
            return slab_alloc_from_new_block(context, size, flags);
        }

        let blocklist = &mut (*slab).blocklist[(*slab).curBlocklistIndex as usize];
        let block = slab_block_from_node((*blocklist).head.next);
        let chunk = slab_get_next_free_chunk(slab, block);
        let new_blocklist_idx = slab_blocklist_index(slab, (*block).nfree);
        if (*slab).curBlocklistIndex != new_blocklist_idx {
            dlist_delete_from(blocklist, &mut (*block).node);
            dlist_push_head(
                &mut (*slab).blocklist[new_blocklist_idx as usize],
                &mut (*block).node,
            );
            if dlist_is_empty(blocklist) {
                (*slab).curBlocklistIndex = slab_find_next_blocklist_index(slab);
            }
        }
        slab_alloc_setup_new_chunk(context, block, chunk)
    }
}

unsafe fn slab_alloc_from_new_block(
    context: MemoryContext,
    _size: Size,
    _flags: i32,
) -> *mut c_void {
    let slab = context.cast::<SlabContext>();
    unsafe {
        let (block, chunk) = if !dclist_is_empty(&(*slab).emptyblocks) {
            let node = dclist_pop_head_node(&mut (*slab).emptyblocks);
            let block = slab_block_from_node(node);
            let chunk = slab_get_next_free_chunk(slab, block);
            (block, chunk)
        } else {
            let block = libc::malloc((*slab).blockSize as Size).cast::<SlabBlockData>();
            if block.is_null() {
                return ptr::null_mut();
            }
            (*block).slab = slab;
            (*context).mem_allocated = (*context)
                .mem_allocated
                .saturating_add((*slab).blockSize as Size);
            let chunk = slab_block_get_chunk(slab, block, 0);
            (*block).nfree = (*slab).chunksPerBlock - 1;
            (*block).unused = slab_block_get_chunk(slab, block, 1);
            (*block).freehead = ptr::null_mut();
            (*block).nunused = (*slab).chunksPerBlock - 1;
            (block, chunk)
        };

        let blocklist_idx = slab_blocklist_index(slab, (*block).nfree);
        dlist_push_head(
            &mut (*slab).blocklist[blocklist_idx as usize],
            &mut (*block).node,
        );
        (*slab).curBlocklistIndex = blocklist_idx;
        slab_alloc_setup_new_chunk(context, block, chunk)
    }
}

unsafe fn slab_alloc_setup_new_chunk(
    context: MemoryContext,
    block: SlabBlock,
    chunk: *mut MemoryChunk,
) -> *mut c_void {
    unsafe {
        let slab = context.cast::<SlabContext>();
        let block_offset = chunk.cast::<c_char>().offset_from(block.cast::<c_char>()) as Size;
        (*chunk).set_hdrmask(
            block_offset,
            maxalign((*slab).chunkSize as Size),
            MCTX_SLAB_ID,
        );
        memory_chunk_get_pointer(chunk)
    }
}

unsafe fn slab_get_next_free_chunk(slab: SlabSet, block: SlabBlock) -> *mut MemoryChunk {
    unsafe {
        let chunk = if !(*block).freehead.is_null() {
            let chunk = (*block).freehead;
            (*block).freehead = *(memory_chunk_get_pointer(chunk).cast::<*mut MemoryChunk>());
            chunk
        } else {
            let chunk = (*block).unused;
            (*block).unused = (*block)
                .unused
                .cast::<c_char>()
                .add((*slab).fullChunkSize as Size)
                .cast();
            (*block).nunused -= 1;
            chunk
        };
        (*block).nfree -= 1;
        chunk
    }
}

unsafe fn slab_free_internal(pointer: *mut c_void) -> PgResult<()> {
    if pointer.is_null() {
        return Err(PgError::error("memory pointer is null"));
    }
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        let block = memory_chunk_get_block(chunk).cast::<SlabBlockData>();
        validate_block(block)?;
        let slab = (*block).slab;

        *(pointer.cast::<*mut MemoryChunk>()) = (*block).freehead;
        (*block).freehead = chunk;
        (*block).nfree += 1;

        let cur_blocklist_idx = slab_blocklist_index(slab, (*block).nfree - 1);
        let new_blocklist_idx = slab_blocklist_index(slab, (*block).nfree);
        if cur_blocklist_idx != new_blocklist_idx {
            dlist_delete_from(
                &mut (*slab).blocklist[cur_blocklist_idx as usize],
                &mut (*block).node,
            );
            dlist_push_head(
                &mut (*slab).blocklist[new_blocklist_idx as usize],
                &mut (*block).node,
            );
            if (*slab).curBlocklistIndex >= cur_blocklist_idx {
                (*slab).curBlocklistIndex = slab_find_next_blocklist_index(slab);
            }
        }

        if (*block).nfree == (*slab).chunksPerBlock {
            dlist_delete_from(
                &mut (*slab).blocklist[new_blocklist_idx as usize],
                &mut (*block).node,
            );
            if (*slab).emptyblocks.count < SLAB_MAXIMUM_EMPTY_BLOCKS {
                dclist_push_head(&mut (*slab).emptyblocks, &mut (*block).node);
            } else {
                libc::free(block.cast());
                (*slab).header.mem_allocated = (*slab)
                    .header
                    .mem_allocated
                    .saturating_sub((*slab).blockSize as Size);
            }
            if (*slab).curBlocklistIndex == new_blocklist_idx
                && dlist_is_empty(&(*slab).blocklist[new_blocklist_idx as usize])
            {
                (*slab).curBlocklistIndex = slab_find_next_blocklist_index(slab);
            }
        }
    }
    Ok(())
}

unsafe fn slab_get_chunk_context_internal(pointer: *mut c_void) -> MemoryContext {
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        let block = memory_chunk_get_block(chunk).cast::<SlabBlockData>();
        (*block).slab.header_ptr()
    }
}

unsafe fn slab_get_chunk_space_internal(pointer: *mut c_void) -> Size {
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        let block = memory_chunk_get_block(chunk).cast::<SlabBlockData>();
        (*(*block).slab).fullChunkSize as Size
    }
}

unsafe fn slab_is_empty_internal(context: MemoryContext) -> bool {
    unsafe { (*context).mem_allocated == 0 }
}

unsafe fn slab_stats_internal(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    let slab = context.cast::<SlabContext>();
    unsafe {
        let mut nblocks = 0;
        let mut freechunks = 0;
        let mut totalspace = std::mem::size_of::<SlabContext>()
            + (*slab).emptyblocks.count as Size * (*slab).blockSize as Size;
        let mut freespace = 0;
        for blocklist in &(*slab).blocklist {
            let mut node = blocklist.head.next;
            while node != &blocklist.head as *const DListNode as *mut DListNode {
                let block = slab_block_from_node(node);
                nblocks += 1;
                totalspace += (*slab).blockSize as Size;
                freespace += (*slab).fullChunkSize as Size * (*block).nfree as Size;
                freechunks += (*block).nfree as Size;
                node = (*node).next;
            }
        }
        if let Some(printfunc) = printfunc {
            let stats = CString::new(format!(
                "{totalspace} total in {nblocks} blocks; {} empty blocks; {freespace} free ({freechunks} chunks); {} used",
                (*slab).emptyblocks.count,
                totalspace - freespace
            ))
            .expect("stats string contains no nul");
            printfunc(context, passthru, stats.as_ptr(), print_to_stderr);
        }
        if !totals.is_null() {
            (*totals).nblocks = (*totals).nblocks.saturating_add(nblocks);
            (*totals).freechunks = (*totals).freechunks.saturating_add(freechunks);
            (*totals).totalspace = (*totals).totalspace.saturating_add(totalspace);
            (*totals).freespace = (*totals).freespace.saturating_add(freespace);
        }
    }
}

unsafe fn slab_check_internal(context: MemoryContext) -> PgResult<()> {
    if context.is_null() {
        return Err(PgError::error("memory context is null"));
    }
    let slab = context.cast::<SlabContext>();
    unsafe {
        if (*context).type_ != T_SLAB_CONTEXT {
            return Err(PgError::error("memory context is not a Slab context"));
        }
        let mut nblocks: Size = (*slab).emptyblocks.count as Size;
        let mut node = (*slab).emptyblocks.dlist.head.next;
        while node != &mut (*slab).emptyblocks.dlist.head {
            let block = slab_block_from_node(node);
            validate_block(block)?;
            if (*block).nfree != (*slab).chunksPerBlock {
                return Err(PgError::error("Slab empty block is not fully free"));
            }
            node = (*node).next;
        }

        for index in 0..SLAB_BLOCKLIST_COUNT {
            let blocklist = &(*slab).blocklist[index];
            let mut node = blocklist.head.next;
            while node != &blocklist.head as *const DListNode as *mut DListNode {
                let block = slab_block_from_node(node);
                validate_block(block)?;
                if slab_blocklist_index(slab, (*block).nfree) != index as i32 {
                    return Err(PgError::error("Slab block is on the wrong blocklist"));
                }
                if (*block).nfree >= (*slab).chunksPerBlock {
                    return Err(PgError::error("Slab empty block is on a blocklist"));
                }
                if (*block).nfree < 0 || (*block).nunused < 0 || (*block).nunused > (*block).nfree {
                    return Err(PgError::error("Slab block counters are invalid"));
                }
                nblocks += 1;
                node = (*node).next;
            }
        }

        if nblocks * (*slab).blockSize as Size != (*context).mem_allocated {
            return Err(PgError::error(
                "Slab context mem_allocated does not match block total",
            ));
        }
    }
    Ok(())
}

trait HeaderPtr {
    unsafe fn header_ptr(self) -> MemoryContext;
}

impl HeaderPtr for SlabSet {
    unsafe fn header_ptr(self) -> MemoryContext {
        unsafe { &mut (*self).header }
    }
}

fn register_owned_metadata(
    context: MemoryContext,
    owned_name: *mut c_char,
    owned_ident: *mut c_char,
) -> PgResult<()> {
    OWNED_METADATA.with(|metadata| {
        let mut metadata = metadata.borrow_mut();
        metadata
            .try_reserve(1)
            .map_err(|_| out_of_memory().with_detail("Failed while tracking Slab metadata."))?;
        metadata.push(SlabMetadata {
            context,
            owned_name,
            owned_ident,
        });
        Ok(())
    })
}

fn replace_owned_identifier(context: MemoryContext, owned_ident: *mut c_char) -> PgResult<()> {
    OWNED_METADATA.with(|metadata| {
        let mut metadata = metadata.borrow_mut();
        let Some(entry) = metadata.iter_mut().find(|entry| entry.context == context) else {
            return Err(PgError::error("Slab metadata is missing"));
        };
        free_cstring(entry.owned_ident);
        entry.owned_ident = owned_ident;
        Ok(())
    })
}

fn release_owned_metadata(context: MemoryContext) {
    OWNED_METADATA.with(|metadata| {
        let mut metadata = metadata.borrow_mut();
        if let Some(index) = metadata.iter().position(|entry| entry.context == context) {
            let entry = metadata.swap_remove(index);
            free_cstring(entry.owned_name);
            free_cstring(entry.owned_ident);
        }
    });
}

fn free_cstring(pointer: *mut c_char) {
    if !pointer.is_null() {
        unsafe {
            drop(CString::from_raw(pointer));
        }
    }
}

fn validate_create_params(
    block_size: Size,
    mut chunk_size: Size,
) -> PgResult<(Size, Size, Size, i32)> {
    if block_size > MEMORYCHUNK_MAX_BLOCKOFFSET as Size {
        return Err(PgError::error("invalid Slab block size"));
    }
    chunk_size = chunk_size.max(std::mem::size_of::<*mut MemoryChunk>());
    let full_chunk_size = slab_chunk_header_size()
        .checked_add(maxalign(chunk_size))
        .ok_or_else(out_of_memory)?;
    if full_chunk_size > MEMORYCHUNK_MAX_VALUE as Size {
        return Err(PgError::error("invalid Slab chunk size"));
    }
    let chunks_per_block = (block_size.saturating_sub(slab_block_header_size())) / full_chunk_size;
    if chunks_per_block == 0 {
        return Err(PgError::error(format!(
            "block size {block_size} for slab is too small for {chunk_size}-byte chunks"
        )));
    }
    Ok((
        block_size,
        chunk_size,
        full_chunk_size,
        chunks_per_block as i32,
    ))
}

fn blocklist_shift(chunks_per_block: i32) -> i32 {
    let mut shift = 0;
    while (chunks_per_block >> shift) >= (SLAB_BLOCKLIST_COUNT as i32 - 1) {
        shift += 1;
    }
    shift
}

unsafe fn slab_blocklist_index(slab: SlabSet, nfree: i32) -> i32 {
    unsafe { -((-nfree) >> (*slab).blocklist_shift) }
}

unsafe fn slab_find_next_blocklist_index(slab: SlabSet) -> i32 {
    unsafe {
        for index in 1..SLAB_BLOCKLIST_COUNT {
            if !dlist_is_empty(&(*slab).blocklist[index]) {
                return index as i32;
            }
        }
    }
    0
}

fn allocation_failure(
    context: MemoryContext,
    size: Size,
    flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    if flags & MCXT_ALLOC_NO_OOM != 0 {
        return Ok(None);
    }
    Err(out_of_memory().with_detail(format!(
        "Failed on request of size {size} in memory context \"{}\".",
        context_name(context)
    )))
}

fn invalid_alloc_size(context: MemoryContext, size: Size) -> PgError {
    let expected = unsafe { (*context.cast::<SlabContext>()).chunkSize };
    PgError::error(format!(
        "unexpected alloc chunk size {size} (expected {expected})"
    ))
}

fn context_name(context: MemoryContext) -> String {
    if context.is_null() {
        return "<null>".to_owned();
    }
    unsafe {
        if (*context).name.is_null() {
            "<unnamed>".to_owned()
        } else {
            CStr::from_ptr((*context).name)
                .to_string_lossy()
                .into_owned()
        }
    }
}

fn out_of_memory() -> PgError {
    PgError::error("out of memory").with_sqlstate(::pg_ffi_fgram::ERRCODE_OUT_OF_MEMORY)
}

fn maxalign(size: Size) -> Size {
    let align = std::mem::align_of::<usize>();
    (size + align - 1) & !(align - 1)
}

fn slab_block_header_size() -> Size {
    maxalign(std::mem::size_of::<SlabBlockData>())
}

fn slab_chunk_header_size() -> Size {
    std::mem::size_of::<MemoryChunk>()
}

unsafe fn slab_block_get_chunk(slab: SlabSet, block: SlabBlock, n: i32) -> *mut MemoryChunk {
    unsafe {
        block
            .cast::<c_char>()
            .add(slab_block_header_size() + n as Size * (*slab).fullChunkSize as Size)
            .cast()
    }
}

unsafe fn slab_block_from_node(node: *mut DListNode) -> SlabBlock {
    unsafe {
        node.cast::<c_char>()
            .sub(std::mem::offset_of!(SlabBlockData, node))
            .cast()
    }
}

fn pointer_get_memory_chunk(pointer: *mut c_void) -> *mut MemoryChunk {
    unsafe {
        pointer
            .cast::<u8>()
            .sub(std::mem::size_of::<MemoryChunk>())
            .cast()
    }
}

unsafe fn memory_chunk_get_pointer(chunk: *mut MemoryChunk) -> *mut c_void {
    unsafe {
        chunk
            .cast::<u8>()
            .add(std::mem::size_of::<MemoryChunk>())
            .cast()
    }
}

unsafe fn memory_chunk_get_block(chunk: *mut MemoryChunk) -> *mut c_void {
    unsafe { chunk.cast::<u8>().sub((*chunk).block_offset()).cast() }
}

unsafe fn validate_block(block: SlabBlock) -> PgResult<()> {
    if block.is_null() {
        return Err(PgError::error("memory block is null"));
    }
    let slab = unsafe { (*block).slab };
    if slab.is_null() || unsafe { (*slab).header.type_ != T_SLAB_CONTEXT } {
        return Err(PgError::error(
            "memory block does not belong to a Slab context",
        ));
    }
    Ok(())
}

unsafe fn dlist_init(head: *mut DListHead) {
    unsafe {
        (*head).head.next = &mut (*head).head;
        (*head).head.prev = &mut (*head).head;
    }
}

unsafe fn dlist_is_empty(head: *const DListHead) -> bool {
    unsafe { std::ptr::addr_of!((*head).head) == (*head).head.next }
}

unsafe fn dlist_push_head(head: *mut DListHead, node: *mut DListNode) {
    unsafe {
        (*node).next = (*head).head.next;
        (*node).prev = &mut (*head).head;
        (*(*head).head.next).prev = node;
        (*head).head.next = node;
    }
}

unsafe fn dlist_delete_from(_head: *mut DListHead, node: *mut DListNode) {
    unsafe { dlist_delete(node) };
}

unsafe fn dlist_delete(node: *mut DListNode) {
    unsafe {
        (*(*node).prev).next = (*node).next;
        (*(*node).next).prev = (*node).prev;
    }
}

unsafe fn dlist_pop_head_node(head: *mut DListHead) -> *mut DListNode {
    unsafe {
        let node = (*head).head.next;
        dlist_delete(node);
        node
    }
}

unsafe fn dclist_init(head: *mut DCListHead) {
    unsafe {
        dlist_init(&mut (*head).dlist);
        (*head).count = 0;
    }
}

unsafe fn dclist_is_empty(head: *const DCListHead) -> bool {
    unsafe { (*head).count == 0 }
}

unsafe fn dclist_push_head(head: *mut DCListHead, node: *mut DListNode) {
    unsafe {
        dlist_push_head(&mut (*head).dlist, node);
        (*head).count += 1;
    }
}

unsafe fn dclist_pop_head_node(head: *mut DCListHead) -> *mut DListNode {
    unsafe {
        (*head).count -= 1;
        dlist_pop_head_node(&mut (*head).dlist)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slab_layout_starts_with_memory_context_header() {
        assert_eq!(std::mem::offset_of!(SlabContext, header), 0);
        assert_eq!(slab_chunk_header_size(), std::mem::size_of::<MemoryChunk>());
        assert_eq!(slab_chunk_header_size(), maxalign(slab_chunk_header_size()));
    }

    #[test]
    fn slab_context_rejects_too_small_blocks() {
        assert!(validate_create_params(64, 1024).is_err());
        assert!(validate_create_params(8 * 1024, 32).is_ok());
    }
}