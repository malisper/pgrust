use std::cell::RefCell;
use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr::{self, NonNull};

use error_fgram::{PgError, PgResult};
use pg_ffi_fgram::{
    MemoryChunk, MemoryContext, MemoryContextCounters, MemoryContextData, MemoryContextMethods,
    MemoryStatsPrintFunc, Size, MAX_ALLOC_HUGE_SIZE, MAX_ALLOC_SIZE, MCTX_GENERATION_ID,
    MCXT_ALLOC_HUGE, MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO, MEMORYCHUNK_MAX_BLOCKOFFSET,
    MEMORYCHUNK_MAX_VALUE, T_GENERATION_CONTEXT,
};

const GENERATION_CHUNK_FRACTION: Size = 8;

type GenerationSet = *mut GenerationContext;
type GenerationBlock = *mut GenerationBlockData;

#[repr(C)]
struct GenerationContext {
    header: MemoryContextData,
    initBlockSize: u32,
    maxBlockSize: u32,
    nextBlockSize: u32,
    allocChunkLimit: u32,
    block: GenerationBlock,
    freeblock: GenerationBlock,
    blocks: DListHead,
}

#[repr(C)]
struct DListNode {
    prev: *mut DListNode,
    next: *mut DListNode,
}

#[repr(C)]
struct DListHead {
    head: DListNode,
}

#[repr(C)]
struct GenerationBlockData {
    node: DListNode,
    context: GenerationSet,
    blksize: Size,
    nchunks: i32,
    nfree: i32,
    freeptr: *mut c_char,
    endptr: *mut c_char,
}

struct GenerationMetadata {
    context: MemoryContext,
    owned_name: *mut c_char,
    owned_ident: *mut c_char,
}

thread_local! {
    static OWNED_METADATA: RefCell<Vec<GenerationMetadata>> = const {
        RefCell::new(Vec::new())
    };
}

pub fn methods() -> *const MemoryContextMethods {
    &GENERATION_METHODS
}

pub fn create_context(
    parent: Option<MemoryContext>,
    name: String,
    min_context_size: Size,
    init_block_size: Size,
    max_block_size: Size,
) -> PgResult<MemoryContext> {
    validate_create_params(min_context_size, init_block_size, max_block_size)?;
    let name = CString::new(name).map_err(|error| PgError::error(error.to_string()))?;
    let name = name.into_raw();

    let alloc_size = initial_allocation_size(min_context_size, init_block_size)?;
    let set = unsafe { libc::malloc(alloc_size).cast::<GenerationContext>() };
    if set.is_null() {
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
        dlist_init(&mut (*set).blocks);
        let block = keeper_block(set);
        let first_block_size = alloc_size - maxalign(std::mem::size_of::<GenerationContext>());
        generation_block_init(set, block, first_block_size);
        dlist_push_head(&mut (*set).blocks, &mut (*block).node);

        (*set).block = block;
        (*set).freeblock = ptr::null_mut();
        (*set).initBlockSize = init_block_size as u32;
        (*set).maxBlockSize = max_block_size as u32;
        (*set).nextBlockSize = init_block_size as u32;
        (*set).allocChunkLimit = alloc_chunk_limit(max_block_size) as u32;

        crate::raw::memory_context_create_common(
            set.cast(),
            T_GENERATION_CONTEXT,
            MCTX_GENERATION_ID,
            parent,
            name,
        )?;
        (*set).header.mem_allocated = first_block_size;
    }

    if let Err(error) = register_owned_metadata(set.cast(), name, ptr::null_mut()) {
        unsafe {
            libc::free(set.cast());
            drop(CString::from_raw(name));
        }
        return Err(error);
    }

    Ok(set.cast())
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
    let pointer = unsafe { generation_alloc_internal(context, size, flags) };
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
    unsafe { generation_free_internal(pointer) }
}

pub unsafe fn realloc(
    pointer: *mut c_void,
    size: Size,
    flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    let context = unsafe { generation_get_chunk_context_internal(pointer) };
    let pointer = unsafe { generation_realloc_internal(pointer, size, flags) };
    NonNull::new(pointer).map_or_else(
        || allocation_failure(context, size, flags),
        |ptr| Ok(Some((ptr, size))),
    )
}

pub unsafe fn reset(context: MemoryContext) -> PgResult<()> {
    unsafe { generation_reset_internal(context) };
    Ok(())
}

pub unsafe fn delete(context: MemoryContext) -> PgResult<()> {
    unsafe { generation_delete_internal(context) };
    Ok(())
}

pub unsafe fn get_chunk_context(pointer: *mut c_void) -> PgResult<MemoryContext> {
    Ok(unsafe { generation_get_chunk_context_internal(pointer) })
}

pub unsafe fn get_chunk_space(pointer: *mut c_void) -> PgResult<Size> {
    Ok(unsafe { generation_get_chunk_space_internal(pointer) })
}

pub unsafe fn is_empty(context: MemoryContext) -> PgResult<bool> {
    Ok(unsafe { generation_is_empty_internal(context) })
}

pub unsafe fn stats(context: MemoryContext) -> PgResult<MemoryContextCounters> {
    let mut totals = MemoryContextCounters::default();
    unsafe { generation_stats_internal(context, None, ptr::null_mut(), &mut totals, false) };
    Ok(totals)
}

pub unsafe fn check(context: MemoryContext) -> PgResult<()> {
    unsafe { generation_check_internal(context) }
}

unsafe extern "C" fn method_alloc(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    unsafe { generation_alloc_internal(context, size, flags) }
}

unsafe extern "C" fn method_free(pointer: *mut c_void) {
    let _ = unsafe { generation_free_internal(pointer) };
}

unsafe extern "C" fn method_realloc(pointer: *mut c_void, size: Size, flags: i32) -> *mut c_void {
    unsafe { generation_realloc_internal(pointer, size, flags) }
}

unsafe extern "C" fn method_reset(context: MemoryContext) {
    unsafe { generation_reset_internal(context) };
}

unsafe extern "C" fn method_delete(context: MemoryContext) {
    unsafe { generation_delete_internal(context) };
}

unsafe extern "C" fn method_get_chunk_context(pointer: *mut c_void) -> MemoryContext {
    unsafe { generation_get_chunk_context_internal(pointer) }
}

unsafe extern "C" fn method_get_chunk_space(pointer: *mut c_void) -> Size {
    unsafe { generation_get_chunk_space_internal(pointer) }
}

unsafe extern "C" fn method_is_empty(context: MemoryContext) -> bool {
    unsafe { generation_is_empty_internal(context) }
}

unsafe extern "C" fn method_stats(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    unsafe { generation_stats_internal(context, printfunc, passthru, totals, print_to_stderr) };
}

const GENERATION_METHODS: MemoryContextMethods = MemoryContextMethods {
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

unsafe fn generation_reset_internal(context: MemoryContext) {
    let set = context.cast::<GenerationContext>();
    unsafe {
        (*set).freeblock = ptr::null_mut();
        let mut node = (*set).blocks.head.next;
        while node != &mut (*set).blocks.head {
            let next = (*node).next;
            let block = node.cast::<GenerationBlockData>();
            if block == keeper_block(set) {
                generation_block_mark_empty(block);
            } else {
                generation_block_free(set, block);
            }
            node = next;
        }
        (*set).block = keeper_block(set);
        (*set).nextBlockSize = (*set).initBlockSize;
    }
}

unsafe fn generation_delete_internal(context: MemoryContext) {
    unsafe {
        generation_reset_internal(context);
        release_owned_metadata(context);
        libc::free(context.cast());
    }
}

unsafe fn generation_alloc_internal(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    let set = context.cast::<GenerationContext>();
    let chunk_size = maxalign(size);
    unsafe {
        if chunk_size > (*set).allocChunkLimit as Size {
            return generation_alloc_large(context, size, flags);
        }
        let required_size = chunk_size + generation_chunk_header_size();
        let block = (*set).block;
        if generation_block_free_bytes(block) < required_size {
            let freeblock = (*set).freeblock;
            if !freeblock.is_null() && generation_block_free_bytes(freeblock) >= required_size {
                (*set).freeblock = ptr::null_mut();
                (*set).block = freeblock;
                return generation_alloc_chunk_from_block(context, freeblock, size, chunk_size);
            }
            return generation_alloc_from_new_block(context, size, flags, chunk_size);
        }
        generation_alloc_chunk_from_block(context, block, size, chunk_size)
    }
}

unsafe fn generation_alloc_large(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    if check_size(size, flags).is_err() {
        return ptr::null_mut();
    }
    let chunk_size = maxalign(size);
    let Some(blksize) = chunk_size
        .checked_add(generation_block_header_size())
        .and_then(|size| size.checked_add(generation_chunk_header_size()))
    else {
        return ptr::null_mut();
    };
    let block = unsafe { libc::malloc(blksize).cast::<GenerationBlockData>() };
    if block.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        let set = context.cast::<GenerationContext>();
        (*context).mem_allocated = (*context).mem_allocated.saturating_add(blksize);
        (*block).context = set;
        (*block).blksize = blksize;
        (*block).nchunks = 1;
        (*block).nfree = 0;
        (*block).freeptr = block.cast::<c_char>().add(blksize);
        (*block).endptr = (*block).freeptr;
        let chunk = block
            .cast::<c_char>()
            .add(generation_block_header_size())
            .cast::<MemoryChunk>();
        (*chunk).set_external(MCTX_GENERATION_ID);
        dlist_push_head(&mut (*set).blocks, &mut (*block).node);
        memory_chunk_get_pointer(chunk)
    }
}

unsafe fn generation_alloc_chunk_from_block(
    _context: MemoryContext,
    block: GenerationBlock,
    _size: Size,
    chunk_size: Size,
) -> *mut c_void {
    unsafe {
        let chunk = (*block).freeptr.cast::<MemoryChunk>();
        (*block).nchunks += 1;
        (*block).freeptr = (*block)
            .freeptr
            .add(generation_chunk_header_size() + chunk_size);
        let block_offset = chunk.cast::<c_char>().offset_from(block.cast::<c_char>()) as Size;
        (*chunk).set_hdrmask(block_offset, chunk_size, MCTX_GENERATION_ID);
        memory_chunk_get_pointer(chunk)
    }
}

unsafe fn generation_alloc_from_new_block(
    context: MemoryContext,
    size: Size,
    _flags: i32,
    chunk_size: Size,
) -> *mut c_void {
    let set = context.cast::<GenerationContext>();
    unsafe {
        let mut blksize = (*set).nextBlockSize as Size;
        (*set).nextBlockSize = ((*set).nextBlockSize.saturating_mul(2)).min((*set).maxBlockSize);
        let required_size =
            chunk_size + generation_chunk_header_size() + generation_block_header_size();
        if blksize < required_size {
            blksize = next_power_of_two(required_size);
        }
        let block = libc::malloc(blksize).cast::<GenerationBlockData>();
        if block.is_null() {
            return ptr::null_mut();
        }
        (*context).mem_allocated = (*context).mem_allocated.saturating_add(blksize);
        generation_block_init(set, block, blksize);
        dlist_push_head(&mut (*set).blocks, &mut (*block).node);
        (*set).block = block;
        generation_alloc_chunk_from_block(context, block, size, chunk_size)
    }
}

unsafe fn generation_free_internal(pointer: *mut c_void) -> PgResult<()> {
    if pointer.is_null() {
        return Err(PgError::error("memory pointer is null"));
    }
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        let block = if (*chunk).is_external() {
            external_chunk_get_block(chunk)
        } else {
            memory_chunk_get_block(chunk).cast::<GenerationBlockData>()
        };
        validate_block(block)?;
        (*block).nfree += 1;
        if (*block).nfree < (*block).nchunks {
            return Ok(());
        }
        let set = (*block).context;
        if block == keeper_block(set) || (*set).block == block {
            generation_block_mark_empty(block);
        } else if (*set).freeblock.is_null() {
            generation_block_mark_empty(block);
            (*set).freeblock = block;
        } else {
            generation_block_free(set, block);
        }
    }
    Ok(())
}

unsafe fn generation_realloc_internal(pointer: *mut c_void, size: Size, flags: i32) -> *mut c_void {
    if pointer.is_null() {
        return ptr::null_mut();
    }
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        let block = if (*chunk).is_external() {
            external_chunk_get_block(chunk)
        } else {
            memory_chunk_get_block(chunk).cast::<GenerationBlockData>()
        };
        if validate_block(block).is_err() {
            return ptr::null_mut();
        }
        let old_size = if (*chunk).is_external() {
            (*block).endptr.offset_from(pointer.cast::<c_char>()) as Size
        } else {
            (*chunk).value()
        };
        if old_size >= size {
            return pointer;
        }
        let set = (*block).context;
        let new_pointer = generation_alloc_internal(set.cast(), size, flags);
        if new_pointer.is_null() {
            return ptr::null_mut();
        }
        ptr::copy_nonoverlapping(pointer.cast::<u8>(), new_pointer.cast::<u8>(), old_size);
        let _ = generation_free_internal(pointer);
        new_pointer
    }
}

unsafe fn generation_get_chunk_context_internal(pointer: *mut c_void) -> MemoryContext {
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        let block = if (*chunk).is_external() {
            external_chunk_get_block(chunk)
        } else {
            memory_chunk_get_block(chunk).cast::<GenerationBlockData>()
        };
        (*block).context.header_ptr()
    }
}

unsafe fn generation_get_chunk_space_internal(pointer: *mut c_void) -> Size {
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        if (*chunk).is_external() {
            let block = external_chunk_get_block(chunk);
            return generation_chunk_header_size()
                + (*block).endptr.offset_from(pointer.cast::<c_char>()) as Size;
        }
        generation_chunk_header_size() + (*chunk).value()
    }
}

unsafe fn generation_is_empty_internal(context: MemoryContext) -> bool {
    let set = context.cast::<GenerationContext>();
    unsafe {
        let mut node = (*set).blocks.head.next;
        while node != &mut (*set).blocks.head {
            let block = node.cast::<GenerationBlockData>();
            if (*block).nchunks > 0 {
                return false;
            }
            node = (*node).next;
        }
    }
    true
}

unsafe fn generation_stats_internal(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    let set = context.cast::<GenerationContext>();
    unsafe {
        let mut nblocks = 0;
        let mut nchunks = 0;
        let mut nfreechunks = 0;
        let mut totalspace = maxalign(std::mem::size_of::<GenerationContext>());
        let mut freespace = 0;
        let mut node = (*set).blocks.head.next;
        while node != &mut (*set).blocks.head {
            let block = node.cast::<GenerationBlockData>();
            nblocks += 1;
            nchunks += (*block).nchunks as Size;
            nfreechunks += (*block).nfree as Size;
            totalspace += (*block).blksize;
            freespace += (*block).endptr.offset_from((*block).freeptr) as Size;
            node = (*node).next;
        }
        if let Some(printfunc) = printfunc {
            let stats = CString::new(format!(
                "{totalspace} total in {nblocks} blocks ({nchunks} chunks); {freespace} free ({nfreechunks} chunks); {} used",
                totalspace - freespace
            ))
            .expect("stats string contains no nul");
            printfunc(context, passthru, stats.as_ptr(), print_to_stderr);
        }
        if !totals.is_null() {
            (*totals).nblocks = (*totals).nblocks.saturating_add(nblocks);
            (*totals).freechunks = (*totals).freechunks.saturating_add(nfreechunks);
            (*totals).totalspace = (*totals).totalspace.saturating_add(totalspace);
            (*totals).freespace = (*totals).freespace.saturating_add(freespace);
        }
    }
}

unsafe fn generation_check_internal(context: MemoryContext) -> PgResult<()> {
    if context.is_null() {
        return Err(PgError::error("memory context is null"));
    }
    let set = context.cast::<GenerationContext>();
    unsafe {
        if (*context).type_ != T_GENERATION_CONTEXT {
            return Err(PgError::error("memory context is not a Generation context"));
        }
        let mut total_allocated: Size = 0;
        let mut node = (*set).blocks.head.next;
        while node != &mut (*set).blocks.head {
            if (node as usize) % std::mem::align_of::<DListNode>() != 0 {
                return Err(PgError::error("Generation block list node is misaligned"));
            }
            let block = node.cast::<GenerationBlockData>();
            validate_block(block)?;
            if (*block).context != set {
                return Err(PgError::error(
                    "Generation block points at the wrong context",
                ));
            }
            if (*block).nfree > (*block).nchunks {
                return Err(PgError::error(
                    "Generation block has more free chunks than chunks",
                ));
            }
            if (*block).freeptr < block.cast::<c_char>() || (*block).freeptr > (*block).endptr {
                return Err(PgError::error("Generation block free pointer is invalid"));
            }
            total_allocated = total_allocated.saturating_add((*block).blksize);
            node = (*node).next;
        }
        if total_allocated != (*context).mem_allocated {
            return Err(PgError::error(
                "Generation context mem_allocated does not match block total",
            ));
        }
    }
    Ok(())
}

trait HeaderPtr {
    unsafe fn header_ptr(self) -> MemoryContext;
}

impl HeaderPtr for GenerationSet {
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
        metadata.try_reserve(1).map_err(|_| {
            out_of_memory().with_detail("Failed while tracking Generation metadata.")
        })?;
        metadata.push(GenerationMetadata {
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
            return Err(PgError::error("Generation metadata is missing"));
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
    min_context_size: Size,
    init_block_size: Size,
    max_block_size: Size,
) -> PgResult<()> {
    if init_block_size < 1024
        || init_block_size != maxalign(init_block_size)
        || max_block_size < init_block_size
        || max_block_size != maxalign(max_block_size)
        || max_block_size > MAX_ALLOC_HUGE_SIZE
        || max_block_size > MEMORYCHUNK_MAX_BLOCKOFFSET as Size
    {
        return Err(PgError::error("invalid Generation context size parameters"));
    }
    if min_context_size != 0
        && (min_context_size < 1024
            || min_context_size > max_block_size
            || min_context_size != maxalign(min_context_size))
    {
        return Err(PgError::error("invalid Generation minimum context size"));
    }
    Ok(())
}

fn initial_allocation_size(min_context_size: Size, init_block_size: Size) -> PgResult<Size> {
    let size = maxalign(std::mem::size_of::<GenerationContext>())
        .checked_add(generation_block_header_size())
        .and_then(|size| size.checked_add(generation_chunk_header_size()))
        .ok_or_else(out_of_memory)?;
    Ok(if min_context_size != 0 {
        size.max(min_context_size)
    } else {
        size.max(init_block_size)
    })
}

fn alloc_chunk_limit(max_block_size: Size) -> Size {
    let mut limit = max_block_size.min(MEMORYCHUNK_MAX_VALUE as Size);
    while limit + generation_chunk_header_size()
        > (max_block_size - generation_block_header_size()) / GENERATION_CHUNK_FRACTION
    {
        limit >>= 1;
    }
    limit
}

fn check_size(size: Size, flags: i32) -> PgResult<()> {
    let valid = if flags & MCXT_ALLOC_HUGE != 0 {
        size <= MAX_ALLOC_HUGE_SIZE
    } else {
        size <= MAX_ALLOC_SIZE
    };
    if valid {
        Ok(())
    } else {
        Err(out_of_memory())
    }
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

fn next_power_of_two(size: Size) -> Size {
    size.checked_next_power_of_two().unwrap_or(size)
}

fn generation_block_header_size() -> Size {
    maxalign(std::mem::size_of::<GenerationBlockData>())
}

fn generation_chunk_header_size() -> Size {
    std::mem::size_of::<MemoryChunk>()
}

unsafe fn keeper_block(set: GenerationSet) -> GenerationBlock {
    unsafe {
        set.cast::<c_char>()
            .add(maxalign(std::mem::size_of::<GenerationContext>()))
            .cast()
    }
}

unsafe fn generation_block_init(context: GenerationSet, block: GenerationBlock, blksize: Size) {
    unsafe {
        (*block).context = context;
        (*block).blksize = blksize;
        (*block).nchunks = 0;
        (*block).nfree = 0;
        (*block).freeptr = block.cast::<c_char>().add(generation_block_header_size());
        (*block).endptr = block.cast::<c_char>().add(blksize);
    }
}

unsafe fn generation_block_mark_empty(block: GenerationBlock) {
    unsafe {
        (*block).nchunks = 0;
        (*block).nfree = 0;
        (*block).freeptr = block.cast::<c_char>().add(generation_block_header_size());
    }
}

unsafe fn generation_block_free_bytes(block: GenerationBlock) -> Size {
    unsafe { (*block).endptr.offset_from((*block).freeptr) as Size }
}

unsafe fn generation_block_free(set: GenerationSet, block: GenerationBlock) {
    unsafe {
        dlist_delete(&mut (*block).node);
        (*set).header.mem_allocated = (*set).header.mem_allocated.saturating_sub((*block).blksize);
        libc::free(block.cast());
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

unsafe fn external_chunk_get_block(chunk: *mut MemoryChunk) -> GenerationBlock {
    unsafe {
        chunk
            .cast::<u8>()
            .sub(generation_block_header_size())
            .cast()
    }
}

unsafe fn validate_block(block: GenerationBlock) -> PgResult<()> {
    if block.is_null() {
        return Err(PgError::error("memory block is null"));
    }
    let set = unsafe { (*block).context };
    if set.is_null() || unsafe { (*set).header.type_ != T_GENERATION_CONTEXT } {
        return Err(PgError::error(
            "memory block does not belong to a Generation context",
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

unsafe fn dlist_push_head(head: *mut DListHead, node: *mut DListNode) {
    unsafe {
        (*node).next = (*head).head.next;
        (*node).prev = &mut (*head).head;
        (*(*head).head.next).prev = node;
        (*head).head.next = node;
    }
}

unsafe fn dlist_delete(node: *mut DListNode) {
    unsafe {
        (*(*node).prev).next = (*node).next;
        (*(*node).next).prev = (*node).prev;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generation_layout_starts_with_memory_context_header() {
        assert_eq!(std::mem::offset_of!(GenerationContext, header), 0);
        assert_eq!(
            generation_chunk_header_size(),
            std::mem::size_of::<MemoryChunk>()
        );
        assert_eq!(
            generation_chunk_header_size(),
            maxalign(generation_chunk_header_size())
        );
    }

    #[test]
    fn generation_alloc_limit_matches_postgres_shape() {
        assert!(alloc_chunk_limit(8 * 1024 * 1024) <= MEMORYCHUNK_MAX_VALUE as Size);
        assert!(alloc_chunk_limit(8 * 1024) < 8 * 1024);
    }
}
