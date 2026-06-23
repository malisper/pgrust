use std::cell::RefCell;
use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr::{self, NonNull};

use ::error_fgram::{PgError, PgResult};
use ::pg_ffi_fgram::{
    MemoryChunk, MemoryContext, MemoryContextCounters, MemoryContextData, MemoryContextMethods,
    MemoryStatsPrintFunc, Size, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MINSIZE,
    ALLOCSET_SMALL_INITSIZE, ALLOCSET_SMALL_MINSIZE, MAX_ALLOC_HUGE_SIZE, MAX_ALLOC_SIZE,
    MCTX_ASET_ID, MCXT_ALLOC_HUGE, MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO, MEMORYCHUNK_MAX_BLOCKOFFSET,
    T_ALLOC_SET_CONTEXT,
};

const ALLOC_MINBITS: usize = 3;
const ALLOCSET_NUM_FREELISTS: usize = 11;
const ALLOC_CHUNK_LIMIT: Size = 1 << (ALLOCSET_NUM_FREELISTS - 1 + ALLOC_MINBITS);
const ALLOC_CHUNK_FRACTION: Size = 4;
const MAX_FREE_CONTEXTS: i32 = 100;

type AllocSet = *mut AllocSetContext;
type AllocBlock = *mut AllocBlockData;

#[repr(C)]
struct AllocSetContext {
    header: MemoryContextData,
    blocks: AllocBlock,
    freelist: [*mut MemoryChunk; ALLOCSET_NUM_FREELISTS],
    initBlockSize: u32,
    maxBlockSize: u32,
    nextBlockSize: u32,
    allocChunkLimit: u32,
    freeListIndex: i32,
}

#[repr(C)]
struct AllocBlockData {
    aset: AllocSet,
    prev: AllocBlock,
    next: AllocBlock,
    freeptr: *mut c_char,
    endptr: *mut c_char,
}

#[repr(C)]
struct AllocFreeListLink {
    next: *mut MemoryChunk,
}

#[derive(Clone, Copy)]
struct AllocSetFreeList {
    num_free: i32,
    first_free: AllocSet,
}

struct AllocSetMetadata {
    context: MemoryContext,
    owned_name: *mut c_char,
    owned_ident: *mut c_char,
}

#[cfg(feature = "memory-context-checking")]
struct DebugAllocation {
    context: MemoryContext,
    pointer: *mut c_void,
    requested_size: Size,
    allocated_size: Size,
}

thread_local! {
    static CONTEXT_FREELISTS: RefCell<[AllocSetFreeList; 2]> = const {
        RefCell::new([
            AllocSetFreeList { num_free: 0, first_free: ptr::null_mut() },
            AllocSetFreeList { num_free: 0, first_free: ptr::null_mut() },
        ])
    };

    static OWNED_METADATA: RefCell<Vec<AllocSetMetadata>> = const {
        RefCell::new(Vec::new())
    };

    #[cfg(feature = "memory-context-checking")]
    static DEBUG_ALLOCATIONS: RefCell<Vec<DebugAllocation>> = const {
        RefCell::new(Vec::new())
    };
}

pub fn methods() -> *const MemoryContextMethods {
    &ALLOC_SET_METHODS
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

    let free_list_index = free_list_index(min_context_size, init_block_size);
    match reuse_context(parent, name, free_list_index, max_block_size) {
        Ok(Some(context)) => return Ok(context),
        Ok(None) => {}
        Err(error) => {
            unsafe {
                drop(CString::from_raw(name));
            }
            return Err(error);
        }
    }

    let mut first_block_size = maxalign(std::mem::size_of::<AllocSetContext>())
        .checked_add(alloc_block_header_size())
        .and_then(|size| size.checked_add(alloc_chunk_header_size()))
        .ok_or_else(out_of_memory)?;
    if min_context_size != 0 {
        first_block_size = first_block_size.max(min_context_size);
    } else {
        first_block_size = first_block_size.max(init_block_size);
    }

    let set = unsafe { libc::malloc(first_block_size).cast::<AllocSetContext>() };
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
        let block = keeper_block(set);
        (*block).aset = set;
        (*block).freeptr = (block.cast::<c_char>()).add(alloc_block_header_size());
        (*block).endptr = (set.cast::<c_char>()).add(first_block_size);
        (*block).prev = ptr::null_mut();
        (*block).next = ptr::null_mut();

        (*set).blocks = block;
        (*set).freelist = [ptr::null_mut(); ALLOCSET_NUM_FREELISTS];
        (*set).initBlockSize = init_block_size as u32;
        (*set).maxBlockSize = max_block_size as u32;
        (*set).nextBlockSize = init_block_size as u32;
        (*set).freeListIndex = free_list_index;
        (*set).allocChunkLimit = alloc_chunk_limit(max_block_size) as u32;

        crate::raw::memory_context_create_common(
            set.cast(),
            T_ALLOC_SET_CONTEXT,
            MCTX_ASET_ID,
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
    let pointer = unsafe { alloc_set_alloc_internal(context, size, flags) };
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
    unsafe { alloc_set_free_internal(pointer) }
}

pub unsafe fn realloc(
    pointer: *mut c_void,
    size: Size,
    flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    let context = unsafe { AllocSetGetChunkContext(pointer) };
    let pointer = unsafe { alloc_set_realloc_internal(pointer, size, flags) };
    NonNull::new(pointer).map_or_else(
        || allocation_failure(context, size, flags),
        |ptr| Ok(Some((ptr, size))),
    )
}

pub unsafe fn reset(context: MemoryContext) -> PgResult<()> {
    unsafe { alloc_set_reset_internal(context) };
    Ok(())
}

pub unsafe fn delete(context: MemoryContext) -> PgResult<()> {
    unsafe { alloc_set_delete_internal(context) };
    Ok(())
}

pub unsafe fn get_chunk_context(pointer: *mut c_void) -> PgResult<MemoryContext> {
    Ok(unsafe { AllocSetGetChunkContext(pointer) })
}

pub unsafe fn get_chunk_space(pointer: *mut c_void) -> PgResult<Size> {
    Ok(unsafe { AllocSetGetChunkSpace(pointer) })
}

pub unsafe fn is_empty(context: MemoryContext) -> PgResult<bool> {
    Ok(unsafe { AllocSetIsEmpty(context) })
}

pub unsafe fn stats(context: MemoryContext) -> PgResult<MemoryContextCounters> {
    let mut totals = MemoryContextCounters::default();
    unsafe { alloc_set_stats_internal(context, None, ptr::null_mut(), &mut totals, false) };
    Ok(totals)
}

pub unsafe fn check(context: MemoryContext) -> PgResult<()> {
    unsafe { alloc_set_check_internal(context) }
}

unsafe extern "C" fn method_alloc(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    unsafe { alloc_set_alloc_internal(context, size, flags) }
}

unsafe extern "C" fn method_free(pointer: *mut c_void) {
    let _ = unsafe { alloc_set_free_internal(pointer) };
}

unsafe extern "C" fn method_realloc(pointer: *mut c_void, size: Size, flags: i32) -> *mut c_void {
    unsafe { alloc_set_realloc_internal(pointer, size, flags) }
}

unsafe extern "C" fn method_reset(context: MemoryContext) {
    unsafe { alloc_set_reset_internal(context) };
}

unsafe extern "C" fn method_delete(context: MemoryContext) {
    unsafe { alloc_set_delete_internal(context) };
}

unsafe extern "C" fn method_get_chunk_context(pointer: *mut c_void) -> MemoryContext {
    unsafe { AllocSetGetChunkContext(pointer) }
}

unsafe extern "C" fn method_get_chunk_space(pointer: *mut c_void) -> Size {
    unsafe { alloc_set_get_chunk_space_internal(pointer) }
}

unsafe extern "C" fn method_is_empty(context: MemoryContext) -> bool {
    unsafe { alloc_set_is_empty_internal(context) }
}

unsafe extern "C" fn method_stats(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    unsafe { alloc_set_stats_internal(context, printfunc, passthru, totals, print_to_stderr) };
}

const ALLOC_SET_METHODS: MemoryContextMethods = MemoryContextMethods {
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

#[no_mangle]
pub unsafe extern "C" fn AllocSetContextCreateInternal(
    parent: MemoryContext,
    name: *const c_char,
    min_context_size: Size,
    init_block_size: Size,
    max_block_size: Size,
) -> MemoryContext {
    if name.is_null() {
        return ptr::null_mut();
    }
    let parent = if parent.is_null() { None } else { Some(parent) };
    let name = unsafe { CStr::from_ptr(name) }
        .to_string_lossy()
        .into_owned();
    create_context(
        parent,
        name,
        min_context_size,
        init_block_size,
        max_block_size,
    )
    .unwrap_or(ptr::null_mut())
}

#[no_mangle]
pub unsafe extern "C" fn AllocSetReset(context: MemoryContext) {
    unsafe { alloc_set_reset_internal(context) };
}

unsafe fn alloc_set_reset_internal(context: MemoryContext) {
    let set = context.cast::<AllocSetContext>();
    unsafe {
        debug_check_context(context);
        remove_debug_allocations_for_context(context);
        (*set).freelist.fill(ptr::null_mut());
        let mut block = (*set).blocks;
        (*set).blocks = keeper_block(set);

        while !block.is_null() {
            let next = (*block).next;
            if block == keeper_block(set) {
                let datastart = block.cast::<c_char>().add(alloc_block_header_size());
                clobber_memory(
                    datastart.cast(),
                    (*block).endptr.offset_from(datastart) as Size,
                );
                (*block).freeptr = datastart;
                (*block).prev = ptr::null_mut();
                (*block).next = ptr::null_mut();
            } else {
                clobber_memory(
                    block.cast(),
                    (*block).endptr.offset_from(block.cast::<c_char>()) as Size,
                );
                (*context).mem_allocated =
                    (*context).mem_allocated.saturating_sub(block_size(block));
                libc::free(block.cast());
            }
            block = next;
        }
        (*set).nextBlockSize = (*set).initBlockSize;
    }
}

#[no_mangle]
pub unsafe extern "C" fn AllocSetDelete(context: MemoryContext) {
    unsafe { alloc_set_delete_internal(context) };
}

unsafe fn alloc_set_delete_internal(context: MemoryContext) {
    let set = context.cast::<AllocSetContext>();
    let free_list_index = unsafe { (*set).freeListIndex };
    if free_list_index >= 0 {
        let should_cache = CONTEXT_FREELISTS.with(|freelists| {
            let mut freelists = freelists.borrow_mut();
            let freelist = &mut freelists[free_list_index as usize];
            unsafe {
                if !(*context).isReset {
                    alloc_set_reset_internal(context);
                }
                remove_debug_allocations_for_context(context);
                release_owned_metadata(context);
                if freelist.num_free >= MAX_FREE_CONTEXTS {
                    while !freelist.first_free.is_null() {
                        let oldset = freelist.first_free;
                        freelist.first_free = (*oldset).header.nextchild.cast();
                        freelist.num_free -= 1;
                        release_owned_metadata(oldset.cast());
                        libc::free(oldset.cast());
                    }
                }
                (*set).header.nextchild = freelist.first_free.cast();
                freelist.first_free = set;
                freelist.num_free += 1;
            }
            true
        });
        if should_cache {
            return;
        }
    }

    unsafe {
        let mut block = (*set).blocks;
        remove_debug_allocations_for_context(context);
        release_owned_metadata(context);
        while !block.is_null() {
            let next = (*block).next;
            if block != keeper_block(set) {
                (*context).mem_allocated =
                    (*context).mem_allocated.saturating_sub(block_size(block));
                libc::free(block.cast());
            }
            block = next;
        }
        libc::free(set.cast());
    }
}

#[no_mangle]
pub unsafe extern "C" fn AllocSetAlloc(
    context: MemoryContext,
    size: Size,
    flags: i32,
) -> *mut c_void {
    unsafe { alloc_set_alloc_internal(context, size, flags) }
}

unsafe fn alloc_set_alloc_internal(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    let set = context.cast::<AllocSetContext>();
    unsafe {
        if size > (*set).allocChunkLimit as Size {
            return alloc_set_alloc_large(context, size, flags);
        }

        let fidx = alloc_set_free_index(size);
        let chunk = (*set).freelist[fidx];
        if !chunk.is_null() {
            let link = free_list_link(chunk);
            (*set).freelist[fidx] = (*link).next;
            let pointer = memory_chunk_get_pointer(chunk);
            let chunk_size = chunk_size_from_freelist_index(fidx);
            randomize_allocated_memory(pointer, chunk_size);
            register_debug_allocation(context, pointer, size, chunk_size);
            return pointer;
        }

        let chunk_size = chunk_size_from_freelist_index(fidx);
        let block = (*set).blocks;
        let availspace = (*block).endptr.offset_from((*block).freeptr) as Size;
        if availspace < chunk_size + alloc_chunk_header_size() {
            return alloc_set_alloc_from_new_block(context, size, flags, fidx);
        }

        alloc_set_alloc_chunk_from_block(context, block, size, chunk_size, fidx)
    }
}

unsafe fn alloc_set_alloc_large(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    if let Err(error) = check_size(size, flags) {
        let _ = error;
        return ptr::null_mut();
    }

    let chunk_size = maxalign(size);
    let Some(blksize) = chunk_size
        .checked_add(alloc_block_header_size())
        .and_then(|size| size.checked_add(alloc_chunk_header_size()))
    else {
        return ptr::null_mut();
    };
    let block = unsafe { libc::malloc(blksize).cast::<AllocBlockData>() };
    if block.is_null() {
        return ptr::null_mut();
    }

    unsafe {
        let set = context.cast::<AllocSetContext>();
        (*context).mem_allocated = (*context).mem_allocated.saturating_add(blksize);
        (*block).aset = set;
        (*block).freeptr = block.cast::<c_char>().add(blksize);
        (*block).endptr = (*block).freeptr;
        let chunk = block
            .cast::<c_char>()
            .add(alloc_block_header_size())
            .cast::<MemoryChunk>();
        (*chunk).set_external(MCTX_ASET_ID);

        if !(*set).blocks.is_null() {
            (*block).prev = (*set).blocks;
            (*block).next = (*(*set).blocks).next;
            if !(*block).next.is_null() {
                (*(*block).next).prev = block;
            }
            (*(*set).blocks).next = block;
        } else {
            (*block).prev = ptr::null_mut();
            (*block).next = ptr::null_mut();
            (*set).blocks = block;
        }
        let pointer = memory_chunk_get_pointer(chunk);
        randomize_allocated_memory(pointer, chunk_size);
        register_debug_allocation(context, pointer, size, chunk_size);
        pointer
    }
}

unsafe fn alloc_set_alloc_chunk_from_block(
    context: MemoryContext,
    block: AllocBlock,
    size: Size,
    chunk_size: Size,
    fidx: usize,
) -> *mut c_void {
    unsafe {
        let chunk = (*block).freeptr.cast::<MemoryChunk>();
        (*block).freeptr = (*block).freeptr.add(chunk_size + alloc_chunk_header_size());
        let block_offset = chunk.cast::<c_char>().offset_from(block.cast::<c_char>()) as Size;
        (*chunk).set_hdrmask(block_offset, fidx, MCTX_ASET_ID);
        let pointer = memory_chunk_get_pointer(chunk);
        randomize_allocated_memory(pointer, chunk_size);
        register_debug_allocation(context, pointer, size, chunk_size);
        pointer
    }
}

unsafe fn alloc_set_alloc_from_new_block(
    context: MemoryContext,
    size: Size,
    _flags: i32,
    fidx: usize,
) -> *mut c_void {
    let set = context.cast::<AllocSetContext>();
    unsafe {
        let block = (*set).blocks;
        let mut availspace = (*block).endptr.offset_from((*block).freeptr) as Size;
        while availspace >= (1 << ALLOC_MINBITS) + alloc_chunk_header_size() {
            let mut availchunk = availspace - alloc_chunk_header_size();
            let mut a_fidx = alloc_set_free_index(availchunk);
            if availchunk != chunk_size_from_freelist_index(a_fidx) {
                a_fidx -= 1;
                availchunk = chunk_size_from_freelist_index(a_fidx);
            }

            let chunk = (*block).freeptr.cast::<MemoryChunk>();
            (*block).freeptr = (*block).freeptr.add(availchunk + alloc_chunk_header_size());
            availspace -= availchunk + alloc_chunk_header_size();
            let block_offset = chunk.cast::<c_char>().offset_from(block.cast::<c_char>()) as Size;
            (*chunk).set_hdrmask(block_offset, a_fidx, MCTX_ASET_ID);

            let link = free_list_link(chunk);
            (*link).next = (*set).freelist[a_fidx];
            (*set).freelist[a_fidx] = chunk;
        }

        let mut blksize = (*set).nextBlockSize as Size;
        (*set).nextBlockSize = ((*set).nextBlockSize.saturating_mul(2)).min((*set).maxBlockSize);

        let chunk_size = chunk_size_from_freelist_index(fidx);
        let required_size = chunk_size + alloc_block_header_size() + alloc_chunk_header_size();
        while blksize < required_size {
            blksize = blksize.saturating_mul(2);
        }

        let mut new_block = libc::malloc(blksize).cast::<AllocBlockData>();
        while new_block.is_null() && blksize > 1024 * 1024 {
            blksize >>= 1;
            if blksize < required_size {
                break;
            }
            new_block = libc::malloc(blksize).cast::<AllocBlockData>();
        }
        if new_block.is_null() {
            return ptr::null_mut();
        }

        (*context).mem_allocated = (*context).mem_allocated.saturating_add(blksize);
        (*new_block).aset = set;
        (*new_block).freeptr = new_block.cast::<c_char>().add(alloc_block_header_size());
        (*new_block).endptr = new_block.cast::<c_char>().add(blksize);
        (*new_block).prev = ptr::null_mut();
        (*new_block).next = (*set).blocks;
        if !(*new_block).next.is_null() {
            (*(*new_block).next).prev = new_block;
        }
        (*set).blocks = new_block;

        alloc_set_alloc_chunk_from_block(context, new_block, size, chunk_size, fidx)
    }
}

#[no_mangle]
pub unsafe extern "C" fn AllocSetFree(pointer: *mut c_void) {
    let _ = unsafe { alloc_set_free_internal(pointer) };
}

unsafe fn alloc_set_free_internal(pointer: *mut c_void) -> PgResult<()> {
    if pointer.is_null() {
        return Err(PgError::error("memory pointer is null"));
    }
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        check_debug_sentinel(pointer)?;
        unregister_debug_allocation(pointer);
        if (*chunk).is_external() {
            let block = external_chunk_get_block(chunk);
            validate_block(block)?;
            let set = (*block).aset;
            if !(*block).prev.is_null() {
                (*(*block).prev).next = (*block).next;
            } else {
                (*set).blocks = (*block).next;
            }
            if !(*block).next.is_null() {
                (*(*block).next).prev = (*block).prev;
            }
            (*set).header.mem_allocated = (*set)
                .header
                .mem_allocated
                .saturating_sub(block_size(block));
            clobber_memory(block.cast(), block_size(block));
            libc::free(block.cast());
        } else {
            let block = memory_chunk_get_block(chunk).cast::<AllocBlockData>();
            validate_block(block)?;
            let set = (*block).aset;
            let fidx = (*chunk).value();
            if fidx >= ALLOCSET_NUM_FREELISTS {
                return Err(PgError::error("memory chunk has invalid freelist index"));
            }
            clobber_memory(pointer, chunk_size_from_freelist_index(fidx));
            let link = free_list_link(chunk);
            (*link).next = (*set).freelist[fidx];
            (*set).freelist[fidx] = chunk;
        }
    }
    Ok(())
}

#[no_mangle]
pub unsafe extern "C" fn AllocSetRealloc(
    pointer: *mut c_void,
    size: Size,
    flags: i32,
) -> *mut c_void {
    unsafe { alloc_set_realloc_internal(pointer, size, flags) }
}

unsafe fn alloc_set_realloc_internal(pointer: *mut c_void, size: Size, flags: i32) -> *mut c_void {
    if pointer.is_null() {
        return ptr::null_mut();
    }
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        if check_debug_sentinel(pointer).is_err() {
            return ptr::null_mut();
        }
        if (*chunk).is_external() {
            let block = external_chunk_get_block(chunk);
            if validate_block(block).is_err() {
                return ptr::null_mut();
            }
            let set = (*block).aset;
            if check_size(size, flags).is_err() {
                return ptr::null_mut();
            }
            let chksize = maxalign(size);
            let blksize = chksize + alloc_block_header_size() + alloc_chunk_header_size();
            let oldblksize = block_size(block);
            let new_block = libc::realloc(block.cast(), blksize).cast::<AllocBlockData>();
            if new_block.is_null() {
                return ptr::null_mut();
            }
            unregister_debug_allocation(pointer);
            (*set).header.mem_allocated = (*set)
                .header
                .mem_allocated
                .saturating_sub(oldblksize)
                .saturating_add(blksize);
            (*new_block).freeptr = new_block.cast::<c_char>().add(blksize);
            (*new_block).endptr = (*new_block).freeptr;
            if !(*new_block).prev.is_null() {
                (*(*new_block).prev).next = new_block;
            } else {
                (*set).blocks = new_block;
            }
            if !(*new_block).next.is_null() {
                (*(*new_block).next).prev = new_block;
            }
            let pointer = memory_chunk_get_pointer(
                new_block
                    .cast::<c_char>()
                    .add(alloc_block_header_size())
                    .cast(),
            );
            register_debug_allocation(set.cast(), pointer, size, chksize);
            return pointer;
        }

        let block = memory_chunk_get_block(chunk).cast::<AllocBlockData>();
        if validate_block(block).is_err() {
            return ptr::null_mut();
        }
        let fidx = (*chunk).value();
        if fidx >= ALLOCSET_NUM_FREELISTS {
            return ptr::null_mut();
        }
        let oldchksize = chunk_size_from_freelist_index(fidx);
        if oldchksize >= size {
            register_debug_allocation((*block).aset.cast(), pointer, size, oldchksize);
            return pointer;
        }

        let set = (*block).aset;
        let new_pointer = alloc_set_alloc_internal(set.cast(), size, flags);
        if new_pointer.is_null() {
            return ptr::null_mut();
        }
        ptr::copy_nonoverlapping(pointer.cast::<u8>(), new_pointer.cast::<u8>(), oldchksize);
        let _ = alloc_set_free_internal(pointer);
        new_pointer
    }
}

#[no_mangle]
pub unsafe extern "C" fn AllocSetGetChunkContext(pointer: *mut c_void) -> MemoryContext {
    unsafe { alloc_set_get_chunk_context_internal(pointer) }
}

unsafe fn alloc_set_get_chunk_context_internal(pointer: *mut c_void) -> MemoryContext {
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        let block = if (*chunk).is_external() {
            external_chunk_get_block(chunk)
        } else {
            memory_chunk_get_block(chunk).cast::<AllocBlockData>()
        };
        (*block).aset.header_ptr()
    }
}

#[no_mangle]
pub unsafe extern "C" fn AllocSetGetChunkSpace(pointer: *mut c_void) -> Size {
    unsafe { alloc_set_get_chunk_space_internal(pointer) }
}

unsafe fn alloc_set_get_chunk_space_internal(pointer: *mut c_void) -> Size {
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe {
        if (*chunk).is_external() {
            let block = external_chunk_get_block(chunk);
            return (*block).endptr.offset_from(chunk.cast::<c_char>()) as Size;
        }
        chunk_size_from_freelist_index((*chunk).value()) + alloc_chunk_header_size()
    }
}

#[no_mangle]
pub unsafe extern "C" fn AllocSetIsEmpty(context: MemoryContext) -> bool {
    unsafe { alloc_set_is_empty_internal(context) }
}

unsafe fn alloc_set_is_empty_internal(context: MemoryContext) -> bool {
    unsafe { (*context).isReset }
}

#[no_mangle]
pub unsafe extern "C" fn AllocSetStats(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    unsafe { alloc_set_stats_internal(context, printfunc, passthru, totals, print_to_stderr) };
}

unsafe fn alloc_set_stats_internal(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    let set = context.cast::<AllocSetContext>();
    unsafe {
        let mut nblocks = 0;
        let mut freechunks = 0;
        let mut totalspace = maxalign(std::mem::size_of::<AllocSetContext>());
        let mut freespace = 0;
        let mut block = (*set).blocks;
        while !block.is_null() {
            nblocks += 1;
            totalspace += block_size(block);
            freespace += (*block).endptr.offset_from((*block).freeptr) as Size;
            block = (*block).next;
        }
        for fidx in 0..ALLOCSET_NUM_FREELISTS {
            let chksz = chunk_size_from_freelist_index(fidx);
            let mut chunk = (*set).freelist[fidx];
            while !chunk.is_null() {
                let link = free_list_link(chunk);
                freechunks += 1;
                freespace += chksz + alloc_chunk_header_size();
                chunk = (*link).next;
            }
        }
        if let Some(printfunc) = printfunc {
            let stats = CString::new(format!(
                "{totalspace} total in {nblocks} blocks; {freespace} free ({freechunks} chunks); {} used",
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

#[no_mangle]
pub unsafe extern "C" fn AllocSetCheck(context: MemoryContext) {
    let _ = unsafe { alloc_set_check_internal(context) };
}

unsafe fn alloc_set_check_internal(context: MemoryContext) -> PgResult<()> {
    if context.is_null() {
        return Err(PgError::error("memory context is null"));
    }
    let set = context.cast::<AllocSetContext>();
    unsafe {
        if (*context).type_ != T_ALLOC_SET_CONTEXT {
            return Err(PgError::error("memory context is not an AllocSet"));
        }
        let mut previous = ptr::null_mut();
        let mut block = (*set).blocks;
        while !block.is_null() {
            validate_block(block)?;
            if (*block).aset != set {
                return Err(PgError::error("AllocSet block points at the wrong context"));
            }
            if (*block).prev != previous {
                return Err(PgError::error("AllocSet block list is corrupted"));
            }
            if (*block).freeptr < block.cast::<c_char>() || (*block).freeptr > (*block).endptr {
                return Err(PgError::error("AllocSet block free pointer is invalid"));
            }
            previous = block;
            block = (*block).next;
        }

        for fidx in 0..ALLOCSET_NUM_FREELISTS {
            let mut chunk = (*set).freelist[fidx];
            while !chunk.is_null() {
                if (chunk as usize) % std::mem::align_of::<MemoryChunk>() != 0 {
                    return Err(PgError::error("AllocSet freelist chunk is misaligned"));
                }
                if (*chunk).method_id() != MCTX_ASET_ID {
                    return Err(PgError::error("AllocSet freelist chunk has wrong method"));
                }
                if (*chunk).value() != fidx {
                    return Err(PgError::error("AllocSet freelist chunk has wrong index"));
                }
                let block = memory_chunk_get_block(chunk).cast::<AllocBlockData>();
                validate_block(block)?;
                if (*block).aset != set {
                    return Err(PgError::error(
                        "AllocSet freelist chunk belongs to another context",
                    ));
                }
                chunk = (*free_list_link(chunk)).next;
            }
        }
    }
    check_debug_sentinels_for_context(context)?;
    Ok(())
}

trait HeaderPtr {
    unsafe fn header_ptr(self) -> MemoryContext;
}

impl HeaderPtr for AllocSet {
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
        if let Some(entry) = metadata.iter_mut().find(|entry| entry.context == context) {
            free_cstring(entry.owned_name);
            free_cstring(entry.owned_ident);
            entry.owned_name = owned_name;
            entry.owned_ident = owned_ident;
            return Ok(());
        }
        metadata
            .try_reserve(1)
            .map_err(|_| out_of_memory().with_detail("Failed while tracking AllocSet metadata."))?;
        metadata.push(AllocSetMetadata {
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
            return Err(PgError::error("AllocSet metadata is missing"));
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
    unsafe {
        (*context).name = ptr::null();
        (*context).ident = ptr::null();
    }
}

fn free_cstring(pointer: *mut c_char) {
    if !pointer.is_null() {
        unsafe {
            drop(CString::from_raw(pointer));
        }
    }
}

fn reuse_context(
    parent: Option<MemoryContext>,
    name: *mut c_char,
    free_list_index: i32,
    max_block_size: Size,
) -> PgResult<Option<MemoryContext>> {
    if free_list_index < 0 {
        return Ok(None);
    }
    CONTEXT_FREELISTS.with(|freelists| {
        let mut freelists = freelists.borrow_mut();
        let freelist = &mut freelists[free_list_index as usize];
        if freelist.first_free.is_null() {
            return Ok(None);
        }
        unsafe {
            let set = freelist.first_free;
            freelist.first_free = (*set).header.nextchild.cast();
            freelist.num_free -= 1;
            (*set).maxBlockSize = max_block_size as u32;
            crate::raw::memory_context_create_common(
                set.cast(),
                T_ALLOC_SET_CONTEXT,
                MCTX_ASET_ID,
                parent,
                name,
            )?;
            (*set).header.mem_allocated = (*keeper_block(set))
                .endptr
                .offset_from(set.cast::<c_char>())
                as Size;
            if let Err(error) = register_owned_metadata(set.cast(), name, ptr::null_mut()) {
                libc::free(set.cast());
                return Err(error);
            }
            Ok(Some(set.cast()))
        }
    })
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
        return Err(PgError::error("invalid AllocSet context size parameters"));
    }
    if min_context_size != 0
        && (min_context_size < 1024
            || min_context_size > max_block_size
            || min_context_size != maxalign(min_context_size))
    {
        return Err(PgError::error("invalid AllocSet minimum context size"));
    }
    Ok(())
}

fn free_list_index(min_context_size: Size, init_block_size: Size) -> i32 {
    if min_context_size == ALLOCSET_DEFAULT_MINSIZE && init_block_size == ALLOCSET_DEFAULT_INITSIZE
    {
        0
    } else if min_context_size == ALLOCSET_SMALL_MINSIZE
        && init_block_size == ALLOCSET_SMALL_INITSIZE
    {
        1
    } else {
        -1
    }
}

fn alloc_chunk_limit(max_block_size: Size) -> Size {
    let mut limit = ALLOC_CHUNK_LIMIT;
    while limit + alloc_chunk_header_size()
        > (max_block_size - alloc_block_header_size()) / ALLOC_CHUNK_FRACTION
    {
        limit >>= 1;
    }
    limit
}

fn alloc_set_free_index(size: Size) -> usize {
    if size > (1 << ALLOC_MINBITS) {
        usize::BITS as usize - (size - 1).leading_zeros() as usize - ALLOC_MINBITS
    } else {
        0
    }
}

fn chunk_size_from_freelist_index(fidx: usize) -> Size {
    (1 << ALLOC_MINBITS) << fidx
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

#[inline]
unsafe fn debug_check_context(context: MemoryContext) {
    #[cfg(feature = "memory-context-checking")]
    {
        let _ = unsafe { alloc_set_check_internal(context) };
    }
    #[cfg(not(feature = "memory-context-checking"))]
    {
        let _ = context;
    }
}

#[cfg(feature = "memory-context-checking")]
const SENTINEL: u8 = 0x7e;

#[cfg(feature = "memory-context-checking")]
fn register_debug_allocation(
    context: MemoryContext,
    pointer: *mut c_void,
    requested_size: Size,
    allocated_size: Size,
) {
    set_debug_sentinel(pointer, requested_size, allocated_size);
    DEBUG_ALLOCATIONS.with(|allocations| {
        let mut allocations = allocations.borrow_mut();
        if let Some(allocation) = allocations
            .iter_mut()
            .find(|allocation| allocation.pointer == pointer)
        {
            clear_debug_sentinel(
                allocation.pointer,
                allocation.requested_size,
                allocation.allocated_size,
            );
            allocation.context = context;
            allocation.requested_size = requested_size;
            allocation.allocated_size = allocated_size;
            set_debug_sentinel(pointer, requested_size, allocated_size);
            return;
        }
        allocations.push(DebugAllocation {
            context,
            pointer,
            requested_size,
            allocated_size,
        });
    });
}

#[cfg(not(feature = "memory-context-checking"))]
fn register_debug_allocation(
    _context: MemoryContext,
    _pointer: *mut c_void,
    _requested_size: Size,
    _allocated_size: Size,
) {
}

#[cfg(feature = "memory-context-checking")]
fn unregister_debug_allocation(pointer: *mut c_void) {
    DEBUG_ALLOCATIONS.with(|allocations| {
        allocations
            .borrow_mut()
            .retain(|allocation| allocation.pointer != pointer);
    });
}

#[cfg(not(feature = "memory-context-checking"))]
fn unregister_debug_allocation(_pointer: *mut c_void) {}

#[cfg(feature = "memory-context-checking")]
fn remove_debug_allocations_for_context(context: MemoryContext) {
    DEBUG_ALLOCATIONS.with(|allocations| {
        allocations
            .borrow_mut()
            .retain(|allocation| allocation.context != context);
    });
}

#[cfg(not(feature = "memory-context-checking"))]
fn remove_debug_allocations_for_context(_context: MemoryContext) {}

#[cfg(feature = "memory-context-checking")]
fn check_debug_sentinel(pointer: *mut c_void) -> PgResult<()> {
    DEBUG_ALLOCATIONS.with(|allocations| {
        let allocations = allocations.borrow();
        let Some(allocation) = allocations
            .iter()
            .find(|allocation| allocation.pointer == pointer)
        else {
            return Ok(());
        };
        if allocation.requested_size < allocation.allocated_size {
            let sentinel = unsafe { *pointer.cast::<u8>().add(allocation.requested_size) };
            if sentinel != SENTINEL {
                return Err(PgError::error("memory chunk sentinel is corrupted"));
            }
        }
        Ok(())
    })
}

#[cfg(not(feature = "memory-context-checking"))]
fn check_debug_sentinel(_pointer: *mut c_void) -> PgResult<()> {
    Ok(())
}

#[cfg(feature = "memory-context-checking")]
fn check_debug_sentinels_for_context(context: MemoryContext) -> PgResult<()> {
    DEBUG_ALLOCATIONS.with(|allocations| {
        for allocation in allocations
            .borrow()
            .iter()
            .filter(|allocation| allocation.context == context)
        {
            check_debug_sentinel(allocation.pointer)?;
        }
        Ok(())
    })
}

#[cfg(not(feature = "memory-context-checking"))]
fn check_debug_sentinels_for_context(_context: MemoryContext) -> PgResult<()> {
    Ok(())
}

#[cfg(feature = "memory-context-checking")]
fn set_debug_sentinel(pointer: *mut c_void, requested_size: Size, allocated_size: Size) {
    if requested_size < allocated_size {
        unsafe {
            *pointer.cast::<u8>().add(requested_size) = SENTINEL;
        }
    }
}

#[cfg(feature = "memory-context-checking")]
fn clear_debug_sentinel(pointer: *mut c_void, requested_size: Size, allocated_size: Size) {
    if requested_size < allocated_size {
        unsafe {
            *pointer.cast::<u8>().add(requested_size) = 0;
        }
    }
}

#[inline]
fn clobber_memory(pointer: *mut c_void, size: Size) {
    #[cfg(feature = "clobber-freed-memory")]
    if !pointer.is_null() {
        unsafe {
            ptr::write_bytes(pointer, 0x7f, size);
        }
    }
    #[cfg(not(feature = "clobber-freed-memory"))]
    {
        let _ = (pointer, size);
    }
}

#[inline]
fn randomize_allocated_memory(pointer: *mut c_void, size: Size) {
    #[cfg(feature = "randomize-allocated-memory")]
    if !pointer.is_null() {
        unsafe {
            ptr::write_bytes(pointer, 0x55, size);
        }
    }
    #[cfg(not(feature = "randomize-allocated-memory"))]
    {
        let _ = (pointer, size);
    }
}

fn maxalign(size: Size) -> Size {
    let align = std::mem::align_of::<usize>();
    (size + align - 1) & !(align - 1)
}

fn alloc_block_header_size() -> Size {
    maxalign(std::mem::size_of::<AllocBlockData>())
}

fn alloc_chunk_header_size() -> Size {
    std::mem::size_of::<MemoryChunk>()
}

unsafe fn keeper_block(set: AllocSet) -> AllocBlock {
    unsafe {
        set.cast::<c_char>()
            .add(maxalign(std::mem::size_of::<AllocSetContext>()))
            .cast()
    }
}

unsafe fn block_size(block: AllocBlock) -> Size {
    unsafe { (*block).endptr.offset_from(block.cast::<c_char>()) as Size }
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

unsafe fn external_chunk_get_block(chunk: *mut MemoryChunk) -> AllocBlock {
    unsafe { chunk.cast::<u8>().sub(alloc_block_header_size()).cast() }
}

unsafe fn free_list_link(chunk: *mut MemoryChunk) -> *mut AllocFreeListLink {
    unsafe {
        chunk
            .cast::<u8>()
            .add(std::mem::size_of::<MemoryChunk>())
            .cast()
    }
}

unsafe fn validate_block(block: AllocBlock) -> PgResult<()> {
    if block.is_null() {
        return Err(PgError::error("memory block is null"));
    }
    let set = unsafe { (*block).aset };
    if set.is_null() || unsafe { (*set).header.type_ != T_ALLOC_SET_CONTEXT } {
        return Err(PgError::error(
            "memory block does not belong to an AllocSet",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ::pg_ffi_fgram::{ALLOCSET_SEPARATE_THRESHOLD, INVALID_ALLOC_SIZE};

    #[test]
    fn allocset_freelist_index_matches_postgres_boundaries() {
        assert_eq!(alloc_set_free_index(0), 0);
        assert_eq!(alloc_set_free_index(8), 0);
        assert_eq!(alloc_set_free_index(9), 1);
        assert_eq!(alloc_set_free_index(16), 1);
        assert_eq!(alloc_set_free_index(17), 2);
        assert_eq!(ALLOC_CHUNK_LIMIT, ALLOCSET_SEPARATE_THRESHOLD);
    }

    #[test]
    fn memory_chunk_header_is_allocset_compatible() {
        assert_eq!(
            alloc_chunk_header_size(),
            std::mem::size_of::<MemoryChunk>()
        );
        assert_eq!(
            alloc_chunk_header_size(),
            maxalign(alloc_chunk_header_size())
        );
        assert!(std::mem::size_of::<AllocFreeListLink>() <= (1 << ALLOC_MINBITS));
    }

    #[test]
    fn invalid_allocset_parameters_are_rejected() {
        assert!(validate_create_params(0, 512, 8192).is_err());
        assert!(validate_create_params(0, 1024, 512).is_err());
        assert!(validate_create_params(0, 1024, 8192).is_ok());
    }

    #[test]
    fn invalid_alloc_size_respects_no_oom_flag() {
        assert!(check_size(INVALID_ALLOC_SIZE, 0).is_err());
        assert!(check_size(MAX_ALLOC_SIZE, 0).is_ok());
        assert!(check_size(MAX_ALLOC_HUGE_SIZE, MCXT_ALLOC_HUGE).is_ok());
    }
}
