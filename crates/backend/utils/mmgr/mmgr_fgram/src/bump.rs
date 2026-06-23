use std::cell::RefCell;
use std::ffi::{c_char, c_void, CStr, CString};
use std::ptr::{self, NonNull};

use ::error_fgram::{PgError, PgResult};
use ::pg_ffi_fgram::{
    MemoryContext, MemoryContextCounters, MemoryContextData, MemoryContextMethods,
    MemoryStatsPrintFunc, Size, MAX_ALLOC_HUGE_SIZE, MAX_ALLOC_SIZE, MCTX_BUMP_ID, MCXT_ALLOC_HUGE,
    MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO, MEMORYCHUNK_MAX_BLOCKOFFSET, MEMORYCHUNK_MAX_VALUE,
    T_BUMP_CONTEXT,
};

const BUMP_CHUNK_FRACTION: Size = 8;

type BumpSet = *mut BumpContext;
type BumpBlock = *mut BumpBlockData;

#[repr(C)]
struct BumpContext {
    header: MemoryContextData,
    initBlockSize: u32,
    maxBlockSize: u32,
    nextBlockSize: u32,
    allocChunkLimit: u32,
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
struct BumpBlockData {
    node: DListNode,
    freeptr: *mut c_char,
    endptr: *mut c_char,
}

struct BumpMetadata {
    context: MemoryContext,
    owned_name: *mut c_char,
    owned_ident: *mut c_char,
}

thread_local! {
    static OWNED_METADATA: RefCell<Vec<BumpMetadata>> = const {
        RefCell::new(Vec::new())
    };
}

pub fn methods() -> *const MemoryContextMethods {
    &BUMP_METHODS
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
    let set = unsafe { libc::malloc(alloc_size).cast::<BumpContext>() };
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
        let first_block_size = alloc_size - maxalign(std::mem::size_of::<BumpContext>());
        bump_block_init(block, first_block_size);
        dlist_push_head(&mut (*set).blocks, &mut (*block).node);
        (*set).initBlockSize = init_block_size as u32;
        (*set).maxBlockSize = max_block_size as u32;
        (*set).nextBlockSize = init_block_size as u32;
        (*set).allocChunkLimit = alloc_chunk_limit(max_block_size) as u32;
        crate::raw::memory_context_create_common(
            set.cast(),
            T_BUMP_CONTEXT,
            MCTX_BUMP_ID,
            parent,
            name,
        )?;
        (*set).header.mem_allocated = alloc_size;
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

pub fn owns(pointer: *mut c_void) -> bool {
    if pointer.is_null() {
        return false;
    }
    OWNED_METADATA.with(|metadata| {
        metadata.borrow().iter().any(|entry| unsafe {
            pointer_belongs_to_context(entry.context.cast::<BumpContext>(), pointer)
        })
    })
}

pub unsafe fn alloc(
    context: MemoryContext,
    size: Size,
    flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    let pointer = unsafe { bump_alloc_internal(context, size, flags) };
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

pub fn free(_pointer: *mut c_void) -> PgResult<()> {
    Err(unsupported("pfree"))
}

pub fn realloc(
    _pointer: *mut c_void,
    _size: Size,
    _flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    Err(unsupported("realloc"))
}

pub unsafe fn reset(context: MemoryContext) -> PgResult<()> {
    unsafe { bump_reset_internal(context) };
    Ok(())
}

pub unsafe fn delete(context: MemoryContext) -> PgResult<()> {
    unsafe { bump_delete_internal(context) };
    Ok(())
}

pub fn get_chunk_context(_pointer: *mut c_void) -> PgResult<MemoryContext> {
    Err(unsupported("GetMemoryChunkContext"))
}

pub fn get_chunk_space(_pointer: *mut c_void) -> PgResult<Size> {
    Err(unsupported("GetMemoryChunkSpace"))
}

pub unsafe fn is_empty(context: MemoryContext) -> PgResult<bool> {
    Ok(unsafe { bump_is_empty_internal(context) })
}

pub unsafe fn stats(context: MemoryContext) -> PgResult<MemoryContextCounters> {
    let mut totals = MemoryContextCounters::default();
    unsafe { bump_stats_internal(context, None, ptr::null_mut(), &mut totals, false) };
    Ok(totals)
}

pub unsafe fn check(context: MemoryContext) -> PgResult<()> {
    unsafe { bump_check_internal(context) }
}

unsafe extern "C" fn method_alloc(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    unsafe { bump_alloc_internal(context, size, flags) }
}

unsafe extern "C" fn method_free(_pointer: *mut c_void) {}

unsafe extern "C" fn method_realloc(
    _pointer: *mut c_void,
    _size: Size,
    _flags: i32,
) -> *mut c_void {
    ptr::null_mut()
}

unsafe extern "C" fn method_reset(context: MemoryContext) {
    unsafe { bump_reset_internal(context) };
}

unsafe extern "C" fn method_delete(context: MemoryContext) {
    unsafe { bump_delete_internal(context) };
}

unsafe extern "C" fn method_get_chunk_context(_pointer: *mut c_void) -> MemoryContext {
    ptr::null_mut()
}

unsafe extern "C" fn method_get_chunk_space(_pointer: *mut c_void) -> Size {
    0
}

unsafe extern "C" fn method_is_empty(context: MemoryContext) -> bool {
    unsafe { bump_is_empty_internal(context) }
}

unsafe extern "C" fn method_stats(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    unsafe { bump_stats_internal(context, printfunc, passthru, totals, print_to_stderr) };
}

const BUMP_METHODS: MemoryContextMethods = MemoryContextMethods {
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

unsafe fn bump_reset_internal(context: MemoryContext) {
    let set = context.cast::<BumpContext>();
    unsafe {
        let mut node = (*set).blocks.head.next;
        while node != &mut (*set).blocks.head {
            let next = (*node).next;
            let block = bump_block_from_node(node);
            if block == keeper_block(set) {
                bump_block_mark_empty(block);
            } else {
                bump_block_free(set, block);
            }
            node = next;
        }
        (*set).nextBlockSize = (*set).initBlockSize;
    }
}

unsafe fn bump_delete_internal(context: MemoryContext) {
    unsafe {
        bump_reset_internal(context);
        release_owned_metadata(context);
        libc::free(context.cast());
    }
}

unsafe fn bump_alloc_internal(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    let set = context.cast::<BumpContext>();
    let chunk_size = maxalign(size);
    unsafe {
        if chunk_size > (*set).allocChunkLimit as Size {
            return bump_alloc_large(context, size, flags);
        }
        let block = bump_block_from_node((*set).blocks.head.next);
        if bump_block_free_bytes(block) < chunk_size {
            return bump_alloc_from_new_block(context, size, flags, chunk_size);
        }
        bump_alloc_chunk_from_block(block, chunk_size)
    }
}

unsafe fn bump_alloc_large(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    if check_size(size, flags).is_err() {
        return ptr::null_mut();
    }
    let chunk_size = maxalign(size);
    let Some(blksize) = chunk_size.checked_add(bump_block_header_size()) else {
        return ptr::null_mut();
    };
    let block = unsafe { libc::malloc(blksize).cast::<BumpBlockData>() };
    if block.is_null() {
        return ptr::null_mut();
    }
    unsafe {
        let set = context.cast::<BumpContext>();
        (*context).mem_allocated = (*context).mem_allocated.saturating_add(blksize);
        (*block).freeptr = block.cast::<c_char>().add(blksize);
        (*block).endptr = (*block).freeptr;
        dlist_push_tail(&mut (*set).blocks, &mut (*block).node);
        block.cast::<c_char>().add(bump_block_header_size()).cast()
    }
}

unsafe fn bump_alloc_from_new_block(
    context: MemoryContext,
    size: Size,
    _flags: i32,
    chunk_size: Size,
) -> *mut c_void {
    let set = context.cast::<BumpContext>();
    unsafe {
        let mut blksize = (*set).nextBlockSize as Size;
        (*set).nextBlockSize = ((*set).nextBlockSize.saturating_mul(2)).min((*set).maxBlockSize);
        let required_size = chunk_size + bump_block_header_size();
        if blksize < required_size {
            blksize = next_power_of_two(required_size);
        }
        let block = libc::malloc(blksize).cast::<BumpBlockData>();
        if block.is_null() {
            return ptr::null_mut();
        }
        (*context).mem_allocated = (*context).mem_allocated.saturating_add(blksize);
        bump_block_init(block, blksize);
        dlist_push_head(&mut (*set).blocks, &mut (*block).node);
        let _ = size;
        bump_alloc_chunk_from_block(block, chunk_size)
    }
}

unsafe fn bump_alloc_chunk_from_block(block: BumpBlock, chunk_size: Size) -> *mut c_void {
    unsafe {
        let pointer = (*block).freeptr.cast::<c_void>();
        (*block).freeptr = (*block).freeptr.add(chunk_size);
        pointer
    }
}

unsafe fn bump_is_empty_internal(context: MemoryContext) -> bool {
    let set = context.cast::<BumpContext>();
    unsafe {
        let mut node = (*set).blocks.head.next;
        while node != &mut (*set).blocks.head {
            let block = bump_block_from_node(node);
            if !bump_block_is_empty(block) {
                return false;
            }
            node = (*node).next;
        }
    }
    true
}

unsafe fn bump_stats_internal(
    context: MemoryContext,
    printfunc: MemoryStatsPrintFunc,
    passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    print_to_stderr: bool,
) {
    let set = context.cast::<BumpContext>();
    unsafe {
        let mut nblocks = 0;
        let mut totalspace = 0;
        let mut freespace = 0;
        let mut node = (*set).blocks.head.next;
        while node != &mut (*set).blocks.head {
            let block = bump_block_from_node(node);
            nblocks += 1;
            totalspace += bump_block_size(block);
            freespace += bump_block_free_bytes(block);
            node = (*node).next;
        }
        if let Some(printfunc) = printfunc {
            let stats = CString::new(format!(
                "{totalspace} total in {nblocks} blocks; {freespace} free; {} used",
                totalspace - freespace
            ))
            .expect("stats string contains no nul");
            printfunc(context, passthru, stats.as_ptr(), print_to_stderr);
        }
        if !totals.is_null() {
            (*totals).nblocks = (*totals).nblocks.saturating_add(nblocks);
            (*totals).totalspace = (*totals).totalspace.saturating_add(totalspace);
            (*totals).freespace = (*totals).freespace.saturating_add(freespace);
        }
    }
}

unsafe fn bump_check_internal(context: MemoryContext) -> PgResult<()> {
    if context.is_null() {
        return Err(PgError::error("memory context is null"));
    }
    let set = context.cast::<BumpContext>();
    unsafe {
        if (*context).type_ != T_BUMP_CONTEXT {
            return Err(PgError::error("memory context is not a Bump context"));
        }
        let mut total_allocated: Size = 0;
        let mut node = (*set).blocks.head.next;
        while node != &mut (*set).blocks.head {
            let block = bump_block_from_node(node);
            if (*block).freeptr < block.cast::<c_char>() || (*block).freeptr > (*block).endptr {
                return Err(PgError::error("Bump block free pointer is invalid"));
            }
            total_allocated = total_allocated.saturating_add(if block == keeper_block(set) {
                (*block).endptr.offset_from(set.cast::<c_char>()) as Size
            } else {
                bump_block_size(block)
            });
            node = (*node).next;
        }
        if total_allocated != (*context).mem_allocated {
            return Err(PgError::error(
                "Bump context mem_allocated does not match block total",
            ));
        }
    }
    Ok(())
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
            .map_err(|_| out_of_memory().with_detail("Failed while tracking Bump metadata."))?;
        metadata.push(BumpMetadata {
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
            return Err(PgError::error("Bump metadata is missing"));
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
        return Err(PgError::error("invalid Bump context size parameters"));
    }
    if min_context_size != 0
        && (min_context_size < 1024
            || min_context_size > max_block_size
            || min_context_size != maxalign(min_context_size))
    {
        return Err(PgError::error("invalid Bump minimum context size"));
    }
    Ok(())
}

fn initial_allocation_size(min_context_size: Size, init_block_size: Size) -> PgResult<Size> {
    let size = maxalign(std::mem::size_of::<BumpContext>())
        .checked_add(bump_block_header_size())
        .ok_or_else(out_of_memory)?;
    Ok(if min_context_size != 0 {
        size.max(min_context_size)
    } else {
        size.max(init_block_size)
    })
}

fn alloc_chunk_limit(max_block_size: Size) -> Size {
    let mut limit = max_block_size.min(MEMORYCHUNK_MAX_VALUE as Size);
    while limit > (max_block_size - bump_block_header_size()) / BUMP_CHUNK_FRACTION {
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

fn unsupported(operation: &str) -> PgError {
    PgError::error(format!(
        "{operation} is not supported by the bump memory allocator"
    ))
}

fn maxalign(size: Size) -> Size {
    let align = std::mem::align_of::<usize>();
    (size + align - 1) & !(align - 1)
}

fn next_power_of_two(size: Size) -> Size {
    size.checked_next_power_of_two().unwrap_or(size)
}

fn bump_block_header_size() -> Size {
    maxalign(std::mem::size_of::<BumpBlockData>())
}

unsafe fn keeper_block(set: BumpSet) -> BumpBlock {
    unsafe {
        set.cast::<c_char>()
            .add(maxalign(std::mem::size_of::<BumpContext>()))
            .cast()
    }
}

unsafe fn bump_block_init(block: BumpBlock, blksize: Size) {
    unsafe {
        (*block).freeptr = block.cast::<c_char>().add(bump_block_header_size());
        (*block).endptr = block.cast::<c_char>().add(blksize);
    }
}

unsafe fn bump_block_is_empty(block: BumpBlock) -> bool {
    unsafe { (*block).freeptr == block.cast::<c_char>().add(bump_block_header_size()) }
}

unsafe fn bump_block_mark_empty(block: BumpBlock) {
    unsafe {
        (*block).freeptr = block.cast::<c_char>().add(bump_block_header_size());
    }
}

unsafe fn bump_block_free_bytes(block: BumpBlock) -> Size {
    unsafe { (*block).endptr.offset_from((*block).freeptr) as Size }
}

unsafe fn bump_block_size(block: BumpBlock) -> Size {
    unsafe { (*block).endptr.offset_from(block.cast::<c_char>()) as Size }
}

unsafe fn bump_block_free(set: BumpSet, block: BumpBlock) {
    unsafe {
        dlist_delete(&mut (*block).node);
        (*set).header.mem_allocated = (*set)
            .header
            .mem_allocated
            .saturating_sub(bump_block_size(block));
        libc::free(block.cast());
    }
}

unsafe fn pointer_belongs_to_context(set: BumpSet, pointer: *mut c_void) -> bool {
    unsafe {
        let mut node = (*set).blocks.head.next;
        while node != &mut (*set).blocks.head {
            let block = bump_block_from_node(node);
            let start = block.cast::<c_char>().add(bump_block_header_size()) as usize;
            let end = (*block).freeptr as usize;
            let pointer = pointer as usize;
            if pointer >= start && pointer < end {
                return true;
            }
            node = (*node).next;
        }
    }
    false
}

unsafe fn bump_block_from_node(node: *mut DListNode) -> BumpBlock {
    unsafe {
        node.cast::<c_char>()
            .sub(std::mem::offset_of!(BumpBlockData, node))
            .cast()
    }
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

unsafe fn dlist_push_tail(head: *mut DListHead, node: *mut DListNode) {
    unsafe {
        (*node).next = &mut (*head).head;
        (*node).prev = (*head).head.prev;
        (*(*head).head.prev).next = node;
        (*head).head.prev = node;
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
    fn bump_layout_starts_with_memory_context_header() {
        assert_eq!(std::mem::offset_of!(BumpContext, header), 0);
        assert_eq!(bump_block_header_size(), maxalign(bump_block_header_size()));
    }

    #[test]
    fn bump_alloc_limit_matches_postgres_shape() {
        assert!(alloc_chunk_limit(8 * 1024 * 1024) <= MEMORYCHUNK_MAX_VALUE as Size);
        assert!(alloc_chunk_limit(8 * 1024) < 8 * 1024);
    }
}
