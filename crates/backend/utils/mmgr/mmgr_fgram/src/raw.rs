use std::alloc::Layout;
use std::cell::RefCell;
use std::ffi::{c_char, c_void, CString};
use std::ptr::{self, NonNull};

use error_fgram::{PgError, PgResult};
use pg_ffi_fgram::{
    MemoryChunk, MemoryContext, MemoryContextCallback, MemoryContextCounters, MemoryContextData,
    MemoryContextMethodID, MemoryContextMethods, NodeTag, Size, MAX_ALLOC_HUGE_SIZE,
    MAX_ALLOC_SIZE, MCTX_ALIGNED_REDIRECT_ID, MCTX_ASET_ID, MCTX_BUMP_ID, MCTX_GENERATION_ID,
    MCTX_SLAB_ID, MCXT_ALLOC_HUGE, MCXT_ALLOC_NO_OOM, MCXT_ALLOC_ZERO, T_ALLOC_SET_CONTEXT,
    T_BUMP_CONTEXT, T_GENERATION_CONTEXT, T_SLAB_CONTEXT,
};

use crate::{aligned, aset, bump, generation, slab, MemoryContextKind};

const MINIMUM_ALIGN: usize = std::mem::align_of::<usize>();

thread_local! {
    static STATE: RefCell<State> = const { RefCell::new(State::new()) };
}

struct State {
    initialized: bool,
    current: MemoryContext,
    top: MemoryContext,
    error: MemoryContext,
}

impl State {
    const fn new() -> Self {
        Self {
            initialized: false,
            current: ptr::null_mut(),
            top: ptr::null_mut(),
            error: ptr::null_mut(),
        }
    }
}

#[repr(C)]
struct ContextNode {
    header: MemoryContextData,
    kind: MemoryContextKind,
    name: CString,
    ident: Option<CString>,
    allocations: Vec<NonNull<AllocationHeader>>,
    deleted: bool,
}

#[repr(C)]
struct AllocationHeader {
    chunk: MemoryChunk,
    context: MemoryContext,
    requested_size: Size,
    allocated_size: Size,
    base: *mut c_void,
    method_id: MemoryContextMethodID,
    alignment: Size,
}

pub fn memory_context_init() -> PgResult<()> {
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        if state.initialized {
            return Ok(());
        }

        let top = create_context_locked(
            &mut state,
            None,
            "TopMemoryContext".to_owned(),
            MemoryContextKind::AllocSet,
            0,
            8 * 1024,
            8 * 1024 * 1024,
        )?;
        let error = create_context_locked(
            &mut state,
            Some(top),
            "ErrorContext".to_owned(),
            MemoryContextKind::AllocSet,
            0,
            8 * 1024,
            8 * 1024 * 1024,
        )?;

        state.top = top;
        state.error = error;
        state.current = top;
        state.initialized = true;
        Ok(())
    })
}

pub fn current_memory_context() -> MemoryContext {
    STATE.with(|state| state.borrow().current)
}

pub fn top_memory_context() -> MemoryContext {
    STATE.with(|state| state.borrow().top)
}

pub fn error_context() -> MemoryContext {
    STATE.with(|state| state.borrow().error)
}

pub fn set_current_memory_context(context: MemoryContext) -> PgResult<MemoryContext> {
    validate_context(context)?;
    STATE.with(|state| {
        let mut state = state.borrow_mut();
        let old = state.current;
        state.current = context;
        Ok(old)
    })
}

pub fn create_context(
    parent: Option<MemoryContext>,
    name: String,
    kind: MemoryContextKind,
    min_context_size: Size,
    init_block_size: Size,
    max_block_size: Size,
) -> PgResult<MemoryContext> {
    memory_context_init()?;
    STATE.with(|state| {
        create_context_locked(
            &mut state.borrow_mut(),
            parent,
            name,
            kind,
            min_context_size,
            init_block_size,
            max_block_size,
        )
    })
}

fn create_context_locked(
    _state: &mut State,
    parent: Option<MemoryContext>,
    name: String,
    kind: MemoryContextKind,
    min_context_size: Size,
    init_block_size: Size,
    max_block_size: Size,
) -> PgResult<MemoryContext> {
    if let Some(parent) = parent {
        validate_context(parent)?;
    }

    if kind == MemoryContextKind::AllocSet {
        return aset::create_context(
            parent,
            name,
            min_context_size,
            init_block_size,
            max_block_size,
        );
    }
    if kind == MemoryContextKind::Generation {
        return generation::create_context(
            parent,
            name,
            min_context_size,
            init_block_size,
            max_block_size,
        );
    }
    if let MemoryContextKind::Slab { chunk_size } = kind {
        return slab::create_context(parent, name, init_block_size, chunk_size);
    }
    if kind == MemoryContextKind::Bump {
        return bump::create_context(
            parent,
            name,
            min_context_size,
            init_block_size,
            max_block_size,
        );
    }

    let name = CString::new(name).map_err(|error| PgError::error(error.to_string()))?;
    let type_ = node_tag(kind);
    let method_id = method_id(kind);
    let methods = methods_for(method_id);
    let mut node = Box::new(ContextNode {
        header: MemoryContextData {
            type_,
            isReset: true,
            allowInCritSection: false,
            mem_allocated: std::mem::size_of::<ContextNode>(),
            methods,
            parent: ptr::null_mut(),
            firstchild: ptr::null_mut(),
            prevchild: ptr::null_mut(),
            nextchild: ptr::null_mut(),
            name: name.as_ptr(),
            ident: ptr::null(),
            reset_cbs: ptr::null_mut(),
        },
        kind,
        name,
        ident: None,
        allocations: Vec::new(),
        deleted: false,
    });

    let context = (&mut node.header) as MemoryContext;
    let _leaked = Box::leak(node);
    if let Some(parent) = parent {
        set_parent(context, Some(parent))?;
    }
    Ok(context)
}

pub fn alloc_raw(
    context: MemoryContext,
    size: Size,
    flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    if method_id_for_context(context)? == MCTX_ASET_ID {
        return unsafe { aset::alloc(context, size, flags) };
    }
    if method_id_for_context(context)? == MCTX_GENERATION_ID {
        return unsafe { generation::alloc(context, size, flags) };
    }
    if method_id_for_context(context)? == MCTX_SLAB_ID {
        return unsafe { slab::alloc(context, size, flags) };
    }
    if method_id_for_context(context)? == MCTX_BUMP_ID {
        return unsafe { bump::alloc(context, size, flags) };
    }
    unsafe {
        alloc_with_alignment(
            context,
            size,
            MINIMUM_ALIGN,
            flags,
            method_id_for_context(context)?,
        )
    }
}

pub fn alloc_aligned_raw(
    context: MemoryContext,
    size: Size,
    alignto: Size,
    flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    aligned::alloc(context, size, alignto, flags)
}

unsafe fn alloc_with_alignment(
    context: MemoryContext,
    size: Size,
    alignment: Size,
    flags: i32,
    allocation_method_id: MemoryContextMethodID,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    validate_context(context)?;
    let node = unsafe { context_node_mut(context)? };
    if let MemoryContextKind::Slab { chunk_size } = node.kind {
        if size > chunk_size {
            return Err(PgError::error(format!(
                "requested size {size} exceeds slab chunk size {chunk_size}"
            )));
        }
    }
    let actual_size = effective_size(size);
    if !allocation_size_is_valid(actual_size, flags) {
        if flags & MCXT_ALLOC_NO_OOM != 0 {
            return Ok(None);
        }
        return Err(PgError::error("out of memory"));
    }

    let header_size = std::mem::size_of::<AllocationHeader>();
    let total = header_size
        .checked_add(actual_size)
        .and_then(|size| size.checked_add(alignment))
        .ok_or_else(|| PgError::error("out of memory"))?;
    let layout = Layout::from_size_align(
        total,
        alignment.max(std::mem::align_of::<AllocationHeader>()),
    )
    .map_err(|error| PgError::error(error.to_string()))?;
    let base = unsafe { std::alloc::alloc(layout) };
    if base.is_null() {
        if flags & MCXT_ALLOC_NO_OOM != 0 {
            return Ok(None);
        }
        return Err(PgError::error("out of memory"));
    }

    let start = unsafe { base.add(header_size) };
    let aligned = align_up(start as usize, alignment) as *mut u8;
    let header = unsafe { aligned.sub(header_size).cast::<AllocationHeader>() };
    unsafe {
        ptr::write(
            header,
            AllocationHeader {
                chunk: MemoryChunk::default(),
                context,
                requested_size: size,
                allocated_size: actual_size,
                base: base.cast(),
                method_id: allocation_method_id,
                alignment,
            },
        );
        (*header).chunk.set_hdrmask(
            aligned as usize - base as usize,
            actual_size,
            allocation_method_id,
        );
        if flags & MCXT_ALLOC_ZERO != 0 {
            ptr::write_bytes(aligned, 0, actual_size);
        }
    }

    let header = NonNull::new(header)
        .ok_or_else(|| PgError::error("alloc_with_alignment: allocation header is null"))?;
    let ptr = NonNull::new(aligned.cast())
        .ok_or_else(|| PgError::error("alloc_with_alignment: allocation pointer is null"))?;
    unsafe {
        context_node_mut(context)?.allocations.push(header);
        (*context).isReset = false;
        (*context).mem_allocated = (*context).mem_allocated.saturating_add(total);
    }
    Ok(Some((ptr, actual_size)))
}

pub fn pfree_raw(pointer: *mut c_void) -> PgResult<()> {
    if bump::owns(pointer) {
        return bump::free(pointer);
    }
    let method_id = chunk_method_id(pointer)?;
    if method_id == MCTX_ASET_ID {
        return unsafe { aset::free(pointer) };
    }
    if method_id == MCTX_GENERATION_ID {
        return unsafe { generation::free(pointer) };
    }
    if method_id == MCTX_SLAB_ID {
        return unsafe { slab::free(pointer) };
    }
    if method_id == MCTX_ALIGNED_REDIRECT_ID {
        return aligned::free(pointer);
    }
    let header = unsafe { allocation_header(pointer)? };
    if unsafe { (*header).method_id == MCTX_BUMP_ID } {
        return Err(PgError::error(
            "pfree is not supported by the bump memory allocator",
        ));
    }
    unsafe { free_allocation(header) }
}

pub fn repalloc_raw(
    pointer: *mut c_void,
    size: Size,
    flags: i32,
) -> PgResult<(NonNull<c_void>, Size)> {
    repalloc_extended_raw(pointer, size, flags)?.ok_or_else(|| PgError::error("out of memory"))
}

pub fn repalloc_extended_raw(
    pointer: *mut c_void,
    size: Size,
    flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    if bump::owns(pointer) {
        return bump::realloc(pointer, size, flags);
    }
    let method_id = chunk_method_id(pointer)?;
    if method_id == MCTX_ASET_ID {
        return unsafe { aset::realloc(pointer, size, flags) };
    }
    if method_id == MCTX_GENERATION_ID {
        return unsafe { generation::realloc(pointer, size, flags) };
    }
    if method_id == MCTX_SLAB_ID {
        return unsafe { slab::realloc(pointer, size, flags) };
    }
    if method_id == MCTX_ALIGNED_REDIRECT_ID {
        return aligned::realloc(pointer, size, flags);
    }
    let header = unsafe { allocation_header(pointer)? };
    if unsafe { (*header).method_id == MCTX_BUMP_ID } {
        return Err(PgError::error(
            "repalloc is not supported by the bump memory allocator",
        ));
    }
    let context = unsafe { (*header).context };
    let old_size = unsafe { (*header).allocated_size };
    let alignment = unsafe { (*header).alignment };
    let method_id = unsafe { (*header).method_id };

    let Some((new_ptr, new_size)) =
        (unsafe { alloc_with_alignment(context, size, alignment, flags, method_id)? })
    else {
        return Ok(None);
    };
    unsafe {
        ptr::copy_nonoverlapping(
            pointer.cast::<u8>(),
            new_ptr.as_ptr().cast::<u8>(),
            old_size.min(new_size),
        );
        if new_size > old_size {
            ptr::write_bytes(
                new_ptr.as_ptr().cast::<u8>().add(old_size),
                0,
                new_size - old_size,
            );
        }
        free_allocation(header)?;
    }
    Ok(Some((new_ptr, new_size)))
}

pub fn get_chunk_context(pointer: *mut c_void) -> PgResult<MemoryContext> {
    if bump::owns(pointer) {
        return bump::get_chunk_context(pointer);
    }
    if chunk_method_id(pointer)? == MCTX_ASET_ID {
        return unsafe { aset::get_chunk_context(pointer) };
    }
    if chunk_method_id(pointer)? == MCTX_GENERATION_ID {
        return unsafe { generation::get_chunk_context(pointer) };
    }
    if chunk_method_id(pointer)? == MCTX_SLAB_ID {
        return unsafe { slab::get_chunk_context(pointer) };
    }
    if chunk_method_id(pointer)? == MCTX_ALIGNED_REDIRECT_ID {
        return aligned::get_chunk_context(pointer);
    }
    let header = unsafe { allocation_header(pointer)? };
    Ok(unsafe { (*header).context })
}

pub fn get_chunk_space(pointer: *mut c_void) -> PgResult<Size> {
    if bump::owns(pointer) {
        return bump::get_chunk_space(pointer);
    }
    if chunk_method_id(pointer)? == MCTX_ASET_ID {
        return unsafe { aset::get_chunk_space(pointer) };
    }
    if chunk_method_id(pointer)? == MCTX_GENERATION_ID {
        return unsafe { generation::get_chunk_space(pointer) };
    }
    if chunk_method_id(pointer)? == MCTX_SLAB_ID {
        return unsafe { slab::get_chunk_space(pointer) };
    }
    if chunk_method_id(pointer)? == MCTX_ALIGNED_REDIRECT_ID {
        return aligned::get_chunk_space(pointer);
    }
    let header = unsafe { allocation_header(pointer)? };
    Ok(unsafe { (*header).allocated_size + std::mem::size_of::<AllocationHeader>() })
}

pub fn reset_context(context: MemoryContext) -> PgResult<()> {
    delete_children(context)?;
    reset_context_only(context)
}

pub fn reset_context_only(context: MemoryContext) -> PgResult<()> {
    validate_context(context)?;
    unsafe {
        call_reset_callbacks(context);
        if method_id_for_context(context)? == MCTX_ASET_ID {
            aset::reset(context)?;
            (*context).isReset = true;
            return Ok(());
        }
        if method_id_for_context(context)? == MCTX_GENERATION_ID {
            generation::reset(context)?;
            (*context).isReset = true;
            return Ok(());
        }
        if method_id_for_context(context)? == MCTX_SLAB_ID {
            slab::reset(context)?;
            (*context).isReset = true;
            return Ok(());
        }
        if method_id_for_context(context)? == MCTX_BUMP_ID {
            bump::reset(context)?;
            (*context).isReset = true;
            return Ok(());
        }
        let node = context_node_mut(context)?;
        let allocations = std::mem::take(&mut node.allocations);
        for allocation in allocations {
            free_allocation_no_unlink(allocation.as_ptr())?;
        }
        (*context).isReset = true;
        (*context).mem_allocated = std::mem::size_of::<ContextNode>();
    }
    Ok(())
}

pub fn reset_children(context: MemoryContext) -> PgResult<()> {
    validate_context(context)?;
    let mut child = unsafe { (*context).firstchild };
    while !child.is_null() {
        let next = unsafe { (*child).nextchild };
        reset_context_only(child)?;
        child = next;
    }
    Ok(())
}

pub fn delete_context(context: MemoryContext) -> PgResult<()> {
    validate_context(context)?;
    if context == top_memory_context() {
        return Err(PgError::error("cannot delete TopMemoryContext"));
    }
    unsafe {
        delete_children(context)?;
        unlink_from_parent(context)?;
        call_reset_callbacks(context);
        if method_id_for_context(context)? == MCTX_ASET_ID {
            aset::delete(context)?;
            return Ok(());
        }
        if method_id_for_context(context)? == MCTX_GENERATION_ID {
            generation::delete(context)?;
            return Ok(());
        }
        if method_id_for_context(context)? == MCTX_SLAB_ID {
            slab::delete(context)?;
            return Ok(());
        }
        if method_id_for_context(context)? == MCTX_BUMP_ID {
            bump::delete(context)?;
            return Ok(());
        }
        reset_context_only(context)?;
        context_node_mut(context)?.deleted = true;
    }
    Ok(())
}

pub fn delete_children(context: MemoryContext) -> PgResult<()> {
    validate_context(context)?;
    loop {
        let child = unsafe { (*context).firstchild };
        if child.is_null() {
            break;
        }
        delete_context(child)?;
    }
    Ok(())
}

pub fn register_reset_callback(
    context: MemoryContext,
    cb: &mut MemoryContextCallback,
) -> PgResult<()> {
    validate_context(context)?;
    unsafe {
        cb.next = (*context).reset_cbs;
        (*context).reset_cbs = cb;
    }
    Ok(())
}

unsafe fn call_reset_callbacks(context: MemoryContext) {
    let mut cb = unsafe { (*context).reset_cbs };
    unsafe {
        (*context).reset_cbs = ptr::null_mut();
    }
    while !cb.is_null() {
        let next = unsafe { (*cb).next };
        if let Some(func) = unsafe { (*cb).func } {
            unsafe { func((*cb).arg) };
        }
        cb = next;
    }
}

pub fn set_identifier(context: MemoryContext, ident: String) -> PgResult<()> {
    validate_context(context)?;
    let ident = CString::new(ident).map_err(|error| PgError::error(error.to_string()))?;
    if method_id_for_context(context)? == MCTX_ASET_ID {
        return aset::set_identifier(context, ident);
    }
    if method_id_for_context(context)? == MCTX_GENERATION_ID {
        return generation::set_identifier(context, ident);
    }
    if method_id_for_context(context)? == MCTX_SLAB_ID {
        return slab::set_identifier(context, ident);
    }
    if method_id_for_context(context)? == MCTX_BUMP_ID {
        return bump::set_identifier(context, ident);
    }
    let node = unsafe { context_node_mut(context)? };
    node.ident = Some(ident);
    unsafe {
        (*context).ident = node.ident.as_ref().unwrap().as_ptr();
    }
    Ok(())
}

pub fn set_parent(context: MemoryContext, new_parent: Option<MemoryContext>) -> PgResult<()> {
    validate_context(context)?;
    if let Some(parent) = new_parent {
        validate_context(parent)?;
        if parent == context {
            return Err(PgError::error("memory context cannot parent itself"));
        }
    }

    unsafe { unlink_from_parent(context)? };
    if let Some(parent) = new_parent {
        unsafe {
            (*context).parent = parent;
            (*context).prevchild = ptr::null_mut();
            (*context).nextchild = (*parent).firstchild;
            if !(*parent).firstchild.is_null() {
                (*(*parent).firstchild).prevchild = context;
            }
            (*parent).firstchild = context;
        }
    }
    Ok(())
}

unsafe fn unlink_from_parent(context: MemoryContext) -> PgResult<()> {
    let parent = unsafe { (*context).parent };
    if parent.is_null() {
        return Ok(());
    }
    unsafe {
        if !(*context).prevchild.is_null() {
            (*(*context).prevchild).nextchild = (*context).nextchild;
        } else {
            (*parent).firstchild = (*context).nextchild;
        }
        if !(*context).nextchild.is_null() {
            (*(*context).nextchild).prevchild = (*context).prevchild;
        }
        (*context).parent = ptr::null_mut();
        (*context).prevchild = ptr::null_mut();
        (*context).nextchild = ptr::null_mut();
    }
    Ok(())
}

pub fn is_empty(context: MemoryContext) -> PgResult<bool> {
    validate_context(context)?;
    if method_id_for_context(context)? == MCTX_ASET_ID {
        return unsafe { aset::is_empty(context) };
    }
    if method_id_for_context(context)? == MCTX_GENERATION_ID {
        return unsafe { generation::is_empty(context) };
    }
    if method_id_for_context(context)? == MCTX_SLAB_ID {
        return unsafe { slab::is_empty(context) };
    }
    if method_id_for_context(context)? == MCTX_BUMP_ID {
        return unsafe { bump::is_empty(context) };
    }
    Ok(unsafe { (*context).isReset })
}

pub fn mem_allocated(context: MemoryContext, recurse: bool) -> PgResult<Size> {
    validate_context(context)?;
    let mut total = unsafe { (*context).mem_allocated };
    if recurse {
        let mut child = unsafe { (*context).firstchild };
        while !child.is_null() {
            total = total.saturating_add(mem_allocated(child, true)?);
            child = unsafe { (*child).nextchild };
        }
    }
    Ok(total)
}

pub fn mem_consumed(context: MemoryContext) -> PgResult<MemoryContextCounters> {
    validate_context(context)?;
    let mut counters = mem_consumed_local(context)?;
    let mut child = unsafe { (*context).firstchild };
    while !child.is_null() {
        let child_counters = mem_consumed(child)?;
        counters.nblocks = counters.nblocks.saturating_add(child_counters.nblocks);
        counters.totalspace = counters
            .totalspace
            .saturating_add(child_counters.totalspace);
        child = unsafe { (*child).nextchild };
    }
    Ok(counters)
}

pub fn mem_consumed_local(context: MemoryContext) -> PgResult<MemoryContextCounters> {
    validate_context(context)?;
    if method_id_for_context(context)? == MCTX_ASET_ID {
        return unsafe { aset::stats(context) };
    }
    if method_id_for_context(context)? == MCTX_GENERATION_ID {
        return unsafe { generation::stats(context) };
    }
    if method_id_for_context(context)? == MCTX_SLAB_ID {
        return unsafe { slab::stats(context) };
    }
    if method_id_for_context(context)? == MCTX_BUMP_ID {
        return unsafe { bump::stats(context) };
    }
    let node = unsafe { context_node(context)? };
    Ok(MemoryContextCounters {
        nblocks: node.allocations.len(),
        freechunks: 0,
        totalspace: unsafe { (*context).mem_allocated },
        freespace: 0,
    })
}

pub fn stats_detail(
    context: MemoryContext,
    max_level: usize,
    max_children: usize,
) -> PgResult<String> {
    validate_context(context)?;
    let mut output = String::new();
    let mut grand_totals = MemoryContextCounters::default();
    stats_detail_internal(
        context,
        0,
        max_level,
        max_children,
        &mut grand_totals,
        &mut output,
    )?;
    output.push_str(&format!(
        "Grand total: {} bytes in {} blocks; {} free ({} chunks); {} used\n",
        grand_totals.totalspace,
        grand_totals.nblocks,
        grand_totals.freespace,
        grand_totals.freechunks,
        grand_totals
            .totalspace
            .saturating_sub(grand_totals.freespace)
    ));
    Ok(output)
}

fn stats_detail_internal(
    context: MemoryContext,
    level: usize,
    max_level: usize,
    max_children: usize,
    totals: &mut MemoryContextCounters,
    output: &mut String,
) -> PgResult<()> {
    let local = mem_consumed_local(context)?;
    totals.nblocks = totals.nblocks.saturating_add(local.nblocks);
    totals.freechunks = totals.freechunks.saturating_add(local.freechunks);
    totals.totalspace = totals.totalspace.saturating_add(local.totalspace);
    totals.freespace = totals.freespace.saturating_add(local.freespace);
    output.push_str(&format!(
        "{}{}: {} total in {} blocks; {} free ({} chunks); {} used\n",
        "  ".repeat(level),
        context_display_name(context),
        local.totalspace,
        local.nblocks,
        local.freespace,
        local.freechunks,
        local.totalspace.saturating_sub(local.freespace)
    ));

    if level >= max_level {
        let hidden = child_summary(context)?;
        if hidden.nblocks != 0 || hidden.totalspace != 0 {
            totals.nblocks = totals.nblocks.saturating_add(hidden.nblocks);
            totals.freechunks = totals.freechunks.saturating_add(hidden.freechunks);
            totals.totalspace = totals.totalspace.saturating_add(hidden.totalspace);
            totals.freespace = totals.freespace.saturating_add(hidden.freespace);
            output.push_str(&format!(
                "{}{} child contexts hidden: {} total in {} blocks; {} free ({} chunks); {} used\n",
                "  ".repeat(level + 1),
                child_count(context),
                hidden.totalspace,
                hidden.nblocks,
                hidden.freespace,
                hidden.freechunks,
                hidden.totalspace.saturating_sub(hidden.freespace)
            ));
        }
        return Ok(());
    }

    let mut child = unsafe { (*context).firstchild };
    let mut shown = 0usize;
    let mut hidden = MemoryContextCounters::default();
    let mut hidden_count = 0usize;
    while !child.is_null() {
        let next = unsafe { (*child).nextchild };
        if shown < max_children {
            stats_detail_internal(child, level + 1, max_level, max_children, totals, output)?;
            shown += 1;
        } else {
            let child_total = mem_consumed(child)?;
            hidden.nblocks = hidden.nblocks.saturating_add(child_total.nblocks);
            hidden.freechunks = hidden.freechunks.saturating_add(child_total.freechunks);
            hidden.totalspace = hidden.totalspace.saturating_add(child_total.totalspace);
            hidden.freespace = hidden.freespace.saturating_add(child_total.freespace);
            hidden_count += 1;
        }
        child = next;
    }
    if hidden_count != 0 {
        totals.nblocks = totals.nblocks.saturating_add(hidden.nblocks);
        totals.freechunks = totals.freechunks.saturating_add(hidden.freechunks);
        totals.totalspace = totals.totalspace.saturating_add(hidden.totalspace);
        totals.freespace = totals.freespace.saturating_add(hidden.freespace);
        output.push_str(&format!(
            "{}{hidden_count} more child contexts containing {} total in {} blocks; {} free ({} chunks); {} used\n",
            "  ".repeat(level + 1),
            hidden.totalspace,
            hidden.nblocks,
            hidden.freespace,
            hidden.freechunks,
            hidden.totalspace.saturating_sub(hidden.freespace)
        ));
    }
    Ok(())
}

fn child_summary(context: MemoryContext) -> PgResult<MemoryContextCounters> {
    let mut summary = MemoryContextCounters::default();
    let mut child = unsafe { (*context).firstchild };
    while !child.is_null() {
        let child_total = mem_consumed(child)?;
        summary.nblocks = summary.nblocks.saturating_add(child_total.nblocks);
        summary.freechunks = summary.freechunks.saturating_add(child_total.freechunks);
        summary.totalspace = summary.totalspace.saturating_add(child_total.totalspace);
        summary.freespace = summary.freespace.saturating_add(child_total.freespace);
        child = unsafe { (*child).nextchild };
    }
    Ok(summary)
}

fn child_count(context: MemoryContext) -> usize {
    let mut count = 0;
    let mut child = unsafe { (*context).firstchild };
    while !child.is_null() {
        count += 1;
        child = unsafe { (*child).nextchild };
    }
    count
}

fn context_display_name(context: MemoryContext) -> String {
    unsafe {
        let name = if (*context).name.is_null() {
            "<unnamed>".to_owned()
        } else {
            std::ffi::CStr::from_ptr((*context).name)
                .to_string_lossy()
                .into_owned()
        };
        if (*context).ident.is_null() {
            name
        } else {
            format!(
                "{}: {}",
                name,
                std::ffi::CStr::from_ptr((*context).ident).to_string_lossy()
            )
        }
    }
}

pub fn check_context(context: MemoryContext) -> PgResult<()> {
    validate_context(context)?;
    if method_id_for_context(context)? == MCTX_ASET_ID {
        return unsafe { aset::check(context) };
    }
    if method_id_for_context(context)? == MCTX_GENERATION_ID {
        return unsafe { generation::check(context) };
    }
    if method_id_for_context(context)? == MCTX_SLAB_ID {
        return unsafe { slab::check(context) };
    }
    if method_id_for_context(context)? == MCTX_BUMP_ID {
        return unsafe { bump::check(context) };
    }
    Ok(())
}

pub fn allow_in_critical_section(context: MemoryContext, allow: bool) -> PgResult<()> {
    validate_context(context)?;
    unsafe {
        (*context).allowInCritSection = allow;
    }
    Ok(())
}

unsafe fn free_allocation(header: *mut AllocationHeader) -> PgResult<()> {
    let context = unsafe { (*header).context };
    let node = unsafe { context_node_mut(context)? };
    node.allocations
        .retain(|allocation| allocation.as_ptr() != header);
    unsafe { free_allocation_no_unlink(header)? };
    if node.allocations.is_empty() {
        unsafe {
            (*context).isReset = true;
        }
    }
    Ok(())
}

unsafe fn free_allocation_no_unlink(header: *mut AllocationHeader) -> PgResult<()> {
    let context = unsafe { (*header).context };
    let base = unsafe { (*header).base };
    let allocated_size = unsafe { (*header).allocated_size };
    let alignment = unsafe { (*header).alignment };
    let total = std::mem::size_of::<AllocationHeader>()
        .saturating_add(allocated_size)
        .saturating_add(alignment);
    let layout = Layout::from_size_align(
        total,
        alignment.max(std::mem::align_of::<AllocationHeader>()),
    )
    .map_err(|error| PgError::error(error.to_string()))?;
    if !context.is_null() {
        unsafe {
            (*context).mem_allocated = (*context).mem_allocated.saturating_sub(total);
        }
    }
    unsafe { std::alloc::dealloc(base.cast(), layout) };
    Ok(())
}

unsafe fn allocation_header(pointer: *mut c_void) -> PgResult<*mut AllocationHeader> {
    if pointer.is_null() {
        return Err(PgError::error("memory pointer is null"));
    }
    let header = unsafe {
        pointer
            .cast::<u8>()
            .sub(std::mem::size_of::<AllocationHeader>())
            .cast::<AllocationHeader>()
    };
    let context = unsafe { (*header).context };
    validate_context(context)?;
    let node = unsafe { context_node(context)? };
    if !node
        .allocations
        .iter()
        .any(|allocation| allocation.as_ptr() == header)
    {
        return Err(PgError::error(
            "memory pointer does not belong to an active context",
        ));
    }
    Ok(header)
}

fn validate_context(context: MemoryContext) -> PgResult<()> {
    if context.is_null() {
        return Err(PgError::error("memory context is null"));
    }
    unsafe {
        let type_ = (*context).type_;
        if type_ != T_ALLOC_SET_CONTEXT
            && type_ != T_GENERATION_CONTEXT
            && type_ != T_SLAB_CONTEXT
            && type_ != T_BUMP_CONTEXT
        {
            return Err(PgError::error("memory context has invalid type"));
        }
        if type_ != T_ALLOC_SET_CONTEXT
            && type_ != T_GENERATION_CONTEXT
            && type_ != T_SLAB_CONTEXT
            && type_ != T_BUMP_CONTEXT
            && (*context.cast::<ContextNode>()).deleted
        {
            return Err(PgError::error("memory context has been deleted"));
        }
    }
    Ok(())
}

pub fn validate_context_public(context: MemoryContext) -> PgResult<()> {
    validate_context(context)
}

unsafe fn context_node(context: MemoryContext) -> PgResult<&'static ContextNode> {
    validate_context(context)?;
    Ok(unsafe { &*context.cast::<ContextNode>() })
}

unsafe fn context_node_mut(context: MemoryContext) -> PgResult<&'static mut ContextNode> {
    validate_context(context)?;
    Ok(unsafe { &mut *context.cast::<ContextNode>() })
}

fn node_tag(kind: MemoryContextKind) -> NodeTag {
    match kind {
        MemoryContextKind::AllocSet => T_ALLOC_SET_CONTEXT,
        MemoryContextKind::Generation => T_GENERATION_CONTEXT,
        MemoryContextKind::Slab { .. } => T_SLAB_CONTEXT,
        MemoryContextKind::Bump => T_BUMP_CONTEXT,
    }
}

fn method_id(kind: MemoryContextKind) -> MemoryContextMethodID {
    match kind {
        MemoryContextKind::AllocSet => MCTX_ASET_ID,
        MemoryContextKind::Generation => MCTX_GENERATION_ID,
        MemoryContextKind::Slab { .. } => MCTX_SLAB_ID,
        MemoryContextKind::Bump => MCTX_BUMP_ID,
    }
}

fn method_id_for_context(context: MemoryContext) -> PgResult<MemoryContextMethodID> {
    validate_context(context)?;
    if unsafe { (*context).type_ == T_ALLOC_SET_CONTEXT } {
        return Ok(MCTX_ASET_ID);
    }
    if unsafe { (*context).type_ == T_GENERATION_CONTEXT } {
        return Ok(MCTX_GENERATION_ID);
    }
    if unsafe { (*context).type_ == T_SLAB_CONTEXT } {
        return Ok(MCTX_SLAB_ID);
    }
    if unsafe { (*context).type_ == T_BUMP_CONTEXT } {
        return Ok(MCTX_BUMP_ID);
    }
    let node = unsafe { context_node(context)? };
    Ok(method_id(node.kind))
}

fn methods_for(method_id: MemoryContextMethodID) -> *const MemoryContextMethods {
    match method_id {
        MCTX_ASET_ID => aset::methods(),
        MCTX_GENERATION_ID => generation::methods(),
        MCTX_SLAB_ID => slab::methods(),
        MCTX_BUMP_ID => bump::methods(),
        MCTX_ALIGNED_REDIRECT_ID => &ALIGNED_METHODS,
        _ => ptr::null(),
    }
}

pub unsafe fn memory_context_create_common(
    node: MemoryContext,
    tag: NodeTag,
    method_id: MemoryContextMethodID,
    parent: Option<MemoryContext>,
    name: *const c_char,
) -> PgResult<()> {
    if let Some(parent) = parent {
        validate_context(parent)?;
    }
    unsafe {
        (*node).type_ = tag;
        (*node).isReset = true;
        (*node).allowInCritSection = false;
        (*node).mem_allocated = 0;
        (*node).methods = methods_for(method_id);
        (*node).parent = ptr::null_mut();
        (*node).firstchild = ptr::null_mut();
        (*node).prevchild = ptr::null_mut();
        (*node).nextchild = ptr::null_mut();
        (*node).name = name;
        (*node).ident = ptr::null();
        (*node).reset_cbs = ptr::null_mut();
    }
    if let Some(parent) = parent {
        set_parent(node, Some(parent))?;
    }
    Ok(())
}

fn chunk_method_id(pointer: *mut c_void) -> PgResult<MemoryContextMethodID> {
    if pointer.is_null() {
        return Err(PgError::error("memory pointer is null"));
    }
    let chunk = unsafe {
        pointer
            .cast::<u8>()
            .sub(std::mem::size_of::<MemoryChunk>())
            .cast::<MemoryChunk>()
    };
    let method_id = unsafe { (*chunk).method_id() };
    match method_id {
        MCTX_ASET_ID
        | MCTX_GENERATION_ID
        | MCTX_SLAB_ID
        | MCTX_BUMP_ID
        | MCTX_ALIGNED_REDIRECT_ID => Ok(method_id),
        _ => Err(PgError::error("memory pointer has invalid chunk method")),
    }
}

fn allocation_size_is_valid(size: Size, flags: i32) -> bool {
    if flags & MCXT_ALLOC_HUGE != 0 {
        size <= MAX_ALLOC_HUGE_SIZE
    } else {
        size <= MAX_ALLOC_SIZE
    }
}

fn effective_size(size: Size) -> Size {
    size.max(1)
}

fn align_up(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

unsafe extern "C" fn method_alloc(context: MemoryContext, size: Size, flags: i32) -> *mut c_void {
    match alloc_raw(context, size, flags) {
        Ok(Some((ptr, _))) => ptr.as_ptr(),
        _ => ptr::null_mut(),
    }
}

unsafe extern "C" fn method_free(pointer: *mut c_void) {
    let _ = pfree_raw(pointer);
}

unsafe extern "C" fn method_realloc(pointer: *mut c_void, size: Size, flags: i32) -> *mut c_void {
    match repalloc_extended_raw(pointer, size, flags) {
        Ok(Some((ptr, _))) => ptr.as_ptr(),
        _ => ptr::null_mut(),
    }
}

unsafe extern "C" fn method_reset(context: MemoryContext) {
    let _ = reset_context_only(context);
}

unsafe extern "C" fn method_delete(context: MemoryContext) {
    let _ = delete_context(context);
}

unsafe extern "C" fn method_get_chunk_context(pointer: *mut c_void) -> MemoryContext {
    get_chunk_context(pointer).unwrap_or(ptr::null_mut())
}

unsafe extern "C" fn method_get_chunk_space(pointer: *mut c_void) -> Size {
    get_chunk_space(pointer).unwrap_or(0)
}

unsafe extern "C" fn method_is_empty(context: MemoryContext) -> bool {
    is_empty(context).unwrap_or(false)
}

unsafe extern "C" fn method_stats(
    context: MemoryContext,
    _printfunc: ::pg_ffi_fgram::MemoryStatsPrintFunc,
    _passthru: *mut c_void,
    totals: *mut MemoryContextCounters,
    _print_to_stderr: bool,
) {
    if totals.is_null() {
        return;
    }
    if let Ok(consumed) = mem_consumed(context) {
        unsafe {
            (*totals).nblocks = (*totals).nblocks.saturating_add(consumed.nblocks);
            (*totals).freechunks = (*totals).freechunks.saturating_add(consumed.freechunks);
            (*totals).totalspace = (*totals).totalspace.saturating_add(consumed.totalspace);
            (*totals).freespace = (*totals).freespace.saturating_add(consumed.freespace);
        }
    }
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

const ALIGNED_METHODS: MemoryContextMethods = ALLOC_SET_METHODS;
