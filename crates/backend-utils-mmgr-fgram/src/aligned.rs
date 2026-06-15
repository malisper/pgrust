use std::ffi::c_void;
use std::ptr::{self, NonNull};

use backend_utils_error::{PgError, PgResult};
use pgrust_pg_ffi::{
    MemoryChunk, MemoryContext, Size, MCTX_ALIGNED_REDIRECT_ID, MCXT_ALLOC_NO_OOM,
};

const MINIMUM_ALIGN: usize = std::mem::align_of::<usize>();

pub fn extra_bytes(alignto: Size) -> Size {
    alignto + std::mem::size_of::<MemoryChunk>() - MINIMUM_ALIGN
}

pub fn alloc(
    context: MemoryContext,
    size: Size,
    alignto: Size,
    flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    let alignment = alignto.max(MINIMUM_ALIGN);
    if !alignment.is_power_of_two() {
        return Err(PgError::error(format!(
            "invalid allocation alignment: {alignment}"
        )));
    }
    if alignment <= MINIMUM_ALIGN {
        return crate::raw::alloc_raw(context, size, flags);
    }

    let alloc_size = size
        .checked_add(extra_bytes(alignment))
        .ok_or_else(|| PgError::error("out of memory"))?;
    let Some((unaligned, _)) = crate::raw::alloc_raw(context, alloc_size, flags)? else {
        return Ok(None);
    };

    let aligned = unsafe {
        unaligned
            .as_ptr()
            .cast::<u8>()
            .add(std::mem::size_of::<MemoryChunk>())
    };
    let aligned = align_up(aligned as usize, alignment) as *mut c_void;
    let aligned_chunk = pointer_get_memory_chunk(aligned);
    let block_offset = (aligned_chunk as usize)
        .checked_sub(unaligned.as_ptr() as usize)
        .ok_or_else(|| PgError::error("invalid aligned allocation offset"))?;
    unsafe {
        (*aligned_chunk).set_hdrmask(block_offset, alignment, MCTX_ALIGNED_REDIRECT_ID);
    }

    Ok(Some((
        NonNull::new(aligned)
            .ok_or_else(|| PgError::error("alloc: aligned allocation pointer is null"))?,
        size,
    )))
}

pub fn free(pointer: *mut c_void) -> PgResult<()> {
    let unaligned = unsafe { unaligned_pointer(pointer) };
    crate::raw::pfree_raw(unaligned)
}

pub fn realloc(
    pointer: *mut c_void,
    size: Size,
    flags: i32,
) -> PgResult<Option<(NonNull<c_void>, Size)>> {
    let redir_chunk = pointer_get_memory_chunk(pointer);
    let alignto = unsafe { (*redir_chunk).value() };
    let unaligned = unsafe { unaligned_pointer(pointer) };
    let old_size = crate::raw::get_chunk_space(unaligned)?
        .saturating_sub(extra_bytes(alignto))
        .saturating_sub(std::mem::size_of::<MemoryChunk>());
    let context = crate::raw::get_chunk_context(unaligned)?;

    let Some((new_pointer, new_size)) = alloc(context, size, alignto, flags)? else {
        if flags & MCXT_ALLOC_NO_OOM != 0 {
            return Ok(None);
        }
        return Err(PgError::error("out of memory"));
    };
    unsafe {
        ptr::copy_nonoverlapping(
            pointer.cast::<u8>(),
            new_pointer.as_ptr().cast::<u8>(),
            old_size.min(size),
        );
    }
    crate::raw::pfree_raw(unaligned)?;
    Ok(Some((new_pointer, new_size)))
}

pub fn get_chunk_context(pointer: *mut c_void) -> PgResult<MemoryContext> {
    let unaligned = unsafe { unaligned_pointer(pointer) };
    crate::raw::get_chunk_context(unaligned)
}

pub fn get_chunk_space(pointer: *mut c_void) -> PgResult<Size> {
    let unaligned = unsafe { unaligned_pointer(pointer) };
    crate::raw::get_chunk_space(unaligned)
}

fn pointer_get_memory_chunk(pointer: *mut c_void) -> *mut MemoryChunk {
    unsafe {
        pointer
            .cast::<u8>()
            .sub(std::mem::size_of::<MemoryChunk>())
            .cast()
    }
}

unsafe fn unaligned_pointer(pointer: *mut c_void) -> *mut c_void {
    let chunk = pointer_get_memory_chunk(pointer);
    unsafe { chunk.cast::<u8>().sub((*chunk).block_offset()).cast() }
}

fn align_up(value: usize, alignment: usize) -> usize {
    (value + alignment - 1) & !(alignment - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_extra_bytes_matches_postgres_macro_shape() {
        assert_eq!(
            extra_bytes(64),
            64 + std::mem::size_of::<MemoryChunk>() - MINIMUM_ALIGN
        );
    }
}
